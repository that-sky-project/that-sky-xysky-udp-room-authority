import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import type { Duplex } from "node:stream";
import { WebSocket, WebSocketServer, type RawData } from "ws";
import { randomUUID } from "node:crypto";
import { jsonStringify } from "../infra/ws/json.js";
import type { Logger } from "../infra/logger/logger.js";
import type { ManagerConfig } from "../config/index.js";
import type { ManagerRuntime } from "./ManagerRuntime.js";
import type { NodeTransport } from "./NodeRegistry.js";
import { HeartbeatMonitor } from "./HeartbeatMonitor.js";
import { MetricsRegistry, registerManagerMetrics } from "./MetricsRegistry.js";
import { asNodeId, asPlayerId, asProtocolVersion, asRoomId, asUnixMs } from "../types/branded.js";
import { AllocationRequestSchema, NodeAckSchema, NodeEventSchema, type IManagerCommand, type INodeEvent } from "../types/contracts.js";
import { ErrorCode, HermesError, isHermesError } from "../types/errors.js";

interface NodeConnectionState {
  nodeId: ReturnType<typeof asNodeId>;
  protocolVersion: number;
  connectedAt: number;
  remoteAddress: string;
}

/** Events that can change the room/level landscape and therefore re-plan. */
const isPlanningEvent = (event: INodeEvent): boolean =>
  event.event === "room.telemetry" || event.event === "player.join" || event.event === "player.leave";

export class ManagerGateway {
  private server: Server | undefined;
  private nodeServer: WebSocketServer | undefined;
  private readonly heartbeat: HeartbeatMonitor;
  private readonly metrics: MetricsRegistry;
  private graceSweepTimer: NodeJS.Timeout | undefined;
  /** Per-room telemetry sequence guard: rejects stale/duplicate snapshots. */
  private readonly telemetrySeq = new Map<string, number>();

  public constructor(
    private readonly config: ManagerConfig,
    private readonly runtime: ManagerRuntime,
    private readonly log: Logger
  ) {
    this.metrics = runtime.metrics;
    registerManagerMetrics(this.metrics, log);
    this.heartbeat = new HeartbeatMonitor(
      {
        intervalMs: config.nodeHeartbeatIntervalMs,
        timeoutMs: config.nodeHeartbeatTimeoutMs,
        onDead: (nodeId) => this.handleNodeUnresponsive(nodeId)
      },
      log
    );
  }

  public async start(): Promise<void> {
    if (this.server) return;

    const server = createServer((req, res) => {
      this.handleHttp(req, res).catch((error) => this.log.error({ err: error }, "http handler error"));
    });
    const nodeServer = new WebSocketServer({ noServer: true, maxPayload: 512 * 1024, perMessageDeflate: false });
    server.on("upgrade", (req, socket, head) => this.handleUpgrade(nodeServer, req, socket, head));
    server.on("error", (error) => this.log.error({ err: error }, "manager HTTP server error"));

    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(this.config.port, this.config.host, () => {
        server.off("error", reject);
        resolve();
      });
    });

    this.server = server;
    this.nodeServer = nodeServer;
    this.heartbeat.start();
    // Reclaim nodes whose grace window keeps expiring: this catches the case
    // where a node never reconnects.
    this.graceSweepTimer = setInterval(() => this.sweepSuspectedNodes(), 2_000);
    this.graceSweepTimer.unref?.();
    this.log.info({ host: this.config.host, port: this.config.port }, "manager gateway listening");
  }

  public async stop(): Promise<void> {
    const server = this.server;
    const nodeServer = this.nodeServer;
    this.server = undefined;
    this.nodeServer = undefined;

    this.heartbeat.stop();
    if (this.graceSweepTimer) clearInterval(this.graceSweepTimer);
    this.graceSweepTimer = undefined;

    if (nodeServer) {
      for (const socket of nodeServer.clients) socket.close(1001, "manager shutting down");
      nodeServer.close();
    }
    if (server) {
      await new Promise<void>((resolve, reject) => {
        server.close((error) => error ? reject(error) : resolve());
      });
    }
  }

  /**
   * Recompute point-in-time gauges. `level_mix_violations` and
   * `players_stranded` are the two numbers that reveal a broken migration
   * loop: mixed rooms that never get split, and players that want to move but
   * have nowhere to go (the cluster needs more empty nodes).
   */
  private refreshGauges(): void {
    const rooms = this.runtime.rooms.listRooms();
    const levelsByRoom = this.runtime.rooms.levelsByRoom();
    this.metrics.set("qwd_rooms", rooms.length);
    this.metrics.set("qwd_players", this.runtime.rooms.listRooms().reduce((sum, room) => sum + room.players, 0));

    let mixed = 0;
    let stranded = 0;
    for (const room of rooms) {
      const levels = levelsByRoom.get(room.roomId);
      if (!levels || levels.size === 0) continue;
      if (levels.size > 1) {
        mixed += 1;
        // Every player in a mixed room that is not part of the majority level
        // is waiting for a target; the planner reports nothing when there is
        // none, so derive it from the same data the planner uses.
        const counts = new Map<number, number>();
        for (const level of levels) counts.set(level, 0);
        for (const location of this.runtime.rooms.listPlayers(room.roomId)) {
          if (location.level === undefined) continue;
          counts.set(location.level, (counts.get(location.level) ?? 0) + 1);
        }
        const sorted = [...counts.values()].sort((a, b) => b - a);
        const majority = sorted[0] ?? 0;
        const total = sorted.reduce((sum, value) => sum + value, 0);
        stranded += Math.max(0, total - majority);
      }
    }
    this.metrics.set("qwd_level_mix_violations", mixed);
    this.metrics.set("qwd_players_stranded", stranded);
  }

  private sweepSuspectedNodes(): void {
    // Players whose leave grace window expired are gone for good; dropping
    // them here keeps room occupancy, level groups and /debug/allnode honest
    // even when nobody reads the room in between.
    this.runtime.rooms.purgeDepartedPlayers();
    for (const { nodeId, rooms } of this.runtime.rooms.sweepSuspectedNodes()) {
      this.metrics.inc("qwd_node_reclaimed_total");
      this.log.warn({ nodeId, rooms: rooms.map((room) => room.roomId) }, "node grace window expired; rooms reclaimed");
    }
  }

  /** Heartbeat declared a node dead: reuse the normal disconnect path. */
  private handleNodeUnresponsive(nodeId: string): void {
    this.metrics.inc("qwd_node_heartbeat_dead_total");
    this.handleNodeDisconnect(asNodeId(nodeId));
  }

  private async handleHttp(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const method = req.method ?? "GET";
    let url: URL;
    try {
      url = new URL(req.url ?? "/", "http://manager.local");
    } catch {
      this.writeJson(res, 400, { code: ErrorCode.BAD_REQUEST, data: { message: "invalid request URL" } });
      return;
    }

    try {
      if (method === "POST" && url.pathname === this.config.allocatePath) {
        await this.handleAllocationBody(res, await this.readBody(req, 16 * 1024));
        return;
      }
      if (method === "POST" && url.pathname === "/telemetry") {
        const parsed = NodeEventSchema.parse(JSON.parse(await this.readBody(req, 128 * 1024)));
        const nodeId = parsed.data.nodeId ?? asNodeId(`http-${parsed.data.roomId ?? randomUUID()}`);
        const event: INodeEvent = { ...parsed, data: { ...parsed.data, nodeId } };
        if (this.applyNodeEvent(event) && isPlanningEvent(event)) {
          this.runtime.moves.planAndExecute().catch((error) => this.log.error({ err: error }, "plan cycle error"));
        }
        this.writeJson(res, 200, { code: ErrorCode.OK });
        return;
      }
      if (method === "GET" && url.pathname === "/healthz") {
        const nodes = this.runtime.nodes.listHealth();
        const healthy = nodes.filter((node) => node.connected && !node.draining).length;
        this.writeJson(res, healthy > 0 || nodes.length === 0 ? 200 : 503, {
          status: healthy > 0 || nodes.length === 0 ? "ok" : "degraded",
          nodes: { total: nodes.length, healthy },
          rooms: this.runtime.rooms.listRooms().length,
          uptimeMs: process.uptime() * 1000
        });
        return;
      }
      if (method === "GET" && url.pathname === "/metrics") {
        this.refreshGauges();
        res.writeHead(200, { "content-type": "text/plain; version=0.0.4; charset=utf-8" });
        res.end(this.metrics.render());
        return;
      }
      if (method === "GET" && url.pathname === "/moves") {
        this.writeJson(res, 200, { moves: this.runtime.moves.list() });
        return;
      }
      if (method === "GET" && url.pathname === "/activity") {
        const moves = this.runtime.moves;
        const rooms = moves.activitySnapshot().map((activity) => ({ activity, players: moves.telemetry.listRoom(asRoomId(activity.roomId)) }));
        this.writeJson(res, 200, { rooms });
        return;
      }
      if (method === "GET" && url.pathname === "/debug/allnode") {
        this.requireDebugToken(req);
        const nodes = this.runtime.nodes.listHealth().map(({ nodeId: internalNodeId, ...node }) => ({
          ...node,
          rooms: this.runtime.rooms.listRoomsByNode(internalNodeId).map((room) => {
            const { nodeId: _roomOwner, ...roomData } = room;
            const players = this.runtime.rooms.listPlayers(room.roomId).map((location) => ({
              playerId: location.playerId,
              updatedAt: location.updatedAt,
              telemetry: this.runtime.moves.telemetry.get(location.playerId) ?? null
            }));
            return { ...roomData, playerCount: players.length, players };
          })
        }));
        this.writeJson(res, 200, { code: ErrorCode.OK, data: { nodes } });
        return;
      }
      if (method === "POST" && url.pathname === "/debug/move") {
        this.requireDebugToken(req);
        const input = JSON.parse(await this.readBody(req, 32 * 1024)) as Record<string, unknown>;
        const sourceRoomId = typeof input.sourceRoomId === "string" ? input.sourceRoomId : "";
        const targetRoomId = typeof input.targetRoomId === "string" ? input.targetRoomId : "";
        const playerIds = Array.isArray(input.playerIds) ? input.playerIds.filter((id): id is string => typeof id === "string") : [];
        const moveInput: { sourceRoomId: string; targetRoomId: string; playerIds: string[]; reason?: string } = { sourceRoomId, targetRoomId, playerIds };
        if (typeof input.reason === "string") moveInput.reason = input.reason;
        const transaction = await this.runtime.moves.manualMove(moveInput);
        this.writeJson(res, 200, { code: ErrorCode.OK, data: transaction });
        return;
      }

      const roomMatch = method === "GET" ? /^\/players\/([^/]+)\/room$/.exec(url.pathname) : undefined;
      if (roomMatch) {
        const encodedPlayerId = roomMatch[1];
        if (!encodedPlayerId) throw new HermesError(ErrorCode.BAD_REQUEST, "playerId is required");
        const playerId = asPlayerId(decodeURIComponent(encodedPlayerId));

        const location = this.runtime.rooms.getPlayerLocation(playerId);
        if (!location) throw new HermesError(ErrorCode.NOT_FOUND, "player is not in any known room", { playerId });

        const room = this.runtime.rooms.getRoom(location.roomId);
        if (!room) throw new HermesError(ErrorCode.NOT_FOUND, "player room is no longer available", { playerId, roomId: location.roomId });

        const telemetry = this.runtime.moves.telemetry.get(playerId);
        this.writeJson(res, 200, {
          code: ErrorCode.OK,
          data: {
            playerId,
            roomId: room.roomId,
            udp: { host: room.udpHost, port: room.udpPort },
            players: room.players,
            capacity: room.capacity,
            level: location.level ?? null,
            levelId: telemetry?.levelId ?? null,
            updatedAt: location.updatedAt
          }
        });
        return;
      }

      this.writeJson(res, 404, { code: ErrorCode.NOT_FOUND, data: { message: "route not found" } });
    } catch (error) {
      const tooLarge = error instanceof HermesError && error.message.endsWith("body too large");
      this.writeJson(res, tooLarge ? 413 : isHermesError(error) ? 400 : 500, this.errorReply(error));
    }
  }

  private handleUpgrade(nodeServer: WebSocketServer, req: IncomingMessage, socket: Duplex, head: Buffer): void {
    let url: URL;
    try {
      url = new URL(req.url ?? "/", "http://manager.local");
    } catch {
      this.rejectUpgrade(socket, 400, "invalid node request URL");
      return;
    }
    if (url.pathname !== "/" && url.pathname !== "") {
      this.rejectUpgrade(socket, 404, "route not found");
      return;
    }

    const protocolVersion = Number(url.searchParams.get("v") ?? "1");
    if (!Number.isInteger(protocolVersion) || protocolVersion < 1) {
      this.rejectUpgrade(socket, 401, "invalid node handshake");
      return;
    }

    const state: NodeConnectionState = {
      nodeId: asNodeId(`connection-${randomUUID()}`),
      protocolVersion,
      connectedAt: Date.now(),
      remoteAddress: req.socket.remoteAddress ?? ""
    };
    nodeServer.handleUpgrade(req, socket, head, (ws) => this.openNode(ws, state));
  }

  private openNode(ws: WebSocket, state: NodeConnectionState): void {
    let closed = false;
    const nodes = this.runtime.nodes;
    const backpressureLimitBytes = this.config.nodeBackpressureLimitBytes;
    const transport: NodeTransport = {
      nodeId: state.nodeId,
      protocolVersion: state.protocolVersion,
      connectedAt: asUnixMs(state.connectedAt),
      remoteAddress: state.remoteAddress,
      get writable(): boolean {
        return !closed && ws.readyState === WebSocket.OPEN && ws.bufferedAmount < backpressureLimitBytes;
      },
      send: async (command: IManagerCommand): Promise<void> => {
        if (closed || ws.readyState !== WebSocket.OPEN) {
          throw new HermesError(ErrorCode.NODE_UNAVAILABLE, "node socket is closed", { nodeId: state.nodeId });
        }
        const before = ws.bufferedAmount;
        nodes.recordBackpressure(state.nodeId, before);
        if (before >= backpressureLimitBytes) {
          throw new HermesError(ErrorCode.NODE_UNAVAILABLE, "node socket is under backpressure", { nodeId: state.nodeId, bufferedBytes: before });
        }
        await new Promise<void>((resolve, reject) => {
          ws.send(jsonStringify({ ...command, v: command.v ?? asProtocolVersion(1) }), (error) => error ? reject(error) : resolve());
        });
        nodes.recordBackpressure(state.nodeId, ws.bufferedAmount);
      },
      close: (code = 1000, reason = "closed"): void => {
        closed = true;
        ws.close(code, reason);
      }
    };

    this.runtime.nodes.register(transport);
    this.heartbeat.track(state.nodeId, ws);
    // A reconnect (same public UDP endpoint) restores its rooms instead of
    // letting the grace sweep reap them.
    if (this.runtime.rooms.restoreNode(state.nodeId)) {
      this.log.info({ nodeId: state.nodeId }, "node reconnected within grace window; state restored");
    }
    this.log.info({ nodeId: state.nodeId, protocolVersion: state.protocolVersion }, "node connected");
    ws.on("message", (message, isBinary) => this.handleNodeMessage(ws, state, message, isBinary).catch((error) => this.log.error({ err: error }, "node message handler error")));
    ws.on("close", () => {
      closed = true;
      this.heartbeat.untrack(state.nodeId);
      this.handleNodeDisconnect(state.nodeId);
    });
    ws.on("error", (error) => this.log.warn({ err: error, nodeId: state.nodeId }, "node socket error"));
  }

  /**
   * Common disconnect path for a socket close and for a heartbeat timeout.
   * Rooms and player locations are retained for a grace window so a transient
   * blip does not 404 live players; the sweep reclaims them if the node never
   * comes back.
   */
  private handleNodeDisconnect(nodeId: ReturnType<typeof asNodeId>): void {
    if (!this.runtime.nodes.getTransport(nodeId)) {
      // Already unregistered (double close), nothing to do.
      return;
    }
    this.runtime.nodes.unregister(nodeId);
    this.runtime.rooms.markNodeSuspected(nodeId);
    for (const roomId of this.telemetrySeq.keys()) {
      const room = this.runtime.rooms.getRoom(asRoomId(roomId));
      if (!room || room.nodeId === nodeId) this.telemetrySeq.delete(roomId);
    }
    this.log.warn({ nodeId, graceMs: this.config.nodeGraceMs }, "node disconnected; state retained for grace window");
  }

  private async handleNodeMessage(ws: WebSocket, state: NodeConnectionState, message: RawData, isBinary: boolean): Promise<void> {
    try {
      if (isBinary) throw new HermesError(ErrorCode.PROTOCOL_ERROR, "node messages must be JSON text");
      const text = Buffer.isBuffer(message)
        ? message.toString("utf8")
        : Array.isArray(message)
          ? Buffer.concat(message).toString("utf8")
          : Buffer.from(message).toString("utf8");
      const raw = JSON.parse(text) as unknown;
      const ack = NodeAckSchema.safeParse(raw);
      if (ack.success) {
        this.runtime.pending.resolve(ack.data);
        this.runtime.nodes.onAck(ack.data);
        return;
      }

      const parsed = NodeEventSchema.parse(raw);
      const event: INodeEvent = { ...parsed, data: { ...parsed.data, nodeId: state.nodeId } };

      if (this.rejectsRoomIdConflict(event, ws)) return;

      if (this.applyNodeEvent(event) && isPlanningEvent(event)) {
        this.runtime.moves.planAndExecute().catch((error) => this.log.error({ err: error }, "plan cycle error"));
      }
    } catch (error) {
      this.log.warn({ err: error, nodeId: state.nodeId }, "invalid node message");
      if (ws.readyState === WebSocket.OPEN) ws.send(jsonStringify(this.errorReply(error)));
    }
  }

  /**
   * Two rooms may not claim the same id from different UDP endpoints: the
   * client would be sent to an address the manager does not control.
   */
  private rejectsRoomIdConflict(event: INodeEvent, ws: WebSocket): boolean {
    if (event.event !== "room.created" || !event.data.roomId) return false;
    const existing = this.runtime.rooms.getRoom(event.data.roomId);
    if (!existing) return false;
    if (existing.udpHost === event.data.udpHost && existing.udpPort === event.data.port) return false;
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(jsonStringify({ v: 1, id: 0, cmd: "room.id_conflict", data: { roomId: event.data.roomId } }));
    }
    this.log.warn(
      { roomId: event.data.roomId, existingUdpHost: existing.udpHost, existingUdpPort: existing.udpPort, udpHost: event.data.udpHost, udpPort: event.data.port },
      "room id conflict rejected"
    );
    return true;
  }

  /**
   * Single ingress for node events, shared by the WebSocket and the HTTP
   * `/telemetry` fallback. Keeping one path is what stops the two from
   * drifting: the HTTP route previously skipped the sequence guard and the
   * reservation confirmation.
   *
   * Returns false when the event was dropped as stale.
   */
  private applyNodeEvent(event: INodeEvent): boolean {
    // Telemetry carries a per-room monotonic sequence. Dropping a stale or
    // duplicated snapshot prevents an out-of-order HTTP-fallback delivery from
    // rolling the player table backwards.
    if (event.event === "room.telemetry" && event.data.roomId && typeof event.data.seq === "number") {
      const roomKey = event.data.roomId as string;
      const last = this.telemetrySeq.get(roomKey);
      if (last !== undefined && event.data.seq <= last) {
        this.metrics.inc("qwd_telemetry_stale_total");
        return false;
      }
      this.telemetrySeq.set(roomKey, event.data.seq);
    }

    this.runtime.nodes.applyEvent(event);
    this.runtime.rooms.applyNodeEvent(event);
    this.runtime.moves.ingest(event);
    if (event.data.player?.playerId && (event.event === "player.join" || event.event === "player.leave")) {
      this.runtime.reservations.confirm(event.data.player.playerId);
    }
    return true;
  }

  private async handleAllocationBody(res: ServerResponse, body: string): Promise<void> {
    const abort = AbortSignal.timeout(this.config.commandTimeoutMs);
    try {
      const request = AllocationRequestSchema.parse(this.parseJsonBody(body));
      this.writeJson(res, 200, await this.runtime.router.allocate(request, abort));
    } catch (error) {
      this.writeJson(res, 200, this.errorReply(error));
    }
  }

  private requireDebugToken(req: IncomingMessage): void {
    if (!this.config.debugApiToken) return;
    const bearer = req.headers.authorization?.startsWith("Bearer ") ? req.headers.authorization.slice(7) : undefined;
    const supplied = req.headers["x-debug-token"] ?? bearer;
    if (typeof supplied !== "string" || supplied !== this.config.debugApiToken) throw new HermesError(ErrorCode.UNAUTHORIZED, "invalid debug API token");
  }

  private readBody(req: IncomingMessage, maxBytes: number): Promise<string> {
    return new Promise((resolve, reject) => {
      const chunks: Buffer[] = [];
      let totalBytes = 0;
      let settled = false;
      const fail = (error: Error): void => {
        if (settled) return;
        settled = true;
        reject(error);
      };
      req.on("data", (chunk: Buffer) => {
        if (settled) return;
        totalBytes += chunk.byteLength;
        if (totalBytes > maxBytes) {
          fail(new HermesError(ErrorCode.BAD_REQUEST, "request body too large"));
          req.resume();
          return;
        }
        chunks.push(chunk);
      });
      req.once("end", () => {
        if (settled) return;
        settled = true;
        resolve(Buffer.concat(chunks).toString("utf8"));
      });
      req.once("aborted", () => fail(new HermesError(ErrorCode.BAD_REQUEST, "request aborted")));
      req.once("error", (error) => fail(error));
    });
  }

  private rejectUpgrade(socket: Duplex, status: number, message: string): void {
    socket.write(`HTTP/1.1 ${status} ${message}\r\nConnection: close\r\nContent-Length: ${Buffer.byteLength(message)}\r\n\r\n${message}`);
    socket.destroy();
  }

  private writeJson(res: ServerResponse, status: number, value: unknown): void {
    if (res.writableEnded || res.destroyed) return;
    const body = jsonStringify(value);
    res.writeHead(status, { "content-type": "application/json; charset=utf-8", "content-length": Buffer.byteLength(body) });
    res.end(body);
  }

  private errorReply(error: unknown): { code: number; data: { message: string } } {
    if (isHermesError(error)) return { code: error.code, data: { message: error.message } };
    if (error instanceof Error) return { code: ErrorCode.INTERNAL, data: { message: error.message } };
    return { code: ErrorCode.INTERNAL, data: { message: String(error) } };
  }

  private parseJsonBody(body: string): unknown {
    try {
      return JSON.parse(body) as unknown;
    } catch (error) {
      throw new HermesError(ErrorCode.BAD_REQUEST, "invalid allocation json body", error);
    }
  }
}
