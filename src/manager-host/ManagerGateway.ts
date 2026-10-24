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

/** Compact, log-safe view of an outbound command's routing fields. */
const summarizeCommand = (command: IManagerCommand): {
  roomId?: string | undefined;
  fromRoomId?: string | undefined;
  moveId?: string | undefined;
  playerIds?: string[] | undefined;
  expectedVersion?: number | undefined;
} => {
  const data = (command.data ?? {}) as Record<string, unknown>;
  const text = (key: string): string | undefined => (typeof data[key] === "string" ? data[key] : undefined);
  const playerIds = Array.isArray(data.playerIds)
    ? data.playerIds.filter((id): id is string => typeof id === "string")
    : typeof data.playerId === "string"
      ? [data.playerId]
      : undefined;
  return {
    roomId: text("roomId") ?? text("toRoomId"),
    fromRoomId: text("fromRoomId"),
    moveId: text("moveId"),
    playerIds,
    expectedVersion: typeof data.expectedVersion === "number" ? data.expectedVersion : undefined
  };
};

export class ManagerGateway {
  private server: Server | undefined;
  private nodeServer: WebSocketServer | undefined;
  private readonly heartbeat: HeartbeatMonitor;
  private readonly metrics: MetricsRegistry;
  private graceSweepTimer: NodeJS.Timeout | undefined;
  /** Per-room telemetry sequence guard: rejects stale/duplicate snapshots. */
  private readonly telemetrySeq = new Map<string, number>();
  /** Rate limit for the sequence-regression warning, per room. */
  private readonly telemetryStaleWarnAt = new Map<string, number>();
  /** In-flight outbound commands, so an ack can be logged with its command name. */
  private readonly outbound = new Map<number, { cmd: string; at: number; roomId?: string | undefined; fromRoomId?: string | undefined; moveId?: string | undefined; playerIds?: string[] | undefined }>();

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
    this.metrics.set("qwd_telemetry_gap_rooms", this.runtime.moves.telemetryGapCount());
  }

  private sweepSuspectedNodes(): void {
    // Players whose leave grace window expired are gone for good; dropping
    // them here keeps room occupancy, level groups and /debug/allnode honest
    // even when nobody reads the room in between.
    this.runtime.rooms.purgeDepartedPlayers();
    for (const { nodeId, rooms } of this.runtime.rooms.sweepSuspectedNodes()) {
      // The rooms are gone for good; drop the coordinator's telemetry
      // bookkeeping too so a future room id reuse starts clean.
      for (const room of rooms) this.runtime.moves.forgetRoom(room.roomId as string);
      this.metrics.inc("qwd_node_reclaimed_total");
      this.log.warn({ nodeId, rooms: rooms.map((room) => room.roomId) }, "node grace window expired; rooms reclaimed");
      this.runtime.events.record({
        category: "node",
        event: "node.reclaimed",
        message: `node ${nodeId} reclaimed after the grace window (${rooms.length} room(s))`,
        nodeId,
        data: { rooms: rooms.map((room) => room.roomId) }
      });
    }
  }

  /** Bound the in-flight command map: acks normally delete entries, timeouts do not. */
  private pruneOutbound(now: number): void {
    if (this.outbound.size < 5000) return;
    for (const [id, entry] of this.outbound) {
      if (now - entry.at > 60_000) this.outbound.delete(id);
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
      if (method === "GET" && url.pathname === "/debug/logs") {
        this.requireDebugToken(req);
        const rawLimit = Number(url.searchParams.get("limit") ?? "");
        const rawSince = Number(url.searchParams.get("since") ?? "");
        const page = this.runtime.events.list({
          limit: Number.isFinite(rawLimit) && rawLimit > 0 ? rawLimit : undefined,
          since: Number.isFinite(rawSince) && rawSince > 0 ? rawSince : undefined,
          category: url.searchParams.get("category") ?? undefined,
          roomId: url.searchParams.get("roomId") ?? undefined,
          playerId: url.searchParams.get("playerId") ?? undefined
        });
        this.writeJson(res, 200, { code: ErrorCode.OK, data: page });
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

    const requestedNodeId = url.searchParams.get("node") ?? "";
    // A node reports one id for the whole lifetime of its process. Without it
    // every reconnect arrives as a brand-new node, so its rooms can only sit
    // under a dead connection id until the grace sweep reclaims them — the
    // restore path below could never run.
    const nodeId = /^[A-Za-z0-9._-]{1,128}$/.test(requestedNodeId)
      ? asNodeId(requestedNodeId)
      : asNodeId(`connection-${randomUUID()}`);
    const state: NodeConnectionState = {
      nodeId,
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
        const summary = summarizeCommand(command);
        this.pruneOutbound(Date.now());
        this.outbound.set(Number(command.id), { cmd: command.cmd, at: Date.now(), ...summary });
        this.runtime.events.record({
          category: "node",
          event: "node.command",
          message: `${command.cmd} -> node ${state.nodeId}`,
          nodeId: state.nodeId,
          roomId: summary.roomId,
          playerIds: summary.playerIds,
          data: { commandId: Number(command.id), cmd: command.cmd, ...summary }
        });
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
    const restored = this.runtime.rooms.restoreNode(state.nodeId);
    if (restored) {
      this.log.info({ nodeId: state.nodeId }, "node reconnected within grace window; state restored");
    }
    this.log.info({ nodeId: state.nodeId, protocolVersion: state.protocolVersion }, "node connected");
    this.runtime.events.record({
      category: "node",
      event: "node.connected",
      message: `node ${state.nodeId} connected${restored ? " (state restored)" : ""}`,
      nodeId: state.nodeId,
      data: { protocolVersion: state.protocolVersion, remoteAddress: state.remoteAddress, restored }
    });
    ws.on("message", (message, isBinary) => this.handleNodeMessage(ws, state, message, isBinary).catch((error) => this.log.error({ err: error }, "node message handler error")));
    ws.on("close", () => {
      closed = true;
      this.heartbeat.untrack(state.nodeId, ws);
      // With a stable node id a reconnect can register a new transport before
      // the old socket's close event is delivered. Only the connection that
      // still owns the id may tear the node down.
      if (this.runtime.nodes.getTransport(state.nodeId) !== transport) {
        this.log.info({ nodeId: state.nodeId }, "stale node socket closed; a newer connection owns the id");
        return;
      }
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
      if (!room || room.nodeId === nodeId) {
        this.telemetrySeq.delete(roomId);
        this.telemetryStaleWarnAt.delete(roomId);
      }
    }
    this.log.warn({ nodeId, graceMs: this.config.nodeGraceMs }, "node disconnected; state retained for grace window");
    this.runtime.events.record({
      category: "node",
      event: "node.disconnected",
      message: `node ${nodeId} disconnected; rooms retained for the grace window`,
      nodeId,
      data: { graceMs: this.config.nodeGraceMs }
    });
  }

  private async handleNodeMessage(ws: WebSocket, state: NodeConnectionState, message: RawData, isBinary: boolean): Promise<void> {
    let raw: unknown;
    try {
      if (isBinary) throw new HermesError(ErrorCode.PROTOCOL_ERROR, "node messages must be JSON text");
      const text = Buffer.isBuffer(message)
        ? message.toString("utf8")
        : Array.isArray(message)
          ? Buffer.concat(message).toString("utf8")
          : Buffer.from(message).toString("utf8");
      raw = JSON.parse(text) as unknown;
      const ack = NodeAckSchema.safeParse(raw);
      if (ack.success) {
        this.runtime.pending.resolve(ack.data);
        this.runtime.nodes.onAck(ack.data);
        const sent = this.outbound.get(Number(ack.data.id));
        if (sent) {
          this.outbound.delete(Number(ack.data.id));
          const ackPayload = ack.data.data as Record<string, unknown> | undefined;
          const ackMessage = ack.data.message
            ?? (typeof ackPayload?.message === "string" ? ackPayload.message : undefined);
          this.runtime.events.record({
            category: "node",
            event: "node.ack",
            message: `${sent.cmd} acked by node ${state.nodeId} (code ${ack.data.code})`,
            nodeId: state.nodeId,
            roomId: sent.roomId,
            playerIds: sent.playerIds,
            data: {
              commandId: Number(ack.data.id),
              cmd: sent.cmd,
              code: ack.data.code,
              latencyMs: Date.now() - sent.at,
              moveId: sent.moveId,
              fromRoomId: sent.fromRoomId,
              ackMessage,
              // Only useful for failures and keeps successful acks compact.
              ackData: ack.data.code === 0 ? undefined : ackPayload
            }
          });
        }
        return;
      }

      const parsed = NodeEventSchema.parse(raw);
      const event: INodeEvent = { ...parsed, data: { ...parsed.data, nodeId: state.nodeId } };

      if (this.rejectsRoomIdConflict(event, ws)) return;

      if (this.applyNodeEvent(event) && isPlanningEvent(event)) {
        this.runtime.moves.planAndExecute().catch((error) => this.log.error({ err: error }, "plan cycle error"));
      }
    } catch (error) {
      const envelope = typeof raw === "object" && raw !== null ? (raw as { event?: unknown; data?: { roomId?: unknown; seq?: unknown } }) : undefined;
      this.log.warn(
        { err: error, nodeId: state.nodeId, event: envelope?.event, roomId: envelope?.data?.roomId, seq: envelope?.data?.seq },
        "invalid node message"
      );
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
    this.runtime.events.record({
      category: "room",
      event: "room.id_conflict",
      message: `room id ${event.data.roomId} already belongs to another endpoint`,
      roomId: event.data.roomId as string,
      nodeId: event.data.nodeId,
      data: {
        existingUdpHost: existing.udpHost,
        existingUdpPort: existing.udpPort,
        udpHost: event.data.udpHost,
        udpPort: event.data.port
      }
    });
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
    // A restarted node can reuse a short numeric room id before the old entry
    // is swept. Its telemetry sequence starts over, so the stale-frame guard
    // recorded for the dead incarnation must be dropped with it.
    if (event.event === "room.created" && event.data.roomId) {
      const existing = this.runtime.rooms.getRoom(event.data.roomId);
      if (existing && existing.nodeId !== event.data.nodeId) {
        this.telemetrySeq.delete(event.data.roomId as string);
        this.telemetryStaleWarnAt.delete(event.data.roomId as string);
        this.log.warn(
          { roomId: event.data.roomId, previousNodeId: existing.nodeId, nodeId: event.data.nodeId },
          "room id reused by a new node incarnation; telemetry sequence reset"
        );
        this.runtime.events.record({
          category: "telemetry",
          event: "telemetry.incarnation_reset",
          message: `room ${event.data.roomId} moved to a new node incarnation; sequence reset`,
          roomId: event.data.roomId as string,
          nodeId: event.data.nodeId,
          data: { previousNodeId: existing.nodeId }
        });
      }
    }
    if (event.event === "room.destroyed" && event.data.roomId) {
      this.telemetrySeq.delete(event.data.roomId as string);
      this.telemetryStaleWarnAt.delete(event.data.roomId as string);
    }
    // Telemetry carries a per-room monotonic sequence. Dropping a stale or
    // duplicated snapshot prevents an out-of-order HTTP-fallback delivery from
    // rolling the player table backwards.
    if (event.event === "room.telemetry" && event.data.roomId && typeof event.data.seq === "number") {
      const roomKey = event.data.roomId as string;
      const last = this.telemetrySeq.get(roomKey);
      if (last !== undefined && event.data.seq <= last) {
        this.metrics.inc("qwd_telemetry_stale_total");
        const at = Date.now();
        this.runtime.events.record({
          category: "telemetry",
          event: "telemetry.seq_regressed",
          message: `telemetry seq ${event.data.seq} <= last ${last}; frame dropped`,
          roomId: roomKey,
          nodeId: event.data.nodeId,
          data: { seq: event.data.seq, lastSeq: last, kind: event.data.kind ?? "full", baseSeq: event.data.baseSeq }
        });
        if (at - (this.telemetryStaleWarnAt.get(roomKey) ?? 0) >= 30_000) {
          this.telemetryStaleWarnAt.set(roomKey, at);
          this.log.warn(
            {
              roomId: roomKey,
              nodeId: event.data.nodeId,
              seq: event.data.seq,
              lastSeq: last,
              kind: event.data.kind ?? "full",
              baseSeq: event.data.baseSeq
            },
            "telemetry sequence regressed; frame dropped"
          );
        }
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
    this.recordAppliedEvent(event);
    return true;
  }

  /** Room/player lifecycle entries for GET /debug/logs. */
  private recordAppliedEvent(event: INodeEvent): void {
    const roomId = event.data.roomId as string | undefined;
    if (!roomId) return;

    if (event.event === "player.join" || event.event === "player.leave") {
      const playerId = event.data.player?.playerId as string | undefined;
      if (!playerId) return;
      this.runtime.events.record({
        category: "player",
        event: event.event,
        message: `${event.event === "player.join" ? "joined" : "left"} room ${roomId}`,
        roomId,
        nodeId: event.data.nodeId,
        playerIds: [playerId],
        data: { level: event.data.player?.level }
      });
      return;
    }

    if (event.event === "room.created" || event.event === "room.destroyed") {
      this.runtime.events.record({
        category: "room",
        event: event.event,
        message: `${event.event === "room.created" ? "room created" : "room destroyed"}: ${roomId}`,
        roomId,
        nodeId: event.data.nodeId,
        data: {
          udpHost: event.data.udpHost,
          udpPort: event.data.port,
          players: typeof event.data.players === "number" ? event.data.players : undefined,
          version: event.data.version
        }
      });
    }
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
