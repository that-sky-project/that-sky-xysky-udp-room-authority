import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import type { Duplex } from "node:stream";
import { WebSocket, WebSocketServer, type RawData } from "ws";
import { randomUUID } from "node:crypto";
import { jsonStringify } from "../infra/ws/json.js";
import type { Logger } from "../infra/logger/logger.js";
import type { ManagerConfig } from "../config/env.js";
import type { ManagerRuntime } from "./ManagerRuntime.js";
import type { NodeTransport } from "./NodeRegistry.js";
import { asNodeId, asProtocolVersion, asRoomId, asUnixMs } from "../types/branded.js";
import { AllocationRequestSchema, NodeAckSchema, NodeEventSchema, type IManagerCommand, type INodeEvent } from "../types/contracts.js";
import { ErrorCode, HermesError, isHermesError } from "../types/errors.js";

interface NodeConnectionState {
  nodeId: ReturnType<typeof asNodeId>;
  protocolVersion: number;
  connectedAt: number;
  remoteAddress: string;
}

export class ManagerGateway {
  private server: Server | undefined;
  private nodeServer: WebSocketServer | undefined;

  public constructor(
    private readonly config: ManagerConfig,
    private readonly runtime: ManagerRuntime,
    private readonly log: Logger
  ) {}

  public async start(): Promise<void> {
    if (this.server) return;

    const server = createServer((req, res) => {
      void this.handleHttp(req, res);
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
    this.log.info({ host: this.config.host, port: this.config.port }, "manager gateway listening");
  }

  public async stop(): Promise<void> {
    const server = this.server;
    const nodeServer = this.nodeServer;
    this.server = undefined;
    this.nodeServer = undefined;

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
        const event: INodeEvent = { ...parsed, data: { ...parsed.data, nodeId: parsed.data.nodeId ?? asNodeId(`http-${parsed.data.roomId ?? randomUUID()}`) } };
        this.runtime.nodes.applyEvent(event);
        this.runtime.rooms.applyNodeEvent(event);
        this.runtime.moves.ingest(event);
        void this.runtime.moves.planAndExecute();
        this.writeJson(res, 200, { code: ErrorCode.OK });
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

      const friendsMatch = method === "GET" ? /^\/players\/([^/]+)\/friends$/.exec(url.pathname) : undefined;
      if (friendsMatch) {
        const encodedPlayerId = friendsMatch[1];
        if (!encodedPlayerId) throw new HermesError(ErrorCode.BAD_REQUEST, "playerId is required");
        const playerId = decodeURIComponent(encodedPlayerId);
        const friendIds = await this.runtime.social.getFriendIds(playerId, url.searchParams.get("refresh") === "1");
        this.writeJson(res, 200, { playerId, friendIds });
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
    this.log.info({ nodeId: state.nodeId, protocolVersion: state.protocolVersion }, "node connected");
    ws.on("message", (message, isBinary) => void this.handleNodeMessage(ws, state, message, isBinary));
    ws.on("close", () => {
      closed = true;
      const removedRooms = this.runtime.rooms.deleteRoomsByNode(state.nodeId);
      this.runtime.rooms.removePlayersByNode(state.nodeId);
      this.runtime.nodes.unregister(state.nodeId);
      this.log.warn({ nodeId: state.nodeId, removedRooms: removedRooms.map((room) => room.roomId) }, "node disconnected; rooms reclaimed");
    });
    ws.on("error", (error) => this.log.warn({ err: error, nodeId: state.nodeId }, "node socket error"));
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
      if (event.event === "room.created" && event.data.roomId) {
        const existing = this.runtime.rooms.getRoom(event.data.roomId);
        if (existing && (existing.udpHost !== parsed.data.udpHost || existing.udpPort !== parsed.data.port)) {
          if (ws.readyState === WebSocket.OPEN) ws.send(jsonStringify({ v: 1, id: 0, cmd: "room.id_conflict", data: { roomId: event.data.roomId } }));
          this.log.warn({ roomId: event.data.roomId, existingUdpHost: existing.udpHost, existingUdpPort: existing.udpPort, udpHost: parsed.data.udpHost, udpPort: parsed.data.port }, "room id conflict rejected");
          return;
        }
      }
      this.runtime.nodes.applyEvent(event);
      this.runtime.rooms.applyNodeEvent(event);
      this.runtime.moves.ingest(event);
      if (event.data.player?.playerId && (event.event === "player.join" || event.event === "player.leave")) {
        this.runtime.reservations.confirm(event.data.player.playerId);
      }
      if (event.event === "room.telemetry" || event.event === "player.join" || event.event === "player.leave") {
        void this.runtime.moves.planAndExecute();
      }
    } catch (error) {
      this.log.warn({ err: error, nodeId: state.nodeId }, "invalid node message");
      if (ws.readyState === WebSocket.OPEN) ws.send(jsonStringify(this.errorReply(error)));
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
