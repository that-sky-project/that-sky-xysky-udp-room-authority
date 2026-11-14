import { ManagerHttpApi } from './ManagerHttpApi.js'
import {
  createServer,
  type IncomingMessage,
  type Server,
  type ServerResponse
} from 'node:http'
import type { Duplex } from 'node:stream'
import { WebSocket, WebSocketServer, type RawData } from 'ws'
import { randomUUID } from 'node:crypto'
import { ZodError } from 'zod'
import { jsonStringify } from '../infra/ws/json.js'
import type { Logger } from '../infra/logger/logger.js'
import type { ManagerConfig } from '../config/index.js'
import type { ManagerRuntime } from './ManagerRuntime.js'
import type { NodeTransport } from './NodeRegistry.js'
import { HeartbeatMonitor } from './HeartbeatMonitor.js'
import { MetricsRegistry, registerManagerMetrics } from './MetricsRegistry.js'
import {
  asNodeId,
  asPlayerId,
  asProtocolVersion,
  asRoomId,
  asUnixMs,
  type PlayerId
} from '../types/branded.js'
import {
  AllocationRequestSchema,
  BatchRoomLookupSchema,
  NodeAckSchema,
  NodeEventSchema,
  type IManagerCommand,
  type INodeEvent,
  type RoomLookup
} from '../types/contracts.js'
import { ErrorCode, HermesError, isHermesError } from '../types/errors.js'

interface NodeConnectionState {
  nodeId: ReturnType<typeof asNodeId>
  protocolVersion: number
  connectedAt: number
  remoteAddress: string
}

/** Events that can change the room/level landscape and therefore re-plan. */
const isPlanningEvent = (event: INodeEvent): boolean =>
  event.event === 'room.telemetry' ||
  event.event === 'player.join' ||
  event.event === 'player.leave'

/** Compact, log-safe view of an outbound command's routing fields. */
const summarizeCommand = (
  command: IManagerCommand
): {
  roomId?: string | undefined
  fromRoomId?: string | undefined
  moveId?: string | undefined
  playerIds?: string[] | undefined
  expectedVersion?: number | undefined
} => {
  const data = (command.data ?? {}) as Record<string, unknown>
  const text = (key: string): string | undefined =>
    typeof data[key] === 'string' ? data[key] : undefined
  const playerIds = Array.isArray(data.playerIds)
    ? data.playerIds.filter((id): id is string => typeof id === 'string')
    : typeof data.playerId === 'string'
    ? [data.playerId]
    : undefined
  return {
    roomId: text('roomId') ?? text('toRoomId'),
    fromRoomId: text('fromRoomId'),
    moveId: text('moveId'),
    playerIds,
    expectedVersion:
      typeof data.expectedVersion === 'number'
        ? data.expectedVersion
        : undefined
  }
}

export class ManagerGateway {
  private server: Server | undefined
  private nodeServer: WebSocketServer | undefined
  private readonly heartbeat: HeartbeatMonitor
  private readonly http: ManagerHttpApi
  private readonly metrics: MetricsRegistry
  private graceSweepTimer: NodeJS.Timeout | undefined
  private starting: Promise<void> | undefined
  private stopping = false
  private shutdown: Promise<void> | undefined
  private readonly abort = new AbortController()
  private readonly tasks = new Set<Promise<void>>()
  private readonly connections = new Set<Duplex>()
  /** In-flight outbound commands, so an ack can be logged with its command name. */
  private readonly outbound = new Map<
    number,
    {
      cmd: string
      at: number
      roomId?: string | undefined
      fromRoomId?: string | undefined
      moveId?: string | undefined
      playerIds?: string[] | undefined
    }
  >()

  public constructor (
    private readonly config: ManagerConfig,
    private readonly runtime: ManagerRuntime,
    private readonly log: Logger
  ) {
    this.metrics = runtime.metrics
    this.http = new ManagerHttpApi(config, runtime, log, this.metrics, {
      signal: this.abort.signal, isStopping: () => this.stopping,
      refreshMetrics: () => this.refreshGauges(), applyEvent: event => this.applyNodeEvent(event)
    })
    registerManagerMetrics(this.metrics, log)
    this.heartbeat = new HeartbeatMonitor(
      {
        intervalMs: config.nodeHeartbeatIntervalMs,
        timeoutMs: config.nodeHeartbeatTimeoutMs,
        onDead: nodeId => this.handleNodeUnresponsive(nodeId)
      },
      log
    )
  }

  public start (): Promise<void> {
    if (this.stopping) return Promise.reject(new Error('manager gateway is stopping'))
    this.starting ??= this.listen()
    return this.starting
  }

  private async listen (): Promise<void> {
    const server = createServer((req, res) => {
      this.track(this.http.handle(req, res), 'http handler', () => res.destroy())
    })
    const nodeServer = new WebSocketServer({
      noServer: true,
      maxPayload: 512 * 1024,
      perMessageDeflate: false
    })
    // Own partially-created resources too, so a listen failure can be drained.
    this.server = server
    this.nodeServer = nodeServer
    server.on('connection', socket => {
      this.connections.add(socket)
      socket.once('close', () => this.connections.delete(socket))
    })
    server.on('upgrade', (req, socket, head) => {
      try {
        this.handleUpgrade(nodeServer, req, socket, head)
      } catch (error) {
        this.log.error({ err: error, task: 'node upgrade' }, 'node upgrade failed')
        socket.destroy()
      }
    })
    server.on('error', error =>
      this.log.error({ err: error }, 'manager HTTP server error')
    )
    nodeServer.on('error', error =>
      this.log.error({ err: error }, 'manager WebSocket server error')
    )

    await new Promise<void>((resolve, reject) => {
      const onClose = (): void => reject(new Error('manager startup cancelled'))
      const cleanup = (): void => {
        server.off('error', onError)
        server.off('close', onClose)
      }
      const onError = (error: Error): void => { cleanup(); reject(error) }
      server.once('error', onError)
      server.once('close', onClose)
      try {
        server.listen(this.config.port, this.config.host, () => { cleanup(); resolve() })
      } catch (error) {
        cleanup()
        reject(error)
      }
    })
    if (this.stopping) throw new Error('manager startup cancelled')
    this.heartbeat.start()
    this.graceSweepTimer = setInterval(() => {
      if (this.stopping) return
      try { this.sweepSuspectedNodes() } catch (error) {
        this.log.error({ err: error, task: 'node grace sweep' }, 'node grace sweep failed')
      }
    }, 2_000)
    this.graceSweepTimer.unref?.()
    this.log.info({ host: this.config.host, port: this.config.port }, 'manager gateway listening')
  }

  private track (operation: Promise<void>, task: string, failed?: () => void): void {
    const pending = operation.catch(error => {
      this.log.error({ err: error, task }, 'manager task failed')
      failed?.()
    }).finally(() => this.tasks.delete(pending))
    this.tasks.add(pending)
  }

  public stop (): Promise<void> {
    if (this.shutdown) return this.shutdown
    this.stopping = true
    this.abort.abort()
    this.heartbeat.stop()
    if (this.graceSweepTimer) clearInterval(this.graceSweepTimer)
    this.graceSweepTimer = undefined
    this.shutdown = this.drain()
    return this.shutdown
  }

  private async drain (): Promise<void> {
    const server = this.server
    const nodeServer = this.nodeServer
    this.runtime.router.stop()
    this.runtime.pending.close(new HermesError(ErrorCode.NODE_UNAVAILABLE, 'manager is shutting down'))
    const movesStopped = this.runtime.moves.stop()
    this.runtime.reservations.stop()
    const forced = setTimeout(() => {
      this.log.warn({ sockets: this.connections.size, nodes: nodeServer?.clients.size ?? 0 }, 'forcing remaining manager connections closed')
      for (const socket of nodeServer?.clients ?? []) socket.terminate()
      for (const socket of this.connections) socket.destroy()
    }, 5_000)
    const closing: Promise<void>[] = [movesStopped]
    if (nodeServer) {
      closing.push(new Promise<void>((resolve, reject) => {
        nodeServer.close(error => error ? reject(error) : resolve())
        for (const socket of nodeServer.clients) socket.close(1001, 'manager shutting down')
      }))
    }
    if (server) {
      closing.push(new Promise<void>((resolve, reject) => {
        server.close(error => {
          if (error && (error as NodeJS.ErrnoException).code !== 'ERR_SERVER_NOT_RUNNING') reject(error)
          else resolve()
        })
        server.closeIdleConnections()
      }))
    }
    try {
      const results = await Promise.allSettled(closing)
      await Promise.all(this.tasks)
      const failures = results.flatMap(result => result.status === 'rejected' ? [result.reason] : [])
      if (failures.length) throw new AggregateError(failures, 'manager gateway shutdown failed')
    } finally {
      clearTimeout(forced)
      this.server = undefined
      this.nodeServer = undefined
      this.outbound.clear()
    }
  }

  /**
   * Recompute point-in-time gauges. `level_mix_violations` and
   * `players_stranded` are the two numbers that reveal a broken migration
   * loop: mixed rooms that never get split, and players that want to move but
   * have nowhere to go (the cluster needs more empty nodes).
   */
  private refreshGauges (): void {
    const rooms = this.runtime.rooms.listRooms()
    const levelsByRoom = this.runtime.rooms.levelsByRoom()
    this.metrics.set('qwd_rooms', rooms.length)
    this.metrics.set(
      'qwd_players',
      this.runtime.rooms
        .listRooms()
        .reduce((sum, room) => sum + room.players, 0)
    )

    let mixed = 0
    let stranded = 0
    for (const room of rooms) {
      const levels = levelsByRoom.get(room.roomId)
      if (!levels || levels.size === 0) continue
      if (levels.size > 1) {
        mixed += 1
        const counts = new Map<number, number>()
        for (const level of levels) counts.set(level, 0)
        for (const location of this.runtime.rooms.listPlayers(room.roomId)) {
          if (location.level === undefined) continue
          counts.set(location.level, (counts.get(location.level) ?? 0) + 1)
        }
        const sorted = [...counts.values()].sort((a, b) => b - a)
        const majority = sorted[0] ?? 0
        const total = sorted.reduce((sum, value) => sum + value, 0)
        stranded += Math.max(0, total - majority)
      }
    }
    this.metrics.set('qwd_level_mix_violations', mixed)
    this.metrics.set('qwd_players_stranded', stranded)
    this.metrics.set(
      'qwd_telemetry_gap_rooms',
      this.runtime.moves.telemetryGapCount()
    )
  }

  private sweepSuspectedNodes (): void {
    this.runtime.rooms.purgeDepartedPlayers()
    for (const { nodeId, rooms } of this.runtime.rooms.sweepSuspectedNodes()) {
      for (const room of rooms) {
        this.runtime.moves.forgetRoom(room.roomId as string)
      }
      this.metrics.inc('qwd_node_reclaimed_total')
      this.log.warn(
        { nodeId, rooms: rooms.map(room => room.roomId) },
        'node grace window expired; rooms reclaimed'
      )
      this.runtime.events.record({
        category: 'node',
        event: 'node.reclaimed',
        message: `node ${nodeId} reclaimed after the grace window (${rooms.length} room(s))`,
        nodeId,
        data: { rooms: rooms.map(room => room.roomId) }
      })
    }
  }

  /** Bound the in-flight command map: acks normally delete entries, timeouts do not. */
  private pruneOutbound (now: number): void {
    if (this.outbound.size < 5000) return
    for (const [id, entry] of this.outbound) {
      if (now - entry.at > 60_000) this.outbound.delete(id)
    }
  }

  /** Heartbeat declared a node dead: reuse the normal disconnect path. */
  private handleNodeUnresponsive (nodeId: string): void {
    this.metrics.inc('qwd_node_heartbeat_dead_total')
    this.handleNodeDisconnect(asNodeId(nodeId))
  }

  private handleUpgrade (
    nodeServer: WebSocketServer,
    req: IncomingMessage,
    socket: Duplex,
    head: Buffer
  ): void {
    if (this.stopping) {
      this.rejectUpgrade(socket, 503, 'manager is shutting down')
      return
    }
    let url: URL
    try {
      url = new URL(req.url ?? '/', 'http://manager.local')
    } catch {
      this.rejectUpgrade(socket, 400, 'invalid node request URL')
      return
    }
    if (url.pathname !== '/' && url.pathname !== '') {
      this.rejectUpgrade(socket, 404, 'route not found')
      return
    }

    const protocolVersion = Number(url.searchParams.get('v') ?? '1')
    if (!Number.isInteger(protocolVersion) || protocolVersion < 1) {
      this.rejectUpgrade(socket, 401, 'invalid node handshake')
      return
    }

    const requestedNodeId = url.searchParams.get('node') ?? ''
    const nodeId = /^[A-Za-z0-9._-]{1,128}$/.test(requestedNodeId)
      ? asNodeId(requestedNodeId)
      : asNodeId(`connection-${randomUUID()}`)
    const state: NodeConnectionState = {
      nodeId,
      protocolVersion,
      connectedAt: Date.now(),
      remoteAddress: req.socket.remoteAddress ?? ''
    }
    nodeServer.handleUpgrade(req, socket, head, ws => this.openNode(ws, state))
  }

  private openNode (ws: WebSocket, state: NodeConnectionState): void {
    let closed = false
    const nodes = this.runtime.nodes
    const backpressureLimitBytes = this.config.nodeBackpressureLimitBytes
    const transport: NodeTransport = {
      nodeId: state.nodeId,
      protocolVersion: state.protocolVersion,
      connectedAt: asUnixMs(state.connectedAt),
      remoteAddress: state.remoteAddress,
      get writable (): boolean {
        return (
          !closed &&
          ws.readyState === WebSocket.OPEN &&
          ws.bufferedAmount < backpressureLimitBytes
        )
      },
      send: async (command: IManagerCommand): Promise<void> => {
        if (closed || ws.readyState !== WebSocket.OPEN) {
          throw new HermesError(
            ErrorCode.NODE_UNAVAILABLE,
            'node socket is closed',
            { nodeId: state.nodeId }
          )
        }
        const before = ws.bufferedAmount
        nodes.recordBackpressure(state.nodeId, before)
        if (before >= backpressureLimitBytes) {
          throw new HermesError(
            ErrorCode.NODE_UNAVAILABLE,
            'node socket is under backpressure',
            { nodeId: state.nodeId, bufferedBytes: before }
          )
        }
        const summary = summarizeCommand(command)
        this.pruneOutbound(Date.now())
        this.outbound.set(Number(command.id), {
          cmd: command.cmd,
          at: Date.now(),
          ...summary
        })
        this.runtime.events.record({
          category: 'node',
          event: 'node.command',
          message: `${command.cmd} -> node ${state.nodeId}`,
          nodeId: state.nodeId,
          roomId: summary.roomId,
          playerIds: summary.playerIds,
          data: { commandId: Number(command.id), cmd: command.cmd, ...summary }
        })
        await new Promise<void>((resolve, reject) => {
          ws.send(
            jsonStringify({ ...command, v: command.v ?? asProtocolVersion(1) }),
            error => (error ? reject(error) : resolve())
          )
        })
        nodes.recordBackpressure(state.nodeId, ws.bufferedAmount)
      },
      close: (code = 1000, reason = 'closed'): void => {
        closed = true
        ws.close(code, reason)
      }
    }

    this.runtime.nodes.register(transport)
    this.heartbeat.track(state.nodeId, ws)
    const restored = this.runtime.rooms.restoreNode(state.nodeId)
    if (restored) {
      this.log.info(
        { nodeId: state.nodeId },
        'node reconnected within grace window; state restored'
      )
    }
    this.log.info(
      { nodeId: state.nodeId, protocolVersion: state.protocolVersion },
      'node connected'
    )
    this.runtime.events.record({
      category: 'node',
      event: 'node.connected',
      message: `node ${state.nodeId} connected${
        restored ? ' (state restored)' : ''
      }`,
      nodeId: state.nodeId,
      data: {
        protocolVersion: state.protocolVersion,
        remoteAddress: state.remoteAddress,
        restored
      }
    })
    ws.on('message', (message, isBinary) => {
      if (this.stopping) return
      this.track(this.handleNodeMessage(ws, state, message, isBinary), 'node message handler')
    })
    ws.on('close', () => {
      closed = true
      this.heartbeat.untrack(state.nodeId, ws)
      if (this.runtime.nodes.getTransport(state.nodeId) !== transport) {
        this.log.info(
          { nodeId: state.nodeId },
          'stale node socket closed; a newer connection owns the id'
        )
        return
      }
      try { this.handleNodeDisconnect(state.nodeId) } catch (error) {
        this.log.error({ err: error, nodeId: state.nodeId }, 'node disconnect cleanup failed')
      }
    })
    ws.on('error', error =>
      this.log.warn({ err: error, nodeId: state.nodeId }, 'node socket error')
    )
  }

  private handleNodeDisconnect (nodeId: ReturnType<typeof asNodeId>): void {
    if (!this.runtime.nodes.getTransport(nodeId)) {
      return
    }
    this.runtime.nodes.unregister(nodeId)
    this.runtime.rooms.markNodeSuspected(nodeId)
    this.log.warn(
      { nodeId, graceMs: this.config.nodeGraceMs },
      'node disconnected; state retained for grace window'
    )
    this.runtime.events.record({
      category: 'node',
      event: 'node.disconnected',
      message: `node ${nodeId} disconnected; rooms retained for the grace window`,
      nodeId,
      data: { graceMs: this.config.nodeGraceMs }
    })
  }

  private async handleNodeMessage (
    ws: WebSocket,
    state: NodeConnectionState,
    message: RawData,
    isBinary: boolean
  ): Promise<void> {
    if (this.runtime.nodes.getTransport(state.nodeId)?.connectedAt !== state.connectedAt) return
    let raw: unknown
    try {
      if (isBinary)
        throw new HermesError(
          ErrorCode.PROTOCOL_ERROR,
          'node messages must be JSON text'
        )
      const text = Buffer.isBuffer(message)
        ? message.toString('utf8')
        : Array.isArray(message)
        ? Buffer.concat(message).toString('utf8')
        : Buffer.from(message).toString('utf8')
      raw = JSON.parse(text) as unknown
      const ack = NodeAckSchema.safeParse(raw)
      if (ack.success) {
        if (!this.runtime.pending.resolve(ack.data, state.nodeId as string)) {
          this.log.warn(
            { commandId: Number(ack.data.id), nodeId: state.nodeId },
            'ack for a command addressed to another node; ignored'
          )
          return
        }
        this.runtime.nodes.onAck(ack.data)
        const sent = this.outbound.get(Number(ack.data.id))
        if (sent) {
          this.outbound.delete(Number(ack.data.id))
          const ackPayload = ack.data.data as
            | Record<string, unknown>
            | undefined
          const ackMessage =
            ack.data.message ??
            (typeof ackPayload?.message === 'string'
              ? ackPayload.message
              : undefined)
          this.runtime.events.record({
            category: 'node',
            event: 'node.ack',
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
              ackData: ack.data.code === 0 ? undefined : ackPayload
            }
          })
        }
        return
      }

      const parsed = NodeEventSchema.parse(raw)
      const event: INodeEvent = {
        ...parsed,
        data: { ...parsed.data, nodeId: state.nodeId }
      }

      if (this.rejectsRoomIdConflict(event, ws)) return

      if (this.applyNodeEvent(event) && isPlanningEvent(event)) {
        this.runtime.moves
          .planAndExecute()
          .catch(error => this.log.error({ err: error }, 'plan cycle error'))
      }
    } catch (error) {
      const envelope =
        typeof raw === 'object' && raw !== null
          ? (raw as {
              event?: unknown
              data?: { roomId?: unknown; seq?: unknown }
            })
          : undefined
      const expected = isHermesError(error) || error instanceof ZodError || error instanceof SyntaxError
      const details = {
        err: error,
        nodeId: state.nodeId,
        event: envelope?.event,
        roomId: envelope?.data?.roomId,
        seq: envelope?.data?.seq
      }
      if (expected) this.log.warn(details, 'invalid node message')
      else this.log.error(details, 'node event processing failed')
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(jsonStringify(this.http.errorReply(error)), failure => {
          if (failure) this.log.warn({ err: failure, nodeId: state.nodeId }, 'node error response failed')
        })
      }
    }
  }

  /**
   * Two rooms may not claim the same id from different UDP endpoints: the
   * client would be sent to an address the manager does not control.
   */
  private rejectsRoomIdConflict (event: INodeEvent, ws: WebSocket): boolean {
    if (event.event !== 'room.created' || !event.data.roomId) return false
    const existing = this.runtime.rooms.getRoom(event.data.roomId)
    if (!existing) return false
    if (
      existing.udpHost === event.data.udpHost &&
      existing.udpPort === event.data.port
    )
      return false
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(
        jsonStringify({
          v: 1,
          id: 0,
          cmd: 'room.id_conflict',
          data: { roomId: event.data.roomId }
        })
      )
    }
    this.log.warn(
      {
        roomId: event.data.roomId,
        existingUdpHost: existing.udpHost,
        existingUdpPort: existing.udpPort,
        udpHost: event.data.udpHost,
        udpPort: event.data.port
      },
      'room id conflict rejected'
    )
    this.runtime.events.record({
      category: 'room',
      event: 'room.id_conflict',
      message: `room id ${event.data.roomId} already belongs to another endpoint`,
      roomId: event.data.roomId as string,
      nodeId: event.data.nodeId,
      data: {
        existingUdpHost: existing.udpHost,
        existingUdpPort: existing.udpPort,
        udpHost: event.data.udpHost,
        udpPort: event.data.port
      }
    })
    return true
  }

  private applyNodeEvent (event: INodeEvent): boolean {
    if (event.data.roomId && event.event !== 'room.created') {
      const owner = this.runtime.rooms.getRoom(event.data.roomId)
      if (owner && owner.nodeId !== event.data.nodeId) return false
    }
    this.runtime.nodes.applyEvent(event)
    if (!this.runtime.moves.ingest(event)) return false
    this.runtime.rooms.applyNodeEvent(event)
    if (
      event.data.player?.playerId &&
      (event.event === 'player.join' || event.event === 'player.leave')
    ) {
      this.runtime.reservations.confirm(event.data.player.playerId)
    }
    this.recordAppliedEvent(event)
    return true
  }

  private recordAppliedEvent (event: INodeEvent): void {
    const roomId = event.data.roomId as string | undefined
    if (!roomId) return

    if (event.event === 'player.join' || event.event === 'player.leave') {
      const playerId = event.data.player?.playerId as string | undefined
      if (!playerId) return
      this.runtime.events.record({
        category: 'player',
        event: event.event,
        message: `${
          event.event === 'player.join' ? 'joined' : 'left'
        } room ${roomId}`,
        roomId,
        nodeId: event.data.nodeId,
        playerIds: [playerId],
        data: { level: event.data.player?.level }
      })
      return
    }

    if (event.event === 'room.created' || event.event === 'room.destroyed') {
      this.runtime.events.record({
        category: 'room',
        event: event.event,
        message: `${
          event.event === 'room.created' ? 'room created' : 'room destroyed'
        }: ${roomId}`,
        roomId,
        nodeId: event.data.nodeId,
        data: {
          udpHost: event.data.udpHost,
          udpPort: event.data.port,
          players:
            typeof event.data.players === 'number'
              ? event.data.players
              : undefined,
          version: event.data.version
        }
      })
    }
  }

  /**
   * Room view for one player, shared by the single and batch lookup routes.
   * Returns null when the player is unknown or their room is already gone;
   * "not in a room" is a normal state for a client that has not been placed yet.
   */
  private rejectUpgrade (socket: Duplex, status: number, message: string): void {
    socket.write(
      `HTTP/1.1 ${status} ${message}\r\nConnection: close\r\nContent-Length: ${Buffer.byteLength(
        message
      )}\r\n\r\n${message}`
    )
    socket.destroy()
  }


}
