import type { IncomingMessage, ServerResponse } from 'node:http'
import { randomUUID } from 'node:crypto'
import { ZodError } from 'zod'
import type { ManagerConfig } from '../config/index.js'
import type { Logger } from '../infra/logger/logger.js'
import { jsonStringify } from '../infra/ws/json.js'
import type { ManagerRuntime } from './ManagerRuntime.js'
import type { MetricsRegistry } from './MetricsRegistry.js'
import { asNodeId, asPlayerId, asRoomId, type PlayerId } from '../types/branded.js'
import { AllocationRequestSchema, BatchRoomLookupSchema, NodeEventSchema, type INodeEvent, type RoomLookup } from '../types/contracts.js'
import { ErrorCode, HermesError, isHermesError } from '../types/errors.js'
interface HttpPorts { signal: AbortSignal; isStopping(): boolean; refreshMetrics(): void; applyEvent(event: INodeEvent): boolean }
const isPlanningEvent = (event: INodeEvent): boolean => ['room.telemetry', 'player.join', 'player.leave'].includes(event.event)

/** HTTP routes own parsing and presentation, not socket lifecycle or business stores. */
export class ManagerHttpApi {
  public constructor(private readonly config: ManagerConfig, private readonly runtime: ManagerRuntime,
    private readonly log: Logger, private readonly metrics: MetricsRegistry, private readonly ports: HttpPorts) {}
  public async handle (
    req: IncomingMessage,
    res: ServerResponse
  ): Promise<void> {
    const method = req.method ?? 'GET'
    if (this.ports.isStopping()) {
      res.setHeader('connection', 'close')
      this.writeJson(res, 503, { code: ErrorCode.NODE_UNAVAILABLE, data: { message: 'manager is shutting down' } })
      return
    }
    let url: URL
    try {
      url = new URL(req.url ?? '/', 'http://manager.local')
    } catch {
      this.writeJson(res, 400, {
        code: ErrorCode.BAD_REQUEST,
        data: { message: 'invalid request URL' }
      })
      return
    }

    try {
      if (method === 'POST' && url.pathname === this.config.allocatePath) {
        await this.handleAllocationBody(
          res,
          await this.readBody(req, 16 * 1024)
        )
        return
      }
      if (method === 'POST' && url.pathname === '/telemetry') {
        const parsed = NodeEventSchema.parse(
          JSON.parse(await this.readBody(req, 128 * 1024))
        )
        const nodeId =
          parsed.data.nodeId ??
          asNodeId(`http-${parsed.data.roomId ?? randomUUID()}`)
        const event: INodeEvent = {
          ...parsed,
          data: { ...parsed.data, nodeId }
        }
        if (this.ports.applyEvent(event) && isPlanningEvent(event)) {
          this.runtime.moves
            .planAndExecute()
            .catch(error => this.log.error({ err: error }, 'plan cycle error'))
        }
        this.writeJson(res, 200, { code: ErrorCode.OK })
        return
      }
      if (method === 'GET' && url.pathname === '/healthz') {
        const nodes = this.runtime.nodes.listHealth()
        const healthy = nodes.filter(
          node => node.connected && !node.draining
        ).length
        this.writeJson(res, healthy > 0 || nodes.length === 0 ? 200 : 503, {
          status: healthy > 0 || nodes.length === 0 ? 'ok' : 'degraded',
          nodes: { total: nodes.length, healthy },
          rooms: this.runtime.rooms.listRooms().length,
          uptimeMs: process.uptime() * 1000
        })
        return
      }
      if (method === 'GET' && url.pathname === '/metrics') {
        this.ports.refreshMetrics()
        res.writeHead(200, {
          'content-type': 'text/plain; version=0.0.4; charset=utf-8'
        })
        res.end(this.metrics.render())
        return
      }
      if (method === 'GET' && url.pathname === '/moves') {
        this.writeJson(res, 200, { moves: this.runtime.moves.list() })
        return
      }
      if (method === 'GET' && url.pathname === '/activity') {
        const moves = this.runtime.moves
        const rooms = moves
          .activitySnapshot()
          .map(activity => ({
            activity,
            players: moves.telemetry.listRoom(asRoomId(activity.roomId))
          }))
        this.writeJson(res, 200, { rooms })
        return
      }
      if (method === 'GET' && url.pathname === '/debug/logs') {
        this.requireDebugToken(req)
        const rawLimit = Number(url.searchParams.get('limit') ?? '')
        const rawSince = Number(url.searchParams.get('since') ?? '')
        const page = this.runtime.events.list({
          limit:
            Number.isFinite(rawLimit) && rawLimit > 0 ? rawLimit : undefined,
          since:
            Number.isFinite(rawSince) && rawSince > 0 ? rawSince : undefined,
          category: url.searchParams.get('category') ?? undefined,
          roomId: url.searchParams.get('roomId') ?? undefined,
          playerId: url.searchParams.get('playerId') ?? undefined
        })
        this.writeJson(res, 200, { code: ErrorCode.OK, data: page })
        return
      }
      if (method === 'GET' && url.pathname === '/debug/allnode') {
        this.requireDebugToken(req)
        const nodes = this.runtime.nodes
          .listHealth()
          .map(({ nodeId: internalNodeId, ...node }) => ({
            ...node,
            rooms: this.runtime.rooms
              .listRoomsByNode(internalNodeId)
              .map(room => {
                const { nodeId: _roomOwner, ...roomData } = room
                const players = this.runtime.rooms
                  .listPlayers(room.roomId)
                  .map(location => ({
                    playerId: location.playerId,
                    updatedAt: location.updatedAt,
                    telemetry:
                      this.runtime.moves.telemetry.get(location.playerId) ??
                      null
                  }))
                return { ...roomData, playerCount: players.length, players }
              })
          }))
        this.writeJson(res, 200, { code: ErrorCode.OK, data: { nodes } })
        return
      }
      if (method === 'POST' && url.pathname === '/debug/move') {
        this.requireDebugToken(req)
        const input = JSON.parse(await this.readBody(req, 32 * 1024)) as Record<
          string,
          unknown
        >
        const sourceRoomId =
          typeof input.sourceRoomId === 'string' ? input.sourceRoomId : ''
        const targetRoomId =
          typeof input.targetRoomId === 'string' ? input.targetRoomId : ''
        const playerIds = Array.isArray(input.playerIds)
          ? input.playerIds.filter((id): id is string => typeof id === 'string')
          : []
        const moveInput: {
          sourceRoomId: string
          targetRoomId: string
          playerIds: string[]
          reason?: string
        } = { sourceRoomId, targetRoomId, playerIds }
        if (typeof input.reason === 'string') moveInput.reason = input.reason
        const transaction = await this.runtime.moves.manualMove(moveInput)
        this.writeJson(res, 200, { code: ErrorCode.OK, data: transaction })
        return
      }

      const roomMatch =
        method === 'GET'
          ? /^\/players\/([^/]+)\/room$/.exec(url.pathname)
          : undefined
      if (roomMatch) {
        const encodedPlayerId = roomMatch[1]
        if (!encodedPlayerId)
          throw new HermesError(ErrorCode.BAD_REQUEST, 'playerId is required')
        const playerId = asPlayerId(decodeURIComponent(encodedPlayerId))

        const data = this.lookupRoom(playerId)
        if (!data)
          throw new HermesError(
            ErrorCode.NOT_FOUND,
            'player is not in any known room',
            { playerId }
          )
        this.writeJson(res, 200, { code: ErrorCode.OK, data })
        return
      }

      if (method === 'POST' && url.pathname === '/players/rooms') {
        const request = BatchRoomLookupSchema.parse(
          JSON.parse(await this.readBody(req, 256 * 1024))
        )
        const rooms: Record<
          string,
          ReturnType<ManagerHttpApi['lookupRoom']>
        > = {}
        for (const playerId of request.playerIds) {
          rooms[playerId] = this.lookupRoom(asPlayerId(playerId))
        }
        this.writeJson(res, 200, { code: ErrorCode.OK, data: { rooms } })
        return
      }

      this.writeJson(res, 404, {
        code: ErrorCode.NOT_FOUND,
        data: { message: 'route not found' }
      })
    } catch (error) {
      const expected = isHermesError(error) || error instanceof ZodError || error instanceof SyntaxError || error instanceof URIError
      if (!expected) this.log.error({ err: error, method, path: url.pathname }, 'manager HTTP request failed')
      const tooLarge =
        error instanceof HermesError && error.message.endsWith('body too large')
      this.writeJson(
        res,
        tooLarge
          ? 413
          : isHermesError(error)
            ? this.httpStatusForError(error)
            : 500,
        this.errorReply(error)
      )
    }
  }

  private httpStatusForError (error: HermesError): number {
    switch (error.code) {
      case ErrorCode.UNAUTHORIZED: return 401
      case ErrorCode.FORBIDDEN: return 403
      case ErrorCode.NOT_FOUND: return 404
      case ErrorCode.CONFLICT: return 409
      case ErrorCode.BAD_REQUEST: return 400
      case ErrorCode.PAYLOAD_TOO_LARGE: return 413
      default: return 400
    }
  }

  private async handleAllocationBody (
    res: ServerResponse,
    body: string
  ): Promise<void> {
    const abort = AbortSignal.any([this.ports.signal, AbortSignal.timeout(this.config.allocationRequestTimeoutMs)])
    try {
      const request = AllocationRequestSchema.parse(this.parseJsonBody(body))
      this.writeJson(
        res,
        200,
        await this.runtime.router.allocate(request, abort)
      )
    } catch (error) {
      this.writeJson(res, 200, this.errorReply(error))
    }
  }

  private lookupRoom (playerId: PlayerId): RoomLookup | null {
    const location = this.runtime.rooms.getPlayerLocation(playerId)
    if (!location) return null

    const room = this.runtime.rooms.getRoom(location.roomId)
    if (!room) return null

    const telemetry = this.runtime.moves.telemetry.get(playerId)
    return {
      playerId,
      roomId: room.roomId,
      udp: { host: room.udpHost, port: room.udpPort },
      players: room.players,
      capacity: room.capacity,
      level: location.level ?? null,
      levelId: telemetry?.levelId ?? null,
      updatedAt: location.updatedAt
    }
  }

  private requireDebugToken (req: IncomingMessage): void {
    if (!this.config.debugApiToken) return
    const bearer = req.headers.authorization?.startsWith('Bearer ')
      ? req.headers.authorization.slice(7)
      : undefined
    const supplied = req.headers['x-debug-token'] ?? bearer
    if (typeof supplied !== 'string' || supplied !== this.config.debugApiToken)
      throw new HermesError(ErrorCode.UNAUTHORIZED, 'invalid debug API token')
  }

  private readBody (req: IncomingMessage, maxBytes: number): Promise<string> {
    return new Promise((resolve, reject) => {
      const chunks: Buffer[] = []
      let totalBytes = 0
      let settled = false
      const fail = (error: Error): void => {
        if (settled) return
        settled = true
        reject(error)
      }
      req.on('data', (chunk: Buffer) => {
        if (settled) return
        totalBytes += chunk.byteLength
        if (totalBytes > maxBytes) {
          fail(new HermesError(ErrorCode.PAYLOAD_TOO_LARGE, 'request body too large'))
          req.resume()
          return
        }
        chunks.push(chunk)
      })
      req.once('end', () => {
        if (settled) return
        settled = true
        resolve(Buffer.concat(chunks).toString('utf8'))
      })
      req.once('aborted', () =>
        fail(new HermesError(ErrorCode.BAD_REQUEST, 'request aborted'))
      )
      req.once('error', error => fail(error))
    })
  }

  public writeJson (res: ServerResponse, status: number, value: unknown): void {
    if (res.writableEnded || res.destroyed) return
    const body = jsonStringify(value)
    res.writeHead(status, {
      'content-type': 'application/json; charset=utf-8',
      'content-length': Buffer.byteLength(body)
    })
    res.end(body)
  }

  public errorReply (error: unknown): {
    code: number
    data: { message: string }
  } {
    if (isHermesError(error))
      return { code: error.code, data: { message: error.message } }
    if (error instanceof Error)
      return { code: ErrorCode.INTERNAL, data: { message: error.message } }
    return { code: ErrorCode.INTERNAL, data: { message: String(error) } }
  }

  private parseJsonBody (body: string): unknown {
    try {
      return JSON.parse(body) as unknown
    } catch (error) {
      throw new HermesError(
        ErrorCode.BAD_REQUEST,
        'invalid allocation json body',
        error
      )
    }
  }
}
