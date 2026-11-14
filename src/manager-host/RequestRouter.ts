import type { CrossCoordinator } from './CrossCoordinator.js'
import { applyCapacityFacts } from './CapacityFacts.js'
import type { DebugEventInput, DebugEventLog } from './DebugEventLog.js'
import type { NodeRegistry } from './NodeRegistry.js'
import type { PolicyEngine } from './PolicyEngine.js'
import type { ReservationTracker } from './ReservationTracker.js'
import type { RoomCache } from './RoomCache.js'
import type { Logger } from '../infra/logger/logger.js'
import { asUnixMs, type PlayerId, type RoomId } from '../types/branded.js'
import type {
  IAllocationReply,
  IAllocationRequest,
  RoomAssignment,
  RoomSnapshot,
  INodeAck
} from '../types/contracts.js'
import { ErrorCode, HermesError, isHermesError } from '../types/errors.js'

export interface RequestRouterOptions {
  nodeStaleMs: number
  assignmentTtlMs: number
  candidateRoomLimit: number
  preferredFillTarget: number
  requestTimeoutMs?: number
  retryBaseMs?: number
  retryMaxMs?: number
  maxAttempts?: number
}

interface AllocationFlight {
  controller: AbortController
  waiters: number
  promise: Promise<IAllocationReply>
}

const EMPTY_LEVELS: ReadonlySet<number> = new Set<number>()

export class RequestRouter {
  private stopped = false
  private readonly flights = new Map<PlayerId, AllocationFlight>()
  private readonly backoff = new Map<RoomId, { failures: number; until: number }>()

  public constructor (
    private readonly rooms: RoomCache,
    private readonly nodes: NodeRegistry,
    private readonly policy: PolicyEngine,
    private readonly reservations: ReservationTracker,
    private readonly coordinator: CrossCoordinator,
    private readonly options: RequestRouterOptions,
    private readonly moveReserved: (roomId: RoomId) => number = () => 0,
    private readonly log?: Logger,
    /** Structured operational history surfaced by GET /debug/logs. */
    private readonly events?: DebugEventLog,
    /** Shared occupancy includes allocation and migration holds exactly once. */
    private readonly effectivePlayersOf?: (room: RoomSnapshot) => number
  ) {}

  public async allocate (
    request: IAllocationRequest,
    signal?: AbortSignal
  ): Promise<IAllocationReply> {
    try {
      if (this.stopped || signal?.aborted) throw this.cancelled()
      let flight = this.flights.get(request.playerId)
      if (!flight) {
        const controller = new AbortController()
        const timeout = setTimeout(
          () => controller.abort(new HermesError(ErrorCode.NODE_TIMEOUT, 'allocation deadline exceeded')),
          this.options.requestTimeoutMs ?? 10_000
        )
        flight = { controller, waiters: 0, promise: undefined as unknown as Promise<IAllocationReply> }
        const current = flight
        this.flights.set(request.playerId, current)
        current.promise = this.requestRoom(request, controller.signal).finally(() => {
          clearTimeout(timeout)
          if (this.flights.get(request.playerId) === current) this.flights.delete(request.playerId)
        })
      }
      return await this.waitFor(flight, signal)
    } catch (error) {
      if (isHermesError(error)) {
        return {
          code: error.code,
          data: { message: error.message, details: error.details }
        }
      }
      return {
        code: ErrorCode.INTERNAL,
        data: {
          message: error instanceof Error ? error.message : String(error)
        }
      }
    }
  }

  public stop(): void {
    if (this.stopped) return
    this.stopped = true
    for (const flight of this.flights.values()) flight.controller.abort(this.cancelled())
    this.backoff.clear()
  }

  private async requestRoom (
    request: IAllocationRequest,
    signal?: AbortSignal
  ): Promise<IAllocationReply> {
    const playerId = request.playerId
    const knownLocation = this.rooms.getPlayerLocation(playerId)
    if (knownLocation) {
      const room = this.rooms.getRoom(knownLocation.roomId)
      if (room) {
        this.log?.info(
          {
            playerId,
            roomId: room.roomId,
            reason: 'known_location',
            players: room.players
          },
          'room allocated'
        )
        this.record({
          category: 'allocation',
          event: 'allocation.known',
          message: `player ${playerId} already belongs to room ${room.roomId}`,
          roomId: room.roomId,
          playerIds: [playerId],
          data: {
            reason: 'known_location',
            players: room.players,
            capacity: room.capacity
          }
        })
        return { code: ErrorCode.OK, data: this.assignmentFor(room, playerId) }
      }
    }

    const held = this.reservations.confirmedHoldFor(playerId)
    if (held) {
      const room = this.rooms.getRoom(held.roomId)
      if (room) {
        this.record({
          category: 'allocation', event: 'allocation.reused',
          message: `player ${playerId} reuses acknowledged room ${room.roomId}`,
          roomId: room.roomId, nodeId: room.nodeId, playerIds: [playerId],
          data: { reason: 'acknowledged_reservation', expiresAt: held.expiresAt }
        })
        return { code: ErrorCode.OK, data: this.assignmentFor(room, playerId, held.expiresAt) }
      }
      this.reservations.release(playerId)
    }

    const nodeHealth = new Map(
      this.nodes
        .listHealthy(this.options.nodeStaleMs)
        .map(node => [node.nodeId as string, node])
    )
    const context = {
      playerId,
      region: request.region,
      level:
        request.level !== undefined && request.level > 0
          ? request.level
          : undefined,
      preferences: request.preferences
    }

    const candidates = this.rooms.listCandidateRooms({
      region: request.region,
      preferredFillTarget: this.options.preferredFillTarget,
      limit: this.options.candidateRoomLimit
    })
    const levelsByRoom = this.rooms.levelsByRoom()
    for (const [roomId, state] of this.backoff) {
      if (state.until <= Date.now()) this.backoff.delete(roomId)
    }
    let attempts = 0

    while (candidates.length > 0 && attempts < (this.options.maxAttempts ?? 8)) {
      if (signal?.aborted) throw signal.reason ?? this.cancelled()
      const selection = this.policy.selectRoom(
        context,
        candidates,
        nodeHealth,
        room => this.effectivePlayersOf?.(room) ??
          (this.reservations.effectivePlayers(room) + this.moveReserved(room.roomId)),
        room => levelsByRoom.get(room.roomId) ?? EMPTY_LEVELS,
        room => (this.backoff.get(room.roomId)?.until ?? 0) <= Date.now()
      )
      if (!selection) break

      const index = candidates.findIndex(
        room => room.roomId === selection.room.roomId
      )
      if (index >= 0) candidates.splice(index, 1)

      if (!this.reservations.tryAcquire(selection.room, playerId)) continue

      attempts += 1
      try {
        const ack = await this.withSignal(this.coordinator.reserveRoom(
          selection.room,
          playerId,
          signal
        ), signal)
        this.applyFacts(selection.room, ack)
        if (ack.code !== ErrorCode.OK) {
          throw new HermesError(
            ack.code,
            ack.message ?? 'node rejected room reservation',
            ack.data
          )
        }
        const data = ack.data as Record<string, unknown> | undefined
        const expiresAt = data?.expiresAt
        if (typeof expiresAt !== 'number' ||
            !this.reservations.acknowledge(playerId, selection.room.roomId, expiresAt)) {
          throw new HermesError(ErrorCode.PROTOCOL_ERROR, 'reservation ACK has no valid unexpired lease', ack.data)
        }
        this.backoff.delete(selection.room.roomId)

        this.log?.info(
          {
            playerId,
            roomId: selection.room.roomId,
            reason: 'policy_score',
            score: Math.round(selection.score),
            players: selection.room.players,
            capacity: selection.room.capacity,
            level: context.level ?? null,
            rawLevel: request.level ?? null,
            candidates: candidates.length + 1
          },
          'room allocated'
        )
        this.record({
          category: 'allocation',
          event: 'allocation.assigned',
          message: `player ${playerId} -> room ${
            selection.room.roomId
          } (score ${Math.round(selection.score)})`,
          roomId: selection.room.roomId,
          nodeId: selection.room.nodeId,
          playerIds: [playerId],
          data: {
            reason: 'policy_score',
            score: Math.round(selection.score),
            players: selection.room.players,
            capacity: selection.room.capacity,
            level: context.level ?? null,
            rawLevel: request.level ?? null,
            candidates: candidates.length + 1
          }
        })
        return {
          code: ErrorCode.OK,
          data: this.assignmentFor(selection.room, playerId, expiresAt)
        }
      } catch (error) {
        this.reservations.release(playerId)
        if (signal?.aborted) throw signal.reason ?? this.cancelled()
        if (isHermesError(error) && [ErrorCode.NODE_UNAVAILABLE, ErrorCode.NODE_TIMEOUT,
          ErrorCode.NO_CAPACITY, ErrorCode.STALE_PLAN].includes(error.code)) {
          this.deferTarget(selection.room.roomId)
          continue
        }
        throw error
      }
    }

    this.log?.warn(
      {
        playerId,
        candidates: candidates.length,
        rooms: this.rooms.listRooms().length,
        level: request.level ?? null
      },
      'allocation found no usable room'
    )
    this.record({
      category: 'allocation',
      event: 'allocation.none',
      message: `no usable room for player ${playerId}`,
      playerIds: [playerId],
      data: {
        level: context.level ?? null,
        rawLevel: request.level ?? null,
        candidates: candidates.length,
        rooms: this.rooms.listRooms().length,
        reservations: this.reservations.size()
      }
    })
    throw new HermesError(
      ErrorCode.NO_CAPACITY,
      'no room capacity is currently available'
    )
  }

  private cancelled (): HermesError {
    return new HermesError(ErrorCode.NODE_UNAVAILABLE, 'allocation caller cancelled')
  }

  private async waitFor (flight: AllocationFlight, signal?: AbortSignal): Promise<IAllocationReply> {
    flight.waiters += 1
    try {
      return await this.withSignal(flight.promise, signal)
    } finally {
      flight.waiters -= 1
      if (flight.waiters === 0) flight.controller.abort(this.cancelled())
    }
  }

  private async withSignal<T> (promise: Promise<T>, signal?: AbortSignal): Promise<T> {
    if (!signal) return promise
    if (signal.aborted) throw signal.reason ?? this.cancelled()
    let onAbort: () => void = () => {}
    const aborted = new Promise<never>((_resolve, reject) => {
      onAbort = () => reject(signal.reason ?? this.cancelled())
      signal.addEventListener('abort', onAbort, { once: true })
    })
    try {
      return await Promise.race([promise, aborted])
    } finally {
      signal.removeEventListener('abort', onAbort)
    }
  }

  private deferTarget (roomId: RoomId): void {
    const previous = this.backoff.get(roomId)
    const failures = Math.min(16, (previous?.failures ?? 0) + 1)
    const delay = Math.min(this.options.retryMaxMs ?? 30_000,
      (this.options.retryBaseMs ?? 2_000) * 2 ** (failures - 1))
    this.backoff.delete(roomId)
    this.backoff.set(roomId, { failures, until: Date.now() + delay })
    // Bound retained rejection history even in very large room fleets.
    if (this.backoff.size > 4096) this.backoff.delete(this.backoff.keys().next().value!)
  }

  private applyFacts(room: RoomSnapshot, ack: INodeAck): void {
    applyCapacityFacts(this.rooms, room, ack.data)
  }

  private record (event: DebugEventInput): void {
    this.events?.record(event)
  }

  private assignmentFor (
    room: {
      roomId: RoomAssignment['roomId']
      nodeId: RoomAssignment['nodeId']
      udpHost: RoomAssignment['udpHost']
      udpPort: RoomAssignment['udpPort']
    },
    playerId: PlayerId,
    leaseExpiresAt?: number
  ): RoomAssignment {
    const expiresAt = asUnixMs(leaseExpiresAt ?? (Date.now() + this.options.assignmentTtlMs))
    return {
      roomId: room.roomId,
      nodeId: room.nodeId,
      udpHost: room.udpHost,
      udpPort: room.udpPort,
      expiresAt
    }
  }
}
