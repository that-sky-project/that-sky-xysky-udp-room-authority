import { randomUUID } from 'node:crypto'
import type { NodeId, RoomId } from '../types/branded.js'
import type { AudienceChatGlobalState, INodeAck, RoomSnapshot } from '../types/contracts.js'
import { ErrorCode, HermesError } from '../types/errors.js'
import type { Logger } from '../infra/logger/logger.js'
import type { RoomCache } from './RoomCache.js'

export interface AudienceChatGlobalOptions {
  maxSeconds: number
  clock?: () => number
  concurrency?: number
  retryMs?: number
  idempotencyLimit?: number
  idempotencyTtlMs?: number
}

interface Sender {
  chatGlobal(room: RoomSnapshot, data: AudienceChatGlobalState, signal?: AbortSignal): Promise<INodeAck>
}

interface LevelState {
  revision: number
  enabled: boolean
  expiresAtMs: number
  lastEmittedRemaining: number
}

interface Delivery {
  room: RoomSnapshot
  levelId: number
  pending: AudienceChatGlobalState | undefined
  appliedRevision: number
  busy: boolean
  retryAt: number
  abort: AbortController
}

interface IdempotencyEntry {
  fingerprint: string
  state: AudienceChatGlobalState
  at: number
}

const sameRoom = (left: RoomSnapshot, right: RoomSnapshot): boolean =>
  left.roomId === right.roomId && left.nodeId === right.nodeId &&
  left.udpHost === right.udpHost && left.udpPort === right.udpPort

/** QWD-authoritative per-level global-chat countdown and reliable room fanout. */
export class AudienceChatGlobalService {
  public readonly authorityEpoch = randomUUID()
  private readonly clock: () => number
  private readonly concurrency: number
  private readonly retryMs: number
  private readonly idempotencyLimit: number
  private readonly idempotencyTtlMs: number
  private readonly levels = new Map<number, LevelState>()
  private readonly deliveries = new Map<string, Delivery>()
  private readonly requests = new Map<string, IdempotencyEntry>()
  private active = 0
  private scheduled = false
  private stopped = false

  public constructor (
    private readonly rooms: Pick<RoomCache, 'getRoom' | 'levelsByRoom' | 'listRoomsByNode'>,
    private readonly sender: Sender,
    private readonly options: AudienceChatGlobalOptions,
    private readonly log?: Logger
  ) {
    this.clock = options.clock ?? Date.now
    this.concurrency = options.concurrency ?? 8
    this.retryMs = options.retryMs ?? 100
    this.idempotencyLimit = options.idempotencyLimit ?? 4096
    this.idempotencyTtlMs = options.idempotencyTtlMs ?? 300_000
    for (const [name, value] of Object.entries({ maxSeconds: options.maxSeconds, concurrency: this.concurrency,
      retryMs: this.retryMs, idempotencyLimit: this.idempotencyLimit, idempotencyTtlMs: this.idempotencyTtlMs })) {
      if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`invalid audience chat global ${name}`)
    }
  }

  private key (roomId: RoomId, levelId: number): string { return JSON.stringify([roomId, levelId]) }

  private validateLevelId (levelId: number): void {
    if (!Number.isSafeInteger(levelId) || levelId < 0 || levelId > 0xffffffff) {
      throw new HermesError(ErrorCode.BAD_REQUEST, 'levelId must be a uint32')
    }
  }

  private remaining (level: LevelState, now: number): number {
    return level.enabled && level.expiresAtMs > now ? Math.ceil((level.expiresAtMs - now) / 1000) : 0
  }

  private snapshot (levelId: number, level: LevelState | undefined, now: number): AudienceChatGlobalState {
    const remainingSeconds = level ? this.remaining(level, now) : 0
    return {
      authorityEpoch: this.authorityEpoch,
      revision: level?.revision ?? 0,
      levelId,
      enabled: remainingSeconds > 0,
      remainingSeconds,
      serverNowMs: now,
      expiresAtMs: remainingSeconds > 0 ? level!.expiresAtMs : now
    }
  }

  public state (levelId: number): AudienceChatGlobalState {
    this.validateLevelId(levelId)
    const now = this.clock()
    this.advanceLevel(levelId, now)
    return this.snapshot(levelId, this.levels.get(levelId), now)
  }

  public set (levelId: number, enabled: boolean, seconds?: number, idempotencyKey?: string): AudienceChatGlobalState {
    this.validateLevelId(levelId)
    if (typeof enabled !== 'boolean') throw new HermesError(ErrorCode.BAD_REQUEST, 'enabled must be a boolean')
    if (enabled && (!Number.isSafeInteger(seconds) || seconds! <= 0 || seconds! > this.options.maxSeconds)) {
      throw new HermesError(ErrorCode.BAD_REQUEST, `seconds must be an integer from 1 to ${this.options.maxSeconds}`)
    }
    if (!enabled && seconds !== undefined && (!Number.isSafeInteger(seconds) || seconds !== 0)) {
      throw new HermesError(ErrorCode.BAD_REQUEST, 'seconds must be omitted or zero when disabled')
    }
    const now = this.clock()
    this.pruneRequests(now)
    const fingerprint = JSON.stringify([levelId, enabled, enabled ? seconds : 0])
    if (idempotencyKey !== undefined) {
      const previous = this.requests.get(idempotencyKey)
      if (previous) {
        if (previous.fingerprint !== fingerprint) {
          throw new HermesError(ErrorCode.CONFLICT, 'idempotency key was already used with a different request')
        }
        return { ...previous.state }
      }
    }
    let level = this.levels.get(levelId)
    if (!level) {
      level = { revision: 0, enabled: false, expiresAtMs: now, lastEmittedRemaining: 0 }
      this.levels.set(levelId, level)
    }
    level.revision++
    level.enabled = enabled
    level.expiresAtMs = enabled ? now + seconds! * 1000 : now
    level.lastEmittedRemaining = enabled ? seconds! : 0
    const state = this.snapshot(levelId, level, now)
    this.queueState(state)
    if (idempotencyKey !== undefined) {
      this.requests.set(idempotencyKey, { fingerprint, state: { ...state }, at: now })
      this.enforceRequestLimit()
    }
    this.schedule()
    return state
  }

  private advanceLevel (levelId: number, now: number): void {
    const level = this.levels.get(levelId)
    if (!level || !level.enabled) return
    const remainingSeconds = this.remaining(level, now)
    if (remainingSeconds === level.lastEmittedRemaining) return
    level.revision++
    if (remainingSeconds === 0) {
      level.enabled = false
      level.expiresAtMs = now
    }
    level.lastEmittedRemaining = remainingSeconds
    this.queueState(this.snapshot(levelId, level, now))
  }

  private queueState (state: AudienceChatGlobalState): void {
    const targets = this.rooms.levelsByRoom(this.clock())
    const current = new Set<string>()
    for (const [roomId, levels] of targets) {
      if (!levels.has(state.levelId)) continue
      const room = this.rooms.getRoom(roomId)
      if (!room) continue
      const key = this.key(roomId, state.levelId)
      current.add(key)
      let delivery = this.deliveries.get(key)
      if (delivery && !sameRoom(delivery.room, room)) {
        delivery.abort.abort()
        this.deliveries.delete(key)
        delivery = undefined
      }
      if (!delivery) {
        delivery = { room: { ...room }, levelId: state.levelId, pending: undefined,
          appliedRevision: -1, busy: false, retryAt: 0, abort: new AbortController() }
        this.deliveries.set(key, delivery)
      }
      if (state.revision > delivery.appliedRevision && (!delivery.pending || state.revision >= delivery.pending.revision)) {
        delivery.pending = { ...state }
        delivery.retryAt = 0
      }
    }
    for (const [key, delivery] of this.deliveries) {
      if (delivery.levelId !== state.levelId || current.has(key)) continue
      // A level removal closes an enabled client state before this room/level is forgotten.
      if (delivery.pending?.enabled || delivery.appliedRevision >= 0) {
        const close = { ...state, enabled: false, remainingSeconds: 0, expiresAtMs: state.serverNowMs }
        delivery.pending = close
        delivery.retryAt = 0
      } else if (!delivery.busy) this.deliveries.delete(key)
    }
  }

  private refreshTargets (now: number): void {
    const levelsByRoom = this.rooms.levelsByRoom(now)
    for (const [levelId, level] of this.levels) {
      const state = this.snapshot(levelId, level, now)
      for (const [roomId, levels] of levelsByRoom) {
        if (!levels.has(levelId)) continue
        const room = this.rooms.getRoom(roomId)
        if (!room) continue
        const key = this.key(roomId, levelId)
        let delivery = this.deliveries.get(key)
        if (delivery && !sameRoom(delivery.room, room)) {
          delivery.abort.abort(); this.deliveries.delete(key); delivery = undefined
        }
        if (!delivery) {
          delivery = { room: { ...room }, levelId, pending: { ...state }, appliedRevision: -1,
            busy: false, retryAt: 0, abort: new AbortController() }
          this.deliveries.set(key, delivery)
        } else if (state.enabled && !delivery.pending?.enabled && delivery.appliedRevision < state.revision) {
          // The room lost this level while an unsent close tombstone (or a
          // revision bumped during the outage window) was parked on the
          // delivery — e.g. a node outage swept its player locations. The
          // level is hosted here again, so that close is obsolete: re-project
          // the enabled countdown instead of delivering a stale close. When
          // the close was already applied, advanceLevel's next second tick
          // re-queues through queueState.
          delivery.pending = { ...state }
          delivery.retryAt = 0
        }
      }
    }
    for (const [key, delivery] of this.deliveries) {
      const room = this.rooms.getRoom(delivery.room.roomId)
      if (!room || !sameRoom(room, delivery.room)) {
        delivery.abort.abort(); this.deliveries.delete(key); continue
      }
      if (levelsByRoom.get(delivery.room.roomId)?.has(delivery.levelId)) continue
      const pending = delivery.pending
      if ((pending?.enabled ?? false) || delivery.appliedRevision >= 0) {
        const level = this.levels.get(delivery.levelId)
        const revision = level?.revision ?? pending?.revision ?? 0
        delivery.pending = { authorityEpoch: this.authorityEpoch, revision, levelId: delivery.levelId,
          enabled: false, remainingSeconds: 0, serverNowMs: now, expiresAtMs: now }
        delivery.retryAt = 0
      } else if (!delivery.busy) this.deliveries.delete(key)
    }
  }

  public tick (): void {
    if (this.stopped) return
    const now = this.clock()
    this.pruneRequests(now)
    for (const levelId of this.levels.keys()) this.advanceLevel(levelId, now)
    this.refreshTargets(now)
    this.schedule()
  }

  public reconnectNode (nodeId: NodeId): void {
    if (this.stopped) return
    const roomIds = new Set(this.rooms.listRoomsByNode(nodeId).map(room => room.roomId))
    for (const [key, delivery] of this.deliveries) {
      if (delivery.room.nodeId === nodeId || roomIds.has(delivery.room.roomId)) {
        delivery.abort.abort(); this.deliveries.delete(key)
      }
    }
    this.tick()
  }

  /** Drops transport state only; authoritative per-level expiry remains intact. */
  public forgetRoom (roomId: RoomId): void {
    for (const [key, delivery] of this.deliveries) {
      if (delivery.room.roomId !== roomId) continue
      delivery.abort.abort(); this.deliveries.delete(key)
    }
  }

  public forgetNode (nodeId: NodeId): void {
    for (const [key, delivery] of this.deliveries) {
      if (delivery.room.nodeId !== nodeId) continue
      delivery.abort.abort(); this.deliveries.delete(key)
    }
  }

  public stop (): void {
    this.stopped = true
    for (const delivery of this.deliveries.values()) delivery.abort.abort()
    this.deliveries.clear()
    this.requests.clear()
  }

  private pruneRequests (now: number): void {
    for (const [key, entry] of this.requests) if (now - entry.at >= this.idempotencyTtlMs) this.requests.delete(key)
  }

  private enforceRequestLimit (): void {
    while (this.requests.size > this.idempotencyLimit) this.requests.delete(this.requests.keys().next().value!)
  }

  private schedule (): void {
    if (this.stopped || this.scheduled) return
    this.scheduled = true
    queueMicrotask(() => { this.scheduled = false; this.pump() })
  }

  private pump (): void {
    if (this.stopped) return
    for (const [key, delivery] of [...this.deliveries]) {
      if (this.active >= this.concurrency) break
      if (delivery.busy || delivery.retryAt > this.clock() || !delivery.pending) continue
      this.deliveries.delete(key); this.deliveries.set(key, delivery)
      delivery.busy = true; this.active++
      void this.flush(delivery).catch(error => {
        delivery.retryAt = this.clock() + this.retryMs
        this.log?.debug({ roomId: delivery.room.roomId, levelId: delivery.levelId,
          reason: error instanceof Error ? error.message : 'delivery failed' }, 'audience chat-global state will retry')
      }).finally(() => { delivery.busy = false; this.active--; this.schedule() })
    }
  }

  private valid (delivery: Delivery): boolean {
    const room = this.rooms.getRoom(delivery.room.roomId)
    return this.deliveries.get(this.key(delivery.room.roomId, delivery.levelId)) === delivery &&
      Boolean(room && sameRoom(room, delivery.room))
  }

  private async flush (delivery: Delivery): Promise<void> {
    const state = delivery.pending
    if (!state || !this.valid(delivery)) return
    const outbound: AudienceChatGlobalState = { ...state, serverNowMs: this.clock() }
    const ack = await this.sender.chatGlobal(delivery.room, outbound, delivery.abort.signal)
    if (this.stopped || !this.valid(delivery) || delivery.pending !== state) return
    const data = ack.data as Record<string, unknown> | undefined
    if (ack.code !== ErrorCode.OK || data?.applied !== true || data.authorityEpoch !== outbound.authorityEpoch ||
      data.revision !== outbound.revision || data.levelId !== outbound.levelId || data.enabled !== outbound.enabled ||
      data.remainingSeconds !== outbound.remainingSeconds) throw new Error('audience chat-global state not applied')
    delivery.appliedRevision = outbound.revision
    delivery.pending = undefined
    delivery.retryAt = 0
    if (!this.rooms.levelsByRoom(this.clock()).get(delivery.room.roomId)?.has(delivery.levelId)) {
      this.deliveries.delete(this.key(delivery.room.roomId, delivery.levelId))
    }
  }
}
