import type { NodeId, PlayerId, RoomId } from '../types/branded.js'
import type { AudienceReactionDelivery, INodeAck, INodeEvent, RoomSnapshot } from '../types/contracts.js'
import { AudienceReactionSchema } from '../types/contracts.js'
import { ErrorCode } from '../types/errors.js'
import type { RoomCache } from './RoomCache.js'
import type { Logger } from '../infra/logger/logger.js'

export interface AudienceMembership {
  readonly authorityEpoch: string
  isAudienceSource(playerId: PlayerId, levelId: number, roomId: RoomId, nodeId: NodeId): boolean
  audienceRooms(levelId: number): RoomSnapshot[]
}
interface Sender {
  audienceReaction(room: RoomSnapshot, data: AudienceReactionDelivery, signal?: AbortSignal): Promise<INodeAck>
}
export interface AudienceReactionOptions {
  ttlMs?: number
  clock?: () => number
  perRoomLimit?: number
  globalLimit?: number
  concurrency?: number
  dedupTtlMs?: number
  dedupLimit?: number
  retryMs?: number
}
interface Pending {
  delivery: AudienceReactionDelivery
  sourceRoomId: RoomId
  sourceNodeId: NodeId
  target: RoomSnapshot
  abort: AbortController
}
interface RoomQueue { queue: Pending[]; busy: boolean; retryAt: number }
export interface AudienceReactionStats {
  accepted: number
  delivered: number
  duplicate: number
  rejected: number
  ignored: number
  expired: number
  stale: number
  capacity: number
  ackFailed: number
  pending: number
}

/** Short-lived ordinary audience pulses, independent of performance sessions. */
export class AudienceReactionService {
  private readonly clock: () => number
  private readonly ttlMs: number
  private readonly dedupTtlMs: number
  private readonly queues = new Map<RoomId, RoomQueue>()
  // These counters deliberately survive TTL pruning, room changes and reconnects.
  private readonly sequences = new Map<string, number>()
  private readonly seen = new Map<string, { fingerprint: string; at: number }>()
  private active = 0
  private scheduled = false
  private stopped = false
  private lastDebugAt = -Infinity
  private readonly counters: AudienceReactionStats = {
    accepted: 0, delivered: 0, duplicate: 0, rejected: 0, ignored: 0,
    expired: 0, stale: 0, capacity: 0, ackFailed: 0, pending: 0
  }
  public constructor (
    private readonly rooms: Pick<RoomCache, 'getRoom'>,
    private readonly membership: AudienceMembership,
    private readonly sender: Sender,
    private readonly options: AudienceReactionOptions = {},
    private readonly log?: Logger
  ) {
    this.clock = options.clock ?? Date.now
    // Allow the next normal 1Hz audience frame plus network/application ACK margin.
    this.ttlMs = options.ttlMs ?? 2000
    this.dedupTtlMs = options.dedupTtlMs ?? Math.max(30_000, this.ttlMs)
    for (const [name, value] of Object.entries({ ttlMs: this.ttlMs, dedupTtlMs: this.dedupTtlMs,
      perRoomLimit: options.perRoomLimit ?? 64, globalLimit: options.globalLimit ?? 4096,
      concurrency: options.concurrency ?? 8, dedupLimit: options.dedupLimit ?? 8192, retryMs: options.retryMs ?? 50 })) {
      if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`invalid reaction ${name}`)
    }
    if (this.dedupTtlMs < this.ttlMs) throw new Error('reaction dedup TTL must cover delivery TTL')
  }
  public stats (): AudienceReactionStats { return { ...this.counters } }
  public handle (event: INodeEvent): boolean {
    if (this.stopped || event.event !== 'audience.reaction') return false
    const d = event.data
    const parsed = AudienceReactionSchema.safeParse(d.reaction)
    if (!d.roomId || !d.player?.playerId || !Number.isSafeInteger(d.levelId) || !d.eventId || d.eventId.length > 128 || !parsed.success ||
      !this.membership.isAudienceSource(d.player.playerId, d.levelId!, d.roomId, d.nodeId)) {
      this.counters.rejected++; return false
    }
    const reaction = parsed.data
    if (reaction.flag7 === 0 && reaction.type === 0) { this.counters.ignored++; return true }
    const now = this.clock()
    this.prune(now)
    const fingerprint = JSON.stringify([d.nodeId, d.roomId, d.player.playerId, d.levelId, reaction.flag7, reaction.type])
    const seen = this.seen.get(d.eventId)
    if (seen) {
      if (seen.fingerprint !== fingerprint) { this.counters.rejected++; return false }
      this.counters.duplicate++; return true
    }
    const targets = this.membership.audienceRooms(d.levelId!).filter(room => room.roomId !== d.roomId)
    if (this.seen.size >= (this.options.dedupLimit ?? 8192) ||
      this.counters.pending + targets.length > (this.options.globalLimit ?? 4096) ||
      targets.some(room => (this.queues.get(room.roomId)?.queue.length ?? 0) >= (this.options.perRoomLimit ?? 64))) {
      this.counters.capacity++; this.debug('capacity'); return false
    }
    const key = JSON.stringify([d.levelId, d.player.playerId])
    const seq = (this.sequences.get(key) ?? 0) + 1
    if (!Number.isSafeInteger(seq)) { this.counters.rejected++; return false }
    this.sequences.set(key, seq)
    this.seen.set(d.eventId, { fingerprint, at: now })
    this.counters.accepted++
    const delivery: AudienceReactionDelivery = { authorityEpoch: this.membership.authorityEpoch,
      levelId: d.levelId!, playerId: d.player.playerId, eventId: d.eventId, eventSeq: seq,
      serverNowMs: now, expiresAtMs: now + this.ttlMs, reaction: { ...reaction } }
    for (const target of targets) {
      let output = this.queues.get(target.roomId)
      if (!output) { output = { queue: [], busy: false, retryAt: 0 }; this.queues.set(target.roomId, output) }
      output.queue.push({ delivery, sourceRoomId: d.roomId, sourceNodeId: d.nodeId, target: { ...target }, abort: new AbortController() })
      this.counters.pending++
    }
    this.schedule(); return true
  }
  private valid (pending: Pending): boolean {
    const d = pending.delivery
    const target = this.rooms.getRoom(pending.target.roomId)
    return Boolean(target && target.nodeId === pending.target.nodeId && target.udpHost === pending.target.udpHost && target.udpPort === pending.target.udpPort &&
      this.membership.isAudienceSource(d.playerId as PlayerId, d.levelId, pending.sourceRoomId, pending.sourceNodeId) &&
      this.membership.audienceRooms(d.levelId).some(room => room.roomId === pending.target.roomId && room.nodeId === pending.target.nodeId))
  }
  private remove (output: RoomQueue, pending: Pending, reason: 'expired' | 'stale' | 'delivered'): void {
    const index = output.queue.indexOf(pending)
    if (index < 0) return
    output.queue.splice(index, 1); this.counters.pending--; this.counters[reason]++
    pending.abort.abort()
    if (reason !== 'delivered') this.debug(reason)
  }
  private prune (now: number): void {
    for (const [id, entry] of this.seen) if (now - entry.at >= this.dedupTtlMs) this.seen.delete(id)
    for (const [roomId, output] of this.queues) {
      for (const pending of [...output.queue]) {
        if (now >= pending.delivery.expiresAtMs) this.remove(output, pending, 'expired')
        else if (!this.valid(pending)) this.remove(output, pending, 'stale')
      }
      if (!output.busy && output.queue.length === 0) this.queues.delete(roomId)
    }
  }
  public tick (): void { if (!this.stopped) { this.prune(this.clock()); this.schedule() } }
  /** Disconnect/reconnect and reclaimed rooms never receive or originate backlog. */
  public forgetRoom (roomId: RoomId): void {
    for (const output of this.queues.values()) for (const pending of [...output.queue]) {
      if (pending.sourceRoomId === roomId || pending.target.roomId === roomId) this.remove(output, pending, 'stale')
    }
  }
  public forgetNode (nodeId: NodeId): void {
    for (const output of this.queues.values()) for (const pending of [...output.queue]) {
      if (pending.sourceNodeId === nodeId || pending.target.nodeId === nodeId) this.remove(output, pending, 'stale')
    }
  }
  public stop (): void {
    this.stopped = true
    for (const output of this.queues.values()) {
      for (const pending of output.queue) pending.abort.abort()
      output.queue.length = 0
    }
    this.queues.clear(); this.seen.clear(); this.counters.pending = 0
  }
  private debug (reason: string): void {
    const now = this.clock()
    if (now - this.lastDebugAt < 5000) return
    this.lastDebugAt = now
    this.log?.debug({ reason, ...this.stats() }, 'audience reaction delivery summary')
  }
  private schedule (): void {
    if (this.stopped || this.scheduled) return
    this.scheduled = true
    queueMicrotask(() => { this.scheduled = false; this.pump() })
  }
  private pump (): void {
    if (this.stopped) return
    this.prune(this.clock())
    for (const [roomId, output] of [...this.queues]) {
      if (this.active >= (this.options.concurrency ?? 8)) break
      if (output.busy || output.retryAt > this.clock() || !output.queue.length) continue
      this.queues.delete(roomId); this.queues.set(roomId, output)
      output.busy = true; this.active++
      void this.flush(output).catch(() => {
        this.counters.ackFailed++; output.retryAt = this.clock() + (this.options.retryMs ?? 50); this.debug('ack_failed')
      }).finally(() => { output.busy = false; this.active--; this.schedule() })
    }
  }
  private async flush (output: RoomQueue): Promise<void> {
    for (let n = 0; n < 16 && !this.stopped; n++) {
      const pending = output.queue[0]
      if (!pending) return
      const now = this.clock()
      if (now >= pending.delivery.expiresAtMs) { this.remove(output, pending, 'expired'); continue }
      if (!this.valid(pending)) { this.remove(output, pending, 'stale'); continue }
      const ack = await this.sender.audienceReaction(pending.target, { ...pending.delivery, serverNowMs: now }, pending.abort.signal)
      if (!output.queue.includes(pending)) continue
      if (this.clock() >= pending.delivery.expiresAtMs) { this.remove(output, pending, 'expired'); continue }
      if (!this.valid(pending)) { this.remove(output, pending, 'stale'); continue }
      const data = ack.data as Record<string, unknown> | undefined
      if (ack.code !== ErrorCode.OK || data?.applied !== true || data.authorityEpoch !== pending.delivery.authorityEpoch ||
        data.eventId !== pending.delivery.eventId || data.eventSeq !== pending.delivery.eventSeq || data.buffered === true) throw new Error('reaction not applied')
      this.remove(output, pending, 'delivered')
    }
  }
}
