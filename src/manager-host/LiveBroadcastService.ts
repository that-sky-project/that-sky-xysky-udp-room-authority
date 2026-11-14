import { createHash, randomUUID } from 'node:crypto'
import type { NodeId, PlayerId, RoomId } from '../types/branded.js'
import { asPlayerId } from '../types/branded.js'
import type { INodeAck, INodeEvent, LiveBroadcastCapability, LiveBroadcastCommand, LiveBroadcastDelivery, RoomSnapshot } from '../types/contracts.js'
import { LiveBroadcastPublishSchema, LiveBroadcastSyncSchema, PlayerUuidSchema } from '../types/contracts.js'
import { audienceLevelIds } from './SeatAllocator.js'
import type { AudienceMembership } from './AudienceReactionService.js'
import type { RoomCache } from './RoomCache.js'
import type { Logger } from '../infra/logger/logger.js'

export interface LiveBroadcastOptions {
  allowedPlayerUuids: string[]
  playerNames?: Record<string, string>
  /** Venue level ids that carry the live-broadcast button; defaults to the
   *  configured audience level union (seated ∪ hall). */
  levels?: number[]
  clock?: () => number
  capabilityTtlMs?: number
  refreshMs?: number
  eventTtlMs?: number
  idleMs?: number
  maxStreamsPerLevel?: number
  perRoomLimit?: number
  globalLimit?: number
  concurrency?: number
  dedupLimit?: number
}
interface Sender {
  liveBroadcast(room: RoomSnapshot, cmd: LiveBroadcastCommand, data: LiveBroadcastCapability | LiveBroadcastDelivery, signal?: AbortSignal): Promise<INodeAck>
}
interface Stream {
  playerId: PlayerId; levelId: number; sourceStreamId: string; streamId: string
  source: RoomSnapshot; capabilityId: string; eventSeq: number; lastAt: number
}
interface Pending { delivery: LiveBroadcastDelivery; stream: Stream; abort: AbortController }
interface Destination {
  room: RoomSnapshot; capability: LiveBroadcastCapability; fingerprint: string
  ackRevision: number; refreshAt: number; retryAt: number; busy: boolean
  abort: AbortController; queue: Pending[]
}
const sameRoom = (a: RoomSnapshot, b: RoomSnapshot): boolean => a.roomId === b.roomId && a.nodeId === b.nodeId && a.udpHost === b.udpHost && a.udpPort === b.udpPort

/** Independent ordinary live streams. The seven-stream policy reserves one playback slot for piano. */
export class LiveBroadcastService {
  private readonly clock: () => number
  private readonly options: Required<Omit<LiveBroadcastOptions, 'clock' | 'allowedPlayerUuids' | 'playerNames' | 'levels'>>
  private readonly levels: number[]
  private readonly allowed: PlayerId[]
  private readonly playerNames = new Map<PlayerId, string>()
  private readonly destinations = new Map<string, Destination>()
  private readonly streams = new Map<string, Stream>()
  private readonly retired = new Map<string, number>()
  private readonly seen = new Map<string, { fingerprint: string; expiresAt: number }>()
  private active = 0
  private scheduled = false
  private stopped = false
  private pending = 0
  // One epoch-wide counter keeps every room/level monotonic across ID rotations without tombstone growth.
  private revision = 0
  private readonly counters = { accepted: 0, delivered: 0, rejected: 0, duplicate: 0, expired: 0, stale: 0, capacity: 0, ackFailed: 0 }
  public constructor (
    private readonly rooms: Pick<RoomCache, 'getRoom'>,
    private readonly membership: AudienceMembership & { currentPerformer(levelId: number): PlayerId | undefined },
    private readonly sender: Sender,
    options: LiveBroadcastOptions,
    private readonly log?: Logger
  ) {
    this.clock = options.clock ?? Date.now
    this.allowed = [...new Set(options.allowedPlayerUuids.map(value => asPlayerId(PlayerUuidSchema.parse(value))))].sort()
    for (const [playerId, name] of Object.entries(options.playerNames ?? {})) {
      const id = asPlayerId(PlayerUuidSchema.parse(playerId))
      if (!this.allowed.includes(id) || !name || name !== name.trim() || Array.from(name).length > 64 || /[\u0000-\u001f\u007f]/u.test(name)) throw new Error('invalid live broadcast player name')
      const previous = this.playerNames.get(id)
      if (previous !== undefined && previous !== name) throw new Error('conflicting live broadcast player name')
      this.playerNames.set(id, name)
    }
    // The capability lease must cover transient node↔QWD WebSocket blips: a
    // 5s lease combined with revoke-on-disconnect made the in-game broadcast
    // state disappear under公网 WSS jitter and required a map re-entry to
    // restore. Nodes keep leases across short blips (they expire them
    // fail-closed at their own deadline), so QWD only needs to keep refreshing
    // the same capabilityId across reconnects — a 30s lease with a 10s refresh
    // gives ~3 refresh attempts of margin while every outage still clears
    // within one TTL window.
    this.options = { capabilityTtlMs: options.capabilityTtlMs ?? 30_000, refreshMs: options.refreshMs ?? 10_000,
      eventTtlMs: options.eventTtlMs ?? 3000, idleMs: options.idleMs ?? 5000,
      maxStreamsPerLevel: options.maxStreamsPerLevel ?? 7, perRoomLimit: options.perRoomLimit ?? 32,
      globalLimit: options.globalLimit ?? 512, concurrency: options.concurrency ?? 8, dedupLimit: options.dedupLimit ?? 8192 }
    for (const [name, value] of Object.entries(this.options)) if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`invalid live broadcast ${name}`)
    if (this.options.maxStreamsPerLevel > 7 || this.options.refreshMs >= this.options.capabilityTtlMs) throw new Error('invalid live broadcast capacity or refresh interval')
    // Button venues default to the configured audience union (seated ∪ hall).
    this.levels = [...new Set(options.levels ?? audienceLevelIds())].filter(id => Number.isInteger(id) && id > 0)
    if (this.levels.length === 0) throw new Error('invalid live broadcast levels: no audience venues configured')
  }
  public displayName (playerId: string): string | undefined {
    const parsed = PlayerUuidSchema.safeParse(playerId)
    return parsed.success ? this.playerNames.get(asPlayerId(parsed.data)) : undefined
  }
  public stats (): ReturnType<LiveBroadcastService['snapshot']> { return this.snapshot() }
  private snapshot () { return { ...this.counters, pending: this.pending, streams: this.streams.size, capabilities: this.destinations.size } }
  private key (roomId: RoomId, levelId: number): string { return JSON.stringify([roomId, levelId]) }
  private sourceAllowed (playerId: PlayerId, levelId: number, room: RoomSnapshot): boolean {
    return this.allowed.includes(playerId) && this.membership.currentPerformer(levelId)?.toLowerCase() !== playerId.toLowerCase() &&
      this.membership.isAudienceSource(playerId, levelId, room.roomId, room.nodeId)
  }
  private allowedFor (room: RoomSnapshot, levelId: number): PlayerId[] {
    // Capability controls the approved button in every role; only ordinary content excludes the formal performer.
    return this.allowed.filter(id => this.membership.isAudienceSource(id, levelId, room.roomId, room.nodeId))
  }
  private fingerprint (room: RoomSnapshot, allowed: string[]): string { return JSON.stringify([room.nodeId, room.udpHost, room.udpPort, allowed]) }
  private refresh (now: number): void {
    const current = new Set<string>()
    for (const levelId of this.levels) for (const candidate of this.membership.audienceRooms(levelId)) {
      const room = this.rooms.getRoom(candidate.roomId)
      if (!room || !sameRoom(room, candidate)) continue
      const key = this.key(room.roomId, levelId)
      current.add(key)
      const allowed = this.allowedFor(room, levelId)
      const fingerprint = this.fingerprint(room, allowed)
      let target = this.destinations.get(key)
      if (target && target.fingerprint !== fingerprint) { this.dropDestination(target); this.destinations.delete(key); target = undefined }
      if (!target) {
        target = { room: { ...room }, fingerprint, capability: { authorityEpoch: this.membership.authorityEpoch,
          capabilityId: randomUUID(), revision: ++this.revision, levelId, allowedPlayerIds: allowed, serverNowMs: now, expiresAtMs: now + this.options.capabilityTtlMs },
          ackRevision: 0, refreshAt: now + this.options.refreshMs, retryAt: 0, busy: false, abort: new AbortController(), queue: [] }
        this.destinations.set(key, target)
      } else if (!target.busy && now >= target.refreshAt) {
        target.capability = { ...target.capability, revision: ++this.revision, serverNowMs: now, expiresAtMs: now + this.options.capabilityTtlMs }
        target.refreshAt = now + this.options.refreshMs
      }
    }
    for (const [key, target] of this.destinations) if (!current.has(key)) { this.dropDestination(target); this.destinations.delete(key) }
  }
  public handle (event: INodeEvent): boolean {
    if (this.stopped) return false
    const d = event.data
    if (event.event === 'liveBroadcast.sync') {
      const parsed = LiveBroadcastSyncSchema.safeParse(d)
      if (!parsed.success || !d.roomId || this.rooms.getRoom(d.roomId)?.nodeId !== d.nodeId) return false
      this.tick()
      for (const levelId of parsed.data.levelIds) {
        const target = this.destinations.get(this.key(d.roomId, levelId))
        if (target) { target.ackRevision = 0; target.retryAt = 0 }
      }
      this.schedule(); return true
    }
    if (event.event !== 'liveBroadcast.publish') return false
    const parsed = LiveBroadcastPublishSchema.safeParse(d)
    if (!parsed.success || !d.roomId || !d.player || !this.levels.includes(parsed.data.levelId)) return this.reject()
    const now = this.clock()
    const lease = this.destinations.get(this.key(d.roomId, parsed.data.levelId))?.capability
    // A late upload cannot renew its own expired authorization. Periodic tick/sync issues the next lease.
    if (!lease || lease.capabilityId !== parsed.data.capabilityId || now >= lease.expiresAtMs) return this.reject()
    this.prune(now); this.refresh(now)
    const input = parsed.data
    const playerId = asPlayerId(input.player.playerId)
    const origin = this.destinations.get(this.key(d.roomId, input.levelId))
    if (!origin || origin.room.nodeId !== d.nodeId || origin.capability.capabilityId !== input.capabilityId ||
      now >= origin.capability.expiresAtMs || Math.abs(input.timestamp * 1000 - now) > 10_000 ||
      !this.sourceAllowed(playerId, input.levelId, origin.room)) return this.reject()
    const fingerprint = createHash('sha256').update(JSON.stringify([d.nodeId, d.roomId, input.levelId, playerId, input.capabilityId,
      input.sourceStreamId, input.timestamp, input.actionId, input.payloadBase64])).digest('hex')
    const seen = this.seen.get(input.eventId)
    if (seen) {
      if (seen.fingerprint !== fingerprint) return this.reject()
      this.counters.duplicate++; return true
    }
    const streamKey = JSON.stringify([input.levelId, playerId])
    const retiredKey = JSON.stringify([playerId, input.sourceStreamId])
    if (this.retired.has(retiredKey)) return this.reject()
    let stream = this.streams.get(streamKey)
    const restarting = stream && (stream.sourceStreamId !== input.sourceStreamId || !sameRoom(stream.source, origin.room) || stream.capabilityId !== input.capabilityId)
    const targets = [...this.destinations.values()].filter(target => target.capability.levelId === input.levelId)
    const roomPending = (roomId: RoomId): number => [...this.destinations.values()].filter(t => t.room.roomId === roomId).reduce((n, t) => n + t.queue.length, 0)
    if (this.seen.size >= this.options.dedupLimit || this.pending + targets.length > this.options.globalLimit ||
      targets.some(target => roomPending(target.room.roomId) >= this.options.perRoomLimit) ||
      (!stream && [...this.streams.values()].filter(s => s.levelId === input.levelId).length >= this.options.maxStreamsPerLevel) ||
      ((!stream || restarting) && this.retired.size + this.streams.size >= this.options.dedupLimit)) { this.counters.capacity++; return false }
    if (restarting && stream) { this.retire(stream, now); stream = undefined }
    if (!stream) {
      stream = { playerId, levelId: input.levelId, sourceStreamId: input.sourceStreamId, streamId: randomUUID(),
        source: { ...origin.room }, capabilityId: input.capabilityId, eventSeq: 0, lastAt: now }
      this.streams.set(streamKey, stream)
    }
    stream.lastAt = now; stream.eventSeq++
    this.seen.set(input.eventId, { fingerprint, expiresAt: now + 30_000 })
    this.counters.accepted++
    for (const target of targets) {
      const delivery: LiveBroadcastDelivery = { authorityEpoch: this.membership.authorityEpoch, capabilityId: target.capability.capabilityId,
        levelId: stream.levelId, playerId, streamId: stream.streamId, eventId: input.eventId, eventSeq: stream.eventSeq,
        serverNowMs: now, expiresAtMs: now + this.options.eventTtlMs, timestamp: input.timestamp,
        actionId: input.actionId, payloadBase64: input.payloadBase64 }
      target.queue.push({ delivery, stream, abort: new AbortController() }); this.pending++
    }
    this.schedule(); return true
  }
  private reject (): false { this.counters.rejected++; return false }
  private sourceValid (stream: Stream): boolean {
    const room = this.rooms.getRoom(stream.source.roomId)
    const origin = this.destinations.get(this.key(stream.source.roomId, stream.levelId))
    return this.streams.get(JSON.stringify([stream.levelId, stream.playerId])) === stream &&
      Boolean(room && sameRoom(room, stream.source) && origin?.capability.capabilityId === stream.capabilityId &&
      this.clock() < origin.capability.expiresAtMs && this.sourceAllowed(stream.playerId, stream.levelId, room))
  }
  private targetValid (target: Destination): boolean {
    const room = this.rooms.getRoom(target.room.roomId)
    return this.destinations.get(this.key(target.room.roomId, target.capability.levelId)) === target && Boolean(room && sameRoom(room, target.room)) &&
      target.fingerprint === this.fingerprint(target.room, this.allowedFor(target.room, target.capability.levelId)) &&
      this.membership.audienceRooms(target.capability.levelId).some(r => sameRoom(r, target.room))
  }
  private remove (target: Destination, pending: Pending, reason: 'delivered' | 'expired' | 'stale'): void {
    const index = target.queue.indexOf(pending)
    if (index < 0) return
    target.queue.splice(index, 1); pending.abort.abort(); this.pending--; this.counters[reason]++
  }
  private dropDestination (target: Destination): void {
    target.abort.abort()
    for (const pending of [...target.queue]) this.remove(target, pending, 'stale')
  }
  private retire (stream: Stream, now: number): void {
    // Admission reserves a tombstone slot for every active stream, including restart replacements.
    this.retired.set(JSON.stringify([stream.playerId, stream.sourceStreamId]), now + 30_000)
    this.streams.delete(JSON.stringify([stream.levelId, stream.playerId]))
  }
  private prune (now: number): void {
    for (const [key, expires] of this.retired) if (now >= expires) this.retired.delete(key)
    for (const [key, entry] of this.seen) if (now >= entry.expiresAt) this.seen.delete(key)
    for (const stream of this.streams.values()) if (now - stream.lastAt >= this.options.idleMs || !this.sourceValid(stream)) this.retire(stream, now)
    for (const target of this.destinations.values()) for (const pending of [...target.queue]) {
      if (now >= pending.delivery.expiresAtMs) this.remove(target, pending, 'expired')
      else if (!this.sourceValid(pending.stream) || !this.targetValid(target)) this.remove(target, pending, 'stale')
    }
  }
  public tick (): void {
    if (this.stopped) return
    const now = this.clock(); this.prune(now); this.refresh(now); this.schedule()
  }
  public forgetRoom (roomId: RoomId): void {
    for (const [key, target] of this.destinations) if (target.room.roomId === roomId) { this.dropDestination(target); this.destinations.delete(key) }
    for (const target of this.destinations.values()) for (const pending of [...target.queue]) if (pending.stream.source.roomId === roomId) this.remove(target, pending, 'stale')
    for (const stream of this.streams.values()) if (stream.source.roomId === roomId) this.retire(stream, this.clock())
  }
  public forgetNode (nodeId: NodeId): void {
    const rooms = new Set<RoomId>()
    for (const target of this.destinations.values()) if (target.room.nodeId === nodeId) rooms.add(target.room.roomId)
    for (const stream of this.streams.values()) if (stream.source.nodeId === nodeId) rooms.add(stream.source.roomId)
    for (const room of rooms) this.forgetRoom(room)
  }
  public reconnectNode (nodeId: NodeId): void { this.forgetNode(nodeId); this.tick() }
  public stop (): void {
    this.stopped = true
    for (const target of this.destinations.values()) this.dropDestination(target)
    this.destinations.clear(); this.streams.clear(); this.retired.clear(); this.seen.clear()
  }
  private schedule (): void {
    if (this.stopped || this.scheduled) return
    this.scheduled = true
    queueMicrotask(() => { this.scheduled = false; this.pump() })
  }
  private pump (): void {
    if (this.stopped) return
    for (const [key, target] of [...this.destinations]) {
      if (this.active >= this.options.concurrency) break
      if (target.busy || target.retryAt > this.clock() || (target.ackRevision === target.capability.revision && !target.queue.length)) continue
      this.destinations.delete(key); this.destinations.set(key, target)
      target.busy = true; this.active++
      void this.flush(target).catch(() => {
        this.counters.ackFailed++; target.retryAt = this.clock() + 100
        this.log?.debug({ roomId: target.room.roomId }, 'live broadcast awaiting application ACK')
      }).finally(() => { target.busy = false; this.active--; this.schedule() })
    }
  }
  private async flush (target: Destination): Promise<void> {
    if (!this.targetValid(target)) {
      this.dropDestination(target)
      const key = this.key(target.room.roomId, target.capability.levelId)
      if (this.destinations.get(key) === target) this.destinations.delete(key)
      return
    }
    if (target.ackRevision !== target.capability.revision || this.clock() >= target.capability.expiresAtMs) {
      if (this.clock() >= target.capability.expiresAtMs) {
        const now = this.clock()
        target.capability = { ...target.capability, revision: ++this.revision, serverNowMs: now, expiresAtMs: now + this.options.capabilityTtlMs }
        target.refreshAt = now + this.options.refreshMs
      }
      const capability = target.capability
      const ack = await this.sender.liveBroadcast(target.room, 'liveBroadcast.capability', { ...capability, serverNowMs: this.clock() }, target.abort.signal)
      if (this.stopped || !this.targetValid(target)) return
      const data = ack.data as Record<string, unknown> | undefined
      if (ack.code !== 0 || data?.applied !== true || data.buffered === true || data.authorityEpoch !== capability.authorityEpoch ||
        data.capabilityId !== capability.capabilityId || data.revision !== capability.revision || this.clock() >= capability.expiresAtMs) throw new Error('live capability not applied')
      target.ackRevision = capability.revision
    }
    for (let n = 0; n < 16 && !this.stopped; n++) {
      const pending = target.queue[0]
      if (!pending) return
      if (this.clock() >= pending.delivery.expiresAtMs) { this.remove(target, pending, 'expired'); continue }
      if (!this.sourceValid(pending.stream) || !this.targetValid(target)) { this.remove(target, pending, 'stale'); continue }
      if (this.clock() >= target.capability.expiresAtMs || target.ackRevision !== target.capability.revision) return
      const delivery = { ...pending.delivery, serverNowMs: this.clock() }
      const ack = await this.sender.liveBroadcast(target.room, 'liveBroadcast.deliver', delivery, pending.abort.signal)
      if (!target.queue.includes(pending)) continue
      if (this.clock() >= delivery.expiresAtMs) { this.remove(target, pending, 'expired'); continue }
      if (!this.sourceValid(pending.stream) || !this.targetValid(target)) { this.remove(target, pending, 'stale'); continue }
      const data = ack.data as Record<string, unknown> | undefined
      if (ack.code !== 0 || data?.applied !== true || data.buffered === true || data.authorityEpoch !== delivery.authorityEpoch ||
        data.streamId !== delivery.streamId || data.eventId !== delivery.eventId || data.eventSeq !== delivery.eventSeq) throw new Error('live event not applied')
      this.remove(target, pending, 'delivered')
    }
  }
}
