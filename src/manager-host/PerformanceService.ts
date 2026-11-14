import { randomUUID, createHash } from 'node:crypto'
import type { NodeId, PlayerId, RoomId } from '../types/branded.js'
import { asPlayerId } from '../types/branded.js'
import type { INodeEvent, PerformanceState, PerformanceDecision, PerformanceDelivery, PerformanceGap, PerformanceCommand, RoomSnapshot, INodeAck } from '../types/contracts.js'
import { ErrorCode } from '../types/errors.js'
import type { RoomCache } from './RoomCache.js'
import type { ReservationService } from './ReservationService.js'
import { levelNeedsSeat } from './SeatAllocator.js'
import type { Logger } from '../infra/logger/logger.js'

interface Sender {
  performance(room: RoomSnapshot, cmd: PerformanceCommand, data: PerformanceState | PerformanceDecision | PerformanceDelivery | PerformanceGap, signal?: AbortSignal): Promise<INodeAck>
}
interface JournalEntry { delivery: Omit<PerformanceDelivery, 'state'>; at: number; bytes: number }
interface Stage {
  revision: number
  preparing: PerformanceState['preparing']
  current: PerformanceState['current']
  queue: PerformanceState['queue']
  lastEnded: PerformanceState['lastEnded']
  eventSeq: number
  journal: JournalEntry[]
  journalBytes: number
  eventIds: Map<string, string>
}
interface Subscription {
  generation: number
  revision: number
  sessionId: string | null
  cursor: number
  force: boolean
  gap?: PerformanceGap | undefined
}
interface RoomDelivery {
  nodeId: NodeId
  generation: number
  levels: Map<number, Subscription>
  decisions: PerformanceDecision[]
  busy: boolean
  retryAt: number
}
export interface PerformanceOptions {
  durationMs: number
  preparationMs?: number
  queueLimit?: number
  clock?: () => number
  concurrency?: number
  leaveGraceMs?: number
  journalTtlMs?: number
  journalMaxEvents?: number
  journalMaxBytes?: number
}

/** QWD owns transitions. Network awaits never occur inside a state transition. */
export class PerformanceService {
  public readonly authorityEpoch = randomUUID()
  private readonly abort = new AbortController()
  private readonly participantRooms = new Map<PlayerId, RoomId>()
  private readonly livePlayers = new Map<PlayerId, { roomId: RoomId; nodeId: NodeId; levelId: number }>()
  private readonly stages = new Map<number, Stage>()
  private readonly deliveries = new Map<RoomId, RoomDelivery>()
  private readonly departures = new Map<PlayerId, { roomId: RoomId; levelId: number; at: number }>()
  private readonly requests = new Map<string, { fingerprint: string; decision: PerformanceDecision; at: number }>()
  private readonly clock: () => number
  private active = 0
  private scheduled = false
  private stopped = false
  private generation = 0
  public constructor (
    private readonly rooms: RoomCache,
    private readonly seating: Pick<ReservationService, 'seatFor'>,
    private readonly sender: Sender,
    private readonly options: PerformanceOptions,
    private readonly log?: Logger
  ) {
    if (!Number.isSafeInteger(options.durationMs) || options.durationMs <= 0) throw new Error('invalid performance durationMs')
    if (!Number.isSafeInteger(options.preparationMs ?? 15_000) || (options.preparationMs ?? 15_000) <= 0) throw new Error('invalid performance preparationMs')
    if (!Number.isInteger(options.queueLimit ?? 1000) || (options.queueLimit ?? 1000) < 1 || (options.queueLimit ?? 1000) > 10000) throw new Error('invalid performance queueLimit')
    this.clock = options.clock ?? Date.now
  }
  public stop (): void { this.stopped = true; this.abort.abort(); this.deliveries.clear(); this.departures.clear(); this.requests.clear(); this.participantRooms.clear(); this.livePlayers.clear() }
  private stage (levelId: number): Stage {
    let stage = this.stages.get(levelId)
    if (!stage) {
      stage = { revision: 0, preparing: null, current: null, queue: [], lastEnded: null, eventSeq: 0, journal: [], journalBytes: 0, eventIds: new Map() }
      this.stages.set(levelId, stage)
    }
    return stage
  }
  public state (levelId: number): PerformanceState {
    const s = this.stage(levelId)
    return { authorityEpoch: this.authorityEpoch, levelId, revision: s.revision, serverNowMs: this.clock(),
      preparing: s.preparing ? { ...s.preparing } : null,
      current: s.current ? { ...s.current } : null, queue: s.queue.map(q => ({ ...q })), lastEnded: s.lastEnded ? { ...s.lastEnded } : null }
  }
  public currentPerformer (levelId: number): PlayerId | undefined {
    const s = this.stages.get(levelId)
    return s?.current && this.clock() < s.current.expiresAtMs ? asPlayerId(s.current.performerId) : undefined
  }
  private ownsRoom (nodeId: NodeId, roomId: RoomId): boolean { return this.rooms.getRoom(roomId)?.nodeId === nodeId }
  private eligible (playerId: string, levelId: number, roomId?: RoomId, nodeId?: NodeId): boolean {
    const loc = this.rooms.getPlayerLocation(asPlayerId(playerId))
    const live = this.livePlayers.get(asPlayerId(playerId))
    // Seat membership only applies to seated venues (需要座位); hall venues
    // (无需座位) run the barn and audience features on presence alone.
    const seat = levelNeedsSeat(levelId) && this.seating.seatFor(asPlayerId(playerId), levelId)
    return Boolean(loc && live && live.levelId === levelId && live.roomId === loc.roomId && live.nodeId === loc.nodeId && loc.leavingAt === undefined && loc.level === levelId &&
      (!roomId || loc.roomId === roomId) && (!nodeId || loc.nodeId === nodeId) &&
      this.rooms.getRoom(loc.roomId)?.nodeId === loc.nodeId && !this.rooms.isNodeSuspected(loc.nodeId) &&
      (!levelNeedsSeat(levelId) || Boolean(seat)))
  }
  /** Read-only audience membership; independent of performance.current and queue.
   *  Hall venues (无需座位) qualify on presence alone — eligible() already
   *  requires a seat only for seated venues. */
  public isAudienceSource (playerId: PlayerId, levelId: number, roomId: RoomId, nodeId: NodeId): boolean {
    return !this.rooms.isNodeSuspected(nodeId) && this.eligible(playerId, levelId, roomId, nodeId)
  }
  public audienceRooms (levelId: number): RoomSnapshot[] {
    const targets = new Map<RoomId, RoomSnapshot>()
    for (const [playerId, live] of this.livePlayers) {
      if (live.levelId !== levelId || !this.isAudienceSource(playerId, levelId, live.roomId, live.nodeId)) continue
      const room = this.rooms.getRoom(live.roomId)
      if (room) targets.set(room.roomId, room)
    }
    return [...targets.values()]
  }
  private prepareNext (levelId: number, now: number): void {
    const s = this.stage(levelId)
    if (s.current || s.preparing) return
    while (s.queue.length) {
      const next = s.queue[0]!
      if (this.eligible(next.playerId, levelId)) {
        const durationMs = this.options.preparationMs ?? 15_000
        s.preparing = { preparationId: randomUUID(), performerId: next.playerId, startedAtMs: now, expiresAtMs: now + durationMs, durationMs }
        s.revision++
        return
      }
      s.queue.shift(); s.revision++
      this.participantRooms.delete(asPlayerId(next.playerId))
    }
  }
  private start (s: Stage, playerId: string, now: number): void {
    s.preparing = null
    s.current = { sessionId: randomUUID(), performerId: playerId, startedAtMs: now, expiresAtMs: now + this.options.durationMs, durationMs: this.options.durationMs }
    s.revision++
    s.eventSeq = 0; s.journal = []; s.journalBytes = 0; s.eventIds.clear()
  }
  private finish (levelId: number, reason: string, now: number): void {
    const s = this.stage(levelId)
    if (!s.current) return
    this.participantRooms.delete(asPlayerId(s.current.performerId))
    s.revision++
    s.lastEnded = { sessionId: s.current.sessionId, performerId: s.current.performerId, endedAtMs: reason === 'expired' ? s.current.expiresAtMs : now, reason, revision: s.revision }
    s.current = null; s.journal = []; s.journalBytes = 0; s.eventIds.clear(); s.eventSeq = 0
    this.prepareNext(levelId, now)
  }
  private expire (levelId: number, now: number): void {
    const s = this.stage(levelId)
    if (s.current && now >= s.current.expiresAtMs) this.finish(levelId, 'expired', now)
    const preparing = s.preparing
    if (preparing && now >= preparing.expiresAtMs) {
      s.preparing = null; s.revision++
      if (s.queue[0]?.playerId === preparing.performerId) {
        s.queue.shift(); s.revision++
        if (this.eligible(preparing.performerId, levelId)) {
          // A delayed tick never renews the broadcast clock. New candidates,
          // however, must always receive a fresh full preparation window.
          this.start(s, preparing.performerId, preparing.expiresAtMs)
          if (now >= s.current!.expiresAtMs) this.finish(levelId, 'expired', now)
        } else this.participantRooms.delete(asPlayerId(preparing.performerId))
      }
    }
    this.prepareNext(levelId, now)
    this.pruneJournal(s, now)
  }
  private pruneJournal (s: Stage, now: number): void {
    while (s.journal.length && (now - s.journal[0]!.at >= (this.options.journalTtlMs ?? 15_000) ||
      s.journal.length > (this.options.journalMaxEvents ?? 256) || s.journalBytes > (this.options.journalMaxBytes ?? 4 * 1024 * 1024))) {
      s.journalBytes -= s.journal.shift()!.bytes
    }
  }
  public handle (event: INodeEvent): boolean {
    const d = event.data
    if (!d.roomId || !this.ownsRoom(d.nodeId, d.roomId)) return false
    if (event.event === 'performance.request') {
      this.request(event); return true
    }
    if (event.event === 'performance.publish') return this.publish(event)
    if (event.event === 'performance.sync') {
      this.sync(d.nodeId, d.roomId, d.levelIds ?? [], d.authorityEpoch, d.eventCursors); return true
    }
    return false
  }
  private request (event: INodeEvent): void {
    const d = event.data
    const roomId = d.roomId!, playerId = d.player!.playerId, levelId = d.levelId!, requestId = d.requestId!, requested = d.requested!
    const output = this.delivery(roomId)
    // Bounded decisions: do not mutate authority if its response cannot be queued.
    if (output.decisions.length >= 128) return
    const now = this.clock()
    if (!levelNeedsSeat(levelId)) return
    this.expire(levelId, now)
    const key = requestId
    const fingerprint = JSON.stringify([playerId, roomId, levelId, requested, d.expectedSessionId, d.expectedPreparationId])
    const previous = this.requests.get(key)
    let decision: PerformanceDecision
    if (previous && previous.fingerprint === fingerprint) {
      decision = { ...previous.decision, state: this.state(levelId) }
    } else {
      const s = this.stage(levelId)
      let status: PerformanceDecision['status'] = 'rejected', reason: string | undefined
      if (previous) reason = 'request_id_conflict'
      else if (!this.eligible(playerId, levelId, roomId, d.nodeId)) reason = 'player_not_live_seated_in_room'
      else if (requested) {
        if (s.current?.performerId === playerId) status = 'started'
        else if (s.queue.some(q => q.playerId === playerId)) status = 'queued'
        else if (s.queue.length >= (this.options.queueLimit ?? 1000)) reason = 'queue_full'
        else { s.queue.push({ playerId, enqueuedAtMs: now }); s.revision++; this.prepareNext(levelId, now); status = 'queued' }
      } else {
        const queued = s.queue.findIndex(q => q.playerId === playerId)
        if (d.expectedPreparationId !== undefined && (s.preparing?.preparationId !== d.expectedPreparationId || s.preparing.performerId !== playerId)) reason = 'preparation_mismatch'
        else if (s.preparing?.performerId === playerId && d.expectedPreparationId !== s.preparing.preparationId) reason = 'preparation_mismatch'
        else if (queued >= 0) {
          if (s.preparing?.performerId === playerId) { s.preparing = null; s.revision++ }
          s.queue.splice(queued, 1); this.participantRooms.delete(playerId); s.revision++; status = 'cancelled'
          this.prepareNext(levelId, now)
        }
        else if (s.current?.performerId !== playerId) reason = 'not_owner'
        else if (!d.expectedSessionId || d.expectedSessionId !== s.current.sessionId) reason = 'session_mismatch'
        else { this.finish(levelId, 'released', now); status = 'released' }
      }
      if (status === 'started' || status === 'queued') this.participantRooms.set(playerId, roomId)
      decision = { requestId, playerId, requested, status, ...(reason ? { reason } : {}), state: this.state(levelId) }
      if (!previous) {
        this.requests.set(key, { fingerprint, decision, at: now })
        while (this.requests.size > 4096) this.requests.delete(this.requests.keys().next().value!)
      }
    }
    this.subscribe(roomId, levelId)
    output.decisions.push(decision)
    this.refreshTargets(); this.schedule()
  }
  private publish (event: INodeEvent): boolean {
    const d = event.data, levelId = d.levelId!, playerId = d.player!.playerId, now = this.clock()
    if (!levelNeedsSeat(levelId)) return false
    this.expire(levelId, now)
    const s = this.stage(levelId), current = s.current
    if (!this.eligible(playerId, levelId, d.roomId, d.nodeId) || !current || current.performerId !== playerId || current.sessionId !== d.sessionId || now >= current.expiresAtMs) return false
    const fingerprint = createHash('sha256').update(JSON.stringify([d.timestamp, d.actionId, d.payloadBase64])).digest('hex')
    const seen = s.eventIds.get(d.eventId!)
    if (seen) return seen === fingerprint
    // Dedup IDs live for the entire session, bounded independently of the short journal.
    if (s.eventIds.size >= 8192) return false
    s.eventIds.set(d.eventId!, fingerprint)
    const delivery = { eventId: d.eventId!, eventSeq: ++s.eventSeq, sessionId: current.sessionId, playerId,
      timestamp: d.timestamp!, actionId: d.actionId!, payloadBase64: d.payloadBase64! }
    const bytes = Buffer.byteLength(delivery.payloadBase64)
    s.journal.push({ delivery, at: now, bytes }); s.journalBytes += bytes
    this.pruneJournal(s, now)
    this.refreshTargets(); this.schedule(); return true
  }
  public sync (nodeId: NodeId, roomId: RoomId, levels: number[], epoch?: string, cursors: Record<string, number> = {}): void {
    if (!this.ownsRoom(nodeId, roomId)) return
    const liveLevels = this.rooms.levelsByRoom().get(roomId)
    for (const levelId of new Set(levels)) {
      if (!levelNeedsSeat(levelId) || (liveLevels?.size && !liveLevels.has(levelId))) continue
      this.expire(levelId, this.clock())
      const sub = this.subscribe(roomId, levelId), s = this.stage(levelId)
      sub.generation++
      sub.force = true
      sub.gap = undefined
      sub.sessionId = s.current?.sessionId ?? null
      sub.cursor = sub.sessionId ? cursors[sub.sessionId] ?? 0 : 0
      const oldest = s.journal[0]?.delivery.eventSeq ?? s.eventSeq + 1
      const reason = epoch !== undefined && epoch !== this.authorityEpoch ? 'epoch_changed'
        : !s.current && Object.keys(cursors).length ? 'session_ended'
        : sub.cursor > s.eventSeq ? 'cursor_ahead'
        : sub.cursor < oldest - 1 ? 'journal_gap' : undefined
      if (reason) this.setGap(levelId, sub, reason, oldest, s.eventSeq)
    }
    this.schedule()
  }
  private delivery (roomId: RoomId): RoomDelivery {
    const nodeId = this.rooms.getRoom(roomId)!.nodeId
    let output = this.deliveries.get(roomId)
    if (!output || output.nodeId !== nodeId) {
      output = { nodeId, generation: ++this.generation, levels: new Map(), decisions: [], busy: false, retryAt: 0 }
      this.deliveries.set(roomId, output)
    }
    return output
  }
  private subscribe (roomId: RoomId, levelId: number): Subscription {
    const output = this.delivery(roomId)
    let sub = output.levels.get(levelId)
    if (!sub) { sub = { generation: 0, revision: -1, sessionId: null, cursor: 0, force: true }; output.levels.set(levelId, sub) }
    return sub
  }
  public reconnectNode (nodeId: NodeId): void {
    for (const room of this.rooms.listRoomsByNode(nodeId)) {
      const old = this.deliveries.get(room.roomId)
      const levels = [...(old?.levels.keys() ?? [])]
      this.deliveries.delete(room.roomId)
      for (const level of levels) this.subscribe(room.roomId, level)
    }
    this.refreshTargets(); this.schedule()
  }
  private refreshTargets (): void {
    for (const [roomId, levels] of this.rooms.levelsByRoom()) {
      if (!this.rooms.getRoom(roomId)) continue
      for (const levelId of levels) if (levelNeedsSeat(levelId)) this.subscribe(roomId, levelId)
    }
    const levelsByRoom = this.rooms.levelsByRoom()
    for (const [roomId, output] of this.deliveries) {
      if (!this.rooms.getRoom(roomId)) { this.deliveries.delete(roomId); continue }
      const liveLevels = levelsByRoom.get(roomId)
      if (liveLevels?.size) for (const level of output.levels.keys()) {
        if (!liveLevels.has(level) && !output.decisions.some(d => d.state.levelId === level)) output.levels.delete(level)
      }
    }
  }
  public notePlayerSeen (playerId: PlayerId, roomId: RoomId, levelId: number): void {
    const room = this.rooms.getRoom(roomId)
    if (!room) return
    this.livePlayers.set(playerId, { roomId, nodeId: room.nodeId, levelId })
    this.departures.delete(playerId)
    if (this.participantRooms.has(playerId)) this.participantRooms.set(playerId, roomId)
    for (const [oldLevel, s] of this.stages) {
      if (oldLevel !== levelId && (s.current?.performerId === playerId || s.queue.some(q => q.playerId === playerId))) this.removePlayer(playerId, 'level_changed', oldLevel)
    }
    if (levelNeedsSeat(levelId) && this.rooms.getRoom(roomId)) this.subscribe(roomId, levelId)
    this.schedule()
  }
  public noteRoomRoster (roomId: RoomId, playerIds: string[], full: boolean, removed: string[] = []): void {
    const present = new Set(playerIds)
    for (const [playerId, live] of this.livePlayers) {
      if (live.roomId === roomId && ((full && !present.has(playerId)) || removed.includes(playerId))) this.notePlayerLeft(playerId, roomId, live.levelId)
    }
  }
  public notePlayerLeft (playerId: PlayerId, roomId: RoomId, levelId?: number): void {
    const loc = this.rooms.getPlayerLocation(playerId)
    if (loc && loc.roomId !== roomId) return
    if (this.livePlayers.get(playerId)?.roomId === roomId) this.livePlayers.delete(playerId)
    for (const [level, s] of this.stages) {
      if (levelId !== undefined && levelId !== level) continue
      if (s.current?.performerId === playerId || s.queue.some(q => q.playerId === playerId)) {
        if (!this.departures.has(playerId)) this.departures.set(playerId, { roomId, levelId: level, at: this.clock() })
      }
    }
  }
  public removePlayer (playerId: PlayerId, reason: string, levelId?: number): void {
    this.departures.delete(playerId)
    this.participantRooms.delete(playerId)
    for (const [level, s] of this.stages) {
      if (levelId !== undefined && levelId !== level) continue
      const index = s.queue.findIndex(q => q.playerId === playerId)
      if (index >= 0) { s.queue.splice(index, 1); s.revision++ }
      if (s.preparing?.performerId === playerId) { s.preparing = null; s.revision++ }
      if (s.current?.performerId === playerId) this.finish(level, reason, this.clock())
      else this.prepareNext(level, this.clock())
    }
    this.schedule()
  }
  public forgetRoom (roomId: RoomId): void {
    // RoomCache may already have removed locations: remember both subscriptions and grace records.
    const players = new Set([...this.participantRooms].filter(([, room]) => room === roomId).map(([player]) => player))
    for (const loc of this.rooms.listPlayers(roomId)) players.add(loc.playerId)
    for (const [playerId, pending] of this.departures) if (pending.roomId === roomId) players.add(playerId)
    for (const [level, s] of this.stages) for (const playerId of [...s.queue.map(q => q.playerId), ...(s.current ? [s.current.performerId] : [])]) {
      const loc = this.rooms.getPlayerLocation(asPlayerId(playerId))
      if (loc?.roomId === roomId || (!loc && !this.eligible(playerId, level))) players.add(asPlayerId(playerId))
    }
    // Remove the entire room's queue before promoting, so a dead peer cannot inherit the stage.
    for (const s of this.stages.values()) {
      const kept = s.queue.filter(q => !players.has(asPlayerId(q.playerId)))
      if (kept.length !== s.queue.length) { s.queue = kept; s.revision++ }
    }
    for (const p of players) this.removePlayer(p, 'room_reclaimed')
    for (const [playerId, live] of this.livePlayers) if (live.roomId === roomId) this.livePlayers.delete(playerId)
    this.deliveries.delete(roomId); this.schedule()
  }
  public tick (): void {
    const now = this.clock()
    for (const level of this.stages.keys()) this.expire(level, now)
    for (const [playerId, pending] of this.departures) if (now - pending.at >= (this.options.leaveGraceMs ?? 15_000)) this.removePlayer(playerId, 'player_left', pending.levelId)
    for (const [key, req] of this.requests) if (now - req.at >= 300_000) this.requests.delete(key)
    this.refreshTargets(); this.schedule()
  }
  private setGap (levelId: number, sub: Subscription, reason: PerformanceGap['reason'], oldest: number, latest: number): void {
    sub.gap = { authorityEpoch: this.authorityEpoch, levelId, sessionId: this.stage(levelId).current?.sessionId ?? null,
      requestedCursor: sub.cursor, oldestAvailableSeq: oldest, latestEventSeq: latest, reason }
    this.log?.warn({ roomLevel: levelId, sessionId: sub.gap.sessionId, reason, requestedCursor: sub.cursor, oldestAvailableSeq: oldest, latestEventSeq: latest }, 'performance content gap; missing actions cannot be recovered')
  }
  private hasWork (output: RoomDelivery): boolean {
    if (output.decisions.length) return true
    for (const [level, sub] of output.levels) {
      const s = this.stage(level)
      if (sub.force || sub.revision !== s.revision || sub.gap || (s.current && (sub.sessionId !== s.current.sessionId || sub.cursor < s.eventSeq))) return true
    }
    return false
  }
  private schedule (): void {
    if (this.stopped || this.scheduled) return
    this.scheduled = true
    queueMicrotask(() => { this.scheduled = false; this.pump() })
  }
  private pump (): void {
    if (this.stopped) return
    for (const [roomId, output] of [...this.deliveries]) {
      if (this.active >= (this.options.concurrency ?? 8)) break
      if (output.busy || output.retryAt > this.clock() || !this.hasWork(output)) continue
      this.deliveries.delete(roomId); this.deliveries.set(roomId, output)
      output.busy = true; this.active++
      void this.flushRoom(roomId, output).catch(error => {
        output.retryAt = this.clock() + 1000
        this.log?.debug({ roomId, reason: error instanceof Error ? error.message : 'delivery failed' }, 'performance delivery will retry')
      }).finally(() => {
        output.busy = false; this.active--; this.schedule()
      })
    }
  }
  private async send (roomId: RoomId, output: RoomDelivery, cmd: PerformanceCommand, payload: PerformanceState | PerformanceDecision | PerformanceDelivery | PerformanceGap): Promise<void> {
    const room = this.rooms.getRoom(roomId)
    if (this.stopped || this.deliveries.get(roomId) !== output || !room || room.nodeId !== output.nodeId) throw new Error('stale performance destination')
    const fresh = cmd === 'performance.state' ? { ...payload, serverNowMs: this.clock() }
      : 'state' in payload ? { ...payload, state: { ...payload.state, serverNowMs: this.clock() } } : payload
    let ack: INodeAck
    try { ack = await this.sender.performance(room, cmd, fresh, this.abort.signal) }
    catch (error) {
      this.log?.debug({ roomId, cmd, reason: error instanceof Error ? error.message : 'transport failed' }, 'performance command failed')
      throw error
    }
    if (ack.code !== ErrorCode.OK || this.deliveries.get(roomId) !== output) throw new Error(cmd + ': command not acknowledged')
    const data = ack.data as Record<string, unknown> | undefined
    if (cmd === 'performance.state') {
      const state = payload as PerformanceState
      if (data?.applied !== true || data.authorityEpoch !== state.authorityEpoch || data.revision !== state.revision) throw new Error(cmd + ': state not applied')
    }
    if (cmd === 'performance.gap') {
      const gap = payload as PerformanceGap
      if (data?.applied !== true || data.authorityEpoch !== gap.authorityEpoch || data.sessionId !== gap.sessionId ||
        typeof data.eventCursor !== 'number' || data.eventCursor < gap.oldestAvailableSeq - 1) throw new Error(cmd + ': gap not applied')
    }
    if (cmd === 'performance.deliver') {
      const delivery = payload as PerformanceDelivery
      if (data?.buffered === true || typeof data?.eventCursor !== 'number' || data.eventCursor < delivery.eventSeq ||
        (data.sessionId !== undefined && data.sessionId !== delivery.sessionId)) throw new Error(cmd + ': event not applied')
    }
  }
  private async flushRoom (roomId: RoomId, output: RoomDelivery): Promise<void> {
    // A bounded batch gives other rooms a turn. Only one worker ever owns a room.
    for (const [levelId, sub] of output.levels) {
      const generation = sub.generation
      this.expire(levelId, this.clock())
      const state = this.state(levelId), sessionId = state.current?.sessionId ?? null
      if (sub.force || sub.revision !== state.revision) {
        await this.send(roomId, output, 'performance.state', state)
        if (sub.generation !== generation) continue
        sub.revision = state.revision; sub.force = false
      }
      const decisionIndex = output.decisions.findIndex(d => d.state.levelId === levelId)
      const decision = output.decisions[decisionIndex]
      if (decision) {
        await this.send(roomId, output, 'performance.decision', { ...decision, state: this.state(levelId) })
        output.decisions.splice(decisionIndex, 1)
        if (sub.generation !== generation) continue
      }
      if (sub.sessionId !== sessionId) { sub.sessionId = sessionId; sub.cursor = 0; sub.gap = undefined }
      let s = this.stage(levelId)
      if (s.revision !== state.revision) continue
      const oldest = s.journal[0]?.delivery.eventSeq ?? s.eventSeq + 1
      if (!sub.gap && sessionId && sub.cursor < oldest - 1) this.setGap(levelId, sub, 'journal_gap', oldest, s.eventSeq)
      if (sub.gap) {
        const gap = sub.gap
        await this.send(roomId, output, 'performance.gap', gap)
        if (sub.generation !== generation) continue
        sub.cursor = gap.oldestAvailableSeq - 1; sub.gap = undefined
      }
      for (let count = 0; count < 32; count++) {
        this.expire(levelId, this.clock()); s = this.stage(levelId)
        if (!sessionId || s.current?.sessionId !== sessionId || s.revision !== state.revision) break
        const entry = s.journal.find(e => e.delivery.eventSeq > sub.cursor)
        if (!entry) break
        if (entry.delivery.eventSeq !== sub.cursor + 1) break
        await this.send(roomId, output, 'performance.deliver', { ...entry.delivery, state: this.state(levelId) })
        if (sub.generation !== generation) break
        sub.cursor = entry.delivery.eventSeq
      }
    }
  }
}
