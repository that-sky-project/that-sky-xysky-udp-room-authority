import type { NodeId, RoomId } from '../types/branded.js'
import type { Logger } from '../infra/logger/logger.js'
import type { NodeRegistry } from './NodeRegistry.js'
import type { PendingRequestMap } from './PendingRequestMap.js'
import type { RoomCache } from './RoomCache.js'
import type { TelemetryStore } from './TelemetryStore.js'

interface Entry {
  roomId: RoomId; moveId: string; playerIds: Set<string>; attempts: number; nextAt: number
  expiresAt: number; nodeId: NodeId; connectedAt: number; reservationToken?: string
}
interface Input { roomId: RoomId; moveId: string; playerIds: string[]; expiresAt: number; reservationToken?: string }
interface Options {
  rooms: RoomCache; nodes: NodeRegistry; pending: PendingRequestMap; telemetry: TelemetryStore
  onReleased: (moveId: string, playerId: string) => boolean
  logger?: Logger | undefined
  clock?: () => number
  retryBaseMs?: number | undefined
  retryMaxMs?: number | undefined
  maxAttempts?: number | undefined
}

/** Retains ambiguous remote capacity until positive cleanup or authoritative expiry. */
export class MoveCancellationQueue {
  private readonly entries = new Map<string, Entry>()
  private readonly inflight = new Map<string, Promise<void>>()
  private readonly abort = new AbortController()
  private readonly clock: () => number
  private closed = false
  private closing: Promise<void> | undefined

  public constructor(private readonly options: Options) { this.clock = options.clock ?? Date.now }
  public has(moveId: string): boolean { return this.entries.has(moveId) }
  public busy(roomId: string, now = this.clock()): boolean { return [...this.entries.values()].some(entry => entry.roomId === roomId && entry.expiresAt > now) }
  public snapshot(): Array<{ moveId: string; roomId: RoomId; playerIds: string[]; attempts: number; expiresAt: number }> {
    return [...this.entries.values()].map(entry => ({ moveId: entry.moveId, roomId: entry.roomId, playerIds: [...entry.playerIds], attempts: entry.attempts, expiresAt: entry.expiresAt }))
  }
  public forget(moveId: string): void { this.entries.delete(moveId) }
  public enqueue(input: Input): void {
    if (this.closed || !input.playerIds.length) return
    const old = this.entries.get(input.moveId)
    if (old) {
      if (old.roomId !== input.roomId || old.reservationToken !== input.reservationToken) return
      for (const id of input.playerIds) old.playerIds.add(id)
    } else {
      const room = this.options.rooms.getRoom(input.roomId)
      const transport = room ? this.options.nodes.getTransport(room.nodeId) : undefined
      if (!room || !transport) return
      this.entries.set(input.moveId, { ...input, playerIds: new Set(input.playerIds), nodeId: room.nodeId,
        connectedAt: transport.connectedAt, attempts: 0, nextAt: 0 })
    }
    this.send(input.moveId)
  }
  public flush(): void { for (const id of this.entries.keys()) this.send(id) }
  private send(moveId: string): void {
    const entry = this.entries.get(moveId)
    if (this.closed || !entry || this.inflight.has(moveId)) return
    if (this.clock() >= entry.expiresAt) {
      this.entries.delete(moveId)
      this.options.telemetry.releaseReservation(entry.roomId, moveId)
      return
    }
    if (this.clock() < entry.nextAt || entry.attempts >= (this.options.maxAttempts ?? 8)) return
    const transport = this.options.nodes.getTransport(entry.nodeId)
    if (!transport?.writable || transport.connectedAt !== entry.connectedAt) return
    const sent = [...entry.playerIds]
    entry.attempts++
    const work = this.options.pending.send({ cmd: 'move.cancel', data: { roomId: entry.roomId, moveId,
      playerIds: sent, ...(entry.reservationToken ? { reservationToken: entry.reservationToken } : {}) } }, command => transport.send(command), this.abort.signal, entry.nodeId)
      .then(ack => {
        if (ack.code !== 0) throw new Error(`move.cancel rejected (${ack.code})`)
        if (this.entries.get(moveId) !== entry) return
        for (const id of sent) {
          entry.playerIds.delete(id)
          if (this.options.onReleased(moveId, id)) this.options.telemetry.releaseReservationSlots(entry.roomId, moveId, 1)
        }
        if (!entry.playerIds.size) this.entries.delete(moveId)
      }).catch(error => {
        if (!this.closed) this.options.logger?.debug({ err: error, moveId, attempt: entry.attempts }, 'move cancellation not confirmed')
      }).finally(() => {
        entry.nextAt = this.clock() + Math.min(this.options.retryMaxMs ?? 30_000, (this.options.retryBaseMs ?? 2_000) * 2 ** Math.max(0, entry.attempts - 1))
        this.inflight.delete(moveId)
      })
    this.inflight.set(moveId, work)
  }
  public stop(): Promise<void> {
    if (this.closing) return this.closing
    this.closed = true
    this.abort.abort(new Error('move cancellation queue stopped'))
    this.entries.clear()
    this.closing = Promise.allSettled([...this.inflight.values()]).then(() => {})
    return this.closing
  }
}
