import type { PlayerId, RoomId } from '../types/branded.js'
import type { PlayerTelemetry } from '../types/contracts.js'
export interface StoredTelemetry extends PlayerTelemetry {
  roomId: RoomId
  receivedAt: number
  emaVelocity?: number | undefined
  lastMigratedAt?: number | undefined
  levelChangedAt?: number | undefined
  roomJoinedAt?: number | undefined
}
export interface TelemetryStoreOptions { stateEmaHalfLifeMs: number }
interface ReservationHold { expiresAt: number; slots: number }
export class TelemetryStore {
  private readonly players = new Map<PlayerId, StoredTelemetry>()
  private readonly playersByRoom = new Map<RoomId, Set<PlayerId>>()
  private readonly reservations = new Map<RoomId, Map<string, ReservationHold>>()
  public constructor (private readonly options: TelemetryStoreOptions = { stateEmaHalfLifeMs: 3_000 }) {}
  public upsert (roomId: RoomId, input: PlayerTelemetry, now = Date.now()): StoredTelemetry {
    const previous = this.players.get(input.playerId)
    // A delayed packet from the same session cannot regress its level sequence.
    if (previous?.roomId === roomId && previous.sessionId === input.sessionId &&
        previous.lvSeq !== undefined && input.lvSeq !== undefined && ((input.lvSeq - previous.lvSeq + 256) & 255) >= 128) return previous
    const roomChanged = previous !== undefined && previous.roomId !== roomId
    const sessionChanged = previous !== undefined && input.sessionId !== undefined && previous.sessionId !== input.sessionId
    const base = sessionChanged ? undefined : previous
    const rawVelocity = input.velocity ?? base?.velocity ?? 0
    const hasLevel = input.levelId !== undefined
    const incomingLevel = input.levelId === 0 ? undefined : input.levelId
    const levelChanged = hasLevel && base?.levelId !== undefined && incomingLevel !== undefined && base.levelId !== incomingLevel
    const next: StoredTelemetry = {
      ...base, ...input, ...(hasLevel ? { levelId: incomingLevel } : {}), roomId, receivedAt: now,
      emaVelocity: this.smooth(base?.emaVelocity, rawVelocity, base?.receivedAt, now),
      lastMigratedAt: previous?.lastMigratedAt,
      levelChangedAt: levelChanged ? now : base?.levelChangedAt,
      roomJoinedAt: roomChanged || sessionChanged || previous === undefined ? now : previous.roomJoinedAt ?? now
    }
    if (roomChanged) this.playersByRoom.get(previous.roomId)?.delete(input.playerId)
    const index = this.playersByRoom.get(roomId) ?? new Set<PlayerId>()
    index.add(input.playerId)
    this.playersByRoom.set(roomId, index)
    this.players.set(input.playerId, next)
    return next
  }
  private smooth (prev: number | undefined, value: number, prevAt: number | undefined, now: number): number {
    if (prev === undefined || prevAt === undefined) return value
    const alpha = 1 - Math.pow(0.5, Math.max(0, now - prevAt) / Math.max(1, this.options.stateEmaHalfLifeMs))
    return prev + alpha * (value - prev)
  }
  public remove (playerId: PlayerId, expectedRoomId?: RoomId, expectedSessionId?: string): boolean {
    const current = this.players.get(playerId)
    if (!current || (expectedRoomId !== undefined && current.roomId !== expectedRoomId) ||
        (expectedSessionId !== undefined && current.sessionId !== undefined && current.sessionId !== expectedSessionId)) return false
    this.players.delete(playerId)
    const index = this.playersByRoom.get(current.roomId)
    index?.delete(playerId)
    if (index?.size === 0) this.playersByRoom.delete(current.roomId)
    return true
  }
  public removeRoom (roomId: RoomId): void {
    for (const id of [...this.playersByRoom.get(roomId) ?? []]) this.remove(id, roomId)
    this.reservations.delete(roomId)
  }
  public get (playerId: PlayerId): StoredTelemetry | undefined { return this.players.get(playerId) }
  public list (): StoredTelemetry[] { return [...this.players.values()] }
  public listRoom (roomId: RoomId, now = Date.now()): StoredTelemetry[] {
    const result: StoredTelemetry[] = []
    for (const id of this.playersByRoom.get(roomId) ?? []) {
      const player = this.players.get(id)
      if (player && now - player.receivedAt <= 5 * 60_000 && player.status !== 'MOVING') result.push(player)
    }
    return result
  }
  public markMigrated (playerIds: PlayerId[], now = Date.now()): void {
    for (const id of playerIds) { const player = this.players.get(id); if (player) this.players.set(id, { ...player, lastMigratedAt: now }) }
  }
  public reserved (roomId: RoomId, now = Date.now()): number {
    const holds = this.reservations.get(roomId)
    if (!holds) return 0
    let total = 0
    for (const [moveId, hold] of holds) { if (hold.expiresAt <= now) holds.delete(moveId); else total += hold.slots }
    if (holds.size === 0) this.reservations.delete(roomId)
    return total
  }
  public reservation (roomId: RoomId, moveId: string): Readonly<ReservationHold> | undefined { return this.reservations.get(roomId)?.get(moveId) }
  public tryReserve (roomId: RoomId, moveId: string, slots: number, capacity: number, occupied: number, ttlMs: number, now = Date.now()): boolean {
    const reserved = this.reserved(roomId, now)
    const holds = this.reservations.get(roomId) ?? new Map<string, ReservationHold>()
    const own = holds.get(moveId)?.slots ?? 0
    if (slots < 0 || occupied + reserved - own + slots > Math.min(8, capacity)) return false
    holds.set(moveId, { expiresAt: now + ttlMs, slots })
    this.reservations.set(roomId, holds)
    return true
  }
  public setReservationExpiry (roomId: RoomId, moveId: string, expiresAt: number): void {
    const hold = this.reservations.get(roomId)?.get(moveId)
    if (hold && Number.isSafeInteger(expiresAt)) hold.expiresAt = expiresAt
  }
  public releaseReservation (roomId: RoomId, moveId: string): void {
    const holds = this.reservations.get(roomId); holds?.delete(moveId)
    if (holds?.size === 0) this.reservations.delete(roomId)
  }
  public releaseReservationSlots (roomId: RoomId, moveId: string, slots: number): void {
    const holds = this.reservations.get(roomId); const hold = holds?.get(moveId)
    if (!holds || !hold) return
    hold.slots = Math.max(0, hold.slots - Math.max(0, slots))
    if (!hold.slots) holds.delete(moveId)
    if (holds.size === 0) this.reservations.delete(roomId)
  }
}
