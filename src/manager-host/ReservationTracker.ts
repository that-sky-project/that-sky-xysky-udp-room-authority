import type { PlayerId, RoomId } from '../types/branded.js'
import type { RoomSnapshot } from '../types/contracts.js'

interface ReservationHold {
  roomId: RoomId
  playerId: PlayerId
  expiresAt: number
  acknowledged: boolean
  timeout: NodeJS.Timeout
}

export interface ReservationTrackerOptions {
  ttlMs: number
}

export class ReservationTracker {
  private readonly holdsByPlayer = new Map<PlayerId, ReservationHold>()
  private readonly playersByRoom = new Map<RoomId, Set<PlayerId>>()
  private stopped = false

  public constructor (private readonly options: ReservationTrackerOptions) {}

  public effectivePlayers (room: RoomSnapshot): number {
    return room.players + this.count(room.roomId)
  }

  public availableSlots (room: RoomSnapshot): number {
    return Math.max(0, room.capacity - this.effectivePlayers(room))
  }

  /** A tentative hold prevents concurrent overbooking; it is not an assignment. */
  public tryAcquire (room: RoomSnapshot, playerId: PlayerId): boolean {
    if (this.stopped || this.holdFor(playerId)) return false
    if (this.availableSlots(room) <= 0) return false
    const hold: ReservationHold = {
      roomId: room.roomId,
      playerId,
      expiresAt: Date.now() + this.options.ttlMs,
      acknowledged: false,
      timeout: undefined as unknown as NodeJS.Timeout
    }
    this.holdsByPlayer.set(playerId, hold)
    let roomPlayers = this.playersByRoom.get(room.roomId)
    if (!roomPlayers) {
      roomPlayers = new Set<PlayerId>()
      this.playersByRoom.set(room.roomId, roomPlayers)
    }
    roomPlayers.add(playerId)
    this.arm(hold)
    return true
  }

  /** Use the node's actual lease, never refresh one when an HTTP caller retries. */
  public acknowledge (playerId: PlayerId, roomId: RoomId, expiresAt: number): boolean {
    const hold = this.holdFor(playerId)
    if (this.stopped || !hold || hold.roomId !== roomId ||
        !Number.isSafeInteger(expiresAt) || expiresAt <= Date.now()) return false
    clearTimeout(hold.timeout)
    hold.expiresAt = expiresAt
    hold.acknowledged = true
    this.arm(hold)
    return true
  }

  public confirmedHoldFor (playerId: PlayerId): Readonly<ReservationHold> | undefined {
    const hold = this.holdFor(playerId)
    return hold?.acknowledged ? hold : undefined
  }

  public confirm (playerId: PlayerId): void {
    this.release(playerId)
  }

  public roomFor (playerId: PlayerId): RoomId | undefined {
    return this.holdFor(playerId)?.roomId
  }

  private holdFor (playerId: PlayerId): ReservationHold | undefined {
    const hold = this.holdsByPlayer.get(playerId)
    if (hold && hold.expiresAt <= Date.now()) {
      this.release(playerId)
      return undefined
    }
    return hold
  }

  private arm (hold: ReservationHold): void {
    hold.timeout = setTimeout(() => {
      if (this.holdsByPlayer.get(hold.playerId) === hold) this.release(hold.playerId)
    }, Math.max(1, hold.expiresAt - Date.now()))
    hold.timeout.unref?.()
  }

  public release (playerId: PlayerId): void {
    const hold = this.holdsByPlayer.get(playerId)
    if (!hold) return
    clearTimeout(hold.timeout)
    this.holdsByPlayer.delete(playerId)
    const roomPlayers = this.playersByRoom.get(hold.roomId)
    roomPlayers?.delete(playerId)
    if (roomPlayers?.size === 0) this.playersByRoom.delete(hold.roomId)
  }

  public count (roomId: RoomId): number {
    for (const playerId of this.playersByRoom.get(roomId) ?? []) this.holdFor(playerId)
    return this.playersByRoom.get(roomId)?.size ?? 0
  }

  public stop (): void {
    if (this.stopped) return
    this.stopped = true
    for (const hold of this.holdsByPlayer.values()) clearTimeout(hold.timeout)
    this.holdsByPlayer.clear()
    this.playersByRoom.clear()
  }

  public size (): number {
    for (const playerId of this.holdsByPlayer.keys()) this.holdFor(playerId)
    return this.holdsByPlayer.size
  }
}
