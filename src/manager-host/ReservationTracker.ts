import type { PlayerId, RoomId } from "../types/branded.js";
import type { RoomSnapshot } from "../types/contracts.js";

interface ReservationHold {
  roomId: RoomId;
  playerId: PlayerId;
  expiresAt: number;
  timeout: NodeJS.Timeout;
}

export interface ReservationTrackerOptions {
  ttlMs: number;
}

export class ReservationTracker {
  private readonly holdsByPlayer = new Map<PlayerId, ReservationHold>();
  private readonly playersByRoom = new Map<RoomId, Set<PlayerId>>();

  public constructor(private readonly options: ReservationTrackerOptions) {}

  public effectivePlayers(room: RoomSnapshot): number {
    return room.players + this.count(room.roomId);
  }

  public availableSlots(room: RoomSnapshot): number {
    return Math.max(0, room.capacity - this.effectivePlayers(room));
  }

  public tryAcquire(room: RoomSnapshot, playerId: PlayerId): boolean {
    this.release(playerId);
    if (this.availableSlots(room) <= 0) return false;

    const timeout = setTimeout(() => this.release(playerId), this.options.ttlMs);
    timeout.unref?.();

    const hold: ReservationHold = {
      roomId: room.roomId,
      playerId,
      expiresAt: Date.now() + this.options.ttlMs,
      timeout
    };
    this.holdsByPlayer.set(playerId, hold);

    let roomPlayers = this.playersByRoom.get(room.roomId);
    if (!roomPlayers) {
      roomPlayers = new Set<PlayerId>();
      this.playersByRoom.set(room.roomId, roomPlayers);
    }
    roomPlayers.add(playerId);
    return true;
  }

  public confirm(playerId: PlayerId): void {
    this.release(playerId);
  }

  public release(playerId: PlayerId): void {
    const hold = this.holdsByPlayer.get(playerId);
    if (!hold) return;
    clearTimeout(hold.timeout);
    this.holdsByPlayer.delete(playerId);
    const roomPlayers = this.playersByRoom.get(hold.roomId);
    roomPlayers?.delete(playerId);
    if (roomPlayers?.size === 0) this.playersByRoom.delete(hold.roomId);
  }

  public count(roomId: RoomId): number {
    return this.playersByRoom.get(roomId)?.size ?? 0;
  }

  public size(): number {
    return this.holdsByPlayer.size;
  }
}
