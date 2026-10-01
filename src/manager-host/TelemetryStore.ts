import type { PlayerId, RoomId } from "../types/branded.js";
import type { PlayerTelemetry } from "../types/contracts.js";

export interface StoredTelemetry extends PlayerTelemetry {
  roomId: RoomId;
  receivedAt: number;
  stableDissatisfaction: number;
  migrationPressure: number;
  emaVelocity?: number | undefined;
  lastMigratedAt?: number | undefined;
}

export interface TelemetryStoreOptions {
  stateEmaHalfLifeMs: number;
}

export class TelemetryStore {
  private readonly players = new Map<PlayerId, StoredTelemetry>();
  private readonly reservations = new Map<RoomId, Map<string, { expiresAt: number; slots: number }>>();

  public constructor(private readonly options: TelemetryStoreOptions = { stateEmaHalfLifeMs: 3_000 }) {}

  public upsert(roomId: RoomId, input: PlayerTelemetry, now = Date.now()): StoredTelemetry {
    const previous = this.players.get(input.playerId);
    const rawVelocity = input.velocity ?? previous?.velocity ?? 0;
    const next: StoredTelemetry = {
      ...previous,
      ...input,
      roomId,
      receivedAt: now,
      stableDissatisfaction: previous?.stableDissatisfaction ?? 0.5,
      migrationPressure: previous?.migrationPressure ?? 0,
      emaVelocity: this.smooth(previous?.emaVelocity, rawVelocity, previous?.receivedAt, now)
    };
    this.players.set(input.playerId, next);
    return next;
  }

  private smooth(prev: number | undefined, value: number, prevAt: number | undefined, now: number): number {
    if (prev === undefined || prevAt === undefined) return value;
    const dt = Math.max(0, now - prevAt);
    const alpha = 1 - Math.pow(0.5, dt / Math.max(1, this.options.stateEmaHalfLifeMs));
    return prev + alpha * (value - prev);
  }

  public remove(playerId: PlayerId): void { this.players.delete(playerId); }
  public get(playerId: PlayerId): StoredTelemetry | undefined { return this.players.get(playerId); }
  public list(): StoredTelemetry[] { return [...this.players.values()]; }
  public listRoom(roomId: RoomId, now = Date.now()): StoredTelemetry[] {
    // Keep stale samples long enough for the planner to identify inactive
    // players. Freshness is scored by MovePlanner; dropping stale samples here
    // would make dead rooms look empty and prevent cleanup migrations.
    const retentionMs = 5 * 60_000;
    return this.list().filter((player) => player.roomId === roomId && now - player.receivedAt <= retentionMs);
  }

  public markMigrated(playerIds: PlayerId[], now = Date.now()): void {
    for (const playerId of playerIds) {
      const player = this.players.get(playerId);
      if (player) this.players.set(playerId, { ...player, lastMigratedAt: now, migrationPressure: 0 });
    }
  }

  public reserved(roomId: RoomId, now = Date.now()): number {
    const holds = this.reservations.get(roomId);
    if (!holds) return 0;
    let total = 0;
    for (const [moveId, hold] of holds) { if (hold.expiresAt <= now) holds.delete(moveId); else total += hold.slots; }
    return total;
  }

  public tryReserve(roomId: RoomId, moveId: string, slots: number, capacity: number, occupied: number, ttlMs: number, now = Date.now()): boolean {
    const holds = this.reservations.get(roomId) ?? new Map<string, { expiresAt: number; slots: number }>();
    this.reservations.set(roomId, holds);
    this.reserved(roomId, now);
    if (occupied + this.reserved(roomId, now) + slots > capacity) return false;
    holds.set(moveId, { expiresAt: now + ttlMs, slots });
    return true;
  }

  public releaseReservation(roomId: RoomId, moveId: string): void {
    const holds = this.reservations.get(roomId);
    holds?.delete(moveId);
    if (holds?.size === 0) this.reservations.delete(roomId);
  }

  public releaseReservationSlots(roomId: RoomId, moveId: string, slots: number): void {
    const holds = this.reservations.get(roomId);
    if (!holds) return;
    const hold = holds?.get(moveId);
    if (!hold) return;
    if (slots >= hold.slots) holds.delete(moveId);
    else holds.set(moveId, { ...hold, slots: hold.slots - Math.max(0, slots) });
    if (holds.size === 0) this.reservations.delete(roomId);
  }
}
