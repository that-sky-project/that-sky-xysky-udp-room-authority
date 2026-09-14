import type { PlayerId, RoomId } from "../types/branded.js";
import type { PlayerTelemetry } from "../types/contracts.js";

export interface StoredTelemetry extends PlayerTelemetry {
  roomId: RoomId;
  receivedAt: number;
  stableDissatisfaction: number;
  migrationPressure: number;
  lastMigratedAt?: number | undefined;
}

export class TelemetryStore {
  private readonly players = new Map<PlayerId, StoredTelemetry>();
  private readonly reservations = new Map<RoomId, Map<string, { expiresAt: number; slots: number }>>();

  public upsert(roomId: RoomId, input: PlayerTelemetry, now = Date.now()): StoredTelemetry {
    const previous = this.players.get(input.playerId);
    const next: StoredTelemetry = {
      ...previous,
      ...input,
      roomId,
      receivedAt: now,
      stableDissatisfaction: previous?.stableDissatisfaction ?? 0.5,
      migrationPressure: previous?.migrationPressure ?? 0
    };
    this.players.set(input.playerId, next);
    return next;
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

  public updatePressure(playerId: PlayerId, dissatisfaction: number, dtMs: number): StoredTelemetry | undefined {
    const player = this.players.get(playerId);
    if (!player) return undefined;
    const alpha = 1 - Math.exp(-Math.max(0, dtMs) / 20_000);
    const stableDissatisfaction = player.stableDissatisfaction + alpha * (dissatisfaction - player.stableDissatisfaction);
    const threshold = 0.35;
    const pressure = Math.max(0, Math.min(1, player.migrationPressure + (dtMs / 1000) * (0.04 * Math.max(0, stableDissatisfaction - threshold) - 0.015 * player.migrationPressure)));
    const next = { ...player, stableDissatisfaction, migrationPressure: pressure };
    this.players.set(playerId, next);
    return next;
  }
}
