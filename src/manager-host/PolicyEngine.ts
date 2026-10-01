import type { NodeHealth } from "./NodeRegistry.js";
import type { PlayerId, Region } from "../types/branded.js";
import type { RoomSnapshot } from "../types/contracts.js";

export interface RoomRequestContext {
  playerId: PlayerId;
  region?: Region | undefined;
  level?: number | undefined;
  preferences?: Record<string, unknown> | undefined;
}

export interface RoomSelection {
  room: RoomSnapshot;
  score: number;
  reason: string;
}

export interface PolicyEngineOptions {
  maxPlayersPerRoom: number;
  staleNodePenalty: number;
  loadPenaltyWeight: number;
  sameRegionBonus: number;
  preferredFillTarget: number;
}

export class PolicyEngine {
  public constructor(private readonly options: PolicyEngineOptions) {}

  public selectRoom(
    context: RoomRequestContext,
    rooms: RoomSnapshot[],
    nodeHealth: Map<string, NodeHealth>,
    effectivePlayersOf: (room: RoomSnapshot) => number = (room) => room.players
  ): RoomSelection | undefined {
    let best: RoomSelection | undefined;
    for (const room of rooms) {
      const effectivePlayers = effectivePlayersOf(room);
      if (room.draining || effectivePlayers >= Math.min(room.capacity, this.options.maxPlayersPerRoom)) continue;

      const health = nodeHealth.get(room.nodeId);
      if (!health || !health.connected || health.draining) continue;

      const freeSlots = room.capacity - effectivePlayers;
      const fillAfterJoin = effectivePlayers + 1;
      const fillDistance = Math.abs(this.options.preferredFillTarget - fillAfterJoin);
      const regionScore = context.region && room.region === context.region ? this.options.sameRegionBonus : 0;
      const loadPenalty = health.load * this.options.loadPenaltyWeight;
      const score = 100 - fillDistance * 8 - freeSlots * 0.5 + regionScore - loadPenalty;

      if (!best || score > best.score) {
        best = { room, score, reason: "highest policy score" };
      }
    }
    return best;
  }
}
