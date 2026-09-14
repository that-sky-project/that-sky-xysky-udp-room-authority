import type { NodeHealth } from "./NodeRegistry.js";
import type { PlayerId, Region, RoomId } from "../types/branded.js";
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

export interface MergeCandidate {
  sourceRoomId: RoomId;
  targetRoomId: RoomId;
  movingPlayers: number;
  score: number;
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

  public planMerge(rooms: RoomSnapshot[], now: number, cooldownMs: number): MergeCandidate[] {
    const underfilled = rooms
      .filter((room) => room.players > 0 && room.players <= 3 && !room.draining && now - (room.lastMergeAt ?? 0) >= cooldownMs)
      .sort((a, b) => a.players - b.players);
    const targets = rooms
      .filter((room) => room.players >= 4 && room.players < room.capacity && !room.draining && now - (room.lastMergeAt ?? 0) >= cooldownMs)
      .sort((a, b) => b.players - a.players);

    const candidates: MergeCandidate[] = [];
    for (const source of underfilled) {
      let best: MergeCandidate | undefined;
      for (const target of targets) {
        if (source.roomId === target.roomId || source.nodeId === target.nodeId) continue;
        const freeSlots = target.capacity - target.players;
        if (freeSlots < source.players) continue;
        const score = 100 - freeSlots + source.players;
        if (!best || score > best.score) {
          best = {
            sourceRoomId: source.roomId,
            targetRoomId: target.roomId,
            movingPlayers: source.players,
            score
          };
        }
      }
      if (best) candidates.push(best);
    }
    return candidates.sort((a, b) => b.score - a.score);
  }
}
