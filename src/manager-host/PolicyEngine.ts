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
  /** Score bonus for a room that already runs the requesting level. */
  sameLevelBonus: number;
  /** Score penalty for a room running a different level; large enough to sink
   *  it below any same-level or empty room while still allowing it as a last
   *  resort when nothing else has capacity. */
  wrongLevelPenalty: number;
  /** Score penalty for placing a player whose level is unknown into a room that
   *  already holds a known level. Unknown plus occupied is exactly how mixed
   *  rooms are created, which isolation then has to undo. */
  unknownLevelPenalty: number;
  /** Level ids (maps) whose rooms are quarantined. Allocation never places a
   *  player of another level into such a room, and a player on a protected
   *  level only joins an empty room or a room already running that level. */
  protectedLevels: number[];
}

export class PolicyEngine {
  private readonly protectedLevels: Set<number>;

  public constructor(private readonly options: PolicyEngineOptions) {
    this.protectedLevels = new Set(options.protectedLevels.filter((id) => Number.isInteger(id) && id > 0));
  }

  public selectRoom(
    context: RoomRequestContext,
    rooms: RoomSnapshot[],
    nodeHealth: Map<string, NodeHealth>,
    effectivePlayersOf: (room: RoomSnapshot) => number = (room) => room.players,
    levelsOf: (room: RoomSnapshot) => ReadonlySet<number> = () => new Set<number>()
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
      const roomLevels = levelsOf(room);
      // Protected rooms are a hard constraint, not a score: placing anyone
      // there would make the isolation pass move them straight back out.
      if (!this.levelAllowed(context.level, roomLevels)) continue;
      const levelScore = this.levelScore(context.level, roomLevels);
      const score = 100 - fillDistance * 8 - freeSlots * 0.5 + regionScore - loadPenalty + levelScore;

      if (!best || score > best.score) {
        best = { room, score, reason: "highest policy score" };
      }
    }
    return best;
  }

  /**
   * Hard filter for protected (quarantined) levels.
   *
   * - A player on a protected level may only join an empty room or a room that
   *   already runs exactly that level. Joining a different-level room would
   *   force the isolation pass to evict the other occupants.
   * - Any other player (including one whose level is unknown) is kept out of a
   *   protected room entirely, because the eviction pass would move them out
   *   again right after the reservation.
   */
  private levelAllowed(level: number | undefined, roomLevels: ReadonlySet<number>): boolean {
    if (this.protectedLevels.size === 0) return true;
    if (level !== undefined && this.protectedLevels.has(level)) {
      if (roomLevels.size === 0) return true;
      return roomLevels.size === 1 && roomLevels.has(level);
    }
    for (const roomLevel of roomLevels) {
      if (this.protectedLevels.has(roomLevel)) return false;
    }
    return true;
  }

  /**
   * One level per room: a room that already runs the requested level is the
   * best target, an empty room is neutral, and a room running another level is
   * heavily penalised. The penalty is a preference rather than a hard filter so
   * a cluster with no matching room still allocates instead of rejecting joins.
   */
  private levelScore(level: number | undefined, roomLevels: ReadonlySet<number>): number {
    if (level === undefined) {
      // We cannot verify compatibility, so prefer a level-less (usually empty)
      // room. Dropping an unknown player into a room with a known level is the
      // main way mixed rooms get created in the first place.
      return roomLevels.size > 0 ? -this.options.unknownLevelPenalty : 0;
    }
    if (roomLevels.size === 0) return 0;
    return roomLevels.has(level) ? this.options.sameLevelBonus : -this.options.wrongLevelPenalty;
  }
}
