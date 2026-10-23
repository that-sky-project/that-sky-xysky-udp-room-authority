import type { RoomSnapshot } from "../types/contracts.js";
import type { Logger } from "../infra/logger/logger.js";
import type { StoredTelemetry, TelemetryStore } from "./TelemetryStore.js";

export interface LevelIsolationPlan {
  playerIds: string[];
  sourceRoomId: string;
  targetRoomId: string;
  levelId: number;
  sourceLevelCount: number;
  targetLevelCount: number;
  reason: "level_isolation" | "level_consolidation";
}

export interface LevelIsolationOptions {
  /** A level group smaller than this is left alone instead of being moved out. */
  minLevelPlayers: number;
  /** Max players moved by a single plan. */
  maxPlayersPerMove: number;
  /** Rooms with this many or fewer players are considered sparse and eligible
   *  for same-level consolidation. */
  sparseRoomThreshold: number;
  /** After any player in a room changes level, wait this long before migrating
   *  anyone from that room. Prevents acting on the brief "false mixed" state
   *  that exists while two hand-holding players switch maps at slightly
   *  different times. */
  levelChangeCooldownMs: number;
  /** A player who just joined a room (friend-join teleport, cross-room move)
   *  is still loading the destination map and may briefly report the previous
   *  level. Leave the room alone for this long before isolating anyone. */
  joinSettleMs: number;
}

interface RoomLevelGroup {
  levelId: number;
  players: StoredTelemetry[];
  newestAt: number;
}

const DEFAULT_OPTIONS: LevelIsolationOptions = {
  minLevelPlayers: 1,
  maxPlayersPerMove: 8,
  sparseRoomThreshold: 2,
  levelChangeCooldownMs: 6_000,
  joinSettleMs: 4_000
};

/**
 * Enforces one level per room: whenever a room ends up holding more than one
 * level, the minority level is moved out. Target priority:
 *   1. Clean same-level room (spatial proximity ranking)
 *   2. Empty room
 *   3. Mixed room that already has this level (last resort — allows mixing
 *      only when no clean room exists anywhere in the cluster)
 * A second pass merges sparse same-level rooms so isolated players can meet.
 * Room cooldown is owned by MoveCoordinator, which applies it uniformly.
 */
export class LevelIsolationPlanner {
  private readonly options: LevelIsolationOptions;

  public constructor(options: Partial<LevelIsolationOptions> = {}) {
    this.options = { ...DEFAULT_OPTIONS, ...options };
  }

  public plan(
    rooms: RoomSnapshot[],
    telemetry: TelemetryStore,
    now = Date.now(),
    log?: Logger
  ): LevelIsolationPlan[] {
    const plans: LevelIsolationPlan[] = [];
    const claimedPlayers = new Set<string>();
    const usedTargets = new Set<string>();
    // maxPlayersPerMove caps a single plan; the cycle budget is "one plan's
    // worth of players per room in the cluster".
    let budget = this.options.maxPlayersPerMove * Math.max(1, rooms.length);

    // Snapshot occupancy per room once; planned moves update it locally so two
    // plans in the same cycle cannot both claim the last empty room.
    const occupancy = new Map<string, number>();
    const byRoom = new Map<string, StoredTelemetry[]>();
    for (const room of rooms) {
      const players = telemetry.listRoom(room.roomId, now);
      byRoom.set(room.roomId, players);
      occupancy.set(room.roomId, players.length);
    }
    const levelsByRoom = new Map<string, Set<number>>();
    for (const [roomId, players] of byRoom) {
      // Only settled levels count: a player who just joined or just switched
      // maps is still loading and must not turn the room into a "mixed" one.
      levelsByRoom.set(roomId, new Set(
        players.filter((p) => this.isSettled(p, now)).map((p) => p.levelId).filter((l): l is number => l !== undefined)
      ));
    }

    for (const room of rooms) {
      if (budget <= 0) break;

      let players = byRoom.get(room.roomId) ?? [];

      // Unsettled players are excluded from the groups below, but the room
      // itself stays usable for its settled occupants.
      let groups = this.groupByLevel(players, now);
      // A room may hold three or more levels. Keep draining its minority levels
      // in the same cycle until one is left, so a single mixed room does not
      // need one cooldown window per stray level.
      while (groups.length >= 2 && budget > 0) {
        const source = this.pickSourceGroup(groups);
        if (!source || source.players.length < this.options.minLevelPlayers) break;

        const movable = source.players.filter((p) => !claimedPlayers.has(p.playerId as string));
        if (movable.length === 0) break;

        // Try candidate targets in priority order; use the first one that can
        // fit the whole bonded group (hand-holding friends travel together).
        let picked: { target: RoomSnapshot; selected: StoredTelemetry[] } | undefined;
        for (const candidate of this.targetCandidates(source.levelId, room.roomId, rooms, levelsByRoom, occupancy, byRoom, movable, now)) {
          const cap = Math.max(0, candidate.capacity - (occupancy.get(candidate.roomId) ?? 0));
          const sel = this.selectBondedGroups(movable, cap, Math.min(this.options.maxPlayersPerMove, budget));
          if (sel.length > 0) { picked = { target: candidate, selected: sel }; break; }
        }
        if (!picked) {
          log?.warn({
            sourceRoomId: room.roomId,
            levelId: source.levelId,
            roomCount: rooms.length,
            emptyRooms: [...occupancy.values()].filter((v) => v === 0).length
          }, "level isolation: no target fits the bonded group");
          break;
        }

        const target = picked.target;
        const selected = picked.selected;

        const targetLevelCount = (byRoom.get(target.roomId) ?? []).filter((p) => p.levelId === source.levelId).length;
        plans.push({
          playerIds: selected.map((p) => p.playerId as string),
          sourceRoomId: room.roomId,
          targetRoomId: target.roomId,
          levelId: source.levelId,
          sourceLevelCount: source.players.length,
          targetLevelCount,
          reason: "level_isolation"
        });

        const movedIds = new Set(selected.map((p) => p.playerId as string));
        for (const id of movedIds) claimedPlayers.add(id);
        occupancy.set(target.roomId, (occupancy.get(target.roomId) ?? 0) + selected.length);
        occupancy.set(room.roomId, Math.max(0, (occupancy.get(room.roomId) ?? 0) - selected.length));
        // The destination now holds this level; later plans in this cycle may use
        // it as a same-level target, but it is still claimed as a target so two
        // plans never write into the same room at once.
        levelsByRoom.get(target.roomId)?.add(source.levelId);
        usedTargets.add(target.roomId);
        budget -= selected.length;

        players = players.filter((p) => !movedIds.has(p.playerId as string));
        // Keep the per-room roster and level set in step with the planned move.
        // Without this a source room that just lost its only player on this
        // level still looked mixed, which hides it from the consolidation pass
        // for the rest of the cycle.
        byRoom.set(room.roomId, players);
        if (!players.some((p) => p.levelId === source.levelId)) {
          levelsByRoom.get(room.roomId)?.delete(source.levelId);
        }
        groups = this.groupByLevel(players, now);
      }
    }

    // Second pass: merge sparse same-level rooms. Two rooms each holding 1-2
    // players on the same level produce no isolation signal (each is "clean"),
    // so without this pass those players never see each other.
    this.consolidateSparseSameLevel(plans, byRoom, occupancy, levelsByRoom, rooms, claimedPlayers, usedTargets, new Set(), budget, now, log);

    return plans;
  }

  private consolidateSparseSameLevel(
    plans: LevelIsolationPlan[],
    byRoom: Map<string, StoredTelemetry[]>,
    occupancy: Map<string, number>,
    levelsByRoom: Map<string, Set<number>>,
    rooms: RoomSnapshot[],
    claimedPlayers: Set<string>,
    usedTargets: Set<string>,
    usedSources: Set<string>,
    budget: number,
    now: number,
    log?: Logger
  ): void {
    const threshold = this.options.sparseRoomThreshold;
    // Classify rooms by their single level into "sparse" (merge source, ≤ threshold
    // players) and "full" (merge target, any occupancy with free capacity).
    // A sparse room merges into any same-level room with space; the threshold
    // only limits the source side, so a 1-player room merges into a 5-player one.
    const sparseByLevel = new Map<number, Array<{ roomId: string; occ: number }>>();
    const targetByLevel = new Map<number, Array<{ roomId: string; occ: number; cap: number }>>();

    for (const room of rooms) {
      const levels = levelsByRoom.get(room.roomId);
      if (!levels || levels.size !== 1) continue;
      const occ = occupancy.get(room.roomId) ?? 0;
      if (occ === 0) continue;
      const levelId = [...levels][0];
      if (levelId === undefined) continue;
      const unsettled = this.hasUnsettled(byRoom.get(room.roomId) ?? [], now);
      // A room with a player who has not settled yet is "expecting" them
      // (friend-join / cross-room arrival). Do not drain it or fill it until
      // the arrival is resolved; moving the resident out is exactly what
      // breaks an in-game join-friend attempt.
      if (unsettled) continue;

      const entry = { roomId: room.roomId, occ, cap: room.capacity };
      if (occ <= threshold) {
        const list = sparseByLevel.get(levelId) ?? [];
        list.push(entry);
        sparseByLevel.set(levelId, list);
      }
      if (occ < room.capacity) {
        const list = targetByLevel.get(levelId) ?? [];
        list.push(entry);
        targetByLevel.set(levelId, list);
      }
    }

    for (const [levelId, sparseRooms] of sparseByLevel) {
      if (budget <= 0) break;
      // Sort sparse rooms by ascending occupancy so the smallest merges first.
      sparseRooms.sort((a, b) => a.occ - b.occ);

      for (const source of sparseRooms) {
        if (budget <= 0) break;
        // The occupancy snapshot was taken before any merges in this pass; a
        // room that just absorbed players is no longer sparse and must not
        // also act as a source, which would bounce players back and forth.
        const currentOcc = occupancy.get(source.roomId) ?? 0;
        if (currentOcc > threshold || usedTargets.has(source.roomId)) continue;

        const target = (targetByLevel.get(levelId) ?? [])
          .filter((t) => t.roomId !== source.roomId && !usedTargets.has(t.roomId) && !usedSources.has(t.roomId) && t.occ < t.cap)
          .sort((a, b) => b.occ - a.occ)[0];
        if (!target) continue;

        const rawCandidates = (byRoom.get(source.roomId) ?? [])
          .filter((p) => p.levelId === levelId && !claimedPlayers.has(p.playerId as string) && this.isSettled(p, now));
        const sourcePlayers = this.filterByAffinity(rawCandidates, byRoom.get(source.roomId) ?? [], claimedPlayers);
        if (sourcePlayers.length === 0) continue;

        // Pick whole bonded groups only. Taking a slice of the candidate list
        // split a hand-holding pair whenever the target had a single free slot;
        // a group that does not fit is skipped instead.
        const capacity = Math.min(target.cap - target.occ, this.options.maxPlayersPerMove, budget);
        const selected = this.selectBondedGroups(sourcePlayers, capacity, capacity);
        if (selected.length === 0) continue;

        plans.push({
          playerIds: selected.map((p) => p.playerId as string),
          sourceRoomId: source.roomId,
          targetRoomId: target.roomId,
          levelId,
          sourceLevelCount: sourcePlayers.length,
          targetLevelCount: target.occ,
          reason: "level_consolidation"
        });

        for (const p of selected) claimedPlayers.add(p.playerId as string);
        occupancy.set(target.roomId, (occupancy.get(target.roomId) ?? 0) + selected.length);
        occupancy.set(source.roomId, Math.max(0, (occupancy.get(source.roomId) ?? 0) - selected.length));
        usedTargets.add(target.roomId);
        usedSources.add(source.roomId);
        // Keep targetByLevel in sync so later merges in this pass see updated occupancy.
        for (const t of targetByLevel.get(levelId) ?? []) {
          if (t.roomId === target.roomId) t.occ += selected.length;
          if (t.roomId === source.roomId) t.occ = occupancy.get(source.roomId) ?? 0;
        }
        budget -= selected.length;

        log?.debug({
          sourceRoomId: source.roomId,
          targetRoomId: target.roomId,
          levelId,
          movedPlayers: selected.length
        }, "sparse same-level room consolidated");
      }
    }
  }

  /**
   * Select players to migrate without splitting bonded pairs (affinity ≥ 0.9).
   * Players are grouped into connected components by their bonds; whole groups
   * are taken or skipped so hand-holding friends always travel together.
   * Unbonded individuals fill remaining capacity freely.
   */
  private selectBondedGroups(
    candidates: StoredTelemetry[],
    capacity: number,
    maxCount: number
  ): StoredTelemetry[] {
    if (capacity <= 0 || maxCount <= 0) return [];
    const limit = Math.min(capacity, maxCount, candidates.length);
    if (limit <= 0) return [];

    // Build connected components via union-find on high-affinity edges.
    const index = new Map<string, number>();
    candidates.forEach((p, i) => index.set(p.playerId as string, i));
    const parent = candidates.map((_, i) => i);
    const find = (x: number): number => { let r = x; while (parent[r] !== r) r = parent[r]!; return r; };
    const union = (a: number, b: number): void => { const ra = find(a), rb = find(b); if (ra !== rb) parent[ra] = rb; };

    for (let i = 0; i < candidates.length; i++) {
      for (let j = i + 1; j < candidates.length; j++) {
        const a = candidates[i]!, b = candidates[j]!;
        const aff = Math.max(a.affinity?.[b.playerId as string] ?? 0, b.affinity?.[a.playerId as string] ?? 0);
        if (aff >= 0.9) union(i, j);
      }
    }

    // Group indices by root.
    const groups = new Map<number, number[]>();
    for (let i = 0; i < candidates.length; i++) {
      const root = find(i);
      const list = groups.get(root) ?? [];
      list.push(i);
      groups.set(root, list);
    }

    // Fill capacity with whole groups: smaller groups first (flexible), then larger.
    const sorted = [...groups.values()].sort((a, b) => a.length - b.length);
    const selected: StoredTelemetry[] = [];
    for (const group of sorted) {
      if (selected.length + group.length > limit) continue;
      for (const i of group) selected.push(candidates[i]!);
      if (selected.length >= limit) break;
    }
    return selected;
  }

  private groupByLevel(players: StoredTelemetry[], now: number): RoomLevelGroup[] {
    const byLevel = new Map<number, StoredTelemetry[]>();
    for (const player of players) {
      if (player.levelId === undefined) continue;
      // A player who just joined / just switched maps still reports the old
      // level; counting it would make a perfectly normal room look mixed.
      if (!this.isSettled(player, now)) continue;
      const group = byLevel.get(player.levelId) ?? [];
      group.push(player);
      byLevel.set(player.levelId, group);
    }
    return [...byLevel.entries()]
      .map(([levelId, groupPlayers]) => ({ levelId, players: groupPlayers, newestAt: this.newestChangeAt(groupPlayers) }))
      .sort((a, b) => b.players.length - a.players.length || b.newestAt - a.newestAt);
  }

  /**
   * Used ONLY by the consolidation pass (merging sparse same-level rooms).
   * Prevents splitting a bonded pair (≥ 0.9) when only some players move.
   *
   * NOT used by the isolation pass: level isolation moves a player alone when
   * they switch level, regardless of affinity. The level group itself defines
   * who travels together (two players who both switched to the same level go
   * as a pair; a single player switching goes alone).
   */
  private filterByAffinity(
    candidates: StoredTelemetry[],
    allRoomPlayers: StoredTelemetry[],
    claimed: Set<string>
  ): StoredTelemetry[] {
    const candidateIds = new Set(candidates.map((p) => p.playerId as string));
    return candidates.filter((player) => {
      for (const other of allRoomPlayers) {
        if (other.playerId === player.playerId) continue;
        // Bonded peer is also migrating (same group or already claimed) → fine.
        if (candidateIds.has(other.playerId as string) || claimed.has(other.playerId as string)) continue;
        // Cross-level bond: ignore. Level isolation wins over friendship.
        if (other.levelId !== player.levelId) continue;
        const affinity = Math.max(
          player.affinity?.[other.playerId as string] ?? 0,
          other.affinity?.[player.playerId as string] ?? 0
        );
        // Same-level bond with someone staying behind → keep together.
        if (affinity >= 0.9) return false;
      }
      return true;
    });
  }

  private newestChangeAt(players: StoredTelemetry[]): number {
    let newest = 0;
    for (const player of players) {
      // A fresh arrival counts as "new" too: when two level groups tie, the
      // player who just joined (friend-join, cross-room move) is the one whose
      // state is least settled, so the newcomer moves — never the resident
      // whose room is being joined.
      const at = Math.max(player.levelChangedAt ?? 0, player.roomJoinedAt ?? 0);
      if (at > newest) newest = at;
    }
    return newest;
  }

  /**
   * A player's level is settled once they have been in the room for a moment
   * and have not just switched level. While unsettled, their reported level is
   * the previous map and must not be treated as a real level group.
   */
  private isSettled(player: StoredTelemetry, now: number): boolean {
    const levelCutoff = now - this.options.levelChangeCooldownMs;
    const joinCutoff = now - this.options.joinSettleMs;
    return (player.levelChangedAt ?? 0) <= levelCutoff && (player.roomJoinedAt ?? 0) <= joinCutoff;
  }

  /** True while any occupant of the room is still settling. */
  private hasUnsettled(players: StoredTelemetry[], now: number): boolean {
    return players.some((player) => !this.isSettled(player, now));
  }

  /**
   * The majority level stays put; the minority moves. Ties go to the group that
   * changed level most recently, which is the group the player action created.
   */
  private pickSourceGroup(groups: RoomLevelGroup[]): RoomLevelGroup | undefined {
    if (groups.length < 2) return undefined;
    const last = groups[groups.length - 1];
    if (!last) return undefined;
    const smallest = last.players.length;
    if (smallest === 0) return undefined;
    // `groups` is sorted by size desc, then newestAt desc, so the first group
    // of the smallest size is also the one that changed level most recently —
    // that is the group this player action just created. Picking the last
    // element instead (as before) moved the players who stayed put whenever
    // both sides had the same size.
    return groups.find((group) => group.players.length === smallest);
  }

  /**
   * Return candidate destinations in priority order. The caller tries each one
   * and uses the first that fits the bonded group, so a target with only one
   * free slot is skipped when the migrating group needs two.
   *
   * Priority: clean same-level room > empty room > mixed room with this level.
   * Within the first tier, rooms are sorted by spatial proximity then occupancy.
   */
  private targetCandidates(
    levelId: number,
    sourceRoomId: string,
    rooms: RoomSnapshot[],
    levelsByRoom: Map<string, Set<number>>,
    occupancy: Map<string, number>,
    byRoom: Map<string, StoredTelemetry[]>,
    sourcePlayers: StoredTelemetry[],
    now: number
  ): RoomSnapshot[] {
    const isUsable = (room: RoomSnapshot): boolean => {
      if (room.roomId === sourceRoomId || room.draining) return false;
      if ((occupancy.get(room.roomId) ?? 0) >= room.capacity) return false;
      // Never pull players into a room that still has an unsettled player.
      return !this.hasUnsettled(byRoom.get(room.roomId) ?? [], now);
    };

    const byProximity = (a: RoomSnapshot, b: RoomSnapshot): number => {
      const proxA = this.proximityScore(sourcePlayers, byRoom.get(a.roomId) ?? []);
      const proxB = this.proximityScore(sourcePlayers, byRoom.get(b.roomId) ?? []);
      return proxB - proxA || (occupancy.get(b.roomId) ?? 0) - (occupancy.get(a.roomId) ?? 0);
    };

    const sameLevel = rooms
      .filter((room) => {
        if (!isUsable(room)) return false;
        const levels = levelsByRoom.get(room.roomId);
        return levels?.size === 1 && levels.has(levelId);
      })
      .sort(byProximity);

    const empty = rooms
      .filter((room) => isUsable(room) && (occupancy.get(room.roomId) ?? 0) === 0)
      .sort((a, b) => b.capacity - a.capacity);

    const mixed = rooms
      .filter((room) => {
        if (!isUsable(room)) return false;
        return levelsByRoom.get(room.roomId)?.has(levelId) === true;
      })
      .sort(byProximity);

    return [...sameLevel, ...empty, ...mixed];
  }

  /** Average inverse-distance between two player groups; 0 when no position data. */
  private proximityScore(a: StoredTelemetry[], b: StoredTelemetry[]): number {
    let total = 0;
    let count = 0;
    for (const pa of a) {
      if (!pa.position) continue;
      for (const pb of b) {
        if (!pb.position) continue;
        const d = Math.hypot(
          pa.position[0] - pb.position[0],
          pa.position[1] - pb.position[1],
          pa.position[2] - pb.position[2]
        );
        total += 1 / (1 + d);
        count += 1;
      }
    }
    return count ? total / count : 0;
  }
}
