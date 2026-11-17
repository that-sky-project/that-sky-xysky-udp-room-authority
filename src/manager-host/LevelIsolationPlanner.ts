import type { RoomSnapshot } from '../types/contracts.js'
import type { Logger } from '../infra/logger/logger.js'
import type { PlanningContext } from './MovePlanner.js'
import type { StoredTelemetry, TelemetryStore } from './TelemetryStore.js'
export interface LevelIsolationPlan {
  playerIds: string[]
  sourceRoomId: string
  targetRoomId: string
  levelId: number
  sourceLevelCount: number
  targetLevelCount: number
  reason: 'level_isolation' | 'level_consolidation'
}
export interface LevelIsolationOptions {
  minLevelPlayers: number
  maxPlayersPerMove: number
  /** Rooms with this many or fewer players are considered sparse and eligible
   *  for same-level consolidation. */
  sparseRoomThreshold: number
  /** Each selected player must settle for this long after changing level.
   *  A hard-bond component waits for its last member to settle. */
  levelChangeCooldownMs: number
  /** A selected player must settle after joining; unrelated visitors do not
   *  delay the room's settled groups. */
  joinSettleMs: number
  /** Level ids (maps) that quarantine their room. A room holding one of these
   *  levels is never merged and never migrated; if another level intrudes, the
   *  protected level keeps the room and every other level is moved out. Players
   *  on a protected level are never moved. Protection is level-based, so it
   *  ends by itself when the last protected occupant changes level or leaves. */
  protectedLevels: number[]
}
interface RoomLevelGroup {
  levelId: number
  players: StoredTelemetry[]
  newestAt: number
}
const DEFAULT_OPTIONS: LevelIsolationOptions = {
  minLevelPlayers: 1,
  // Must stay <= the node executor's per-move group cap (movePlayersAtomic
  // rejects groups larger than 7 with move_group_too_large): an 8-player plan
  // failed wholesale at commit and fell back to single-player migrations.
  maxPlayersPerMove: 7,
  sparseRoomThreshold: 2,
  levelChangeCooldownMs: 6_000,
  joinSettleMs: 6_000,
  protectedLevels: []
}
export class LevelIsolationPlanner {
  private readonly options: LevelIsolationOptions
  private readonly protectedLevels: Set<number>
  public constructor (options: Partial<LevelIsolationOptions> = {}) {
    this.options = { ...DEFAULT_OPTIONS, ...options }
    if (!Number.isInteger(this.options.maxPlayersPerMove) || this.options.maxPlayersPerMove < 1 || this.options.maxPlayersPerMove > 7) {
      throw new RangeError('maxPlayersPerMove must be between 1 and 7')
    }
    this.protectedLevels = new Set(
      (options.protectedLevels ?? DEFAULT_OPTIONS.protectedLevels).filter(
        id => Number.isInteger(id) && id > 0
      )
    )
  }
  public get levelChangeCooldownMs (): number {
    return this.options.levelChangeCooldownMs
  }
  public plan (
    rooms: RoomSnapshot[],
    telemetry: TelemetryStore,
    now = Date.now(),
    log?: Logger,
    context: PlanningContext = {}
  ): LevelIsolationPlan[] {
    const plans: LevelIsolationPlan[] = []
    const claimedPlayers = new Set<string>()
    const usedTargets = new Set<string>()
    let budget = this.options.maxPlayersPerMove * Math.max(1, rooms.length)
    const occupancy = new Map<string, number>()
    const byRoom = new Map<string, StoredTelemetry[]>()
    const physicalByRoom = new Map<string, StoredTelemetry[]>()
    // Age never removes an unresolved occupant from composition or bond
    // closure. It only removes that observation from the move candidate set.
    const known = telemetry.list()
    for (const room of rooms) {
      const physical = context.physicalPlayersOf?.(room.roomId) ??
        (typeof telemetry.listKnownRoom === 'function' ? telemetry.listKnownRoom(room.roomId) : known.filter(player => player.roomId === room.roomId))
      physicalByRoom.set(room.roomId, physical)
      byRoom.set(room.roomId, physical.filter(player => player.receivedAt <= now && now - player.receivedAt <= 10_000))
      occupancy.set(room.roomId, Math.max(context.effectivePlayersOf?.(room) ?? room.players, physical.length))
    }
    // Churn gates optional consolidation only; mandatory isolation waits six
    // seconds for coherence without imposing the optional quarantine.
    const churningIds = new Set(known.filter(player => context.isChurning?.(player.playerId, now) ?? telemetry.isChurning?.(player.playerId, now) ?? false).map(player => player.playerId as string))
    const levelsByRoom = new Map<string, Set<number>>()
    for (const [roomId, players] of physicalByRoom) {
      levelsByRoom.set(roomId, new Set(players.map(p => p.levelId).filter((level): level is number => level !== undefined)))
    }
    for (const room of rooms) {
      if (budget <= 0) break
      let players = byRoom.get(room.roomId) ?? []
      if (room.draining || players.length > 8 ||
          (context.isIsolationSourceRoom?.(room, now) ?? context.isCandidateRoom?.(room, 'source', now)) === false) continue
      const physical = physicalByRoom.get(room.roomId) ?? []
      const protectedRoom = this.hasProtected(physical)
      const composition = new Map<number, StoredTelemetry[]>()
      for (const player of physical) {
        if (player.levelId === undefined) continue
        const group = composition.get(player.levelId) ?? []
        group.push(player)
        composition.set(player.levelId, group)
      }
      if (!protectedRoom && composition.size < 2) continue
      // Retain the largest occupied level, including loading/stale occupants.
      // Ties retain the older group (then the later insertion, as before).
      const ranked = [...composition].sort((a, b) => b[1].length - a[1].length || this.newestChangeAt(b[1]) - this.newestChangeAt(a[1]))
      const maxCount = ranked[0]?.[1].length
      const retainedLevel = ranked.filter(([, group]) => group.length === maxCount).at(-1)?.[0]
      const excludedLevels = new Set<number>()
      let groups = this.groupByLevel(players, now).filter(group => protectedRoom
        ? !this.isProtectedLevel(group.levelId) : group.levelId !== retainedLevel)
      while (groups.length > 0 && budget > 0) {
        const source = groups.at(-1)!
        if (!protectedRoom && source.players.length < this.options.minLevelPlayers) break
        const movable = this.filterByAffinity(source.players.filter(
          p => !claimedPlayers.has(p.playerId as string)
        ), physicalByRoom.get(room.roomId) ?? players, claimedPlayers)
        if (movable.length === 0) {
          // Defer a conflicting bond, but still permit unrelated coherent groups.
          context.onBlocked?.(room.roomId, 'bond_incomplete', { levelId: source.levelId, playerIds: source.players.map(player => player.playerId) })
          excludedLevels.add(source.levelId)
          groups = groups.filter(group => group !== source)
          continue
        }
        let picked:
          | { target: RoomSnapshot; selected: StoredTelemetry[] }
          | undefined
        for (const candidate of this.targetCandidates(
          source.levelId,
          room.roomId,
          rooms,
          levelsByRoom,
          occupancy,
          physicalByRoom,
          movable,
          now,
          context
        )) {
          const cap = Math.max(
            0,
            candidate.capacity - (occupancy.get(candidate.roomId) ?? 0)
          )
          const sel = this.selectBondedGroups(
            movable,
            cap,
            Math.min(this.options.maxPlayersPerMove, budget)
          )
          if (sel.length > 0) {
            picked = { target: candidate, selected: sel }
            break
          }
        }
        if (!picked) {
          context.onBlocked?.(room.roomId as string, 'isolation_no_target', {
            levelId: source.levelId,
            playerIds: movable.map(player => player.playerId),
            emptyRooms: [...occupancy.values()].filter(v => v === 0).length
          })
          excludedLevels.add(source.levelId)
          groups = groups.filter(group => group !== source)
          continue
        }
        const target = picked.target
        const selected = picked.selected
        const targetLevelCount = (byRoom.get(target.roomId) ?? []).filter(
          p => p.levelId === source.levelId
        ).length
        plans.push({
          playerIds: selected.map(p => p.playerId as string),
          sourceRoomId: room.roomId,
          targetRoomId: target.roomId,
          levelId: source.levelId,
          sourceLevelCount: source.players.length,
          targetLevelCount,
          reason: 'level_isolation'
        })
        const movedIds = new Set(selected.map(p => p.playerId as string))
        for (const id of movedIds) claimedPlayers.add(id)
        occupancy.set(
          target.roomId,
          (occupancy.get(target.roomId) ?? 0) + selected.length
        )
        occupancy.set(
          room.roomId,
          Math.max(0, (occupancy.get(room.roomId) ?? 0) - selected.length)
        )
        levelsByRoom.get(target.roomId)?.add(source.levelId)
        usedTargets.add(target.roomId)
        budget -= selected.length
        players = players.filter(p => !movedIds.has(p.playerId as string))
        byRoom.set(room.roomId, players)
        const remaining = (physicalByRoom.get(room.roomId) ?? []).filter(p => !movedIds.has(p.playerId))
        physicalByRoom.set(room.roomId, remaining)
        physicalByRoom.set(target.roomId, [...physicalByRoom.get(target.roomId) ?? [], ...selected])
        if (!remaining.some(p => p.levelId === source.levelId)) levelsByRoom.get(room.roomId)?.delete(source.levelId)
        groups = this.groupByLevel(players, now).filter(group => !excludedLevels.has(group.levelId) &&
          (protectedRoom ? !this.isProtectedLevel(group.levelId) : group.levelId !== retainedLevel))
      }
    }
    this.consolidateSparseSameLevel(
      plans,
      byRoom,
      occupancy,
      levelsByRoom,
      rooms,
      claimedPlayers,
      usedTargets,
      new Set(),
      budget,
      now,
      log,
      context,
      churningIds,
      physicalByRoom
    )
    return plans
  }
  private consolidateSparseSameLevel (
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
    log?: Logger,
    context: PlanningContext = {},
    churningIds: Set<string> = new Set(),
    physicalByRoom: Map<string, StoredTelemetry[]> = byRoom
  ): void {
    const threshold = this.options.sparseRoomThreshold
    const sparseByLevel = new Map<
      number,
      Array<{ roomId: string; occ: number }>
    >()
    const targetByLevel = new Map<
      number,
      Array<{ roomId: string; occ: number; cap: number }>
    >()
    for (const room of rooms) {
      const levels = levelsByRoom.get(room.roomId)
      if (!levels || levels.size !== 1) continue
      const occ = occupancy.get(room.roomId) ?? 0
      if (occ === 0) continue
      const levelId = [...levels][0]
      if (levelId === undefined) continue
      const unsettled = this.hasUnsettled(physicalByRoom.get(room.roomId) ?? [], now)
      if (room.draining || this.hasProtected(physicalByRoom.get(room.roomId) ?? [])) continue
      if (unsettled) continue
      const entry = { roomId: room.roomId, occ, cap: room.capacity }
      if (occ <= threshold) {
        const list = sparseByLevel.get(levelId) ?? []
        list.push(entry)
        sparseByLevel.set(levelId, list)
      }
      if (occ < room.capacity) {
        const list = targetByLevel.get(levelId) ?? []
        list.push(entry)
        targetByLevel.set(levelId, list)
      }
    }
    for (const [levelId, sparseRooms] of sparseByLevel) {
      if (budget <= 0) break
      sparseRooms.sort((a, b) => a.occ - b.occ)
      for (const source of sparseRooms) {
        if (budget <= 0) break
        const currentOcc = occupancy.get(source.roomId) ?? 0
        const sourceRoom = rooms.find(room => room.roomId === source.roomId)
        if (!sourceRoom || context.isCandidateRoom?.(sourceRoom, 'source', now) === false ||
            context.isOptionalSourceRoom?.(sourceRoom, now) === false ||
            currentOcc > threshold || usedTargets.has(source.roomId)) continue
        const targets = (targetByLevel.get(levelId) ?? [])
          .filter(
            t =>
              t.roomId !== source.roomId &&
              !usedTargets.has(t.roomId) &&
              !usedSources.has(t.roomId) &&
              context.isCandidateRoom?.(rooms.find(room => room.roomId === t.roomId)!, 'target', now) !== false &&
              t.occ < t.cap
          )
          .sort((a, b) => b.occ - a.occ)
        if (targets.length === 0) continue
        const rawCandidates = (byRoom.get(source.roomId) ?? []).filter(
          p =>
            p.levelId === levelId &&
            !claimedPlayers.has(p.playerId as string) &&
            !churningIds.has(p.playerId as string) &&
            this.isSettled(p, now)
        )
        const sourcePlayers = this.filterByAffinity(
          rawCandidates,
          physicalByRoom.get(source.roomId) ?? [],
          claimedPlayers
        )
        if (sourcePlayers.length === 0) continue
        let picked: { target: typeof targets[number]; selected: StoredTelemetry[] } | undefined
        for (const target of targets) {
          const capacity = Math.min(target.cap - target.occ, this.options.maxPlayersPerMove, budget)
          const selected = this.selectBondedGroups(sourcePlayers, capacity, capacity)
          if (selected.length > 0) { picked = { target, selected }; break }
        }
        if (!picked) continue
        const { target, selected } = picked
        plans.push({
          playerIds: selected.map(p => p.playerId as string),
          sourceRoomId: source.roomId,
          targetRoomId: target.roomId,
          levelId,
          sourceLevelCount: sourcePlayers.length,
          targetLevelCount: target.occ,
          reason: 'level_consolidation'
        })
        for (const p of selected) claimedPlayers.add(p.playerId as string)
        occupancy.set(
          target.roomId,
          (occupancy.get(target.roomId) ?? 0) + selected.length
        )
        occupancy.set(
          source.roomId,
          Math.max(0, (occupancy.get(source.roomId) ?? 0) - selected.length)
        )
        usedTargets.add(target.roomId)
        usedSources.add(source.roomId)
        for (const t of targetByLevel.get(levelId) ?? []) {
          if (t.roomId === target.roomId) t.occ += selected.length
          if (t.roomId === source.roomId)
            t.occ = occupancy.get(source.roomId) ?? 0
        }
        budget -= selected.length
        log?.debug(
          {
            sourceRoomId: source.roomId,
            targetRoomId: target.roomId,
            levelId,
            movedPlayers: selected.length
          },
          'sparse same-level room consolidated'
        )
      }
    }
  }
  private selectBondedGroups (
    candidates: StoredTelemetry[],
    capacity: number,
    maxCount: number
  ): StoredTelemetry[] {
    if (capacity <= 0 || maxCount <= 0 || candidates.length > 8) return []
    const limit = Math.min(capacity, maxCount, candidates.length, 7)
    if (limit <= 0) return []
    const index = new Map<string, number>()
    candidates.forEach((p, i) => index.set(p.playerId as string, i))
    const parent = candidates.map((_, i) => i)
    const find = (x: number): number => {
      let r = x
      while (parent[r] !== r) r = parent[r]!
      return r
    }
    const union = (a: number, b: number): void => {
      const ra = find(a),
        rb = find(b)
      if (ra !== rb) parent[ra] = rb
    }
    for (let i = 0; i < candidates.length; i++) {
      for (let j = i + 1; j < candidates.length; j++) {
        const a = candidates[i]!,
          b = candidates[j]!
        const aff = Math.max(
          a.affinity?.[b.playerId as string] ?? 0,
          b.affinity?.[a.playerId as string] ?? 0
        )
        if (aff >= 0.9) union(i, j)
      }
    }
    const groups = new Map<number, number[]>()
    for (let i = 0; i < candidates.length; i++) {
      const root = find(i)
      const list = groups.get(root) ?? []
      list.push(i)
      groups.set(root, list)
    }
    const sorted = [...groups.values()].sort((a, b) => a.length - b.length)
    const selected: StoredTelemetry[] = []
    for (const group of sorted) {
      if (selected.length + group.length > limit) continue
      for (const i of group) selected.push(candidates[i]!)
      if (selected.length >= limit) break
    }
    return selected
  }
  private groupByLevel (
    players: StoredTelemetry[],
    now: number,
    churningIds: Set<string> = new Set()
  ): RoomLevelGroup[] {
    const byLevel = new Map<number, StoredTelemetry[]>()
    for (const player of players) {
      if (player.levelId === undefined) continue
      if (churningIds.has(player.playerId as string)) continue
      if (!this.isSettled(player, now)) continue
      const group = byLevel.get(player.levelId) ?? []
      group.push(player)
      byLevel.set(player.levelId, group)
    }
    return [...byLevel.entries()]
      .map(([levelId, groupPlayers]) => ({
        levelId,
        players: groupPlayers,
        newestAt: this.newestChangeAt(groupPlayers)
      }))
      .sort(
        (a, b) => b.players.length - a.players.length || b.newestAt - a.newestAt
      )
  }
  private filterByAffinity (
    candidates: StoredTelemetry[],
    allRoomPlayers: StoredTelemetry[],
    claimed: Set<string>
  ): StoredTelemetry[] {
    const candidateIds = new Set(candidates.map(p => p.playerId as string))
    // Reject an entire connected component when any hard-bond mate is outside
    // the coherent level/settled candidate set. `allRoomPlayers` is the room's
    // full physical roster (incl. MOVING and churning players), so bonds to a
    // peer that is MOVING, mid-cooldown or churning still block the partner.
    // Never expand across levels.
    const blocked = new Set<string>()
    let changed = true
    while (changed) {
      changed = false
      for (const player of candidates) {
        const id = player.playerId as string
        if (blocked.has(id)) continue
        for (const other of allRoomPlayers) {
          if (other.playerId === player.playerId) continue
          const affinity = Math.max(player.affinity?.[other.playerId as string] ?? 0,
            other.affinity?.[id] ?? 0)
          if (affinity >= 0.9 && (!candidateIds.has(other.playerId as string) ||
              claimed.has(other.playerId as string) || blocked.has(other.playerId as string))) {
            blocked.add(id)
            changed = true
            break
          }
        }
      }
    }
    return candidates.filter(player => !blocked.has(player.playerId as string))
  }
  private newestChangeAt (players: StoredTelemetry[]): number {
    let newest = 0
    for (const player of players) {
      const at = Math.max(player.levelChangedAt ?? 0, player.roomJoinedAt ?? 0)
      if (at > newest) newest = at
    }
    return newest
  }
  private isProtectedLevel (levelId: number | undefined): boolean {
    return levelId !== undefined && this.protectedLevels.has(levelId)
  }
  private hasProtected (players: StoredTelemetry[]): boolean {
    return players.some(player => this.isProtectedLevel(player.levelId))
  }
  private isSettled (player: StoredTelemetry, now: number): boolean {
    const levelCutoff = now - this.options.levelChangeCooldownMs
    const joinCutoff = now - this.options.joinSettleMs
    return (
      now - player.receivedAt <= 10_000 &&
      player.receivedAt <= now &&
      player.status !== 'MOVING' &&
      (player.levelChangedAt ?? 0) <= levelCutoff &&
      (player.roomJoinedAt ?? 0) <= joinCutoff
    )
  }
  private hasUnsettled (players: StoredTelemetry[], now: number): boolean {
    return players.some(player => !this.isSettled(player, now))
  }
  private targetCandidates (
    levelId: number,
    sourceRoomId: string,
    rooms: RoomSnapshot[],
    levelsByRoom: Map<string, Set<number>>,
    occupancy: Map<string, number>,
    byRoom: Map<string, StoredTelemetry[]>,
    sourcePlayers: StoredTelemetry[],
    now: number,
    context: PlanningContext = {}
  ): RoomSnapshot[] {
    const isUsable = (room: RoomSnapshot): boolean => {
      if (room.roomId === sourceRoomId || room.draining ||
          context.isCandidateRoom?.(room, 'target', now) === false) return false
      if ((occupancy.get(room.roomId) ?? 0) >= room.capacity) return false
      if (this.hasProtected(byRoom.get(room.roomId) ?? [])) return false
      return !this.hasUnsettled(byRoom.get(room.roomId) ?? [], now)
    }
    const byProximity = (a: RoomSnapshot, b: RoomSnapshot): number => {
      const proxA = this.proximityScore(
        sourcePlayers,
        byRoom.get(a.roomId) ?? []
      )
      const proxB = this.proximityScore(
        sourcePlayers,
        byRoom.get(b.roomId) ?? []
      )
      return (
        proxB - proxA ||
        (occupancy.get(b.roomId) ?? 0) - (occupancy.get(a.roomId) ?? 0)
      )
    }
    const sameLevel = rooms
      .filter(room => {
        if (!isUsable(room)) return false
        const levels = levelsByRoom.get(room.roomId)
        return levels?.size === 1 && levels.has(levelId)
      })
      .sort(byProximity)
    const empty = rooms
      .filter(room => isUsable(room) && (occupancy.get(room.roomId) ?? 0) === 0)
      .sort((a, b) => b.capacity - a.capacity)
    return [...sameLevel, ...empty]
  }
  private proximityScore (a: StoredTelemetry[], b: StoredTelemetry[]): number {
    let total = 0
    let count = 0
    for (const pa of a) {
      if (!pa.position) continue
      for (const pb of b) {
        if (!pb.position) continue
        const d = Math.hypot(
          pa.position[0] - pb.position[0],
          pa.position[1] - pb.position[1],
          pa.position[2] - pb.position[2]
        )
        total += 1 / (1 + d)
        count += 1
      }
    }
    return count ? total / count : 0
  }
}
