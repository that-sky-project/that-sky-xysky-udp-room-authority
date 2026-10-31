import type { RoomSnapshot } from '../types/contracts.js'
import type { Logger } from '../infra/logger/logger.js'
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
  /** After any player in a room changes level, wait this long before migrating
   *  anyone from that room. Prevents acting on the brief "false mixed" state
   *  that exists while two hand-holding players switch maps at slightly
   *  different times. */
  levelChangeCooldownMs: number
  /** A player who just joined a room (friend-join teleport, cross-room move)
   *  is still loading the destination map and may briefly report the previous
   *  level. Leave the room alone for this long before isolating anyone. */
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
  maxPlayersPerMove: 8,
  sparseRoomThreshold: 2,
  levelChangeCooldownMs: 6_000,
  joinSettleMs: 4_000,
  protectedLevels: []
}
export class LevelIsolationPlanner {
  private readonly options: LevelIsolationOptions
  private readonly protectedLevels: Set<number>
  public constructor (options: Partial<LevelIsolationOptions> = {}) {
    this.options = { ...DEFAULT_OPTIONS, ...options }
    this.protectedLevels = new Set(
      (options.protectedLevels ?? DEFAULT_OPTIONS.protectedLevels).filter(
        id => Number.isInteger(id) && id > 0
      )
    )
  }
  public plan (
    rooms: RoomSnapshot[],
    telemetry: TelemetryStore,
    now = Date.now(),
    log?: Logger
  ): LevelIsolationPlan[] {
    const plans: LevelIsolationPlan[] = []
    const claimedPlayers = new Set<string>()
    const usedTargets = new Set<string>()
    let budget = this.options.maxPlayersPerMove * Math.max(1, rooms.length)
    const occupancy = new Map<string, number>()
    const byRoom = new Map<string, StoredTelemetry[]>()
    for (const room of rooms) {
      const players = telemetry.listRoom(room.roomId, now)
      byRoom.set(room.roomId, players)
      occupancy.set(room.roomId, players.length)
    }
    const levelsByRoom = new Map<string, Set<number>>()
    for (const [roomId, players] of byRoom) {
      levelsByRoom.set(
        roomId,
        new Set(
          players
            .filter(p => this.isSettled(p, now))
            .map(p => p.levelId)
            .filter((l): l is number => l !== undefined)
        )
      )
    }
    for (const room of rooms) {
      if (budget <= 0) break
      let players = byRoom.get(room.roomId) ?? []
      let forcedGroups: RoomLevelGroup[] = []
      const protectedPlayers =
        this.protectedLevels.size > 0
          ? players.filter(p => this.isProtectedLevel(p.levelId))
          : []
      if (protectedPlayers.length > 0) {
        const others = players.filter(p => !this.isProtectedLevel(p.levelId))
        if (others.length === 0) continue
        forcedGroups = this.groupByLevel(others, now)
        if (forcedGroups.length === 0) continue
      }
      let groups =
        protectedPlayers.length > 0 ? [] : this.groupByLevel(players, now)
      while ((forcedGroups.length > 0 || groups.length >= 2) && budget > 0) {
        const forced = forcedGroups.shift()
        const source: RoomLevelGroup | undefined =
          forced ?? this.pickSourceGroup(groups)
        if (!source) break
        if (!forced && source.players.length < this.options.minLevelPlayers)
          break
        const movable = source.players.filter(
          p => !claimedPlayers.has(p.playerId as string)
        )
        if (movable.length === 0) {
          if (forced) continue
          break
        }
        if (!forced) this.expandBonded(movable, players, claimedPlayers)
        let picked:
          | { target: RoomSnapshot; selected: StoredTelemetry[] }
          | undefined
        for (const candidate of this.targetCandidates(
          source.levelId,
          room.roomId,
          rooms,
          levelsByRoom,
          occupancy,
          byRoom,
          movable,
          now,
          !forced
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
          log?.warn(
            {
              sourceRoomId: room.roomId,
              levelId: source.levelId,
              roomCount: rooms.length,
              emptyRooms: [...occupancy.values()].filter(v => v === 0).length
            },
            'level isolation: no target fits the bonded group'
          )
          if (forced) continue
          break
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
        if (!players.some(p => p.levelId === source.levelId)) {
          levelsByRoom.get(room.roomId)?.delete(source.levelId)
        }
        if (forced) continue
        groups = this.groupByLevel(players, now)
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
      log
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
    log?: Logger
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
      const unsettled = this.hasUnsettled(byRoom.get(room.roomId) ?? [], now)
      if (this.hasProtected(byRoom.get(room.roomId) ?? [])) continue
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
        if (currentOcc > threshold || usedTargets.has(source.roomId)) continue
        const target = (targetByLevel.get(levelId) ?? [])
          .filter(
            t =>
              t.roomId !== source.roomId &&
              !usedTargets.has(t.roomId) &&
              !usedSources.has(t.roomId) &&
              t.occ < t.cap
          )
          .sort((a, b) => b.occ - a.occ)[0]
        if (!target) continue
        const rawCandidates = (byRoom.get(source.roomId) ?? []).filter(
          p =>
            p.levelId === levelId &&
            !claimedPlayers.has(p.playerId as string) &&
            this.isSettled(p, now)
        )
        const sourcePlayers = this.filterByAffinity(
          rawCandidates,
          byRoom.get(source.roomId) ?? [],
          claimedPlayers
        )
        if (sourcePlayers.length === 0) continue
        const capacity = Math.min(
          target.cap - target.occ,
          this.options.maxPlayersPerMove,
          budget
        )
        const selected = this.selectBondedGroups(
          sourcePlayers,
          capacity,
          capacity
        )
        if (selected.length === 0) continue
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
    if (capacity <= 0 || maxCount <= 0) return []
    const limit = Math.min(capacity, maxCount, candidates.length)
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
    now: number
  ): RoomLevelGroup[] {
    const byLevel = new Map<number, StoredTelemetry[]>()
    for (const player of players) {
      if (player.levelId === undefined) continue
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
    return candidates.filter(player => {
      for (const other of allRoomPlayers) {
        if (other.playerId === player.playerId) continue
        if (
          candidateIds.has(other.playerId as string) ||
          claimed.has(other.playerId as string)
        )
          continue
        if (other.levelId !== player.levelId) continue
        const affinity = Math.max(
          player.affinity?.[other.playerId as string] ?? 0,
          other.affinity?.[player.playerId as string] ?? 0
        )
        if (affinity >= 0.9) return false
      }
      return true
    })
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
      (player.levelChangedAt ?? 0) <= levelCutoff &&
      (player.roomJoinedAt ?? 0) <= joinCutoff
    )
  }
  private hasUnsettled (players: StoredTelemetry[], now: number): boolean {
    return players.some(player => !this.isSettled(player, now))
  }
  private expandBonded (
    movable: StoredTelemetry[],
    roomPlayers: StoredTelemetry[],
    claimed: Set<string>
  ): void {
    const included = new Set(movable.map(player => player.playerId as string))
    const queue = [...movable]
    while (queue.length > 0) {
      const player = queue.pop()
      if (!player) break
      for (const other of roomPlayers) {
        const otherId = other.playerId as string
        if (included.has(otherId) || claimed.has(otherId)) continue
        const affinity = Math.max(
          player.affinity?.[otherId] ?? 0,
          other.affinity?.[player.playerId as string] ?? 0
        )
        if (affinity < 0.9) continue
        included.add(otherId)
        movable.push(other)
        queue.push(other)
      }
    }
  }
  private pickSourceGroup (
    groups: RoomLevelGroup[]
  ): RoomLevelGroup | undefined {
    if (groups.length < 2) return undefined
    const last = groups[groups.length - 1]
    if (!last) return undefined
    const smallest = last.players.length
    if (smallest === 0) return undefined
    return groups.find(group => group.players.length === smallest)
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
    allowMixed = true
  ): RoomSnapshot[] {
    const isUsable = (room: RoomSnapshot): boolean => {
      if (room.roomId === sourceRoomId || room.draining) return false
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
    const mixed = rooms
      .filter(room => {
        if (!isUsable(room)) return false
        return levelsByRoom.get(room.roomId)?.has(levelId) === true
      })
      .sort(byProximity)
    if (!allowMixed) return [...sameLevel, ...empty]
    return [...sameLevel, ...empty, ...mixed]
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
