import type { NodeHealth } from './NodeRegistry.js'
import type { PlayerId, Region } from '../types/branded.js'
import type { RoomSnapshot } from '../types/contracts.js'
export interface RoomRequestContext {
  playerId: PlayerId
  region?: Region | undefined
  level?: number | undefined
  preferences?: Record<string, unknown> | undefined
}
export interface RoomSelection {
  room: RoomSnapshot
  score: number
  reason: string
}
export interface PolicyEngineOptions {
  maxPlayersPerRoom: number
  staleNodePenalty: number
  loadPenaltyWeight: number
  sameRegionBonus: number
  preferredFillTarget: number
  sameLevelBonus: number
  wrongLevelPenalty: number
  unknownLevelPenalty: number
  protectedLevels: number[]
}
export class PolicyEngine {
  private readonly protectedLevels: Set<number>
  public constructor (private readonly options: PolicyEngineOptions) {
    this.protectedLevels = new Set(
      options.protectedLevels.filter(id => Number.isInteger(id) && id > 0)
    )
  }
  public selectRoom (
    context: RoomRequestContext,
    rooms: RoomSnapshot[],
    nodeHealth: Map<string, NodeHealth>,
    effectivePlayersOf: (room: RoomSnapshot) => number = room => room.players,
    levelsOf: (room: RoomSnapshot) => ReadonlySet<number> = () =>
      new Set<number>()
  ): RoomSelection | undefined {
    let best: RoomSelection | undefined
    for (const room of rooms) {
      const effectivePlayers = effectivePlayersOf(room)
      if (
        room.draining ||
        effectivePlayers >=
          Math.min(room.capacity, this.options.maxPlayersPerRoom)
      )
        continue
      const health = nodeHealth.get(room.nodeId)
      if (!health || !health.connected || health.draining) continue
      const freeSlots = room.capacity - effectivePlayers
      const fillAfterJoin = effectivePlayers + 1
      const fillDistance = Math.abs(
        this.options.preferredFillTarget - fillAfterJoin
      )
      const regionScore =
        context.region && room.region === context.region
          ? this.options.sameRegionBonus
          : 0
      const loadPenalty = health.load * this.options.loadPenaltyWeight
      const roomLevels = levelsOf(room)
      if (!this.levelAllowed(context.level, roomLevels)) continue
      const levelScore = this.levelScore(context.level, roomLevels)
      const score =
        100 -
        fillDistance * 8 -
        freeSlots * 0.5 +
        regionScore -
        loadPenalty +
        levelScore
      if (!best || score > best.score) {
        best = { room, score, reason: 'highest policy score' }
      }
    }
    return best
  }
  private levelAllowed (
    level: number | undefined,
    roomLevels: ReadonlySet<number>
  ): boolean {
    if (this.protectedLevels.size === 0) return true
    if (level !== undefined && this.protectedLevels.has(level)) {
      if (roomLevels.size === 0) return true
      return roomLevels.size === 1 && roomLevels.has(level)
    }
    for (const roomLevel of roomLevels) {
      if (this.protectedLevels.has(roomLevel)) return false
    }
    return true
  }
  private levelScore (
    level: number | undefined,
    roomLevels: ReadonlySet<number>
  ): number {
    if (level === undefined) {
      return roomLevels.size > 0 ? -this.options.unknownLevelPenalty : 0
    }
    if (roomLevels.size === 0) return 0
    return roomLevels.has(level)
      ? this.options.sameLevelBonus
      : -this.options.wrongLevelPenalty
  }
}
