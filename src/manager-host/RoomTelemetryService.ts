import type { NodeId, PlayerId, RoomId } from '../types/branded.js'
import type { INodeEvent, PlayerTelemetry, RoomSnapshot } from '../types/contracts.js'
import type { NodeRegistry } from './NodeRegistry.js'
import type { RoomCache } from './RoomCache.js'
import type { TelemetryStore } from './TelemetryStore.js'

interface AcceptedRoster {
  seq: number
  players: Map<string, PlayerTelemetry>
  nodeId: NodeId
  connectedAt?: number
}
interface Options {
  rooms: RoomCache
  nodes: NodeRegistry
  telemetry: TelemetryStore
  requestFull: (roomId: RoomId) => void
  onGap?: (() => void) | undefined
  onReset?: ((roomId: RoomId) => void) | undefined
  onAccepted?: (() => void) | undefined
  clock?: () => number
}

/** Single acceptance owner. All validation precedes writes to room/location/player indexes. */
export class RoomTelemetryService {
  private readonly rosters = new Map<RoomId, AcceptedRoster>()
  private readonly gaps = new Map<RoomId, { since: number; dropped: number }>()
  private readonly clock: () => number

  public constructor(private readonly options: Options) { this.clock = options.clock ?? Date.now }
  public count(roomId: RoomId): number { return this.rosters.get(roomId)?.players.size ?? 0 }
  public revision(roomId: RoomId): number | undefined { return this.rosters.get(roomId)?.seq }
  public hasGap(roomId: RoomId): boolean { return this.gaps.has(roomId) }
  public gapIds(): RoomId[] { return [...this.gaps.keys()] }
  public gapCount(): number { return this.gaps.size }
  public forget(roomId: RoomId): void {
    this.rosters.delete(roomId)
    this.gaps.delete(roomId)
    this.options.onReset?.(roomId)
  }
  public noteJoin(roomId: RoomId, player: PlayerTelemetry): void {
    const roster = this.rosters.get(roomId)
    // The next accepted frame is the observation used for stability; a join is
    // not a fabricated telemetry revision.
    if (roster) roster.players.set(player.playerId, player)
    this.options.onReset?.(roomId)
  }
  public noteLeave(roomId: RoomId, playerId: PlayerId, sessionId?: string): void {
    const roster = this.rosters.get(roomId)
    const before = roster?.players.get(playerId)
    if (!sessionId || !before?.sessionId || before.sessionId === sessionId) roster?.players.delete(playerId)
    this.options.onReset?.(roomId)
  }

  public accept(event: INodeEvent, roomId: RoomId): boolean {
    const { rooms, nodes, telemetry } = this.options
    const data = event.data
    const room = rooms.getRoom(roomId)
    if (!room || room.nodeId !== data.nodeId || !Array.isArray(data.players) || !Number.isSafeInteger(data.seq)) return false
    const epoch = nodes.getTransport(data.nodeId)?.connectedAt
    if (data.connectedAt !== undefined && data.connectedAt !== epoch) return false
    const previous = this.rosters.get(roomId)
    const sameEpoch = !previous || (previous.nodeId === data.nodeId && previous.connectedAt === epoch)
    const seq = data.seq!
    const delta = data.kind === 'delta'
    if (seq < room.version || (previous && sameEpoch && seq <= previous.seq)) return false
    if (delta && (!previous || !sameEpoch || this.gaps.has(roomId) || data.baseSeq !== previous.seq)) return this.reject(roomId)
    const next = delta ? new Map(previous!.players) : new Map<string, PlayerTelemetry>()
    for (const id of data.removedPlayers ?? []) next.delete(id)
    const incoming = new Set<string>()
    for (const player of data.players) {
      if (incoming.has(player.playerId)) return this.reject(roomId)
      incoming.add(player.playerId)
      const location = rooms.getPlayerLocation(player.playerId)
      if (location && location.roomId !== roomId && location.leavingAt === undefined) {
        next.delete(player.playerId)
        continue
      }
      const before = previous?.players.get(player.playerId) ?? telemetry.get(player.playerId)
      if (before && before.sessionId === player.sessionId && before.lvSeq !== undefined && player.lvSeq !== undefined) {
        const distance = (player.lvSeq - before.lvSeq + 256) & 255
        if (distance >= 128 || (distance === 0 && before.levelId !== undefined && player.levelId !== undefined && before.levelId !== player.levelId)) return this.reject(roomId)
      }
      next.set(player.playerId, { ...player })
    }
    if (next.size > room.capacity || next.size > 8) return this.reject(roomId)
    const physical = data.physicalPlayers ?? Math.max(next.size, data.activePlayers ?? next.size)
    if (!Number.isInteger(physical) || physical < next.size || physical > room.capacity || physical > 8 ||
        (data.activePlayers !== undefined && data.activePlayers > physical) ||
        (data.reservedSlots !== undefined && physical + data.reservedSlots > room.capacity)) return this.reject(roomId)

    // No awaited work below: all views commit as one synchronous accepted update.
    if (!rooms.acceptTelemetryRoom({ ...room, players: physical, version: seq })) return false
    const oldPlayers = new Map<string, PlayerTelemetry>(previous?.players)
    if (!delta) for (const player of telemetry.listRoom(roomId)) oldPlayers.set(player.playerId, player)
    for (const before of oldPlayers.values()) if (!next.has(before.playerId)) {
      telemetry.remove(before.playerId, roomId, before.sessionId)
      rooms.removePlayer(before.playerId, roomId, before.sessionId)
    }
    for (const player of next.values()) {
      telemetry.upsert(roomId, player, this.clock())
      rooms.trackPlayer(player.playerId, data.nodeId, roomId, player.levelId, player.sessionId, player.lvSeq)
    }
    this.rosters.set(roomId, { seq, players: next, nodeId: data.nodeId, ...(epoch !== undefined ? { connectedAt: epoch } : {}) })
    if (!sameEpoch || this.gaps.delete(roomId)) this.options.onReset?.(roomId)
    this.options.onAccepted?.()
    return true
  }

  private reject(roomId: RoomId): false {
    const previous = this.gaps.get(roomId)
    this.gaps.set(roomId, { since: previous?.since ?? this.clock(), dropped: (previous?.dropped ?? 0) + 1 })
    this.options.onReset?.(roomId)
    this.options.onGap?.()
    this.options.requestFull(roomId)
    return false
  }
}
