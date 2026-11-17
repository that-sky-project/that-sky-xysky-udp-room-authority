import type { NodeId, PlayerId, RoomId } from '../types/branded.js'
import type { INodeEvent, PlayerTelemetry, RoomSnapshot } from '../types/contracts.js'
import type { NodeRegistry } from './NodeRegistry.js'
import type { RoomCache } from './RoomCache.js'
import { generationRejection, type TelemetryStore } from './TelemetryStore.js'

export interface TelemetryRejection {
  reason: string
  seq: number
  playerId?: string
  view?: 'roster' | 'telemetry' | 'location'
  previousLvSeq?: number
  incomingLvSeq?: number
  missingFields?: string[]
  baseSeq?: number
  expectedBaseSeq?: number
}

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
  /** Bounded event delivery is owned by the coordinator; retain only the last rejection here. */
  onRejected?: ((roomId: RoomId, rejection: TelemetryRejection) => void) | undefined
  /** A node still runs a build that does not report connection identities. */
  onLegacyPlayer?: ((roomId: RoomId, playerId: string) => void) | undefined
  clock?: () => number
}

/** Single acceptance owner. All validation precedes writes to room/location/player indexes. */
export class RoomTelemetryService {
  private readonly rosters = new Map<RoomId, AcceptedRoster>()
  private readonly gaps = new Map<RoomId, { since: number; dropped: number; rejection: TelemetryRejection }>()
  private readonly clock: () => number

  public constructor(private readonly options: Options) { this.clock = options.clock ?? Date.now }
  public count(roomId: RoomId): number { return this.rosters.get(roomId)?.players.size ?? 0 }
  public revision(roomId: RoomId): number | undefined { return this.rosters.get(roomId)?.seq }
  public hasGap(roomId: RoomId): boolean { return this.gaps.has(roomId) }
  public gapIds(): RoomId[] { return [...this.gaps.keys()] }
  public gapCount(): number { return this.gaps.size }
  public gapSnapshot(roomId: RoomId) { return this.gaps.get(roomId) }
  public forget(roomId: RoomId): void {
    this.rosters.delete(roomId)
    this.gaps.delete(roomId)
    this.options.onReset?.(roomId)
  }
  public noteJoin(roomId: RoomId, player: PlayerTelemetry): void {
    const roster = this.rosters.get(roomId)
    // The next accepted frame is the observation used for stability; a join is
    // not a fabricated telemetry revision. Roster view only: stability is NOT
    // reset here, so a stream of visitors joining/leaving cannot freeze
    // planning for the room's settled core.
    if (roster) roster.players.set(player.playerId, player)
  }
  public noteLeave(roomId: RoomId, playerId: PlayerId, sessionId?: string): void {
    const roster = this.rosters.get(roomId)
    const before = roster?.players.get(playerId)
    // Roster view only; see noteJoin — no stability reset on transient churn.
    if (!sessionId || !before?.sessionId || before.sessionId === sessionId) roster?.players.delete(playerId)
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
    const reject = (reason: string, detail: Omit<TelemetryRejection, 'reason' | 'seq'> = {}): false =>
      this.reject(roomId, { reason, seq, ...detail })
    if (delta && (!previous || !sameEpoch || this.gaps.has(roomId) || data.baseSeq !== previous.seq)) {
      return reject('delta_gap', {
        ...(typeof data.baseSeq === 'number' ? { baseSeq: data.baseSeq } : {}),
        ...(previous ? { expectedBaseSeq: previous.seq } : {})
      })
    }
    const next = delta ? new Map(previous!.players) : new Map<string, PlayerTelemetry>()
    for (const id of data.removedPlayers ?? []) next.delete(id)
    const incoming = new Set<string>()
    const acceptedIncoming = new Map<string, PlayerTelemetry>()
    for (const raw of data.players) {
      if (incoming.has(raw.playerId)) return reject('duplicate_player', { playerId: raw.playerId })
      // Rolling deployments still contain pre-repair nodes that never emit a
      // connection identity. A stable uuid-derived pseudo-session keeps the
      // generation ordering meaningful per player instead of bricking the room.
      const missingFields = ['lvSeq', 'status'].filter(field =>
        raw[field as 'lvSeq' | 'status'] === undefined)
      if (missingFields.length) return reject('missing_identity', { playerId: raw.playerId, missingFields })
      const player: PlayerTelemetry = {
        ...raw,
        sessionId: raw.sessionId ?? `legacy:${raw.playerId}`,
        status: raw.status ?? 'ACTIVE'
      }
      if (raw.sessionId === undefined) {
        this.options.onLegacyPlayer?.(roomId, raw.playerId)
      }
      incoming.add(player.playerId)
      const location = rooms.getPlayerLocation(player.playerId)
      if (location && location.roomId !== roomId && location.leavingAt === undefined) {
        next.delete(player.playerId)
        continue
      }
      const stored = telemetry.get(player.playerId)
      // These views can advance independently via lifecycle events. Every
      // authoritative generation must approve before any indexed state changes.
      const views: Array<[NonNullable<TelemetryRejection['view']>, PlayerTelemetry | undefined]> = [
        ['roster', previous?.players.get(player.playerId)],
        ['telemetry', stored?.roomId === roomId ? stored : undefined],
        ['location', location?.roomId === roomId ? { playerId: player.playerId,
          sessionId: location.sessionId, lvSeq: location.lvSeq, levelId: location.level } : undefined]
      ]
      for (const [view, before] of views) {
        const reason = generationRejection(before, player)
        if (reason) return reject(reason, { playerId: player.playerId, view,
          ...(before?.lvSeq !== undefined ? { previousLvSeq: before.lvSeq } : {}),
          ...(player.lvSeq !== undefined ? { incomingLvSeq: player.lvSeq } : {}) })
      }
      next.set(player.playerId, { ...player })
      acceptedIncoming.set(player.playerId, player)
    }
    if (next.size > room.capacity || next.size > 8) return reject('invalid_capacity')
    const physical = data.physicalPlayers ?? Math.max(next.size, data.activePlayers ?? next.size)
    if (!Number.isInteger(physical) || physical < next.size || physical > room.capacity || physical > 8 ||
        (data.activePlayers !== undefined && data.activePlayers > physical) ||
        (data.reservedSlots !== undefined && physical + data.reservedSlots > room.capacity)) return reject('invalid_capacity')

    // No awaited work below: all views commit as one synchronous accepted update.
    if (!rooms.acceptTelemetryRoom({ ...room, players: physical, version: seq })) return false
    const oldPlayers = new Map<string, PlayerTelemetry>(previous?.players)
    if (!delta) {
      for (const player of telemetry.listKnownRoom(roomId)) oldPlayers.set(player.playerId, player)
    }
    for (const before of oldPlayers.values()) if (!next.has(before.playerId)) {
      telemetry.remove(before.playerId, roomId, before.sessionId)
      rooms.removePlayer(before.playerId, roomId, before.sessionId)
    }
    // A delta carries only changed/keepalive players. Refresh exactly those
    // observations; copied roster entries retain their last real observation
    // time. A full frame is authoritative for every member in `next`.
    const refreshed = delta ? acceptedIncoming.values() : next.values()
    for (const player of refreshed) {
      telemetry.upsert(roomId, player, this.clock())
      rooms.trackPlayer(player.playerId, data.nodeId, roomId, player.levelId, player.sessionId, player.lvSeq)
    }
    this.rosters.set(roomId, { seq, players: next, nodeId: data.nodeId, ...(epoch !== undefined ? { connectedAt: epoch } : {}) })
    // Stability resets ONLY on an epoch change or a repaired gap — never on
    // ordinary join/leave churn (see noteJoin/noteLeave).
    if (!sameEpoch || this.gaps.delete(roomId)) this.options.onReset?.(roomId)
    this.options.onAccepted?.()
    return true
  }

  private reject(roomId: RoomId, rejection: TelemetryRejection): false {
    const previous = this.gaps.get(roomId)
    this.gaps.set(roomId, { since: previous?.since ?? this.clock(), dropped: (previous?.dropped ?? 0) + 1, rejection })
    this.options.onRejected?.(roomId, rejection)
    this.options.onReset?.(roomId)
    this.options.onGap?.()
    this.options.requestFull(roomId)
    return false
  }
}
