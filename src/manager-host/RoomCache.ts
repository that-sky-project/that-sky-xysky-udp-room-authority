import { LRUCache } from 'lru-cache'
import type {
  NodeId,
  PlayerId,
  Region,
  RoomId,
  UnixMs
} from '../types/branded.js'
import { asUnixMs } from '../types/branded.js'
import type { INodeEvent, RoomSnapshot } from '../types/contracts.js'
export interface PlayerLocation {
  playerId: PlayerId
  nodeId: NodeId
  roomId: RoomId
  updatedAt: UnixMs
  level?: number | undefined
  leavingAt?: number | undefined
}
export interface RoomCacheOptions {
  maxRooms: number
  staleRoomMs: number
  nodeGraceMs?: number
}
export interface CandidateRoomQuery {
  region?: Region | undefined
  preferredFillTarget: number
  limit: number
}
export class RoomCache {
  private static readonly ALL_REGIONS = '__all__'
  private static readonly PLAYER_LEAVE_GRACE_MS = 5_000
  private readonly rooms: LRUCache<RoomId, RoomSnapshot>
  private readonly roomsByNode = new Map<NodeId, Set<RoomId>>()
  private readonly playerLocations = new Map<PlayerId, PlayerLocation>()
  private readonly awaitingTelemetry = new Set<RoomId>()
  private readonly candidateBuckets = new Map<string, Array<Set<RoomId>>>()
  private readonly candidateIndex = new Map<
    RoomId,
    Array<{ key: string; bucket: number }>
  >()
  private readonly suspectNodes = new Map<NodeId, number>()
  private readonly nodeGraceMs: number
  public constructor (options: RoomCacheOptions) {
    this.nodeGraceMs = options.nodeGraceMs ?? 10_000
    this.rooms = new LRUCache<RoomId, RoomSnapshot>({
      max: options.maxRooms,
      ttl: options.staleRoomMs,
      updateAgeOnGet: false,
      allowStale: false,
      dispose: (_value, roomId) => {
        this.removeRoomIndex(roomId)
        this.removeCandidateIndex(roomId)
      }
    })
  }
  public upsertRoom (room: RoomSnapshot): boolean {
    const previous = this.rooms.get(room.roomId)
    if (
      previous &&
      previous.nodeId === room.nodeId &&
      room.version < previous.version
    ) {
      return false
    }
    if (previous && previous.nodeId !== room.nodeId) {
      this.roomsByNode.get(previous.nodeId)?.delete(room.roomId)
    }
    this.removeCandidateIndex(room.roomId)
    const next = { ...room, lastChangedAt: asUnixMs(Date.now()) }
    this.rooms.set(room.roomId, next)
    this.indexRoom(room.nodeId, room.roomId)
    this.indexCandidate(next)
    return true
  }
  public deleteRoom (roomId: RoomId): void {
    this.removeCandidateIndex(roomId)
    this.rooms.delete(roomId)
    this.removeRoomIndex(roomId)
    this.awaitingTelemetry.delete(roomId)
  }
  public getRoom (roomId: RoomId): RoomSnapshot | undefined {
    return this.rooms.get(roomId)
  }
  public listRooms (): RoomSnapshot[] {
    return [...this.rooms.values()]
  }
  public listRoomsByNode (nodeId: NodeId): RoomSnapshot[] {
    const ids = this.roomsByNode.get(nodeId)
    if (!ids) return []
    return [...ids]
      .map(roomId => this.rooms.get(roomId))
      .filter((room): room is RoomSnapshot => Boolean(room))
  }
  public deleteRoomsByNode (nodeId: NodeId): RoomSnapshot[] {
    const rooms = this.listRoomsByNode(nodeId)
    for (const room of rooms) this.deleteRoom(room.roomId)
    return rooms
  }
  public removePlayersByNode (nodeId: NodeId): void {
    for (const [playerId, location] of this.playerLocations) {
      if (location.nodeId === nodeId) this.playerLocations.delete(playerId)
    }
  }
  public markNodeSuspected (nodeId: NodeId, now = Date.now()): void {
    this.suspectNodes.set(nodeId, now)
    for (const roomId of this.roomsByNode.get(nodeId) ?? []) {
      this.removeCandidateIndex(roomId)
    }
  }
  public restoreNode (nodeId: NodeId): boolean {
    const wasSuspected = this.suspectNodes.delete(nodeId)
    if (wasSuspected) {
      for (const roomId of this.roomsByNode.get(nodeId) ?? []) {
        const room = this.rooms.get(roomId)
        if (room) this.indexCandidate(room)
      }
    }
    return wasSuspected
  }
  public isNodeSuspected (nodeId: NodeId): boolean {
    return this.suspectNodes.has(nodeId)
  }
  public sweepSuspectedNodes (
    now = Date.now()
  ): Array<{ nodeId: NodeId; rooms: RoomSnapshot[] }> {
    const expired: Array<{ nodeId: NodeId; rooms: RoomSnapshot[] }> = []
    for (const [nodeId, suspectedAt] of this.suspectNodes) {
      if (now - suspectedAt < this.nodeGraceMs) continue
      this.suspectNodes.delete(nodeId)
      const rooms = this.deleteRoomsByNode(nodeId)
      this.removePlayersByNode(nodeId)
      expired.push({ nodeId, rooms })
    }
    return expired
  }
  public suspectedNodeIds (): NodeId[] {
    return [...this.suspectNodes.keys()]
  }
  public listCandidateRooms (query: CandidateRoomQuery): RoomSnapshot[] {
    const results: RoomSnapshot[] = []
    const seen = new Set<RoomId>()
    const keys = query.region
      ? [query.region as string, RoomCache.ALL_REGIONS]
      : [RoomCache.ALL_REGIONS]
    const bucketOrder = this.bucketOrder(query.preferredFillTarget)
    for (const key of keys) {
      const buckets = this.candidateBuckets.get(key)
      if (!buckets) continue
      for (const bucket of bucketOrder) {
        const ids = buckets[bucket]
        if (!ids) continue
        for (const roomId of ids) {
          if (seen.has(roomId)) continue
          const room = this.rooms.get(roomId)
          if (!room || room.draining || room.players >= room.capacity) continue
          if (this.suspectNodes.has(room.nodeId)) continue
          seen.add(roomId)
          results.push(room)
          if (results.length >= query.limit) return results
        }
      }
    }
    return results
  }
  public trackPlayer (
    playerId: PlayerId,
    nodeId: NodeId,
    roomId: RoomId,
    level?: number
  ): void {
    const previous = this.playerLocations.get(playerId)
    this.playerLocations.set(playerId, {
      playerId,
      nodeId,
      roomId,
      updatedAt: asUnixMs(Date.now()),
      level: level !== undefined && level > 0 ? level : previous?.level
    })
  }
  public untrackPlayer (playerId: PlayerId, roomId?: RoomId): void {
    const current = this.playerLocations.get(playerId)
    if (!roomId || !current || current.roomId === roomId) {
      if (current) {
        this.playerLocations.set(playerId, {
          ...current,
          leavingAt: Date.now()
        })
      }
    }
  }
  public getPlayerLocation (playerId: PlayerId): PlayerLocation | undefined {
    const loc = this.playerLocations.get(playerId)
    if (!loc) return undefined
    if (this.isDeparted(loc)) {
      this.playerLocations.delete(playerId)
      return undefined
    }
    return loc
  }
  public purgeDepartedPlayers (now = Date.now()): number {
    let removed = 0
    for (const [playerId, location] of this.playerLocations) {
      if (!this.isDeparted(location, now)) continue
      this.playerLocations.delete(playerId)
      removed += 1
    }
    return removed
  }
  private isDeparted (location: PlayerLocation, now = Date.now()): boolean {
    return (
      location.leavingAt !== undefined &&
      now - location.leavingAt > RoomCache.PLAYER_LEAVE_GRACE_MS
    )
  }
  public listPlayers (roomId: RoomId, now = Date.now()): PlayerLocation[] {
    const players: PlayerLocation[] = []
    for (const [playerId, location] of this.playerLocations) {
      if (location.roomId !== roomId) continue
      if (this.isDeparted(location, now)) {
        this.playerLocations.delete(playerId)
        continue
      }
      players.push(location)
    }
    return players
  }
  public levelsByRoom (now = Date.now()): Map<RoomId, Set<number>> {
    const result = new Map<RoomId, Set<number>>()
    for (const location of this.playerLocations.values()) {
      if (location.level === undefined) continue
      if (this.isDeparted(location, now)) continue
      const levels = result.get(location.roomId) ?? new Set<number>()
      levels.add(location.level)
      result.set(location.roomId, levels)
    }
    return result
  }
  public applyNodeEvent (event: INodeEvent): void {
    if (event.data.rooms) {
      for (const room of event.data.rooms) this.upsertRoom(room)
    }
    if (event.event === 'room.telemetry' && event.data.roomId) {
      this.awaitingTelemetry.delete(event.data.roomId)
    }
    if (event.event === 'room.destroyed' && event.data.roomId) {
      this.deleteRoom(event.data.roomId)
      return
    }
    if (
      (event.event === 'room.created' || event.event === 'room.updated') &&
      event.data.roomId &&
      event.data.port &&
      event.data.udpHost
    ) {
      const existing = this.rooms.get(event.data.roomId)
      const capacity =
        typeof event.data.capacity === 'number' && event.data.capacity > 0
          ? Math.min(8, event.data.capacity)
          : existing?.capacity ?? 8
      const reported =
        typeof event.data.players === 'number' ? event.data.players : undefined
      this.upsertRoom({
        roomId: event.data.roomId,
        nodeId: event.data.nodeId,
        udpHost: event.data.udpHost,
        udpPort: event.data.port,
        players: Math.min(capacity, reported ?? existing?.players ?? 0),
        capacity,
        region: existing?.region,
        version: event.data.version ?? existing?.version ?? 0,
        draining: existing?.draining ?? false,
        lastChangedAt: asUnixMs(Date.now())
      })
      this.awaitingTelemetry.add(event.data.roomId)
    }
    if (
      event.event === 'player.join' &&
      event.data.player?.playerId &&
      event.data.roomId
    ) {
      this.trackPlayer(
        event.data.player.playerId,
        event.data.nodeId,
        event.data.roomId,
        event.data.player.level
      )
      this.bumpPlayers(event.data.roomId, 1, event.data.version)
    }
    if (
      event.event === 'player.leave' &&
      event.data.player?.playerId &&
      event.data.roomId
    ) {
      this.untrackPlayer(event.data.player.playerId, event.data.roomId)
      this.bumpPlayers(event.data.roomId, -1, event.data.version)
    }
  }
  private bumpPlayers (roomId: RoomId, delta: number, version?: number): void {
    if (this.awaitingTelemetry.has(roomId)) return
    const room = this.rooms.get(roomId)
    if (!room) return
    this.upsertRoom({
      ...room,
      players: Math.max(0, Math.min(room.capacity, room.players + delta)),
      version: version ?? room.version,
      lastChangedAt: asUnixMs(Date.now())
    })
  }
  private indexRoom (nodeId: NodeId, roomId: RoomId): void {
    let bucket = this.roomsByNode.get(nodeId)
    if (!bucket) {
      bucket = new Set<RoomId>()
      this.roomsByNode.set(nodeId, bucket)
    }
    bucket.add(roomId)
  }
  private removeRoomIndex (roomId: RoomId): void {
    for (const bucket of this.roomsByNode.values()) {
      bucket.delete(roomId)
    }
  }
  private indexCandidate (room: RoomSnapshot): void {
    if (room.draining || room.players >= room.capacity) return
    const bucket = Math.max(0, Math.min(room.capacity - 1, room.players))
    const keys = [RoomCache.ALL_REGIONS]
    if (room.region) keys.push(room.region as string)
    const records: Array<{ key: string; bucket: number }> = []
    for (const key of keys) {
      const buckets = this.getCandidateBuckets(key)
      buckets[bucket]?.add(room.roomId)
      records.push({ key, bucket })
    }
    this.candidateIndex.set(room.roomId, records)
  }
  private removeCandidateIndex (roomId: RoomId): void {
    const records = this.candidateIndex.get(roomId)
    if (!records) return
    for (const record of records) {
      this.candidateBuckets.get(record.key)?.[record.bucket]?.delete(roomId)
    }
    this.candidateIndex.delete(roomId)
  }
  private getCandidateBuckets (key: string): Array<Set<RoomId>> {
    let buckets = this.candidateBuckets.get(key)
    if (!buckets) {
      buckets = Array.from({ length: 8 }, () => new Set<RoomId>())
      this.candidateBuckets.set(key, buckets)
    }
    return buckets
  }
  private bucketOrder (preferredFillTarget: number): number[] {
    const preferredCurrentPlayers = Math.max(
      0,
      Math.min(7, preferredFillTarget - 1)
    )
    const order: number[] = []
    for (let players = preferredCurrentPlayers; players >= 0; players--)
      order.push(players)
    for (let players = preferredCurrentPlayers + 1; players <= 7; players++)
      order.push(players)
    return order
  }
}
