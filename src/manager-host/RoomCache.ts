import { LRUCache } from "lru-cache";
import type { NodeId, PlayerId, Region, RoomId, UnixMs } from "../types/branded.js";
import { asUnixMs } from "../types/branded.js";
import type { INodeEvent, RoomSnapshot } from "../types/contracts.js";

export interface PlayerLocation {
  playerId: PlayerId;
  nodeId: NodeId;
  roomId: RoomId;
  updatedAt: UnixMs;
  level?: number | undefined;
  /** Set when player.leave arrives but the player may be mid-migration. */
  leavingAt?: number | undefined;
}

export interface RoomCacheOptions {
  maxRooms: number;
  staleRoomMs: number;
  /**
   * After a node disconnects its rooms and player locations are kept this long
   * before being reclaimed, so a brief socket blip does not make /players/:id/room
   * return 404 mid-session. During the window the data is marked suspected and
   * excluded from allocation and migration planning.
   */
  nodeGraceMs?: number;
}

export interface CandidateRoomQuery {
  region?: Region | undefined;
  preferredFillTarget: number;
  limit: number;
}

export class RoomCache {
  private static readonly ALL_REGIONS = "__all__";
  /**
   * How long a location survives `player.leave`. It covers only the gap
   * between leave and join during a cross-room move; anything older is a
   * player that really is gone and must stop counting towards the room.
   */
  private static readonly PLAYER_LEAVE_GRACE_MS = 5_000;

  private readonly rooms: LRUCache<RoomId, RoomSnapshot>;
  private readonly roomsByNode = new Map<NodeId, Set<RoomId>>();
  private readonly playerLocations = new Map<PlayerId, PlayerLocation>();
  private readonly candidateBuckets = new Map<string, Array<Set<RoomId>>>();
  private readonly candidateIndex = new Map<RoomId, Array<{ key: string; bucket: number }>>();
  /** nodeId -> 时刻，表示该节点的连接已断开但仍处于宽限期。 */
  private readonly suspectNodes = new Map<NodeId, number>();
  private readonly nodeGraceMs: number;

  public constructor(options: RoomCacheOptions) {
    this.nodeGraceMs = options.nodeGraceMs ?? 10_000;
    this.rooms = new LRUCache<RoomId, RoomSnapshot>({
      max: options.maxRooms,
      ttl: options.staleRoomMs,
      updateAgeOnGet: false,
      allowStale: false,
      dispose: (_value, roomId) => {
        this.removeRoomIndex(roomId);
        this.removeCandidateIndex(roomId);
      }
    });
  }

  public upsertRoom(room: RoomSnapshot): void {
    const previous = this.rooms.get(room.roomId);
    if (previous && room.version < previous.version) {
      return;
    }
    if (previous && previous.nodeId !== room.nodeId) {
      this.roomsByNode.get(previous.nodeId)?.delete(room.roomId);
    }
    this.removeCandidateIndex(room.roomId);
    const next = { ...room, lastChangedAt: asUnixMs(Date.now()) };
    this.rooms.set(room.roomId, next);
    this.indexRoom(room.nodeId, room.roomId);
    this.indexCandidate(next);
  }

  public deleteRoom(roomId: RoomId): void {
    this.removeCandidateIndex(roomId);
    this.rooms.delete(roomId);
    this.removeRoomIndex(roomId);
  }

  public getRoom(roomId: RoomId): RoomSnapshot | undefined {
    return this.rooms.get(roomId);
  }

  public listRooms(): RoomSnapshot[] {
    return [...this.rooms.values()];
  }

  public listRoomsByNode(nodeId: NodeId): RoomSnapshot[] {
    const ids = this.roomsByNode.get(nodeId);
    if (!ids) return [];
    return [...ids].map((roomId) => this.rooms.get(roomId)).filter((room): room is RoomSnapshot => Boolean(room));
  }

  public deleteRoomsByNode(nodeId: NodeId): RoomSnapshot[] {
    const rooms = this.listRoomsByNode(nodeId);
    for (const room of rooms) this.deleteRoom(room.roomId);
    return rooms;
  }

  public removePlayersByNode(nodeId: NodeId): void {
    for (const [playerId, location] of this.playerLocations) {
      if (location.nodeId === nodeId) this.playerLocations.delete(playerId);
    }
  }

  /**
   * A node's socket dropped. Keep its rooms and player locations for a grace
   * window so a reconnect or a transient network blip does not erase live
   * sessions. While suspected the node is excluded from allocation and from
   * migration targets/sources.
   */
  public markNodeSuspected(nodeId: NodeId, now = Date.now()): void {
    this.suspectNodes.set(nodeId, now);
    for (const roomId of this.roomsByNode.get(nodeId) ?? []) {
      this.removeCandidateIndex(roomId);
    }
  }

  /**
   * The node reconnected (or a duplicate connection took over). Clear the
   * suspect mark and re-index its rooms for allocation.
   */
  public restoreNode(nodeId: NodeId): boolean {
    const wasSuspected = this.suspectNodes.delete(nodeId);
    if (wasSuspected) {
      for (const roomId of this.roomsByNode.get(nodeId) ?? []) {
        const room = this.rooms.get(roomId);
        if (room) this.indexCandidate(room);
      }
    }
    return wasSuspected;
  }

  public isNodeSuspected(nodeId: NodeId): boolean {
    return this.suspectNodes.has(nodeId);
  }

  /**
   * Reclaim every node whose grace window elapsed. Returns the rooms that were
   * dropped so the caller can log/count them.
   */
  public sweepSuspectedNodes(now = Date.now()): Array<{ nodeId: NodeId; rooms: RoomSnapshot[] }> {
    const expired: Array<{ nodeId: NodeId; rooms: RoomSnapshot[] }> = [];
    for (const [nodeId, suspectedAt] of this.suspectNodes) {
      if (now - suspectedAt < this.nodeGraceMs) continue;
      this.suspectNodes.delete(nodeId);
      const rooms = this.deleteRoomsByNode(nodeId);
      this.removePlayersByNode(nodeId);
      expired.push({ nodeId, rooms });
    }
    return expired;
  }

  /** Room ids belonging to nodes that are currently suspected. */
  public suspectedNodeIds(): NodeId[] {
    return [...this.suspectNodes.keys()];
  }

  public listCandidateRooms(query: CandidateRoomQuery): RoomSnapshot[] {
    const results: RoomSnapshot[] = [];
    const seen = new Set<RoomId>();
    const keys = query.region ? [query.region as string, RoomCache.ALL_REGIONS] : [RoomCache.ALL_REGIONS];
    const bucketOrder = this.bucketOrder(query.preferredFillTarget);

    for (const key of keys) {
      const buckets = this.candidateBuckets.get(key);
      if (!buckets) continue;

      for (const bucket of bucketOrder) {
        const ids = buckets[bucket];
        if (!ids) continue;

        for (const roomId of ids) {
          if (seen.has(roomId)) continue;
          const room = this.rooms.get(roomId);
          if (!room || room.draining || room.players >= room.capacity) continue;
          // Never allocate into a room whose node just dropped; it may still be
          // gone, and the player would get an unreachable UDP endpoint.
          if (this.suspectNodes.has(room.nodeId)) continue;
          seen.add(roomId);
          results.push(room);
          if (results.length >= query.limit) return results;
        }
      }
    }
    return results;
  }

  public trackPlayer(playerId: PlayerId, nodeId: NodeId, roomId: RoomId, level?: number): void {
    const previous = this.playerLocations.get(playerId);
    this.playerLocations.set(playerId, {
      playerId,
      nodeId,
      roomId,
      updatedAt: asUnixMs(Date.now()),
      // Keep a previously reported level if this update omits it (e.g. re-tracking without player payload).
      level: level ?? previous?.level
      // leavingAt is intentionally omitted: joining a new room clears the mark.
    });
  }

  public untrackPlayer(playerId: PlayerId, roomId?: RoomId): void {
    const current = this.playerLocations.get(playerId);
    if (!roomId || !current || current.roomId === roomId) {
      // Mark as leaving instead of deleting immediately: during cross-room
      // migration there is a gap between player.leave (old room) and
      // player.join (new room). Keeping the location for a short grace period
      // lets /players/:id/room still resolve the player mid-migration.
      if (current) {
        this.playerLocations.set(playerId, { ...current, leavingAt: Date.now() });
      }
    }
  }

  public getPlayerLocation(playerId: PlayerId): PlayerLocation | undefined {
    const loc = this.playerLocations.get(playerId);
    if (!loc) return undefined;
    if (this.isDeparted(loc)) {
      this.playerLocations.delete(playerId);
      return undefined;
    }
    return loc;
  }

  /**
   * Drop locations whose leave grace window has elapsed.
   *
   * `untrackPlayer` only marks a location as leaving; without a sweep the
   * entry would live forever, so an offline player kept showing up in the
   * room's player list (and kept voting on its level).
   */
  public purgeDepartedPlayers(now = Date.now()): number {
    let removed = 0;
    for (const [playerId, location] of this.playerLocations) {
      if (!this.isDeparted(location, now)) continue;
      this.playerLocations.delete(playerId);
      removed += 1;
    }
    return removed;
  }

  private isDeparted(location: PlayerLocation, now = Date.now()): boolean {
    return location.leavingAt !== undefined && now - location.leavingAt > RoomCache.PLAYER_LEAVE_GRACE_MS;
  }

  public listPlayers(roomId: RoomId, now = Date.now()): PlayerLocation[] {
    const players: PlayerLocation[] = [];
    for (const [playerId, location] of this.playerLocations) {
      if (location.roomId !== roomId) continue;
      if (this.isDeparted(location, now)) {
        this.playerLocations.delete(playerId);
        continue;
      }
      players.push(location);
    }
    return players;
  }

  /**
   * Levels currently occupied in each room, from the tracked player locations.
   * A room with an empty set has no level signal (no player reported one), which
   * allocation treats as "usable by any level" rather than "wrong level".
   */
  public levelsByRoom(now = Date.now()): Map<RoomId, Set<number>> {
    const result = new Map<RoomId, Set<number>>();
    for (const location of this.playerLocations.values()) {
      if (location.level === undefined) continue;
      if (this.isDeparted(location, now)) continue;
      const levels = result.get(location.roomId) ?? new Set<number>();
      levels.add(location.level);
      result.set(location.roomId, levels);
    }
    return result;
  }

  public applyNodeEvent(event: INodeEvent): void {
    if (event.data.rooms) {
      for (const room of event.data.rooms) this.upsertRoom(room);
    }

    if (event.event === "room.destroyed" && event.data.roomId) {
      this.deleteRoom(event.data.roomId);
      return;
    }

    if ((event.event === "room.created" || event.event === "room.updated") && event.data.roomId && event.data.port && event.data.udpHost) {
      const existing = this.rooms.get(event.data.roomId);
      this.upsertRoom({
        roomId: event.data.roomId,
        nodeId: event.data.nodeId,
        udpHost: event.data.udpHost,
        udpPort: event.data.port,
        players: existing?.players ?? 0,
        capacity: existing?.capacity ?? 8,
        region: existing?.region,
        version: event.data.version ?? existing?.version ?? 0,
        draining: existing?.draining ?? false,
        lastChangedAt: asUnixMs(Date.now())
      });
    }

    if (event.event === "player.join" && event.data.player?.playerId && event.data.roomId) {
      this.trackPlayer(event.data.player.playerId, event.data.nodeId, event.data.roomId, event.data.player.level);
      this.bumpPlayers(event.data.roomId, 1, event.data.version);
    }

    if (event.event === "player.leave" && event.data.player?.playerId && event.data.roomId) {
      this.untrackPlayer(event.data.player.playerId, event.data.roomId);
      this.bumpPlayers(event.data.roomId, -1, event.data.version);
    }
  }

  private bumpPlayers(roomId: RoomId, delta: number, version?: number): void {
    const room = this.rooms.get(roomId);
    if (!room) return;
    this.upsertRoom({
      ...room,
      players: Math.max(0, Math.min(room.capacity, room.players + delta)),
      version: version ?? room.version + 1,
      lastChangedAt: asUnixMs(Date.now())
    });
  }

  private indexRoom(nodeId: NodeId, roomId: RoomId): void {
    let bucket = this.roomsByNode.get(nodeId);
    if (!bucket) {
      bucket = new Set<RoomId>();
      this.roomsByNode.set(nodeId, bucket);
    }
    bucket.add(roomId);
  }

  private removeRoomIndex(roomId: RoomId): void {
    for (const bucket of this.roomsByNode.values()) {
      bucket.delete(roomId);
    }
  }

  private indexCandidate(room: RoomSnapshot): void {
    if (room.draining || room.players >= room.capacity) return;
    const bucket = Math.max(0, Math.min(room.capacity - 1, room.players));
    const keys = [RoomCache.ALL_REGIONS];
    if (room.region) keys.push(room.region as string);

    const records: Array<{ key: string; bucket: number }> = [];
    for (const key of keys) {
      const buckets = this.getCandidateBuckets(key);
      buckets[bucket]?.add(room.roomId);
      records.push({ key, bucket });
    }
    this.candidateIndex.set(room.roomId, records);
  }

  private removeCandidateIndex(roomId: RoomId): void {
    const records = this.candidateIndex.get(roomId);
    if (!records) return;
    for (const record of records) {
      this.candidateBuckets.get(record.key)?.[record.bucket]?.delete(roomId);
    }
    this.candidateIndex.delete(roomId);
  }

  private getCandidateBuckets(key: string): Array<Set<RoomId>> {
    let buckets = this.candidateBuckets.get(key);
    if (!buckets) {
      buckets = Array.from({ length: 8 }, () => new Set<RoomId>());
      this.candidateBuckets.set(key, buckets);
    }
    return buckets;
  }

  private bucketOrder(preferredFillTarget: number): number[] {
    const preferredCurrentPlayers = Math.max(0, Math.min(7, preferredFillTarget - 1));
    const order: number[] = [];
    for (let players = preferredCurrentPlayers; players >= 0; players--) order.push(players);
    for (let players = preferredCurrentPlayers + 1; players <= 7; players++) order.push(players);
    return order;
  }
}
