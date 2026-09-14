import { Redis } from "ioredis";
import type { NodeHealth } from "../../manager-host/NodeRegistry.js";
import type { RoomSnapshot } from "../../types/contracts.js";
import type { Logger } from "../logger/logger.js";

export interface StateStore {
  upsertRooms(rooms: RoomSnapshot[]): Promise<void>;
  deleteRoom(roomId: string): Promise<void>;
  upsertNodeHealth(health: NodeHealth): Promise<void>;
  close(): Promise<void>;
}

export class NoopStateStore implements StateStore {
  public async upsertRooms(_rooms: RoomSnapshot[]): Promise<void> {}
  public async deleteRoom(_roomId: string): Promise<void> {}
  public async upsertNodeHealth(_health: NodeHealth): Promise<void> {}
  public async close(): Promise<void> {}
}

export class RedisStateStore implements StateStore {
  private readonly redis: Redis;

  public constructor(redisUrl: string) {
    this.redis = new Redis(redisUrl, {
      lazyConnect: true,
      maxRetriesPerRequest: 2,
      enableReadyCheck: true
    });
  }

  public async connect(): Promise<void> {
    await this.redis.connect();
  }

  public async upsertRooms(rooms: RoomSnapshot[]): Promise<void> {
    if (rooms.length === 0) return;
    const pipeline = this.redis.pipeline();
    for (const room of rooms) {
      pipeline.hset(`hermes:room:${room.roomId}`, {
        nodeId: room.nodeId,
        udpHost: room.udpHost,
        udpPort: String(room.udpPort),
        players: String(room.players),
        capacity: String(room.capacity),
        region: room.region ?? "",
        version: String(room.version),
        draining: room.draining ? "1" : "0",
        updatedAt: String(Date.now())
      });
      pipeline.sadd(`hermes:node:${room.nodeId}:rooms`, room.roomId);
      pipeline.expire(`hermes:room:${room.roomId}`, 180);
    }
    await pipeline.exec();
  }

  public async deleteRoom(roomId: string): Promise<void> {
    await this.redis.del(`hermes:room:${roomId}`);
  }

  public async upsertNodeHealth(health: NodeHealth): Promise<void> {
    await this.redis.hset(`hermes:node:${health.nodeId}:health`, {
      load: String(health.load),
      capacity: String(health.capacity),
      lastSeenAt: String(health.lastSeenAt),
      draining: health.draining ? "1" : "0",
      connected: health.connected ? "1" : "0",
      protocolVersion: String(health.protocolVersion)
    });
    await this.redis.expire(`hermes:node:${health.nodeId}:health`, 30);
  }

  public async close(): Promise<void> {
    this.redis.disconnect();
  }
}

type ProjectionOperation =
  | { kind: "rooms"; rooms: RoomSnapshot[] }
  | { kind: "delete-room"; roomId: string }
  | { kind: "node-health"; health: NodeHealth };

export interface AsyncProjectionOptions {
  queueLimit: number;
  flushMs: number;
  batchSize: number;
}

export class AsyncProjectionStateStore implements StateStore {
  private readonly queue: ProjectionOperation[] = [];
  private timer: NodeJS.Timeout | undefined;
  private flushing = false;
  private closed = false;
  private dropped = 0;

  public constructor(
    private readonly delegate: StateStore,
    private readonly options: AsyncProjectionOptions,
    private readonly log: Logger
  ) {}

  public async upsertRooms(rooms: RoomSnapshot[]): Promise<void> {
    if (rooms.length === 0) return;
    this.enqueue({ kind: "rooms", rooms });
  }

  public async deleteRoom(roomId: string): Promise<void> {
    this.enqueue({ kind: "delete-room", roomId });
  }

  public async upsertNodeHealth(health: NodeHealth): Promise<void> {
    this.enqueue({ kind: "node-health", health });
  }

  public async close(): Promise<void> {
    this.closed = true;
    if (this.timer) clearTimeout(this.timer);
    while (this.queue.length > 0) {
      await this.flush();
    }
    await this.delegate.close();
  }

  private enqueue(operation: ProjectionOperation): void {
    if (this.closed) return;
    if (this.queue.length >= this.options.queueLimit) {
      this.dropped++;
      if (this.dropped % 1000 === 1) {
        this.log.warn({ dropped: this.dropped, queueSize: this.queue.length }, "redis projection queue is full");
      }
      return;
    }
    this.queue.push(operation);
    this.schedule();
  }

  private schedule(): void {
    if (this.timer || this.flushing) return;
    this.timer = setTimeout(() => {
      this.timer = undefined;
      void this.flush();
    }, this.options.flushMs);
    this.timer.unref?.();
  }

  private async flush(): Promise<void> {
    if (this.flushing) return;
    this.flushing = true;
    try {
      const batch = this.queue.splice(0, this.options.batchSize);
      const rooms: RoomSnapshot[] = [];
      const deletedRooms: string[] = [];
      const latestHealth = new Map<string, NodeHealth>();

      for (const operation of batch) {
        if (operation.kind === "rooms") rooms.push(...operation.rooms);
        if (operation.kind === "delete-room") deletedRooms.push(operation.roomId);
        if (operation.kind === "node-health") latestHealth.set(operation.health.nodeId, operation.health);
      }

      if (rooms.length > 0) await this.delegate.upsertRooms(rooms);
      for (const roomId of deletedRooms) await this.delegate.deleteRoom(roomId);
      for (const health of latestHealth.values()) await this.delegate.upsertNodeHealth(health);
    } catch (error) {
      this.log.warn({ err: error }, "redis projection flush failed");
    } finally {
      this.flushing = false;
      if (!this.closed && this.queue.length > 0) this.schedule();
    }
  }
}
