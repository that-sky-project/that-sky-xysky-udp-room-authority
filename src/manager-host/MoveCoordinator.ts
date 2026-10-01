import type { NodeId, PlayerId, RoomId } from "../types/branded.js";
import { asProtocolVersion, asUnixMs } from "../types/branded.js";
import type { INodeEvent, RoomSnapshot } from "../types/contracts.js";
import { ErrorCode } from "../types/errors.js";
import type { CrossCoordinator } from "./CrossCoordinator.js";
import type { NodeRegistry } from "./NodeRegistry.js";
import type { PendingRequestMap } from "./PendingRequestMap.js";
import type { RoomCache } from "./RoomCache.js";
import { MovePlanner, type MovePlan, type MovePlannerOptions } from "./MovePlanner.js";
import { TelemetryStore } from "./TelemetryStore.js";

interface MoveTransaction extends MovePlan { moveId: string; status: "prepared" | "committed" | "completed" | "failed"; createdAt: number; joinedPlayers: string[]; leftPlayers: string[]; }

export interface MoveCoordinatorOptions {
  moveBudgetPerCycle?: number;
  moveTransactionTimeoutMs?: number;
  roomCooldownMs?: number;
  stateEmaHalfLifeMs?: number;
  planner?: Partial<MovePlannerOptions>;
}

export class MoveCoordinator {
  public readonly telemetry: TelemetryStore;
  private readonly planner: MovePlanner;
  private readonly transactions = new Map<string, MoveTransaction>();
  private readonly roomCooldownUntil = new Map<string, number>();
  private readonly pendingCancels = new Map<string, { roomId: RoomId; moveId: string; playerIds: string[] }>();
  private lastPlanAt = 0;

  public constructor(private readonly rooms: RoomCache, private readonly nodes: NodeRegistry, private readonly pending: PendingRequestMap, private readonly cross: CrossCoordinator, private readonly options: MoveCoordinatorOptions = {}) {
    this.telemetry = new TelemetryStore({ stateEmaHalfLifeMs: options.stateEmaHalfLifeMs ?? 3_000 });
    this.planner = new MovePlanner(options.planner ?? {});
  }

  public ingest(event: INodeEvent): void {
    if (event.event === "room.telemetry" && event.data.roomId) {
      const telemetryPlayers = Array.isArray(event.data.players) ? event.data.players : [];
      for (const player of telemetryPlayers) {
        this.telemetry.upsert(event.data.roomId, player);
      }
      const room = this.rooms.getRoom(event.data.roomId);
      if (room) {
        const count = Array.isArray(event.data.players) ? event.data.players.length : typeof event.data.players === "number" ? event.data.players : room.players;
        this.rooms.upsertRoom({ ...room, players: Math.min(room.capacity, count), version: room.version });
      }
    }
    if ((event.event === "player.join" || event.event === "player.leave") && event.data.player?.playerId) {
      if (event.event === "player.leave") {
        for (const tx of this.transactions.values()) {
          if (tx.status === "committed" && tx.sourceRoomId === event.data.roomId && tx.playerIds.includes(event.data.player.playerId as string)) {
            if (!tx.leftPlayers.includes(event.data.player.playerId as string)) tx.leftPlayers.push(event.data.player.playerId as string);
            if (tx.leftPlayers.length === tx.playerIds.length && tx.joinedPlayers.length === tx.playerIds.length) { tx.status = "completed"; this.telemetry.releaseReservation(tx.targetRoomId as RoomId, tx.moveId); }
          }
        }
        const current = this.telemetry.get(event.data.player.playerId);
        if (!current || current.roomId === event.data.roomId) this.telemetry.remove(event.data.player.playerId);
      } else if (event.data.roomId) this.telemetry.upsert(event.data.roomId, { playerId: event.data.player.playerId });
      if (event.event === "player.join" && event.data.roomId) {
        for (const tx of this.transactions.values()) {
          if (tx.status === "committed" && tx.targetRoomId === event.data.roomId && tx.playerIds.includes(event.data.player.playerId as string)) {
            if (!tx.joinedPlayers.includes(event.data.player.playerId as string)) tx.joinedPlayers.push(event.data.player.playerId as string);
            if (tx.leftPlayers.length === tx.playerIds.length && tx.joinedPlayers.length === tx.playerIds.length) { tx.status = "completed"; this.telemetry.releaseReservation(tx.targetRoomId as RoomId, tx.moveId); }
          }
        }
      }
    }
    if (event.event === "move.result" && event.data.moveId) {
      const tx = this.transactions.get(event.data.moveId);
      if (tx && event.data.accepted === false) {
        const failedIds = event.data.failedPlayerIds?.length ? event.data.failedPlayerIds : tx.playerIds;
        tx.playerIds = tx.playerIds.filter((id) => !failedIds.includes(id));
        this.telemetry.releaseReservationSlots(tx.targetRoomId as RoomId, tx.moveId, failedIds.length);
        if (tx.playerIds.length === 0) tx.status = "failed";
        if (failedIds.length) this.queueCancel(tx.targetRoomId as RoomId, tx.moveId, failedIds);
      }
    }
  }

  public async planAndExecute(signal?: AbortSignal, now = Date.now()): Promise<MoveTransaction[]> {
    if (now - this.lastPlanAt < 2_000) return [...this.transactions.values()];
    this.lastPlanAt = now;
    this.drainPendingCancels();
    for (const tx of this.transactions.values()) {
      if ((tx.status === "prepared" || tx.status === "committed") && now - tx.createdAt > (this.options.moveTransactionTimeoutMs ?? 15_000)) {
        tx.status = "failed";
        this.telemetry.releaseReservation(tx.targetRoomId as RoomId, tx.moveId);
        this.queueCancel(tx.targetRoomId as RoomId, tx.moveId, tx.playerIds);
      }
    }
    const rooms = this.rooms.listRooms();
    const byRoom = new Map(rooms.map((room) => [room.roomId as string, this.telemetry.listRoom(room.roomId, now)]));
    this.planner.calibrate(rooms, byRoom, now);
    const plans = this.planner.plan(rooms, byRoom, now);
    const usedSources = new Set<string>(); const usedTargets = new Set<string>(); let moveBudget = this.options.moveBudgetPerCycle ?? 8;
    for (const plan of plans) {
      if (moveBudget < plan.playerIds.length || usedSources.has(plan.sourceRoomId) || usedTargets.has(plan.targetRoomId) || this.inCooldown(plan.sourceRoomId, now) || this.inCooldown(plan.targetRoomId, now)) continue;
      const source = this.rooms.getRoom(plan.sourceRoomId as RoomId);
      const target = this.rooms.getRoom(plan.targetRoomId as RoomId);
      if (!source || !target || this.transactionsForPlayer(plan.playerIds).length > 0) continue;
      const moveId = `move-${now}-${plan.playerIds.join("-")}`;
      if (!this.telemetry.tryReserve(target.roomId, moveId, plan.playerIds.length, target.capacity, target.players, this.options.moveTransactionTimeoutMs ?? 15_000, now)) continue;
      const tx: MoveTransaction = { ...plan, moveId, status: "prepared", createdAt: now, joinedPlayers: [], leftPlayers: [] };
      this.transactions.set(moveId, tx);
      try {
        const targetTransport = this.nodes.getTransport(target.nodeId);
        if (!targetTransport || !this.nodes.isAvailableForCommand(target.nodeId)) throw new Error("target node unavailable");
        const ack = await this.pending.send({ v: asProtocolVersion(1), cmd: "move.prepare", data: { moveId, roomId: target.roomId, playerIds: plan.playerIds, expectedVersion: target.version, ttlMs: 10_000 } }, (command) => targetTransport.send(command), signal);
        if (ack.code !== ErrorCode.OK) throw new Error(ack.message ?? "target rejected move");
        const first = plan.playerIds[0];
        if (!first) throw new Error("empty move plan");
        await this.cross.redirectPlayers({ playerIds: plan.playerIds.map((id) => id as PlayerId), moveId, playerId: first as PlayerId, fromRoomId: source.roomId, to: { roomId: target.roomId, nodeId: target.nodeId, udpHost: target.udpHost, udpPort: target.udpPort, expiresAt: asUnixMs(now + 10_000) }, reason: plan.reason === "low_activity_merge" ? "merge" : "capacity-rebalance" }, signal);
        this.transactions.set(moveId, { ...tx, status: "committed" });
        this.telemetry.markMigrated(plan.playerIds.map((id) => id as PlayerId), now);
        usedSources.add(plan.sourceRoomId); usedTargets.add(plan.targetRoomId); moveBudget -= plan.playerIds.length;
        const cooldownUntil = now + (this.options.roomCooldownMs ?? 20_000);
        this.roomCooldownUntil.set(plan.sourceRoomId, cooldownUntil); this.roomCooldownUntil.set(plan.targetRoomId, cooldownUntil);
      } catch {
        this.queueCancel(target.roomId, moveId, plan.playerIds);
        this.telemetry.releaseReservation(target.roomId, moveId);
        this.transactions.set(moveId, { ...tx, status: "failed" });
      }
    }
    return [...this.transactions.values()];
  }

  public list(): MoveTransaction[] { return [...this.transactions.values()]; }
  public async manualMove(input: { sourceRoomId: string; targetRoomId: string; playerIds: string[]; reason?: string }, signal?: AbortSignal, now = Date.now()): Promise<MoveTransaction> {
    const playerIds = [...new Set(input.playerIds.map(String).filter(Boolean))];
    if (!input.sourceRoomId || !input.targetRoomId || input.sourceRoomId === input.targetRoomId || playerIds.length === 0 || playerIds.length > 7) {
      throw new Error("invalid debug move request");
    }
    if (this.transactionsForPlayer(playerIds).length > 0) throw new Error("one or more players already have a pending move");
    const source = this.rooms.getRoom(input.sourceRoomId as RoomId);
    const target = this.rooms.getRoom(input.targetRoomId as RoomId);
    if (!source || !target) throw new Error("source or target room not found");
    const locations = playerIds.map((id) => this.rooms.getPlayerLocation(id as PlayerId));
    if (locations.some((location) => !location || location.roomId !== source.roomId)) throw new Error("one or more players are not in the source room");
    if (!this.telemetry.tryReserve(target.roomId, `debug-${now}-${playerIds.join("-")}`, playerIds.length, target.capacity, target.players, this.options.moveTransactionTimeoutMs ?? 15_000, now)) {
      throw new Error("target room has insufficient capacity");
    }
    const moveId = `debug-move-${now}-${playerIds.join("-")}`;
    const tx: MoveTransaction = {
      playerIds,
      sourceRoomId: source.roomId,
      targetRoomId: target.roomId,
      gain: 0,
      reason: input.reason === "merge" ? "low_activity_merge" : "player_rebalance",
      moveId,
      status: "prepared",
      createdAt: now,
      joinedPlayers: [],
      leftPlayers: []
    };
    this.telemetry.releaseReservation(target.roomId, `debug-${now}-${playerIds.join("-")}`);
    if (!this.telemetry.tryReserve(target.roomId, moveId, playerIds.length, target.capacity, target.players, this.options.moveTransactionTimeoutMs ?? 15_000, now)) throw new Error("target room reservation race");
    this.transactions.set(moveId, tx);
    try {
      const targetTransport = this.nodes.getTransport(target.nodeId);
      if (!targetTransport || !this.nodes.isAvailableForCommand(target.nodeId)) throw new Error("target node unavailable");
      const ack = await this.pending.send({ v: asProtocolVersion(1), cmd: "move.prepare", data: { moveId, roomId: target.roomId, playerIds, expectedVersion: target.version, ttlMs: this.options.moveTransactionTimeoutMs ?? 15_000 } }, (command) => targetTransport.send(command), signal);
      if (ack.code !== ErrorCode.OK) throw new Error(ack.message ?? "target rejected move");
      const first = playerIds[0];
      if (!first) throw new Error("empty debug move");
      await this.cross.redirectPlayers({ playerIds: playerIds.map((id) => id as PlayerId), moveId, playerId: first as PlayerId, fromRoomId: source.roomId, to: { roomId: target.roomId, nodeId: target.nodeId, udpHost: target.udpHost, udpPort: target.udpPort, expiresAt: asUnixMs(now + (this.options.moveTransactionTimeoutMs ?? 15_000)) }, reason: tx.reason === "low_activity_merge" ? "merge" : "capacity-rebalance" }, signal);
      const committed = { ...tx, status: "committed" as const };
      this.transactions.set(moveId, committed);
      this.telemetry.markMigrated(playerIds.map((id) => id as PlayerId), now);
      return committed;
    } catch (error) {
      this.telemetry.releaseReservation(target.roomId, moveId);
      this.transactions.set(moveId, { ...tx, status: "failed" });
      this.queueCancel(target.roomId, moveId, playerIds);
      throw error;
    }
  }
  public activitySnapshot(now = Date.now()): Array<ReturnType<MovePlanner["activity"]>> {
    return this.rooms.listRooms().map((room) => this.planner.activity(room, this.telemetry.listRoom(room.roomId, now), now));
  }
  private transactionsForPlayer(playerIds: string[]): MoveTransaction[] { return [...this.transactions.values()].filter((tx) => tx.status === "prepared" || tx.status === "committed").filter((tx) => tx.playerIds.some((id) => playerIds.includes(id))); }

  private inCooldown(roomId: string, now: number): boolean {
    const until = this.roomCooldownUntil.get(roomId);
    if (until === undefined) return false;
    if (until <= now) { this.roomCooldownUntil.delete(roomId); return false; }
    return true;
  }

  private queueCancel(roomId: RoomId, moveId: string, playerIds: string[]): void {
    this.pendingCancels.set(moveId, { roomId, moveId, playerIds });
    this.flushCancel(moveId);
  }

  private flushCancel(moveId: string): void {
    const entry = this.pendingCancels.get(moveId);
    if (!entry) return;
    const room = this.rooms.getRoom(entry.roomId);
    const transport = room ? this.nodes.getTransport(room.nodeId) : undefined;
    if (!transport?.writable) return; // stays queued; retried by drainPendingCancels when the node is reachable
    void this.pending.send({ v: asProtocolVersion(1), cmd: "move.cancel", data: { moveId: entry.moveId, playerIds: entry.playerIds } }, (command) => transport.send(command))
      .then(() => { this.pendingCancels.delete(entry.moveId); })
      .catch(() => undefined);
  }

  private drainPendingCancels(): void {
    for (const moveId of [...this.pendingCancels.keys()]) this.flushCancel(moveId);
  }
}
