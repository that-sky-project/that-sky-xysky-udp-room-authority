import type { PlayerId, RoomId } from "../types/branded.js";
import { asProtocolVersion, asUnixMs } from "../types/branded.js";
import type { INodeEvent } from "../types/contracts.js";
import { ErrorCode } from "../types/errors.js";
import type { Logger } from "../infra/logger/logger.js";
import type { CrossCoordinator } from "./CrossCoordinator.js";
import type { NodeRegistry } from "./NodeRegistry.js";
import type { PendingRequestMap } from "./PendingRequestMap.js";
import type { RoomCache } from "./RoomCache.js";
import { MovePlanner, type MovePlan, type MovePlannerOptions } from "./MovePlanner.js";
import { TelemetryStore } from "./TelemetryStore.js";
import { LevelIsolationPlanner, type LevelIsolationPlan, type LevelIsolationOptions } from "./LevelIsolationPlanner.js";

interface MoveTransaction extends MovePlan { moveId: string; status: "prepared" | "committed" | "completed" | "failed"; createdAt: number; joinedPlayers: string[]; leftPlayers: string[]; }
type LevelMoveTransaction = Omit<MoveTransaction, keyof MovePlan> & LevelIsolationPlan;

const TRANSACTION_RETENTION_MS = 5 * 60_000;

export interface MoveCoordinatorOptions {
  moveBudgetPerCycle?: number;
  moveTransactionTimeoutMs?: number;
  roomCooldownMs?: number;
  stateEmaHalfLifeMs?: number;
  planner?: Partial<MovePlannerOptions>;
  levelIsolation?: Partial<LevelIsolationOptions>;
}

export class MoveCoordinator {
  public readonly telemetry: TelemetryStore;
  private readonly planner: MovePlanner;
  private readonly levelIsolationPlanner: LevelIsolationPlanner;
  private readonly transactions = new Map<string, MoveTransaction | LevelMoveTransaction>();
  private readonly roomCooldownUntil = new Map<string, number>();
  private readonly pendingCancels = new Map<string, { roomId: RoomId; moveId: string; playerIds: string[] }>();
  private lastPlanAt = 0;
  private planning = false;

  public constructor(
    private readonly rooms: RoomCache,
    private readonly nodes: NodeRegistry,
    private readonly pending: PendingRequestMap,
    private readonly cross: CrossCoordinator,
    private readonly options: MoveCoordinatorOptions = {},
    private readonly log?: Logger
  ) {
    this.telemetry = new TelemetryStore({ stateEmaHalfLifeMs: options.stateEmaHalfLifeMs ?? 3_000 });
    this.planner = new MovePlanner(options.planner ?? {});
    this.levelIsolationPlanner = new LevelIsolationPlanner(options.levelIsolation ?? {});
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

  public async planAndExecute(signal?: AbortSignal, now = Date.now()): Promise<Array<MoveTransaction | LevelMoveTransaction>> {
    if (now - this.lastPlanAt < 2_000) return [...this.transactions.values()];
    // Nodes push telemetry ~1/s per room, so several events can land while a
    // cycle is still awaiting node acks. Without this guard the same rooms are
    // planned twice and the second pass fights the first one's reservations.
    if (this.planning) return [...this.transactions.values()];
    this.planning = true;
    this.lastPlanAt = now;
    try {
      return await this.runCycle(signal, now);
    } finally {
      this.planning = false;
    }
  }

  private async runCycle(signal: AbortSignal | undefined, now: number): Promise<Array<MoveTransaction | LevelMoveTransaction>> {
    this.drainPendingCancels();
    this.expireTransactions(now);

    const rooms = this.rooms.listRooms();
    const byRoom = new Map(rooms.map((room) => [room.roomId as string, this.telemetry.listRoom(room.roomId, now)]));
    this.planner.calibrate(rooms, byRoom, now);

    // One level per room is the hard rule, so it runs before the activity-based
    // planner and consumes the budget first.
    const isolationPlans = this.levelIsolationPlanner.plan(rooms, this.telemetry, now, this.log);

    const plans = this.planner.plan(rooms, byRoom, now);
    const usedTargets = new Set<string>(); let moveBudget = this.options.moveBudgetPerCycle ?? 8;

    const usedIsolationRooms: string[] = [];
    for (const isolationPlan of isolationPlans) {
      // A room with three or more levels produces one plan per stray level, so
      // the source room is allowed to repeat; only the destination must be
      // unique, since one target room cannot absorb two concurrent moves.
      if (moveBudget < isolationPlan.playerIds.length || usedTargets.has(isolationPlan.targetRoomId)) continue;
      const executed = await this.executeLevelIsolation(isolationPlan, now, signal, usedIsolationRooms);
      if (executed) {
        usedTargets.add(isolationPlan.targetRoomId);
        moveBudget -= isolationPlan.playerIds.length;
      }
    }
    if (usedIsolationRooms.length > 0) {
      const cooldownUntil = now + (this.options.roomCooldownMs ?? 20_000);
      for (const roomId of usedIsolationRooms) this.roomCooldownUntil.set(roomId, cooldownUntil);
    }

    for (const plan of plans) {
      if (moveBudget < plan.playerIds.length || usedTargets.has(plan.targetRoomId) || this.inCooldown(plan.sourceRoomId, now) || this.inCooldown(plan.targetRoomId, now)) continue;
      if (isolationPlans.some((isolationPlan) => isolationPlan.sourceRoomId === plan.sourceRoomId)) continue;
      const source = this.rooms.getRoom(plan.sourceRoomId as RoomId);
      const target = this.rooms.getRoom(plan.targetRoomId as RoomId);
      if (!source || !target || this.transactionsForPlayer(plan.playerIds).length > 0) continue;
      const moveId = `move-${now}-${plan.playerIds.join("-")}`;
      if (!this.telemetry.tryReserve(target.roomId, moveId, plan.playerIds.length, target.capacity, target.players, this.options.moveTransactionTimeoutMs ?? 15_000, now)) {
        this.log?.debug({ plan, targetPlayers: target.players, targetCapacity: target.capacity }, "move plan skipped: no capacity");
        continue;
      }
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
        usedTargets.add(plan.targetRoomId); moveBudget -= plan.playerIds.length;
        const cooldownUntil = now + (this.options.roomCooldownMs ?? 20_000);
        this.roomCooldownUntil.set(plan.sourceRoomId, cooldownUntil); this.roomCooldownUntil.set(plan.targetRoomId, cooldownUntil);
        this.log?.info({ moveId, playerIds: plan.playerIds, sourceRoomId: plan.sourceRoomId, targetRoomId: plan.targetRoomId, reason: plan.reason, gain: plan.gain }, "move committed");
      } catch (error) {
        this.queueCancel(target.roomId, moveId, plan.playerIds);
        this.telemetry.releaseReservation(target.roomId, moveId);
        this.transactions.set(moveId, { ...tx, status: "failed" });
        this.log?.warn({ err: error, moveId, playerIds: plan.playerIds, sourceRoomId: plan.sourceRoomId, targetRoomId: plan.targetRoomId }, "move failed");
      }
    }
    return [...this.transactions.values()];
  }

  private expireTransactions(now: number): void {
    const timeoutMs = this.options.moveTransactionTimeoutMs ?? 15_000;
    for (const [moveId, tx] of this.transactions) {
      if (tx.status === "prepared") {
        // Not yet sent to the client; safe to abandon and retry later.
        if (now - tx.createdAt > timeoutMs) {
          tx.status = "failed";
          this.telemetry.releaseReservation(tx.targetRoomId as RoomId, tx.moveId);
          this.queueCancel(tx.targetRoomId as RoomId, tx.moveId, tx.playerIds);
          this.log?.warn({ moveId, playerIds: tx.playerIds, sourceRoomId: tx.sourceRoomId, targetRoomId: tx.targetRoomId }, "prepared move timed out");
        }
        continue;
      }
      if (tx.status === "committed") {
        // move.commit was already sent; the client may be mid-migration. Do NOT
        // mark this failed immediately: that would free the player for re-planning
        // while the client is still between rooms. But if neither player.leave
        // nor player.join resolves it within 2x the timeout, the client likely
        // crashed mid-migration — release the player so they are not locked out.
        if (now - tx.createdAt > timeoutMs * 2) {
          tx.status = "failed";
          this.telemetry.releaseReservation(tx.targetRoomId as RoomId, tx.moveId);
          this.queueCancel(tx.targetRoomId as RoomId, tx.moveId, tx.playerIds);
          this.log?.warn({ moveId, playerIds: tx.playerIds, sourceRoomId: tx.sourceRoomId, targetRoomId: tx.targetRoomId }, "committed move never resolved (client lost mid-migration?)");
        }
        continue;
      }
      // Terminal transactions only feed /moves introspection; without eviction the
      // map grows without bound on a long-running manager.
      if (now - tx.createdAt > TRANSACTION_RETENTION_MS) this.transactions.delete(moveId);
    }
  }

  private async executeLevelIsolation(plan: LevelIsolationPlan, now: number, signal: AbortSignal | undefined, usedRooms: string[]): Promise<boolean> {
    const source = this.rooms.getRoom(plan.sourceRoomId as RoomId);
    const target = this.rooms.getRoom(plan.targetRoomId as RoomId);
    if (!source || !target || this.transactionsForPlayer(plan.playerIds).length > 0) return false;
    // The source room may legitimately appear in several plans in one cycle when
    // it holds three or more levels; only the destination has to be exclusive.
    if (this.inCooldown(plan.targetRoomId, now)) return false;

    const moveId = `level-isolation-${plan.levelId}-${now}-${plan.playerIds.join("-")}`;
    if (!this.telemetry.tryReserve(target.roomId, moveId, plan.playerIds.length, target.capacity, target.players, this.options.moveTransactionTimeoutMs ?? 15_000, now)) {
      this.log?.debug({ plan, targetPlayers: target.players, targetCapacity: target.capacity }, "level isolation skipped: no capacity");
      return false;
    }

    const tx: LevelMoveTransaction = {
      ...plan,
      moveId,
      status: "prepared",
      createdAt: now,
      joinedPlayers: [],
      leftPlayers: []
    };
    this.transactions.set(moveId, tx);

    try {
      const targetTransport = this.nodes.getTransport(target.nodeId);
      if (!targetTransport || !this.nodes.isAvailableForCommand(target.nodeId)) throw new Error("target node unavailable");
      const ack = await this.pending.send({ v: asProtocolVersion(1), cmd: "move.prepare", data: { moveId, roomId: target.roomId, playerIds: plan.playerIds, expectedVersion: target.version, ttlMs: 10_000 } }, (command) => targetTransport.send(command), signal);
      if (ack.code !== ErrorCode.OK) throw new Error(ack.message ?? "target rejected move");
      const first = plan.playerIds[0];
      if (!first) throw new Error("empty level isolation plan");
      await this.cross.redirectPlayers({ playerIds: plan.playerIds.map((id) => id as PlayerId), moveId, playerId: first as PlayerId, fromRoomId: source.roomId, to: { roomId: target.roomId, nodeId: target.nodeId, udpHost: target.udpHost, udpPort: target.udpPort, expiresAt: asUnixMs(now + 10_000) }, reason: plan.reason === "level_consolidation" ? "level-consolidation" : "level-isolation" }, signal);
      this.transactions.set(moveId, { ...tx, status: "committed" });
      this.telemetry.markMigrated(plan.playerIds.map((id) => id as PlayerId), now);
      // Cooldowns are applied by the caller once the whole cycle is planned, so a
      // room with several stray levels can be drained in one pass.
      usedRooms.push(plan.sourceRoomId, plan.targetRoomId);
      this.log?.info({ moveId, playerIds: plan.playerIds, sourceRoomId: plan.sourceRoomId, targetRoomId: plan.targetRoomId, levelId: plan.levelId, sourceLevelCount: plan.sourceLevelCount, targetLevelCount: plan.targetLevelCount }, "level isolation committed");
      return true;
    } catch (error) {
      this.queueCancel(target.roomId, moveId, plan.playerIds);
      this.telemetry.releaseReservation(target.roomId, moveId);
      this.transactions.set(moveId, { ...tx, status: "failed" });
      this.log?.warn({ err: error, moveId, playerIds: plan.playerIds, sourceRoomId: plan.sourceRoomId, targetRoomId: plan.targetRoomId }, "level isolation failed");
      return false;
    }
  }

  public list(): Array<MoveTransaction | LevelMoveTransaction> { return [...this.transactions.values()]; }
  public async manualMove(input: { sourceRoomId: string; targetRoomId: string; playerIds: string[]; reason?: string }, signal?: AbortSignal, now = Date.now()): Promise<MoveTransaction | LevelMoveTransaction> {
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
  private transactionsForPlayer(playerIds: string[]): Array<MoveTransaction | LevelMoveTransaction> { return [...this.transactions.values()].filter((tx) => tx.status === "prepared" || tx.status === "committed").filter((tx) => tx.playerIds.some((id) => playerIds.includes(id))); }

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
    this.pending.send({ v: asProtocolVersion(1), cmd: "move.cancel", data: { moveId: entry.moveId, playerIds: entry.playerIds } }, (command) => transport.send(command))
      .then(() => { this.pendingCancels.delete(entry.moveId); })
      .catch((error) => { this.log?.debug({ err: error, moveId }, "move.cancel send failed (will retry)"); });
  }

  private drainPendingCancels(): void {
    for (const moveId of [...this.pendingCancels.keys()]) this.flushCancel(moveId);
  }
}
