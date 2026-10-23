import type { PlayerId, RoomId } from "../types/branded.js";
import { asProtocolVersion } from "../types/branded.js";
import type { INodeEvent, PlayerTelemetry, RoomSnapshot } from "../types/contracts.js";
import type { Logger } from "../infra/logger/logger.js";
import type { CrossCoordinator } from "./CrossCoordinator.js";
import type { NodeRegistry } from "./NodeRegistry.js";
import type { PendingRequestMap } from "./PendingRequestMap.js";
import type { RoomCache } from "./RoomCache.js";
import { MovePlanner, type MovePlan, type MovePlannerOptions } from "./MovePlanner.js";
import { TelemetryStore } from "./TelemetryStore.js";
import { LevelIsolationPlanner, type LevelIsolationPlan, type LevelIsolationOptions } from "./LevelIsolationPlanner.js";
import { MoveExecutor } from "./MoveExecutor.js";

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
  private readonly executor: MoveExecutor;
  private readonly transactions = new Map<string, MoveTransaction | LevelMoveTransaction>();
  private readonly roomCooldownUntil = new Map<string, number>();
  private readonly pendingCancels = new Map<string, { roomId: RoomId; moveId: string; playerIds: string[] }>();
  private lastPlanAt = 0;
  private planning = false;
  /** Latest full roster per room, used to apply delta telemetry frames. */
  private readonly roomRoster = new Map<string, { seq: number; players: Map<string, PlayerTelemetry> }>();
  /** Rooms whose delta stream is currently out of sync with our roster. */
  private readonly telemetryGap = new Map<string, { since: number; dropped: number; haveSeq?: number | undefined }>();
  /** Rate limit for the delta-gap warning, per room. */
  private readonly telemetryGapWarnAt = new Map<string, number>();
  /** Rate limit for the version-regression warning, per room. */
  private readonly telemetryVersionWarnAt = new Map<string, number>();

  public constructor(
    private readonly rooms: RoomCache,
    private readonly nodes: NodeRegistry,
    private readonly pending: PendingRequestMap,
    private readonly cross: CrossCoordinator,
    private readonly options: MoveCoordinatorOptions = {},
    private readonly log?: Logger,
    /** Optional counter sink so the gateway can expose move outcomes as metrics. */
    private readonly onMoveOutcome?: (reason: string, status: "committed" | "failed") => void,
    /** Counts delta frames dropped because the manager does not know their base. */
    private readonly onTelemetryGap?: () => void
  ) {
    this.telemetry = new TelemetryStore({ stateEmaHalfLifeMs: options.stateEmaHalfLifeMs ?? 3_000 });
    this.planner = new MovePlanner(options.planner ?? {});
    this.levelIsolationPlanner = new LevelIsolationPlanner(options.levelIsolation ?? {});
    const transactionTimeoutMs = options.moveTransactionTimeoutMs ?? 15_000;
    this.executor = new MoveExecutor(nodes, pending, cross, this.telemetry, {
      reservationTtlMs: transactionTimeoutMs,
      prepareTtlMs: 10_000,
      redirectTtlMs: 10_000
    }, log);
  }

  public ingest(event: INodeEvent): void {
    if (event.event === "room.destroyed" && event.data.roomId) {
      // A later room may reuse this id; a leftover roster would make every
      // delta look like a base mismatch.
      const key = event.data.roomId as string;
      this.roomRoster.delete(key);
      this.telemetryGap.delete(key);
      this.telemetryGapWarnAt.delete(key);
      this.telemetryVersionWarnAt.delete(key);
    }
    if (event.event === "room.telemetry" && event.data.roomId) {
      const roomId = event.data.roomId;
      const incoming = Array.isArray(event.data.players) ? event.data.players : [];

      // Delta frames only carry players whose state changed. Apply them onto the
      // roster we already hold for the room so occupancy and level grouping stay
      // correct; a delta whose baseSeq we do not recognise is treated as a full
      // snapshot by the sender, so we simply replace the roster.
      let roster = this.roomRoster.get(roomId as string);
      if (event.data.kind === "delta") {
        const baseOk = roster !== undefined && roster.seq === event.data.baseSeq;
        if (!baseOk) {
          // The next full resync repairs this, but until then the room's version
          // and roster are frozen. Track the gap and surface it: without these
          // logs a lost baseSeq silently freezes a room's state.
          this.onTelemetryGap?.();
          const at = Date.now();
          const gap = this.telemetryGap.get(roomId) ?? { since: at, dropped: 0, haveSeq: roster?.seq };
          gap.dropped += 1;
          this.telemetryGap.set(roomId, gap);
          const fields = {
            roomId,
            nodeId: event.data.nodeId,
            seq: event.data.seq,
            baseSeq: event.data.baseSeq,
            haveSeq: roster?.seq,
            droppedFrames: gap.dropped,
            gapMs: at - gap.since,
            players: incoming.length
          };
          if (at - (this.telemetryGapWarnAt.get(roomId) ?? 0) >= 30_000) {
            this.telemetryGapWarnAt.set(roomId, at);
            this.log?.warn(fields, "telemetry delta gap; awaiting full snapshot");
          } else {
            this.log?.debug(fields, "telemetry delta gap; awaiting full snapshot");
          }
          return;
        }
        for (const playerId of event.data.removedPlayers ?? []) {
          roster!.players.delete(playerId as string);
        }
        for (const player of incoming) roster!.players.set(player.playerId as string, player);
        roster!.seq = typeof event.data.seq === "number" ? event.data.seq : roster!.seq + 1;
      } else {
        const players = new Map<string, (typeof incoming)[number]>();
        for (const player of incoming) players.set(player.playerId as string, player);
        roster = { seq: typeof event.data.seq === "number" ? event.data.seq : 0, players };
        this.roomRoster.set(roomId as string, roster);

        // A full frame is authoritative for membership. Drop tracked players it
        // no longer contains: a player.leave lost while the control connection
        // was down would otherwise stay in the telemetry store for the whole
        // five-minute retention window and keep voting in the planner.
        for (const tracked of this.telemetry.listRoom(roomId)) {
          if (!players.has(tracked.playerId as string)) this.telemetry.remove(tracked.playerId);
        }

        const gap = this.telemetryGap.get(roomId);
        if (gap) {
          this.telemetryGap.delete(roomId);
          this.telemetryGapWarnAt.delete(roomId);
          this.log?.warn(
            {
              roomId,
              nodeId: event.data.nodeId,
              gapMs: Date.now() - gap.since,
              droppedFrames: gap.dropped,
              fromSeq: gap.haveSeq,
              toSeq: event.data.seq,
              players: incoming.length
            },
            "telemetry resynchronized after gap"
          );
        }
      }

      for (const player of incoming) {
        this.telemetry.upsert(roomId, player);
        // Keep playerLocations.level in sync from telemetry. The initial
        // player.join from some node versions omits level, which would leave
        // levelsByRoom() empty and disable level-aware allocation entirely.
        if (player.levelId !== undefined) {
          this.rooms.trackPlayer(player.playerId, event.data.nodeId, roomId, player.levelId);
        }
      }
      const room = this.rooms.getRoom(roomId);
      if (room) {
        const count = roster ? roster.players.size : incoming.length;
        // Track the node's telemetry sequence as the room version so a move plan
        // can carry expectedVersion and the node can reject a plan built from a
        // snapshot that has already moved on.
        const version = typeof event.data.seq === "number" ? event.data.seq : room.version;
        const applied = this.rooms.upsertRoom({ ...room, players: Math.min(room.capacity, count), version });
        if (!applied) {
          // The frame passed the gateway but is older than the cached snapshot
          // (same node). That should not happen with a monotonic sequence, so
          // treat it as a desync and report it.
          this.onTelemetryGap?.();
          const at = Date.now();
          if (at - (this.telemetryVersionWarnAt.get(roomId) ?? 0) >= 30_000) {
            this.telemetryVersionWarnAt.set(roomId, at);
            this.log?.warn(
              { roomId, nodeId: event.data.nodeId, seq: version, cachedVersion: room.version, kind: event.data.kind ?? "full" },
              "telemetry version regression rejected"
            );
          }
        }
      }
    }
    if ((event.event === "player.join" || event.event === "player.leave") && event.data.player?.playerId) {
      // Keep the delta roster in step with join/leave so the next delta frame
      // can be applied even before the next full snapshot arrives.
      if (event.data.roomId) {
        const roster = this.roomRoster.get(event.data.roomId as string);
        if (roster && event.event === "player.leave") {
          roster.players.delete(event.data.player.playerId as string);
        }
      }
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
    // The destination node dropped a reservation we are tracking (its TTL ran
    // out before the players arrived). Settle the transaction now instead of
    // waiting for our own timeout.
    if (event.event === "reservation.expired" && event.data.moveId) {
      const tx = this.transactions.get(event.data.moveId);
      if (tx && (tx.status === "prepared" || tx.status === "committed")) {
        tx.status = "failed";
        this.telemetry.releaseReservation(tx.targetRoomId as RoomId, tx.moveId);
        this.onMoveOutcome?.(tx.reason, "failed");
        this.log?.warn({ moveId: tx.moveId, playerIds: tx.playerIds }, "target reservation expired before players arrived");
      }
    }
    // Positive completion signal from the destination node: the entire group
    // landed. This is stronger than inferring completion from join/leave pairs
    // and lets a transaction settle even if a leave event was lost.
    if (event.event === "move.completed" && event.data.moveId) {
      const tx = this.transactions.get(event.data.moveId);
      if (tx && (tx.status === "committed" || tx.status === "prepared")) {
        tx.status = "completed";
        this.telemetry.releaseReservation(tx.targetRoomId as RoomId, tx.moveId);
        this.log?.info(
          { moveId: tx.moveId, playerIds: tx.playerIds, targetRoomId: tx.targetRoomId },
          "move completed (node confirmation)"
        );
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

    // Rooms kept for a disconnected node's grace window stay in the cache so
    // allocation and /players can still resolve them, but they must never be
    // planned against. A room on a dead or circuit-open node is not a real
    // target: the executor would fail the move, and because the plan picks the
    // first candidate that fits, it could shadow a live room for the cycle.
    const rooms = this.rooms.listRooms().filter((room) => this.isRoomPlannable(room));
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
      const tx: MoveTransaction = { ...plan, moveId, status: "prepared", createdAt: now, joinedPlayers: [], leftPlayers: [] };
      this.transactions.set(moveId, tx);

      const outcome = await this.executor.execute({
        moveId,
        playerIds: plan.playerIds,
        source,
        target,
        reason: plan.reason === "low_activity_merge" ? "merge" : "capacity-rebalance",
        now,
        signal
      });

      if (outcome.ok) {
        this.transactions.set(moveId, { ...tx, status: "committed" });
        usedTargets.add(plan.targetRoomId); moveBudget -= plan.playerIds.length;
        this.applyCooldown([plan.sourceRoomId, plan.targetRoomId], now);
        this.onMoveOutcome?.(plan.reason, "committed");
        this.log?.info({ moveId, playerIds: plan.playerIds, sourceRoomId: plan.sourceRoomId, targetRoomId: plan.targetRoomId, reason: plan.reason, gain: plan.gain }, "move committed");
      } else if (outcome.reason === "failed") {
        this.queueCancel(target.roomId, moveId, plan.playerIds);
        this.transactions.set(moveId, { ...tx, status: "failed" });
        this.onMoveOutcome?.(plan.reason, "failed");
      } else {
        // no_capacity / stale_plan / no_players: the executor already released
        // the reservation. Drop the record so it does not look in-flight.
        this.transactions.delete(moveId);
      }
    }
    return [...this.transactions.values()];
  }

  /**
   * A move source or target must live on a node the manager can actually talk
   * to. Rooms of a disconnected (grace-window), draining or circuit-open node
   * are excluded from planning; they remain in the cache for allocation and
   * player lookup, which have their own availability checks.
   */
  private isRoomPlannable(room: RoomSnapshot): boolean {
    if (room.draining) return false;
    if (this.rooms.isNodeSuspected(room.nodeId)) return false;
    return this.nodes.isAvailableForCommand(room.nodeId);
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
    if (!source || !target) {
      this.log?.warn({ sourceRoomId: plan.sourceRoomId, targetRoomId: plan.targetRoomId, levelId: plan.levelId }, "level isolation skipped: room no longer cached");
      return false;
    }
    if (this.transactionsForPlayer(plan.playerIds).length > 0) {
      this.log?.debug({ playerIds: plan.playerIds }, "level isolation skipped: player already has a pending move");
      return false;
    }
    // The source room may legitimately appear in several plans in one cycle when
    // it holds three or more levels; only the destination has to be exclusive.
    if (this.inCooldown(plan.targetRoomId, now)) {
      this.log?.debug({ targetRoomId: plan.targetRoomId, levelId: plan.levelId }, "level isolation skipped: target room in cooldown");
      return false;
    }

    const moveId = `level-isolation-${plan.levelId}-${now}-${plan.playerIds.join("-")}`;
    const tx: LevelMoveTransaction = { ...plan, moveId, status: "prepared", createdAt: now, joinedPlayers: [], leftPlayers: [] };
    this.transactions.set(moveId, tx);

    const outcome = await this.executor.execute({
      moveId,
      playerIds: plan.playerIds,
      source,
      target,
      reason: plan.reason === "level_consolidation" ? "level-consolidation" : "level-isolation",
      now,
      signal
    });

    if (outcome.ok) {
      this.transactions.set(moveId, { ...tx, status: "committed" });
      // Cooldowns are applied by the caller once the whole cycle is planned, so a
      // room with several stray levels can be drained in one pass.
      usedRooms.push(plan.sourceRoomId, plan.targetRoomId);
      this.onMoveOutcome?.(plan.reason, "committed");
      this.log?.info({ moveId, reason: plan.reason, playerIds: plan.playerIds, sourceRoomId: plan.sourceRoomId, targetRoomId: plan.targetRoomId, levelId: plan.levelId, sourceLevelCount: plan.sourceLevelCount, targetLevelCount: plan.targetLevelCount }, "level isolation committed");
      return true;
    }

    if (outcome.reason === "failed") {
      this.queueCancel(target.roomId, moveId, plan.playerIds);
      this.transactions.set(moveId, { ...tx, status: "failed" });
      this.onMoveOutcome?.(plan.reason, "failed");
    } else {
      // no_capacity / stale_plan / no_players never reach the warn path in the
      // executor, so a rejected merge used to disappear without a trace.
      this.log?.info({ moveId, reason: outcome.reason, planReason: plan.reason, playerIds: plan.playerIds, sourceRoomId: plan.sourceRoomId, targetRoomId: plan.targetRoomId, levelId: plan.levelId }, "level isolation not executed");
      this.transactions.delete(moveId);
    }
    return false;
  }

  public list(): Array<MoveTransaction | LevelMoveTransaction> { return [...this.transactions.values()]; }
  /** Number of rooms whose delta telemetry is currently out of sync. */
  public telemetryGapCount(): number { return this.telemetryGap.size; }
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
    this.transactions.set(moveId, tx);

    const outcome = await this.executor.execute({
      moveId,
      playerIds,
      source,
      target,
      reason: tx.reason === "low_activity_merge" ? "merge" : "capacity-rebalance",
      now,
      signal
    });

    if (!outcome.ok) {
      if (outcome.reason === "failed") {
        this.queueCancel(target.roomId, moveId, playerIds);
        this.transactions.set(moveId, { ...tx, status: "failed" });
        throw outcome.error ?? new Error("move failed");
      }
      this.transactions.delete(moveId);
      throw new Error(
        outcome.reason === "no_capacity" ? "target room has insufficient capacity"
          : outcome.reason === "stale_plan" ? "target room state changed; retry"
          : "move could not be executed"
      );
    }

    const committed = { ...tx, status: "committed" as const };
    this.transactions.set(moveId, committed);
    return committed;
  }
  public activitySnapshot(now = Date.now()): Array<ReturnType<MovePlanner["activity"]>> {
    return this.rooms.listRooms().map((room) => this.planner.activity(room, this.telemetry.listRoom(room.roomId, now), now));
  }
  private transactionsForPlayer(playerIds: string[]): Array<MoveTransaction | LevelMoveTransaction> { return [...this.transactions.values()].filter((tx) => tx.status === "prepared" || tx.status === "committed").filter((tx) => tx.playerIds.some((id) => playerIds.includes(id))); }

  private applyCooldown(roomIds: string[], now: number): void {
    const until = now + (this.options.roomCooldownMs ?? 20_000);
    for (const roomId of roomIds) this.roomCooldownUntil.set(roomId, until);
  }

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
