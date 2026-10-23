import type { PlayerId, RoomId } from "../types/branded.js";
import { asProtocolVersion, asUnixMs } from "../types/branded.js";
import type { RoomSnapshot } from "../types/contracts.js";
import { ErrorCode } from "../types/errors.js";
import type { Logger } from "../infra/logger/logger.js";
import type { CrossCoordinator, RedirectReason } from "./CrossCoordinator.js";
import type { NodeRegistry } from "./NodeRegistry.js";
import type { PendingRequestMap } from "./PendingRequestMap.js";
import { TelemetryStore } from "./TelemetryStore.js";

/** The minimum a plan must expose to be executed. */
export interface ExecutableMove {
  playerIds: string[];
  sourceRoomId: string;
  targetRoomId: string;
}

export interface MoveExecutorOptions {
  /** How long the destination holds slots before the players arrive. */
  reservationTtlMs: number;
  /** TTL carried on the move.prepare command itself. */
  prepareTtlMs: number;
  /** How long the redirect address handed to the client stays valid. */
  redirectTtlMs: number;
}

export type MoveExecuteOutcome =
  | { ok: true; moveId: string }
  | { ok: false; moveId: string; reason: "no_capacity" | "stale_plan" | "no_players" | "failed"; error?: unknown };

/**
 * One place that performs a single cross-room move: reserve capacity on the
 * target, send move.prepare, redirect the players, then hand back a
 * transaction record for the caller to store.
 *
 * MoveCoordinator previously repeated this sequence three times (activity
 * merge, level isolation, manual debug move) with subtle differences in
 * timeout handling and error paths; the drift is what let the stale-plan
 * check exist on only one of them.
 */
export class MoveExecutor {
  public constructor(
    private readonly nodes: NodeRegistry,
    private readonly pending: PendingRequestMap,
    private readonly cross: CrossCoordinator,
    private readonly telemetry: TelemetryStore,
    private readonly options: MoveExecutorOptions,
    private readonly log?: Logger
  ) {}

  public async execute(input: {
    moveId: string;
    playerIds: string[];
    source: RoomSnapshot;
    target: RoomSnapshot;
    reason: RedirectReason;
    now: number;
    signal?: AbortSignal | undefined;
  }): Promise<MoveExecuteOutcome> {
    const { moveId, playerIds, source, target, reason, now, signal } = input;

    if (playerIds.length === 0) return { ok: false, moveId, reason: "no_players" };

    if (!this.telemetry.tryReserve(
      target.roomId,
      moveId,
      playerIds.length,
      target.capacity,
      target.players,
      this.options.reservationTtlMs,
      now
    )) {
      this.log?.debug(
        { moveId, targetRoomId: target.roomId, targetPlayers: target.players, targetCapacity: target.capacity },
        "move skipped: target has no capacity"
      );
      return { ok: false, moveId, reason: "no_capacity" };
    }

    try {
      const transport = this.nodes.getTransport(target.nodeId);
      if (!transport || !this.nodes.isAvailableForCommand(target.nodeId)) {
        throw new Error("target node unavailable");
      }

      const ack = await this.pending.send(
        {
          v: asProtocolVersion(1),
          cmd: "move.prepare",
          data: {
            moveId,
            roomId: target.roomId,
            playerIds,
            expectedVersion: target.version,
            ttlMs: this.options.prepareTtlMs
          }
        },
        (command) => transport.send(command),
        signal
      );

      if (ack.code === ErrorCode.STALE_PLAN) {
        this.log?.info({ moveId, targetRoomId: target.roomId, details: ack.data }, "move plan rejected as stale");
        this.telemetry.releaseReservation(target.roomId, moveId);
        return { ok: false, moveId, reason: "stale_plan" };
      }
      if (ack.code !== ErrorCode.OK) throw new Error(ack.message ?? "target rejected move");

      const first = playerIds[0];
      if (!first) throw new Error("empty move group");

      await this.cross.redirectPlayers(
        {
          playerIds: playerIds.map((id) => id as PlayerId),
          moveId,
          playerId: first as PlayerId,
          fromRoomId: source.roomId,
          to: {
            roomId: target.roomId,
            nodeId: target.nodeId,
            udpHost: target.udpHost,
            udpPort: target.udpPort,
            expiresAt: asUnixMs(now + this.options.redirectTtlMs)
          },
          reason
        },
        signal
      );

      this.telemetry.markMigrated(playerIds.map((id) => id as PlayerId), now);
      return { ok: true, moveId };
    } catch (error) {
      this.telemetry.releaseReservation(target.roomId as RoomId, moveId);
      this.log?.warn({ err: error, moveId, sourceRoomId: source.roomId, targetRoomId: target.roomId }, "move failed");
      return { ok: false, moveId, reason: "failed", error };
    }
  }
}
