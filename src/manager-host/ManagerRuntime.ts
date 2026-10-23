import { CrossCoordinator } from "./CrossCoordinator.js";
import { DebugEventLog } from "./DebugEventLog.js";
import { NodeRegistry } from "./NodeRegistry.js";
import { PendingRequestMap } from "./PendingRequestMap.js";
import { PolicyEngine } from "./PolicyEngine.js";
import { ReservationTracker } from "./ReservationTracker.js";
import { RequestRouter } from "./RequestRouter.js";
import { RoomCache } from "./RoomCache.js";
import { MoveCoordinator } from "./MoveCoordinator.js";
import { MetricsRegistry } from "./MetricsRegistry.js";
import type { ManagerConfig } from "../config/index.js";
import type { Logger } from "../infra/logger/logger.js";

export interface ManagerRuntime {
  nodes: NodeRegistry;
  rooms: RoomCache;
  pending: PendingRequestMap;
  policy: PolicyEngine;
  reservations: ReservationTracker;
  coordinator: CrossCoordinator;
  router: RequestRouter;
  moves: MoveCoordinator;
  metrics: MetricsRegistry;
  events: DebugEventLog;
}

export const createManagerRuntime = (config: ManagerConfig, logger?: Logger): ManagerRuntime => {
  const metrics = new MetricsRegistry();
  // Operational history for GET /debug/logs. Ring-buffered, never used for
  // planning decisions.
  const events = new DebugEventLog(config.debugLogBufferSize);
  const nodes = new NodeRegistry({
    circuitOpenMs: config.nodeCircuitOpenMs,
    failureThreshold: config.nodeCircuitFailureThreshold,
    backpressureLimitBytes: config.nodeBackpressureLimitBytes
  });
  const rooms = new RoomCache({ maxRooms: config.maxRooms, staleRoomMs: config.staleRoomMs, nodeGraceMs: config.nodeGraceMs });
  const reservations = new ReservationTracker({ ttlMs: config.reservationShadowTtlMs });
  const pending = new PendingRequestMap({ timeoutMs: config.commandTimeoutMs, maxPending: config.maxPendingCommands });
  const policy = new PolicyEngine({
    maxPlayersPerRoom: config.maxPlayersPerRoom,
    staleNodePenalty: 30,
    loadPenaltyWeight: config.loadPenaltyWeight,
    sameRegionBonus: 12,
    // Fill existing rooms before opening new ones: players on the same level
    // belong together, and the level-aware score below already keeps different
    // levels apart. A low target scattered every join into its own empty room
    // that only the consolidation pass merged later.
    preferredFillTarget: config.preferredFillTarget,
    // A wrong-level room must lose to any same-level or empty room that has
    // capacity, so the penalty exceeds the widest possible fill bonus (8*7=56).
    sameLevelBonus: 20,
    wrongLevelPenalty: 80
  });
  const coordinator = new CrossCoordinator(nodes, rooms, pending, { commandTimeoutMs: config.commandTimeoutMs });
  const moves = new MoveCoordinator(rooms, nodes, pending, coordinator, {
    moveBudgetPerCycle: config.moveBudgetPerCycle,
    moveTransactionTimeoutMs: config.moveTransactionTimeoutMs,
    roomCooldownMs: config.moveRoomCooldownMs,
    stateEmaHalfLifeMs: config.moveStateEmaHalfLifeMs,
    planner: {
      distanceRadius: config.moveDistanceRadius,
      clusterRadius: config.moveClusterRadius,
      isolationPressureWeight: config.moveIsolationPressureWeight,
      stalePressureWeight: config.moveStalePressureWeight,
      mergePressureThreshold: config.moveMergePressureThreshold,
      minimumGain: config.moveMinimumGain,
      levelChangeCooldownMs: config.levelChangeCooldownMs
    },
    levelIsolation: {
      minLevelPlayers: config.levelIsolationMinLevelPlayers,
      maxPlayersPerMove: config.levelIsolationMaxPlayers,
      sparseRoomThreshold: config.levelIsolationSparseRoomThreshold,
      levelChangeCooldownMs: config.levelChangeCooldownMs
    }
  }, logger, (reason, status) => metrics.inc("qwd_move_total", [reason, status]), () => metrics.inc("qwd_telemetry_gap_total"), events);
  const router = new RequestRouter(rooms, nodes, policy, reservations, coordinator, {
    nodeStaleMs: config.nodeStaleMs,
    assignmentTtlMs: config.assignmentTtlMs,
    candidateRoomLimit: config.candidateRoomLimit,
    preferredFillTarget: config.preferredFillTarget
  }, (roomId) => moves.telemetry.reserved(roomId), logger, events);

  return { nodes, rooms, pending, policy, reservations, coordinator, router, moves, metrics, events };
};
