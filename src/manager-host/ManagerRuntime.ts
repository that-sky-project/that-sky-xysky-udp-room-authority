import { CrossCoordinator } from "./CrossCoordinator.js";
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
}

export const createManagerRuntime = (config: ManagerConfig, logger?: Logger): ManagerRuntime => {
  const metrics = new MetricsRegistry();
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
    loadPenaltyWeight: 40,
    sameRegionBonus: 12,
    // 7 means "fill rooms to 7 before opening a new one", which concentrates
    // every player into a single room and defeats level isolation. 2 spreads
    // players across rooms so the level-aware score can separate them.
    preferredFillTarget: 2,
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
      minimumGain: config.moveMinimumGain
    },
    levelIsolation: {
      minLevelPlayers: config.levelIsolationMinLevelPlayers,
      maxPlayersPerMove: config.levelIsolationMaxPlayers,
      sparseRoomThreshold: config.levelIsolationSparseRoomThreshold,
      levelChangeCooldownMs: config.levelIsolationLevelChangeCooldownMs
    }
  }, logger, (reason, status) => metrics.inc("qwd_move_total", [reason, status]));
  const router = new RequestRouter(rooms, nodes, policy, reservations, coordinator, {
    nodeStaleMs: config.nodeStaleMs,
    assignmentTtlMs: config.assignmentTtlMs,
    candidateRoomLimit: config.candidateRoomLimit
  }, (roomId) => moves.telemetry.reserved(roomId));

  return { nodes, rooms, pending, policy, reservations, coordinator, router, moves, metrics };
};
