import { CrossCoordinator } from './CrossCoordinator.js'
import { DebugEventLog } from './DebugEventLog.js'
import { NodeRegistry } from './NodeRegistry.js'
import { PendingRequestMap } from './PendingRequestMap.js'
import { PolicyEngine } from './PolicyEngine.js'
import { ReservationTracker } from './ReservationTracker.js'
import { ReservationService } from './ReservationService.js'
import { RequestRouter } from './RequestRouter.js'
import { RoomCache } from './RoomCache.js'
import { SeatAllocator } from './SeatAllocator.js'
import { MoveCoordinator } from './MoveCoordinator.js'
import { MetricsRegistry } from './MetricsRegistry.js'
import type { ManagerConfig } from '../config/index.js'
import type { Logger } from '../infra/logger/logger.js'
export interface ManagerRuntime {
  nodes: NodeRegistry
  rooms: RoomCache
  pending: PendingRequestMap
  policy: PolicyEngine
  reservations: ReservationTracker
  coordinator: CrossCoordinator
  router: RequestRouter
  moves: MoveCoordinator
  metrics: MetricsRegistry
  events: DebugEventLog
  seating: ReservationService
}
export const createManagerRuntime = (
  config: ManagerConfig,
  logger?: Logger
): ManagerRuntime => {
  const metrics = new MetricsRegistry()
  const events = new DebugEventLog(config.debugLogBufferSize)
  const nodes = new NodeRegistry({
    circuitOpenMs: config.nodeCircuitOpenMs,
    failureThreshold: config.nodeCircuitFailureThreshold,
    backpressureLimitBytes: config.nodeBackpressureLimitBytes
  })
  const rooms = new RoomCache({
    maxRooms: config.maxRooms,
    staleRoomMs: config.staleRoomMs,
    nodeGraceMs: config.nodeGraceMs
  })
  const reservations = new ReservationTracker({
    ttlMs: config.reservationShadowTtlMs
  })
  const pending = new PendingRequestMap({
    timeoutMs: config.commandTimeoutMs,
    maxPending: config.maxPendingCommands
  })
  const policy = new PolicyEngine({
    maxPlayersPerRoom: config.maxPlayersPerRoom,
    staleNodePenalty: 30,
    loadPenaltyWeight: config.loadPenaltyWeight,
    sameRegionBonus: 12,
    preferredFillTarget: config.preferredFillTarget,
    sameLevelBonus: 20,
    wrongLevelPenalty: 80,
    unknownLevelPenalty: config.unknownLevelPenalty,
    protectedLevels: config.protectedLevels
  })
  const coordinator = new CrossCoordinator(nodes, rooms, pending, {
    commandTimeoutMs: config.commandTimeoutMs
  })
  const moves = new MoveCoordinator(
    rooms,
    nodes,
    pending,
    coordinator,
    {
      moveBudgetPerCycle: config.moveBudgetPerCycle,
      moveTransactionTimeoutMs: config.moveTransactionTimeoutMs,
      confirmationCycles: config.moveConfirmationCycles,
      stateEmaHalfLifeMs: config.moveStateEmaHalfLifeMs,
      planner: {
        distanceRadius: config.moveDistanceRadius,
        clusterRadius: config.moveClusterRadius,
        isolationPressureWeight: config.moveIsolationPressureWeight,
        stalePressureWeight: config.moveStalePressureWeight,
        mergePressureThreshold: config.moveMergePressureThreshold,
        minimumGain: config.moveMinimumGain,
        levelChangeCooldownMs: config.levelChangeCooldownMs,
        joinSettleMs: config.joinSettleMs,
        clusterSplitPenalty: config.moveClusterSplitPenalty,
        protectedLevels: config.protectedLevels
      },
      levelIsolation: {
        minLevelPlayers: config.levelIsolationMinLevelPlayers,
        maxPlayersPerMove: config.levelIsolationMaxPlayers,
        sparseRoomThreshold: config.levelIsolationSparseRoomThreshold,
        levelChangeCooldownMs: config.levelChangeCooldownMs,
        joinSettleMs: config.joinSettleMs,
        protectedLevels: config.protectedLevels
      }
    },
    logger,
    (reason, status) => metrics.inc('qwd_move_total', [reason, status]),
    () => metrics.inc('qwd_telemetry_gap_total'),
    events
  )
  const router = new RequestRouter(
    rooms,
    nodes,
    policy,
    reservations,
    coordinator,
    {
      nodeStaleMs: config.nodeStaleMs,
      assignmentTtlMs: config.assignmentTtlMs,
      candidateRoomLimit: config.candidateRoomLimit,
      preferredFillTarget: config.preferredFillTarget
    },
    roomId => moves.telemetry.reserved(roomId),
    logger,
    events
  )
  const seating = new ReservationService(
    new SeatAllocator(),
    rooms,
    router,
    coordinator,
    logger
  )
  return {
    nodes,
    rooms,
    pending,
    policy,
    reservations,
    coordinator,
    router,
    moves,
    metrics,
    events,
    seating
  }
}
