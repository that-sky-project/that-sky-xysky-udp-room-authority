import { readFileSync } from "node:fs";
import path from "node:path";
import { parse as parseYaml } from "yaml";
import { configSchema } from "./schema.js";
export type LogLevel = 'fatal' | 'error' | 'warn' | 'info' | 'debug' | 'trace' | 'silent';
export interface ManagerConfig {
  host: string;
  port: number;
  allocatePath: string;
  logLevel: LogLevel;
  logPretty: boolean;
  maxRooms: number;
  candidateRoomLimit: number;
  staleRoomMs: number;
  nodeStaleMs: number;
  assignmentTtlMs: number;
  reservationShadowTtlMs: number;
  commandTimeoutMs: number;
  maxPendingCommands: number;
  maxPlayersPerRoom: number;
  preferredFillTarget: number;
  loadPenaltyWeight: number;
  unknownLevelPenalty: number;
  nodeBackpressureLimitBytes: number;
  nodeCircuitOpenMs: number;
  nodeCircuitFailureThreshold: number;
  nodeHeartbeatIntervalMs: number;
  nodeHeartbeatTimeoutMs: number;
  nodeGraceMs: number;
  moveBudgetPerCycle: number;
  moveConfirmationCycles: number;
  moveClusterSplitPenalty: number;
  moveTransactionTimeoutMs: number;
  movePrepareTimeoutMs: number;
  moveArrivalTimeoutMs: number;
  moveReservationTtlMs: number;
  moveRetryBaseMs: number;
  moveRetryMaxMs: number;
  moveMaxAttempts: number;
  moveMaxCycleMs: number;
  moveArrivalCooldownMs: number;
  moveRemigrateCooldownMs: number;
  allocationRequestTimeoutMs: number;
  allocationMaxAttempts: number;
  moveStateEmaHalfLifeMs: number;
  moveDistanceRadius: number;
  moveClusterRadius: number;
  moveIsolationPressureWeight: number;
  moveStalePressureWeight: number;
  moveMergePressureThreshold: number;
  moveMinimumGain: number;
  levelIsolationMinLevelPlayers: number;
  levelIsolationMaxPlayers: number;
  levelIsolationSparseRoomThreshold: number;
  levelChangeCooldownMs: number;
  joinSettleMs: number;
  protectedLevels: number[];
  debugApiToken: string;
  debugLogBufferSize: number;
}
export const loadConfig = (configFile: string = path.resolve(process.cwd(), "config.yml")): ManagerConfig => {
  const raw = parseYaml(readFileSync(configFile, "utf8"));
  const parsed = configSchema.parse(raw ?? {});
  return {
    host: parsed.host,
    port: parsed.port,
    allocatePath: parsed.allocate_path,
    logLevel: parsed.logging.level,
    logPretty: parsed.logging.pretty,
    maxRooms: parsed.rooms.max_rooms,
    candidateRoomLimit: parsed.rooms.candidate_room_limit,
    staleRoomMs: parsed.rooms.stale_room_ms,
    maxPlayersPerRoom: parsed.rooms.max_players_per_room,
    preferredFillTarget: parsed.rooms.preferred_fill_target,
    loadPenaltyWeight: parsed.rooms.load_penalty_weight,
    unknownLevelPenalty: parsed.rooms.unknown_level_penalty,
    nodeStaleMs: parsed.nodes.stale_ms,
    nodeBackpressureLimitBytes: parsed.nodes.backpressure_limit_bytes,
    nodeCircuitOpenMs: parsed.nodes.circuit_open_ms,
    nodeCircuitFailureThreshold: parsed.nodes.circuit_failure_threshold,
    nodeHeartbeatIntervalMs: parsed.nodes.heartbeat_interval_ms,
    nodeHeartbeatTimeoutMs: parsed.nodes.heartbeat_timeout_ms,
    nodeGraceMs: parsed.nodes.grace_ms,
    assignmentTtlMs: parsed.assignment.ttl_ms,
    reservationShadowTtlMs: parsed.assignment.reservation_shadow_ttl_ms,
    commandTimeoutMs: parsed.commands.timeout_ms,
    maxPendingCommands: parsed.commands.max_pending,
    moveBudgetPerCycle: parsed.move.budget_per_cycle,
    moveConfirmationCycles: parsed.move.confirmation_cycles,
    moveClusterSplitPenalty: parsed.move.cluster_split_penalty,
    moveTransactionTimeoutMs: parsed.move.prepare_timeout_ms ?? parsed.move.transaction_timeout_ms ?? 15_000,
    movePrepareTimeoutMs: parsed.move.prepare_timeout_ms ?? parsed.move.transaction_timeout_ms ?? 15_000,
    moveArrivalTimeoutMs: parsed.move.arrival_timeout_ms,
    moveReservationTtlMs: parsed.move.reservation_ttl_ms,
    moveRetryBaseMs: parsed.move.retry_base_ms,
    moveRetryMaxMs: parsed.move.retry_max_ms,
    moveMaxAttempts: parsed.move.max_attempts,
    moveMaxCycleMs: parsed.move.max_cycle_ms,
    moveArrivalCooldownMs: parsed.move.arrival_cooldown_ms,
    moveRemigrateCooldownMs: parsed.move.remigrate_cooldown_ms,
    allocationRequestTimeoutMs: parsed.assignment.request_timeout_ms,
    allocationMaxAttempts: parsed.assignment.max_attempts,
    moveStateEmaHalfLifeMs: parsed.move.state_ema_half_life_ms,
    moveDistanceRadius: parsed.move.distance_radius,
    moveClusterRadius: parsed.move.cluster_radius,
    moveIsolationPressureWeight: parsed.move.isolation_pressure_weight,
    moveStalePressureWeight: parsed.move.stale_pressure_weight,
    moveMergePressureThreshold: parsed.move.merge_pressure_threshold,
    moveMinimumGain: parsed.move.minimum_gain,
    levelIsolationMinLevelPlayers: parsed.move.level_isolation_min_level_players,
    levelIsolationMaxPlayers: parsed.move.level_isolation_max_players,
    levelIsolationSparseRoomThreshold: parsed.move.level_isolation_sparse_room_threshold,
    levelChangeCooldownMs: parsed.move.level_change_cooldown_ms,
    joinSettleMs: parsed.move.join_settle_ms,
    protectedLevels: parsed.move.protected_levels,
    debugApiToken: parsed.debug.api_token,
    debugLogBufferSize: parsed.debug.log_buffer_size
  };
};
