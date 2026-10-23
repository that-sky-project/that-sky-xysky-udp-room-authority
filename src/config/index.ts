import { readFileSync } from "node:fs";
import path from "node:path";
import { parse as parseYaml } from "yaml";
import { z } from "zod";

export interface ManagerConfig {
  host: string;
  port: number;
  allocatePath: string;
  logLevel: string;
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
  nodeBackpressureLimitBytes: number;
  nodeCircuitOpenMs: number;
  nodeCircuitFailureThreshold: number;
  nodeHeartbeatIntervalMs: number;
  nodeHeartbeatTimeoutMs: number;
  nodeGraceMs: number;
  moveBudgetPerCycle: number;
  moveTransactionTimeoutMs: number;
  moveRoomCooldownMs: number;
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
  debugApiToken: string;
  debugLogBufferSize: number;
}

const positiveInt = (fallback: number) => z.coerce.number().int().positive().default(fallback);
const floatFrom = (fallback: number) => z.coerce.number().default(fallback);

const configSchema = z
  .object({
    host: z.string().trim().min(1).default("0.0.0.0"),
    port: positiveInt(1131),
    allocate_path: z.string().trim().min(1).default("/allocate"),
    logging: z
      .object({ level: z.string().trim().min(1).default("info") })
      .default({}),
    rooms: z
      .object({
        max_rooms: positiveInt(100_000),
        candidate_room_limit: positiveInt(256),
        stale_room_ms: positiveInt(120_000),
        max_players_per_room: positiveInt(8),
        // Room fill level the allocator aims for. High values keep piling
        // same-level players into an existing room; low values scatter them
        // into empty rooms that only get merged later.
        preferred_fill_target: positiveInt(6),
        // Weight of the node load penalty. One node hosts one room here, so
        // load already shows up as room fill; keep this small or it cancels
        // the level bonus and pushes joins into empty rooms.
        load_penalty_weight: floatFrom(10)
      })
      .default({}),
    nodes: z
      .object({
        stale_ms: positiveInt(10_000),
        backpressure_limit_bytes: positiveInt(8 * 1024 * 1024),
        circuit_open_ms: positiveInt(5_000),
        circuit_failure_threshold: positiveInt(5),
        heartbeat_interval_ms: positiveInt(5_000),
        heartbeat_timeout_ms: positiveInt(15_000),
        grace_ms: positiveInt(10_000)
      })
      .default({}),
    assignment: z
      .object({
        ttl_ms: positiveInt(30_000),
        reservation_shadow_ttl_ms: positiveInt(10_000)
      })
      .default({}),
    commands: z
      .object({
        timeout_ms: positiveInt(3_000),
        max_pending: positiveInt(100_000)
      })
      .default({}),
    move: z
      .object({
        budget_per_cycle: positiveInt(8),
        transaction_timeout_ms: positiveInt(15_000),
        room_cooldown_ms: positiveInt(20_000),
        state_ema_half_life_ms: positiveInt(3_000),
        distance_radius: floatFrom(30),
        cluster_radius: floatFrom(12),
        isolation_pressure_weight: floatFrom(0.6),
        stale_pressure_weight: floatFrom(0.4),
        merge_pressure_threshold: floatFrom(0.25),
        minimum_gain: floatFrom(0.15),
        level_isolation_min_level_players: positiveInt(1),
        level_isolation_max_players: positiveInt(8),
        level_isolation_sparse_room_threshold: positiveInt(2),
        // Shared settling window for every migration path (isolation,
        // same-level consolidation and activity merge): after anyone in a room
        // changes level, nobody is moved out of it for this long.
        level_change_cooldown_ms: positiveInt(6000)
      })
      .default({}),
    debug: z
      .object({
        api_token: z.string().default(""),
        // Ring buffer behind GET /debug/logs (moves, allocations, node traffic,
        // telemetry gaps). In-memory only; lost on restart.
        log_buffer_size: positiveInt(2000)
      })
      .default({})
  })
  .strict();

export const loadConfig = (configFile: string = path.resolve(process.cwd(), "config.yml")): ManagerConfig => {
  const raw = parseYaml(readFileSync(configFile, "utf8"));
  const parsed = configSchema.parse(raw ?? {});

  return {
    host: parsed.host,
    port: parsed.port,
    allocatePath: parsed.allocate_path,
    logLevel: parsed.logging.level,
    maxRooms: parsed.rooms.max_rooms,
    candidateRoomLimit: parsed.rooms.candidate_room_limit,
    staleRoomMs: parsed.rooms.stale_room_ms,
    maxPlayersPerRoom: parsed.rooms.max_players_per_room,
    preferredFillTarget: parsed.rooms.preferred_fill_target,
    loadPenaltyWeight: parsed.rooms.load_penalty_weight,
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
    moveTransactionTimeoutMs: parsed.move.transaction_timeout_ms,
    moveRoomCooldownMs: parsed.move.room_cooldown_ms,
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
    debugApiToken: parsed.debug.api_token,
    debugLogBufferSize: parsed.debug.log_buffer_size
  };
};
