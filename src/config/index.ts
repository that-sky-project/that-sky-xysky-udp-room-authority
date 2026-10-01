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
  nodeBackpressureLimitBytes: number;
  nodeCircuitOpenMs: number;
  nodeCircuitFailureThreshold: number;
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
  debugApiToken: string;
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
        max_players_per_room: positiveInt(8)
      })
      .default({}),
    nodes: z
      .object({
        stale_ms: positiveInt(10_000),
        backpressure_limit_bytes: positiveInt(8 * 1024 * 1024),
        circuit_open_ms: positiveInt(5_000),
        circuit_failure_threshold: positiveInt(5)
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
        merge_pressure_threshold: floatFrom(0.55),
        minimum_gain: floatFrom(0.2)
      })
      .default({}),
    debug: z
      .object({ api_token: z.string().default("") })
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
    nodeStaleMs: parsed.nodes.stale_ms,
    nodeBackpressureLimitBytes: parsed.nodes.backpressure_limit_bytes,
    nodeCircuitOpenMs: parsed.nodes.circuit_open_ms,
    nodeCircuitFailureThreshold: parsed.nodes.circuit_failure_threshold,
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
    debugApiToken: parsed.debug.api_token
  };
};
