import { z } from 'zod'

const integer = (fallback: number, maximum = 2_147_483_647) => z.coerce.number().int().positive().max(maximum).default(fallback)
const unit = (fallback: number) => z.number().finite().min(0).max(1).default(fallback)
const nonnegative = (fallback: number) => z.number().finite().nonnegative().default(fallback)
const levelIds = z.array(z.number().int().positive().max(0xffffffff)).max(256).default([])

/** Configuration is validated once; runtime services consume the projected values. */
export const configSchema = z.object({
  host: z.string().trim().min(1).default('0.0.0.0'),
  port: integer(1131, 65535),
  allocate_path: z.string().trim().startsWith('/').default('/allocate'),
  logging: z.object({ level: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent']).default('info'), pretty: z.boolean().default(true) }).strict().default({}),
  rooms: z.object({
    max_rooms: integer(100_000), candidate_room_limit: integer(256, 100_000), stale_room_ms: integer(120_000),
    max_players_per_room: integer(8, 8), preferred_fill_target: integer(6, 8),
    load_penalty_weight: nonnegative(10), unknown_level_penalty: nonnegative(50)
  }).strict().default({}),
  nodes: z.object({
    stale_ms: integer(10_000), backpressure_limit_bytes: integer(8 * 1024 * 1024), circuit_open_ms: integer(5_000),
    circuit_failure_threshold: integer(5), heartbeat_interval_ms: integer(5_000), heartbeat_timeout_ms: integer(15_000), grace_ms: integer(10_000)
  }).strict().default({}),
  assignment: z.object({ ttl_ms: integer(30_000, 120_000), reservation_shadow_ttl_ms: integer(10_000, 120_000),
    request_timeout_ms: integer(10_000, 120_000), max_attempts: integer(8, 64) }).strict().default({}),
  commands: z.object({ timeout_ms: integer(3_000, 120_000), max_pending: integer(100_000) }).strict().default({}),
  move: z.object({
    budget_per_cycle: integer(8, 1024), confirmation_cycles: integer(3, 100), cluster_split_penalty: nonnegative(0.05),
    // Legacy spelling remains an explicit alias for the preparation phase only.
    transaction_timeout_ms: z.coerce.number().int().positive().max(120_000).optional(),
    prepare_timeout_ms: z.coerce.number().int().positive().max(120_000).optional(),
    arrival_timeout_ms: integer(30_000, 120_000), reservation_ttl_ms: integer(35_000, 120_000),
    retry_base_ms: integer(2_000, 120_000), retry_max_ms: integer(30_000, 120_000),
    max_attempts: integer(8, 64), max_cycle_ms: integer(2_000, 30_000),
    arrival_cooldown_ms: integer(15_000, 120_000), remigrate_cooldown_ms: integer(20_000, 300_000),
    state_ema_half_life_ms: integer(3_000), distance_radius: z.number().finite().positive().default(30),
    cluster_radius: z.number().finite().positive().default(12), isolation_pressure_weight: unit(0.6), stale_pressure_weight: unit(0.4),
    merge_pressure_threshold: unit(0.25), minimum_gain: z.number().finite().default(0.15),
    level_isolation_min_level_players: integer(1, 7), level_isolation_max_players: integer(7, 7),
    level_isolation_sparse_room_threshold: integer(2, 8), level_change_cooldown_ms: integer(8_000, 120_000),
    join_settle_ms: integer(6_000, 120_000), protected_levels: levelIds
  }).strict().default({}),
  debug: z.object({ api_token: z.string().default(''), log_buffer_size: integer(2000) }).strict().default({})
}).strict().superRefine((config, context) => {
  const fail = (path: (string | number)[], message: string) => context.addIssue({ code: z.ZodIssueCode.custom, path, message })
  const preparation = config.move.prepare_timeout_ms ?? config.move.transaction_timeout_ms ?? 15_000
  if (config.move.prepare_timeout_ms !== undefined && config.move.transaction_timeout_ms !== undefined && config.move.prepare_timeout_ms !== config.move.transaction_timeout_ms) fail(['move', 'prepare_timeout_ms'], 'conflicts with legacy transaction_timeout_ms')
  if (config.rooms.preferred_fill_target > config.rooms.max_players_per_room) fail(['rooms', 'preferred_fill_target'], 'must not exceed room capacity')
  if (config.move.level_isolation_min_level_players > config.move.level_isolation_max_players) fail(['move', 'level_isolation_min_level_players'], 'must not exceed move group limit')
  if (config.move.budget_per_cycle < config.move.level_isolation_max_players) fail(['move', 'budget_per_cycle'], 'must fit the configured maximum move group')
  if (config.move.retry_base_ms > config.move.retry_max_ms) fail(['move', 'retry_max_ms'], 'must be at least retry_base_ms')
  if (config.nodes.heartbeat_timeout_ms <= config.nodes.heartbeat_interval_ms) fail(['nodes', 'heartbeat_timeout_ms'], 'must exceed heartbeat interval')
  if (config.assignment.reservation_shadow_ttl_ms < config.commands.timeout_ms) fail(['assignment', 'reservation_shadow_ttl_ms'], 'must cover a node command wait')
  if (config.assignment.ttl_ms < config.commands.timeout_ms) fail(['assignment', 'ttl_ms'], 'must cover a node command wait')
  if (config.move.reservation_ttl_ms <= preparation || config.move.reservation_ttl_ms < config.move.arrival_timeout_ms + config.commands.timeout_ms) fail(['move', 'reservation_ttl_ms'], 'must cover preparation and arrival deadline plus command margin')
})
