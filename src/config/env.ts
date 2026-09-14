export interface ManagerConfig {
  host: string;
  port: number;
  allocatePath: string;
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
  socialApiUrl: string;
  socialFriendsPath: string;
  socialApiToken: string;
  socialCacheTtlMs: number;
  debugApiToken: string;
}

const numberFromEnv = (key: string, fallback: number): number => {
  const value = process.env[key];
  if (!value) return fallback;
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) throw new Error(`Invalid numeric env ${key}: ${value}`);
  return parsed;
};

export const loadConfig = (): ManagerConfig => ({
  host: process.env.HERMES_HOST ?? "0.0.0.0",
  port: numberFromEnv("HERMES_PORT", 1131),
  allocatePath: process.env.HERMES_ALLOCATE_PATH ?? "/allocate",
  maxRooms: numberFromEnv("HERMES_MAX_ROOMS", 100_000),
  candidateRoomLimit: numberFromEnv("HERMES_CANDIDATE_ROOM_LIMIT", 256),
  staleRoomMs: numberFromEnv("HERMES_STALE_ROOM_MS", 120_000),
  nodeStaleMs: numberFromEnv("HERMES_NODE_STALE_MS", 10_000),
  assignmentTtlMs: numberFromEnv("HERMES_ASSIGNMENT_TTL_MS", 30_000),
  reservationShadowTtlMs: numberFromEnv("HERMES_RESERVATION_SHADOW_TTL_MS", 10_000),
  commandTimeoutMs: numberFromEnv("HERMES_COMMAND_TIMEOUT_MS", 3_000),
  maxPendingCommands: numberFromEnv("HERMES_MAX_PENDING_COMMANDS", 100_000),
  maxPlayersPerRoom: numberFromEnv("HERMES_MAX_PLAYERS_PER_ROOM", 8),
  nodeBackpressureLimitBytes: numberFromEnv("HERMES_NODE_BACKPRESSURE_LIMIT_BYTES", 8 * 1024 * 1024),
  nodeCircuitOpenMs: numberFromEnv("HERMES_NODE_CIRCUIT_OPEN_MS", 5_000),
  nodeCircuitFailureThreshold: numberFromEnv("HERMES_NODE_CIRCUIT_FAILURE_THRESHOLD", 5),
  
  moveBudgetPerCycle: numberFromEnv("HERMES_MOVE_BUDGET_PER_CYCLE", 8)
  ,moveTransactionTimeoutMs: numberFromEnv("HERMES_MOVE_TRANSACTION_TIMEOUT_MS", 15_000)
  ,socialApiUrl: process.env.HERMES_SOCIAL_API_URL ?? ""
  ,socialFriendsPath: process.env.HERMES_SOCIAL_FRIENDS_PATH ?? "/api/internal/friends/{playerId}"
  ,socialApiToken: process.env.HERMES_SOCIAL_API_TOKEN ?? ""
  ,socialCacheTtlMs: numberFromEnv("HERMES_SOCIAL_CACHE_TTL_MS", 60_000)
  ,debugApiToken: process.env.HERMES_DEBUG_API_TOKEN ?? ""
});
