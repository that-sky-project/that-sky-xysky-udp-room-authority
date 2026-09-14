import { CrossCoordinator } from "./CrossCoordinator.js";
import { NodeRegistry } from "./NodeRegistry.js";
import { PendingRequestMap } from "./PendingRequestMap.js";
import { PolicyEngine } from "./PolicyEngine.js";
import { ReservationTracker } from "./ReservationTracker.js";
import { RequestRouter } from "./RequestRouter.js";
import { RoomCache } from "./RoomCache.js";
import { MoveCoordinator } from "./MoveCoordinator.js";
import { SocialDirectory } from "./SocialDirectory.js";
import type { ManagerConfig } from "../config/env.js";

export interface ManagerRuntime {
  nodes: NodeRegistry;
  rooms: RoomCache;
  pending: PendingRequestMap;
  policy: PolicyEngine;
  reservations: ReservationTracker;
  coordinator: CrossCoordinator;
  router: RequestRouter;
  moves: MoveCoordinator;
  social: SocialDirectory;
}

export const createManagerRuntime = (config: ManagerConfig): ManagerRuntime => {
  const nodes = new NodeRegistry({
    circuitOpenMs: config.nodeCircuitOpenMs,
    failureThreshold: config.nodeCircuitFailureThreshold,
    backpressureLimitBytes: config.nodeBackpressureLimitBytes
  });
  const rooms = new RoomCache({ maxRooms: config.maxRooms, staleRoomMs: config.staleRoomMs });
  const reservations = new ReservationTracker({ ttlMs: config.reservationShadowTtlMs });
  const pending = new PendingRequestMap({ timeoutMs: config.commandTimeoutMs, maxPending: config.maxPendingCommands });
  const policy = new PolicyEngine({
    maxPlayersPerRoom: config.maxPlayersPerRoom,
    staleNodePenalty: 30,
    loadPenaltyWeight: 40,
    sameRegionBonus: 12,
    preferredFillTarget: 7
  });
  const coordinator = new CrossCoordinator(nodes, rooms, pending, { commandTimeoutMs: config.commandTimeoutMs });
  const router = new RequestRouter(rooms, nodes, policy, reservations, coordinator, {
    nodeStaleMs: config.nodeStaleMs,
    assignmentTtlMs: config.assignmentTtlMs,
    candidateRoomLimit: config.candidateRoomLimit
  });
  const social = new SocialDirectory({ baseUrl: config.socialApiUrl, friendsPath: config.socialFriendsPath, token: config.socialApiToken, ttlMs: config.socialCacheTtlMs });
  const moves = new MoveCoordinator(rooms, nodes, pending, coordinator, { moveBudgetPerCycle: config.moveBudgetPerCycle, moveTransactionTimeoutMs: config.moveTransactionTimeoutMs }, social);

  return { nodes, rooms, pending, policy, reservations, coordinator, router, moves, social };
};
