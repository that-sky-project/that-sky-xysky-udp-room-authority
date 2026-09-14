import type { CrossCoordinator } from "./CrossCoordinator.js";
import type { NodeRegistry } from "./NodeRegistry.js";
import type { PolicyEngine } from "./PolicyEngine.js";
import type { ReservationTracker } from "./ReservationTracker.js";
import type { RoomCache } from "./RoomCache.js";
import { asUnixMs, type PlayerId } from "../types/branded.js";
import type { IAllocationReply, IAllocationRequest, RoomAssignment } from "../types/contracts.js";
import { ErrorCode, HermesError, isHermesError } from "../types/errors.js";

export interface RequestRouterOptions {
  nodeStaleMs: number;
  assignmentTtlMs: number;
  candidateRoomLimit: number;
}

export class RequestRouter {
  public constructor(
    private readonly rooms: RoomCache,
    private readonly nodes: NodeRegistry,
    private readonly policy: PolicyEngine,
    private readonly reservations: ReservationTracker,
    private readonly coordinator: CrossCoordinator,
    private readonly options: RequestRouterOptions
  ) {}

  public async allocate(request: IAllocationRequest, signal?: AbortSignal): Promise<IAllocationReply> {
    try {
      return await this.requestRoom(request, signal);
    } catch (error) {
      if (isHermesError(error)) {
        return { code: error.code, data: { message: error.message, details: error.details } };
      }
      return { code: ErrorCode.INTERNAL, data: { message: error instanceof Error ? error.message : String(error) } };
    }
  }

  private async requestRoom(request: IAllocationRequest, signal?: AbortSignal): Promise<IAllocationReply> {
    const playerId = request.playerId;
    const knownLocation = this.rooms.getPlayerLocation(playerId);
    if (knownLocation) {
      const room = this.rooms.getRoom(knownLocation.roomId);
      if (room) return { code: ErrorCode.OK, data: this.assignmentFor(room, playerId) };
    }

    const nodeHealth = new Map(this.nodes.listHealthy(this.options.nodeStaleMs).map((node) => [node.nodeId as string, node]));
    const context = {
      playerId,
      region: request.region,
      level: request.level,
      preferences: request.preferences
    };

    const candidates = this.rooms.listCandidateRooms({
      region: request.region,
      preferredFillTarget: 7,
      limit: this.options.candidateRoomLimit
    });

    while (candidates.length > 0) {
      const selection = this.policy.selectRoom(context, candidates, nodeHealth, (room) => this.reservations.effectivePlayers(room));
      if (!selection) break;

      const index = candidates.findIndex((room) => room.roomId === selection.room.roomId);
      if (index >= 0) candidates.splice(index, 1);

      if (!this.reservations.tryAcquire(selection.room, playerId)) continue;

      try {
        const ack = await this.coordinator.reserveRoom(selection.room, playerId, signal);
        if (ack.code !== ErrorCode.OK) {
          this.reservations.release(playerId);
          throw new HermesError(ack.code, ack.message ?? "node rejected room reservation", ack.data);
        }

        return { code: ErrorCode.OK, data: this.assignmentFor(selection.room, playerId) };
      } catch (error) {
        this.reservations.release(playerId);
        if (isHermesError(error) && error.code === ErrorCode.NODE_UNAVAILABLE) continue;
        throw error;
      }
    }

    throw new HermesError(ErrorCode.NO_CAPACITY, "no room capacity is currently available");
  }

  private assignmentFor(
    room: { roomId: RoomAssignment["roomId"]; nodeId: RoomAssignment["nodeId"]; udpHost: RoomAssignment["udpHost"]; udpPort: RoomAssignment["udpPort"] },
    playerId: PlayerId
  ): RoomAssignment {
    const expiresAt = asUnixMs(Date.now() + this.options.assignmentTtlMs);
    return {
      roomId: room.roomId,
      nodeId: room.nodeId,
      udpHost: room.udpHost,
      udpPort: room.udpPort,
      expiresAt
    };
  }
}
