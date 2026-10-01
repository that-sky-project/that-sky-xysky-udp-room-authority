import type { NodeRegistry } from "./NodeRegistry.js";
import type { PendingRequestMap } from "./PendingRequestMap.js";
import type { RoomCache } from "./RoomCache.js";
import type { NodeId, PlayerId, RoomId } from "../types/branded.js";
import { asProtocolVersion } from "../types/branded.js";
import type { IManagerCommand, INodeAck, RoomAssignment, RoomSnapshot } from "../types/contracts.js";
import { ErrorCode, HermesError } from "../types/errors.js";

export interface CrossCoordinatorOptions {
  commandTimeoutMs: number;
}

export interface RedirectPlan {
  playerId: PlayerId;
  fromRoomId: RoomId;
  to: RoomAssignment;
  reason: "merge" | "split" | "manual" | "capacity-rebalance";
}

export class CrossCoordinator {
  public constructor(
    private readonly nodes: NodeRegistry,
    private readonly rooms: RoomCache,
    private readonly pending: PendingRequestMap,
    private readonly options: CrossCoordinatorOptions
  ) {}

  public async reserveRoom(room: RoomSnapshot, playerId: PlayerId, signal?: AbortSignal): Promise<INodeAck> {
    const transport = this.nodes.getTransport(room.nodeId);
    if (!transport || !transport.writable || !this.nodes.isAvailableForCommand(room.nodeId)) {
      throw new HermesError(ErrorCode.NODE_UNAVAILABLE, "target node is not writable", { nodeId: room.nodeId });
    }
    return this.sendToNode(
      room.nodeId,
      {
        v: asProtocolVersion(1),
        cmd: "room.reserve",
        data: {
          roomId: room.roomId,
          playerId,
          expectedVersion: room.version,
          ttlMs: this.options.commandTimeoutMs
        }
      },
      (command) => transport.send(command),
      signal
    );
  }

  public async redirectPlayers(plan: RedirectPlan & { playerIds: PlayerId[]; moveId?: string }, signal?: AbortSignal): Promise<INodeAck> {
    const firstPlayerId = plan.playerIds[0];
    if (!firstPlayerId) throw new HermesError(ErrorCode.BAD_REQUEST, "move has no players");
    const first = this.rooms.getPlayerLocation(firstPlayerId);
    if (!first) throw new HermesError(ErrorCode.NOT_FOUND, "player location is unknown", { playerId: plan.playerIds[0] });
    const transport = this.nodes.getTransport(first.nodeId);
    if (!transport || !transport.writable || !this.nodes.isAvailableForCommand(first.nodeId)) {
      throw new HermesError(ErrorCode.NODE_UNAVAILABLE, "source node is not writable", { nodeId: first.nodeId });
    }
    return this.sendToNode(first.nodeId, {
      v: asProtocolVersion(1), cmd: "move.commit", data: {
        moveId: plan.moveId,
        playerIds: plan.playerIds,
        fromRoomId: plan.fromRoomId,
        toRoomId: plan.to.roomId,
        udpHost: plan.to.udpHost,
        udpPort: plan.to.udpPort,
        reason: plan.reason
      }
    }, (command) => transport.send(command), signal);
  }

  private async sendToNode<TData>(
    nodeId: NodeId,
    command: Omit<IManagerCommand<TData>, "id">,
    sender: (command: IManagerCommand<TData>) => Promise<void>,
    signal?: AbortSignal
  ): Promise<INodeAck> {
    try {
      const ack = await this.pending.send(command, sender, signal);
      this.nodes.recordCommandSuccess(nodeId);
      return ack;
    } catch (error) {
      this.nodes.recordCommandFailure(nodeId);
      throw error;
    }
  }
}
