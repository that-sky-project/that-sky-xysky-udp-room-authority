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

export type RedirectReason = "merge" | "split" | "manual" | "capacity-rebalance" | "level-isolation" | "level-consolidation";

/**
 * OLD node builds validate the destination as a TgcUuid text and reject a bare
 * numeric room id. New room ids are 8-digit numbers, so send the same
 * deterministic uuid the node derives locally (RoomServer.destinationUuid).
 * This keeps cross-room moves working while the cluster is half upgraded.
 */
const destinationUuidText = (roomId: string): string | undefined => {
  const value = String(roomId ?? "");
  if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value)) return value;
  if (!/^\d{1,15}$/.test(value)) return undefined;
  const numeric = Number(value);
  if (!Number.isSafeInteger(numeric) || numeric > 0xffffffffffff) return undefined;
  return `00000000-0000-4000-8000-${numeric.toString(16).padStart(12, "0")}`;
};

export interface RedirectPlan {
  playerId: PlayerId;
  fromRoomId: RoomId;
  to: RoomAssignment;
  reason: RedirectReason;
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
    // Player locations are only maintained from player.join/leave events, so a
    // node reconnect can leave them missing. The source room already tells us
    // which node to address, so fall back to it instead of failing the move.
    const sourceNodeId = this.rooms.getPlayerLocation(firstPlayerId)?.nodeId
      ?? this.rooms.getRoom(plan.fromRoomId)?.nodeId;
    if (!sourceNodeId) throw new HermesError(ErrorCode.NOT_FOUND, "player location is unknown", { playerId: firstPlayerId, fromRoomId: plan.fromRoomId });
    const transport = this.nodes.getTransport(sourceNodeId);
    if (!transport || !transport.writable || !this.nodes.isAvailableForCommand(sourceNodeId)) {
      throw new HermesError(ErrorCode.NODE_UNAVAILABLE, "source node is not writable", { nodeId: sourceNodeId });
    }
    return this.sendToNode(sourceNodeId, {
      v: asProtocolVersion(1), cmd: "move.commit", data: {
        moveId: plan.moveId,
        playerIds: plan.playerIds,
        fromRoomId: plan.fromRoomId,
        toRoomId: plan.to.roomId,
        // Numeric room ids are not valid TgcUuid text; old nodes need this.
        toRoomUuid: destinationUuidText(plan.to.roomId),
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
