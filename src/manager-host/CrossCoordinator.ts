import type { NodeRegistry } from './NodeRegistry.js'
import type { PendingRequestMap } from './PendingRequestMap.js'
import type { RoomCache } from './RoomCache.js'
import type { NodeId, PlayerId, RoomId } from '../types/branded.js'
import { asProtocolVersion } from '../types/branded.js'
import type {
  IManagerCommand,
  INodeAck,
  RoomAssignment,
  RoomSnapshot
} from '../types/contracts.js'
import { ErrorCode, HermesError } from '../types/errors.js'
export interface CrossCoordinatorOptions {
  commandTimeoutMs: number
  reservationTtlMs?: number
}
export type RedirectReason =
  | 'merge'
  | 'split'
  | 'manual'
  | 'capacity-rebalance'
  | 'level-isolation'
  | 'level-consolidation'
const destinationUuidText = (roomId: string): string | undefined => {
  const value = String(roomId ?? '')
  if (
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
      value
    )
  )
    return value
  if (!/^\d{1,15}$/.test(value)) return undefined
  const numeric = Number(value)
  if (!Number.isSafeInteger(numeric) || numeric > 0xffffffffffff)
    return undefined
  return `00000000-0000-4000-8000-${numeric.toString(16).padStart(12, '0')}`
}
export interface SourceParticipantSnapshot {
  playerId: PlayerId
  sessionId?: string | undefined
  levelId?: number | undefined
  lvSeq?: number | undefined
}
export interface RedirectPlan {
  playerId: PlayerId
  fromRoomId: RoomId
  to: RoomAssignment
  reason: RedirectReason
}
export class CrossCoordinator {
  public constructor (
    private readonly nodes: NodeRegistry,
    private readonly rooms: RoomCache,
    private readonly pending: PendingRequestMap,
    private readonly options: CrossCoordinatorOptions
  ) {}
  public async reserveRoom (
    room: RoomSnapshot,
    playerId: PlayerId,
    signal?: AbortSignal
  ): Promise<INodeAck> {
    const transport = this.nodes.getTransport(room.nodeId)
    if (
      !transport ||
      !transport.writable ||
      !this.nodes.isAvailableForCommand(room.nodeId)
    ) {
      throw new HermesError(
        ErrorCode.NODE_UNAVAILABLE,
        'target node is not writable',
        { nodeId: room.nodeId }
      )
    }
    return this.sendToNode(
      room.nodeId,
      {
        v: asProtocolVersion(1),
        cmd: 'room.reserve',
        data: {
          roomId: room.roomId,
          playerId,
          expectedVersion: room.version,
          ttlMs: this.options.reservationTtlMs ?? 30_000
        }
      },
      command => transport.send(command),
      signal
    )
  }
  public async redirectPlayers (
    plan: RedirectPlan & {
      playerIds: PlayerId[]; moveId?: string; sourceNodeId?: NodeId;
      sourceConnectedAt?: number; expectedParticipants?: SourceParticipantSnapshot[];
      reservationToken?: string | undefined
    },
    signal?: AbortSignal
  ): Promise<INodeAck> {
    const firstPlayerId = plan.playerIds[0]
    if (!firstPlayerId)
      throw new HermesError(ErrorCode.BAD_REQUEST, 'move has no players')
    const sourceNodeId = plan.sourceNodeId ?? this.rooms.getRoom(plan.fromRoomId)?.nodeId
    if (!sourceNodeId)
      throw new HermesError(ErrorCode.NOT_FOUND, 'player location is unknown', {
        playerId: firstPlayerId,
        fromRoomId: plan.fromRoomId
      })
    const transport = this.nodes.getTransport(sourceNodeId)
    if (
      !transport ||
      !transport.writable ||
      !this.nodes.isAvailableForCommand(sourceNodeId)
    ) {
      throw new HermesError(
        ErrorCode.NODE_UNAVAILABLE,
        'source node is not writable',
        { nodeId: sourceNodeId }
      )
    }
    if (plan.sourceConnectedAt !== undefined && transport.connectedAt !== plan.sourceConnectedAt) {
      throw new HermesError(ErrorCode.STALE_PLAN, 'source connection changed during prepare')
    }
    for (const playerId of plan.playerIds) {
      const location = this.rooms.getPlayerLocation(playerId)
      if (!location || location.roomId !== plan.fromRoomId || location.nodeId !== sourceNodeId || location.leavingAt !== undefined) {
        throw new HermesError(ErrorCode.STALE_PLAN, 'source participant location changed during prepare')
      }
    }
    return this.sendToNode(
      sourceNodeId,
      {
        v: asProtocolVersion(1),
        cmd: 'move.commit',
        data: {
          moveId: plan.moveId,
          playerIds: plan.playerIds,
          fromRoomId: plan.fromRoomId,
          toRoomId: plan.to.roomId,
          toRoomUuid: destinationUuidText(plan.to.roomId),
          udpHost: plan.to.udpHost,
          udpPort: plan.to.udpPort,
          reason: plan.reason,
          expiresAt: plan.to.expiresAt,
          ...(plan.expectedParticipants ? { expectedParticipants: plan.expectedParticipants } : {}),
          ...(plan.reservationToken ? { reservationToken: plan.reservationToken } : {})
        }
      },
      command => transport.send(command),
      signal
    )
  }
  private async sendToNode<TData> (
    nodeId: NodeId,
    command: Omit<IManagerCommand<TData>, 'id'>,
    sender: (command: IManagerCommand<TData>) => Promise<void>,
    signal?: AbortSignal
  ): Promise<INodeAck> {
    try {
      const ack = await this.pending.send(command, sender, signal, nodeId as string)
      this.nodes.recordCommandSuccess(nodeId)
      return ack
    } catch (error) {
      this.nodes.recordCommandFailure(nodeId)
      throw error
    }
  }
}
