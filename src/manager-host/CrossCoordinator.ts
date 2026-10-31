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
          ttlMs: this.options.commandTimeoutMs
        }
      },
      command => transport.send(command),
      signal
    )
  }
  public async seatPlayer (
    room: RoomSnapshot,
    seat: {
      playerId: PlayerId
      levelId: number
      benchId: number
      seatId: number
      entityId: number
      roomMembers?: Array<{ benchId: number; seatId: number }> | undefined
    },
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
    const data: Record<string, unknown> = {
      roomId: room.roomId,
      playerId: seat.playerId,
      levelId: seat.levelId,
      benchId: seat.benchId,
      seatId: seat.seatId,
      entityId: seat.entityId
    }
    if (seat.roomMembers?.length) data.roomMembers = seat.roomMembers
    return this.sendToNode(
      room.nodeId,
      { v: asProtocolVersion(1), cmd: 'seat.assign', data },
      command => transport.send(command),
      signal
    )
  }
  public async audienceSnapshot (
    room: RoomSnapshot,
    snapshot: {
      levelId: number
      seats: Array<{
        playerId: PlayerId
        entityId: number
        benchId: number
        seatId: number
        position: [number, number, number] | null
        levelTime: number
        spotlight?: boolean | undefined
      }>
      removed?: string[] | undefined
      /** Payload carries every remote seat; the node may drop unlisted silhouettes. */
      full?: boolean | undefined
      /** The level's performance session, when someone is on stage. */
      spotlightSession?: {
        sessionId: string
        performerId: string
        startedAt: number
      } | null | undefined
    },
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
        cmd: 'audience.snapshot',
        data: {
          roomId: room.roomId,
          levelId: snapshot.levelId,
          seats: snapshot.seats.map(seat => ({
            playerId: seat.playerId,
            entityId: seat.entityId,
            benchId: seat.benchId,
            seatId: seat.seatId,
            position: seat.position,
            levelTime: seat.levelTime,
            // Hall-wide stage state: the receiving node flips the entity into
            // the spotlight render path (type 0xE + stage extra).
            spotlight: seat.spotlight ?? false
          })),
          removed: snapshot.removed ?? [],
          full: snapshot.full ?? false,
          spotlightSession: snapshot.spotlightSession ?? null
        }
      },
      command => transport.send(command),
      signal
    )
  }
  public async levelAudience (
    room: RoomSnapshot,
    payload: {
      levelId: number
      players: Array<{
        playerId: PlayerId
        roomId: RoomId
        position: [number, number, number] | null
        levelTime: number
      }>
      removed?: string[] | undefined
    },
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
        cmd: 'level.audience',
        data: {
          roomId: room.roomId,
          levelId: payload.levelId,
          players: payload.players.map(player => ({
            playerId: player.playerId,
            roomId: player.roomId,
            position: player.position,
            levelTime: player.levelTime
          })),
          removed: payload.removed ?? []
        }
      },
      command => transport.send(command),
      signal
    )
  }
  public async seatRelease (
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
        cmd: 'seat.release',
        data: { roomId: room.roomId, playerId }
      },
      command => transport.send(command),
      signal
    )
  }
  public async redirectPlayers (
    plan: RedirectPlan & { playerIds: PlayerId[]; moveId?: string },
    signal?: AbortSignal
  ): Promise<INodeAck> {
    const firstPlayerId = plan.playerIds[0]
    if (!firstPlayerId)
      throw new HermesError(ErrorCode.BAD_REQUEST, 'move has no players')
    const sourceNodeId =
      this.rooms.getPlayerLocation(firstPlayerId)?.nodeId ??
      this.rooms.getRoom(plan.fromRoomId)?.nodeId
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
          reason: plan.reason
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
