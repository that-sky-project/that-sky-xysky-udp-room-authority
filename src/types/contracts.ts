import { z } from 'zod'
import type {
  CommandId,
  NodeId,
  PlayerId,
  Port,
  ProtocolVersion,
  Region,
  RoomId,
  UdpHost,
  UnixMs
} from './branded.js'
import {
  asCommandId,
  asNodeId,
  asPlayerId,
  asPort,
  asProtocolVersion,
  asRegion,
  asRoomId,
  asUdpHost,
  asUnixMs
} from './branded.js'

const stringId = z.string().min(1).max(128)
// Node ConnectionSession uses positive numeric ids. Normalize only validated
// numeric ids; do not coerce null, booleans or arbitrary objects into identities.
const sessionId = z.union([
  stringId,
  z.number().int().positive().safe().transform(value => String(value))
])
const commandId = z.number().int().positive().transform(asCommandId)
const port = z.number().int().min(1).max(65535).transform(asPort)
const unixMs = z.number().int().nonnegative().transform(asUnixMs)
const protocolVersion = z.number().int().positive().transform(asProtocolVersion)

/** Preserve ordinary extension metadata, but never retain retired feature telemetry. */
const omitRetiredFields = <T extends Record<string, unknown>>(
  value: T,
  fields: readonly string[]
): T => {
  const result = { ...value }
  for (const field of fields) delete result[field]
  return result
}

export const PlayerSchema = z
  .object({
    playerId: stringId.transform(asPlayerId),
    nickname: z.string().min(1).max(64).optional(),
    region: z.string().min(1).max(32).transform(asRegion).optional(),
    level: z.number().int().min(0).max(0xffffffff).optional(),
    sessionId: sessionId.optional(),
    lvSeq: z.number().int().nonnegative().optional(),
    metadata: z.record(z.unknown()).optional()
  })
  .strict()

export type PlayerInfo = z.infer<typeof PlayerSchema>

export const RoomSnapshotSchema = z
  .object({
    roomId: stringId.transform(asRoomId),
    nodeId: stringId.transform(asNodeId),
    udpHost: z.string().min(1).max(255).transform(asUdpHost),
    udpPort: port,
    players: z.number().int().min(0).max(8),
    capacity: z.number().int().min(1).max(8).default(8),
    region: z.string().min(1).max(32).transform(asRegion).optional(),
    version: z.number().int().nonnegative().default(0),
    draining: z.boolean().default(false),
    lastChangedAt: unixMs.optional(),
    lastMergeAt: unixMs.optional(),
    lastSplitAt: unixMs.optional()
  })
  .strict()

export type RoomSnapshot = z.infer<typeof RoomSnapshotSchema>

export const AllocationRequestSchema = z
  .object({
    playerId: stringId.transform(asPlayerId),
    region: z.string().min(1).max(32).transform(asRegion).optional(),
    level: z.number().int().min(0).max(0xffffffff).optional(),
    preferences: z.record(z.unknown()).optional()
  })
  .strict()

export interface IAllocationRequest {
  playerId: PlayerId
  region?: Region | undefined
  level?: number | undefined
  preferences?: Record<string, unknown> | undefined
}

export interface IAllocationReply {
  code: number
  data?: RoomAssignment | { message: string; details?: unknown } | undefined
}

export const NodeEventSchema = z
  .object({
    v: protocolVersion.default(asProtocolVersion(1)),
    event: z.enum([
      'player.join',
      'player.leave',
      'room.created',
      'room.updated',
      'room.destroyed',
      'heartbeat',
      'node.ready',
      'node.draining',
      'room.telemetry',
      'move.result',
      'move.completed',
      'reservation.expired'
    ]),
    data: z
      .object({
        nodeId: stringId.transform(asNodeId).optional(),
        roomId: stringId.transform(asRoomId).optional(),
        port: port.optional(),
        udpHost: z.string().min(1).max(255).transform(asUdpHost).optional(),
        player: PlayerSchema.optional(),
        players: z
          .union([
            z.number().int().min(0).max(8),
            z
              .array(
                z
                  .object({
                    playerId: stringId.transform(asPlayerId),
                    sessionId: sessionId.optional(),
                    lvSeq: z.number().int().nonnegative().optional(),
                    status: z.enum(['ACTIVE', 'MOVING']).optional(),
                    levelId: z.number().int().nonnegative().optional(),
                    position: z
                      .tuple([z.number(), z.number(), z.number()])
                      .optional(),
                    velocity: z.number().nonnegative().optional(),
                    stateAt: unixMs.optional(),
                    affinity: z.record(z.number().min(0).max(1)).optional(),
                    lastMoveAt: unixMs.optional()
                  })
                  .passthrough()
                  .transform(value => omitRetiredFields(value, ['benchId', 'seatId']))
              )
              .max(8)
          ])
          .optional(),
        kind: z.enum(['full', 'delta']).optional(),
        baseSeq: z.number().int().nonnegative().optional(),
        removedPlayers: z.array(stringId).max(8).optional(),
        seq: z.number().int().nonnegative().optional(),
        moveId: z.string().min(1).max(256).optional(),
        playerIds: z.array(stringId).max(8).optional(),
        admittedPlayerIds: z.array(stringId).max(8).optional(),
        accepted: z.boolean().optional(),
        movedPlayerIds: z.array(stringId).max(8).optional(),
        failedPlayerIds: z.array(stringId).max(8).optional(),
        reason: z.string().max(128).optional(),
        occupiedPorts: z.array(port).max(65535).optional(),
        rooms: z.array(RoomSnapshotSchema).max(5000).optional(),
        version: z.number().int().nonnegative().optional(),
        connectedAt: unixMs.optional(),
        sessionId: sessionId.optional(),
        lvSeq: z.number().int().nonnegative().optional(),
        physicalPlayers: z.number().int().min(0).max(8).optional(),
        activePlayers: z.number().int().min(0).max(8).optional(),
        reservedSlots: z.number().int().min(0).max(8).optional(),
        expiresAt: unixMs.optional(),
        reservationToken: stringId.optional(),
        phase: z.enum(['source_commit', 'source_ack', 'target_arrival']).optional(),
        capacity: z.number().int().min(0).optional(),
        load: z.number().min(0).max(1).optional(),
        timestamp: z.number().finite().optional()
      })
      .passthrough()
      .transform(value => omitRetiredFields(value, [
        'benchId', 'seatId', 'entityId', 'levelTime',
        'requestId', 'requested', 'expectedSessionId', 'expectedPreparationId',
        'capabilityId', 'sourceStreamId', 'eventId', 'actionId',
        'payloadBase64', 'levelIds', 'authorityEpoch', 'eventCursors', 'reaction'
      ]))
  })
  .passthrough()
  .superRefine((value, ctx) => {
    if (value.data.timestamp !== undefined && (!Number.isSafeInteger(value.data.timestamp) || value.data.timestamp < 0)) {
      ctx.addIssue({ code: z.ZodIssueCode.custom, path: ['data', 'timestamp'], message: 'expected Unix milliseconds' })
    }
  })

export interface INodeEvent {
  v?: ProtocolVersion | undefined
  event:
    | 'player.join'
    | 'player.leave'
    | 'room.created'
    | 'room.updated'
    | 'room.destroyed'
    | 'heartbeat'
    | 'node.ready'
    | 'node.draining'
    | 'room.telemetry'
    | 'move.result'
    | 'move.completed'
    | 'reservation.expired'
  data: {
    nodeId: NodeId
    roomId?: RoomId | undefined
    port?: Port | undefined
    udpHost?: UdpHost | undefined
    player?: PlayerInfo | undefined
    players?: PlayerTelemetry[] | number | undefined
    kind?: 'full' | 'delta' | undefined
    baseSeq?: number | undefined
    removedPlayers?: string[] | undefined
    seq?: number | undefined
    moveId?: string | undefined
    playerIds?: string[] | undefined
    admittedPlayerIds?: string[] | undefined
    accepted?: boolean | undefined
    movedPlayerIds?: string[] | undefined
    failedPlayerIds?: string[] | undefined
    reason?: string | undefined
    occupiedPorts?: Port[] | undefined
    rooms?: RoomSnapshot[] | undefined
    version?: number | undefined
    connectedAt?: UnixMs | undefined
    sessionId?: string | undefined
    lvSeq?: number | undefined
    physicalPlayers?: number | undefined
    activePlayers?: number | undefined
    reservedSlots?: number | undefined
    expiresAt?: UnixMs | undefined
    reservationToken?: string | undefined
    phase?: 'source_commit' | 'source_ack' | 'target_arrival' | undefined
    capacity?: number | undefined
    load?: number | undefined
    timestamp?: number | undefined
  }
}

export interface PlayerTelemetry {
  playerId: PlayerId
  sessionId?: string | undefined
  lvSeq?: number | undefined
  status?: 'ACTIVE' | 'MOVING' | undefined
  levelId?: number | undefined
  position?: [number, number, number] | undefined
  velocity?: number | undefined
  stateAt?: UnixMs | undefined
  affinity?: Record<string, number> | undefined
  lastMoveAt?: UnixMs | undefined
}

export const ManagerCommandSchema = z
  .object({
    v: protocolVersion.default(asProtocolVersion(1)),
    id: commandId,
    cmd: z.enum([
      'player.redirect',
      'room.destroy',
      'node.drain',
      'room.reserve',
      'move.prepare',
      'move.renew',
      'move.commit',
      'move.cancel',
      'room.sync'
    ]),
    data: z.record(z.unknown()).optional()
  })
  .strict()

export interface IManagerCommand<TData = unknown> {
  v?: ProtocolVersion | undefined
  id: CommandId
  cmd:
    | 'player.redirect'
    | 'room.destroy'
    | 'node.drain'
    | 'room.reserve'
    | 'move.prepare'
    | 'move.renew'
    | 'move.commit'
    | 'move.cancel'
    | 'room.sync'
  data?: TData | undefined
}

export const NodeAckSchema = z
  .object({
    v: protocolVersion.default(asProtocolVersion(1)),
    id: commandId,
    code: z.number().int(),
    message: z.string().max(512).optional(),
    data: z.record(z.unknown()).optional()
  })
  .strict()

export interface INodeAck<TData = unknown> {
  v?: ProtocolVersion | undefined
  id: CommandId
  code: number
  message?: string | undefined
  data?: TData | undefined
}

export interface RoomAssignment {
  roomId: RoomId
  nodeId: NodeId
  udpHost: UdpHost
  udpPort: Port
  expiresAt: UnixMs
}

/** Batched ordinary room lookup avoids one HTTP request per recipient. */
export const BatchRoomLookupSchema = z
  .object({
    playerIds: z.array(stringId).min(1).max(1000)
  })
  .strict()

export type BatchRoomLookup = z.infer<typeof BatchRoomLookupSchema>

/**
 * A player the manager does not know (never placed, or already left) maps to
 * `null`: "not in a room" is a normal state, not a failed lookup, and the caller
 * must be able to tell it apart from an error.
 *
 * The shape matches the existing GET /players/:id/room reply so both routes can
 * share one builder and current consumers keep working unchanged.
 */
export interface RoomLookup {
  playerId: PlayerId
  roomId: RoomId
  udp: { host: UdpHost; port: Port }
  players: number
  capacity: number
  level: number | null
  levelId: number | null
  updatedAt: UnixMs
}
