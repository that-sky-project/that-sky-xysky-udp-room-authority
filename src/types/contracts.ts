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

export interface AudienceChatGlobalState {
  authorityEpoch: string
  revision: number
  levelId: number
  enabled: boolean
  remainingSeconds: number
  serverNowMs: number
  expiresAtMs: number
}
export type AudienceChatGlobalCommand = 'audience.chatGlobal'
export const AudienceChatGlobalRequestSchema = z.object({
  levelId: z.number().int().min(0).max(0xffffffff),
  enabled: z.boolean(),
  seconds: z.number().int().optional()
}).strict()

export interface LiveBroadcastCapability {
  authorityEpoch: string
  capabilityId: string
  revision: number
  levelId: number
  allowedPlayerIds: string[]
  serverNowMs: number
  expiresAtMs: number
}
export interface LiveBroadcastDelivery {
  authorityEpoch: string
  /** Destination room capability, never the source upload capability. */
  capabilityId: string
  levelId: number
  playerId: string
  streamId: string
  eventId: string
  eventSeq: number
  serverNowMs: number
  expiresAtMs: number
  /** QWD clock in seconds. */
  timestamp: number
  actionId: number
  payloadBase64: string
}
export type LiveBroadcastCommand = 'liveBroadcast.capability' | 'liveBroadcast.deliver'
export const LIVE_BROADCAST_MAX_PAYLOAD_BYTES = 48 * 1024
export const PlayerUuidSchema = z.string().regex(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i).transform(value => value.toLowerCase())
const liveId = z.string().min(1).max(128)
const liveLevel = z.number().int().min(0).max(0xffffffff)
export const LiveBroadcastPublishSchema = z.object({
  roomId: liveId, player: z.object({ playerId: PlayerUuidSchema }).passthrough(), levelId: liveLevel,
  capabilityId: liveId, sourceStreamId: liveId, eventId: liveId,
  timestamp: z.number().finite().min(0).max(Number.MAX_SAFE_INTEGER),
  actionId: z.number().int().min(0).max(0xffffffff),
  payloadBase64: z.string().max(4 * Math.ceil(LIVE_BROADCAST_MAX_PAYLOAD_BYTES / 3)).refine(value =>
    value.length > 0 && value.length % 4 === 0 &&
    /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value) &&
    Buffer.byteLength(value, 'base64') <= LIVE_BROADCAST_MAX_PAYLOAD_BYTES &&
    Buffer.from(value, 'base64').toString('base64') === value, 'invalid or oversized canonical base64')
})
export const LiveBroadcastSyncSchema = z.object({ roomId: liveId, levelIds: z.array(liveLevel).max(16) })

export interface AudienceReaction {
  flag7: number
  /** Ordinary audience reactions only; spotlight types 14/15 are excluded. */
  type: number
}
export interface AudienceReactionDelivery {
  authorityEpoch: string
  levelId: number
  playerId: string
  eventId: string
  /** QWD sequence scoped to (authorityEpoch, levelId, playerId); not a wire counter. */
  eventSeq: number
  serverNowMs: number
  expiresAtMs: number
  reaction: AudienceReaction
}
export const AudienceReactionSchema = z.object({
  flag7: z.number().int().min(0).max(7),
  type: z.number().int().min(0).max(13)
}).strict()

export interface PerformanceState {
  authorityEpoch: string
  levelId: number
  revision: number
  serverNowMs: number
  /** Preparation keeps its player at queue[0]; no broadcast session exists yet. */
  preparing: {
    preparationId: string
    performerId: string
    startedAtMs: number
    expiresAtMs: number
    durationMs: number
  } | null
  current: {
    sessionId: string
    performerId: string
    startedAtMs: number
    // The authoritative deadline is never shifted. Client StreamInfo deadline
    // encoding (including any performer-only compensation) belongs to the node.
    expiresAtMs: number
    durationMs: number
  } | null
  /** FIFO array order is the receiver's zero-based queue index; QWD emits no wire fields. */
  queue: Array<{ playerId: string; enqueuedAtMs: number }>
  lastEnded: {
    sessionId: string
    performerId: string
    endedAtMs: number
    reason: string
    revision: number
  } | null
}
export interface PerformanceDecision {
  requestId: string
  playerId: string
  requested: boolean
  status: 'started' | 'queued' | 'released' | 'cancelled' | 'rejected'
  reason?: string | undefined
  state: PerformanceState
}
export interface PerformanceDelivery {
  state: PerformanceState
  eventId: string
  eventSeq: number
  sessionId: string
  playerId: string
  timestamp: number
  actionId: number
  payloadBase64: string
}
export interface PerformanceGap {
  authorityEpoch: string
  levelId: number
  sessionId: string | null
  requestedCursor: number
  oldestAvailableSeq: number
  latestEventSeq: number
  reason: 'epoch_changed' | 'cursor_ahead' | 'journal_gap' | 'session_ended'
}
export type PerformanceCommand = 'performance.state' | 'performance.decision' | 'performance.deliver' | 'performance.gap'
export const PERFORMANCE_MAX_PAYLOAD_BYTES = 64 * 1024
const strictBase64 = z.string().max(4 * Math.ceil(PERFORMANCE_MAX_PAYLOAD_BYTES / 3))
  .refine(value => value.length > 0 && value.length % 4 === 0 &&
    /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value) &&
    Buffer.byteLength(value, 'base64') <= PERFORMANCE_MAX_PAYLOAD_BYTES &&
    Buffer.from(value, 'base64').toString('base64') === value, 'invalid or oversized canonical base64')

const stringId = z.string().min(1).max(128)
const commandId = z.number().int().positive().transform(asCommandId)
const port = z.number().int().min(1).max(65535).transform(asPort)
const unixMs = z.number().int().nonnegative().transform(asUnixMs)
const protocolVersion = z.number().int().positive().transform(asProtocolVersion)

export const PlayerSchema = z
  .object({
    playerId: stringId.transform(asPlayerId),
    nickname: z.string().min(1).max(64).optional(),
    region: z.string().min(1).max(32).transform(asRegion).optional(),
    level: z.number().int().min(0).max(0xffffffff).optional(),
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

const audienceReaction = z.object({
  roomId: stringId, player: PlayerSchema, levelId: z.number().int().positive(),
  eventId: stringId, reaction: AudienceReactionSchema
})
const performanceRequest = z.object({
  roomId: stringId, player: PlayerSchema, levelId: z.number().int().positive(),
  requestId: stringId, requested: z.boolean(), expectedSessionId: stringId.optional(), expectedPreparationId: stringId.optional()
})
const performancePublish = z.object({
  roomId: stringId, player: PlayerSchema, levelId: z.number().int().positive(),
  sessionId: stringId, eventId: stringId, timestamp: z.number().finite(),
  actionId: z.number().int().min(0).max(0xffffffff), payloadBase64: strictBase64
})
const performanceSync = z.object({
  roomId: stringId, levelIds: z.array(z.number().int().positive()).max(16),
  authorityEpoch: stringId.optional(),
  eventCursors: z.record(stringId, z.number().int().nonnegative().safe()).refine(value => Object.keys(value).length <= 16).optional()
})

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
      'reservation.expired',
      'seat.occupied',
      'seat.released',
      'audience.reaction',
      'performance.request',
      'performance.publish',
      'performance.sync',
      'liveBroadcast.sync',
      'liveBroadcast.publish'
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
        accepted: z.boolean().optional(),
        movedPlayerIds: z.array(stringId).max(8).optional(),
        failedPlayerIds: z.array(stringId).max(8).optional(),
        reason: z.string().max(128).optional(),
        levelId: z.number().int().nonnegative().optional(),
        benchId: z.number().int().nonnegative().optional(),
        seatId: z.number().int().min(0).max(7).optional(),
        entityId: z.number().int().nonnegative().optional(),
        position: z.tuple([z.number(), z.number(), z.number()]).optional(),
        levelTime: z.number().nonnegative().optional(),
        occupiedPorts: z.array(port).max(65535).optional(),
        rooms: z.array(RoomSnapshotSchema).max(5000).optional(),
        version: z.number().int().nonnegative().optional(),
        capacity: z.number().int().min(0).optional(),
        load: z.number().min(0).max(1).optional(),
        timestamp: z.number().finite().optional(),
        requestId: stringId.optional(), requested: z.boolean().optional(),
        expectedSessionId: stringId.optional(), expectedPreparationId: stringId.optional(), sessionId: stringId.optional(),
        capabilityId: liveId.optional(), sourceStreamId: liveId.optional(),
        eventId: stringId.optional(), actionId: z.number().optional(), payloadBase64: z.string().optional(),
        levelIds: z.array(z.number()).optional(), authorityEpoch: stringId.optional(),
        eventCursors: z.record(z.number()).optional(),
        reaction: AudienceReactionSchema.optional()
      })
      .passthrough()
  })
  .passthrough()
  .superRefine((value, ctx) => {
    const schema = value.event === 'performance.request' ? performanceRequest
      : value.event === 'performance.publish' ? performancePublish
      : value.event === 'performance.sync' ? performanceSync
      : value.event === 'liveBroadcast.publish' ? LiveBroadcastPublishSchema
      : value.event === 'liveBroadcast.sync' ? LiveBroadcastSyncSchema
      : value.event === 'audience.reaction' ? audienceReaction : undefined
    if (schema) {
      const parsed = schema.safeParse(value.data)
      if (!parsed.success) for (const issue of parsed.error.issues) ctx.addIssue({ ...issue, path: ['data', ...issue.path] })
    } else if (value.data.timestamp !== undefined && (!Number.isSafeInteger(value.data.timestamp) || value.data.timestamp < 0)) {
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
    | 'seat.occupied'
    | 'seat.released'
    | 'audience.reaction'
    | 'performance.request'
    | 'performance.publish'
    | 'performance.sync'
    | 'liveBroadcast.sync'
    | 'liveBroadcast.publish'
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
    accepted?: boolean | undefined
    movedPlayerIds?: string[] | undefined
    failedPlayerIds?: string[] | undefined
    reason?: string | undefined
    levelId?: number | undefined
    benchId?: number | undefined
    seatId?: number | undefined
    entityId?: number | undefined
    position?: [number, number, number] | undefined
    levelTime?: number | undefined
    requestId?: string | undefined
    requested?: boolean | undefined
    expectedSessionId?: string | undefined
    expectedPreparationId?: string | undefined
    sessionId?: string | undefined
    capabilityId?: string | undefined
    sourceStreamId?: string | undefined
    eventId?: string | undefined
    actionId?: number | undefined
    payloadBase64?: string | undefined
    levelIds?: number[] | undefined
    authorityEpoch?: string | undefined
    eventCursors?: Record<string, number> | undefined
    reaction?: AudienceReaction | undefined
    occupiedPorts?: Port[] | undefined
    rooms?: RoomSnapshot[] | undefined
    version?: number | undefined
    capacity?: number | undefined
    load?: number | undefined
    timestamp?: number | undefined
  }
}

export interface PlayerTelemetry {
  playerId: PlayerId
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
      'move.commit',
      'move.cancel',
      'seat.assign',
      'seat.release',
      'audience.snapshot',
      'audience.reaction',
      'audience.chatGlobal',
      'level.audience',
      'performance.state',
      'performance.decision',
      'performance.deliver',
      'performance.gap',
      'liveBroadcast.capability',
      'liveBroadcast.deliver'
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
    | 'move.commit'
    | 'move.cancel'
    | 'seat.assign'
    | 'seat.release'
    | 'audience.snapshot'
    | 'audience.reaction'
    | AudienceChatGlobalCommand
    | 'level.audience'
    | PerformanceCommand
    | LiveBroadcastCommand
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

/**
 * Chat is delivered per room, so the caller needs the room of every online
 * player at once. The single-player route would turn one broadcast into one
 * HTTP request per recipient.
 */
export const BatchRoomLookupSchema = z
  .object({
    playerIds: z.array(stringId).min(1).max(1000)
  })
  .strict()

export type BatchRoomLookup = z.infer<typeof BatchRoomLookupSchema>

/**
 * Concert seat reservation, forwarded by the account server on behalf of the
 * game client. `benchId`/`seatId` are present only when the client picked a
 * different bench (an in-game seat change); otherwise the manager assigns the
 * lowest free seat. `level` is the concert map, used only when a bench still
 * needs a UDP room allocated.
 *
 * `recentPlayerUuids` / `allPlayerUuids` mirror the client's own request fields
 * (`sub_141998B70` writes both for a free reservation): the people the client
 * wants to sit with. They are seated on the same bench as the caller, because a
 * bench is a UDP room and only same-room players render each other in full.
 * Both lists are merged and capped at one room's worth of companions.
 */
export const ReserveRequestSchema = z
  .object({
    playerId: stringId.transform(asPlayerId),
    levelId: z.number().int().nonnegative(),
    benchId: z.number().int().nonnegative().optional(),
    seatId: z.number().int().min(0).max(7).optional(),
    level: z.number().int().min(0).max(0xffffffff).optional(),
    recentPlayerUuids: z.array(stringId).max(8).optional(),
    allPlayerUuids: z.array(stringId).max(8).optional()
  })
  .strict()

export type IReserveRequest = z.infer<typeof ReserveRequestSchema>

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
