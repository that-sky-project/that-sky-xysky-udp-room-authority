import { z } from "zod";
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
} from "./branded.js";
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
} from "./branded.js";

const stringId = z.string().min(1).max(128);
const commandId = z.number().int().positive().transform(asCommandId);
const port = z.number().int().min(1).max(65535).transform(asPort);
const unixMs = z.number().int().nonnegative().transform(asUnixMs);
const protocolVersion = z.number().int().positive().transform(asProtocolVersion);

export const PlayerSchema = z
  .object({
    playerId: stringId.transform(asPlayerId),
    nickname: z.string().min(1).max(64).optional(),
    region: z.string().min(1).max(32).transform(asRegion).optional(),
    level: z.number().int().min(0).max(0xffffffff).optional(),
    metadata: z.record(z.unknown()).optional()
  })
  .strict();

export type PlayerInfo = z.infer<typeof PlayerSchema>;

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
  .strict();

export type RoomSnapshot = z.infer<typeof RoomSnapshotSchema>;

export const AllocationRequestSchema = z
  .object({
    playerId: stringId.transform(asPlayerId),
    region: z.string().min(1).max(32).transform(asRegion).optional(),
    level: z.number().int().min(0).max(0xffffffff).optional(),
    preferences: z.record(z.unknown()).optional()
  })
  .strict();

export interface IAllocationRequest {
  playerId: PlayerId;
  region?: Region | undefined;
  level?: number | undefined;
  preferences?: Record<string, unknown> | undefined;
}

export interface IAllocationReply {
  code: number;
  data?: RoomAssignment | { message: string; details?: unknown } | undefined;
}

export const NodeEventSchema = z
  .object({
    v: protocolVersion.default(asProtocolVersion(1)),
    event: z.enum(["player.join", "player.leave", "room.created", "room.updated", "room.destroyed", "heartbeat", "node.ready", "node.draining", "room.telemetry", "move.result"]),
    data: z
      .object({
        nodeId: stringId.transform(asNodeId).optional(),
        roomId: stringId.transform(asRoomId).optional(),
        port: port.optional(),
        udpHost: z.string().min(1).max(255).transform(asUdpHost).optional(),
        player: PlayerSchema.optional(),
        // Room lifecycle events use this field as an occupancy count; telemetry events use an array.
        players: z.union([
          z.number().int().min(0).max(8),
          z.array(z.object({
            playerId: stringId.transform(asPlayerId),
            levelId: z.number().int().nonnegative().optional(),
            position: z.tuple([z.number(), z.number(), z.number()]).optional(),
            velocity: z.number().nonnegative().optional(),
            stateAt: unixMs.optional(),
            friendIds: z.array(stringId.transform(asPlayerId)).max(32).optional(),
            affinity: z.record(z.number().min(0).max(1)).optional(),
            lastMoveAt: unixMs.optional()
          }).strict()).max(8)
        ]).optional(),
        seq: z.number().int().nonnegative().optional(),
        moveId: stringId.optional(),
        accepted: z.boolean().optional(),
        movedPlayerIds: z.array(stringId).max(8).optional(),
        failedPlayerIds: z.array(stringId).max(8).optional(),
        reason: z.string().max(128).optional(),
        occupiedPorts: z.array(port).max(65535).optional(),
        rooms: z.array(RoomSnapshotSchema).max(5000).optional(),
        version: z.number().int().nonnegative().optional(),
        capacity: z.number().int().min(0).optional(),
        load: z.number().min(0).max(1).optional(),
        timestamp: unixMs.optional()
      })
      .strict()
  })
  .strict();

export interface INodeEvent {
  v?: ProtocolVersion | undefined;
  event: "player.join" | "player.leave" | "room.created" | "room.updated" | "room.destroyed" | "heartbeat" | "node.ready" | "node.draining" | "room.telemetry" | "move.result";
  data: {
    nodeId: NodeId;
    roomId?: RoomId | undefined;
    port?: Port | undefined;
    udpHost?: UdpHost | undefined;
    player?: PlayerInfo | undefined;
    players?: PlayerTelemetry[] | number | undefined;
    seq?: number | undefined;
    moveId?: string | undefined;
    accepted?: boolean | undefined;
    movedPlayerIds?: string[] | undefined;
    failedPlayerIds?: string[] | undefined;
    reason?: string | undefined;
    occupiedPorts?: Port[] | undefined;
    rooms?: RoomSnapshot[] | undefined;
    version?: number | undefined;
    capacity?: number | undefined;
    load?: number | undefined;
    timestamp?: UnixMs | undefined;
  };
}

export interface PlayerTelemetry {
  playerId: PlayerId;
  levelId?: number | undefined;
  position?: [number, number, number] | undefined;
  velocity?: number | undefined;
  stateAt?: UnixMs | undefined;
  friendIds?: PlayerId[] | undefined;
  affinity?: Record<string, number> | undefined;
  lastMoveAt?: UnixMs | undefined;
}

export const ManagerCommandSchema = z
  .object({
    v: protocolVersion.default(asProtocolVersion(1)),
    id: commandId,
    cmd: z.enum(["player.redirect", "room.destroy", "node.drain", "room.reserve", "move.prepare", "move.commit", "move.cancel"]),
    data: z.record(z.unknown()).optional()
  })
  .strict();

export interface IManagerCommand<TData = unknown> {
  v?: ProtocolVersion | undefined;
  id: CommandId;
  cmd: "player.redirect" | "room.destroy" | "node.drain" | "room.reserve" | "move.prepare" | "move.commit" | "move.cancel";
  data?: TData | undefined;
}

export const NodeAckSchema = z
  .object({
    v: protocolVersion.default(asProtocolVersion(1)),
    id: commandId,
    code: z.number().int(),
    message: z.string().max(512).optional(),
    data: z.record(z.unknown()).optional()
  })
  .strict();

export interface INodeAck<TData = unknown> {
  v?: ProtocolVersion | undefined;
  id: CommandId;
  code: number;
  message?: string | undefined;
  data?: TData | undefined;
}

export interface RoomAssignment {
  roomId: RoomId;
  nodeId: NodeId;
  udpHost: UdpHost;
  udpPort: Port;
  expiresAt: UnixMs;
}
