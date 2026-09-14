export type Brand<T, TBrand extends string> = T & { readonly __brand: TBrand };

export type PlayerId = Brand<string, "PlayerId">;
export type RoomId = Brand<string, "RoomId">;
export type NodeId = Brand<string, "NodeId">;
export type Region = Brand<string, "Region">;
export type UdpHost = Brand<string, "UdpHost">;
export type Port = Brand<number, "Port">;
export type CommandId = Brand<number, "CommandId">;
export type UnixMs = Brand<number, "UnixMs">;
export type ProtocolVersion = Brand<number, "ProtocolVersion">;

export const asPlayerId = (value: string): PlayerId => value as PlayerId;
export const asRoomId = (value: string): RoomId => value as RoomId;
export const asNodeId = (value: string): NodeId => value as NodeId;
export const asRegion = (value: string): Region => value as Region;
export const asUdpHost = (value: string): UdpHost => value as UdpHost;
export const asPort = (value: number): Port => value as Port;
export const asCommandId = (value: number): CommandId => value as CommandId;
export const asUnixMs = (value: number): UnixMs => value as UnixMs;
export const asProtocolVersion = (value: number): ProtocolVersion => value as ProtocolVersion;
