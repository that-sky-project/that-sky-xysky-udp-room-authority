import type { RoomSnapshot } from '../types/contracts.js'
import type { RoomCache } from './RoomCache.js'

export interface CapacityFacts {
  physicalPlayers: number
  capacity: number
  version: number
  activePlayers?: number
  reservedSlots?: number
}

/** Validate an entire fact set before using it to change an authoritative snapshot. */
export function readCapacityFacts(value: unknown): CapacityFacts | undefined {
  if (!value || typeof value !== 'object') return undefined
  const data = value as Record<string, unknown>
  const integer = (key: string, max: number): boolean => Number.isSafeInteger(data[key]) && Number(data[key]) >= 0 && Number(data[key]) <= max
  if (!integer('physicalPlayers', 8) || !integer('capacity', 8) || data.capacity === 0 ||
      !integer('version', Number.MAX_SAFE_INTEGER) || Number(data.physicalPlayers) > Number(data.capacity)) return undefined
  if (data.activePlayers !== undefined && (!integer('activePlayers', 8) || Number(data.activePlayers) > Number(data.physicalPlayers))) return undefined
  if (data.reservedSlots !== undefined && (!integer('reservedSlots', 8) || Number(data.reservedSlots) + Number(data.physicalPlayers) > Number(data.capacity))) return undefined
  return { physicalPlayers: Number(data.physicalPlayers), capacity: Number(data.capacity), version: Number(data.version),
    ...(data.activePlayers !== undefined ? { activePlayers: Number(data.activePlayers) } : {}),
    ...(data.reservedSlots !== undefined ? { reservedSlots: Number(data.reservedSlots) } : {}) }
}

export function applyCapacityFacts(rooms: RoomCache, expected: RoomSnapshot, value: unknown): boolean {
  const facts = readCapacityFacts(value)
  const current = rooms.getRoom(expected.roomId)
  if (!facts || !current || current.nodeId !== expected.nodeId || facts.version < current.version) return false
  // Remote reservedSlots is a diagnostic total, not an extra local hold: adding
  // it here would count our acknowledged allocation/move reservations twice.
  return rooms.upsertRoom({ ...current, players: facts.physicalPlayers, capacity: facts.capacity, version: facts.version })
}
