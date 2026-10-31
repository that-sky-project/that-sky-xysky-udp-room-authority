export type DebugEventCategory =
  | 'move'
  | 'level'
  | 'room'
  | 'player'
  | 'node'
  | 'telemetry'
  | 'allocation'
export interface DebugEvent {
  id: number
  at: number
  category: DebugEventCategory
  event: string
  message: string
  roomId?: string | undefined
  nodeId?: string | undefined
  playerIds?: string[] | undefined
  data?: Record<string, unknown> | undefined
}
export interface DebugEventInput {
  at?: number
  category: DebugEventCategory
  event: string
  message: string
  roomId?: string | undefined
  nodeId?: string | undefined
  playerIds?: string[] | undefined
  data?: Record<string, unknown> | undefined
}
export interface DebugEventQuery {
  since?: number | undefined
  limit?: number | undefined
  category?: string | undefined
  roomId?: string | undefined
  playerId?: string | undefined
}
export interface DebugEventPage {
  entries: DebugEvent[]
  nextSince: number
  oldestId: number | null
  newestId: number | null
  buffered: number
  capacity: number
  truncated: boolean
}
export class DebugEventLog {
  public readonly capacity: number
  private readonly buffer: Array<DebugEvent | undefined>
  private writeIndex = 0
  private size = 0
  private nextId = 1
  public constructor (capacity: number) {
    this.capacity = Math.max(1, Math.floor(capacity))
    this.buffer = new Array<DebugEvent | undefined>(this.capacity)
  }
  public record (input: DebugEventInput): DebugEvent {
    const { at, ...rest } = input
    const entry: DebugEvent = { id: this.nextId, at: at ?? Date.now(), ...rest }
    this.nextId += 1
    this.buffer[this.writeIndex] = entry
    this.writeIndex = (this.writeIndex + 1) % this.capacity
    if (this.size < this.capacity) this.size += 1
    return entry
  }
  public list (query: DebugEventQuery = {}): DebugEventPage {
    const limit = Math.max(1, Math.min(1000, Math.floor(query.limit ?? 200)))
    const category = nonEmpty(query.category)
    const roomId = nonEmpty(query.roomId)
    const playerId = nonEmpty(query.playerId)
    const since =
      typeof query.since === 'number' && Number.isFinite(query.since)
        ? query.since
        : undefined
    const matches: DebugEvent[] = []
    let oldestId: number | null = null
    let newestId: number | null = null
    for (let index = 0; index < this.size; index += 1) {
      const slot =
        (this.writeIndex - this.size + index + this.capacity * 2) %
        this.capacity
      const entry = this.buffer[slot]
      if (!entry) continue
      if (oldestId === null) oldestId = entry.id
      newestId = entry.id
      if (since !== undefined && entry.id <= since) continue
      if (category && entry.category !== category) continue
      if (roomId && entry.roomId !== roomId) continue
      if (playerId && !(entry.playerIds?.includes(playerId) ?? false)) continue
      matches.push(entry)
    }
    const entries =
      matches.length > limit ? matches.slice(matches.length - limit) : matches
    const last = entries[entries.length - 1]
    return {
      entries,
      nextSince: last?.id ?? (since !== undefined ? since : newestId ?? 0),
      oldestId,
      newestId,
      buffered: this.size,
      capacity: this.capacity,
      truncated:
        since !== undefined && oldestId !== null && since < oldestId - 1
    }
  }
}
const nonEmpty = (value: string | undefined): string | undefined => {
  const trimmed = value?.trim()
  return trimmed ? trimmed : undefined
}
