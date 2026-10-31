import type { PlayerId, RoomId } from '../types/branded.js'
export const SEATS_PER_BENCH = 8
export const BENCH_COUNT = 1267
export interface Seat {
  benchId: number
  seatId: number
}
export interface Seated extends Seat {
  levelId: number
}
export const AURORA_CONCERT_LEVEL = 263580627
export const AURORA_HALL_LEVEL = 1559613961
export const SEATED_LEVELS: ReadonlySet<number> = new Set([
  AURORA_CONCERT_LEVEL,
  AURORA_HALL_LEVEL
])
export const CONCERT_LEVEL_ID = AURORA_CONCERT_LEVEL
export const levelNeedsSeat = (levelId: number): boolean =>
  SEATED_LEVELS.has(Number(levelId))
export interface BenchRoom {
  host: string
  port: number
  roomId: RoomId
}
export interface ReserveRequest {
  playerId: PlayerId
  levelId: number
  benchId?: number | undefined
  seatId?: number | undefined
}
export class SeatAllocator {
  private readonly byLevel = new Map<number, Map<PlayerId, Seat>>()
  /** levelId -> occupied flat seat ids (`benchId * seatsPerBench + seatId`). */
  private readonly occupancy = new Map<number, Set<number>>()
  private readonly positionsByLevel = new Map<
    number,
    Map<
      PlayerId,
      {
        position: [number, number, number]
        levelTime: number
        roomId?: RoomId | undefined
      }
    >
  >()
  private readonly benchRooms = new Map<number, BenchRoom>()
  public constructor (
    private readonly benchCount: number = BENCH_COUNT,
    private readonly seatsPerBench: number = SEATS_PER_BENCH
  ) {}
  public bindBenchRoom (benchId: number, room: BenchRoom): void {
    this.benchRooms.set(benchId, { ...room })
  }
  public roomForBench (benchId: number): BenchRoom | undefined {
    return this.benchRooms.get(benchId)
  }
  /** Drop a bench binding so a later attempt can reclaim it. */
  public unbindBenchRoom (benchId: number): void {
    this.benchRooms.delete(benchId)
  }
  public get benchCapacity (): number {
    return this.benchCount
  }
  public allocateOnBench (
    playerId: PlayerId,
    levelId: number,
    benchId: number
  ): Seated | undefined {
    if (!Number.isInteger(benchId) || benchId < 0 || benchId >= this.benchCount)
      return undefined
    for (let seatId = 0; seatId < this.seatsPerBench; seatId += 1) {
      const candidate = { benchId, seatId }
      if (!this.isFree(levelId, candidate)) continue
      this.claim(levelId, playerId, candidate)
      return { ...candidate, levelId }
    }
    return undefined
  }
  public allocate (request: ReserveRequest): Seated {
    const { playerId, levelId } = request
    const seating = this.seatingFor(levelId)
    const wanted = this.normalize(request)
    const existing = seating.get(playerId)
    if (existing && !wanted) return { ...existing, levelId }
    if (
      wanted &&
      (!existing ||
        existing.benchId !== wanted.benchId ||
        existing.seatId !== wanted.seatId)
    ) {
      if (this.isFree(levelId, wanted)) {
        this.claim(levelId, playerId, wanted)
        return { ...wanted, levelId }
      }
    }
    if (existing) return { ...existing, levelId }
    const free = this.lowestFree(levelId)
    if (!free) {
      throw new Error(
        `no free seat in event ${levelId} (${this.benchCount}x${this.seatsPerBench})`
      )
    }
    this.claim(levelId, playerId, free)
    return { ...free, levelId }
  }
  public allocateGroup (
    request: ReserveRequest,
    playerIds: PlayerId[]
  ): Map<PlayerId, Seated> {
    const { levelId } = request
    const seating = this.seatingFor(levelId)
    const taken = new Map<PlayerId, Seated>()
    const pending: PlayerId[] = []
    for (const playerId of playerIds) {
      const existing = seating.get(playerId)
      if (existing) {
        taken.set(playerId, { ...existing, levelId })
        continue
      }
      if (taken.has(playerId)) continue
      pending.push(playerId)
    }
    if (pending.length === 0) return taken
    const seatedMembers = [...taken.values()]
    const anchor =
      seatedMembers[0]?.benchId ??
      this.normalize(request)?.benchId ??
      this.findBenchWithRoom(levelId, pending.length)
    if (anchor === undefined) return taken
    let seatId = 0
    for (const playerId of pending) {
      while (
        seatId < this.seatsPerBench &&
        !this.isFree(levelId, { benchId: anchor, seatId })
      ) {
        seatId += 1
      }
      if (seatId >= this.seatsPerBench) break
      const seat = { benchId: anchor, seatId }
      this.claim(levelId, playerId, seat)
      taken.set(playerId, { ...seat, levelId })
      seatId += 1
    }
    return taken
  }
  private findBenchWithRoom (
    levelId: number,
    count: number
  ): number | undefined {
    if (count > this.seatsPerBench) return undefined
    for (let benchId = 0; benchId < this.benchCount; benchId += 1) {
      let free = 0
      for (
        let seatId = 0;
        seatId < this.seatsPerBench && free < count;
        seatId += 1
      ) {
        if (this.isFree(levelId, { benchId, seatId })) free += 1
      }
      if (free >= count) return benchId
    }
    return undefined
  }
  public setPosition (
    levelId: number,
    playerId: PlayerId,
    position: [number, number, number],
    levelTime: number,
    roomId?: RoomId
  ): void {
    let table = this.positionsByLevel.get(levelId)
    if (!table) {
      table = new Map<
        PlayerId,
        {
          position: [number, number, number]
          levelTime: number
          roomId?: RoomId | undefined
        }
      >()
      this.positionsByLevel.set(levelId, table)
    }
    const previous = table.get(playerId)
    table.set(playerId, {
      position,
      levelTime,
      roomId: roomId ?? previous?.roomId
    })
  }
  public levels (): number[] {
    const out: number[] = []
    for (const [levelId, seating] of this.byLevel) {
      if (seating.size > 0) out.push(levelId)
    }
    return out
  }
  public snapshot (levelId: number): Array<{
    playerId: PlayerId
    benchId: number
    seatId: number
    entityId: number
    position: [number, number, number] | null
    levelTime: number
    roomId: RoomId | null
  }> {
    const seating = this.byLevel.get(levelId)
    if (!seating) return []
    const positions = this.positionsByLevel.get(levelId)
    const out = []
    for (const [playerId, seat] of seating) {
      const known = positions?.get(playerId)
      out.push({
        playerId,
        benchId: seat.benchId,
        seatId: seat.seatId,
        entityId: this.entityIndexOf(seat.benchId, seat.seatId),
        position: known ? known.position : null,
        levelTime: known ? known.levelTime : 0,
        roomId: known?.roomId ?? null
      })
    }
    return out
  }
  public release (levelId: number, playerId: PlayerId): boolean {
    this.positionsByLevel.get(levelId)?.delete(playerId)
    const seat = this.byLevel.get(levelId)?.get(playerId)
    if (!seat) return false
    this.byLevel.get(levelId)?.delete(playerId)
    this.occupancyFor(levelId).delete(this.flatSeat(seat.benchId, seat.seatId))
    return true
  }
  public seatOf (levelId: number, playerId: PlayerId): Seated | undefined {
    const seat = this.byLevel.get(levelId)?.get(playerId)
    return seat ? { ...seat, levelId } : undefined
  }
  public latestSeatOf (playerId: PlayerId): Seated | undefined {
    for (const [levelId, seating] of this.byLevel) {
      const seat = seating.get(playerId)
      if (seat) return { ...seat, levelId }
    }
    return undefined
  }
  public occupied (levelId: number): Array<{ playerId: PlayerId } & Seat> {
    const seating = this.byLevel.get(levelId)
    return seating
      ? [...seating.entries()].map(([playerId, seat]) => ({
          playerId,
          ...seat
        }))
      : []
  }
  public freeSlots (levelId: number): number {
    return (
      this.benchCount * this.seatsPerBench -
      (this.byLevel.get(levelId)?.size ?? 0)
    )
  }
  public entityIndexOf (benchId: number, seatId: number): number {
    return benchId * this.seatsPerBench + seatId + 1
  }
  private seatingFor (levelId: number): Map<PlayerId, Seat> {
    let seating = this.byLevel.get(levelId)
    if (!seating) {
      seating = new Map<PlayerId, Seat>()
      this.byLevel.set(levelId, seating)
    }
    return seating
  }
  /**
   * Occupied-seat set for one level: `benchId * seatsPerBench + seatId`.
   *
   * `isFree` used to scan every seated player per probe — with a full hall a
   * single `lowestFree` was ~10^7 comparisons. The set makes every probe O(1)
   * and `lowestFree` a single pass over the seat space.
   */
  private occupancyFor (levelId: number): Set<number> {
    let occupancy = this.occupancy.get(levelId)
    if (!occupancy) {
      occupancy = new Set<number>()
      this.occupancy.set(levelId, occupancy)
    }
    return occupancy
  }
  private flatSeat (benchId: number, seatId: number): number {
    return benchId * this.seatsPerBench + seatId
  }
  private normalize (request: ReserveRequest): Seat | undefined {
    const { benchId, seatId } = request
    if (typeof benchId !== 'number' || typeof seatId !== 'number')
      return undefined
    if (!Number.isInteger(benchId) || !Number.isInteger(seatId))
      return undefined
    if (benchId < 0 || benchId >= this.benchCount) return undefined
    if (seatId < 0 || seatId >= this.seatsPerBench) return undefined
    return { benchId, seatId }
  }
  /** O(1) probe against the level's occupancy set. */
  private isFree (levelId: number, candidate: Seat): boolean {
    return !this.occupancyFor(levelId).has(
      this.flatSeat(candidate.benchId, candidate.seatId)
    )
  }
  private claim (levelId: number, playerId: PlayerId, seat: Seat): void {
    this.seatingFor(levelId).set(playerId, seat)
    this.occupancyFor(levelId).add(this.flatSeat(seat.benchId, seat.seatId))
  }
  private lowestFree (levelId: number): Seat | undefined {
    const occupancy = this.occupancyFor(levelId)
    const total = this.benchCount * this.seatsPerBench
    for (let flat = 0; flat < total; flat += 1) {
      if (occupancy.has(flat)) continue
      return {
        benchId: Math.floor(flat / this.seatsPerBench),
        seatId: flat % this.seatsPerBench
      }
    }
    return undefined
  }
}
