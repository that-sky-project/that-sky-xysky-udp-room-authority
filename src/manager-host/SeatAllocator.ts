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
/**
 * Venue lists come from config (`audience.seated_levels` / `hall_levels`)
 * through {@link configureSeatLevels}, installed by the runtime at startup.
 * The literals below are the fallback defaults so tools and tests work
 * without configuration — there are no other named level constants.
 */
let seatedLevelSet: ReadonlySet<number> = new Set([263580627])
let hallLevelSet: ReadonlySet<number> = new Set([1559613961])

/** Install the configured seated/hall venue lists (uint32 level ids). */
export function configureSeatLevels ({
  seated = [],
  hall = []
}: { seated?: readonly number[]; hall?: readonly number[] } = {}): void {
  const clean = (ids: readonly number[]): ReadonlySet<number> =>
    new Set(ids.filter(id => Number.isInteger(id) && id > 0))
  if (seated.length > 0) seatedLevelSet = clean(seated)
  if (hall.length > 0) hallLevelSet = clean(hall)
}

/** Venues that run the audience barn — seats or not (seated ∪ hall). */
export function audienceLevelIds (): number[] {
  return [...new Set([...seatedLevelSet, ...hallLevelSet])]
}

/** The venue a bare reserve (client event id 8) opens: the first seated.
 *  Never undefined — the seated set starts with one entry and
 *  {@link configureSeatLevels} rejects empty lists. */
export function defaultSeatedLevel (): number {
  return [...seatedLevelSet][0]!
}

/** True only for levels that get seat assignment (需要座位). */
export const levelNeedsSeat = (levelId: number): boolean =>
  seatedLevelSet.has(Number(levelId))

/**
 * Resolve the client's `event_id` (the account layer forwards it verbatim) to
 * the seated level it opens. Event 8 — the client's reserve default — opens
 * the first configured seated venue. An unknown event id is rejected so the
 * miss is visible instead of silently seating players in the wrong hall;
 * register a mapping here when a future activity ships with its own event id.
 */
export function eventLevel (eventId: number): number | undefined {
  // 0 = the client is not (yet) in a reservation event: open the first
  // configured seated venue. 8 is the concert reserve default.
  if (eventId === 8 || eventId === 0) return defaultSeatedLevel()
  return undefined
}

/** True when the level is a configured audience venue (seated or hall). */
export function isAudienceVenue (levelId: number): boolean {
  const id = Number(levelId)
  return seatedLevelSet.has(id) || hallLevelSet.has(id)
}
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
  /**
   * Bench↔room is 1:1: one bench renders eight side-by-side seats and those
   * eight players must all belong to one UDP room (and one room owns at most
   * one bench — room capacity is 8). Binding over an existing room binding
   * releases the room's previous bench so the invariant can never drift back
   * into mixed benches.
   */
  public bindBenchRoom (benchId: number, room: BenchRoom): void {
    for (const [other, bound] of this.benchRooms) {
      if (other !== benchId && bound.roomId === room.roomId) {
        this.benchRooms.delete(other)
      }
    }
    this.benchRooms.set(benchId, { ...room })
  }
  public roomForBench (benchId: number): BenchRoom | undefined {
    return this.benchRooms.get(benchId)
  }
  public benchOfRoom (roomId: RoomId): number | undefined {
    for (const [benchId, bound] of this.benchRooms) {
      if (bound.roomId === roomId) return benchId
    }
    return undefined
  }
  public nextUnboundBench (): number | undefined {
    for (let benchId = 0; benchId < this.benchCount; benchId += 1) {
      if (!this.benchRooms.has(benchId)) return benchId
    }
    return undefined
  }
  /** Drop every binding held by a room that is gone for good. */
  public unbindRoom (roomId: RoomId): number[] {
    const freed: number[] = []
    for (const [benchId, bound] of this.benchRooms) {
      if (bound.roomId !== roomId) continue
      this.benchRooms.delete(benchId)
      freed.push(benchId)
    }
    return freed
  }
  /**
   * Claim one exact seat for a player, replacing whatever the allocator
   * believed about them and any stale claimant of that seat. Used when the
   * node's own roster (telemetry) reports where a player actually sits: the
   * node is authoritative and the allocator must mirror it without churn.
   */
  public claimExact (
    levelId: number,
    playerId: PlayerId,
    seat: Seat
  ): Seated | undefined {
    const wanted = this.normalize(seat)
    if (!wanted) return undefined
    for (const [otherLevel, seating] of this.byLevel) {
      const existing = seating.get(playerId)
      if (existing && (otherLevel !== levelId ||
        existing.benchId !== wanted.benchId || existing.seatId !== wanted.seatId)) {
        this.release(otherLevel, playerId)
      }
    }
    for (const [owner, owned] of this.byLevel.get(levelId) ?? []) {
      if (owner !== playerId && owned.benchId === wanted.benchId &&
        owned.seatId === wanted.seatId) {
        // A stale allocator entry (manager restart, lost release). The node
        // roster cannot double-book a seat; the claimant's real seat arrives
        // with their own telemetry.
        this.release(levelId, owner)
      }
    }
    this.claim(levelId, playerId, wanted)
    return { ...wanted, levelId }
  }
  /** Drop a bench binding so a later attempt can reclaim it. */
  public unbindBenchRoom (benchId: number): void {
    this.benchRooms.delete(benchId)
  }
  public get benchCapacity (): number {
    return this.benchCount
  }
  /** Free seats on one bench for the level (the party must fit on ONE bench). */
  public freeOnBench (levelId: number, benchId: number): number {
    if (!Number.isInteger(benchId) || benchId < 0 || benchId >= this.benchCount) return 0
    let free = 0
    for (let seatId = 0; seatId < this.seatsPerBench; seatId += 1) {
      if (this.isFree(levelId, { benchId, seatId })) free += 1
    }
    return free
  }
  public isSeatFree (levelId: number, seat: Seat): boolean {
    const wanted = this.normalize(seat)
    return Boolean(wanted && this.isFree(levelId, wanted))
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
  private normalize (
    request: Pick<ReserveRequest, 'benchId' | 'seatId'>
  ): Seat | undefined {
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
