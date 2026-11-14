import type { Logger } from '../infra/logger/logger.js'
import type { PlayerId, RoomId } from '../types/branded.js'
import type { IAllocationRequest, RoomSnapshot } from '../types/contracts.js'
import { ErrorCode, HermesError } from '../types/errors.js'
import type { CrossCoordinator } from './CrossCoordinator.js'
import type { RequestRouter } from './RequestRouter.js'
import type { RoomCache } from './RoomCache.js'
import {
  SeatAllocator,
  SEATS_PER_BENCH,
  levelNeedsSeat,
  CONCERT_LEVEL_ID,
  type BenchRoom,
  type Seated
} from './SeatAllocator.js'

export interface ReserveInput {
  playerId: PlayerId
  levelId?: number | undefined
  benchId?: number | undefined
  seatId?: number | undefined
  level?: number | undefined
  group?: PlayerId[] | undefined
}

interface SnapshotSeat {
  playerId: PlayerId
  entityId: number
  benchId: number
  seatId: number
  position: [number, number, number] | null
  levelTime: number
  /** True while this seat's player is the hall-wide spotlight performer. */
  spotlight?: boolean | undefined
}

type CachedSeat = Omit<SnapshotSeat, 'playerId'>

const POSITION_EPSILON = 0.05
const sameSeat = (a: CachedSeat, b: CachedSeat): boolean => {
  if (
    a.entityId !== b.entityId ||
    a.benchId !== b.benchId ||
    a.seatId !== b.seatId ||
    Boolean(a.spotlight) !== Boolean(b.spotlight)
  )
    return false
  if (a.position === null || b.position === null)
    return a.position === b.position
  return (
    Math.abs(a.position[0] - b.position[0]) <= POSITION_EPSILON &&
    Math.abs(a.position[1] - b.position[1]) <= POSITION_EPSILON &&
    Math.abs(a.position[2] - b.position[2]) <= POSITION_EPSILON
  )
}

/**
 * Spotlight (concert stage) sessions, hall-wide.
 *
 * One performer per level. The requesting node reports the change
 * (`spotlight.changed`), the manager records it and every node rendering the
 * level receives it back through the audience snapshot, so all rooms — not
 * just the performer's — flip the entity into the spotlight render path.
 */

const DEFAULT_SEAT_RELEASE_GRACE_MS = 15_000

export interface ReserveResult extends Seated {
  roomIp: string
  roomPort: number
  roomId: RoomId
  entityId: number
  group: Array<{ playerId: PlayerId } & Seated>
}

export class ReservationService {
  /**
   * `roomId:levelId` -> what that room was last told about its remote seats.
   *
   * Keyed per ROOM, not per level: each room renders a different slice of the
   * hall (its own bench excluded), so a shared level baseline meant a room that
   * joined late (node restart, new reservation) was diffed against seats it had
   * never been sent and received nothing — the one-way-visibility bug where A
   * could see B's silhouette but B could not see A's.
   */
  private readonly snapshotCache = new Map<string, Map<string, CachedSeat>>()
  /**
   * levelId -> the whole hall's last known seat state, for the global diff and
   * diagnostics. The authoritative per-room diff runs against each room's own
   * `snapshotCache` entry below.
   */
  private readonly levelBaselines = new Map<number, Map<string, CachedSeat>>()
  /** Set while a snapshot round is being awaited; guards re-entrant rounds. */
  private snapshotInFlight = false
  private performance: {
    currentPerformer(levelId: number): PlayerId | undefined
    removePlayer(playerId: PlayerId, reason: string, levelId?: number): void
  } | undefined
  public setPerformanceService (service: NonNullable<ReservationService['performance']>): void { this.performance = service }
  public stop (): void {
    for (const timer of this.pendingReleases.values()) clearTimeout(timer)
    this.pendingReleases.clear()
  }
  /**
   * Rooms whose last diff frame failed to reach the node. The next round for
   * them sends every remote seat with `full: true` instead of a bare diff,
   * because the node may still hold silhouettes for seats that have since
   * emptied.
   */
  private readonly pendingFullSnapshots = new Set<RoomId>()
  /**
   * playerId -> pending release. A node reports `seat.released` on every
   * disconnect, including the ones a reconnect or a migration immediately
   * follows, so the seat is only freed after {@link DEFAULT_SEAT_RELEASE_GRACE_MS}.
   */
  private readonly pendingReleases = new Map<PlayerId, NodeJS.Timeout>()
  /**
   * playerId -> the (level, room) pair whose node acknowledged a seat.assign.
   *
   * The manager sees a room's telemetry every second. Re-sending seat.assign on
   * every frame would be wasteful, but a failed assignment must retry, so this
   * only short-circuits the success case. The room is part of the key: a player
   * that migrates to another room on the same level needs a seat that belongs
   * to that room.
   */
  private readonly seated = new Map<
    PlayerId,
    { levelId: number; roomId: RoomId }
  >()

  public constructor (
    private readonly allocator: SeatAllocator,
    private readonly rooms: RoomCache,
    private readonly router: RequestRouter,
    private readonly coordinator: CrossCoordinator,
    private readonly log?: Logger,
    private readonly seatReleaseGraceMs: number = DEFAULT_SEAT_RELEASE_GRACE_MS
  ) {}

  public async reserve (
    input: ReserveInput,
    signal?: AbortSignal
  ): Promise<ReserveResult> {
    const requestedLevel = input.levelId
    const levelId: number =
      typeof requestedLevel === 'number' &&
      Number.isInteger(requestedLevel) &&
      requestedLevel > 0
        ? requestedLevel
        : CONCERT_LEVEL_ID
    const companions = this.companionsOf(input)
    const party = [input.playerId, ...companions]
    if (party.length > SEATS_PER_BENCH) {
      throw new HermesError(
        ErrorCode.BAD_REQUEST,
        'party does not fit on one bench of eight same-room seats'
      )
    }

    // Bench↔room is 1:1 and one bench seats exactly one UDP room's players
    // side by side. The bench that hosts the WHOLE party is chosen before any
    // seat is claimed, so a party can never straddle benches or rooms, and
    // seat claims can never mix two rooms onto one bench.
    const bench = await this.benchForParty(levelId, party.length, input, signal)

    // Re-reserve keeps the caller's existing seat when it is on the chosen
    // bench; an explicit free seatId is honored; otherwise the lowest free
    // seat on the chosen bench. Claims never leave the chosen bench.
    let seat =
      this.allocator.seatOf(levelId, input.playerId)?.benchId === bench.benchId
        ? this.allocator.seatOf(levelId, input.playerId)
        : undefined
    if (!seat && input.playerId) {
      for (const other of this.allocator.levels()) {
        if (other !== levelId && this.allocator.seatOf(other, input.playerId)) {
          this.allocator.release(other, input.playerId)
        }
      }
    }
    if (!seat && Number.isInteger(input.seatId) &&
      this.allocator.isSeatFree(levelId, { benchId: bench.benchId, seatId: input.seatId as number })) {
      seat = this.allocator.claimExact(levelId, input.playerId, {
        benchId: bench.benchId,
        seatId: input.seatId as number
      })
    }
    if (!seat) {
      seat = this.allocator.allocateOnBench(input.playerId, levelId, bench.benchId)
    }
    if (!seat) {
      throw new HermesError(
        ErrorCode.NO_CAPACITY,
        'chosen bench cannot host the party'
      )
    }

    let group: Array<{ playerId: PlayerId } & Seated> = []
    if (companions.length > 0) {
      // allocateGroup anchors on the first already-seaten member — the caller,
      // just claimed on the chosen bench — so companions claim seats on the
      // same bench of the same room.
      const taken = this.allocator.allocateGroup({ ...input, levelId }, party)
      group = [...taken.entries()].map(([playerId, seated]) => ({
        playerId,
        ...seated
      }))
    }

    return {
      ...seat,
      roomIp: bench.room.host,
      roomPort: bench.room.port,
      roomId: bench.room.roomId,
      entityId: this.allocator.entityIndexOf(seat.benchId, seat.seatId),
      group
    }
  }

  /**
   * Choose the bench (and therefore the UDP room, 1:1) that will host a whole
   * reservation party. Order: explicit bench request → the caller's existing
   * bench → any bound bench whose room can host the party → a fresh room
   * bound to an unbound bench.
   */
  private async benchForParty (
    levelId: number,
    partySize: number,
    input: ReserveInput,
    signal?: AbortSignal
  ): Promise<{ benchId: number; room: BenchRoom }> {
    const roomCanHost = (roomId: RoomId): RoomSnapshot | undefined => {
      const room = this.rooms.getRoom(roomId)
      if (!room) return undefined
      // RoomCache always fills players/capacity (defaults 0/8); the defaults
      // keep partial snapshots hostable like an empty room instead of NaN.
      const players = Number.isFinite(room.players) ? room.players : 0
      const capacity = Number.isFinite(room.capacity) ? room.capacity : SEATS_PER_BENCH
      return players + partySize <= capacity ? room : undefined
    }
    const benchHasRoom = (benchId: number, room: RoomSnapshot): boolean =>
      this.allocator.freeOnBench(levelId, benchId) >= partySize

    if (Number.isInteger(input.benchId) && (input.benchId as number) >= 0) {
      const benchId = input.benchId as number
      const bound = this.allocator.roomForBench(benchId)
      if (bound) {
        const room = roomCanHost(bound.roomId)
        if (room && benchHasRoom(benchId, room)) return { benchId, room: bound }
        if (room) {
          throw new HermesError(
            ErrorCode.NO_CAPACITY,
            'requested bench cannot host the party'
          )
        }
        // The bound room is gone (node reclaimed while the manager was down):
        // rebind the bench to a room that exists, preserving the seat plan.
      }
      const room = await this.allocateRoomForBench(benchId, input, signal)
      return { benchId, room }
    }

    const existing = this.allocator.seatOf(levelId, input.playerId)
    if (existing) {
      const bound = this.allocator.roomForBench(existing.benchId)
      const room = bound && roomCanHost(bound.roomId)
      if (bound && room && benchHasRoom(existing.benchId, room)) {
        return { benchId: existing.benchId, room: bound }
      }
    }

    for (let benchId = 0; benchId < this.allocator.benchCapacity; benchId += 1) {
      const bound = this.allocator.roomForBench(benchId)
      if (!bound) continue
      const room = roomCanHost(bound.roomId)
      if (room && benchHasRoom(benchId, room)) return { benchId, room: bound }
    }

    const benchId = this.allocator.nextUnboundBench()
    if (benchId === undefined) {
      throw new HermesError(ErrorCode.NO_CAPACITY, 'no unbound bench for a fresh room')
    }
    const room = await this.allocateRoomForBench(benchId, input, signal)
    return { benchId, room }
  }

  private companionsOf (input: ReserveInput): PlayerId[] {
    const seen = new Set<PlayerId>([input.playerId])
    const result: PlayerId[] = []
    for (const id of input.group ?? []) {
      if (!id || seen.has(id)) continue
      seen.add(id)
      result.push(id)
    }
    return result
  }

  public recordPosition (
    levelId: number,
    playerId: PlayerId,
    position: [number, number, number],
    levelTime: number,
    roomId?: RoomId
  ): void {
    if (!Array.isArray(position) || position.length < 3) return
    if (!position.every(v => typeof v === 'number' && Number.isFinite(v)))
      return
    this.allocator.setPosition(
      levelId,
      playerId,
      [position[0], position[1], position[2]],
      levelTime,
      roomId
    )
  }

  public async broadcastSnapshot (signal?: AbortSignal): Promise<number> {
    if (this.snapshotInFlight) return 0
    this.snapshotInFlight = true
    try {
      return await this.runSnapshotRound(signal)
    } finally {
      this.snapshotInFlight = false
    }
  }

  private async runSnapshotRound (signal?: AbortSignal): Promise<number> {
    const levelIds = this.allocator.levels()
    let sent = 0
    const seenRooms = new Set<string>()

    for (const levelId of levelIds) {
      const seats = this.allocator.snapshot(levelId)
      if (seats.length === 0) continue

      const seated = levelNeedsSeat(levelId)

      const baseline =
        this.levelBaselines.get(levelId) ?? new Map<string, CachedSeat>()
      const current = new Map<string, CachedSeat>()
      const changed: SnapshotSeat[] = []
      const spotlighted = this.performance?.currentPerformer(levelId)
      for (const seat of seats) {
        const entry: CachedSeat = {
          entityId: seat.entityId,
          benchId: seat.benchId,
          seatId: seat.seatId,
          position: seat.position,
          levelTime: seat.levelTime,
          spotlight: spotlighted === seat.playerId
        }
        current.set(seat.playerId, entry)
        const before = baseline.get(seat.playerId)
        if (!before || !sameSeat(before, entry)) {
          changed.push({ playerId: seat.playerId, ...entry })
        }
      }
      this.levelBaselines.set(levelId, current)

      const benchHosts = new Map<number, RoomId>()
      if (seated) {
        for (const seat of seats) {
          const host = this.allocator.roomForBench(seat.benchId)
          if (!host) continue
          benchHosts.set(seat.benchId, host.roomId)
        }
        // R7: rebind benches that lost their room binding. `bindBenchRoom`
        // happens on seat allocation, but a manager restart keeps seats in
        // memory only through allocations — a bench whose host room vanished
        // (node reclaimed, crash) leaves its players permanently invisible:
        // no receiving room exists for their bench, so the per-room diff
        // below never reaches anyone. The seat's own telemetry remembers the
        // room the player is actually in (`setPosition` from seat.occupied),
        // so rebind there — but only when that room is still alive, and only
        // when the bench is genuinely unbound (never steal a live binding).
        for (const seat of seats) {
          if (benchHosts.has(seat.benchId)) continue
          const reported = seat.roomId
          if (!reported) continue
          const room = this.rooms.getRoom(reported)
          if (!room) continue
          this.allocator.bindBenchRoom(seat.benchId, {
            host: room.udpHost,
            port: room.udpPort,
            roomId: reported
          })
          benchHosts.set(seat.benchId, reported)
          // A binding that comes back after being gone usually means this
          // manager (or its snapshot cache) restarted while the seats
          // survived in the allocator. Rooms whose cache was rebuilt from
          // nothing would otherwise see an empty diff and never learn about
          // the rebound bench's players — mark every OTHER receiving room for
          // a full snapshot this round.
          for (const other of seats) {
            if (other.benchId === seat.benchId) continue
            const otherHost = benchHosts.get(other.benchId) ?? other.roomId
            if (otherHost && otherHost !== reported) {
              this.pendingFullSnapshots.add(otherHost)
            }
          }
          this.log?.info(
            { levelId, benchId: seat.benchId, roomId: reported },
            'bench rebound to the room its players report from'
          )
        }
      }

      // Rooms that must render this level's crowd: on a seated level that is
      // every room hosting a bench with a seat in it; on an ordinary level the
      // crowd is described by each player's own room.
      const allRooms = new Set<RoomId>()
      let unmapped = 0
      for (const seat of seats) {
        if (seated) {
          const room = benchHosts.get(seat.benchId)
          if (room) allRooms.add(room)
          else {
            unmapped += 1
            // An unmapped bench cannot be told about anyone, and worse, its
            // players are silently invisible to the rest of the hall: no
            // receiving room exists for their bench, so their seats never
            // leave this loop. This is the residual one-way-visibility case —
            // log it loudly instead of counting.
            this.log?.warn(
              { levelId, playerId: seat.playerId, benchId: seat.benchId },
              'bench not bound to a room; its players are invisible to other rooms'
            )
          }
        } else if (seat.roomId) {
          allRooms.add(seat.roomId)
        } else {
          unmapped += 1
        }
      }

      // One shared diff, sliced per *receiving* room: a room gets every changed
      // seat its own bench does not host. Those are exactly the silhouettes it
      // should render — its own bench arrives as full players through
      // `seat.assign`, and sending those back would draw room-mates as
      // badminton-bird silhouettes standing next to them.
      // Per-second telemetry, so it is debug: an idle hall would otherwise
      // write one line per level every second forever.
      this.log?.debug(
        {
          levelId,
          seated,
          seats: seats.length,
          changed: changed.length,
          rooms: allRooms.size
        },
        'audience snapshot targets'
      )

      for (const roomId of allRooms) {
        const snapshot = this.rooms.getRoom(roomId)
        if (!snapshot) continue
        const key = this.cacheKey(roomId, levelId)
        seenRooms.add(key)

        // Diff against what THIS room was last told, not only against the
        // global level baseline. The global diff answers "what changed in the
        // hall since the last round", but a room that joined late — node
        // restart, fresh reservation, a round whose send failed — was never
        // sent the pre-existing seats at all, so a global-only diff hands it
        // nothing and it renders an empty hall. That is the one-way-visibility
        // bug: A (seated before B's room existed) is visible to B, while B
        // never appears for A.
        const allRemote = seats.filter(
          seat => !seated || benchHosts.get(seat.benchId) !== roomId
        )
        const previous =
          this.snapshotCache.get(key) ?? new Map<string, CachedSeat>()
        const currentRoom = new Map<string, CachedSeat>()
        const remoteSeats: SnapshotSeat[] = []
        const spotlighted = this.performance?.currentPerformer(levelId)
        for (const seat of allRemote) {
          const entry: CachedSeat = {
            entityId: seat.entityId,
            benchId: seat.benchId,
            seatId: seat.seatId,
            position: seat.position,
            levelTime: seat.levelTime,
            spotlight: spotlighted === seat.playerId
          }
          currentRoom.set(seat.playerId, entry)
          const before = previous.get(seat.playerId)
          if (!before || !sameSeat(before, entry)) {
            remoteSeats.push({ playerId: seat.playerId, ...entry })
          }
        }

        // Seats this room was told about that no longer exist anywhere are
        // ghosts on its side; name them so the node drops their silhouettes.
        // Diffing per room is what makes this reliable — a departure only
        // reaches rooms whose cache still holds the departed seat.
        const removed: string[] = []
        for (const playerId of previous.keys()) {
          if (!currentRoom.has(playerId)) removed.push(playerId as string)
        }

        // No cache for this room means we have never told it anything we can
        // still account for — manager (re)start or first appearance. Diffing
        // against nothing produced empty `removed`, so nodes kept every
        // silhouette from before the restart forever (the persistent ghost
        // birds). A cache-less room always gets a full frame: the node drops
        // every unlisted silhouette and the cache is rebuilt from truth.
        const full = this.pendingFullSnapshots.has(roomId) || !this.snapshotCache.has(key)
        // A full snapshot carries EVERY remote seat, not the diff: the room's
        // cache is the thing that is suspect (send failed earlier, or the
        // room's own state was rebuilt), so diffing against it proves nothing.
        const outboundSeats: SnapshotSeat[] = full
          ? allRemote.map(seat => ({
              playerId: seat.playerId,
              entityId: seat.entityId,
              benchId: seat.benchId,
              seatId: seat.seatId,
              position: seat.position,
              levelTime: seat.levelTime,
              spotlight: spotlighted === seat.playerId
            }))
          : remoteSeats

        // Performance state has its own versioned delivery channel, including idle.
        if (outboundSeats.length === 0 && removed.length === 0 && !full) {
          continue
        }

        try {
          const ack = await this.coordinator.audienceSnapshot(
            snapshot,
            {
              levelId,
              seats: outboundSeats,
              removed,
              full
            },
            signal
          )
          if (ack.code !== ErrorCode.OK) throw new Error('audience snapshot rejected')
          // Remember the room's full remote state, not just the seats this
          // frame happened to carry: caching a partial frame would make the
          // seats it omitted look deleted on the next round.
          this.snapshotCache.set(key, currentRoom)
          this.pendingFullSnapshots.delete(roomId)
          sent += 1
        } catch (error) {
          this.snapshotCache.delete(key)
          this.pendingFullSnapshots.add(roomId)
          this.log?.debug?.({ err: error, roomId }, 'audience snapshot failed')
        }
      }
    }


    const seenRoomIds = new Set(
      [...seenRooms].map(key => key.slice(0, key.lastIndexOf(':')))
    )
    for (const key of this.snapshotCache.keys()) {
      if (!seenRooms.has(key)) this.snapshotCache.delete(key)
    }
    for (const roomId of this.pendingFullSnapshots) {
      if (!seenRoomIds.has(roomId)) this.pendingFullSnapshots.delete(roomId)
    }
    for (const levelId of [...this.levelBaselines.keys()]) {
      if (!levelIds.includes(levelId)) this.levelBaselines.delete(levelId)
    }
    return sent
  }


  /** Diff-baseline key. One room can host two levels, so the level is part of it. */
  private cacheKey (roomId: RoomId, levelId: number): string {
    return `${roomId}:${levelId}`
  }

  public seatFor (playerId: PlayerId, levelId?: number): Seated | undefined {
    return levelId === undefined
      ? this.allocator.latestSeatOf(playerId)
      : this.allocator.seatOf(levelId, playerId)
  }

  public roomForBench (benchId: number): BenchRoom | undefined {
    return this.allocator.roomForBench(benchId)
  }

  public release (playerId: PlayerId, levelId?: number): boolean {
    this.cancelPendingRelease(playerId)
    this.seated.delete(playerId)
    this.performance?.removePlayer(playerId, 'seat_released', levelId)
    if (levelId !== undefined) return this.allocator.release(levelId, playerId)
    const seat = this.allocator.latestSeatOf(playerId)
    return seat ? this.allocator.release(seat.levelId, playerId) : false
  }

  /**
   * Drop every seat a room was hosting, without the release grace window.
   *
   * Called when a room is gone for good (its node was reclaimed after the grace
   * window). Every other release path is driven by an event from the node, and a
   * dead node sends none — so without this the seats it held stayed reserved
   * until the manager process restarted.
   *
   * @returns the number of seats freed
   */
  public forgetRoom (roomId: RoomId): number {
    let freed = 0
    for (const levelId of this.allocator.levels()) {
      for (const seat of this.allocator.occupied(levelId)) {
        const bound = this.allocator.roomForBench(seat.benchId)
        if (!bound || bound.roomId !== roomId) continue
        this.cancelPendingRelease(seat.playerId)
        this.seated.delete(seat.playerId)
        this.performance?.removePlayer(seat.playerId, 'room_reclaimed', levelId)
        if (this.allocator.release(levelId, seat.playerId)) freed += 1
      }
    }
    // Cache keys are `roomId:levelId`; deleting the bare roomId never matched
    // anything, so a reclaimed room kept a stale baseline and the room that
    // replaced it was diffed against seats it had never received.
    for (const key of [...this.snapshotCache.keys()]) {
      if (key.startsWith(`${roomId}:`)) this.snapshotCache.delete(key)
    }
    // The room is gone for good: release its bench binding too, or the bench
    // stayed bound to a dead room forever and every later seater had to walk
    // past it.
    this.allocator.unbindRoom(roomId)
    return freed
  }

  /**
   * A node reported the player's barn entity gone.
   *
   * Schedule the seat release instead of freeing it now: a reconnect (new
   * session, same uuid) and a room migration both produce this event, and the
   * player has to keep their seat through them. The release is cancelled by
   * {@link handlePlayerJoined} if they come back inside the grace window.
   *
   * `levelId` is the level the node had on file. When it is missing the
   * player's latest seat (across events) is used instead.
   */
  public noteSeatReleased (playerId: PlayerId, levelId?: number): void {
    this.cancelPendingRelease(playerId)
    this.seated.delete(playerId)
    const timer = setTimeout(() => {
      this.pendingReleases.delete(playerId)
      const seat =
        levelId === undefined
          ? this.allocator.latestSeatOf(playerId)
          : this.allocator.seatOf(levelId, playerId)
      if (!seat) return
      this.allocator.release(seat.levelId, playerId)
      this.performance?.removePlayer(playerId, 'seat_released', seat.levelId)
      this.log?.info(
        {
          playerId,
          levelId: seat.levelId,
          benchId: seat.benchId,
          seatId: seat.seatId
        },
        'seat released after grace window'
      )
    }, this.seatReleaseGraceMs)
    timer.unref?.()
    this.pendingReleases.set(playerId, timer)
  }

  public notePlayerLeft (playerId: PlayerId): void {
    const seat = this.allocator.latestSeatOf(playerId)
    if (seat && !levelNeedsSeat(seat.levelId))
      this.noteSeatReleased(playerId, seat.levelId)
  }

  public cancelPendingRelease (playerId: PlayerId): void {
    const timer = this.pendingReleases.get(playerId)
    if (!timer) return
    clearTimeout(timer)
    this.pendingReleases.delete(playerId)
  }

  public async handlePlayerJoined (
    playerId: PlayerId,
    roomId: RoomId,
    levelId: number,
    signal?: AbortSignal
  ): Promise<void> {
    this.cancelPendingRelease(playerId)

    if (!levelNeedsSeat(levelId)) {
      this.seated.delete(playerId)
      this.allocator.release(levelId, playerId)
      return
    }

    await this.ensureSeated(playerId, roomId, levelId, signal)
  }

  public async ensureSeated (
    playerId: PlayerId,
    roomId: RoomId,
    levelId: number,
    signal?: AbortSignal
  ): Promise<void> {
    if (!levelNeedsSeat(levelId)) return
    this.cancelPendingRelease(playerId)
    const acked = this.seated.get(playerId)
    if (acked && acked.levelId === levelId && acked.roomId === roomId) return

    const snapshot = this.rooms.getRoom(roomId)
    if (!snapshot) return

    let seat = this.allocator.seatOf(levelId, playerId)
    if (seat) {
      const bound = this.allocator.roomForBench(seat.benchId)
      if (bound && bound.roomId !== roomId) {
        this.log?.warn(
          {
            playerId,
            roomId,
            benchId: seat.benchId,
            benchRoomId: bound.roomId
          },
          'seat belongs to another room; reassigning'
        )
        this.allocator.release(levelId, playerId)
        seat = undefined
      }
    }

    if (!seat) {
      for (const other of this.allocator.levels()) {
        if (other !== levelId && this.allocator.seatOf(other, playerId)) {
          this.allocator.release(other, playerId)
        }
      }
      seat = this.allocateSeatInRoom(playerId, levelId, roomId, snapshot)
    }
    if (!seat) {
      this.log?.warn(
        { playerId, roomId, levelId },
        'no seat available in this room'
      )
      return
    }

    const assignment = this.assignmentFor(seat)
    this.log?.info({ playerId, roomId, seat: assignment }, 'seat.assign sent')
    try {
      const ack = await this.coordinator.seatPlayer(
        snapshot,
        {
          playerId,
          ...assignment
        },
        signal
      )
      if (ack.code === ErrorCode.OK) {
        this.seated.set(playerId, { levelId, roomId })
        // Remember the room the seat lives in even before the first
        // seat.occupied telemetry: the snapshot round uses this to rebind a
        // bench whose host room binding was lost, and a seat recorded without
        // a room can never be rebound (roomId stays null forever). Reuse any
        // position already reported; if none yet, record the room alone by
        // passing the seat's placeholder position — setPosition keeps the
        // previous position when only the room is new information.
        const known = this.allocator
          .snapshot(levelId)
          .find(entry => entry.playerId === playerId)
        this.allocator.setPosition(
          levelId,
          playerId,
          known?.position ?? [0, 0, 0],
          known?.levelTime ?? Date.now() / 1000,
          roomId
        )
      } else {
        this.log?.warn(
          { playerId, roomId, levelId, code: ack.code },
          'node rejected seat assignment'
        )
      }
    } catch (error) {
      this.log?.debug?.(
        { err: error, playerId, roomId, levelId },
        'seat assignment failed'
      )
    }
  }

  private allocateSeatInRoom (
    playerId: PlayerId,
    levelId: number,
    roomId: RoomId,
    snapshot: { udpHost: string; udpPort: number }
  ): Seated | undefined {
    // Bench↔room is 1:1, so this room owns at most one bench; find it,
    // skipping bindings whose host room has vanished (those are stale and get
    // reclaimed below).
    const roomAlive = this.rooms.getRoom(roomId)
    for (
      let benchId = 0;
      roomAlive && benchId < this.allocator.benchCapacity;
      benchId += 1
    ) {
      const bound = this.allocator.roomForBench(benchId)
      if (bound?.roomId !== roomId) continue
      const seat = this.allocator.allocateOnBench(playerId, levelId, benchId)
      if (seat) return seat
    }
    for (
      let benchId = 0;
      benchId < this.allocator.benchCapacity;
      benchId += 1
    ) {
      const bound = this.allocator.roomForBench(benchId)
      // Bindable: unbound, or bound only to a room that no longer exists —
      // a dead binding is reclaimed rather than walked past forever.
      const reclaimable = !bound || !this.rooms.getRoom(bound.roomId)
      if (!reclaimable) continue
      // Bind BEFORE claiming: a snapshot round running concurrently reads
      // `roomForBench` to decide which rooms receive which seats. Claiming
      // first left a window where a seated player's bench had no host — the
      // round skipped it entirely and every other room never learned about
      // that player (the probabilistic one-way visibility).
      this.allocator.bindBenchRoom(benchId, {
        host: snapshot.udpHost,
        port: snapshot.udpPort,
        roomId
      })
      this.log?.info(
        { playerId, roomId, benchId, rebound: Boolean(bound) },
        "bench bound to the player's current room"
      )
      const seat = this.allocator.allocateOnBench(playerId, levelId, benchId)
      if (seat) return seat
      // The seat was taken between the checks; unbind so a later attempt can
      // reuse this bench.
      this.allocator.unbindBenchRoom(benchId)
      benchId -= 1
    }
    return undefined
  }

  public async notePlayerLevel (
    playerId: PlayerId,
    roomId: RoomId,
    levelId: number,
    seat?: { benchId: number; seatId: number } | undefined,
    position?: [number, number, number] | undefined,
    levelTime?: number | undefined,
    signal?: AbortSignal
  ): Promise<void> {
    if (levelNeedsSeat(levelId)) {
      if (seat) {
        // The node's roster reports where this player actually sits. Mirror it
        // into the allocator without re-assigning: after a manager restart
        // this rebuilds the whole hall with zero churn — no renumbered
        // benches, no seat.assign storm, no renumbered entity projections.
        this.rebuildSeat(playerId, roomId, levelId, seat, position, levelTime)
        return
      }
      await this.ensureSeated(playerId, roomId, levelId, signal)
      return
    }
    const held = this.allocator.latestSeatOf(playerId)
    if (held && levelNeedsSeat(held.levelId)) {
      if (this.pendingReleases.has(playerId)) return
      this.log?.info(
        {
          playerId,
          levelId,
          seatLevel: held.levelId,
          benchId: held.benchId,
          seatId: held.seatId
        },
        'player left the concert level; scheduling seat release'
      )
      this.noteSeatReleased(playerId, held.levelId)
    }
  }

  /**
   * Mirror a node-reported seat into the allocator. The node roster is the
   * source of truth for who sits where (nodes survive manager restarts with
   * their rosters intact), so telemetry rebuilds occupancy and bench bindings
   * instead of re-deriving them.
   */
  private rebuildSeat (
    playerId: PlayerId,
    roomId: RoomId,
    levelId: number,
    seat: { benchId: number; seatId: number },
    position?: [number, number, number] | undefined,
    levelTime?: number | undefined
  ): void {
    this.cancelPendingRelease(playerId)
    this.seated.set(playerId, { levelId, roomId })
    const applied = this.allocator.claimExact(levelId, playerId, seat)
    if (!applied) {
      this.log?.warn(
        { playerId, roomId, levelId, seat },
        'telemetry reported an out-of-range seat'
      )
      this.seated.delete(playerId)
      return
    }
    const room = this.rooms.getRoom(roomId)
    if (room) {
      const bound = this.allocator.roomForBench(seat.benchId)
      if (!bound || bound.roomId !== roomId ||
        bound.host !== room.udpHost || bound.port !== room.udpPort) {
        this.allocator.bindBenchRoom(seat.benchId, {
          host: room.udpHost,
          port: room.udpPort,
          roomId
        })
      }
    }
    if (position && position.every(v => Number.isFinite(v))) {
      this.allocator.setPosition(
        levelId,
        playerId,
        [position[0], position[1], position[2]],
        Number.isFinite(levelTime) ? (levelTime as number) : Date.now() / 1000,
        roomId
      )
    } else {
      // Remember the room even without a position so a lost bench binding can
      // be rebound from this seat's own telemetry.
      const known = this.allocator
        .snapshot(levelId)
        .find(entry => entry.playerId === playerId)
      this.allocator.setPosition(
        levelId,
        playerId,
        known?.position ?? [0, 0, 0],
        known?.levelTime ?? Date.now() / 1000,
        roomId
      )
    }
  }

  public async assignSeatToNode (
    playerId: PlayerId,
    seat: Seated,
    signal?: AbortSignal
  ): Promise<void> {
    const room = this.allocator.roomForBench(seat.benchId)
    if (!room) {
      throw new HermesError(ErrorCode.NOT_FOUND, 'bench has no bound room', {
        benchId: seat.benchId
      })
    }
    const snapshot = this.rooms.getRoom(room.roomId)
    if (!snapshot) {
      throw new HermesError(ErrorCode.NODE_UNAVAILABLE, 'bench room is gone', {
        roomId: room.roomId
      })
    }
    const ack = await this.coordinator.seatPlayer(
      snapshot,
      {
        playerId,
        ...this.assignmentFor(seat)
      },
      signal
    )
    if (ack.code !== ErrorCode.OK) {
      throw new HermesError(
        ack.code,
        ack.message ?? 'node rejected seat assignment',
        ack.data
      )
    }
  }

  private assignmentFor (seat: Seated): {
    levelId: number
    benchId: number
    seatId: number
    entityId: number
    roomMembers: Array<{ benchId: number; seatId: number }>
  } {
    const roomMembers = this.allocator
      .occupied(seat.levelId)
      .filter(entry => entry.benchId === seat.benchId)
      .map(entry => ({ benchId: entry.benchId, seatId: entry.seatId }))

    return {
      levelId: seat.levelId,
      benchId: seat.benchId,
      seatId: seat.seatId,
      entityId: this.allocator.entityIndexOf(seat.benchId, seat.seatId),
      roomMembers
    }
  }

  private async allocateRoomForBench (
    benchId: number,
    input: ReserveInput,
    signal?: AbortSignal
  ): Promise<BenchRoom> {
    const request: IAllocationRequest = { playerId: input.playerId }
    if (input.level !== undefined) request.level = input.level

    const reply = await this.router.allocate(request, signal)
    if (
      reply.code !== ErrorCode.OK ||
      !reply.data ||
      !('udpHost' in reply.data)
    ) {
      const message =
        reply.data && 'message' in reply.data
          ? reply.data.message
          : 'no room available for bench'
      throw new HermesError(reply.code || ErrorCode.NO_CAPACITY, message, {
        benchId
      })
    }

    const room: BenchRoom = {
      host: reply.data.udpHost,
      port: reply.data.udpPort,
      roomId: reply.data.roomId
    }
    this.allocator.bindBenchRoom(benchId, room)
    this.log?.info(
      { benchId, roomId: room.roomId, host: room.host, port: room.port },
      'bench bound to udp room'
    )
    return room
  }
}
