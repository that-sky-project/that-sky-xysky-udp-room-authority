import type { Logger } from '../infra/logger/logger.js'
import type { PlayerId, RoomId } from '../types/branded.js'
import type { IAllocationRequest } from '../types/contracts.js'
import { ErrorCode, HermesError } from '../types/errors.js'
import type { CrossCoordinator } from './CrossCoordinator.js'
import type { RequestRouter } from './RequestRouter.js'
import type { RoomCache } from './RoomCache.js'
import {
  SeatAllocator,
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
interface SpotlightState {
  playerId: PlayerId
  startedAt: number
}

/** How long a performer stays on stage when nobody releases them. */
export const SPOTLIGHT_MAX_MS = 5 * 60 * 1000

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
  /** levelId -> the current hall-wide performer. One at a time. */
  private readonly spotlights = new Map<number, SpotlightState>()
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
    const seat = this.allocator.allocate({ ...input, levelId })

    const companions = this.companionsOf(input)
    let group: Array<{ playerId: PlayerId } & Seated> = []
    if (companions.length > 0) {
      const taken = this.allocator.allocateGroup({ ...input, levelId }, [
        input.playerId,
        ...companions
      ])
      group = [...taken.entries()].map(([playerId, seated]) => ({
        playerId,
        ...seated
      }))
    }

    const room = await this.roomForSeat(seat, input, signal)

    return {
      ...seat,
      roomIp: room.host,
      roomPort: room.port,
      roomId: room.roomId,
      entityId: this.allocator.entityIndexOf(seat.benchId, seat.seatId),
      group
    }
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
      const spotlighted = this.currentSpotlight(levelId)
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
        const spotlighted = this.currentSpotlight(levelId)
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

        const full = this.pendingFullSnapshots.has(roomId)

        // The performance session (packet 24) rides on this frame. The live
        // server pushes it every second for as long as a performance exists
        // (3294 packets in the 54 s reference capture), so a seat-less diff
        // must still go out while someone is on stage — a room whose bench
        // diff is otherwise empty (the usual single-room case) would otherwise
        // never learn who is performing.
        const spotlightSession = this.spotlightSession(levelId)
        if (remoteSeats.length === 0 && removed.length === 0 && !spotlightSession) {
          continue
        }

        try {
          await this.coordinator.audienceSnapshot(
            snapshot,
            {
              levelId,
              seats: remoteSeats,
              removed,
              full,
              spotlightSession
            },
            signal
          )
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
        // A reclaimed room cannot hold the stage any more; the diff would
        // otherwise keep telling every room the departed player is lit.
        if (this.spotlights.get(levelId)?.playerId === seat.playerId) {
          this.spotlights.delete(levelId)
          this.log?.info({ levelId, playerId: seat.playerId }, 'spotlight ended: performer left')
        }
        if (this.allocator.release(levelId, seat.playerId)) freed += 1
      }
    }
    // Cache keys are `roomId:levelId`; deleting the bare roomId never matched
    // anything, so a reclaimed room kept a stale baseline and the room that
    // replaced it was diffed against seats it had never received.
    for (const key of [...this.snapshotCache.keys()]) {
      if (key.startsWith(`${roomId}:`)) this.snapshotCache.delete(key)
    }
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

  /**
   * Record a hall-wide spotlight change reported by a node.
   *
   * One performer per level: a new request while someone else is on stage
   * replaces them. The change reaches every room through the next audience
   * snapshot round, because the diff treats `spotlight` as part of seat state.
   *
   * @returns whether the state was applied (a change for an unknown level or a
   *   player without a seat in it is ignored)
   */
  public noteSpotlightChanged (levelId: number, playerId: PlayerId, active: boolean): boolean {
    if (!levelNeedsSeat(levelId)) return false
    const existing = this.spotlights.get(levelId)
    if (!active) {
      if (!existing || existing.playerId !== playerId) return false
      this.spotlights.delete(levelId)
      this.log?.info({ levelId, playerId }, 'spotlight released')
      return true
    }
    const seat = this.allocator.seatOf(levelId, playerId)
    if (!seat) {
      this.log?.warn({ levelId, playerId }, 'spotlight request from a player without a seat; ignored')
      return false
    }
    this.spotlights.set(levelId, { playerId, startedAt: Date.now() })
    this.log?.info(
      { levelId, playerId, benchId: seat.benchId, seatId: seat.seatId, replaced: existing?.playerId },
      'spotlight started'
    )
    return true
  }

  /** The player currently on stage in a level, if any and not expired. */
  public currentSpotlight (levelId: number): PlayerId | undefined {
    const state = this.spotlights.get(levelId)
    if (!state) return undefined
    if (Date.now() - state.startedAt > SPOTLIGHT_MAX_MS) {
      this.spotlights.delete(levelId)
      this.log?.info({ levelId, playerId: state.playerId }, 'spotlight expired')
      return undefined
    }
    return state.playerId
  }

  /**
   * The performance-session descriptor for a level's current spotlight, in the
   * shape the client's packet-24 handler parses (`sub_1409508C0` reads
   * `plyr[0].phdr.id` — the performer's uuid — and `sub_1417F8190` keys the
   * stage session by `stid`). Null when nobody is on stage.
   */
  public spotlightSession (levelId: number): {
    sessionId: string
    performerId: string
    startedAt: number
  } | null {
    const state = this.spotlights.get(levelId)
    if (!state) return null
    if (Date.now() - state.startedAt > SPOTLIGHT_MAX_MS) return null
    return {
      // One stage per level, so the level id is the stable session key.
      sessionId: `stage-${levelId}`,
      performerId: state.playerId,
      startedAt: state.startedAt
    }
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
    for (
      let benchId = 0;
      benchId < this.allocator.benchCapacity;
      benchId += 1
    ) {
      if (this.allocator.roomForBench(benchId)?.roomId !== roomId) continue
      const seat = this.allocator.allocateOnBench(playerId, levelId, benchId)
      if (seat) return seat
    }
    for (
      let benchId = 0;
      benchId < this.allocator.benchCapacity;
      benchId += 1
    ) {
      if (this.allocator.roomForBench(benchId)) continue
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
        { playerId, roomId, benchId },
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
    signal?: AbortSignal
  ): Promise<void> {
    if (levelNeedsSeat(levelId)) {
      await this.ensureSeated(playerId, roomId, levelId, signal)
      return
    }
    const seat = this.allocator.latestSeatOf(playerId)
    if (seat && levelNeedsSeat(seat.levelId)) {
      if (this.pendingReleases.has(playerId)) return
      this.log?.info(
        {
          playerId,
          levelId,
          seatLevel: seat.levelId,
          benchId: seat.benchId,
          seatId: seat.seatId
        },
        'player left the concert level; scheduling seat release'
      )
      this.noteSeatReleased(playerId, seat.levelId)
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

  private async roomForSeat (
    seat: Seated,
    input: ReserveInput,
    signal?: AbortSignal
  ): Promise<BenchRoom> {
    const bound = this.allocator.roomForBench(seat.benchId)
    if (bound && this.rooms.getRoom(bound.roomId)) return bound
    return this.allocateRoomForBench(seat.benchId, input, signal)
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
