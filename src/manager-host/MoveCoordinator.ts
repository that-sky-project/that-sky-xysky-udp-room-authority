import type { PlayerId, RoomId } from '../types/branded.js'
import { asProtocolVersion, asUnixMs } from '../types/branded.js'
import type {
  INodeEvent,
  PlayerTelemetry,
  RoomSnapshot
} from '../types/contracts.js'
import type { Logger } from '../infra/logger/logger.js'
import type { CrossCoordinator } from './CrossCoordinator.js'
import type { DebugEventInput, DebugEventLog } from './DebugEventLog.js'
import type { NodeRegistry } from './NodeRegistry.js'
import type { PendingRequestMap } from './PendingRequestMap.js'
import type { RoomCache } from './RoomCache.js'
import {
  MovePlanner,
  type MovePlan,
  type MovePlannerOptions
} from './MovePlanner.js'
import { TelemetryStore } from './TelemetryStore.js'
import {
  LevelIsolationPlanner,
  type LevelIsolationPlan,
  type LevelIsolationOptions
} from './LevelIsolationPlanner.js'
import { MoveExecutor } from './MoveExecutor.js'
interface MoveTransaction extends MovePlan {
  moveId: string
  status: 'prepared' | 'committed' | 'completed' | 'failed'
  createdAt: number
  joinedPlayers: string[]
  leftPlayers: string[]
}
type LevelMoveTransaction = Omit<MoveTransaction, keyof MovePlan> &
  LevelIsolationPlan
const TRANSACTION_RETENTION_MS = 5 * 60_000
export interface MoveCoordinatorOptions {
  moveBudgetPerCycle?: number
  moveTransactionTimeoutMs?: number
  stateEmaHalfLifeMs?: number
  confirmationCycles?: number
  planner?: Partial<MovePlannerOptions>
  levelIsolation?: Partial<LevelIsolationOptions>
}
export class MoveCoordinator {
  public readonly telemetry: TelemetryStore
  private readonly planner: MovePlanner
  private readonly levelIsolationPlanner: LevelIsolationPlanner
  private readonly executor: MoveExecutor
  private readonly transactions = new Map<
    string,
    MoveTransaction | LevelMoveTransaction
  >()
  private readonly roomStability = new Map<
    string,
    { signature: string; cycles: number }
  >()
  private readonly suppressedPlans = new Map<string, string>()
  private readonly pendingCancels = new Map<
    string,
    { roomId: RoomId; moveId: string; playerIds: string[] }
  >()
  private lastPlanAt = 0
  private planning = false
  private moveSeq = 0
  private readonly roomRoster = new Map<
    string,
    { seq: number; players: Map<string, PlayerTelemetry> }
  >()
  private readonly telemetryGap = new Map<
    string,
    { since: number; dropped: number; haveSeq?: number | undefined }
  >()
  private readonly telemetryGapWarnAt = new Map<string, number>()
  private readonly telemetryVersionWarnAt = new Map<string, number>()
  private readonly clockSkewWarnAt = new Map<string, number>()
  public constructor (
    private readonly rooms: RoomCache,
    private readonly nodes: NodeRegistry,
    private readonly pending: PendingRequestMap,
    private readonly cross: CrossCoordinator,
    private readonly options: MoveCoordinatorOptions = {},
    private readonly log?: Logger,
    private readonly onMoveOutcome?: (
      reason: string,
      status: 'committed' | 'failed'
    ) => void,
    private readonly onTelemetryGap?: () => void,
    private readonly events?: DebugEventLog
  ) {
    this.telemetry = new TelemetryStore({
      stateEmaHalfLifeMs: options.stateEmaHalfLifeMs ?? 3_000
    })
    this.planner = new MovePlanner(options.planner ?? {})
    this.levelIsolationPlanner = new LevelIsolationPlanner(
      options.levelIsolation ?? {}
    )
    const transactionTimeoutMs = options.moveTransactionTimeoutMs ?? 15_000
    this.executor = new MoveExecutor(
      nodes,
      pending,
      cross,
      this.telemetry,
      {
        reservationTtlMs: transactionTimeoutMs,
        prepareTtlMs: 10_000,
        redirectTtlMs: 10_000
      },
      log
    )
  }
  public ingest (event: INodeEvent): void {
    if (event.event === 'room.destroyed' && event.data.roomId) {
      this.forgetRoom(event.data.roomId as string)
    }
    if (event.event === 'room.telemetry' && event.data.roomId) {
      const roomId = event.data.roomId
      const incoming = Array.isArray(event.data.players)
        ? event.data.players
        : []
      const receivedAt = Date.now()
      const frameTimestamp =
        typeof event.data.timestamp === 'number'
          ? event.data.timestamp
          : undefined
      const rawOffset =
        frameTimestamp === undefined ? 0 : frameTimestamp - receivedAt
      const clockOffset = Math.abs(rawOffset) >= 1_000 ? rawOffset : 0
      if (Math.abs(rawOffset) > 10_000) {
        const at = receivedAt
        if (at - (this.clockSkewWarnAt.get(roomId) ?? 0) >= 300_000) {
          this.clockSkewWarnAt.set(roomId, at)
          const direction = rawOffset < 0 ? 'behind' : 'ahead of'
          this.log?.warn(
            {
              roomId,
              nodeId: event.data.nodeId,
              offsetMs: Math.round(rawOffset)
            },
            `node clock is ${Math.round(
              Math.abs(rawOffset) / 1000
            )}s ${direction} the manager; normalising telemetry timestamps`
          )
          this.record({
            category: 'telemetry',
            event: 'telemetry.clock_skew',
            message: `node clock is ${Math.round(
              Math.abs(rawOffset) / 1000
            )}s ${direction} the manager`,
            roomId,
            nodeId: event.data.nodeId,
            data: { offsetMs: Math.round(rawOffset) }
          })
        }
      }
      let roster = this.roomRoster.get(roomId as string)
      if (event.data.kind === 'delta') {
        const baseOk = roster !== undefined && roster.seq === event.data.baseSeq
        if (!baseOk) {
          this.onTelemetryGap?.()
          const at = Date.now()
          const gap = this.telemetryGap.get(roomId) ?? {
            since: at,
            dropped: 0,
            haveSeq: roster?.seq
          }
          gap.dropped += 1
          this.telemetryGap.set(roomId, gap)
          const fields = {
            roomId,
            nodeId: event.data.nodeId,
            seq: event.data.seq,
            baseSeq: event.data.baseSeq,
            haveSeq: roster?.seq,
            droppedFrames: gap.dropped,
            gapMs: at - gap.since,
            players: incoming.length
          }
          if (gap.dropped === 1) {
            this.record({
              category: 'telemetry',
              event: 'telemetry.gap',
              message:
                'delta telemetry out of sync; room state frozen until the next full snapshot',
              roomId,
              nodeId: event.data.nodeId,
              data: {
                seq: event.data.seq,
                baseSeq: event.data.baseSeq,
                haveSeq: roster?.seq,
                players: incoming.length
              }
            })
          }
          if (at - (this.telemetryGapWarnAt.get(roomId) ?? 0) >= 30_000) {
            this.telemetryGapWarnAt.set(roomId, at)
            this.log?.warn(
              fields,
              'telemetry delta gap; awaiting full snapshot'
            )
          } else {
            this.log?.debug(
              fields,
              'telemetry delta gap; awaiting full snapshot'
            )
          }
          return
        }
        for (const playerId of event.data.removedPlayers ?? []) {
          roster!.players.delete(playerId as string)
        }
        for (const player of incoming)
          roster!.players.set(player.playerId as string, player)
        roster!.seq =
          typeof event.data.seq === 'number' ? event.data.seq : roster!.seq + 1
      } else {
        const players = new Map<string, typeof incoming[number]>()
        for (const player of incoming)
          players.set(player.playerId as string, player)
        roster = {
          seq: typeof event.data.seq === 'number' ? event.data.seq : 0,
          players
        }
        this.roomRoster.set(roomId as string, roster)
        for (const tracked of this.telemetry.listRoom(roomId)) {
          if (!players.has(tracked.playerId as string))
            this.telemetry.remove(tracked.playerId)
        }
        const gap = this.telemetryGap.get(roomId)
        if (gap) {
          this.telemetryGap.delete(roomId)
          this.telemetryGapWarnAt.delete(roomId)
          const fields = {
            roomId,
            nodeId: event.data.nodeId,
            gapMs: Date.now() - gap.since,
            droppedFrames: gap.dropped,
            fromSeq: gap.haveSeq,
            toSeq: event.data.seq,
            players: incoming.length
          }
          this.log?.warn(fields, 'telemetry resynchronized after gap')
          this.record({
            category: 'telemetry',
            event: 'telemetry.resynced',
            message: 'full snapshot restored the room roster after a delta gap',
            roomId,
            nodeId: event.data.nodeId,
            data: fields
          })
        }
      }
      for (const player of incoming) {
        const normalized: PlayerTelemetry =
          clockOffset === 0
            ? player
            : {
                ...player,
                ...(player.stateAt !== undefined
                  ? { stateAt: asUnixMs(player.stateAt - clockOffset) }
                  : {}),
                ...(player.lastMoveAt !== undefined
                  ? { lastMoveAt: asUnixMs(player.lastMoveAt - clockOffset) }
                  : {})
              }
        const previous = this.telemetry.get(player.playerId)
        const stored = this.telemetry.upsert(roomId, normalized, receivedAt)
        if (
          previous?.levelId !== undefined &&
          stored.levelId !== undefined &&
          previous.levelId !== stored.levelId
        ) {
          this.record({
            category: 'player',
            event: 'player.level_changed',
            message: `level ${previous.levelId} -> ${stored.levelId}`,
            roomId,
            nodeId: event.data.nodeId,
            playerIds: [player.playerId],
            data: { from: previous.levelId, to: stored.levelId }
          })
        }
        this.rooms.trackPlayer(
          player.playerId,
          event.data.nodeId,
          roomId,
          player.levelId
        )
      }
      const room = this.rooms.getRoom(roomId)
      if (room) {
        const count = roster ? roster.players.size : incoming.length
        const version =
          typeof event.data.seq === 'number' ? event.data.seq : room.version
        const applied = this.rooms.upsertRoom({
          ...room,
          players: Math.min(room.capacity, count),
          version
        })
        if (!applied) {
          this.onTelemetryGap?.()
          const at = Date.now()
          if (at - (this.telemetryVersionWarnAt.get(roomId) ?? 0) >= 30_000) {
            this.telemetryVersionWarnAt.set(roomId, at)
            this.log?.warn(
              {
                roomId,
                nodeId: event.data.nodeId,
                seq: version,
                cachedVersion: room.version,
                kind: event.data.kind ?? 'full'
              },
              'telemetry version regression rejected'
            )
            this.record({
              category: 'telemetry',
              event: 'telemetry.version_rejected',
              message:
                'telemetry frame older than the cached room snapshot was ignored',
              roomId,
              nodeId: event.data.nodeId,
              data: {
                seq: version,
                cachedVersion: room.version,
                kind: event.data.kind ?? 'full'
              }
            })
          }
        }
      }
    }
    if (
      (event.event === 'player.join' || event.event === 'player.leave') &&
      event.data.player?.playerId
    ) {
      if (event.data.roomId) {
        const roster = this.roomRoster.get(event.data.roomId as string)
        if (roster && event.event === 'player.leave') {
          roster.players.delete(event.data.player.playerId as string)
        }
      }
      if (event.event === 'player.leave') {
        for (const tx of this.transactions.values()) {
          if (
            tx.status === 'committed' &&
            tx.sourceRoomId === event.data.roomId &&
            tx.playerIds.includes(event.data.player.playerId as string)
          ) {
            if (!tx.leftPlayers.includes(event.data.player.playerId as string))
              tx.leftPlayers.push(event.data.player.playerId as string)
            if (
              tx.leftPlayers.length === tx.playerIds.length &&
              tx.joinedPlayers.length === tx.playerIds.length
            ) {
              tx.status = 'completed'
              this.telemetry.releaseReservation(
                tx.targetRoomId as RoomId,
                tx.moveId
              )
            }
          }
        }
        const current = this.telemetry.get(event.data.player.playerId)
        if (!current || current.roomId === event.data.roomId)
          this.telemetry.remove(event.data.player.playerId)
      } else if (event.data.roomId) {
        const joinLevel = event.data.player.level
        const previous = this.telemetry.get(event.data.player.playerId)
        const stored = this.telemetry.upsert(event.data.roomId, {
          playerId: event.data.player.playerId,
          ...(joinLevel !== undefined ? { levelId: joinLevel } : {})
        })
        if (
          previous?.levelId !== undefined &&
          stored.levelId !== undefined &&
          previous.levelId !== stored.levelId
        ) {
          this.record({
            category: 'player',
            event: 'player.level_changed',
            message: `level ${previous.levelId} -> ${stored.levelId}`,
            roomId: event.data.roomId,
            nodeId: event.data.nodeId,
            playerIds: [event.data.player.playerId],
            data: {
              from: previous.levelId,
              to: stored.levelId,
              source: 'player.join'
            }
          })
        }
      }
      if (event.event === 'player.join' && event.data.roomId) {
        for (const tx of this.transactions.values()) {
          if (
            tx.status === 'committed' &&
            tx.targetRoomId === event.data.roomId &&
            tx.playerIds.includes(event.data.player.playerId as string)
          ) {
            if (
              !tx.joinedPlayers.includes(event.data.player.playerId as string)
            )
              tx.joinedPlayers.push(event.data.player.playerId as string)
            if (
              tx.leftPlayers.length === tx.playerIds.length &&
              tx.joinedPlayers.length === tx.playerIds.length
            ) {
              tx.status = 'completed'
              this.telemetry.releaseReservation(
                tx.targetRoomId as RoomId,
                tx.moveId
              )
            }
          }
        }
      }
    }
    if (event.event === 'move.result' && event.data.moveId) {
      const tx = this.transactions.get(event.data.moveId)
      if (tx && event.data.accepted === false) {
        const failedIds = event.data.failedPlayerIds?.length
          ? event.data.failedPlayerIds
          : tx.playerIds
        tx.playerIds = tx.playerIds.filter(id => !failedIds.includes(id))
        this.telemetry.releaseReservationSlots(
          tx.targetRoomId as RoomId,
          tx.moveId,
          failedIds.length
        )
        if (tx.playerIds.length === 0) tx.status = 'failed'
        if (failedIds.length)
          this.queueCancel(tx.targetRoomId as RoomId, tx.moveId, failedIds)
        this.record({
          category: 'move',
          event: 'move.clients_rejected',
          message: `clients rejected part of ${tx.moveId} (${failedIds.length} player(s))`,
          roomId: tx.targetRoomId,
          playerIds: failedIds,
          data: {
            moveId: tx.moveId,
            failedPlayerIds: failedIds,
            reason: event.data.reason
          }
        })
      }
    }
    if (event.event === 'reservation.expired' && event.data.moveId) {
      const tx = this.transactions.get(event.data.moveId)
      if (tx && (tx.status === 'prepared' || tx.status === 'committed')) {
        tx.status = 'failed'
        this.telemetry.releaseReservation(tx.targetRoomId as RoomId, tx.moveId)
        this.onMoveOutcome?.(tx.reason, 'failed')
        this.log?.warn(
          { moveId: tx.moveId, playerIds: tx.playerIds },
          'target reservation expired before players arrived'
        )
        this.record({
          category: 'move',
          event: 'move.reservation_expired',
          message: `target reservation expired for ${tx.moveId}`,
          roomId: tx.targetRoomId,
          playerIds: tx.playerIds,
          data: { moveId: tx.moveId, reason: tx.reason }
        })
      }
    }
    if (event.event === 'move.completed' && event.data.moveId) {
      const tx = this.transactions.get(event.data.moveId)
      if (tx && (tx.status === 'committed' || tx.status === 'prepared')) {
        tx.status = 'completed'
        this.telemetry.releaseReservation(tx.targetRoomId as RoomId, tx.moveId)
        this.log?.info(
          {
            moveId: tx.moveId,
            playerIds: tx.playerIds,
            targetRoomId: tx.targetRoomId
          },
          'move completed (node confirmation)'
        )
        this.record({
          category: 'move',
          event: 'move.completed',
          message: `${tx.reason}: ${tx.sourceRoomId} -> ${tx.targetRoomId} completed`,
          roomId: tx.targetRoomId,
          playerIds: tx.playerIds,
          data: {
            moveId: tx.moveId,
            reason: tx.reason,
            sourceRoomId: tx.sourceRoomId,
            targetRoomId: tx.targetRoomId
          }
        })
      }
    }
  }
  public async planAndExecute (
    signal?: AbortSignal,
    now = Date.now()
  ): Promise<Array<MoveTransaction | LevelMoveTransaction>> {
    if (now - this.lastPlanAt < 2_000) return [...this.transactions.values()]
    if (this.planning) return [...this.transactions.values()]
    this.planning = true
    this.lastPlanAt = now
    try {
      return await this.runCycle(signal, now)
    } finally {
      this.planning = false
    }
  }
  private async runCycle (
    signal: AbortSignal | undefined,
    now: number
  ): Promise<Array<MoveTransaction | LevelMoveTransaction>> {
    this.drainPendingCancels()
    this.expireTransactions(now)
    const plannableRooms = this.rooms
      .listRooms()
      .filter(room => this.isRoomPlannable(room))
    const allRosters = new Map(
      plannableRooms.map(room => [
        room.roomId as string,
        this.telemetry.listRoom(room.roomId, now)
      ])
    )
    const rooms = plannableRooms.filter(room =>
      this.isRoomStable(
        room.roomId as string,
        allRosters.get(room.roomId as string) ?? []
      )
    )
    const byRoom = new Map(
      rooms.map(room => [
        room.roomId as string,
        allRosters.get(room.roomId as string) ?? []
      ])
    )
    this.planner.calibrate(rooms, byRoom, now)
    const isolationPlans = this.levelIsolationPlanner.plan(
      rooms,
      this.telemetry,
      now,
      this.log
    )
    const plans = this.planner.plan(rooms, byRoom, now)
    const usedTargets = new Set<string>()
    let moveBudget = this.options.moveBudgetPerCycle ?? 8
    const isolationRooms = new Set<string>()
    for (const isolationPlan of isolationPlans) {
      isolationRooms.add(isolationPlan.sourceRoomId)
      isolationRooms.add(isolationPlan.targetRoomId)
    }
    for (const isolationPlan of isolationPlans) {
      if (
        moveBudget < isolationPlan.playerIds.length ||
        usedTargets.has(isolationPlan.targetRoomId)
      )
        continue
      if (
        this.roomBusy(isolationPlan.sourceRoomId, now) ||
        this.roomBusy(isolationPlan.targetRoomId, now)
      )
        continue
      const executed = await this.executeLevelIsolation(
        isolationPlan,
        now,
        signal
      )
      if (executed) {
        usedTargets.add(isolationPlan.targetRoomId)
        moveBudget -= isolationPlan.playerIds.length
      }
    }
    for (const plan of plans) {
      if (
        moveBudget < plan.playerIds.length ||
        usedTargets.has(plan.targetRoomId)
      )
        continue
      if (
        isolationRooms.has(plan.sourceRoomId) ||
        isolationRooms.has(plan.targetRoomId)
      )
        continue
      if (
        this.roomBusy(plan.sourceRoomId, now) ||
        this.roomBusy(plan.targetRoomId, now)
      )
        continue
      const source = this.rooms.getRoom(plan.sourceRoomId as RoomId)
      const target = this.rooms.getRoom(plan.targetRoomId as RoomId)
      if (
        !source ||
        !target ||
        this.transactionsForPlayer(plan.playerIds).length > 0
      )
        continue
      if (
        this.planSuppressed(
          plan.sourceRoomId,
          plan.targetRoomId,
          plan.playerIds,
          byRoom.get(plan.sourceRoomId) ?? []
        )
      )
        continue
      const moveId = this.nextMoveId('move', now)
      const tx: MoveTransaction = {
        ...plan,
        moveId,
        status: 'prepared',
        createdAt: now,
        joinedPlayers: [],
        leftPlayers: []
      }
      this.transactions.set(moveId, tx)
      const outcome = await this.executor.execute({
        moveId,
        playerIds: plan.playerIds,
        source,
        target,
        reason:
          plan.reason === 'low_activity_merge' ? 'merge' : 'capacity-rebalance',
        now,
        signal
      })
      if (outcome.ok) {
        this.transactions.set(moveId, { ...tx, status: 'committed' })
        usedTargets.add(plan.targetRoomId)
        moveBudget -= plan.playerIds.length
        this.onMoveOutcome?.(plan.reason, 'committed')
        this.log?.info(
          {
            moveId,
            playerIds: plan.playerIds,
            sourceRoomId: plan.sourceRoomId,
            targetRoomId: plan.targetRoomId,
            reason: plan.reason,
            gain: plan.gain
          },
          'move committed'
        )
        this.record({
          category: 'move',
          event: 'move.committed',
          message: `${plan.reason}: ${plan.sourceRoomId} -> ${plan.targetRoomId} (${plan.playerIds.length} player(s))`,
          roomId: plan.targetRoomId,
          playerIds: plan.playerIds,
          data: {
            moveId,
            reason: plan.reason,
            sourceRoomId: plan.sourceRoomId,
            targetRoomId: plan.targetRoomId,
            gain: plan.gain
          }
        })
      } else if (outcome.reason === 'failed') {
        if (outcome.permanent) {
          this.suppressPlan(
            plan.sourceRoomId,
            plan.targetRoomId,
            plan.playerIds,
            byRoom.get(plan.sourceRoomId) ?? []
          )
        }
        this.queueCancel(target.roomId, moveId, plan.playerIds)
        this.transactions.set(moveId, { ...tx, status: 'failed' })
        this.onMoveOutcome?.(plan.reason, 'failed')
        this.record({
          category: 'move',
          event: 'move.failed',
          message: `${plan.reason}: ${plan.sourceRoomId} -> ${plan.targetRoomId} failed`,
          roomId: plan.targetRoomId,
          playerIds: plan.playerIds,
          data: {
            moveId,
            reason: plan.reason,
            sourceRoomId: plan.sourceRoomId,
            targetRoomId: plan.targetRoomId,
            error:
              outcome.error instanceof Error
                ? outcome.error.message
                : String(outcome.error ?? '')
          }
        })
      } else {
        this.transactions.delete(moveId)
        this.record({
          category: 'move',
          event: 'move.rejected',
          message: `${plan.reason}: ${plan.sourceRoomId} -> ${plan.targetRoomId} rejected (${outcome.reason})`,
          roomId: plan.targetRoomId,
          playerIds: plan.playerIds,
          data: {
            moveId,
            reason: plan.reason,
            outcome: outcome.reason,
            sourceRoomId: plan.sourceRoomId,
            targetRoomId: plan.targetRoomId,
            gain: plan.gain
          }
        })
      }
    }
    return [...this.transactions.values()]
  }
  private isRoomPlannable (room: RoomSnapshot): boolean {
    if (room.draining) return false
    if (this.rooms.isNodeSuspected(room.nodeId)) return false
    return this.nodes.isAvailableForCommand(room.nodeId)
  }
  private expireTransactions (now: number): void {
    const timeoutMs = this.options.moveTransactionTimeoutMs ?? 15_000
    for (const [moveId, tx] of this.transactions) {
      if (tx.status === 'prepared') {
        if (now - tx.createdAt > timeoutMs) {
          tx.status = 'failed'
          this.telemetry.releaseReservation(
            tx.targetRoomId as RoomId,
            tx.moveId
          )
          this.queueCancel(tx.targetRoomId as RoomId, tx.moveId, tx.playerIds)
          this.log?.warn(
            {
              moveId,
              playerIds: tx.playerIds,
              sourceRoomId: tx.sourceRoomId,
              targetRoomId: tx.targetRoomId
            },
            'prepared move timed out'
          )
          this.recordMoveExpired(tx, 'prepared')
        }
        continue
      }
      if (tx.status === 'committed') {
        if (now - tx.createdAt > timeoutMs * 2) {
          tx.status = 'failed'
          this.telemetry.releaseReservation(
            tx.targetRoomId as RoomId,
            tx.moveId
          )
          this.queueCancel(tx.targetRoomId as RoomId, tx.moveId, tx.playerIds)
          this.log?.warn(
            {
              moveId,
              playerIds: tx.playerIds,
              sourceRoomId: tx.sourceRoomId,
              targetRoomId: tx.targetRoomId
            },
            'committed move never resolved (client lost mid-migration?)'
          )
          this.recordMoveExpired(tx, 'committed')
        }
        continue
      }
      if (now - tx.createdAt > TRANSACTION_RETENTION_MS)
        this.transactions.delete(moveId)
    }
  }
  private async executeLevelIsolation (
    plan: LevelIsolationPlan,
    now: number,
    signal: AbortSignal | undefined
  ): Promise<boolean> {
    const source = this.rooms.getRoom(plan.sourceRoomId as RoomId)
    const target = this.rooms.getRoom(plan.targetRoomId as RoomId)
    if (!source || !target) {
      this.log?.warn(
        {
          sourceRoomId: plan.sourceRoomId,
          targetRoomId: plan.targetRoomId,
          levelId: plan.levelId
        },
        'level isolation skipped: room no longer cached'
      )
      this.recordSkippedLevelPlan(plan, 'room_missing')
      return false
    }
    if (this.transactionsForPlayer(plan.playerIds).length > 0) {
      this.log?.debug(
        { playerIds: plan.playerIds },
        'level isolation skipped: player already has a pending move'
      )
      this.recordSkippedLevelPlan(plan, 'player_pending')
      return false
    }
    if (this.roomBusy(plan.targetRoomId, now)) {
      this.log?.debug(
        { targetRoomId: plan.targetRoomId, levelId: plan.levelId },
        'level isolation skipped: target has an in-flight move'
      )
      this.recordSkippedLevelPlan(plan, 'target_busy')
      return false
    }
    const sourcePlayers = this.telemetry.listRoom(
      plan.sourceRoomId as RoomId,
      now
    )
    if (
      this.planSuppressed(
        plan.sourceRoomId,
        plan.targetRoomId,
        plan.playerIds,
        sourcePlayers
      )
    ) {
      this.log?.debug(
        { sourceRoomId: plan.sourceRoomId, targetRoomId: plan.targetRoomId },
        'level isolation skipped: identical plan was rejected'
      )
      this.recordSkippedLevelPlan(plan, 'previously_rejected')
      return false
    }
    const moveId = this.nextMoveId('isolation', now, String(plan.levelId))
    const tx: LevelMoveTransaction = {
      ...plan,
      moveId,
      status: 'prepared',
      createdAt: now,
      joinedPlayers: [],
      leftPlayers: []
    }
    this.transactions.set(moveId, tx)
    const outcome = await this.executor.execute({
      moveId,
      playerIds: plan.playerIds,
      source,
      target,
      reason:
        plan.reason === 'level_consolidation'
          ? 'level-consolidation'
          : 'level-isolation',
      now,
      signal
    })
    if (outcome.ok) {
      this.transactions.set(moveId, { ...tx, status: 'committed' })
      this.onMoveOutcome?.(plan.reason, 'committed')
      this.log?.info(
        {
          moveId,
          reason: plan.reason,
          playerIds: plan.playerIds,
          sourceRoomId: plan.sourceRoomId,
          targetRoomId: plan.targetRoomId,
          levelId: plan.levelId,
          sourceLevelCount: plan.sourceLevelCount,
          targetLevelCount: plan.targetLevelCount
        },
        'level isolation committed'
      )
      this.record({
        category: 'level',
        event:
          plan.reason === 'level_consolidation'
            ? 'level.consolidated'
            : 'level.isolated',
        message: `${plan.reason}: ${plan.sourceRoomId} -> ${plan.targetRoomId} (level ${plan.levelId}, ${plan.playerIds.length} player(s))`,
        roomId: plan.targetRoomId,
        playerIds: plan.playerIds,
        data: {
          moveId,
          reason: plan.reason,
          sourceRoomId: plan.sourceRoomId,
          targetRoomId: plan.targetRoomId,
          levelId: plan.levelId,
          sourceLevelCount: plan.sourceLevelCount,
          targetLevelCount: plan.targetLevelCount
        }
      })
      return true
    }
    if (outcome.reason === 'failed') {
      if (outcome.permanent)
        this.suppressPlan(
          plan.sourceRoomId,
          plan.targetRoomId,
          plan.playerIds,
          sourcePlayers
        )
      this.queueCancel(target.roomId, moveId, plan.playerIds)
      this.transactions.set(moveId, { ...tx, status: 'failed' })
      this.onMoveOutcome?.(plan.reason, 'failed')
      this.record({
        category: 'level',
        event: 'level.migration_failed',
        message: `${plan.reason}: ${plan.sourceRoomId} -> ${plan.targetRoomId} failed`,
        roomId: plan.targetRoomId,
        playerIds: plan.playerIds,
        data: {
          moveId,
          reason: plan.reason,
          sourceRoomId: plan.sourceRoomId,
          targetRoomId: plan.targetRoomId,
          levelId: plan.levelId
        }
      })
    } else {
      this.log?.info(
        {
          moveId,
          reason: outcome.reason,
          planReason: plan.reason,
          playerIds: plan.playerIds,
          sourceRoomId: plan.sourceRoomId,
          targetRoomId: plan.targetRoomId,
          levelId: plan.levelId
        },
        'level isolation not executed'
      )
      this.transactions.delete(moveId)
      this.record({
        category: 'level',
        event: 'level.migration_rejected',
        message: `${plan.reason}: ${plan.sourceRoomId} -> ${plan.targetRoomId} rejected (${outcome.reason})`,
        roomId: plan.targetRoomId,
        playerIds: plan.playerIds,
        data: {
          moveId,
          reason: plan.reason,
          outcome: outcome.reason,
          sourceRoomId: plan.sourceRoomId,
          targetRoomId: plan.targetRoomId,
          levelId: plan.levelId
        }
      })
    }
    return false
  }
  private nextMoveId (kind: string, now: number, tag?: string): string {
    this.moveSeq += 1
    const suffix = this.moveSeq.toString(36)
    return tag === undefined
      ? `${kind}-${now.toString(36)}-${suffix}`
      : `${kind}-${tag}-${now.toString(36)}-${suffix}`
  }
  public forgetRoom (roomId: string): void {
    this.roomRoster.delete(roomId)
    this.roomStability.delete(roomId)
    this.telemetryGap.delete(roomId)
    this.telemetryGapWarnAt.delete(roomId)
    this.telemetryVersionWarnAt.delete(roomId)
    this.clockSkewWarnAt.delete(roomId)
    for (const key of [...this.suppressedPlans.keys()]) {
      const pair = key.slice(0, key.indexOf(':')).split('->')
      if (pair[0] === roomId || pair[1] === roomId)
        this.suppressedPlans.delete(key)
    }
  }
  private record (event: DebugEventInput): void {
    this.events?.record(event)
  }
  private recordSkippedLevelPlan (
    plan: LevelIsolationPlan,
    reason: string
  ): void {
    this.record({
      category: 'level',
      event: 'level.migration_skipped',
      message: `${plan.reason}: ${plan.sourceRoomId} -> ${plan.targetRoomId} skipped (${reason})`,
      roomId: plan.targetRoomId,
      playerIds: plan.playerIds,
      data: {
        reason: plan.reason,
        outcome: reason,
        sourceRoomId: plan.sourceRoomId,
        targetRoomId: plan.targetRoomId,
        levelId: plan.levelId
      }
    })
  }
  private recordMoveExpired (
    tx: MoveTransaction | LevelMoveTransaction,
    status: 'prepared' | 'committed'
  ): void {
    this.record({
      category: 'move',
      event: 'move.expired',
      message: `${tx.reason}: ${tx.sourceRoomId} -> ${tx.targetRoomId} expired (${status})`,
      roomId: tx.targetRoomId,
      playerIds: tx.playerIds,
      data: {
        moveId: tx.moveId,
        reason: tx.reason,
        status,
        sourceRoomId: tx.sourceRoomId,
        targetRoomId: tx.targetRoomId
      }
    })
  }
  public list (): Array<MoveTransaction | LevelMoveTransaction> {
    return [...this.transactions.values()]
  }
  public telemetryGapCount (): number {
    return this.telemetryGap.size
  }
  public async manualMove (
    input: {
      sourceRoomId: string
      targetRoomId: string
      playerIds: string[]
      reason?: string
    },
    signal?: AbortSignal,
    now = Date.now()
  ): Promise<MoveTransaction | LevelMoveTransaction> {
    const playerIds = [...new Set(input.playerIds.map(String).filter(Boolean))]
    if (
      !input.sourceRoomId ||
      !input.targetRoomId ||
      input.sourceRoomId === input.targetRoomId ||
      playerIds.length === 0 ||
      playerIds.length > 7
    ) {
      throw new Error('invalid debug move request')
    }
    if (this.transactionsForPlayer(playerIds).length > 0)
      throw new Error('one or more players already have a pending move')
    const source = this.rooms.getRoom(input.sourceRoomId as RoomId)
    const target = this.rooms.getRoom(input.targetRoomId as RoomId)
    if (!source || !target) throw new Error('source or target room not found')
    const locations = playerIds.map(id =>
      this.rooms.getPlayerLocation(id as PlayerId)
    )
    if (
      locations.some(location => !location || location.roomId !== source.roomId)
    )
      throw new Error('one or more players are not in the source room')
    const moveId = this.nextMoveId('debug', now)
    const tx: MoveTransaction = {
      playerIds,
      sourceRoomId: source.roomId,
      targetRoomId: target.roomId,
      gain: 0,
      reason:
        input.reason === 'merge' ? 'low_activity_merge' : 'player_rebalance',
      moveId,
      status: 'prepared',
      createdAt: now,
      joinedPlayers: [],
      leftPlayers: []
    }
    this.transactions.set(moveId, tx)
    const outcome = await this.executor.execute({
      moveId,
      playerIds,
      source,
      target,
      reason:
        tx.reason === 'low_activity_merge' ? 'merge' : 'capacity-rebalance',
      now,
      signal
    })
    if (!outcome.ok) {
      if (outcome.reason === 'failed') {
        this.queueCancel(target.roomId, moveId, playerIds)
        this.transactions.set(moveId, { ...tx, status: 'failed' })
        throw outcome.error ?? new Error('move failed')
      }
      this.transactions.delete(moveId)
      throw new Error(
        outcome.reason === 'no_capacity'
          ? 'target room has insufficient capacity'
          : outcome.reason === 'stale_plan'
          ? 'target room state changed; retry'
          : 'move could not be executed'
      )
    }
    const committed = { ...tx, status: 'committed' as const }
    this.transactions.set(moveId, committed)
    return committed
  }
  public activitySnapshot (
    now = Date.now()
  ): Array<ReturnType<MovePlanner['activity']>> {
    return this.rooms
      .listRooms()
      .map(room =>
        this.planner.activity(
          room,
          this.telemetry.listRoom(room.roomId, now),
          now
        )
      )
  }
  private transactionsForPlayer (
    playerIds: string[]
  ): Array<MoveTransaction | LevelMoveTransaction> {
    return [...this.transactions.values()]
      .filter(tx => tx.status === 'prepared' || tx.status === 'committed')
      .filter(tx => tx.playerIds.some(id => playerIds.includes(id)))
  }
  private roomBusy (roomId: string, cycleNow: number): boolean {
    for (const tx of this.transactions.values()) {
      if (tx.status !== 'prepared' && tx.status !== 'committed') continue
      if (tx.createdAt >= cycleNow) continue
      if (tx.sourceRoomId === roomId || tx.targetRoomId === roomId) return true
    }
    return false
  }
  private roomSignature (players: PlayerTelemetry[]): string {
    return players
      .map(player => `${player.playerId}:${player.levelId ?? '?'}`)
      .sort()
      .join(',')
  }
  private planKey (
    sourceRoomId: string,
    targetRoomId: string,
    playerIds: string[]
  ): string {
    return `${sourceRoomId}->${targetRoomId}:${[...playerIds].sort().join(',')}`
  }
  private planSuppressed (
    sourceRoomId: string,
    targetRoomId: string,
    playerIds: string[],
    players: PlayerTelemetry[]
  ): boolean {
    const key = this.planKey(sourceRoomId, targetRoomId, playerIds)
    const signature = this.suppressedPlans.get(key)
    if (signature === undefined) return false
    if (signature === this.roomSignature(players)) return true
    this.suppressedPlans.delete(key)
    return false
  }
  private suppressPlan (
    sourceRoomId: string,
    targetRoomId: string,
    playerIds: string[],
    players: PlayerTelemetry[]
  ): void {
    if (this.suppressedPlans.size >= 1000) {
      const oldest = this.suppressedPlans.keys().next().value
      if (oldest !== undefined) this.suppressedPlans.delete(oldest)
    }
    this.suppressedPlans.set(
      this.planKey(sourceRoomId, targetRoomId, playerIds),
      this.roomSignature(players)
    )
  }
  private isRoomStable (roomId: string, players: PlayerTelemetry[]): boolean {
    const signature = this.roomSignature(players)
    const previous = this.roomStability.get(roomId)
    const cycles = previous?.signature === signature ? previous.cycles + 1 : 1
    this.roomStability.set(roomId, { signature, cycles })
    return cycles >= (this.options.confirmationCycles ?? 2)
  }
  private queueCancel (
    roomId: RoomId,
    moveId: string,
    playerIds: string[]
  ): void {
    this.pendingCancels.set(moveId, { roomId, moveId, playerIds })
    this.flushCancel(moveId)
  }
  private flushCancel (moveId: string): void {
    const entry = this.pendingCancels.get(moveId)
    if (!entry) return
    const room = this.rooms.getRoom(entry.roomId)
    const transport = room ? this.nodes.getTransport(room.nodeId) : undefined
    if (!transport?.writable) return
    this.pending
      .send(
        {
          v: asProtocolVersion(1),
          cmd: 'move.cancel',
          data: { moveId: entry.moveId, playerIds: entry.playerIds }
        },
        command => transport.send(command)
      )
      .then(() => {
        this.pendingCancels.delete(entry.moveId)
      })
      .catch(error => {
        this.log?.debug(
          { err: error, moveId },
          'move.cancel send failed (will retry)'
        )
      })
  }
  private drainPendingCancels (): void {
    for (const moveId of [...this.pendingCancels.keys()])
      this.flushCancel(moveId)
  }
}
