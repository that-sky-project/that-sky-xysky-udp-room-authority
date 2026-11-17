import { MoveTransactions, type MoveTransaction } from './MoveTransactions.js'
import { PlanningScheduler } from './PlanningScheduler.js'
import { MoveCancellationQueue } from './MoveCancellationQueue.js'
import { RoomTelemetryService } from './RoomTelemetryService.js'
import { applyCapacityFacts } from './CapacityFacts.js'
import { randomUUID } from 'node:crypto'
import type { NodeId, PlayerId, RoomId } from '../types/branded.js'
import { asProtocolVersion } from '../types/branded.js'
import type { INodeEvent, PlayerTelemetry, RoomSnapshot } from '../types/contracts.js'
import type { Logger } from '../infra/logger/logger.js'
import type { CrossCoordinator, SourceParticipantSnapshot, RedirectReason } from './CrossCoordinator.js'
import type { DebugEventInput, DebugEventLog } from './DebugEventLog.js'
import type { NodeRegistry } from './NodeRegistry.js'
import type { PendingRequestMap } from './PendingRequestMap.js'
import type { RoomCache } from './RoomCache.js'
import { MovePlanner, type MovePlan, type MovePlannerOptions } from './MovePlanner.js'
import { TelemetryStore } from './TelemetryStore.js'
import { LevelIsolationPlanner, type LevelIsolationPlan, type LevelIsolationOptions } from './LevelIsolationPlanner.js'
import { MoveExecutor, type MoveExecuteOutcome } from './MoveExecutor.js'

export interface MoveCoordinatorOptions {
  moveBudgetPerCycle?: number
  moveTransactionTimeoutMs?: number
  prepareTimeoutMs?: number
  arrivalTimeoutMs?: number
  reservationTtlMs?: number
  retryBaseMs?: number
  retryMaxMs?: number
  maxAttempts?: number
  maxCycleMs?: number
  arrivalCooldownMs?: number
  remigrateCooldownMs?: number
  effectivePlayersOf?: ((room: RoomSnapshot) => number) | undefined
  stateEmaHalfLifeMs?: number
  confirmationCycles?: number
  planner?: Partial<MovePlannerOptions>
  levelIsolation?: Partial<LevelIsolationOptions>
  /** Level-flap quarantine knobs passed through to the telemetry store. */
  churnMaxChanges?: number
  churnWindowMs?: number
  churnQuarantineMs?: number
}

/** Reasons a planned move never reached the executor; recorded as plan.skipped. */
export type PlanSkipReason =
  | 'busy_source'
  | 'busy_target'
  | 'tx_active_for_player'
  | 'target_capacity'
  | 'bond_incomplete'
  | 'budget_exhausted'
  | 'room_claimed'
  | 'source_not_candidate'
  | 'target_not_candidate'
  | 'plan_too_large'
  | 'isolation_no_target'
  | 'stale_observation'

const SKIP_RATE_LIMIT_MS = 15_000

/** Node admission is final authority; this coordinator owns bounded, fenced control-plane transactions. */
export class MoveCoordinator {
  public readonly telemetry: TelemetryStore
  private readonly planner: MovePlanner
  private readonly levelIsolationPlanner: LevelIsolationPlanner
  private readonly executor: MoveExecutor
  private readonly transactions = new MoveTransactions()
  private readonly roomTelemetry: RoomTelemetryService
  private readonly roomStability = new Map<string, { signature: string; cycles: number; seq: number }>()
  private readonly telemetryGap = new Map<string, { since: number; dropped: number }>()
  private readonly recentArrivals = new Map<string, number>()
  private readonly migratedPlayers = new Map<string, number>()
  private readonly cancellations: MoveCancellationQueue
  private readonly scheduler: PlanningScheduler
  private readonly targetBackoff = new Map<string, { until: number; attempts: number; version: number; players: number }>()
  private readonly syncRequestedAt = new Map<string, number>()
  private readonly skipEvents = new Map<string, number>()
  private readonly lastSkipByRoom = new Map<string, { reason: PlanSkipReason; at: number }>()
  private stopPromise: Promise<void> | undefined
  private moveSeq = 0

  public constructor (
    private readonly rooms: RoomCache, private readonly nodes: NodeRegistry,
    private readonly pending: PendingRequestMap, private readonly cross: CrossCoordinator,
    private readonly options: MoveCoordinatorOptions = {}, private readonly log?: Logger,
    private readonly onMoveOutcome?: (reason: string, status: 'committed' | 'failed') => void,
    private readonly onTelemetryGap?: () => void, private readonly events?: DebugEventLog
  ) {
    this.telemetry = new TelemetryStore({
      stateEmaHalfLifeMs: options.stateEmaHalfLifeMs ?? 3_000,
      churnMaxChanges: options.churnMaxChanges ?? 3,
      churnWindowMs: options.churnWindowMs ?? 60_000,
      churnQuarantineMs: options.churnQuarantineMs ?? 120_000
    })
    this.roomTelemetry = new RoomTelemetryService({ rooms, nodes, telemetry: this.telemetry,
      requestFull: roomId => this.requestFull(roomId), onGap: onTelemetryGap,
      onReset: roomId => this.roomStability.delete(roomId), onAccepted: () => this.requestPlan() })
    this.planner = new MovePlanner(options.planner)
    this.levelIsolationPlanner = new LevelIsolationPlanner(options.levelIsolation)
    this.executor = new MoveExecutor(nodes, pending, cross, this.telemetry, {
      reservationTtlMs: options.reservationTtlMs ?? 35_000, prepareTtlMs: options.reservationTtlMs ?? 35_000,
      prepareTimeoutMs: options.prepareTimeoutMs ?? options.moveTransactionTimeoutMs ?? 15_000,
      redirectTtlMs: options.arrivalTimeoutMs ?? 30_000,
      effectivePlayersOf: room => options.effectivePlayersOf?.(room) ?? this.effectivePlayers(room),
      applyTargetFacts: (room, facts) => this.applyTargetFacts(room, facts)
    }, log)
    this.cancellations = new MoveCancellationQueue({ rooms, nodes, pending, telemetry: this.telemetry, logger: log,
      retryBaseMs: options.retryBaseMs, retryMaxMs: options.retryMaxMs, maxAttempts: options.maxAttempts,
      onReleased: (moveId, id) => this.transactions.releaseOnce(moveId, id) })
    this.scheduler = new PlanningScheduler({ logger: log,
      run: async (signal, now) => { await this.runCycle(signal, now) },
      maintain: now => { this.expireTransactions(now); this.cancellations.flush() } })
  }

  public trustedRosterCount (roomId: RoomId): number { return this.roomTelemetry.count(roomId) }
  public effectivePlayers (room: RoomSnapshot, now = Date.now()): number {
    return Math.max(room.players, this.trustedRosterCount(room.roomId)) + this.telemetry.reserved(room.roomId, now)
  }
  private capacity (room: RoomSnapshot): number { return this.options.effectivePlayersOf?.(room) ?? this.effectivePlayers(room) }
  private active(tx: MoveTransaction): boolean { return this.transactions.active(tx) }
  private matchesEvent(tx: MoveTransaction, event: INodeEvent, side: 'source' | 'target'): boolean {
    return this.transactions.matches(tx, event, side, this.nodes)
  }
  private markArrived (tx: MoveTransaction, id: string): void {
    if (tx.arrived.has(id)) return
    tx.arrived.add(id)
    tx.joinedPlayers = [...tx.arrived]
    this.recentArrivals.set(tx.targetRoomId, Date.now())
    this.migratedPlayers.set(id, Date.now())
    if (!tx.released.has(id)) {
      tx.released.add(id)
      this.telemetry.releaseReservationSlots(tx.targetRoomId as RoomId, tx.moveId, 1)
    }
  }
  private completeIfResolved (tx: MoveTransaction): void {
    if (this.transactions.resolve(tx)) {
      if (!this.cancellations.has(tx.moveId)) this.telemetry.releaseReservation(tx.targetRoomId as RoomId, tx.moveId)
      this.record({ category: 'move', event: 'move.completed', message: `move ${tx.moveId} resolved`, roomId: tx.targetRoomId, playerIds: [...tx.plannedIds], data: { moveId: tx.moveId, failedPlayerIds: [...tx.failed], arrived: [...tx.arrived], left: [...tx.left] } })
    }
  }

  public ingest (event: INodeEvent): boolean {
    if (this.scheduler.stopped) return false
    const roomId = event.data.roomId
    const owner = roomId ? this.rooms.getRoom(roomId) : undefined
    if (owner && owner.nodeId !== event.data.nodeId) return false
    if (event.data.connectedAt !== undefined && this.nodes.getTransport(event.data.nodeId)?.connectedAt !== event.data.connectedAt) return false
    if (event.event === 'room.telemetry' && roomId) return this.roomTelemetry.accept(event, roomId)
    if (event.event === 'room.destroyed' && roomId) { this.forgetRoom(roomId); return true }
    const player = event.data.player
    if ((event.event === 'player.join' || event.event === 'player.leave') && player && roomId) {
      const id = player.playerId as string
      const sessionId = player.sessionId ?? event.data.sessionId
      const lvSeq = player.lvSeq ?? event.data.lvSeq
      if (event.event === 'player.leave') {
        this.roomTelemetry.noteLeave(roomId, player.playerId, sessionId)
        this.telemetry.remove(player.playerId, roomId, sessionId)
        this.rooms.removePlayer(player.playerId, roomId, sessionId)
        for (const tx of this.transactions.values()) {
          if (!this.active(tx) || !this.matchesEvent(tx, event, 'source') || !tx.plannedIds.includes(id)) continue
          const expected = tx.expectedParticipants.find(item => item.playerId === id)
          if (sessionId && expected?.sessionId && expected.sessionId !== sessionId) continue
          this.transactions.leave(tx, id); this.completeIfResolved(tx)
        }
      } else {
        if (sessionId === undefined || lvSeq === undefined) {
          this.recordPlanSkipped('stale_observation', roomId, {
            playerId: id,
            reasonDetail: 'player.join missing sessionId or lvSeq'
          })
          this.requestFull(roomId)
          return false
        }
        const joined: PlayerTelemetry = { playerId: player.playerId, status: 'ACTIVE', sessionId, lvSeq,
          ...(player.level !== undefined ? { levelId: player.level } : {}) }
        this.telemetry.upsert(roomId, joined)
        this.rooms.trackPlayer(player.playerId, event.data.nodeId, roomId, player.level, sessionId, lvSeq)
        this.roomTelemetry.noteJoin(roomId, joined)
        for (const tx of this.transactions.values()) if (this.active(tx) && this.matchesEvent(tx, event, 'target') && tx.plannedIds.includes(id)) { this.markArrived(tx, id); this.completeIfResolved(tx) }
      }
      // Room stability is intentionally NOT reset here: the stability
      // signature only covers the settled, non-churning core, so a stream of
      // visitors joining/leaving cannot freeze planning for that core. A real
      // roster change of a settled member still shifts the signature once they
      // pass the join-settle window.
    }
    if (event.data.moveId) {
      const tx = this.transactions.get(event.data.moveId)
      if (tx && this.active(tx)) {
        if (event.event === 'move.result' && !event.data.accepted && this.matchesEvent(tx, event, 'source')) {
          const ids = event.data.failedPlayerIds?.length ? event.data.failedPlayerIds : tx.plannedIds
          const failed = this.transactions.reject(tx, ids)
          if (failed.length) this.queueCancel(tx.targetRoomId as RoomId, tx.moveId, failed)
          this.completeIfResolved(tx)
        }
        if (event.event === 'reservation.expired' && this.matchesEvent(tx, event, 'target')) {
          for (const id of event.data.admittedPlayerIds ?? []) if (tx.plannedIds.includes(id)) this.markArrived(tx, id)
          this.transactions.fail(tx); this.telemetry.releaseReservation(tx.targetRoomId as RoomId, tx.moveId); this.cancellations.forget(tx.moveId)
        }
        if (event.event === 'move.completed' && this.matchesEvent(tx, event, 'target')) {
          for (const id of event.data.admittedPlayerIds ?? event.data.playerIds ?? []) if (tx.plannedIds.includes(id)) this.markArrived(tx, id)
          this.completeIfResolved(tx)
        }
      }
    }
    this.requestPlan()
    return true
  }

  private requestFull (roomId: RoomId): void {
    const now = Date.now()
    if (now - (this.syncRequestedAt.get(roomId) ?? 0) < 2_000) return
    const room = this.rooms.getRoom(roomId)
    const transport = room ? this.nodes.getTransport(room.nodeId) : undefined
    if (!room || !transport?.writable) return
    this.syncRequestedAt.set(roomId, now)
    this.track(this.pending.send({ cmd: 'room.sync', data: { roomId } }, command => transport.send(command), this.scheduler.workSignal(), room.nodeId).catch(error => { if (!this.scheduler.stopped) this.log?.debug({ err: error, roomId }, 'full telemetry request failed') }))
  }
  private applyTargetFacts (room: RoomSnapshot, facts: Record<string, unknown>): void {
    applyCapacityFacts(this.rooms, room, facts)
  }

  public requestPlan (): void { this.scheduler.request() }
  public async planAndExecute (signal?: AbortSignal, now = Date.now()): Promise<MoveTransaction[]> {
    await this.scheduler.runNow(signal, now)
    return this.list()
  }
  private async runCycle (signal: AbortSignal, now: number): Promise<MoveTransaction[]> {
    this.expireTransactions(now); this.drainPendingCancels()
    const allRooms = this.rooms.listRooms().filter(room => this.isRoomPlannable(room))
    for (const room of allRooms) {
      const stale = this.telemetry.listKnownRoom(room.roomId)
        .filter(player => player.receivedAt > now || now - player.receivedAt > 10_000)
      if (stale.length) {
        this.recordPlanSkipped('stale_observation', room.roomId, {
          stalePlayers: stale.map(player => ({ playerId: player.playerId, ageMs: Math.max(0, now - player.receivedAt) }))
        })
        this.requestFull(room.roomId)
      }
    }
    // Mandatory level isolation sees every healthy room. Ordinary merge and
    // activity rebalance still require the configured stability confirmation.
    const stableRooms = allRooms.filter(room => this.isRoomStable(room.roomId, this.stablePlayers(room.roomId, now)))
    const byRoom = new Map(stableRooms.map(room => [room.roomId as string, this.telemetry.listRoom(room.roomId, now)]))
    const context = {
      effectivePlayersOf: (room: RoomSnapshot) => this.capacity(room),
      isCandidateRoom: (room: RoomSnapshot, role: 'source' | 'target', at: number) => this.isCandidateRoom(room, role, at),
      physicalPlayersOf: (roomId: string) => this.telemetry.listKnownRoom(roomId as RoomId),
      isChurning: (playerId: string, at: number) => this.telemetry.isChurning(playerId as PlayerId, at),
      isIsolationSourceRoom: (room: RoomSnapshot, at: number) => this.isIsolationSourceRoom(room, at),
      onBlocked: (roomId: string, reason: string, data?: Record<string, unknown>) =>
        this.recordPlannerBlocked(roomId, reason, data)
    }
    this.planner.calibrate(stableRooms, byRoom, now)
    const plans = [
      ...this.levelIsolationPlanner.plan(allRooms, this.telemetry, now, this.log, context),
      ...this.planner.plan(stableRooms, byRoom, now, context)
    ]
    const claimed = new Set<string>()
    const started = Date.now()
    let attempts = 0, budget = this.options.moveBudgetPerCycle ?? 8
    for (const plan of plans) {
      if (this.scheduler.stopped || signal.aborted || attempts >= (this.options.maxAttempts ?? 8) || Date.now() - started >= (this.options.maxCycleMs ?? 2_000)) break
      const source = this.rooms.getRoom(plan.sourceRoomId as RoomId), target = this.rooms.getRoom(plan.targetRoomId as RoomId)
      if (!source) { this.recordPlanSkipped('source_not_candidate', plan.sourceRoomId); continue }
      if (!target) { this.recordPlanSkipped('target_not_candidate', plan.sourceRoomId); continue }
      if (plan.playerIds.length > 7) { this.recordPlanSkipped('plan_too_large', plan.sourceRoomId); continue }
      if (plan.playerIds.length > budget) { this.recordPlanSkipped('budget_exhausted', plan.sourceRoomId); continue }
      if (claimed.has(source.roomId) || claimed.has(target.roomId)) { this.recordPlanSkipped('room_claimed', plan.sourceRoomId); continue }
      const isolation = plan.reason === 'level_isolation'
      if (!(isolation ? this.isIsolationSourceRoom(source) : this.isCandidateRoom(source, 'source'))) {
        this.recordPlanSkipped('source_not_candidate', plan.sourceRoomId)
        continue
      }
      if (!this.isCandidateRoom(target, 'target')) { this.recordPlanSkipped('target_not_candidate', plan.sourceRoomId); continue }
      attempts++; claimed.add(source.roomId); claimed.add(target.roomId)
      const { tx, reason } = await this.executePlan(plan, source, target, signal)
      if (reason) this.recordPlanSkipped(reason, plan.sourceRoomId)
      if (tx?.status === 'committed' || tx?.status === 'completed') budget -= plan.playerIds.length
    }
    return this.list()
  }

  /** Stability signature roster: settled, non-churning players only. Visitors
   *  inside their join/level-settle window, MOVING players and level flappers
   *  never enter the signature, so their churn cannot reset confirmation. */
  private stablePlayers (roomId: RoomId, now: number): PlayerTelemetry[] {
    const levelCutoff = now - this.planner.levelChangeCooldownMs
    const joinCutoff = now - this.planner.joinSettleMs
    return this.telemetry.listRoom(roomId, now).filter(player =>
      player.status !== 'MOVING' &&
      (player.levelChangedAt ?? 0) <= levelCutoff &&
      (player.roomJoinedAt ?? 0) <= joinCutoff &&
      !this.telemetry.isChurning(player.playerId, now)
    )
  }

  private newTransaction(plan: MovePlan | LevelIsolationPlan, source: RoomSnapshot, target: RoomSnapshot): MoveTransaction {
    const participants = plan.playerIds.map(playerId => {
      const item = this.telemetry.get(playerId as PlayerId)
      return { playerId: playerId as PlayerId, ...(item?.sessionId ? { sessionId: item.sessionId } : {}), ...(item?.levelId !== undefined ? { levelId: item.levelId } : {}), ...(item?.lvSeq !== undefined ? { lvSeq: item.lvSeq } : {}) }
    })
    return this.transactions.create(plan, source, target, participants, this.nodes, this.options.reservationTtlMs ?? 35_000)
  }
  /** A hard bond (>= 0.9 either way) from any planned player to a physical
   *  source-room player outside the plan makes the group incomplete. */
  private bondedPeerOutsidePlan (plannedIds: readonly string[], sourceRoomId: string, now: number): string | undefined {
    const planned = new Set<string>(plannedIds)
    for (const peer of this.telemetry.listKnownRoom(sourceRoomId as RoomId)) {
      const peerId = peer.playerId as string
      if (planned.has(peerId)) continue
      for (const id of plannedIds) {
        const item = this.telemetry.get(id as PlayerId)
        const affinity = Math.max(item?.affinity?.[peerId] ?? 0, peer.affinity?.[id] ?? 0)
        if (affinity >= 0.9) return peerId
      }
    }
    return undefined
  }
  private sourceUnchanged (tx: MoveTransaction): boolean {
    if (!this.active(tx)) return false
    const unchanged = tx.plannedIds.every(id => {
      const location = this.rooms.getPlayerLocation(id as PlayerId), item = this.telemetry.get(id as PlayerId)
      const expected = tx.expectedParticipants.find(player => player.playerId === id)!
      return location?.roomId === tx.sourceRoomId && location.nodeId === tx.sourceNodeId && location.leavingAt === undefined &&
        item?.roomId === tx.sourceRoomId && (expected.sessionId === undefined || item.sessionId === expected.sessionId) &&
        (expected.levelId === undefined || item.levelId === expected.levelId) && (expected.lvSeq === undefined || item.lvSeq === expected.lvSeq)
    })
    if (!unchanged) return false
    // Commit-time bond closure: a bond formed after planning (peer switched
    // into the source room / updated affinity) blocks the redirect; the
    // caller fails the transaction and queues cancellation, never redirects.
    return this.bondedPeerOutsidePlan(tx.plannedIds, tx.sourceRoomId, Date.now()) === undefined
  }
  private async executePlan (plan: MovePlan | LevelIsolationPlan, source: RoomSnapshot, target: RoomSnapshot, signal?: AbortSignal): Promise<{ tx: MoveTransaction | undefined; reason: PlanSkipReason | undefined }> {
    const now = Date.now()
    if (!plan.playerIds.length) return { tx: undefined, reason: undefined }
    if (plan.playerIds.length > 7) return { tx: undefined, reason: 'plan_too_large' }
    if (this.transactionsForPlayer(plan.playerIds).length) return { tx: undefined, reason: 'tx_active_for_player' }
    if (this.roomBusy(source.roomId, now)) return { tx: undefined, reason: 'busy_source' }
    if (this.roomBusy(target.roomId, now)) return { tx: undefined, reason: 'busy_target' }
    if (this.capacity(target) + plan.playerIds.length > target.capacity) return { tx: undefined, reason: 'target_capacity' }
    const bondedPeerId = this.bondedPeerOutsidePlan(plan.playerIds, source.roomId, now)
    if (bondedPeerId !== undefined) {
      this.record({ category: 'move', event: 'plan.rejected', message: `plan rejected: ${plan.playerIds.join(',')} hard-bonded to ${bondedPeerId} outside the plan`, roomId: source.roomId, playerIds: [...plan.playerIds], data: { bondedPeerId } })
      return { tx: undefined, reason: 'bond_incomplete' }
    }
    const tx = this.newTransaction(plan, source, target)
    const reason: RedirectReason = plan.reason === 'level_isolation' ? 'level-isolation' : plan.reason === 'level_consolidation' ? 'level-consolidation' : plan.reason === 'low_activity_merge' ? 'merge' : 'capacity-rebalance'
    const outcome = await this.executor.execute({ moveId: tx.moveId, playerIds: [...tx.plannedIds], source, target, reason,
      now: Date.now(), ...(signal ? { signal } : {}), expectedParticipants: tx.expectedParticipants,
      validateSource: () => this.sourceUnchanged(tx), isLive: () => this.active(tx),
      onPrepared: facts => this.transactions.prepared(tx, facts)
    })
    this.applyOutcome(tx, outcome)
    return { tx, reason: undefined }
  }
  private applyOutcome (tx: MoveTransaction, outcome: MoveExecuteOutcome): void {
    if (!this.active(tx)) return
    if (outcome.ok) {
      if (!this.transactions.committed(tx, outcome, this.options.arrivalTimeoutMs ?? 30_000)) return
      this.onMoveOutcome?.(tx.reason, 'committed'); this.completeIfResolved(tx)
      this.record({ category: 'move', event: 'move.committed', message: `${tx.reason}: ${tx.sourceRoomId} -> ${tx.targetRoomId}`, roomId: tx.targetRoomId, playerIds: [...tx.plannedIds], data: { moveId: tx.moveId, reason: tx.reason, committedAt: tx.committedAt, deliveryDeadline: tx.deliveryDeadline } })
    } else {
      this.transactions.fail(tx)
      if (this.telemetry.reservation(tx.targetRoomId as RoomId, tx.moveId)) this.queueCancel(tx.targetRoomId as RoomId, tx.moveId, [...tx.plannedIds])
      const room = this.rooms.getRoom(tx.targetRoomId as RoomId)
      if (room) {
        const previous = this.targetBackoff.get(room.roomId)
        const attempts = Math.min(16, (previous?.attempts ?? 0) + 1)
        this.targetBackoff.set(room.roomId, { attempts, until: Date.now() + Math.min(this.options.retryMaxMs ?? 30_000, (this.options.retryBaseMs ?? 2_000) * 2 ** (attempts - 1)), version: room.version, players: room.players })
        if (this.targetBackoff.size > 4096) this.targetBackoff.delete(this.targetBackoff.keys().next().value!)
      }
      this.onMoveOutcome?.(tx.reason, 'failed')
      this.record({ category: 'move', event: 'move.failed', message: `move ${tx.moveId} failed (${outcome.reason})`, roomId: tx.targetRoomId, playerIds: [...tx.plannedIds], data: { moveId: tx.moveId, reason: outcome.reason } })
    }
  }
  private async executeLevelIsolation (plan: LevelIsolationPlan, _now: number, signal?: AbortSignal): Promise<boolean> {
    const source = this.rooms.getRoom(plan.sourceRoomId as RoomId), target = this.rooms.getRoom(plan.targetRoomId as RoomId)
    if (!source || !target) return false
    const { tx } = await this.executePlan(plan, source, target, signal)
    return tx?.status === 'committed' || tx?.status === 'completed'
  }
  public manualMove (input: { sourceRoomId: string; targetRoomId: string; playerIds: string[]; reason?: string }, signal?: AbortSignal): Promise<MoveTransaction> {
    if (this.scheduler.stopped || signal?.aborted) return Promise.reject(new Error('move coordinator stopped or aborted'))
    return this.track(this.runManualMove(input, this.workSignal(signal)))
  }
  private async runManualMove (input: { sourceRoomId: string; targetRoomId: string; playerIds: string[]; reason?: string }, signal: AbortSignal): Promise<MoveTransaction> {
    if (!Array.isArray(input.playerIds) || !input.playerIds.length || input.playerIds.length > 7 || new Set(input.playerIds).size !== input.playerIds.length || input.sourceRoomId === input.targetRoomId) throw new Error('invalid debug move request')
    const source = this.rooms.getRoom(input.sourceRoomId as RoomId), target = this.rooms.getRoom(input.targetRoomId as RoomId)
    if (!source || !target) throw new Error('source or target room not found')
    if (!this.isRoomPlannable(source) || !this.isRoomPlannable(target)) throw new Error('move room is unhealthy or telemetry is incomplete')
    for (const id of input.playerIds) if (this.rooms.getPlayerLocation(id as PlayerId)?.roomId !== source.roomId) throw new Error('player not in source room')
    const { tx, reason } = await this.executePlan({ ...input, gain: 0, reason: input.reason === 'merge' ? 'low_activity_merge' : 'player_rebalance' }, source, target, signal)
    if (!tx) throw new Error(reason === 'bond_incomplete' ? 'move rejected: planned players are hard-bonded to someone outside the plan' : 'target capacity or source/target is busy')
    if (tx.status === 'failed' && signal.aborted) throw new Error('move aborted')
    return tx
  }

  private expireTransactions (now: number): void {
    for (const tx of this.transactions.values()) this.completeIfResolved(tx)
    for (const tx of this.transactions.expire(now, this.options.prepareTimeoutMs ?? this.options.moveTransactionTimeoutMs ?? 15_000)) {
      this.queueCancel(tx.targetRoomId as RoomId, tx.moveId, tx.plannedIds.filter(player => !tx.arrived.has(player)))
      this.log?.warn({ moveId: tx.moveId, arrived: [...tx.arrived], left: [...tx.left] }, 'move deadline expired')
    }
    for (const [id, at] of this.migratedPlayers) if (now - at > 5 * 60_000) this.migratedPlayers.delete(id)
    for (const [id, at] of this.recentArrivals) if (now - at > (this.options.arrivalCooldownMs ?? 15_000)) this.recentArrivals.delete(id)
    for (const [key, at] of this.skipEvents) if (now - at > 60_000) this.skipEvents.delete(key)
  }
  private recordPlannerBlocked (roomId: string, reason: string, data?: Record<string, unknown>): void {
    const mapped: PlanSkipReason = reason === 'isolation_no_target' ? 'isolation_no_target' : 'source_not_candidate'
    this.recordPlanSkipped(mapped, roomId, data)
  }
  /** Rate-limited plan.skipped event: once per sourceRoomId+reason per 15s. */
  private recordPlanSkipped (reason: PlanSkipReason, sourceRoomId: string, extra: Record<string, unknown> = {}): void {
    const now = Date.now()
    this.lastSkipByRoom.set(sourceRoomId, { reason, at: now })
    const key = `${sourceRoomId}\u0000${reason}`
    const last = this.skipEvents.get(key)
    if (last !== undefined && now - last < SKIP_RATE_LIMIT_MS) return
    if (this.skipEvents.size > 4096) this.skipEvents.clear()
    this.skipEvents.set(key, now)
    this.record({ category: 'move', event: 'plan.skipped', message: `plan skipped: ${reason}`, roomId: sourceRoomId, data: { reason, ...extra } })
  }
  public stabilitySnapshot (now = Date.now()): Array<{
    roomId: string
    cycles: number
    seq: number | undefined
    gap: boolean
    busyTx: boolean
    busyCancellations: boolean
    backoffUntil: number | undefined
    recentArrivalAgoMs: number | undefined
    remigrateBlockedPlayerIds: string[]
    lastSkip: { reason: PlanSkipReason; at: number } | undefined
  }> {
    return this.rooms.listRooms().map(room => {
      const roomId = room.roomId as string
      const stability = this.roomStability.get(roomId)
      const remigrateBlockedPlayerIds = this.telemetry.listRoom(room.roomId, now)
        .filter(player => now - (this.migratedPlayers.get(player.playerId) ?? player.lastMigratedAt ?? -Infinity) < (this.options.remigrateCooldownMs ?? 20_000))
        .map(player => player.playerId as string)
      return {
        roomId,
        cycles: stability?.cycles ?? 0,
        seq: stability?.seq ?? this.roomTelemetry.revision(room.roomId),
        gap: this.roomTelemetry.hasGap(room.roomId),
        busyTx: this.transactions.busy(roomId),
        busyCancellations: this.cancellations.busy(room.roomId, now),
        backoffUntil: this.targetBackoff.get(roomId)?.until,
        recentArrivalAgoMs: this.recentArrivals.has(roomId) ? Math.max(0, now - this.recentArrivals.get(roomId)!) : undefined,
        remigrateBlockedPlayerIds,
        lastSkip: this.lastSkipByRoom.get(roomId)
      }
    })
  }
  private queueCancel (roomId: RoomId, moveId: string, playerIds: string[]): void {
    const tx = this.transactions.get(moveId)
    this.cancellations.enqueue({ roomId, moveId, playerIds, expiresAt: tx?.expiresAt ?? Date.now() + (this.options.reservationTtlMs ?? 35_000),
      ...(tx?.reservationToken ? { reservationToken: tx.reservationToken } : {}) })
  }
  private drainPendingCancels (): void { this.cancellations.flush() }
  private isRoomPlannable (room: RoomSnapshot): boolean {
    return !room.draining && !this.rooms.isNodeSuspected(room.nodeId) && !this.roomTelemetry.hasGap(room.roomId) && this.nodes.isAvailableForCommand(room.nodeId)
  }
  /** Mandatory mixed-level cleanup bypasses ordinary arrival/remigration
   *  cooldowns. Only transport health and in-flight room ownership can block it;
   *  each selected group still passes its own six-second settle and bond guards. */
  private isIsolationSourceRoom (room: RoomSnapshot, now = Date.now()): boolean {
    return this.isRoomPlannable(room) && !this.roomBusy(room.roomId, now)
  }
  public isCandidateRoom (room: RoomSnapshot, role: 'source' | 'target' = 'target', now = Date.now()): boolean {
    if (!this.isRoomPlannable(room) || this.roomBusy(room.roomId, now)) return false
    const rejection = this.targetBackoff.get(room.roomId)
    if (rejection) {
      if (rejection.until <= now || room.players < rejection.players) this.targetBackoff.delete(room.roomId)
      else if (role === 'target') return false
    }
    if (role === 'source') {
      if (now - (this.recentArrivals.get(room.roomId) ?? -Infinity) < (this.options.arrivalCooldownMs ?? 15_000)) return false
      if (this.telemetry.listRoom(room.roomId, now).some(player => now - (this.migratedPlayers.get(player.playerId) ?? player.lastMigratedAt ?? -Infinity) < (this.options.remigrateCooldownMs ?? 20_000))) return false
    }
    return true
  }
  public filterCandidateRooms (rooms: RoomSnapshot[], role: 'source' | 'target' = 'target', now = Date.now()): RoomSnapshot[] { return rooms.filter(room => this.isCandidateRoom(room, role, now)) }
  public safetySnapshot (now = Date.now()) {
    return { busyRoomIds: this.rooms.listRooms().filter(room => this.roomBusy(room.roomId, now)).map(room => room.roomId), gapRoomIds: this.roomTelemetry.gapIds(), backoffRoomIds: [...this.targetBackoff].filter(([, value]) => value.until > now).map(([id]) => id), activePlayerIds: [...this.transactions.values()].filter(tx => this.active(tx)).flatMap(tx => tx.playerIds) }
  }
  private roomBusy (roomId: string, _now: number): boolean {
    return this.transactions.busy(roomId) || this.cancellations.busy(roomId)
  }
  private isRoomStable (roomId: string, players: PlayerTelemetry[]): boolean {
    const seq = this.roomTelemetry.revision(roomId as RoomId)
    if (seq === undefined) return (this.options.confirmationCycles ?? 3) <= 1
    const signature = players.map(player => `${player.playerId}:${player.sessionId ?? ''}:${player.levelId ?? ''}:${player.lvSeq ?? ''}`).sort().join(',')
    const old = this.roomStability.get(roomId)
    if (old?.seq === seq) return old.cycles >= (this.options.confirmationCycles ?? 3)
    const cycles = old?.signature === signature ? old.cycles + 1 : 1
    this.roomStability.set(roomId, { signature, seq, cycles })
    return cycles >= (this.options.confirmationCycles ?? 3)
  }
  private transactionsForPlayer (ids: string[]): MoveTransaction[] { return [...this.transactions.values()].filter(tx => this.active(tx) && tx.plannedIds.some(id => ids.includes(id))) }
  public forgetRoom (roomId: string): void {
    this.roomTelemetry.forget(roomId as RoomId); this.roomStability.delete(roomId); this.syncRequestedAt.delete(roomId)
    this.lastSkipByRoom.delete(roomId)
    this.telemetry.removeRoom(roomId as RoomId)
    for (const tx of this.transactions.values()) if (this.active(tx) && (tx.sourceRoomId === roomId || tx.targetRoomId === roomId)) {
      this.transactions.fail(tx); if (tx.targetRoomId !== roomId) this.queueCancel(tx.targetRoomId as RoomId, tx.moveId, [...tx.plannedIds])
    }
  }
  public list (): MoveTransaction[] { return [...this.transactions.values()] }
  public activitySnapshot (now = Date.now()): Array<ReturnType<MovePlanner['activity']>> { return this.rooms.listRooms().map(room => this.planner.activity(room, this.telemetry.listRoom(room.roomId, now), now)) }
  public telemetryGapCount (): number { return this.roomTelemetry.gapCount() }
  public hasTelemetryGap (roomId: RoomId): boolean { return this.roomTelemetry.hasGap(roomId) }
  private record (event: DebugEventInput): void { this.events?.record(event) }
  private track<T>(work: Promise<T>): Promise<T> { return this.scheduler.track(work) }
  private workSignal(signal?: AbortSignal): AbortSignal { return this.scheduler.workSignal(signal) }
  public stop (): Promise<void> {
    if (this.stopPromise) return this.stopPromise
    this.stopPromise = Promise.all([this.cancellations.stop(), this.scheduler.stop()]).then(() => {
      for (const tx of this.transactions.values()) { this.transactions.fail(tx); this.telemetry.releaseReservation(tx.targetRoomId as RoomId, tx.moveId) }
    })
    return this.stopPromise
  }
}
