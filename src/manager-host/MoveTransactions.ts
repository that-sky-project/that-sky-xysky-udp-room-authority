import { randomUUID } from 'node:crypto'
import type { NodeId, RoomId } from '../types/branded.js'
import type { INodeEvent, RoomSnapshot } from '../types/contracts.js'
import type { SourceParticipantSnapshot } from './CrossCoordinator.js'
import type { MovePlan } from './MovePlanner.js'
import type { LevelIsolationPlan } from './LevelIsolationPlanner.js'
import type { NodeRegistry } from './NodeRegistry.js'

export interface MoveTransaction {
  moveId: string
  playerIds: string[]
  plannedIds: readonly string[]
  sourceRoomId: string
  targetRoomId: string
  reason: MovePlan['reason'] | LevelIsolationPlan['reason']
  gain?: number
  levelId?: number
  status: 'prepared' | 'committed' | 'completed' | 'failed'
  createdAt: number
  committedAt?: number
  deliveryDeadline?: number
  expiresAt: number
  reservationToken?: string
  failed: Set<string>
  arrived: Set<string>
  left: Set<string>
  released: Set<string>
  joinedPlayers: string[]
  leftPlayers: string[]
  sourceNodeId: NodeId
  targetNodeId: NodeId
  sourceConnectedAt?: number
  targetConnectedAt?: number
  expectedParticipants: SourceParticipantSnapshot[]
}

/** All transaction transitions and participant-set mutations have one owner. */
export class MoveTransactions {
  private readonly entries = new Map<string, MoveTransaction>()
  public constructor(private readonly clock: () => number = Date.now) {}
  public list(): MoveTransaction[] { return [...this.entries.values()] }
  public values(): IterableIterator<MoveTransaction> { return this.entries.values() }
  public get(id: string): MoveTransaction | undefined { return this.entries.get(id) }
  public active(tx: MoveTransaction): boolean { return tx.status === 'prepared' || tx.status === 'committed' }
  public forPlayers(ids: readonly string[]): MoveTransaction[] { return this.list().filter(tx => this.active(tx) && tx.plannedIds.some(id => ids.includes(id))) }
  public busy(roomId: string): boolean { return this.list().some(tx => this.active(tx) && (tx.sourceRoomId === roomId || tx.targetRoomId === roomId)) }

  public create(plan: MovePlan | LevelIsolationPlan, source: RoomSnapshot, target: RoomSnapshot,
    expectedParticipants: SourceParticipantSnapshot[], nodes: NodeRegistry, ttlMs: number): MoveTransaction {
    const now = this.clock()
    const tx: MoveTransaction = { ...plan, moveId: `move-${now.toString(36)}-${randomUUID()}`,
      playerIds: [...plan.playerIds], plannedIds: Object.freeze([...plan.playerIds]), status: 'prepared', createdAt: now,
      expiresAt: now + ttlMs, failed: new Set(), arrived: new Set(), left: new Set(), released: new Set(),
      joinedPlayers: [], leftPlayers: [], sourceNodeId: source.nodeId, targetNodeId: target.nodeId,
      expectedParticipants: expectedParticipants.map(player => ({ ...player })),
      ...(nodes.getTransport(source.nodeId)?.connectedAt !== undefined ? { sourceConnectedAt: nodes.getTransport(source.nodeId)!.connectedAt } : {}),
      ...(nodes.getTransport(target.nodeId)?.connectedAt !== undefined ? { targetConnectedAt: nodes.getTransport(target.nodeId)!.connectedAt } : {}) }
    this.entries.set(tx.moveId, tx)
    return tx
  }
  public matches(tx: MoveTransaction, event: INodeEvent, side: 'source' | 'target', nodes: NodeRegistry): boolean {
    const source = side === 'source'
    if (event.data.roomId !== (source ? tx.sourceRoomId : tx.targetRoomId) || event.data.nodeId !== (source ? tx.sourceNodeId : tx.targetNodeId)) return false
    const epoch = source ? tx.sourceConnectedAt : tx.targetConnectedAt
    if (epoch !== undefined && nodes.getTransport(event.data.nodeId)?.connectedAt !== epoch) return false
    if (event.data.connectedAt !== undefined && event.data.connectedAt !== epoch) return false
    return !event.data.moveId || tx.reservationToken === undefined || event.data.reservationToken === tx.reservationToken
  }
  public prepared(tx: MoveTransaction, facts: { expiresAt: number; reservationToken?: string }): void {
    if (!this.active(tx)) return
    tx.expiresAt = facts.expiresAt
    if (facts.reservationToken) tx.reservationToken = facts.reservationToken
  }
  public committed(tx: MoveTransaction, facts: { committedAt?: number; deliveryDeadline?: number; expiresAt?: number; reservationToken?: string }, arrivalMs: number): boolean {
    if (tx.status !== 'prepared') return false
    tx.status = 'committed'
    tx.committedAt = facts.committedAt ?? this.clock()
    tx.deliveryDeadline = facts.deliveryDeadline ?? tx.committedAt + arrivalMs
    tx.expiresAt = facts.expiresAt ?? tx.expiresAt
    if (facts.reservationToken) tx.reservationToken = facts.reservationToken
    return true
  }
  public fail(tx: MoveTransaction): boolean {
    if (!this.active(tx)) return false
    tx.status = 'failed'
    return true
  }
  public reject(tx: MoveTransaction, ids: readonly string[]): string[] {
    if (!this.active(tx)) return []
    const added = [...new Set(ids)].filter(id => tx.plannedIds.includes(id) && !tx.failed.has(id))
    for (const id of added) tx.failed.add(id)
    if (tx.failed.size === tx.plannedIds.length) tx.status = 'failed'
    return added
  }
  public arrive(tx: MoveTransaction, id: string): boolean {
    if (!this.active(tx) || !tx.plannedIds.includes(id) || tx.arrived.has(id)) return false
    tx.arrived.add(id)
    tx.joinedPlayers = [...tx.arrived]
    return true
  }
  public leave(tx: MoveTransaction, id: string): void {
    if (!this.active(tx) || !tx.plannedIds.includes(id)) return
    tx.left.add(id)
    tx.leftPlayers = [...tx.left]
  }
  public releaseOnce(moveId: string, id: string): boolean {
    const tx = this.entries.get(moveId)
    if (!tx) return true
    if (tx.released.has(id)) return false
    tx.released.add(id)
    return true
  }
  public resolve(tx: MoveTransaction): boolean {
    if (tx.status !== 'committed') return false
    const remaining = tx.plannedIds.filter(id => !tx.failed.has(id))
    if (!remaining.every(id => tx.arrived.has(id) && tx.left.has(id))) return false
    tx.status = remaining.length ? 'completed' : 'failed'
    return true
  }
  public expire(now: number, preparationMs: number): MoveTransaction[] {
    const expired: MoveTransaction[] = []
    for (const [id, tx] of this.entries) {
      const deadline = tx.status === 'prepared' ? tx.createdAt + preparationMs : tx.deliveryDeadline ?? Infinity
      if (this.active(tx) && now >= deadline) { this.fail(tx); expired.push(tx) }
      if (!this.active(tx) && now - tx.createdAt > 5 * 60_000) this.entries.delete(id)
    }
    return expired
  }
}
