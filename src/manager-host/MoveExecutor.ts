import type { PlayerId } from '../types/branded.js'
import { asProtocolVersion, asUnixMs } from '../types/branded.js'
import type { RoomSnapshot } from '../types/contracts.js'
import { ErrorCode } from '../types/errors.js'
import type { Logger } from '../infra/logger/logger.js'
import type { CrossCoordinator, RedirectReason, SourceParticipantSnapshot } from './CrossCoordinator.js'
import type { NodeRegistry } from './NodeRegistry.js'
import type { PendingRequestMap } from './PendingRequestMap.js'
import { TelemetryStore } from './TelemetryStore.js'
export interface ExecutableMove { playerIds: string[]; sourceRoomId: string; targetRoomId: string }
export interface MoveExecutorOptions {
  reservationTtlMs: number
  prepareTtlMs: number
  redirectTtlMs: number
  prepareTimeoutMs?: number
  effectivePlayersOf?: ((room: RoomSnapshot) => number) | undefined
  applyTargetFacts?: ((room: RoomSnapshot, facts: Record<string, unknown>) => void) | undefined
}
export type MoveExecuteOutcome =
  | { ok: true; moveId: string; committedAt?: number; deliveryDeadline?: number; expiresAt?: number; reservationToken?: string }
  | { ok: false; moveId: string; reason: 'no_capacity' | 'stale_plan' | 'no_players' | 'failed'; error?: unknown; permanent?: boolean }
const ackDetail = (ack: { code: number; message?: string | undefined; data?: unknown }): string => {
  const payload = ack.data as { message?: unknown } | undefined
  return payload && typeof payload.message === 'string' ? payload.message : ack.message ?? `node ack code ${ack.code}`
}
export class MoveExecutor {
  public constructor (
    private readonly nodes: NodeRegistry, private readonly pending: PendingRequestMap,
    private readonly cross: CrossCoordinator, private readonly telemetry: TelemetryStore,
    private readonly options: MoveExecutorOptions, private readonly log?: Logger
  ) {}
  public async execute (input: {
    moveId: string; playerIds: string[]; source: RoomSnapshot; target: RoomSnapshot; reason: RedirectReason; now: number;
    signal?: AbortSignal | undefined; expectedParticipants?: SourceParticipantSnapshot[] | undefined;
    validateSource?: (() => boolean) | undefined; isLive?: (() => boolean) | undefined;
    onPrepared?: ((facts: { expiresAt: number; reservationToken?: string }) => void) | undefined
  }): Promise<MoveExecuteOutcome> {
    const { moveId, playerIds, source, target, reason, signal } = input
    if (!playerIds.length) return { ok: false, moveId, reason: 'no_players' }
    const now = Date.now()
    const localHolds = this.telemetry.reserved(target.roomId, now)
    const effective = this.options.effectivePlayersOf?.(target) ?? Math.max(target.players, this.telemetry.listRoom(target.roomId, now).length) + localHolds
    if (!this.telemetry.tryReserve(target.roomId, moveId, playerIds.length, target.capacity, Math.max(0, effective - localHolds), this.options.reservationTtlMs, now)) {
      return { ok: false, moveId, reason: 'no_capacity' }
    }
    let prepareSent = false
    let remoteExpiresAt = now + this.options.reservationTtlMs
    try {
      const transport = this.nodes.getTransport(target.nodeId)
      const sourceTransport = this.nodes.getTransport(source.nodeId)
      if (!transport?.writable || !sourceTransport?.writable || !this.nodes.isAvailableForCommand(target.nodeId)) throw new Error('move node unavailable')
      const sourceConnectedAt = sourceTransport.connectedAt
      const targetConnectedAt = transport.connectedAt
      const deadline = AbortSignal.timeout(this.options.prepareTimeoutMs ?? 15_000)
      const prepareSignal = signal ? AbortSignal.any([signal, deadline]) : deadline
      prepareSent = true
      const ack = await this.pending.send({ v: asProtocolVersion(1), cmd: 'move.prepare', data: {
        moveId, roomId: target.roomId, playerIds, expectedVersion: target.version, ttlMs: this.options.prepareTtlMs
      } }, command => transport.send(command), prepareSignal, target.nodeId)
      const facts = ack.data && typeof ack.data === 'object' ? ack.data as Record<string, unknown> : {}
      this.options.applyTargetFacts?.(target, facts)
      if (typeof facts.expiresAt === 'number' && Number.isSafeInteger(facts.expiresAt)) remoteExpiresAt = facts.expiresAt
      this.telemetry.setReservationExpiry(target.roomId, moveId, remoteExpiresAt)
      const reservationToken = typeof facts.reservationToken === 'string' ? facts.reservationToken : undefined
      input.onPrepared?.({ expiresAt: remoteExpiresAt, ...(reservationToken ? { reservationToken } : {}) })
      if (ack.code !== ErrorCode.OK) {
        // A negative prepare ACK did not create a remote hold.
        this.telemetry.releaseReservation(target.roomId, moveId)
        if (ack.code === ErrorCode.STALE_PLAN) return { ok: false, moveId, reason: 'stale_plan' }
        return { ok: false, moveId, reason: 'failed', permanent: ack.code === ErrorCode.BAD_REQUEST || ack.code === ErrorCode.NOT_FOUND, error: new Error(ackDetail(ack)) }
      }
      if (signal?.aborted || input.isLive?.() === false || input.validateSource?.() === false ||
          this.nodes.getTransport(source.nodeId)?.connectedAt !== sourceConnectedAt ||
          this.nodes.getTransport(target.nodeId)?.connectedAt !== targetConnectedAt) throw new Error('source participants or node epoch changed during prepare')
      const first = playerIds[0]!
      const deliveryDeadline = Math.min(remoteExpiresAt, Date.now() + this.options.redirectTtlMs)
      if (deliveryDeadline <= Date.now()) throw new Error('target reservation expired before commit')
      const commitAck = await this.cross.redirectPlayers({
        playerIds: playerIds.map(id => id as PlayerId), moveId, playerId: first as PlayerId,
        sourceNodeId: source.nodeId, sourceConnectedAt, fromRoomId: source.roomId,
        ...(input.expectedParticipants ? { expectedParticipants: input.expectedParticipants } : {}),
        ...(reservationToken ? { reservationToken } : {}),
        to: { roomId: target.roomId, nodeId: target.nodeId, udpHost: target.udpHost, udpPort: target.udpPort, expiresAt: asUnixMs(deliveryDeadline) }, reason
      }, signal)
      if (commitAck.code !== ErrorCode.OK) return { ok: false, moveId, reason: 'failed', permanent: commitAck.code === ErrorCode.BAD_REQUEST || commitAck.code === ErrorCode.NOT_FOUND, error: new Error(ackDetail(commitAck)) }
      const committedAt = Date.now()
      this.telemetry.markMigrated(playerIds.map(id => id as PlayerId), committedAt)
      return { ok: true, moveId, committedAt, deliveryDeadline, expiresAt: remoteExpiresAt, ...(reservationToken ? { reservationToken } : {}) }
    } catch (error) {
      // Ambiguous sends retain capacity until cancel OK or the remote expiry.
      if (!prepareSent) this.telemetry.releaseReservation(target.roomId, moveId)
      this.log?.warn({ err: error, moveId, sourceRoomId: source.roomId, targetRoomId: target.roomId }, 'move failed')
      return { ok: false, moveId, reason: 'failed', error }
    }
  }
}
