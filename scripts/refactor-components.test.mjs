import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { RoomCache } from '../dist/manager-host/RoomCache.js'
import { NodeRegistry } from '../dist/manager-host/NodeRegistry.js'
import { TelemetryStore } from '../dist/manager-host/TelemetryStore.js'
import { RoomTelemetryService } from '../dist/manager-host/RoomTelemetryService.js'
import { MoveTransactions } from '../dist/manager-host/MoveTransactions.js'
import { MoveCancellationQueue } from '../dist/manager-host/MoveCancellationQueue.js'
import { PlanningScheduler } from '../dist/manager-host/PlanningScheduler.js'
import { readCapacityFacts, applyCapacityFacts } from '../dist/manager-host/CapacityFacts.js'
import { loadConfig } from '../dist/config/index.js'
import { createManagerRuntime } from '../dist/manager-host/ManagerRuntime.js'

const room = (id = 'r', nodeId = 'n') => ({ roomId: id, nodeId, udpHost: '127.0.0.1', udpPort: 41000, capacity: 8, players: 0, version: 0, draining: false })
const event = (seq, players, extra = {}) => ({ event: 'room.telemetry', data: { nodeId: 'n', roomId: 'r', kind: 'full', seq, players, ...extra } })
function fixture () {
  const rooms = new RoomCache({ maxRooms: 100, staleRoomMs: 600000 })
  rooms.upsertRoom(room())
  const nodes = new NodeRegistry({ circuitOpenMs: 10, failureThreshold: 5, backpressureLimitBytes: 1000 })
  nodes.register({ nodeId: 'n', connectedAt: 10, protocolVersion: 1, writable: true, remoteAddress: '', async send () {}, close () {} })
  const telemetry = new TelemetryStore()
  let resyncs = 0
  const service = new RoomTelemetryService({ rooms, nodes, telemetry, requestFull: () => resyncs++ })
  return { rooms, nodes, telemetry, service, resyncs: () => resyncs }
}
const flush = () => new Promise(resolve => setImmediate(resolve))

test('full snapshots validate player generation before any indexed view changes; u8 wrap survives', () => {
  const f = fixture()
  assert.equal(f.service.accept(event(1, [{ playerId: 'p', sessionId: 's', levelId: 1, lvSeq: 254 }]), 'r'), true)
  assert.equal(f.service.accept(event(2, [{ playerId: 'p', sessionId: 's', levelId: 2, lvSeq: 255 }]), 'r'), true)
  assert.equal(f.service.accept(event(3, [{ playerId: 'p', sessionId: 's', levelId: 3, lvSeq: 0 }]), 'r'), true)
  assert.equal(f.service.accept(event(4, [{ playerId: 'p', sessionId: 's', levelId: 99, lvSeq: 255 }]), 'r'), false)
  assert.equal(f.rooms.getRoom('r').version, 3)
  assert.equal(f.rooms.getPlayerLocation('p').level, 3)
  assert.equal(f.telemetry.get('p').levelId, 3)
  assert.equal(f.service.revision('r'), 3)
  assert.equal(f.resyncs(), 1)
})

test('rejected telemetry cannot sneak room mutations through data.rooms; cumulative roster is bounded', () => {
  const f = fixture()
  f.service.accept(event(1, Array.from({ length: 8 }, (_, i) => ({ playerId: String(i) }))), 'r')
  const bad = event(2, [{ playerId: 'ninth' }], { kind: 'delta', baseSeq: 1, rooms: [{ ...room(), players: 0, version: 99 }] })
  f.rooms.applyNodeEvent(bad)
  assert.equal(f.rooms.getRoom('r').version, 1)
  assert.equal(f.service.accept(bad, 'r'), false)
  assert.equal(f.rooms.getPlayerLocation('ninth'), undefined)
  assert.equal(f.service.count('r'), 8)
})

test('session replacement and explicit unknown level clear the old level in every view', () => {
  const f = fixture()
  f.service.accept(event(1, [{ playerId: 'p', sessionId: 'old', levelId: 42, lvSeq: 5 }]), 'r')
  f.service.accept(event(2, [{ playerId: 'p', sessionId: 'new', levelId: 0, lvSeq: 0 }]), 'r')
  assert.equal(f.rooms.getPlayerLocation('p').level, undefined)
  assert.equal(f.telemetry.get('p').levelId, undefined)
  f.service.accept(event(3, []), 'r')
  assert.equal(f.telemetry.get('p'), undefined)
  assert.equal(f.rooms.getPlayerLocation('p'), undefined)
})

test('capacity facts validate whole records and never add remote reservations twice', () => {
  const f = fixture()
  assert.equal(readCapacityFacts({ physicalPlayers: 7, activePlayers: 8, capacity: 8, version: 1 }), undefined)
  assert.equal(readCapacityFacts({ physicalPlayers: 7, reservedSlots: 2, capacity: 8, version: 1 }), undefined)
  assert.equal(applyCapacityFacts(f.rooms, room(), { physicalPlayers: 6, activePlayers: 5, reservedSlots: 2, capacity: 8, version: 1 }), true)
  assert.equal(f.rooms.getRoom('r').players, 6)
})

test('transaction owner preserves immutable participant plan, terminal states and release idempotence', () => {
  let now = 100
  const f = fixture(), transactions = new MoveTransactions(() => now)
  const tx = transactions.create({ playerIds: ['a', 'b'], sourceRoomId: 'source', targetRoomId: 'r', reason: 'player_rebalance', gain: 1 }, room('source'), room(), [], f.nodes, 35000)
  assert.ok(Object.isFrozen(tx.plannedIds))
  transactions.arrive(tx, 'a'); transactions.leave(tx, 'a')
  assert.deepEqual(transactions.reject(tx, ['b', 'stranger', 'b']), ['b'])
  assert.equal(transactions.committed(tx, {}, 30000), true)
  assert.equal(transactions.resolve(tx), true)
  assert.equal(tx.status, 'completed')
  assert.equal(transactions.fail(tx), false)
  assert.equal(transactions.committed(tx, {}, 30000), false)
  assert.equal(transactions.releaseOnce(tx.moveId, 'a'), true)
  assert.equal(transactions.releaseOnce(tx.moveId, 'a'), false)
  now += 400000; transactions.expire(now, 15000)
  assert.equal(transactions.list().length, 0)
})

test('cancellation owner merges, retries only after backoff, and expires after attempt cap', async () => {
  const f = fixture()
  let now = 100, finish, calls = 0
  f.telemetry.tryReserve('r', 'm', 2, 8, 0, 5000, now)
  const queue = new MoveCancellationQueue({ ...f, clock: () => now, retryBaseMs: 10, retryMaxMs: 20, maxAttempts: 2,
    pending: { send: () => { calls++; return new Promise(resolve => { finish = resolve }) } }, onReleased: () => true })
  queue.enqueue({ roomId: 'r', moveId: 'm', playerIds: ['a'], expiresAt: 1000 })
  queue.enqueue({ roomId: 'r', moveId: 'm', playerIds: ['b'], expiresAt: 1000 })
  assert.deepEqual(queue.snapshot()[0].playerIds.sort(), ['a', 'b'])
  finish({ code: 500 }); await flush()
  queue.flush(); assert.equal(calls, 1)
  now = 111; queue.flush(); assert.equal(calls, 2)
  finish({ code: 500 }); await flush()
  now = 200; queue.flush(); assert.equal(calls, 2)
  now = 1000; queue.flush(); assert.equal(queue.has('m'), false)
  assert.equal(f.telemetry.reserved('r', now), 0)
  await queue.stop()
})

test('scheduler coalesces an in-flight request and stop aborts/drains tracked work', async () => {
  let runs = 0, unblock
  const scheduler = new PlanningScheduler({ intervalMs: 100000, maintenanceMs: 100000,
    maintain () {}, run: signal => { runs++; return new Promise(resolve => { unblock = resolve; signal.addEventListener('abort', resolve, { once: true }) }) } })
  const first = scheduler.runNow()
  scheduler.request(); scheduler.request()
  await scheduler.runNow()
  assert.equal(runs, 1)
  const closing = scheduler.stop()
  assert.equal(scheduler.stop(), closing)
  await closing; await first
  unblock?.(); await scheduler.runNow()
  assert.equal(runs, 1)
})

function configFrom (yaml) {
  const dir = mkdtempSync(path.join(tmpdir(), 'qwd-config-refactor-'))
  try { const file = path.join(dir, 'config.yml'); writeFileSync(file, yaml); return loadConfig(file) }
  finally { rmSync(dir, { recursive: true }) }
}

test('config phase limits project into the real runtime with shared allocation and move occupancy', async () => {
  const config = configFrom('{}')
  assert.equal(config.levelIsolationMaxPlayers, 7)
  assert.equal(config.moveConfirmationCycles, 3)
  assert.equal(config.movePrepareTimeoutMs, 15000)
  assert.equal(config.moveReservationTtlMs, 35000)
  assert.equal(config.levelChangeCooldownMs, 8000)
  for (const yaml of ['move: {level_isolation_max_players: 8}', 'rooms: {max_players_per_room: 9}',
    'rooms: {max_players_per_room: 4, preferred_fill_target: 6}', 'move: {retry_base_ms: 20, retry_max_ms: 10}',
    'move: {arrival_timeout_ms: 40000, reservation_ttl_ms: 35000}']) assert.throws(() => configFrom(yaml))
  const runtime = createManagerRuntime(config)
  try {
    const r = room(); runtime.rooms.upsertRoom(r)
    runtime.rooms.upsertRoom({ ...r, players: 5 })
    assert.equal(runtime.reservations.tryAcquire(runtime.rooms.getRoom('r'), 'allocation'), true)
    assert.equal(runtime.moves.telemetry.tryReserve('r', 'move', 2, 8, 5, 35000), true)
    assert.equal(runtime.moves.capacity(runtime.rooms.getRoom('r')), 8)
  } finally { runtime.router.stop(); runtime.pending.close(); runtime.reservations.stop(); await runtime.moves.stop() }
})
