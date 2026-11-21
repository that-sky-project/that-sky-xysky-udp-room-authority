import assert from 'node:assert/strict'
import test from 'node:test'
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { request } from 'node:http'
import { LevelIsolationPlanner } from '../dist/manager-host/LevelIsolationPlanner.js'
import { MovePlanner } from '../dist/manager-host/MovePlanner.js'
import { TelemetryStore } from '../dist/manager-host/TelemetryStore.js'
import { RoomTelemetryService } from '../dist/manager-host/RoomTelemetryService.js'
import { RoomCache } from '../dist/manager-host/RoomCache.js'
import { NodeRegistry } from '../dist/manager-host/NodeRegistry.js'
import { PendingRequestMap } from '../dist/manager-host/PendingRequestMap.js'
import { CrossCoordinator } from '../dist/manager-host/CrossCoordinator.js'
import { MoveCoordinator } from '../dist/manager-host/MoveCoordinator.js'
import { DebugEventLog } from '../dist/manager-host/DebugEventLog.js'
import { ManagerGateway } from '../dist/manager-host/ManagerGateway.js'
import { createManagerRuntime } from '../dist/manager-host/ManagerRuntime.js'
import * as contracts from '../dist/types/contracts.js'
import { loadConfig } from '../dist/config/index.js'

const NOW = 1_000_000
const room = (id, players, capacity = 8) => ({ roomId: id, nodeId: id, udpHost: '127.0.0.1', udpPort: 3000, players, capacity, version: 1, draining: false })
const player = (id, roomId, levelId, extra = {}) => ({ playerId: id, roomId, levelId, receivedAt: NOW, stateAt: NOW, position: [0, 0, 0], velocity: 0, roomJoinedAt: 0, levelChangedAt: 0, ...extra })
const store = players => ({ list: () => players, listRoom: id => players.filter(p => p.roomId === id) })
const options = { levelChangeCooldownMs: 6000, joinSettleMs: 6000 }
const idsOf = plans => plans.flatMap(plan => plan.playerIds).sort()

// ---------------------------------------------------------------------------
// LevelIsolationPlanner: bond closure keeps affinity groups whole
// ---------------------------------------------------------------------------

test('bonded pair on the minority level is planned together when the target has >= 2 free', () => {
  const rooms = [room('s', 5), room('t', 0)]
  const players = [
    player('a', 's', 1, { affinity: { b: 1 } }), player('b', 's', 1),
    player('c', 's', 2), player('d', 's', 2), player('e', 's', 2)
  ]
  const plans = new LevelIsolationPlanner(options).plan(rooms, store(players), NOW)
  assert.equal(plans.length, 1)
  assert.deepEqual(idsOf(plans), ['a', 'b'])
  assert.equal(plans[0].targetRoomId, 't')
})

test('7-occupant target with 1 free slot never yields a singleton split; 6-occupant fits the pair', () => {
  const sourcePlayers = [
    player('a', 's', 1, { affinity: { b: 1 } }), player('b', 's', 1),
    player('c', 's', 2), player('d', 's', 2), player('e', 's', 2)
  ]
  const full = [room('s', 5), room('t', 7)]
  const targets = Array.from({ length: 7 }, (_, i) => player(`t${i}`, 't', 1, { position: [i * 1000, 0, 0] }))
  assert.deepEqual(new LevelIsolationPlanner(options).plan(full, store([...sourcePlayers, ...targets]), NOW), [])
  const roomy = [room('s', 5), room('t', 6)]
  const plans = new LevelIsolationPlanner(options).plan(roomy, store([...sourcePlayers, ...targets.slice(0, 6)]), NOW)
  assert.equal(plans.length, 1)
  assert.deepEqual(idsOf(plans), ['a', 'b'])
  assert.equal(plans[0].targetRoomId, 't')
})

test('one-way bonds in BOTH directions block singletons (LevelIsolationPlanner)', () => {
  for (const direction of ['forward', 'reverse']) {
    const rooms = [room('s', 2), room('t', 0)]
    const bond = direction === 'forward' ? { affinity: { b: 1 } } : {}
    const reverse = direction === 'reverse' ? { affinity: { a: 1 } } : {}
    const players = [player('a', 's', 1, bond), player('b', 's', 1, reverse)]
    assert.deepEqual(new LevelIsolationPlanner(options).plan(rooms, store(players), NOW), [], direction)
  }
})

test('one-way bonds in BOTH directions block singletons (MovePlanner)', () => {
  for (const direction of ['forward', 'reverse']) {
    const rooms = [room('s', 2), room('t', 3)]
    const targets = Array.from({ length: 3 }, (_, i) => player(`q${i}`, 't', 1, { velocity: 5, position: [i * 1000, 0, 0] }))
    const bond = direction === 'forward' ? { affinity: { b: 1 } } : {}
    const reverse = direction === 'reverse' ? { affinity: { a: 1 } } : {}
    const players = [player('a', 's', 1, { position: [0, 0, 0], ...bond }), player('b', 's', 1, { position: [1000, 0, 0], ...reverse })]
    const planner = new MovePlanner({ minimumGain: -10, mergePressureThreshold: 0 })
    const plans = planner.plan(rooms, new Map([['s', players], ['t', targets]]), NOW)
    assert.deepEqual(idsOf(plans), ['a', 'b'], direction)
  }
})

test('bonded peer MOVING blocks the partner against the physical roster (LevelIsolationPlanner)', () => {
  const rooms = [room('s', 4), room('t', 0)]
  const moving = [
    player('a', 's', 1, { affinity: { b: 1 } }), player('b', 's', 1, { status: 'MOVING' }),
    player('c', 's', 2), player('e', 's', 2)
  ]
  assert.deepEqual(new LevelIsolationPlanner(options).plan(rooms, store(moving), NOW), [])
  const settled = moving.map(p => p.playerId === 'b' ? { ...p, status: 'ACTIVE' } : p)
  const plans = new LevelIsolationPlanner(options).plan(rooms, store(settled), NOW)
  assert.deepEqual(idsOf(plans), ['a', 'b'])
})

test('bonded peer MOVING blocks the partner against the physical roster (MovePlanner)', () => {
  const rooms = [room('s', 2), room('t', 1)]
  const a = player('a', 's', 1, { affinity: { b: 1 } })
  const moving = player('b', 's', 1, { status: 'MOVING' })
  const target = player('q', 't', 1, { velocity: 5 })
  const byRoom = new Map([['s', [a]], ['t', [target]]])
  const planner = new MovePlanner({ minimumGain: -10, mergePressureThreshold: 0 })
  const context = { physicalPlayersOf: roomId => roomId === 's' ? [a, moving] : byRoom.get(roomId) ?? [] }
  assert.deepEqual(planner.plan(rooms, byRoom, NOW, context), [], 'MOVING peer on the physical roster must block the singleton')
  assert.deepEqual(idsOf(planner.plan(rooms, byRoom, NOW, {})), ['a'], 'settled-view roster alone would split the pair')
})

test('churning bonded pair remains a complete mandatory-isolation candidate', () => {
  const churningStore = () => {
    const telemetry = new TelemetryStore()
    telemetry.upsert('s', { playerId: 'a', levelId: 1, affinity: { b: 1 }, roomJoinedAt: NOW - 60000 }, NOW - 60000)
    telemetry.upsert('s', { playerId: 'b', levelId: 9 }, NOW - 60000)
    telemetry.upsert('s', { playerId: 'c', levelId: 2 }, NOW - 60000)
    telemetry.upsert('s', { playerId: 'e', levelId: 2 }, NOW - 60000)
    // b flaps three times inside the 60s window, last change old enough to be settled
    telemetry.upsert('s', { playerId: 'b', levelId: 4 }, NOW - 30000)
    telemetry.upsert('s', { playerId: 'b', levelId: 5 }, NOW - 25000)
    telemetry.upsert('s', { playerId: 'b', levelId: 1 }, NOW - 20000)
    telemetry.upsert('s', { playerId: 'a', levelId: 1, affinity: { b: 1 } }, NOW)
    telemetry.upsert('s', { playerId: 'b', levelId: 1 }, NOW)
    telemetry.upsert('s', { playerId: 'c', levelId: 2 }, NOW)
    telemetry.upsert('s', { playerId: 'e', levelId: 2 }, NOW)
    assert.equal(telemetry.isChurning('b', NOW), true)
    assert.equal(telemetry.get('b').levelChangedAt <= NOW - 6000, true, 'b is otherwise settled')
    return telemetry
  }
  const rooms = [room('s', 4), room('t', 0)]
  const forced = new LevelIsolationPlanner(options).plan(rooms, churningStore(), NOW)
  assert.deepEqual(idsOf(forced), ['a', 'b'], 'churn never overrides mandatory isolation or splits the bond')
  const calmStore = () => {
    const telemetry = new TelemetryStore()
    telemetry.upsert('s', { playerId: 'a', levelId: 1, affinity: { b: 1 } }, NOW - 60000)
    telemetry.upsert('s', { playerId: 'b', levelId: 1 }, NOW - 60000)
    telemetry.upsert('s', { playerId: 'c', levelId: 2 }, NOW - 60000)
    telemetry.upsert('s', { playerId: 'e', levelId: 2 }, NOW - 60000)
    telemetry.upsert('s', { playerId: 'a', levelId: 1, affinity: { b: 1 } }, NOW)
    telemetry.upsert('s', { playerId: 'b', levelId: 1 }, NOW)
    telemetry.upsert('s', { playerId: 'c', levelId: 2 }, NOW)
    telemetry.upsert('s', { playerId: 'e', levelId: 2 }, NOW)
    return telemetry
  }
  const plans = new LevelIsolationPlanner(options).plan(rooms, calmStore(), NOW)
  assert.deepEqual(idsOf(plans), ['a', 'b'], 'without churn the pair moves together')
})

test('churn does not quarantine mandatory isolation of a coherent level', () => {
  const telemetry = new TelemetryStore()
  telemetry.upsert('s', { playerId: 'a', levelId: 1 }, NOW - 60000)
  telemetry.upsert('s', { playerId: 'b', levelId: 9 }, NOW - 60000)
  telemetry.upsert('s', { playerId: 'c', levelId: 2 }, NOW - 60000)
  telemetry.upsert('s', { playerId: 'b', levelId: 4 }, NOW - 30000)
  telemetry.upsert('s', { playerId: 'b', levelId: 5 }, NOW - 25000)
  telemetry.upsert('s', { playerId: 'b', levelId: 1 }, NOW - 20000)
  telemetry.upsert('s', { playerId: 'a', levelId: 1 }, NOW)
  telemetry.upsert('s', { playerId: 'b', levelId: 1 }, NOW)
  telemetry.upsert('s', { playerId: 'c', levelId: 2 }, NOW)
  const rooms = [room('s', 3), room('t', 0)]
  const plans = new LevelIsolationPlanner(options).plan(rooms, telemetry, NOW)
  assert.deepEqual(idsOf(plans), ['c'], 'the coherent minority moves and the room becomes single-level')
})

test('unsettled visitors do not freeze isolation planning for the settled minorities', () => {
  const rooms = [room('s', 3), room('t', 0)]
  const players = [
    player('a', 's', 1), player('b', 's', 2),
    player('v', 's', 3, { roomJoinedAt: NOW - 1000, levelChangedAt: NOW - 1000 })
  ]
  const plans = new LevelIsolationPlanner(options).plan(rooms, store(players), NOW)
  assert.equal(plans.length, 1, 'room is still planned despite the unsettled visitor')
  assert.equal(idsOf(plans).includes('v'), false)
})

test('real full/delta snapshot with a fifteen-minute frozen record still isolates fresh minority levels', () => {
  let now = NOW - 900000
  const rooms = new RoomCache({ maxRooms: 100, staleRoomMs: 600000 })
  const nodes = new NodeRegistry({ circuitOpenMs: 10, failureThreshold: 5, backpressureLimitBytes: 1000 })
  nodes.register({ nodeId: 'n', connectedAt: 10, protocolVersion: 1, writable: true, remoteAddress: '', async send () {}, close () {} })
  rooms.upsertRoom({ ...room('s', 0), nodeId: 'n', version: 0 })
  rooms.upsertRoom({ ...room('t1', 0), nodeId: 'n' })
  rooms.upsertRoom({ ...room('t2', 0), nodeId: 'n' })
  const telemetry = new TelemetryStore()
  const service = new RoomTelemetryService({ rooms, nodes, telemetry, requestFull () {}, clock: () => now })
  const observed = (playerId, levelId, extra = {}) => ({ playerId, sessionId: `s-${playerId}`, lvSeq: 1, status: 'ACTIVE', levelId, ...extra })
  const frozen = observed('frozen', 1)
  const core = [observed('core1', 1), observed('core2', 1), observed('a', 2), observed('b', 3)]
  const frame = (seq, players, extra = {}) => ({ event: 'room.telemetry', data: { nodeId: 'n', roomId: 's', kind: 'full', seq, players, ...extra } })
  assert.equal(service.accept(frame(1, [frozen, ...core]), 's'), true)
  now = NOW
  assert.equal(service.accept(frame(2, core, { kind: 'delta', baseSeq: 1 }), 's'), true)
  assert.equal(telemetry.get('frozen').receivedAt, NOW - 900000)
  const plans = new LevelIsolationPlanner(options).plan(rooms.listRooms(), telemetry, now)
  assert.deepEqual(idsOf(plans), ['a', 'b'])
  assert.equal(new Set(plans.map(plan => plan.targetRoomId)).size, 2)
  assert.ok(plans.every(plan => plan.reason === 'level_isolation'))
})

test('stale unresolved bond mate survives age and blocks until authoritative full removal', () => {
  let now = NOW - 900000, resyncs = 0
  const rooms = new RoomCache({ maxRooms: 100, staleRoomMs: 600000 })
  const nodes = new NodeRegistry({ circuitOpenMs: 10, failureThreshold: 5, backpressureLimitBytes: 1000 })
  nodes.register({ nodeId: 'n', connectedAt: 10, protocolVersion: 1, writable: true, remoteAddress: '', async send () {}, close () {} })
  rooms.upsertRoom({ ...room('s', 0), nodeId: 'n', version: 0 })
  rooms.upsertRoom({ ...room('t', 0), nodeId: 'n' })
  const telemetry = new TelemetryStore()
  const service = new RoomTelemetryService({ rooms, nodes, telemetry, requestFull () { resyncs++ }, clock: () => now })
  const observed = (playerId, levelId, extra = {}) => ({ playerId, sessionId: `s-${playerId}`, lvSeq: 1, status: 'ACTIVE', levelId, ...extra })
  const mate = observed('b', 2, { affinity: { a: 1 }, status: 'MOVING' })
  const core = [observed('a', 2), observed('c', 1), observed('d', 1), observed('e', 1)]
  const frame = (seq, players, extra = {}) => ({ event: 'room.telemetry', data: { nodeId: 'n', roomId: 's', kind: 'full', seq, players, ...extra } })
  service.accept(frame(1, [mate, ...core]), 's')
  now = NOW
  service.accept(frame(2, core, { kind: 'delta', baseSeq: 1 }), 's')
  const planner = new LevelIsolationPlanner(options)
  assert.deepEqual(planner.plan(rooms.listRooms(), telemetry, now), [])
  now += 900000
  service.accept(frame(3, core, { kind: 'delta', baseSeq: 2 }), 's')
  assert.deepEqual(planner.plan(rooms.listRooms(), telemetry, now), [], 'another fifteen minutes never waives the bond')
  assert.ok(telemetry.get('b'))
  assert.equal(service.accept(frame(4, core), 's'), true)
  assert.equal(telemetry.get('b'), undefined)
  assert.deepEqual(idsOf(planner.plan(rooms.listRooms(), telemetry, now)), ['a'])
  assert.equal(resyncs, 0)
})

test('full known stale or loading majority retains its level while a fresh settled minority isolates', () => {
  for (const state of ['stale', 'loading']) {
    const players = [player('a', 's', 2), ...['b', 'c', 'd'].map(id => player(id, 's', 1,
      state === 'stale' ? { receivedAt: NOW - 900000 } : { roomJoinedAt: NOW - 1000 }))]
    const plans = new LevelIsolationPlanner(options).plan([room('s', 4), room('t', 0)], store(players), NOW)
    assert.deepEqual(idsOf(plans), ['a'], state)
    assert.equal(plans[0].reason, 'level_isolation')
  }
})

test('staggered bonded level changes wait six seconds for the last member and move complete transitive component', () => {
  const telemetry = new TelemetryStore()
  const base = NOW - 60000
  const upsert = (id, levelId, at, extra = {}) => telemetry.upsert('s', { playerId: id, sessionId: `s-${id}`, lvSeq: levelId, status: 'ACTIVE', levelId, ...extra }, at)
  for (const id of ['a', 'b', 'c', 'm1', 'm2', 'm3', 'm4']) upsert(id, 1, base)
  upsert('a', 2, NOW, { affinity: { b: 1 } })
  upsert('b', 2, NOW + 1000, { affinity: { c: 1 } })
  upsert('c', 2, NOW + 2000)
  const refresh = at => { for (const p of telemetry.list()) telemetry.upsert('s', p, at) }
  const planner = new LevelIsolationPlanner(options)
  const rooms = [room('s', 7), room('t', 0)]
  refresh(NOW + 7999)
  assert.deepEqual(planner.plan(rooms, telemetry, NOW + 7999), [])
  refresh(NOW + 8000)
  assert.deepEqual(idsOf(planner.plan(rooms, telemetry, NOW + 8000)), ['a', 'b', 'c'])
})

test('optional consolidation tries a fitting target after the fullest room cannot fit the bonded pair', () => {
  const players = [player('a', 's', 1, { affinity: { b: 1 } }), player('b', 's', 1),
    ...Array.from({ length: 7 }, (_, i) => player(`x${i}`, 'full', 1)),
    ...Array.from({ length: 6 }, (_, i) => player(`y${i}`, 'fit', 1))]
  const plans = new LevelIsolationPlanner(options).plan([room('s', 2), room('full', 7), room('fit', 6)], store(players), NOW)
  assert.equal(plans.length, 1)
  assert.deepEqual(idsOf(plans), ['a', 'b'])
  assert.equal(plans[0].targetRoomId, 'fit')
  assert.equal(plans[0].reason, 'level_consolidation')
})

// ---------------------------------------------------------------------------
// TelemetryStore: physical roster and churn quarantine
// ---------------------------------------------------------------------------

test('listPhysical keeps MOVING occupants inside the same freshness bound as listRoom', () => {
  const now = Date.now()
  const telemetry = new TelemetryStore()
  telemetry.upsert('r', { playerId: 'moving', status: 'MOVING', levelId: 1 }, now)
  telemetry.upsert('r', { playerId: 'stale-ok' }, now - 300000)
  telemetry.upsert('r', { playerId: 'gone' }, now - 300001)
  assert.deepEqual(telemetry.listPhysical('r', now).map(p => p.playerId).sort(), ['moving', 'stale-ok'])
  assert.deepEqual(telemetry.listRoom('r', now).map(p => p.playerId), ['stale-ok'])
})

test('isChurning triggers at 3 changes per 60s, latches, and clears only after quarantine', () => {
  const telemetry = new TelemetryStore()
  const base = Date.now() - 200000
  telemetry.upsert('r', { playerId: 'p', levelId: 1 }, base)
  telemetry.upsert('r', { playerId: 'p', levelId: 2 }, base + 1000)
  telemetry.upsert('r', { playerId: 'p', levelId: 3 }, base + 2000)
  telemetry.upsert('r', { playerId: 'p', levelId: 4 }, base + 3000)
  assert.equal(telemetry.isChurning('p', base + 3000), true)
  assert.equal(telemetry.isChurning('p', base + 63000), true, 'latched although the 60s window went quiet')
  assert.equal(telemetry.isChurning('p', base + 123000), false, 'quarantine elapsed and the window stays quiet')
  assert.equal(telemetry.isChurning('unknown', base), false)
})

test('churn knobs are configurable through the telemetry store options', () => {
  const telemetry = new TelemetryStore({ stateEmaHalfLifeMs: 3000, churnMaxChanges: 2, churnWindowMs: 1000, churnQuarantineMs: 5000 })
  const base = Date.now() - 200000
  telemetry.upsert('r', { playerId: 'p', levelId: 1 }, base)
  telemetry.upsert('r', { playerId: 'p', levelId: 2 }, base + 100)
  assert.equal(telemetry.isChurning('p', base + 200), false)
  telemetry.upsert('r', { playerId: 'p', levelId: 3 }, base + 200)
  assert.equal(telemetry.isChurning('p', base + 300), true)
  assert.equal(telemetry.isChurning('p', base + 3000), true)
  assert.equal(telemetry.isChurning('p', base + 6000), false)
})

// ---------------------------------------------------------------------------
// MoveCoordinator: guards, stability scoping, observability
// ---------------------------------------------------------------------------

const logger = { info () {}, warn () {}, error () {}, debug () {}, trace () {}, child () { return this } }
const coordRoom = (roomId, players = 0) => ({ roomId, nodeId: roomId === 'a' ? 'na' : 'nb', udpHost: '127.0.0.1', udpPort: roomId === 'a' ? 41000 : 41001, capacity: 8, players, version: 1, draining: false })
const ev = (event, roomId, data = {}, nodeId = roomId === 'a' ? 'na' : 'nb') => ({ event, data: { nodeId, roomId, ...data } })

const fixture = (options = {}, events) => {
  const rooms = new RoomCache({ maxRooms: 100, staleRoomMs: 600000 })
  const nodes = new NodeRegistry({ circuitOpenMs: 500, failureThreshold: 10, backpressureLimitBytes: 999999 })
  const pending = new PendingRequestMap({ timeoutMs: 100, maxPending: 100 })
  const sent = []
  let sendHook = null
  for (const nodeId of ['na', 'nb']) nodes.register({ nodeId, connectedAt: 100, protocolVersion: 1, writable: true, remoteAddress: '', close () {},
    async send (command) { sent.push({ nodeId, ...command }); if (sendHook) sendHook(command); pending.resolve({ id: command.id, code: 0 }, nodeId) } })
  rooms.upsertRoom(coordRoom('a', 2)); rooms.upsertRoom(coordRoom('b'))
  const cross = new CrossCoordinator(nodes, rooms, pending, { commandTimeoutMs: 100 })
  const moves = new MoveCoordinator(rooms, nodes, pending, cross, { confirmationCycles: 1, ...options }, logger, undefined, undefined, events)
  const cleanup = async () => { pending.close(); await moves.stop() }
  return { rooms, nodes, pending, sent, cross, moves, cleanup, onSend: fn => { sendHook = fn } }
}
const seed = (f, roomId, playerId, levelId, extra = {}, now = Date.now()) => {
  f.rooms.trackPlayer(playerId, 'na', roomId, levelId, 's-' + playerId, 1)
  f.moves.telemetry.upsert(roomId, { playerId, levelId, sessionId: 's-' + playerId, lvSeq: 1, status: 'ACTIVE', ...extra }, now - 60000)
  f.moves.telemetry.upsert(roomId, { playerId, levelId, sessionId: 's-' + playerId, lvSeq: 1, status: 'ACTIVE', ...extra }, now)
}
const seedLevelChange = (f, roomId, playerId, fromLevel, toLevel, extra = {}, now = Date.now()) => {
  f.rooms.trackPlayer(playerId, 'na', roomId, toLevel, 's-' + playerId, 2)
  f.moves.telemetry.upsert(roomId, { playerId, levelId: fromLevel, sessionId: 's-' + playerId, lvSeq: 1, status: 'ACTIVE', ...extra }, now - 60000)
  f.moves.telemetry.upsert(roomId, { playerId, levelId: toLevel, sessionId: 's-' + playerId, lvSeq: 2, status: 'ACTIVE', ...extra }, now)
}

test('debug manual move overrides hard bonds outside the requested player group', async () => {
  const events = new DebugEventLog(100)
  const f = fixture({}, events)
  try {
    seed(f, 'a', 'p', 1, { affinity: { q: 1 } })
    seed(f, 'a', 'q', 1)
    const tx = await f.moves.manualMove({ sourceRoomId: 'a', targetRoomId: 'b', playerIds: ['p'] })
    assert.equal(tx.status, 'committed')
    assert.deepEqual(tx.plannedIds, ['p'], 'manual request moves exactly the explicitly requested players')
    assert.ok(f.sent.some(command => command.cmd === 'move.commit'))
    assert.equal(events.list({ category: 'move' }).entries.some(event => event.event === 'plan.rejected'), false)
  } finally { await f.cleanup() }
})

test('post-prepare reverse bond fails the transaction and queues cancellation without redirect', async () => {
  const events = new DebugEventLog(100)
  const f = fixture({}, events)
  try {
    seed(f, 'a', 'p', 1)
    seed(f, 'a', 'q', 1)
    // the bond appears only after the pre-prepare guard passed (during prepare)
    f.onSend(command => {
      if (command.cmd === 'move.prepare') f.moves.telemetry.upsert('a', { playerId: 'q', levelId: 1, sessionId: 's-q', lvSeq: 1, status: 'ACTIVE', affinity: { p: 1 } })
    })
    const original = f.cross.redirectPlayers.bind(f.cross)
    let redirects = 0
    f.cross.redirectPlayers = async (...args) => { redirects++; return original(...args) }
    // 通过自动规划执行，调试手动迁移的豁免不应影响自动路径。
    f.moves.planner.calibrate = () => {}
    f.moves.planner.plan = () => [{ sourceRoomId: 'a', targetRoomId: 'b', playerIds: ['p'], gain: 1, reason: 'player_rebalance' }]
    f.moves.levelIsolationPlanner.plan = () => []
    const txs = await f.moves.planAndExecute()
    const tx = txs[0]
    assert.ok(tx)
    assert.equal(tx.status, 'failed')
    assert.equal(redirects, 0, 'commit-time closure blocks the redirect')
    assert.ok(f.sent.some(c => c.cmd === 'move.cancel' && c.data.moveId === tx.moveId), 'cancellation queued')
    assert.ok(events.list({ category: 'move' }).entries.some(e => e.event === 'move.failed'))
  } finally { await f.cleanup() }
})

test('plan.skipped is rate-limited per sourceRoomId+reason and re-fires on reason change', async () => {
  const events = new DebugEventLog(100)
  const f = fixture({}, events)
  try {
    seed(f, 'a', 'p', 1)
    seed(f, 'a', 'q', 1)
    f.moves.planner.calibrate = () => {}
    f.moves.levelIsolationPlanner.plan = () => []
    const plan = { sourceRoomId: 'a', targetRoomId: 'b', playerIds: ['p', 'q'], gain: 1, reason: 'player_rebalance' }
    f.moves.planner.plan = () => [plan]
    const runCycle = async () => { await f.moves.runCycle(new AbortController().signal, Date.now()) }
    f.rooms.upsertRoom(coordRoom('b', 8))
    await runCycle()
    await runCycle()
    let skips = events.list({ category: 'move' }).entries.filter(e => e.event === 'plan.skipped')
    assert.equal(skips.length, 1, 'same reason within the window is rate-limited')
    assert.equal(skips[0].data.reason, 'target_capacity')
    f.rooms.upsertRoom(coordRoom('b', 0))
    f.moves.executor.execute = async ({ moveId }) => ({ ok: true, moveId })
    await f.moves.manualMove({ sourceRoomId: 'a', targetRoomId: 'b', playerIds: ['p', 'q'] })
    await runCycle()
    await runCycle()
    skips = events.list({ category: 'move' }).entries.filter(e => e.event === 'plan.skipped')
    assert.deepEqual(skips.map(e => e.data.reason), ['target_capacity', 'source_not_candidate'], 'a new reason records a new event')
  } finally { await f.cleanup() }
})

test('stability signature covers only the settled core; visitor churn cannot reset confirmation', async () => {
  const f = fixture({ confirmationCycles: 3 })
  try {
    seed(f, 'a', 'p', 1)
    seed(f, 'a', 'q', 1)
    const frame = seq => ({ event: 'room.telemetry', data: { nodeId: 'na', roomId: 'a', kind: 'full', seq,
      players: [
        { playerId: 'p', sessionId: 's-p', levelId: 1, lvSeq: 1, status: 'ACTIVE' },
        { playerId: 'q', sessionId: 's-q', levelId: 1, lvSeq: 1, status: 'ACTIVE' }
      ] } })
    const runCycle = async () => { await f.moves.runCycle(new AbortController().signal, Date.now()) }
    assert.equal(f.moves.ingest(frame(1)), true)
    await runCycle()
    for (const [seq, name] of [[2, 'v1'], [3, 'v2']]) {
      assert.equal(f.moves.ingest(ev('player.join', 'a', { player: { playerId: name, sessionId: 's-' + name, level: 3, lvSeq: 1 } })), true)
      assert.equal(f.moves.ingest(ev('player.leave', 'a', { player: { playerId: name, sessionId: 's-' + name } })), true)
      assert.equal(f.moves.ingest(frame(seq)), true)
      await runCycle()
    }
    const entry = f.moves.stabilitySnapshot().find(e => e.roomId === 'a')
    assert.equal(entry.cycles, 3, 'visitor join/leave churn did not reset confirmation')
    assert.equal(entry.seq, 3)
    assert.equal(entry.gap, false)
  } finally { await f.cleanup() }
})

test('visitor join/leave churn does not freeze planning for the settled core (end to end)', async () => {
  const f = fixture({}, new DebugEventLog(100))
  try {
    seed(f, 'a', 'p', 1)
    seed(f, 'a', 'q', 1)
    seed(f, 'a', 'r', 2)
    assert.equal(f.moves.ingest(ev('player.join', 'a', { player: { playerId: 'v', sessionId: 's-v', level: 3, lvSeq: 1 } })), true)
    const txs = await f.moves.planAndExecute()
    const tx = txs.find(t => t.plannedIds.includes('r'))
    assert.ok(tx, 'settled minority is still planned despite the unsettled visitor')
    assert.equal(tx.status, 'committed')
    assert.ok(txs.every(t => !t.plannedIds.includes('v')), 'unsettled visitor is never moved')
  } finally { await f.cleanup() }
})

test('bonded pair that switches level together is eventually planned and moved together', async () => {
  const f = fixture({ planner: { levelChangeCooldownMs: 0 }, levelIsolation: { levelChangeCooldownMs: 0 } }, new DebugEventLog(100))
  try {
    const now = Date.now()
    seedLevelChange(f, 'a', 'p', 2, 1, { affinity: { q: 1 } }, now)
    seedLevelChange(f, 'a', 'q', 2, 1, {}, now)
    seed(f, 'a', 'r', 2, {}, now)
    seed(f, 'a', 's', 2, {}, now)
    seed(f, 'a', 't', 2, {}, now)
    const txs = await f.moves.planAndExecute()
    const tx = txs.find(t => t.plannedIds.includes('p'))
    assert.ok(tx, 'the pair is planned once both pass the (zero) level-change cooldown')
    assert.deepEqual([...tx.plannedIds].sort(), ['p', 'q'], 'never split')
    assert.equal(tx.status, 'committed')
    assert.equal(txs.filter(t => t.plannedIds.length === 1 && t.plannedIds.some(id => id === 'p' || id === 'q')).length, 0, 'no singleton split')
  } finally { await f.cleanup() }
})

test('stabilitySnapshot exposes per-room planner state', async () => {
  const f = fixture({}, new DebugEventLog(100))
  try {
    seed(f, 'a', 'p', 1)
    const snapshot = f.moves.stabilitySnapshot()
    const entry = snapshot.find(e => e.roomId === 'a')
    assert.ok(entry)
    assert.equal(typeof entry.cycles, 'number')
    assert.equal(typeof entry.busyTx, 'boolean')
    assert.equal(typeof entry.busyCancellations, 'boolean')
    assert.equal(typeof entry.gap, 'boolean')
    assert.deepEqual(entry.remigrateBlockedPlayerIds, [])
    assert.equal(entry.lastSkip, undefined)
    assert.equal(typeof f.moves.stabilitySnapshot().find(e => e.roomId === 'b').busyTx, 'boolean')
  } finally { await f.cleanup() }
})

test('levelChangeCooldownMs default is 6000 in both planners', () => {
  assert.equal(new MovePlanner().levelChangeCooldownMs, 6000)
  assert.equal(new LevelIsolationPlanner().levelChangeCooldownMs, 6000)
})

test('mandatory isolation bypasses confirmation seven and unrelated room cooldowns', async () => {
  const f = fixture({ confirmationCycles: 7 }, new DebugEventLog(100))
  try {
    const now = Date.now()
    seed(f, 'a', 'p', 1, { affinity: { q: 1 } }, now)
    seed(f, 'a', 'q', 1, {}, now)
    for (const id of ['r', 's', 't']) seed(f, 'a', id, 2, {}, now)
    f.rooms.upsertRoom(coordRoom('a', 5))
    f.moves.recentArrivals.set('a', now)
    f.moves.migratedPlayers.set('r', now)
    const txs = await f.moves.planAndExecute()
    const tx = txs.find(item => item.reason === 'level_isolation')
    assert.ok(tx)
    assert.deepEqual([...tx.plannedIds].sort(), ['p', 'q'])
    assert.equal(tx.status, 'committed')
  } finally { await f.cleanup() }
})

test('optional consolidation still waits for source confirmation seven', async () => {
  const f = fixture({ confirmationCycles: 7 })
  try {
    const now = Date.now()
    seed(f, 'a', 'p', 1, { affinity: { q: 1 } }, now)
    seed(f, 'a', 'q', 1, {}, now)
    f.rooms.trackPlayer('target', 'nb', 'b', 1, 's-target', 1)
    f.moves.telemetry.upsert('b', { playerId: 'target', sessionId: 's-target', lvSeq: 1, levelId: 1, status: 'ACTIVE' }, now - 60000)
    f.moves.telemetry.upsert('b', { playerId: 'target', sessionId: 's-target', lvSeq: 1, levelId: 1, status: 'ACTIVE' }, now)
    f.rooms.upsertRoom(coordRoom('b', 6))
    await f.moves.planAndExecute()
    assert.equal(f.sent.some(command => command.cmd === 'move.prepare'), false)
  } finally { await f.cleanup() }
})

test('source becoming stale or MOVING during prepare cancels before redirect', async () => {
  for (const state of ['stale', 'MOVING']) {
    const f = fixture()
    try {
      seed(f, 'a', 'p', 1)
      f.onSend(command => {
        if (command.cmd !== 'move.prepare') return
        const item = f.moves.telemetry.get('p')
        if (state === 'stale') item.receivedAt = Date.now() - 11000
        else item.status = 'MOVING'
      })
      const tx = await f.moves.manualMove({ sourceRoomId: 'a', targetRoomId: 'b', playerIds: ['p'] })
      assert.equal(tx.status, 'failed', state)
      assert.equal(f.sent.some(command => command.cmd === 'move.commit'), false)
      assert.ok(f.sent.some(command => command.cmd === 'move.cancel'))
    } finally { await f.cleanup() }
  }
})

// ---------------------------------------------------------------------------
// HTTP seam: /debug/allnode planner snapshot and isHost passthrough
// ---------------------------------------------------------------------------

const httpJson = (port, path, headers = {}) => new Promise((resolve, reject) => {
  const req = request({ host: '127.0.0.1', port, path, method: 'GET', headers: { connection: 'close', ...headers } }, res => {
    const chunks = []
    res.on('data', chunk => chunks.push(chunk))
    res.on('error', reject)
    res.on('end', () => {
      try { resolve({ status: res.statusCode, data: JSON.parse(Buffer.concat(chunks).toString('utf8')) }) }
      catch (error) { reject(error) }
    })
  })
  req.on('error', reject)
  req.setTimeout(3000, () => req.destroy(new Error('HTTP test timed out')))
  req.end()
})

test('/debug/allnode exposes the per-room planner snapshot and isHost telemetry flows through', async () => {
  const directory = mkdtempSync(join(tmpdir(), 'qwd-group-safety-'))
  let runtime, gateway
  try {
    const file = join(directory, 'config.yml')
    writeFileSync(file, '{}')
    const config = { ...loadConfig(file), host: '127.0.0.1', port: 0, debugApiToken: 'group-safety-secret' }
    runtime = createManagerRuntime(config, logger)
    gateway = new ManagerGateway(config, runtime, logger)
    await gateway.start()
    const port = gateway.server.address().port
    runtime.nodes.register({ nodeId: 'n', protocolVersion: 1, connectedAt: Date.now(), remoteAddress: '127.0.0.1', writable: true, async send () {}, close () {} })
    runtime.rooms.upsertRoom(contracts.RoomSnapshotSchema.parse({ roomId: 'r', nodeId: 'n', udpHost: '127.0.0.1', udpPort: 41000, players: 1 }))
    assert.equal(runtime.moves.ingest({ event: 'room.telemetry', data: { nodeId: 'n', roomId: 'r', kind: 'full', seq: 1,
      players: [{ playerId: 'p', sessionId: 's-p', levelId: 1, lvSeq: 1, status: 'ACTIVE', isHost: true }] } }), true)
    assert.equal(runtime.moves.telemetry.get('p').isHost, true, 'node-reported isHost is stored, not computed')
    const denied = await httpJson(port, '/debug/allnode')
    assert.equal(denied.status, 401)
    const reply = await httpJson(port, '/debug/allnode', { 'x-debug-token': 'group-safety-secret' })
    assert.equal(reply.status, 200)
    const room = reply.data.data.nodes[0].rooms[0]
    assert.equal(room.roomId, 'r')
    assert.ok(room.planner, 'per-room planner snapshot is attached')
    assert.equal(typeof room.planner.cycles, 'number')
    assert.equal(room.players[0].telemetry.isHost, true, 'isHost flows into the allnode serialization')
  } finally {
    if (gateway) await gateway.stop()
    if (runtime) { runtime.pending.close?.(); runtime.reservations.stop?.(); await runtime.moves.stop?.() }
    rmSync(directory, { recursive: true, force: true })
  }
})
