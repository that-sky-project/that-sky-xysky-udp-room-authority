import assert from 'node:assert/strict'
import test from 'node:test'
import { LevelIsolationPlanner } from '../dist/manager-host/LevelIsolationPlanner.js'
import { MovePlanner } from '../dist/manager-host/MovePlanner.js'

const NOW = 100000
const room = (id, players, capacity = 8) => ({ roomId: id, nodeId: id, udpHost: '127.0.0.1', udpPort: 3000, players, capacity, version: 1, draining: false })
const player = (id, roomId, levelId, extra = {}) => ({ playerId: id, roomId, levelId, receivedAt: NOW, stateAt: NOW, position: [0, 0, 0], velocity: 0, roomJoinedAt: 0, levelChangedAt: 0, ...extra })
const store = players => ({ list: () => players, listRoom: id => players.filter(p => p.roomId === id) })
const options = { levelChangeCooldownMs: 8000, joinSettleMs: 6000 }

test('hard bonds spanning levels defer the whole bond, never drag an unsettled player', () => {
  const rooms = [room('s', 3), room('t', 0)]
  const players = [player('anchor', 's', 1), player('a', 's', 1, { affinity: { b: 1 } }), player('b', 's', 2, { levelChangedAt: NOW - 1 })]
  const planner = new LevelIsolationPlanner(options)
  assert.deepEqual(planner.plan(rooms, store(players), NOW), [])
  players[2].levelChangedAt = 0
  assert.deepEqual(planner.plan(rooms, store(players), NOW), [], 'stable but cross-level bond must still defer')
  players[1].levelId = 2
  players.push(player('anchor2', 's', 1), player('anchor3', 's', 1))
  rooms[0].players = 5
  const plans = planner.plan(rooms, store(players), NOW)
  assert.equal(plans.length, 1, 'coherent same-level bond must recover')
  assert.deepEqual(new Set(plans[0].playerIds), new Set(['a', 'b']))
})
test('protected hard bond conflict defers intruder rather than breaking players', () => {
  const rooms = [room('s', 2), room('t', 0)]
  const players = [player('p', 's', 9, { affinity: { q: 1 } }), player('q', 's', 2)]
  const planner = new LevelIsolationPlanner({ ...options, protectedLevels: [9] })
  assert.deepEqual(planner.plan(rooms, store(players), NOW), [])
  players[0].affinity = {}
  assert.equal(planner.plan(rooms, store(players), NOW).length, 1)
})
test('ordinary legitimate mixed target remains allowed when no clean target exists', () => {
  const rooms = [room('s', 3), room('t', 2)]
  const players = [player('s1', 's', 1), player('s2', 's', 1), player('s3', 's', 2), player('t1', 't', 2), player('t2', 't', 3)]
  const plans = new LevelIsolationPlanner(options).plan(rooms, store(players), NOW)
  assert.ok(plans.some(p => p.sourceRoomId === 's' && p.targetRoomId === 't'))
})
test('capacity includes snapshot occupants with missing telemetry and allocation/move holds', () => {
  const rooms = [room('s', 3), room('ghost', 8), room('held', 0), room('t', 0)]
  const players = [player('s1', 's', 1), player('s2', 's', 1), player('s3', 's', 2)]
  const context = { effectivePlayersOf: r => r.roomId === 'held' ? 8 : r.players }
  const plans = new LevelIsolationPlanner(options).plan(rooms, store(players), NOW, undefined, context)
  assert.equal(plans[0].targetRoomId, 't')
})
test('busy/backoff/gap candidate hook filters before target choice, not after choosing', () => {
  const rooms = [room('s', 3), room('busy', 0), room('t', 0)]
  const players = [player('s1', 's', 1), player('s2', 's', 1), player('s3', 's', 2)]
  const context = { isCandidateRoom: r => r.roomId !== 'busy' }
  assert.equal(new LevelIsolationPlanner(options).plan(rooms, store(players), NOW, undefined, context)[0].targetRoomId, 't')
})
test('frozen roster is not a fresh trusted observation; refreshed observation recovers', () => {
  const rooms = [room('s', 3), room('t', 0)]
  const players = [player('s1', 's', 1), player('s2', 's', 1), player('s3', 's', 2)]
  const planner = new LevelIsolationPlanner(options)
  for (const p of players) p.receivedAt = NOW - 30000
  assert.deepEqual(planner.plan(rooms, store(players), NOW), [])
  for (const p of players) p.receivedAt = NOW
  assert.equal(planner.plan(rooms, store(players), NOW).length, 1)
})
test('eight-player hard bond cannot be split into a seven-player move; oversized inputs are bounded', () => {
  const rooms = [room('s', 8), room('t', 1)]
  const players = Array.from({ length: 8 }, (_, i) => player(`p${i}`, 's', 1, { affinity: Object.fromEntries(Array.from({ length: 8 }, (_, j) => [`p${j}`, 1])) }))
  players.push(player('q', 't', 1))
  assert.deepEqual(new LevelIsolationPlanner({ ...options, sparseRoomThreshold: 8 }).plan(rooms, store(players), NOW), [])
  assert.throws(() => new LevelIsolationPlanner({ maxPlayersPerMove: 8 }), /7/)
  const over = Array.from({ length: 40 }, (_, i) => player(`x${i}`, 's', 1))
  const planner = new MovePlanner({ minimumGain: -10, mergePressureThreshold: 0 })
  const started = performance.now()
  assert.deepEqual(planner.plan(rooms, new Map([['s', over], ['t', [player('q', 't', 1, { velocity: 5 })]]]), NOW), [])
  assert.ok(performance.now() - started < 100)
})
