import test from 'node:test'
import assert from 'node:assert/strict'
import { getEventListeners } from 'node:events'
import { setTimeout as sleep } from 'node:timers/promises'
import { PendingRequestMap } from '../dist/manager-host/PendingRequestMap.js'
import { ReservationTracker } from '../dist/manager-host/ReservationTracker.js'
import { MoveCoordinator } from '../dist/manager-host/MoveCoordinator.js'

const command = { v: 1, cmd: 'move.cancel', data: {} }
const ack = id => ({ id, code: 0, message: 'OK' })
const deferred = () => {
  let resolve, reject
  const promise = new Promise((yes, no) => { resolve = yes; reject = no })
  return { promise, resolve, reject }
}
const map = timeoutMs => new PendingRequestMap({ timeoutMs: timeoutMs ?? 100, maxPending: 4 })
const waitForSend = async () => { await Promise.resolve(); await Promise.resolve() }

test('ACK ordering, authenticated node binding and listener cleanup', async () => {
  const pending = map()
  const send = deferred()
  const controller = new AbortController()
  let id, done = false
  const result = pending.send(command, c => { id = c.id; return send.promise }, controller.signal, 'node-a')
  result.then(() => { done = true })
  await waitForSend()
  assert.equal(pending.resolve(ack(id)), false)
  assert.equal(pending.resolve(ack(id), 'node-b'), false)
  assert.equal(pending.resolve(ack(id), 'node-a'), true)
  assert.equal(pending.resolve(ack(id), 'node-a'), false)
  await waitForSend()
  assert.equal(done, false)
  assert.equal(pending.size(), 1)
  send.resolve()
  assert.equal((await result).id, id)
  assert.equal(pending.size(), 0)
  assert.equal(getEventListeners(controller.signal, 'abort').length, 0)
})

test('pre-aborted signal never sends; abort returns promptly while sender is stuck', async () => {
  const pending = map(5_000)
  const pre = AbortSignal.abort()
  let calls = 0
  await assert.rejects(pending.send(command, async () => { calls++ }, pre), /aborted/)
  assert.equal(calls, 0)
  const controller = new AbortController()
  const stuck = deferred()
  const result = pending.send(command, () => stuck.promise, controller.signal)
  const check = assert.rejects(result, /aborted/)
  await waitForSend()
  controller.abort()
  await check
  assert.equal(pending.size(), 0)
  assert.equal(getEventListeners(controller.signal, 'abort').length, 0)
  stuck.reject(new Error('late sender rejection'))
  await sleep(10)
})

test('timeout returns despite a stuck sender and early ACK; no orphan rejection', async () => {
  const pending = map(20)
  const controller = new AbortController()
  const stuck = deferred()
  let id
  const result = pending.send(command, c => { id = c.id; return stuck.promise }, controller.signal, 'a')
  const check = assert.rejects(result, /timed out/)
  await waitForSend()
  assert.equal(pending.resolve(ack(id), 'a'), true)
  // Timers in production are unref; this test keeps one referenced timer.
  await sleep(35)
  await check
  assert.equal(getEventListeners(controller.signal, 'abort').length, 0)
  assert.equal(pending.size(), 0)
  stuck.reject(new Error('transport failed after deadline'))
  await sleep(10)
})

test('send failure wins over early ACK, including sync/non-Error failures', async () => {
  const pending = map()
  const send = deferred()
  let id
  const error = new Error('send failed')
  const result = pending.send(command, c => { id = c.id; return send.promise }, undefined, 'a')
  const check = assert.rejects(result, error)
  await waitForSend()
  pending.resolve(ack(id), 'a')
  send.reject(error)
  await check
  await assert.rejects(pending.send(command, () => { throw new Error('sync send failed') }), /sync send failed/)
  const unknown = { problem: 'unknown' }
  await pending.send(command, () => Promise.reject(unknown)).then(() => assert.fail(), value => assert.equal(value, unknown))
  assert.equal(pending.size(), 0)
  await sleep(10)
})

test('rejectAll is reusable; close permanently rejects pending/future work and clears listeners', async () => {
  const pending = map()
  const controller = new AbortController()
  const one = pending.send(command, async () => {}, controller.signal)
  const checkOne = assert.rejects(one, /drain/)
  pending.rejectAll(new Error('drain'))
  await checkOne
  assert.equal(getEventListeners(controller.signal, 'abort').length, 0)
  const two = pending.send(command, async () => {})
  const checkTwo = assert.rejects(two, /stopped/)
  pending.close()
  pending.close(new Error('different reason'))
  await checkTwo
  await assert.rejects(pending.send(command, async () => assert.fail()), /stopped/)
  assert.equal(pending.size(), 0)
})

test('reservation stop clears holds/timers and forbids reacquisition', async () => {
  const tracker = new ReservationTracker({ ttlMs: 30_000 })
  const room = { roomId: 'r', players: 0, capacity: 8 }
  assert.equal(tracker.tryAcquire(room, 'p'), true)
  assert.equal(tracker.count('r'), 1)
  tracker.stop()
  tracker.stop()
  assert.equal(tracker.size(), 0)
  assert.equal(tracker.count('r'), 0)
  assert.equal(tracker.tryAcquire(room, 'p'), false)
})

const moveFixture = () => {
  const pending = map(5_000)
  const source = { roomId: 'source', nodeId: 'a', players: 1, capacity: 8, version: 1 }
  const target = { roomId: 'target', nodeId: 'b', players: 0, capacity: 8, version: 1 }
  const rooms = {
    getRoom: id => id === 'source' ? source : id === 'target' ? target : undefined,
    getPlayerLocation: () => ({ roomId: 'source' }),
    listRooms: () => [source, target], isNodeSuspected: () => false
  }
  const nodes = { isAvailableForCommand: () => true, getTransport: () => ({ writable: true, send: () => new Promise(() => {}) }) }
  const moves = new MoveCoordinator(rooms, nodes, pending, { redirectPlayers: async () => ({ code: 0 }) })
  return { moves, pending }
}

test('move stop aborts manual work and queued cancellation, awaits it, and admits no new work', async () => {
  const { moves, pending } = moveFixture()
  const work = moves.manualMove({ sourceRoomId: 'source', targetRoomId: 'target', playerIds: ['p'] })
  const check = assert.rejects(work, /aborted/)
  await waitForSend()
  moves.queueCancel('target', 'cancel-test', ['p'])
  const stop = moves.stop()
  assert.equal(moves.stop(), stop)
  await stop
  await check
  assert.equal(pending.size(), 0)
  assert.equal(moves.telemetry.reserved('target'), 0)
  await assert.rejects(moves.manualMove({}), /stopped/)
  await moves.planAndExecute()
  assert.equal(pending.size(), 0)
})

test('move stop aborts a planning cycle and never starts its next plan', async () => {
  const { moves } = moveFixture()
  let calls = 0
  moves.isRoomStable = () => true
  moves.levelIsolationPlanner.plan = () => []
  moves.planner.calibrate = () => {}
  moves.planner.plan = () => [1, 2].map(n => ({ sourceRoomId: 'source', targetRoomId: 'target', playerIds: ['p' + n], gain: 1, reason: 'player_rebalance' }))
  moves.executor.execute = ({ signal, moveId }) => {
    calls++
    return new Promise(resolve => signal.addEventListener('abort', () => resolve({ ok: false, moveId, reason: 'failed', error: new Error('aborted') }), { once: true }))
  }
  const cycle = moves.planAndExecute(undefined, Date.now())
  await waitForSend()
  await moves.stop()
  await cycle
  assert.equal(calls, 1)
})
