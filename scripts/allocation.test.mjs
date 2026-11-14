import assert from 'node:assert/strict'
import test from 'node:test'
import { setTimeout as delay } from 'node:timers/promises'
import { RequestRouter } from '../dist/manager-host/RequestRouter.js'
import { ReservationTracker } from '../dist/manager-host/ReservationTracker.js'
import { PolicyEngine } from '../dist/manager-host/PolicyEngine.js'
import { ErrorCode } from '../dist/types/errors.js'

const room = (id, players = 5) => ({ roomId: id, nodeId: id, udpHost: '127.0.0.1', udpPort: 3000, players, capacity: 8, version: 1, draining: false })
function fixture (reserve, options = {}) {
  const candidates = [room('a'), room('b')]
  const rooms = { getPlayerLocation: () => undefined, getRoom: id => candidates.find(r => r.roomId === id), listCandidateRooms: () => [...candidates], levelsByRoom: () => new Map(), listRooms: () => candidates, upsertRoom: next => Object.assign(candidates.find(r => r.roomId === next.roomId), next) }
  const nodes = { listHealthy: () => candidates.map(r => ({ nodeId: r.nodeId, connected: true, draining: false, load: 0 })) }
  const reservations = new ReservationTracker({ ttlMs: 30000 })
  const policy = new PolicyEngine({ maxPlayersPerRoom: 8, staleNodePenalty: 30, loadPenaltyWeight: 10, sameRegionBonus: 12, preferredFillTarget: 6, sameLevelBonus: 20, wrongLevelPenalty: 80, unknownLevelPenalty: 50, protectedLevels: [] })
  const router = new RequestRouter(rooms, nodes, policy, reservations, { reserveRoom: reserve }, { nodeStaleMs: 10000, assignmentTtlMs: 30000, candidateRoomLimit: 256, preferredFillTarget: 6, requestTimeoutMs: 10000, retryBaseMs: 2000, retryMaxMs: 30000, maxAttempts: 8, ...options })
  return { router, reservations, candidates }
}
const ok = expiresAt => ({ code: 0, data: { expiresAt, capacity: 8, reserved: 1, version: 1 } })

for (const code of [ErrorCode.NO_CAPACITY, ErrorCode.STALE_PLAN]) {
  test(`allocation ACK ${code} tries the next target in the same request and backs off`, async t => {
    const calls = []
    const { router, reservations } = fixture(async r => { calls.push(r.roomId); return r.roomId === 'a' ? { code } : ok(Date.now() + 30000) })
    t.after(() => reservations.stop())
    assert.equal((await router.allocate({ playerId: 'p' })).data.roomId, 'b')
    assert.equal((await router.allocate({ playerId: 'q' })).data.roomId, 'b')
    assert.deepEqual(calls, ['a', 'b', 'b'])
  })
}
test('same UUID singleflight waits for the actual ACK and reuses its real expiry', async t => {
  let resolveAck
  let calls = 0
  const expiresAt = Date.now() + 18000
  const { router, reservations } = fixture(() => { calls++; return new Promise(resolve => { resolveAck = resolve }) })
  t.after(() => reservations.stop())
  const first = router.allocate({ playerId: 'p' })
  let secondDone = false
  const second = router.allocate({ playerId: 'p' }).then(reply => { secondDone = true; return reply })
  await delay(5)
  assert.equal(secondDone, false, 'an acquiring shadow is not an assignment')
  assert.equal(calls, 1)
  resolveAck(ok(expiresAt))
  const replies = await Promise.all([first, second])
  assert.equal(replies[0].data.expiresAt, expiresAt)
  assert.deepEqual(replies[0], replies[1])
  await delay(5)
  assert.equal((await router.allocate({ playerId: 'p' })).data.expiresAt, expiresAt)
  assert.equal(calls, 1)
})
test('one singleflight caller cancelling does not abort the other waiter', async t => {
  let resolveAck
  let sharedSignal
  const { router, reservations } = fixture((_r, _p, signal) => { sharedSignal = signal; return new Promise(resolve => { resolveAck = resolve }) })
  t.after(() => reservations.stop())
  const controller = new AbortController()
  const first = router.allocate({ playerId: 'p' }, controller.signal)
  const second = router.allocate({ playerId: 'p' })
  controller.abort()
  await first
  assert.equal(sharedSignal.aborted, false)
  resolveAck(ok(Date.now() + 30000))
  assert.equal((await second).code, 0)
})
test('total allocation deadline bounds an unresponsive transport', async t => {
  const { router, reservations } = fixture(() => new Promise(() => {}), { requestTimeoutMs: 25 })
  t.after(() => reservations.stop())
  const started = Date.now()
  const result = await router.allocate({ playerId: 'p' })
  assert.notEqual(result.code, 0)
  assert.ok(Date.now() - started < 500)
  assert.equal(reservations.size(), 0)
})
test('BAD_REQUEST ACK is permanent and missing or expired expiry never succeeds', async t => {
  let calls = 0
  const a = fixture(async () => { calls++; return { code: ErrorCode.BAD_REQUEST } })
  t.after(() => a.reservations.stop())
  assert.equal((await a.router.allocate({ playerId: 'p' })).code, ErrorCode.BAD_REQUEST)
  assert.equal(calls, 1)
  for (const data of [{}, { expiresAt: Date.now() - 1 }]) {
    const b = fixture(async () => ({ code: 0, data }))
    t.after(() => b.reservations.stop())
    assert.notEqual((await b.router.allocate({ playerId: 'p' })).code, 0)
    assert.equal(b.reservations.size(), 0)
  }
})
test('stopped reservation tracker never reacquires and acquiring hold cannot refresh its TTL', () => {
  const tracker = new ReservationTracker({ ttlMs: 30000 })
  assert.equal(tracker.tryAcquire(room('a'), 'p'), true)
  assert.equal(tracker.tryAcquire(room('b'), 'p'), false)
  assert.equal(tracker.roomFor('p'), 'a')
  tracker.stop()
  tracker.stop()
  assert.equal(tracker.size(), 0)
  assert.equal(tracker.tryAcquire(room('a'), 'p'), false)
})
