import assert from 'node:assert/strict'
import { once } from 'node:events'
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import path from 'node:path'
import test from 'node:test'
import { setTimeout as delay } from 'node:timers/promises'
import { WebSocket } from 'ws'
import { loadConfig } from '../dist/config/index.js'
import { ManagerGateway } from '../dist/manager-host/ManagerGateway.js'
import { createManagerRuntime } from '../dist/manager-host/ManagerRuntime.js'
import { NodeRegistry } from '../dist/manager-host/NodeRegistry.js'
import { PolicyEngine } from '../dist/manager-host/PolicyEngine.js'
import { RequestRouter } from '../dist/manager-host/RequestRouter.js'
import { ReservationTracker } from '../dist/manager-host/ReservationTracker.js'
import { RoomCache } from '../dist/manager-host/RoomCache.js'
import { NodeEventSchema } from '../dist/types/contracts.js'
import { ErrorCode } from '../dist/types/errors.js'

const logger = { info () {}, warn () {}, error () {}, debug () {}, trace () {}, child () { return this } }
const options = { nodeStaleMs: 10000, assignmentTtlMs: 30000, candidateRoomLimit: 256, preferredFillTarget: 6, requestTimeoutMs: 1000 }
const room = (roomId = 'a', udpHost = 'udp.example.test') => ({ roomId, nodeId: roomId, udpHost, udpPort: 41000, players: 0, capacity: 8, version: 1, draining: false })
const ack = () => ({ code: 0, data: { expiresAt: Date.now() + 30000 } })

function fixture (t, resolveIpv4, candidates = [room()], overrides = {}) {
  const rooms = new RoomCache({ maxRooms: 100, staleRoomMs: 120000 })
  const nodes = new NodeRegistry({ circuitOpenMs: 5000, failureThreshold: 5, backpressureLimitBytes: 1024 * 1024 })
  for (const candidate of candidates) {
    rooms.upsertRoom(candidate)
    nodes.applyEvent({ event: 'room.created', data: { nodeId: candidate.nodeId, udpHost: candidate.udpHost, capacity: 8 } })
  }
  const reservations = new ReservationTracker({ ttlMs: 30000 })
  const policy = new PolicyEngine({ maxPlayersPerRoom: 8, staleNodePenalty: 30, loadPenaltyWeight: 10, sameRegionBonus: 12, preferredFillTarget: 6, sameLevelBonus: 20, wrongLevelPenalty: 80, unknownLevelPenalty: 50, protectedLevels: [] })
  const reserved = []
  const router = new RequestRouter(rooms, nodes, policy, reservations, {
    reserveRoom: async candidate => { reserved.push(candidate.roomId); return ack() }
  }, { ...options, resolveIpv4, ...overrides })
  t.after(() => { router.stop(); reservations.stop() })
  return { rooms, nodes, router, reservations, reserved }
}

async function until (predicate) {
  for (let attempt = 0; attempt < 100; attempt++) {
    if (predicate()) return
    await delay(10)
  }
  assert.fail('等待的状态未出现')
}

test('注册时自行派生域名标记，更新地址时重算，普通状态更新保留标记', t => {
  const { rooms, nodes } = fixture(t, async () => assert.fail('注册时不应解析 DNS'))
  const publish = (event, udpHost, version) => {
    const parsed = NodeEventSchema.parse({ v: 1, event, data: { nodeId: 'a', roomId: 'a', udpHost, port: 41000, capacity: 8, version, isDomainNode: false } })
    nodes.applyEvent(parsed)
    rooms.applyNodeEvent(parsed)
  }
  publish('room.created', 'udp.example.test', 2)
  assert.equal(nodes.getHealth('a').isDomainNode, true)
  assert.equal(rooms.getRoom('a').isDomainNode, true)
  rooms.upsertRoom({ ...rooms.getRoom('a'), isDomainNode: false, players: 1 })
  nodes.applyEvent({ event: 'heartbeat', data: { nodeId: 'a', isDomainNode: false } })
  assert.equal(nodes.getHealth('a').isDomainNode, true)
  assert.equal(rooms.getRoom('a').isDomainNode, true)
  publish('room.updated', '192.0.2.10', 3)
  assert.equal(nodes.getHealth('a').isDomainNode, false)
  assert.equal(rooms.getRoom('a').isDomainNode, false)
  publish('room.updated', 'new.example.test', 4)
  assert.equal(nodes.getHealth('a').isDomainNode, true)
  assert.equal(rooms.getRoom('a').isDomainNode, true)
  assert.equal(rooms.upsertRoom({ ...rooms.getRoom('a'), udpHost: '192.0.2.20', version: 3 }), false)
  assert.equal(rooms.getRoom('a').isDomainNode, true)
  rooms.upsertRoom(room('ipv6', '2001:db8::1'))
  assert.equal(rooms.getRoom('ipv6').isDomainNode, false, 'IPv6 字面量不能误判为域名')
})

test('IPv4 公告地址原样返回，端口不变且不查询 DNS', async t => {
  const { router, reserved } = fixture(t, async () => assert.fail('IPv4 不应查询 DNS'), [room('a', '192.0.2.10')])
  const reply = await router.allocate({ playerId: 'p' })
  assert.equal(reply.code, 0)
  assert.equal(reply.data.udpHost, '192.0.2.10')
  assert.equal(reply.data.udpPort, 41000)
  assert.deepEqual(reserved, ['a'])
})

test('域名选中后才解析 A 记录，各次分配现场解析且不覆盖公告域名', async t => {
  const queried = []
  const { rooms, router } = fixture(t, async hostname => {
    queried.push(hostname)
    return queried.length === 1 ? ['192.0.2.10', '192.0.2.11'] : ['192.0.2.20']
  })
  assert.deepEqual(queried, [])
  const first = await router.allocate({ playerId: 'p' })
  const second = await router.allocate({ playerId: 'q' })
  assert.equal(first.code, 0)
  assert.equal(second.code, 0)
  assert.equal(first.data.udpHost, '192.0.2.10')
  assert.equal(second.data.udpHost, '192.0.2.20')
  assert.equal(first.data.udpPort, 41000)
  assert.equal(second.data.udpPort, 41000)
  assert.deepEqual(queried, ['udp.example.test', 'udp.example.test'])
  assert.equal(rooms.getRoom('a').udpHost, 'udp.example.test')
  assert.equal(rooms.getRoom('a').isDomainNode, true)
})

test('已有玩家位置的返回分支同样现场解析，且不新建预留', async t => {
  let queries = 0
  const { rooms, router, reserved } = fixture(t, async () => [`192.0.2.${++queries}`])
  rooms.trackPlayer('p', 'a', 'a')
  assert.equal((await router.allocate({ playerId: 'p' })).data.udpHost, '192.0.2.1')
  assert.equal((await router.allocate({ playerId: 'p' })).data.udpHost, '192.0.2.2')
  assert.deepEqual(reserved, [])
})

test('复用已确认预留时重新解析 DNS，但不刷新节点租约或重复申请', async t => {
  let queries = 0
  const { router, reserved } = fixture(t, async () => [`192.0.2.${++queries}`])
  const first = await router.allocate({ playerId: 'p' })
  const second = await router.allocate({ playerId: 'p' })
  assert.equal(first.data.udpHost, '192.0.2.1')
  assert.equal(second.data.udpHost, '192.0.2.2')
  assert.equal(second.data.expiresAt, first.data.expiresAt)
  assert.deepEqual(reserved, ['a'])
})

test('DNS 失败跳过候选并退避，不为该候选申请远端预留', async t => {
  let queries = 0
  const { router, rooms, reservations, reserved } = fixture(t, async () => {
    queries++
    throw Object.assign(new Error('DNS 查询失败'), { code: 'ENOTFOUND' })
  }, [room(), room('b', '192.0.2.20')])
  for (const playerId of ['p', 'q']) {
    const reply = await router.allocate({ playerId })
    assert.equal(reply.code, 0)
    assert.equal(reply.data.roomId, 'b')
    assert.equal(reply.data.udpHost, '192.0.2.20')
  }
  assert.equal(queries, 1)
  assert.equal(reservations.count('a'), 0)
  assert.deepEqual(reserved, ['b', 'b'])
  assert.equal(rooms.getRoom('a').udpHost, 'udp.example.test')
})

for (const addresses of [[], ['2001:db8::1'], ['not-an-ip']]) {
  test(`没有有效 IPv4 A 记录时不返回域名或成功分配：${JSON.stringify(addresses)}`, async t => {
    const { router, reservations, reserved } = fixture(t, async () => addresses)
    const reply = await router.allocate({ playerId: 'p' })
    assert.equal(reply.code, ErrorCode.NO_CAPACITY)
    assert.equal(reservations.size(), 0)
    assert.deepEqual(reserved, [])
  })
}

test('已有位置或确认预留解析失败时返回节点不可用，不返回原域名', async t => {
  for (const branch of ['known', 'held']) {
    const { rooms, router, reservations, reserved } = fixture(t, async () => { throw new Error('DNS 查询失败') })
    if (branch === 'known') rooms.trackPlayer('p', 'a', 'a')
    else {
      reservations.tryAcquire(rooms.getRoom('a'), 'p')
      reservations.acknowledge('p', 'a', Date.now() + 30000)
    }
    const reply = await router.allocate({ playerId: 'p' })
    assert.equal(reply.code, ErrorCode.NODE_UNAVAILABLE)
    assert.equal(reply.data.details.hostname, 'udp.example.test')
    assert.deepEqual(reserved, [])
  }
})

test('分配总超时约束悬挂的 DNS，晚到的解析结果不会继续申请房间', async t => {
  let finishDns
  const { router, reservations, reserved } = fixture(t, () => new Promise(resolve => { finishDns = resolve }), [room()], { requestTimeoutMs: 25 })
  const reply = await router.allocate({ playerId: 'p' })
  assert.equal(reply.code, ErrorCode.NODE_TIMEOUT)
  assert.equal(reservations.size(), 0)
  finishDns(['192.0.2.10'])
  await delay(5)
  assert.deepEqual(reserved, [])
})

test('并发同玩家请求共享一次 DNS，单个调用取消不打断另一调用', async t => {
  let finishDns
  let queries = 0
  const { router, reservations, reserved } = fixture(t, () => {
    queries++
    return new Promise(resolve => { finishDns = resolve })
  })
  const controller = new AbortController()
  const first = router.allocate({ playerId: 'p' }, controller.signal)
  const second = router.allocate({ playerId: 'p' })
  controller.abort()
  assert.notEqual((await first).code, 0)
  assert.equal(queries, 1)
  assert.equal(reservations.size(), 1)
  finishDns(['192.0.2.10'])
  const reply = await second
  assert.equal(reply.code, 0)
  assert.equal(reply.data.udpHost, '192.0.2.10')
  assert.deepEqual(reserved, ['a'])
})

test('停止路由器可中止待完成的 DNS 并清理影子预留', async t => {
  const { router, reservations, reserved } = fixture(t, () => new Promise(() => {}))
  const pending = router.allocate({ playerId: 'p' })
  router.stop()
  assert.equal((await pending).code, ErrorCode.NODE_UNAVAILABLE)
  assert.equal(reservations.size(), 0)
  assert.deepEqual(reserved, [])
})

test('复用租约在 DNS 查询期间过期时重新分配，不返回过期 expiresAt', async t => {
  let finishDns
  let queries = 0
  const { router, rooms, reservations, reserved } = fixture(t, () => {
    if (++queries === 1) return new Promise(resolve => { finishDns = resolve })
    return Promise.resolve(['192.0.2.20'])
  })
  reservations.tryAcquire(rooms.getRoom('a'), 'p')
  reservations.acknowledge('p', 'a', Date.now() + 30000)
  const held = reservations.confirmedHoldFor('p')
  const pending = router.allocate({ playerId: 'p' })
  held.expiresAt = Date.now() - 1
  finishDns(['192.0.2.10'])
  const reply = await pending
  assert.equal(reply.code, 0)
  assert.equal(reply.data.udpHost, '192.0.2.20')
  assert.ok(reply.data.expiresAt > Date.now())
  assert.equal(queries, 2)
  assert.deepEqual(reserved, ['a'])
})

test('真实 WS 房间公告标记域名节点，HTTP /allocate 返回解析后的 IPv4 和原端口', async t => {
  const directory = mkdtempSync(path.join(tmpdir(), 'qwd-dns-test-'))
  const configFile = path.join(directory, 'config.yml')
  writeFileSync(configFile, '{}')
  const config = { ...loadConfig(configFile), host: '127.0.0.1', port: 0 }
  rmSync(directory, { recursive: true })
  const runtime = createManagerRuntime(config, logger)
  runtime.router.stop()
  let queries = 0
  runtime.router = new RequestRouter(runtime.rooms, runtime.nodes, runtime.policy, runtime.reservations, runtime.coordinator, {
    ...options,
    resolveIpv4: async hostname => {
      assert.equal(hostname, 'udp.example.test')
      return [`192.0.2.${++queries}`]
    }
  })
  const gateway = new ManagerGateway(config, runtime, logger)
  t.after(() => gateway.stop())
  await gateway.start()
  const address = `127.0.0.1:${gateway.server.address().port}`
  const socket = new WebSocket(`ws://${address}/?v=1&node=dns-node`)
  t.after(() => socket.terminate())
  const commands = []
  socket.on('message', message => {
    const command = JSON.parse(message.toString())
    commands.push(command)
    if (command.cmd) socket.send(JSON.stringify({ v: 1, id: command.id, ...ack() }))
  })
  await once(socket, 'open')
  const publish = (event, data) => socket.send(JSON.stringify({ v: 1, event, data }))
  publish('room.created', { roomId: 'dns-room', udpHost: 'udp.example.test', port: 41000, capacity: 8, players: 0, version: 1, isDomainNode: false })
  await until(() => runtime.rooms.getRoom('dns-room'))
  assert.equal(queries, 0)
  assert.equal(runtime.nodes.getHealth('dns-node').isDomainNode, true)
  const allocate = async playerId => {
    const response = await fetch(`http://${address}/allocate`, {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ playerId })
    })
    assert.equal(response.status, 200)
    return response.json()
  }
  const first = await allocate('p')
  const second = await allocate('p')
  assert.equal(first.code, 0)
  assert.equal(first.data.udpHost, '192.0.2.1')
  assert.equal(second.data.udpHost, '192.0.2.2')
  assert.equal(second.data.expiresAt, first.data.expiresAt)
  assert.equal(second.data.udpPort, 41000)
  assert.deepEqual(commands.map(command => command.cmd), ['room.reserve'])
  assert.equal(runtime.rooms.getRoom('dns-room').udpHost, 'udp.example.test')
  const debug = await (await fetch(`http://${address}/debug/allnode`)).json()
  assert.equal(debug.data.nodes[0].isDomainNode, true)
  assert.equal(debug.data.nodes[0].rooms[0].isDomainNode, true)
  publish('room.updated', { roomId: 'dns-room', udpHost: '192.0.2.99', port: 42000, capacity: 8, version: 2 })
  await until(() => runtime.rooms.getRoom('dns-room').udpHost === '192.0.2.99')
  assert.equal(runtime.nodes.getHealth('dns-node').isDomainNode, false)
  assert.equal(runtime.rooms.getRoom('dns-room').isDomainNode, false)
  const third = await allocate('p')
  assert.equal(third.data.udpHost, '192.0.2.99')
  assert.equal(third.data.udpPort, 42000)
  assert.equal(queries, 2)
})
