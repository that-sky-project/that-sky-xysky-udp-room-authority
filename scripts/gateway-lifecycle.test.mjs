import assert from 'node:assert/strict'
import { once } from 'node:events'
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { createServer, connect } from 'node:net'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { Writable } from 'node:stream'
import { setTimeout as delay } from 'node:timers/promises'
import test from 'node:test'
import pino from 'pino'
import { WebSocket } from 'ws'
import { loadConfig } from '../dist/config/index.js'
import { ManagerGateway } from '../dist/manager-host/ManagerGateway.js'
import { createManagerRuntime } from '../dist/manager-host/ManagerRuntime.js'

function fixture () {
  const directory = mkdtempSync(path.join(tmpdir(), 'qwd-gateway-test-'))
  const file = path.join(directory, 'config.yml')
  writeFileSync(file, 'host: 127.0.0.1\nlogging:\n  pretty: false\n')
  const config = { ...loadConfig(file), port: 0 }
  rmSync(directory, { recursive: true })
  const logs = []
  const logger = pino({ level: 'trace' }, new Writable({
    write (chunk, encoding, callback) {
      logs.push(JSON.parse(chunk.toString()))
      callback()
    }
  }))
  const runtime = createManagerRuntime(config, logger)
  const gateway = new ManagerGateway(config, runtime, logger)
  return { gateway, runtime, config, logs }
}

function addressOf (gateway) {
  return `127.0.0.1:${gateway.server.address().port}`
}

async function eventually (predicate) {
  for (let attempt = 0; attempt < 100; attempt++) {
    if (predicate()) return
    await delay(10)
  }
  assert.fail('condition did not become true')
}

test('gateway drains real WebSockets, rejects pending work, and stop is idempotent', async t => {
  const { gateway, runtime } = fixture()
  t.after(() => gateway.stop())
  await gateway.start()
  const address = addressOf(gateway)
  const ws = new WebSocket(`ws://${address}/?v=1&node=gateway-test`)
  t.after(() => ws.terminate())
  await once(ws, 'open')
  const closed = once(ws, 'close')
  const pending = runtime.pending.send({ cmd: 'room.reserve' }, async () => {}, undefined, 'gateway-test')
  const rejected = assert.rejects(pending, /shutting down/)
  const stopping = gateway.stop()
  assert.strictEqual(gateway.stop(), stopping)
  await Promise.all([stopping, rejected])
  const [code] = await closed
  assert.equal(code, 1001)
  assert.equal(runtime.pending.size(), 0)
  assert.equal(runtime.reservations.size(), 0)
  assert.equal(gateway.tasks.size, 0)
  assert.equal(gateway.connections.size, 0)
  await assert.rejects(fetch(`http://${address}/healthz`))
  await assert.rejects(gateway.start(), /stopping/)
})

test('shutdown force-closes an unfinished HTTP body and settles its handler', { timeout: 9000 }, async t => {
  const { gateway, logs } = fixture()
  t.after(() => gateway.stop())
  await gateway.start()
  const port = gateway.server.address().port
  const socket = connect({ host: '127.0.0.1', port })
  socket.on('error', () => {})
  t.after(() => socket.destroy())
  await once(socket, 'connect')
  socket.write('POST /allocate HTTP/1.1\r\nHost: localhost\r\nContent-Length: 500\r\n\r\n{')
  await eventually(() => gateway.tasks.size === 1)
  await gateway.stop()
  assert.equal(gateway.tasks.size, 0)
  assert.equal(gateway.connections.size, 0)
  assert.ok(logs.some(log => log.msg === 'forcing remaining manager connections closed'))
})

test('failed listen owns resources and can still close cleanly', async t => {
  const occupied = createServer()
  occupied.listen(0, '127.0.0.1')
  await once(occupied, 'listening')
  t.after(() => new Promise(resolve => occupied.close(resolve)))
  const { gateway, config } = fixture()
  config.port = occupied.address().port
  await assert.rejects(gateway.start(), error => error.code === 'EADDRINUSE')
  await gateway.stop()
  assert.equal(gateway.server, undefined)
  assert.equal(gateway.nodeServer, undefined)
})

test('unexpected HTTP and node processing failures include stack and task context', async t => {
  const { gateway, runtime, logs } = fixture()
  t.after(() => gateway.stop())
  await gateway.start()
  const address = addressOf(gateway)
  runtime.moves.activitySnapshot = () => { throw new Error('activity failure') }
  const response = await fetch(`http://${address}/activity`)
  assert.equal(response.status, 500)
  assert.ok(logs.some(log => log.msg === 'manager HTTP request failed' && log.err.stack.includes('activity failure')))
  const ws = new WebSocket(`ws://${address}/?v=1&node=unexpected-test`)
  t.after(() => ws.terminate())
  await once(ws, 'open')
  runtime.moves.ingest = () => { throw new Error('ingest failure') }
  const reply = once(ws, 'message')
  ws.send(JSON.stringify({ v: 1, event: 'heartbeat', data: {} }))
  await reply
  assert.ok(logs.some(log => log.msg === 'node event processing failed' && log.err.stack.includes('ingest failure')))
  assert.ok(!logs.some(log => log.msg === 'invalid node message' && log.err?.message === 'ingest failure'))
})
