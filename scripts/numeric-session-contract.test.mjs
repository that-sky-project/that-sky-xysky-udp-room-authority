import test from 'node:test'
import assert from 'node:assert/strict'
import { once } from 'node:events'
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { WebSocket } from 'ws'
import { NodeEventSchema } from '../dist/types/contracts.js'
import { loadConfig } from '../dist/config/index.js'
import { createManagerRuntime } from '../dist/manager-host/ManagerRuntime.js'
import { ManagerGateway } from '../dist/manager-host/ManagerGateway.js'
import { ConnectionManager } from '../../that-sky-xysky-udp-team/src/connection/ConnectionManager.js'
import { ConnectionState } from '../../that-sky-xysky-udp-team/src/connection/states.js'
import { PlayerStore } from '../../that-sky-xysky-udp-team/src/domain/PlayerStore.js'
import { QwdTelemetry } from '../../that-sky-xysky-udp-team/src/qwd/QwdTelemetry.js'
import { createPlayerLifecycle } from '../../that-sky-xysky-udp-team/src/runtime/playerLifecycle.js'

const logger = { info(){}, warn(){}, error(){}, debug(){}, trace(){}, child(){ return this } }
const envelope = data => ({ event: 'room.telemetry', data: { nodeId: 'n', roomId: 'r', ...data } })

test('numeric session IDs normalize at every incoming identity position without broad coercion', () => {
  const parsed = NodeEventSchema.parse(envelope({ sessionId: 3, player: { playerId: 'p', sessionId: 2 }, players: [{ playerId: 'p', sessionId: 1 }] }))
  assert.equal(parsed.data.sessionId, '3')
  assert.equal(parsed.data.player.sessionId, '2')
  assert.equal(parsed.data.players[0].sessionId, '1')
  for (const sessionId of [null, true, false, {}, [], 0, -1, 1.5, Infinity, Number.MAX_SAFE_INTEGER + 1, '']) {
    assert.equal(NodeEventSchema.safeParse(envelope({ players: [{ playerId: 'p', sessionId }] })).success, false, String(sessionId))
  }
  assert.equal(NodeEventSchema.parse(envelope({ players: [{ playerId: 'p', sessionId: 'session-p' }] })).data.players[0].sessionId, 'session-p')
})

async function until(predicate) {
  for (let attempt = 0; attempt < 200; attempt++) {
    if (await predicate()) return
    await new Promise(resolve => setTimeout(resolve, 10))
  }
  assert.fail('numeric-session event did not reach manager state')
}

test('real numeric node join/full/delta/leave traverse WS validation and populate debug player data', async () => {
  const dir = mkdtempSync(path.join(tmpdir(), 'qwd-numeric-session-'))
  const configFile = path.join(dir, 'config.yml')
  writeFileSync(configFile, '{}')
  const config = { ...loadConfig(configFile), host: '127.0.0.1', port: 0 }
  rmSync(dir, { recursive: true })
  const runtime = createManagerRuntime(config, logger)
  const gateway = new ManagerGateway(config, runtime, logger)
  let socket
  try {
    await gateway.start()
    const port = gateway.server.address().port
    socket = new WebSocket(`ws://127.0.0.1:${port}/?node=n&v=1`)
    const errors = []
    socket.on('message', message => {
      const reply = JSON.parse(message.toString())
      if (reply.cmd) socket.send(JSON.stringify({ id: reply.id, code: 0 }))
      else if (reply.code) errors.push(reply)
    })
    await once(socket, 'open')
    const publish = (event, data) => socket.send(JSON.stringify({ v: 1, event, data }))
    publish('room.created', { roomId: 'r', udpHost: '127.0.0.1', port: 41000, players: 0, capacity: 8, version: 0 })
    await until(() => runtime.rooms.getRoom('r'))

    const players = new PlayerStore()
    const sessions = new ConnectionManager({ roomId: 'r' })
    const session = sessions.add(1)
    session.transition(ConnectionState.PENDING)
    const player = players.add({ session, uuid: { toString: () => 'actual-node-player' }, levelId: 3526133726, levelChangeCount: 1 })
    session.attachPlayer(player)
    session.transition(ConnectionState.ACTIVE)
    assert.equal(typeof session.id, 'number', 'fixture uses the actual numeric ConnectionSession implementation')
    const lifecycle = createPlayerLifecycle({ identity: { id: 'r' }, publish, sessions, players,
      reservations: { consume(){} }, broadcaster: { broadcast(){} }, transport: { disconnect(){} } })
    lifecycle.joined(player)
    let now = Date.now()
    const telemetry = new QwdTelemetry({ players, now: () => now,
      publishTelemetry: (roster, seq, framing) => publish('room.telemetry', { roomId: 'r', players: roster, seq,
        physicalPlayers: players.size, activePlayers: players.listActive().length, capacity: 8, reservedSlots: 0, ...framing }) })
    telemetry.publish()
    await until(() => runtime.moves.telemetry.get('actual-node-player'))
    assert.equal(runtime.moves.telemetry.get('actual-node-player').sessionId, String(session.id))
    assert.equal(runtime.rooms.getPlayerLocation('actual-node-player').sessionId, String(session.id))
    const response = await fetch(`http://127.0.0.1:${port}/debug/allnode`)
    const debug = await response.json()
    const room = debug.data.nodes.flatMap(node => node.rooms).find(room => room.roomId === 'r')
    assert.equal(room.playerCount, 1)
    assert.equal(room.players[0].telemetry.levelId, 3526133726)

    player.levelId = 1638008359
    player.lvSeq = 2
    now += 1000
    telemetry.publish()
    await until(() => runtime.moves.telemetry.get('actual-node-player')?.levelId === 1638008359)
    lifecycle.notifyLeft(player)
    await until(() => !runtime.moves.telemetry.get('actual-node-player'))
    assert.deepEqual(errors, [])
  } finally {
    socket?.terminate()
    await gateway.stop()
  }
})
