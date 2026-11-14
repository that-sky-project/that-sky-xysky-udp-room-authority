import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { request } from 'node:http';
import { once } from 'node:events';
import { WebSocket } from 'ws';
import * as contracts from '../dist/types/contracts.js';
import { loadConfig } from '../dist/config/index.js';
import { createManagerRuntime } from '../dist/manager-host/ManagerRuntime.js';
import { ManagerGateway } from '../dist/manager-host/ManagerGateway.js';
import { CrossCoordinator } from '../dist/manager-host/CrossCoordinator.js';
import { ErrorCode } from '../dist/types/errors.js';

const removedEvents = [
  'seat.occupied', 'seat.released', 'audience.reaction', 'spotlight.changed',
  'performance.request', 'performance.publish', 'performance.sync',
  'liveBroadcast.sync', 'liveBroadcast.publish'
];
const removedCommands = [
  'seat.assign', 'seat.release', 'audience.snapshot', 'audience.reaction',
  'audience.chatGlobal', 'level.audience', 'performance.state',
  'performance.decision', 'performance.deliver', 'performance.gap',
  'liveBroadcast.capability', 'liveBroadcast.deliver'
];
const coreEvents = [
  'player.join', 'player.leave', 'room.created', 'room.updated', 'room.destroyed',
  'heartbeat', 'node.ready', 'node.draining', 'room.telemetry',
  'move.result', 'move.completed', 'reservation.expired'
];
const coreCommands = [
  'player.redirect', 'room.destroy', 'node.drain', 'room.reserve',
  'move.prepare', 'move.commit', 'move.cancel'
];
const logger = {
  info() {}, warn() {}, error() {}, debug() {}, trace() {}, fatal() {},
  child() { return this; }
};
const directory = mkdtempSync(join(tmpdir(), 'qwd-core-contracts-'));
let defaults;
try {
  const file = join(directory, 'config.yml');
  writeFileSync(file, '{}');
  defaults = loadConfig(file);
} finally {
  rmSync(directory, { recursive: true, force: true });
}
const config = { ...defaults, host: '127.0.0.1', port: 0, debugApiToken: 'regression-secret' };
const shutdownRuntime = runtime => {
  runtime.pending.close?.();
  runtime.reservations.stop?.();
  runtime.moves.stop?.();
};
const event = (name, data = {}) => ({ event: name, data: { nodeId: 'n', ...data } });
const httpJson = (port, path, method = 'GET', body) => new Promise((resolve, reject) => {
  const req = request({ host: '127.0.0.1', port, path, method, headers: {
    connection: 'close', ...(body === undefined ? {} : { 'content-type': 'application/json' })
  } }, res => {
    const chunks = [];
    res.on('data', chunk => chunks.push(chunk));
    res.on('error', reject);
    res.on('end', () => {
      try { resolve({ status: res.statusCode, data: JSON.parse(Buffer.concat(chunks).toString('utf8')) }); }
      catch (error) { reject(error); }
    });
  });
  req.on('error', reject);
  req.setTimeout(3000, () => req.destroy(new Error('HTTP test timed out')));
  req.end(body);
});
const waitUntil = async (predicate, message) => {
  const deadline = Date.now() + 3000;
  while (!predicate()) {
    assert.ok(Date.now() < deadline, message);
    await new Promise(resolve => setTimeout(resolve, 10));
  }
};

test('removed schemas/events/commands have no active contracts; core names remain', () => {
  for (const name of Object.keys(contracts)) {
    assert.doesNotMatch(name, /Audience|Performance|LiveBroadcast|ReserveRequest|PlayerUuid/);
  }
  for (const name of removedEvents) assert.equal(contracts.NodeEventSchema.safeParse(event(name)).success, false, name);
  for (const name of removedCommands) assert.equal(contracts.ManagerCommandSchema.safeParse({ id: 1, cmd: name }).success, false, name);
  for (const name of coreEvents) assert.equal(contracts.NodeEventSchema.safeParse(event(name)).success, true, name);
  for (const name of coreCommands) assert.equal(contracts.ManagerCommandSchema.safeParse({ id: 1, cmd: name }).success, true, name);
  for (const name of ['seatPlayer', 'seatRelease', 'audienceSnapshot', 'levelAudience', 'chatGlobal', 'audienceReaction', 'liveBroadcast', 'performance']) {
    assert.equal(name in CrossCoordinator.prototype, false, name);
  }
});

test('ordinary eight-player room schema, metadata, ACKs and telemetry remain intact', () => {
  const snapshot = { roomId: 'r', nodeId: 'n', udpHost: '127.0.0.1', udpPort: 41000, players: 8 };
  assert.equal(contracts.RoomSnapshotSchema.parse(snapshot).capacity, 8);
  assert.equal(contracts.RoomSnapshotSchema.safeParse({ ...snapshot, players: 9 }).success, false);
  assert.equal(contracts.RoomSnapshotSchema.safeParse({ ...snapshot, capacity: 9 }).success, false);
  const player = { playerId: 'p', level: 42, metadata: { future: { ordinary: true } } };
  assert.deepEqual(contracts.PlayerSchema.parse(player), player);
  const parsed = contracts.NodeEventSchema.parse(event('room.telemetry', {
    roomId: 'r', seq: 7, kind: 'delta', baseSeq: 6, removedPlayers: ['left'],
    benchId: 3, seatId: 2, entityId: 27, reaction: { type: 1 },
    requestId: 'retired', payloadBase64: 'YQ==', futureMetadata: { ordinary: true },
    players: [{ playerId: 'p', levelId: 42, benchId: 3, seatId: 2,
      position: [1, 2, 3], velocity: 0.5, stateAt: 1000, affinity: { friend: 1 }, lastMoveAt: 0,
      futureMetadata: { ordinary: true } }]
  }));
  for (const field of ['benchId', 'seatId', 'entityId', 'reaction', 'requestId', 'payloadBase64']) assert.equal(field in parsed.data, false, field);
  for (const field of ['benchId', 'seatId']) assert.equal(field in parsed.data.players[0], false, field);
  assert.deepEqual(parsed.data.futureMetadata, { ordinary: true });
  assert.deepEqual(parsed.data.players[0].futureMetadata, { ordinary: true });
  assert.deepEqual(parsed.data.players[0].position, [1, 2, 3]);
  assert.equal(parsed.data.seq, 7);
  assert.equal(parsed.data.baseSeq, 6);
  assert.deepEqual(parsed.data.removedPlayers, ['left']);
  assert.equal(contracts.NodeEventSchema.safeParse(event('room.telemetry', { players: Array.from({ length: 9 }, (_, i) => ({ playerId: String(i) })) })).success, false);
  for (const timestamp of [-1, 1.5, Number.MAX_SAFE_INTEGER + 1]) assert.equal(contracts.NodeEventSchema.safeParse(event('heartbeat', { timestamp })).success, false);
  const ack = { id: 1, code: 0, data: { ordinary: true } };
  assert.deepEqual(contracts.NodeAckSchema.parse(ack).data, ack.data);
  assert.deepEqual(contracts.BatchRoomLookupSchema.parse({ playerIds: ['p', 'unknown'] }).playerIds, ['p', 'unknown']);
});

test('core allocation still uses node-bound room.reserve and eight-player capacity holds', async () => {
  for (const level of [42, 263580627, 1559613961]) {
    const runtime = createManagerRuntime(config, logger);
    try {
      for (const property of ['seating', 'performance', 'audienceReactions', 'liveBroadcasts', 'audienceChatGlobal']) assert.equal(property in runtime, false, property);
      const sent = [];
      runtime.nodes.register({ nodeId: 'n', protocolVersion: 1, connectedAt: Date.now(), remoteAddress: '127.0.0.1', writable: true,
        async send(command) {
          sent.push(command);
          const data = { expiresAt: Date.now() + config.assignmentTtlMs, physicalPlayers: 7, activePlayers: 7,
            reservedSlots: 1, capacity: 8, version: 9 };
          assert.equal(runtime.pending.resolve({ id: command.id, code: 0, data }, 'wrong-node'), false);
          assert.equal(runtime.pending.resolve({ id: command.id, code: 0, data }, 'n'), true);
        }, close() {} });
      runtime.nodes.applyEvent(event('node.ready', { capacity: 1 }));
      runtime.rooms.upsertRoom(contracts.RoomSnapshotSchema.parse({ roomId: 'r', nodeId: 'n', udpHost: '127.0.0.1', udpPort: 41000, players: 7, version: 9 }));
      const first = await runtime.router.allocate({ playerId: 'p', level });
      assert.equal(first.code, ErrorCode.OK);
      assert.equal(first.data.roomId, 'r');
      assert.equal(sent.length, 1);
      assert.equal(sent[0].cmd, 'room.reserve');
      assert.deepEqual(sent[0].data, { roomId: 'r', playerId: 'p', expectedVersion: 9, ttlMs: config.assignmentTtlMs });
      assert.equal(runtime.reservations.roomFor('p'), 'r');
      const repeated = await runtime.router.allocate({ playerId: 'p', level });
      assert.equal(repeated.code, ErrorCode.OK);
      assert.equal(sent.length, 1, 'retry reuses the existing hold');
      assert.equal((await runtime.router.allocate({ playerId: 'second', level })).code, ErrorCode.NO_CAPACITY);
      runtime.reservations.confirm('p');
      assert.equal(runtime.reservations.roomFor('p'), undefined);
      assert.equal((await runtime.router.allocate({ playerId: 'second', level })).code, ErrorCode.OK);
      assert.equal(sent.length, 2);
      assert.equal(runtime.pending.size(), 0);
    } finally { shutdownRuntime(runtime); }
  }
});

test('removed HTTP routes return ordinary 404, with no authentication or JSON handling', async () => {
  const runtime = createManagerRuntime(config, logger);
  const gateway = new ManagerGateway(config, runtime, logger);
  await gateway.start();
  try {
    const port = gateway.server.address().port;
    for (const path of ['/reserve', '/audience/chat-global']) {
      for (const method of ['GET', 'POST']) {
        const reply = await httpJson(port, path, method, method === 'POST' ? '{invalid' : undefined);
        assert.equal(reply.status, 404, `${method} ${path}`);
        assert.deepEqual(reply.data, { code: ErrorCode.NOT_FOUND, data: { message: 'route not found' } });
      }
    }
    assert.equal((await httpJson(port, '/healthz')).status, 200);
    const allocated = await httpJson(port, config.allocatePath, 'POST', JSON.stringify({ playerId: 'p', level: 42 }));
    assert.equal(allocated.status, 200);
    assert.equal(allocated.data.code, ErrorCode.NO_CAPACITY);
  } finally { await gateway.stop(); shutdownRuntime(runtime); }
});

test('node socket rejects retired events without fanout, while ownership and telemetry guards survive', async () => {
  const runtime = createManagerRuntime(config, logger);
  const gateway = new ManagerGateway(config, runtime, logger);
  await gateway.start();
  let socket;
  try {
    const port = gateway.server.address().port;
    socket = new WebSocket(`ws://127.0.0.1:${port}/?node=n&v=1`);
    await once(socket, 'open');
    const replies = [];
    socket.on('message', message => replies.push(JSON.parse(message.toString())));
    const send = (name, data) => socket.send(JSON.stringify(event(name, data)));
    send('room.created', { roomId: 'r', udpHost: '127.0.0.1', port: 41000, players: 0, version: 1 });
    await waitUntil(() => runtime.rooms.getRoom('r'), 'room.created not applied');
    const telemetry = (seq, fields = {}) => contracts.NodeEventSchema.parse(event('room.telemetry', {
      roomId: 'r', seq, kind: 'full', players: [{ playerId: 'p', levelId: 42, position: [1, 2, 3], stateAt: Date.now() }], ...fields
    }));
    assert.equal(gateway.applyNodeEvent(telemetry(1)), true);
    assert.equal(gateway.applyNodeEvent(telemetry(1)), false, 'duplicate sequence is rejected');
    assert.equal(gateway.applyNodeEvent(telemetry(2, { nodeId: 'intruder' })), false, 'foreign owner is rejected');
    assert.equal(gateway.applyNodeEvent(telemetry(2)), true);
    assert.equal(runtime.rooms.getPlayerLocation('p').nodeId, 'n');
    assert.equal(runtime.moves.telemetry.get('p').levelId, 42);
    assert.equal(gateway.applyNodeEvent(telemetry(3, { kind: 'delta', baseSeq: 999 })), false);
    assert.equal(runtime.moves.hasTelemetryGap('r'), true, 'delta base gap guard is preserved');
    assert.equal(gateway.applyNodeEvent(telemetry(4)), true);
    assert.equal(runtime.moves.hasTelemetryGap('r'), false, 'full snapshot repairs the gap');
    for (const name of removedEvents) {
      const count = replies.length;
      send(name, { roomId: 'r', player: { playerId: 'p' } });
      await waitUntil(() => replies.length > count, `${name} was not rejected`);
      assert.ok(replies.at(-1).code !== ErrorCode.OK, name);
      assert.equal(replies.at(-1).cmd, undefined, 'retired event cannot cause a manager command');
    }
    const lookup = await httpJson(port, '/players/p/room');
    assert.equal(lookup.status, 200);
    assert.equal(lookup.data.data.levelId, 42);
    assert.equal(lookup.data.data.roomId, 'r');
  } finally {
    if (socket && socket.readyState !== WebSocket.CLOSED) {
      const closed = once(socket, 'close');
      socket.close();
      await closed;
    }
    await gateway.stop(); shutdownRuntime(runtime);
  }
});
