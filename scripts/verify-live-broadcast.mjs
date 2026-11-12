import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, rmSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { LiveBroadcastService } from '../dist/manager-host/LiveBroadcastService.js';
import { PerformanceService } from '../dist/manager-host/PerformanceService.js';
import { RoomCache } from '../dist/manager-host/RoomCache.js';
import { NodeEventSchema, ManagerCommandSchema } from '../dist/types/contracts.js';
import { loadConfig } from '../dist/config/index.js';
const LEVEL = 263580627, OTHER = 1559613961;
const UUID = 'abcdef01-0000-4000-8000-000000000001';
const BOB = 'abcdef02-0000-4000-8000-000000000002';
const drain = async () => { for (let n = 0; n < 20; n++) await new Promise(resolve => setImmediate(resolve)); };
const ack = (cmd, data) => ({ code: 0, data: cmd.endsWith('capability')
  ? { authorityEpoch: data.authorityEpoch, capabilityId: data.capabilityId, revision: data.revision, applied: true }
  : { authorityEpoch: data.authorityEpoch, streamId: data.streamId, eventId: data.eventId, eventSeq: data.eventSeq, applied: true } });
function fixture(options = {}) {
  let now = 1_000_000;
  const rooms = new RoomCache({ maxRooms: 100, staleRoomMs: 60000 });
  const seats = new Set(), sent = [];
  const performance = new PerformanceService(rooms, { seatFor: (id, level) => seats.has(`${id}:${level}`) ? { levelId: level, benchId: 0, seatId: 0 } : undefined }, {
    async performance(room, cmd, data) { return { code: 0, data: { authorityEpoch: data.authorityEpoch, revision: data.revision, applied: true } }; }
  }, { durationMs: 300000, clock: () => now });
  let sender = async (room, cmd, data) => ack(cmd, data);
  const service = new LiveBroadcastService(rooms, performance, {
    async liveBroadcast(room, cmd, data, signal) { sent.push({ roomId: room.roomId, cmd, data: structuredClone(data), signal }); return sender(room, cmd, data, signal); }
  }, { allowedPlayerUuids: [UUID.toUpperCase(), UUID], clock: () => now, ...options });
  const add = (id, roomId, level = LEVEL, trusted = true) => {
    rooms.upsertRoom(rooms.getRoom(roomId) ?? { roomId, nodeId: `n-${roomId}`, udpHost: '127.0.0.1', udpPort: 42000 + rooms.listRooms().length, version: 1, players: 1, capacity: 8, draining: false });
    rooms.trackPlayer(id, `n-${roomId}`, roomId, level); seats.add(`${id}:${level}`);
    if (trusted) performance.notePlayerSeen(id, roomId, level);
  };
  const cap = (roomId = 'a', level = LEVEL) => sent.filter(s => s.roomId === roomId && s.cmd.endsWith('capability') && s.data.levelId === level).at(-1)?.data;
  const event = (eventId, fields = {}) => ({ event: 'liveBroadcast.publish', data: {
    nodeId: 'n-a', roomId: 'a', player: { playerId: UUID }, levelId: LEVEL, capabilityId: cap()?.capabilityId,
    sourceStreamId: 'source-1', eventId, timestamp: now / 1000, actionId: 0, payloadBase64: 'e30=', ...fields
  } });
  const start = async () => { add(UUID, 'a'); add(BOB, 'b'); service.tick(); await drain(); };
  const close = () => { service.stop(); performance.stop(); };
  return { rooms, performance, service, seats, sent, add, cap, event, start, close, sender: fn => { sender = fn; }, advance: n => { now += n; }, deliveries: () => sent.filter(s => s.cmd.endsWith('deliver')) };
}
const cases = [];
const test = (name, fn) => cases.push([name, fn]);
test('UUID configuration defaults, canonical lowercase dedupe, and strict validation', () => {
  const dir = mkdtempSync(join(tmpdir(), 'qwd-live-config-'));
  try {
    const file = join(dir, 'config.yml'); writeFileSync(file, '{}');
    assert.deepEqual(loadConfig(file).liveBroadcastAllowedPlayerUuids, []); assert.equal(loadConfig(file).liveBroadcastMaxStreamsPerLevel, 7);
    writeFileSync(file, `liveBroadcast:\n  allowedPlayerUuids: [${UUID.toUpperCase()}, ${UUID}]`);
    assert.deepEqual(loadConfig(file).liveBroadcastAllowedPlayerUuids, [UUID]);
    assert.deepEqual(loadConfig(file).liveBroadcastPlayerNames, {});
    writeFileSync(file, `liveBroadcast:\n  allowedPlayerUuids: ["Thexiaoyu:${UUID.toUpperCase()}", "${UUID}", "Thexiaoyu:${UUID}"]`);
    const named = loadConfig(file);
    assert.deepEqual(named.liveBroadcastAllowedPlayerUuids, [UUID]);
    assert.deepEqual(named.liveBroadcastPlayerNames, { [UUID]: 'Thexiaoyu' });
    const f = fixture({ allowedPlayerUuids: named.liveBroadcastAllowedPlayerUuids, playerNames: named.liveBroadcastPlayerNames });
    assert.equal(f.service.displayName(UUID.toUpperCase()), 'Thexiaoyu');
    assert.equal(f.service.displayName(BOB), undefined); f.close();
    for (const body of [`liveBroadcast: {allowedPlayerUuids: [":${UUID}"]}`, `liveBroadcast: {allowedPlayerUuids: ["Thexiaoyu:not-a-uuid"]}`, `liveBroadcast: {allowedPlayerUuids: ["One:${UUID}", "Two:${UUID.toUpperCase()}"]}`, `liveBroadcast: {allowedPlayerUuids: ["${'x'.repeat(65)}:${UUID}"]}`, 'liveBroadcast: {allowedPlayerUuids: [not-a-uuid]}', 'liveBroadcast: {maxStreamsPerLevel: 8}', 'liveBroadcast: {eventTtlMs: 9999}']) {
      writeFileSync(file, body); assert.throws(() => loadConfig(file));
    }
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
test('all audience rooms receive capabilities, empty local upload list enables receivers, same-room delivery included', async () => {
  const f = fixture(); await f.start();
  assert.deepEqual(f.cap().allowedPlayerIds, [UUID]); assert.deepEqual(f.cap('b').allowedPlayerIds, []);
  assert.equal(f.cap().authorityEpoch, f.performance.authorityEpoch);
  assert.equal(f.service.handle(f.event('one')), true); await drain();
  assert.deepEqual(f.deliveries().map(s => s.roomId).sort(), ['a', 'b']);
  for (const d of f.deliveries()) { assert.equal(d.data.capabilityId, f.cap(d.roomId).capabilityId); assert.equal(d.data.timestamp, 1000); assert.equal(d.data.expiresAtMs, 1003000); }
  assert.equal(new Set(f.deliveries().map(s => s.data.streamId)).size, 1); f.close();
});
test('capability application ACK gates content and retries negative, buffered and identity mismatch', async () => {
  const f = fixture(); f.sender(async (room, cmd, data) => cmd.endsWith('capability') ? { code: 0, data: { ...ack(cmd, data).data, buffered: true } } : ack(cmd, data));
  await f.start(); assert.equal(f.service.handle(f.event('gated')), true); await drain(); assert.equal(f.deliveries().length, 0);
  for (const fail of [{ code: 4004 }, { code: 0, data: { applied: false } }, null]) {
    f.sender(async (room, cmd, data) => fail ?? { code: 0, data: { ...ack(cmd, data).data, revision: -1 } });
    f.advance(100); f.service.tick(); await drain(); assert.equal(f.deliveries().length, 0);
  }
  f.sender(async (room, cmd, data) => ack(cmd, data)); f.advance(100); f.service.tick(); await drain();
  assert.equal(f.service.stats().delivered, 2); assert.equal(f.service.stats().pending, 0); f.close();
});
test('content ACK must be applied with exact epoch stream event sequence; FIFO retries preserve fixed TTL', async () => {
  const f = fixture(); await f.start();
  let faults = [{ applied: false }, { buffered: true }, { authorityEpoch: 'wrong' }, { streamId: 'wrong' }, { eventId: 'wrong' }, { eventSeq: 999 }];
  let fault = faults.shift();
  f.sender(async (room, cmd, data) => cmd.endsWith('deliver') ? { code: 0, data: { ...ack(cmd, data).data, ...fault } } : ack(cmd, data));
  f.service.handle(f.event('one')); f.service.handle(f.event('two')); await drain();
  for (fault of faults) { f.advance(100); f.service.tick(); await drain(); assert.equal(f.service.stats().delivered, 0); }
  f.sender(async (room, cmd, data) => ack(cmd, data)); f.advance(100); f.service.tick(); await drain();
  assert.equal(f.service.stats().delivered, 4);
  const b = f.deliveries().filter(s => s.roomId === 'b'); assert.equal(b.at(-1).data.eventId, 'two');
  assert.ok(b.filter(s => s.data.eventId === 'one').every(s => s.data.expiresAtMs === 1003000));
  assert.ok(b.at(-2).data.serverNowMs > b[0].data.serverNowMs); f.close();
});
test('capability refresh stable IDs monotonic revisions and exact expiration; reconnect rotates ID without replay', async () => {
  const f = fixture(); await f.start(); const first = f.cap();
  f.advance(2000); f.service.tick(); await drain(); const next = f.cap();
  assert.equal(next.capabilityId, first.capabilityId); assert.ok(next.revision > first.revision); assert.equal(next.expiresAtMs, 1007000);
  f.sender(async (room, cmd, data) => cmd.endsWith('deliver') ? { code: 0 } : ack(cmd, data)); f.service.handle(f.event('old')); await drain();
  f.service.reconnectNode('n-a'); await drain(); assert.notEqual(f.cap().capabilityId, next.capabilityId); assert.ok(f.cap().revision > next.revision);
  assert.equal(f.service.stats().pending, 0); assert.equal(f.service.handle(f.event('forged', { capabilityId: next.capabilityId })), false); f.close();
  const g = fixture(); await g.start(); const expired = g.cap(); g.advance(5000);
  assert.equal(g.service.handle(g.event('expired', { capabilityId: expired.capabilityId })), false);
  g.service.tick(); await drain(); assert.equal(g.cap().capabilityId, expired.capabilityId);
  assert.equal(g.service.handle(g.event('renewed')), true); g.close();
});
test('deadline drops and aborts in-flight events, stop aborts capability/content', async () => {
  const f = fixture(); await f.start(); const releases = [];
  f.sender((room, cmd, data) => cmd.endsWith('deliver') ? new Promise(resolve => releases.push(() => resolve(ack(cmd, data)))) : Promise.resolve(ack(cmd, data)));
  f.service.handle(f.event('expiry')); await drain(); const pending = f.deliveries(); assert.equal(pending.length, 2);
  f.advance(3000); f.service.tick(); assert.ok(pending.every(s => s.signal.aborted)); assert.equal(f.service.stats().expired, 2);
  releases.forEach(fn => fn()); await drain(); assert.equal(f.service.stats().delivered, 0); f.close();
  const g = fixture(); g.sender(() => new Promise(() => {})); await g.start(); g.service.stop(); assert.ok(g.sent.every(s => s.signal.aborted)); assert.equal(g.service.handle(g.event('stop')), false); g.performance.stop();
});
test('dedup uses fixed hash fingerprints, conflict rejects; new event IDs remain independent', async () => {
  const f = fixture(); await f.start(); const one = f.event('one');
  assert.equal(f.service.handle(one), true); assert.equal(f.service.handle(one), true);
  assert.equal(f.service.handle(f.event('one', { payloadBase64: 'W10=' })), false);
  assert.equal(f.service.handle(f.event('two')), true); await drain();
  assert.deepEqual(f.deliveries().filter(s => s.roomId === 'b').map(s => s.data.eventSeq), [1, 2]); f.close();
});
test('restarts allocate new streams, discard old pending and reject retired IDs for 30 seconds', async () => {
  const f = fixture(); await f.start(); f.sender(async (room, cmd, data) => cmd.endsWith('deliver') ? { code: 1 } : ack(cmd, data));
  f.service.handle(f.event('one')); await drain(); const old = f.deliveries()[0].data.streamId;
  assert.equal(f.service.handle(f.event('two', { sourceStreamId: 'source-2' })), true); f.advance(100);
  f.sender(async (room, cmd, data) => ack(cmd, data)); f.service.tick(); await drain();
  const latest = f.deliveries().at(-1).data; assert.notEqual(latest.streamId, old); assert.equal(latest.eventSeq, 1);
  assert.equal(f.service.handle(f.event('retired')), false); assert.equal(f.service.stats().pending, 0);
  f.advance(5000); f.service.tick(); await drain(); assert.equal(f.service.handle(f.event('idle-old', { sourceStreamId: 'source-2' })), false);
  assert.equal(f.service.handle(f.event('idle-new', { sourceStreamId: 'source-3' })), true); await drain(); assert.equal(f.deliveries().at(-1).data.eventSeq, 1); f.close();
});
test('bounded retirement cache reserves slots, fails closed and recovers after tombstone TTL', async () => {
  const f = fixture({ dedupLimit: 2 }); await f.start(); f.service.handle(f.event('one')); await drain();
  f.advance(5000); f.service.tick(); await drain(); assert.equal(f.service.handle(f.event('two', { sourceStreamId: 'source-2' })), true); await drain();
  f.advance(5000); f.service.tick(); await drain(); assert.equal(f.service.handle(f.event('old')), false);
  assert.equal(f.service.handle(f.event('three', { sourceStreamId: 'source-3' })), false);
  f.advance(31000); f.service.tick(); await drain(); assert.equal(f.service.handle(f.event('four', { sourceStreamId: 'source-3' })), true); f.close();
});
test('allowlist seat live-roster owner level and formal role checked before publish and delivery', async () => {
  for (const kind of ['seat', 'roster', 'owner', 'level', 'allowlist', 'formal']) {
    const f = fixture(); await f.start();
    if (kind === 'seat') f.seats.delete(`${UUID}:${LEVEL}`);
    if (kind === 'roster') f.performance.notePlayerLeft(UUID, 'a');
    if (kind === 'owner') f.rooms.upsertRoom({ ...f.rooms.getRoom('a'), nodeId: 'different' });
    if (kind === 'level') { f.rooms.trackPlayer(UUID, 'n-a', 'a', OTHER); f.performance.notePlayerSeen(UUID, 'a', OTHER); }
    if (kind === 'formal') f.performance.currentPerformer = () => UUID;
    assert.equal(f.service.handle(f.event(`bad-${kind}`, kind === 'allowlist' ? { player: { playerId: BOB }, roomId: 'b', nodeId: 'n-b', capabilityId: f.cap('b').capabilityId } : {})), false, kind); f.close();
  }
  const f = fixture(); await f.start(); f.sender(async (room, cmd, data) => cmd.endsWith('deliver') ? { code: 1 } : ack(cmd, data));
  f.service.handle(f.event('before-role')); await drain(); const count = f.deliveries().length;
  const beforeRole = f.cap();
  f.performance.currentPerformer = () => UUID; f.advance(2000); f.service.tick(); await drain();
  assert.equal(f.deliveries().length, count); assert.equal(f.service.stats().pending, 0);
  assert.ok(f.cap().allowedPlayerIds.includes(UUID)); assert.equal(f.cap().capabilityId, beforeRole.capabilityId);
  assert.equal(f.service.handle(f.event('formal-live-rejected')), false);
  f.performance.currentPerformer = () => undefined; f.service.tick(); await drain(); assert.ok(f.cap().allowedPlayerIds.includes(UUID)); f.close();
});
test('target generation changes and late joins cannot replay queued history', async () => {
  for (const kind of ['endpoint', 'disconnect', 'reclaim', 'target-level']) {
    const f = fixture(); await f.start(); f.sender(async (room, cmd, data) => cmd.endsWith('deliver') ? { code: 1 } : ack(cmd, data));
    f.service.handle(f.event('old')); await drain(); f.add('abcdef03-0000-4000-8000-000000000003', 'c');
    if (kind === 'endpoint') f.rooms.upsertRoom({ ...f.rooms.getRoom('b'), udpPort: 49999 });
    if (kind === 'disconnect') f.service.forgetNode('n-b');
    if (kind === 'reclaim') f.service.forgetRoom('b');
    if (kind === 'target-level') { f.rooms.trackPlayer(BOB, 'n-b', 'b', OTHER); f.performance.notePlayerSeen(BOB, 'b', OTHER); }
    f.advance(100); f.sender(async (room, cmd, data) => ack(cmd, data)); f.service.tick(); await drain();
    assert.equal(f.deliveries().filter(s => s.roomId === 'c').length, 0, kind);
    assert.equal(f.deliveries().filter(s => s.roomId === 'b').length, 1, kind); f.close();
  }
});
test('FIFO per-room/global queue and stream policy limits are bounded', async () => {
  for (const options of [{ perRoomLimit: 1 }, { globalLimit: 2 }]) {
    const f = fixture(options); await f.start(); assert.equal(f.service.handle(f.event('one')), true);
    assert.equal(f.service.handle(f.event('two')), false); assert.equal(f.service.stats().pending, 2); f.close();
  }
  const f = fixture({ allowedPlayerUuids: [UUID, BOB], maxStreamsPerLevel: 1 }); await f.start();
  assert.equal(f.service.handle(f.event('one')), true);
  assert.equal(f.service.handle(f.event('bob', { roomId: 'b', nodeId: 'n-b', player: { playerId: BOB }, capabilityId: f.cap('b').capabilityId })), false); f.close();
});
test('strict live schema canonical 48KiB payload boundaries without changing performance schema', async () => {
  const f = fixture(); await f.start();
  assert.equal(NodeEventSchema.safeParse(f.event('good')).success, true);
  for (const fields of [{ payloadBase64: 'Zh==' }, { payloadBase64: '' }, { payloadBase64: Buffer.alloc(48 * 1024 + 1).toString('base64') }, { timestamp: -1 }, { timestamp: Infinity }, { actionId: -1 }, { actionId: 2 ** 32 }, { levelId: 2 ** 32 }, { sourceStreamId: '' }, { player: { playerId: 'notuuid' } }]) {
    assert.equal(NodeEventSchema.safeParse(f.event('bad', fields)).success, false, JSON.stringify(fields).slice(0, 100));
  }
  assert.equal(NodeEventSchema.safeParse(f.event('max', { payloadBase64: Buffer.alloc(48 * 1024).toString('base64') })).success, true);
  assert.equal(f.service.handle(f.event('past', { timestamp: 980 })), false); assert.equal(f.service.handle(f.event('future', { timestamp: 1020 })), false);
  assert.equal(NodeEventSchema.safeParse({ event: 'performance.publish', data: { roomId: 'a', player: { playerId: 'alice' }, levelId: LEVEL, sessionId: 'session', eventId: 'event', timestamp: 1, actionId: 0, payloadBase64: Buffer.alloc(64 * 1024).toString('base64') } }).success, true);
  for (const cmd of ['liveBroadcast.capability', 'liveBroadcast.deliver']) assert.equal(ManagerCommandSchema.safeParse({ id: 1, cmd, data: {} }).success, true);
  f.close();
});
test('sync validates owner, refreshes no content history; ordinary payload contains no invented stop or piano clear', async () => {
  const f = fixture(); await f.start();
  assert.equal(f.service.handle({ event: 'liveBroadcast.sync', data: { nodeId: 'wrong', roomId: 'a', levelIds: [LEVEL] } }), false);
  assert.equal(f.service.handle({ event: 'liveBroadcast.sync', data: { nodeId: 'n-a', roomId: 'a', levelIds: [LEVEL] } }), true); await drain();
  f.service.handle(f.event('ordinary')); await drain(); const data = f.deliveries()[0].data;
  assert.equal('stop' in data, false); assert.equal('clear' in data, false); assert.equal('prefix' in data, false);
  const source = readFileSync(new URL('../src/manager-host/LiveBroadcastService.ts', import.meta.url), 'utf8'); assert.equal(source.includes('10145'), false); f.close();
});
for (const [name, body] of cases) { await body(); console.log(`ok - ${name}`); }
console.log(`All ${cases.length} live broadcast checks passed.`);
