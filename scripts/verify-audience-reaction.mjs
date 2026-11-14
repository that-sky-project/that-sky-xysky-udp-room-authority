import assert from 'node:assert/strict';
import { AudienceReactionService } from '../dist/manager-host/AudienceReactionService.js';
import { PerformanceService } from '../dist/manager-host/PerformanceService.js';
import { RoomCache } from '../dist/manager-host/RoomCache.js';
import { defaultSeatedLevel } from '../dist/manager-host/SeatAllocator.js';
const LEVEL = defaultSeatedLevel();
const OTHER = 1559613961; // hall venue (无需座位) — see qwd config audience.hall_levels
import { NodeEventSchema } from '../dist/types/contracts.js';
const drain = async () => { for (let n = 0; n < 15; n++) await new Promise(resolve => setImmediate(resolve)); };
const ack = data => ({ code: 0, data: { authorityEpoch: data.authorityEpoch, eventId: data.eventId, eventSeq: data.eventSeq, applied: true } });
function fixture(options = {}) {
  let now = 1_000_000;
  const rooms = new RoomCache({ maxRooms: 100, staleRoomMs: 60000 });
  const seats = new Set(), sent = [], debug = [];
  const performance = new PerformanceService(rooms, { seatFor: (id, level) => seats.has(`${id}:${level}`) ? { levelId: level, benchId: 0, seatId: 0 } : undefined }, {
    async performance(room, cmd, data) { return { code: 0, data: { authorityEpoch: data.authorityEpoch, revision: data.revision, applied: true } }; }
  }, { durationMs: 300000, clock: () => now });
  let sender = async (room, data) => ack(data);
  const service = new AudienceReactionService(rooms, performance, {
    async audienceReaction(room, data, signal) { sent.push({ roomId: room.roomId, data: structuredClone(data), signal }); return sender(room, data, signal); }
  }, { clock: () => now, ...options }, { debug: (...args) => debug.push(args) });
  const add = (id, roomId, levelId = LEVEL, trusted = true) => {
    const existing = rooms.getRoom(roomId);
    rooms.upsertRoom(existing ?? { roomId, nodeId: `n-${roomId}`, udpHost: '127.0.0.1', udpPort: 42000 + rooms.listRooms().length, version: 1, players: 1, capacity: 8, draining: false });
    rooms.trackPlayer(id, `n-${roomId}`, roomId, levelId); seats.add(`${id}:${levelId}`);
    if (trusted) performance.notePlayerSeen(id, roomId, levelId);
  };
  const event = (eventId, reaction = { flag7: 1, type: 3 }, fields = {}) => ({ event: 'audience.reaction', data: { roomId: 'a', nodeId: 'n-a', player: { playerId: 'alice' }, levelId: LEVEL, eventId, reaction, ...fields } });
  const close = () => { service.stop(); performance.stop(); };
  return { rooms, seats, performance, service, sent, debug, add, event, close, advance: n => { now += n; }, now: () => now, sender: fn => { sender = fn; } };
}
const cases = [];
const test = (name, fn) => cases.push([name, fn]);
test('ordinary nonperformer fanout uses live level, excludes source and late subscribers', async () => {
  const f = fixture(); f.add('alice', 'a'); f.add('bob', 'b'); f.add('carol', 'c', OTHER);
  assert.equal(f.performance.state(LEVEL).current, null);
  assert.equal(f.service.handle(f.event('first')), true);
  f.add('dave', 'd'); await drain();
  assert.deepEqual(f.sent.map(x => x.roomId), ['b']);
  assert.deepEqual(f.sent[0].data.reaction, { flag7: 1, type: 3 });
  assert.equal(f.sent[0].data.authorityEpoch, f.performance.authorityEpoch);
  assert.equal(f.service.stats().delivered, 1); f.close();
});
test('source room/node/live registration/level/seat checks reject forged sources', async () => {
  const f = fixture(); f.add('alice', 'a'); f.add('bob', 'b'); f.add('http-only', 'a', LEVEL, false);
  for (const fields of [{ nodeId: 'n-b' }, { roomId: 'b' }, { levelId: OTHER }, { player: { playerId: 'http-only' } }]) assert.equal(f.service.handle(f.event(`bad-${JSON.stringify(fields)}`, undefined, fields)), false);
  f.seats.delete(`alice:${LEVEL}`); assert.equal(f.service.handle(f.event('no-seat')), false);
  f.seats.add(`alice:${LEVEL}`); f.performance.notePlayerLeft('alice', 'a'); assert.equal(f.service.handle(f.event('left')), false);
  await drain(); assert.equal(f.sent.length, 0); f.close();
});
test('same value different IDs survive; exact duplicate is not resent; conflicts rejected', async () => {
  const f = fixture(); f.add('alice', 'a'); f.add('bob', 'b');
  assert.equal(f.service.handle(f.event('one')), true); assert.equal(f.service.handle(f.event('one')), true);
  assert.equal(f.service.handle(f.event('one', { flag7: 2, type: 3 })), false);
  assert.equal(f.service.handle(f.event('two')), true); await drain();
  assert.deepEqual(f.sent.map(x => x.data.eventId), ['one', 'two']); assert.deepEqual(f.sent.map(x => x.data.eventSeq), [1, 2]);
  assert.equal(f.service.handle(f.event('one')), true); await drain(); assert.equal(f.sent.length, 2);
  assert.equal(f.service.stats().duplicate, 2); f.close();
});
test('strict ACK identity/applied checks retry within fixed TTL with fresh serverNow', async () => {
  const f = fixture(); f.add('alice', 'a'); f.add('bob', 'b');
  const replies = [{ code: 4004, data: { applied: false } }, { code: 0, data: { applied: false } }, { code: 0, data: { applied: true, authorityEpoch: 'wrong', eventId: 'retry', eventSeq: 1 } }];
  f.sender(async () => replies.shift());
  f.service.handle(f.event('retry')); await drain();
  for (let n = 0; n < 2; n++) { f.advance(100); f.service.tick(); await drain(); }
  assert.equal(f.service.stats().delivered, 0); assert.equal(f.service.stats().pending, 1);
  f.sender(async (room, data) => ack(data)); f.advance(100); f.service.tick(); await drain();
  assert.equal(f.service.stats().delivered, 1); assert.equal(f.service.stats().ackFailed, 3);
  assert.ok(f.sent.every(x => x.data.expiresAtMs === 1002000));
  assert.deepEqual(f.sent.map(x => x.data.serverNowMs), [1000000, 1000100, 1000200, 1000300]); f.close();
});
test('exact deadline aborts stale in-flight delivery, no reconnect or old-effect replay', async () => {
  // Preserve the explicit 1s boundary independently of the normal-frame default TTL.
  const f = fixture({ ttlMs: 1000 }); f.add('alice', 'a'); f.add('bob', 'b');
  let release;
  f.sender((room, data, signal) => new Promise(resolve => { release = () => resolve(ack(data)); }));
  f.service.handle(f.event('expires')); await drain(); const signal = f.sent[0].signal;
  f.advance(1000); f.service.tick(); assert.equal(signal.aborted, true); assert.equal(f.service.stats().expired, 1);
  release(); await drain(); f.service.forgetNode('n-b'); f.service.tick(); await drain(); assert.equal(f.sent.length, 1); assert.equal(f.service.stats().pending, 0); f.close();
});
test('source or target map changes invalidate queued pulses; connection generations drop backlog', async () => {
  for (const who of ['source', 'target', 'disconnect', 'reconnect', 'endpoint']) {
    const f = fixture(); f.add('alice', 'a'); f.add('bob', 'b'); f.sender(async () => ({ code: 4004 }));
    f.service.handle(f.event('old')); await drain();
    if (who === 'source') { f.rooms.trackPlayer('alice', 'n-a', 'a', OTHER); f.performance.notePlayerSeen('alice', 'a', OTHER); }
    if (who === 'target') { f.rooms.trackPlayer('bob', 'n-b', 'b', OTHER); f.performance.notePlayerSeen('bob', 'b', OTHER); }
    if (who === 'disconnect') { f.rooms.markNodeSuspected('n-b'); f.service.forgetNode('n-b'); }
    if (who === 'reconnect') f.service.forgetNode('n-a');
    if (who === 'endpoint') f.rooms.upsertRoom({ ...f.rooms.getRoom('b'), udpPort: 49999 });
    f.advance(100); f.sender(async (room, data) => ack(data)); f.service.tick(); await drain();
    assert.equal(f.sent.length, 1, who); assert.equal(f.service.stats().pending, 0, who); f.close();
  }
});
test('per-room/global capacity and dedup caches have bounded lifetimes', async () => {
  for (const options of [{ perRoomLimit: 2 }, { globalLimit: 2 }]) {
    const f = fixture(options); f.add('alice', 'a'); f.add('bob', 'b');
    assert.equal(f.service.handle(f.event('one')), true); assert.equal(f.service.handle(f.event('two')), true);
    assert.equal(f.service.handle(f.event('three')), false); assert.equal(f.service.stats().capacity, 1);
    f.close(); await drain();
  }
  const f = fixture({ ttlMs: 1000, dedupLimit: 1, dedupTtlMs: 1000 }); f.add('alice', 'a');
  assert.equal(f.service.handle(f.event('one')), true); assert.equal(f.service.handle(f.event('two')), false);
  f.advance(1000); f.service.tick(); assert.equal(f.service.handle(f.event('two')), true); assert.equal(f.service.stats().accepted, 2); f.close();
});
test('per-source sequence survives TTL pruning and reconnect; source/level scopes are independent', async () => {
  const f = fixture({ ttlMs: 1000, dedupTtlMs: 1000 }); f.add('alice', 'a'); f.add('bob', 'b');
  f.service.handle(f.event('one')); await drain(); f.advance(1100); f.service.tick(); f.service.forgetNode('n-a');
  f.service.handle(f.event('two')); await drain();
  f.service.handle(f.event('bob', undefined, { player: { playerId: 'bob' }, nodeId: 'n-b', roomId: 'b' })); await drain();
  f.add('carol', 'c', OTHER); f.add('alice', 'a', OTHER);
  f.service.handle(f.event('other-level', undefined, { levelId: OTHER })); await drain();
  assert.deepEqual(f.sent.map(x => [x.data.playerId, x.data.levelId, x.data.eventSeq]), [['alice', LEVEL, 1], ['alice', LEVEL, 2], ['bob', LEVEL, 1], ['alice', OTHER, 1]]); f.close();
});
test('bounded concurrency and per-room order; stop cancels in-flight and future work', async () => {
  const f = fixture({ concurrency: 2 }); f.add('alice', 'a'); f.add('bob', 'b'); f.add('carol', 'c'); f.add('dave', 'd');
  const pending = [];
  f.sender((room, data, signal) => new Promise(resolve => pending.push({ resolve: () => resolve(ack(data)), signal })));
  f.service.handle(f.event('one')); f.service.handle(f.event('two')); await drain(); assert.equal(f.sent.length, 2);
  assert.ok(f.sent.every(x => x.data.eventSeq === 1));
  f.service.stop(); assert.ok(pending.every(x => x.signal.aborted)); for (const p of pending) p.resolve(); await drain();
  assert.equal(f.service.handle(f.event('after-stop')), false); f.service.tick(); await drain(); assert.equal(f.sent.length, 2); assert.equal(f.service.stats().pending, 0); f.performance.stop();
});
test('schema ordinary reaction range and no-op/shout semantics', async () => {
  const f = fixture(); f.add('alice', 'a'); f.add('bob', 'b');
  for (const reaction of [{ flag7: 8, type: 1 }, { flag7: 1, type: 14 }, { flag7: 1, type: 15 }, { flag7: 1, type: 3, extraVal: 1 }, { flag7: -1, type: 0 }, { flag7: 1, type: 1.5 }]) {
    assert.equal(NodeEventSchema.safeParse(f.event('bad', reaction)).success, false); assert.equal(f.service.handle(f.event('bad', reaction)), false);
  }
  assert.equal(f.service.handle(f.event('zero', { flag7: 0, type: 0 })), true);
  assert.equal(f.service.handle(f.event('shout', { flag7: 4, type: 0 })), true); await drain();
  assert.equal(f.sent.length, 1); assert.deepEqual(f.sent[0].data.reaction, { flag7: 4, type: 0 }); assert.equal(f.sent[0].data.eventSeq, 1); f.close();
});
for (const [name, body] of cases) { await body(); console.log(`ok - ${name}`); }
console.log(`All ${cases.length} audience reaction checks passed.`);
