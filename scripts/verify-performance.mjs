import assert from 'node:assert/strict';
import { PerformanceService } from '../dist/manager-host/PerformanceService.js';
import { RoomCache } from '../dist/manager-host/RoomCache.js';
import { NodeEventSchema, PERFORMANCE_MAX_PAYLOAD_BYTES } from '../dist/types/contracts.js';
import { defaultSeatedLevel } from '../dist/manager-host/SeatAllocator.js';
const LEVEL = defaultSeatedLevel();
const HALL = 1559613961; // hall venue (无需座位) — see qwd config audience.hall_levels
const ackFor = ({ cmd, data }) => ({ code: 0, data: cmd === 'performance.state'
  ? { authorityEpoch: data.authorityEpoch, revision: data.revision, applied: true }
  : cmd === 'performance.deliver' ? { eventCursor: data.eventSeq, buffered: false }
  : cmd === 'performance.gap' ? { authorityEpoch: data.authorityEpoch, sessionId: data.sessionId, applied: true, eventCursor: data.oldestAvailableSeq - 1 } : {} });
const drain = async () => { for (let i = 0; i < 20; i++) await new Promise(resolve => setImmediate(resolve)); };
function fixture(options = {}) {
  let now = 1_000_000;
  const rooms = new RoomCache({ maxRooms: 100, staleRoomMs: 60_000 });
  const seats = new Map(), frames = [];
  let send = async frame => ackFor(frame);
  const service = new PerformanceService(rooms, { seatFor: (p, l) => seats.get(`${p}:${l}`) }, {
    async performance(room, cmd, data, signal) {
      const frame = { roomId: room.roomId, cmd, data: structuredClone(data) }; frames.push(frame);
      return send(frame, signal);
    }
  }, { durationMs: 1000, preparationMs: 100, leaveGraceMs: 100, clock: () => now, ...options });
  function add(p, r, level = LEVEL) {
    const nodeId = `n-${r}`;
    rooms.upsertRoom({ roomId: r, nodeId, udpHost: '127.0.0.1', udpPort: 41000 + rooms.listRooms().length, players: 1, capacity: 8, version: 1, draining: false });
    rooms.trackPlayer(p, nodeId, r, level); seats.set(`${p}:${level}`, { levelId: level, benchId: 0, seatId: 0 });
    service.notePlayerSeen(p, r, level);
  }
  const event = (name, p, r, fields = {}) => ({ event: name, data: { nodeId: `n-${r}`, roomId: r, player: { playerId: p }, levelId: LEVEL, ...fields } });
  let req = 0;
  const request = (p, r, requested = true, fields = {}) => service.handle(event('performance.request', p, r, { requestId: `req-${++req}`, requested, ...fields }));
  const publish = (p, r, fields = {}) => service.handle(event('performance.publish', p, r, {
    sessionId: service.state(LEVEL).current?.sessionId, eventId: `e-${req++}`, timestamp: 1234.125, actionId: 7,
    payloadBase64: Buffer.from('{"original":true}').toString('base64'), ...fields
  }));
  return { service, rooms, seats, frames, add, event, request, publish, now: () => now, advance: n => { now += n; }, sender: fn => { send = fn; } };
}
// Existing broadcast checks advance the real authority clock through preparation;
// no fixture starts a session directly or disables the preparation phase.
function completePreparation(f, performerId, levelId = LEVEL) {
  const before = f.service.state(levelId), prep = before.preparing;
  assert.equal(before.current, null, 'preparation cannot expose a formal session');
  assert.ok(prep, 'expected an explicit preparation phase');
  assert.equal(prep.performerId, performerId ?? before.queue[0]?.playerId);
  assert.equal(before.queue[0]?.playerId, prep.performerId, 'preparer remains the FIFO head');
  assert.ok(prep.durationMs > 0);
  assert.equal(prep.expiresAtMs, prep.startedAtMs + prep.durationMs);
  assert.ok(f.now() <= prep.expiresAtMs, 'helper must not mask a late tick');
  f.advance(prep.expiresAtMs - f.now()); f.service.tick();
  const state = f.service.state(levelId);
  assert.equal(state.preparing, null);
  assert.equal(state.current?.performerId, prep.performerId);
  assert.equal(state.current.startedAtMs, prep.expiresAtMs, 'formal starts at fixed preparation deadline');
  assert.equal(state.current.expiresAtMs, prep.expiresAtMs + state.current.durationMs);
  assert.ok(!state.queue.some(q => q.playerId === prep.performerId), 'formal performer leaves the queue');
  return state.current;
}
const cases = [];
const test = (name, body) => cases.push([name, body]);
test('FIFO, repeat requests, exact deadline, unique rounds, owner cancellation', async () => {
  const f = fixture(); for (const p of ['a', 'b', 'c']) f.add(p, p);
  f.request('a', 'a'); const first = completePreparation(f, 'a');
  f.advance(20); f.request('a', 'a'); f.request('b', 'b'); f.request('c', 'c'); f.request('b', 'b');
  let state = f.service.state(LEVEL);
  assert.deepEqual(state.current, first); assert.deepEqual(state.queue.map(q => q.playerId), ['b', 'c']);
  assert.equal(first.expiresAtMs, first.startedAtMs + first.durationMs, 'no client +5s compensation in authority');
  f.request('c', 'c', false); assert.deepEqual(f.service.state(LEVEL).queue.map(q => q.playerId), ['b']);
  f.request('c', 'c', false, { expectedSessionId: first.sessionId }); assert.equal(f.service.state(LEVEL).current.performerId, 'a');
  f.request('a', 'a', false); assert.equal(f.service.state(LEVEL).current.performerId, 'a');
  f.advance(980); f.service.tick(); state = f.service.state(LEVEL);
  assert.equal(state.current, null); assert.equal(state.preparing.performerId, 'b');
  assert.equal(state.preparing.startedAtMs, f.now()); assert.equal(state.lastEnded.reason, 'expired');
  completePreparation(f, 'b'); state = f.service.state(LEVEL);
  assert.equal(state.current.performerId, 'b'); assert.notEqual(state.current.sessionId, first.sessionId);
  assert.equal(state.lastEnded.reason, 'expired'); assert.equal(state.current.startedAtMs, f.now());
  f.request('b', 'b', false, { expectedSessionId: first.sessionId }); assert.equal(f.service.state(LEVEL).current.performerId, 'b');
  f.request('b', 'b', false, { expectedSessionId: state.current.sessionId }); assert.equal(f.service.state(LEVEL).current, null);
  await drain(); assert.ok(f.frames.some(x => x.cmd === 'performance.decision' && x.data.status === 'rejected'));
  f.service.stop();
});
test('request ID idempotence and conflicting player/operation', async () => {
  const f = fixture(); f.add('a', 'a'); f.add('b', 'b');
  f.request('a', 'a', true, { requestId: 'same' }); completePreparation(f, 'a'); const first = f.service.state(LEVEL);
  f.request('a', 'a', true, { requestId: 'same' }); assert.deepEqual(f.service.state(LEVEL), first);
  f.request('b', 'b', true, { requestId: 'same' }); f.request('a', 'a', false, { requestId: 'same', expectedSessionId: first.current.sessionId });
  await drain(); assert.equal(f.service.state(LEVEL).current.performerId, 'a');
  assert.equal(f.frames.filter(x => x.cmd === 'performance.decision' && x.data.reason === 'request_id_conflict').length, 2);
  f.service.stop();
});
test('leave grace, returning player, change map, room reclaim, final seat release', async () => {
  const f = fixture(); f.add('a', 'a'); f.add('b', 'b'); f.request('a', 'a'); completePreparation(f, 'a'); f.request('b', 'b');
  f.service.notePlayerLeft('a', 'a'); f.advance(99); f.service.tick(); assert.equal(f.service.state(LEVEL).current.performerId, 'a');
  f.service.notePlayerSeen('a', 'a', LEVEL); f.advance(1); f.service.tick(); assert.equal(f.service.state(LEVEL).current.performerId, 'a');
  f.rooms.trackPlayer('a', 'n-a', 'a', HALL); f.service.notePlayerSeen('a', 'a', HALL);
  assert.equal(f.service.state(LEVEL).current, null); assert.equal(f.service.state(LEVEL).preparing.performerId, 'b');
  completePreparation(f, 'b'); assert.equal(f.service.state(LEVEL).current.performerId, 'b');
  f.rooms.untrackPlayer('b', 'b'); f.service.notePlayerLeft('b', 'b'); f.advance(100); f.service.tick(); assert.equal(f.service.state(LEVEL).current, null);
  f.rooms.trackPlayer('a', 'n-a', 'a', LEVEL); f.service.notePlayerSeen('a', 'a', LEVEL); f.request('a', 'a'); completePreparation(f, 'a');
  f.rooms.deleteRoom('a'); f.rooms.removePlayersByNode('n-a'); f.service.forgetRoom('a'); assert.equal(f.service.state(LEVEL).current, null);
  f.rooms.trackPlayer('b', 'n-b', 'b', LEVEL); f.service.notePlayerSeen('b', 'b', LEVEL); f.request('b', 'b'); completePreparation(f, 'b'); f.service.removePlayer('b', 'seat_released', LEVEL);
  assert.equal(f.service.state(LEVEL).lastEnded.reason, 'seat_released'); f.service.stop();
});
test('independent idle/tombstone delivery, rejected ACK retry, full reconnect', async () => {
  const f = fixture(); f.add('a', 'a'); f.service.tick(); await drain();
  assert.ok(f.frames.some(x => x.cmd === 'performance.state' && x.data.current === null), 'new subscriber gets idle without seat diff');
  f.request('a', 'a'); completePreparation(f, 'a'); await drain(); f.frames.length = 0;
  f.sender(async () => ({ code: 4000 })); const id = f.service.state(LEVEL).current.sessionId; f.request('a', 'a', false, { expectedSessionId: id }); await drain();
  assert.ok(f.frames.some(x => x.cmd === 'performance.state' && x.data.lastEnded?.sessionId === id));
  f.sender(async frame => ackFor(frame)); f.frames.length = 0; f.advance(1000); f.service.tick(); await drain();
  assert.ok(f.frames.some(x => x.cmd === 'performance.state' && x.data.current === null && x.data.lastEnded?.sessionId === id));
  f.frames.length = 0; f.service.reconnectNode('n-a'); await drain(); assert.ok(f.frames.some(x => x.cmd === 'performance.state' && x.data.current === null));
  f.service.stop();
});
test('publish fanout including source, level filtering, dedup, original payload, strict authorization', async () => {
  const f = fixture(); f.add('a', 'a'); f.add('b', 'b'); f.add('c', 'c', HALL); f.request('a', 'a'); completePreparation(f, 'a'); await drain(); f.frames.length = 0;
  const body = { eventId: 'original', actionId: 77, timestamp: 12.5, payloadBase64: Buffer.from('raw\u0000bytes').toString('base64') };
  assert.equal(f.publish('a', 'a', body), true); assert.equal(f.publish('a', 'a', body), true);
  assert.equal(f.publish('a', 'a', { ...body, payloadBase64: 'Yg==' }), false);
  assert.equal(f.publish('b', 'b'), false); assert.equal(f.publish('a', 'b'), false); assert.equal(f.publish('a', 'a', { sessionId: 'old' }), false);
  await drain(); const deliveries = f.frames.filter(x => x.cmd === 'performance.deliver'); assert.deepEqual(deliveries.map(x => x.roomId).sort(), ['a', 'b']);
  for (const d of deliveries) { assert.equal(d.data.eventSeq, 1); for (const [key, value] of Object.entries(body)) assert.equal(d.data[key], value); }
  f.advance(1000); assert.equal(f.publish('a', 'a', { eventId: 'expired' }), false); f.service.stop();
});
test('bounded replay, explicit cursor gap, state/gap/replay order, epoch reset, no old session replay', async () => {
  const f = fixture({ journalMaxEvents: 2 }); f.add('a', 'a'); f.request('a', 'a'); completePreparation(f, 'a'); await drain();
  for (let i = 1; i <= 4; i++) f.publish('a', 'a', { eventId: `e${i}` }); await drain(); f.frames.length = 0;
  const sessionId = f.service.state(LEVEL).current.sessionId;
  f.service.sync('n-a', 'a', [LEVEL], f.service.authorityEpoch, { [sessionId]: 0 }); await drain();
  const commands = f.frames.map(x => x.cmd); assert.deepEqual(commands, ['performance.state', 'performance.gap', 'performance.deliver', 'performance.deliver']);
  assert.equal(f.frames[1].data.reason, 'journal_gap'); assert.equal(f.frames[1].data.oldestAvailableSeq, 3);
  assert.deepEqual(f.frames.filter(x => x.cmd === 'performance.deliver').map(x => x.data.eventSeq), [3, 4]);
  f.frames.length = 0; f.service.sync('n-a', 'a', [LEVEL], 'old-epoch', { [sessionId]: 999 }); await drain();
  assert.equal(f.frames.find(x => x.cmd === 'performance.gap').data.reason, 'epoch_changed');
  f.request('a', 'a', false, { expectedSessionId: sessionId }); f.request('a', 'a'); f.frames.length = 0;
  f.service.sync('n-a', 'a', [LEVEL], f.service.authorityEpoch, { [sessionId]: 4 }); await drain();
  assert.equal(f.frames.filter(x => x.cmd === 'performance.deliver').length, 0); f.service.stop();
});
test('slow room does not block others; per room ordering; stop aborts pending', async () => {
  const f = fixture({ concurrency: 2 }); f.add('a', 'a'); f.add('b', 'b');
  let pending, signal;
  f.sender((frame, s) => frame.roomId === 'a' ? new Promise(resolve => { pending = () => resolve(ackFor(frame)); signal = s; }) : Promise.resolve(ackFor(frame)));
  f.request('a', 'a'); completePreparation(f, 'a'); await drain(); assert.ok(f.frames.some(x => x.roomId === 'b' && x.cmd === 'performance.state'));
  assert.equal(f.frames.filter(x => x.roomId === 'a').length, 1);
  f.publish('a', 'a'); await drain(); assert.ok(f.frames.some(x => x.roomId === 'b' && x.cmd === 'performance.deliver'));
  f.advance(20); pending({ code: 0 }); await drain();
  assert.ok(f.frames.filter(x => x.roomId === 'a').length >= 2); f.service.stop(); assert.equal(signal.aborted, true); pending({ code: 0 }); await drain();
});
test('schemas reject legacy approval, missing fields, bad/oversize base64, unsafe cursors', () => {
  const f = fixture(); f.add('a', 'a');
  assert.equal(NodeEventSchema.safeParse(f.event('spotlight.changed', 'a', 'a', { active: true })).success, false);
  assert.equal(NodeEventSchema.safeParse(f.event('performance.request', 'a', 'a', {})).success, false);
  const base = f.event('performance.publish', 'a', 'a', { sessionId: 's', eventId: 'e', timestamp: 12.5, actionId: 1, payloadBase64: 'YQ==' });
  assert.equal(NodeEventSchema.safeParse(base).success, true);
  for (const payloadBase64 of ['YQ', 'YR==', '', Buffer.alloc(PERFORMANCE_MAX_PAYLOAD_BYTES + 1).toString('base64')]) assert.equal(NodeEventSchema.safeParse({ ...base, data: { ...base.data, payloadBase64 } }).success, false);
  assert.equal(NodeEventSchema.safeParse(f.event('performance.sync', 'a', 'a', { levelIds: [LEVEL], eventCursors: { s: -1 } })).success, false);
  f.service.stop();
});
test('buffered and missing event cursors retry rather than advancing content', async () => {
  const f = fixture({ durationMs: 10000 }); f.add('a', 'a'); f.request('a', 'a'); completePreparation(f, 'a'); await drain(); f.frames.length = 0;
  f.sender(async frame => frame.cmd === 'performance.deliver' ? { code: 0, data: { buffered: true, eventCursor: 0 } } : ackFor(frame));
  f.publish('a', 'a', { eventId: 'buffered' }); await drain();
  const first = f.frames.find(x => x.cmd === 'performance.deliver'); assert.equal(first.data.eventSeq, 1);
  f.sender(async frame => frame.cmd === 'performance.deliver' ? { code: 0 } : ackFor(frame));
  f.advance(1000); f.service.tick(); await drain();
  f.sender(async frame => ackFor(frame)); f.advance(1000); f.service.tick(); await drain();
  assert.equal(f.frames.filter(x => x.cmd === 'performance.deliver' && x.data.eventSeq === 1).length, 3);
  f.service.stop();
});
test('state and gap require applied identity acknowledgments', async () => {
  const f = fixture({ durationMs: 10000, journalMaxEvents: 1 }); f.add('a', 'a');
  f.sender(async () => ({ code: 0 })); f.service.tick(); await drain();
  assert.equal(f.frames.filter(x => x.cmd === 'performance.state').length, 1);
  f.sender(async frame => ackFor(frame)); f.advance(1000); f.service.tick(); await drain();
  assert.equal(f.frames.filter(x => x.cmd === 'performance.state').length, 2);
  f.request('a', 'a'); completePreparation(f, 'a'); await drain(); f.publish('a', 'a', { eventId: 'one' }); f.publish('a', 'a', { eventId: 'two' }); await drain();
  f.frames.length = 0; f.sender(async frame => frame.cmd === 'performance.gap' ? { code: 0, data: { applied: true, authorityEpoch: 'wrong' } } : ackFor(frame));
  const id = f.service.state(LEVEL).current.sessionId; f.service.sync('n-a', 'a', [LEVEL], f.service.authorityEpoch, { [id]: 0 }); await drain();
  assert.equal(f.frames.filter(x => x.cmd === 'performance.deliver').length, 0);
  f.sender(async frame => ackFor(frame)); f.advance(1000); f.service.tick(); await drain();
  assert.ok(f.frames.some(x => x.cmd === 'performance.deliver' && x.data.eventSeq === 2)); f.service.stop();
});
test('queue limits, journal TTL, and full telemetry removal are bounded', async () => {
  assert.throws(() => fixture({ queueLimit: 10001 }), /queueLimit/);
  const f = fixture({ durationMs: 10000, queueLimit: 1, journalTtlMs: 50 });
  for (const p of ['a', 'b', 'c']) f.add(p, p); f.request('a', 'a'); completePreparation(f, 'a'); f.request('b', 'b'); f.request('c', 'c'); await drain();
  assert.deepEqual(f.service.state(LEVEL).queue.map(q => q.playerId), ['b']);
  assert.ok(f.frames.some(x => x.cmd === 'performance.decision' && x.data.reason === 'queue_full'));
  f.publish('a', 'a', { eventId: 'ttl' }); await drain(); f.advance(50); f.frames.length = 0;
  const id = f.service.state(LEVEL).current.sessionId; f.service.sync('n-a', 'a', [LEVEL], f.service.authorityEpoch, { [id]: 0 }); await drain();
  assert.ok(f.frames.some(x => x.cmd === 'performance.gap' && x.data.oldestAvailableSeq === 2));
  assert.equal(f.frames.filter(x => x.cmd === 'performance.deliver').length, 0);
  f.service.noteRoomRoster('a', [], true); f.advance(100); f.service.tick();
  assert.equal(f.service.state(LEVEL).current, null); assert.equal(f.service.state(LEVEL).preparing.performerId, 'b');
  completePreparation(f, 'b'); assert.equal(f.service.state(LEVEL).current.performerId, 'b');
  assert.equal(f.service.participantRooms.has('a'), false); f.service.stop();
});
test('HTTP-only room cache membership cannot approve a performance', async () => {
  const f = fixture(); f.add('a', 'a');
  f.rooms.trackPlayer('untrusted', 'n-a', 'a', LEVEL); f.seats.set(`untrusted:${LEVEL}`, { levelId: LEVEL, benchId: 0, seatId: 1 });
  f.request('untrusted', 'a'); await drain();
  assert.equal(f.service.state(LEVEL).current, null);
  assert.ok(f.frames.some(x => x.cmd === 'performance.decision' && x.data.reason === 'player_not_live_seated_in_room')); f.service.stop();
});
test('preparation admits immediately, keeps FIFO head, and starts only at the fixed deadline', async () => {
  const f = fixture(); f.add('a', 'a'); f.add('b', 'b'); f.request('a', 'a');
  const first = f.service.state(LEVEL), prep = first.preparing;
  assert.equal(first.current, null); assert.equal(first.queue[0].playerId, 'a');
  assert.equal(f.service.currentPerformer(LEVEL), undefined);
  assert.equal(f.publish('a', 'a'), false, 'preparing player cannot publish');
  await drain();
  assert.ok(f.frames.some(x => x.cmd === 'performance.decision' && x.data.status === 'queued' && x.data.state.current === null));
  f.advance(99); f.request('a', 'a'); f.request('b', 'b'); f.service.tick();
  assert.deepEqual(f.service.state(LEVEL).preparing, prep);
  assert.equal(f.service.state(LEVEL).current, null);
  const current = completePreparation(f, 'a');
  assert.equal(current.startedAtMs, prep.expiresAtMs);
  assert.deepEqual(f.service.state(LEVEL).queue.map(q => q.playerId), ['b']);
  f.request('a', 'a', false, { expectedPreparationId: prep.preparationId, expectedSessionId: current.sessionId });
  assert.deepEqual(f.service.state(LEVEL).current, current, 'old preparation cancellation cannot cancel a running session');
  f.service.stop();
});
test('preparation cancellation is identity-bound and replacement gets its own full window', async () => {
  const f = fixture(); for (const p of ['a', 'b', 'c']) f.add(p, p);
  f.request('a', 'a'); f.request('b', 'b'); f.request('c', 'c');
  const prep = f.service.state(LEVEL).preparing;
  f.request('a', 'a', false); f.request('a', 'a', false, { expectedPreparationId: 'wrong' });
  assert.deepEqual(f.service.state(LEVEL).preparing, prep);
  f.advance(30); f.request('a', 'a', false, { expectedPreparationId: prep.preparationId });
  const next = f.service.state(LEVEL);
  assert.equal(next.current, null); assert.equal(next.lastEnded, null);
  assert.equal(next.preparing.performerId, 'b'); assert.notEqual(next.preparing.preparationId, prep.preparationId);
  assert.equal(next.preparing.startedAtMs, f.now()); assert.equal(next.preparing.expiresAtMs, f.now() + 100);
  f.request('c', 'c', false); assert.deepEqual(f.service.state(LEVEL).queue.map(q => q.playerId), ['b']);
  completePreparation(f, 'b'); await drain(); f.service.stop();
});
test('preparation reconnect preserves deadlines and disappeared candidate never activates', async () => {
  const f = fixture({ leaveGraceMs: 1000 }); f.add('a', 'a'); f.add('b', 'b');
  f.request('a', 'a'); f.request('b', 'b'); const prep = f.service.state(LEVEL).preparing;
  f.advance(20); f.service.notePlayerLeft('a', 'a'); f.service.notePlayerSeen('a', 'a', LEVEL);
  f.service.reconnectNode('n-a'); f.service.sync('n-a', 'a', [LEVEL], f.service.authorityEpoch, {});
  assert.deepEqual(f.service.state(LEVEL).preparing, prep);
  await drain(); assert.ok(f.frames.some(x => x.cmd === 'performance.state' && x.data.preparing?.preparationId === prep.preparationId));
  f.service.notePlayerLeft('a', 'a'); f.advance(80); f.service.tick();
  const state = f.service.state(LEVEL);
  assert.equal(state.current, null); assert.equal(state.lastEnded, null);
  assert.equal(state.preparing.performerId, 'b'); assert.equal(state.preparing.expiresAtMs, f.now() + 100);
  assert.ok(!state.queue.some(q => q.playerId === 'a'));
  f.service.stop();
});
test('room reclaim and final seat release reconcile the preparing queue after batch removal', () => {
  const f = fixture(); f.add('a', 'dead'); f.add('b', 'dead'); f.add('c', 'live');
  f.request('a', 'dead'); f.request('b', 'dead'); f.request('c', 'live');
  f.advance(25); f.rooms.deleteRoom('dead'); f.rooms.removePlayersByNode('n-dead'); f.service.forgetRoom('dead');
  let state = f.service.state(LEVEL);
  assert.equal(state.current, null); assert.equal(state.preparing.performerId, 'c');
  assert.deepEqual(state.queue.map(q => q.playerId), ['c']); assert.equal(state.preparing.expiresAtMs, f.now() + 100);
  f.service.removePlayer('c', 'seat_released', LEVEL); state = f.service.state(LEVEL);
  assert.equal(state.preparing, null); assert.equal(state.current, null); assert.equal(state.lastEnded, null);
  f.service.stop();
});
test('late ticks keep formal deadline anchored and give successor a fresh preparation', () => {
  const f = fixture(); f.add('a', 'a'); f.add('b', 'b'); f.request('a', 'a'); f.request('b', 'b');
  const prep = f.service.state(LEVEL).preparing;
  f.advance(250); f.service.tick(); const current = f.service.state(LEVEL).current;
  assert.equal(current.startedAtMs, prep.expiresAtMs); assert.equal(current.expiresAtMs, prep.expiresAtMs + 1000);
  f.advance(2000); f.service.tick(); const next = f.service.state(LEVEL);
  assert.equal(next.current, null); assert.equal(next.lastEnded.endedAtMs, current.expiresAtMs);
  assert.equal(next.preparing.performerId, 'b'); assert.equal(next.preparing.expiresAtMs, f.now() + 100);
  f.service.stop();
});
test('preparation counts toward bounded queue and cancellation schema validates its identifier', () => {
  assert.throws(() => fixture({ preparationMs: 0 }), /preparationMs/);
  assert.throws(() => fixture({ preparationMs: NaN }), /preparationMs/);
  const f = fixture({ queueLimit: 1 }); f.add('a', 'a'); f.add('b', 'b'); f.request('a', 'a'); f.request('b', 'b');
  assert.deepEqual(f.service.state(LEVEL).queue.map(q => q.playerId), ['a']);
  assert.equal(NodeEventSchema.safeParse(f.event('performance.request', 'a', 'a', { requestId: 'cancel', requested: false, expectedPreparationId: 123 })).success, false);
  assert.equal(NodeEventSchema.safeParse(f.event('performance.request', 'a', 'a', { requestId: 'cancel', requested: false, expectedPreparationId: 'valid' })).success, true);
  f.service.stop();
});
for (const [name, body] of cases) { await body(); console.log(`ok - ${name}`); }
console.log(`All ${cases.length} performance checks passed.`);
