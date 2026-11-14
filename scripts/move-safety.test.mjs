import assert from 'node:assert/strict';
import { test } from 'node:test';
import { RoomCache } from '../dist/manager-host/RoomCache.js';
import { NodeRegistry } from '../dist/manager-host/NodeRegistry.js';
import { PendingRequestMap } from '../dist/manager-host/PendingRequestMap.js';
import { CrossCoordinator } from '../dist/manager-host/CrossCoordinator.js';
import { MoveCoordinator } from '../dist/manager-host/MoveCoordinator.js';
import { MoveExecutor } from '../dist/manager-host/MoveExecutor.js';
import { TelemetryStore } from '../dist/manager-host/TelemetryStore.js';
import { ManagerGateway } from '../dist/manager-host/ManagerGateway.js';
import { NodeEventSchema, ManagerCommandSchema } from '../dist/types/contracts.js';
const logger = { info(){}, warn(){}, error(){}, debug(){}, trace(){}, child(){return this;} };
const ev = (event, roomId, data = {}, nodeId = roomId === 'a' ? 'na' : 'nb') => ({ event, data: { nodeId, roomId, ...data } });
const room = (roomId, players = 0) => ({ roomId, nodeId: roomId === 'a' ? 'na' : 'nb', udpHost: '127.0.0.1', udpPort: roomId === 'a' ? 41000 : 41001, capacity: 8, players, version: 1, draining: false });
const fixture = (options = {}) => {
  const rooms = new RoomCache({ maxRooms: 100, staleRoomMs: 600000 });
  const nodes = new NodeRegistry({ circuitOpenMs: 500, failureThreshold: 10, backpressureLimitBytes: 999999 });
  const pending = new PendingRequestMap({ timeoutMs: 100, maxPending: 100 });
  const sent = [];
  for (const nodeId of ['na', 'nb']) nodes.register({ nodeId, connectedAt: 100, protocolVersion: 1, writable: true, remoteAddress: '', close(){}, async send(command) { sent.push({ nodeId, ...command }); pending.resolve({ id: command.id, code: 0 }, nodeId); } });
  rooms.upsertRoom(room('a', 2)); rooms.upsertRoom(room('b'));
  const cross = new CrossCoordinator(nodes, rooms, pending, { commandTimeoutMs: 100 });
  const moves = new MoveCoordinator(rooms, nodes, pending, cross, { confirmationCycles: 1, ...options }, logger);
  for (const playerId of ['p', 'q']) { rooms.trackPlayer(playerId, 'na', 'a'); moves.telemetry.upsert('a', { playerId, levelId: 1, sessionId: 's-'+playerId, lvSeq: 1, status: 'ACTIVE' }); }
  const cleanup = async () => { pending.close(); await moves.stop(); };
  return { rooms, nodes, pending, sent, cross, moves, cleanup };
};
const plan = { sourceRoomId: 'a', targetRoomId: 'b', playerIds: ['p','q'], reason: 'player_rebalance', gain: 1 };
const isolation = { ...plan, reason: 'level_isolation', levelId: 1, sourceLevelCount: 2, targetLevelCount: 0 };
const begin = async (f, mode) => {
  if (mode === 'manual') return f.moves.manualMove({ ...plan });
  if (mode === 'isolation') { await f.moves.executeLevelIsolation(isolation, Date.now()); return f.moves.list()[0]; }
  f.moves.planner.plan = () => [plan]; f.moves.planner.calibrate = () => {}; f.moves.levelIsolationPlanner.plan = () => [];
  await f.moves.planAndExecute(); return f.moves.list()[0];
};
for (const mode of ['manual','auto','isolation']) test(`${mode}: delayed OK cannot resurrect a failed transaction`, async () => {
  const f = fixture();
  try {
    f.moves.executor.execute = async ({ moveId }) => { f.moves.ingest(ev('reservation.expired', 'b', { moveId })); return { ok: true, moveId }; };
    await begin(f, mode);
    assert.equal(f.moves.list()[0].status, 'failed');
  } finally { await f.cleanup(); }
});
test('partial failures are immutable, idempotent, and completion uses surviving IDs, not stale array lengths', async () => {
  const f = fixture();
  try {
    f.moves.executor.execute = async ({ moveId }) => ({ ok: true, moveId });
    const tx = await begin(f, 'manual');
    f.moves.telemetry.tryReserve('b', tx.moveId, 2, 8, 0, 35000);
    f.moves.ingest(ev('player.join', 'b', { player: { playerId: 'q' } }));
    f.moves.ingest(ev('player.leave', 'a', { player: { playerId: 'q' } }));
    f.moves.ingest(ev('move.result', 'a', { moveId: tx.moveId, accepted: false, failedPlayerIds: ['q', 'foreign'] }));
    f.moves.ingest(ev('move.result', 'a', { moveId: tx.moveId, accepted: false, failedPlayerIds: ['q'] }));
    assert.deepEqual(f.moves.list()[0].playerIds, ['p','q']);
    assert.equal(f.moves.telemetry.reserved('b'), 1, 'arrived q converts its slot; remaining p stays held until arrival or cancellation ACK');
    f.moves.ingest(ev('player.join', 'b', { player: { playerId: 'p' } }));
    f.moves.ingest(ev('player.leave', 'a', { player: { playerId: 'p' } }));
    assert.equal(f.moves.list()[0].status, 'completed');
  } finally { await f.cleanup(); }
});
test('move events are fenced to participants node, room and connection epoch; target completion is arrival only', async () => {
  const f = fixture();
  try {
    f.moves.executor.execute = async ({ moveId }) => ({ ok: true, moveId });
    const tx = await begin(f, 'manual');
    f.moves.ingest(ev('reservation.expired', 'b', { moveId: tx.moveId }, 'na'));
    f.moves.ingest(ev('reservation.expired', 'b', { moveId: tx.moveId, connectedAt: 99 }));
    assert.equal(f.moves.list()[0].status, 'committed');
    f.moves.ingest(ev('move.completed', 'b', { moveId: tx.moveId, playerIds: ['p','q'] }));
    assert.equal(f.moves.list()[0].status, 'committed');
    for (const playerId of ['p','q']) f.moves.ingest(ev('player.leave', 'a', { player: { playerId } }));
    assert.equal(f.moves.list()[0].status, 'completed');
  } finally { await f.cleanup(); }
});
test('same-cycle transactions make both rooms busy', async () => {
  const f = fixture(); try { f.moves.executor.execute = async ({ moveId }) => ({ ok:true, moveId }); const tx = await begin(f,'manual'); assert.equal(f.moves.roomBusy('a', tx.createdAt), true); assert.equal(f.moves.roomBusy('b', tx.createdAt), true); } finally { await f.cleanup(); }
});
test('cancel merges disjoint IDs, rejects non-OK ACKs and retries with a bound', async () => {
  const f = fixture({ retryBaseMs: 10, retryMaxMs: 20, maxAttempts: 2 });
  let finish;
  try {
    f.pending.send = () => new Promise(resolve => { finish = resolve; });
    f.moves.queueCancel('b','m',['p']); f.moves.queueCancel('b','m',['q']);
    assert.deepEqual(f.moves.cancellations.snapshot().find(entry => entry.moveId === 'm').playerIds.sort(), ['p','q']);
    finish({code:500}); await new Promise(resolve=>setImmediate(resolve));
    assert.ok(f.moves.cancellations.has('m'));
  } finally { finish?.({code:0}); await f.cleanup(); }
});
test('telemetry gap/version failures are atomic across room, location, roster and telemetry; full resync removes omissions', async () => {
  const f = fixture();
  try {
    const runtime = { ...f, metrics: { inc(){}, set(){} }, events: { record(){} }, reservations: { confirm(){} } };
    const gateway = Object.create(ManagerGateway.prototype); Object.assign(gateway, { runtime, log:logger, metrics:runtime.metrics, telemetrySeq:new Map(), telemetryStaleWarnAt:new Map() });
    assert.equal(gateway.applyNodeEvent(ev('room.telemetry','a',{seq:1,kind:'full',players:[{playerId:'p',levelId:1,sessionId:'s-p'}]})),true);
    gateway.applyNodeEvent(ev('room.telemetry','a',{seq:2,kind:'delta',baseSeq:999,players:[{playerId:'evil',levelId:9}]}));
    assert.equal(f.rooms.getPlayerLocation('evil'),undefined);
    assert.equal(f.moves.telemetry.get('evil'),undefined);
    assert.equal(f.moves.hasTelemetryGap('a'),true);
    await new Promise(resolve=>setImmediate(resolve)); assert.ok(f.sent.some(command=>command.cmd==='room.sync'));
    gateway.applyNodeEvent(ev('room.telemetry','a',{seq:3,kind:'delta',baseSeq:1,players:[{playerId:'evil'}]}));
    assert.equal(f.moves.telemetry.get('evil'),undefined,'gap remains frozen until full');
    gateway.applyNodeEvent(ev('room.telemetry','a',{seq:4,kind:'full',players:[]}));
    assert.equal(f.moves.telemetry.get('p'),undefined); assert.equal(f.rooms.getPlayerLocation('p'),undefined);
    f.rooms.upsertRoom({...room('a'),version:10});
    gateway.applyNodeEvent(ev('room.telemetry','a',{seq:5,kind:'full',players:[{playerId:'old',levelId:99}]}));
    assert.equal(f.rooms.getPlayerLocation('old'),undefined); assert.equal(f.moves.telemetry.get('old'),undefined);
  } finally { await f.cleanup(); }
});
test('late source telemetry cannot overwrite a target join; expected-room removals preserve the target', () => {
  const store = new TelemetryStore();
  store.upsert('a',{playerId:'p',sessionId:'old'}); store.upsert('b',{playerId:'p',sessionId:'new'});
  const f = fixture();
  try {
    f.moves.ingest(ev('player.join','b',{player:{playerId:'p',sessionId:'new'}}));
    f.moves.ingest(ev('room.telemetry','a',{seq:2,kind:'full',players:[{playerId:'p',sessionId:'s-p',levelId:99}]}));
    assert.equal(f.moves.telemetry.get('p').roomId,'b');
    store.remove('p','a','old'); assert.equal(store.get('p').roomId,'b');
  } finally { void f.cleanup(); }
});
test('MOVING physical occupants and remote reservations count against target capacity', async () => {
  const f = fixture(); try {
    f.rooms.upsertRoom(room('b',8)); f.moves.telemetry.upsert('b',{playerId:'moving',status:'MOVING'});
    const executor = new MoveExecutor(f.nodes,f.pending,f.cross,f.moves.telemetry,{reservationTtlMs:35000,prepareTtlMs:35000,redirectTtlMs:30000});
    assert.equal((await executor.execute({moveId:'m',playerIds:['p'],source:room('a',2),target:room('b',8),reason:'merge',now:Date.now()})).reason,'no_capacity');
  } finally { await f.cleanup(); }
});
test('control contracts preserve ordinary session fences and expose room.sync', () => {
  const data = NodeEventSchema.parse(ev('player.join','a',{player:{playerId:'p',sessionId:'s',lvSeq:1},sessionId:'s',connectedAt:100})).data;
  assert.equal(data.player.sessionId,'s'); assert.equal(data.sessionId,'s');
  assert.equal(ManagerCommandSchema.safeParse({id:1,cmd:'room.sync',data:{roomId:'a'}}).success,true);
});
