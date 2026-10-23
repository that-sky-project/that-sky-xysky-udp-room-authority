#!/usr/bin/env node
/**
 * Cluster simulation for the QWD merge/migration algorithm.
 *
 * Runs the real compiled MoveCoordinator (dist/) against an in-memory cluster:
 * one room per node, telemetry published every tick exactly like a UDP node
 * does, and fake node command transports that always ack. It answers questions
 * such as "four lone players in four rooms — will the same-level rooms merge?"
 * without needing a live cluster.
 *
 * Usage:
 *   node scripts/simulate-merge.mjs                 # the reported 4-player snapshot
 *   node scripts/simulate-merge.mjs isolated        # one room per level (no merge)
 *   node scripts/simulate-merge.mjs cooldown        # same as snapshot, level changed 1s before start
 *   node scripts/simulate-merge.mjs seconds=120     # run longer
 *
 * Requires `npm run build` first: it imports from dist/, not src/.
 */
import { RoomCache } from "../dist/manager-host/RoomCache.js";
import { DebugEventLog } from "../dist/manager-host/DebugEventLog.js";
import { MoveCoordinator } from "../dist/manager-host/MoveCoordinator.js";

// Numeric level ids as the UDP nodes report them. Only equality matters.
const LEVEL = { Sunset_YetiPark: 10, Aviary: 11, DuskStart: 12 };

const args = process.argv.slice(2);
const scenario = args.find((arg) => !arg.includes("=")) ?? "snapshot";
const seconds = Number(args.find((arg) => arg.startsWith("seconds="))?.split("=")[1] ?? 60);
const TICK_MS = 2_100; // > the coordinator's 2s planning throttle
const TICKS = Math.ceil((seconds * 1000) / TICK_MS);

const specs = {
  snapshot: [
    { id: "1d59ce20", level: "Sunset_YetiPark", players: ["bc41b0ad"] },
    { id: "43bea560", level: "Sunset_YetiPark", players: ["0d17e2c6"] },
    { id: "38145cb6", level: "Aviary", players: ["116af776"] },
    { id: "565a1a2a", level: "DuskStart", players: ["6db751d9"] }
  ],
  isolated: [
    { id: "38145cb6", level: "Aviary", players: ["116af776"] },
    { id: "565a1a2a", level: "DuskStart", players: ["6db751d9"] }
  ],
  // The same-level peer's node is unreachable. A third, reachable room on the
  // same level proves the planner still merges into the live one and never
  // targets the dead node's room.
  offline: [
    { id: "1d59ce20", level: "Sunset_YetiPark", players: ["bc41b0ad"] },
    { id: "43bea560", level: "Sunset_YetiPark", players: ["0d17e2c6"], offline: true },
    { id: "yeti-3", level: "Sunset_YetiPark", players: ["aaa11111", "bbb22222"] },
    { id: "38145cb6", level: "Aviary", players: ["116af776"] },
    { id: "565a1a2a", level: "DuskStart", players: ["6db751d9"] }
  ],
  cooldown: [
    { id: "1d59ce20", level: "Sunset_YetiPark", players: ["bc41b0ad"], switchOnFirstTick: true },
    { id: "43bea560", level: "Sunset_YetiPark", players: ["0d17e2c6"] },
    { id: "38145cb6", level: "Aviary", players: ["116af776"] },
    { id: "565a1a2a", level: "DuskStart", players: ["6db751d9"] }
  ]
}[scenario];

if (!specs) {
  console.error(`unknown scenario "${scenario}" (snapshot | isolated | cooldown)`);
  process.exit(1);
}

// Mirror the live cluster: 26 additional empty rooms (roomsCount: 30).
const rooms = specs.map((spec) => ({ ...spec, levelId: LEVEL[spec.level], tempLevelId: undefined }));
for (let i = 0; i < 26; i += 1) {
  rooms.push({ id: `empty-${i}`, level: null, levelId: null, players: [], tempLevelId: undefined });
}

// "cooldown" reproduces a level switch that just happened: the first telemetry
// frame reports the previous level, so the manager records a level change and
// starts its settling window.
//
// The manager stamps telemetry with Date.now() (not with the `now` passed to
// planAndExecute), so the simulated clock must drive Date.now() as well.
// Otherwise the accelerating test clock runs ahead of the real one and every
// cooldown already looks expired.
let now = Date.now();
Date.now = () => now;

const cache = new RoomCache({ maxRooms: 1000, staleRoomMs: 120_000, nodeGraceMs: 10_000 });
for (const room of rooms) {
  cache.upsertRoom({
    roomId: room.id,
    nodeId: `node-${room.id}`,
    udpHost: "127.0.0.1",
    udpPort: 19133,
    players: 0,
    capacity: 8,
    version: 0,
    draining: false
  });
}

// Node command side: every command is acked, so only planning/execution is
// under test. The manager's real coordinator logic is untouched.
const nodes = {
  getTransport: (nodeId) => ({ nodeId, writable: true, send: async () => {} }),
  isAvailableForCommand: (nodeId) => !rooms.some((room) => room.offline && `node-${room.id}` === nodeId)
};
const pending = { send: async (command) => ({ v: 1, id: command.id, code: 0, data: {} }) };
const cross = { redirectPlayers: async () => ({ v: 1, id: 0, code: 0, data: {} }) };

const silence = { info() {}, warn() {}, error() {}, debug() {}, trace() {}, fatal() {} };
const events = new DebugEventLog(500);
const moves = new MoveCoordinator(cache, nodes, pending, cross, {}, silence, undefined, undefined, events);

const seq = new Map();
function publishTelemetry(room) {
  const next = (seq.get(room.id) ?? 0) + 1;
  seq.set(room.id, next);
  const levelId = room.tempLevelId ?? room.levelId;
  room.tempLevelId = undefined;
  moves.ingest({
    v: 1,
    event: "room.telemetry",
    data: {
      nodeId: `node-${room.id}`,
      roomId: room.id,
      seq: next,
      players: room.players.map((playerId, index) => ({
        playerId,
        levelId: levelId ?? undefined,
        // Give bonded-looking players a little separation so spatial signal exists.
        position: [index * 2, 0, 0],
        velocity: 0,
        stateAt: now
      }))
    }
  });
}

function describe(room) {
  const label = room.level ?? "empty";
  return `${room.id}(${room.players.length},${label}${room.offline ? ",offline" : ""})`;
}

function applyCommitted(tx) {
  const source = rooms.find((room) => room.id === tx.sourceRoomId);
  const target = rooms.find((room) => room.id === tx.targetRoomId);
  if (!source || !target) return;
  for (const playerId of tx.playerIds) {
    const index = source.players.indexOf(playerId);
    if (index >= 0) source.players.splice(index, 1);
    if (!target.players.includes(playerId)) target.players.push(playerId);
    // Real nodes emit leave/join around the client's reconnect; the manager
    // needs both to drop the old telemetry entry and re-track the location.
    moves.ingest({
      v: 1,
      event: "player.leave",
      data: { nodeId: `node-${source.id}`, roomId: source.id, player: { playerId } }
    });
    moves.ingest({
      v: 1,
      event: "player.join",
      data: {
        nodeId: `node-${target.id}`,
        roomId: target.id,
        player: { playerId, level: target.levelId ?? undefined }
      }
    });
  }
}

const seen = new Set();
console.log(
  `scenario=${scenario} ticks=${TICKS} rooms=${rooms.length} ` +
  `(occupied ${rooms.filter((room) => room.players.length > 0).length})`
);
console.log(`initial: ${rooms.filter((room) => room.players.length > 0).map(describe).join("  ")}`);

for (let tick = 0; tick < TICKS; tick += 1) {
  now += TICK_MS;
  for (const room of rooms) {
    if (tick === 0 && room.switchOnFirstTick) room.tempLevelId = (room.levelId ?? 0) - 1;
    publishTelemetry(room);
  }

  const transactions = await moves.planAndExecute(undefined, now);
  for (const tx of transactions) {
    if (seen.has(tx.moveId)) continue;
    seen.add(tx.moveId);
    console.log(
      `t+${((tick + 1) * TICK_MS / 1000).toFixed(1)}s  ${tx.status.padEnd(9)} ` +
      `${String(tx.reason).padEnd(20)} ${tx.sourceRoomId} -> ${tx.targetRoomId} ` +
      `players=[${tx.playerIds.join(",")}]`
    );
    if (tx.status === "committed") applyCommitted(tx);
  }
}

const occupied = rooms.filter((room) => room.players.length > 0);
console.log(`final:   ${occupied.map(describe).join("  ")}`);
// Rooms on an unreachable node cannot be planned for; report them separately so
// they do not look like a stuck merge.
const reachable = occupied.filter((room) => !room.offline);
const byLevel = new Map();
for (const room of reachable) {
  const list = byLevel.get(room.level) ?? [];
  list.push(room.players.length);
  byLevel.set(room.level, list);
}
for (const [level, counts] of byLevel) {
  const total = counts.reduce((sum, count) => sum + count, 0);
  console.log(`level ${level}: ${counts.length} room(s), ${total} player(s) ${counts.length > 1 ? "<-- still split" : ""}`);
}
const unreachable = occupied.filter((room) => room.offline);
if (unreachable.length > 0) {
  console.log(`unreachable node: ${unreachable.map(describe).join("  ")}`);
}

// The same structured history GET /debug/logs exposes.
const recorded = events.list({ limit: 20 }).entries;
if (recorded.length > 0) {
  console.log("debug events:");
  for (const entry of recorded) console.log(`  ${entry.category}/${entry.event}  ${entry.message}`);
}
