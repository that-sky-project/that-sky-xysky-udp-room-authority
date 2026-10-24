# Hermes Room Manager (QWD)

English | [中文](README_zh.md)

**Hermes** is the central room-orchestration manager — referred to as **QWD** (Room Authority /
Room Manager) across the XYSky ecosystem. It coordinates a fleet of
[XYSKY UDP](https://github.com/that-sky-project/that-sky-xysky-udp-team/blob/main/README.md) room nodes: it allocates players to rooms,
maintains a live view of the fleet, and autonomously migrates players between rooms to
consolidate and balance activity.

It is a single-process Node.js / TypeScript service. All authoritative runtime state is kept
**in memory** (there is no database in the live path).

> Independent community project; not affiliated with or endorsed by thatgamecompany.

## Role & Architecture

QWD never handles UDP game packets. It is a **control plane**: gameplay runs over UDP directly
between clients and the room nodes, and QWD only decides *which* node a client should use.

A single HTTP server exposes **two transports**:

- **WebSocket control channel** (path `/`) — room nodes connect here and stay connected. Nodes
  *push* events up (room lifecycle, player join/leave, heartbeats, telemetry) and QWD *sends*
  commands down (`room.reserve`, `player.redirect`, `move.prepare` / `move.commit` /
  `move.cancel`, `room.destroy`, `node.drain`). Every command is request/response with an ack.
- **HTTP JSON API** — called by XYSky / matchmakers and operators:
  - `POST /allocate` — ask for a room; returns `{ roomId, nodeId, udpHost, udpPort, expiresAt }`.
  - `GET /moves`, `GET /activity` — fleet introspection.
  - `GET /players/:id/room` — player lookup.
  - `POST /telemetry` — HTTP fallback for node events.
  - `POST /debug/move`, `GET /debug/allnode` — token-gated operator tools.

Authority is split: **nodes are authoritative** over room existence, occupancy, and player
presence; **QWD is authoritative** over allocation and migration decisions.

```text
XYSky / matchmaker ──HTTP(S) /allocate──▶ QWD ◀──WebSocket──▶ XYSKY UDP nodes ──UDP──▶ clients
```

## Requirements

- Node.js 22 or newer
- npm

## Install, Build, Run

The project is TypeScript and compiles to `dist/`.

```bash
npm install
npm run build
npm start          # runs dist/runtime/main.js
```

For development with rebuild + watch:

```bash
npm run dev
```

Type-check only (no emit): `npm run typecheck`.

### Cluster simulation

`scripts/simulate-merge.mjs` drives the compiled `MoveCoordinator` against an in-memory
cluster (one room per node, telemetry every 2.1s, node commands auto-acked). It shows
whether same-level rooms merge, which room is chosen, and how the level-change settling
window delays a merge — without a live cluster. Run `npm run build` first.

```bash
node scripts/simulate-merge.mjs                # four lone players across three levels
node scripts/simulate-merge.mjs isolated       # one room per level: nothing to merge
node scripts/simulate-merge.mjs cooldown       # a level change just happened
node scripts/simulate-merge.mjs offline        # a same-level peer node is unreachable
node scripts/simulate-merge.mjs snapshot seconds=120
```

## Configuration

All configuration lives in **`config.yml`** (loaded from the working directory). The manager
reads **no environment variables** — `config.yml` is the single source of configuration.

```yaml
host: 0.0.0.0
port: 1131
allocate_path: /allocate

logging:
  level: info

rooms:
  max_rooms: 100000
  candidate_room_limit: 256
  stale_room_ms: 120000
  max_players_per_room: 8

nodes:
  stale_ms: 10000
  backpressure_limit_bytes: 8388608
  circuit_open_ms: 5000
  circuit_failure_threshold: 5

assignment:
  ttl_ms: 30000
  reservation_shadow_ttl_ms: 10000

commands:
  timeout_ms: 3000
  max_pending: 100000

move:
  budget_per_cycle: 8
  confirmation_cycles: 2
  transaction_timeout_ms: 15000
  level_change_cooldown_ms: 6000
  join_settle_ms: 4000

debug:
  api_token: ""
  log_buffer_size: 2000
```

| Key | Default | Description |
| --- | --- | --- |
| `host` / `port` | `0.0.0.0` / `1131` | HTTP + WebSocket listen address |
| `allocate_path` | `/allocate` | HTTP path for room allocation |
| `logging.level` | `info` | Pino log level |
| `rooms.max_rooms` | `100000` | Room snapshot cache capacity |
| `rooms.candidate_room_limit` | `256` | Max rooms scanned per allocation |
| `rooms.stale_room_ms` | `120000` | Drop a room snapshot after this idle time |
| `rooms.max_players_per_room` | `8` | Hard per-room cap |
| `rooms.unknown_level_penalty` | `50` | Score penalty for placing a player with an unknown level into a room that already has one |
| `nodes.stale_ms` | `10000` | Node considered unhealthy after this idle time |
| `nodes.backpressure_limit_bytes` | `8388608` | Pause sends above this WS buffer size |
| `nodes.circuit_open_ms` | `5000` | Circuit-open duration after tripping |
| `nodes.circuit_failure_threshold` | `5` | Consecutive failures that trip the circuit |
| `assignment.ttl_ms` | `30000` | Lifetime of a returned room assignment |
| `assignment.reservation_shadow_ttl_ms` | `10000` | Local shadow-hold lifetime |
| `commands.timeout_ms` | `3000` | Node command ack timeout |
| `commands.max_pending` | `100000` | Max outstanding commands |
| `move.budget_per_cycle` | `8` | Max moves started per planning cycle |
| `move.confirmation_cycles` | `2` | Consecutive cycles a room's roster/level signature must stay unchanged before it may be a move source or target |
| `move.cluster_split_penalty` | `0.05` | Extra gain penalty per spatial-cluster mate a partial migration leaves behind |
| `move.transaction_timeout_ms` | `15000` | Abort a stalled move transaction after this |
| `move.level_change_cooldown_ms` | `6000` | Per-player settling window after a level change; the player is excluded from level grouping and migration decisions during it |
| `move.join_settle_ms` | `4000` | Per-player settling window after joining a room (friend-join teleport, cross-room move) |
| `debug.api_token` | `""` | Token for `/debug/*`; empty disables the check |
| `debug.log_buffer_size` | `2000` | In-memory event history behind `GET /debug/logs` |

### Debug endpoints

`GET /debug/logs`, `GET /debug/allnode` and `POST /debug/move` accept an optional
`debug.api_token`. When a token is configured, send it as the `x-debug-token` header or an
`Authorization: Bearer <token>` header; when it is empty the check is disabled.

`GET /debug/logs` returns the recent structured event history (in-memory ring buffer, lost on
restart): room moves/merges/splits, level isolation and consolidation, allocations, room and
player lifecycle, outbound node commands with their acks, and telemetry desync events.

Query parameters:

- `limit` — newest N entries (default 200, max 1000)
- `since` — only entries with `id` greater than this; poll with the returned `nextSince`
- `category` — `move`, `level`, `room`, `player`, `node`, `telemetry`, `allocation`
- `roomId` / `playerId` — filter by room or by player

```bash
curl -s localhost:1131/debug/logs | jq -r '.data.entries[] | "\(.at) \(.event) \(.message)"'
curl -s 'localhost:1131/debug/logs?category=move&limit=50'
curl -s 'localhost:1131/debug/logs?roomId=10000001'
```

The response is `{ code, data: { entries, nextSince, oldestId, newestId, buffered, capacity,
truncated } }`. `truncated: true` means `since` predates the buffer, so part of the requested
window is already gone.

## Connecting XYSky and nodes

Two distinct addresses are involved when running QWD:

- **XYSky → QWD:** XYSky's `udp.uri` must be QWD's HTTP(S) `/allocate` URL,
  e.g. `https://thatroom.example.cn/allocate`.
- **Node → QWD:** each XYSKY UDP node's `qwd.url` must be QWD's **WebSocket** URL,
  e.g. `wss://thatroom.example.cc`.

Nodes register their `public_uri` on connect; QWD hands that address back to clients via
`/allocate`. Terminate TLS / route the WebSocket upgrade (path `/`) at your reverse proxy as
needed.

A node may add a stable identity to the upgrade URL (`?v=1&node=<id>`). QWD reuses that id
across reconnects, so a short socket drop restores the node's rooms within the grace window
instead of reaping them. Nodes that omit it get a per-connection id.

## License

[GNU General Public License v3.0](LICENSE).
