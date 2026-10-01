# Hermes Room Manager (QWD)

English | [中文](README_zh.md)

**Hermes** is the central room-orchestration manager — referred to as **QWD** (Room Authority /
Room Manager) across the XYSky ecosystem. It coordinates a fleet of
[XYSKY UDP](../that-sky-xysky-udp-team/README.md) room nodes: it allocates players to rooms,
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
  transaction_timeout_ms: 15000

debug:
  api_token: ""
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
| `nodes.stale_ms` | `10000` | Node considered unhealthy after this idle time |
| `nodes.backpressure_limit_bytes` | `8388608` | Pause sends above this WS buffer size |
| `nodes.circuit_open_ms` | `5000` | Circuit-open duration after tripping |
| `nodes.circuit_failure_threshold` | `5` | Consecutive failures that trip the circuit |
| `assignment.ttl_ms` | `30000` | Lifetime of a returned room assignment |
| `assignment.reservation_shadow_ttl_ms` | `10000` | Local shadow-hold lifetime |
| `commands.timeout_ms` | `3000` | Node command ack timeout |
| `commands.max_pending` | `100000` | Max outstanding commands |
| `move.budget_per_cycle` | `8` | Max moves started per planning cycle |
| `move.transaction_timeout_ms` | `15000` | Abort a stalled move transaction after this |
| `debug.api_token` | `""` | Token for `/debug/*`; empty keeps them locked |

### Debug endpoints

`GET /debug/allnode` and `POST /debug/move` require `debug.api_token`. When set, send it as the
`x-debug-token` header or an `Authorization: Bearer <token>` header. Leaving it empty keeps the
endpoints inaccessible.

## Connecting XYSky and nodes

Two distinct addresses are involved when running QWD:

- **XYSky → QWD:** XYSky's `udp.uri` must be QWD's HTTP(S) `/allocate` URL,
  e.g. `https://thatroom.example.cn/allocate`.
- **Node → QWD:** each XYSKY UDP node's `qwd.url` must be QWD's **WebSocket** URL,
  e.g. `wss://thatroom.example.cc`.

Nodes register their `public_uri` on connect; QWD hands that address back to clients via
`/allocate`. Terminate TLS / route the WebSocket upgrade (path `/`) at your reverse proxy as
needed.

## License

[GNU General Public License v3.0](LICENSE).
