# Hermes 房间管理器（QWD）

[English](README.md) | 中文

**Hermes** 是中心化的房间编排管理器——在 XYSky 生态中称为 **QWD**（房间权威 / 房间管理器）。
它负责协调一组 [XYSKY UDP](https://github.com/that-sky-project/that-sky-xysky-udp-team/blob/main/README_zh.md) 房间节点：将玩家分配到房间、
维护整个集群的实时视图，并自动在房间之间迁移玩家以合并与均衡活跃度。

它是单进程的 Node.js / TypeScript 服务。所有权威运行时状态都保存在**内存**中（实时路径中没有数据库）。

> 独立社区项目，与 thatgamecompany 无隶属或官方合作关系。

## 角色与架构

QWD 不处理任何 UDP 游戏包。它是**控制面**：游戏数据直接在客户端与房间节点之间通过 UDP 传输，
QWD 只决定客户端*应该*使用哪个节点。

单个 HTTP 服务同时承载**两种传输**：

- **WebSocket 控制通道**（路径 `/`）——房间节点连接到这里并保持长连。节点向上*推送*事件
  （房间生命周期、玩家加入/离开、心跳、遥测），QWD 向下*下发*命令（`room.reserve`、
  `player.redirect`、`move.prepare` / `move.commit` / `move.cancel`、`room.destroy`、`node.drain`）。
  每条命令都是带 ack 的请求/响应。
- **HTTP JSON API**——由 XYSky / 匹配服务和运维调用：
  - `POST /allocate`——请求房间；返回 `{ roomId, nodeId, udpHost, udpPort, expiresAt }`。
  - `GET /moves`、`GET /activity`——集群自省。
  - `GET /players/:id/room`——玩家查询。
  - `POST /telemetry`——节点事件的 HTTP 回退通道。
  - `POST /debug/move`、`GET /debug/allnode`——需要 token 的运维工具。

权威划分：**节点**对房间存在性、占用数和玩家在场权威；**QWD** 对分配与迁移决策权威。

```text
XYSky / 匹配服务 ──HTTP(S) /allocate──▶ QWD ◀──WebSocket──▶ XYSKY UDP 节点 ──UDP──▶ 客户端
```

## 环境要求

- Node.js 22 或更高版本
- npm

## 安装、构建、运行

项目使用 TypeScript，编译输出到 `dist/`。

```bash
npm install
npm run build
npm start          # 运行 dist/runtime/main.js
```

开发模式（重建 + 监听）：

```bash
npm run dev
```

仅类型检查（不产出）：`npm run typecheck`。

### 集群模拟

`scripts/simulate-merge.mjs` 用编译后的 `MoveCoordinator` 驱动一个内存集群（一个节点一个房、
每 2.1 秒一帧 telemetry、节点命令自动 ack），不用真实集群就能看同一 level 的房会不会合并、
选中哪个房，以及切图沉降窗口如何延后合并。运行前先执行 `npm run build`。

```bash
node scripts/simulate-merge.mjs                # 4 个单人房、3 个 level
node scripts/simulate-merge.mjs isolated       # 每个 level 只有一个房：无合并目标
node scripts/simulate-merge.mjs cooldown       # 刚发生过切图
node scripts/simulate-merge.mjs offline        # 同 level 的对端节点不可达
node scripts/simulate-merge.mjs snapshot seconds=120
```

## 配置说明

全部配置都在 **`config.yml`** 中（从工作目录加载）。管理器**不读取任何环境变量**，`config.yml`
是唯一的配置来源。

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

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `host` / `port` | `0.0.0.0` / `1131` | HTTP + WebSocket 监听地址 |
| `allocate_path` | `/allocate` | 房间分配的 HTTP 路径 |
| `logging.level` | `info` | Pino 日志级别 |
| `rooms.max_rooms` | `100000` | 房间快照缓存容量 |
| `rooms.candidate_room_limit` | `256` | 每次分配扫描的最大房间数 |
| `rooms.stale_room_ms` | `120000` | 房间快照空闲多久后丢弃 |
| `rooms.max_players_per_room` | `8` | 单房间硬上限 |
| `nodes.stale_ms` | `10000` | 节点空闲多久后视为不健康 |
| `nodes.backpressure_limit_bytes` | `8388608` | WS 发送缓冲超过该值时暂停发送 |
| `nodes.circuit_open_ms` | `5000` | 熔断后保持开路的时长 |
| `nodes.circuit_failure_threshold` | `5` | 触发熔断的连续失败次数 |
| `assignment.ttl_ms` | `30000` | 返回的房间分配的有效期 |
| `assignment.reservation_shadow_ttl_ms` | `10000` | 本地影子预留的有效期 |
| `commands.timeout_ms` | `3000` | 节点命令 ack 超时 |
| `commands.max_pending` | `100000` | 最大未完成命令数 |
| `move.budget_per_cycle` | `8` | 每轮规划最多启动的迁移数 |
| `move.confirmation_cycles` | `2` | 房间名单/level 签名需连续保持不变的规划轮数，之后才可作为迁移源或目标 |
| `move.cluster_split_penalty` | `0.05` | 部分迁移每留下一个空间聚类同伴的额外收益惩罚 |
| `move.transaction_timeout_ms` | `15000` | 迁移事务停滞多久后中止 |
| `move.level_change_cooldown_ms` | `6000` | 玩家切图后的沉降窗口；窗口内该玩家不计入 level 分组、也不参与迁移决策 |
| `move.join_settle_ms` | `4000` | 玩家刚加入房间（好友传送、跨房迁移）后的沉降窗口 |
| `debug.api_token` | `""` | `/debug/*` 的 token；留空则不校验 |
| `debug.log_buffer_size` | `2000` | `GET /debug/logs` 使用的内存事件环形缓冲区大小 |

### 调试接口

`GET /debug/logs`、`GET /debug/allnode` 和 `POST /debug/move` 支持可选的 `debug.api_token`。
配置了 token 时，通过 `x-debug-token` 请求头或 `Authorization: Bearer <token>` 传入；留空则不校验。

`GET /debug/logs` 返回最近的结构化事件历史（内存环形缓冲区，重启即清空）：房间移动/合并/拆分、
level 隔离与整理、分配结果、房间与玩家生命周期、发往节点的命令及其 ack、telemetry 失步事件。

查询参数：

- `limit` — 取最新 N 条（默认 200，最大 1000）
- `since` — 只返回 `id` 大于该值的事件；配合返回的 `nextSince` 轮询
- `category` — `move`、`level`、`room`、`player`、`node`、`telemetry`、`allocation`
- `roomId` / `playerId` — 按房间或玩家过滤

```bash
curl -s localhost:1131/debug/logs | jq -r '.data.entries[] | "\(.at) \(.event) \(.message)"'
curl -s 'localhost:1131/debug/logs?category=move&limit=50'
curl -s 'localhost:1131/debug/logs?roomId=10000001'
```

返回结构为 `{ code, data: { entries, nextSince, oldestId, newestId, buffered, capacity, truncated } }`。
`truncated: true` 表示 `since` 早于缓冲区最旧记录，部分事件已经被覆盖。

## 连接 XYSky 与节点

运行 QWD 时涉及两个不同的地址：

- **XYSky → QWD：** XYSky 的 `udp.uri` 必须填写 QWD 的 HTTP(S) `/allocate` 地址，
  例如 `https://thatroom.example.cn/allocate`。
- **节点 → QWD：** 每个 XYSKY UDP 节点的 `qwd.url` 必须填写 QWD 的 **WebSocket** 地址，
  例如 `wss://thatroom.example.cc`。

节点在连接时注册自己的 `public_uri`，QWD 通过 `/allocate` 把该地址返回给客户端。请在你的反向代理
处按需终止 TLS / 转发 WebSocket 升级（路径 `/`）。

节点可以在升级 URL 上带一个稳定标识（`?v=1&node=<id>`）。QWD 在重连之间复用该 id，短暂的
socket 断开会在宽限期内恢复原有房间，而不是把它们回收掉。未携带该参数的节点会得到一个按连接
生成的 id。

## 许可证

[GNU General Public License v3.0](LICENSE)。
