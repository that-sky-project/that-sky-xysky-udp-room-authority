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
  transaction_timeout_ms: 15000

debug:
  api_token: ""
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
| `move.transaction_timeout_ms` | `15000` | 迁移事务停滞多久后中止 |
| `debug.api_token` | `""` | `/debug/*` 的 token；为空则保持关闭 |

### 调试接口

`GET /debug/allnode` 和 `POST /debug/move` 需要 `debug.api_token`。设置后，通过 `x-debug-token`
请求头或 `Authorization: Bearer <token>` 请求头传入。留空则这些接口不可访问。

## 连接 XYSky 与节点

运行 QWD 时涉及两个不同的地址：

- **XYSky → QWD：** XYSky 的 `udp.uri` 必须填写 QWD 的 HTTP(S) `/allocate` 地址，
  例如 `https://thatroom.example.cn/allocate`。
- **节点 → QWD：** 每个 XYSKY UDP 节点的 `qwd.url` 必须填写 QWD 的 **WebSocket** 地址，
  例如 `wss://thatroom.example.cc`。

节点在连接时注册自己的 `public_uri`，QWD 通过 `/allocate` 把该地址返回给客户端。请在你的反向代理
处按需终止 TLS / 转发 WebSocket 升级（路径 `/`）。

## 许可证

[GNU General Public License v3.0](LICENSE)。
