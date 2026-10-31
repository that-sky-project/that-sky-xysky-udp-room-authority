import type { WebSocket } from 'ws'
import type { Logger } from '../infra/logger/logger.js'
export interface HeartbeatMonitorOptions {
  intervalMs: number
  timeoutMs: number
  onDead: (nodeId: string) => void
}
interface TrackedSocket {
  ws: WebSocket
  lastPongAt: number
  lastPingAt: number
}
export class HeartbeatMonitor {
  private readonly sockets = new Map<string, TrackedSocket>()
  private timer: NodeJS.Timeout | undefined
  public constructor (
    private readonly options: HeartbeatMonitorOptions,
    private readonly log: Logger
  ) {}
  public track (nodeId: string, ws: WebSocket): void {
    const tracked: TrackedSocket = { ws, lastPongAt: Date.now(), lastPingAt: 0 }
    this.sockets.set(nodeId, tracked)
    ws.on('pong', () => {
      tracked.lastPongAt = Date.now()
    })
  }
  public untrack (nodeId: string, ws?: WebSocket): void {
    if (ws) {
      const tracked = this.sockets.get(nodeId)
      if (tracked && tracked.ws !== ws) return
    }
    this.sockets.delete(nodeId)
  }
  public start (): void {
    if (this.timer) return
    this.timer = setInterval(() => this.tick(), this.options.intervalMs)
    this.timer.unref?.()
  }
  public stop (): void {
    if (this.timer) clearInterval(this.timer)
    this.timer = undefined
    this.sockets.clear()
  }
  public tick (now = Date.now()): void {
    for (const [nodeId, tracked] of this.sockets) {
      if (tracked.ws.readyState !== 1) continue
      const silentFor = now - tracked.lastPongAt
      if (silentFor > this.options.timeoutMs) {
        this.log.warn(
          { nodeId, silentForMs: silentFor },
          'node failed heartbeat; terminating connection'
        )
        this.sockets.delete(nodeId)
        try {
          tracked.ws.terminate()
        } finally {
          this.options.onDead(nodeId)
        }
        continue
      }
      try {
        tracked.ws.ping()
      } catch (error) {
        this.log.debug({ err: error, nodeId }, 'heartbeat ping failed')
      }
    }
  }
}
