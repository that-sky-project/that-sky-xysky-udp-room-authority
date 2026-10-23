import type { WebSocket } from "ws";
import type { Logger } from "../infra/logger/logger.js";

export interface HeartbeatMonitorOptions {
  /** Interval between server-initiated pings. */
  intervalMs: number;
  /** A node that has not answered within this window is considered dead. */
  timeoutMs: number;
  /** Called for every node whose connection is deemed dead. */
  onDead: (nodeId: string) => void;
}

interface TrackedSocket {
  ws: WebSocket;
  lastPongAt: number;
  lastPingAt: number;
}

/**
 * Server-side application heartbeat for node connections. WebSocket/TCP alone
 * cannot detect half-open connections (NAT expiry, silent network partition):
 * the node keeps sending telemetry into a void while the manager keeps a
 * phantom entry. Pinging on a fixed interval and requiring a pong inside a
 * bounded window detects both directions within `intervalMs + timeoutMs`.
 */
export class HeartbeatMonitor {
  private readonly sockets = new Map<string, TrackedSocket>();
  private timer: NodeJS.Timeout | undefined;

  public constructor(
    private readonly options: HeartbeatMonitorOptions,
    private readonly log: Logger
  ) {}

  public track(nodeId: string, ws: WebSocket): void {
    const tracked: TrackedSocket = { ws, lastPongAt: Date.now(), lastPingAt: 0 };
    this.sockets.set(nodeId, tracked);
    ws.on("pong", () => {
      // A pong proves the link was alive at the moment we last pinged, so it
      // refreshes the clock even if the reply arrives slightly after the tick.
      tracked.lastPongAt = Date.now();
    });
  }

  public untrack(nodeId: string): void {
    this.sockets.delete(nodeId);
  }

  public start(): void {
    if (this.timer) return;
    this.timer = setInterval(() => this.tick(), this.options.intervalMs);
    this.timer.unref?.();
  }

  public stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
    this.sockets.clear();
  }

  /** Exposed for tests: one heartbeat sweep. */
  public tick(now = Date.now()): void {
    for (const [nodeId, tracked] of this.sockets) {
      if (tracked.ws.readyState !== 1 /* WebSocket.OPEN */) continue;
      // Ping first: a pong may be in flight from the previous round, and a node
      // that answers must never be declared dead. The timeout only trips when
      // the silence window has already elapsed *before* this ping.
      const silentFor = now - tracked.lastPongAt;
      if (silentFor > this.options.timeoutMs) {
        this.log.warn({ nodeId, silentForMs: silentFor }, "node failed heartbeat; terminating connection");
        this.sockets.delete(nodeId);
        try {
          tracked.ws.terminate();
        } finally {
          this.options.onDead(nodeId);
        }
        continue;
      }
      try {
        tracked.ws.ping();
      } catch (error) {
        this.log.debug({ err: error, nodeId }, "heartbeat ping failed");
      }
    }
  }
}
