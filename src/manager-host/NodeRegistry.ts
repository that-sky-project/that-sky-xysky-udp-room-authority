import type { NodeId, UnixMs } from "../types/branded.js";
import { asUnixMs } from "../types/branded.js";
import type { IManagerCommand, INodeAck, INodeEvent } from "../types/contracts.js";

export interface NodeTransport {
  readonly nodeId: NodeId;
  readonly protocolVersion: number;
  readonly connectedAt: UnixMs;
  readonly remoteAddress: string;
  readonly writable: boolean;
  send(command: IManagerCommand): Promise<void>;
  close(code?: number, reason?: string): void;
}

export interface NodeHealth {
  nodeId: NodeId;
  load: number;
  capacity: number;
  lastSeenAt: UnixMs;
  draining: boolean;
  connected: boolean;
  protocolVersion: number;
  backpressureBytes: number;
  consecutiveFailures: number;
  circuitOpenUntil: UnixMs;
}

export interface NodeRegistryOptions {
  circuitOpenMs: number;
  failureThreshold: number;
  backpressureLimitBytes: number;
}

export class NodeRegistry {
  private readonly transports = new Map<NodeId, NodeTransport>();
  private readonly health = new Map<NodeId, NodeHealth>();

  public constructor(private readonly options: NodeRegistryOptions) {}

  public register(transport: NodeTransport): void {
    this.transports.set(transport.nodeId, transport);
    this.health.set(transport.nodeId, {
      nodeId: transport.nodeId,
      load: 0,
      capacity: 0,
      lastSeenAt: asUnixMs(Date.now()),
      draining: false,
      connected: true,
      protocolVersion: transport.protocolVersion,
      backpressureBytes: 0,
      consecutiveFailures: 0,
      circuitOpenUntil: asUnixMs(0)
    });
  }

  public unregister(nodeId: NodeId): void {
    // A nodeId represents one live WebSocket connection. Once it closes, its
    // rooms must be reclaimed immediately; retaining a disconnected health
    // record makes stale rooms look allocatable after reconnects.
    this.transports.delete(nodeId);
    this.health.delete(nodeId);
  }

  public applyEvent(event: INodeEvent): void {
    const existing = this.health.get(event.data.nodeId);
    const next: NodeHealth = {
      nodeId: event.data.nodeId,
      load: event.data.load ?? existing?.load ?? 0,
      capacity: event.data.capacity ?? event.data.rooms?.length ?? existing?.capacity ?? 0,
      lastSeenAt: asUnixMs(Date.now()),
      draining: event.event === "node.draining" ? true : existing?.draining ?? false,
      connected: true,
      protocolVersion: event.v ?? existing?.protocolVersion ?? 1,
      backpressureBytes: existing?.backpressureBytes ?? 0,
      consecutiveFailures: existing?.consecutiveFailures ?? 0,
      circuitOpenUntil: existing?.circuitOpenUntil ?? asUnixMs(0)
    };
    this.health.set(event.data.nodeId, next);
  }

  public markDraining(nodeId: NodeId, draining: boolean): void {
    const existing = this.health.get(nodeId);
    if (!existing) return;
    this.health.set(nodeId, { ...existing, draining, lastSeenAt: asUnixMs(Date.now()) });
  }

  public getTransport(nodeId: NodeId): NodeTransport | undefined {
    return this.transports.get(nodeId);
  }

  public getHealth(nodeId: NodeId): NodeHealth | undefined {
    return this.health.get(nodeId);
  }

  public listHealth(): NodeHealth[] {
    return [...this.health.values()];
  }

  public recordBackpressure(nodeId: NodeId, bytes: number): void {
    const existing = this.health.get(nodeId);
    if (!existing) return;
    const circuitOpenUntil = bytes >= this.options.backpressureLimitBytes ? asUnixMs(Date.now() + this.options.circuitOpenMs) : existing.circuitOpenUntil;
    this.health.set(nodeId, { ...existing, backpressureBytes: bytes, circuitOpenUntil, lastSeenAt: asUnixMs(Date.now()) });
  }

  public recordCommandSuccess(nodeId: NodeId): void {
    const existing = this.health.get(nodeId);
    if (!existing) return;
    this.health.set(nodeId, { ...existing, consecutiveFailures: 0, circuitOpenUntil: asUnixMs(0), lastSeenAt: asUnixMs(Date.now()) });
  }

  public recordCommandFailure(nodeId: NodeId): void {
    const existing = this.health.get(nodeId);
    if (!existing) return;
    const consecutiveFailures = existing.consecutiveFailures + 1;
    const circuitOpenUntil =
      consecutiveFailures >= this.options.failureThreshold ? asUnixMs(Date.now() + this.options.circuitOpenMs) : existing.circuitOpenUntil;
    this.health.set(nodeId, { ...existing, consecutiveFailures, circuitOpenUntil, lastSeenAt: asUnixMs(Date.now()) });
  }

  public isAvailableForCommand(nodeId: NodeId): boolean {
    const health = this.health.get(nodeId);
    if (!health) return false;
    return health.connected && !health.draining && Date.now() >= health.circuitOpenUntil && health.backpressureBytes < this.options.backpressureLimitBytes;
  }

  public listHealthy(maxStaleMs: number): NodeHealth[] {
    const now = Date.now();
    return [...this.health.values()].filter((node) => {
      return (
        node.connected &&
        !node.draining &&
        node.capacity > 0 &&
        node.backpressureBytes < this.options.backpressureLimitBytes &&
        now >= node.circuitOpenUntil &&
        now - node.lastSeenAt <= maxStaleMs
      );
    });
  }

  public async broadcast(command: IManagerCommand): Promise<void> {
    await Promise.all([...this.transports.values()].filter((transport) => transport.writable).map((transport) => transport.send(command)));
  }

  public onAck(_ack: INodeAck): void {
    // Hook intentionally kept on the registry boundary for metrics/adapters.
  }
}
