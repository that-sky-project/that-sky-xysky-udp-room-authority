import type { Logger } from "../infra/logger/logger.js";

interface CounterState {
  help: string;
  labels: string;
  values: Map<string, number>;
}

/**
 * Minimal Prometheus text-format exporter. Counters and gauges only — the
 * manager only needs to answer "is the cluster healthy and are migrations
 * working", not to be a full metrics platform.
 *
 * A metric name is declared once with its label names; `inc`/`set` take label
 * values in the same order.
 */
export class MetricsRegistry {
  private readonly counters = new Map<string, CounterState>();
  private readonly gauges = new Map<string, CounterState>();
  private readonly series: Array<{ name: string; kind: "counter" | "gauge"; help: string; labels: string[]; value: number; tag: string }> = [];

  public declareCounter(name: string, help: string, labels: string[] = []): void {
    if (!this.counters.has(name)) this.counters.set(name, { help, labels: labels.join(","), values: new Map() });
  }

  public declareGauge(name: string, help: string, labels: string[] = []): void {
    if (!this.gauges.has(name)) this.gauges.set(name, { help, labels: labels.join(","), values: new Map() });
  }

  public inc(name: string, labelValues: string[] = [], delta = 1): void {
    this.add(this.counters, name, "counter", labelValues, delta);
  }

  public set(name: string, value: number, labelValues: string[] = []): void {
    const state = this.gauges.get(name);
    if (!state) return;
    state.values.set(this.tagOf(state.labels, labelValues), value);
  }

  public render(): string {
    const out: string[] = [];
    for (const [name, state] of [...this.counters, ...this.gauges]) {
      const kind = this.counters.has(name) ? "counter" : "gauge";
      out.push(`# HELP ${name} ${state.help}`);
      out.push(`# TYPE ${name} ${kind}`);
      const labels = this.parseLabels(state.labels);
      for (const [tag, value] of state.values) {
        const rendered = this.renderTag(labels, tag);
        out.push(`${name}${rendered} ${value}`);
      }
    }
    return `${out.join("\n")}\n`;
  }

  private add(
    registry: Map<string, CounterState>,
    name: string,
    _kind: "counter" | "gauge",
    labelValues: string[],
    delta: number
  ): void {
    const state = registry.get(name);
    if (!state) return;
    const tag = this.tagOf(state.labels, labelValues);
    state.values.set(tag, (state.values.get(tag) ?? 0) + delta);
  }

  private tagOf(labels: string, labelValues: string[]): string {
    if (!labels) return "";
    return labelValues.map((value) => encodeURIComponent(value)).join("|");
  }

  private parseLabels(labels: string): string[] {
    return labels ? labels.split(",") : [];
  }

  private renderTag(labels: string[], tag: string): string {
    if (labels.length === 0) return "";
    const values = tag.split("|");
    const pairs = labels.map((label, index) => `${label}="${decodeURIComponent(values[index] ?? "")}"`);
    return `{${pairs.join(",")}}`;
  }
}

/** Registers the metric set the manager exposes. */
export const registerManagerMetrics = (metrics: MetricsRegistry, _log: Logger): void => {
  metrics.declareCounter("qwd_ws_queue_dropped_total", "Telemetry frames dropped because the node queue was full");
  metrics.declareCounter("qwd_node_reclaimed_total", "Nodes reclaimed after their disconnect grace window expired");
  metrics.declareCounter("qwd_node_heartbeat_dead_total", "Nodes terminated by a missed heartbeat");
  metrics.declareCounter("qwd_move_total", "Move transactions by reason and terminal status", ["reason", "status"]);
  metrics.declareCounter("qwd_telemetry_stale_total", "Telemetry snapshots discarded due to a non-monotonic sequence");
  metrics.declareCounter("qwd_telemetry_gap_total", "Delta telemetry frames discarded because their base sequence was unknown");
  metrics.declareGauge("qwd_rooms", "Rooms known to the manager");
  metrics.declareGauge("qwd_players", "Tracked players");
  metrics.declareGauge("qwd_level_mix_violations", "Rooms currently holding more than one level");
  metrics.declareGauge("qwd_players_stranded", "Players that want to be isolated but have no target room");
  metrics.declareGauge("qwd_telemetry_gap_rooms", "Rooms whose delta telemetry is currently out of sync");
};
