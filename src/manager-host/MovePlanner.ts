import type { RoomSnapshot } from "../types/contracts.js";
import type { StoredTelemetry } from "./TelemetryStore.js";

export interface RoomActivity {
  roomId: string;
  occupancy: number;
  freshness: number;
  movement: number;
  social: number;
  spatial: number;
  activity: number;
  mergePressure: number;
}

export interface MovePlan {
  playerIds: string[];
  sourceRoomId: string;
  targetRoomId: string;
  gain: number;
  reason: "low_activity_merge" | "player_rebalance";
}

export interface MovePlannerOptions {
  distanceRadius: number;
  clusterRadius: number;
  maxStateAgeMs: number;
  moveCooldownMs: number;
  minimumGain: number;
  mergePressureThreshold: number;
  isolationPressureWeight: number;
  stalePressureWeight: number;
}

const clamp = (value: number): number => Math.max(0, Math.min(1, value));

const WEIGHT_BASE = { occupancy: 0.2, freshness: 0.25, movement: 0.2, social: 0.15, spatial: 0.2 };
type WeightField = keyof typeof WEIGHT_BASE;
const WEIGHT_FIELDS: WeightField[] = ["occupancy", "freshness", "movement", "social", "spatial"];

const DEFAULT_OPTIONS: MovePlannerOptions = {
  distanceRadius: 30,
  clusterRadius: 12,
  maxStateAgeMs: 5_000,
  moveCooldownMs: 30_000,
  minimumGain: 0.2,
  mergePressureThreshold: 0.55,
  isolationPressureWeight: 0.6,
  stalePressureWeight: 0.4
};

export class MovePlanner {
  private weights = { ...WEIGHT_BASE };
  private reliability: Record<WeightField, number> = { occupancy: 0.5, freshness: 0.5, movement: 0.5, social: 0.5, spatial: 0.5 };
  private readonly options: MovePlannerOptions;

  public constructor(options: Partial<MovePlannerOptions> = {}) {
    this.options = { ...DEFAULT_OPTIONS, ...options };
  }

  public getWeights(): Readonly<typeof this.weights> { return { ...this.weights }; }

  public activity(room: RoomSnapshot, players: StoredTelemetry[], now = Date.now()): RoomActivity {
    const occupancy = clamp(room.players / Math.max(1, room.capacity));
    if (players.length === 0) return { roomId: room.roomId, occupancy, freshness: 0, movement: 0, social: 0, spatial: 0, activity: 0, mergePressure: 0 };
    const freshness = players.reduce((sum, p) => sum + this.freshnessOf(p, now), 0) / players.length;
    const movement = players.reduce((sum, p) => sum + clamp(this.velocityOf(p) / 5), 0) / players.length;
    const social = this.averageSocial(players);
    const spatial = this.averageSpatial(players, now);
    const w = this.weights;
    const activity = freshness * (w.occupancy * occupancy + w.freshness * freshness + w.movement * movement + w.social * social + w.spatial * spatial);
    const occupiedFactor = occupancy > 0 ? 1 : 0;
    const cohesion = clamp(0.5 * social + 0.5 * spatial);
    const mergePressure = occupiedFactor * ((1 - freshness) * 0.65 + (1 - occupancy) * 0.2 + (1 - activity) * 0.15) * (1 - 0.35 * cohesion);
    return { roomId: room.roomId, occupancy, freshness, movement, social, spatial, activity, mergePressure };
  }
  // __PART3__

  public calibrate(rooms: RoomSnapshot[], byRoom: Map<string, StoredTelemetry[]>, now = Date.now()): void {
    const samples = rooms.map((room) => this.activity(room, byRoom.get(room.roomId) ?? [], now)).filter((sample) => sample.occupancy > 0);
    if (samples.length < 2) return;
    const variance = Object.fromEntries(WEIGHT_FIELDS.map((field) => {
      const mean = samples.reduce((sum, sample) => sum + sample[field], 0) / samples.length;
      return [field, samples.reduce((sum, sample) => sum + (sample[field] - mean) ** 2, 0) / samples.length];
    })) as Record<WeightField, number>;
    const maxVariance = Math.max(...WEIGHT_FIELDS.map((field) => variance[field]), 1e-6);
    const alpha = 0.05;
    for (const field of WEIGHT_FIELDS) {
      const signal = clamp(variance[field] / maxVariance);
      this.reliability[field] = this.reliability[field] + alpha * (signal - this.reliability[field]);
    }
    const raw = Object.fromEntries(WEIGHT_FIELDS.map((field) => [field, (this.reliability[field] + 0.25) * WEIGHT_BASE[field]])) as Record<WeightField, number>;
    const total = WEIGHT_FIELDS.reduce((sum, field) => sum + raw[field], 0);
    for (const field of WEIGHT_FIELDS) this.weights[field] = raw[field] / total;
  }
  // __PART4__

  public plan(rooms: RoomSnapshot[], byRoom: Map<string, StoredTelemetry[]>, now = Date.now()): MovePlan[] {
    const activities = rooms.map((room) => this.activity(room, byRoom.get(room.roomId) ?? [], now));
    const plans: MovePlan[] = [];
    for (const source of activities.filter((a) => a.mergePressure >= this.options.mergePressureThreshold)) {
      const sourceRoom = rooms.find((room) => room.roomId === source.roomId);
      const sourcePlayers = (byRoom.get(source.roomId) ?? []).filter((p) => !p.lastMigratedAt || now - p.lastMigratedAt >= this.options.moveCooldownMs);
      if (!sourceRoom || sourcePlayers.length === 0) continue;
      const sourceLevels = new Set(sourcePlayers.map((p) => p.levelId).filter((l): l is number => l !== undefined));
      const target = activities
        .filter((t) => t.roomId !== source.roomId && t.activity > source.activity + 0.05)
        .sort((a, b) => b.activity - a.activity)
        .map((candidate) => rooms.find((room) => room.roomId === candidate.roomId))
        .find((room) => {
          if (!room || room.capacity - room.players <= 0) return false;
          const tp = byRoom.get(room.roomId) ?? [];
          if (tp.length === 0 || sourceLevels.size === 0) return true;
          return tp.some((p) => p.levelId !== undefined && sourceLevels.has(p.levelId));
        });
      if (!target) continue;
      const targetPlayers = byRoom.get(target.roomId) ?? [];
      const capacity = Math.max(0, target.capacity - Math.max(target.players, targetPlayers.length));
      const targetActivity = this.activity(target, targetPlayers, now).activity;
      const groups = this.candidateGroups(sourcePlayers, Math.min(sourcePlayers.length, capacity), now);
      let best: MovePlan | undefined;
      for (const group of groups) {
        const gain = this.moveGain(group, sourcePlayers, source.activity, targetActivity, now);
        if (gain > (best?.gain ?? this.options.minimumGain)) best = { playerIds: group.map((p) => p.playerId), sourceRoomId: source.roomId, targetRoomId: target.roomId, gain, reason: group.length > 1 ? "low_activity_merge" : "player_rebalance" };
      }
      if (best) plans.push(best);
    }
    return plans.sort((a, b) => b.gain - a.gain);
  }
  // __PART5__

  private velocityOf(p: StoredTelemetry): number { return p.emaVelocity ?? p.velocity ?? 0; }

  private freshnessOf(p: StoredTelemetry, now: number): number {
    return Math.exp(-Math.max(0, now - (p.stateAt ?? p.receivedAt)) / this.options.maxStateAgeMs);
  }

  private freshPosition(p: StoredTelemetry, now: number): boolean {
    return Boolean(p.position) && now - (p.stateAt ?? p.receivedAt) <= this.options.maxStateAgeMs;
  }

  private distance(a: StoredTelemetry, b: StoredTelemetry, now: number): number | undefined {
    if (!a.position || !b.position) return undefined;
    if (a.levelId === undefined || a.levelId !== b.levelId) return undefined;
    if (!this.freshPosition(a, now) || !this.freshPosition(b, now)) return undefined;
    return Math.hypot(a.position[0] - b.position[0], a.position[1] - b.position[1], a.position[2] - b.position[2]);
  }

  private proximity(a: StoredTelemetry, b: StoredTelemetry, now: number): number | undefined {
    const d = this.distance(a, b, now);
    return d === undefined ? undefined : Math.exp(-d / this.options.distanceRadius);
  }

  private social(a: StoredTelemetry, b: StoredTelemetry): number {
    return clamp(a.affinity?.[b.playerId as string] ?? b.affinity?.[a.playerId as string] ?? 0);
  }

  private averageSocial(players: StoredTelemetry[]): number {
    let total = 0; let count = 0;
    for (const a of players) for (const b of players) if (a.playerId !== b.playerId) { total += this.social(a, b); count += 1; }
    return count ? total / count : 0;
  }

  private averageSpatial(players: StoredTelemetry[], now: number): number {
    let total = 0; let count = 0;
    for (const a of players) for (const b of players) if (a.playerId !== b.playerId) {
      const prox = this.proximity(a, b, now);
      if (prox !== undefined) { total += prox; count += 1; }
    }
    return count ? total / count : 0;
  }
  // __PART6__

  private cohesionPair(a: StoredTelemetry, b: StoredTelemetry, now: number): number {
    return clamp(0.5 * this.social(a, b) + 0.5 * (this.proximity(a, b, now) ?? 0));
  }

  private averageCohesion(players: StoredTelemetry[], now: number): number {
    let total = 0; let count = 0;
    for (const a of players) for (const b of players) if (a.playerId !== b.playerId) { total += this.cohesionPair(a, b, now); count += 1; }
    return count ? total / count : 0;
  }

  private isolation(player: StoredTelemetry, peers: StoredTelemetry[], now: number): number {
    if (!this.freshPosition(player, now)) return 0;
    let nearest = 0; let found = false;
    for (const peer of peers) {
      if (peer.playerId === player.playerId) continue;
      const prox = this.proximity(player, peer, now);
      if (prox === undefined) continue;
      found = true;
      if (prox > nearest) nearest = prox;
    }
    return found ? 1 - nearest : 0;
  }

  private effectivePressure(player: StoredTelemetry, peers: StoredTelemetry[], now: number): number {
    const iso = this.isolation(player, peers, now);
    const stale = 1 - this.freshnessOf(player, now);
    return clamp(this.options.isolationPressureWeight * iso + this.options.stalePressureWeight * stale);
  }

  private moveGain(group: StoredTelemetry[], source: StoredTelemetry[], sourceActivity: number, targetActivity: number, now: number): number {
    const before = this.averageCohesion(source, now);
    const afterSource = source.filter((player) => !group.includes(player));
    const after = this.averageCohesion(afterSource, now);
    const separationLoss = Math.max(0, before - after);
    const pressure = group.length ? group.reduce((sum, player) => sum + this.effectivePressure(player, source, now), 0) / group.length : 0;
    const sourceFreshness = source.length === 0 ? 0 : source.reduce((sum, player) => sum + this.freshnessOf(player, now), 0) / source.length;
    const staleCleanupBonus = 1 - sourceFreshness;
    return (targetActivity - sourceActivity) * 0.45 + staleCleanupBonus * 0.35 + pressure * 0.2 - separationLoss * 0.35 - 0.06 * group.length;
  }
  // __PART7__

  private candidateGroups(players: StoredTelemetry[], maxSize: number, now: number): StoredTelemetry[][] {
    const limit = Math.min(maxSize, players.length, 7);
    if (limit <= 0) return [];
    const groups: StoredTelemetry[][] = [];
    const seen = new Set<string>();
    const add = (group: StoredTelemetry[]): void => {
      if (group.length === 0 || group.length > limit) return;
      if (!this.respectsHardRelations(group, players)) return;
      const key = group.map((p) => p.playerId).slice().sort().join(",");
      if (seen.has(key)) return;
      seen.add(key);
      groups.push(group);
    };
    const valid = players.filter((p) => this.freshPosition(p, now));
    if (valid.length < 2) {
      for (const subset of this.enumerate(players, limit)) add(subset);
      return groups;
    }
    for (const cluster of this.clusterByDistance(valid, now)) for (const subset of this.enumerate(cluster, limit)) add(subset);
    for (const p of players) if (!this.freshPosition(p, now)) add([p]);
    if (groups.length === 0) for (const subset of this.enumerate(players, limit)) add(subset);
    return groups;
  }

  private enumerate(players: StoredTelemetry[], limit: number): StoredTelemetry[][] {
    const groups: StoredTelemetry[][] = [];
    const n = players.length;
    for (let mask = 1; mask < (1 << n); mask += 1) {
      const group = players.filter((_player, index) => (mask & (1 << index)) !== 0);
      if (group.length <= limit) groups.push(group);
    }
    return groups;
  }
  // __PART8__

  private clusterByDistance(players: StoredTelemetry[], now: number): StoredTelemetry[][] {
    const n = players.length;
    const visited = new Array<boolean>(n).fill(false);
    const clusters: StoredTelemetry[][] = [];
    for (let i = 0; i < n; i += 1) {
      if (visited[i]) continue;
      visited[i] = true;
      const stack = [i];
      const component: StoredTelemetry[] = [];
      while (stack.length) {
        const k = stack.pop();
        if (k === undefined) break;
        const pk = players[k];
        if (!pk) continue;
        component.push(pk);
        for (let j = 0; j < n; j += 1) {
          if (visited[j]) continue;
          const pj = players[j];
          if (!pj) continue;
          const d = this.distance(pk, pj, now);
          if (d !== undefined && d < this.options.clusterRadius) { visited[j] = true; stack.push(j); }
        }
      }
      clusters.push(component);
    }
    return clusters;
  }

  private respectsHardRelations(group: StoredTelemetry[], all: StoredTelemetry[]): boolean {
    for (const player of group) for (const other of all) {
      if (player.playerId === other.playerId) continue;
      const affinity = Math.max(player.affinity?.[other.playerId as string] ?? 0, other.affinity?.[player.playerId as string] ?? 0);
      // In-game affinity is a cohesion constraint: strongly bonded players must move together.
      if (affinity >= 0.9 && !group.includes(other)) return false;
    }
    return true;
  }
}
