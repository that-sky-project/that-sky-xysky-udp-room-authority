import type { RoomSnapshot } from "../types/contracts.js";
import type { StoredTelemetry } from "./TelemetryStore.js";

export interface RoomActivity {
  roomId: string;
  occupancy: number;
  freshness: number;
  movement: number;
  interaction: number;
  cohesion: number;
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
  maxStateAgeMs: number;
  moveCooldownMs: number;
  minimumGain: number;
}

const clamp = (value: number): number => Math.max(0, Math.min(1, value));

export class MovePlanner {
  private weights = { occupancy: 0.2, freshness: 0.25, movement: 0.2, interaction: 0.2, cohesion: 0.15 };
  private reliability = { occupancy: 0.5, freshness: 0.5, movement: 0.5, interaction: 0.5, cohesion: 0.5 };
  public constructor(private readonly options: MovePlannerOptions = {
    distanceRadius: 30,
    maxStateAgeMs: 5_000,
    moveCooldownMs: 30_000,
    minimumGain: 0.2
  }) {}

  public activity(room: RoomSnapshot, players: StoredTelemetry[], now = Date.now()): RoomActivity {
    // Occupancy comes from the node-authoritative room snapshot. Telemetry can
    // be delayed; using its sample size here would turn a full room into a
    // falsely empty merge candidate during a temporary reporting gap.
    const occupancy = clamp(room.players / Math.max(1, room.capacity));
    if (players.length === 0) return { roomId: room.roomId, occupancy, freshness: 0, movement: 0, interaction: 0, cohesion: 0, activity: 0, mergePressure: 0 };
    const freshness = players.reduce((sum, p) => sum + Math.exp(-Math.max(0, now - (p.stateAt ?? p.receivedAt)) / this.options.maxStateAgeMs), 0) / players.length;
    const movement = players.reduce((sum, p) => sum + clamp((p.velocity ?? 0) / 5), 0) / players.length;
    const interaction = this.averageRelation(players, players, now);
    const cohesion = interaction;
    const activity = freshness * (this.weights.occupancy * occupancy + this.weights.freshness * freshness + this.weights.movement * movement + this.weights.interaction * interaction + this.weights.cohesion * cohesion);
    // Stale telemetry is itself evidence of an inactive room. Occupancy keeps
    // empty rooms out of migration planning, while stale occupied rooms become
    // easier to drain into a fresh destination.
    const occupiedFactor = occupancy > 0 ? 1 : 0;
    const mergePressure = occupiedFactor * ((1 - freshness) * 0.65 + (1 - occupancy) * 0.2 + (1 - activity) * 0.15) * (1 - 0.35 * cohesion);
    return { roomId: room.roomId, occupancy, freshness, movement, interaction, cohesion, activity, mergePressure };
  }

  public calibrate(rooms: RoomSnapshot[], byRoom: Map<string, StoredTelemetry[]>, now = Date.now()): void {
    const samples = rooms.map((room) => this.activity(room, byRoom.get(room.roomId) ?? [], now)).filter((sample) => sample.occupancy > 0);
    if (samples.length < 2) return;
    const fields = ["occupancy", "freshness", "movement", "interaction", "cohesion"] as const;
    const variance = Object.fromEntries(fields.map((field) => {
      const mean = samples.reduce((sum, sample) => sum + sample[field], 0) / samples.length;
      return [field, samples.reduce((sum, sample) => sum + (sample[field] - mean) ** 2, 0) / samples.length];
    })) as Record<typeof fields[number], number>;
    const maxVariance = Math.max(...fields.map((field) => variance[field]), 1e-6);
    const alpha = 0.05;
    for (const field of fields) {
      const signal = clamp(variance[field] / maxVariance);
      this.reliability[field] = this.reliability[field] + alpha * (signal - this.reliability[field]);
    }
    const raw = Object.fromEntries(fields.map((field) => [field, (this.reliability[field] + 0.25) * ({ occupancy: 0.2, freshness: 0.25, movement: 0.2, interaction: 0.2, cohesion: 0.15 }[field])])) as Record<typeof fields[number], number>;
    const total = fields.reduce((sum, field) => sum + raw[field], 0);
    for (const field of fields) this.weights[field] = raw[field] / total;
  }

  public getWeights(): Readonly<typeof this.weights> { return { ...this.weights }; }

  public plan(rooms: RoomSnapshot[], byRoom: Map<string, StoredTelemetry[]>, now = Date.now()): MovePlan[] {
    const activities = rooms.map((room) => this.activity(room, byRoom.get(room.roomId) ?? [], now));
    const plans: MovePlan[] = [];
    for (const source of activities.filter((activity) => activity.mergePressure >= 0.55)) {
      const sourceRoom = rooms.find((room) => room.roomId === source.roomId);
      const sourcePlayers = (byRoom.get(source.roomId) ?? []).filter((player) => !player.lastMigratedAt || now - player.lastMigratedAt >= this.options.moveCooldownMs);
      if (!sourceRoom || sourcePlayers.length === 0) continue;
      const sourceLevels = new Set(sourcePlayers.map((player) => player.levelId).filter((level): level is number => level !== undefined));
      const targets = activities.filter((target) => target.roomId !== source.roomId && target.activity > source.activity + 0.05)
        .sort((a, b) => b.activity - a.activity);
      const target = targets.map((candidate) => rooms.find((room) => room.roomId === candidate.roomId)).find((room) => {
        if (!room || room.capacity - room.players <= 0) return false;
        const targetPlayers = byRoom.get(room.roomId) ?? [];
        if (targetPlayers.length === 0 || sourceLevels.size === 0) return true;
        return targetPlayers.some((player) => player.levelId !== undefined && sourceLevels.has(player.levelId));
      });
      if (!target) continue;
      const targetPlayers = byRoom.get(target.roomId) ?? [];
      const capacity = Math.max(0, target.capacity - Math.max(target.players, targetPlayers.length));
      const groups = this.candidateGroups(sourcePlayers, Math.min(sourcePlayers.length, capacity));
      let best: MovePlan | undefined;
      for (const group of groups) {
        const gain = this.moveGain(group, sourcePlayers, source.activity, this.activity(target, targetPlayers, now).activity, now);
        if (gain > (best?.gain ?? this.options.minimumGain)) best = { playerIds: group.map((p) => p.playerId), sourceRoomId: source.roomId, targetRoomId: target.roomId, gain, reason: group.length > 1 ? "low_activity_merge" : "player_rebalance" };
      }
      if (best) plans.push(best);
    }
    return plans.sort((a, b) => b.gain - a.gain);
  }

  private relation(a: StoredTelemetry, b: StoredTelemetry, now: number): number {
    const friend = a.friendIds?.includes(b.playerId) || b.friendIds?.includes(a.playerId) ? 1 : 0;
    const affinity = clamp(a.affinity?.[b.playerId as string] ?? b.affinity?.[a.playerId as string] ?? 0);
    const sameLevel = a.levelId !== undefined && a.levelId === b.levelId ? 1 : 0;
    let proximity = 0;
    if (a.position && b.position && a.levelId === b.levelId && now - (a.stateAt ?? a.receivedAt) <= this.options.maxStateAgeMs && now - (b.stateAt ?? b.receivedAt) <= this.options.maxStateAgeMs) {
      const distance = Math.hypot(a.position[0] - b.position[0], a.position[1] - b.position[1], a.position[2] - b.position[2]);
      proximity = Math.exp(-distance / this.options.distanceRadius);
    }
    return sameLevel * (0.45 * friend + 0.25 * affinity + 0.30 * proximity);
  }

  private averageRelation(left: StoredTelemetry[], right: StoredTelemetry[], now: number): number {
    let total = 0; let count = 0;
    for (const a of left) for (const b of right) if (a.playerId !== b.playerId) { total += this.relation(a, b, now); count += 1; }
    return count ? total / count : 0;
  }

  private candidateGroups(players: StoredTelemetry[], maxSize: number): StoredTelemetry[][] {
    const groups: StoredTelemetry[][] = [];
    const limit = Math.min(maxSize, players.length, 7);
    for (let mask = 1; mask < (1 << players.length); mask += 1) {
      const group = players.filter((_player, index) => (mask & (1 << index)) !== 0);
      if (group.length <= limit && this.respectsHardRelations(group, players)) groups.push(group);
    }
    return groups;
  }

  private respectsHardRelations(group: StoredTelemetry[], all: StoredTelemetry[]): boolean {
    for (const player of group) for (const other of all) {
      if (player.playerId === other.playerId) continue;
      const formalFriend = Boolean(player.friendIds?.includes(other.playerId) || other.friendIds?.includes(player.playerId));
      const affinity = Math.max(player.affinity?.[other.playerId as string] ?? 0, other.affinity?.[player.playerId as string] ?? 0);
      // Relationship is a cohesion constraint, not a reason to select a target.
      // Strongly connected players must move together or remain together.
      const hard = (formalFriend && affinity >= 0.75) || affinity >= 0.9;
      if (hard && !group.includes(other)) return false;
    }
    return true;
  }

  private moveGain(group: StoredTelemetry[], source: StoredTelemetry[], sourceActivity: number, targetActivity: number, now: number): number {
    const before = this.averageRelation(source, source, now);
    const afterSource = source.filter((player) => !group.includes(player));
    const after = this.averageRelation(afterSource, afterSource, now);
    const pressure = group.reduce((sum, player) => sum + player.migrationPressure, 0) / group.length;
    const sourceFreshness = source.length === 0 ? 0 : source.reduce((sum, player) => sum + Math.exp(-Math.max(0, now - (player.stateAt ?? player.receivedAt)) / this.options.maxStateAgeMs), 0) / source.length;
    const staleCleanupBonus = 1 - sourceFreshness;
    // Social affinity only contributes as a penalty when a move would split a
    // cohesive source group. It never rewards moving toward known players.
    const separationLoss = Math.max(0, before - after);
    return (targetActivity - sourceActivity) * 0.45 + staleCleanupBonus * 0.35 + pressure * 0.2 - separationLoss * 0.35 - 0.06 * group.length;
  }
}
