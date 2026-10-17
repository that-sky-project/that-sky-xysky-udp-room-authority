import type { PlayerId } from "../types/branded.js";
import type { StoredTelemetry } from "./TelemetryStore.js";

export interface SpatialCluster {
  levelId: number;
  center: [number, number, number];
  radius: number;
  players: StoredTelemetry[];
  avgVelocity: number;
  density: number; // players.length / (4/3 * π * radius³)
}

export interface SpatialClusterDetectorOptions {
  clusterRadius: number;
  minClusterSize: number;
  maxVelocityForStatic: number;
}

export class SpatialClusterDetector {
  private readonly options: SpatialClusterDetectorOptions;

  public constructor(options: Partial<SpatialClusterDetectorOptions> = {}) {
    this.options = {
      clusterRadius: options.clusterRadius ?? 15,
      minClusterSize: options.minClusterSize ?? 3,
      maxVelocityForStatic: options.maxVelocityForStatic ?? 2
    };
  }

  public detectClusters(
    players: StoredTelemetry[],
    levelId: number,
    now = Date.now()
  ): SpatialCluster[] {
    const candidates = this.filterStaticPlayers(players, levelId, now);

    if (candidates.length < this.options.minClusterSize) {
      return [];
    }

    const clusters: SpatialCluster[] = [];
    const assigned = new Set<PlayerId>();

    while (candidates.length - assigned.size >= this.options.minClusterSize) {
      const cluster = this.findDensestCluster(candidates, assigned);

      if (!cluster || cluster.players.length < this.options.minClusterSize) {
        break;
      }

      clusters.push(cluster);
      cluster.players.forEach(p => assigned.add(p.playerId));
    }

    return clusters.sort((a, b) => b.density - a.density);
  }

  public findLargestCluster(clusters: SpatialCluster[]): SpatialCluster | undefined {
    if (clusters.length === 0) return undefined;
    return clusters.reduce((max, cluster) =>
      cluster.players.length > max.players.length ? cluster : max
    );
  }

  public isNearHotspot(
    player: StoredTelemetry,
    hotspot: SpatialCluster,
    tolerance = 1.5
  ): boolean {
    if (!player.position) return false;
    const dist = this.distance(player.position, hotspot.center);
    return dist < hotspot.radius * tolerance;
  }

  public evaluateHotspotQuality(
    room: { roomId: string; players: StoredTelemetry[] },
    levelId: number
  ): { score: number; largestHotspot?: SpatialCluster; totalLevelPlayers: number } {
    const levelPlayers = room.players.filter(p => p.levelId === levelId);

    if (levelPlayers.length === 0) {
      return { score: 0, totalLevelPlayers: 0 };
    }

    const clusters = this.detectClusters(room.players, levelId);
    const largestHotspot = this.findLargestCluster(clusters);

    if (!largestHotspot) {
      return { score: 0, totalLevelPlayers: levelPlayers.length };
    }

    const hotspotRatio = largestHotspot.players.length / levelPlayers.length;
    const densityFactor = Math.min(1, largestHotspot.density / 0.01); // 0.01 是归一化因子
    const score = hotspotRatio * (0.7 + densityFactor * 0.3);

    return {
      score: Math.min(1, score),
      largestHotspot,
      totalLevelPlayers: levelPlayers.length
    };
  }


  private filterStaticPlayers(
    players: StoredTelemetry[],
    levelId: number,
    now: number
  ): StoredTelemetry[] {
    return players.filter(player => {
      if (player.levelId !== levelId) return false;

      if (!player.position || player.position.length !== 3) return false;

      if (now - player.receivedAt > 10_000) return false;

      const velocity = player.emaVelocity ?? player.velocity ?? 0;
      if (velocity > this.options.maxVelocityForStatic) return false;

      return true;
    });
  }

  private findDensestCluster(
    candidates: StoredTelemetry[],
    assigned: Set<PlayerId>
  ): SpatialCluster | undefined {
    let bestCluster: SpatialCluster | undefined;
    let bestDensity = 0;

    for (const seed of candidates) {
      if (assigned.has(seed.playerId)) continue;

      const cluster = this.growClusterFromSeed(seed, candidates, assigned);

      if (cluster && cluster.density > bestDensity) {
        bestCluster = cluster;
        bestDensity = cluster.density;
      }
    }

    return bestCluster;
  }

  private growClusterFromSeed(
    seed: StoredTelemetry,
    candidates: StoredTelemetry[],
    assigned: Set<PlayerId>
  ): SpatialCluster | undefined {
    let center = seed.position!;
    let clusterPlayers: StoredTelemetry[] = [];
    let prevSize = 0;

    for (let iter = 0; iter < 5; iter++) {
      clusterPlayers = candidates.filter(player => {
        if (assigned.has(player.playerId)) return false;
        const dist = this.distance(player.position!, center);
        return dist < this.options.clusterRadius;
      });

      if (clusterPlayers.length === prevSize) {
        break;
      }
      prevSize = clusterPlayers.length;

      if (clusterPlayers.length > 0) {
        center = this.calculateCenter(clusterPlayers.map(p => p.position!));
      }
    }

    if (clusterPlayers.length < this.options.minClusterSize) {
      return undefined;
    }

    const avgVelocity = clusterPlayers.reduce((sum, p) => sum + (p.emaVelocity ?? p.velocity ?? 0), 0) / clusterPlayers.length;

    const volume = (4 / 3) * Math.PI * Math.pow(this.options.clusterRadius, 3);
    const density = clusterPlayers.length / volume;

    return {
      levelId: seed.levelId!,
      center,
      radius: this.options.clusterRadius,
      players: clusterPlayers,
      avgVelocity,
      density
    };
  }

  private calculateCenter(positions: Array<[number, number, number]>): [number, number, number] {
    if (positions.length === 0) {
      return [0, 0, 0];
    }

    const sum = positions.reduce(
      (acc, pos) => [acc[0] + pos[0], acc[1] + pos[1], acc[2] + pos[2]],
      [0, 0, 0]
    );

    return [
      sum[0] / positions.length,
      sum[1] / positions.length,
      sum[2] / positions.length
    ];
  }

  private distance(a: [number, number, number], b: [number, number, number]): number {
    return Math.sqrt(
      Math.pow(a[0] - b[0], 2) +
      Math.pow(a[1] - b[1], 2) +
      Math.pow(a[2] - b[2], 2)
    );
  }
}
