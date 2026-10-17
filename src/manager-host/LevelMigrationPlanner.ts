import type { RoomId, PlayerId } from "../types/branded.js";
import { asPlayerId } from "../types/branded.js";
import type { RoomSnapshot } from "../types/contracts.js";
import type { StoredTelemetry, TelemetryStore } from "./TelemetryStore.js";
import { SpatialClusterDetector, type SpatialCluster } from "./SpatialClusterDetector.js";


export interface LevelMigrationPlan {
  playerIds: string[];
  sourceRoomId: string;
  targetRoomId: string;
  levelId: number;
  sourceLevelCount: number;
  targetLevelCount: number;
  targetHotspot?: SpatialCluster | undefined;  // 目标房间的热点信息
  hotspotQuality?: number | undefined;         // 热点质量评分 0-1
  reason: "level_consolidation";
}

export interface LevelMigrationOptions {
  /** 源房间该 level 玩家少于此数量时考虑迁移 */
  minSourceLevelPlayers: number;
  /** 目标房间该 level 玩家至少要有此数量才考虑作为目标 */
  minTargetLevelPlayers: number;
  /** 玩家切换 level 后的冷却时间（避免频繁触发） */
  levelChangeCooldownMs: number;
  /** 最大迁移玩家数 */
  maxPlayersPerMove: number;
}

const DEFAULT_OPTIONS: LevelMigrationOptions = {
  minSourceLevelPlayers: 2,
  minTargetLevelPlayers: 3,
  levelChangeCooldownMs: 5_000,
  maxPlayersPerMove: 4
};

export class LevelMigrationPlanner {
  private readonly options: LevelMigrationOptions;
  private readonly lastLevelChangeTime = new Map<string, number>();
  private readonly spatialDetector: SpatialClusterDetector;

  public constructor(options: Partial<LevelMigrationOptions> = {}) {
    this.options = { ...DEFAULT_OPTIONS, ...options };
    // 初始化空间聚类检测器
    this.spatialDetector = new SpatialClusterDetector({
      clusterRadius: 15,           // 15 个游戏单位内算一个聚类
      minClusterSize: 3,           // 至少 3 人才算热点
      maxVelocityForStatic: 2      // 速度 < 2 才算静止
    });
  }

  public onPlayerLevelChange(playerId: string, newLevelId: number, now = Date.now()): boolean {
    const lastChange = this.lastLevelChangeTime.get(playerId);
    if (lastChange && now - lastChange < this.options.levelChangeCooldownMs) {
      return false;
    }
    this.lastLevelChangeTime.set(playerId, now);
    return true;
  }

  /**
   * 为特定玩家规划 level 聚合迁移
   *
   * @param playerId 触发迁移的玩家 ID
   * @param rooms 所有房间快照
   * @param telemetry 遥测存储
   * @param now 当前时间戳
   */
  public planForPlayer(
    playerId: string,
    rooms: RoomSnapshot[],
    telemetry: TelemetryStore,
    now = Date.now()
  ): LevelMigrationPlan | undefined {
    const player = telemetry.get(asPlayerId(playerId));
    if (!player || player.levelId === undefined) {
      return undefined;
    }

    const sourceRoom = rooms.find((room) => room.roomId === player.roomId);
    if (!sourceRoom) {
      return undefined;
    }

    const levelId = player.levelId;
    const sourceRoomPlayers = telemetry.listRoom(sourceRoom.roomId, now);
    const sourceLevelPlayers = sourceRoomPlayers.filter((p) => p.levelId === levelId);

    if (sourceLevelPlayers.length > this.options.minSourceLevelPlayers) {
      return undefined;
    }

    const candidateEvaluations = rooms
      .filter((room) => room.roomId !== sourceRoom.roomId)
      .map((room) => {
        const roomPlayers = telemetry.listRoom(room.roomId, now);
        const evaluation = this.spatialDetector.evaluateHotspotQuality(
          { roomId: room.roomId, players: roomPlayers },
          levelId
        );

        return {
          room,
          roomPlayers,
          ...evaluation
        };
      })
      .filter((e) => e.totalLevelPlayers >= this.options.minTargetLevelPlayers)
      .sort((a, b) => {
        const scoreA = a.score * 10 + a.totalLevelPlayers * 0.1;
        const scoreB = b.score * 10 + b.totalLevelPlayers * 0.1;
        return scoreB - scoreA;
      });

    if (candidateEvaluations.length === 0) {
      return undefined;
    }

    const bestCandidate = candidateEvaluations[0];
    if (!bestCandidate) {
      return undefined;
    }

    const targetRoom = bestCandidate.room;

    const availableSlots = targetRoom.capacity - targetRoom.players;
    const migrationSize = Math.min(
      sourceLevelPlayers.length,
      this.options.maxPlayersPerMove,
      availableSlots
    );

    if (migrationSize === 0) {
      return undefined;
    }

    const candidatePlayers = this.selectMigrationCandidates(
      sourceLevelPlayers,
      sourceRoomPlayers,
      bestCandidate.roomPlayers,
      availableSlots,
      bestCandidate.largestHotspot,
      now
    );

    if (candidatePlayers.length === 0) {
      return undefined;
    }

    return {
      playerIds: candidatePlayers.map((p) => p.playerId as string),
      sourceRoomId: sourceRoom.roomId,
      targetRoomId: targetRoom.roomId,
      levelId,
      sourceLevelCount: sourceLevelPlayers.length,
      targetLevelCount: bestCandidate.totalLevelPlayers,
      targetHotspot: bestCandidate.largestHotspot,
      hotspotQuality: bestCandidate.score,
      reason: "level_consolidation"
    };
  }

  private selectMigrationCandidates(
    sourceLevelPlayers: StoredTelemetry[],
    allSourcePlayers: StoredTelemetry[],
    targetPlayers: StoredTelemetry[],
    availableSlots: number,
    targetHotspot: SpatialCluster | undefined,
    now: number
  ): StoredTelemetry[] {
    const maxSize = Math.min(
      sourceLevelPlayers.length,
      this.options.maxPlayersPerMove,
      availableSlots
    );

    if (maxSize <= 0) {
      return [];
    }

    const scored = sourceLevelPlayers.map((player) => ({
      player,
      score: this.calculateMigrationScore(player, allSourcePlayers, targetPlayers, targetHotspot, now)
    }));

    scored.sort((a, b) => b.score - a.score);
    return scored.slice(0, maxSize).map((s) => s.player);
  }

  private calculateMigrationScore(
    player: StoredTelemetry,
    sourceRoomPlayers: StoredTelemetry[],
    targetPlayers: StoredTelemetry[],
    targetHotspot: SpatialCluster | undefined,
    now: number
  ): number {
    let score = 0;

    const timeSinceLastMove = player.lastMoveAt ? now - player.lastMoveAt : Infinity;
    if (timeSinceLastMove < 30_000) {
      score += 10 * (1 - timeSinceLastMove / 30_000);
    }

    if (targetHotspot && player.position) {
      const distanceToHotspot = this.distance(player.position, targetHotspot.center);

      if (distanceToHotspot < 100) {
        score += 8 * (1 - distanceToHotspot / 100);
      }

      const velocity = player.emaVelocity ?? player.velocity ?? 0;
      if (velocity < 2) {
        score += 3;
      }
    }

    if (player.affinity) {
      const targetPlayerIds = new Set(targetPlayers.map((p) => p.playerId as string));
      let totalAffinity = 0;
      let affinityCount = 0;

      for (const [targetId, affinityValue] of Object.entries(player.affinity)) {
        if (targetPlayerIds.has(targetId)) {
          totalAffinity += affinityValue;
          affinityCount += 1;
        }
      }

      if (affinityCount > 0) {
        score += (totalAffinity / affinityCount) * 5;
      }
    }

    if (player.affinity) {
      const otherLevelPlayers = sourceRoomPlayers.filter(
        (p) => p.levelId !== player.levelId && p.playerId !== player.playerId
      );
      let totalAffinity = 0;
      let affinityCount = 0;

      for (const otherPlayer of otherLevelPlayers) {
        const affinity = player.affinity[otherPlayer.playerId as string] ?? 0;
        totalAffinity += affinity;
        affinityCount += 1;
      }

      if (affinityCount > 0) {
        score -= (totalAffinity / affinityCount) * 3;
      }
    }

    const freshness = Math.exp(-Math.max(0, now - player.receivedAt) / 5_000);
    score += freshness * 2;

    return Math.max(0, score);
  }

  private distance(a: [number, number, number], b: [number, number, number]): number {
    return Math.sqrt(
      Math.pow(a[0] - b[0], 2) +
      Math.pow(a[1] - b[1], 2) +
      Math.pow(a[2] - b[2], 2)
    );
  }

  public planBatch(
    rooms: RoomSnapshot[],
    telemetry: TelemetryStore,
    now = Date.now()
  ): LevelMigrationPlan[] {
    const plans: LevelMigrationPlan[] = [];
    const processedPlayers = new Set<string>();

    const playersByLevel = new Map<number, StoredTelemetry[]>();
    for (const room of rooms) {
      const roomPlayers = telemetry.listRoom(room.roomId, now);
      for (const player of roomPlayers) {
        if (player.levelId === undefined) continue;
        const levelPlayers = playersByLevel.get(player.levelId) ?? [];
        levelPlayers.push(player);
        playersByLevel.set(player.levelId, levelPlayers);
      }
    }

    for (const [levelId, allLevelPlayers] of playersByLevel) {
      if (allLevelPlayers.length < this.options.minTargetLevelPlayers) {
        continue;
      }

      const roomEvaluations = rooms.map((room) => {
        const roomPlayers = telemetry.listRoom(room.roomId, now);
        const evaluation = this.spatialDetector.evaluateHotspotQuality(
          { roomId: room.roomId, players: roomPlayers },
          levelId
        );
        return {
          room,
          roomPlayers,
          ...evaluation
        };
      });

      const bestTarget = roomEvaluations
        .filter((e) => e.totalLevelPlayers >= this.options.minTargetLevelPlayers)
        .sort((a, b) => {
          const scoreA = a.score * 10 + a.totalLevelPlayers * 0.1;
          const scoreB = b.score * 10 + b.totalLevelPlayers * 0.1;
          return scoreB - scoreA;
        })[0];

      if (!bestTarget) {
        continue;
      }

      const playersByRoom = new Map<string, StoredTelemetry[]>();
      for (const player of allLevelPlayers) {
        const roomPlayers = playersByRoom.get(player.roomId) ?? [];
        roomPlayers.push(player);
        playersByRoom.set(player.roomId, roomPlayers);
      }

      if (playersByRoom.size <= 1) {
        continue;
      }

      const targetRoom = bestTarget.room;

      for (const [sourceRoomId, sourceLevelPlayers] of playersByRoom) {
        if (sourceRoomId === targetRoom.roomId) {
          continue;
        }

        if (sourceLevelPlayers.length > this.options.minSourceLevelPlayers) {
          continue;
        }

        const sourceRoom = rooms.find((r) => r.roomId === sourceRoomId);
        if (!sourceRoom) {
          continue;
        }

        const availableSlots = targetRoom.capacity - targetRoom.players;
        if (availableSlots <= 0) {
          continue;
        }

        const candidatePlayers = sourceLevelPlayers.filter(
          (p) => !processedPlayers.has(p.playerId as string)
        );

        if (candidatePlayers.length === 0) {
          continue;
        }

        const sourceRoomPlayers = telemetry.listRoom(sourceRoom.roomId, now);
        const selectedPlayers = this.selectMigrationCandidates(
          candidatePlayers,
          sourceRoomPlayers,
          bestTarget.roomPlayers,
          availableSlots,
          bestTarget.largestHotspot,
          now
        );

        if (selectedPlayers.length === 0) {
          continue;
        }

        for (const player of selectedPlayers) {
          processedPlayers.add(player.playerId as string);
        }

        plans.push({
          playerIds: selectedPlayers.map((p) => p.playerId as string),
          sourceRoomId,
          targetRoomId: targetRoom.roomId,
          levelId,
          sourceLevelCount: sourceLevelPlayers.length,
          targetLevelCount: bestTarget.totalLevelPlayers,
          targetHotspot: bestTarget.largestHotspot,
          hotspotQuality: bestTarget.score,
          reason: "level_consolidation"
        });

        targetRoom.players += selectedPlayers.length;
      }
    }

    return plans;
  }
}
