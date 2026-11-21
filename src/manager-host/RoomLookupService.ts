import type { PlayerId } from '../types/branded.js'
import type { RoomLookup } from '../types/contracts.js'
import { ErrorCode, HermesError } from '../types/errors.js'
import type { RoomCache } from './RoomCache.js'
import type { TelemetryStore } from './TelemetryStore.js'
import { UdpEndpointResolver, type UdpResolutionScope } from './UdpEndpointResolver.js'

/** 玩家房间查询的单一应用入口；DNS 等待之后重新读取玩家及房间事实。 */
export class RoomLookupService {
  public constructor (
    private readonly rooms: RoomCache,
    private readonly telemetry: TelemetryStore,
    private readonly endpoints: UdpEndpointResolver
  ) {}

  public lookup (playerId: PlayerId, signal?: AbortSignal): Promise<RoomLookup | null> {
    return this.lookupInScope(playerId, this.endpoints.createScope(signal))
  }

  public async lookupMany (playerIds: PlayerId[], signal?: AbortSignal): Promise<Record<string, RoomLookup | null>> {
    const scope = this.endpoints.createScope(signal)
    const result: Record<string, RoomLookup | null> = Object.create(null) as Record<string, RoomLookup | null>
    const unique = [...new Set(playerIds)]
    let index = 0
    const workers = Array.from({ length: Math.min(16, unique.length) }, async () => {
      while (index < unique.length) {
        const id = unique[index++]!
        result[id] = await this.lookupInScope(id, scope)
      }
    })
    await Promise.all(workers)
    return result
  }

  private async lookupInScope (playerId: PlayerId, scope: UdpResolutionScope): Promise<RoomLookup | null> {
    for (let attempt = 0; attempt < 3; attempt++) {
      const location = this.rooms.getPlayerLocation(playerId)
      if (!location) return null
      const room = this.rooms.getRoom(location.roomId)
      if (!room) return null
      const endpoint = await scope.resolve(room)
      const currentLocation = this.rooms.getPlayerLocation(playerId)
      if (!currentLocation) return null
      const current = this.rooms.getRoom(currentLocation.roomId)
      if (!current) return null
      if (currentLocation.roomId !== location.roomId || currentLocation.nodeId !== location.nodeId ||
          current.nodeId !== room.nodeId || current.udpHost !== room.udpHost || current.udpPort !== room.udpPort) continue
      return {
        playerId, roomId: current.roomId, udp: { host: endpoint.udpHost, port: endpoint.udpPort },
        players: current.players, capacity: current.capacity, level: currentLocation.level ?? null,
        levelId: this.telemetry.get(playerId)?.levelId ?? null, updatedAt: currentLocation.updatedAt
      }
    }
    throw new HermesError(ErrorCode.STALE_PLAN, 'player room changed repeatedly during UDP address resolution')
  }
}
