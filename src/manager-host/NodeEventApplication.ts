import type { INodeEvent } from '../types/contracts.js'
import type { ManagerRuntime } from './ManagerRuntime.js'

/** HTTP and authenticated sockets share one event-to-domain application order. */
export function applyNodeEvent(runtime: ManagerRuntime, event: INodeEvent): boolean {
  const { roomId, nodeId, connectedAt } = event.data
  if (roomId && event.event !== 'room.created') {
    const owner = runtime.rooms.getRoom(roomId)
    if (owner && owner.nodeId !== nodeId) return false
  }
  if (connectedAt !== undefined && runtime.nodes.getTransport(nodeId)?.connectedAt !== connectedAt) return false
  // Even a rejected business snapshot can establish transport liveness, but it
  // cannot advance accepted roster/location/occupancy state or release a hold.
  runtime.nodes.applyEvent(event)
  if (!runtime.moves.ingest(event)) return false
  runtime.rooms.applyNodeEvent(event)
  if (event.data.player?.playerId && event.event === 'player.join') {
    if (runtime.reservations.roomFor(event.data.player.playerId) === roomId) runtime.reservations.confirm(event.data.player.playerId)
  }
  recordAppliedEvent(runtime, event)
  return true
}

function recordAppliedEvent(runtime: ManagerRuntime, event: INodeEvent): void {
  const roomId = event.data.roomId
  if (!roomId) return
  if ((event.event === 'player.join' || event.event === 'player.leave') && event.data.player) {
    runtime.events.record({ category: 'player', event: event.event,
      message: `${event.event === 'player.join' ? 'joined' : 'left'} room ${roomId}`,
      roomId, nodeId: event.data.nodeId, playerIds: [event.data.player.playerId],
      data: { level: event.data.player.level } })
  } else if (event.event === 'room.created' || event.event === 'room.destroyed') {
    runtime.events.record({ category: 'room', event: event.event,
      message: `${event.event === 'room.created' ? 'room created' : 'room destroyed'}: ${roomId}`,
      roomId, nodeId: event.data.nodeId, data: { udpHost: event.data.udpHost, udpPort: event.data.port,
        players: typeof event.data.players === 'number' ? event.data.players : undefined, version: event.data.version } })
  }
}
