import type { CommandId } from '../types/branded.js'
import { asCommandId } from '../types/branded.js'
import type { IManagerCommand, INodeAck } from '../types/contracts.js'
import { ErrorCode, HermesError } from '../types/errors.js'

interface PendingEntry {
  resolve: (ack: INodeAck) => void
  reject: (error: Error) => void
  timeout: NodeJS.Timeout
  abortListener?: (() => void) | undefined
  /**
   * Node the command was addressed to.
   *
   * Command ids come from one global counter, so matching an ack on the id
   * alone lets any node answer a command that was sent to a different node —
   * a bug or a malicious node would then be taken as the truth about someone
   * else's rooms and players.
   */
  nodeId?: string | undefined
}

export interface PendingRequestOptions {
  timeoutMs: number
  maxPending: number
}

export class PendingRequestMap {
  private readonly pending = new Map<CommandId, PendingEntry>()
  private sequence = 1

  public constructor (private readonly options: PendingRequestOptions) {}

  public nextId (): CommandId {
    const id = this.sequence
    this.sequence =
      this.sequence >= Number.MAX_SAFE_INTEGER ? 1 : this.sequence + 1
    return asCommandId(id)
  }

  public async send<TData> (
    command: Omit<IManagerCommand<TData>, 'id'>,
    sender: (command: IManagerCommand<TData>) => Promise<void>,
    signal?: AbortSignal,
    nodeId?: string
  ): Promise<INodeAck> {
    if (this.pending.size >= this.options.maxPending) {
      throw new HermesError(
        ErrorCode.NO_CAPACITY,
        'too many pending node commands'
      )
    }

    const fullCommand: IManagerCommand<TData> = {
      ...command,
      id: this.nextId()
    }
    const ackPromise = this.wait(fullCommand.id, signal, nodeId)

    try {
      await sender(fullCommand)
      return await ackPromise
    } catch (error) {
      this.reject(
        fullCommand.id,
        error instanceof Error ? error : new Error(String(error))
      )
      throw error
    }
  }

  /**
   * Resolve a command from a node's ack.
   *
   * An ack whose id belongs to a command addressed to another node is rejected
   * and left pending, so the real node's answer still has a chance to arrive.
   */
  public resolve (ack: INodeAck, fromNodeId?: string): boolean {
    const entry = this.pending.get(ack.id)
    if (!entry) return false
    if (entry.nodeId !== undefined && fromNodeId !== undefined && entry.nodeId !== fromNodeId) {
      return false
    }
    clearTimeout(entry.timeout)
    entry.abortListener?.()
    this.pending.delete(ack.id)
    entry.resolve(ack)
    return true
  }

  public reject (id: CommandId, error: Error): boolean {
    const entry = this.pending.get(id)
    if (!entry) return false
    clearTimeout(entry.timeout)
    entry.abortListener?.()
    this.pending.delete(id)
    entry.reject(error)
    return true
  }

  public size (): number {
    return this.pending.size
  }

  private wait (id: CommandId, signal?: AbortSignal, nodeId?: string): Promise<INodeAck> {
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pending.delete(id)
        reject(
          new HermesError(ErrorCode.NODE_TIMEOUT, 'node command timed out', {
            id
          })
        )
      }, this.options.timeoutMs)
      timeout.unref?.()

      let abortListener: (() => void) | undefined
      if (signal) {
        const onAbort = (): void => {
          this.reject(
            id,
            new HermesError(ErrorCode.NODE_TIMEOUT, 'node command aborted', {
              id
            })
          )
        }
        signal.addEventListener('abort', onAbort, { once: true })
        abortListener = () => signal.removeEventListener('abort', onAbort)
      }

      this.pending.set(id, { resolve, reject, timeout, abortListener, nodeId })
    })
  }
}
