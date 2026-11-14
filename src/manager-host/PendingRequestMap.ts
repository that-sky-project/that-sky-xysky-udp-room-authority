import type { CommandId } from '../types/branded.js'
import { asCommandId } from '../types/branded.js'
import type { IManagerCommand, INodeAck } from '../types/contracts.js'
import { ErrorCode, HermesError } from '../types/errors.js'

interface PendingEntry {
  resolve: (ack: INodeAck) => void
  reject: (error: unknown) => void
  timeout: NodeJS.Timeout
  removeAbortListener?: (() => void) | undefined
  nodeId?: string | undefined
  sent: boolean
  ack?: INodeAck | undefined
}

export interface PendingRequestOptions {
  timeoutMs: number
  maxPending: number
}

export class PendingRequestMap {
  private readonly pending = new Map<CommandId, PendingEntry>()
  private sequence = 1
  private closedError: Error | undefined

  public constructor (private readonly options: PendingRequestOptions) {}

  public nextId (): CommandId {
    // Do not reuse a live command id when the global sequence wraps.
    while (true) {
      const id = asCommandId(this.sequence)
      this.sequence = this.sequence >= Number.MAX_SAFE_INTEGER ? 1 : this.sequence + 1
      if (!this.pending.has(id)) return id
    }
  }

  public send<TData> (
    command: Omit<IManagerCommand<TData>, 'id'>,
    sender: (command: IManagerCommand<TData>) => Promise<void>,
    signal?: AbortSignal,
    nodeId?: string
  ): Promise<INodeAck> {
    if (this.closedError) return Promise.reject(this.closedError)
    if (signal?.aborted) return Promise.reject(this.abortError())
    if (this.pending.size >= this.options.maxPending) {
      return Promise.reject(new HermesError(ErrorCode.NO_CAPACITY, 'too many pending node commands'))
    }
    const fullCommand: IManagerCommand<TData> = { ...command, id: this.nextId() }
    // A single public promise owns both the send and ACK. Its deadline stays
    // active even if an ACK arrives before the transport's send callback. No
    // hidden rejected ACK promise, and abort/timeout never await a stuck sender.
    return new Promise<INodeAck>((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.reject(fullCommand.id, new HermesError(ErrorCode.NODE_TIMEOUT, 'node command timed out', { id: fullCommand.id }))
      }, this.options.timeoutMs)
      timeout.unref?.()
      const entry: PendingEntry = { resolve, reject, timeout, nodeId, sent: false }
      this.pending.set(fullCommand.id, entry)
      if (signal) {
        const onAbort = (): void => { this.reject(fullCommand.id, this.abortError(fullCommand.id)) }
        signal.addEventListener('abort', onAbort, { once: true })
        entry.removeAbortListener = () => signal.removeEventListener('abort', onAbort)
        if (signal.aborted) onAbort()
      }
      // Attach rejection handling immediately, including synchronous sender
      // throws and failures that happen after this command already timed out.
      void Promise.resolve().then(() => {
        if (this.pending.get(fullCommand.id) !== entry) return
        return sender(fullCommand)
      }).then(() => {
        if (this.pending.get(fullCommand.id) !== entry) return
        entry.sent = true
        if (entry.ack) this.complete(fullCommand.id, entry, entry.ack)
      }, error => {
        const current = this.pending.get(fullCommand.id)
        if (current !== entry) return
        this.remove(fullCommand.id, entry)
        entry.reject(error)
      })
    })
  }

  /** A node-bound command requires the authenticated source, including when omitted. */
  public resolve (ack: INodeAck, fromNodeId?: string): boolean {
    const entry = this.pending.get(ack.id)
    if (!entry || entry.ack) return false
    if (entry.nodeId !== undefined && entry.nodeId !== fromNodeId) return false
    entry.ack = ack
    if (entry.sent) this.complete(ack.id, entry, ack)
    return true
  }

  public reject (id: CommandId, error: Error): boolean {
    const entry = this.pending.get(id)
    if (!entry) return false
    this.remove(id, entry)
    entry.reject(error)
    return true
  }

  public rejectAll (error: Error): void {
    for (const id of this.pending.keys()) this.reject(id, error)
  }

  public close (error: Error = new HermesError(ErrorCode.NODE_UNAVAILABLE, 'node command channel stopped')): void {
    if (this.closedError) return
    this.closedError = error
    this.rejectAll(error)
  }

  public size (): number { return this.pending.size }

  private abortError (id?: CommandId): Error {
    return new HermesError(ErrorCode.NODE_TIMEOUT, 'node command aborted', { id })
  }

  private remove (id: CommandId, entry: PendingEntry): void {
    clearTimeout(entry.timeout)
    entry.removeAbortListener?.()
    this.pending.delete(id)
  }

  private complete (id: CommandId, entry: PendingEntry, ack: INodeAck): void {
    this.remove(id, entry)
    entry.resolve(ack)
  }
}
