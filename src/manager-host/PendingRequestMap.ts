import type { CommandId } from "../types/branded.js";
import { asCommandId } from "../types/branded.js";
import type { IManagerCommand, INodeAck } from "../types/contracts.js";
import { ErrorCode, HermesError } from "../types/errors.js";

interface PendingEntry {
  resolve: (ack: INodeAck) => void;
  reject: (error: Error) => void;
  timeout: NodeJS.Timeout;
  abortListener?: (() => void) | undefined;
}

export interface PendingRequestOptions {
  timeoutMs: number;
  maxPending: number;
}

export class PendingRequestMap {
  private readonly pending = new Map<CommandId, PendingEntry>();
  private sequence = 1;

  public constructor(private readonly options: PendingRequestOptions) {}

  public nextId(): CommandId {
    const id = this.sequence;
    this.sequence = this.sequence >= Number.MAX_SAFE_INTEGER ? 1 : this.sequence + 1;
    return asCommandId(id);
  }

  public async send<TData>(
    command: Omit<IManagerCommand<TData>, "id">,
    sender: (command: IManagerCommand<TData>) => Promise<void>,
    signal?: AbortSignal
  ): Promise<INodeAck> {
    if (this.pending.size >= this.options.maxPending) {
      throw new HermesError(ErrorCode.NO_CAPACITY, "too many pending node commands");
    }

    const fullCommand: IManagerCommand<TData> = { ...command, id: this.nextId() };
    const ackPromise = this.wait(fullCommand.id, signal);

    try {
      await sender(fullCommand);
      return await ackPromise;
    } catch (error) {
      this.reject(fullCommand.id, error instanceof Error ? error : new Error(String(error)));
      throw error;
    }
  }

  public resolve(ack: INodeAck): boolean {
    const entry = this.pending.get(ack.id);
    if (!entry) return false;
    clearTimeout(entry.timeout);
    entry.abortListener?.();
    this.pending.delete(ack.id);
    entry.resolve(ack);
    return true;
  }

  public reject(id: CommandId, error: Error): boolean {
    const entry = this.pending.get(id);
    if (!entry) return false;
    clearTimeout(entry.timeout);
    entry.abortListener?.();
    this.pending.delete(id);
    entry.reject(error);
    return true;
  }

  public size(): number {
    return this.pending.size;
  }

  private wait(id: CommandId, signal?: AbortSignal): Promise<INodeAck> {
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pending.delete(id);
        reject(new HermesError(ErrorCode.NODE_TIMEOUT, "node command timed out", { id }));
      }, this.options.timeoutMs);
      timeout.unref?.();

      let abortListener: (() => void) | undefined;
      if (signal) {
        abortListener = () => signal.removeEventListener("abort", onAbort);
        const onAbort = () => {
          this.reject(id, new HermesError(ErrorCode.NODE_TIMEOUT, "node command aborted", { id }));
        };
        signal.addEventListener("abort", onAbort, { once: true });
        abortListener = () => signal.removeEventListener("abort", onAbort);
      }

      this.pending.set(id, { resolve, reject, timeout, abortListener });
    });
  }
}
