declare module "ws" {
  import type { EventEmitter } from "node:events";
  export type RawData = Buffer | ArrayBuffer | Buffer[];
  export class WebSocket extends EventEmitter {
    static readonly OPEN: number;
    readonly readyState: number;
    readonly bufferedAmount: number;
    constructor(address: string | URL);
    send(data: string, callback?: (error?: Error) => void): void;
    close(code?: number, reason?: string): void;
  }
  export class WebSocketServer extends EventEmitter {
    readonly clients: Set<WebSocket>;
    constructor(options?: Record<string, unknown>);
    handleUpgrade(request: unknown, socket: unknown, head: Buffer, callback: (socket: WebSocket) => void): void;
    close(callback?: (error?: Error) => void): void;
  }
}
