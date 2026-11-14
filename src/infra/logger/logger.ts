import { writeSync } from 'node:fs'
import pino from 'pino'
import pretty from 'pino-pretty'
import type { LogLevel } from '../../config/index.js'
import { normalizeError } from './diagnostics.js'
export { normalizeError, writeDiagnosticSync } from './diagnostics.js'

export type Logger = pino.Logger
export interface Logging {
  logger: Logger
  close: () => Promise<void>
}
export interface LoggingOptions {
  level?: LogLevel
  pretty?: boolean
}
const pad2 = (value: number): string => String(value).padStart(2, '0')
const localTimestamp = (): string => {
  const now = new Date()
  const offsetMinutes = -now.getTimezoneOffset()
  const sign = offsetMinutes >= 0 ? '+' : '-'
  const absolute = Math.abs(offsetMinutes)
  return `${now.getFullYear()}-${pad2(now.getMonth() + 1)}-${pad2(now.getDate())}`
    + `T${pad2(now.getHours())}:${pad2(now.getMinutes())}:${pad2(now.getSeconds())}`
    + `.${String(now.getMilliseconds()).padStart(3, '0')}`
    + `${sign}${pad2(Math.floor(absolute / 60))}:${pad2(absolute % 60)}`
}

/** No worker/transport process or pending output: formatting and writes are in-process and synchronous. */
export const createLogging = (options: LoggingOptions = {}): Logging => {
  const prettify = (options.pretty ?? true) ? pretty.prettyFactory({
    colorize: Boolean(process.stdout.isTTY),
    translateTime: false,
    ignore: 'hostname',
    errorLikeObjectKeys: ['err', 'error'],
    errorProps: '*'
  }) : undefined
  let closed = false
  let closePromise: Promise<void> | undefined
  const logger = pino({
    level: options.level ?? 'info',
    base: { service: 'hermes-room-manager', pid: process.pid },
    timestamp: () => `,"time":"${localTimestamp()}"`,
    serializers: { err: normalizeError, error: normalizeError },
    redact: {
      paths: ['secret', 'authorization', 'password', 'token', 'debugApiToken', '*.secret', '*.authorization', 'req.headers.authorization', 'req.headers["x-debug-token"]'],
      remove: true
    }
  }, {
    write (line: string): void {
      if (!closed) writeSync(1, prettify ? prettify(line) : line)
    }
  })
  return {
    logger,
    close (): Promise<void> {
      if (!closePromise) {
        // All previous writes have already reached the OS; do not close stdout.
        closed = true
        closePromise = Promise.resolve()
      }
      return closePromise
    }
  }
}
