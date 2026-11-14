import type { Logging } from '../infra/logger/logger.js'
import { writeDiagnosticSync } from '../infra/logger/diagnostics.js'

export interface LifecycleOptions {
  deadlineMs?: number
  exit?: (code: number) => void
}

/** One owner of exit status and deadline. Gateway.stop must disable admission and drain resources. */
export const createLifecycle = (options: LifecycleOptions = {}) => {
  const exit = options.exit ?? ((code: number): void => { process.exit(code) })
  let logging: Logging | undefined
  let stop: (() => Promise<void> | void) | undefined
  let shutdown: Promise<number> | undefined
  let stopping = false
  let exitCode = 0
  let installed = false
  let phase = 'running'
  const startedAt = performance.now()
  const details = (): Record<string, unknown> => ({ phase, uptimeMs: Math.round(performance.now() - startedAt) })

  const markFailure = (message: string, error: unknown): void => {
    exitCode = Math.max(exitCode, 1)
    process.exitCode = exitCode
    writeDiagnosticSync(message, error, details(), 50)
  }
  const requestShutdown = (reason: string, error?: unknown): Promise<number> => {
    if (error !== undefined) markFailure(reason, error)
    if (shutdown) return shutdown
    stopping = true
    const shutdownStartedAt = performance.now()
    process.exitCode = exitCode
    // Referenced on purpose: a stuck close must not let the process disappear
    // successfully just because the remaining work has no referenced handles.
    const deadline = setTimeout(() => {
      exitCode = 2
      process.exitCode = 2
      writeDiagnosticSync('shutdown deadline exceeded; forcing exit', undefined, { ...details(), reason, deadlineMs: options.deadlineMs ?? 10_000, shutdownMs: Math.round(performance.now() - shutdownStartedAt), exitCode: 2 })
      exit(2)
    }, options.deadlineMs ?? 10_000)
    shutdown = Promise.resolve().then(async () => {
      phase = 'gateway.stop'
      try { logging?.logger.warn({ reason, exitCode, ...details() }, 'shutting down manager') } catch (error) { markFailure('shutdown logging failed', error) }
      try { await stop?.() } catch (error) { markFailure('manager shutdown failed', error) }
      phase = 'logger.close'
      try { await logging?.close() } catch (error) { markFailure('logger close failed', error) }
      phase = 'stopped'
      writeDiagnosticSync('manager stopped', undefined, { ...details(), reason, exitCode, shutdownMs: Math.round(performance.now() - shutdownStartedAt) }, exitCode === 0 ? 30 : 50)
      // Let drained handles/output finish naturally. The unref fallback remains
      // armed if a stray referenced handle survives a supposedly complete stop.
      // Do not remove fatal handlers: late failures must still upgrade status.
      process.exitCode = exitCode
      deadline.unref()
      return exitCode
    })
    return shutdown
  }
  const fatal = (origin: string, reason: unknown): void => {
    // No business callbacks/async cleanup after uncaught exceptions: state is
    // unsafe. This fd-2 write is synchronous even at logging.level: silent.
    exitCode = Math.max(exitCode, 1)
    process.exitCode = exitCode
    writeDiagnosticSync(`fatal ${origin}; terminating manager`, reason, { ...details(), exitCode })
    exit(exitCode)
  }
  const onTerm = (): void => { void requestShutdown('SIGTERM') }
  const onInt = (): void => { void requestShutdown('SIGINT') }
  const onException = (error: Error, origin: string): void => { fatal(origin, error) }
  const onRejection = (reason: unknown): void => { fatal('unhandledRejection', reason) }
  return {
    setLogging (value: Logging): void { logging = value },
    setPhase (value: string): void { phase = value },
    setStop (value: () => Promise<void> | void): void { stop = value },
    isStopping (): boolean { return stopping },
    requestShutdown,
    fail (reason: string, error: unknown): Promise<number> {
      markFailure(reason, error)
      return requestShutdown(reason)
    },
    fatal,
    install (): void {
      if (installed) return
      installed = true
      process.on('SIGTERM', onTerm)
      process.on('SIGINT', onInt)
      process.on('uncaughtException', onException)
      process.on('unhandledRejection', onRejection)
    },
    dispose (): void {
      if (!installed) return
      installed = false
      process.off('SIGTERM', onTerm)
      process.off('SIGINT', onInt)
      process.off('uncaughtException', onException)
      process.off('unhandledRejection', onRejection)
    }
  }
}
