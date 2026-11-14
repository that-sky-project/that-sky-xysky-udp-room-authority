import type { Logger } from '../infra/logger/logger.js'

interface Options {
  run: (signal: AbortSignal, now: number) => Promise<void>
  maintain: (now: number) => void
  logger?: Logger | undefined
  clock?: () => number
  intervalMs?: number
  maintenanceMs?: number
}

/** Owns coalescing, timer lifetime and all in-flight work for one planner. */
export class PlanningScheduler {
  private readonly abort = new AbortController()
  private readonly work = new Set<Promise<unknown>>()
  private readonly maintenance: NodeJS.Timeout
  private readonly clock: () => number
  private timer: NodeJS.Timeout | undefined
  private dirty = false
  private running = false
  private closed = false
  private lastRun = -Infinity
  private closing: Promise<void> | undefined

  public constructor(private readonly options: Options) {
    this.clock = options.clock ?? Date.now
    this.maintenance = setInterval(() => {
      if (this.closed) return
      try { options.maintain(this.clock()) } catch (error) { options.logger?.error({ err: error }, 'planner maintenance failed') }
      if (this.dirty) this.request()
    }, options.maintenanceMs ?? 500)
    this.maintenance.unref()
  }
  public get stopped(): boolean { return this.closed }
  public workSignal(signal?: AbortSignal): AbortSignal { return signal ? AbortSignal.any([signal, this.abort.signal]) : this.abort.signal }
  public track<T>(operation: Promise<T>): Promise<T> {
    this.work.add(operation)
    void operation.then(() => this.work.delete(operation), () => this.work.delete(operation))
    return operation
  }
  public request(): void {
    if (this.closed) return
    this.dirty = true
    if (this.timer || this.running) return
    this.timer = setTimeout(() => {
      this.timer = undefined
      void this.runNow().catch(error => this.options.logger?.error({ err: error }, 'scheduled planning failed'))
    }, Math.max(0, this.lastRun + (this.options.intervalMs ?? 2_000) - this.clock()))
    this.timer.unref()
  }
  public async runNow(signal?: AbortSignal, now = this.clock()): Promise<void> {
    if (this.closed || signal?.aborted) return
    if (this.running || now - this.lastRun < (this.options.intervalMs ?? 2_000)) { this.request(); return }
    if (this.timer) clearTimeout(this.timer)
    this.timer = undefined
    this.running = true
    this.dirty = false
    this.lastRun = now
    try { await this.track(this.options.run(this.workSignal(signal), now)) }
    finally { this.running = false; if (this.dirty) this.request() }
  }
  public stop(): Promise<void> {
    if (this.closing) return this.closing
    this.closed = true
    clearInterval(this.maintenance)
    if (this.timer) clearTimeout(this.timer)
    this.timer = undefined
    this.abort.abort(new Error('planning scheduler stopped'))
    this.closing = Promise.allSettled([...this.work]).then(() => {})
    return this.closing
  }
}
