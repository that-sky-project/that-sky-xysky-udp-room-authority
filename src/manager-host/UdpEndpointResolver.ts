import { Resolver } from 'node:dns/promises'
import type { Port, UdpHost } from '../types/branded.js'
import { ErrorCode, HermesError, isHermesError } from '../types/errors.js'
import { asIpv4Host, parseUdpHost, type Ipv4Host } from '../types/udpEndpoint.js'

export interface AdvertisedUdpEndpoint { udpHost: UdpHost; udpPort: Port }
export interface ResolvedUdpEndpoint { udpHost: Ipv4Host; udpPort: Port }
export interface UdpEndpointResolverOptions {
  timeoutMs?: number
  maxConcurrent?: number
  resolveIpv4?: (hostname: string) => Promise<string[]>
}
interface WaitingQuery { start(): void; abort(): void }

/** 全部客户端 UDP 地址共用此服务。只合并同一操作内的查询，不缓存跨操作 DNS 结果。 */
export class UdpEndpointResolver {
  private readonly shutdown = new AbortController()
  private readonly waiting = new Set<WaitingQuery>()
  private active = 0
  public constructor (private readonly options: UdpEndpointResolverOptions = {}) {}

  public createScope (signal?: AbortSignal): UdpResolutionScope {
    return new UdpResolutionScope(this, AbortSignal.any([
      this.shutdown.signal,
      AbortSignal.timeout(this.options.timeoutMs ?? 3000),
      ...(signal ? [signal] : [])
    ]))
  }

  public stop (): void {
    this.shutdown.abort(new HermesError(ErrorCode.NODE_UNAVAILABLE, 'UDP address resolver stopped'))
  }

  public async query (hostname: string, signal: AbortSignal): Promise<Ipv4Host> {
    await this.acquire(signal)
    // 每次查询有独立 Resolver；取消一名调用者不能取消其他操作的查询。
    const resolver = this.options.resolveIpv4 ? undefined : new Resolver()
    try {
      const addresses = await this.wait(
        Promise.resolve().then(() => {
          this.check(signal)
          return this.options.resolveIpv4 ? this.options.resolveIpv4(hostname) : resolver!.resolve4(hostname)
        }), signal
      )
      const address = addresses.find(value => parseUdpHost(value)?.kind === 'ipv4')
      if (!address) throw new HermesError(ErrorCode.NODE_UNAVAILABLE, 'UDP hostname has no IPv4 A record', { hostname })
      return asIpv4Host(address)
    } catch (error) {
      if (signal.aborted) this.check(signal)
      if (isHermesError(error)) throw error
      throw new HermesError(ErrorCode.NODE_UNAVAILABLE, 'UDP hostname could not be resolved to IPv4', {
        hostname, dnsCode: error instanceof Error ? (error as NodeJS.ErrnoException).code : undefined
      })
    } finally {
      resolver?.cancel()
      this.active--
      this.waiting.values().next().value?.start()
    }
  }

  public check (signal: AbortSignal): void {
    if (!signal.aborted) return
    if (isHermesError(signal.reason)) throw signal.reason
    throw new HermesError(
      signal.reason instanceof Error && signal.reason.name === 'TimeoutError' ? ErrorCode.NODE_TIMEOUT : ErrorCode.NODE_UNAVAILABLE,
      signal.reason instanceof Error && signal.reason.name === 'TimeoutError' ? 'UDP address resolution timed out' : 'UDP address resolution cancelled'
    )
  }

  private async acquire (signal: AbortSignal): Promise<void> {
    this.check(signal)
    if (this.active < (this.options.maxConcurrent ?? 32)) { this.active++; return }
    if (this.waiting.size >= 4096) throw new HermesError(ErrorCode.NODE_UNAVAILABLE, 'UDP DNS query queue is full')
    await new Promise<void>((resolve, reject) => {
      const entry: WaitingQuery = {
        start: () => { cleanup(); this.active++; resolve() },
        abort: () => {
          cleanup()
          try { this.check(signal) } catch (error) { reject(error) }
        }
      }
      const cleanup = (): void => { this.waiting.delete(entry); signal.removeEventListener('abort', entry.abort) }
      this.waiting.add(entry)
      signal.addEventListener('abort', entry.abort, { once: true })
    })
  }

  private async wait<T> (promise: Promise<T>, signal: AbortSignal): Promise<T> {
    let onAbort: () => void = () => {}
    const aborted = new Promise<never>((_resolve, reject) => {
      onAbort = () => { try { this.check(signal) } catch (error) { reject(error) } }
      signal.addEventListener('abort', onAbort, { once: true })
      if (signal.aborted) onAbort()
    })
    try { return await Promise.race([promise, aborted]) }
    finally { signal.removeEventListener('abort', onAbort) }
  }
}

/** 生命周期绑定到单次分配、查询批次或迁移事务。 */
export class UdpResolutionScope {
  private readonly hosts = new Map<string, Promise<Ipv4Host>>()
  public constructor (private readonly resolver: UdpEndpointResolver, public readonly signal: AbortSignal) {}

  public async resolve (endpoint: AdvertisedUdpEndpoint): Promise<ResolvedUdpEndpoint> {
    this.resolver.check(this.signal)
    const { udpHost, udpPort } = endpoint
    const parsed = parseUdpHost(udpHost)
    if (!parsed || !Number.isInteger(udpPort) || udpPort < 1 || udpPort > 65535) {
      throw new HermesError(ErrorCode.PROTOCOL_ERROR, 'invalid advertised UDP endpoint')
    }
    if (parsed.kind === 'ipv6') throw new HermesError(ErrorCode.NODE_UNAVAILABLE, 'client UDP endpoint requires IPv4, not an IPv6 literal')
    if (parsed.kind === 'ipv4') return { udpHost: asIpv4Host(parsed.host), udpPort }
    let pending = this.hosts.get(parsed.host)
    if (!pending) { pending = this.resolver.query(parsed.host, this.signal); this.hosts.set(parsed.host, pending) }
    const resolved = await pending
    this.resolver.check(this.signal)
    return { udpHost: resolved, udpPort }
  }
}
