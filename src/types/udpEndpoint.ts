import { isIP } from 'node:net'
import { domainToASCII } from 'node:url'
import { asUdpHost, type UdpHost } from './branded.js'

export type UdpHostKind = 'ipv4' | 'ipv6' | 'hostname'
export interface ParsedUdpHost { host: UdpHost; kind: UdpHostKind }

/** 公告 host 的唯一校验和规范化入口；端口独立传递，不接受 URL 或 host:port。 */
export function parseUdpHost (value: string): ParsedUdpHost | undefined {
  const trimmed = value.trim()
  const literal = trimmed.startsWith('[') && trimmed.endsWith(']') ? trimmed.slice(1, -1) : trimmed
  const family = isIP(literal)
  if (family) return { host: asUdpHost(literal.toLowerCase()), kind: family === 4 ? 'ipv4' : 'ipv6' }
  if (!trimmed || /[\s/:@?#\\\[\]%]/u.test(trimmed) || /^[\d.]+$/u.test(trimmed)) return undefined
  const ascii = domainToASCII(trimmed).toLowerCase()
  const hostname = ascii.endsWith('.') ? ascii.slice(0, -1) : ascii
  if (!hostname || hostname.length > 253 || !hostname.split('.').every(label =>
    label.length <= 63 && /^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$/u.test(label))) return undefined
  return { host: asUdpHost(ascii), kind: 'hostname' }
}

export const isDomainUdpHost = (host: string): boolean => parseUdpHost(host)?.kind === 'hostname'

/** 公告地址与客户端地址分开表示，防止域名意外进入仅接受 IPv4 的 MoveGame。 */
export type Ipv4Host = UdpHost & { readonly __ipv4: true }
export const asIpv4Host = (value: string): Ipv4Host => {
  if (isIP(value) !== 4) throw new Error('expected an IPv4 literal')
  return asUdpHost(value) as Ipv4Host
}
