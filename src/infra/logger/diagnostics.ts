import { writeSync } from 'node:fs'

const MAX_DEPTH = 6
const MAX_ITEMS = 16
const MAX_TEXT = 8_192
const sensitiveKey = /^(secret|authorization|password|token|api[_-]?token|debugApiToken|x-debug-token)$/i
const text = (value: string): string => value.length > MAX_TEXT ? value.slice(0, MAX_TEXT) + '…[truncated]' : value

/** Bounded, cycle-safe diagnostics; never calls toJSON or custom inspectors. */
export const normalizeError = (reason: unknown): Record<string, unknown> => {
  const seen = new WeakSet<object>()
  let remaining = 128
  const read = (value: object, key: string): unknown => {
    try { return Reflect.get(value, key) } catch { return '[unreadable]' }
  }
  const visit = (value: unknown, depth: number): unknown => {
    if (--remaining < 0 || depth > MAX_DEPTH) return '[truncated]'
    if (value === null || typeof value === 'boolean' || typeof value === 'number') return value
    if (typeof value === 'string') return text(value)
    if (typeof value !== 'object') {
      try { return text(String(value)) } catch { return '[unreadable primitive]' }
    }
    if (seen.has(value)) return '[circular]'
    seen.add(value)
    try {
      if (value instanceof Error) {
        const name = read(value, 'name')
        // YAML parser messages can echo arbitrary configuration source (tokens).
        const yamlError = name === 'YAMLParseError' || name === 'YAMLWarning'
        const result: Record<string, unknown> = {
          type: typeof name === 'string' ? text(name) : 'Error',
          message: yamlError ? 'Invalid YAML configuration' : visit(read(value, 'message'), depth + 1),
          stack: yamlError ? '[source excerpt omitted]' : visit(read(value, 'stack'), depth + 1)
        }
        for (const key of ['code', 'details', 'cause', 'errors', 'issues', 'linePos']) {
          const item = read(value, key)
          if (item !== undefined) result[key] = visit(item, depth + 1)
        }
        for (const key of Object.keys(value).slice(0, MAX_ITEMS)) {
          if (sensitiveKey.test(key) || key in result || ['name', 'message', 'stack'].includes(key)) continue
          result[key] = visit(read(value, key), depth + 1)
        }
        return result
      }
      if (Array.isArray(value)) {
        const result = value.slice(0, MAX_ITEMS).map(item => visit(item, depth + 1))
        if (value.length > MAX_ITEMS) result.push('[truncated]')
        return result
      }
      const result: Record<string, unknown> = Object.create(null) as Record<string, unknown>
      for (const key of Object.keys(value).slice(0, MAX_ITEMS)) {
        if (sensitiveKey.test(key)) continue
        const descriptor = Object.getOwnPropertyDescriptor(value, key)
        result[key] = descriptor && 'value' in descriptor ? visit(descriptor.value, depth + 1) : '[accessor omitted]'
      }
      return result
    } catch { return '[unreadable object]' }
  }
  const normalized = visit(reason, 0)
  try {
    if (reason instanceof Error && typeof normalized === 'object' && normalized !== null) return normalized as Record<string, unknown>
  } catch { /* revoked proxies are valid rejection reasons */ }
  return { type: 'NonError', message: 'Non-Error thrown or rejected', value: normalized }
}

/** Independent of Pino, imports/config and stream draining; safe in fatal handlers. */
export const writeDiagnosticSync = (message: string, reason?: unknown, fields: Record<string, unknown> = {}, level: 30 | 40 | 50 | 60 = 60): void => {
  try {
    const line = JSON.stringify({
      level, time: new Date().toISOString(), pid: process.pid,
      service: 'hermes-room-manager', msg: message,
      ...normalizeError(fields).value as Record<string, unknown>,
      ...(reason === undefined ? {} : { err: normalizeError(reason) })
    }) + '\n'
    writeSync(2, line)
  } catch {
    try { writeSync(2, 'hermes-room-manager: fatal diagnostic unavailable\n') } catch { /* stderr unavailable */ }
  }
}
