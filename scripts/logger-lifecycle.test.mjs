import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, writeFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { spawn } from 'node:child_process'
import { createServer } from 'node:net'
import { loadConfig } from '../dist/config/index.js'
import { normalizeError } from '../dist/infra/logger/diagnostics.js'

const project = fileURLToPath(new URL('../', import.meta.url))
const loggerUrl = new URL('../dist/infra/logger/logger.js', import.meta.url).href
const lifecycleUrl = new URL('../dist/runtime/lifecycle.js', import.meta.url).href
const main = fileURLToPath(new URL('../dist/runtime/main.js', import.meta.url))
const run = (args, cwd = project, env = {}) => new Promise((resolve, reject) => {
  const child = spawn(process.execPath, ['--enable-source-maps', ...args], { cwd, env: { ...process.env, ...env }, stdio: ['ignore', 'pipe', 'pipe'] })
  let stdout = '', stderr = ''
  child.stdout.on('data', data => { stdout += data })
  child.stderr.on('data', data => { stderr += data })
  const timeout = setTimeout(() => { child.kill(); reject(new Error('child test exceeded deadline')) }, 8_000)
  child.on('error', error => { clearTimeout(timeout); reject(error) })
  child.on('close', (code, signal) => { clearTimeout(timeout); resolve({ code, signal, stdout, stderr }) })
})
const withConfig = async (yaml, action) => {
  const dir = await mkdtemp(path.join(tmpdir(), 'qwd-lifecycle-'))
  try { await writeFile(path.join(dir, 'config.yml'), yaml); return await action(dir) }
  finally { await rm(dir, { recursive: true, force: true }) }
}

test('sample config enforces move group seven, defaults pretty true and rejects removed feature fields', async () => {
  const sample = loadConfig(path.join(project, 'config.yml'))
  assert.equal(sample.logPretty, true)
  assert.equal(sample.levelIsolationMaxPlayers, 7)
  await withConfig('{}', dir => { assert.equal(loadConfig(path.join(dir, 'config.yml')).logPretty, true) })
  for (const field of ['performance', 'liveBroadcast', 'audience', 'audienceReaction', 'audienceChatGlobal']) {
    await withConfig(`${field}: {}`, dir => assert.throws(() => loadConfig(path.join(dir, 'config.yml'))))
  }
})

test('bootstrap invalid YAML/loglevel/pretty/port/missing file exits1 with synchronous diagnostics', async () => {
  for (const [yaml, expected] of [
    ['logging: [', 'Invalid YAML'],
    ['logging:\n  level: invalid', 'logging'],
    ['logging:\n  pretty: "true"', 'pretty'],
    ['port: 65536', 'port'],
    ['port: 0', 'port']
  ]) {
    const result = await withConfig(yaml, dir => run([main], dir))
    assert.equal(result.code, 1, result.stderr)
    assert.match(result.stderr, /manager startup failed \(configuration\)/)
    assert.ok(result.stderr.includes(expected), result.stderr)
    assert.match(result.stderr, /"pid":\d+/)
  }
  const dir = await mkdtemp(path.join(tmpdir(), 'qwd-missing-'))
  try {
    const result = await run([main], dir)
    assert.equal(result.code, 1)
    assert.match(result.stderr, /ENOENT/)
  } finally { await rm(dir, { recursive: true, force: true }) }
})

test('listen errors exit1 and report address/port/stack through startup catch', async () => {
  const server = createServer()
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  try {
    const port = server.address().port
    const result = await withConfig(`host: 127.0.0.1\nport: ${port}\nlogging:\n  pretty: false`, dir => run([main], dir))
    assert.equal(result.code, 1, result.stderr)
    assert.match(result.stderr, /manager startup failed \(listen\)/)
    assert.match(result.stderr, /EADDRINUSE/)
    assert.ok(result.stderr.includes(String(port)))
  } finally { await new Promise(resolve => server.close(resolve)) }
})

test('bootstrap catches dynamic import/runtime/gateway/listen failures with source-map stacks', { skip: typeof (await import('node:module')).registerHooks !== 'function' }, async () => {
  const fixture = new URL('./lifecycle-bootstrap-fixture.mjs', import.meta.url).href
  for (const [mode, stage] of [['import', 'manager imports'], ['runtime', 'runtime construction'], ['constructor', 'gateway construction'], ['listen', 'listen']]) {
    const result = await withConfig('logging:\n  pretty: false', dir => run(['--import', fixture, main], dir, { QWD_BOOTSTRAP_TEST: mode }))
    assert.equal(result.code, 1, result.stderr)
    assert.ok(result.stderr.includes(`manager startup failed (${stage})`), result.stderr)
    assert.match(result.stderr, /fixture.*failure/)
    const diagnostic = result.stderr.trim().split('\n').map(JSON.parse).find(item => item.err)
    if (mode !== 'import') assert.match(diagnostic.err.stack, /src[\\/]runtime[\\/]main\.ts/)
    else assert.match(diagnostic.err.stack, /lifecycle-bootstrap-fixture\.mjs/)
  }
})

const loggingProgram = pretty => `
  import { createLogging } from ${JSON.stringify(loggerUrl)};
  const logging = createLogging({level:'trace',pretty:${pretty}});
  const err = new AggregateError([new Error('aggregate-member'), {secret:'aggregate-secret', detail:'unknown-member'}], 'aggregate-root', {cause:new Error('cause-root', {cause:new Error('cause-leaf')})});
  logging.logger.error({err, object:{secret:'hidden-secret',authorization:'hidden-auth',safe:'visible'}, req:{headers:{authorization:'hidden-bearer','x-debug-token':'hidden-debug'}}, secret:'root-secret'}, 'diagnostic-test');
  const unknown = {detail:'unknown-reason', token:'unknown-token'}; unknown.self=unknown;
  logging.logger.error({err:unknown}, 'unknown-test');
  logging.logger.info({event:'last'}, 'last-line');
  const one=logging.close(); if(logging.close()!==one) throw Error('close not idempotent'); await one;
`

test('JSON logs preserve pid/local timestamp/redaction and complete cause/aggregate/nonError diagnostics', async () => {
  const result = await run(['--input-type=module', '-e', loggingProgram(false)])
  assert.equal(result.code, 0, result.stderr)
  const records = result.stdout.trim().split('\n').map(JSON.parse)
  assert.equal(records.length, 3)
  assert.equal(records[0].service, 'hermes-room-manager')
  assert.ok(records[0].pid > 0)
  assert.match(records[0].time, /T.*[+-]\d{2}:\d{2}$/)
  assert.equal(records[0].err.type, 'AggregateError')
  assert.equal(records[0].err.cause.cause.message, 'cause-leaf')
  assert.equal(records[0].err.errors[0].message, 'aggregate-member')
  assert.equal(records[1].err.type, 'NonError')
  assert.equal(records[1].err.value.self, '[circular]')
  assert.equal(records[2].msg, 'last-line')
  for (const secret of ['hidden-secret', 'hidden-auth', 'hidden-bearer', 'hidden-debug', 'root-secret', 'aggregate-secret', 'unknown-token']) assert.ok(!result.stdout.includes(secret))
  assert.match(result.stdout, /src[\\/]infra[\\/]logger|aggregate-root/)
})

test('pretty logs are readable in-process and retain cause, aggregate and final lines', async () => {
  const result = await run(['--input-type=module', '-e', loggingProgram(true)])
  assert.equal(result.code, 0, result.stderr)
  assert.match(result.stdout, /ERROR.*diagnostic-test/)
  for (const message of ['cause-leaf', 'aggregate-member', 'unknown-reason', 'last-line']) assert.ok(result.stdout.includes(message), result.stdout)
  assert.ok(!result.stdout.includes('hidden-secret'))
  assert.ok(!result.stdout.includes('unknown-token'))
})

test('diagnostics are bounded, cycle-safe, getter-safe and redact parser source', () => {
  const cycle = new Error('x'.repeat(100_000)); cycle.cause = cycle
  const result = normalizeError(cycle)
  assert.equal(result.cause, '[circular]')
  assert.ok(JSON.stringify(result).length < 20_000)
  const unknown = {}; Object.defineProperty(unknown, 'bad', { enumerable: true, get () { throw Error('getter ran') } })
  assert.equal(normalizeError(unknown).value.bad, '[accessor omitted]')
  const { proxy, revoke } = Proxy.revocable({}, {}); revoke()
  assert.doesNotThrow(() => normalizeError(proxy))
  const parser = new Error('secret-token shown in source'); parser.name = 'YAMLParseError'
  assert.ok(!JSON.stringify(normalizeError(parser)).includes('secret-token'))
})

const lifecycleProgram = body => `
  import {createLifecycle} from ${JSON.stringify(lifecycleUrl)};
  import {createLogging} from ${JSON.stringify(loggerUrl)};
  const lifecycle=createLifecycle({deadlineMs:150}); lifecycle.setLogging(createLogging({pretty:false,level:'silent'}));
  ${body}
`

test('shutdown duplicate signals run once; normal0, failed1, late failure1, hanging2', async () => {
  for (const [body, code, message] of [
    [`let count=0; lifecycle.setStop(async()=>{process.stdout.write('stop-'+(++count)+'\\n'); await new Promise(r=>setTimeout(r,20))}); lifecycle.install(); process.emit('SIGTERM'); process.emit('SIGINT');`, 0, 'manager stopped'],
    [`lifecycle.setStop(async()=>{throw new Error('cleanup-failure')}); await lifecycle.requestShutdown('test');`, 1, 'cleanup-failure'],
    [`lifecycle.setStop(()=>new Promise(r=>setTimeout(r,20))); const first=lifecycle.requestShutdown('normal'); if(first!==lifecycle.fail('late-error',new Error('late-failure'))) throw Error('duplicate'); await first;`, 1, 'late-failure'],
    [`lifecycle.setStop(()=>new Promise(()=>{})); await lifecycle.requestShutdown('hang');`, 2, 'shutdown deadline exceeded'],
    [`lifecycle.setStop(()=>{setInterval(()=>{},1000)}); await lifecycle.requestShutdown('stray-handle');`, 2, 'shutdown deadline exceeded'],
    [`lifecycle.setLogging({logger:createLogging({pretty:false}).logger,close:()=>Promise.reject(new Error('close-failure'))}); await lifecycle.requestShutdown('test');`, 1, 'close-failure']
  ]) {
    const result = await run(['--input-type=module', '-e', lifecycleProgram(body)])
    assert.equal(result.code, code, result.stderr)
    assert.ok(result.stderr.includes(message), result.stderr)
    if (code === 0) {
      assert.equal(result.stdout, 'stop-1\n')
      const final = result.stderr.trim().split('\n').map(JSON.parse).at(-1)
      assert.equal(final.level, 30)
      assert.equal(final.phase, 'stopped')
      assert.ok(final.uptimeMs >= 0)
      assert.ok(final.shutdownMs >= 0)
    }
  }
})

test('fatal uncaughtException and non-Error unhandledRejection terminate without business recovery', async () => {
  for (const fatal of [`setImmediate(()=>{throw new Error('uncaught-test',{cause:new Error('uncaught-cause')})})`, `Promise.reject({detail:'rejection-test',secret:'fatal-secret'})`]) {
    const result = await run(['--input-type=module', '-e', lifecycleProgram(`lifecycle.setStop(()=>{process.stdout.write('must-not-stop')}); lifecycle.install(); ${fatal}; setTimeout(()=>process.stdout.write('must-not-recover'),500);`)])
    assert.equal(result.code, 1, result.stderr)
    assert.match(result.stderr, /fatal.*terminating manager/)
    assert.equal(result.stdout, '')
    assert.ok(!result.stderr.includes('fatal-secret'))
  }
})
