import { createLifecycle } from './lifecycle.js'

// Only built-in diagnostics are statically reachable. Install fatal handlers
// before loading YAML/Pino/manager modules so import and bootstrap failures are
// reported even when configured logging cannot be constructed.
process.setSourceMapsEnabled(true)
const lifecycle = createLifecycle()
lifecycle.install()
let stage = 'module imports'
const setStage = (value: string): void => {
  stage = value
  lifecycle.setPhase(value)
}
setStage(stage)
try {
  const [{ loadConfig }, { createLogging }] = await Promise.all([
    import('../config/index.js'),
    import('../infra/logger/logger.js')
  ])
  if (!lifecycle.isStopping()) {
    setStage('configuration')
    const config = loadConfig()
    setStage('logger construction')
    const logging = createLogging({ level: config.logLevel, pretty: config.logPretty })
    lifecycle.setLogging(logging)
    setStage('manager imports')
    const [{ ManagerGateway }, { createManagerRuntime }] = await Promise.all([
      import('../manager-host/ManagerGateway.js'),
      import('../manager-host/ManagerRuntime.js')
    ])
    if (!lifecycle.isStopping()) {
      setStage('runtime construction')
      const runtime = createManagerRuntime(config, logging.logger)
      // Clean up the runtime even if the gateway constructor fails.
      lifecycle.setStop(async () => {
        runtime.pending.close()
        runtime.reservations.stop()
        await runtime.moves.stop()
      })
      setStage('gateway construction')
      const gateway = new ManagerGateway(config, runtime, logging.logger)
      lifecycle.setStop(() => gateway.stop())
      setStage('listen')
      await gateway.start()
      if (!lifecycle.isStopping()) setStage('running')
    }
  }
} catch (error) {
  await lifecycle.fail(`manager startup failed (${stage})`, error)
}
