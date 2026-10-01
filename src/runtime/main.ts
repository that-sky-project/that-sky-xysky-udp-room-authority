import { loadConfig } from "../config/index.js";
import { logger } from "../infra/logger/logger.js";
import { ManagerGateway } from "../manager-host/ManagerGateway.js";
import { createManagerRuntime } from "../manager-host/ManagerRuntime.js";

const config = loadConfig();
logger.level = config.logLevel;
const runtime = createManagerRuntime(config);
const gateway = new ManagerGateway(config, runtime, logger);
await gateway.start();

const shutdown = async (signal: NodeJS.Signals): Promise<void> => {
  logger.warn({ signal }, "shutting down manager");
  await gateway.stop();
  process.exit(0);
};

process.once("SIGTERM", shutdown);
process.once("SIGINT", shutdown);
