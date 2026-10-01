import pino from "pino";

// Level defaults to "info" and is overridden from config.yml at startup
// (see runtime/main.ts). The manager does not read environment variables.
export const logger = pino({
  level: "info",
  base: {
    service: "hermes-room-manager"
  },
  timestamp: pino.stdTimeFunctions.isoTime,
  redact: {
    paths: ["*.secret", "*.authorization", "req.headers.authorization"],
    remove: true
  }
});

export type Logger = typeof logger;
