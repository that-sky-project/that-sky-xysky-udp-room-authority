import pino from "pino";

export const logger = pino({
  level: process.env.LOG_LEVEL ?? "info",
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
