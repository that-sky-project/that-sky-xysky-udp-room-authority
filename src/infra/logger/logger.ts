import pino from "pino";

const pad2 = (value: number): string => String(value).padStart(2, "0");

/**
 * ISO-8601 timestamp in the process's local timezone, including the offset.
 * pino's isoTime always prints UTC, so a container with TZ=Asia/Shanghai still
 * logged `...Z` while the UDP nodes logged `+0800`. This keeps the two logs
 * readable side by side; protocol timestamps (Date.now()) are unaffected.
 */
const localTimestamp = (): string => {
  const now = new Date();
  const offsetMinutes = -now.getTimezoneOffset();
  const sign = offsetMinutes >= 0 ? "+" : "-";
  const absolute = Math.abs(offsetMinutes);
  return `${now.getFullYear()}-${pad2(now.getMonth() + 1)}-${pad2(now.getDate())}`
    + `T${pad2(now.getHours())}:${pad2(now.getMinutes())}:${pad2(now.getSeconds())}`
    + `.${String(now.getMilliseconds()).padStart(3, "0")}`
    + `${sign}${pad2(Math.floor(absolute / 60))}:${pad2(absolute % 60)}`;
};

// Level defaults to "info" and is overridden from config.yml at startup
// (see runtime/main.ts). The manager does not read environment variables.
export const logger = pino({
  level: "info",
  base: {
    service: "hermes-room-manager"
  },
  timestamp: () => `,"time":"${localTimestamp()}"`,
  redact: {
    paths: ["*.secret", "*.authorization", "req.headers.authorization"],
    remove: true
  }
});

export type Logger = typeof logger;
