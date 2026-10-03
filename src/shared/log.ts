// @ts-nocheck
import { logs, SeverityNumber } from "@opentelemetry/api-logs";

const LEVELS = {
  info: { severityNumber: SeverityNumber.INFO, severityText: "INFO", write: console.log },
  warn: { severityNumber: SeverityNumber.WARN, severityText: "WARN", write: console.warn },
  error: { severityNumber: SeverityNumber.ERROR, severityText: "ERROR", write: console.error },
};

/**
 * A logger for one scope of alasio. Each line goes to the console as `[scope] message`
 * and, when logs are exported, as a log record of that scope, with `attributes` and
 * the trace it was written in.
 */
export function createLogger(scope) {
  const records = logs.getLogger(scope);
  return Object.fromEntries(Object.entries(LEVELS).map(([level, { severityNumber, severityText, write }]) => [
    level,
    (message, attributes) => {
      write(`[${scope}] ${message}`);
      records.emit({ severityNumber, severityText, body: String(message), attributes });
    },
  ]));
}
