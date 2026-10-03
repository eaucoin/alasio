import { logs, SeverityNumber, type LogAttributes, type LogRecord } from "@opentelemetry/api-logs";

/** Writes one line at one level: to the console, and as a log record when logs are exported. */
export type LogMethod = (message: string, attributes?: LogAttributes) => void;

/** A logger for one scope of alasio, as `createLogger` makes it. */
export interface Logger {
  readonly info: LogMethod;
  readonly warn: LogMethod;
  readonly error: LogMethod;
}

interface LogLevel {
  readonly severityNumber: SeverityNumber;
  readonly severityText: string;
  readonly write: (line: string) => void;
}

const LEVELS = {
  info: { severityNumber: SeverityNumber.INFO, severityText: "INFO", write: console.log },
  warn: { severityNumber: SeverityNumber.WARN, severityText: "WARN", write: console.warn },
  error: { severityNumber: SeverityNumber.ERROR, severityText: "ERROR", write: console.error },
} as const satisfies Record<keyof Logger, LogLevel>;

/**
 * A logger for one scope of alasio. Each line goes to the console as `[scope] message`
 * and, when logs are exported, as a log record of that scope, with `attributes` and
 * the trace it was written in.
 */
export function createLogger(scope: string): Logger {
  const records = logs.getLogger(scope);
  const method = ({ severityNumber, severityText, write }: LogLevel): LogMethod => (message, attributes) => {
    write(`[${scope}] ${message}`);
    const record: LogRecord = { severityNumber, severityText, body: String(message) };
    if (attributes !== undefined) {
      record.attributes = attributes;
    }
    records.emit(record);
  };
  return { info: method(LEVELS.info), warn: method(LEVELS.warn), error: method(LEVELS.error) };
}
