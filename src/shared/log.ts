import { type AnyValue, logs, SeverityNumber, type LogAttributes, type LogRecord } from "@opentelemetry/api-logs";
import { Array as Arr, Cause, Effect, Logger as EffectLogger, type LogLevel as EffectLogLevel, References } from "effect";

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

/** The log annotation naming the scope an effect's log lines belong to. */
const SCOPE = "scope";

/** Puts an effect's log lines in `scope`, as `createLogger(scope)` puts a logger's. */
export const withLogScope = (scope: string): (<A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>) =>
  Effect.annotateLogs(SCOPE, scope);

const loggers = new Map<string, Logger>();

function loggerFor(scope: string): Logger {
  let logger = loggers.get(scope);
  if (logger === undefined) {
    logger = createLogger(scope);
    loggers.set(scope, logger);
  }
  return logger;
}

/** The method an Effect log level writes with; alasio's loggers have three. */
function methodFor(level: EffectLogLevel.LogLevel): keyof Logger | null {
  switch (level) {
    case "Fatal":
    case "Error":
      return "error";
    case "Warn":
      return "warn";
    case "All":
    case "Info":
    case "Debug":
    case "Trace":
      return "info";
    case "None":
      return null;
  }
}

/** A log annotation as a log record attribute: as it is when it is one, as JSON otherwise. */
function attributeOf(value: unknown): AnyValue {
  return typeof value === "string" || typeof value === "number" || typeof value === "boolean" ? value : JSON.stringify(value);
}

/**
 * Effects' log lines, written as createLogger writes them: `[scope] message` on the
 * console and a log record of the scope, the line's other annotations its attributes.
 * A line logged with a failure carries the failure, rendered as Effect renders it.
 */
const alasioLogger = EffectLogger.make<unknown, void>(({ message, logLevel, cause, fiber }) => {
  const method = methodFor(logLevel);
  if (method === null) return;
  const { [SCOPE]: scope, ...annotations } = fiber.getRef(References.CurrentLogAnnotations);
  const text = Arr.ensure(message).map((part) => (typeof part === "string" ? part : JSON.stringify(part))).join(" ");
  const line = cause.reasons.length > 0 ? `${text}\n${Cause.pretty(cause)}` : text;
  const names = Object.keys(annotations);
  const attributes = names.length > 0
    ? Object.fromEntries(names.map((name) => [name, attributeOf(annotations[name])]))
    : undefined;
  loggerFor(typeof scope === "string" ? scope : "alasio")[method](line, attributes);
});

/** alasio's logger in place of Effect's default one. */
export const AlasioLoggerLayer = EffectLogger.layer([alasioLogger]);
