// @ts-nocheck
/**
 * Claude Code's own telemetry, sent where alasio sends its. Claude Code exports when
 * CLAUDE_CODE_ENABLE_TELEMETRY is set and reads the standard variables for the rest,
 * so each signal alasio exports is set for it explicitly, and each it does not is off:
 * its metrics, its events as logs, and its traces, a beta of Claude Code's.
 *
 * Claude Code's process serves one conversation across many turns, so its telemetry
 * carries the conversation as a resource attribute, and its traces are its own rather
 * than children of the turn that started it (see ./live-sessions.ts).
 */
import { conversationTelemetryEnv } from "../../telemetry/index.ts";

/** Claude Code's own settings of what its telemetry includes, which the operator sets for it. */
const CLAUDE_CODE_SETTING = /^OTEL_(LOG_.*|METRICS_INCLUDE_.*)$/u;

export function claudeTelemetryEnv({ conversationId }, env = process.env) {
  const settings = conversationTelemetryEnv({ conversationId }, env);
  if (Object.keys(settings).length === 0) {
    return {};
  }
  settings.CLAUDE_CODE_ENABLE_TELEMETRY = "1";
  // Claude Code defaults to delta; the OpenTelemetry default, which alasio uses, is cumulative.
  settings.OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE = env.OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE ?? "cumulative";
  if (settings.OTEL_TRACES_EXPORTER === "otlp") {
    settings.CLAUDE_CODE_ENHANCED_TELEMETRY_BETA = "1";
  }
  for (const [name, value] of Object.entries(env)) {
    if (CLAUDE_CODE_SETTING.test(name)) settings[name] = value;
  }
  return settings;
}
