/**
 * Codex's own telemetry, sent where alasio sends its: the app-server exports its logs,
 * traces, and metrics through the `[otel]` section of its config, set here as `-c`
 * overrides for each signal alasio exports. A signal alasio does not export is left to
 * the operator's config.toml.
 */
import {
  parseKeyValueList,
  resolveTelemetry,
  type Signal,
  type SignalExporter,
  sharedResourceAttributes,
} from "../../telemetry/index.ts";

/** Each signal, with the `[otel]` key that configures its exporter. */
const EXPORTER_KEYS: readonly (readonly [Signal, string])[] = [["logs", "exporter"], ["traces", "trace_exporter"], ["metrics", "metrics_exporter"]];

const toml = (value: string) => JSON.stringify(value);

function tomlTable(entries: readonly (readonly [string, string])[]) {
  return `{${entries.map(([key, value]) => `${toml(key)}=${value}`).join(",")}}`;
}

/** A signal's exporter as Codex writes it: OTLP over gRPC, or over HTTP as protobuf or JSON. */
function exporterValue({ endpoint, protocol, headers }: SignalExporter) {
  const settings: (readonly [string, string])[] = [["endpoint", toml(endpoint)], ["headers", tomlTable(Object.entries(parseKeyValueList(headers)).map(([key, value]) => [key, toml(value)]))]];
  if (protocol === "grpc") {
    return tomlTable([["otlp-grpc", tomlTable(settings)]]);
  }
  return tomlTable([["otlp-http", tomlTable([...settings, ["protocol", toml(protocol === "http/json" ? "json" : "binary")]])]]);
}

/** The app-server arguments that export Codex's telemetry with alasio's. */
export function codexTelemetryArgs(env: Readonly<NodeJS.ProcessEnv> = process.env): string[] {
  const telemetry = resolveTelemetry(env);
  return EXPORTER_KEYS.flatMap(([signal, key]) => (
    telemetry[signal] ? ["-c", `otel.${key}=${exporterValue(telemetry[signal])}`] : []
  ));
}

/** The app-server's environment additions: the deployment's resource attributes. */
export function codexTelemetryEnv(env: Readonly<NodeJS.ProcessEnv> = process.env): { OTEL_RESOURCE_ATTRIBUTES?: string } {
  const attributes = sharedResourceAttributes(env);
  return attributes ? { OTEL_RESOURCE_ATTRIBUTES: attributes } : {};
}
