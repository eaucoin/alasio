/**
 * Codex's own telemetry, sent where alasio sends its: the app-server exports its logs,
 * traces, and metrics through the `[otel]` section of its config, set here as `-c`
 * overrides for each signal alasio exports. A signal alasio does not export is left to
 * the operator's config.toml.
 */
import { parseHeaders, resolveTelemetry, sharedResourceAttributes } from "../../telemetry/index.js";

/** The `[otel]` key that configures each signal's exporter. */
const EXPORTER_KEYS = { logs: "exporter", traces: "trace_exporter", metrics: "metrics_exporter" };

const toml = (value) => JSON.stringify(value);

function tomlTable(entries) {
  return `{${entries.map(([key, value]) => `${toml(key)}=${value}`).join(",")}}`;
}

/** A signal's exporter as Codex writes it: OTLP over gRPC, or over HTTP as protobuf or JSON. */
function exporterValue({ endpoint, protocol, headers }) {
  const settings = [["endpoint", toml(endpoint)], ["headers", tomlTable(Object.entries(parseHeaders(headers)).map(([key, value]) => [key, toml(value)]))]];
  if (protocol === "grpc") {
    return tomlTable([["otlp-grpc", tomlTable(settings)]]);
  }
  return tomlTable([["otlp-http", tomlTable([...settings, ["protocol", toml(protocol === "http/json" ? "json" : "binary")]])]]);
}

/** The app-server arguments that export Codex's telemetry with alasio's. */
export function codexTelemetryArgs(env = process.env) {
  const telemetry = resolveTelemetry(env);
  return Object.entries(EXPORTER_KEYS).flatMap(([signal, key]) => (
    telemetry[signal] ? ["-c", `otel.${key}=${exporterValue(telemetry[signal])}`] : []
  ));
}

/** The app-server's environment additions: the deployment's resource attributes. */
export function codexTelemetryEnv(env = process.env) {
  const attributes = sharedResourceAttributes(env);
  return attributes ? { OTEL_RESOURCE_ATTRIBUTES: attributes } : {};
}
