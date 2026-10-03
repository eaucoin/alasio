// @ts-nocheck
/**
 * Which of alasio's signals are exported, and where, decided from the standard
 * OpenTelemetry environment variables alone, so any OTLP backend works and none is
 * assumed.
 *
 * A signal is exported when it has an endpoint, its own
 * (`OTEL_EXPORTER_OTLP_<SIGNAL>_ENDPOINT`) or the shared one
 * (`OTEL_EXPORTER_OTLP_ENDPOINT`), and `OTEL_<SIGNAL>_EXPORTER` is unset or `otlp`;
 * `OTEL_SDK_DISABLED=true` turns every signal off. With no endpoint at all alasio
 * exports nothing and loads no SDK.
 */

export const SIGNALS = ["traces", "metrics", "logs"];

const DEFAULT_PROTOCOL = "http/protobuf";

/** Variables that configure telemetry, which alasio hands no child as it is. */
const TELEMETRY_VARIABLE = /^(OTEL_.*|TRACEPARENT|TRACESTATE)$/u;

function setting(env, name) {
  return env[name]?.trim() || null;
}

/**
 * The OTLP endpoint a signal is sent to: its own as given, else the shared one, to
 * which OTLP over HTTP adds the signal's path and OTLP over gRPC adds nothing.
 */
function signalEndpoint(env, signal, protocol) {
  const own = setting(env, `OTEL_EXPORTER_OTLP_${signal.toUpperCase()}_ENDPOINT`);
  if (own) return own;
  const shared = setting(env, "OTEL_EXPORTER_OTLP_ENDPOINT");
  if (!shared || protocol === "grpc") return shared;
  return `${shared.replace(/\/+$/u, "")}/v1/${signal}`;
}

/**
 * `{ traces, metrics, logs }`, each `{ endpoint, protocol, headers }` for a signal
 * alasio exports or null for one it does not. `headers` is the standard
 * `key=value,key=value` list, or null.
 */
export function resolveTelemetry(env = process.env) {
  const disabled = setting(env, "OTEL_SDK_DISABLED")?.toLowerCase() === "true";
  return Object.fromEntries(SIGNALS.map((signal) => {
    const upper = signal.toUpperCase();
    const exporter = setting(env, `OTEL_${upper}_EXPORTER`) ?? "otlp";
    const protocol = setting(env, `OTEL_EXPORTER_OTLP_${upper}_PROTOCOL`)
      ?? setting(env, "OTEL_EXPORTER_OTLP_PROTOCOL")
      ?? DEFAULT_PROTOCOL;
    const endpoint = signalEndpoint(env, signal, protocol);
    if (disabled || exporter !== "otlp" || !endpoint) {
      return [signal, null];
    }
    return [signal, { endpoint, protocol, headers: signalHeaders(env, signal) }];
  }));
}

/**
 * A signal's OTLP exporter setting `name` (HEADERS, TIMEOUT, ...): its own
 * (`OTEL_EXPORTER_OTLP_<SIGNAL>_<NAME>`), else the shared one, or null.
 */
export function signalSetting(env, signal, name) {
  return setting(env, `OTEL_EXPORTER_OTLP_${signal.toUpperCase()}_${name}`) ?? setting(env, `OTEL_EXPORTER_OTLP_${name}`);
}

/** The headers a signal is sent with: its own, else the shared ones, or null. */
export function signalHeaders(env, signal) {
  return signalSetting(env, signal, "HEADERS");
}

/** Whether any signal is exported. */
export function telemetryEnabled(telemetry) {
  return SIGNALS.some((signal) => telemetry[signal]);
}

/** An environment without the variables that configure telemetry. */
export function withoutTelemetry(env) {
  return Object.fromEntries(Object.entries(env).filter(([name]) => !TELEMETRY_VARIABLE.test(name)));
}

/**
 * The resource attributes a harness exporting telemetry of its own shares with alasio:
 * the operator's `OTEL_RESOURCE_ATTRIBUTES`, which describe the deployment, without
 * `service.name`, which is alasio's. The standard `key=value,key=value` list, or null.
 */
export function sharedResourceAttributes(env = process.env) {
  const shared = (setting(env, "OTEL_RESOURCE_ATTRIBUTES") ?? "")
    .split(",")
    .filter((pair) => pair.trim() && pair.split("=")[0].trim() !== "service.name");
  return shared.length ? shared.join(",") : null;
}

/**
 * The standard variables that have a process serving one conversation, Claude Code
 * or bayma, export each signal alasio exports where alasio exports it, and no other:
 * each exported signal's exporter, endpoint, protocol, and headers, every other
 * signal's exporter none, and the deployment's resource attributes with the
 * conversation's. Nothing when alasio exports nothing.
 */
export function conversationTelemetryEnv({ conversationId }, env = process.env) {
  const telemetry = resolveTelemetry(env);
  if (!telemetryEnabled(telemetry)) return {};
  const settings = {
    OTEL_RESOURCE_ATTRIBUTES: [sharedResourceAttributes(env), `alasio.conversation.id=${encodeURIComponent(conversationId)}`]
      .filter(Boolean)
      .join(","),
  };
  for (const signal of SIGNALS) {
    const upper = signal.toUpperCase();
    const exporter = telemetry[signal];
    settings[`OTEL_${upper}_EXPORTER`] = exporter ? "otlp" : "none";
    if (!exporter) continue;
    settings[`OTEL_EXPORTER_OTLP_${upper}_ENDPOINT`] = exporter.endpoint;
    settings[`OTEL_EXPORTER_OTLP_${upper}_PROTOCOL`] = exporter.protocol;
    if (exporter.headers) settings[`OTEL_EXPORTER_OTLP_${upper}_HEADERS`] = exporter.headers;
  }
  return settings;
}

/**
 * A standard `key=value,key=value` list, such as OTLP headers or resource attributes, as
 * an object, values URL-decoded.
 */
export function parseKeyValueList(list) {
  if (!list) return {};
  return Object.fromEntries(list.split(",").flatMap((pair) => {
    const at = pair.indexOf("=");
    if (at <= 0) return [];
    return [[pair.slice(0, at).trim(), decodeURIComponent(pair.slice(at + 1).trim())]];
  }));
}
