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

export const SIGNALS = ["traces", "metrics", "logs"] as const;

/** One of the OpenTelemetry signals alasio exports. */
export type Signal = (typeof SIGNALS)[number];

/** How alasio exports one signal. */
export interface SignalExporter {
  readonly endpoint: string;
  readonly protocol: string;
  /** The standard `key=value,key=value` list, or null. */
  readonly headers: string | null;
}

/** Each signal's exporter, or null for a signal alasio does not export. */
export type Telemetry = Readonly<Record<Signal, SignalExporter | null>>;

const DEFAULT_PROTOCOL = "http/protobuf";

/** Variables that configure telemetry, which alasio hands no child as it is. */
const TELEMETRY_VARIABLE = /^(OTEL_.*|TRACEPARENT|TRACESTATE)$/u;

function setting(env: Readonly<NodeJS.ProcessEnv>, name: string): string | null {
  return env[name]?.trim() || null;
}

/**
 * The OTLP endpoint a signal is sent to: its own as given, else the shared one, to
 * which OTLP over HTTP adds the signal's path and OTLP over gRPC adds nothing.
 */
function signalEndpoint(env: Readonly<NodeJS.ProcessEnv>, signal: Signal, protocol: string): string | null {
  const own = setting(env, `OTEL_EXPORTER_OTLP_${signal.toUpperCase()}_ENDPOINT`);
  if (own) return own;
  const shared = setting(env, "OTEL_EXPORTER_OTLP_ENDPOINT");
  if (!shared || protocol === "grpc") return shared;
  return `${shared.replace(/\/+$/u, "")}/v1/${signal}`;
}

/** How `signal` is exported, or null when it is not. */
function signalExporter(env: Readonly<NodeJS.ProcessEnv>, signal: Signal, disabled: boolean): SignalExporter | null {
  const upper = signal.toUpperCase();
  const exporter = setting(env, `OTEL_${upper}_EXPORTER`) ?? "otlp";
  const protocol = setting(env, `OTEL_EXPORTER_OTLP_${upper}_PROTOCOL`)
    ?? setting(env, "OTEL_EXPORTER_OTLP_PROTOCOL")
    ?? DEFAULT_PROTOCOL;
  const endpoint = signalEndpoint(env, signal, protocol);
  if (disabled || exporter !== "otlp" || !endpoint) {
    return null;
  }
  return { endpoint, protocol, headers: signalHeaders(env, signal) };
}

/**
 * `{ traces, metrics, logs }`, each `{ endpoint, protocol, headers }` for a signal
 * alasio exports or null for one it does not. `headers` is the standard
 * `key=value,key=value` list, or null.
 */
export function resolveTelemetry(env: Readonly<NodeJS.ProcessEnv> = process.env): Telemetry {
  const disabled = setting(env, "OTEL_SDK_DISABLED")?.toLowerCase() === "true";
  return {
    traces: signalExporter(env, "traces", disabled),
    metrics: signalExporter(env, "metrics", disabled),
    logs: signalExporter(env, "logs", disabled),
  };
}

/**
 * A signal's OTLP exporter setting `name` (HEADERS, TIMEOUT, ...): its own
 * (`OTEL_EXPORTER_OTLP_<SIGNAL>_<NAME>`), else the shared one, or null.
 */
export function signalSetting(env: Readonly<NodeJS.ProcessEnv>, signal: Signal, name: string): string | null {
  return setting(env, `OTEL_EXPORTER_OTLP_${signal.toUpperCase()}_${name}`) ?? setting(env, `OTEL_EXPORTER_OTLP_${name}`);
}

/** The headers a signal is sent with: its own, else the shared ones, or null. */
export function signalHeaders(env: Readonly<NodeJS.ProcessEnv>, signal: Signal): string | null {
  return signalSetting(env, signal, "HEADERS");
}

/** Whether any signal is exported. */
export function telemetryEnabled(telemetry: Telemetry): boolean {
  return SIGNALS.some((signal) => telemetry[signal]);
}

/** An environment without the variables that configure telemetry. */
export function withoutTelemetry(env: Readonly<NodeJS.ProcessEnv>): NodeJS.ProcessEnv {
  return Object.fromEntries(Object.entries(env).filter(([name]) => !TELEMETRY_VARIABLE.test(name)));
}

/**
 * The resource attributes a harness exporting telemetry of its own shares with alasio:
 * the operator's `OTEL_RESOURCE_ATTRIBUTES`, which describe the deployment, without
 * `service.name`, which is alasio's. The standard `key=value,key=value` list, or null.
 */
export function sharedResourceAttributes(env: Readonly<NodeJS.ProcessEnv> = process.env): string | null {
  const shared = (setting(env, "OTEL_RESOURCE_ATTRIBUTES") ?? "")
    .split(",")
    .filter((pair) => pair.trim() && pair.split("=")[0]?.trim() !== "service.name");
  return shared.length ? shared.join(",") : null;
}

/**
 * The standard variables that have a process serving one conversation, Claude Code
 * or bayma, export each signal alasio exports where alasio exports it, and no other:
 * each exported signal's exporter, endpoint, protocol, and headers, every other
 * signal's exporter none, and the deployment's resource attributes with the
 * conversation's. Nothing when alasio exports nothing.
 */
export function conversationTelemetryEnv(
  { conversationId }: { readonly conversationId: string },
  env: Readonly<NodeJS.ProcessEnv> = process.env,
): Record<string, string> {
  const telemetry = resolveTelemetry(env);
  if (!telemetryEnabled(telemetry)) return {};
  const settings: Record<string, string> = {
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
export function parseKeyValueList(list: string | null): Record<string, string> {
  if (!list) return {};
  return Object.fromEntries(list.split(",").flatMap((pair): [string, string][] => {
    const at = pair.indexOf("=");
    if (at <= 0) return [];
    return [[pair.slice(0, at).trim(), decodeURIComponent(pair.slice(at + 1).trim())]];
  }));
}
