/**
 * What bayma inside a session's sandbox exports its telemetry with, and what alasio
 * stamps on it. The sandbox's one way out without internet is alasio's OTLP receiver
 * (./telemetry-receiver.ts), so bayma exports there, with its session's token, each
 * signal alasio exports; the receiver stamps what arrives with what alasio knows about
 * the session and exports it where alasio exports its own.
 */
import { parseKeyValueList, resolveTelemetry, sharedResourceAttributes, SIGNALS, telemetryEnabled } from "../telemetry/index.ts";
import type { ResourceStamp } from "./otlp-resource.ts";

/**
 * The standard variables bayma inside a sandbox exports with, but for the endpoint and
 * the token it sends, which are the sandbox's own: each signal alasio exports, in the
 * protocol alasio exports it in, and no other. Empty when alasio exports nothing.
 */
export function sandboxBaymaTelemetryEnv(env: Readonly<NodeJS.ProcessEnv> = process.env): Record<string, string> {
  const telemetry = resolveTelemetry(env);
  if (!telemetryEnabled(telemetry)) return {};
  const settings: Record<string, string> = {};
  for (const signal of SIGNALS) {
    const upper = signal.toUpperCase();
    const exporter = telemetry[signal];
    settings[`OTEL_${upper}_EXPORTER`] = exporter ? "otlp" : "none";
    if (exporter) settings[`OTEL_EXPORTER_OTLP_${upper}_PROTOCOL`] = exporter.protocol;
  }
  return settings;
}

/** The resource attributes a session's telemetry is stamped with. */
export function sandboxResource(volumeId: string, env: Readonly<NodeJS.ProcessEnv> = process.env): ResourceStamp {
  return { ...parseKeyValueList(sharedResourceAttributes(env)), "service.name": "bayma", "alasio.volume.id": volumeId };
}
