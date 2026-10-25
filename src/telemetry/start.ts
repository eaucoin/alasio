/**
 * Starts the OpenTelemetry SDK for the signals ./config.ts resolves, before the rest of
 * alasio is loaded (see src/index.ts), and stops it, flushing what it holds, as alasio
 * exits. With no signal to export the SDK is never loaded.
 */
import { diag, type DiagLogFunction, type DiagLogger, DiagLogLevel } from "@opentelemetry/api";
import type { NodeSDK } from "@opentelemetry/sdk-node";

import { resolveTelemetry, SIGNALS, telemetryEnabled } from "./config.ts";

/** The modules alasio's instrumentations patch, which the import hook must intercept. */
const INSTRUMENTED_MODULES = ["pg", "pg-pool", "http", "https", "node:http", "node:https"];

let sdk: NodeSDK | null = null;

/** The variable alasio's image says the commit it was built from in (Dockerfile's REVISION). */
export const REVISION_VARIABLE = "ALASIO_REVISION";

/**
 * What alasio's telemetry says of the service it is of: alasio, and the commit its image was
 * built from, where the image says one (REVISION_VARIABLE); none is asked of git as alasio
 * starts, which would spawn a process on its way to its first answer. Pure, for tests.
 */
export function serviceAttributes(env: Readonly<NodeJS.ProcessEnv>): Record<string, string> {
  const revision = env[REVISION_VARIABLE]?.trim();
  return { "service.name": "alasio", ...(revision ? { "vcs.ref.head.revision": revision } : {}) };
}

/** The SDK's own problems, exporting above all, as alasio logs: on the console only. */
const diagnostic = (level: keyof DiagLogger): DiagLogFunction =>
  (message, ...args) => (level === "error" ? console.error : console.warn)(`[telemetry] ${[message, ...args].join(" ")}`);
const diagnostics: DiagLogger = {
  error: diagnostic("error"),
  warn: diagnostic("warn"),
  info: diagnostic("info"),
  debug: diagnostic("debug"),
  verbose: diagnostic("verbose"),
};

export async function startTelemetry(env: NodeJS.ProcessEnv = process.env): Promise<void> {
  const telemetry = resolveTelemetry(env);
  if (!telemetryEnabled(telemetry)) {
    return;
  }
  // The SDK reads each signal's exporter from the environment and defaults to OTLP for
  // one left unset, so a signal alasio does not export is set to none.
  for (const signal of SIGNALS) {
    env[`OTEL_${signal.toUpperCase()}_EXPORTER`] = telemetry[signal] ? "otlp" : "none";
  }
  diag.setLogger(diagnostics, DiagLogLevel.WARN);
  const { register } = await import("import-in-the-middle/register-hooks.mjs");
  register({ include: INSTRUMENTED_MODULES });
  const [{ NodeSDK }, { defaultResource, resourceFromAttributes }, { HttpInstrumentation }, { PgInstrumentation }, { RuntimeNodeInstrumentation }] = await Promise.all([
    import("@opentelemetry/sdk-node"),
    import("@opentelemetry/resources"),
    import("@opentelemetry/instrumentation-http"),
    import("@opentelemetry/instrumentation-pg"),
    import("@opentelemetry/instrumentation-runtime-node"),
  ]);
  sdk = new NodeSDK({
    // OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES, read by the SDK, override these.
    resource: defaultResource().merge(resourceFromAttributes(serviceAttributes(env))),
    instrumentations: [
      new HttpInstrumentation(),
      // Queries outside a trace (the transcript indexer's polling) would each be a
      // trace of their own; they still count in the pool's metrics.
      new PgInstrumentation({ requireParentSpan: true }),
      new RuntimeNodeInstrumentation(),
    ],
  });
  sdk.start();
}

/** Flushes and stops the SDK, if it was started. */
export async function stopTelemetry(): Promise<void> {
  await sdk?.shutdown();
  sdk = null;
}
