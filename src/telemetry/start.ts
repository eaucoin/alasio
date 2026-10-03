// @ts-nocheck
/**
 * Starts the OpenTelemetry SDK for the signals ./config.ts resolves, before the rest of
 * alasio is loaded (see src/index.ts), and stops it, flushing what it holds, as alasio
 * exits. With no signal to export the SDK is never loaded.
 */
import { execFileSync } from "node:child_process";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";

import { diag, DiagLogLevel } from "@opentelemetry/api";

import { resolveTelemetry, SIGNALS, telemetryEnabled } from "./config.ts";

/** The modules alasio's instrumentations patch, which the import hook must intercept. */
const INSTRUMENTED_MODULES = ["pg", "pg-pool", "http", "https", "node:http", "node:https"];

let sdk = null;

/** The commit alasio runs from, or null outside a git checkout. */
function revision() {
  try {
    return execFileSync("git", ["rev-parse", "HEAD"], {
      cwd: dirname(fileURLToPath(import.meta.url)),
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
    }).trim();
  } catch {
    return null;
  }
}

/** The SDK's own problems, exporting above all, as alasio logs: on the console only. */
const diagnostics = Object.fromEntries(["error", "warn", "info", "debug", "verbose"].map((level) => [
  level,
  (message, ...args) => (level === "error" ? console.error : console.warn)(`[telemetry] ${[message, ...args].join(" ")}`),
]));

export async function startTelemetry(env = process.env) {
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
  const commit = revision();
  sdk = new NodeSDK({
    // OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES, read by the SDK, override these.
    resource: defaultResource().merge(resourceFromAttributes({
      "service.name": "alasio",
      ...(commit ? { "vcs.ref.head.revision": commit } : {}),
    })),
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
export async function stopTelemetry() {
  await sdk?.shutdown();
  sdk = null;
}
