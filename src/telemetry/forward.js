/**
 * Exports OTLP requests that something else encoded, as they are, over OTLP/HTTP to
 * where alasio exports its own signals: what alasio relays from inside a session's sandbox
 * (src/sandbox/telemetry.js). Each signal goes to its endpoint with its headers,
 * timeout, compression, and TLS files from the standard variables, as OpenTelemetry's
 * own exporters send alasio's, and is retried as they retry: on 429, 502, 503, and 504
 * and on a failed connection, after the server's Retry-After or a jittered backoff,
 * within the signal's timeout.
 *
 * A signal alasio exports over gRPC is not forwarded: what is relayed comes from bayma,
 * which exports over OTLP's HTTP protocols only.
 */
import { readFileSync } from "node:fs";
import { Agent as HttpAgent, request as httpRequest } from "node:http";
import { Agent as HttpsAgent, request as httpsRequest } from "node:https";
import { setTimeout as sleep } from "node:timers/promises";
import { gzipSync } from "node:zlib";

import { context } from "@opentelemetry/api";
import { suppressTracing } from "@opentelemetry/core";

import { parseKeyValueList, resolveTelemetry, SIGNALS, signalSetting } from "./config.js";

const CONTENT_TYPES = { protobuf: "application/x-protobuf", json: "application/json" };
const DEFAULT_TIMEOUT_MS = 10_000;
const RETRYABLE_STATUS = new Set([429, 502, 503, 504]);
const MAX_RETRIES = 5;
const INITIAL_BACKOFF_MS = 1000;
const MAX_BACKOFF_MS = 5000;
const BACKOFF_MULTIPLIER = 1.5;
const JITTER = 0.2;
// A response body is never needed, only drained; one this large is not an OTLP server's.
const MAX_RESPONSE_BYTES = 4 * 1024 * 1024;

/** The milliseconds a Retry-After header asks to wait, or null. */
export function retryAfterMs(header, now = Date.now()) {
  if (!header) return null;
  const seconds = Number(header);
  if (Number.isFinite(seconds) && seconds >= 0) return seconds * 1000;
  const at = Date.parse(header);
  return Number.isNaN(at) ? null : Math.max(0, at - now);
}

function tlsFiles(env, signal, warn) {
  const files = { ca: "CERTIFICATE", cert: "CLIENT_CERTIFICATE", key: "CLIENT_KEY" };
  return Object.fromEntries(Object.entries(files).flatMap(([option, name]) => {
    const path = signalSetting(env, signal, name);
    if (!path) return [];
    try {
      return [[option, readFileSync(path)]];
    } catch (error) {
      warn(`${signal}: cannot read OTEL_EXPORTER_OTLP_${name} ${path}: ${error.message}`);
      return [];
    }
  }));
}

/** How each exported signal is sent, from the standard variables. */
function signalTargets(env, warn) {
  const telemetry = resolveTelemetry(env);
  return Object.fromEntries(SIGNALS.flatMap((signal) => {
    const exporter = telemetry[signal];
    if (!exporter) return [];
    if (!exporter.protocol.startsWith("http/")) {
      warn(`${signal} relayed from session sandboxes are not exported: alasio exports ${signal} over ${exporter.protocol}, and bayma exports over http/protobuf or http/json only`);
      return [];
    }
    const url = new URL(exporter.endpoint);
    const timeout = Number(signalSetting(env, signal, "TIMEOUT"));
    const https = url.protocol === "https:";
    return [[signal, {
      url,
      protocol: exporter.protocol,
      headers: parseKeyValueList(exporter.headers),
      timeoutMs: Number.isFinite(timeout) && timeout > 0 ? timeout : DEFAULT_TIMEOUT_MS,
      gzip: signalSetting(env, signal, "COMPRESSION") === "gzip",
      request: https ? httpsRequest : httpRequest,
      agent: https ? new HttpsAgent({ keepAlive: true, ...tlsFiles(env, signal, warn) }) : new HttpAgent({ keepAlive: true }),
    }]];
  }));
}

/** One POST: `{ status, retryAfter }`, or `{ error }` for a connection that failed. */
function post(target, body, headers, timeoutMs) {
  return new Promise((resolve) => {
    // alasio's own HTTP instrumentation would otherwise trace every export it relays.
    context.with(suppressTracing(context.active()), () => {
      const req = target.request(target.url, { method: "POST", agent: target.agent, headers, timeout: timeoutMs }, (res) => {
        let received = 0;
        res.on("data", (chunk) => {
          received += chunk.length;
          if (received > MAX_RESPONSE_BYTES) res.destroy();
        });
        res.on("error", () => {});
        res.on("close", () => resolve({ status: res.statusCode, retryAfter: retryAfterMs(res.headers["retry-after"]) }));
      });
      req.on("timeout", () => req.destroy(new Error(`no response within ${timeoutMs}ms`)));
      req.on("error", (error) => resolve({ error }));
      req.end(body);
    });
  });
}

/**
 * The forwarder for the signals `env` exports. `protocols` maps each signal it forwards
 * to the protocol alasio exports it over (http/protobuf or http/json); a signal it does
 * not forward is absent. `export(signal, encoding, body)` sends one request, encoded as
 * `encoding` ("protobuf" or "json"), and resolves `{ ok: true }` or `{ ok: false, error }`
 * once it is sent or given up on. `close()` ends the connections it keeps open.
 */
export function createOtlpForwarder(env = process.env, { warn = () => {} } = {}) {
  const targets = signalTargets(env, warn);
  return {
    protocols: Object.fromEntries(Object.entries(targets).map(([signal, target]) => [signal, target.protocol])),

    async export(signal, encoding, body) {
      const target = targets[signal];
      if (!target) return { ok: false, error: `${signal} are not forwarded` };
      const headers = {
        ...target.headers,
        "content-type": CONTENT_TYPES[encoding],
        ...(target.gzip ? { "content-encoding": "gzip" } : {}),
      };
      const payload = target.gzip ? gzipSync(body) : body;
      const deadline = Date.now() + target.timeoutMs;
      let backoff = INITIAL_BACKOFF_MS;
      for (let attempt = 0; ; attempt += 1) {
        const result = await post(target, payload, headers, Math.max(1, deadline - Date.now()));
        if (result.status >= 200 && result.status < 300) return { ok: true };
        const error = result.error?.message ?? `HTTP ${result.status}`;
        if (attempt === MAX_RETRIES || !(result.error || RETRYABLE_STATUS.has(result.status))) return { ok: false, error };
        const wait = result.retryAfter ?? Math.min(backoff * (1 + (Math.random() * 2 - 1) * JITTER), MAX_BACKOFF_MS);
        backoff *= BACKOFF_MULTIPLIER;
        if (Date.now() + wait >= deadline) return { ok: false, error };
        await sleep(wait);
      }
    },

    close() {
      for (const target of Object.values(targets)) target.agent.destroy();
    },
  };
}
