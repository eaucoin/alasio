/**
 * Exports OTLP requests that something else encoded, as they are, over OTLP/HTTP to
 * where alasio exports its own signals: what alasio relays from inside a session's sandbox
 * (src/sandbox/telemetry.ts). Each signal goes to its endpoint with its headers,
 * timeout, compression, and TLS files from the standard variables, as OpenTelemetry's
 * own exporters send alasio's, and is retried as they retry: on 429, 502, 503, and 504
 * and on a failed connection, after the server's Retry-After or a jittered backoff,
 * within the signal's timeout.
 *
 * A signal alasio exports over gRPC is not forwarded: what is relayed comes from bayma,
 * which exports over OTLP's HTTP protocols only.
 */
import { readFileSync } from "node:fs";
import { Agent as HttpAgent, type OutgoingHttpHeaders, request as httpRequest } from "node:http";
import { Agent as HttpsAgent, request as httpsRequest } from "node:https";
import { setTimeout as sleep } from "node:timers/promises";
import { gzipSync } from "node:zlib";

import { context } from "@opentelemetry/api";
import { suppressTracing } from "@opentelemetry/core";

import { parseKeyValueList, resolveTelemetry, type Signal, SIGNALS, signalSetting } from "./config.ts";

/** How an OTLP request is encoded: binary protobuf or JSON. */
export type OtlpEncoding = "protobuf" | "json";

/** What exporting one request came to. */
export type ForwardResult = { readonly ok: true } | { readonly ok: false; readonly error: string };

/** What the forwarder offers; see createOtlpForwarder. */
export interface OtlpForwarder {
  readonly protocols: Partial<Record<Signal, string>>;
  export(signal: Signal, encoding: OtlpEncoding, body: Buffer): Promise<ForwardResult>;
  close(): void;
}

type Warn = (message: string) => void;

/** How one signal is sent. */
interface SignalTarget {
  readonly url: URL;
  readonly protocol: string;
  readonly headers: Record<string, string>;
  readonly timeoutMs: number;
  readonly gzip: boolean;
  readonly request: typeof httpRequest;
  readonly agent: HttpAgent;
}

/** What one POST came to: the response's status and Retry-After, or the error a failed connection gave. */
interface PostResult {
  readonly status?: number | undefined;
  readonly retryAfter?: number | null;
  readonly error?: Error;
}

/** The TLS files a signal's connections use. */
interface TlsFiles {
  ca?: Buffer;
  cert?: Buffer;
  key?: Buffer;
}

const CONTENT_TYPES: Readonly<Record<OtlpEncoding, string>> = { protobuf: "application/x-protobuf", json: "application/json" };
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
export function retryAfterMs(header: string | undefined, now: number = Date.now()): number | null {
  if (!header) return null;
  const seconds = Number(header);
  if (Number.isFinite(seconds) && seconds >= 0) return seconds * 1000;
  const at = Date.parse(header);
  return Number.isNaN(at) ? null : Math.max(0, at - now);
}

const TLS_FILES = [["ca", "CERTIFICATE"], ["cert", "CLIENT_CERTIFICATE"], ["key", "CLIENT_KEY"]] as const;

function tlsFiles(env: Readonly<NodeJS.ProcessEnv>, signal: Signal, warn: Warn): TlsFiles {
  const files: TlsFiles = {};
  for (const [option, name] of TLS_FILES) {
    const path = signalSetting(env, signal, name);
    if (!path) continue;
    try {
      files[option] = readFileSync(path);
    } catch (error) {
      warn(`${signal}: cannot read OTEL_EXPORTER_OTLP_${name} ${path}: ${error instanceof Error ? error.message : String(error)}`);
    }
  }
  return files;
}

/** How each exported signal is sent, from the standard variables. */
function signalTargets(env: Readonly<NodeJS.ProcessEnv>, warn: Warn): Partial<Record<Signal, SignalTarget>> {
  const telemetry = resolveTelemetry(env);
  const targets: Partial<Record<Signal, SignalTarget>> = {};
  for (const signal of SIGNALS) {
    const exporter = telemetry[signal];
    if (!exporter) continue;
    if (!exporter.protocol.startsWith("http/")) {
      warn(`${signal} relayed from session sandboxes are not exported: alasio exports ${signal} over ${exporter.protocol}, and bayma exports over http/protobuf or http/json only`);
      continue;
    }
    const url = new URL(exporter.endpoint);
    const timeout = Number(signalSetting(env, signal, "TIMEOUT"));
    const https = url.protocol === "https:";
    targets[signal] = {
      url,
      protocol: exporter.protocol,
      headers: parseKeyValueList(exporter.headers),
      timeoutMs: Number.isFinite(timeout) && timeout > 0 ? timeout : DEFAULT_TIMEOUT_MS,
      gzip: signalSetting(env, signal, "COMPRESSION") === "gzip",
      request: https ? httpsRequest : httpRequest,
      agent: https ? new HttpsAgent({ keepAlive: true, ...tlsFiles(env, signal, warn) }) : new HttpAgent({ keepAlive: true }),
    };
  }
  return targets;
}

/** One POST: `{ status, retryAfter }`, or `{ error }` for a connection that failed. */
function post(target: SignalTarget, body: Buffer, headers: OutgoingHttpHeaders, timeoutMs: number): Promise<PostResult> {
  return new Promise((resolve) => {
    // alasio's own HTTP instrumentation would otherwise trace every export it relays.
    context.with(suppressTracing(context.active()), () => {
      const req = target.request(target.url, { method: "POST", agent: target.agent, headers, timeout: timeoutMs }, (res) => {
        let received = 0;
        res.on("data", (chunk: Buffer) => {
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
export function createOtlpForwarder(
  env: Readonly<NodeJS.ProcessEnv> = process.env,
  { warn = () => {} }: { readonly warn?: Warn } = {},
): OtlpForwarder {
  const targets = signalTargets(env, warn);
  const protocols: Partial<Record<Signal, string>> = {};
  for (const signal of SIGNALS) {
    const target = targets[signal];
    if (target) protocols[signal] = target.protocol;
  }
  return {
    protocols,

    async export(signal, encoding, body) {
      const target = targets[signal];
      if (!target) return { ok: false, error: `${signal} are not forwarded` };
      const headers: OutgoingHttpHeaders = {
        ...target.headers,
        "content-type": CONTENT_TYPES[encoding],
        ...(target.gzip ? { "content-encoding": "gzip" } : {}),
      };
      const payload = target.gzip ? gzipSync(body) : body;
      const deadline = Date.now() + target.timeoutMs;
      let backoff = INITIAL_BACKOFF_MS;
      for (let attempt = 0; ; attempt += 1) {
        const result = await post(target, payload, headers, Math.max(1, deadline - Date.now()));
        if (result.status !== undefined && result.status >= 200 && result.status < 300) return { ok: true };
        const error = result.error?.message ?? `HTTP ${result.status}`;
        const retryable = result.error || (result.status !== undefined && RETRYABLE_STATUS.has(result.status));
        if (attempt === MAX_RETRIES || !retryable) return { ok: false, error };
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
