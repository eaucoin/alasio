/**
 * bayma's telemetry from session sandboxes, received over OTLP/HTTP and exported where
 * alasio exports its own. A session's one permitted egress without internet is this
 * receiver's port on alasio's pod, and bayma inside exports here directly with its
 * Sandbox's token as the bearer.
 *
 * What arrives is untrusted, since anything in the session can send it: the token says
 * which session sent it, each request's resources are stamped with what alasio knows
 * about that session in place of what they say, a request that does not parse is
 * dropped, and each session is held to a byte rate, past which it is answered 429 so
 * its exporter backs off and retries.
 */
import { createServer, type IncomingMessage, type OutgoingHttpHeaders } from "node:http";
import type { AddressInfo } from "node:net";
import { promisify } from "node:util";
import { gunzip } from "node:zlib";

import type { Attributes } from "@opentelemetry/api";

import { createLogger } from "../shared/log.ts";
import type { OtlpEncoding, OtlpForwarder } from "../telemetry/forward.ts";
import { meter, SIGNALS } from "../telemetry/index.ts";
import { MalformedRequest, type ResourceStamp, stampResources } from "./otlp-resource.ts";

const log = createLogger("sandbox-telemetry-receiver");
const gunzipAsync = promisify(gunzip);

const MAX_BODY_BYTES = 4 * 1024 * 1024;
// Far beyond what bayma records, and a bound on what anything else in a sandbox can push
// through alasio.
const RATE_BYTES_PER_SECOND = 128 * 1024;
const BURST_BYTES = 16 * 1024 * 1024;
const ENCODINGS: ReadonlyMap<string, OtlpEncoding> = new Map([["application/x-protobuf", "protobuf"], ["application/json", "json"]]);

const requests = meter.createCounter("alasio.sandbox.telemetry.received", {
  description: "OTLP requests received from bayma inside session sandboxes, by outcome",
  unit: "{request}",
});

/** What createRateLimiter is given: bytes per second, the most a budget holds, and the clock in ms. */
export interface RateLimiterOptions {
  readonly rate?: number;
  readonly burst?: number;
  readonly now?: () => number;
}

/** Per-session byte budgets; see createRateLimiter. */
export interface RateLimiter {
  take(key: string, bytes: number): number;
}

/** What startTelemetryReceiver is given; see there. */
export interface TelemetryReceiverOptions {
  readonly port: number;
  readonly host?: string;
  readonly authenticate: (token: string) => Promise<string | null>;
  readonly forwarder: OtlpForwarder;
  readonly stampFor: (volumeId: string) => ResourceStamp;
  readonly limiter?: RateLimiter;
}

/** A listening receiver. */
export interface TelemetryReceiver {
  readonly port: number;
  close(): Promise<void>;
}

/** An answer to a request: its status, and headers and body if it has any. */
type Reply = readonly [status: number, headers?: OutgoingHttpHeaders, body?: string];

/** A failure to read a request that carries the status to answer it with. */
interface RequestError extends Error {
  status?: number;
}

/** A per-session byte budget refilled at `rate` up to `burst`. */
export function createRateLimiter({ rate = RATE_BYTES_PER_SECOND, burst = BURST_BYTES, now = () => performance.now() }: RateLimiterOptions = {}): RateLimiter {
  const buckets = new Map<string, { tokens: number; at: number }>();
  return {
    /** Takes `bytes` from `key`'s budget; the seconds to wait first when it lacks them, else 0. */
    take(key, bytes) {
      const at = now();
      const bucket = buckets.get(key) ?? { tokens: burst, at };
      bucket.tokens = Math.min(burst, bucket.tokens + ((at - bucket.at) / 1000) * rate);
      bucket.at = at;
      buckets.set(key, bucket);
      if (bucket.tokens < bytes) return Math.ceil((bytes - bucket.tokens) / rate);
      bucket.tokens -= bytes;
      return 0;
    },
  };
}

function readBody(request: IncomingMessage, limit: number): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let size = 0;
    request.on("data", (chunk: Buffer) => {
      size += chunk.length;
      if (size > limit) {
        reject(Object.assign(new Error("request body too large"), { status: 413 }) satisfies RequestError);
        request.resume();
        return;
      }
      chunks.push(chunk);
    });
    request.on("end", () => resolve(Buffer.concat(chunks)));
    request.on("error", reject);
  });
}

/**
 * Serves OTLP/HTTP on `port`. `authenticate(token)` resolves the session (its volume
 * id) a bearer token belongs to, or null; `forwarder` exports
 * (../telemetry/forward.ts); `stampFor(volumeId)` is what that session's resources
 * are stamped with. Resolves `{ port, close() }` once listening.
 */
export async function startTelemetryReceiver({
  port,
  host = "0.0.0.0",
  authenticate,
  forwarder,
  stampFor,
  limiter = createRateLimiter(),
}: TelemetryReceiverOptions): Promise<TelemetryReceiver> {
  const count = (outcome: string, attributes: Attributes = {}) => requests.add(1, { outcome, ...attributes });
  let failing = false;

  async function handle(request: IncomingMessage): Promise<Reply> {
    const signal = SIGNALS.find((name) => request.url === `/v1/${name}`);
    if (request.method !== "POST" || !signal) return [404];
    const bearer = /^Bearer (.+)$/u.exec(request.headers.authorization ?? "")?.[1];
    const volumeId = bearer ? await authenticate(bearer) : null;
    if (!volumeId) {
      count("refused", { reason: "unauthenticated" });
      return [401];
    }
    const [mediaType = ""] = (request.headers["content-type"] ?? "").split(";");
    const type = mediaType.trim();
    const encoding = ENCODINGS.get(type);
    if (!encoding) return [415];
    // A full success, in the request's own encoding: an empty message, or an empty object.
    const ok: Reply = [200, { "content-type": type }, encoding === "json" ? "{}" : ""];
    let body: Buffer | null = await readBody(request, MAX_BODY_BYTES);
    const wait = limiter.take(volumeId, body.length);
    if (wait > 0) {
      count("refused", { signal, reason: "rate" });
      return [429, { "retry-after": String(wait) }];
    }
    if (request.headers["content-encoding"] === "gzip") {
      body = await gunzipAsync(body, { maxOutputLength: MAX_BODY_BYTES }).catch(() => null);
      if (!body) {
        count("dropped", { signal, reason: "malformed" });
        return [400];
      }
    }
    // A signal alasio does not export is accepted and let go, as the exporter expects.
    if (!forwarder.protocols[signal]) return ok;
    let stamped: Buffer;
    try {
      stamped = stampResources(signal, encoding, body, stampFor(volumeId));
    } catch (error) {
      if (!(error instanceof MalformedRequest)) throw error;
      count("dropped", { signal, reason: "malformed" });
      return [400];
    }
    const result = await forwarder.export(signal, encoding, stamped);
    count(result.ok ? "exported" : "dropped", { signal, ...(result.ok ? {} : { reason: "export_failed" }) });
    if (!result.ok && !failing) log.warn(`exporting sessions' ${signal} failed: ${result.error}`);
    else if (result.ok && failing) log.info("exporting sessions' telemetry recovered");
    failing = !result.ok;
    return ok;
  }

  const server = createServer((request, response) => {
    handle(request).then(
      ([status, headers = {}, body = ""]) => {
        response.writeHead(status, headers).end(body);
      },
      // What handle rejects with is an Error: readBody's, or one of what it calls.
      (error: RequestError) => {
        const status = error.status ?? 500;
        if (status === 500) log.warn(`receiving sessions' telemetry failed: ${error.message}`);
        response.writeHead(status).end();
      },
    );
  });
  server.requestTimeout = 30_000;
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, host, resolve);
  });
  return {
    // Listening on a TCP port, its address is one.
    port: (server.address() as AddressInfo).port,
    close: () => new Promise<void>((resolve) => {
      server.close(() => resolve());
      server.closeAllConnections();
    }),
  };
}
