/**
 * bayma's telemetry from session sandboxes on Kubernetes, received over OTLP/HTTP and
 * exported where alasio exports its own (decision 010 of the Kubernetes design). A
 * sandbox's one permitted egress in "none" mode is this receiver's port on alasio's pod,
 * and bayma inside exports here directly with its Sandbox's token as the bearer.
 *
 * What arrives is untrusted, exactly as what alasio read from the Docker sandboxes'
 * drain was (../telemetry.js): the token says which session sent it, each request's
 * resources are stamped with what alasio knows about that session in place of what they
 * say, a request that does not parse is dropped, and each session is held to a byte
 * rate, past which it is answered 429 so its exporter backs off and retries.
 */
import { createServer } from "node:http";
import { promisify } from "node:util";
import { gunzip } from "node:zlib";

import { createLogger } from "../../shared/log.js";
import { meter, SIGNALS } from "../../telemetry/index.js";
import { MalformedRequest, stampResources } from "../otlp-resource.js";

const log = createLogger("sandbox-telemetry-receiver");
const gunzipAsync = promisify(gunzip);

const MAX_BODY_BYTES = 4 * 1024 * 1024;
// As the drain's relay: far beyond what bayma records, and a bound on what anything
// else in a sandbox can push through alasio.
const RATE_BYTES_PER_SECOND = 128 * 1024;
const BURST_BYTES = 16 * 1024 * 1024;
const ENCODINGS = { "application/x-protobuf": "protobuf", "application/json": "json" };

const requests = meter.createCounter("alasio.sandbox.telemetry.received", {
  description: "OTLP requests received from bayma inside session sandboxes, by outcome",
  unit: "{request}",
});

/** A per-session byte budget refilled at `rate` up to `burst`. */
export function createRateLimiter({ rate = RATE_BYTES_PER_SECOND, burst = BURST_BYTES, now = () => performance.now() } = {}) {
  const buckets = new Map();
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

function readBody(request, limit) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    request.on("data", (chunk) => {
      size += chunk.length;
      if (size > limit) {
        reject(Object.assign(new Error("request body too large"), { status: 413 }));
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
 * (../../telemetry/forward.js); `stampFor(volumeId)` is what that session's resources
 * are stamped with. Resolves `{ port, close() }` once listening.
 */
export async function startTelemetryReceiver({ port, host = "0.0.0.0", authenticate, forwarder, stampFor, limiter = createRateLimiter() }) {
  const count = (outcome, attributes = {}) => requests.add(1, { outcome, ...attributes });
  let failing = false;

  async function handle(request) {
    const signal = SIGNALS.find((name) => request.url === `/v1/${name}`);
    if (request.method !== "POST" || !signal) return [404];
    const bearer = /^Bearer (.+)$/u.exec(request.headers.authorization ?? "")?.[1];
    const volumeId = bearer ? await authenticate(bearer) : null;
    if (!volumeId) {
      count("refused", { reason: "unauthenticated" });
      return [401];
    }
    const encoding = ENCODINGS[(request.headers["content-type"] ?? "").split(";")[0].trim()];
    if (!encoding) return [415];
    let body = await readBody(request, MAX_BODY_BYTES);
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
    if (!forwarder.protocols[signal]) return [200];
    let stamped;
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
    return [200];
  }

  const server = createServer((request, response) => {
    handle(request).then(
      ([status, headers = {}]) => {
        // An empty JSON object is a full success in either encoding's response.
        response.writeHead(status, { "content-type": "application/json", ...headers }).end(status === 200 ? "{}" : "");
      },
      (error) => {
        const status = error.status ?? 500;
        if (status === 500) log.warn(`receiving sessions' telemetry failed: ${error.message}`);
        response.writeHead(status).end();
      },
    );
  });
  server.requestTimeout = 30_000;
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, host, resolve);
  });
  return {
    port: server.address().port,
    close: () => new Promise((resolve) => {
      server.close(() => resolve());
      server.closeAllConnections();
    }),
  };
}
