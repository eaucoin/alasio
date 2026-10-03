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
import { createServer } from "node:http";
import { promisify } from "node:util";
import { gunzip } from "node:zlib";

import { NodeHttpServer } from "@effect/platform-node";
import type { Attributes } from "@opentelemetry/api";
import { Clock, Effect, Option, Ref, Schema, type Scope, Stream } from "effect";
import { type HttpServerError, HttpServerRequest, HttpServerResponse } from "effect/http";

import { withLogScope } from "../shared/log.ts";
import type { Signal } from "../telemetry/config.ts";
import type { OtlpEncoding, OtlpForwarder } from "../telemetry/forward.ts";
import { meter, SIGNALS } from "../telemetry/index.ts";
import { MalformedRequest, type ResourceStamp, stampResources } from "./otlp-resource.ts";

const gunzipAsync = promisify(gunzip);

const MAX_BODY_BYTES = 4 * 1024 * 1024;
// Far beyond what bayma records, and a bound on what anything else in a sandbox can push
// through alasio.
const RATE_BYTES_PER_SECOND = 128 * 1024;
const BURST_BYTES = 16 * 1024 * 1024;
/** How long a request may take to arrive whole. */
const REQUEST_TIMEOUT_MS = 30_000;

const requests = meter.createCounter("alasio.sandbox.telemetry.received", {
  description: "OTLP requests received from bayma inside session sandboxes, by outcome",
  unit: "{request}",
});

/** The bearer token a request presents, from its `Authorization` header. */
const BearerToken = Schema.TemplateLiteralParser(["Bearer ", Schema.NonEmptyString]);

const ENCODINGS = { "application/x-protobuf": "protobuf", "application/json": "json" } as const satisfies Record<string, OtlpEncoding>;

/** A media type OTLP/HTTP is sent in. */
const OtlpMediaType = Schema.Literals(["application/x-protobuf", "application/json"]);

/** The media type a `Content-Type` header names, without its parameters. */
const mediaTypeOf = (contentType: string | undefined): string => (contentType ?? "").split(";")[0]?.trim() ?? "";

/** What makeRateLimiter is given: bytes per second, and the most a budget holds. */
export interface RateLimiterOptions {
  readonly rate?: number;
  readonly burst?: number;
}

/** Per-session byte budgets; see makeRateLimiter. */
export interface RateLimiter {
  /** Takes `bytes` from `key`'s budget: the seconds to wait first when it lacks them, else 0. */
  readonly take: (key: string, bytes: number) => Effect.Effect<number>;
}

/** What serveTelemetryReceiver is given; see there. */
export interface TelemetryReceiverOptions {
  readonly port: number;
  readonly host?: string;
  /** The session (its volume id) a bearer token belongs to, or null. */
  readonly authenticate: (token: string) => Effect.Effect<string | null>;
  readonly forwarder: Pick<OtlpForwarder, "protocols" | "export">;
  readonly stampFor: (volumeId: string) => ResourceStamp;
  readonly limiter?: RateLimiter;
}

/** A listening receiver. */
export interface TelemetryReceiver {
  readonly port: number;
}

/** A per-session byte budget refilled at `rate` up to `burst`, on the Clock's monotonic time. */
export const makeRateLimiter = ({ rate = RATE_BYTES_PER_SECOND, burst = BURST_BYTES }: RateLimiterOptions = {}): Effect.Effect<RateLimiter> =>
  Effect.sync(() => {
    const buckets = new Map<string, { tokens: number; at: number }>();
    return {
      take: (key, bytes) =>
        Clock.monotonicTimeNanos.pipe(Effect.map((nanos) => {
          const at = Number(nanos / 1_000_000n);
          const bucket = buckets.get(key) ?? { tokens: burst, at };
          bucket.tokens = Math.min(burst, bucket.tokens + ((at - bucket.at) / 1000) * rate);
          bucket.at = at;
          buckets.set(key, bucket);
          if (bucket.tokens < bytes) return Math.ceil((bytes - bucket.tokens) / rate);
          bucket.tokens -= bytes;
          return 0;
        })),
    };
  });

/** A request's body, read whole; one past `limit` is read to its end and then null, so it can still be answered. */
const readBody = (request: HttpServerRequest.HttpServerRequest, limit: number) =>
  request.stream.pipe(
    Stream.runFold(() => ({ chunks: new Array<Uint8Array>(), size: 0 }), (body, chunk) => {
      body.size += chunk.length;
      if (body.size <= limit) body.chunks.push(chunk);
      return body;
    }),
    Effect.map(({ chunks, size }) => (size > limit ? null : Buffer.concat(chunks))),
  );

const status = (code: number): HttpServerResponse.HttpServerResponse => HttpServerResponse.empty({ status: code });

/**
 * Serves OTLP/HTTP on `port` until the scope closes. `authenticate(token)` is the
 * session (its volume id) a bearer token belongs to, or null; `forwarder` exports
 * (../telemetry/forward.ts); `stampFor(volumeId)` is what that session's resources are
 * stamped with. The port it listens on, once it does.
 */
export const serveTelemetryReceiver = Effect.fnUntraced(function*({
  port,
  host = "0.0.0.0",
  authenticate,
  forwarder,
  stampFor,
  limiter,
}: TelemetryReceiverOptions): Effect.fn.Return<TelemetryReceiver, HttpServerError.ServeError, Scope.Scope> {
  const budgets = limiter ?? (yield* makeRateLimiter());
  const failing = yield* Ref.make(false);
  const count = (outcome: string, attributes: Attributes = {}) => Effect.sync(() => requests.add(1, { outcome, ...attributes }));

  const handle = Effect.gen(function*() {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const signal = SIGNALS.find((name) => request.url === `/v1/${name}`);
    if (request.method !== "POST" || !signal) return status(404);
    const bearer = Schema.decodeUnknownOption(BearerToken)(request.headers["authorization"]);
    const volumeId = Option.isSome(bearer) ? yield* authenticate(bearer.value[1]) : null;
    if (!volumeId) {
      yield* count("refused", { reason: "unauthenticated" });
      return status(401);
    }
    const mediaType = Schema.decodeUnknownOption(OtlpMediaType)(mediaTypeOf(request.headers["content-type"]));
    if (Option.isNone(mediaType)) return status(415);
    const encoding = ENCODINGS[mediaType.value];
    // A full success, in the request's own encoding: an empty message, or an empty object.
    const ok = encoding === "json"
      ? HttpServerResponse.text("{}", { contentType: mediaType.value })
      : HttpServerResponse.uint8Array(new Uint8Array(), { contentType: mediaType.value });
    let body = yield* readBody(request, MAX_BODY_BYTES);
    if (!body) return status(413);
    const wait = yield* budgets.take(volumeId, body.length);
    if (wait > 0) {
      yield* count("refused", { signal, reason: "rate" });
      return HttpServerResponse.empty({ status: 429, headers: { "retry-after": String(wait) } });
    }
    if (request.headers["content-encoding"] === "gzip") {
      const gzipped = body;
      body = yield* Effect.promise(() => gunzipAsync(gzipped, { maxOutputLength: MAX_BODY_BYTES }).catch(() => null));
      if (!body) {
        yield* count("dropped", { signal, reason: "malformed" });
        return status(400);
      }
    }
    // A signal alasio does not export is accepted and let go, as the exporter expects.
    if (!forwarder.protocols[signal]) return ok;
    const stamped = yield* stamp(signal, encoding, body, stampFor(volumeId));
    if (!stamped) {
      yield* count("dropped", { signal, reason: "malformed" });
      return status(400);
    }
    const result = yield* Effect.promise(() => forwarder.export(signal, encoding, stamped));
    yield* count(result.ok ? "exported" : "dropped", { signal, ...(result.ok ? {} : { reason: "export_failed" }) });
    const wasFailing = yield* Ref.getAndSet(failing, !result.ok);
    if (!result.ok && !wasFailing) yield* Effect.logWarning(`exporting sessions' ${signal} failed: ${result.error}`);
    else if (result.ok && wasFailing) yield* Effect.logInfo("exporting sessions' telemetry recovered");
    return ok;
  }).pipe(
    Effect.catchTag("HttpServerError", (error) => failed(error.message)),
    Effect.catchDefect((defect) => failed(defect instanceof Error ? defect.message : String(defect))),
    withLogScope("sandbox-telemetry-receiver"),
  );

  const server = yield* NodeHttpServer.make(() => {
    const server = createServer();
    server.requestTimeout = REQUEST_TIMEOUT_MS;
    return server;
  }, { port, host });
  yield* server.serve(handle);
  // Listening on a TCP port, its address is an internet one.
  return { port: server.address._tag === "UnixPathAddress" ? port : server.address.port };
});

/** A request that could not be received, answered 500. */
const failed = (message: string): Effect.Effect<HttpServerResponse.HttpServerResponse> =>
  Effect.logWarning(`receiving sessions' telemetry failed: ${message}`).pipe(Effect.as(status(500)));

/** `body` with its resources stamped, or null for a request that does not parse. */
const stamp = (signal: Signal, encoding: OtlpEncoding, body: Buffer, resource: ResourceStamp): Effect.Effect<Buffer | null> =>
  Effect.try({ try: () => stampResources(signal, encoding, body, resource), catch: (error) => error }).pipe(
    Effect.catchIf((error) => error instanceof MalformedRequest, () => Effect.succeed(null)),
    // Anything else stampResources throws is a bug in it.
    Effect.orDie,
  );
