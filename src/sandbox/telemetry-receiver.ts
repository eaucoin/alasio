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
 *
 * A request is answered 200 only once the collector has taken it: the collector's gateway
 * answers an export only once the log holds it, and keeps no queue of its own, so a request
 * the receiver answered 200 and did not forward would be lost. Where the collector does not
 * take it within FORWARD_WITHIN_MS, as while the log is down, it is answered 503 with a
 * Retry-After, which bayma's exporter retries within its own timeout (10 s by default),
 * and drops after; where the collector refuses it as one not to send again, it is answered
 * 400, which the exporter drops. Nothing is queued: what the receiver holds is the requests
 * it is answering, at most MAX_HELD_BYTES of them at once, and a quarter of that of any one
 * session's, past which it answers 503 too; a request whose sender went away stops being
 * forwarded, and gives back what it held once its forward has ended.
 */
import { createServer } from "node:http";
import { promisify } from "node:util";
import { gunzip } from "node:zlib";

import { NodeHttpServer } from "@effect/platform-node";
import type { Attributes } from "@opentelemetry/api";
import { Clock, Effect, Option, Ref, Schema, type Scope, Stream } from "effect";
import { type HttpServerError, HttpServerRequest, HttpServerResponse } from "effect/http";

import { withLogScope } from "../shared/log.ts";
import { BearerToken } from "../shared/tokens.ts";
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
/**
 * How long the collector is given to take a request, well within bayma's exporter's timeout
 * of 10 s, so it is answered while the exporter waits, and has time to send it again.
 */
const FORWARD_WITHIN_MS = 4000;
/** What a 503 asks the exporter to wait, where the collector asked for no wait of its own. */
const RETRY_AFTER_S = 1;
/**
 * The most bytes the requests being answered may hold at once, read, inflated and stamped:
 * sixteen of the largest at once, past which a request is answered 503.
 */
const MAX_HELD_BYTES = 16 * 3 * MAX_BODY_BYTES;

const requests = meter.createCounter("alasio.sandbox.telemetry.received", {
  description: "OTLP requests received from bayma inside session sandboxes, by outcome",
  unit: "{request}",
});

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
  /**
   * The session a bearer token belongs to, or null; failing with AuthenticationUnknown where
   * that cannot be told now, to be asked again.
   */
  readonly authenticate: (token: string) => Effect.Effect<Authenticated | null, AuthenticationUnknown>;
  readonly forwarder: Pick<OtlpForwarder, "protocols" | "export">;
  readonly limiter?: RateLimiter;
  /** The most bytes the requests being answered may hold at once (MAX_HELD_BYTES). */
  readonly maxHeldBytes?: number;
  /** The most of them one session's may hold: a quarter of maxHeldBytes unless given. */
  readonly maxSessionHeldBytes?: number;
}

/** The session a token is of: its volume id, which its rate and what its requests hold are counted by, and what its resources are stamped with. */
export interface Authenticated {
  readonly session: string;
  readonly stamp: ResourceStamp;
}

/** Whose a token is cannot be told now, as while the bindings it is checked against are not known: answered 503, to be sent again. */
export class AuthenticationUnknown extends Schema.TaggedError<AuthenticationUnknown>()("AuthenticationUnknown", {}) {}

/** A listening receiver. */
export interface TelemetryReceiver {
  readonly port: number;
  /** The bytes the requests it is answering hold now, all of them and by session. */
  readonly held: () => { readonly bytes: number; readonly sessions: ReadonlyMap<string, number> };
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

/** The bytes a request holds, taken from what every request being answered, and its session's, may hold. */
interface Holding {
  /** The session the request is of, whose share it takes from from then on. */
  readonly of: (volumeId: string) => void;
  /** Takes `bytes` more: whether there were that many left to take. */
  readonly take: (bytes: number) => boolean;
  /** Gives back all it took once `work` has ended: what the request's forward holds until it ends. */
  readonly until: (work: Promise<unknown>) => void;
  /** Gives back all it took, at once or once what `until` was given has ended. */
  readonly release: () => void;
}

/** A request's body, read whole; one past `limit`, or past what `held` can take, is read to its end and then said to be so, so it can still be answered. */
const readBody = (request: HttpServerRequest.HttpServerRequest, limit: number, held: Holding) =>
  request.stream.pipe(
    Stream.runFold(() => ({ chunks: new Array<Uint8Array>(), size: 0, busy: false }), (body, chunk) => {
      body.size += chunk.length;
      if (body.size > limit || body.busy) return body;
      if (held.take(chunk.length)) body.chunks.push(chunk);
      else body.busy = true;
      return body;
    }),
    Effect.map(({ chunks, size, busy }) => (size > limit ? "too large" as const : busy ? "busy" as const : Buffer.concat(chunks))),
  );

const status = (code: number): HttpServerResponse.HttpServerResponse => HttpServerResponse.empty({ status: code });

/** An answer the exporter sends the request again after, `seconds` on. */
const retryLater = (seconds: number): HttpServerResponse.HttpServerResponse =>
  HttpServerResponse.empty({ status: 503, headers: { "retry-after": String(seconds) } });

/**
 * Serves OTLP/HTTP on `port` until the scope closes. `authenticate(token)` is the
 * session a bearer token belongs to, and what its resources are stamped with, or null;
 * `forwarder` exports (../telemetry/forward.ts). A request is answered as the module says. The port it listens on, once it
 * does.
 */
export const serveTelemetryReceiver = Effect.fnUntraced(function*({
  port,
  host = "0.0.0.0",
  authenticate,
  forwarder,
  limiter,
  maxHeldBytes = MAX_HELD_BYTES,
  maxSessionHeldBytes = maxHeldBytes / 4,
}: TelemetryReceiverOptions): Effect.fn.Return<TelemetryReceiver, HttpServerError.ServeError, Scope.Scope> {
  const budgets = limiter ?? (yield* makeRateLimiter());
  const failing = yield* Ref.make(false);
  const count = (outcome: string, attributes: Attributes = {}) => Effect.sync(() => requests.add(1, { outcome, ...attributes }));
  let heldBytes = 0;
  const sessionsHeld = new Map<string, number>();
  const holding = (): Holding => {
    let session: string | null = null;
    let mine = 0;
    let pending: Promise<unknown> | null = null;
    const giveBack = () => {
      heldBytes -= mine;
      if (session !== null) {
        const left = (sessionsHeld.get(session) ?? 0) - mine;
        if (left > 0) sessionsHeld.set(session, left);
        else sessionsHeld.delete(session);
      }
      mine = 0;
    };
    return {
      of: (volumeId) => void (session = volumeId),
      take: (bytes) => {
        const ofSession = session === null ? 0 : sessionsHeld.get(session) ?? 0;
        if (heldBytes + bytes > maxHeldBytes || ofSession + bytes > maxSessionHeldBytes) return false;
        heldBytes += bytes;
        if (session !== null) sessionsHeld.set(session, ofSession + bytes);
        mine += bytes;
        return true;
      },
      until: (work) => void (pending = work),
      release: () => {
        if (pending === null) giveBack();
        else void pending.finally(giveBack);
      },
    };
  };

  const receive = (held: Holding) => Effect.gen(function*() {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const signal = SIGNALS.find((name) => request.url === `/v1/${name}`);
    if (request.method !== "POST" || !signal) return status(404);
    const bearer = Schema.decodeUnknownOption(BearerToken)(request.headers["authorization"]);
    const authenticated = Option.isSome(bearer) ? yield* authenticate(bearer.value[1]).pipe(Effect.option) : Option.some(null);
    if (Option.isNone(authenticated)) {
      yield* count("refused", { signal, reason: "unconfirmed" });
      return retryLater(RETRY_AFTER_S);
    }
    if (!authenticated.value) {
      yield* count("refused", { reason: "unauthenticated" });
      return status(401);
    }
    const { session, stamp: resource } = authenticated.value;
    held.of(session);
    const mediaType = Schema.decodeUnknownOption(OtlpMediaType)(mediaTypeOf(request.headers["content-type"]));
    if (Option.isNone(mediaType)) return status(415);
    const encoding = ENCODINGS[mediaType.value];
    // A full success, in the request's own encoding: an empty message, or an empty object.
    const ok = encoding === "json"
      ? HttpServerResponse.text("{}", { contentType: mediaType.value })
      : HttpServerResponse.uint8Array(new Uint8Array(), { contentType: mediaType.value });
    const busy = (atSignal: Signal) => count("refused", { signal: atSignal, reason: "busy" }).pipe(Effect.as(retryLater(RETRY_AFTER_S)));
    const read = yield* readBody(request, MAX_BODY_BYTES, held);
    if (read === "too large") return status(413);
    if (read === "busy") return yield* busy(signal);
    let body: Buffer = read;
    const wait = yield* budgets.take(session, body.length);
    if (wait > 0) {
      yield* count("refused", { signal, reason: "rate" });
      return HttpServerResponse.empty({ status: 429, headers: { "retry-after": String(wait) } });
    }
    if (request.headers["content-encoding"] === "gzip") {
      const gzipped = body;
      const inflated = yield* Effect.promise(() => gunzipAsync(gzipped, { maxOutputLength: MAX_BODY_BYTES }).catch(() => null));
      if (!inflated) {
        yield* count("dropped", { signal, reason: "malformed" });
        return status(400);
      }
      if (!held.take(inflated.length)) return yield* busy(signal);
      body = inflated;
    }
    // A signal alasio does not export is accepted and let go, as the exporter expects.
    if (!forwarder.protocols[signal]) return ok;
    const stamped = yield* stamp(signal, encoding, body, resource);
    if (!stamped) {
      yield* count("dropped", { signal, reason: "malformed" });
      return status(400);
    }
    if (!held.take(stamped.length)) return yield* busy(signal);
    // Ended where the request is, as when its sender goes away; what it holds given back once it has.
    const result = yield* Effect.promise((abort) => {
      const forwarded = forwarder.export(signal, encoding, stamped, { withinMs: FORWARD_WITHIN_MS, signal: abort });
      held.until(forwarded);
      return forwarded;
    });
    const wasFailing = yield* Ref.getAndSet(failing, !result.ok);
    if (result.ok) {
      yield* count("exported", { signal });
      if (wasFailing) yield* Effect.logInfo("exporting sessions' telemetry recovered");
      return ok;
    }
    if (!wasFailing) yield* Effect.logWarning(`exporting sessions' ${signal} failed: ${result.error}`);
    if (!result.retryable) {
      yield* count("dropped", { signal, reason: "export_refused" });
      return status(400);
    }
    yield* count("refused", { signal, reason: "export_failed" });
    return retryLater(result.retryAfterMs === null ? RETRY_AFTER_S : Math.max(1, Math.ceil(result.retryAfterMs / 1000)));
  });

  const handle = Effect.suspend(() => {
    const held = holding();
    // Interruptible, as the server runs a request's handler uninterruptibly: so the request's
    // sender going away, which interrupts it, ends its forward too.
    return Effect.interruptible(receive(held)).pipe(Effect.ensuring(Effect.sync(held.release)));
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
  return {
    port: server.address._tag === "UnixPathAddress" ? port : server.address.port,
    held: () => ({ bytes: heldBytes, sessions: new Map(sessionsHeld) }),
  };
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
