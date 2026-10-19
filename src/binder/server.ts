/**
 * The binder's HTTP front: what callers reach, each with a caller's token (./tokens.ts)
 * naming the key it may reach, as `Authorization: Bearer <token>`; and what KEDA asks how
 * many each pool waits for.
 *
 * - `POST /bind`: binds the token's key ahead of any request (204, BINDING_HEADER naming its
 *   binding), so its pod is got while nothing waits on it, as the key is bound now, whatever
 *   route the front last knew of it;
 *   503 with a Retry-After where its pool had no pod for it in time.
 * - `POST /release`: releases the token's key's binding, as its placement says what comes
 *   before, and resets what its placement keeps of its binds (204), as it does where it has
 *   none; 409 `{ "busy": true }` with a Retry-After where another bind or release of it is
 *   under way, so none was released; 503 where it could not be released now, its binding kept.
 * - `GET /counts/<namespace>/<pool>`: `{ free, bound, waiting }`, the pool's free pods running,
 *   its bindings, and the binds waiting for a free pod in every replica of the binder, as
 *   `peers` names them; asked without a token, as KEDA's metrics-api trigger asks.
 * - `GET /waiting/<namespace>/<pool>`: `{ waiting }`, this replica's alone, as its peers ask.
 * - `GET /healthz`: 200 once it serves, asked without a token.
 * - `/workspace`, `/snapshots` and `/folders`, and what is under them: what the front answers itself of
 *   the token's key, beyond binding it, or of its caller, as `operate` says, such as the files
 *   of what the key binds; its body of MAX_OPERATION_BYTES at most (413 past it).
 * - anything else: passed to the pod the token's key is bound to, the key bound first where
 *   it is not (./proxy.ts), but for a request that says it must bind nothing (NO_BIND_HEADER
 *   `never`), as one that reads what a binding has, answered 409 `{ "bound": false }` where the
 *   key has no binding serving, its Lease untaken and no pod claimed; with the binding's token for the caller's, the key's Lease
 *   stamped active now and then, as what releases it reads; 503 with a Retry-After
 *   where it could not be bound now (its pool had no pod for it in time, its pod could not
 *   be readied), or its pod was not reached, its route then forgotten, so the next request
 *   binds the key anew, the 503 saying why (`reason`); 403 where the gateway binds it nowhere, as
 *   its token says it is bound; and 503 `{ "held": true }`, with no Retry-After, where its binds
 *   failed one after another, so it is bound again only as its caller asks.
 *
 * A token that is no caller's, has stopped, or is good for longer than MAX_CALLER_TOKEN_MS, is
 * answered 401. The gateway that serves on
 * it says which pool each key is bound in, and how its pod is made where it makes its own
 * (`poolOf`), as each request's token is asked first: what the front keeps of a binding, its
 * route, its stamps and its requests under way, is by its pool and key, never its key alone,
 * as environments share keys (a branch's forks of main's sessions keep their ids), each
 * binding its own.
 */
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";

import { Clock, Effect, Option, Schema, type Scope } from "effect";

import { type KubeApiError, KubeClient } from "../kube/client.ts";
import { withLogScope } from "../shared/log.ts";
import { BearerToken } from "../shared/tokens.ts";
import { type BindOptions, Binder, type BindStepFailed, type BindTimeout } from "./binder.ts";
import { counts, touchLease } from "./bindings.ts";
import { BINDING_HEADER, type Pool } from "./names.ts";
import { proxy, type Route } from "./proxy.ts";
import type { Held } from "./live.ts";
import { bindingToken, type TokenClaims, verifyToken } from "./tokens.ts";

/** The most a request the front answers itself may carry: a file of a workspace's, as its node agent writes one, with room. */
export const MAX_OPERATION_BYTES = 64 * 1024 ** 2;

/** The paths of the requests the front answers itself (`operate`): /workspace, /snapshots and /folders, and what is under them. */
const OPERATED = /^\/(workspace|snapshots|folders)(\/|$)/u;

/** A caller's request the front answers itself (`operate`): its method, its path, under /workspace, /snapshots or /folders, its query, and its body. */
export interface Operation {
  readonly method: string;
  readonly path: string;
  readonly query: URLSearchParams;
  readonly body: Uint8Array;
}

/** How the front answers an Operation: its status, and its body, as JSON or as bytes. */
export interface Answer {
  readonly status: number;
  readonly json?: unknown;
  readonly bytes?: Uint8Array;
}

/** The header of a request that binds nothing: passed on where its key is bound, and otherwise answered as unbound. */
export const NO_BIND_HEADER = "x-alasio-bind";

/**
 * The longest a caller's token may stay good for, from now: alasio's (src/sandbox/gateway-client.ts
 * TOKEN_TTL_MS, six hours), with room for clocks a few minutes apart. One good for longer, as a
 * key leaked would mint, is answered 401.
 */
export const MAX_CALLER_TOKEN_MS = 6 * 60 * 60_000 + 5 * 60_000;

/** What a Retry-After asks a caller to wait, in seconds, before it asks again. */
const RETRY_AFTER_S = 1;
/** How often a key's Lease is stamped with its requests, at most, by one replica: well within any wait to release it. */
const ACTIVE_STAMP_MS = 30_000;

/** A key bound nowhere, as its caller's token says it is bound: why, answered 403. */
export class PlacementRefused extends Schema.TaggedError<PlacementRefused>()("PlacementRefused", { reason: Schema.String }) {
  override get message(): string {
    return this.reason;
  }
}

/** A key is bound again only as its caller asks, its binds having failed one after another: why the last did. */
export class PlacementHeld extends Schema.TaggedError<PlacementHeld>()("PlacementHeld", { reason: Schema.String }) {
  override get message(): string {
    return this.reason;
  }
}

/**
 * Where a key is bound: its pool, how its pod is made, where it is not claimed, and what comes
 * before its release; whether a bind of it is held, and why (`held`), as one that is is
 * refused; and what is told of each bind of it as it ends (`settled`): why it failed, or null
 * where it went through; and what its caller's release resets of what is kept of its binds
 * (`reset`), as their failures, whether or not it was bound.
 */
export interface Placement extends BindOptions {
  readonly pool: Pool;
  readonly release?: ((bound: { readonly pod: string; readonly binding: string }) => Effect.Effect<void, { readonly message: string }>) | undefined;
  readonly held?: Effect.Effect<string | null> | undefined;
  readonly settled?: ((failure: string | null) => Effect.Effect<void>) | undefined;
  readonly reset?: Effect.Effect<void, { readonly message: string }> | undefined;
}

/** What serveBinder is given; see the module. */
export interface BinderServerOptions {
  readonly port: number;
  readonly host?: string;
  /** The key caller's tokens are signed with, or which, of what each says (./tokens.ts verifyToken). */
  readonly callerKey: Buffer | ((claims: TokenClaims) => Buffer | null);
  /** The key the binding's tokens, which bound pods take requests with, are signed with. */
  readonly bindingKey: Buffer;
  /** The port bound pods serve on. */
  readonly podPort: number;
  /** Where `key` is bound, with what its caller's token says it is bound with; refused where it is bound nowhere. */
  readonly poolOf: (key: string, options: Readonly<Record<string, string>>) => Effect.Effect<Placement, PlacementRefused>;
  /** The addresses (`host:port`) of the binder's other replicas. */
  readonly peers?: () => Effect.Effect<readonly string[]>;
  /** What the front answers itself of a caller's key, or its caller, as its token says them, under /workspace, /snapshots and /folders; none where it answers nothing. */
  readonly operate?: (claims: TokenClaims, operation: Operation) => Effect.Effect<Answer>;
  /**
   * Whether the binding `binding` of `key`, its Lease's uid in `namespace`, is held now, as the
   * bindings' Leases are watched (./live.ts): a route the front knew of a binding gone, as
   * another replica released it, is forgotten before a request is passed by it, and the key
   * bound anew. None where routes are trusted until a request finds their pod unreached.
   */
  readonly routeHeld?: (key: string, binding: string, namespace: string) => Effect.Effect<Held>;
}

/** A listening front: its port, and how many requests of each key of each pool it is passing on now. */
export interface BinderServer {
  readonly port: number;
  readonly inFlight: (pool: Pool, key: string) => number;
}

const answer = (outgoing: ServerResponse, status: number, body?: unknown, headers: Record<string, string> = {}): void => {
  if (body === undefined) outgoing.writeHead(status, headers).end();
  else outgoing.writeHead(status, { ...headers, "content-type": "application/json" }).end(JSON.stringify(body));
};

/** The body of `incoming`, of `limit` bytes at most; null past it. */
async function bodyOf(incoming: IncomingMessage, limit: number): Promise<Uint8Array | null> {
  const chunks: Buffer[] = [];
  let bytes = 0;
  for await (const chunk of incoming as AsyncIterable<Buffer>) {
    bytes += chunk.length;
    if (bytes > limit) {
      incoming.resume();
      return null;
    }
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}

const Waiting = Schema.Struct({ waiting: Schema.Number });
const decodeWaiting = Schema.decodeUnknownOption(Waiting);

/** Serves the front on `port` until the scope closes, as the module says. */
export const serveBinder = Effect.fnUntraced(function*({ port, host = "0.0.0.0", callerKey, bindingKey, podPort, poolOf, peers = () => Effect.succeed([]), operate, routeHeld }: BinderServerOptions): Effect.fn.Return<BinderServer, never, Binder | KubeClient | Scope.Scope> {
  const binder = yield* Binder;
  const kube = yield* KubeClient;
  const runPromise = Effect.runPromiseWith(yield* Effect.context<never>());
  // What the front knows of each binding, by its pool's namespace and its key, never the key
  // alone: environments share keys (a branch's sessions are forks of main's, under their ids),
  // and each binds its own, which a caller of one never reaches by another's.
  const routes = new Map<string, Route & { readonly pool: Pool; readonly binding: string }>();
  const flights = new Map<string, number>();
  const stamped = new Map<string, number>();
  /** The key of `key`, as `pool` binds it, in what the front keeps: a namespace has no slash. */
  const slot = (pool: Pool, key: string): string => `${pool.namespace}/${key}`;

  /** Stamps `key`'s Lease in `pool` with a request now, at most every ACTIVE_STAMP_MS from this replica, so whatever releases it sees it active. */
  const active = (key: string, pool: Pool): Effect.Effect<void> =>
    Effect.gen(function*() {
      const now = yield* Clock.currentTimeMillis;
      if (now - (stamped.get(slot(pool, key)) ?? 0) < ACTIVE_STAMP_MS) return;
      stamped.set(slot(pool, key), now);
      yield* Effect.provideService(touchLease(pool, key, now), KubeClient, kube).pipe(Effect.catch((error) => Effect.logDebug(`stamping ${key}'s binding active failed: ${error.message}`)));
    });

  /**
   * The route to `key`'s pod, where `options` place it, as they are asked first, so a caller
   * is refused what they refuse, and reaches no route of another pool's: the one known, but
   * where it is to be asked `anew`, or its pod bound now; or, where it is to bind nothing
   * (`unbound`), its pod as its binding is now, null where none serves.
   */
  const routeOf = (key: string, options: Readonly<Record<string, string>>, { unbound, anew }: { readonly unbound: boolean; readonly anew: boolean }): Effect.Effect<(Route & { readonly pool: Pool; readonly binding: string }) | null, KubeApiError | BindTimeout | BindStepFailed | PlacementRefused | PlacementHeld> =>
    Effect.gen(function*() {
      const { pool, held, settled, ...placed } = yield* poolOf(key, options);
      const known = anew ? undefined : routes.get(slot(pool, key));
      // One whose binding is gone, as its watch says, is bound anew; one not yet known of is used as it is.
      if (known && (!routeHeld || (yield* routeHeld(key, known.binding, known.pool.namespace)) !== "gone")) return known;
      routes.delete(slot(pool, key));
      if (!unbound && held) {
        const reason = yield* held;
        if (reason !== null) return yield* new PlacementHeld({ reason });
      }
      const bound = unbound ? yield* binder.bound(pool, key) : yield* binder.bind(pool, key, placed).pipe(
        // One that waited out another's bind or release of the key failed of none of its own: that one tells of its own end.
        Effect.tapError((error) => (error._tag === "BindTimeout" && error.waited === "binding" ? Effect.void : settled?.(error.message) ?? Effect.void)),
        Effect.tap(() => settled?.(null) ?? Effect.void),
      );
      if (!bound) return null;
      const route = { address: bound.address, port: podPort, token: bindingToken(bindingKey, { key, binding: bound.binding, namespace: pool.namespace }), pool, binding: bound.binding };
      routes.set(slot(pool, key), route);
      return route;
    });

  /** The pool's counts, its waiting summed over every replica's. */
  const countsOf = (pool: Pool) =>
    Effect.gen(function*() {
      const { free, bound } = yield* Effect.provideService(counts(pool), KubeClient, kube);
      const theirs = yield* Effect.forEach(yield* peers(), (peer) =>
        Effect.tryPromise(() => fetch(`http://${peer}/waiting/${pool.namespace}/${pool.name}`, { signal: AbortSignal.timeout(2_000) }).then((response) => response.json())).pipe(
          Effect.map((body) => Option.match(decodeWaiting(body), { onNone: () => 0, onSome: ({ waiting }) => waiting })),
          // A peer that does not answer waits for none it can say.
          Effect.orElseSucceed(() => 0),
        ), { concurrency: "unbounded" });
      return { free, bound, waiting: (yield* binder.waiting(pool)) + theirs.reduce((sum, each) => sum + each, 0) };
    });

  const handle = (incoming: IncomingMessage, outgoing: ServerResponse): Effect.Effect<void> =>
    Effect.gen(function*() {
      const url = incoming.url ?? "/";
      if (incoming.method === "GET" && url === "/healthz") return answer(outgoing, 200, { ok: true });
      const asked = /^\/(counts|waiting)\/([a-z0-9-]+)\/([a-z0-9-]+)$/u.exec(url);
      if (incoming.method === "GET" && asked) {
        const pool = { namespace: asked[2]!, name: asked[3]! };
        if (asked[1] === "waiting") return answer(outgoing, 200, { waiting: yield* binder.waiting(pool) });
        return answer(outgoing, 200, yield* countsOf(pool));
      }
      const bearer = Schema.decodeUnknownOption(BearerToken)(incoming.headers.authorization);
      const now = yield* Clock.currentTimeMillis;
      const claims = Option.isSome(bearer) ? verifyToken(callerKey, bearer.value[1], now) : null;
      // A caller's token, which stops within MAX_CALLER_TOKEN_MS; a binding's, which a bound pod takes requests with, is none.
      if (!claims || claims.expires === undefined || claims.expires > now + MAX_CALLER_TOKEN_MS || claims.binding !== undefined) return answer(outgoing, 401, { error: "no caller's token" });
      const { key } = claims;
      const [path = "", search = ""] = url.split("?", 2);
      if (operate && OPERATED.test(path)) {
        const body = yield* Effect.promise(() => bodyOf(incoming, MAX_OPERATION_BYTES));
        if (body === null) return answer(outgoing, 413, { error: `a request answered here is ${MAX_OPERATION_BYTES} bytes at most` });
        const answered = yield* operate(claims, { method: incoming.method ?? "GET", path, query: new URLSearchParams(search), body });
        if (answered.bytes !== undefined) return void outgoing.writeHead(answered.status, { "content-type": "application/octet-stream" }).end(answered.bytes);
        return answer(outgoing, answered.status, answered.json);
      }
      if (incoming.method === "POST" && url === "/release") {
        incoming.resume();
        const released = yield* poolOf(key, claims.options ?? {}).pipe(
          Effect.tap(({ pool }) => Effect.sync(() => routes.delete(slot(pool, key)))),
          Effect.flatMap(({ pool, release, reset }) => Effect.andThen(reset ?? Effect.void, binder.release(pool, key, release))),
          Effect.result,
        );
        if (released._tag === "Success" && released.success === "busy") {
          return answer(outgoing, 409, { error: `${key} is being bound or released now: none was released`, busy: true }, { "retry-after": String(RETRY_AFTER_S) });
        }
        if (released._tag === "Success") return answer(outgoing, 204);
        if (released.failure instanceof PlacementRefused) return answer(outgoing, 403, { error: released.failure.message });
        yield* Effect.logWarning(`releasing ${key} failed: ${released.failure.message}`);
        return answer(outgoing, 503, { error: `${key} could not be released now` }, { "retry-after": String(RETRY_AFTER_S) });
      }
      const unbound = incoming.headers[NO_BIND_HEADER] === "never";
      delete incoming.headers[NO_BIND_HEADER];
      // A bind asked ahead, and a request that binds nothing, asks how the key is bound now, not
      // the route this front last knew, which a release by another, as an idle or broken binding's,
      // leaves behind.
      const routed = yield* routeOf(key, claims.options ?? {}, { unbound, anew: unbound || (incoming.method === "POST" && url === "/bind") }).pipe(Effect.result);
      if (routed._tag === "Failure") {
        if (routed.failure._tag === "PlacementRefused") return answer(outgoing, 403, { error: routed.failure.message });
        // Held, it is bound again only as its caller asks: no Retry-After.
        if (routed.failure._tag === "PlacementHeld") return answer(outgoing, 503, { error: `${key} is bound again only as its caller asks`, reason: routed.failure.message, held: true });
        yield* Effect.logWarning(`binding ${key} failed: ${routed.failure.message}`);
        return answer(outgoing, 503, { error: `${key} could not be bound now`, reason: routed.failure.message }, { "retry-after": String(RETRY_AFTER_S) });
      }
      const route = routed.success;
      if (!route) {
        incoming.resume();
        return answer(outgoing, 409, { bound: false });
      }
      if (incoming.method === "POST" && url === "/bind") {
        incoming.resume();
        return answer(outgoing, 204, undefined, { [BINDING_HEADER]: route.binding });
      }
      yield* active(key, route.pool);
      const flight = slot(route.pool, key);
      flights.set(flight, (flights.get(flight) ?? 0) + 1);
      const proxied = yield* Effect.promise(() => proxy(incoming, outgoing, route)).pipe(
        Effect.ensuring(Effect.sync(() => void flights.set(flight, (flights.get(flight) ?? 1) - 1))),
      );
      if (proxied.reached) return;
      routes.delete(flight);
      yield* Effect.logWarning(`the pod of ${key} was not reached: ${proxied.error}`);
      answer(outgoing, 503, { error: `the pod of ${key} was not reached` }, { "retry-after": String(RETRY_AFTER_S) });
    }).pipe(
      Effect.catchCause((cause) => Effect.logWarning("answering a request failed", cause).pipe(Effect.andThen(Effect.sync(() => {
        if (!outgoing.headersSent) answer(outgoing, 500, { error: "answering the request failed" });
        else outgoing.destroy();
      })))),
      withLogScope("binder"),
    );

  const server = createServer((incoming, outgoing) => void runPromise(handle(incoming, outgoing)));
  // A request holds its stream for as long as its call runs: no time limit of the server's own.
  server.requestTimeout = 0;
  yield* Effect.acquireRelease(
    Effect.callback<void>((resume) => void server.listen(port, host, () => resume(Effect.void))),
    () => Effect.callback<void>((resume) => {
      server.close(() => resume(Effect.void));
      server.closeAllConnections();
    }),
  );
  return {
    // Listening on a TCP port, its address is an internet one.
    port: (server.address() as AddressInfo).port,
    inFlight: (pool, key) => flights.get(slot(pool, key)) ?? 0,
  };
});
