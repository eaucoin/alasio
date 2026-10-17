/**
 * The binder's HTTP front: what callers reach, each with a caller's token (./tokens.ts)
 * naming the key it may reach, as `Authorization: Bearer <token>`; and what KEDA asks how
 * many each pool waits for.
 *
 * - `POST /bind`: binds the token's key ahead of any request (204), so its pod is got while
 *   nothing waits on it; 503 with a Retry-After where its pool had no pod for it in time.
 * - `POST /release`: releases the token's key's binding, as its placement says what comes
 *   before (204), as it does where it has none; 503 where it could not be released now, its
 *   binding kept.
 * - `GET /counts/<namespace>/<pool>`: `{ free, bound, waiting }`, the pool's free pods running,
 *   its bindings, and the binds waiting for a free pod in every replica of the binder, as
 *   `peers` names them; asked without a token, as KEDA's metrics-api trigger asks.
 * - `GET /waiting/<namespace>/<pool>`: `{ waiting }`, this replica's alone, as its peers ask.
 * - `GET /healthz`: 200 once it serves, asked without a token.
 * - anything else: passed to the pod the token's key is bound to, the key bound first where
 *   it is not (./proxy.ts), but for a request that says it must bind nothing (NO_BIND_HEADER
 *   `never`), as one that reads what a binding has, answered 409 `{ "bound": false }` where the
 *   key has no binding serving, its Lease untaken and no pod claimed; with the binding's token for the caller's, the key's Lease
 *   stamped active now and then, as what releases it reads; 503 with a Retry-After
 *   where it could not be bound now (its pool had no pod for it in time, its pod could not
 *   be readied), or its pod was not reached, its route then forgotten, so the next request
 *   binds the key anew; 403 where the gateway binds it nowhere, as its token says it is bound.
 *
 * A token that is no caller's, or has stopped, is answered 401. The gateway that serves on
 * it says which pool each key is bound in, and how its pod is made where it makes its own
 * (`poolOf`).
 */
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";

import { Clock, Effect, Option, Schema, type Scope } from "effect";

import { type KubeApiError, KubeClient } from "../kube/client.ts";
import { withLogScope } from "../shared/log.ts";
import { BearerToken } from "../shared/tokens.ts";
import { type BindOptions, Binder, type BindStepFailed, type BindTimeout } from "./binder.ts";
import { counts, touchLease } from "./bindings.ts";
import { type Pool } from "./names.ts";
import { proxy, type Route } from "./proxy.ts";
import { bindingToken, verifyToken } from "./tokens.ts";

/** The header of a request that binds nothing: passed on where its key is bound, and otherwise answered as unbound. */
export const NO_BIND_HEADER = "x-alasio-bind";

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

/** Where a key is bound: its pool, how its pod is made, where it is not claimed, and what comes before its release. */
export interface Placement extends BindOptions {
  readonly pool: Pool;
  readonly release?: ((bound: { readonly pod: string; readonly binding: string }) => Effect.Effect<void, { readonly message: string }>) | undefined;
}

/** What serveBinder is given; see the module. */
export interface BinderServerOptions {
  readonly port: number;
  readonly host?: string;
  /** The key caller's tokens are signed with. */
  readonly callerKey: Buffer;
  /** The key the binding's tokens, which bound pods take requests with, are signed with. */
  readonly bindingKey: Buffer;
  /** The port bound pods serve on. */
  readonly podPort: number;
  /** Where `key` is bound, with what its caller's token says it is bound with; refused where it is bound nowhere. */
  readonly poolOf: (key: string, options: Readonly<Record<string, string>>) => Effect.Effect<Placement, PlacementRefused>;
  /** The addresses (`host:port`) of the binder's other replicas. */
  readonly peers?: () => Effect.Effect<readonly string[]>;
}

/** A listening front: its port, and how many requests of each key it is passing on now. */
export interface BinderServer {
  readonly port: number;
  readonly inFlight: (key: string) => number;
}

const answer = (outgoing: ServerResponse, status: number, body?: unknown, headers: Record<string, string> = {}): void => {
  if (body === undefined) outgoing.writeHead(status, headers).end();
  else outgoing.writeHead(status, { ...headers, "content-type": "application/json" }).end(JSON.stringify(body));
};

const Waiting = Schema.Struct({ waiting: Schema.Number });
const decodeWaiting = Schema.decodeUnknownOption(Waiting);

/** Serves the front on `port` until the scope closes, as the module says. */
export const serveBinder = Effect.fnUntraced(function*({ port, host = "0.0.0.0", callerKey, bindingKey, podPort, poolOf, peers = () => Effect.succeed([]) }: BinderServerOptions): Effect.fn.Return<BinderServer, never, Binder | KubeClient | Scope.Scope> {
  const binder = yield* Binder;
  const kube = yield* KubeClient;
  const runPromise = Effect.runPromiseWith(yield* Effect.context<never>());
  const routes = new Map<string, Route & { readonly pool: Pool }>();
  const flights = new Map<string, number>();
  const stamped = new Map<string, number>();

  /** Stamps `key`'s Lease with a request now, at most every ACTIVE_STAMP_MS from this replica, so whatever releases it sees it active. */
  const active = (key: string, pool: Pool): Effect.Effect<void> =>
    Effect.gen(function*() {
      const now = yield* Clock.currentTimeMillis;
      if (now - (stamped.get(key) ?? 0) < ACTIVE_STAMP_MS) return;
      stamped.set(key, now);
      yield* Effect.provideService(touchLease(pool, key, now), KubeClient, kube).pipe(Effect.catch((error) => Effect.logDebug(`stamping ${key}'s binding active failed: ${error.message}`)));
    });

  /**
   * The route to `key`'s pod: the one known, or its pod bound now, with `options`; or, where
   * it is to bind nothing (`unbound`), its pod as its binding is now, null where none serves.
   */
  const routeOf = (key: string, options: Readonly<Record<string, string>>, unbound: boolean): Effect.Effect<(Route & { readonly pool: Pool }) | null, KubeApiError | BindTimeout | BindStepFailed | PlacementRefused> =>
    Effect.gen(function*() {
      const known = routes.get(key);
      if (known) return known;
      const { pool, ...placed } = yield* poolOf(key, options);
      const bound = unbound ? yield* binder.bound(pool, key) : yield* binder.bind(pool, key, placed);
      if (!bound) return null;
      const route = { address: bound.address, port: podPort, token: bindingToken(bindingKey, { key, binding: bound.binding, namespace: pool.namespace }), pool };
      routes.set(key, route);
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
      const claims = Option.isSome(bearer) ? verifyToken(callerKey, bearer.value[1], yield* Clock.currentTimeMillis) : null;
      // A caller's token, which stops; a binding's, which a bound pod takes requests with, is none.
      if (!claims || claims.expires === undefined || claims.binding !== undefined) return answer(outgoing, 401, { error: "no caller's token" });
      const { key } = claims;
      if (incoming.method === "POST" && url === "/release") {
        incoming.resume();
        routes.delete(key);
        const released = yield* poolOf(key, claims.options ?? {}).pipe(
          Effect.flatMap(({ pool, release }) => binder.release(pool, key, release)),
          Effect.result,
        );
        if (released._tag === "Success") return answer(outgoing, 204);
        if (released.failure instanceof PlacementRefused) return answer(outgoing, 403, { error: released.failure.message });
        yield* Effect.logWarning(`releasing ${key} failed: ${released.failure.message}`);
        return answer(outgoing, 503, { error: `${key} could not be released now` }, { "retry-after": String(RETRY_AFTER_S) });
      }
      const unbound = incoming.headers[NO_BIND_HEADER] === "never";
      delete incoming.headers[NO_BIND_HEADER];
      const routed = yield* routeOf(key, claims.options ?? {}, unbound).pipe(Effect.result);
      if (routed._tag === "Failure") {
        if (routed.failure._tag === "PlacementRefused") return answer(outgoing, 403, { error: routed.failure.message });
        yield* Effect.logWarning(`binding ${key} failed: ${routed.failure.message}`);
        return answer(outgoing, 503, { error: `${key} could not be bound now` }, { "retry-after": String(RETRY_AFTER_S) });
      }
      const route = routed.success;
      if (!route) {
        incoming.resume();
        return answer(outgoing, 409, { bound: false });
      }
      if (incoming.method === "POST" && url === "/bind") {
        incoming.resume();
        return answer(outgoing, 204);
      }
      yield* active(key, route.pool);
      flights.set(key, (flights.get(key) ?? 0) + 1);
      const proxied = yield* Effect.promise(() => proxy(incoming, outgoing, route)).pipe(
        Effect.ensuring(Effect.sync(() => void flights.set(key, (flights.get(key) ?? 1) - 1))),
      );
      if (proxied.reached) return;
      routes.delete(key);
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
    inFlight: (key) => flights.get(key) ?? 0,
  };
});
