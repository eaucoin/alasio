/**
 * The binder's HTTP front: what callers reach, each with a caller's token (./tokens.ts)
 * naming the key it may reach, as `Authorization: Bearer <token>`; and what KEDA asks how
 * many each pool waits for.
 *
 * - `POST /bind`: binds the token's key ahead of any request, so its pod is got while nothing
 *   waits on it, as the key is bound now, whatever route the front last knew of it: 200
 *   `{ binding, namespace }`, its binding, its Lease's uid, and the namespace that is in, which
 *   a token may be held to (`grants`); 503 with a Retry-After where its pool had no pod for it
 *   in time.
 * - `POST /release`: releases the token's key's binding, as its placement says what comes
 *   before, and resets what its placement keeps of its binds (204), as it does where it has
 *   none; 409 `{ "busy": true }` with a Retry-After where another bind or release of it is
 *   under way, so none was released; 503 where it could not be released now, its binding kept.
 *   It waits first for the binding's calls under way (its requests but GETs, as an MCP client's
 *   stream it listens on is) through any replica of the front to end, for RELEASE_HOLD_MS at most, so no call to its pod is cut short by its caller's release,
 *   as an exec's would be, its pod's state saved with the call open: only a real loss, its pod
 *   gone, ends one.
 * - `GET /counts/<namespace>/<pool>`: `{ free, bound, waiting }`, the pool's free pods running,
 *   its bindings, and the binds waiting for a free pod in every replica of the binder, as
 *   `peers` names them; asked without a token, as KEDA's metrics-api trigger asks.
 * - `GET /counts/<namespace>`: `{ bound }`, the bindings of every pool in the namespace, as an
 *   environment's alasio is scaled on its harnesses bound, whichever pool's; asked so too.
 * - `GET /waiting/<namespace>/<pool>`: `{ waiting }`, this replica's alone, as its peers ask.
 * - `GET /flights/<namespace>/<key>`: `{ flights }`, the calls of the key's binding in the
 *   namespace this replica is passing on now, its requests but GETs, as its peers ask before a
 *   release, each with a token of its own replicas' (peerToken), which no pod can make, so a pod
 *   learns nothing of another binding's calls: 401 to any other, 400 for a key that is none. A
 *   peer that does not answer is taken as passing on none, as one that cannot be reached is
 *   passing no call on either.
 * - `POST /events`: what a bound pod tells of what its harness did on its own, for its caller's
 *   control plane, as `relay` says it, of TOLD_BYTES at most (413 past it), asked with the
 *   binding's token alone, never a caller's (401 for any other): while its binding is held, as
 *   the bindings' Leases are watched; 503 with a Retry-After while whether it is held is not
 *   known yet, and 401 once it is gone.
 * - `GET /healthz`: 200 once it serves, asked without a token.
 * - `/workspace`, `/snapshots`, `/folders` and `/harnesses`, and what is under them: what the
 *   front answers itself of the token's key, beyond binding it, or of its caller, as `operate`
 *   says, such as the files of what the key binds; its body of MAX_OPERATION_BYTES at most
 *   (413 past it).
 * - anything else: passed to the pod the token's key is bound to, the key bound first where
 *   it is not (./proxy.ts), but for a request that says it must bind nothing (NO_BIND_HEADER
 *   `never`), as one that reads what a binding has, answered 409 `{ "bound": false }` where the
 *   key has no binding serving, its Lease untaken and no pod claimed; with the binding's token for the caller's, the key's Lease
 *   stamped active as each bind of it is through and as its requests that may bind come, now and
 *   then, as what releases it reads; passed again, where a route the front knew of was of a binding
 *   gone, released by another replica or its sweep before its watch said so, to the key
 *   bound anew, within its bind's own wait, where the request's body was one the front could
 *   read whole first and its connection to the gone pod was never made (NEVER_CONNECTED), so
 *   that pod had nothing of it; 503 with a Retry-After where it could not be bound now (its pool had no
 *   pod for it in time, its pod could not be readied), or its pod was not reached, its route
 *   then forgotten, so the next request binds the key anew, the 503 saying why (`reason`); 403 where the gateway binds it nowhere, as
 *   its token says it is bound; and 503 `{ "held": true }`, with no Retry-After, where its binds
 *   failed one after another, so it is bound again only as its caller asks. A request for one
 *   binding alone (PINNED_HEADER), whose key is bound to another by the time it would be passed,
 *   is passed to no pod and answered 409 `{ moved: { binding, namespace } }`, the binding now.
 *
 * A token that is no caller's, has stopped, or is good for longer than MAX_CALLER_TOKEN_MS, is
 * answered 401; so is one that stops at no time, but where it is held to a binding of another
 * key that `grants` lets it be, and that binding is held, as the bindings' Leases are watched
 * (`routeHeld`): it is good for as long as that is, 503 with a Retry-After meanwhile where
 * whether it is held is not known yet, and only for what a harness bound elsewhere asks
 * (heldMayAsk): what its key's pod serves, and its workspace's files, 403 for the rest. The
 * gateway that serves on
 * it says which pool each key is bound in, and how its pod is made where it makes its own
 * (`poolOf`), as each request's token is asked first: what the front keeps of a binding, its
 * route, its stamps and its requests under way, is by its pool and key, never its key alone,
 * as environments share keys (a branch's forks of main's sessions keep their ids), each
 * binding its own.
 *
 * As its scope closes, as its pod is told to stop (STOP_GRACE_SECONDS), it takes no new
 * connection at once, its listener closed, so that one made meanwhile, routed to it still as
 * its endpoints are let go of, is refused at once rather than left to wait on a pod gone; and a
 * request that comes on a connection it kept, as a keep-alive caller's, is answered 503 saying
 * no pod was given it (NOT_PASSED_HEADER), for its caller to ask again, its connection closed.
 * Its callers' streams, the GETs it passes on that are answered as event streams, it cuts at
 * once, and one whose head comes as it stops as that comes, for each to be opened again where
 * a gateway is. It lets its requests under way end, another GET among them, each told to close
 * its connection as it does, for STOP_CALLS_MS at most, and runs on for STOP_LINGER_MS at
 * least, so it is there to refuse while the routes to it are let go of; what is left then is
 * cut.
 */
import { createHmac } from "node:crypto";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";

import { Clock, Deferred, Effect, Exit, Option, Schema, type Scope } from "effect";

import { type KubeApiError, KubeClient } from "../kube/client.ts";
import { trace, withLogScope } from "../shared/log.ts";
import { BearerToken } from "../shared/tokens.ts";
import { type BindOptions, Binder, type BindStepFailed, type BindTimeout } from "./binder.ts";
import { boundIn, counts, touchLease } from "./bindings.ts";
import { EVENTS_PATH, GATEWAY_READINESS, NO_BIND_HEADER, NOT_PASSED_HEADER, PINNED_HEADER, type Pool, STOP_GRACE_SECONDS } from "./names.ts";
import { eventStream, proxy, type Route } from "./proxy.ts";
import type { Held } from "./live.ts";
import { bindingToken, type HeldTo, mintToken, type TokenClaims, verifyToken } from "./tokens.ts";

/**
 * How far apart the clocks of the front and of what mints its callers' tokens may be: room in
 * the time a token may stay good for, and in when one held to a binding was minted, which the
 * bindings' watch is asked of, so a front whose clock runs ahead refuses no new binding's.
 */
const CLOCKS_APART_MS = 5 * 60_000;

/** The most a request the front answers itself may carry: a file of a workspace's, as its node agent writes one, with room. */
export const MAX_OPERATION_BYTES = 64 * 1024 ** 2;

/** The paths of the requests the front answers itself (`operate`): /workspace, /snapshots, /folders and /harnesses, and what is under them. */
const OPERATED = /^\/(workspace|snapshots|folders|harnesses)(\/|$)/u;
/** Where a caller asks the front to bind its token's key ahead of any request. */
const BIND_PATH = "/bind";
/** Where a caller asks the front to release its token's key's binding. */
const RELEASE_PATH = "/release";
/** The most a pod's telling may carry: an event's few fields, with room. */
export const TOLD_BYTES = 4 * 1024;

/** Whether the front answers a request to `path` itself, passing none to a pod: what a bound pod serves is at none of them. */
/** How many bindings not known yet the front remembers when each was first told of, at most (/events). */
export const FIRST_TOLD_KEPT = 1024;
/** How long it remembers when one was first told of: past any watch's catching up, after which it is asked of anew. */
const FIRST_TOLD_FOR_MS = 10 * 60_000;

export const answeredByFront = (path: string): boolean => path === BIND_PATH || path === RELEASE_PATH || path === EVENTS_PATH || OPERATED.test(path);

/** Where a workspace's files are read and written, as the front answers them (`operate`). */
const FILES_PATH = "/workspace/files";

/**
 * Whether a token held to a binding may ask `method path`: what its key's pod serves, as its
 * bayma, and its workspace's files read and written, as a harness bound elsewhere asks them;
 * none of what only its caller's control plane asks, a bind or release of it, a listing, or a
 * workspace made, forked or deleted.
 */
const heldMayAsk = (method: string, path: string): boolean => !answeredByFront(path) || (path === FILES_PATH && (method === "GET" || method === "PUT"));

/** A caller's request the front answers itself (`operate`): its method, its path, under /workspace, /snapshots, /folders or /harnesses, its query, and its body. */
export interface Operation {
  readonly method: string;
  readonly path: string;
  readonly query: URLSearchParams;
  readonly body: Uint8Array;
}

/** How the front answers an Operation: its status, its body, as JSON or as bytes, and any headers beside. */
export interface Answer {
  readonly status: number;
  readonly json?: unknown;
  readonly bytes?: Uint8Array;
  readonly headers?: Readonly<Record<string, string>>;
}

/**
 * The longest a caller's token may stay good for, from now: alasio's (src/sandbox/gateway-client.ts
 * TOKEN_TTL_MS, five minutes), with room for clocks apart (CLOCKS_APART_MS). One good for
 * longer, as a key leaked would mint, is answered 401.
 */
export const MAX_CALLER_TOKEN_MS = 5 * 60_000 + CLOCKS_APART_MS;

/** What a Retry-After asks a caller to wait, in seconds, before it asks again. */
const RETRY_AFTER_S = 1;
/**
 * How long a release its caller asks waits at most for its binding's requests under way to end:
 * well within the caller's own wait for it (../sandbox/gateway-client.ts LONG_CALL_TIMEOUT_MS),
 * and past any one call of an exec's, which bayma yields from within minutes. One open longer is
 * taken as stuck, and the release goes on, saying so.
 */
export const RELEASE_HOLD_MS = 5 * 60_000;
/** How often a release waiting on its binding's requests under way asks whether they have ended. */
const FLIGHTS_POLL_MS = 250;
/** How long a token a replica asks a peer of a binding's calls with is good for. */
const PEER_TOKEN_MS = 60_000;

/** What of its pod's grace stops after the front: the gateway's sweeps, its bindings' watch, and its telemetry flushed. */
const STOP_AFTER_FRONT_SECONDS = 10;
/** How long a front told to stop lets its requests under way end, at most, before it cuts them: its pod's grace, but for what stops after it. */
export const STOP_CALLS_MS = (STOP_GRACE_SECONDS - STOP_AFTER_FRONT_SECONDS) * 1000;
/** How long past its pod being marked unready, its third probe unanswered, a front told to stop runs on, for the routes to it to be let go of. */
const STOP_UNREADY_MARGIN_SECONDS = 5;
/**
 * How long a front told to stop runs on, at least, refusing what is routed to it still: until its
 * pod is marked unready, as its probes go unanswered, and the routes to it let go of.
 */
export const STOP_LINGER_MS = (GATEWAY_READINESS.periodSeconds * GATEWAY_READINESS.failureThreshold + STOP_UNREADY_MARGIN_SECONDS) * 1000;

/**
 * The key the front's replicas sign the tokens they ask one another with, of the key a binding's
 * tokens are signed with, which every replica has and no pod does: a pod holds tokens signed
 * with that one, never it, and so can make none of these.
 */
export const peerKeyOf = (bindingKey: Buffer): Buffer => createHmac("sha256", bindingKey).update("alasio binder peers").digest();

/** How often a binding's Lease is stamped with its requests, at most, by one replica: well within any wait to release it. */
const ACTIVE_STAMP_MS = 30_000;
/**
 * The largest body of a request the front reads whole before it passes it on a route it knew
 * of, so it may pass it again where that route was of a binding gone: an MCP call's, with room.
 * One larger, or of no stated length, is streamed as it comes, and passed once.
 */
const REPLAYED_BYTES = 1024 ** 2;
/**
 * The errors of a connection to a pod that was never made, of which the pod had nothing: refused,
 * no route to it, or reset before it was made. A request so failed may be passed again; one
 * that timed out, or failed once its connection was made, may have reached the pod, as an MCP
 * call that runs an exec, and is not.
 */
const NEVER_CONNECTED: ReadonlySet<string> = new Set(["ECONNREFUSED", "EHOSTUNREACH", "ENETUNREACH", "ECONNRESET"]);

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
  /** The port its pod serves on, where its pool's pods serve on another than the front's `podPort`. */
  readonly port?: number | undefined;
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
  /** The port bound pods serve on, unless their placement says another. */
  readonly podPort: number;
  /** Where `key` is bound, with what its caller's token says it is bound with; refused where it is bound nowhere. */
  readonly poolOf: (key: string, options: Readonly<Record<string, string>>) => Effect.Effect<Placement, PlacementRefused>;
  /** The addresses (`host:port`) of the binder's other replicas. */
  readonly peers?: () => Effect.Effect<readonly string[]>;
  /** What the front answers itself of a caller's key, or its caller, as its token says them, under /workspace, /snapshots, /folders and /harnesses; none where it answers nothing. */
  readonly operate?: (claims: TokenClaims, operation: Operation) => Effect.Effect<Answer>;
  /**
   * Whether the binding `binding` of `key`, its Lease's uid in `namespace`, is held now, as the
   * bindings' Leases are watched (./live.ts): a route the front knew of a binding gone, as
   * another replica released it, is forgotten before a request is passed by it, and the key
   * bound anew. None where routes are trusted until a request finds their pod unreached. Not
   * known, for one not seen, where nothing has been heard of the bindings since `since`.
   */
  readonly routeHeld?: (key: string, binding: string, namespace: string, since?: number) => Effect.Effect<Held>;
  /**
   * Whether a caller's token of `key` may be held to the binding `held` in place of a time it
   * stops (TokenClaims.held), as a harness bound elsewhere is given its workspace's; none may
   * where none is given, nor where the bindings are not watched (`routeHeld`).
   */
  readonly grants?: (key: string, held: HeldTo) => boolean;
  /**
   * What a bound pod tells, asked with its binding's token (`POST /events`): the binding, its
   * body, and how it is answered; none where no pod tells anything, which is answered 404, nor
   * where the bindings are not watched (`routeHeld`).
   */
  readonly relay?: (bound: HeldTo, body: Uint8Array) => Effect.Effect<Answer>;
  /** How it stops, as the module says: STOP_CALLS_MS and STOP_LINGER_MS but for tests. */
  readonly stopping?: { readonly callsMs: number; readonly lingerMs: number };
}

/** A listening front: its port, and how many calls (requests but GETs) of each key of each pool it is passing on now. */
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

/** A route the front keeps of a binding: its pod's, and the binding's pool and Lease's uid. */
type Routed = Route & { readonly pool: Pool; readonly binding: string };

/** Whether `incoming` has no body, or one of a stated length within `limit`: one the front may read whole, to pass again. */
const replayable = (incoming: IncomingMessage, limit: number): boolean => {
  const length = incoming.headers["content-length"];
  if (length === undefined) return incoming.headers["transfer-encoding"] === undefined;
  return Number(length) <= limit;
};

const Waiting = Schema.Struct({ waiting: Schema.Number });
const decodeWaiting = Schema.decodeUnknownOption(Waiting);
const Flights = Schema.Struct({ flights: Schema.Number });
const decodeFlights = Schema.decodeUnknownOption(Flights);

/** Serves the front on `port` until the scope closes, as the module says. */
export const serveBinder = Effect.fnUntraced(function*({ port, host = "0.0.0.0", callerKey, bindingKey, podPort, poolOf, peers = () => Effect.succeed([]), operate, routeHeld, grants, relay, stopping: stopTimings = { callsMs: STOP_CALLS_MS, lingerMs: STOP_LINGER_MS } }: BinderServerOptions): Effect.fn.Return<BinderServer, never, Binder | KubeClient | Scope.Scope> {
  const binder = yield* Binder;
  const kube = yield* KubeClient;
  const runPromise = Effect.runPromiseWith(yield* Effect.context<never>());
  // What the front knows of each binding, by its pool's namespace and its key, never the key
  // alone: environments share keys (a branch's sessions are forks of main's, under their ids),
  // and each binds its own, which a caller of one never reaches by another's.
  const routes = new Map<string, Routed>();
  const flights = new Map<string, number>();
  // When each binding the watch did not know yet was first told of by its pod (/events), by its namespace and uid, until it is
  // known held or gone; FIRST_TOLD_KEPT at most, the oldest let go of first, and none for longer than FIRST_TOLD_FOR_MS.
  const firstTold = new Map<string, number>();
  // When this replica last stamped each key's binding active, by the binding: a new binding's first request is stamped whatever its key's last was.
  const stamped = new Map<string, { readonly binding: string; readonly at: number }>();
  /** The key of `key`, as `pool` binds it, in what the front keeps: a namespace has no slash. */
  const slot = (pool: Pool, key: string): string => `${pool.namespace}/${key}`;

  /**
   * Stamps the binding `binding` of `key`'s Lease in `pool` active now, so whatever releases it
   * sees it so: as a request comes, at most every ACTIVE_STAMP_MS a binding from this replica;
   * or, `bound`, as a bind of it is through, whatever was stamped before, so a binding is idle
   * from when it was bound at the earliest, not from when its pod was made, which a restore
   * makes some while before.
   */
  const active = (key: string, pool: Pool, binding: string, { bound = false }: { readonly bound?: boolean } = {}): Effect.Effect<void> =>
    Effect.gen(function*() {
      const now = yield* Clock.currentTimeMillis;
      const last = stamped.get(slot(pool, key));
      if (!bound && last?.binding === binding && now - last.at < ACTIVE_STAMP_MS) return;
      stamped.set(slot(pool, key), { binding, at: now });
      yield* Effect.provideService(touchLease(pool, key, now), KubeClient, kube).pipe(Effect.catch((error) => Effect.logDebug(`stamping ${key}'s binding active failed: ${error.message}`)));
    });

  /**
   * The route to `key`'s pod, where `options` place it, as they are asked first, so a caller
   * is refused what they refuse, and reaches no route of another pool's: the one known, but
   * where it is to be asked `anew`, or its pod bound now; or, where it is to bind nothing
   * (`unbound`), its pod as its binding is now, null where none serves.
   */
  const routeOf = (key: string, options: Readonly<Record<string, string>>, { unbound, anew }: { readonly unbound: boolean; readonly anew: boolean }): Effect.Effect<{ readonly route: Routed; readonly known: boolean } | null, KubeApiError | BindTimeout | BindStepFailed | PlacementRefused | PlacementHeld> =>
    Effect.gen(function*() {
      if (!unbound) yield* trace(`${key} front route`);
      const { pool, held, settled, port, ...placed } = yield* poolOf(key, options);
      const known = anew ? undefined : routes.get(slot(pool, key));
      // One whose binding is gone, as its watch says, is bound anew; one not yet known of is used as it is.
      if (known && (!routeHeld || (yield* routeHeld(key, known.binding, known.pool.namespace)) !== "gone")) return { route: known, known: true };
      routes.delete(slot(pool, key));
      if (!unbound && held) {
        const reason = yield* held;
        if (reason !== null) return yield* new PlacementHeld({ reason });
      }
      if (!unbound) yield* trace(`${key} front bind-asked`);
      const bound = unbound ? yield* binder.bound(pool, key) : yield* binder.bind(pool, key, placed).pipe(
        // One that waited out another's bind or release of the key failed of none of its own: that one tells of its own end.
        Effect.tapError((error) => (error._tag === "BindTimeout" && error.waited === "binding" ? Effect.void : settled?.(error.message) ?? Effect.void)),
        Effect.tap(() => settled?.(null) ?? Effect.void),
      );
      if (!bound) return null;
      const route = { address: bound.address, port: port ?? podPort, token: bindingToken(bindingKey, { key, binding: bound.binding, namespace: pool.namespace }), pool, binding: bound.binding };
      routes.set(slot(pool, key), route);
      if (!unbound) yield* trace(`${key} front routed`);
      // Bound now, as a bind of it is through: idle from now at the earliest.
      if (!unbound) yield* active(key, pool, bound.binding, { bound: true });
      return { route, known: false };
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

  const peerKey = peerKeyOf(bindingKey);
  /** The calls of `key`'s binding in `pool`'s namespace under way through every replica of the front, a peer that does not answer taken as passing none on. */
  const flightsOf = (pool: Pool, key: string) =>
    Effect.gen(function*() {
      const token = mintToken(peerKey, { key: slot(pool, key), expires: (yield* Clock.currentTimeMillis) + PEER_TOKEN_MS });
      const theirs = yield* Effect.forEach(yield* peers(), (peer) =>
        Effect.tryPromise(() =>
          fetch(`http://${peer}/flights/${pool.namespace}/${encodeURIComponent(key)}`, { headers: { authorization: `Bearer ${token}` }, signal: AbortSignal.timeout(2_000) }).then((response) => response.json())
        ).pipe(
          Effect.map((body) => Option.match(decodeFlights(body), { onNone: () => 0, onSome: ({ flights: under }) => under })),
          // A peer that does not answer passes on none it can say.
          Effect.orElseSucceed(() => 0),
        ), { concurrency: "unbounded" });
      return (flights.get(slot(pool, key)) ?? 0) + theirs.reduce((sum, each) => sum + each, 0);
    });

  /** Waits for `key`'s binding in `pool` to have no request under way through any replica, as the module says, for RELEASE_HOLD_MS at most. */
  const quiet = (pool: Pool, key: string): Effect.Effect<void> =>
    Effect.gen(function*() {
      const deadline = (yield* Clock.currentTimeMillis) + RELEASE_HOLD_MS;
      for (;;) {
        const under = yield* flightsOf(pool, key);
        if (under === 0) return;
        if ((yield* Clock.currentTimeMillis) >= deadline) {
          return yield* Effect.logWarning(`releasing ${key} with ${under} request(s) to its pod under way still after ${RELEASE_HOLD_MS / 1000} s, as they are taken as stuck`);
        }
        yield* Effect.sleep(FLIGHTS_POLL_MS);
      }
    });

  const handle = (incoming: IncomingMessage, outgoing: ServerResponse): Effect.Effect<void> =>
    Effect.gen(function*() {
      const url = incoming.url ?? "/";
      if (incoming.method === "GET" && url === "/healthz") return answer(outgoing, 200, { ok: true });
      const ofNamespace = /^\/counts\/([a-z0-9-]+)$/u.exec(url);
      if (incoming.method === "GET" && ofNamespace) return answer(outgoing, 200, { bound: yield* Effect.provideService(boundIn(ofNamespace[1]!), KubeClient, kube) });
      const asked = /^\/(counts|waiting)\/([a-z0-9-]+)\/([a-z0-9-]+)$/u.exec(url);
      if (incoming.method === "GET" && asked) {
        const pool = { namespace: asked[2]!, name: asked[3]! };
        if (asked[1] === "waiting") return answer(outgoing, 200, { waiting: yield* binder.waiting(pool) });
        return answer(outgoing, 200, yield* countsOf(pool));
      }
      const flying = /^\/flights\/([a-z0-9-]+)\/([^/]+)$/u.exec(url);
      if (incoming.method === "GET" && flying) {
        const asked = Option.getOrUndefined(Schema.decodeUnknownOption(BearerToken)(incoming.headers.authorization));
        let flight: string;
        try {
          flight = `${flying[1]}/${decodeURIComponent(flying[2] ?? "")}`;
        } catch {
          return answer(outgoing, 400, { error: "no key of a binding" });
        }
        // A peer's alone, of this binding: no pod, nor a caller, learns of its calls.
        if (!asked || verifyToken(peerKey, asked[1], yield* Clock.currentTimeMillis)?.key !== flight) return answer(outgoing, 401, { error: "no peer's token of this binding" });
        return answer(outgoing, 200, { flights: flights.get(flight) ?? 0 });
      }
      const bearer = Schema.decodeUnknownOption(BearerToken)(incoming.headers.authorization);
      const now = yield* Clock.currentTimeMillis;
      if (url.split("?", 1)[0] === EVENTS_PATH) {
        if (!relay || !routeHeld) return answer(outgoing, 404, { error: "no pod tells anything here" });
        if (incoming.method !== "POST") return answer(outgoing, 405, { error: `${incoming.method} ${EVENTS_PATH} is no telling` });
        // A binding's token alone, as its pod is given it: a caller's is signed by no binding key, and no binding's names no binding.
        const bound = Option.isSome(bearer) ? verifyToken(bindingKey, bearer.value[1], now) : null;
        if (!bound || bound.binding === undefined || bound.namespace === undefined || bound.expires !== undefined || bound.held !== undefined) return answer(outgoing, 401, { error: "no binding's token" });
        const binding = { key: bound.key, binding: bound.binding, namespace: bound.namespace };
        // One not seen yet is not known until the watch has heard of its namespace since it was first told of, as a binding just
        // made may not be heard of yet; from then on, gone, as one let go of is, however often its pod tells again.
        const told = `${binding.namespace}/${binding.binding}`;
        const first = firstTold.get(told);
        const since = first !== undefined && now - first < FIRST_TOLD_FOR_MS ? first : now;
        const holding = yield* routeHeld(binding.key, binding.binding, binding.namespace, since);
        if (holding === "unknown") {
          firstTold.delete(told);
          firstTold.set(told, since);
          for (const [oldest] of firstTold) {
            if (firstTold.size <= FIRST_TOLD_KEPT) break;
            firstTold.delete(oldest);
          }
          return answer(outgoing, 503, { error: `whether ${binding.key}'s binding is held is not known yet` }, { "retry-after": String(RETRY_AFTER_S) });
        }
        firstTold.delete(told);
        if (holding === "gone") return answer(outgoing, 401, { error: `the binding of ${binding.key} is gone` });
        const body = yield* Effect.promise(() => bodyOf(incoming, TOLD_BYTES));
        if (body === null) return answer(outgoing, 413, { error: `a telling is ${TOLD_BYTES} bytes at most` });
        const answered = yield* relay(binding, body);
        return answer(outgoing, answered.status, answered.json, answered.headers);
      }
      const claims = Option.isSome(bearer) ? verifyToken(callerKey, bearer.value[1], now) : null;
      // A caller's token, which stops within MAX_CALLER_TOKEN_MS, or is held to a binding; a binding's, which a bound pod takes requests with, is none.
      if (!claims || claims.binding !== undefined || (claims.expires !== undefined && claims.expires > now + MAX_CALLER_TOKEN_MS)) return answer(outgoing, 401, { error: "no caller's token" });
      if (claims.expires === undefined) {
        const { held } = claims;
        if (!held || !routeHeld || !grants?.(claims.key, held)) return answer(outgoing, 401, { error: "no caller's token" });
        const asked = url.split("?", 1)[0] ?? "";
        if (!heldMayAsk(incoming.method ?? "GET", asked)) return answer(outgoing, 403, { error: `a token held to a binding is for its workspace's bayma and files alone, not ${incoming.method} ${asked}` });
        // Good for as long as the binding it is held to is: a harness's token of a workspace it was
        // given, a session's its conversation has switched from among them, which stays good until
        // that binding is released, as the same conversation's own. As the watch says it: a binding
        // not seen where the watch has heard nothing since the token was minted, its clock allowed
        // for (CLOCKS_APART_MS), is not known, as one made then may not be heard of yet, and one
        // released within that time is refused so too, 503 for 401; and one whose Lease was
        // deleted while the watch was cut is held still until the watch, resumed, hears of it, or
        // it has heard nothing for its silence's bound (../kube/client.ts WATCH_SILENCE_MS).
        const holding = yield* routeHeld(held.key, held.binding, held.namespace, claims.issued === undefined ? undefined : claims.issued + CLOCKS_APART_MS);
        if (holding === "unknown") return answer(outgoing, 503, { error: `whether ${held.key}'s binding is held is not known yet` }, { "retry-after": String(RETRY_AFTER_S), [NOT_PASSED_HEADER]: "true" });
        if (holding === "gone") return answer(outgoing, 401, { error: `the binding of ${held.key} the token is held to is gone` });
      }
      const { key } = claims;
      const [path = "", search = ""] = url.split("?", 2);
      if (operate && OPERATED.test(path)) {
        const body = yield* Effect.promise(() => bodyOf(incoming, MAX_OPERATION_BYTES));
        if (body === null) return answer(outgoing, 413, { error: `a request answered here is ${MAX_OPERATION_BYTES} bytes at most` });
        const answered = yield* operate(claims, { method: incoming.method ?? "GET", path, query: new URLSearchParams(search), body });
        if (answered.bytes !== undefined) return void outgoing.writeHead(answered.status, { ...answered.headers, "content-type": "application/octet-stream" }).end(answered.bytes);
        return answer(outgoing, answered.status, answered.json, answered.headers);
      }
      if (incoming.method === "POST" && url === RELEASE_PATH) {
        incoming.resume();
        const released = yield* poolOf(key, claims.options ?? {}).pipe(
          // Its requests under way through any replica let end first; then its route forgotten, as its binding is released.
          Effect.tap(({ pool }) => Effect.andThen(quiet(pool, key), Effect.sync(() => routes.delete(slot(pool, key))))),
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
      const pinned = incoming.headers[PINNED_HEADER];
      delete incoming.headers[PINNED_HEADER];
      /** Whether `to` is of another binding than the request is for alone, as answered then: the binding the key is bound to now told, nothing passed. */
      const movedFrom = (to: Routed): boolean => {
        if (pinned === undefined || pinned === to.binding) return false;
        incoming.resume();
        answer(outgoing, 409, { error: `${key} is bound to another binding than ${String(pinned)} now`, moved: { binding: to.binding, namespace: to.pool.namespace } }, { [NOT_PASSED_HEADER]: "true" });
        return true;
      };
      // A bind asked ahead, and a request that binds nothing, asks how the key is bound now, not
      // the route this front last knew, which a release by another, as an idle or broken binding's,
      // leaves behind.
      const options = claims.options ?? {};
      /** Why `key` could not be routed, answered. */
      const refused = (failure: KubeApiError | BindTimeout | BindStepFailed | PlacementRefused | PlacementHeld) =>
        Effect.gen(function*() {
          if (failure._tag === "PlacementRefused") return answer(outgoing, 403, { error: failure.message });
          // Held, it is bound again only as its caller asks: no Retry-After.
          if (failure._tag === "PlacementHeld") return answer(outgoing, 503, { error: `${key} is bound again only as its caller asks`, reason: failure.message, held: true });
          yield* Effect.logWarning(`binding ${key} failed: ${failure.message}`);
          return answer(outgoing, 503, { error: `${key} could not be bound now`, reason: failure.message }, { "retry-after": String(RETRY_AFTER_S) });
        });
      const routed = yield* routeOf(key, options, { unbound, anew: unbound || (incoming.method === "POST" && url === BIND_PATH) }).pipe(Effect.result);
      if (routed._tag === "Failure") return yield* refused(routed.failure);
      if (!routed.success) {
        incoming.resume();
        return answer(outgoing, 409, { bound: false });
      }
      if (incoming.method === "POST" && url === BIND_PATH) {
        incoming.resume();
        const { binding, pool } = routed.success.route;
        return answer(outgoing, 200, { binding, namespace: pool.namespace });
      }
      const { known } = routed.success;
      let { route } = routed.success;
      if (movedFrom(route)) return;
      // On a route it knew of, which may be of a binding released since, as its watch has yet to
      // say, a request whose body it may read whole is read first, to pass it again.
      const body = known && replayable(incoming, REPLAYED_BYTES) ? (yield* Effect.promise(() => bodyOf(incoming, REPLAYED_BYTES))) ?? undefined : undefined;
      const passed = (to: Routed) =>
        Effect.gen(function*() {
          // One that binds nothing only reads what the binding has, as whether it is idle, and so keeps it no longer.
          if (!unbound) yield* active(key, to.pool, to.binding);
          // A call holds its binding against its caller's release while it is under way; a GET does not, as the
          // stream an MCP client listens on for what its server sends of its own, open for as long as the client is,
          // waits on nothing it asked.
          if (incoming.method === "GET") {
            // Answered as an event stream, a stream, which a stop cuts at once, as it does one
            // whose head comes as it stops; another GET is a request under way, let end.
            return yield* Effect.promise(() =>
              proxy(incoming, outgoing, to, body, (answer) => {
                if (!eventStream(answer)) return;
                streams.add(outgoing);
                if (stopping) outgoing.destroy();
              })
            );
          }
          const flight = slot(to.pool, key);
          flights.set(flight, (flights.get(flight) ?? 0) + 1);
          return yield* Effect.promise(() => proxy(incoming, outgoing, to, body)).pipe(
            Effect.ensuring(Effect.sync(() => void flights.set(flight, (flights.get(flight) ?? 1) - 1))),
          );
        });
      let proxied = yield* passed(route);
      if (!proxied.reached && known && body !== undefined && !proxied.connected && NEVER_CONNECTED.has(proxied.code ?? "")) {
        // The pod not reached by a route it knew of, nothing of the request sent it: where its
        // binding is gone, released by another replica or by this one's sweep, the key is
        // routed anew, bound as a request binds it, within its bind's own wait, and the request
        // passed again.
        routes.delete(slot(route.pool, key));
        const now = yield* binder.bound(route.pool, key).pipe(Effect.orElseSucceed(() => undefined));
        if (now !== undefined && now?.binding !== route.binding) {
          yield* Effect.logInfo(`the route of ${key} was of its binding ${route.binding}, gone: routed anew`);
          const again = yield* routeOf(key, options, { unbound, anew: true }).pipe(Effect.result);
          if (again._tag === "Failure") return yield* refused(again.failure);
          if (!again.success) return answer(outgoing, 409, { bound: false });
          route = again.success.route;
          if (movedFrom(route)) return;
          proxied = yield* passed(route);
        }
      }
      if (proxied.reached) return;
      routes.delete(slot(route.pool, key));
      yield* Effect.logWarning(`the pod of ${key} was not reached: ${proxied.error}`);
      // Asked again only where the pod had nothing of it: one whose connection was made may have run it.
      const unsent = !proxied.connected && NEVER_CONNECTED.has(proxied.code ?? "");
      answer(outgoing, 503, { error: `the pod of ${key} was not reached` }, unsent ? { "retry-after": String(RETRY_AFTER_S), [NOT_PASSED_HEADER]: "true" } : {});
    }).pipe(
      Effect.catchCause((cause) => Effect.logWarning("answering a request failed", cause).pipe(Effect.andThen(Effect.sync(() => {
        if (!outgoing.headersSent) answer(outgoing, 500, { error: "answering the request failed" });
        else outgoing.destroy();
      })))),
      withLogScope("binder"),
    );

  /** Whether the front is told to stop, as the module says; what it is answering, and of that the streams it passes on, answered as event streams. */
  let stopping = false;
  const answering = new Set<ServerResponse>();
  const streams = new Set<ServerResponse>();
  const server = createServer((incoming, outgoing) => {
    // Stopping, it passes on no request, but tells its caller to ask again, as no pod was given
    // it, its connection closed, as one a keep-alive caller made before the stop brings it.
    if (stopping) {
      outgoing.shouldKeepAlive = false;
      return answer(outgoing, 503, { error: "the gateway is stopping" }, { "retry-after": String(RETRY_AFTER_S), [NOT_PASSED_HEADER]: "true" });
    }
    answering.add(outgoing);
    outgoing.on("close", () => void (answering.delete(outgoing), streams.delete(outgoing)));
    void runPromise(handle(incoming, outgoing));
  });
  // A request holds its stream for as long as its call runs: no time limit of the server's own.
  server.requestTimeout = 0;
  /** The requests under way but the streams passed on, which a stop lets end. */
  const underWay = () => answering.size - streams.size;
  yield* Effect.acquireRelease(
    Effect.callback<void>((resume) => void server.listen(port, host, () => resume(Effect.void))),
    // Stopped as the module says.
    () =>
      Effect.gen(function*() {
        const closed = yield* Deferred.make<void>();
        stopping = true;
        // An answer under way told to close its connection as it ends, its headers not sent yet,
        // so its caller sends nothing more on it, which the cut as the stop ends would reset.
        for (const outgoing of answering) if (!outgoing.headersSent) outgoing.shouldKeepAlive = false;
        // Its callers' streams cut at once, not left open past what reaches it: a server's request
        // sent on one, as bayma's ping of its client, would be answered by a call the front refuses,
        // and go unanswered for as long as no gateway is reached, its MCP session closed for it;
        // cut, its caller opens it again where a gateway is (as bayma counts a ping whose stream
        // ended for nothing).
        for (const stream of streams) stream.destroy();
        server.close(() => Deferred.doneUnsafe(closed, Exit.void));
        // Those idle now closed, once: a request on one kept after is answered as the module
        // says, never cut, as a connection closed as its caller writes on it would be.
        server.closeIdleConnections();
        const began = yield* Clock.currentTimeMillis;
        yield* Effect.logInfo(`stopping: no new connection taken, ${underWay()} call(s) under way let end`);
        for (;;) {
          const stoppingFor = (yield* Clock.currentTimeMillis) - began;
          if (underWay() === 0 && stoppingFor >= stopTimings.lingerMs) break;
          if (stoppingFor >= stopTimings.callsMs) {
            yield* Effect.logWarning(`stopping: ${underWay()} call(s) under way still after ${stopTimings.callsMs / 1000} s, cut`);
            break;
          }
          yield* Effect.sleep(FLIGHTS_POLL_MS);
        }
        server.closeAllConnections();
        yield* Deferred.await(closed);
      }).pipe(withLogScope("binder")),
  );
  return {
    // Listening on a TCP port, its address is an internet one.
    port: (server.address() as AddressInfo).port,
    inFlight: (pool, key) => flights.get(slot(pool, key)) ?? 0,
  };
});
