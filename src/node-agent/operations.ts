/**
 * What the node agent does for longer than a request should wait, by workspace: a
 * checkpoint, or a prepare. Each runs on its own, in the agent's scope, whatever becomes
 * of the request that started it, as a client that gives up (alasio restarting, its call
 * timed out) would otherwise interrupt runsc in the middle of a save, or a download into
 * the cache. The client asks how it went until it has ended (./server.ts).
 *
 * A workspace has one of each kind at a time: one started while one runs is that one. Its
 * outcome is kept until the next starts, or for OUTCOME_TTL_MS after it ended, whichever
 * is first, so an outcome no client asks for is not kept for ever.
 *
 * As the agent stops, those that run are let end, not interrupted, before the scope they run
 * in closes: interrupted, runsc's save would end its sandbox unsaved, its snapshot lost with
 * it, and a prepare leave the cache half written. Its pod's termination grace
 * (OPERATION_LIMIT_SECONDS) covers the longest one.
 */
import { Cause, Clock, Effect, Exit, Fiber, Scope } from "effect";

/** How long an outcome no one asked for is kept. */
export const OUTCOME_TTL_MS = 60 * 60_000;

/** How an operation failed, as the API answers a request that failed so: its status, and why. */
export interface Failure {
  readonly status: number;
  readonly error: string;
}

/** How an operation stands: running, or how it ended. */
export type Outcome<A> =
  | { readonly state: "running" }
  | { readonly state: "done"; readonly value: A }
  | ({ readonly state: "failed" } & Failure);

/** One kind of operation, by workspace; see makeOperations. */
export interface Operations<A, E> {
  /** Starts `operation` for `key`, unless one runs for it, which goes on instead. */
  readonly start: (key: string, operation: Effect.Effect<A, E>) => Effect.Effect<void>;
  /** How the operation of `key` stands, or null when there is none, or its outcome is no longer kept. */
  readonly outcome: (key: string) => Effect.Effect<Outcome<A> | null>;
}

/**
 * Operations of one kind, run in a scope of `scope`'s, as the module says, a failure's
 * cause told as `failure` tells it; as `scope` closes, each running is waited for, and
 * what was made in `scope` after these, the API's server, is closed first.
 */
export const makeOperations = Effect.fnUntraced(function*<A, E>(scope: Scope.Scope, failure: (cause: Cause.Cause<E>) => Failure): Effect.fn.Return<Operations<A, E>> {
  const entries = new Map<string, { readonly outcome: Outcome<A>; readonly endedAt: number | null }>();
  const running = new Set<Fiber.Fiber<A, E>>();
  const runIn = yield* Scope.fork(scope);
  // Run before the scope's own closes runIn, as finalizers run in the reverse of their order.
  yield* Scope.addFinalizer(scope, Effect.suspend(() =>
    running.size === 0 ? Effect.void : Effect.logInfo(`the agent stops once the ${running.size} operations it runs have ended`).pipe(Effect.andThen(Fiber.awaitAll(running)))));

  /** Forgets the outcomes kept longer than OUTCOME_TTL_MS. */
  const expire = Effect.map(Clock.currentTimeMillis, (now) => {
    for (const [key, { endedAt }] of entries) if (endedAt !== null && now - endedAt > OUTCOME_TTL_MS) entries.delete(key);
  });

  /** Keeps how the operation of `key` ended; one interrupted, as the agent stops, ended with no outcome. */
  const ended = (key: string) => (exit: Exit.Exit<A, E>) =>
    Effect.map(Clock.currentTimeMillis, (now) => {
      if (Exit.isSuccess(exit)) entries.set(key, { outcome: { state: "done", value: exit.value }, endedAt: now });
      else if (Cause.hasInterruptsOnly(exit.cause)) entries.delete(key);
      else entries.set(key, { outcome: { state: "failed", ...failure(exit.cause) }, endedAt: now });
    });

  return {
    start: (key, operation) =>
      Effect.gen(function*() {
        yield* expire;
        if (entries.get(key)?.outcome.state === "running") return;
        entries.set(key, { outcome: { state: "running" }, endedAt: null });
        const fiber = yield* operation.pipe(Effect.onExit(ended(key)), Effect.forkIn(runIn));
        running.add(fiber);
        fiber.addObserver(() => running.delete(fiber));
      }),
    outcome: (key) => Effect.map(expire, () => entries.get(key)?.outcome ?? null),
  };
});
