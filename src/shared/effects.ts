/**
 * How alasio runs effects from outside Effect: where a library calls alasio back as
 * promises (Claude Code's hooks, which the Agent SDK awaits), or where a fiber must start
 * outside the trace that is active (src/harness/claude/live-sessions.ts). Everything
 * else in alasio is an effect run by the program src/alasio.ts makes.
 */
import { Context, Effect, type Fiber } from "effect";

/** Runs effects needing services `R`, as a promise or as a fiber of its own. */
export interface EffectRunner<R> {
  readonly runPromise: <A, E>(effect: Effect.Effect<A, E, R>) => Promise<A>;
  readonly runFork: <A, E>(effect: Effect.Effect<A, E, R>) => Fiber.Fiber<A, E>;
}

/**
 * An EffectRunner over `services` that runs effects as the effect making it runs: with
 * its logger, tracer, and the rest of its context too.
 */
export const effectRunnerHere = <R>(services: Context.Context<R>): Effect.Effect<EffectRunner<R>> =>
  Effect.map(Effect.context<never>(), (context) => {
    const merged = Context.merge(context, services);
    return { runPromise: Effect.runPromiseWith(merged), runFork: Effect.runForkWith(merged) };
  });
