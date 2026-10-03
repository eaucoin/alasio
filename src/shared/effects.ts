/**
 * How code not yet written in Effect runs effects: in the services the program that made
 * it built, so it reaches the same services, not copies of its own.
 */
import { Context, Effect, type Fiber } from "effect";

/** Runs effects needing services `R`, as a promise, a fiber, or, for an effect that never waits, synchronously. */
export interface EffectRunner<R> {
  readonly runPromise: <A, E>(effect: Effect.Effect<A, E, R>) => Promise<A>;
  readonly runFork: <A, E>(effect: Effect.Effect<A, E, R>) => Fiber.Fiber<A, E>;
  readonly runSync: <A, E>(effect: Effect.Effect<A, E, R>) => A;
}

/** An EffectRunner over `services`. */
export function effectRunner<R>(services: Context.Context<R>): EffectRunner<R> {
  return { runPromise: Effect.runPromiseWith(services), runFork: Effect.runForkWith(services), runSync: Effect.runSyncWith(services) };
}

/**
 * An EffectRunner over `services` that runs effects as the effect making it runs: with
 * its logger, tracer, and the rest of its context too.
 */
export const effectRunnerHere = <R>(services: Context.Context<R>): Effect.Effect<EffectRunner<R>> =>
  Effect.map(Effect.context<never>(), (context) => effectRunner(Context.merge(context, services)));
