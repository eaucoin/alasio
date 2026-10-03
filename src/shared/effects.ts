/**
 * How code not yet written in Effect runs effects: in the services the program that made
 * it built, so it reaches the same services, not copies of its own.
 */
import { type Context, Effect, type Fiber } from "effect";

/** Runs effects needing services `R`, as a promise or a fiber. */
export interface EffectRunner<R> {
  readonly runPromise: <A, E>(effect: Effect.Effect<A, E, R>) => Promise<A>;
  readonly runFork: <A, E>(effect: Effect.Effect<A, E, R>) => Fiber.Fiber<A, E>;
}

/** An EffectRunner over `services`. */
export function effectRunner<R>(services: Context.Context<R>): EffectRunner<R> {
  return { runPromise: Effect.runPromiseWith(services), runFork: Effect.runForkWith(services) };
}
