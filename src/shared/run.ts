/**
 * A program of alasio's image as the process it is, alasio itself (../main.ts) among
 * them: logging and tracing as alasio does, and flushing its telemetry as it exits.
 */
import { NodeRuntime } from "@effect/platform-node";
import { Cause, Effect, Exit, Layer, type Scope } from "effect";

import { TracingLayer } from "../telemetry/index.ts";
import { stopTelemetry } from "../telemetry/start.ts";
import { AlasioLoggerLayer } from "./log.ts";

/**
 * Exits once the telemetry is flushed: 0 when the program was stopped, 1 when what it
 * runs failed.
 */
function teardown<E, A>(exit: Exit.Exit<A, E>, onExit: (code: number) => void): void {
  void stopTelemetry().finally(() => onExit(Exit.isFailure(exit) && !Cause.hasInterruptsOnly(exit.cause) ? 1 : 0));
}

/**
 * Runs `program` as alasio's process: with alasio's logging and tracing, until it is told
 * to stop (SIGTERM, SIGINT), when what it acquired is released in reverse, or fails.
 */
export function runAlasio<E>(program: Effect.Effect<void, E, Scope.Scope>): void {
  Layer.launch(Layer.effectDiscard(program)).pipe(
    Effect.provide([AlasioLoggerLayer, TracingLayer]),
    NodeRuntime.runMain({ disableErrorReporting: true, teardown }),
  );
}
