/**
 * alasio assembled: the services it runs on, the app serving Telegram over them, and the
 * process that runs it. src/main.ts runs it in production, test/support/alasio-main.ts
 * against the tests' stand-ins.
 */
import { NodeRuntime } from "@effect/platform-node";
import { Cause, Effect, Exit, Layer, type Scope } from "effect";

import { Store } from "./persistence/store.ts";
import { AlasioLoggerLayer } from "./shared/log.ts";
import { type EffectRunner, effectRunnerHere } from "./shared/effects.ts";
import { TracingLayer } from "./telemetry/index.ts";
import { stopTelemetry } from "./telemetry/start.ts";
import { TelegramCodexApp, type TelegramCodexAppConfig } from "./telegram/app.ts";
import { TelegramClient } from "./telegram/client.ts";
import { Outbox } from "./telegram/outbox.ts";

/** The services alasio runs on, which its app's code not yet written in Effect reaches through an EffectRunner. */
export type AlasioServices = Store | TelegramClient | Outbox;

/** What alasio is made with: the app's configuration, but for what alasio makes itself. */
export type AlasioOptions = Omit<TelegramCodexAppConfig, "effects">;

/** alasio's services, made for `options`. */
export function alasioServices(options: AlasioOptions): Layer.Layer<AlasioServices> {
  return Outbox.layer.pipe(
    Layer.provideMerge(Layer.mergeAll(Store.layer(options), TelegramClient.layer(options.telegramBotToken))),
  );
}

/** What runs the effects of alasio's code not yet written in Effect, in alasio's services. */
export type AlasioEffects = EffectRunner<AlasioServices>;

/**
 * alasio serving Telegram, in the scope it is run in: its services made, its app started
 * on them, and both stopped, the app first, when the scope closes.
 */
export const serveAlasio = (options: AlasioOptions): Effect.Effect<void, never, Scope.Scope> =>
  Effect.gen(function*() {
    // Built in the scope alasio runs in, so they last as long as it does.
    const effects = yield* effectRunnerHere(yield* Layer.build(alasioServices(options)));
    yield* Effect.acquireRelease(
      Effect.promise(async () => {
        const app = new TelegramCodexApp({ ...options, effects });
        await app.start();
        return app;
      }),
      (app) => Effect.logInfo("Shutting down...").pipe(Effect.andThen(Effect.promise(() => app.stop()))),
    );
  });

/**
 * Exits once the telemetry is flushed: 0 when alasio was stopped, 1 when what it runs
 * failed.
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
