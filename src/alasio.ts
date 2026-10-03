/**
 * alasio assembled: the services it runs on, the app serving Telegram over them, and the
 * process that runs it. src/main.ts runs it in production, test/support/alasio-main.ts
 * against the tests' stand-ins.
 */
import { NodeRuntime } from "@effect/platform-node";
import { Cause, Effect, Exit, Layer, type Scope } from "effect";

import { CodexAppServer } from "./codex/app-server/client.ts";
import { SessionFsCodex, sessionFsCodexHome } from "./codex/sessionfs.ts";
import { Turns } from "./codex/turn-controller.ts";
import { ActiveTurns } from "./harness/active-turns.ts";
import { Harnesses } from "./harness/index.ts";
import { KubeClient } from "./kube/client.ts";
import { HostBayma } from "./mcp/bayma.ts";
import { Store } from "./persistence/store.ts";
import { SessionSandboxes } from "./sandbox/index.ts";
import { AlasioLoggerLayer } from "./shared/log.ts";
import { type EffectRunner, effectRunnerHere } from "./shared/effects.ts";
import { TracingLayer } from "./telemetry/index.ts";
import { stopTelemetry } from "./telemetry/start.ts";
import { TelegramCodexApp, type TelegramCodexAppConfig } from "./telegram/app.ts";
import { TelegramClient } from "./telegram/client.ts";
import { Outbox } from "./telegram/outbox.ts";
import { WorkflowHooks } from "./workflow/hook-server.ts";

/** The services alasio runs on, which its app's code not yet written in Effect reaches through an EffectRunner. */
export type AlasioServices = Store | TelegramClient | Outbox | WorkflowHooks | CodexAppServer | ActiveTurns | Harnesses | Turns;

/** What alasio is made with: the app's configuration, but for what alasio makes itself. */
export type AlasioOptions = Omit<TelegramCodexAppConfig, "effects">;

/**
 * alasio's services, made for `options`: each made after what it runs on, and stopped
 * before it. The turns are made last, so that stopping alasio interrupts the turns running
 * while the harnesses, Telegram and the store they report to are still there.
 */
export function alasioServices(options: AlasioOptions): Layer.Layer<AlasioServices> {
  return Turns.layer(options).pipe(
    Layer.provideMerge(Harnesses.layer({
      sessionStore: options.sessionStore,
      codexRollouts: options.codexRollouts,
      sessionFsCodexRollouts: options.sessionFsCodexRollouts,
      folderBayma: options.folderBayma,
      claudeQueryFactory: options.claudeQueryFactory,
    })),
    Layer.provideMerge(Layer.mergeAll(Outbox.layer, WorkflowHooks.layer(options.hookPort), workspaceServices(options), codexServices(options), ActiveTurns.layer)),
    Layer.provideMerge(Layer.mergeAll(Store.layer(options), TelegramClient.layer(options.telegramBotToken))),
  );
}

/**
 * The services of the workspaces the deployment's templates offer, each made only where
 * its template is rendered, on one KubeClient: session filesystems (SessionSandboxes)
 * and folder workspaces' bayma (HostBayma). They are not among AlasioServices, which are
 * always there; what uses them asks whether they are.
 */
function workspaceServices({ kubeTemplates, stateDir }: AlasioOptions): Layer.Layer<never> {
  const sessions = kubeTemplates?.sessions ?? null;
  const host = kubeTemplates?.host ?? null;
  if (!sessions && !host) return Layer.empty;
  return Layer.mergeAll(
    // Settings the deployment gives wrongly stop alasio as it starts.
    sessions ? Layer.orDie(SessionSandboxes.layer({ profile: sessions, stateDir })) : Layer.empty,
    host ? HostBayma.layer(host) : Layer.empty,
  ).pipe(Layer.provide(KubeClient.layer));
}

/**
 * Codex's app-servers: the operator's (CodexAppServer), which folder workspaces' turns
 * run on, and where the deployment offers session filesystems, theirs (SessionFsCodex),
 * from a Codex home of alasio's own; that one is not among AlasioServices, and what uses
 * it asks whether it is.
 */
function codexServices({ kubeTemplates, stateDir }: AlasioOptions): Layer.Layer<CodexAppServer> {
  return kubeTemplates?.sessions
    ? Layer.merge(CodexAppServer.layer, SessionFsCodex.layer({ home: sessionFsCodexHome(stateDir) }))
    : CodexAppServer.layer;
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
