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
import type { AlasioConfig } from "./config.ts";
import { ActiveTurns } from "./harness/active-turns.ts";
import type { ClaudeQueryFactory } from "./harness/claude/runtime.ts";
import { Harnesses } from "./harness/index.ts";
import type { KubeTemplates } from "./kube/config.ts";
import { KubeClient } from "./kube/client.ts";
import { type FolderBayma, HostBayma } from "./mcp/bayma.ts";
import { Mounts } from "./operator/mounts.ts";
import { Store } from "./persistence/store.ts";
import { SessionSandboxes } from "./sandbox/index.ts";
import { AlasioLoggerLayer } from "./shared/log.ts";
import { TracingLayer } from "./telemetry/index.ts";
import { stopTelemetry } from "./telemetry/start.ts";
import { serveTelegram, type TelegramAppConfig, type TelegramAppError } from "./telegram/app.ts";
import { Authorizer } from "./telegram/authorizer.ts";
import { TelegramClient } from "./telegram/client.ts";
import { MediaGroups } from "./telegram/media-group-buffer.ts";
import { Outbox } from "./telegram/outbox.ts";
import { WorkflowHooks } from "./workflow/hook-server.ts";

/** What alasio is made with: its configuration, what main keeps in Neon, and what the deployment's templates offer. */
export interface AlasioOptions extends AlasioConfig, TelegramAppConfig {
  readonly kubeTemplates?: KubeTemplates | null | undefined;
  /** Stand-ins for a folder workspace's bayma and for Claude Code, in the harnesses alasio makes. */
  readonly folderBayma?: FolderBayma | undefined;
  readonly claudeQueryFactory?: ClaudeQueryFactory | undefined;
}

/** The services alasio's app runs on, which are always there. */
export type AlasioServices =
  | Store
  | TelegramClient
  | Outbox
  | WorkflowHooks
  | CodexAppServer
  | ActiveTurns
  | Harnesses
  | Turns
  | Mounts
  | Authorizer
  | MediaGroups;

/**
 * alasio's services, made for `options`: each made after what it runs on, and stopped
 * before it. The turns are made after the harnesses, Telegram and the store they report
 * to, so that stopping alasio interrupts the turns running while those are still there.
 */
export function alasioServices(options: AlasioOptions): Layer.Layer<AlasioServices> {
  return MediaGroups.layer().pipe(
    Layer.provideMerge(Layer.mergeAll(Mounts.layer(options), Authorizer.layer(options.allowedUserIds))),
    Layer.provideMerge(Turns.layer(options)),
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

/**
 * alasio serving Telegram, in the scope it is run in: its services made, then its app
 * started on them; when the scope closes, the app stops first, then each service in the
 * reverse of the order it was made in.
 */
export const serveAlasio = Effect.fnUntraced(function*(options: AlasioOptions): Effect.fn.Return<void, TelegramAppError, Scope.Scope> {
  yield* Layer.build(serveTelegram(options).pipe(Layer.provide(alasioServices(options))));
  // Added last, so the first thing a stop does.
  yield* Effect.addFinalizer(() => Effect.logInfo("Shutting down..."));
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
