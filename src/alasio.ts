/**
 * alasio assembled: the services it runs on, and the app serving Telegram over them.
 * src/main.ts runs it in production, as ./shared/run.ts runs a process,
 * test/support/alasio-main.ts against the tests' stand-ins.
 */
import { Effect, Layer, Option, type Scope } from "effect";
import type { Pool } from "pg";

import { parentForks, serveBranchForks } from "./branch/fork.ts";
import { CodexAppServer } from "./codex/app-server/client.ts";
import { codexHome } from "./codex/env.ts";
import { type CodexLoginError, keepCodexLogin } from "./codex/login.ts";
import { SessionFsCodex, sessionFsCodexHome } from "./codex/sessionfs.ts";
import { Turns } from "./codex/turns.ts";
import type { AlasioConfig, BranchEnvironment } from "./config.ts";
import { ActiveTurns } from "./harness/active-turns.ts";
import type { ClaudeQueryFactory } from "./harness/claude/runtime.ts";
import { Harnesses } from "./harness/index.ts";
import type { KubeTemplates } from "./kube/config.ts";
import { BusyPods } from "./kube/busy.ts";
import { KubeClient } from "./kube/client.ts";
import { type FolderBayma, HostBayma } from "./mcp/bayma.ts";
import { Mounts } from "./operator/mounts.ts";
import type { StoreError } from "./persistence/sql.ts";
import { Store } from "./persistence/store.ts";
import { type Inheritance, SessionInheritError, SessionSandboxes, SessionUnavailable } from "./sandbox/index.ts";
import { serveTelegram, type TelegramAppConfig, type TelegramAppError } from "./telegram/app.ts";
import { Authorizer } from "./telegram/authorizer.ts";
import { TelegramClient } from "./telegram/client.ts";
import { ReceivedFiles } from "./telegram/files.ts";
import { MediaGroups } from "./telegram/media-group-buffer.ts";
import { Outbox } from "./telegram/outbox.ts";
import type { TelegramUpdates } from "./telegram/update-consumer.ts";

/** What alasio is made with: its configuration, its Neon and what main keeps there, what the deployment's templates offer, and where its updates come from. */
export interface AlasioOptions extends Omit<AlasioConfig, "telegramLog" | "lakeWake">, TelegramAppConfig {
  /** Telegram's updates: its environment's topic in the log (TelegramUpdates.layer), or a stand-in's in alasio's tests. */
  readonly updates: Layer.Layer<TelegramUpdates>;
  /** The pool of alasio's Neon database, which its state is kept in. */
  readonly pool: Pool;
  /** The schema alasio's state is kept in: `state`, unless a test gives one of its own. */
  readonly stateSchema?: string | undefined;
  readonly kubeTemplates?: KubeTemplates | null | undefined;
  /** Stand-ins for a folder workspace's bayma and for Claude Code, in the harnesses alasio makes. */
  readonly folderBayma?: FolderBayma | undefined;
  readonly claudeQueryFactory?: ClaudeQueryFactory | undefined;
}

/** The services alasio's app runs on, which are always there. */
export type AlasioServices =
  | Store
  | ReceivedFiles
  | TelegramClient
  | Outbox
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
export function alasioServices(options: AlasioOptions): Layer.Layer<AlasioServices, StoreError | CodexLoginError> {
  return MediaGroups.layer().pipe(
    Layer.provideMerge(Layer.mergeAll(
      Mounts.layer({ workspaceRoot: options.workspaceRoot, branch: options.branch?.name }),
      Authorizer.layer(options.allowedUserIds),
      branchForks(options),
    )),
    Layer.provideMerge(Turns.layer()),
    Layer.provideMerge(Harnesses.layer({
      branch: options.branch?.name,
      sessionStore: options.sessionStore,
      codexRollouts: options.codexRollouts,
      sessionFsCodexRollouts: options.sessionFsCodexRollouts,
      folderBayma: options.folderBayma,
      claudeQueryFactory: options.claudeQueryFactory,
    })),
    Layer.provideMerge(Layer.mergeAll(
      Outbox.layer,
      ReceivedFiles.layer(options),
      // Before Codex is first started, which is not before a turn needs it.
      options.keepCodexLogin ? Layer.effectDiscard(keepCodexLogin(codexHome())) : Layer.empty,
      workspaceServices(options),
      codexServices(options),
      activeTurns(options),
    )),
    Layer.provideMerge(Layer.mergeAll(
      Store.layer({ pool: options.pool, schema: options.stateSchema, workingDirectory: options.workingDirectory, branch: options.branch?.name }),
      TelegramClient.layer(options.telegramBotToken),
    )),
  );
}

/**
 * The services of the workspaces the deployment's templates offer, each made only where
 * its template is rendered, on one KubeClient: session filesystems (SessionSandboxes)
 * and folder workspaces' bayma (HostBayma). They are not among AlasioServices, which are
 * always there; what uses them asks whether they are.
 */
function workspaceServices({ kubeTemplates, stateDir, branch }: AlasioOptions): Layer.Layer<never, never, Store> {
  const sessions = kubeTemplates?.sessions ?? null;
  const host = kubeTemplates?.host ?? null;
  if (!sessions && !host) return Layer.empty;
  return Layer.mergeAll(
    // Settings the deployment gives wrongly stop alasio as it starts.
    sessions
      ? Layer.unwrap(Effect.gen(function*() {
        const store = yield* Store;
        const inherit = branch ? inheritance(branch, store) : undefined;
        // A session's internet, as the store records what it was made with.
        const netModeOf = (volumeId: string) =>
          store.listSessionWorkspaces.pipe(
            Effect.map((workspaces) => workspaces.find((workspace) => workspace.volumeId === volumeId)?.netMode ?? null),
            Effect.mapError((error) => new SessionUnavailable({ message: `the session ${volumeId}'s internet could not be read: ${error.message}` })),
          );
        return Layer.orDie(SessionSandboxes.layer({ profile: sessions, stateDir, inherit, netModeOf }));
      }))
      : Layer.empty,
    host ? HostBayma.layer(host) : Layer.empty,
  ).pipe(Layer.provide(KubeClient.layer));
}

/**
 * The turns running, which hold the pods they run in busy (./kube/busy.ts) where alasio
 * runs in Kubernetes, as the deployment's templates say it does, on a KubeClient of their
 * own: its own, and its workspaces', in the namespaces the templates say; elsewhere they
 * hold none.
 */
function activeTurns({ kubeTemplates }: AlasioOptions): Layer.Layer<ActiveTurns> {
  if (!kubeTemplates) return ActiveTurns.layer;
  const namespaces = [kubeTemplates.sessions?.namespace, kubeTemplates.host?.namespace].filter((namespace) => namespace !== undefined);
  return ActiveTurns.layer.pipe(Layer.provide(Layer.orDie(BusyPods.layer({ namespaces })).pipe(Layer.provide(KubeClient.layer))));
}

/**
 * A branch environment's sessions it inherited: forked by its parent, which it asks
 * (./branch/fork.ts), and recorded as made in its own store, as it records those it makes.
 */
function inheritance(branch: BranchEnvironment, store: Store["Service"]): Inheritance {
  return {
    fork: parentForks({ url: branch.parentForks, branch: branch.name, tokenFile: branch.tokenFile }),
    record: (volumeId, netMode) =>
      store.recordInheritedSessionWorkspace(volumeId, netMode).pipe(
        Effect.mapError((error) => new SessionInheritError({ message: `the session ${volumeId} forked from the alasio this branch was branched from could not be recorded: ${error.message}` })),
      ),
  };
}

/**
 * Where the deployment gives alasio the key branch environments' tokens are signed with,
 * and it has sessions, the server they ask to fork the sessions they inherited
 * (./branch/fork.ts), for as long as alasio runs; a server that cannot listen stops
 * alasio as it starts.
 */
function branchForks({ branchForkKeyFile }: AlasioOptions): Layer.Layer<never, never, Store | ActiveTurns | Turns> {
  if (!branchForkKeyFile) return Layer.empty;
  return Layer.effectDiscard(
    Effect.serviceOption(SessionSandboxes).pipe(
      Effect.flatMap(Option.match({
        onNone: () => Effect.void,
        onSome: (sandboxes) => Effect.provideService(serveBranchForks({ keyFile: branchForkKeyFile }), SessionSandboxes, sandboxes),
      })),
      Effect.orDie,
    ),
  );
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
export const serveAlasio = Effect.fnUntraced(function*(options: AlasioOptions): Effect.fn.Return<void, TelegramAppError | CodexLoginError, Scope.Scope> {
  yield* Layer.build(serveTelegram(options).pipe(Layer.provide(Layer.merge(alasioServices(options), options.updates))));
  // Added last, so the first thing a stop does.
  yield* Effect.addFinalizer(() => Effect.logInfo("Shutting down..."));
});
