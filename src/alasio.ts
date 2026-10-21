/**
 * alasio assembled: the services it runs on, and the app serving Telegram over them.
 * src/main.ts runs it in production, as ./shared/run.ts runs a process,
 * test/support/alasio-main.ts against the tests' stand-ins.
 */
import { join } from "node:path";

import { Effect, Layer, Option, type Scope } from "effect";
import type { Pool } from "pg";

import { ForkAsks, forkAnswered, ForkRequests, parentForks, serveBranchForks } from "./branch/fork.ts";
import { CodexAppServer } from "./codex/app-server/client.ts";
import { codexHome } from "./codex/env.ts";
import { type CodexLoginError, keepCodexLogin } from "./codex/login.ts";
import { SessionFsCodex, sessionFsCodexHome } from "./codex/sessionfs.ts";
import { Turns } from "./codex/turns.ts";
import type { AlasioConfig, BranchEnvironment } from "./config.ts";
import { type EnvironmentEvents, EventsConsumer } from "./events/consume.ts";
import type { Events } from "./events/events.ts";
import { ActiveTurns } from "./harness/active-turns.ts";
import type { ClaudeQueryFactory } from "./harness/claude/runtime.ts";
import { HarnessHost } from "./harness/host.ts";
import { HarnessEvents, HostEvents } from "./harness/host-events.ts";
import { pooledHarnessHost } from "./harness/pooled-host.ts";
import { CODEX_LOGIN_PORT, poolLoginKey, poolLoginRefusal, startLoginRelay } from "./codex/login-relay.ts";
import { Harnesses } from "./harness/index.ts";
import { BusyPods } from "./kube/busy.ts";
import { KubeClient } from "./kube/client.ts";
import { type FolderBayma, HostBayma } from "./mcp/bayma.ts";
import { BaymaProxy } from "./mcp/bayma-proxy.ts";
import { Mounts } from "./operator/mounts.ts";
import type { StoreError } from "./persistence/sql.ts";
import { Store } from "./persistence/store.ts";
import { makeGatewayWorkspaces } from "./sandbox/gateway-client.ts";
import { type Inheritance, type NetMode, SessionInheritError, SessionSandboxes } from "./sandbox/index.ts";
import { serveTelegram, type TelegramAppConfig, type TelegramAppError } from "./telegram/app.ts";
import { Authorizer } from "./telegram/authorizer.ts";
import { TelegramClient } from "./telegram/client.ts";
import { ReceivedFiles } from "./telegram/files.ts";
import { MediaGroups } from "./telegram/media-group-buffer.ts";
import { Outbox } from "./telegram/outbox.ts";
import type { TelegramUpdates } from "./telegram/update-consumer.ts";

/** What alasio is made with: its configuration, its Neon and what main keeps there, what the deployment's templates offer, and where its updates come from. */
export interface AlasioOptions extends Omit<AlasioConfig, "telegramLog" | "log" | "events" | "lakeWake">, TelegramAppConfig {
  /** Telegram's updates: its environment's topic in the log (TelegramUpdates.layer), or a stand-in's in alasio's tests. */
  readonly updates: Layer.Layer<TelegramUpdates>;
  /**
   * In a branch, what it asks its parent to fork the sessions it inherited with (ForkAsks), on
   * the log, or a stand-in in tests; a branch given none inherits no session.
   */
  readonly parentLog?: Layer.Layer<ForkAsks> | undefined;
  /**
   * The environment's events, read in its group (EnvironmentEvents), where alasio reads them:
   * where its harnesses run in pools, and in a branch, whose parent answers its forks there; on
   * the log, or a stand-in in tests.
   */
  readonly events?: Layer.Layer<EnvironmentEvents> | undefined;
  /** Where alasio forks its sessions for branches, the requests they make (ForkRequests) and what it answers on (Events), on the log, or stand-ins in tests. */
  readonly branchesLog?: Layer.Layer<ForkRequests | Events> | undefined;
  /** The pool of alasio's Neon database, which its state is kept in. */
  readonly pool: Pool;
  /** The schema alasio's state is kept in: `state`, unless a test gives one of its own. */
  readonly stateSchema?: string | undefined;
  /** The harness host's role, which Neon has (../neon/harness.ts), granted its part of the state; none in a test without one. */
  readonly harnessRole?: string | undefined;
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
  | HarnessHost
  | HarnessEvents
  | Harnesses
  | Turns
  | Mounts
  | Authorizer
  | MediaGroups;

/**
 * alasio's services, made for `options`: each made after what it runs on, and stopped
 * before it. The turns are made after the harnesses, Telegram and the store they report
 * to, so that stopping alasio interrupts the turns running while those are still there. The
 * environment's events are read last, by one member of their group, once every part has added
 * its handling of them (EventsConsumer).
 */
export function alasioServices(options: AlasioOptions): Layer.Layer<AlasioServices, StoreError | CodexLoginError> {
  return Layer.effectDiscard(Effect.flatMap(EventsConsumer, (consumer) => consumer.start)).pipe(Layer.provideMerge(MediaGroups.layer().pipe(
    Layer.provideMerge(Layer.mergeAll(
      Mounts.layer({ workspaceRoot: options.workspaceRoot, branch: options.branch?.name }),
      Authorizer.layer(options.allowedUserIds),
      branchForks(options),
      branchEvents(options),
    )),
    Layer.provideMerge(Turns.layer()),
    Layer.provideMerge(harnessHost(options)),
    Layer.provideMerge(Harnesses.layer({
      branch: options.branch?.name,
      sessionStore: options.sessionStore,
      codexRollouts: options.codexRollouts,
      sessionFsCodexRollouts: options.sessionFsCodexRollouts,
      folderBayma: options.folderBayma,
      claudeQueryFactory: options.claudeQueryFactory,
    })),
    // After the session filesystems, which it writes a session's files into.
    Layer.provideMerge(ReceivedFiles.layer(options)),
    Layer.provideMerge(Layer.mergeAll(
      Outbox.layer,
      // Before Codex is first started, which is not before a turn needs it.
      options.keepCodexLogin ? Layer.effectDiscard(keepCodexLogin(codexHome())) : Layer.empty,
      workspaceServices(options),
      codexServices(options),
      activeTurns(options),
    )),
    Layer.provideMerge(eventsConsumer(options)),
    Layer.provideMerge(Layer.mergeAll(
      stateStore(options),
      TelegramClient.layer(options.telegramBotToken),
    )),
  )));
}

/**
 * alasio's store, its state made as it opens: its tables, a branch's state its own, and the
 * harness host's grants; as alasio starts, and as `alasio setup` makes it before (./setup/program.ts).
 */
export const stateStore = (options: Pick<AlasioOptions, "pool" | "stateSchema" | "workingDirectory" | "branch" | "harnessRole">): Layer.Layer<Store, StoreError> =>
  Store.layer({ pool: options.pool, schema: options.stateSchema, workingDirectory: options.workingDirectory, branch: options.branch?.name, harnessRole: options.harnessRole });

/**
 * The one member of the environment's events' group every part of alasio adds its handling
 * to: on its events topic where alasio reads it (AlasioOptions.events); elsewhere none, which
 * reads nothing, and to which a part adding a handling is a defect.
 */
function eventsConsumer({ events }: AlasioOptions): Layer.Layer<EventsConsumer, never, Store> {
  if (events) return EventsConsumer.layer.pipe(Layer.provide(events));
  return Layer.succeed(EventsConsumer, EventsConsumer.of({
    handle: (handlers) => Effect.die(new Error(`alasio reads no events topic here, so it handles no event of ${Object.keys(handlers).join(", ")}`)),
    start: Effect.void,
  }));
}

/**
 * The harness host alasio's turns run on, and where it hears what their harnesses do on their
 * own: where its harnesses run in pools of their own, each conversation's, bound in its
 * harness's pool through the sandbox gateway, heard on its environment's events topic; and
 * otherwise its own, in its process, told and heard there.
 */
function harnessHost({ harnessPools, sessionsGateway }: AlasioOptions): Layer.Layer<HarnessHost | HarnessEvents, never, Store | Harnesses | ActiveTurns | ReceivedFiles | EventsConsumer | CodexAppServer> {
  if (harnessPools && sessionsGateway) {
    const home = codexHome();
    // Its Codex login relayed to its pools' pods, on its pod's port, to each Codex turn's token alone; held by none of them.
    const relay = Layer.effectDiscard(Effect.ignore(startLoginRelay({
      authFile: join(home, "auth.json"),
      listen: { host: "0.0.0.0", port: CODEX_LOGIN_PORT },
      authorize: poolLoginRefusal(poolLoginKey(sessionsGateway.callerKey), Date.now),
    }).pipe(Effect.tapError((error) => Effect.logError(`the Codex login could not be relayed to the harness pools: ${error.message}`)))));
    return Layer.mergeAll(
      pooledHarnessHost({ ...sessionsGateway, codexLogin: { url: harnessPools.codexLoginUrl, home } }),
      HarnessEvents.fromLog,
      relay,
    );
  }
  return HarnessHost.layer.pipe(Layer.provideMerge(HostEvents.inProcess));
}

/**
 * The services of the workspaces the deployment offers, each made only where it offers them:
 * session filesystems (SessionSandboxes), through the sandbox gateway it gives alasio, and
 * folder workspaces' bayma (HostBayma), the gateway's folder instances, where it serves them;
 * both reached by the harnesses at one loopback of alasio's (BaymaProxy). They are not among
 * AlasioServices, which are always there; what uses them asks whether they are.
 */
function workspaceServices({ folders, sessionsGateway, stateDir, branch, parentLog }: AlasioOptions): Layer.Layer<never, never, Store | TelegramClient> {
  if (!sessionsGateway) return Layer.empty;
  return Layer.mergeAll(
    Layer.unwrap(Effect.gen(function*() {
      const store = yield* Store;
      const asks = Option.getOrUndefined(yield* Effect.serviceOption(ForkAsks));
      const inherit = branch && asks ? inheritance(branch, store, asks) : undefined;
      return SessionSandboxes.layer({ workspaces: makeGatewayWorkspaces(sessionsGateway), environment: sessionsGateway.environment, stateDir, netModeOf: netModeIn(store), inherit });
    })).pipe(Layer.provide(parentLog ?? Layer.empty)),
    folders ? HostBayma.layer(makeGatewayWorkspaces(sessionsGateway).folders) : Layer.empty,
  ).pipe(Layer.provide(BaymaProxy.layer));
}

/** The internet each session filesystem was made with, as `store` records it; null for one it records not, or cannot read. */
const netModeIn = (store: Store["Service"]) => (volumeId: string): Effect.Effect<NetMode | null> =>
  store.listSessionWorkspaces.pipe(
    Effect.map((workspaces) => workspaces.find((workspace) => workspace.volumeId === volumeId)?.netMode ?? null),
    Effect.catch((error) => Effect.as(Effect.logWarning(`could not read the internet session ${volumeId} was made with: ${error.message}`), null)),
  );

/**
 * The turns running, which hold alasio's own pod busy (./kube/busy.ts) where alasio runs in
 * Kubernetes, on a KubeClient of their own, as the turns it drives in its harness pools do
 * (./harness/pooled-host.ts); elsewhere they hold none. The pods its workspaces run in are the
 * gateway's, which holds each busy as it is bound.
 */
function activeTurns({ inKubernetes }: AlasioOptions): Layer.Layer<ActiveTurns> | Layer.Layer<ActiveTurns | BusyPods> {
  if (!inKubernetes) return ActiveTurns.layer;
  return ActiveTurns.layer.pipe(Layer.provideMerge(Layer.orDie(BusyPods.layer({})).pipe(Layer.provide(KubeClient.layer))));
}

/**
 * A branch environment's sessions it inherited: forked by its parent, which it asks
 * (./branch/fork.ts), and recorded as made in its own store, as it records those it makes.
 */
function inheritance(branch: BranchEnvironment, store: Store["Service"], asks: ForkAsks["Service"]): Inheritance {
  const fork = parentForks({ branch: branch.name, tokenFile: branch.tokenFile });
  return {
    fork: (volumeId) => fork(volumeId).pipe(Effect.provideService(ForkAsks, asks), Effect.provideService(Store, store)),
    record: (volumeId, netMode) =>
      store.recordInheritedSessionWorkspace(volumeId, netMode).pipe(
        Effect.mapError((error) => new SessionInheritError({ message: `the session ${volumeId} forked from the alasio this branch was branched from could not be recorded: ${error.message}` })),
      ),
  };
}

/**
 * Where the deployment gives alasio the key branch environments' tokens are signed with,
 * and it has sessions, its answers to their requests to fork the sessions they inherited
 * (./branch/fork.ts), for as long as alasio runs.
 */
function branchForks({ branchForkKeyFile, branchesLog }: AlasioOptions): Layer.Layer<never, never, Store | HarnessHost | Turns> {
  if (!branchForkKeyFile || !branchesLog) return Layer.empty;
  return Layer.effectDiscard(
    Effect.serviceOption(SessionSandboxes).pipe(
      Effect.flatMap(Option.match({
        onNone: () => Effect.void,
        onSome: (sandboxes) => Effect.asVoid(Effect.forkScoped(Effect.provideService(serveBranchForks({ keyFile: branchForkKeyFile }), SessionSandboxes, sandboxes))),
      })),
    ),
  ).pipe(Layer.provide(branchesLog));
}

/**
 * In a branch that asks its parent to fork the sessions it inherited, its parent's answers
 * (./branch/fork.ts), handled by the environment's one member of its events' group
 * (EventsConsumer).
 */
function branchEvents({ branch, parentLog }: AlasioOptions): Layer.Layer<never, never, EventsConsumer | Store> {
  if (!branch || !parentLog) return Layer.empty;
  return Layer.effectDiscard(Effect.flatMap(EventsConsumer, (consumer) => consumer.handle({ "workspace.fork-answered": forkAnswered({ branch: branch.name, tokenFile: branch.tokenFile }) })));
}

/**
 * Codex's app-servers: the operator's (CodexAppServer), which folder workspaces' turns
 * run on, and where the deployment offers session filesystems, theirs (SessionFsCodex),
 * from a Codex home of alasio's own; that one is not among AlasioServices, and what uses
 * it asks whether it is.
 */
function codexServices({ sessionsGateway, stateDir }: AlasioOptions): Layer.Layer<CodexAppServer> {
  return sessionsGateway
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
