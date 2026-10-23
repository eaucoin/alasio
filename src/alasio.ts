/**
 * alasio assembled: the services it runs on, and the app serving Telegram over them.
 * src/main.ts runs it in production, as ./shared/run.ts runs a process,
 * test/support/alasio-main.ts against the tests' stand-ins.
 */
import { Effect, Layer, Option, type Scope } from "effect";
import type { Pool } from "pg";

import { FORK_EVENT_KINDS, ForkAsks, forkAnswered, ForkRequests, parentForks, serveBranchForks } from "./branch/fork.ts";
import { CodexAppServer } from "./codex/app-server/client.ts";
import { CodexLoginFreshness } from "./codex/login-refresh.ts";
import { codexHome } from "./codex/env.ts";
import { type CodexLoginError, keepCodexLogin } from "./codex/login.ts";
import type { NeonRolloutStore } from "./codex/rollouts/store.ts";
import { Turns } from "./codex/turns.ts";
import type { AlasioConfig, BranchEnvironment } from "./config.ts";
import { type EnvironmentEvents, EventsConsumer } from "./events/consume.ts";
import type { Events } from "./events/events.ts";
import type { NeonSessionStore } from "./harness/claude/session-store.ts";
import type { HarnessHost } from "./harness/host.ts";
import { HarnessEvents } from "./harness/host-events.ts";
import { Panels } from "./harness/panels.ts";
import { pooledHarnessHost } from "./harness/pooled-host.ts";
import { BusyPods } from "./kube/busy.ts";
import { KubeClient } from "./kube/client.ts";
import { recordStart } from "./kube/started.ts";
import { HostBayma } from "./mcp/bayma.ts";
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
import { answeringClient, WakeLatency } from "./telegram/wake-latency.ts";
import type { TelegramUpdates } from "./telegram/update-consumer.ts";

/** What alasio is made with: its configuration, its Neon and what main keeps there, and where its updates come from. */
export interface AlasioOptions extends Omit<AlasioConfig, "telegramLog" | "log" | "events" | "lakeWake">, TelegramAppConfig {
  /** Telegram's updates: its environment's topic in the log (TelegramUpdates.layer), or a stand-in's in alasio's tests. */
  readonly updates: Layer.Layer<TelegramUpdates>;
  /**
   * In a branch, what it asks its parent to fork the sessions it inherited with (ForkAsks), on
   * the log, or a stand-in in tests; a branch given none inherits no session.
   */
  readonly parentLog?: Layer.Layer<ForkAsks> | undefined;
  /**
   * The environment's events, read in its group (EnvironmentEvents): what its harness pools'
   * hosts and the sandbox gateway tell of them, and, in a branch, its parent's answers to its
   * forks; on the log, or a stand-in in tests.
   */
  readonly events: Layer.Layer<EnvironmentEvents>;
  /** Where alasio forks its sessions for branches, the requests they make (ForkRequests) and what it answers on (Events), on the log, or stand-ins in tests. */
  readonly branchesLog?: Layer.Layer<ForkRequests | Events> | undefined;
  /** The pool of alasio's Neon database, which its state is kept in. */
  readonly pool: Pool;
  /** The schema alasio's state is kept in: `state`, unless a test gives one of its own. */
  readonly stateSchema?: string | undefined;
  /** What Neon keeps of the harnesses' sessions, which the operator's panels read: Claude Code's transcripts, and Codex's rollouts, the operator's home's and the session filesystems'. */
  readonly sessionStore: NeonSessionStore;
  readonly rolloutStores: { readonly codex: NeonRolloutStore; readonly sessionFs: NeonRolloutStore };
  /** Tells the lake's loader of what a harness pool's pod committed, which tells it nothing itself (../neon/connect.ts ConnectedNeon.written); none where nothing loads it. */
  readonly lakeWritten?: (() => void) | undefined;
  /** The harness host's role, which Neon has (../neon/harness.ts), granted its part of the state. */
  readonly harnessRole: string;
}

/** The services alasio's app runs on, which are always there. */
export type AlasioServices =
  | Store
  | ReceivedFiles
  | TelegramClient
  | Outbox
  | CodexAppServer
  | HarnessHost
  | HarnessEvents
  | Panels
  | Turns
  | Mounts
  | Authorizer
  | MediaGroups;

/**
 * alasio's services, made for `options`: each made after what it runs on, and stopped
 * before it. The turns are made after the harnesses' host, Telegram and the store they report
 * to, so that stopping alasio interrupts the turns it drives while those are still there. The
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
    Layer.provideMerge(Turns.layer({ pod: options.pod })),
    Layer.provideMerge(Panels.layer({ sessionStore: options.sessionStore, codexRollouts: options.rolloutStores.codex, sessionFsCodexRollouts: options.rolloutStores.sessionFs, stateDir: options.stateDir, branch: options.branch?.name })),
    Layer.provideMerge(harnessHost(options)),
    // After the session filesystems, whose sessions' files it names.
    Layer.provideMerge(ReceivedFiles.layer(options)),
    Layer.provideMerge(Layer.mergeAll(
      Outbox.layer,
      // Before Codex is first started, which is not before a turn needs it.
      options.keepCodexLogin ? Layer.effectDiscard(keepCodexLogin(codexHome())) : Layer.empty,
      workspaceServices(options),
      busyPods(options),
      recordStart(options.inKubernetes),
    )),
    // Of alasio's own app-server, which refreshes it, and its store, which the alasios on it share it by.
    Layer.provideMerge(CodexLoginFreshness.layer(codexHome())),
    Layer.provideMerge(CodexAppServer.layer),
    Layer.provideMerge(EventsConsumer.layer.pipe(Layer.provide(options.events))),
    Layer.provideMerge(Layer.mergeAll(
      stateStore(options),
      // What it sends at once told the start's timing, as an answer is, beside what the outbox queues.
      answeringClient.pipe(Layer.provide(TelegramClient.layer(options.telegramBotToken))),
    )),
    // Below all: the outbox tells it what it queues, the Telegram client what it sends at once, and the app the updates it handles.
    Layer.provideMerge(WakeLatency.layer),
  )));
}

/**
 * alasio's store, its state made as it opens: its tables, a branch's state its own, and the
 * harness host's grants; as alasio starts, and as `alasio setup` makes it before (./setup/program.ts).
 */
export const stateStore = (options: Pick<AlasioOptions, "pool" | "stateSchema" | "workingDirectory" | "branch" | "harnessRole">): Layer.Layer<Store, StoreError> =>
  Store.layer({ pool: options.pool, schema: options.stateSchema, workingDirectory: options.workingDirectory, branch: options.branch?.name, harnessRole: options.harnessRole });

/**
 * The host alasio's turns run on, each conversation's, bound in its harness's pool through the
 * sandbox gateway, and where it hears what their harnesses do on their own, on its environment's
 * events topic; its Codex turns given tokens for its environment's relay of its Codex login
 * (./codex/relay.ts), a component of its own, which no pod holds the login past.
 */
function harnessHost({ codexLoginUrl, sessionsGateway, lakeWritten }: AlasioOptions): Layer.Layer<HarnessHost | HarnessEvents, never, Store | EventsConsumer | CodexLoginFreshness> {
  return Layer.mergeAll(
    pooledHarnessHost({ ...sessionsGateway, codexLogin: { url: codexLoginUrl, home: codexHome() }, written: lakeWritten }),
    HarnessEvents.fromLog({ written: lakeWritten }),
  );
}

/**
 * The services of the workspaces the deployment offers: session filesystems (SessionSandboxes),
 * through the sandbox gateway, and folder workspaces' bayma (HostBayma), the gateway's folder
 * instances, where it serves them; both reached at one loopback of alasio's (BaymaProxy). They
 * are not among AlasioServices, which are always there; what uses them asks whether they are.
 */
function workspaceServices({ folders, sessionsGateway, stateDir, branch, parentLog }: AlasioOptions): Layer.Layer<never, never, Store | TelegramClient> {
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
 * What holds alasio's own pod busy (./kube/busy.ts) where alasio runs in Kubernetes, on a
 * KubeClient of its own: the turns it drives in its harness pools (./harness/pooled-host.ts);
 * elsewhere nothing. The pods its harnesses and workspaces run in are the gateway's, which
 * holds each busy as it is bound.
 */
function busyPods({ inKubernetes }: AlasioOptions): Layer.Layer<never> | Layer.Layer<BusyPods> {
  if (!inKubernetes) return Layer.empty;
  return Layer.orDie(BusyPods.layer({})).pipe(Layer.provide(KubeClient.layer));
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
  return Layer.effectDiscard(Effect.flatMap(EventsConsumer, (consumer) => consumer.handle({ "workspace.fork-answered": forkAnswered({ branch: branch.name, tokenFile: branch.tokenFile }) } satisfies { readonly [K in (typeof FORK_EVENT_KINDS)[number]]: unknown })));
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
