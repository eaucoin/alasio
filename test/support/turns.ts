/**
 * alasio's services for a unit test: the turns and the operator's side, as alasio makes
 * them (src/alasio.ts), on the test's store, a Telegram client the test records, and
 * harnesses it stands in, with alasio's tracing; built for one test and taken down after
 * it.
 */
import { Effect, Layer, ManagedRuntime } from "effect";

import { CodexAppServer } from "../../src/codex/app-server/client.ts";
import { Turns } from "../../src/codex/turns.ts";
import { ActiveTurns } from "../../src/harness/active-turns.ts";
import { HarnessHost } from "../../src/harness/host.ts";
import { HostEvents } from "../../src/harness/host-events.ts";
import { type Harness, Harnesses } from "../../src/harness/index.ts";
import type { HarnessName } from "../../src/harness/names.ts";
import { HostBayma } from "../../src/mcp/bayma.ts";
import { Mounts } from "../../src/operator/mounts.ts";
import { inlineKeyboard } from "../../src/operator/panel.ts";
import { StoreError } from "../../src/persistence/sql.ts";
import { Store } from "../../src/persistence/store.ts";
import type { AlasioServices } from "../../src/alasio.ts";
import { SessionSandboxes } from "../../src/sandbox/index.ts";
import { Authorizer } from "../../src/telegram/authorizer.ts";
import { ReceivedFiles } from "../../src/telegram/files.ts";
import type { TelegramClient } from "../../src/telegram/client.ts";
import { MediaGroups } from "../../src/telegram/media-group-buffer.ts";
import { Outbox, type OutboxStatus, type OutboxText } from "../../src/telegram/outbox.ts";
import { TracingLayer } from "../../src/telemetry/index.ts";
import { recordingTelegram } from "./telegram-calls.ts";

/** An outbox that queues nothing, for turns whose replies a test does not follow. */
const unusedOutbox: Layer.Layer<Outbox> = Layer.succeed(Outbox, Outbox.of({
  enqueueText: () => Effect.succeed("outbox-1"),
  enqueuePanel: () => Effect.succeed("outbox-panel-1"),
  enqueueNotice: () => Effect.succeed(true),
  enqueueStatus: () => Effect.succeed("outbox-status-1"),
  enqueueEdit: () => Effect.succeed("outbox-edit-1"),
  deliverDue: Effect.void,
  deliverChat: () => Effect.void,
}));

/** What a recording outbox was given to queue: replies, status messages, and edits of them, each with the id it answered. */
export interface QueuedToOutbox {
  readonly texts: (OutboxText & { readonly id: string })[];
  readonly statuses: (OutboxStatus & { readonly id: string })[];
  readonly edits: (OutboxStatus & { readonly edits: string })[];
}

/**
 * An outbox that delivers nothing and records what it is given to queue, in `queued`: a
 * reply with buttons among its replies, its buttons' actions kept in the store, as the
 * outbox keeps them. The first `failedPanels` replies with buttons fail, keeping nothing,
 * as the store failing them would, and so does each reply `refusesText` says to. A notice is
 * recorded as told in the store, as the outbox records it, and among the replies where it
 * is queued now.
 */
export function recordingOutbox({ failedPanels = 0, refusesText = () => false }: {
  readonly failedPanels?: number;
  readonly refusesText?: (text: OutboxText) => boolean;
} = {}): { readonly layer: Layer.Layer<Outbox, never, Store>; readonly queued: QueuedToOutbox } {
  const queued: QueuedToOutbox = { texts: [], statuses: [], edits: [] };
  let ids = 0;
  let panels = 0;
  const id = () => `outbox-${(ids += 1)}`;
  const record = (text: OutboxText) => Effect.sync(() => queued.texts[queued.texts.push({ ...text, id: id() }) - 1]!.id);
  return {
    queued,
    layer: Layer.effect(Outbox, Effect.map(Effect.service(Store), (store) => Outbox.of({
      enqueueText: (text) => (refusesText(text) ? Effect.fail(new StoreError({ cause: new Error("the store is down") })) : record(text)),
      enqueuePanel: ({ conversationId, chatId, text, keyboard }) =>
        (panels += 1) <= failedPanels
          ? Effect.fail(new StoreError({ cause: new Error("the store is down") }))
          : Effect.flatMap(store.createCallbackActions(conversationId, keyboard.flat()), (actionIds) => record({ chatId, text, options: { reply_markup: inlineKeyboard(keyboard, actionIds) } })),
      enqueueNotice: ({ conversationId, chatId, text }) =>
        refusesText({ chatId, text })
          ? Effect.fail(new StoreError({ cause: new Error("the store is down") }))
          : Effect.flatMap(store.enqueueOutboxNotice(conversationId, { chatId, text }), (fresh) => (fresh ? Effect.as(record({ chatId, text }), true) : Effect.succeed(false))),
      enqueueStatus: (status) => Effect.sync(() => queued.statuses[queued.statuses.push({ ...status, id: id() }) - 1]!.id),
      enqueueEdit: (edits, edit) => Effect.sync(() => (queued.edits.push({ ...edit, edits }), id())),
      deliverDue: Effect.void,
      deliverChat: () => Effect.void,
    }))),
  };
}

/** Turns standing in for alasio's, each of its effects dying unless `turns` gives it, as a test expects none of the rest. */
export function turnsStub(turns: Partial<Turns["Service"]>): Turns["Service"] {
  const unused = (name: string) => Effect.die(new Error(`the test's turns run no ${name}`));
  return Turns.of({
    submit: () => unused("submit"),
    schedule: () => unused("schedule"),
    setPromptDisposition: () => unused("setPromptDisposition"),
    run: () => unused("run"),
    runGoalTurn: () => unused("runGoalTurn"),
    startNewSession: () => unused("startNewSession"),
    reconcilePersistentState: unused("reconcilePersistentState"),
    flushCompletedResponses: unused("flushCompletedResponses"),
    recoverInterruptedTurns: unused("recoverInterruptedTurns"),
    resumePendingPrompts: unused("resumePendingPrompts"),
    ...turns,
  });
}

/** What the test's services are made with: the store, and what the test stands in or changes. */
export interface TestServicesOptions {
  readonly store: Store["Service"];
  /** The Telegram client: one recording every call unless given. */
  readonly telegram?: Layer.Layer<TelegramClient> | undefined;
  /** Stand-ins for a harness in every folder. */
  readonly harnesses?: Partial<Record<HarnessName, Harness>> | undefined;
  /** Stands in for alasio's turns. */
  readonly turns?: Turns["Service"] | undefined;
  readonly workspaceRoot?: string | undefined;
  /** Where a reply's media are copied, and received files written; replies go as text only without it. */
  readonly stateDir?: string | undefined;
  readonly allowedUserIds?: string | undefined;
  /** The outbox replies are queued to: unusedOutbox unless given. */
  readonly outbox?: Layer.Layer<Outbox, never, Store> | undefined;
  /** Session filesystems, where a test offers them. */
  readonly sandbox?: SessionSandboxes["Service"] | undefined;
  /** Folder workspaces' bayma, the gateway's folder instances, where a test offers them. */
  readonly hostBayma?: HostBayma["Service"] | undefined;
  /** The branch environment alasio is, if it is one. */
  readonly branch?: string | undefined;
  /** What the harness host is made into, for a test that stands in for part of it: as made unless given. */
  readonly host?: ((host: HarnessHost["Service"]) => HarnessHost["Service"]) | undefined;
}

/** alasio's services over `options`, as src/alasio.ts makes them but for what the test stands in. */
function testServices({
  store,
  telegram = recordingTelegram().layer,
  harnesses = {},
  turns,
  workspaceRoot = "/nonexistent-workspace-root",
  stateDir,
  allowedUserIds = "",
  outbox = unusedOutbox,
  sandbox,
  hostBayma,
  branch,
  host,
}: TestServicesOptions): Layer.Layer<AlasioServices> {
  return MediaGroups.layer().pipe(
    Layer.provideMerge(Layer.mergeAll(Mounts.layer({ workspaceRoot, branch }), Authorizer.layer(allowedUserIds))),
    Layer.provideMerge(turns ? Layer.succeed(Turns, turns) : Turns.layer()),
    Layer.provideMerge(host ? Layer.effect(HarnessHost, Effect.map(HarnessHost, host)).pipe(Layer.provide(HarnessHost.layer)) : HarnessHost.layer),
    Layer.provideMerge(Harnesses.layer({ overrides: harnesses, branch })),
    // Received files are written nowhere a test does not say; into a session through the test's session filesystems.
    Layer.provideMerge(ReceivedFiles.layer({ stateDir: stateDir ?? "/nonexistent-state-dir" })),
    Layer.provideMerge(HostEvents.inProcess),
    Layer.provideMerge(Layer.mergeAll(
      ActiveTurns.layer,
      outbox,
      // Started only by a turn on alasio's own Codex harness, which a test stands in for.
      CodexAppServer.layer,
      sandbox ? Layer.succeed(SessionSandboxes, sandbox) : Layer.empty,
      hostBayma ? Layer.succeed(HostBayma, hostBayma) : Layer.empty,
    )),
    Layer.provideMerge(Layer.mergeAll(Layer.succeed(Store, store), telegram)),
  );
}

/** alasio's services for one test, which runs its effects in them. */
export type TestAlasio = ManagedRuntime.ManagedRuntime<AlasioServices | Layer.Success<typeof TracingLayer>, never>;

/**
 * Runs `use` on alasio's services over `options`, and takes them down after, however it
 * ends. They are made with alasio's tracing, as their background work runs with it too.
 */
export async function withServices<T>(options: TestServicesOptions, use: (alasio: TestAlasio) => T | Promise<T>): Promise<T> {
  const alasio = ManagedRuntime.make(testServices(options).pipe(Layer.provideMerge(TracingLayer)));
  try {
    return await use(alasio);
  } finally {
    await alasio.dispose();
  }
}
