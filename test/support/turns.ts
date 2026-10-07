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
import { type Harness, Harnesses } from "../../src/harness/index.ts";
import type { HarnessName } from "../../src/harness/names.ts";
import { Mounts } from "../../src/operator/mounts.ts";
import { Store } from "../../src/persistence/store.ts";
import type { AlasioServices } from "../../src/alasio.ts";
import { SessionSandboxes } from "../../src/sandbox/index.ts";
import { Authorizer } from "../../src/telegram/authorizer.ts";
import { ReceivedFiles } from "../../src/telegram/files.ts";
import type { TelegramClient } from "../../src/telegram/client.ts";
import { MediaGroups } from "../../src/telegram/media-group-buffer.ts";
import { Outbox } from "../../src/telegram/outbox.ts";
import { TracingLayer } from "../../src/telemetry/index.ts";
import { WorkflowHooks } from "../../src/workflow/hook-server.ts";
import { recordingTelegram } from "./telegram-calls.ts";

/** An outbox that queues nothing, for turns whose replies a test does not follow. */
const unusedOutbox: Layer.Layer<Outbox> = Layer.succeed(Outbox, Outbox.of({ enqueueText: () => Effect.succeed("outbox-1"), deliverDue: Effect.void }));

/** No workflow hook server: no workflow waits are reported. */
export const noWorkflowHooks: Layer.Layer<WorkflowHooks> = Layer.succeed(WorkflowHooks, WorkflowHooks.of({ port: 0, waits: new Map() }));

/** Turns standing in for alasio's, each of its effects dying unless `turns` gives it, as a test expects none of the rest. */
export function turnsStub(turns: Partial<Turns["Service"]>): Turns["Service"] {
  const unused = (name: string) => Effect.die(new Error(`the test's turns run no ${name}`));
  return Turns.of({
    submit: () => unused("submit"),
    enqueueMessage: () => unused("enqueueMessage"),
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
  readonly outbox?: Layer.Layer<Outbox> | undefined;
  /** Session filesystems, where a test offers them. */
  readonly sandbox?: SessionSandboxes["Service"] | undefined;
  /** The branch environment alasio is, if it is one. */
  readonly branch?: string | undefined;
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
  branch,
}: TestServicesOptions): Layer.Layer<AlasioServices> {
  return MediaGroups.layer().pipe(
    Layer.provideMerge(Layer.mergeAll(Mounts.layer({ workspaceRoot }), Authorizer.layer(allowedUserIds))),
    Layer.provideMerge(turns ? Layer.succeed(Turns, turns) : Turns.layer()),
    Layer.provideMerge(Harnesses.layer({ overrides: harnesses, branch })),
    Layer.provideMerge(Layer.mergeAll(
      ActiveTurns.layer,
      // Received files are written nowhere a test does not say.
      ReceivedFiles.layer({ stateDir: stateDir ?? "/nonexistent-state-dir" }),
      outbox,
      noWorkflowHooks,
      // Started only by a turn on alasio's own Codex harness, which a test stands in for.
      CodexAppServer.layer,
      sandbox ? Layer.succeed(SessionSandboxes, sandbox) : Layer.empty,
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
