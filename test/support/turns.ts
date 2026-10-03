/**
 * alasio's turns for a unit test: the Turns service, and the turn controller's façade over
 * it, on the test's store, a Telegram client the test records, and harnesses it stands
 * in, with ActiveTurns and the Harnesses of their own; built for one test and taken
 * down after it.
 */
import { Effect, Exit, Layer, Scope } from "effect";

import { CodexAppServer } from "../../src/codex/app-server/client.ts";
import { TurnController, type TurnControllerConfig, Turns } from "../../src/codex/turn-controller.ts";
import { ActiveTurns } from "../../src/harness/active-turns.ts";
import { type Harness, Harnesses } from "../../src/harness/index.ts";
import type { HarnessName } from "../../src/harness/names.ts";
import { type SqliteStore, Store } from "../../src/persistence/store.ts";
import { type EffectRunner, effectRunnerHere } from "../../src/shared/effects.ts";
import { type Client, TelegramClient, TelegramTransportError } from "../../src/telegram/client.ts";
import { Outbox } from "../../src/telegram/outbox.ts";
import { TracingLayer } from "../../src/telemetry/index.ts";
import { WorkflowHooks } from "../../src/workflow/hook-server.ts";

/** The Telegram calls turns make, as a test records them. */
export type TurnsClient = Pick<Client, "sendMessage" | "editMessageText">;

/** A TelegramClient making `client`'s calls; any other call fails the test as Telegram would refuse it. */
export function telegramClientOf(client: TurnsClient): TelegramClient["Service"] {
  const unused = (method: string) => Effect.fail(TelegramTransportError.of(method, new Error(`the test's Telegram client makes no ${method} call`)));
  const call = <A>(method: string, promise: () => Promise<A>) =>
    Effect.tryPromise({ try: promise, catch: (cause) => TelegramTransportError.of(method, cause) });
  return TelegramClient.of({
    call: (method) => unused(method),
    callMultipart: (method) => unused(method),
    getMe: unused("getMe"),
    deleteWebhook: () => unused("deleteWebhook"),
    setMyCommands: () => unused("setMyCommands"),
    setChatMenuButton: () => unused("setChatMenuButton"),
    getUpdates: () => unused("getUpdates"),
    sendMessage: (chatId, text, options) => call("sendMessage", () => client.sendMessage(chatId, text, options)),
    editMessageText: (chatId, messageId, text, options) => call("editMessageText", () => client.editMessageText(chatId, messageId, text, options)),
    deleteMessage: () => unused("deleteMessage"),
    answerCallbackQuery: () => unused("answerCallbackQuery"),
    getFile: () => unused("getFile"),
    downloadTelegramFile: () => unused("downloadTelegramFile"),
    sendDocument: () => unused("sendDocument"),
  });
}

/** An outbox that queues nothing, for turns whose replies a test does not follow. */
export const unusedOutbox: Layer.Layer<Outbox> = Layer.succeed(Outbox, Outbox.of({ enqueueText: () => Effect.succeed("outbox-1"), deliverDue: Effect.void }));

/** What turnsFor is given: the store, the Telegram client, the harnesses standing in, and what else a test changes. */
export interface TurnsOptions {
  readonly store: SqliteStore;
  readonly client: TurnsClient;
  readonly harnesses: Partial<Record<HarnessName, Harness>>;
  readonly config?: Partial<TurnControllerConfig> | undefined;
  /** The outbox replies are queued to: unusedOutbox unless given. */
  readonly outbox?: Layer.Layer<Outbox> | undefined;
}

/** The services the turn controller runs on in a test. */
export type TestTurnServices = Turns | ActiveTurns | Harnesses | Store | Outbox;

/** Turns, their façade, and the EffectRunner of their services, until `close`. */
export interface TestTurns {
  readonly turns: TurnController;
  readonly effects: EffectRunner<TestTurnServices>;
  readonly close: () => Promise<void>;
}

/**
 * alasio's turns over `options`, as alasio makes them (src/alasio.ts) but for what the test
 * stands in, with alasio's tracing.
 */
export async function turnsFor({ store, client, harnesses, config = {}, outbox = unusedOutbox }: TurnsOptions): Promise<TestTurns> {
  const scope = Effect.runSync(Scope.make());
  const layer = Turns.layer(config).pipe(
    Layer.provideMerge(Harnesses.layer({ overrides: harnesses })),
    Layer.provideMerge(Layer.mergeAll(
      ActiveTurns.layer,
      outbox,
      Layer.succeed(WorkflowHooks, WorkflowHooks.of({ port: 0, waits: new Map(), wakeEvents: new Map() })),
      // Started only by a turn on alasio's own Codex harness, which a test stands in for.
      CodexAppServer.layer,
    )),
    Layer.provideMerge(Layer.mergeAll(Layer.succeed(Store, store), Layer.succeed(TelegramClient, telegramClientOf(client)))),
  );
  const effects = await Effect.runPromise(Layer.buildWithScope(layer, scope).pipe(Effect.flatMap(effectRunnerHere), Effect.provide(TracingLayer)));
  const turns = new TurnController({ config: { workspaceRoot: "/nonexistent-workspace-root", ...config }, client, store, effects });
  return { turns, effects, close: () => Effect.runPromise(Scope.close(scope, Exit.void)) };
}

/** Runs `use` on turnsFor(`options`), and takes them down after, however it ends. */
export async function withTurns<T>(options: TurnsOptions, use: (turns: TestTurns) => T | Promise<T>): Promise<T> {
  const made = await turnsFor(options);
  try {
    return await use(made);
  } finally {
    await made.close();
  }
}
