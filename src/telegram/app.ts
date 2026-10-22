/**
 * alasio serving Telegram: as it starts, what the last alasio left is settled (prompt jobs,
 * undelivered responses, albums, interrupted turns); then Telegram's updates are taken from
 * the environment's topic in the log (./update-consumer.ts) and processed, each once,
 * undelivered responses are looked for every thirty seconds, what the store keeps only in
 * flight is pruned daily, and Telegram is asked how its deliveries fare every five minutes
 * (./delivery-watch.ts). All of it stops as its scope
 * closes, consuming first. Telegram is told where to deliver, and what the bot's menu
 * offers, by alasio's command line, never by alasio: a start asks Telegram nothing but who
 * the bot is.
 */
import type { Update } from "@grammyjs/types";
import { randomUUID } from "node:crypto";

import { Duration, Effect, Fiber, FiberSet, Layer, Option, Schedule, type Scope } from "effect";

import { Turns } from "../codex/turns.ts";
import type { AlasioConfig } from "../config.ts";
import type { CommandError, OperatorServices } from "../operator/command-handler.ts";
import { Mounts } from "../operator/mounts.ts";
import type { StoreError } from "../persistence/sql.ts";
import { Store, type UpdateMarks } from "../persistence/store.ts";
import { SessionSandboxes } from "../sandbox/index.ts";
import { withLogScope } from "../shared/log.ts";
import { SpanKind, withAlasioSpan } from "../telemetry/index.ts";
import { Authorizer } from "./authorizer.ts";
import { takePress } from "./callback-handler.ts";
import { TelegramClient, type TelegramError } from "./client.ts";
import { watchDeliveries } from "./delivery-watch.ts";
import { MediaGroups } from "./media-group-buffer.ts";
import { handleMessage, processIncomingPrompt } from "./message-handler.ts";
import type { Outbox } from "./outbox.ts";
import { chatOfPress, holdWhileRunning, PRESS_AWAITED, PRESS_HOLD, type PressAnswers, PRESSES_DUE_EVERY, pressAnswers, recoverPresses } from "./presses.ts";
import { consumeUpdates, type TelegramUpdates } from "./update-consumer.ts";

/** How often completed responses that were never delivered are looked for. */
const COMPLETED_RESPONSE_RECOVERY = "30 seconds";

/** How often albums due are looked for, besides the wait of the alasio that kept their last message. */
const MEDIA_GROUPS_DUE_EVERY = "10 seconds";

/**
 * How long the store keeps what it holds only while in flight (updates processed, albums
 * handled, replies delivered) after it lands; conversations, their messages, files,
 * prompts, responses and the actions of buttons not yet pressed are kept.
 */
const TRANSIENT_RETENTION = "7 days";
/** How often what outlived TRANSIENT_RETENTION is pruned, by whichever alasio on the store looks when it is due. */
const PRUNE_EVERY = "1 day";
/** How often alasio looks whether pruning is due, which costs a read. */
const PRUNE_LOOKS_EVERY = "1 hour";

/** What the app is made with: alasio's configuration. */
export type TelegramAppConfig = Pick<AlasioConfig, "workspaceRoot" | "workingDirectory" | "defaultHarness">;

/** What the app runs on. */
export type TelegramAppServices = Authorizer | MediaGroups | Outbox | TelegramUpdates | OperatorServices;

/** How the app fails to start. */
export type TelegramAppError = TelegramError | CommandError | StoreError;

/**
 * The app, started in its layer's scope: what it starts with runs before it serves, and
 * what it runs (polling, recovery) is interrupted as the scope closes.
 */
export const serveTelegram = (config: TelegramAppConfig): Layer.Layer<never, TelegramAppError, TelegramAppServices> =>
  Layer.effectDiscard(startTelegram(config));

const startTelegram = Effect.fnUntraced(function*(config: TelegramAppConfig): Effect.fn.Return<
  void,
  TelegramAppError,
  TelegramAppServices | Scope.Scope
> {
  const client = yield* TelegramClient;
  const store = yield* Store;
  const turns = yield* Turns;
  const mediaGroups = yield* MediaGroups;
  const sandbox = Option.getOrNull(yield* Effect.serviceOption(SessionSandboxes));
  // What an update is processed with, given to it as the poller hands it over.
  const services = yield* Effect.context<TelegramAppServices>();
  // What an update leads to that outlasts it (a prompt, a button's control), stopped as the app stops.
  const handling = yield* FiberSet.make();

  /**
   * A chat's update is handled holding the chat's handling lock (Store.withHandlingLock),
   * which every alasio on the store takes for it, so one at a time handles its updates;
   * one that waits for it too long fails as the store does, and is taken again.
   * Each update starts a trace of its own, which everything it leads to joins: the
   * turn its prompt queues, however much later it runs, and the reply's delivery.
   * What it leads to runs on after it, so that consuming goes on meanwhile: a prompt's
   * turn, and what a press does past PRESS_AWAITED. One the store
   * has as processed already, handed over again after a restart, is skipped; one kept but
   * not processed is handled again. It fails where the store failed, keeping it, handling
   * it or marking it processed, so that it is taken again; Telegram failing its handling
   * is a defect, and the update is skipped (./update-consumer.ts).
   */
  const processUpdate = (update: Update): Effect.Effect<void, StoreError> =>
    Effect.suspend(() => {
      const chatId = update.message?.chat?.id ?? update.callback_query?.message?.chat?.id;
      // Its marks made in the lock's transaction, where it has a chat's: a lock lost rolls them back.
      const underLock = <A, E, R>(handle: (marks: UpdateMarks) => Effect.Effect<A, E, R>) => chatId === undefined ? handle(store) : store.withHandlingLock(chatId, handle);
      // A press kept as its update is marked processed, held, once that is committed, while it runs on.
      let kept: { readonly id: string; readonly holder: string; readonly pressing: Fiber.Fiber<void>; readonly answers: PressAnswers } | null = null;
      return underLock((marks) => Effect.gen(function*() {
        if (!(yield* marks.recordTelegramUpdate(update))) {
          return yield* Effect.logInfo(`Skipping update ${update.update_id}, processed already`);
        }
        const callbackQuery = update.callback_query;
        if (callbackQuery) {
          // Taken from the store before the update is marked processed, as a message is: the
          // store failing that fails the update, taken again. What the press then does is
          // awaited for PRESS_AWAITED, so a control that is quick, as most are, is done before
          // its update is told handled; one that takes longer, a steer or a workspace's, is
          // kept, as the update is marked processed, and runs on after it (./presses.ts), as a
          // prompt's turn does, so it holds back no later update for longer; its failing is said.
          const { handling: press, action } = yield* takePress(callbackQuery).pipe(Effect.provideContext(services));
          const answers = pressAnswers(callbackQuery);
          const pressing = yield* FiberSet.run(handling, answers.through(press).pipe(
            Effect.catch((error) => Effect.logError(`Failed to process callback ${callbackQuery.id}: ${error.message}`)),
            Effect.provideContext(services),
            Effect.interruptible,
          ));
          const done = yield* Fiber.await(pressing).pipe(Effect.timeoutOption(PRESS_AWAITED));
          if (Option.isNone(done) && action !== null && chatOfPress(callbackQuery) !== undefined) {
            const holder = randomUUID();
            yield* marks.keepPress({ callbackQuery, action, holder, holdMs: Duration.toMillis(PRESS_HOLD) });
            yield* answers.keep;
            kept = { id: callbackQuery.id, holder, pressing, answers };
          }
        } else if (update.message) {
          const prompt = yield* handleMessage(update.message, update.update_id, marks).pipe(
            Effect.provideContext(services),
            Effect.catchTag(["TelegramApiError", "TelegramTransportError"], Effect.die),
          );
          if (prompt) {
            yield* FiberSet.run(handling, processIncomingPrompt(prompt).pipe(Effect.provideContext(services), Effect.interruptible));
          }
        }
        yield* marks.markTelegramUpdateProcessed(update.update_id);
      })).pipe(
        // Once the update is through: the press held while it runs, and its query answered.
        Effect.andThen(Effect.suspend(() => {
          if (kept === null) return Effect.void;
          const { id, holder, pressing, answers } = kept;
          return Effect.asVoid(Effect.all([
            FiberSet.run(handling, holdWhileRunning(id, holder, pressing, answers.unsent).pipe(Effect.provideService(Store, store))),
            FiberSet.run(handling, answers.acknowledge.pipe(Effect.provideContext(services))),
          ]));
        })),
        Effect.tapError((error) => Effect.logError(`Failed to keep update ${update.update_id}: ${error}`)),
        withAlasioSpan("alasio.update", {
          kind: SpanKind.CONSUMER,
          parent: null,
          attributes: {
            "telegram.update.id": update.update_id,
            "telegram.update.type": update.callback_query ? "callback_query" : "message",
            ...(chatId === undefined ? {} : { "telegram.chat.id": String(chatId) }),
          },
        }),
      );
    });

  const me = yield* client.getMe;
  yield* Effect.logInfo(`Starting Telegram alasio bot as @${me.username ?? me.id}`);
  yield* Effect.logInfo(`  Workspace root: ${config.workspaceRoot}`);
  yield* Effect.logInfo(`  Default folder: ${config.workingDirectory ?? "none (operator chooses with /workspace)"}`);
  yield* Effect.logInfo(`  Default service: ${config.defaultHarness ?? "none (operator chooses with /service)"}`);
  yield* Effect.logInfo(`  Session filesystems: ${sandbox ? "enabled" : "off (the deployment renders no sessions template)"}`);
  yield* turns.reconcilePersistentState;
  // Before anything can make another, the session filesystems a crash left unmade go.
  yield* Effect.flatMap(Mounts, (mounts) => mounts.reconcileSessionWorkspaces);
  yield* turns.flushCompletedResponses;
  yield* mediaGroups.flushDue;
  yield* turns.recoverInterruptedTurns;
  yield* turns.resumePendingPrompts;
  // Presses kept as an alasio before stopped, run again before updates come.
  yield* recoverPresses(handling).pipe(Effect.provideContext(services));
  yield* consumeUpdates(processUpdate).pipe(Effect.forkScoped);
  // Responses completed but never delivered (their delivery failed, or alasio stopped first) go out.
  yield* turns.flushCompletedResponses.pipe(
    Effect.catch((error) => Effect.logWarning(`Completed response recovery failed: ${error.message}`)),
    Effect.catchDefect((defect) => Effect.logWarning(`Completed response recovery failed: ${defect instanceof Error ? defect.message : String(defect)}`)),
    Effect.schedule(Schedule.spaced(COMPLETED_RESPONSE_RECOVERY)),
    Effect.forkScoped,
  );
  // Albums due, which no alasio's wait is for (it stopped, or their chat's updates went to another).
  yield* mediaGroups.flushDue.pipe(Effect.schedule(Schedule.spaced(MEDIA_GROUPS_DUE_EVERY)), Effect.forkScoped);
  // Presses kept whose alasio let go of them, or crashed, run again.
  yield* recoverPresses(handling).pipe(Effect.provideContext(services), Effect.schedule(Schedule.spaced(PRESSES_DUE_EVERY)), Effect.forkScoped);
  // Pruned once a day, by whichever alasio looks when it is due: as one starts, and every hour one runs.
  yield* store.pruneTransientIfDue(TRANSIENT_RETENTION, PRUNE_EVERY).pipe(
    Effect.catch((error) => Effect.logWarning(`Pruning the store failed: ${error.message}`).pipe(Effect.as(false))),
    Effect.repeat(Schedule.spaced(PRUNE_LOOKS_EVERY)),
    Effect.forkScoped,
  );
  yield* watchDeliveries;
}, withLogScope("telegram-app"));
