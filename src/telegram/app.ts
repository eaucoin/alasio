/**
 * alasio serving Telegram: as it starts, Telegram's updates are taken from the environment's
 * topic in the log (./update-consumer.ts) and processed, each once, as soon as what their
 * handling needs is done, and what the last alasio left is settled as they are (prompt jobs,
 * undelivered responses, albums, interrupted turns, each conversation's under its claim);
 * undelivered responses are looked for every thirty seconds, what the store keeps only in
 * flight is pruned daily, and Telegram is asked how its deliveries fare every five minutes
 * (./delivery-watch.ts). All of it stops as its scope
 * closes, consuming first. Telegram is told where to deliver, and what the bot's menu
 * offers, by alasio's command line, never by alasio: a start asks Telegram nothing but who
 * the bot is.
 */
import type { Update } from "@grammyjs/types";

import { Effect, Fiber, FiberSet, Layer, Option, Schedule, type Scope } from "effect";

import { Turns } from "../codex/turns.ts";
import { WORKER_SWEEP_EVERY } from "../codex/worker-claims.ts";
import type { AlasioConfig } from "../config.ts";
import type { CommandError, OperatorServices } from "../operator/command-handler.ts";
import { admitPrompt, runCommand } from "../operator/prompts.ts";
import { Mounts } from "../operator/mounts.ts";
import type { StoreError } from "../persistence/sql.ts";
import { Store, type UpdateMarks } from "../persistence/store.ts";
import { SessionSandboxes } from "../sandbox/index.ts";
import { withLogScope } from "../shared/log.ts";
import { currentTraceparent, SpanKind, withAlasioSpan } from "../telemetry/index.ts";
import { Authorizer } from "./authorizer.ts";
import { takePress } from "./callback-handler.ts";
import { TelegramClient, type TelegramError } from "./client.ts";
import { watchDeliveries } from "./delivery-watch.ts";
import { drain, stopToldAt } from "./drain.ts";
import { MediaGroups } from "./media-group-buffer.ts";
import { handleMessage } from "./message-handler.ts";
import type { Outbox } from "./outbox.ts";
import { type Awaited, awaitOrKeep, chatOfPress, CONTROL_AWAITED, CONTROLS_DUE_EVERY, holdWhileRunning, type KeptRunning, pressAnswers, recoverControls, rolledBack } from "./controls.ts";
import { consumeUpdates, type TelegramUpdates } from "./update-consumer.ts";
import { WakeLatency } from "./wake-latency.ts";

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
  // What times the start's first answer, where alasio's is given (./wake-latency.ts).
  const wake = Option.getOrNull(yield* Effect.serviceOption(WakeLatency));
  // What an update is processed with, given to it as the poller hands it over.
  const services = yield* Effect.context<TelegramAppServices>();
  // What an update leads to that outlasts it (a prompt's job carried on, a control), stopped as the app stops.
  const handling = yield* FiberSet.make();

  /**
   * A chat's update is handled holding the chat's handling lock (Store.withHandlingLock),
   * which every alasio on the store takes for it, so one at a time handles its updates;
   * one that waits for it too long fails as the store does, and is taken again.
   * Each update starts a trace of its own, which everything it leads to joins: the
   * turn its prompt queues, however much later it runs, and the reply's delivery.
   * It is told handled, its marks committed with the lock's transaction, only once what it
   * asks for is kept with them: its prompt's job queued, the setup step it lacks sent, or
   * its control, a press or a command, done or kept to run on (./controls.ts). What it
   * leads to runs on after it, so that consuming goes on meanwhile: a prompt's turn, and
   * what a control does past CONTROL_AWAITED; so a stop that cuts it short loses nothing of
   * it, which is either taken again or kept. One the store has as processed already, handed
   * over again after a restart, is skipped; one kept but not processed is handled again. It
   * fails where the store failed, keeping it, handling it or marking it processed, so that
   * it is taken again; Telegram failing its handling is a defect, and the update is skipped
   * (./update-consumer.ts).
   */
  const processUpdate = (update: Update): Effect.Effect<void, StoreError> =>
    Effect.suspend(() => {
      const chatId = update.message?.chat?.id ?? update.callback_query?.message?.chat?.id;
      // Its marks made in the lock's transaction, where it has a chat's: a lock lost rolls them back.
      const underLock = <A, E, R>(handle: (marks: UpdateMarks) => Effect.Effect<A, E, R>) => chatId === undefined ? handle(store) : store.withHandlingLock(chatId, handle);
      // What it leads to once its transaction is through: a control kept, held while it runs, a
      // kept press's query answered, a prompt's job carried on.
      let after: Effect.Effect<void> = Effect.void;
      // Its command's work, cut short where its transaction fails after all, keeping nothing of it (rolledBack).
      const awaited: Awaited[] = [];
      const holding = (kept: KeptRunning, unsent?: () => readonly string[]) =>
        FiberSet.run(handling, holdWhileRunning(kept.id, kept.holder, kept.running, unsent).pipe(Effect.provideService(Store, store)));
      return underLock((marks) => Effect.gen(function*() {
        if (!(yield* marks.recordTelegramUpdate(update))) {
          return yield* Effect.logInfo(`Skipping update ${update.update_id}, processed already`);
        }
        if (wake) yield* wake.handling(update, currentTraceparent());
        const callbackQuery = update.callback_query;
        if (callbackQuery) {
          // Taken from the store before the update is marked processed, as a message is: the
          // store failing that fails the update, taken again. What the press then does is
          // awaited for CONTROL_AWAITED, so a control that is quick, as most are, is done before
          // its update is told handled; one that takes longer, a steer or a workspace's, is
          // kept, as the update is marked processed, and runs on after it (./controls.ts), as a
          // prompt's turn does, so it holds back no later update for longer; its failing is said.
          const { handling: press, action } = yield* takePress(callbackQuery).pipe(Effect.provideContext(services));
          const answers = pressAnswers(callbackQuery);
          const work = answers.through(press).pipe(
            Effect.catch((error) => Effect.logError(`Failed to process callback ${callbackQuery.id}: ${error.message}`)),
            Effect.provideContext(services),
          );
          if (action === null || chatOfPress(callbackQuery) === undefined) {
            // Nothing to run again from, or no chat to answer in: awaited as long, kept never.
            const running = yield* FiberSet.run(handling, Effect.interruptible(work));
            yield* Fiber.await(running).pipe(Effect.timeoutOption(CONTROL_AWAITED));
          } else {
            // Not among the awaited the transaction's failing cuts short, as a command is: its action consumed already (rolledBack).
            const { kept } = yield* awaitOrKeep(handling, { kind: "press", callbackQuery, action }, work, marks, answers.keep);
            if (Option.isSome(kept)) after = Effect.asVoid(Effect.all([holding(kept.value, answers.unsent), FiberSet.run(handling, answers.acknowledge.pipe(Effect.provideContext(services)))]));
          }
        } else if (update.message) {
          const prompt = yield* handleMessage(update.message, update.update_id, marks).pipe(
            Effect.provideContext(services),
            Effect.catchTag(["TelegramApiError", "TelegramTransportError"], Effect.die),
          );
          const admitted = prompt === null ? null : yield* admitPrompt(prompt, marks).pipe(
            Effect.provideContext(services),
            Effect.catchTag(["TelegramApiError", "TelegramTransportError"], Effect.die),
          );
          if (admitted?._tag === "Command") {
            const { kept } = awaited[awaited.push(yield* awaitOrKeep(handling, admitted.command, runCommand(admitted.command).pipe(Effect.provideContext(services)), marks)) - 1]!;
            if (Option.isSome(kept)) after = Effect.asVoid(holding(kept.value));
          } else if (admitted?._tag === "Queued") {
            const { job } = admitted;
            after = Effect.asVoid(FiberSet.run(handling, turns.admitted(job).pipe(
              Effect.catch((error) => Effect.logError(`Could not carry on ${job.conversation_id}'s prompt ${job.id}, which waits for what takes up waiting prompts: ${error.message}`)),
            )));
          }
        }
        yield* marks.markTelegramUpdateProcessed(update.update_id);
      })).pipe(
        Effect.onError(() => rolledBack(awaited)),
        Effect.andThen(Effect.suspend(() => after)),
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
  // Updates are taken first of all but what their handling needs done before, as alasio woken by an update answers it
  // as soon as it can, a press's query before Telegram gives up on it:
  // - the claims of the processes of this pod before this one let go of, as a container restarted in it leaves them, so
  //   a conversation of theirs is taken at once;
  // - the controls kept as an alasio before stopped run again, forked, as before: a head start on their chats' later
  //   updates, not a guarantee they end first;
  // - responses completed but never delivered, and albums due, queued, so each goes out before the reply to an update of
  //   its chat taken after: one statement each, which finds none as a rule.
  yield* turns.lapseOwnClaims;
  yield* recoverControls(handling).pipe(Effect.provideContext(services));
  yield* turns.flushCompletedResponses;
  yield* mediaGroups.flushDue;
  // As alasio stops: its member of the Telegram group, forked after, gone first, then what it
  // owes delivered (./drain.ts), within the grace counted from when it was told to stop.
  const toldAt = yield* stopToldAt;
  yield* Effect.addFinalizer(() => drain({ toldAt }).pipe(Effect.provideContext(services)));
  yield* consumeUpdates(processUpdate).pipe(Effect.forkScoped);
  // Then, as updates are taken: the session filesystems the gateway holds set against those the store records, and
  // where they differ said, which asks the gateway, forked, a failure of it logged and alasio started all the same; the
  // conversations with work left that no alasio drives, as theirs stopped or crashed, taken up, what each left recovered
  // under its claim, which an update of its conversation waits for and no other's (../codex/worker-claims.ts), its turn
  // running on its pod attached to again in the background (Turns.takeUnheld); and the prompts waiting, to run or to be
  // asked about again.
  yield* Effect.flatMap(Mounts, (mounts) => mounts.reconcileSessionWorkspaces).pipe(
    Effect.catch((error) => Effect.logWarning(`Setting the session filesystems the gateway holds against the store's failed: ${error.message}`)),
    Effect.forkScoped,
  );
  yield* turns.takeUnheld;
  yield* turns.resumePendingPrompts;
  // Responses completed but never delivered (their delivery failed, or alasio stopped first) go out.
  yield* turns.flushCompletedResponses.pipe(
    Effect.catch((error) => Effect.logWarning(`Completed response recovery failed: ${error.message}`)),
    Effect.catchDefect((defect) => Effect.logWarning(`Completed response recovery failed: ${defect instanceof Error ? defect.message : String(defect)}`)),
    Effect.schedule(Schedule.spaced(COMPLETED_RESPONSE_RECOVERY)),
    Effect.forkScoped,
  );
  // The conversations whose worker's claims lapsed since, as another alasio crashed, taken up.
  yield* turns.takeUnheld.pipe(
    Effect.catch((error) => Effect.logWarning(`Taking up the conversations no alasio drives failed: ${error.message}`)),
    Effect.schedule(Schedule.spaced(WORKER_SWEEP_EVERY)),
    Effect.forkScoped,
  );
  // Albums due, which no alasio's wait is for (it stopped, or their chat's updates went to another).
  yield* mediaGroups.flushDue.pipe(Effect.schedule(Schedule.spaced(MEDIA_GROUPS_DUE_EVERY)), Effect.forkScoped);
  // Controls kept whose alasio let go of them, or crashed, run again.
  yield* recoverControls(handling).pipe(Effect.provideContext(services), Effect.schedule(Schedule.spaced(CONTROLS_DUE_EVERY)), Effect.forkScoped);
  // Pruned once a day, by whichever alasio looks when it is due: as one starts, and every hour one runs.
  yield* store.pruneTransientIfDue(TRANSIENT_RETENTION, PRUNE_EVERY).pipe(
    Effect.catch((error) => Effect.logWarning(`Pruning the store failed: ${error.message}`).pipe(Effect.as(false))),
    Effect.repeat(Schedule.spaced(PRUNE_LOOKS_EVERY)),
    Effect.forkScoped,
  );
  yield* watchDeliveries;
}, withLogScope("telegram-app"));
