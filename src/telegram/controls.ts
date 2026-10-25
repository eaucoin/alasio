/**
 * Controls whose work runs on past their updates: presses of alasio's buttons, and the
 * operator's commands. An update awaits what its control does for CONTROL_AWAITED (./app.ts):
 * a control quick enough, as most are, is done before its update is told handled. One still
 * running then is kept in the store, in the update's transaction, so the update is told
 * handled only once its control is kept; it is held by the alasio running it, by a token of
 * its own, which renews the hold as the control runs, forgets the control once it is done,
 * and lets go of it as alasio stops. A control let go of, or whose hold lapsed as its alasio
 * crashed, is run again from what it is by whichever alasio looks for controls next
 * (recoverControls), as alasio starts and every CONTROLS_DUE_EVERY: so each kept control runs
 * to its end at least once, and one cut short runs again from its start.
 *
 * A kept press's answers go through the outbox from then on, as replies in its chat, which
 * are sent once whichever alasio sends them, the query itself answered "Working..." once the
 * update is through, where nothing answered it before; an answer the store does not take is
 * kept with its press, once its control is done, for the next look to queue, its control never
 * run again. A command answers its chat as it runs, kept or not, as it has no query to answer.
 */
import { randomUUID } from "node:crypto";

import type { CallbackQuery } from "@grammyjs/types";
import { Duration, Effect, Exit, Fiber, FiberSet, Option, Schedule } from "effect";

import type { CommandError, OperatorServices } from "../operator/command-handler.ts";
import { runCommand } from "../operator/prompts.ts";
import { type Control, controlConversation, controlId, type KeptControl } from "../persistence/control-repository.ts";
import type { StoreError } from "../persistence/sql.ts";
import { Store, type UpdateMarks } from "../persistence/store.ts";
import { type PressHandling, replayPress } from "./callback-handler.ts";
import { TelegramClient } from "./client.ts";
import { Outbox } from "./outbox.ts";

/**
 * How long an update waits for what its control does before it is told handled: a few
 * seconds, so a control never holds its chat's later updates for longer, the rest of one
 * slower than that running on after it, kept.
 */
export const CONTROL_AWAITED = "3 seconds";
/** How long a kept control is held by the alasio running it past its last renewal. */
export const CONTROL_HOLD = Duration.minutes(1);
/** How often the alasio running a kept control renews its hold. */
const CONTROL_RENEWAL = "15 seconds";
/** How often alasio looks for kept controls let go of, or whose holds lapsed, to run again. */
export const CONTROLS_DUE_EVERY = "30 seconds";

/** The chat a press was made in: none for a button of an inline message, which alasio sends none of. */
export const chatOfPress = (callbackQuery: CallbackQuery): number | undefined => callbackQuery.message?.chat.id;

/** How a kept press's answer is queued again where the store does not take it: a few times, within a few seconds. */
const ANSWER_QUEUED = Schedule.max([Schedule.exponential("200 millis"), Schedule.recurs(4)]);

/** A press's answers: to its query, until it is kept; then through the outbox, in its chat. */
export interface PressAnswers {
  /** `handling`, its answers given as the press is. */
  readonly through: (handling: PressHandling) => Effect.Effect<void, CommandError, OperatorServices | Outbox>;
  /** Has its answers from now go through the outbox, as it is kept, in its update's transaction. */
  readonly keep: Effect.Effect<void>;
  /**
   * Answers its query "Working...", where nothing answered it before: once its update's
   * transaction is through, as Telegram may take its time, which no update should wait for.
   */
  readonly acknowledge: Effect.Effect<void, never, TelegramClient>;
  /** The answers of it that could not be queued, the store not taking them, in order: none where every one was. */
  readonly unsent: () => readonly string[];
}

/** The answers of the press `callbackQuery`, kept from the first where `kept` says, as a press run again is. */
export function pressAnswers(callbackQuery: CallbackQuery, { kept: keptFirst = false }: { readonly kept?: boolean } = {}): PressAnswers {
  let kept = keptFirst;
  let answered = false;
  const unsent: string[] = [];
  const chatId = chatOfPress(callbackQuery);
  return {
    through: (handling) =>
      Effect.flatMap(Outbox, (outbox) =>
        handling.pipe(Effect.updateService(TelegramClient, (client) =>
          TelegramClient.of({
            ...client,
            answerCallbackQuery: (id, text) =>
              Effect.suspend(() => {
                if (!kept || chatId === undefined) {
                  answered = true;
                  return client.answerCallbackQuery(id, text);
                }
                if (!text) return Effect.succeed(null);
                // Queued again a few times; where the store still does not take it, kept with the press, to be queued by the next look.
                return outbox.enqueueText({ chatId, text }).pipe(
                  Effect.retry(ANSWER_QUEUED),
                  Effect.as(null),
                  Effect.catch((error) =>
                    Effect.sync(() => void unsent.push(text)).pipe(
                      Effect.andThen(Effect.logError(`Could not queue an answer of kept press ${callbackQuery.id}, which is kept to be queued again: ${error.message}`)),
                      Effect.as(null),
                    )
                  ),
                );
              }),
          })
        ))),
    keep: Effect.sync(() => void (kept = true)),
    acknowledge: Effect.suspend(() => {
      if (answered) return Effect.void;
      answered = true;
      return Effect.flatMap(TelegramClient, (client) => client.answerCallbackQuery(callbackQuery.id, "Working...")).pipe(
        Effect.catch((error) => Effect.logWarning(`Could not answer press ${callbackQuery.id} as it was kept: ${error.message}`)),
        Effect.asVoid,
      );
    }),
    unsent: () => [...unsent],
  };
}

/** A control kept as its update was handled, to hold once its update's transaction is through (holdWhileRunning): its id, its holder's token, and its work running. */
export interface KeptRunning {
  readonly id: string;
  readonly holder: string;
  readonly running: Fiber.Fiber<void>;
}

/** A control awaited as its update was handled (awaitOrKeep): its work, running or done, and whether it was kept. */
export interface Awaited {
  readonly running: Fiber.Fiber<void>;
  readonly kept: Option.Option<KeptRunning>;
}

/**
 * Runs `work`, what `control` does, in `handling`, which outlasts the update, awaiting it
 * for CONTROL_AWAITED in its update's transaction, whose marks are `marks`: done by then,
 * nothing more; still running, or cut short, as a stop interrupts it, kept with `marks`,
 * held by a token of its own, and `kept` run, as a press's answers are made to go through
 * the outbox, for the kept control to be held while it runs, or let go of to run again, once
 * the transaction is through (holdWhileRunning). Its work, so that where the transaction
 * fails after all, which keeps nothing of it, its caller cuts a command's short
 * (rolledBack), the update to be handled again, so it never runs twice at once.
 */
export const awaitOrKeep = <R>(handling: FiberSet.FiberSet, control: Control, work: Effect.Effect<void, never, R>, marks: Pick<UpdateMarks, "keepControl">, kept: Effect.Effect<void> = Effect.void): Effect.Effect<Awaited, StoreError, R> =>
  Effect.gen(function*() {
    const running = yield* FiberSet.run(handling, Effect.interruptible(work));
    const done = yield* Fiber.await(running).pipe(Effect.timeoutOption(CONTROL_AWAITED));
    if (Option.isSome(done) && !Exit.hasInterrupts(done.value)) return { running, kept: Option.none() };
    const holder = randomUUID();
    // Not kept, the transaction failing with it: a command's cut short here, as the caller has
    // none of it; a press's not (rolledBack).
    yield* marks.keepControl({ control, holder, holdMs: Duration.toMillis(CONTROL_HOLD) }).pipe(Effect.onError(() => (control.kind === "command" ? Fiber.interrupt(running) : Effect.void)));
    yield* kept;
    return { running, kept: Option.some({ id: controlId(control), holder, running }) };
  });

/**
 * Cuts short the work of each command `awaited` as its update's transaction failed, as
 * awaitOrKeep says, waiting for it to end. A press's runs on: its action consumed by itself
 * as it was taken, the update handled again finds it consumed, and does nothing of it again,
 * where a press cut short would be done by neither.
 */
export const rolledBack = (awaited: readonly Awaited[]): Effect.Effect<void> => Effect.forEach(awaited, ({ running }) => Fiber.interrupt(running), { discard: true });

/**
 * Holds the kept control `id` for `holder` while `running` runs, renewing its hold; then
 * forgets it, done; or lets go of it, to be run again, where it was cut short, as alasio
 * stops, or the holding itself is; or, done but answers of it `unsent`, keeps those with it,
 * to be queued by the next look, the control never run again.
 */
export const holdWhileRunning = (id: string, holder: string, running: Fiber.Fiber<void>, unsent: () => readonly string[] = () => []): Effect.Effect<void, never, Store> =>
  Effect.flatMap(Store, (store) => {
    const holdMs = Duration.toMillis(CONTROL_HOLD);
    const said = (what: string) => (error: { readonly message: string }) => Effect.logWarning(`Could not ${what} kept control ${id}: ${error.message}`);
    const release = store.releaseControl(id, holder).pipe(Effect.catch(said("let go of")));
    const renewing = Effect.forever(Effect.sleep(CONTROL_RENEWAL).pipe(
      Effect.andThen(store.renewControl(id, holder, holdMs)),
      Effect.flatMap((held) => (held ? Effect.void : Effect.logWarning(`Kept control ${id} is held by another alasio now, which may run it again`))),
      Effect.catch(said("renew the hold of")),
    ));
    return Fiber.await(running).pipe(
      Effect.raceFirst(renewing),
      Effect.flatMap((exit) => {
        if (Exit.hasInterrupts(exit)) return release;
        const answers = unsent();
        if (answers.length > 0) return store.pendControlAnswer(id, holder, answers.join("\n\n")).pipe(Effect.catch(said("keep the answer of")));
        return store.forgetControl(id, holder).pipe(Effect.catch(said("forget")));
      }),
      Effect.onInterrupt(() => release),
    );
  });

/**
 * Runs each kept control let go of, or whose hold lapsed, again from what it is, a press's
 * answers through the outbox, each held while it runs, in `handling`, which outlasts the
 * look; or, a press done but whose answer was kept unsent, queues its answer alone.
 */
export const recoverControls = (handling: FiberSet.FiberSet): Effect.Effect<void, never, Store | OperatorServices | Outbox> =>
  Effect.gen(function*() {
    const store = yield* Store;
    const holder = randomUUID();
    const controls = yield* store.takeLapsedControls(holder, Duration.toMillis(CONTROL_HOLD));
    yield* Effect.forEach(controls, (kept) => runAgain(handling, kept, holder), { discard: true });
  }).pipe(Effect.catch((error) => Effect.logError(`Looking for kept controls failed: ${error.message}`)));

/** Runs the kept control `control` again, held by `holder` while it runs; or queues its answer, where it was done. */
const runAgain = (handling: FiberSet.FiberSet, { control, answer }: KeptControl, holder: string) =>
  Effect.gen(function*() {
    const id = controlId(control);
    const conversationId = controlConversation(control);
    if (control.kind === "command") {
      yield* Effect.logInfo(`Running kept command ${id} of ${conversationId} again`);
      const running = yield* FiberSet.run(handling, Effect.interruptible(runCommand(control)));
      return yield* FiberSet.run(handling, holdWhileRunning(id, holder, running));
    }
    const { callbackQuery, action } = control;
    const chatId = chatOfPress(callbackQuery);
    if (answer !== null && chatId !== undefined) {
      const store = yield* Store;
      const outbox = yield* Outbox;
      yield* Effect.logInfo(`Queuing the answer of kept press ${id} of ${conversationId}, its control done`);
      return yield* outbox.enqueueText({ chatId, text: answer }).pipe(
        Effect.andThen(store.forgetControl(id, holder)),
        Effect.catch((error) =>
          Effect.logWarning(`Could not queue the answer of kept press ${id}, kept for the next look: ${error.message}`).pipe(
            Effect.andThen(store.releaseControl(id, holder).pipe(Effect.ignore)),
          )
        ),
      );
    }
    yield* Effect.logInfo(`Running kept press ${id} of ${conversationId} again`);
    const answers = pressAnswers(callbackQuery, { kept: true });
    const running = yield* FiberSet.run(handling, answers.through(replayPress(callbackQuery, action)).pipe(
      Effect.catch((error) => Effect.logError(`Failed to process kept press ${id}: ${error.message}`)),
      Effect.interruptible,
    ));
    yield* FiberSet.run(handling, holdWhileRunning(id, holder, running, answers.unsent));
  });
