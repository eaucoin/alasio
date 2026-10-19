/**
 * Presses of alasio's buttons whose controls run on past their updates. An update awaits what
 * its press does for PRESS_AWAITED (./app.ts): a control quick enough, as most are, is done
 * before its update is told handled. One still running then is kept in the store, in the
 * update's transaction, so the update is told handled only once its press is kept; it is held
 * by the alasio running it, by a token of its own, which renews the hold as the control runs,
 * forgets the press once it is done, and lets go of it as alasio stops. From then on its
 * answers go through the outbox, as replies in its chat, which are sent once whichever alasio
 * sends them, the query itself answered "Working..." as the press is kept, where nothing
 * answered it before. A press let go of, or whose hold lapsed as its alasio crashed, is run
 * again from its action by whichever alasio looks for presses next (recoverPresses), as alasio
 * starts and every PRESSES_DUE_EVERY: so each kept press runs to its end at least once.
 */
import { randomUUID } from "node:crypto";

import type { CallbackQuery } from "@grammyjs/types";
import { Duration, Effect, Exit, Fiber, FiberSet } from "effect";

import type { CommandError, OperatorServices } from "../operator/command-handler.ts";
import type { KeptPress } from "../persistence/press-repository.ts";
import { Store } from "../persistence/store.ts";
import { type PressHandling, replayPress } from "./callback-handler.ts";
import { TelegramClient } from "./client.ts";
import { Outbox } from "./outbox.ts";

/**
 * How long an update waits for what its press does before it is told handled: a few
 * seconds, so a press never holds its chat's later updates for longer, the rest of a control
 * slower than that running on after it, kept.
 */
export const PRESS_AWAITED = "3 seconds";
/** How long a kept press is held by the alasio running it past its last renewal. */
export const PRESS_HOLD = Duration.minutes(1);
/** How often the alasio running a kept press renews its hold. */
const PRESS_RENEWAL = "15 seconds";
/** How often alasio looks for kept presses let go of, or whose holds lapsed, to run again. */
export const PRESSES_DUE_EVERY = "30 seconds";

/** The chat a press was made in: none for a button of an inline message, which alasio sends none of. */
export const chatOfPress = (callbackQuery: CallbackQuery): number | undefined => callbackQuery.message?.chat.id;

/** A press's answers: to its query, until it is kept; then through the outbox, in its chat. */
export interface PressAnswers {
  /** `handling`, its answers given as the press is. */
  readonly through: (handling: PressHandling) => Effect.Effect<void, CommandError, OperatorServices | Outbox>;
  /** Has its answers from now go through the outbox, its query answered "Working..." where nothing answered it before. */
  readonly kept: Effect.Effect<void, never, TelegramClient>;
}

/** The answers of the press `callbackQuery`, kept from the first where `kept` says, as a press run again is. */
export function pressAnswers(callbackQuery: CallbackQuery, { kept: keptFirst = false }: { readonly kept?: boolean } = {}): PressAnswers {
  let kept = keptFirst;
  let answered = false;
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
                // A store that does not take it fails the press, as one whose answer is lost.
                return text ? outbox.enqueueText({ chatId, text }).pipe(Effect.orDie, Effect.as(null)) : Effect.succeed(null);
              }),
          })
        ))),
    kept: Effect.suspend(() => {
      kept = true;
      if (answered) return Effect.void;
      answered = true;
      return Effect.flatMap(TelegramClient, (client) => client.answerCallbackQuery(callbackQuery.id, "Working...")).pipe(
        Effect.catch((error) => Effect.logWarning(`Could not answer press ${callbackQuery.id} as it was kept: ${error.message}`)),
        Effect.asVoid,
      );
    }),
  };
}

/**
 * Holds the kept press `id` for `holder` while `pressing` runs, renewing its hold; then
 * forgets it, its control done, or lets go of it, where its control was cut short, as alasio
 * stops, or the holding itself is.
 */
export const holdWhileRunning = (id: string, holder: string, pressing: Fiber.Fiber<void>): Effect.Effect<void, never, Store> =>
  Effect.flatMap(Store, (store) => {
    const holdMs = Duration.toMillis(PRESS_HOLD);
    const said = (what: string) => (error: { readonly message: string }) => Effect.logWarning(`Could not ${what} kept press ${id}: ${error.message}`);
    const release = store.releasePress(id, holder).pipe(Effect.catch(said("let go of")));
    const renewing = Effect.forever(Effect.sleep(PRESS_RENEWAL).pipe(
      Effect.andThen(store.renewPress(id, holder, holdMs)),
      Effect.flatMap((held) => (held ? Effect.void : Effect.logWarning(`Kept press ${id} is held by another alasio now, which may run it again`))),
      Effect.catch(said("renew the hold of")),
    ));
    return Fiber.await(pressing).pipe(
      Effect.raceFirst(renewing),
      Effect.flatMap((exit) => (Exit.hasInterrupts(exit) ? release : store.forgetPress(id, holder).pipe(Effect.catch(said("forget"))))),
      Effect.onInterrupt(() => release),
    );
  });

/**
 * Runs each kept press let go of, or whose hold lapsed, again from its action, its answers
 * through the outbox, each held while it runs, in `handling`, which outlasts the look.
 */
export const recoverPresses = (handling: FiberSet.FiberSet): Effect.Effect<void, never, Store | OperatorServices | Outbox> =>
  Effect.gen(function*() {
    const store = yield* Store;
    const holder = randomUUID();
    const presses = yield* store.takeLapsedPresses(holder, Duration.toMillis(PRESS_HOLD));
    yield* Effect.forEach(presses, (press) => runAgain(handling, press, holder), { discard: true });
  }).pipe(Effect.catch((error) => Effect.logError(`Looking for kept presses failed: ${error.message}`)));

/** Runs the kept press `press` again, held by `holder` while it runs. */
const runAgain = (handling: FiberSet.FiberSet, { callbackQuery, action }: KeptPress, holder: string) =>
  Effect.gen(function*() {
    yield* Effect.logInfo(`Running kept press ${callbackQuery.id} of ${action.conversationId} again`);
    const answers = pressAnswers(callbackQuery, { kept: true });
    const pressing = yield* FiberSet.run(handling, answers.through(replayPress(callbackQuery, action)).pipe(
      Effect.catch((error) => Effect.logError(`Failed to process kept press ${callbackQuery.id}: ${error.message}`)),
      Effect.interruptible,
    ));
    yield* FiberSet.run(handling, holdWhileRunning(callbackQuery.id, holder, pressing));
  });
