/**
 * How long alasio, started, took to answer the first update it handled (alasio.wake.latency):
 * from the date of the update's message, as Telegram gives it, to the first reply, status
 * message, notice or panel queued to its chat in the update's own trace, or sent to it at once,
 * as a command's answer is (answeringClient), as everything the
 * update leads to joins it (./app.ts), its turn however much later it runs; so, of an alasio
 * KEDA woke from none for the update, the wait its sender saw from sending it, and not a reply
 * of a turn that ran on meanwhile. Timed once a start, and given up on, nothing timed, where no
 * answer is queued within WAKE_ANSWER_WITHIN of the update's handling. A first update with no
 * date of its own, a button's press, or handled in no trace, as alasio exports no telemetry, is
 * not timed, nor any after it; a start whose first update came while alasio ran already, another
 * replica's, is timed as well. Telegram's date is to the second, so a latency may read up to a
 * second more than it took.
 */
import type { Update } from "@grammyjs/types";
import { Clock, Context, Duration, Effect, Layer, Ref } from "effect";

import { currentTraceparent, meter } from "../telemetry/index.ts";
import { TelegramClient } from "./client.ts";

export const WAKE_LATENCY = "alasio.wake.latency";
const wakeLatency = meter.createHistogram(WAKE_LATENCY, {
  description: "Time from the first update an alasio handled after it started, as its message's date says (to the second), to the first answer queued or sent to its chat in the update's trace; once a start, none past five minutes",
  unit: "s",
});

/** How long after it is handled the start's first update's answer is waited for, before none is timed: past a turn's start, however slow its harness's bind. */
export const WAKE_ANSWER_WITHIN = Duration.minutes(5);

/** Where the start's timing is: no update handled yet, the first's answer awaited, or done with. */
type Timing =
  | { readonly _tag: "Unseen" }
  | { readonly _tag: "Awaiting"; readonly chatId: string; readonly traceId: string; readonly sentAtMs: number; readonly handledAtMs: number }
  | { readonly _tag: "Done" };

/** The trace a W3C traceparent names, or null where it names none. */
const traceOf = (traceparent: string | null): string | null => traceparent?.split("-")[1] ?? null;

/** What WakeLatency is made with: how long an answer is waited for, WAKE_ANSWER_WITHIN but for tests. */
export interface WakeLatencyOptions {
  readonly answerWithin?: Duration.Input | undefined;
}

/** The start's timing, as the module says. */
export class WakeLatency extends Context.Service<WakeLatency, {
  /** An update alasio handles, not handled before, in the trace `traceparent` names: the first of the start's, where it is a message, awaiting its answer. */
  readonly handling: (update: Update, traceparent: string | null) => Effect.Effect<void>;
  /** Something queued to the chat `chatId` in the trace `traceparent` names: the answer the start's first update awaits, where it is in its trace. */
  readonly queued: (chatId: string | number, traceparent: string | null) => Effect.Effect<void>;
}>()("alasio/telegram/WakeLatency") {
  /** The start's timing, an answer waited for as `options` say. */
  static readonly layerWith = ({ answerWithin = WAKE_ANSWER_WITHIN }: WakeLatencyOptions = {}): Layer.Layer<WakeLatency> =>
    Layer.effect(WakeLatency, Effect.map(Ref.make<Timing>({ _tag: "Unseen" }), (timing) => {
      const withinMs = Duration.toMillis(answerWithin);
      return WakeLatency.of({
        handling: (update, traceparent) =>
          Effect.flatMap(Clock.currentTimeMillis, (nowMs) =>
            Ref.modify(timing, (now): readonly [string | null, Timing] => {
              if (now._tag !== "Unseen") return [null, now];
              const traceId = traceOf(traceparent);
              if (!update.message) return [`not timing the start's first update, ${update.update_id}, as it is no message, which alone Telegram dates`, { _tag: "Done" }];
              if (!traceId) return [`not timing the start's first update, ${update.update_id}, as it is handled in no trace to tell its answer by`, { _tag: "Done" }];
              return [`timing the start's first update, ${update.update_id}, to its first answer`, { _tag: "Awaiting", chatId: String(update.message.chat.id), traceId, sentAtMs: update.message.date * 1_000, handledAtMs: nowMs }];
            })).pipe(Effect.flatMap((said) => (said === null ? Effect.void : Effect.logInfo(said)))),
        queued: (chatId, traceparent) =>
          Effect.flatMap(Clock.currentTimeMillis, (nowMs) =>
            Ref.modify(timing, (now): readonly [{ readonly tookMs: number } | { readonly gaveUp: true } | null, Timing] => {
              if (now._tag !== "Awaiting") return [null, now];
              // Past the wait, given up on, whatever is queued.
              if (nowMs - now.handledAtMs > withinMs) return [{ gaveUp: true }, { _tag: "Done" }];
              return now.chatId === String(chatId) && traceOf(traceparent) === now.traceId ? [{ tookMs: nowMs - now.sentAtMs }, { _tag: "Done" }] : [null, now];
            })).pipe(Effect.flatMap((ended) => {
              if (ended === null) return Effect.void;
              if ("gaveUp" in ended) return Effect.logInfo(`gave up timing the start's first update, answered past ${withinMs / 1_000} s`);
              const seconds = Math.max(0, ended.tookMs) / 1_000;
              wakeLatency.record(seconds);
              return Effect.logInfo(`the start's first update was answered ${seconds} s after it was sent`);
            })),
      });
    }));

  /** The start's timing, as alasio's is. */
  static readonly layer: Layer.Layer<WakeLatency> = WakeLatency.layerWith();
}

/**
 * The Telegram client of the layer it is given, which tells WakeLatency of each message it sends
 * a chat at once, with the trace it is sent in, as an answer queued is told: a command's answer,
 * which no outbox queues, among them.
 */
export const answeringClient: Layer.Layer<TelegramClient, never, TelegramClient | WakeLatency> = Layer.effect(TelegramClient, Effect.gen(function*() {
  const client = yield* TelegramClient;
  const wake = yield* WakeLatency;
  const answered = (chatId: string | number) => Effect.suspend(() => wake.queued(chatId, currentTraceparent()));
  return TelegramClient.of({
    ...client,
    sendMessage: (chatId, text, options) => Effect.tap(client.sendMessage(chatId, text, options), () => answered(chatId)),
    sendDocument: (chatId, filePath, caption) => Effect.tap(client.sendDocument(chatId, filePath, caption), () => answered(chatId)),
  });
}));
