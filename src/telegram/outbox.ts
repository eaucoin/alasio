import type { ObservableResult } from "@opentelemetry/api";
import { Clock, Context, Effect, Latch, Layer, Semaphore } from "effect";

import type { NewOutboxText, OutboxEntry } from "../persistence/outbox-repository.ts";
import type { StoreError } from "../persistence/sql.ts";
import { Store } from "../persistence/store.ts";
import { withLogScope } from "../shared/log.ts";
import { currentTraceparent, meter, withAlasioSpan } from "../telemetry/index.ts";
import { type TelegramError, TelegramClient } from "./client.ts";

const DEFAULT_RETRY_MS = 10_000;
const MAX_RETRY_MS = 5 * 60_000;
/** How often delivery looks for replies due, besides when one is queued. */
const DELIVERY_INTERVAL = "5 seconds";

const deliveryLag = meter.createHistogram("alasio.delivery.lag", {
  description: "Time from a reply entering the outbox to Telegram accepting it, retries included",
  unit: "s",
});

const pendingReplies = meter.createObservableGauge("alasio.outbox.pending", {
  description: "Replies waiting in the outbox for delivery to Telegram",
  unit: "{reply}",
});

/** A reply to queue: everything but the trace, which the outbox takes from where it is queued. */
export type OutboxText = Omit<NewOutboxText, "traceparent">;

/** How long a failed delivery waits: what Telegram asked for, or longer as attempts mount. */
function retryDelayMs(item: OutboxEntry, error: TelegramError): number {
  const retryAfterMs = error._tag === "TelegramApiError" ? error.retryAfterMs : 0;
  const exponentialMs = Math.min(DEFAULT_RETRY_MS * (2 ** Math.min(item.attempts, 5)), MAX_RETRY_MS);
  return Math.max(retryAfterMs, exponentialMs);
}

/**
 * alasio's replies to Telegram, kept in the store until Telegram accepts them, so none is
 * lost to a failure or a restart. Delivery runs while the service does: every few
 * seconds and whenever a reply is queued, one pass at a time, each chat's replies in
 * the order they were queued.
 */
export class Outbox extends Context.Service<Outbox, {
  /** Queues a reply and wakes delivery; its delivery joins the trace it is queued in. The reply's id. */
  readonly enqueueText: (text: OutboxText) => Effect.Effect<string, StoreError>;
  /** A delivery pass, after the one running if one is. */
  readonly deliverDue: Effect.Effect<void, StoreError>;
}>()("alasio/telegram/Outbox") {
  static readonly layer: Layer.Layer<Outbox, never, Store | TelegramClient> = Layer.effect(Outbox, Effect.gen(function*() {
    const store = yield* Store;
    const client = yield* TelegramClient;
    const passes = yield* Semaphore.make(1);
    const wakeup = yield* Latch.make();

    // The gauge is read as metrics are exported, outside every fiber of alasio's.
    const runPromise = Effect.runPromiseWith(yield* Effect.context<never>());
    yield* Effect.acquireRelease(
      Effect.sync(() => {
        const observe = (result: ObservableResult) =>
          runPromise(store.getPendingOutboxCount.pipe(
            Effect.map((count) => result.observe(count)),
            Effect.catch((error) => Effect.logWarning(`could not count the outbox's pending replies: ${error.message}`)),
          ));
        pendingReplies.addCallback(observe);
        return observe;
      }),
      (observe) => Effect.sync(() => pendingReplies.removeCallback(observe)),
    );

    /** Sends one reply with its media, or defers it with backoff and fails with what stopped it. */
    const deliver = Effect.fnUntraced(function*(item: OutboxEntry): Effect.fn.Return<void, TelegramError | StoreError> {
      const media = yield* store.getOutboxMedia(item.id);
      yield* client.sendMessage(item.chat_id, item.text, media.length ? { ...item.options, media } : item.options).pipe(
        Effect.tapError((error) => {
          const delayMs = retryDelayMs(item, error);
          return store.rescheduleOutbox(item.id, error, delayMs).pipe(
            Effect.andThen(Effect.logWarning(`Telegram outbox delivery deferred id=${item.id} delay_ms=${delayMs} error=${error.message}`)),
          );
        }),
      );
      yield* store.markOutboxSent(item.id);
      deliveryLag.record(((yield* Clock.currentTimeMillis) - item.created_at.getTime()) / 1000);
    });

    const deliverDue = passes.withPermit(Effect.gen(function*() {
      for (const item of yield* store.getDueOutbox(20)) {
        const delivered = yield* deliver(item).pipe(
          withAlasioSpan("alasio.delivery", {
            parent: item.traceparent,
            attributes: { "alasio.outbox.id": item.id, "alasio.delivery.attempt": item.attempts + 1, "telegram.chat.id": item.chat_id },
          }),
          Effect.match({ onFailure: () => false, onSuccess: () => true }),
        );
        // Deferred; the rest wait behind it so replies keep their order.
        if (!delivered) break;
      }
    })).pipe(withLogScope("telegram-app"));

    // A pass that fails is tried again with the next.
    yield* wakeup.close.pipe(
      Effect.andThen(deliverDue),
      Effect.catch((error) => Effect.logError(`Telegram outbox delivery failed: ${error.message}`).pipe(withLogScope("telegram-app"))),
      Effect.catchDefect((defect) => Effect.logError(`Telegram outbox delivery failed: ${String(defect)}`).pipe(withLogScope("telegram-app"))),
      Effect.andThen(wakeup.await.pipe(Effect.timeoutOption(DELIVERY_INTERVAL))),
      Effect.forever,
      Effect.forkScoped,
    );

    return Outbox.of({
      enqueueText: (text) =>
        Effect.suspend(() => store.enqueueOutboxText({ ...text, traceparent: currentTraceparent() })).pipe(
          Effect.tap(() => Effect.sync(() => wakeup.openUnsafe())),
        ),
      deliverDue,
    });
  }));
}
