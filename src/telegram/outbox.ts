import type { ObservableResult } from "@opentelemetry/api";
import { Clock, Context, Effect, Latch, Layer, Option, Semaphore } from "effect";

import type { DueOutboxEntry, NewOutboxStatus, NewOutboxText, OutboxEntry } from "../persistence/outbox-repository.ts";
import type { StoreError } from "../persistence/sql.ts";
import { type DeliveryMarks, Store } from "../persistence/store.ts";
import { withLogScope } from "../shared/log.ts";
import { currentTraceparent, meter, withAlasioSpan } from "../telemetry/index.ts";
import { type TelegramError, TelegramClient } from "./client.ts";

const DEFAULT_RETRY_MS = 10_000;
const MAX_RETRY_MS = 5 * 60_000;
/** How often delivery looks for replies due, besides when one is queued. */
const DELIVERY_INTERVAL = "5 seconds";
/** How soon deliverChat looks again while another pass delivers the chat. */
const CHAT_PASS_INTERVAL = "50 millis";
/** How many of an alasio's passes hold a chat's delivery lock at once, at most: a bound on the pool's connections they take. */
const LOCK_HOLDERS = 2;

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
/** A status message, or an edit of one, to queue: everything but the trace, as a reply. */
export type OutboxStatus = Omit<NewOutboxStatus, "traceparent">;

/**
 * Whether Telegram refused a status message, or an edit of one, for good: one it answers
 * 400 to (the chat refuses it, the message is gone, too old, or already says it) is given
 * up on, so it holds nothing of the chat's back, its reply above all. A status message
 * given up on has no message, so its edits are given up on too. A reply is never given
 * up on.
 */
const refusedForGood = (item: DueOutboxEntry, error: TelegramError): boolean =>
  item.kind !== "text" && error._tag === "TelegramApiError" && error.status === 400;

/** How long a failed delivery waits: what Telegram asked for, or longer as attempts mount. */
function retryDelayMs(item: OutboxEntry, error: TelegramError): number {
  const retryAfterMs = error._tag === "TelegramApiError" ? error.retryAfterMs : 0;
  const exponentialMs = Math.min(DEFAULT_RETRY_MS * (2 ** Math.min(item.attempts, 5)), MAX_RETRY_MS);
  return Math.max(retryAfterMs, exponentialMs);
}

/**
 * What alasio sends to Telegram, kept in the store until Telegram accepts it, so nothing
 * is lost to a failure or a restart: replies, and turns' status messages and their
 * edits. Delivery runs while the service does: every few seconds and whenever something
 * is queued, each chat's in the order it was queued, by one pass at a time of all the
 * alasios on the store.
 */
export class Outbox extends Context.Service<Outbox, {
  /** Queues a reply and wakes delivery; its delivery joins the trace it is queued in. The reply's id. */
  readonly enqueueText: (text: OutboxText) => Effect.Effect<string, StoreError>;
  /** Queues a turn's status message and wakes delivery: its id, which an edit of it names. */
  readonly enqueueStatus: (status: OutboxStatus) => Effect.Effect<string, StoreError>;
  /** Queues an edit of the status message `edits` and wakes delivery. */
  readonly enqueueEdit: (edits: string, edit: OutboxStatus) => Effect.Effect<string, StoreError>;
  /** A delivery pass, of each chat no other pass is delivering: what another is, it leaves to it. */
  readonly deliverDue: Effect.Effect<void, StoreError>;
  /**
   * Delivers the chat's entries, its own alone, until it has nothing due: what another pass
   * is delivering of it, delivered by that one or, where it is cut short, by this. Unbounded,
   * as Telegram may be refusing the chat's next entry for a while; its caller bounds it.
   */
  readonly deliverChat: (chatId: string | number) => Effect.Effect<void, StoreError>;
}>()("alasio/telegram/Outbox") {
  static readonly layer: Layer.Layer<Outbox, never, Store | TelegramClient> = Layer.effect(Outbox, Effect.gen(function*() {
    const store = yield* Store;
    const client = yield* TelegramClient;
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

    /**
     * Sends one entry: a reply with its media, a status message, whose message's id is
     * recorded, or an edit of one. An edit of a status message that has no message (it was
     * pruned, refused, or Telegram gave none), and a status message or an edit Telegram
     * refuses for good, are given up on; anything else that fails is deferred with
     * backoff, failing with what stopped it.
     */
    const deliver = Effect.fnUntraced(function*(item: DueOutboxEntry, marks: DeliveryMarks): Effect.fn.Return<void, TelegramError | StoreError> {
      const deferred = (error: TelegramError) => {
        const delayMs = retryDelayMs(item, error);
        return marks.rescheduleOutbox(item.id, error, delayMs).pipe(
          Effect.andThen(Effect.logWarning(`Telegram outbox delivery deferred id=${item.id} delay_ms=${delayMs} error=${error.message}`)),
        );
      };
      if (item.kind === "edit") {
        if (item.edited_message_id === null) {
          yield* marks.markOutboxSent(item.id, { refused: `the status message ${item.edits} has no message to edit` });
          return;
        }
        // Sent only while the chat's lock still holds, so no other pass sends it too.
        yield* marks.fence;
        const edited = yield* client.editMessageText(item.chat_id, Number(item.edited_message_id), item.text, item.options).pipe(
          Effect.as(null),
          Effect.catchIf((error) => refusedForGood(item, error), (error) => Effect.succeed(error.message)),
          Effect.tapError(deferred),
        );
        yield* marks.markOutboxSent(item.id, { refused: edited });
      } else {
        const media = yield* store.getOutboxMedia(item.id);
        yield* marks.fence;
        const sent = yield* client.sendMessage(item.chat_id, item.text, media.length ? { ...item.options, media } : item.options).pipe(
          Effect.map(([message]) => ({ telegramMessageId: item.kind === "status" ? (message?.message_id ?? null) : null })),
          Effect.catchIf((error) => refusedForGood(item, error), (error) => Effect.succeed({ refused: error.message })),
          Effect.tapError(deferred),
        );
        yield* marks.markOutboxSent(item.id, sent);
      }
      deliveryLag.record(((yield* Clock.currentTimeMillis) - item.created_at.getTime()) / 1000);
    });

    // At most LOCK_HOLDERS of this alasio's passes hold a chat's lock at once, each holding a
    // connection of the pool for its lock and taking others for its reads: so however many
    // passes run at once, the store's writes are never starved of connections. It bounds
    // connections alone; which chat each pass delivers is the locks' to say.
    const holders = yield* Semaphore.make(LOCK_HOLDERS);

    /**
     * The chat's next entry delivered under its delivery lock, read again once it is held:
     * another pass may have delivered it since. None where another pass holds the lock;
     * else whether it was delivered, or nothing was due, rather than deferred.
     */
    const deliverNext = (chat: string): Effect.Effect<Option.Option<boolean>, StoreError> =>
      // Uninterruptible from the lock's being taken to its transaction's commit
      // (Store.withDeliveryLock), so an entry sent as alasio stops is marked sent with it;
      // but where the client waits out a rate limit before Telegram has taken any call of
      // it (./client.ts).
      holders.withPermit(store.withDeliveryLock(chat, (marks) =>
        Effect.gen(function*() {
          const item = yield* marks.getDueOutboxOf(chat);
          if (item === null) return true;
          return yield* deliver(item, marks).pipe(
            withAlasioSpan("alasio.delivery", {
              parent: item.traceparent,
              attributes: { "alasio.outbox.id": item.id, "alasio.outbox.kind": item.kind, "alasio.delivery.attempt": item.attempts + 1, "telegram.chat.id": item.chat_id },
            }),
            Effect.match({ onFailure: () => false, onSuccess: () => true }),
          );
        })
      ));

    // What is due is each chat's next entry, so a pass asks again until nothing is due. An
    // entry's delivery is not interrupted but where it waits out a rate limit before
    // Telegram has taken any call of it (./client.ts): so a pass cut short, as alasio stops,
    // never leaves an entry sent, or a part of one, and not marked so, to be sent again
    // after the restart. Each call of it is bounded by its timeout.
    //
    // A chat's entry is delivered under the chat's delivery lock, which every alasio's
    // passes take for it, its marks made in the lock's transaction, so a lock lost leaves the
    // entry due rather than marked by a holder that no longer holds it. A chat whose lock
    // another pass holds is that pass's to deliver, and this one passes over it, waiting on
    // no lock, so no pass holds a connection of the pool while it waits; what is left of
    // it, the next pass delivers.
    const deliverDue = Effect.gen(function*() {
      const passedOver = new Set<string>();
      for (;;) {
        const due = (yield* store.getDueOutbox(20)).filter(({ chat_id: chat }) => !passedOver.has(chat));
        if (due.length === 0) return;
        for (const { chat_id: chat } of due) {
          const taken = yield* deliverNext(chat);
          if (Option.isNone(taken)) {
            passedOver.add(chat);
            continue;
          }
          // Deferred; the rest wait behind it so each chat's entries keep their order.
          if (!taken.value) return;
        }
      }
    }).pipe(withLogScope("telegram-app"));

    // A pass that fails is tried again with the next.
    yield* wakeup.close.pipe(
      Effect.andThen(deliverDue),
      Effect.catch((error) => Effect.logError(`Telegram outbox delivery failed: ${error.message}`).pipe(withLogScope("telegram-app"))),
      Effect.catchDefect((defect) => Effect.logError(`Telegram outbox delivery failed: ${String(defect)}`).pipe(withLogScope("telegram-app"))),
      Effect.andThen(wakeup.await.pipe(Effect.timeoutOption(DELIVERY_INTERVAL))),
      Effect.forever,
      Effect.forkScoped,
    );

    /** Queues with the trace it is queued in, and wakes delivery. */
    const queued = (enqueue: (traceparent: string | null) => Effect.Effect<string, StoreError>) =>
      Effect.suspend(() => enqueue(currentTraceparent())).pipe(Effect.tap(() => Effect.sync(() => wakeup.openUnsafe())));

    return Outbox.of({
      enqueueText: (text) => queued((traceparent) => store.enqueueOutboxText({ ...text, traceparent })),
      enqueueStatus: (status) => queued((traceparent) => store.enqueueOutboxStatus({ ...status, traceparent })),
      enqueueEdit: (edits, edit) => queued((traceparent) => store.enqueueOutboxEdit(edits, { ...edit, traceparent })),
      deliverDue,
      deliverChat: (chatId) =>
        Effect.gen(function*() {
          const chat = String(chatId);
          for (;;) {
            const taken = yield* deliverNext(chat);
            if ((yield* store.getDueOutboxOf(chat)) === null) return;
            // Another pass delivers it, or its entry was deferred: looked at again shortly.
            if (Option.isNone(taken) || !taken.value) yield* Effect.sleep(CHAT_PASS_INTERVAL);
          }
        }),
    });
  }));
}
