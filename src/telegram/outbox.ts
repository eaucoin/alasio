import { randomUUID } from "node:crypto";

import type { ObservableResult } from "@opentelemetry/api";
import { Clock, Context, Duration, Effect, Latch, Layer, Option, Schedule } from "effect";

import type { DueOutboxEntry, NewOutboxStatus, NewOutboxText, OutboxEntry } from "../persistence/outbox-repository.ts";
import type { StoreError } from "../persistence/sql.ts";
import { type OutboxClaim, type OutboxDelivered, Store } from "../persistence/store.ts";
import { inlineKeyboard, type PanelDraft } from "../operator/panel.ts";
import { withLogScope } from "../shared/log.ts";
import { currentTraceparent, meter, withAlasioSpan } from "../telemetry/index.ts";
import { type TelegramError, TelegramClient } from "./client.ts";

const DEFAULT_RETRY_MS = 10_000;
const MAX_RETRY_MS = 5 * 60_000;
/** How often delivery looks for replies due, besides when one is queued. */
const DELIVERY_INTERVAL = "5 seconds";
/** How soon deliverChat looks again while another pass delivers the chat. */
const CHAT_PASS_INTERVAL = "50 millis";
/**
 * How long a pass's claim on the entry it delivers holds unless it is renewed
 * (Store.claimOutboxOf): past it, the pass is taken to be gone, and another delivers the
 * entry. The pass renews it in the store right before it sends the entry, and sends nothing
 * where that finds it no longer holds or cannot say; and every CLAIM_RENEWAL as it delivers,
 * so a send that outlasts CLAIM_LIFE keeps it, where the store answers meanwhile.
 */
const CLAIM_LIFE = Duration.minutes(1);
/** How often a pass renews its claim as it delivers: a few times within CLAIM_LIFE. */
const CLAIM_RENEWAL = Duration.seconds(15);
/** How a mark of an entry is tried again: one that fails leaves the entry to be sent again once its claim lapses. */
const MARK_RETRY = { times: 4, schedule: Schedule.exponential("200 millis") };

/** How an outbox's passes claim what they deliver: CLAIM_LIFE and CLAIM_RENEWAL, unless a test says. */
export interface OutboxOptions {
  readonly claimLife?: Duration.Input | undefined;
  readonly claimRenewal?: Duration.Input | undefined;
}

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
/** A reply with buttons to queue: the conversation and chat it is to, its text, and its buttons, row by row. */
export interface OutboxPanel extends PanelDraft {
  readonly conversationId: string;
  readonly chatId: number | string;
}

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
 * is queued, each chat's in the order it was queued, each entry sent by the one pass of all
 * the alasios on the store that claimed it.
 */
export class Outbox extends Context.Service<Outbox, {
  /** Queues a reply and wakes delivery; its delivery joins the trace it is queued in. The reply's id. */
  readonly enqueueText: (text: OutboxText) => Effect.Effect<string, StoreError>;
  /**
   * Queues a reply with buttons to the conversation's chat, as enqueueText does, the
   * buttons' actions kept with it, in one transaction: neither is kept without the other.
   */
  readonly enqueuePanel: (panel: OutboxPanel) => Effect.Effect<string, StoreError>;
  /**
   * Queues a notice of the conversation's turns besides their replies, as enqueueText does,
   * unless the conversation was told it already, by this alasio or another (the store's
   * enqueueOutboxNotice): whether it was queued now.
   */
  readonly enqueueNotice: (notice: { readonly conversationId: string; readonly chatId: number | string; readonly text: string }) => Effect.Effect<boolean, StoreError>;
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
  /** The outbox, its passes claiming as `options` say. */
  static readonly layerWith = ({ claimLife = CLAIM_LIFE, claimRenewal = CLAIM_RENEWAL }: OutboxOptions = {}): Layer.Layer<Outbox, never, Store | TelegramClient> => Layer.effect(Outbox, Effect.gen(function*() {
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
    const deliver = Effect.fnUntraced(function*(claim: OutboxClaim): Effect.fn.Return<void, TelegramError | StoreError> {
      const item = claim.entry;
      /** Marks the entry sent; one whose claim another pass took over since is that pass's to mark, and is said. */
      const markSent = (delivered: OutboxDelivered) =>
        store.markOutboxSent(claim, delivered).pipe(
          Effect.retry(MARK_RETRY),
          Effect.flatMap((held) => held ? Effect.void : Effect.logWarning(`Telegram outbox entry id=${item.id} was sent as another pass took it over, its claim lapsed: it may be delivered twice`)),
        );
      const deferred = (error: TelegramError) => {
        const delayMs = retryDelayMs(item, error);
        return store.rescheduleOutbox(claim, error, delayMs).pipe(
          Effect.retry(MARK_RETRY),
          Effect.flatMap((held) =>
            held
              ? Effect.logWarning(`Telegram outbox delivery deferred id=${item.id} delay_ms=${delayMs} error=${error.message}`)
              : Effect.logWarning(`Telegram outbox entry id=${item.id}, not taken by Telegram, is the pass's that took it over, its claim lapsed`)
          ),
        );
      };
      /**
       * Whether the claim still holds, renewed in the store right before the entry is sent:
       * one taken over is that pass's, and nothing of it is sent; one the store cannot say of
       * fails the delivery, nothing of it sent, its claim left to lapse.
       */
      const stillClaimed = Effect.flatMap(store.renewOutboxClaim(claim, claimLife), (held) =>
        held ? Effect.succeed(true) : Effect.as(Effect.logWarning(`Telegram outbox entry id=${item.id} left to the pass that took it over, its claim lapsed`), false));
      if (item.kind === "edit") {
        if (item.edited_message_id === null) {
          yield* markSent({ refused: `the status message ${item.edits} has no message to edit` });
          return;
        }
        if (!(yield* stillClaimed)) return;
        const edited = yield* client.editMessageText(item.chat_id, Number(item.edited_message_id), item.text, item.options).pipe(
          Effect.as(null),
          Effect.catchIf((error) => refusedForGood(item, error), (error) => Effect.succeed(error.message)),
          Effect.tapError(deferred),
        );
        yield* markSent({ refused: edited });
      } else {
        const media = yield* store.getOutboxMedia(item.id);
        if (!(yield* stillClaimed)) return;
        const sent = yield* client.sendMessage(item.chat_id, item.text, media.length ? { ...item.options, media } : item.options).pipe(
          Effect.map(([message]) => ({ telegramMessageId: item.kind === "status" ? (message?.message_id ?? null) : null })),
          Effect.catchIf((error) => refusedForGood(item, error), (error) => Effect.succeed({ refused: error.message })),
          Effect.tapError(deferred),
        );
        yield* markSent(sent);
      }
      deliveryLag.record(((yield* Clock.currentTimeMillis) - item.created_at.getTime()) / 1000);
    });

    /**
     * Keeps the claim for as long as its pass delivers, renewing it every `claimRenewal`, so a
     * send that outlasts its life keeps it; one found taken over, or that the store does not
     * renew, is said, the pass's own check before it sends being what keeps it from sending.
     */
    const renewing = (claim: OutboxClaim): Effect.Effect<void> =>
      Effect.gen(function*() {
        for (;;) {
          yield* Effect.sleep(claimRenewal);
          const held = yield* store.renewOutboxClaim(claim, claimLife).pipe(
            Effect.catch((error) => Effect.as(Effect.logWarning(`renewing the claim on Telegram outbox entry id=${claim.entry.id} failed: ${error.message}`), true)),
          );
          if (!held) return yield* Effect.logWarning(`the claim on Telegram outbox entry id=${claim.entry.id} lapsed, and another pass took it over`);
        }
      });

    /**
     * The chat's next entry delivered by this pass, which claims it first: none where nothing
     * is due or another pass's claim holds it; else whether it was delivered, rather than
     * deferred. Interruptible until it is claimed; from then on, but where the client waits
     * out a rate limit before Telegram has taken any call of it (./client.ts), it is not, so
     * an entry sent as alasio stops is marked sent. Cut short, it lets go of its claim, as
     * nothing of its entry was sent, which the next alasio then sends with no lapse to wait.
     */
    const deliverNext = (chat: string): Effect.Effect<Option.Option<boolean>, StoreError> =>
      Effect.suspend(() => {
        const token = randomUUID();
        return Effect.uninterruptibleMask((restore) =>
          Effect.gen(function*() {
            const claim = yield* restore(store.claimOutboxOf(chat, token, claimLife));
            if (claim === null) return Option.none();
            const item = claim.entry;
            return yield* Effect.scoped(Effect.gen(function*() {
              yield* Effect.forkScoped(Effect.interruptible(renewing(claim)));
              return yield* deliver(claim);
            })).pipe(
              withAlasioSpan("alasio.delivery", {
                parent: item.traceparent,
                attributes: { "alasio.outbox.id": item.id, "alasio.outbox.kind": item.kind, "alasio.delivery.attempt": item.attempts + 1, "telegram.chat.id": item.chat_id },
              }),
              Effect.match({ onFailure: () => Option.some(false), onSuccess: () => Option.some(true) }),
            );
          })
        ).pipe(Effect.onInterrupt(() => Effect.ignore(store.releaseOutboxClaim(token))));
      });

    // What is due is each chat's next entry, so a pass asks again until nothing is due. An
    // entry's delivery is not interrupted but where it waits out a rate limit before
    // Telegram has taken any call of it (./client.ts): so a pass cut short, as alasio stops,
    // never leaves an entry sent, or a part of one, and not marked so, to be sent again
    // after the restart. Each call of it is bounded by its timeout.
    //
    // A chat's entry is delivered by the pass that claimed it, of all the alasios' passes,
    // its claim kept in the store, so a pass whose connection is lost as it sends still holds
    // it, and no other sends it too; only a pass gone for the claim's life lets another take
    // it over. A chat whose next entry another pass claimed is that pass's to deliver, and
    // this one passes over it; what is left of it, the next pass delivers.
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
    const queued = <A>(enqueue: (traceparent: string | null) => Effect.Effect<A, StoreError>) =>
      Effect.suspend(() => enqueue(currentTraceparent())).pipe(Effect.tap(() => Effect.sync(() => wakeup.openUnsafe())));

    return Outbox.of({
      enqueueText: (text) => queued((traceparent) => store.enqueueOutboxText({ ...text, traceparent })),
      enqueuePanel: ({ conversationId, chatId, text, keyboard }) =>
        queued((traceparent) =>
          store.enqueueOutboxPanel(conversationId, keyboard.flat(), (ids) => ({ chatId, text, options: { reply_markup: inlineKeyboard(keyboard, ids) }, traceparent }))
        ),
      enqueueNotice: ({ conversationId, chatId, text }) => queued((traceparent) => store.enqueueOutboxNotice(conversationId, { chatId, text, traceparent })),
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

  /** The outbox, its passes claiming as alasio's do. */
  static readonly layer: Layer.Layer<Outbox, never, Store | TelegramClient> = Outbox.layerWith();
}
