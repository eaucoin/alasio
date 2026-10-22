import { Clock, Deferred, Effect, Fiber, Schedule, type Scope, Semaphore } from "effect";

import type { StoreError } from "../persistence/sql.ts";
import { Store } from "../persistence/store.ts";
import { formatDuration } from "../shared/human-time.ts";
import type { ChatId } from "../telegram/client.ts";
import { Outbox } from "../telegram/outbox.ts";
import type { PreparedReply, ReplyMedia } from "./reply-media.ts";
import { finalResponseToMarkdown } from "./response-markdown.ts";

/** Final responses render as Telegram rich messages: tables, headings, lists, and code. */
const FINAL_RESPONSE_OPTIONS = Object.freeze({ format: "rich" });

/** How often a running turn's row is read for a change of its status worth showing (a workflow wait). */
const STATUS_CHECK = "30 seconds";
/** How long alasio, as it stops, waits for a turn's status message to say so. */
const RESTARTING_DELIVERY = "5 seconds";

function escapeHtml(value: string): string {
  return String(value).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

export interface WorkingStatus {
  readonly harnessName: string;
  readonly startedAtMs: number;
  /** What the turn waits on, as its row says (turns.waiting_on); null while it works. */
  readonly waitingOn?: string | null;
}

/**
 * A running turn's status line. The start is a Telegram date-time entity in relative form
 * ("started 3 minutes ago"), which the user's app keeps current itself, so the line needs
 * no edits to show how long the turn has run; it changes only when what it says does.
 */
export function workingStatusHtml({ harnessName, startedAtMs, waitingOn = null }: WorkingStatus): string {
  const unix = Math.floor(startedAtMs / 1000);
  const since = `started <tg-time unix="${unix}" format="r">just now</tg-time>`;
  const doing = waitingOn ? `${escapeHtml(harnessName)} is waiting on ${escapeHtml(waitingOn)}` : `${escapeHtml(harnessName)} is working`;
  return `${doing} · ${since}`;
}

/** What a turn's status message says when alasio stops during the turn, which its pod runs on, its reply following the restart. */
function restartingStatusText(harnessName: string): string {
  return `alasio is restarting; ${harnessName} goes on with this turn, and its reply follows the restart.`;
}

/** What a turn's status message says when alasio does not reach its pod, which it asks again. */
function unreachedStatusText(harnessName: string): string {
  return `${harnessName}'s harness could not be reached; alasio asks it again for this turn.`;
}

/** A final response to queue for delivery, as the pending response it delivers. */
export interface FinalResponse {
  readonly chatId: ChatId;
  readonly text: string;
  readonly pendingResponseId: string | null | undefined;
}

/** A turn's status message, once queued: its outbox entry, which its edits name, and when the turn started. */
export interface PostedStatus {
  readonly outboxId: string;
  readonly startTime: number;
}

/** The turn whose status message to queue and keep current: the conversation's of `epoch`, in the chat; its message the one posted `ahead` of it, where one was. */
export interface StatusUpdates {
  readonly chatId: ChatId;
  readonly conversationId: string;
  readonly epoch: number;
  readonly harnessName?: string;
  readonly ahead?: StatusAhead | undefined;
}

/**
 * A turn's status message posted ahead of the turn, while what it runs on is readied before it
 * can start, as a harness pool's pod is bound to its conversation (StatusReporter.ahead).
 */
export interface StatusAhead {
  /** Says on the message what the turn waits on, posting it the first time it waits on anything; null once it waits on nothing. */
  readonly waiting: (what: string | null) => Effect.Effect<void>;
  /** The message, once posted; null while none was. */
  readonly posted: Effect.Effect<PostedStatus | null>;
  /** Says `text` on the message, where one was posted, as the turn it was posted for does not start. */
  readonly end: (text: string) => Effect.Effect<void>;
}

/** A turn's reply to queue: of the conversation `conversationId`, whose host answered with `pendingResponseId`. */
export interface PostedResponse {
  readonly conversationId: string;
  readonly chatId: ChatId;
  readonly response: string;
  readonly pendingResponseId: string | null | undefined;
  readonly status: PostedStatus | null;
  readonly harnessName?: string;
}

/** A turn that ended without its reply: of the conversation `conversationId`, whose host answered with `pendingResponseId`. */
export interface UnansweredTurn {
  readonly conversationId: string;
  readonly chatId: ChatId;
  readonly pendingResponseId: string | null | undefined;
  readonly status: PostedStatus | null;
  readonly statusText?: string | null;
  readonly harnessName?: string;
}

/** What a turn shows the operator: its status message as it runs and as it ends, and its reply. */
export interface StatusReporter {
  /**
   * Queues a final response for delivery, with the media it shows. A file that cannot
   * be attached never holds the response back: the response says why in its place.
   */
  readonly enqueueFinalResponse: (response: FinalResponse) => Effect.Effect<void, StoreError>;
  /**
   * Queues the turn's status message, recorded on its row, and keeps it current as its row
   * says what it does, in a fiber of the scope, until the scope closes. The status message
   * once queued; null if it could not be.
   */
  readonly statusUpdates: (updates: StatusUpdates) => Effect.Effect<Deferred.Deferred<PostedStatus | null>, never, Scope.Scope>;
  /**
   * A status message of `harnessName`'s turn in the chat, posted ahead of it as soon as it
   * waits on anything before it starts, so the operator sees it waits from the first, and taken
   * on as its status message once it starts (StatusUpdates.ahead); none posted where it waits
   * on nothing.
   */
  readonly ahead: (chatId: ChatId, harnessName: string) => Effect.Effect<StatusAhead>;
  readonly postResponse: (response: PostedResponse) => Effect.Effect<void, StoreError>;
  readonly finishWithoutResponse: (turn: UnansweredTurn) => Effect.Effect<void, StoreError>;
  /** Says on the status message that alasio is restarting and the turn goes on, its reply following the restart. */
  readonly restarting: (chatId: ChatId, status: PostedStatus | null, harnessName: string) => Effect.Effect<void>;
  /** Says on the status message that alasio did not reach the turn's pod, which it asks again. */
  readonly unreached: (chatId: ChatId, status: PostedStatus | null, harnessName: string) => Effect.Effect<void>;
  /** Queues every completed response that has not been delivered. */
  readonly flushCompletedResponses: Effect.Effect<void, StoreError>;
}

/**
 * The status reporter of alasio's store and outbox: what a turn shows is
 * queued in the outbox, its status message and that message's edits as its reply, each
 * chat's in order, so none of it is lost to a restart. With `replyMedia`
 * (codex/reply-media.ts), the media a response shows are delivered with it; without,
 * responses go as rich text only.
 */
export const makeStatusReporter = Effect.fnUntraced(function*(
  { replyMedia = null }: { readonly replyMedia?: Pick<ReplyMedia, "prepare"> | null } = {},
): Effect.fn.Return<StatusReporter, never, Store | Outbox> {
  const store = yield* Store;
  const outbox = yield* Outbox;

  const enqueueFinalResponse = Effect.fnUntraced(function*({ chatId, text, pendingResponseId }: FinalResponse) {
    const prepared: PreparedReply = replyMedia ? yield* replyMedia.prepare({ chatId, text }) : { text, options: FINAL_RESPONSE_OPTIONS };
    yield* outbox.enqueueText({ chatId, text: prepared.text, options: prepared.options, pendingResponseId });
  });

  /** Queues an edit of the status message; one that cannot be queued is said, and left out. */
  const edit = (chatId: ChatId, status: PostedStatus, text: string, options: { readonly parse_mode?: "HTML" } = {}): Effect.Effect<void> =>
    outbox.enqueueEdit(status.outboxId, { chatId, text, options }).pipe(
      Effect.asVoid,
      Effect.catch((error) => Effect.logWarning(`Failed to queue an edit of the status message: ${error.message}`)),
    );

  /**
   * Keeps a queued status message current: every STATUS_CHECK, the turn's row is read, and
   * the message edited if what the row says the turn does has changed. A turn its
   * conversation no longer runs (ended, or claimed since) changes it no more.
   */
  const keepCurrent = (
    { chatId, conversationId, epoch }: StatusUpdates,
    shownAtFirst: string,
    statusHtml: (waitingOn: string | null) => string,
    status: PostedStatus,
  ): Effect.Effect<void> => {
    let shown = shownAtFirst;
    const check = store.getTurn(conversationId).pipe(
      Effect.flatMap((turn) => {
        if (!turn || Number(turn.epoch) !== epoch || turn.state !== "active") {
          return Effect.void;
        }
        const next = statusHtml(turn.status === "waiting" ? turn.waiting_on : null);
        if (next === shown) {
          return Effect.void;
        }
        shown = next;
        return edit(chatId, status, next, { parse_mode: "HTML" });
      }),
      Effect.catch((error) => Effect.logWarning(`Could not read what turn ${conversationId} does: ${error.message}`)),
    );
    return check.pipe(Effect.schedule(Schedule.spaced(STATUS_CHECK)), Effect.asVoid);
  };

  const statusUpdates = Effect.fnUntraced(function*(updates: StatusUpdates) {
    const { chatId, conversationId, epoch, harnessName = "Codex", ahead } = updates;
    const posted = yield* Deferred.make<PostedStatus | null>();
    const early = ahead ? yield* ahead.posted : null;
    const startTime = early?.startTime ?? (yield* Clock.currentTimeMillis);
    const statusHtml = (waitingOn: string | null) => workingStatusHtml({ harnessName, startedAtMs: startTime, waitingOn });
    const shown = statusHtml(null);
    // The message is queued, or the one posted ahead taken on, which says what it shows now,
    // and recorded on the turn's row for a start after a crash to edit, whole however soon the
    // turn ends; only keeping it current is interrupted.
    const post = (early ? Effect.succeed(early.outboxId) : outbox.enqueueStatus({ chatId, text: shown, options: { parse_mode: "HTML" } })).pipe(
      Effect.tap((outboxId) =>
        store.setTurnStatusMessage(conversationId, epoch, outboxId).pipe(
          Effect.catch((error) => Effect.logWarning(`Failed to record the status message on turn ${conversationId}: ${error.message}`)),
        )
      ),
      Effect.map((outboxId): PostedStatus | null => ({ outboxId, startTime })),
      Effect.catch((error) => Effect.logWarning(`Failed to queue the status message: ${error.message}`).pipe(Effect.as(null))),
      Effect.tap((status) => Deferred.succeed(posted, status)),
      Effect.uninterruptible,
    );
    yield* post.pipe(
      Effect.flatMap((status) => (status ? keepCurrent(updates, shown, statusHtml, status) : Effect.void)),
      Effect.ensuring(Deferred.succeed(posted, null)),
      Effect.forkScoped,
    );
    return posted;
  });

  const ahead = (chatId: ChatId, harnessName: string): Effect.Effect<StatusAhead> =>
    Effect.gen(function*() {
      const startTime = yield* Clock.currentTimeMillis;
      const statusHtml = (waitingOn: string | null) => workingStatusHtml({ harnessName, startedAtMs: startTime, waitingOn });
      const oneAtATime = yield* Semaphore.make(1);
      let posted: PostedStatus | null = null;
      let shown = statusHtml(null);
      return {
        waiting: (what) =>
          Effect.gen(function*() {
            const next = statusHtml(what);
            if (posted === null) {
              if (what === null) return;
              const outboxId = yield* outbox.enqueueStatus({ chatId, text: next, options: { parse_mode: "HTML" } }).pipe(
                Effect.catch((error) => Effect.as(Effect.logWarning(`Failed to queue the status message: ${error.message}`), null)),
              );
              if (outboxId !== null) [posted, shown] = [{ outboxId, startTime }, next];
              return;
            }
            if (next === shown) return;
            shown = next;
            yield* edit(chatId, posted, next, { parse_mode: "HTML" });
          }).pipe(Semaphore.withPermit(oneAtATime)),
        posted: Effect.sync(() => posted),
        end: (text) => Effect.suspend(() => (posted ? edit(chatId, posted, text) : Effect.void)).pipe(Semaphore.withPermit(oneAtATime)),
      };
    });

  const postResponse = Effect.fnUntraced(function*({ conversationId, chatId, response, pendingResponseId, status, harnessName = "Codex" }: PostedResponse) {
    const trimmedResponse = response.trim();
    if (status) {
      const elapsedSeconds = ((yield* Clock.currentTimeMillis) - status.startTime) / 1000;
      yield* edit(chatId, status, `${harnessName} worked for ${formatDuration(elapsedSeconds)}.`);
    }
    if (!trimmedResponse) {
      if (pendingResponseId) {
        yield* store.markAnswerPosted(conversationId, pendingResponseId);
      }
      return;
    }
    yield* enqueueFinalResponse({ chatId, text: trimmedResponse, pendingResponseId });
  });

  const finishWithoutResponse = Effect.fnUntraced(function*({ conversationId, chatId, pendingResponseId, status, statusText = null, harnessName = "Codex" }: UnansweredTurn) {
    if (status) {
      yield* edit(chatId, status, statusText ?? `${harnessName} interrupted.`);
    }
    if (pendingResponseId) {
      yield* store.markAnswerPosted(conversationId, pendingResponseId);
    }
  });

  return {
    enqueueFinalResponse,
    statusUpdates,
    ahead,
    postResponse,
    finishWithoutResponse,
    restarting: (chatId, status, harnessName) =>
      status
        // Delivered now if it can be, as alasio stops, by its passes, the outbox's own among
        // them; else after the restart, before anything the turn sends then. The wait is
        // RESTARTING_DELIVERY at most: the passes go on detached, so an entry one is
        // delivering then, which is not cut short, holds alasio's stopping back no longer.
        ? edit(chatId, status, restartingStatusText(harnessName)).pipe(
          Effect.andThen(Effect.forkDetach(outbox.deliverChat(chatId))),
          Effect.flatMap((pass) => Fiber.await(pass).pipe(Effect.timeoutOption(RESTARTING_DELIVERY))),
          Effect.asVoid,
        )
        : Effect.void,
    unreached: (chatId, status, harnessName) => (status ? edit(chatId, status, unreachedStatusText(harnessName)) : Effect.void),
    flushCompletedResponses: Effect.gen(function*() {
      for (const completed of yield* store.getCompletedResponsesPendingDelivery) {
        const response = finalResponseToMarkdown(completed.blocks);
        if (response.trim()) {
          yield* enqueueFinalResponse({ chatId: completed.chatId, text: response, pendingResponseId: completed.id });
        } else {
          yield* store.markPendingAsPosted(completed.id);
        }
      }
    }),
  };
});
