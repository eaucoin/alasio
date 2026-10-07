import { Clock, Deferred, Effect, Schedule, type Scope } from "effect";

import type { StoreError } from "../persistence/sql.ts";
import { Store } from "../persistence/store.ts";
import { formatDuration } from "../shared/human-time.ts";
import { type ChatId, TelegramClient } from "../telegram/client.ts";
import { Outbox } from "../telegram/outbox.ts";
import { type WorkflowWait, WorkflowHooks } from "../workflow/hook-server.ts";
import type { PreparedReply, ReplyMedia } from "./reply-media.ts";
import { finalResponseToMarkdown } from "./response-markdown.ts";

/** Final responses render as Telegram rich messages: tables, headings, lists, and code. */
const FINAL_RESPONSE_OPTIONS = Object.freeze({ format: "rich" });

/** How often a running turn's status is checked for a change worth showing (a workflow wait). */
const STATUS_CHECK = "30 seconds";

function escapeHtml(value: string): string {
  return String(value).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

export interface WorkingStatus {
  readonly harnessName: string;
  readonly startedAtMs: number;
  readonly workflowWait?: Pick<WorkflowWait, "runId" | "waitType"> | null;
}

/**
 * A running turn's status line. The start is a Telegram date-time entity in relative form
 * ("started 3 minutes ago"), which the user's app keeps current itself, so the line needs
 * no edits to show how long the turn has run; it changes only when what it says does.
 */
export function workingStatusHtml({ harnessName, startedAtMs, workflowWait = null }: WorkingStatus): string {
  const unix = Math.floor(startedAtMs / 1000);
  const since = `started <tg-time unix="${unix}" format="r">just now</tg-time>`;
  const doing = workflowWait
    ? `${escapeHtml(harnessName)} is watching workflow ${escapeHtml(workflowWait.runId)} (${escapeHtml(workflowWait.waitType)})`
    : `${escapeHtml(harnessName)} is working`;
  return `${doing} · ${since}`;
}

/** What a turn's status message says when alasio stops during the turn, which it continues after the restart. */
function restartingStatusText(harnessName: string): string {
  return `alasio is restarting; ${harnessName} continues this turn after the restart.`;
}

/** A final response to queue for delivery, as the pending response it delivers. */
export interface FinalResponse {
  readonly chatId: ChatId;
  readonly text: string;
  readonly pendingResponseId: string | null | undefined;
}

/** A turn's status message, once posted: its id, and when the turn started. */
export interface PostedStatus {
  readonly messageId: number;
  readonly startTime: number;
}

export interface StatusUpdates {
  readonly chatId: ChatId;
  readonly sessionId: string | null;
  readonly harnessName?: string;
}

export interface PostedResponse {
  readonly chatId: ChatId;
  readonly response: string;
  readonly pendingResponseId: string | null | undefined;
  readonly status: PostedStatus | null;
  readonly harnessName?: string;
}

export interface UnansweredTurn {
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
   * Posts the turn's status message and keeps it current, in a fiber of the scope, until
   * the scope closes. The status message once posted; null if it could not be.
   */
  readonly statusUpdates: (updates: StatusUpdates) => Effect.Effect<Deferred.Deferred<PostedStatus | null>, never, Scope.Scope>;
  readonly postResponse: (response: PostedResponse) => Effect.Effect<void, StoreError>;
  readonly finishWithoutResponse: (turn: UnansweredTurn) => Effect.Effect<void, StoreError>;
  /** Says on the status message that alasio is restarting and the turn continues after it. */
  readonly restarting: (chatId: ChatId, status: PostedStatus | null, harnessName: string) => Effect.Effect<void>;
  /** Queues every completed response that has not been delivered. */
  readonly flushCompletedResponses: Effect.Effect<void, StoreError>;
}

/**
 * The status reporter of alasio's store, Telegram client, outbox and workflow hooks. With
 * `replyMedia` (codex/reply-media.ts), the media a response shows are delivered with it;
 * without, responses go as rich text only.
 */
export const makeStatusReporter = Effect.fnUntraced(function*(
  { replyMedia = null }: { readonly replyMedia?: Pick<ReplyMedia, "prepare"> | null } = {},
): Effect.fn.Return<StatusReporter, never, Store | TelegramClient | Outbox | WorkflowHooks> {
  const store = yield* Store;
  const client = yield* TelegramClient;
  const outbox = yield* Outbox;
  const { waits } = yield* WorkflowHooks;

  const enqueueFinalResponse = Effect.fnUntraced(function*({ chatId, text, pendingResponseId }: FinalResponse) {
    const prepared: PreparedReply = replyMedia ? yield* replyMedia.prepare({ chatId, text }) : { text, options: FINAL_RESPONSE_OPTIONS };
    yield* outbox.enqueueText({ chatId, text: prepared.text, options: prepared.options, pendingResponseId });
  });

  /** Keeps a posted status message current: every STATUS_CHECK, it is edited if what it says has changed. */
  const keepCurrent = (chatId: ChatId, sessionId: string | null, shownAtFirst: string, statusHtml: (wait: WorkflowWait | null) => string, messageId: number): Effect.Effect<void> => {
    let shown = shownAtFirst;
    const check = Effect.suspend(() => {
      const next = statusHtml(sessionId ? waits.get(sessionId) ?? null : null);
      if (next === shown) {
        return Effect.void;
      }
      shown = next;
      return client.editMessageText(chatId, messageId, next, { parse_mode: "HTML" }).pipe(
        Effect.catch((error) => Effect.logWarning(`Failed to edit status message: ${error}`)),
      );
    });
    return check.pipe(Effect.schedule(Schedule.spaced(STATUS_CHECK)), Effect.asVoid);
  };

  const statusUpdates = Effect.fnUntraced(function*({ chatId, sessionId, harnessName = "Codex" }: StatusUpdates) {
    const posted = yield* Deferred.make<PostedStatus | null>();
    const startTime = yield* Clock.currentTimeMillis;
    const statusHtml = (workflowWait: WorkflowWait | null) => workingStatusHtml({ harnessName, startedAtMs: startTime, workflowWait });
    const shown = statusHtml(null);
    // The message is posted whole however soon the turn ends; only keeping it current is interrupted.
    const post = client.sendMessage(chatId, shown, { parse_mode: "HTML" }).pipe(
      Effect.map(([sent]) => (sent?.message_id ? { messageId: sent.message_id, startTime } : null)),
      Effect.catch((error) => Effect.logWarning(`Failed to post status message: ${error}`).pipe(Effect.as(null))),
      Effect.tap((status) => Deferred.succeed(posted, status)),
      Effect.uninterruptible,
    );
    yield* post.pipe(
      Effect.flatMap((status) => (status ? keepCurrent(chatId, sessionId, shown, statusHtml, status.messageId) : Effect.void)),
      Effect.ensuring(Deferred.succeed(posted, null)),
      Effect.forkScoped,
    );
    return posted;
  });

  const postResponse = Effect.fnUntraced(function*({ chatId, response, pendingResponseId, status, harnessName = "Codex" }: PostedResponse) {
    const trimmedResponse = response.trim();
    if (status) {
      const elapsedSeconds = ((yield* Clock.currentTimeMillis) - status.startTime) / 1000;
      yield* client.editMessageText(chatId, status.messageId, `${harnessName} worked for ${formatDuration(elapsedSeconds)}.`).pipe(Effect.ignore);
    }
    if (!trimmedResponse) {
      if (pendingResponseId) {
        yield* store.markPendingAsPosted(pendingResponseId);
      }
      return;
    }
    yield* enqueueFinalResponse({ chatId, text: trimmedResponse, pendingResponseId });
  });

  const finishWithoutResponse = Effect.fnUntraced(function*({ chatId, pendingResponseId, status, statusText = null, harnessName = "Codex" }: UnansweredTurn) {
    if (status) {
      yield* client.editMessageText(chatId, status.messageId, statusText ?? `${harnessName} interrupted.`).pipe(Effect.ignore);
    }
    if (pendingResponseId) {
      yield* store.markPendingAsPosted(pendingResponseId);
    }
  });

  return {
    enqueueFinalResponse,
    statusUpdates,
    postResponse,
    finishWithoutResponse,
    restarting: (chatId, status, harnessName) =>
      status ? client.editMessageText(chatId, status.messageId, restartingStatusText(harnessName)).pipe(Effect.ignore) : Effect.void,
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
