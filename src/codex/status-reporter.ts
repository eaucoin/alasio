import { randomUUID } from "node:crypto";
import type { SqliteStore } from "../persistence/store.ts";
import { sleep } from "../shared/async.ts";
import type { Logger } from "../shared/log.ts";
import type { ChatId, Client } from "../telegram/client.ts";
import type { TelegramOutbox } from "../telegram/outbox.ts";
import type { WorkflowWait, WorkflowWakeEvent } from "../workflow/hook-server.ts";
import type { PreparedReply, ReplyMedia } from "./reply-media.ts";
import { finalResponseToMarkdown } from "./response-markdown.ts";
import { formatDuration } from "../shared/human-time.ts";

/** Final responses render as Telegram rich messages: tables, headings, lists, and code. */
const FINAL_RESPONSE_OPTIONS = Object.freeze({ format: "rich" });

/** How often a running turn's status is checked for a change worth showing (a workflow wait). */
const STATUS_CHECK_MS = 30_000;

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

/** Where delivered and undelivered responses are recorded: alasio's store. */
export type StatusReporterStore = Pick<SqliteStore, "markPendingAsPosted" | "getCompletedResponsesPendingDelivery">;

export interface StatusReporterOptions {
  readonly client: Pick<Client, "sendMessage" | "editMessageText">;
  readonly store: StatusReporterStore;
  readonly outbox: Pick<TelegramOutbox, "enqueueText">;
  readonly workflowWaits: ReadonlyMap<string, WorkflowWait>;
  readonly workflowWakeEvents: ReadonlyMap<string, WorkflowWakeEvent>;
  readonly log: Logger;
  readonly replyMedia?: Pick<ReplyMedia, "prepare"> | null;
}

/** A final response to queue for delivery, as the pending response it delivers. */
export interface FinalResponse {
  readonly chatId: ChatId;
  readonly text: string;
  readonly pendingResponseId: string | null | undefined;
}

export interface StatusUpdates {
  readonly chatId: ChatId;
  /** Ends the updates when aborted: the turn is over. */
  readonly signal: AbortSignal;
  readonly sessionId: string | null;
  /** Called with the status message's id and the turn's start once the message is posted. */
  readonly onStatusMessageCreated: (statusMessageId: number, startTime: number) => void;
  readonly harnessName?: string;
}

export interface PostedResponse {
  readonly chatId: ChatId;
  readonly response: string;
  readonly pendingResponseId: string | null | undefined;
  readonly statusMessageId: number | null;
  readonly statusStartTime: number | null;
  readonly harnessName?: string;
}

export interface UnansweredTurn {
  readonly chatId: ChatId;
  readonly pendingResponseId: string | null | undefined;
  readonly statusMessageId: number | null;
  readonly statusText?: string | null;
  readonly harnessName?: string;
}

export class StatusReporter {
  private readonly client: Pick<Client, "sendMessage" | "editMessageText">;
  private readonly store: StatusReporterStore;
  private readonly outbox: Pick<TelegramOutbox, "enqueueText">;
  private readonly workflowWaits: ReadonlyMap<string, WorkflowWait>;
  private readonly workflowWakeEvents: ReadonlyMap<string, WorkflowWakeEvent>;
  private readonly log: Logger;
  private readonly replyMedia: Pick<ReplyMedia, "prepare"> | null;

  constructor({ client, store, outbox, workflowWaits, workflowWakeEvents, log, replyMedia = null }: StatusReporterOptions) {
    if (!outbox) {
      throw new Error("StatusReporter requires a Telegram outbox");
    }
    this.client = client;
    this.store = store;
    this.outbox = outbox;
    this.workflowWaits = workflowWaits;
    this.workflowWakeEvents = workflowWakeEvents;
    this.log = log;
    // Resolves the media a response shows (codex/reply-media.ts); without it, responses
    // go as rich text only.
    this.replyMedia = replyMedia;
  }

  /**
   * Queue a final response for delivery, with any media it shows copied alongside. A
   * failure to prepare the media never holds the response back: it goes as text.
   */
  async enqueueFinalResponse({ chatId, text, pendingResponseId }: FinalResponse): Promise<void> {
    let prepared: PreparedReply = { text, options: FINAL_RESPONSE_OPTIONS };
    if (this.replyMedia) {
      try {
        prepared = await this.replyMedia.prepare({ chatId, text, key: pendingResponseId ?? randomUUID() });
      } catch (error) {
        this.log.warn(`Reply media could not be prepared; sending the response as text: ${error instanceof Error ? error.message : String(error)}`);
      }
    }
    this.outbox.enqueueText({ chatId, text: prepared.text, options: prepared.options, pendingResponseId });
  }

  async postStatusUpdates({ chatId, signal, sessionId, onStatusMessageCreated, harnessName = "Codex" }: StatusUpdates): Promise<void> {
    const startTime = Date.now();
    const statusHtml = (workflowWait: WorkflowWait | null) => workingStatusHtml({ harnessName, startedAtMs: startTime, workflowWait });
    let statusMessageId: number | null = null;
    let shown = statusHtml(null);
    try {
      const [sent] = await this.client.sendMessage(chatId, shown, { parse_mode: "HTML" });
      statusMessageId = sent?.message_id ?? null;
      if (statusMessageId) {
        onStatusMessageCreated(statusMessageId, startTime);
      }
    } catch (error) {
      this.log.warn(`Failed to post status message: ${error}`);
      return;
    }
    while (!signal.aborted) {
      try {
        const wakeEvent = sessionId ? this.workflowWakeEvents.get(sessionId) : null;
        if (wakeEvent) {
          await Promise.race([sleep(STATUS_CHECK_MS, signal), wakeEvent.promise]);
        } else {
          await sleep(STATUS_CHECK_MS, signal);
        }
      } catch {
        return;
      }
      if (signal.aborted || !statusMessageId) {
        return;
      }
      const next = statusHtml(sessionId ? this.workflowWaits.get(sessionId) ?? null : null);
      if (next === shown) {
        continue;
      }
      shown = next;
      await this.client.editMessageText(chatId, statusMessageId, next, { parse_mode: "HTML" }).catch((error) => {
        this.log.warn(`Failed to edit status message: ${error}`);
      });
    }
  }

  async postResponse({ chatId, response, pendingResponseId, statusMessageId, statusStartTime, harnessName = "Codex" }: PostedResponse): Promise<void> {
    const trimmedResponse = response.trim();
    if (statusMessageId && statusStartTime) {
      const elapsedSeconds = (Date.now() - statusStartTime) / 1000;
      await this.client.editMessageText(chatId, statusMessageId, `${harnessName} worked for ${formatDuration(elapsedSeconds)}.`).catch(() => undefined);
    }
    if (!trimmedResponse) {
      if (pendingResponseId) {
        this.store.markPendingAsPosted(pendingResponseId);
      }
      return;
    }
    await this.enqueueFinalResponse({ chatId, text: trimmedResponse, pendingResponseId });
  }

  async finishWithoutResponse({ chatId, pendingResponseId, statusMessageId, statusText = null, harnessName = "Codex" }: UnansweredTurn): Promise<void> {
    if (statusMessageId) {
      statusText = statusText ?? `${harnessName} interrupted.`;
      await this.client.editMessageText(chatId, statusMessageId, statusText).catch(() => undefined);
    }
    if (pendingResponseId) {
      this.store.markPendingAsPosted(pendingResponseId);
    }
  }

  async flushCompletedResponses(): Promise<void> {
    const completedResponses = this.store.getCompletedResponsesPendingDelivery();
    for (const completed of completedResponses) {
      const response = finalResponseToMarkdown(completed.blocks);
      if (response.trim()) {
        await this.enqueueFinalResponse({ chatId: completed.chatId, text: response, pendingResponseId: completed.id });
      } else {
        this.store.markPendingAsPosted(completed.id);
      }
    }
  }
}
