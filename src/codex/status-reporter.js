import { sleep } from "../shared/async.js";
import { finalResponseToMarkdown } from "./response-markdown.js";
import { formatDuration } from "../shared/human-time.js";

/** Final responses render as Telegram rich messages: tables, headings, lists, and code. */
const FINAL_RESPONSE_OPTIONS = Object.freeze({ format: "rich" });

/** How often a running turn's status is checked for a change worth showing (a workflow wait). */
const STATUS_CHECK_MS = 30_000;

function escapeHtml(value) {
  return String(value).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

/**
 * A running turn's status line. The start is a Telegram date-time entity in relative form
 * ("started 3 minutes ago"), which the user's app keeps current itself, so the line needs
 * no edits to show how long the turn has run; it changes only when what it says does.
 */
export function workingStatusHtml({ harnessName, startedAtMs, workflowWait = null }) {
  const unix = Math.floor(startedAtMs / 1000);
  const since = `started <tg-time unix="${unix}" format="r">just now</tg-time>`;
  const doing = workflowWait
    ? `${escapeHtml(harnessName)} is watching workflow ${escapeHtml(workflowWait.runId)} (${escapeHtml(workflowWait.waitType)})`
    : `${escapeHtml(harnessName)} is working`;
  return `${doing} · ${since}`;
}

export class StatusReporter {
  constructor({ client, store, outbox, workflowWaits, workflowWakeEvents, log }) {
    if (!outbox) {
      throw new Error("StatusReporter requires a Telegram outbox");
    }
    this.client = client;
    this.store = store;
    this.outbox = outbox;
    this.workflowWaits = workflowWaits;
    this.workflowWakeEvents = workflowWakeEvents;
    this.log = log;
  }

  async postStatusUpdates({ chatId, signal, sessionId, onStatusMessageCreated, harnessName = "Codex" }) {
    const startTime = Date.now();
    const statusHtml = (workflowWait) => workingStatusHtml({ harnessName, startedAtMs: startTime, workflowWait });
    let statusMessageId = null;
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

  async postResponse({ chatId, response, pendingResponseId, statusMessageId, statusStartTime, harnessName = "Codex" }) {
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
    this.outbox.enqueueText({ chatId, text: trimmedResponse, options: FINAL_RESPONSE_OPTIONS, pendingResponseId });
  }

  async finishWithoutResponse({ chatId, pendingResponseId, statusMessageId, statusText = null, harnessName = "Codex" }) {
    if (statusMessageId) {
      statusText = statusText ?? `${harnessName} interrupted.`;
      await this.client.editMessageText(chatId, statusMessageId, statusText).catch(() => undefined);
    }
    if (pendingResponseId) {
      this.store.markPendingAsPosted(pendingResponseId);
    }
  }

  async flushCompletedResponses() {
    const completedResponses = this.store.getCompletedResponsesPendingDelivery();
    for (const completed of completedResponses) {
      const response = finalResponseToMarkdown(completed.blocks);
      if (response.trim()) {
        this.outbox.enqueueText({ chatId: completed.chatId, text: response, options: FINAL_RESPONSE_OPTIONS, pendingResponseId: completed.id });
      } else {
        this.store.markPendingAsPosted(completed.id);
      }
    }
  }
}
