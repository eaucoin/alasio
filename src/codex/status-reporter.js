import { sleep } from "../shared/async.js";
import { finalResponseToMarkdown } from "./response-markdown.js";
import { formatDuration } from "../shared/human-time.js";

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
    let statusMessageId = null;
    try {
      const [sent] = await this.client.sendMessage(chatId, `${harnessName} is now working...`);
      statusMessageId = sent?.message_id ?? null;
      if (statusMessageId) {
        onStatusMessageCreated(statusMessageId, startTime);
      }
    } catch (error) {
      this.log.warn(`Failed to post status message: ${error}`);
      return;
    }
    let waitMs = 60_000;
    while (!signal.aborted) {
      try {
        const wakeEvent = sessionId ? this.workflowWakeEvents.get(sessionId) : null;
        if (wakeEvent) {
          await Promise.race([sleep(waitMs, signal), wakeEvent.promise]);
        } else {
          await sleep(waitMs, signal);
        }
      } catch {
        return;
      }
      if (signal.aborted || !statusMessageId) {
        return;
      }
      const elapsedSeconds = (Date.now() - startTime) / 1000;
      const workflowWait = sessionId ? this.workflowWaits.get(sessionId) : null;
      const text = workflowWait
        ? `${harnessName} is watching workflow ${workflowWait.runId} (${workflowWait.waitType}).`
        : `${harnessName} has been working for ${formatDuration(elapsedSeconds)}.`;
      await this.client.editMessageText(chatId, statusMessageId, text).catch((error) => {
        this.log.warn(`Failed to edit status message: ${error}`);
      });
      waitMs = Math.min(waitMs * 2, 16 * 60_000);
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
    this.outbox.enqueueText({ chatId, text: trimmedResponse, pendingResponseId });
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
        this.outbox.enqueueText({ chatId: completed.chatId, text: response, pendingResponseId: completed.id });
      } else {
        this.store.markPendingAsPosted(completed.id);
      }
    }
  }
}
