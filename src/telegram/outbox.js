import { rmSync } from "node:fs";

import { currentTraceparent, inSpan, meter } from "../telemetry/index.js";

const DEFAULT_RETRY_MS = 10_000;
const MAX_RETRY_MS = 5 * 60_000;

const deliveryLag = meter.createHistogram("alasio.delivery.lag", {
  description: "Time from a reply entering the outbox to Telegram accepting it, retries included",
  unit: "s",
});

export class TelegramOutbox {
  constructor({ client, store, log }) {
    this.client = client;
    this.store = store;
    this.log = log;
    this.timer = null;
    this.flushPromise = null;
    meter.createObservableGauge("alasio.outbox.pending", {
      description: "Replies waiting in the outbox for delivery to Telegram",
      unit: "{reply}",
    }).addCallback((result) => result.observe(this.store.getPendingOutboxCount()));
  }

  start() {
    if (!this.timer) {
      this.timer = setInterval(() => void this.flushDue(), 5_000);
      this.timer.unref?.();
    }
    void this.flushDue();
  }

  stop() {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  /** Queues a reply; its delivery joins the trace it is queued in. */
  enqueueText(args) {
    const id = this.store.enqueueOutboxText({ ...args, traceparent: currentTraceparent() });
    void this.flushDue();
    return id;
  }

  flushDue() {
    if (this.flushPromise) {
      return this.flushPromise;
    }
    this.flushPromise = this.flushInner().finally(() => {
      this.flushPromise = null;
    });
    return this.flushPromise;
  }

  async flushInner() {
    for (const item of this.store.getDueOutbox(20)) {
      try {
        await inSpan("alasio.delivery", {
          parent: item.traceparent,
          attributes: { "alasio.outbox.id": item.id, "alasio.delivery.attempt": item.attempts + 1, "telegram.chat.id": item.chat_id },
        }, () => this.deliver(item));
      } catch {
        // Deferred; the rest wait behind it so replies keep their order.
        break;
      }
    }
  }

  /** Sends one reply, or defers it with backoff and throws what stopped it. */
  async deliver(item) {
    try {
      await this.client.sendMessage(item.chat_id, item.text, item.options);
    } catch (error) {
      const retryAfterMs = Number(error?.retryAfterMs) || 0;
      const exponentialMs = Math.min(DEFAULT_RETRY_MS * (2 ** Math.min(item.attempts, 5)), MAX_RETRY_MS);
      const delayMs = Math.max(retryAfterMs, exponentialMs);
      this.store.rescheduleOutbox(item.id, error, delayMs);
      this.log.warn(`Telegram outbox delivery deferred id=${item.id} delay_ms=${delayMs} error=${error instanceof Error ? error.message : String(error)}`);
      throw error;
    }
    this.store.markOutboxSent(item.id);
    deliveryLag.record((Date.now() - Date.parse(item.created_at)) / 1000);
    // The copies of the media a reply showed (codex/reply-media.js) are its own.
    if (item.options?.mediaDir) {
      rmSync(item.options.mediaDir, { recursive: true, force: true });
    }
  }
}
