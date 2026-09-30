import { rmSync } from "node:fs";

const DEFAULT_RETRY_MS = 10_000;
const MAX_RETRY_MS = 5 * 60_000;

export class TelegramOutbox {
  constructor({ client, store, log }) {
    this.client = client;
    this.store = store;
    this.log = log;
    this.timer = null;
    this.flushPromise = null;
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

  enqueueText(args) {
    const id = this.store.enqueueOutboxText(args);
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
        await this.client.sendMessage(item.chat_id, item.text, item.options);
        this.store.markOutboxSent(item.id);
        // The copies of the media a reply showed (codex/reply-media.js) are its own.
        if (item.options?.mediaDir) {
          rmSync(item.options.mediaDir, { recursive: true, force: true });
        }
      } catch (error) {
        const retryAfterMs = Number(error?.retryAfterMs) || 0;
        const exponentialMs = Math.min(DEFAULT_RETRY_MS * (2 ** Math.min(item.attempts, 5)), MAX_RETRY_MS);
        const delayMs = Math.max(retryAfterMs, exponentialMs);
        this.store.rescheduleOutbox(item.id, error, delayMs);
        this.log.warn(`Telegram outbox delivery deferred id=${item.id} delay_ms=${delayMs} error=${error instanceof Error ? error.message : String(error)}`);
        break;
      }
    }
  }
}
