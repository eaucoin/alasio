import { sleep } from "../shared/async.js";

const POLL_BACKOFF_MS = 3_000;

export class UpdatePoller {
  constructor({ client, store, processUpdate, log }) {
    this.client = client;
    this.store = store;
    this.processUpdate = processUpdate;
    this.log = log;
    this.abortController = new AbortController();
    this.promise = null;
  }

  start() {
    if (this.abortController.signal.aborted) {
      this.abortController = new AbortController();
    }
    this.promise = this.loop();
    return this.promise;
  }

  async stop() {
    this.abortController.abort();
    try {
      await this.promise;
    } catch {
      // Expected when the abort signal stops long polling.
    }
  }

  async loop() {
    while (!this.abortController.signal.aborted) {
      try {
        const updates = await this.client.getUpdates({
          offset: this.store.getTelegramOffset(),
          timeout: 50,
          allowedUpdates: ["message", "callback_query"],
          signal: this.abortController.signal,
        });
        for (const update of updates) {
          await this.processUpdate(update);
          this.store.setTelegramOffset(update.update_id + 1);
        }
      } catch (error) {
        if (this.abortController.signal.aborted) {
          return;
        }
        this.log.error(`Polling failed: ${error}`);
        await sleep(POLL_BACKOFF_MS, this.abortController.signal).catch(() => undefined);
      }
    }
  }
}
