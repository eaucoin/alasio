import type { Update } from "@grammyjs/types";
import type { SqliteStore } from "../persistence/store.ts";
import { sleep } from "../shared/async.ts";
import type { Logger } from "../shared/log.ts";
import type { Client } from "./client.ts";

const POLL_BACKOFF_MS = 3_000;

/** Where the poller keeps the offset of the next update to fetch. */
type OffsetStore = Pick<SqliteStore, "getTelegramOffset" | "setTelegramOffset">;

export type ProcessUpdate = (update: Update) => Promise<void>;

export interface UpdatePollerOptions {
  client: Pick<Client, "getUpdates">;
  store: OffsetStore;
  processUpdate: ProcessUpdate;
  log: Logger;
}

export class UpdatePoller {
  private readonly client: Pick<Client, "getUpdates">;
  private readonly store: OffsetStore;
  private readonly processUpdate: ProcessUpdate;
  private readonly log: Logger;
  private abortController: AbortController;
  private promise: Promise<void> | null;

  constructor({ client, store, processUpdate, log }: UpdatePollerOptions) {
    this.client = client;
    this.store = store;
    this.processUpdate = processUpdate;
    this.log = log;
    this.abortController = new AbortController();
    this.promise = null;
  }

  start(): Promise<void> {
    if (this.abortController.signal.aborted) {
      this.abortController = new AbortController();
    }
    this.promise = this.loop();
    return this.promise;
  }

  async stop(): Promise<void> {
    this.abortController.abort();
    try {
      await this.promise;
    } catch {
      // Expected when the abort signal stops long polling.
    }
  }

  async loop(): Promise<void> {
    while (!this.abortController.signal.aborted) {
      try {
        const updates = await this.client.getUpdates({
          offset: this.store.getTelegramOffset(),
          timeout: 50,
          allowedUpdates: ["message", "callback_query"],
          signal: this.abortController.signal,
        });
        for (const update of updates) {
          try {
            await this.processUpdate(update);
          } catch (error) {
            // The raw update is already persisted before processing, so skipping it
            // loses nothing durable; re-fetching it forever would wedge the bot.
            this.log.error(`Skipping update ${update.update_id} after processing failure: ${error instanceof Error ? error.stack ?? error.message : String(error)}`);
          }
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
