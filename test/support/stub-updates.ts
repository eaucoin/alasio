/**
 * Telegram's updates for alasio's own tests, which run no log: the Telegram stand-in's
 * (test/e2e/telegram-stub.ts), taken as alasio takes its topic's (src/telegram/
 * update-consumer.ts), the stand-in keeping where its group was last told, so an alasio
 * restarted goes on from there as it does on the log.
 */
import type { Update } from "@grammyjs/types";
import { Effect, Layer } from "effect";

import { TelegramUpdates, type UpdateFeed, UpdateFeedError } from "../../src/telegram/update-consumer.ts";

/** The stand-in at `apiRoot`'s updates, as alasio's group on its topic. */
export function stubUpdates(apiRoot: string): Layer.Layer<TelegramUpdates> {
  const join = async (): Promise<UpdateFeed> => {
    const { updateId } = (await (await fetch(`${apiRoot}/control/committed`)).json()) as { updateId: number };
    let after = updateId;
    let stopped = false;
    return {
      async next() {
        for (;;) {
          if (stopped) throw new Error("the stand-in's feed was closed");
          const updates = (await (await fetch(`${apiRoot}/control/updates?after=${after}`)).json()) as Update[];
          const [update] = updates;
          if (!update) continue;
          after = update.update_id;
          return {
            update,
            position: `stand-in/${update.update_id}`,
            commit: async () => {
              await fetch(`${apiRoot}/control/commit`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ updateId: update.update_id }) });
            },
          };
        }
      },
      async close() {
        stopped = true;
      },
    };
  };
  return Layer.succeed(TelegramUpdates, TelegramUpdates.of({
    join: Effect.tryPromise({ try: join, catch: (error) => new UpdateFeedError({ message: error instanceof Error ? error.message : String(error) }) }),
  }));
}
