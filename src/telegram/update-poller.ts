import type { Update } from "@grammyjs/types";
import { Effect } from "effect";

import { Store } from "../persistence/store.ts";
import { withLogScope } from "../shared/log.ts";
import { TelegramClient } from "./client.ts";

/** How long polling waits after a failed poll before the next. */
const POLL_BACKOFF = "3 seconds";

/** What is done with each update, as the app's code not yet written in Effect does it. */
export type ProcessUpdate = (update: Update) => Promise<void>;

/**
 * Long-polls Telegram for updates and hands each to `processUpdate` in turn, until it is
 * interrupted. Where to poll from is kept in the store, past each update handled, so a
 * restart neither loses nor repeats one; an update being handled when polling is
 * interrupted is finished first.
 */
export const pollUpdates = Effect.fnUntraced(
  function*(processUpdate: ProcessUpdate): Effect.fn.Return<never, never, TelegramClient | Store> {
    const client = yield* TelegramClient;
    const store = yield* Store;

    const handle = (update: Update): Effect.Effect<void> =>
      Effect.tryPromise(() => processUpdate(update)).pipe(
        // The raw update is already persisted before processing, so skipping it loses
        // nothing durable; re-fetching it forever would wedge the bot.
        Effect.catch(({ cause }) =>
          Effect.logError(`Skipping update ${update.update_id} after processing failure: ${cause instanceof Error ? cause.stack ?? cause.message : String(cause)}`)),
        Effect.andThen(Effect.sync(() => store.setTelegramOffset(update.update_id + 1))),
        Effect.uninterruptible,
      );

    const poll = Effect.suspend(() => client.getUpdates({
      offset: store.getTelegramOffset(),
      timeout: 50,
      allowedUpdates: ["message", "callback_query"],
    })).pipe(
      Effect.flatMap((updates) => Effect.forEach(updates, handle, { discard: true })),
      Effect.catch((error) => Effect.logError(`Polling failed: ${error}`).pipe(Effect.andThen(Effect.sleep(POLL_BACKOFF)))),
    );

    return yield* Effect.forever(poll);
  },
  withLogScope("telegram-app"),
);
