/**
 * A branch environment's state, made its own as it first starts. Its database is a
 * copy-on-write copy of the alasio it was branched from, as that one's was at the branch
 * point, and so holds what was in flight there: Telegram's updates and their offset,
 * which are another bot's; prompts queued and turns running, which a restart's recovery
 * would run again; replies on their way, which would be sent again, by this bot; buttons,
 * which this bot never sent; and Codex's login, whose refresh token works once, so that
 * using it here would log the other out. All of it goes, once, in one transaction; the
 * conversations, what they mount, and their history stay.
 */
import { Effect } from "effect";

import type { Database, StoreError } from "./sql.ts";
import { TELEGRAM_OFFSET } from "./state-repository.ts";

/** The key of bot_state that names the branch whose state this is. */
const BRANCH_KEY = "branch";

/**
 * Makes the state in `schema` the branch `branch`'s, unless it is already: whether it
 * did.
 */
export const startBranch = (database: Database, schema: string, branch: string): Effect.Effect<boolean, StoreError> =>
  database.transaction((sql) =>
    Effect.gen(function*() {
      // A row only where the state was another's: main's, or the branch it was branched from.
      const claimed = yield* sql.query(
        `insert into ${schema}.bot_state (key, value) values ($1, $2)
         on conflict (key) do update set value = excluded.value, updated_at = now() where bot_state.value <> excluded.value
         returning value`,
        [BRANCH_KEY, branch],
      );
      if (claimed.length === 0) return false;
      yield* sql.query(`delete from ${schema}.bot_state where key = $1`, [TELEGRAM_OFFSET]);
      yield* sql.query(`
        delete from ${schema}.telegram_updates;
        delete from ${schema}.media_groups;
        delete from ${schema}.prompt_jobs;
        delete from ${schema}.restart_events;
        update ${schema}.turns set state = 'completed', completed_at = now() where state = 'active';
        update ${schema}.responses set posted = true where not posted;
        delete from ${schema}.telegram_outbox;
        delete from ${schema}.callback_actions;
        delete from ${schema}.codex_login;
      `);
      return true;
    })
  );
