/**
 * What the cut-over from 9.0.0 gives each conversation of the sessions its rows name
 * (upgrades/from-9.0.0.sh, its SESSIONS_CLAIMED), as 9.0.0's rows recorded none of them: on this
 * release's state, made as alasio makes it, its rows written, and what was recorded of them as
 * they were let go of, as 9.0.0's was never recorded, it gives each conversation the sessions
 * its rows name, as this release records them as they are written, and the Codex threads their
 * histories start in, as the rollouts say; 9.0.0's tally of albums whose files failed gone, as
 * this release keeps none; run again, nothing more.
 */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { test } from "node:test";

import { Effect } from "effect";
import pg from "pg";

import { CODEX_HARNESS } from "../src/harness/names.ts";
import { NeonRolloutStore } from "../src/codex/rollouts/store.ts";
import { Store } from "../src/persistence/store.ts";
import { startPostgres } from "./support/postgres.ts";

/**
 * The cut-over's SQL, as bash reads the script's definition of it, and as its pod is given it,
 * in an environment variable (its one_off), whose value Kubernetes gives with each `$$` a `$`,
 * as that escapes a `$(VAR)` reference.
 */
const SESSIONS_CLAIMED = spawnSync("bash", ["-c", 'eval "$(sed -n \'/^SESSIONS_CLAIMED="/,/^select count[^"]*"$/p\' "$0")"; printf %s "$SESSIONS_CLAIMED"', fileURLToPath(new URL("../upgrades/from-9.0.0.sh", import.meta.url))], { encoding: "utf8" }).stdout
  .split("$$").join("$");

test("the cut-over gives each conversation the sessions its rows name, as this release records them as they are written, and the threads their histories start in, drops 9.0.0's tally of albums and its restart events, and nothing more run again", async () => {
  assert.match(SESSIONS_CLAIMED, /insert into state\.conversation_sessions/u);
  const database = await startPostgres();
  const pool = new pg.Pool({ connectionString: database.url, max: 2 });
  try {
    const store = await Effect.runPromise(Effect.provide(Store, Store.layer({ pool })));
    // Codex's rollouts: a fork's history starting in its parent's, which starts in one before, which no row names.
    const rollouts = new NeonRolloutStore(pool);
    await rollouts.ensureSchema();
    const kept = (threadId: string, historyBase: string | null) =>
      rollouts.save({ name: `rollout-${threadId}.jsonl`, path: `sessions/rollout-${threadId}.jsonl`, threadId, rolloutId: threadId, historyBase, size: 1, headDigest: "d", modifiedMs: 1 }, { start: 0, bytes: Buffer.from("x") });
    await kept("a-root", null);
    await kept("a-parent", "a-root");
    await kept("a-mounted", "a-parent");
    const run = <A, E>(effect: Effect.Effect<A, E>) => Effect.runPromise(effect);
    const naming = async (chatId: string, session: string) => {
      const conversationId = await run(store.upsertConversation({ chatId, user: { id: Number(chatId) } }));
      await run(store.setActiveHarness(conversationId, CODEX_HARNESS));
      // Mounted as its operator chose it, a thread kept before any row named it.
      await run(store.mountGivenSession(conversationId, `${session}-mounted`));
      await run(store.claimTurn({ conversationId, chatId, messageId: "1", sessionId: `${session}-turn`, harness: CODEX_HARNESS, prompt: "go" }));
      await pool.query("insert into state.workspace_sessions (conversation_id, harness, working_directory, session_id) values ($1, 'claude', '/before', $2)", [conversationId, `${session}-before`]);
      return conversationId;
    };
    const first = await naming("41", "a");
    const second = await naming("42", "b");
    const recorded = async () => (await pool.query<{ session_id: string; conversation_id: string }>("select session_id, conversation_id from state.conversation_sessions order by 1, 2")).rows;
    const named = await recorded();
    // As this release records them, the mounted thread's lineage with it.
    assert.deepEqual(named.map(({ session_id }) => session_id), ["a-before", "a-mounted", "a-parent", "a-root", "a-turn", "b-before", "b-mounted", "b-turn"]);
    assert.deepEqual(new Set(named.map(({ conversation_id }) => conversation_id)), new Set([first, second]));
    // As 9.0.0's rows were: none of it recorded; its restart events, which name the sessions of the turns its stop cut short,
    // and which this release reads nothing of, as a turn's row names its session; its tally of albums whose files failed:
    // both of which this release keeps none of; and its turns, which have no column of the binding each runs on.
    await pool.query("delete from state.conversation_sessions");
    await pool.query("alter table state.turns drop column binding");
    await pool.query("create table state.media_group_failures (media_group_id text primary key, failures integer not null default 1)");
    await pool.query("create table state.restart_events (thread_key text primary key, cause text not null, channel text not null, thread_ts text not null, session_id text, command text, recorded_at timestamptz not null default now())");
    await pool.query("insert into state.restart_events (thread_key, cause, channel, thread_ts, session_id) values ($1, 'external', '41', '1', 'a-restarted')", [first]);
    /** How many sessions the conversations have, as the cut-over's SQL says last. */
    const claimed = async () => {
      const results = (await pool.query(SESSIONS_CLAIMED)) as unknown as { rows: { count: string }[] }[];
      return results.at(-1)?.rows[0]?.count;
    };
    assert.equal(await claimed(), "8");
    assert.equal((await pool.query("select to_regclass('state.media_group_failures') as kept")).rows[0]?.kept, null, "9.0.0's tally of albums is kept");
    assert.equal((await pool.query("select to_regclass('state.restart_events') as kept")).rows[0]?.kept, null, "9.0.0's restart events are kept");
    assert.equal((await pool.query("select count(*) from information_schema.columns where table_schema = 'state' and table_name = 'turns' and column_name = 'binding'")).rows[0]?.count, "1", "9.0.0's turns have no binding");
    // The rows' sessions, and the first's mounted thread's lineage, its: as this release records them; none its restart event alone named.
    assert.deepEqual(await recorded(), named);
    // Run again, as a cut-over run again past it would: the same.
    assert.equal(await claimed(), "8");
  } finally {
    await pool.end();
    await database.stop();
  }
});
