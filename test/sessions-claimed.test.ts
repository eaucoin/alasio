/**
 * What the cut-over from 9.0.0 gives each conversation of the sessions its rows name
 * (upgrades/from-9.0.0.sh, its SESSIONS_CLAIMED), as 9.0.0's rows recorded none of them: on this
 * release's state, made as alasio makes it, its rows written, and what was recorded of them as
 * they were let go of, as 9.0.0's was never recorded, it gives each conversation the sessions
 * its rows name, as this release records them as they are written, and the Codex threads their
 * histories start in, as the rollouts say; run again, nothing more.
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

/** The cut-over's SQL, as bash reads the script's definition of it. */
const SESSIONS_CLAIMED = spawnSync("bash", ["-c", 'eval "$(sed -n \'/^SESSIONS_CLAIMED="/,/^select count[^"]*"$/p\' "$0")"; printf %s "$SESSIONS_CLAIMED"', fileURLToPath(new URL("../upgrades/from-9.0.0.sh", import.meta.url))], { encoding: "utf8" }).stdout;

test("the cut-over gives each conversation the sessions its rows name, as this release records them as they are written, and the threads their histories start in, and nothing more run again", async () => {
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
    // As 9.0.0's rows were: none of it recorded.
    await pool.query("delete from state.conversation_sessions");
    /** How many sessions the conversations have, as the cut-over's SQL says last. */
    const claimed = async () => {
      const results = (await pool.query(SESSIONS_CLAIMED)) as unknown as { rows: { count: string }[] }[];
      return results.at(-1)?.rows[0]?.count;
    };
    assert.equal(await claimed(), "8");
    // The rows' sessions, and the first's mounted thread's lineage, its: as this release records them.
    assert.deepEqual(await recorded(), named);
    assert.equal(await claimed(), "8");
  } finally {
    await pool.end();
    await database.stop();
  }
});
