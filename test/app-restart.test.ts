/**
 * alasio stopped and started again on the same state, as a rollout restart does, from the
 * outside: what becomes of a reply it had not delivered.
 */
import assert from "node:assert/strict";
import { test } from "node:test";

import { codexThread, threadResumeResponse } from "./support/codex-protocol.ts";
import type { RunningAlasio } from "./support/alasio.ts";
import {
  answerLoadedThreads,
  answerThreadStart,
  answerTurn,
  answerTurnStart,
  mount,
  alasioFor,
} from "./support/scenarios.ts";
import { eventually } from "./support/wait.ts";

/**
 * Whether alasio has recorded that Codex accepted the turn `turnId`: a prompt whose turn
 * Codex accepted is not run again after a restart.
 */
async function accepted(alasio: RunningAlasio, turnId: string): Promise<true | undefined> {
  // The store has no reader for a prompt job's upstream turn but its database.
  const { rows } = await alasio.pool.query(`select 1 from ${alasio.stateSchema}.prompt_jobs where upstream_turn_id = $1`, [turnId]);
  return rows.length > 0 ? true : undefined;
}

/** Mounts Codex on alpha and starts a turn, which Codex accepts as turn-1 on thread-1 and runs until the test ends it. */
async function runningTurn(alasio: RunningAlasio): Promise<void> {
  await mount(alasio, "codex", "alpha");
  alasio.telegram.say("Apply the change and restart");
  await answerThreadStart(alasio.codex, "thread-1", alasio.folder("alpha"));
  await answerTurnStart(alasio.codex, "thread-1", "turn-1");
  await eventually("alasio to record that Codex accepted the turn", () => accepted(alasio, "turn-1"));
}

test("a reply completed but not delivered when alasio stops is delivered after the restart, exactly once", async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram, codex } = alasio;
  await runningTurn(alasio);
  // Telegram holds the reply off for longer than alasio keeps running.
  telegram.fail("sendRichMessage", { status: 429, description: "Too Many Requests: retry after 30", retryAfter: 30 });
  answerTurn(codex, "thread-1", "turn-1", "Change applied.");
  await telegram.waitFor("sendRichMessage", (call) => call.status === 429);

  assert.deepEqual(await alasio.stop(), { code: 0, signal: null });
  const mark = telegram.mark();
  await alasio.start();
  await telegram.waitFor("sendRichMessage", () => true, { mark });
  // Ask something of the restarted alasio, so that a second delivery would have come by its reply.
  telegram.say("Thanks");
  await answerLoadedThreads(codex, []);
  codex.answer(await codex.next("thread/resume"), threadResumeResponse(codexThread("thread-1", { cwd: alasio.folder("alpha") })));
  await answerTurnStart(codex, "thread-1", "turn-2");
  answerTurn(codex, "thread-1", "turn-2", "You're welcome.");
  await telegram.waitFor("sendRichMessage", (call) => call.params.rich_message.markdown === "You're welcome.");

  assert.deepEqual(telegram.callsTo("sendRichMessage").map((call) => [call.params.rich_message.markdown, call.status]), [
    ["Change applied.", 429],
    ["Change applied.", 200],
    ["You're welcome.", 200],
  ]);
});
