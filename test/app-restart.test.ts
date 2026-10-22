/**
 * alasio stopped and started again on the same state, as a rollout restart does, from the
 * outside, its conversations' pods running on: what becomes of a reply it had not delivered,
 * and of a turn it left running.
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
import { DRAINING } from "../src/telegram/drain.ts";
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

test("a reply Telegram holds off as alasio is told to stop is delivered before it stops, exactly once", async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram, codex } = alasio;
  await runningTurn(alasio);
  // Telegram holds the reply off for a few seconds, as alasio is told to stop.
  telegram.fail("sendRichMessage", { status: 429, description: "Too Many Requests: retry after 3", retryAfter: 3 });
  answerTurn(codex, "thread-1", "turn-1", "Change applied.");
  await telegram.waitFor("sendRichMessage", (call) => call.status === 429);

  assert.deepEqual(await alasio.stop(), { code: 0, signal: null });
  // Delivered as it stopped: none is left for the alasio after it.
  assert.deepEqual(telegram.callsTo("sendRichMessage").map((call) => call.status), [429, 200]);
  await alasio.start();
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

test("a turn running as alasio restarts runs on on its pod, and its reply, which the pod finishes after the restart, is posted once", async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram, codex } = alasio;
  await runningTurn(alasio);
  assert.deepEqual(await alasio.stop(), { code: 0, signal: null });
  await alasio.start();
  answerTurn(codex, "thread-1", "turn-1", "Change applied.");
  await telegram.waitFor("sendRichMessage", (call) => call.params.rich_message.markdown === "Change applied.");
  // Ask something of the restarted alasio, so that a second delivery, or the turn run again, would have come by its reply.
  telegram.say("Thanks");
  // On the pod that ran the turn, its thread still loaded.
  await answerLoadedThreads(codex, ["thread-1"]);
  await answerTurnStart(codex, "thread-1", "turn-2");
  answerTurn(codex, "thread-1", "turn-2", "You're welcome.");
  await telegram.waitFor("sendRichMessage", (call) => call.params.rich_message.markdown === "You're welcome.");

  assert.deepEqual(telegram.callsTo("sendRichMessage").map((call) => [call.params.rich_message.markdown, call.status]), [
    ["Change applied.", 200],
    ["You're welcome.", 200],
  ]);
});

test("an update that comes as a stopping alasio delivers what it owes is another alasio's, its member having left the group before it delivers", async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram, codex } = alasio;
  await runningTurn(alasio);
  // Telegram holds the reply off, so the stop delivers it for a few seconds.
  telegram.fail("sendRichMessage", { status: 429, description: "Too Many Requests: retry after 5", retryAfter: 5 });
  answerTurn(codex, "thread-1", "turn-1", "Change applied.");
  await telegram.waitFor("sendRichMessage", (call) => call.status === 429);
  const told = await telegram.committed();

  let ended = false;
  const stopped = alasio.stop().then((exit) => ((ended = true), exit));
  await alasio.logged(DRAINING);
  telegram.say("Meanwhile");
  // Taken by another member of the group, as the stopping alasio still delivers.
  assert.equal((await telegram.updateAfter(told)).message?.text, "Meanwhile");
  assert.equal(ended, false, "alasio ended before another member took the update");
  assert.deepEqual(await stopped, { code: 0, signal: null });
  // The stopping alasio handled none of it, and delivered what it owed.
  assert.equal(await telegram.committed(), told);
  assert.deepEqual(telegram.callsTo("sendRichMessage").map((call) => call.status), [429, 200]);
});
