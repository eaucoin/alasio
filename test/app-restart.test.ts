/**
 * alasio stopped and started again on the same state, as a rollout restart does, from the
 * outside: what becomes of the turn it cut short, of a restart the agent itself ran,
 * and of a reply it had not delivered.
 */
import assert from "node:assert/strict";
import { test } from "node:test";

import { codexThread, commandExecution, itemCompleted, threadResumeResponse } from "./support/codex-protocol.ts";
import type { RunningAlasio } from "./support/alasio.ts";
import {
  WORKING,
  answerLoadedThreads,
  answerThreadStart,
  answerTurn,
  answerTurnStart,
  inputText,
  mount,
  alasioFor,
  timeless,
} from "./support/scenarios.ts";
import { OPERATOR_ID } from "./support/telegram.ts";
import { eventually } from "./support/wait.ts";

/** What the recovered turn tells Codex after a restart alasio cannot attribute. */
const EXTERNAL_RESTART = [
  "[SYSTEM RESTART EVENT]",
  "",
  "You are Codex, connected through the Kubernetes Deployment alasio in namespace alasio.",
  "",
  "Fact: the service restart that just occurred was external to your prior action in this conversation.",
  "Fact: the runtime cannot verify whether it was initiated by the user, an operator, or other external automation.",
  "Fact: the documented Alasio restart is `kubectl -n alasio rollout restart deployment/alasio`.",
  "",
  "Instruction: do not automatically resume your prior task as though nothing changed.",
  "Instruction: acknowledge the interruption and ask how the user wants to proceed.",
  "Instruction: you may briefly summarize the interrupted task, but do not attribute the restart to the user unless that fact is explicitly recorded.",
].join("\n");

/** What the recovered turn tells Codex after a restart its own command ran. */
const SELF_INDUCED_RESTART = [
  "[SYSTEM RESTART EVENT]",
  "",
  "You are Codex, connected through the Kubernetes Deployment alasio in namespace alasio.",
  "",
  "Fact: the service restart that just occurred was initiated by your own prior action in this conversation.",
  "Fact: the restart completed successfully and this is the post-restart continuation context for the same session.",
  "",
  "Fact: the documented Alasio restart is `kubectl -n alasio rollout restart deployment/alasio`.",
  "",
  "Instruction: continue exactly where you left off.",
  "Instruction: do not ask the user whether to continue solely because of this restart.",
  "Instruction: use and document that command as the normal Alasio restart path, not deleting alasio's pod.",
  "Instruction: if the restart was intended to apply a configuration or code change, verify the expected post-restart state, then proceed with the interrupted task.",
].join("\n");

/**
 * Whether alasio has recorded that Codex accepted the turn `turnId`: a prompt whose turn
 * Codex accepted is not run again after a restart.
 */
function accepted(alasio: RunningAlasio, turnId: string): true | undefined {
  // The store has no reader for a prompt job's upstream turn but its database.
  const count = alasio.readStore((store) => store.db.prepare<[string], { count: number }>("select count(*) count from prompt_jobs where upstream_turn_id = ?").get(turnId)?.count);
  return count ? true : undefined;
}

/** Mounts Codex on alpha and starts a turn, which Codex accepts as turn-1 on thread-1 and runs until the test ends it. */
async function runningTurn(alasio: RunningAlasio): Promise<void> {
  await mount(alasio, "codex", "alpha");
  alasio.telegram.say("Apply the change and restart");
  await answerThreadStart(alasio.codex, "thread-1", alasio.folder("alpha"));
  await answerTurnStart(alasio.codex, "thread-1", "turn-1");
  await eventually("alasio to record that Codex accepted the turn", () => accepted(alasio, "turn-1"));
}

/** After a restart, alasio resumes thread-1 on a new app-server and starts the recovery turn: its request. */
async function recoveryTurn(alasio: RunningAlasio) {
  const { codex } = alasio;
  await answerLoadedThreads(codex, []);
  const resume = await codex.next("thread/resume");
  assert.equal(resume.params.threadId, "thread-1");
  codex.answer(resume, threadResumeResponse(codexThread("thread-1", { cwd: alasio.folder("alpha") })));
  return await answerTurnStart(codex, "thread-1", "turn-2");
}

test("a turn alasio stops during is recovered after the restart, with unknown provenance, as a turn of its own", async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram, codex } = alasio;
  await runningTurn(alasio);
  const mark = telegram.mark();

  assert.deepEqual(await alasio.stop(), { code: 0, signal: null });
  // Stopping says nothing to the operator: the status message stays as it was.
  assert.deepEqual(telegram.shown(mark), []);
  await alasio.start();
  const recovery = await recoveryTurn(alasio);
  assert.equal(inputText(recovery), EXTERNAL_RESTART);
  assert.equal(codex.processes.length, 2);

  answerTurn(codex, "thread-1", "turn-2", "alasio restarted while I worked. Should I carry on?");
  await telegram.waitFor("sendRichMessage", () => true, { mark });
  assert.deepEqual(timeless(telegram.shown(mark)), [
    { method: "sendMessage", text: WORKING("Codex") },
    { method: "editMessageText", text: "Codex worked for a moment." },
    { method: "sendRichMessage", text: "alasio restarted while I worked. Should I carry on?" },
  ]);
  // Nothing else is left to recover: the next prompt is a turn of its own.
  telegram.say("Yes");
  await answerLoadedThreads(codex, ["thread-1"]);
  assert.equal(inputText(await answerTurnStart(codex, "thread-1", "turn-3")), "Yes");
});

test("a prompt alasio had sent to Codex but not heard back on when it stopped is not run again after the restart; the recovery turn continues it", async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram, codex } = alasio;
  await mount(alasio, "codex", "alpha");
  telegram.say("Apply the change and restart");
  await answerThreadStart(codex, "thread-1", alasio.folder("alpha"));
  await codex.next("turn/start");
  const mark = telegram.mark();

  assert.deepEqual(await alasio.stop(), { code: 0, signal: null });
  await alasio.start();
  const recovery = await recoveryTurn(alasio);
  assert.equal(inputText(recovery), EXTERNAL_RESTART);
  answerTurn(codex, "thread-1", "turn-2", "alasio restarted. Should I carry on?");
  await telegram.waitFor("sendRichMessage", (call) => call.params.rich_message.markdown === "alasio restarted. Should I carry on?", { mark });
  // Codex may have acted on the prompt, so it is not sent again: the next turn is the
  // operator's next prompt.
  telegram.say("Yes");
  await answerLoadedThreads(codex, ["thread-1"]);
  assert.equal(inputText(await answerTurnStart(codex, "thread-1", "turn-3")), "Yes");
});

test("a turn that ran alasio's rollout restart is continued after it as one the agent caused", async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram, codex } = alasio;
  await runningTurn(alasio);
  codex.notify(itemCompleted("thread-1", "turn-1", commandExecution("c1", "kubectl -n alasio rollout restart deployment/alasio")));
  // The rollout replaces alasio while the command's turn runs, once alasio has recorded
  // that the restart is the agent's own.
  const recorded = await eventually("alasio to record the restart", () => alasio.readStore((store) => {
    const conversation = store.getConversationByChatId(OPERATOR_ID);
    return (conversation && store.getRestartEvent(conversation.id)) ?? undefined;
  }));
  assert.deepEqual([recorded.cause, recorded.command, recorded.session_id], ["self_induced", "kubectl -n alasio rollout restart deployment/alasio", "thread-1"]);
  const mark = telegram.mark();

  assert.deepEqual(await alasio.stop(), { code: 0, signal: null });
  await alasio.start();
  const recovery = await recoveryTurn(alasio);
  assert.equal(inputText(recovery), SELF_INDUCED_RESTART);
  answerTurn(codex, "thread-1", "turn-2", "Restarted; the change is live.");
  await telegram.waitFor("sendRichMessage", () => true, { mark });
  assert.deepEqual(timeless(telegram.shown(mark)), [
    { method: "sendMessage", text: WORKING("Codex") },
    { method: "editMessageText", text: "Codex worked for a moment." },
    { method: "sendRichMessage", text: "Restarted; the change is live." },
  ]);
});

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
