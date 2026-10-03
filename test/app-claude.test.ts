/**
 * alasio with Claude Code, from the outside: what the operator says in Telegram, what
 * alasio pushes into the Claude Code process serving the conversation, and what the
 * operator sees come back, including the turns Claude Code starts on its own.
 */
import assert from "node:assert/strict";
import { test } from "node:test";

import { assistantMessage, backgroundTasksChanged, errorResult, initMessage, successResult, text } from "./support/claude-sdk.ts";
import type { FakeClaudeQuery } from "./support/claude.ts";
import { FOLDER_BAYMA, type RunningAlasio } from "./support/alasio.ts";
import { WORKING, inAnyOrder, mount, alasioFor, timeless } from "./support/scenarios.ts";

/** Mounts Claude on alpha and starts a turn of `prompt`: the Claude Code process serving it, and the prompt's uuid. */
async function runningTurn(alasio: RunningAlasio, prompt = "Fix the build"): Promise<{ readonly query: FakeClaudeQuery; readonly uuid: string }> {
  await mount(alasio, "claude", "alpha");
  alasio.telegram.say(prompt);
  const query = await alasio.claude.query(0);
  const pushed = await query.nextPrompt();
  query.emit(initMessage("session-1"));
  await alasio.telegram.waitFor("sendMessage", (call) => call.params.text.startsWith("Claude is working"));
  return { query, uuid: pushed.uuid };
}

/** The replies alasio delivered, in order. */
function replies(alasio: RunningAlasio): string[] {
  return alasio.telegram.callsTo("sendRichMessage").map((call) => call.params.rich_message.markdown ?? "");
}

test("a Claude turn: the prompt goes into the conversation's Claude Code process, whose result is the reply; the next prompt goes into the same process", async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram, claude } = alasio;
  await mount(alasio, "claude", "alpha");
  let mark = telegram.mark();

  telegram.say("Fix the build");
  const query = await claude.query(0);
  assert.deepEqual(query.options, {
    cwd: alasio.folder("alpha"),
    model: "claude-opus-5-5[1m]",
    effort: "high",
    mcpServers: { bayma: FOLDER_BAYMA },
    disallowedTools: ["Bash", "Monitor", "Grep", "Glob"],
    permissionMode: "bypassPermissions",
    persistSession: true,
  });
  const prompt = await query.nextPrompt();
  assert.deepEqual({ ...prompt, uuid: "…" }, { type: "user", uuid: "…", message: { role: "user", content: "Fix the build" }, parent_tool_use_id: null, origin: { kind: "human" } });
  query.emit(
    initMessage("session-1"),
    assistantMessage([text("Looking at the failing step.")], { session_id: "session-1" }),
    successResult({ result: "Fixed: the lockfile was stale.", session_id: "session-1", user_message_uuids: [prompt.uuid] }),
  );
  await telegram.waitFor("sendRichMessage", () => true, { mark });
  assert.deepEqual(timeless(telegram.shown(mark)), [
    { method: "sendMessage", text: WORKING("Claude") },
    { method: "editMessageText", text: "Claude worked for a moment." },
    { method: "sendRichMessage", text: "Fixed: the lockfile was stale." },
  ]);

  mark = telegram.mark();
  telegram.say("And the docs");
  const next = await query.nextPrompt();
  assert.equal(next.message.content, "And the docs");
  query.emit(successResult({ result: "Docs updated.", session_id: "session-1", user_message_uuids: [next.uuid] }));
  await telegram.waitFor("sendRichMessage", () => true, { mark });
  assert.deepEqual(replies(alasio), ["Fixed: the lockfile was stale.", "Docs updated."]);
  assert.equal(claude.queries.length, 1);
});

test("Steer pushes the concurrent message into the running Claude turn, which ends on the result answering both", async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram } = alasio;
  const { query, uuid } = await runningTurn(alasio);

  let mark = telegram.mark();
  telegram.say("Also update the docs");
  assert.deepEqual(await telegram.waitForShown(1, { mark }), [{
    method: "sendMessage",
    text: "Claude is currently working. What should I do with this message?\n\nAlso update the docs",
    buttons: [["Steer", "Queue"], ["Swerve", "Discard"]],
  }]);
  mark = telegram.mark();
  telegram.press("Steer");
  const steered = await query.nextPrompt();
  assert.equal(steered.message.content, "Also update the docs");
  assert.deepEqual(await telegram.waitForShown(2, { mark }), [
    { method: "answerCallbackQuery", text: "Steered." },
    { method: "editMessageText", text: "Sent as guidance to the active Claude turn." },
  ]);

  query.emit(successResult({ result: "Fixed, docs too.", session_id: "session-1", user_message_uuids: [uuid, steered.uuid] }));
  await telegram.waitFor("sendRichMessage", () => true, { mark });
  assert.deepEqual(replies(alasio), ["Fixed, docs too."]);
});

test("/stop interrupts the Claude turn; its late result is ignored, and the next prompt runs on the same process", async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram, claude } = alasio;
  const { query, uuid } = await runningTurn(alasio);

  let mark = telegram.mark();
  telegram.say("/stop");
  const shown = await telegram.waitForShown(3, { mark });
  assert.deepEqual(shown[0], { method: "sendMessage", text: "Stopping Claude..." });
  assert.deepEqual(inAnyOrder(shown.slice(1)), inAnyOrder([
    { method: "editMessageText", text: "Claude interrupted." },
    { method: "editMessageText", text: "Claude stopped." },
  ]));
  assert.equal(query.interrupts, 1);
  // Claude Code reports the interrupted turn's end.
  query.emit(errorResult({ subtype: "error_during_execution", errors: ["interrupted"], session_id: "session-1", user_message_uuids: [uuid] }));
  await query.handled();

  mark = telegram.mark();
  telegram.say("Try again");
  const next = await query.nextPrompt();
  assert.equal(next.message.content, "Try again");
  query.emit(successResult({ result: "Done.", session_id: "session-1", user_message_uuids: [next.uuid] }));
  await telegram.waitFor("sendRichMessage", () => true, { mark });
  assert.deepEqual(replies(alasio), ["Done."]);
  assert.equal(claude.queries.length, 1);
});

test("a turn Claude Code starts on its own is delivered as a reply of its own, and a prompt while it runs gets the concurrency panel", async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram } = alasio;
  const { query, uuid } = await runningTurn(alasio, "Run the build in the background");
  query.emit(
    backgroundTasksChanged([{ task_id: "task-1", task_type: "local_bash", description: "npm run build" }]),
    successResult({ result: "The build is running in the background.", session_id: "session-1", user_message_uuids: [uuid] }),
  );
  await telegram.waitFor("sendRichMessage");

  // The background build settles, and Claude Code reports on it in a turn of its own.
  let mark = telegram.mark();
  query.emit(backgroundTasksChanged([]), assistantMessage([text("The build finished.")], { session_id: "session-1" }));
  await query.handled();
  telegram.say("Is it done?");
  assert.deepEqual(await telegram.waitForShown(1, { mark }), [{
    method: "sendMessage",
    text: "Claude is currently working. What should I do with this message?\n\nIs it done?",
    buttons: [["Steer", "Queue"], ["Swerve", "Discard"]],
  }]);
  telegram.press("Queue");
  await telegram.waitFor("editMessageText", (call) => call.params.text?.startsWith("Queued.") ?? false, { mark });

  mark = telegram.mark();
  query.emit(successResult({ result: "Background build passed.", session_id: "session-1" }));
  await telegram.waitFor("sendRichMessage", () => true, { mark });
  // The queued prompt runs once Claude Code's own turn is over.
  const queued = await query.nextPrompt();
  assert.equal(queued.message.content, "Is it done?");
  query.emit(successResult({ result: "Yes, it passed.", session_id: "session-1", user_message_uuids: [queued.uuid] }));
  await telegram.waitFor("sendRichMessage", (call) => call.params.rich_message.markdown === "Yes, it passed.", { mark });

  assert.deepEqual(replies(alasio), ["The build is running in the background.", "Background build passed.", "Yes, it passed."]);
  // Claude Code's own turn has no status message; the queued turn has its own.
  assert.deepEqual(timeless(telegram.shown(mark)).filter(({ method }) => method !== "sendRichMessage"), [
    { method: "sendMessage", text: WORKING("Claude") },
    { method: "editMessageText", text: "Claude worked for a moment." },
  ]);
});
