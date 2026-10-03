/**
 * Telegram at alasio's edges, from the outside: albums arriving as one prompt, an update
 * alasio fails to process, and replies Telegram does not take at once.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";

import type { RunningAlasio } from "./support/alasio.ts";
import {
  answerLoadedThreads,
  answerThreadStart,
  answerTurn,
  answerTurnStart,
  inputText,
  mount,
  alasioFor,
} from "./support/scenarios.ts";

/** Mounts Codex on alpha and has the operator send `first`, then `second` while it runs, queued behind it. */
async function twoQueuedPrompts(alasio: RunningAlasio, first: string, second: string): Promise<void> {
  const { telegram, codex } = alasio;
  await mount(alasio, "codex", "alpha");
  telegram.say(first);
  await answerThreadStart(codex, "thread-1", alasio.folder("alpha"));
  await answerTurnStart(codex, "thread-1", "turn-1");
  const mark = telegram.mark();
  telegram.say(second);
  await telegram.waitFor("sendMessage", (call) => call.params.text.startsWith("Codex is currently working."), { mark });
  telegram.press("Queue");
  await telegram.waitFor("editMessageText", (call) => call.params.text?.startsWith("Queued.") ?? false, { mark });
}

/** What alasio tried to deliver as replies, in order: each attempt's text and Telegram's status. */
function replyAttempts(alasio: RunningAlasio): [string, number][] {
  return alasio.telegram.callsTo("sendRichMessage").map((call) => [call.params.rich_message.markdown ?? "", call.status]);
}

test("two photos of one album are one turn, whose prompt is the caption and both files' paths", async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram, codex } = alasio;
  await mount(alasio, "codex", "alpha");
  const mark = telegram.mark();
  const firstPhoto = Buffer.from("first photo");
  const secondPhoto = Buffer.from("second photo");

  const first = telegram.sendPhoto("photo-a", firstPhoto, { caption: "What is in these?", mediaGroupId: "album-1" });
  const second = telegram.sendPhoto("photo-b", secondPhoto, { mediaGroupId: "album-1" });
  await answerThreadStart(codex, "thread-1", alasio.folder("alpha"));
  const turnStart = await answerTurnStart(codex, "thread-1", "turn-1");
  const prompt = /^What is in these\?\n\nYou can see the 1st file at (\S+)\nYou can see the 2nd file at (\S+)$/u.exec(inputText(turnStart));
  assert.ok(prompt, inputText(turnStart));
  const [, firstPath = "", secondPath = ""] = prompt;
  assert.ok(firstPath.endsWith(`/photo-${first.message_id}.jpg`), firstPath);
  assert.ok(secondPath.endsWith(`/photo-${second.message_id}.jpg`), secondPath);
  assert.deepEqual([readFileSync(firstPath), readFileSync(secondPath)], [firstPhoto, secondPhoto]);
  assert.deepEqual(telegram.callsTo("getFile", mark).map((call) => call.params.file_id), ["photo-a", "photo-b"]);

  answerTurn(codex, "thread-1", "turn-1", "Two photos.");
  await telegram.waitFor("sendRichMessage", () => true, { mark });
  assert.equal(codex.requests("turn/start").length, 1);
});

test("an update whose processing fails is skipped, and the updates after it are processed", async (t) => {
  const alasio = await alasioFor(t);
  const { telegram } = alasio;
  telegram.fail("sendMessage", { status: 400, description: "Bad Request: chat not found" });

  telegram.say("/start");
  await telegram.waitFor("sendMessage", (call) => call.status === 400);
  telegram.say("/start");
  telegram.say("/stop");
  await telegram.waitFor("sendMessage", (call) => call.params.text === "No active query to stop.");
  // The failed update was not fetched and processed again: the panel went once more, for
  // the second /start, before the reply to /stop.
  assert.deepEqual(telegram.callsTo("sendMessage").map((call) => [call.params.text.split("\n")[0], call.status]), [
    ["Service", 400],
    ["Service", 200],
    ["No active query to stop.", 200],
  ]);
});

test("a reply Telegram rate-limits is sent again after the wait it asks for, once, and the reply after it follows it", async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram, codex } = alasio;
  await twoQueuedPrompts(alasio, "First", "Second");
  telegram.fail("sendRichMessage", { status: 429, description: "Too Many Requests: retry after 1", retryAfter: 1 });

  answerTurn(codex, "thread-1", "turn-1", "Reply one");
  await answerLoadedThreads(codex, ["thread-1"]);
  await answerTurnStart(codex, "thread-1", "turn-2");
  answerTurn(codex, "thread-1", "turn-2", "Reply two");
  // A reply queued while the outbox delivers another waits for its next pass, at most 5 seconds on.
  await telegram.waitFor("sendRichMessage", (call) => call.params.rich_message.markdown === "Reply two", { timeoutMs: 10_000 });

  assert.deepEqual(replyAttempts(alasio), [["Reply one", 429], ["Reply one", 200], ["Reply two", 200]]);
  const [refused, delivered] = telegram.callsTo("sendRichMessage");
  assert.ok(refused && delivered && delivered.at - refused.at >= 1_000, "the reply waited out retry_after");
});

test("a reply Telegram refuses outright is retried by the outbox later, and a later reply overtakes it", { timeout: 30_000 }, async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram, codex } = alasio;
  await twoQueuedPrompts(alasio, "First", "Second");
  telegram.fail("sendRichMessage", { status: 502, description: "Bad Gateway" });

  answerTurn(codex, "thread-1", "turn-1", "Reply one");
  await answerLoadedThreads(codex, ["thread-1"]);
  await answerTurnStart(codex, "thread-1", "turn-2");
  answerTurn(codex, "thread-1", "turn-2", "Reply two");
  // The outbox retries a refused reply 10 seconds on, at its next pass every 5 seconds.
  await telegram.waitFor("sendRichMessage", (call) => call.params.rich_message.markdown === "Reply one" && call.status === 200, { timeoutMs: 20_000 });

  // characterizes current behaviour: replies keep their order only within one pass of the
  // outbox; a reply deferred to a later pass is overtaken by one queued after it.
  assert.deepEqual(replyAttempts(alasio), [["Reply one", 502], ["Reply two", 200], ["Reply one", 200]]);
  const [refused, , retried] = telegram.callsTo("sendRichMessage");
  assert.ok(refused && retried && retried.at - refused.at >= 10_000, "the outbox waited its backoff");
});
