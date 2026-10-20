/**
 * Telegram at alasio's edges, from the outside: albums arriving as one prompt, an update
 * alasio fails to process, and replies Telegram does not take at once.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";

import type { RunningAlasio } from "./support/alasio.ts";
import { codexThread, threadStartResponse } from "./support/codex-protocol.ts";
import {
  answerLoadedThreads,
  answerThreadStart,
  answerTurn,
  answerTurnStart,
  inputText,
  mount,
  alasioFor,
} from "./support/scenarios.ts";
import { holdHandlingLock, testPool } from "./support/store.ts";
import { LATE_ALBUM_PART } from "../src/telegram/media-group-buffer.ts";
import { OPERATOR_ID } from "./support/telegram.ts";
import { eventually } from "./support/wait.ts";

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

test("a photo of an album that comes once the album was handled is told of in its chat, once, though its update is handled again", { timeout: 60_000 }, async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram, codex, stateSchema } = alasio;
  await mount(alasio, "codex", "alpha");
  telegram.sendPhoto("photo-a", Buffer.from("first photo"), { caption: "What is in these?", mediaGroupId: "album-1" });
  telegram.sendPhoto("photo-b", Buffer.from("second photo"), { mediaGroupId: "album-1" });
  await answerThreadStart(codex, "thread-1", alasio.folder("alpha"));
  await answerTurnStart(codex, "thread-1", "turn-1");
  // The store refuses the first update it marks processed from now: the late photo's, which is handled again.
  await (await testPool()).query(`
    create sequence ${stateSchema}.updates_marked;
    create function ${stateSchema}.refuse_first_mark() returns trigger language plpgsql as $$
      begin
        if old.processed_at is null and new.processed_at is not null and nextval('${stateSchema}.updates_marked') = 1 then raise exception 'the store is down'; end if;
        return new;
      end
    $$;
    create trigger refuse_first_mark before update on ${stateSchema}.telegram_updates for each row execute function ${stateSchema}.refuse_first_mark();
  `);
  const mark = telegram.mark();
  telegram.sendPhoto("photo-c", Buffer.from("late photo"), { mediaGroupId: "album-1" });
  await telegram.waitFor("sendMessage", (call) => call.params.text === LATE_ALBUM_PART, { mark });
  await eventually("the late photo's update handled again", async () => (Number((await (await testPool()).query<{ n: string }>(`select last_value as n from ${stateSchema}.updates_marked`)).rows[0]?.n) >= 2 ? true : undefined));
  await sleep(1000);
  const told = (await (await testPool()).query<{ text: string }>(`select text from ${stateSchema}.telegram_outbox where text = $1`, [LATE_ALBUM_PART])).rows;
  assert.equal(told.length, 1);
});

test("an album whose prompt fails is said to have failed in its chat, as a single message's is", { timeout: 60_000 }, async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram, stateSchema } = alasio;
  await mount(alasio, "codex", "alpha");
  // The store refuses the album's prompt as its job is queued.
  await (await testPool()).query(`
    create function ${stateSchema}.refuse_prompts() returns trigger language plpgsql as $$
      begin raise exception 'the store is down'; end
    $$;
    create trigger refuse_prompts before insert on ${stateSchema}.prompt_jobs for each row execute function ${stateSchema}.refuse_prompts();
  `);
  const mark = telegram.mark();
  telegram.sendPhoto("photo-a", Buffer.from("first photo"), { caption: "What is in these?", mediaGroupId: "album-1" });
  telegram.sendPhoto("photo-b", Buffer.from("second photo"), { mediaGroupId: "album-1" });
  const said = await telegram.waitFor("sendMessage", (call) => call.params.text.startsWith("Codex hit an error"), { mark, timeoutMs: 15_000 });
  assert.match(said.params.text, /the store is down/u);
});

test("an album another alasio kept, and left waiting as it stopped, is handled by this one as it looks for albums due, with no restart", { timeout: 60_000 }, async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { codex, stateSchema } = alasio;
  await mount(alasio, "codex", "alpha");
  // Kept by the other a moment ago, its wait gone with it.
  await (await testPool()).query(
    `with album as (
       insert into ${stateSchema}.media_groups (id, conversation_id, first_update_id, created_at) values ('left', $1, 9001, now() - interval '5 seconds')
     )
     insert into ${stateSchema}.messages (id, conversation_id, direction, kind, transport_message_id, text, media_group_id, created_at)
     values ('left-1', $1, 'in', 'text', '9001', 'What is left?', 'left', now() - interval '5 seconds')`,
    [`telegram:${OPERATOR_ID}`],
  );
  // Within one look for albums due, every ten seconds.
  codex.answer(await codex.next("thread/start", { timeoutMs: 15_000 }), threadStartResponse(codexThread("thread-1", { cwd: alasio.folder("alpha") })));
  assert.equal(inputText(await answerTurnStart(codex, "thread-1", "turn-1")), "What is left?");
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

test("a chat's update waits for its handling lock, which another alasio on the store holds, and is handled once it is let go of", { timeout: 30_000 }, async (t) => {
  const alasio = await alasioFor(t);
  const { telegram, stateSchema } = alasio;
  // Another alasio, handling the operator's chat.
  const letGo = await holdHandlingLock(stateSchema, OPERATOR_ID);
  try {
    telegram.say("/stop");
    await sleep(1500);
    assert.deepEqual(telegram.callsTo("sendMessage").map((call) => call.params.text), []);
  } finally {
    await letGo();
  }
  await telegram.waitFor("sendMessage", (call) => call.params.text === "No active query to stop.");
});

test("an update whose handling the store fails is handled once the store answers, and answered once", { timeout: 30_000 }, async (t) => {
  const alasio = await alasioFor(t);
  const { telegram, stateSchema } = alasio;
  // The store refuses the first message alasio keeps: a sequence's count outlasts the
  // transaction the refusal rolls back.
  await (await testPool()).query(`
    create sequence ${stateSchema}.messages_kept;
    create function ${stateSchema}.refuse_first_message() returns trigger language plpgsql as $$
      begin
        if nextval('${stateSchema}.messages_kept') = 1 then raise exception 'the store is down'; end if;
        return new;
      end
    $$;
    create trigger refuse_first_message before insert on ${stateSchema}.messages for each row execute function ${stateSchema}.refuse_first_message();
  `);

  telegram.say("/start");
  telegram.say("/stop");
  await telegram.waitFor("sendMessage", (call) => call.params.text === "No active query to stop.");
  // /start was taken again once the group was joined again, and answered, once, before /stop.
  assert.deepEqual(telegram.callsTo("sendMessage").map((call) => call.params.text.split("\n")[0]), ["Service", "No active query to stop."]);
});

test("a button press whose handling the store fails is handled once the store answers", { timeout: 30_000 }, async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { stateSchema } = alasio;
  // The store refuses the first press it takes, as it consumes the press's action.
  await (await testPool()).query(`
    create sequence ${stateSchema}.presses_taken;
    create function ${stateSchema}.refuse_first_press() returns trigger language plpgsql as $$
      begin
        if nextval('${stateSchema}.presses_taken') = 1 then raise exception 'the store is down'; end if;
        return old;
      end
    $$;
    create trigger refuse_first_press before delete on ${stateSchema}.callback_actions for each row execute function ${stateSchema}.refuse_first_press();
  `);
  // The second prompt held behind the first, its Queue pressed and, once taken again, queued.
  await twoQueuedPrompts(alasio, "First", "Second");
});

test("a press whose control waits on its harness holds back no later update", { timeout: 30_000 }, async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram, codex } = alasio;
  await mount(alasio, "codex", "alpha");
  telegram.say("First");
  await answerThreadStart(codex, "thread-1", alasio.folder("alpha"));
  await answerTurnStart(codex, "thread-1", "turn-1");
  let mark = telegram.mark();
  telegram.say("Second");
  await telegram.waitFor("sendMessage", (call) => call.params.text.startsWith("Codex is currently working."), { mark });
  // Steered, the turn's app-server not answering the steer yet.
  telegram.press("Steer");
  const steer = await codex.next("turn/steer");
  mark = telegram.mark();
  telegram.say("/start");
  await telegram.waitFor("sendMessage", (call) => call.params.text === "Alasio is ready.", { mark, timeoutMs: 10_000 });
  codex.answer(steer, { turnId: "turn-1" });
  await telegram.waitFor("answerCallbackQuery", (call) => call.params.text === "Steered.");
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

test("a reply Telegram refuses outright is retried by the outbox later, and those queued after it wait for it", { timeout: 30_000 }, async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram, codex } = alasio;
  await twoQueuedPrompts(alasio, "First", "Second");
  telegram.fail("sendRichMessage", { status: 502, description: "Bad Gateway" });

  answerTurn(codex, "thread-1", "turn-1", "Reply one");
  await answerLoadedThreads(codex, ["thread-1"]);
  await answerTurnStart(codex, "thread-1", "turn-2");
  answerTurn(codex, "thread-1", "turn-2", "Reply two");
  // The outbox retries a refused reply 10 seconds on, at its next pass every 5 seconds.
  await telegram.waitFor("sendRichMessage", (call) => call.params.rich_message.markdown === "Reply two" && call.status === 200, { timeoutMs: 20_000 });

  assert.deepEqual(replyAttempts(alasio), [["Reply one", 502], ["Reply one", 200], ["Reply two", 200]]);
  const [refused, retried] = telegram.callsTo("sendRichMessage");
  assert.ok(refused && retried && retried.at - refused.at >= 10_000, "the outbox waited its backoff");
});
