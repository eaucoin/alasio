/**
 * Telegram at alasio's edges, from the outside: albums arriving as one prompt, an update
 * alasio fails to process, and replies Telegram does not take at once.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join, relative } from "node:path";
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
import { holdHandlingLock } from "./support/store.ts";
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
  // Written where the prompt names them, of the conversation's pod's own filesystem, alasio's path there.
  const podStateDir = alasio.pools.stateDir(`telegram:${OPERATOR_ID}`) ?? assert.fail("the conversation has no pod bound");
  const onPod = (path: string) => join(podStateDir, relative(alasio.stateDir, path));
  assert.deepEqual([readFileSync(onPod(firstPath)), readFileSync(onPod(secondPath))], [firstPhoto, secondPhoto]);
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
  await alasio.pool.query(`
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
  await eventually("the late photo's update handled again", async () => (Number((await alasio.pool.query<{ n: string }>(`select last_value as n from ${stateSchema}.updates_marked`)).rows[0]?.n) >= 2 ? true : undefined));
  await sleep(1000);
  const told = (await alasio.pool.query<{ text: string }>(`select text from ${stateSchema}.telegram_outbox where text = $1`, [LATE_ALBUM_PART])).rows;
  assert.equal(told.length, 1);
});

test("an album whose prompt's job the store refuses at first is not lost, but waits, its claim rolled back with it, and runs once the store takes it", { timeout: 60_000 }, async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram, codex, stateSchema } = alasio;
  await mount(alasio, "codex", "alpha");
  // The store refuses the album's prompt the first time its job is queued: a sequence's count outlasts the transaction the refusal rolls back.
  await alasio.pool.query(`
    create sequence ${stateSchema}.prompts_queued;
    create function ${stateSchema}.refuse_first_prompt() returns trigger language plpgsql as $$
      begin
        if nextval('${stateSchema}.prompts_queued') = 1 then raise exception 'the store is down'; end if;
        return new;
      end
    $$;
    create trigger refuse_first_prompt before insert on ${stateSchema}.prompt_jobs for each row execute function ${stateSchema}.refuse_first_prompt();
  `);
  const mark = telegram.mark();
  telegram.sendPhoto("photo-a", Buffer.from("first photo"), { caption: "What is in these?", mediaGroupId: "album-1" });
  telegram.sendPhoto("photo-b", Buffer.from("second photo"), { mediaGroupId: "album-1" });
  // Within a look for albums due, every ten seconds, past the refusal.
  codex.answer(await codex.next("thread/start", { timeoutMs: 30_000 }), threadStartResponse(codexThread("thread-1", { cwd: alasio.folder("alpha") })));
  assert.match(inputText(await answerTurnStart(codex, "thread-1", "turn-1")), /^What is in these\?\n\nYou can see the 1st file at /u);
  assert.equal(Number((await alasio.pool.query<{ n: string }>(`select last_value as n from ${stateSchema}.prompts_queued`)).rows[0]?.n), 2);
  assert.deepEqual(telegram.callsTo("sendMessage", mark).map((call) => call.params.text).filter((text) => text.includes("hit an error")), []);
  answerTurn(codex, "thread-1", "turn-1", "Two photos.");
  await telegram.waitFor("sendRichMessage", () => true, { mark });
  assert.equal(codex.requests("turn/start").length, 1);
});

test("an album another alasio kept, and left waiting as it stopped, is handled by this one as it looks for albums due, with no restart", { timeout: 60_000 }, async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { codex, stateSchema } = alasio;
  await mount(alasio, "codex", "alpha");
  // Kept by the other a moment ago, its wait gone with it.
  await alasio.pool.query(
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
  const letGo = await holdHandlingLock(stateSchema, OPERATOR_ID, alasio.pool);
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
  await alasio.pool.query(`
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
  await alasio.pool.query(`
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

/** Has the store refuse the first update it marks processed from now, as one failing then would: the count of those it was asked to mark, outlasting the refusal. */
async function refuseNextMark(alasio: RunningAlasio): Promise<() => Promise<number>> {
  const { pool, stateSchema } = alasio;
  await pool.query(`
    create sequence ${stateSchema}.updates_marked;
    create function ${stateSchema}.refuse_next_mark() returns trigger language plpgsql as $$
      begin
        if old.processed_at is null and new.processed_at is not null and nextval('${stateSchema}.updates_marked') = 1 then raise exception 'the store is down'; end if;
        return new;
      end
    $$;
    create trigger refuse_next_mark before update on ${stateSchema}.telegram_updates for each row execute function ${stateSchema}.refuse_next_mark();
  `);
  return async () => Number((await pool.query<{ n: string }>(`select last_value as n from ${stateSchema}.updates_marked`)).rows[0]?.n);
}

test("a press done as its update is handled, its update's mark then failing, is done once: handled again, its action is consumed already", { timeout: 60_000 }, async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram, codex } = alasio;
  await mount(alasio, "codex", "alpha");
  telegram.say("First");
  await answerThreadStart(codex, "thread-1", alasio.folder("alpha"));
  await answerTurnStart(codex, "thread-1", "turn-1");
  const mark = telegram.mark();
  telegram.say("Second");
  await telegram.waitFor("sendMessage", (call) => call.params.text.startsWith("Codex is currently working."), { mark });
  await eventually("Second's update told handled", async () => {
    const committed = await telegram.committed();
    return committed > 0 && (await sleep(300), committed === (await telegram.committed())) ? committed : undefined;
  });
  const marked = await refuseNextMark(alasio);
  telegram.press("Queue");
  await eventually("the press's update handled again", async () => ((await marked()) >= 2 ? true : undefined));
  await sleep(1000);
  assert.deepEqual(telegram.callsTo("editMessageText", mark).map((call) => call.params.text?.split(".")[0]), ["Queued"]);
  // Its query answered as it was done, and, handled again, as one whose action is gone.
  assert.deepEqual(telegram.callsTo("answerCallbackQuery", mark).map((call) => call.params.text), ["Queued.", "This action is no longer available."]);
});

test("a press still running as its update's mark fails runs on, to its end, its update handled again doing nothing of it: done once", { timeout: 60_000 }, async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram, codex } = alasio;
  await mount(alasio, "codex", "alpha");
  telegram.say("First");
  await answerThreadStart(codex, "thread-1", alasio.folder("alpha"));
  await answerTurnStart(codex, "thread-1", "turn-1");
  const mark = telegram.mark();
  telegram.say("Second");
  await telegram.waitFor("sendMessage", (call) => call.params.text.startsWith("Codex is currently working."), { mark });
  await eventually("Second's update told handled", async () => {
    const committed = await telegram.committed();
    return committed > 0 && (await sleep(300), committed === (await telegram.committed())) ? committed : undefined;
  });
  // Its steer unanswered past CONTROL_AWAITED, its update's mark then refused.
  const marked = await refuseNextMark(alasio);
  telegram.press("Steer");
  const steer = await codex.next("turn/steer");
  await eventually("the press's update handled again", async () => ((await marked()) >= 2 ? true : undefined), { timeoutMs: 15_000 });
  codex.answer(steer, { turnId: "turn-1" });
  await eventually("the steer answered", async () =>
    telegram.callsTo("answerCallbackQuery", mark).some((call) => call.params.text === "Steered.") || telegram.shown(mark).some((shown) => shown.text === "Steered.") ? true : undefined);
  await sleep(1000);
  // Steered once: no steer asked again.
  assert.equal(codex.requests("turn/steer").length, 1);
  assert.equal(telegram.shown(mark).filter(({ text }) => text === "Steered.").length + telegram.callsTo("answerCallbackQuery", mark).filter((call) => call.params.text === "Steered.").length, 1);
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
  // Answered to its query, or, kept past CONTROL_AWAITED, through the outbox.
  await eventually("the steer answered", async () =>
    telegram.callsTo("answerCallbackQuery").some((call) => call.params.text === "Steered.") || telegram.shown(mark).some((shown) => shown.text === "Steered.") ? true : undefined);
});

/** Has the operator press Steer on a prompt held while Codex's first turn runs, its app-server not answering the steer: the steer's request, and the last update told handled before the press. */
async function steerUnanswered(alasio: RunningAlasio) {
  const { telegram, codex } = alasio;
  await mount(alasio, "codex", "alpha");
  telegram.say("First");
  await answerThreadStart(codex, "thread-1", alasio.folder("alpha"));
  await answerTurnStart(codex, "thread-1", "turn-1");
  const mark = telegram.mark();
  telegram.say("Second");
  await telegram.waitFor("sendMessage", (call) => call.params.text.startsWith("Codex is currently working."), { mark });
  const before = await eventually("Second's update told handled", async () => {
    const committed = await telegram.committed();
    return committed > 0 && (await sleep(300), committed === (await telegram.committed())) ? committed : undefined;
  });
  telegram.press("Steer");
  return { steer: await codex.next("turn/steer"), before };
}

/** The controls kept in alasio's store, each with whether its hold is past. */
async function keptControls(alasio: RunningAlasio): Promise<{ id: string; lapsed: boolean }[]> {
  return (await alasio.pool.query<{ id: string; lapsed: boolean }>(`select id, held_until <= now() as lapsed from ${alasio.stateSchema}.control_jobs`)).rows;
}

/** Once alasio has shown the operator `text` after `mark`. */
const shownText = (alasio: RunningAlasio, text: string | RegExp, mark: number) =>
  eventually(`alasio to show ${text}`, async () => alasio.telegram.shown(mark).find((shown) => (typeof text === "string" ? shown.text === text : text.test(shown.text))));

test("a press's update is told handled once what the press does is done, or once CONTROL_AWAITED has passed, its press then kept, its query answered Working..., and its answer sent through the outbox", { timeout: 30_000 }, async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram, codex } = alasio;
  const mark = telegram.mark();
  const { steer, before } = await steerUnanswered(alasio);
  await sleep(1000);
  assert.equal(await telegram.committed(), before, "not told handled while its steer is under way");
  assert.deepEqual(await keptControls(alasio), []);
  await eventually("the press's update told handled past CONTROL_AWAITED", async () => ((await telegram.committed()) > before ? true : undefined), { timeoutMs: 5_000 });
  // Kept, and held by this alasio, before its update was told handled.
  assert.deepEqual((await keptControls(alasio)).map(({ lapsed }) => lapsed), [false]);
  await telegram.waitFor("answerCallbackQuery", (call) => call.params.text === "Working...", { mark });
  codex.answer(steer, { turnId: "turn-1" });
  await shownText(alasio, "Steered.", mark);
  assert.deepEqual(telegram.callsTo("answerCallbackQuery", mark).map((call) => call.params.text), ["Working..."]);
  await eventually("the press forgotten once done", async () => ((await keptControls(alasio)).length === 0 ? true : undefined));
});

test("a kept press's query is answered Working... once its update is told handled: Telegram rate-limiting that answer holds the update back no longer than CONTROL_AWAITED", { timeout: 60_000 }, async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram, codex } = alasio;
  await mount(alasio, "codex", "alpha");
  telegram.say("First");
  await answerThreadStart(codex, "thread-1", alasio.folder("alpha"));
  await answerTurnStart(codex, "thread-1", "turn-1");
  const mark = telegram.mark();
  telegram.say("Second");
  await telegram.waitFor("sendMessage", (call) => call.params.text.startsWith("Codex is currently working."), { mark });
  const before = await eventually("Second's update told handled", async () => {
    const committed = await telegram.committed();
    return committed > 0 && (await sleep(300), committed === (await telegram.committed())) ? committed : undefined;
  });
  // The press's query's first answer asked to wait 8 s, as Telegram asks a bot that answers too fast.
  telegram.fail("answerCallbackQuery", { status: 429, description: "Too Many Requests: retry after 8", retryAfter: 8 });
  telegram.press("Steer");
  const pressed = Date.now();
  const steer = await codex.next("turn/steer");
  await eventually("the press's update told handled", async () => ((await telegram.committed()) > before ? true : undefined), { timeoutMs: 15_000 });
  const handledAfter = Date.now() - pressed;
  assert.ok(handledAfter < 7_000, `told handled ${handledAfter} ms after the press, its answer's wait among it`);
  await telegram.waitFor("answerCallbackQuery", (call) => call.params.text === "Working..." && call.status === 200, { mark, timeoutMs: 20_000 });
  codex.answer(steer, { turnId: "turn-1" });
  await shownText(alasio, "Steered.", mark);
});

// Bounded by two starts of alasio's, the test's first (some 10 s) and its restart (some 6 s), and the press's handling between.
test("a press kept as alasio stops is let go of, and run again from its action once alasio starts, its answer through the outbox", { timeout: 60_000 }, async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram, codex } = alasio;
  const { before } = await steerUnanswered(alasio);
  await eventually("the press's update told handled past CONTROL_AWAITED", async () => ((await telegram.committed()) > before ? true : undefined), { timeoutMs: 5_000 });
  assert.deepEqual((await keptControls(alasio)).map(({ lapsed }) => lapsed), [false]);
  assert.deepEqual(await alasio.stop(), { code: 0, signal: null });
  // Let go of as alasio stopped, for the next to run at once.
  assert.deepEqual((await keptControls(alasio)).map(({ lapsed }) => lapsed), [true]);
  const mark = telegram.mark();
  // Run again as alasio starts: steering the turn recovered from the restart, or queued where none runs to take it, whose
  // turn then asks Codex the threads it has loaded. Answered whichever comes, as neither is waited for where it never does.
  codex.answerEvery("turn/steer", () => ({ turnId: "turn-2" }));
  codex.answerEvery("thread/loaded/list", () => ({ data: [], nextCursor: null }));
  await alasio.start();
  await shownText(alasio, /^(Steered|Queued)\.$/u, mark);
  await eventually("the press forgotten once run again", async () => ((await keptControls(alasio)).length === 0 ? true : undefined));
});

test("a press kept by an alasio that crashed, its hold lapsed, is run again by the next look of one that runs, its answer through the outbox", { timeout: 90_000 }, async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram, stateSchema } = alasio;
  await mount(alasio, "codex", "alpha");
  const mark = telegram.mark();
  // As an alasio that crashed as it ran the press left it: held by it, its hold past.
  const callbackQuery = {
    id: "lapsed-press",
    from: { id: OPERATOR_ID, is_bot: false, first_name: "Operator" },
    chat_instance: "1",
    data: "gone",
    message: { message_id: 1, date: 0, chat: { id: OPERATOR_ID, type: "private", first_name: "Operator" } },
  };
  const action = { id: "gone", conversationId: `telegram:${OPERATOR_ID}`, kind: "workspace:refresh", payload: { expectedSessionId: null, expectedHarness: "codex" } };
  await alasio.pool.query(
    `insert into ${stateSchema}.control_jobs (id, conversation_id, control, holder, held_until) values ($1, $2, $3, 'crashed', now() - interval '1 second')`,
    [callbackQuery.id, action.conversationId, JSON.stringify({ kind: "press", callbackQuery, action })],
  );
  // Within a look, every thirty seconds.
  await eventually("the lapsed press run again", async () => telegram.shown(mark).find((shown) => shown.text === "Refreshed."), { timeoutMs: 45_000 });
  await eventually("the press forgotten once run again", async () => ((await keptControls(alasio)).length === 0 ? true : undefined));
  assert.deepEqual(telegram.callsTo("answerCallbackQuery", mark), []);
});

/** The last update told handled once none has been for a moment. */
const settledCommitted = (alasio: RunningAlasio) =>
  eventually("the updates before told handled", async () => {
    const committed = await alasio.telegram.committed();
    return committed > 0 && (await sleep(300), committed === (await alasio.telegram.committed())) ? committed : undefined;
  });

test("a command still running past CONTROL_AWAITED is kept as its update is told handled, let go of as alasio stops, and run again from its message once alasio starts, its result told once", { timeout: 60_000 }, async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram, codex } = alasio;
  await mount(alasio, "codex", "alpha");
  const before = await settledCommitted(alasio);
  const command = telegram.say("/sessions new");
  // Its new session's thread asked of Codex, which does not answer it yet.
  await codex.next("thread/start");
  await eventually("the command's update told handled past CONTROL_AWAITED", async () => ((await telegram.committed()) > before ? true : undefined), { timeoutMs: 5_000 });
  // Kept, and held by this alasio, before its update was told handled.
  assert.deepEqual(await keptControls(alasio), [{ id: `command:telegram:${OPERATOR_ID}:${command.message_id}`, lapsed: false }]);
  assert.deepEqual(await alasio.stop(), { code: 0, signal: null });
  // Let go of as alasio stopped, for the next to run at once.
  assert.deepEqual((await keptControls(alasio)).map(({ lapsed }) => lapsed), [true]);
  const mark = telegram.mark();
  await alasio.start();
  codex.answer(await codex.next("thread/start"), threadStartResponse(codexThread("thread-2", { cwd: alasio.folder("alpha") })));
  await shownText(alasio, /^New Codex session mounted: thread-2\./u, mark);
  await eventually("the command forgotten once run again", async () => ((await keptControls(alasio)).length === 0 ? true : undefined));
  assert.deepEqual(telegram.shown(mark).filter(({ text }) => text.startsWith("New Codex session mounted")).length, 1);
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
