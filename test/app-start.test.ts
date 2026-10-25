/**
 * alasio, as it starts, takes Telegram's updates before it settles what the last alasio left
 * (src/telegram/app.ts): an alasio woken by an update answers it while its start's slower
 * looks, as at the sandbox gateway's workspaces, are still under way; but what the last alasio
 * left to say in a chat, a response completed and never delivered, an album due, it queues
 * before it takes any update, so nothing of an update taken after goes before it.
 */
import assert from "node:assert/strict";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";

import { codexThread, threadStartResponse } from "./support/codex-protocol.ts";
import { alasioFor, answerTurnStart, inputText, mount } from "./support/scenarios.ts";
import { OPERATOR_ID } from "./support/telegram.ts";

/** How long an update is given to be taken while what the start queues first is held: past a member's join and a turn's status message. */
const OVERTAKE_MS = 5_000;

test("alasio answers the operator while its start's look at the sandbox gateway's workspaces waits on the gateway", { timeout: 60_000 }, async (t) => {
  let answerListing = () => {};
  const workspacesListed = new Promise<void>((resolve) => void (answerListing = resolve));
  t.after(() => answerListing());
  const { telegram } = await alasioFor(t, { workspacesListed });
  const mark = telegram.mark();
  telegram.say("/service");
  const panel = await telegram.waitFor("sendMessage", (call) => call.params.text.startsWith("Service"), { mark, timeoutMs: 20_000 });
  assert.match(panel.params.text, /^Service\n\nActive: none/u);
});

test("a response completed before the restart, never delivered, is delivered before anything of an update taken after, in its chat", { timeout: 90_000 }, async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram, codex, stateSchema, pool } = alasio;
  await mount(alasio, "codex", "alpha");
  assert.deepEqual(await alasio.stop(), { code: 0, signal: null });
  // Completed as the last alasio stopped, before it queued it.
  await pool.query(`insert into ${stateSchema}.responses (id, chat_id, message_id, completed) values ('left', $1, '1', true)`, [String(OPERATOR_ID)]);
  await pool.query(`insert into ${stateSchema}.response_blocks (response_id, block) values ('left', $1)`, [JSON.stringify({ type: "text", content: "What the last alasio left.", phase: "final_answer" })]);
  // Its queuing held, as a slow statement holds it, while an update comes: the outbox's insert of it marks it posted in
  // the same statement (src/persistence/outbox-repository.ts enqueueText), whose update waits on this lock of its row;
  // the select that finds it undelivered takes none, and is not held.
  const holder = await pool.connect();
  await holder.query("begin");
  await holder.query(`select 1 from ${stateSchema}.responses where id = 'left' for update`);
  const mark = telegram.mark();
  const starting = alasio.start();
  telegram.say("A new prompt");
  await sleep(OVERTAKE_MS);
  await holder.query("commit");
  holder.release();
  await starting;
  codex.answer(await codex.next("thread/start"), threadStartResponse(codexThread("thread-1", { cwd: alasio.folder("alpha") })));
  await answerTurnStart(codex, "thread-1", "turn-1");
  const shown = await telegram.waitForShown(2, { mark });
  assert.equal(shown[0]?.text, "What the last alasio left.", JSON.stringify(shown));
});

test("an album due as alasio starts is run before the prompt of an update taken after, in its chat", { timeout: 90_000 }, async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram, codex, stateSchema, pool } = alasio;
  await mount(alasio, "codex", "alpha");
  assert.deepEqual(await alasio.stop(), { code: 0, signal: null });
  // Kept by the last alasio, its wait gone with it.
  await pool.query(
    `with album as (
       insert into ${stateSchema}.media_groups (id, conversation_id, first_update_id, created_at) values ('left', $1, 9001, now() - interval '5 seconds')
     )
     insert into ${stateSchema}.messages (id, conversation_id, direction, kind, transport_message_id, text, media_group_id, created_at)
     values ('left-1', $1, 'in', 'text', '9001', 'What is left?', 'left', now() - interval '5 seconds')`,
    [`telegram:${OPERATOR_ID}`],
  );
  // Its look held, as a slow statement holds it, while an update comes.
  const holder = await pool.connect();
  await holder.query("begin");
  await holder.query(`lock table ${stateSchema}.media_groups in access exclusive mode`);
  const starting = alasio.start();
  telegram.say("A new prompt");
  await sleep(OVERTAKE_MS);
  await holder.query("commit");
  holder.release();
  await starting;
  codex.answer(await codex.next("thread/start"), threadStartResponse(codexThread("thread-1", { cwd: alasio.folder("alpha") })));
  assert.equal(inputText(await answerTurnStart(codex, "thread-1", "turn-1")), "What is left?");
});
