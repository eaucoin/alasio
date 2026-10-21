/**
 * What the operator is told, and what goes on, where a conversation's harness host is not
 * reached (src/harness/remote-host.ts fails each call with HarnessHostError): what decides
 * by whether it is busy refuses, or shows it working; a stop says why it stopped nothing;
 * and what goes on without the stop says so in alasio's log.
 */
import assert from "node:assert/strict";
import { test } from "node:test";

import { Array as Arr, Effect, Logger } from "effect";

import { shownBusy, stopTurnSaying, swerveTo } from "../src/harness/commands.ts";
import type { HarnessHost } from "../src/harness/host.ts";
import { type Harness, type HarnessGoals, HarnessHostError } from "../src/harness/index.ts";
import { CODEX_HARNESS, harnessDisplayName } from "../src/harness/names.ts";
import { handleGoalTextCommand } from "../src/operator/goal-control.ts";
import { Mounts } from "../src/operator/mounts.ts";
import { takeCallbackQuery } from "../src/telegram/callback-handler.ts";
import { run, testStore } from "./support/store.ts";
import { recordingTelegram } from "./support/telegram-calls.ts";
import { turnsStub, withServices } from "./support/turns.ts";

const CHAT = 41;
const unreached = new HarnessHostError({ reason: "connect ECONNREFUSED 10.43.0.9:8080" });

/** The host elsewhere, not reached: what it would say of its conversation, and stop, fails as not reached. */
const notReached = (host: HarnessHost["Service"]): HarnessHost["Service"] => ({
  ...host,
  reserve: () => Effect.fail(unreached),
  status: () => Effect.fail(unreached),
  interrupt: () => Effect.fail(unreached),
});

/** The goals of a thread on the host not reached, as Codex keeps them there: each read or change fails as not reached. */
const unreachedGoals: HarnessGoals = {
  read: () => Effect.fail(unreached),
  set: () => Effect.fail(unreached),
  clear: () => Effect.fail(unreached),
  waitForTurnId: () => Effect.fail(unreached),
};

/** Codex in the conversation's folder, its goals `goals`, and nothing else of it used. */
function codexWithGoals(goals: HarnessGoals): Harness {
  const unused = (name: string) => () => assert.fail(`Codex here has no ${name}`);
  return {
    name: CODEX_HARNESS,
    displayName: harnessDisplayName(CODEX_HARNESS),
    supportsGoals: true,
    supportsWarmup: false,
    sessions: {
      listSessions: unused("listSessions"),
      getTotalSessionPages: unused("getTotalSessionPages"),
      getSessionByNumber: unused("getSessionByNumber"),
      getSessionLastMessage: unused("getSessionLastMessage"),
      listSessionMessages: unused("listSessionMessages"),
      getTotalRewindPages: unused("getTotalRewindPages"),
      createForkedSession: unused("createForkedSession"),
    },
    goals,
    startFreshSession: unused("startFreshSession"),
    warmSession: () => Effect.succeed(false),
    runTurn: unused("runTurn"),
    listModels: unused("listModels"),
    defaultModelChoice: unused("defaultModelChoice"),
  };
}

/** `effect`, with what it logs, as written: what it came to, and each line. */
const logging = <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<readonly [A, readonly string[]], E, R> =>
  Effect.suspend(() => {
    const lines: string[] = [];
    return effect.pipe(
      Effect.map((value) => [value, lines] as const),
      Effect.provide(Logger.layer([Logger.make(({ message }) => void lines.push(Arr.ensure(message).join(" ")))])),
    );
  });

/** A conversation on Codex in a folder, with a session, and alasio's services over it, its host not reached, its turns' scheduling recorded in `scheduled`. */
async function unreachedConversation<T>(use: (services: { readonly alasio: Parameters<Parameters<typeof withServices>[1]>[0]; readonly conversationId: string; readonly store: Awaited<ReturnType<typeof testStore>>; readonly telegram: ReturnType<typeof recordingTelegram>; readonly scheduled: string[] }) => Promise<T>, { goals }: { readonly goals?: HarnessGoals } = {}): Promise<T> {
  const store = await testStore();
  const conversationId = await run(store.upsertConversation({ chatId: String(CHAT), user: { id: CHAT } }));
  await run(store.setActiveHarness(conversationId, CODEX_HARNESS));
  await run(store.setWorkingDirectory(conversationId, "/work"));
  await run(store.setSessionId(conversationId, "session-1"));
  const telegram = recordingTelegram();
  const scheduled: string[] = [];
  return await withServices(
    { store, telegram: telegram.layer, allowedUserIds: String(CHAT), host: notReached, harnesses: goals ? { [CODEX_HARNESS]: codexWithGoals(goals) } : {}, turns: turnsStub({ schedule: (id) => Effect.sync(() => void scheduled.push(id)) }) },
    (alasio) => use({ alasio, conversationId, store, telegram, scheduled }),
  );
}

test("the panels show a conversation whose host is not reached as working, and a stop of it says why it stopped nothing", async () => {
  await unreachedConversation(async ({ alasio, conversationId }) => {
    const [shown, said] = await alasio.runPromise(logging(shownBusy(conversationId)));
    assert.deepEqual([shown, said], [true, [`could not tell whether ${conversationId} is busy: ${unreached.message}`]]);
    assert.equal(await alasio.runPromise(stopTurnSaying(conversationId, "interrupt")), unreached.message);
  });
});

test("switching the service of a conversation whose host is not reached is refused, saying why", async () => {
  await unreachedConversation(async ({ alasio, conversationId }) => {
    const refused = await alasio.runPromise(Effect.flip(Effect.flatMap(Mounts, (mounts) => mounts.switchHarness(conversationId, "claude"))));
    assert.equal(refused._tag, "MountRefused");
    assert.match(refused.message, /ECONNREFUSED 10\.43\.0\.9:8080, so whether it is working cannot be told/u);
  });
});

test("a press of a goal panel, of a conversation whose host is not reached, is answered with why", async () => {
  await unreachedConversation(async ({ alasio, conversationId, store, telegram }) => {
    const [actionId = ""] = await run(store.createCallbackActions(conversationId, [{ kind: "goal:pause" }]));
    const press = await alasio.runPromise(takeCallbackQuery({
      id: "press-1",
      from: { id: CHAT, is_bot: false, first_name: "Operator" },
      chat_instance: "chat",
      data: actionId,
      message: { message_id: 5, date: 0, chat: { id: CHAT, type: "private", first_name: "Operator" } },
    }));
    await alasio.runPromise(press);
    assert.deepEqual(telegram.calls.answerCallbackQuery.map(([id, text]) => [id, text]), [["press-1", "Working..."]]);
    assert.deepEqual(telegram.calls.editMessageText.map(([, , text]) => text), [`Goal\n\nFailed to update goal: ${unreached.message}`]);
  }, { goals: unreachedGoals });
});

test("a swerve to a held prompt, of a conversation whose host is not reached, queues the prompt first in line, the turn not stopped, as alasio's log says", async () => {
  await unreachedConversation(async ({ alasio, conversationId, store, scheduled }) => {
    const job = await run(store.enqueuePromptJob({ conversationId, chatId: CHAT, messageId: 9, prompt: "go this way", state: "awaiting_choice" }));
    const [swerved, said] = await alasio.runPromise(logging(swerveTo(conversationId, job.id)));
    assert.deepEqual([swerved, said], [true, [`could not swerve ${conversationId}'s turn: ${unreached.message}`]]);
    assert.deepEqual([(await run(store.getPromptJob(job.id)))?.state, scheduled], ["pending", [conversationId]]);
  });
});

test("a goal cleared, in a conversation whose host is not reached, is cleared and its panel shown, its turn not stopped, as alasio's log says", async () => {
  await unreachedConversation(async ({ alasio, conversationId, telegram }) => {
    const cleared: string[] = [];
    const goals: HarnessGoals = {
      read: () => Effect.succeed(null),
      set: () => Effect.die(new Error("no goal is set")),
      clear: ({ threadId }) => Effect.sync(() => (cleared.push(threadId), { cleared: true })),
      waitForTurnId: () => Effect.succeed(null),
    };
    const [, said] = await alasio.runPromise(logging(handleGoalTextCommand({ conversationId, chatId: CHAT, messageId: 7, args: "clear", goals })));
    assert.ok(said.includes(`could not stop ${conversationId}'s turn: ${unreached.message}`), JSON.stringify(said));
    assert.deepEqual(cleared, ["session-1"]);
    const sent = telegram.calls.sendMessage.map(([, text]) => String(text));
    assert.ok(sent.length > 0 && sent.every((text) => !text.includes("Failed to update goal")), JSON.stringify(sent));
  });
});
