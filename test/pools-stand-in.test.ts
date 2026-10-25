/**
 * The stand-in of the harness pools (test/support/pools.ts) as a test's services end: its pods'
 * hosts end with them, the turns running there and the binds under way interrupted, however
 * their harnesses run on, so a test that fails as a turn runs fails at once.
 */
import assert from "node:assert/strict";
import { connect } from "node:net";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";

import { Effect } from "effect";

import type { Harness } from "../src/harness/index.ts";
import { CODEX_HARNESS, harnessDisplayName } from "../src/harness/names.ts";
import { Pools, pooledStore } from "./support/pools.ts";
import { run } from "./support/store.ts";
import { submitted, withServices } from "./support/turns.ts";
import { eventually } from "./support/wait.ts";

/** Codex, whose turns never end, as a harness that runs on does, recording each one started. */
function endlessHarness(started: string[]): Harness {
  const unused = (name: string) => () => assert.fail(`the endless harness has no ${name}`);
  return {
    name: CODEX_HARNESS,
    displayName: harnessDisplayName(CODEX_HARNESS),
    supportsGoals: false,
    sessions: {
      listSessions: unused("listSessions"),
      getTotalSessionPages: unused("getTotalSessionPages"),
      getSessionByNumber: unused("getSessionByNumber"),
      getSessionLastMessage: unused("getSessionLastMessage"),
      listSessionMessages: unused("listSessionMessages"),
      getTotalRewindPages: unused("getTotalRewindPages"),
      createForkedSession: unused("createForkedSession"),
    },
    startFreshSession: unused("startFreshSession"),
    runTurn: (params) => Effect.andThen(Effect.sync(() => void started.push(params.prompt)), Effect.never),
    listModels: unused("listModels"),
    defaultModelChoice: unused("defaultModelChoice"),
  };
}

/** A Codex conversation in a folder, on a store its pods reach. */
async function conversation() {
  const store = await pooledStore();
  const conversationId = await run(store.upsertConversation({ chatId: "41", user: { id: 41 } }));
  await run(store.setActiveHarness(conversationId, CODEX_HARNESS));
  await run(store.setWorkingDirectory(conversationId, "/work"));
  return { store, conversationId };
}

/** How long a test's services may take to end: well within any test's time, and far from waiting for ever. */
const ENDS_WITHIN_MS = 10_000;

/** Whether something listens on `port` of the loopback. */
const listened = (port: number): Promise<boolean> =>
  new Promise((resolve) => {
    const socket = connect(port, "127.0.0.1");
    socket.once("connect", () => (socket.destroy(), resolve(true)));
    socket.once("error", () => resolve(false));
  });

/**
 * `use` on the services, then how long they took to end once it was done, and whether any of
 * their pods serves a while after: none, its host ended with them, whatever it was doing.
 */
async function endedAfter(options: Parameters<typeof withServices>[0], use: Parameters<typeof withServices>[1]): Promise<{ readonly took: number; readonly serving: readonly number[] }> {
  let done = 0;
  let ports: readonly number[] = [];
  await withServices(options, async (alasio) => {
    await use(alasio);
    ports = [...(await alasio.runPromise(Effect.map(Pools, (pools) => pools.ports()))).values()];
    done = Date.now();
  });
  const took = Date.now() - done;
  // Long enough for a host whose start went on past them to be serving.
  await sleep(3_000);
  const serving: number[] = [];
  for (const port of ports) if (await listened(port)) serving.push(port);
  return { took, serving };
}

test("a test's services end as its pod's turn runs on, its harness never ending, the turn interrupted with them", { timeout: 60_000 }, async () => {
  const { store, conversationId } = await conversation();
  const started: string[] = [];
  const { took, serving } = await endedAfter({ store, harnesses: { [CODEX_HARNESS]: endlessHarness(started) } }, async (alasio) => {
    alasio.runFork(submitted({ conversationId, chatId: 41, messageId: 1, prompt: "go on", fileIds: [], visibleText: "go on" }));
    await eventually("the turn to run in its pod", async () => (started.length > 0 ? true : undefined));
    assert.equal(await alasio.runPromise(Effect.map(Pools, (pools) => pools.bound().has(conversationId))), true);
  });
  assert.ok(took < ENDS_WITHIN_MS, `the services took ${took} ms to end`);
  assert.deepEqual(serving, [], "a pod serves on past them");
});

test("a test's services end as a bind of its conversation's harness is under way, the bind ended with them", { timeout: 60_000 }, async () => {
  const { store, conversationId } = await conversation();
  const started: string[] = [];
  const { took, serving } = await endedAfter({ store, harnesses: { [CODEX_HARNESS]: endlessHarness(started) } }, async (alasio) => {
    // Its turn asked for, and the services ended as its pod is still being bound.
    alasio.runFork(submitted({ conversationId, chatId: 41, messageId: 1, prompt: "go on", fileIds: [], visibleText: "go on" }));
    await eventually("its harness to be being bound", async () => (await alasio.runPromise(Effect.map(Pools, (pools) => pools.binding().has(conversationId))) ? true : undefined));
  });
  assert.ok(took < ENDS_WITHIN_MS, `the services took ${took} ms to end`);
  assert.deepEqual(serving, [], "a pod serves on past them");
});
