import assert from "node:assert/strict";
import { test } from "node:test";

import { Effect, Layer, Logger, Stream } from "effect";

import type { AppServer } from "../src/codex/app-server/client.ts";
import type { AppServerEvent } from "../src/codex/app-server/protocol.ts";
import { executeCodexTurn } from "../src/codex/runtime.ts";
import { ActiveTurns } from "../src/harness/active-turns.ts";
import type { TurnPersistence } from "../src/harness/index.ts";
import type { FolderBayma, FolderBaymaScope } from "../src/mcp/bayma.ts";
import { type Hold, recordingBusyPods } from "./support/busy-pods.ts";
import { ownTurnsOn } from "./support/persistence.ts";

/** The prompts the test's Codex was given, in order. */
const prompted: string[] = [];

/** The events of the test's Codex's every turn: it edits three files, answers, and completes. */
const TURN: readonly AppServerEvent[] = [
  { type: "item.completed", item: { id: "item-1", type: "file_change", status: "completed", changes: ["a", "b", "c"].map((path) => ({ path, kind: { type: "update", move_path: null }, diff: "" })) } },
  { type: "item.completed", item: { id: "item-2", type: "agent_message", text: "done", phase: null, memoryCitation: null, delivery: null, questions: null } },
  { type: "turn.completed", usage: null },
];

/** An app-server whose every thread runs TURN, and which dies of anything else a turn would ask of it. */
function codexAppServer(): AppServer {
  const answers: Partial<AppServer> = {
    startThread: () => Effect.succeed("thread-1"),
    loginModels: () => Effect.succeed([]),
    startTurn: ({ prompt }) => Effect.sync(() => (prompted.push(prompt), "turn-1")),
    eventsForTurn: () => Stream.fromIterable(TURN),
    interrupt: () => Effect.succeed(false),
  };
  return new Proxy(answers as AppServer, {
    get: (target, name) => target[name as keyof AppServer] ?? (() => Effect.die(new Error(`the test's app-server was asked ${String(name)}`))),
  });
}

/** alasio's persistence, recording which of its methods are called, in order, and the blocks each store of them is given. */
function recordingPersistence(calls: string[], stored: unknown[][] = []): TurnPersistence {
  // The proxy answers every property with a method that records its call, so it has
  // whichever of the store's methods the turn calls; none returns anything the turn
  // reads but createPendingResponse, which returns the pending response's id.
  return new Proxy({} as TurnPersistence, {
    get: (_target, name) => (...args: unknown[]) =>
      Effect.sync(() => {
        calls.push(String(name));
        if (name === "appendBlocksToPending") stored.push(args[1] as unknown[]);
        return name === "createPendingResponse" ? "pending-1" : undefined;
      }),
  });
}

/**
 * Runs a turn of the test's Codex (codexAppServer) on `persistence`, its response made complete once
 * `beforeResponseComplete` is done, holding alasio's pod busy in `holds`.
 */
const runTurn = (
  persistence: TurnPersistence,
  beforeResponseComplete: (sessionId: string | null | undefined) => Effect.Effect<void> = () => Effect.void,
  { holds = [], folderBayma = () => Effect.succeed({ server: { type: "http", url: "http://bayma.alasio-host.svc:7290/mcp", headers: {} }, notice: null }) }: { readonly holds?: Hold[]; readonly folderBayma?: FolderBayma } = {},
) =>
  Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    return yield* executeCodexTurn({
      prompt: "hello",
      resumeSession: null,
      threadKey: "conversation-1",
      chatId: "1",
      messageId: "2",
      workingDirectory: process.cwd(),
      modelChoice: null,
      ownTurns: ownTurnsOn(persistence),
      persistence,
      appServer: codexAppServer(),
      folderBayma,
      beforeResponseComplete,
    });
  })).pipe(Effect.provide([ActiveTurns.layer.pipe(Layer.provide(recordingBusyPods(holds).layer)), Logger.layer([])])));

test("a turn holds alasio's pod busy until it ends", async () => {
  const holds: Hold[] = [];
  let heldMeanwhile: readonly Hold[] = [];
  await runTurn(recordingPersistence([]), () => Effect.sync(() => (heldMeanwhile = structuredClone(holds))), { holds });
  assert.deepEqual(heldMeanwhile, [{ released: false }]);
  assert.deepEqual(holds, [{ released: true }]);
});

test("a turn's response is marked complete only once beforeResponseComplete has finished", async () => {
  const calls: string[] = [];
  const result = await runTurn(recordingPersistence(calls), (sessionId) =>
    Effect.sleep("20 millis").pipe(Effect.andThen(Effect.sync(() => calls.push(`beforeResponseComplete:${sessionId}`)))));

  assert.equal(result.responseCompleted, true);
  const flushed = calls.indexOf("beforeResponseComplete:thread-1");
  assert.ok(flushed >= 0);
  assert.ok(flushed < calls.indexOf("markPendingResponseComplete"));
});

test("the blocks an event adds to a turn's response are stored together, in one statement", async () => {
  const stored: unknown[][] = [];
  const result = await runTurn(recordingPersistence([], stored));

  assert.deepEqual(stored, [
    [{ type: "tool", name: "Edit" }, { type: "tool", name: "Edit" }, { type: "tool", name: "Edit" }],
    [{ type: "text", content: "done", phase: null }],
  ]);
  assert.deepEqual(result.blockSequence, stored.flat());
});

test("a turn whose folder's bayma did not start runs without it, its prompt told so first, its chat given to tell", async () => {
  const asked: FolderBaymaScope[] = [];
  const notice = "Folder workspace /work: its bayma did not start (its pod is pending). This turn runs without bayma's REPL tools; your REPL sessions are kept and come back when it starts.";
  prompted.length = 0;
  await runTurn(recordingPersistence([]), () => Effect.void, { folderBayma: (scope) => Effect.sync(() => (asked.push(scope), { server: null, notice })) });
  assert.deepEqual(prompted, [`${notice}\n\nhello`]);
  assert.deepEqual(asked.map(({ harness, threadKey, chatId }) => ({ harness, threadKey, chatId })), [{ harness: "codex", threadKey: "conversation-1", chatId: "1" }]);
});
