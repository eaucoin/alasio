import assert from "node:assert/strict";
import { chmodSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { Codex, type Thread, type ThreadEvent } from "@openai/codex-sdk";
import { Effect, Layer, Logger } from "effect";

import { makeAppServer } from "../src/codex/app-server/client.ts";
import { executeCodexTurn } from "../src/codex/runtime.ts";
import { type CodexExecClient, execCodexOptions } from "../src/codex/transport.ts";
import { ActiveTurns } from "../src/harness/active-turns.ts";
import type { TurnPersistence } from "../src/harness/index.ts";
import type { FolderBayma, FolderBaymaScope } from "../src/mcp/bayma.ts";
import { type Hold, recordingBusyPods } from "./support/busy-pods.ts";
import { ownTurnsOn } from "./support/persistence.ts";

/** The prompts the test's Codex was given, in order. */
const prompted: string[] = [];

/** A Codex whose every thread runs one turn that edits three files, answers, and completes. */
function codexFactory(): CodexExecClient {
  const thread = {
    async runStreamed(prompt: Parameters<Pick<Thread, "runStreamed">["runStreamed"]>[0]) {
      prompted.push(String(prompt));
      return {
        events: (async function* (): AsyncGenerator<ThreadEvent> {
          yield { type: "thread.started", thread_id: "thread-1" };
          yield {
            type: "item.completed",
            item: { id: "item-1", type: "file_change", status: "completed", changes: ["a", "b", "c"].map((path) => ({ path, kind: "update" as const })) },
          };
          yield { type: "item.completed", item: { id: "item-2", type: "agent_message", text: "done" } };
          yield { type: "turn.completed", usage: {
            input_tokens: 1,
            cached_input_tokens: 0,
            cache_write_input_tokens: 0,
            output_tokens: 1,
            reasoning_output_tokens: 0,
          } };
        })(),
      };
    },
  };
  return { startThread: () => thread, resumeThread: () => thread };
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
 * Runs a turn of `codexFactory`'s Codex on `persistence`, its response made complete once
 * `beforeResponseComplete` is done, holding alasio's pod busy in `holds`.
 */
const runTurn = (
  persistence: TurnPersistence,
  beforeResponseComplete: (sessionId: string | null | undefined) => Effect.Effect<void> = () => Effect.void,
  { holds = [], folderBayma = () => Effect.succeed({ server: { type: "http", url: "http://bayma.alasio-host.svc:7290/mcp", headers: {} }, notice: null }) }: { readonly holds?: Hold[]; readonly folderBayma?: FolderBayma } = {},
) =>
  Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    // The exec transport's turn, which no app-server serves.
    const appServer = yield* makeAppServer({ spawn: () => Effect.sync(() => assert.fail("no app-server runs in this test")) });
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
      appServer,
      codexFactory,
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

test("codex exec, which the exec transport runs, is run without the login's apps nor plugins", async () => {
  const directory = mkdtempSync(join(tmpdir(), "alasio-codex-exec-"));
  try {
    // A codex that records how it was run, reads its prompt, and runs nothing.
    const codex = join(directory, "codex");
    writeFileSync(codex, `#!/bin/sh\nprintf '%s\\n' "$@" > ${JSON.stringify(join(directory, "argv"))}\ncat > /dev/null\nexit 1\n`);
    chmodSync(codex, 0o755);
    const client = new Codex({ ...execCodexOptions({ PATH: process.env["PATH"] ?? "" }, { developer_instructions: "", mcp_servers: {} }), codexPathOverride: codex });
    await client.startThread({ skipGitRepoCheck: true }).run("hello").then(() => assert.fail("the recording codex runs nothing"), () => undefined);
    const argv = readFileSync(join(directory, "argv"), "utf8").split("\n");
    const configs = argv.flatMap((arg, index) => (arg === "--config" ? [argv[index + 1]] : []));
    assert.equal(argv[0], "exec");
    for (const feature of ["features.apps=false", "features.plugins=false"]) assert.ok(configs.includes(feature), `${feature} in ${JSON.stringify(configs)}`);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});
