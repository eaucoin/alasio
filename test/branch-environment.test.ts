/**
 * A branch environment's alasio (cli/src/commands/branch.ts): its state made its own as
 * it first starts, and the sessions it inherited forked for it by its parent.
 */
import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { Deferred, Effect, Exit, Fiber, Layer, Option, Scope } from "effect";

import { parentForks, serveBranchForks } from "../src/branch/fork.ts";
import { forkToken } from "../src/branch/names.ts";
import { Turns } from "../src/codex/turns.ts";
import { ActiveTurns } from "../src/harness/active-turns.ts";
import { Harnesses } from "../src/harness/index.ts";
import { KubeApiError } from "../src/kube/client.ts";
import { Store } from "../src/persistence/store.ts";
import { SessionForkError, SessionSandboxes } from "../src/sandbox/index.ts";
import { newSchema, run, testStore } from "./support/store.ts";
import { recordingTelegram } from "./support/telegram-calls.ts";
import { withServices } from "./support/turns.ts";

test("a branch's state, as it first starts, keeps its conversations and loses what was in flight where it was branched from, once", async () => {
  const schema = newSchema();
  const main = await testStore({ schema });
  const conversationId = await run(main.upsertConversation({ chatId: 1001 }));
  await run(main.setActiveHarness(conversationId, "codex"));
  await run(main.setWorkingDirectory(conversationId, "sessionfs:fs-0123456789abcdef"));
  await run(main.setTelegramOffset(41));
  await run(main.recordTelegramUpdate({ update_id: 41, message: { message_id: 7, date: 0, from: { id: 1001, is_bot: false, first_name: "Operator" }, chat: { id: 1001, type: "private", first_name: "Operator" }, text: "queued" } }));
  await run(main.enqueuePromptJob({ conversationId, chatId: 1001, messageId: 7, prompt: "queued", harness: "codex" }));
  const pending = await run(main.createPendingResponse(1001, 8));
  await run(main.upsertActiveTurn({ conversationId, chatId: "1001", messageId: "8", harness: "codex", pendingResponseId: pending }));
  await run(main.markPendingResponseComplete(pending));
  await run(main.enqueueOutboxText({ chatId: 1001, text: "on its way" }));
  const [button] = await run(main.createCallbackActions(conversationId, [{ kind: "refresh" }]));
  await run(main.recordRestartEvent({ cause: "operator_induced", thread_key: conversationId, channel: "1001", thread_ts: "8", session_id: null }));
  await run(main.setCodexLogin('{"tokens":{"refresh_token":"once"}}'));

  const branch = await testStore({ schema, branch: "try-codex" });
  assert.deepEqual(await run(branch.getMount(conversationId)), { harness: "codex", workingDirectory: "sessionfs:fs-0123456789abcdef", sessionId: null });
  assert.equal(await run(branch.getTelegramOffset), undefined);
  assert.deepEqual(await run(branch.listPendingPromptConversations), []);
  assert.equal(await run(branch.hasOpenPromptJobs(conversationId)), false);
  assert.deepEqual(await run(branch.getActiveTurns), []);
  assert.deepEqual(await run(branch.getCompletedResponsesPendingDelivery), []);
  assert.equal(await run(branch.getPendingOutboxCount), 0);
  assert.equal(await run(branch.getRestartEvent(conversationId)), null);
  assert.equal(await run(branch.getCodexLogin), null);
  assert.equal(await run(branch.consumeCallbackAction(button ?? "")), null);
  assert.equal(await run(branch.getState("branch")), "try-codex");

  // Started once: what the branch does itself is kept when it starts again.
  await run(branch.setTelegramOffset(3));
  assert.equal(await run((await testStore({ schema, branch: "try-codex" })).getTelegramOffset), 3);
  // A branch of the branch is made its own in turn.
  assert.equal(await run((await testStore({ schema, branch: "try-again" })).getTelegramOffset), undefined);
});

test("an alasio forks a session one of its conversations knows for the branch whose token asks, into the branch's sessions, and none while a turn runs in it", async () => {
  const directory = mkdtempSync(join(tmpdir(), "alasio-branch-forks-"));
  const keyFile = join(directory, "fork-key");
  const store = await testStore();
  const conversationId = await run(store.upsertConversation({ chatId: 1001 }));
  await run(store.setWorkingDirectory(conversationId, "sessionfs:fs-abc123"));
  const forked: string[][] = [];
  let outcome: Effect.Effect<void, KubeApiError | SessionForkError> = Effect.void;
  const sandboxes = SessionSandboxes.of({
    volumes: {
      create: () => Effect.die("the parent makes no session for a branch"),
      fork: (source, volumeId, namespace = "alasio-sessions") =>
        Effect.sync(() => forked.push([source, volumeId, namespace])).pipe(Effect.andThen(outcome), Effect.as({ volumeId, netMode: "none" as const })),
      destroy: () => Effect.die("the parent deletes no session for a branch"),
      forks: Effect.die("the parent lists no fork for a branch"),
    },
    harnessDirectory: () => directory,
    ensureSession: () => Effect.die("the parent brings no session up for a branch"),
    readFile: () => Effect.die("the parent reads no file for a branch"),
  });
  const scope = Effect.runSync(Scope.make());
  try {
    const services = await Effect.runPromise(Layer.buildWithScope(Layer.mergeAll(ActiveTurns.layer, Layer.succeed(Store, store), Layer.succeed(SessionSandboxes, sandboxes)), scope));
    const { port } = await Effect.runPromise(Effect.provide(serveBranchForks({ port: 0, host: "127.0.0.1", keyFile }), services).pipe(Scope.provide(scope)));
    const tokenFile = join(directory, "token");
    const asking = (branch: string, volumeId: string) =>
      Effect.runPromiseExit(parentForks({ url: `http://127.0.0.1:${port}`, branch, tokenFile })(volumeId));
    const refusal = (exit: Exit.Exit<boolean, { readonly message: string }>) => (Exit.isFailure(exit) ? Option.getOrThrow(Exit.findErrorOption(exit)).message : "");

    // No key yet: no branch is let in.
    writeFileSync(tokenFile, forkToken("key", "try"));
    assert.match(refusal(await asking("try", "fs-abc123")), /did not fork the session fs-abc123: the request bears no branch's token$/u);
    writeFileSync(keyFile, "key\n");
    assert.match(refusal(await asking("other", "fs-abc123")), /no branch's token/u);
    assert.deepEqual(forked, []);

    assert.deepEqual(await asking("try", "fs-abc123"), Exit.succeed(true));
    assert.deepEqual(forked, [["fs-abc123", "fs-abc123", "alasio-branch-try-sessions"]]);
    forked.length = 0;
    // One the parent never had is not its to give; one it made, which no conversation mounts, is.
    assert.deepEqual(await asking("try", "fs-zzz999"), Exit.succeed(false));
    await run(store.recordSessionWorkspace({ volumeId: "fs-made01", forkedFrom: null }));
    assert.deepEqual(await asking("try", "fs-made01"), Exit.succeed(false));
    await run(store.markSessionWorkspaceMade("fs-made01", "full"));
    assert.deepEqual(await asking("try", "fs-made01"), Exit.succeed(true));
    // Forked already, by an earlier ask.
    outcome = Effect.fail(new KubeApiError({ status: 409, cause: new Error("exists") }));
    assert.deepEqual(await asking("try", "fs-abc123"), Exit.succeed(true));
    outcome = Effect.fail(new SessionForkError({ message: "the clone of alasio-sessions/data-fs-abc123 failed: BackoffLimitExceeded" }));
    assert.match(refusal(await asking("try", "fs-abc123")), /did not fork the session fs-abc123: the clone of .* failed: BackoffLimitExceeded$/u);
    outcome = Effect.void;

    // While a turn runs in it, which the fork would suspend.
    const turning = await Effect.runPromise(Deferred.make<void>());
    const turn = Effect.runFork(Effect.scoped(Effect.gen(function*() {
      const turns = yield* ActiveTurns;
      yield* turns.register(conversationId, { stop: () => Effect.void, steer: () => Effect.succeed(false), cliInitiated: false });
      yield* Deferred.await(turning);
    })).pipe(Effect.provide(services)));
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.match(refusal(await asking("try", "fs-abc123")), /a turn runs in the session fs-abc123 now/u);
    await Effect.runPromise(Deferred.succeed(turning, undefined));
    await Effect.runPromise(Fiber.join(turn));
    assert.deepEqual(await asking("try", "fs-abc123"), Exit.succeed(true));
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
    rmSync(directory, { recursive: true, force: true });
  }
});

test("a branch environment runs no turn in a folder, which it cannot copy, and says why, failing the prompt", async () => {
  const store = await testStore();
  const conversationId = await run(store.upsertConversation({ chatId: 1001 }));
  await run(store.setActiveHarness(conversationId, "codex"));
  await run(store.setWorkingDirectory(conversationId, "/home/operator/project"));
  const telegram = recordingTelegram();
  await withServices({ store, telegram: telegram.layer, branch: "try" }, async (alasio) => {
    await alasio.runPromise(Effect.flatMap(Turns, (turns) => turns.submit({ conversationId, chatId: 1001, messageId: 7, prompt: "hi", fileIds: [], visibleText: "hi" })));
    for (let waited = 0; telegram.calls.sendMessage.length === 0 && waited < 5000; waited += 20) await new Promise((resolve) => setTimeout(resolve, 20));
    const [[chatId, text] = []] = telegram.calls.sendMessage;
    assert.equal(chatId, "1001");
    assert.match(String(text), /^Codex hit an error: This is the branch environment try, .* A folder is this machine's own files, which cannot be copied on write, so a branch never works in one: use \/workspace for a session workspace\.$/u);
    assert.equal(await run(store.hasOpenPromptJobs(conversationId)), false);
    // A session workspace is the branch's to work in, where its deployment has them.
    const refusal = await alasio.runPromise(Effect.flip(Effect.flatMap(Harnesses, (harnesses) => harnesses.getFor("codex", "sessionfs:fs-abc123"))));
    assert.equal(refusal._tag, "SessionFilesystemsDisabled");
  });
});

test("a session a branch inherited is recorded as one it made, keeping what its parent recorded of it", async () => {
  const store = await testStore({ branch: "try" });
  await run(store.recordInheritedSessionWorkspace("fs-legacy1", "none"));
  await run(store.recordSessionWorkspace({ volumeId: "fs-fork001", forkedFrom: "fs-legacy1" }));
  await run(store.markSessionWorkspaceMade("fs-fork001", "full"));
  const before = (await run(store.listSessionWorkspaces)).find(({ volumeId }) => volumeId === "fs-fork001");
  await run(store.recordInheritedSessionWorkspace("fs-fork001", "full"));
  const workspaces = await run(store.listSessionWorkspaces);
  assert.deepEqual(workspaces.find(({ volumeId }) => volumeId === "fs-fork001"), before);
  const legacy = workspaces.find(({ volumeId }) => volumeId === "fs-legacy1");
  assert.deepEqual([legacy?.forkedFrom, legacy?.netMode, legacy?.madeAt instanceof Date], [null, "none", true]);
});

test("main's state is never made a branch's", async () => {
  const schema = newSchema();
  await run((await testStore({ schema })).setTelegramOffset(41));
  const main = await testStore({ schema });
  assert.equal(await run(main.getTelegramOffset), 41);
  assert.equal(await run(main.getState("branch")), null);
});
