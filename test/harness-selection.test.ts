import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, realpathSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import type { CallbackQuery, InlineKeyboardButton, Message } from "@grammyjs/types";
import { Effect, Exit, Scope } from "effect";

import { ActiveTurns, type RunningTurn } from "../src/harness/active-turns.ts";
import {
  type FreshSessionParams,
  type Harness,
  NO_SERVICE_MOUNTED,
  NO_WORKSPACE_MOUNTED,
} from "../src/harness/index.ts";
import {
  CLAUDE_HARNESS,
  CODEX_HARNESS,
  type HarnessName,
  getDefaultHarness,
  harnessDisplayName,
  normalizeHarnessName,
} from "../src/harness/names.ts";
import { parseCommand } from "../src/operator/command-parser.ts";
import { Turns } from "../src/codex/turns.ts";
import { Mounts } from "../src/operator/mounts.ts";
import { processPrompt } from "../src/operator/prompts.ts";
import { buildRestartSyntheticText } from "../src/operator/restart-prompts.ts";
import {
  CHOOSE_SERVICE_NOTICE,
  buildServicePanel,
  handleServiceControlCallback,
  handleServiceTextCommand,
} from "../src/operator/service-control.ts";
import { harnessSessionOf } from "../src/persistence/conversation-repository.ts";
import type { Store } from "../src/persistence/store.ts";
import type { SessionSandboxes } from "../src/sandbox/index.ts";
import { handleCallbackQuery } from "../src/telegram/callback-handler.ts";
import type { TextMessageOptions } from "../src/telegram/client.ts";
import { handleMessage } from "../src/telegram/message-handler.ts";
import { listWorkspaceCandidates } from "../src/workspace/policy.ts";
import { run, testStore } from "./support/store.ts";
import { type RecordingTelegram, recordingTelegram } from "./support/telegram-calls.ts";
import { type TestAlasio, type TestServicesOptions, withServices } from "./support/turns.ts";

/** Where a test's workspace root and its folders are. */
interface WorkspacePaths {
  readonly root: string;
  readonly workspaceRoot: string;
  readonly repo: string;
  readonly plain: string;
}

/**
 * Each test gets a store plus a workspace root holding `repo` (a git
 * repository), `plain` (a bare folder), a hidden folder, a file and a symlink
 * that escapes the root.
 */
async function withStore<T>(use: (store: Store["Service"], paths: WorkspacePaths) => T | Promise<T>): Promise<T> {
  const root = realpathSync(mkdtempSync(join(tmpdir(), "alasio-harness-")));
  const workspaceRoot = join(root, "workspaces");
  mkdirSync(join(workspaceRoot, "repo"), { recursive: true });
  execFileSync("git", ["init", "-q", "-b", "main"], { cwd: join(workspaceRoot, "repo") });
  mkdirSync(join(workspaceRoot, "plain"));
  mkdirSync(join(workspaceRoot, ".hidden"));
  writeFileSync(join(workspaceRoot, "notes.txt"), "not a folder");
  symlinkSync(root, join(workspaceRoot, "escape"));
  try {
    return await use(await testStore(), { root, workspaceRoot, repo: join(workspaceRoot, "repo"), plain: join(workspaceRoot, "plain") });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}

/** A harness that only starts sessions, and records each start. */
interface FakeHarness extends Harness {
  readonly calls: ["startFreshSession", FreshSessionParams][];
}

function createFakeHarness(name: HarnessName, { sessionId = `${name}-fresh` } = {}): FakeHarness {
  const calls: FakeHarness["calls"] = [];
  return {
    name,
    displayName: harnessDisplayName(name),
    supportsGoals: name === CODEX_HARNESS,
    supportsWarmup: false,
    sessions: {
      listSessions: () => assert.fail("listSessions"),
      getTotalSessionPages: () => assert.fail("getTotalSessionPages"),
      getSessionByNumber: () => assert.fail("getSessionByNumber"),
      getSessionLastMessage: () => assert.fail("getSessionLastMessage"),
      listSessionMessages: () => assert.fail("listSessionMessages"),
      getTotalRewindPages: () => assert.fail("getTotalRewindPages"),
      createForkedSession: () => assert.fail("createForkedSession"),
    },
    calls,
    startFreshSession: (args) =>
      Effect.sync(() => {
        calls.push(["startFreshSession", args]);
        return sessionId;
      }),
    warmSession: () => Effect.succeed(false),
    runTurn: () => Effect.die(new Error("not exercised")),
    listModels: () => assert.fail("listModels"),
    defaultModelChoice: () => assert.fail("defaultModelChoice"),
  };
}

/** Both harnesses, standing in for every folder. */
function createHarnesses() {
  const codex = createFakeHarness(CODEX_HARNESS);
  const claude = createFakeHarness(CLAUDE_HARNESS);
  return { harnesses: { [CODEX_HARNESS]: codex, [CLAUDE_HARNESS]: claude }, codex, claude };
}

/** alasio's services over `store`, both stand-in harnesses, and a Telegram client recording its calls, for one test. */
function withAlasio<T>(
  store: Store["Service"],
  use: (alasio: TestAlasio, harnesses: ReturnType<typeof createHarnesses>) => T | Promise<T>,
  { telegram = recordingTelegram(), ...options }: Omit<TestServicesOptions, "store" | "harnesses" | "telegram"> & { readonly telegram?: RecordingTelegram } = {},
): Promise<T> {
  const harnesses = createHarnesses();
  return withServices({ store, harnesses: harnesses.harnesses, telegram: telegram.layer, ...options }, (alasio) => use(alasio, harnesses));
}

/** Holds the conversation busy, as a running turn does, with `turn`'s stop and steer, until the returned release. */
async function occupy(alasio: TestAlasio, conversationId: string, turn: Partial<RunningTurn> = {}): Promise<() => Promise<void>> {
  const scope = await alasio.runPromise(Scope.make());
  await alasio.runPromise(Effect.flatMap(ActiveTurns, (activeTurns) =>
    activeTurns.register(conversationId, { stop: () => Effect.void, steer: () => Effect.succeed(false), cliInitiated: false, ...turn })).pipe(Scope.provide(scope)));
  return () => alasio.runPromise(Scope.close(scope, Exit.void));
}

/** Runs `f` on alasio's Mounts, failing as it does. */
const mounts = <A, E>(alasio: TestAlasio, f: (mounts: Mounts["Service"]) => Effect.Effect<A, E>): Promise<A> =>
  alasio.runPromise(Effect.flatMap(Mounts, f));

/** Session filesystems a test offers without making any. */
const unusedSandbox: SessionSandboxes["Service"] = {
  volumes: {
    create: () => Effect.die(new Error("no volume is made")),
    fork: () => Effect.die(new Error("no volume is forked")),
    destroy: () => Effect.die(new Error("no volume is destroyed")),
    forks: Effect.die(new Error("no fork is listed")),
  },
  harnessDirectory: () => assert.fail("harnessDirectory"),
  ensureSession: () => Effect.die(new Error("no session is ensured")),
  readFile: () => Effect.die(new Error("no file is read")),
};

/** The buttons of a message's inline keyboard. */
function inlineKeyboard(options: TextMessageOptions | undefined): InlineKeyboardButton[][] {
  assert.ok(options?.reply_markup, "the message has an inline keyboard");
  return options.reply_markup.inline_keyboard;
}

/** A press of the button `data` under message `messageId` in the private chat of user `userId`. */
function buttonPress(id: string, data: string, userId: number, messageId: number): CallbackQuery {
  return {
    id,
    chat_instance: `instance-${userId}`,
    data,
    from: { id: userId, is_bot: false, first_name: "Operator" },
    message: { message_id: messageId, date: 0, chat: { id: userId, type: "private", first_name: "Operator" } },
  };
}

// A harness name from outside HarnessName, as a typo or an old row would hold it.
const UNKNOWN_HARNESS: string = "gemini";

test("harness names normalize operator spellings", () => {
  assert.equal(normalizeHarnessName("claude"), CLAUDE_HARNESS);
  assert.equal(normalizeHarnessName("Claude-Code"), CLAUDE_HARNESS);
  assert.equal(normalizeHarnessName("codex"), CODEX_HARNESS);
  assert.equal(normalizeHarnessName("gemini"), CODEX_HARNESS);
  assert.equal(normalizeHarnessName("gemini", null), null);
  assert.equal(harnessDisplayName(CLAUDE_HARNESS), "Claude");
});

test("new conversations are neutral unless ALASIO_DEFAULT_HARNESS opts into a service", () => {
  assert.equal(getDefaultHarness({}), null);
  assert.equal(getDefaultHarness({ ALASIO_DEFAULT_HARNESS: "" }), null);
  assert.equal(getDefaultHarness({ ALASIO_DEFAULT_HARNESS: "gemini" }), null);
  assert.equal(getDefaultHarness({ ALASIO_DEFAULT_HARNESS: "codex" }), CODEX_HARNESS);
  assert.equal(getDefaultHarness({ ALASIO_DEFAULT_HARNESS: "Claude Code" }), CLAUDE_HARNESS);
});

test("command parser recognizes /service controls", () => {
  assert.deepEqual(parseCommand("/service"), { type: "service", target: "" });
  assert.deepEqual(parseCommand("/service@AlasioBot"), { type: "service", target: "" });
  assert.deepEqual(parseCommand("/service claude"), { type: "service", target: "claude" });
  assert.deepEqual(parseCommand("/service Codex"), { type: "service", target: "codex" });
  assert.equal(parseCommand("/services"), null);
});

test("conversations keep one parked session pointer per harness", async () => {
  await withStore(async (store) => {
    const conversationId = await run(store.upsertConversation({ chatId: "1", user: { id: 1 } }));
    assert.equal((await run(store.getMount(conversationId))).harness, null);
    assert.equal((await run(store.getMount(conversationId))).sessionId, null);
    // With no service mounted, a session has no harness to belong to, and a prompt none to run on.
    await run(store.setSessionId(conversationId, "orphan"));
    const orphaned = await run(store.getConversation(conversationId));
    assert.deepEqual([orphaned?.codex_session_id, orphaned?.claude_session_id], [null, null]);
    await assert.rejects(run(store.enqueuePromptJob({ conversationId, chatId: "1", messageId: "1", prompt: "hi" })), /harness/);

    await run(store.setActiveHarness(conversationId, CODEX_HARNESS));
    assert.equal((await run(store.getMount(conversationId))).harness, CODEX_HARNESS);
    assert.deepEqual(await run(store.listConversationsWithSessions(CODEX_HARNESS)), []);
    await run(store.setWorkingDirectory(conversationId, "/work/a"));
    await run(store.setSessionId(conversationId, "codex-session"));
    assert.equal((await run(store.getMount(conversationId))).sessionId, "codex-session");

    await run(store.setActiveHarness(conversationId, CLAUDE_HARNESS));
    assert.equal((await run(store.getMount(conversationId))).harness, CLAUDE_HARNESS);
    assert.equal((await run(store.getMount(conversationId))).sessionId, null);
    await run(store.setSessionId(conversationId, "claude-session"));
    assert.equal((await run(store.getMount(conversationId))).sessionId, "claude-session");
    assert.equal(harnessSessionOf(await run(store.getConversation(conversationId)), CODEX_HARNESS), "codex-session");

    await run(store.setActiveHarness(conversationId, CODEX_HARNESS));
    assert.equal((await run(store.getMount(conversationId))).sessionId, "codex-session");
    assert.equal(harnessSessionOf(await run(store.getConversation(conversationId)), CLAUDE_HARNESS), "claude-session");

    assert.deepEqual(
      (await run(store.listConversationsWithSessions(CODEX_HARNESS))).map((row) => [row.session_id, row.working_directory]),
      [["codex-session", "/work/a"]],
    );
    assert.deepEqual(await run(store.listConversationsWithSessions(CLAUDE_HARNESS)), []);
  });
});

test("switching folders parks both harness session pointers per folder", async () => {
  await withStore(async (store) => {
    const conversationId = await run(store.upsertConversation({ chatId: "15", user: { id: 15 } }));
    assert.equal((await run(store.getMount(conversationId))).workingDirectory, null);
    await run(store.setActiveHarness(conversationId, CODEX_HARNESS));
    await run(store.setWorkingDirectory(conversationId, "/work/a"));
    await run(store.setSessionId(conversationId, "codex-a"));
    await run(store.setActiveHarness(conversationId, CLAUDE_HARNESS));
    await run(store.setSessionId(conversationId, "claude-a"));
    await run(store.setActiveHarness(conversationId, CODEX_HARNESS));

    await run(store.setWorkingDirectory(conversationId, "/work/b"));
    assert.equal((await run(store.getMount(conversationId))).workingDirectory, "/work/b");
    assert.equal((await run(store.getMount(conversationId))).sessionId, null);
    assert.equal(harnessSessionOf(await run(store.getConversation(conversationId)), CLAUDE_HARNESS), null);
    await run(store.setSessionId(conversationId, "codex-b"));

    await run(store.setWorkingDirectory(conversationId, "/work/a"));
    assert.equal((await run(store.getMount(conversationId))).sessionId, "codex-a");
    assert.equal(harnessSessionOf(await run(store.getConversation(conversationId)), CLAUDE_HARNESS), "claude-a");
    await run(store.setWorkingDirectory(conversationId, "/work/b"));
    assert.equal((await run(store.getMount(conversationId))).sessionId, "codex-b");
    assert.equal(harnessSessionOf(await run(store.getConversation(conversationId)), CLAUDE_HARNESS), null);
  });
});

test("every session a harness is pointed at is listed with its folder, once", async () => {
  await withStore(async (store) => {
    const parked = await run(store.upsertConversation({ chatId: "16", user: { id: 16 } }));
    await run(store.setActiveHarness(parked, CLAUDE_HARNESS));
    await run(store.setWorkingDirectory(parked, "/work/a"));
    await run(store.setSessionId(parked, "claude-a"));
    await run(store.setWorkingDirectory(parked, "/work/b"));
    await run(store.setSessionId(parked, "claude-b"));

    const live = await run(store.upsertConversation({ chatId: "17", user: { id: 17 } }));
    await run(store.setActiveHarness(live, CLAUDE_HARNESS));
    await run(store.setWorkingDirectory(live, "/work/c"));
    await run(store.upsertActiveTurn({ conversationId: live, chatId: "17", messageId: "1", sessionId: null, harness: CLAUDE_HARNESS, prompt: "hi" }));
    await run(store.updateActiveTurnSessionId(live, "claude-live"));

    const codex = await run(store.upsertConversation({ chatId: "18", user: { id: 18 } }));
    await run(store.setActiveHarness(codex, CODEX_HARNESS));
    await run(store.setWorkingDirectory(codex, "/work/d"));
    await run(store.setSessionId(codex, "codex-d"));

    assert.deepEqual(
      (await run(store.listHarnessSessionReferences(CLAUDE_HARNESS))).sort((x, y) => x.sessionId.localeCompare(y.sessionId)),
      [
        { sessionId: "claude-a", workingDirectory: "/work/a" },
        { sessionId: "claude-b", workingDirectory: "/work/b" },
        { sessionId: "claude-live", workingDirectory: "/work/c" },
      ],
    );
    assert.deepEqual(await run(store.listHarnessSessionReferences(CODEX_HARNESS)), [{ sessionId: "codex-d", workingDirectory: "/work/d" }]);
  });
});

test("callback actions capture the active harness generation", async () => {
  await withStore(async (store) => {
    const conversationId = await run(store.upsertConversation({ chatId: "2", user: { id: 2 } }));
    const [neutralId = ""] = await run(store.createCallbackActions(conversationId, [{ kind: "service:use" }]));
    assert.equal((await run(store.consumeCallbackAction(neutralId)))?.payload["expectedHarness"], null);
    await run(store.setActiveHarness(conversationId, CODEX_HARNESS));
    const [actionId = ""] = await run(store.createCallbackActions(conversationId, [{ kind: "control:current" }]));
    const action = await run(store.consumeCallbackAction(actionId));
    assert.equal(action?.payload["expectedHarness"], CODEX_HARNESS);
  });
});

test("switching services is refused while a turn is active or prompts are queued", async () => {
  await withStore(async (store, paths) => {
    const conversationId = await run(store.upsertConversation({ chatId: "3", user: { id: 3 } }));
    await run(store.setActiveHarness(conversationId, CODEX_HARNESS));
    await run(store.setWorkingDirectory(conversationId, paths.repo));
    await withAlasio(store, async (alasio) => {
      let release = await occupy(alasio, conversationId);
      await assert.rejects(mounts(alasio, (m) => m.switchHarness(conversationId, CLAUDE_HARNESS)), /Codex is currently working/);
      await release();

      const job = await run(store.enqueuePromptJob({ conversationId, chatId: "3", messageId: "10", prompt: "later" }));
      assert.equal(job.harness, CODEX_HARNESS);
      await assert.rejects(mounts(alasio, (m) => m.switchHarness(conversationId, CLAUDE_HARNESS)), /Queued prompts are still waiting/);
      await run(store.setPromptJobDisposition(job.id, "cancelled"));

      const result = await mounts(alasio, (m) => m.switchHarness(conversationId, CLAUDE_HARNESS));
      assert.deepEqual(result, { switched: true, previous: CODEX_HARNESS, next: CLAUDE_HARNESS, sessionId: null, workingDirectory: paths.repo });
      assert.equal((await run(store.getMount(conversationId))).harness, CLAUDE_HARNESS);
      const again = await mounts(alasio, (m) => m.switchHarness(conversationId, CLAUDE_HARNESS));
      assert.equal(again.switched, false);
      await assert.rejects(mounts(alasio, (m) => m.switchHarness(conversationId, UNKNOWN_HARNESS)), /Unknown service/);

      release = await occupy(alasio, conversationId);
      await assert.rejects(mounts(alasio, (m) => m.switchWorkspace(conversationId, "plain")), /Claude is currently working/);
      await assert.rejects(mounts(alasio, (m) => m.createWorkspace(conversationId, "fresh")), /Claude is currently working/);
      await release();
      const same = await mounts(alasio, (m) => m.switchWorkspace(conversationId, "repo"));
      assert.deepEqual(same, { switched: false, previous: paths.repo, workingDirectory: paths.repo });
    }, { workspaceRoot: paths.workspaceRoot });
  });
});

test("workspace policy keeps every folder under the root", async () => {
  await withStore(async (store, paths) => {
    const conversationId = await run(store.upsertConversation({ chatId: "16", user: { id: 16 } }));
    await run(store.setActiveHarness(conversationId, CODEX_HARNESS));
    await withAlasio(store, async (alasio) => {
      const switchTo = (target: string) => mounts(alasio, (m) => m.switchWorkspace(conversationId, target));
      const create = (name: string) => mounts(alasio, (m) => m.createWorkspace(conversationId, name));
      const byName = await switchTo("repo");
      assert.deepEqual(byName, { switched: true, previous: null, workingDirectory: paths.repo });
      const byAbsolute = await switchTo(paths.plain);
      assert.equal(byAbsolute.workingDirectory, paths.plain);
      assert.equal((await run(store.getMount(conversationId))).workingDirectory, paths.plain);

      await assert.rejects(switchTo("missing"), /does not exist under/);
      await assert.rejects(switchTo("../"), /outside the workspace root/);
      await assert.rejects(switchTo("escape"), /outside the workspace root/);
      await assert.rejects(switchTo("/etc"), /outside the workspace root/);
      await assert.rejects(switchTo("notes.txt"), /not a directory/);
      await assert.rejects(switchTo(""), /Folder path is empty/);
      assert.equal((await run(store.getMount(conversationId))).workingDirectory, paths.plain);

      await assert.rejects(create("../oops"), /Folder names may only use/);
      await assert.rejects(create(".hidden"), /Folder names may only use/);
      await assert.rejects(create("repo"), /already exists/);
      const created = await create("fresh-1");
      assert.deepEqual(created, { switched: true, created: true, previous: paths.plain, workingDirectory: join(paths.workspaceRoot, "fresh-1") });
      assert.equal((await run(store.getMount(conversationId))).workingDirectory, created.workingDirectory);
      assert.equal(execFileSync("git", ["symbolic-ref", "--short", "HEAD"], { cwd: created.workingDirectory }).toString().trim(), "main");

      const candidates = (await listWorkspaceCandidates(paths.workspaceRoot)).map((candidate) => [candidate.name, candidate.git]);
      assert.deepEqual(candidates, [["fresh-1", true], ["repo", true], ["plain", false]]);
    }, { workspaceRoot: paths.workspaceRoot });
  });
});

test("new sessions and active turns are recorded under the active harness", async () => {
  await withStore(async (store, paths) => {
    const conversationId = await run(store.upsertConversation({ chatId: "4", user: { id: 4 } }));
    await withAlasio(store, async (alasio, { claude, codex }) => {
      const startNewSession = () => alasio.runPromise(Effect.flatMap(Turns, (turns) => turns.startNewSession(conversationId)));
      await run(store.setActiveHarness(conversationId, CLAUDE_HARNESS));
      await assert.rejects(startNewSession(), /No folder is mounted/);
      await run(store.setWorkingDirectory(conversationId, paths.repo));
      const sessionId = await startNewSession();
      assert.equal(sessionId, "claude-fresh");
      assert.equal(claude.calls.length, 1);
      assert.equal(claude.calls[0]?.[1].workingDirectory, paths.repo);
      assert.equal(codex.calls.length, 0);
      assert.equal(harnessSessionOf(await run(store.getConversation(conversationId)), CLAUDE_HARNESS), "claude-fresh");
      assert.equal(harnessSessionOf(await run(store.getConversation(conversationId)), CODEX_HARNESS), null);

      await run(store.upsertActiveTurn({ conversationId, chatId: "4", messageId: "1", sessionId: null, harness: CLAUDE_HARNESS, prompt: "hi" }));
      const [turn] = await run(store.getActiveTurns);
      assert.equal(turn?.harness, CLAUDE_HARNESS);
      await run(store.updateActiveTurnSessionId(conversationId, "claude-live"));
      assert.equal(harnessSessionOf(await run(store.getConversation(conversationId)), CLAUDE_HARNESS), "claude-live");
      assert.equal(harnessSessionOf(await run(store.getConversation(conversationId)), CODEX_HARNESS), null);
    });
  });
});

test("restart recovery restores the interrupted turn's own harness session", async () => {
  await withStore(async (store) => {
    const conversationId = await run(store.upsertConversation({ chatId: "5", user: { id: 5 } }));
    await run(store.setActiveHarness(conversationId, CLAUDE_HARNESS));
    await run(store.upsertActiveTurn({ conversationId, chatId: "5", messageId: "8", sessionId: "claude-1", harness: CLAUDE_HARNESS, prompt: "restart please" }));
    await run(store.recordRestartEvent({ cause: "self_induced", thread_key: conversationId, channel: "5", thread_ts: "8", session_id: "claude-1" }));
    const [turn] = await run(store.getActiveTurns);
    assert.ok(turn);
    // An empty environment gives the defaults even when the tests themselves run in a
    // release whose names differ.
    const job = await run(store.stageRestartRecovery({ turn, prompt: buildRestartSyntheticText("self_induced", turn.harness, {}) }));
    assert.ok(job);
    assert.equal(job.harness, CLAUDE_HARNESS);
    assert.match(job.prompt, /You are Claude, connected through the Kubernetes Deployment alasio in namespace alasio/);
    assert.match(job.prompt, /restart is `kubectl -n alasio rollout restart deployment\/alasio`/);
    assert.match(job.prompt, /not deleting alasio's pod/);
    assert.equal(harnessSessionOf(await run(store.getConversation(conversationId)), CLAUDE_HARNESS), "claude-1");
    assert.equal(harnessSessionOf(await run(store.getConversation(conversationId)), CODEX_HARNESS), null);
    const operator = buildRestartSyntheticText("operator_induced", undefined, {});
    assert.match(operator, /You are Codex/);
    const named = buildRestartSyntheticText("self_induced", CLAUDE_HARNESS, { ALASIO_DEPLOYMENT: "bot-alasio", ALASIO_NAMESPACE: "bots" });
    assert.match(named, /connected through the Kubernetes Deployment bot-alasio in namespace bots/);
    assert.match(named, /`kubectl -n bots rollout restart deployment\/bot-alasio`/);
  });
});

test("service panel lists both harness mounts and offers the inactive switch", async () => {
  await withStore(async (store) => {
    const conversationId = await run(store.upsertConversation({ chatId: "6", user: { id: 6 } }));
    const neutral = buildServicePanel({ conversation: await run(store.getConversation(conversationId)), working: false, notice: CHOOSE_SERVICE_NOTICE });
    assert.match(neutral.text, /Active: none/);
    assert.match(neutral.text, /Nothing runs until a service is chosen/);
    assert.match(neutral.text, /No service is mounted/);
    assert.deepEqual(
      neutral.keyboard.flat().map((button) => button.text),
      ["Use Codex", "Use Claude", "Close"],
    );

    await run(store.setActiveHarness(conversationId, CODEX_HARNESS));
    await run(store.setSessionId(conversationId, "codex-abcdef12"));
    const panel = buildServicePanel({ conversation: await run(store.getConversation(conversationId)), working: true });
    assert.match(panel.text, /Active: Codex/);
    assert.match(panel.text, /Status: working/);
    assert.match(panel.text, /\* Codex: session codex-ab/);
    assert.match(panel.text, /Claude: no mounted session/);
    const buttons = panel.keyboard.flat().map((button) => button.text);
    assert.deepEqual(buttons, ["Use Claude", "Close"]);
  });
});

test("/service text command switches and reports the outcome", async () => {
  await withStore(async (store, paths) => {
    const conversationId = await run(store.upsertConversation({ chatId: "7", user: { id: 7 } }));
    // With a folder mounted, a newly mounted service needs no folder picker after it.
    await run(store.setWorkingDirectory(conversationId, paths.repo));
    const telegram = recordingTelegram();
    await withAlasio(store, async (alasio) => {
      const command = (target: string) => alasio.runPromise(handleServiceTextCommand({ conversationId, chatId: "7", target }));
      await command("claude");
      assert.equal((await run(store.getMount(conversationId))).harness, CLAUDE_HARNESS);
      assert.match(telegram.calls.sendMessage[0]?.[1] ?? "", /Mounted Claude\. Send a message to start\./);
      assert.match(telegram.calls.sendMessage[0]?.[1] ?? "", /Active: Claude/);

      await command("codex");
      assert.match(telegram.calls.sendMessage[1]?.[1] ?? "", /Switched to Codex\./);

      await command("gemini");
      assert.match(telegram.calls.sendMessage[2]?.[1] ?? "", /Unknown service "gemini"/);
      assert.equal((await run(store.getMount(conversationId))).harness, CODEX_HARNESS);
      assert.equal(telegram.calls.sendMessage.length, 3);
    }, { telegram, workspaceRoot: paths.workspaceRoot });
  });
});

test("service callbacks switch harness and surface refusals in the panel", async () => {
  await withStore(async (store) => {
    const conversationId = await run(store.upsertConversation({ chatId: "8", user: { id: 8 } }));
    await run(store.setActiveHarness(conversationId, CODEX_HARNESS));
    const telegram = recordingTelegram();
    await withAlasio(store, async (alasio) => {
      await occupy(alasio, conversationId);
      const [actionId = ""] = await run(store.createCallbackActions(conversationId, [{ kind: "service:use", payload: { harness: CLAUDE_HARNESS } }]));
      const action = await run(store.consumeCallbackAction(actionId));
      assert.ok(action);
      await alasio.runPromise(handleServiceControlCallback({ action, callbackQueryId: "cb-1", chatId: "8", messageId: 5 }));
      assert.match(telegram.calls.answerCallbackQuery[0]?.[1] ?? "", /Codex is currently working/);
      assert.match(telegram.calls.editMessageText[0]?.[2] ?? "", /Active: Codex/);
      assert.match(telegram.calls.editMessageText[0]?.[2] ?? "", /Status: working/);
      assert.match(telegram.calls.editMessageText[0]?.[2] ?? "", /Codex is currently working/);
      assert.equal((await run(store.getMount(conversationId))).harness, CODEX_HARNESS);
    }, { telegram });
  });
});

test("choosing a service from its panel chains into a folder picker that offers a new empty workspace", async () => {
  await withStore(async (store, paths) => {
    const conversationId = await run(store.upsertConversation({ chatId: "10", user: { id: 10 } }));
    const [actionId = ""] = await run(store.createCallbackActions(conversationId, [{ kind: "service:use", payload: { harness: CLAUDE_HARNESS } }]));
    const telegram = recordingTelegram();
    await withAlasio(store, async (alasio) => {
      await alasio.runPromise(handleCallbackQuery(buttonPress("cb-10", actionId, 10, 4)));
      assert.equal((await run(store.getMount(conversationId))).harness, CLAUDE_HARNESS);
      const picker = telegram.calls.sendMessage.at(-1);
      assert.match(picker?.[1] ?? "", /Folder: none/);
      assert.ok(inlineKeyboard(picker?.[2]).flat().some((button) => button.text === "New empty workspace…"));
    }, { telegram, workspaceRoot: paths.workspaceRoot, allowedUserIds: "10", sandbox: unusedSandbox });
  });
});

test("callback handler rejects panels created under another harness", async () => {
  await withStore(async (store, paths) => {
    const conversationId = await run(store.upsertConversation({ chatId: "9", user: { id: 9 } }));
    await run(store.setActiveHarness(conversationId, CODEX_HARNESS));
    const [actionId = ""] = await run(store.createCallbackActions(conversationId, [{ kind: "control:current" }]));
    await run(store.setActiveHarness(conversationId, CLAUDE_HARNESS));
    await run(store.setWorkingDirectory(conversationId, paths.repo));
    const telegram = recordingTelegram();
    await withAlasio(store, async (alasio) => {
      await alasio.runPromise(handleCallbackQuery(buttonPress("cb-9", actionId, 9, 3)));
      assert.deepEqual(telegram.calls.answerCallbackQuery, [["cb-9", "This panel belongs to another service. Open it again."]]);
    }, { telegram, allowedUserIds: "9" });
  });
});

test("prompts sent before a service and folder are chosen only get the pickers and are not queued", async () => {
  await withStore(async (store, paths) => {
    const conversationId = await run(store.upsertConversation({ chatId: "12", user: { id: 12 } }));
    const telegram = recordingTelegram();
    const sent = telegram.calls.sendMessage;
    await withAlasio(store, async (alasio, { codex, claude }) => {
      const prompt = (messageId: number, text: string) => alasio.runPromise(processPrompt({ conversationId, chatId: "12", messageId, text, files: [] }));
      await prompt(1, "hello there");
      assert.equal(sent.length, 1);
      assert.match(sent[0]?.[1] ?? "", /Active: none/);
      assert.match(sent[0]?.[1] ?? "", /your message was not queued/);
      assert.deepEqual(
        inlineKeyboard(sent[0]?.[2]).flat().map((button) => button.text),
        ["Use Codex", "Use Claude", "Close"],
      );
      assert.equal(await run(store.hasOpenPromptJobs(conversationId)), false);
      assert.equal(await run(store.claimNextPromptJob(conversationId)), null);
      assert.equal(codex.calls.length, 0);
      assert.equal(claude.calls.length, 0);

      await prompt(2, "/sessions");
      assert.match(sent[1]?.[1] ?? "", /Active: none/);
      await prompt(3, "/stop");
      assert.equal(sent[2]?.[1], "No active query to stop.");

      await prompt(4, "/service claude");
      assert.match(sent[3]?.[1] ?? "", /Mounted Claude/);
      assert.equal((await run(store.getMount(conversationId))).harness, CLAUDE_HARNESS);
      // Service first, then folder: the folder picker follows immediately.
      assert.match(sent[4]?.[1] ?? "", /Workspace\n\nFolder: none/);
      assert.match(sent[4]?.[1] ?? "", /No folder is mounted/);
      assert.deepEqual(
        inlineKeyboard(sent[4]?.[2]).flat().map((button) => button.text),
        ["repo", "· plain", "New folder…", "Refresh", "Close"],
      );

      await prompt(5, "hello again");
      assert.match(sent[5]?.[1] ?? "", /Folder: none/);
      assert.equal(await run(store.hasOpenPromptJobs(conversationId)), false);
      await prompt(6, "/sessions new");
      assert.match(sent[6]?.[1] ?? "", /Folder: none/);
      assert.equal(claude.calls.length, 0);

      await prompt(7, "/workspace escape");
      assert.match(sent[7]?.[1] ?? "", /outside the workspace root/);
      assert.equal((await run(store.getMount(conversationId))).workingDirectory, null);
      await prompt(8, "/workspace repo");
      assert.match(sent[8]?.[1] ?? "", /Mounted repo \(/);
      assert.match(sent[8]?.[1] ?? "", new RegExp(`Folder: ${paths.repo}`));
      assert.equal(inlineKeyboard(sent[8]?.[2])[0]?.[0]?.text, "* repo");
      assert.equal((await run(store.getMount(conversationId))).workingDirectory, paths.repo);

      await prompt(9, "/sessions new");
      assert.match(sent[9]?.[1] ?? "", /New Claude session mounted: claude-f/);
      assert.equal(claude.calls.length, 1);
      assert.equal(codex.calls.length, 0);

      await prompt(10, "/workspace new made-here");
      assert.match(sent[10]?.[1] ?? "", /Created and mounted made-here/);
      assert.equal((await run(store.getMount(conversationId))).workingDirectory, join(paths.workspaceRoot, "made-here"));
      assert.equal((await run(store.getMount(conversationId))).sessionId, null);
      await prompt(11, "/workspace repo");
      assert.equal((await run(store.getMount(conversationId))).sessionId, "claude-fresh");
    }, { telegram, workspaceRoot: paths.workspaceRoot });
  });
});

test("/start offers the pickers until a service and folder are mounted", async () => {
  await withStore(async (store, paths) => {
    const telegram = recordingTelegram();
    const sent = telegram.calls.sendMessage;
    await withAlasio(store, async (alasio) => {
      const message: Message = {
        message_id: 1,
        date: 0,
        chat: { id: 13, type: "private", first_name: "Operator" },
        from: { id: 13, is_bot: false, first_name: "Operator" },
        text: "/start",
      };
      const handle = (messageId: number) => alasio.runPromise(handleMessage({ ...message, message_id: messageId }, messageId));
      assert.equal(await handle(1), null, "/start is no prompt");
      assert.match(sent[0]?.[1] ?? "", /Active: none/);

      await run(store.setActiveHarness("telegram:13", CODEX_HARNESS));
      await handle(2);
      assert.match(sent[1]?.[1] ?? "", /Folder: none/);

      await run(store.setWorkingDirectory("telegram:13", paths.repo));
      await handle(3);
      assert.equal(sent[2]?.[1], "Alasio is ready.");
    }, { telegram, workspaceRoot: paths.workspaceRoot, allowedUserIds: "13" });
  });
});

test("callbacks other than service and workspace controls are refused while nothing is mounted", async () => {
  await withStore(async (store, paths) => {
    const conversationId = await run(store.upsertConversation({ chatId: "14", user: { id: 14 } }));
    const [actionId = ""] = await run(store.createCallbackActions(conversationId, [{ kind: "queue", payload: { prompt: "later" } }]));
    const telegram = recordingTelegram();
    const { calls } = telegram;
    await withAlasio(store, async (alasio) => {
      const press = (id: string, data: string, messageId: number) => alasio.runPromise(handleCallbackQuery(buttonPress(id, data, 14, messageId)));
      await press("cb-14", actionId, 3);
      assert.deepEqual(calls.answerCallbackQuery, [["cb-14", NO_SERVICE_MOUNTED]]);
      assert.equal(calls.editMessageText.length, 0);

      const [useId = ""] = await run(store.createCallbackActions(conversationId, [{ kind: "service:use", payload: { harness: CODEX_HARNESS } }]));
      await press("cb-15", useId, 4);
      assert.equal((await run(store.getMount(conversationId))).harness, CODEX_HARNESS);
      assert.match(calls.answerCallbackQuery[1]?.[1] ?? "", /Mounted Codex/);
      assert.match(calls.editMessageText[0]?.[2] ?? "", /Active: Codex/);
      // Mounting a service chains into the folder picker.
      assert.match(calls.sendMessage[0]?.[1] ?? "", /Folder: none/);

      const [queueAgain = ""] = await run(store.createCallbackActions(conversationId, [{ kind: "queue", payload: { prompt: "later" } }]));
      await press("cb-16", queueAgain, 5);
      assert.deepEqual(calls.answerCallbackQuery[2], ["cb-16", NO_WORKSPACE_MOUNTED]);

      const pick = inlineKeyboard(calls.sendMessage[0]?.[2])[0]?.[0];
      assert.ok(pick && "callback_data" in pick);
      await press("cb-17", pick.callback_data, 6);
      assert.equal((await run(store.getMount(conversationId))).workingDirectory, paths.repo);
      assert.match(calls.answerCallbackQuery[3]?.[1] ?? "", /Mounted repo/);
      assert.match(calls.editMessageText[1]?.[2] ?? "", new RegExp(`Folder: ${paths.repo}`));
    }, { telegram, workspaceRoot: paths.workspaceRoot, allowedUserIds: "14" });
  });
});

test("Steer says whether the running turn took the message, and queues it when it did not", async () => {
  await withStore(async (store, paths) => {
    const conversationId = await run(store.upsertConversation({ chatId: "15", user: { id: 15 } }));
    await run(store.setActiveHarness(conversationId, CODEX_HARNESS));
    await run(store.setWorkingDirectory(conversationId, paths.repo));
    const telegram = recordingTelegram();
    const { calls } = telegram;
    await withAlasio(store, async (alasio) => {
      const steered: string[] = [];
      /** A press of Steer on a concurrent prompt held as a prompt job. */
      const pressSteer = async (id: string) => {
        const job = await run(store.enqueuePromptJob({ conversationId, chatId: "15", messageId: id, prompt: `also ${id}`, state: "awaiting_choice" }));
        const [steer = ""] = await run(store.createCallbackActions(conversationId, [{ kind: "steer", payload: { jobId: job.id, prompt: job.prompt } }]));
        await alasio.runPromise(handleCallbackQuery(buttonPress(id, steer, 15, 7)));
        return job.id;
      };

      // A Codex turn not yet started upstream takes no guidance: the message waits for the turn after it.
      let release = await occupy(alasio, conversationId, { steer: () => Effect.succeed(false) });
      const notTaken = await pressSteer("1");
      assert.deepEqual(calls.answerCallbackQuery.at(-1), ["1", "Queued."]);
      assert.equal(calls.editMessageText.at(-1)?.[2], "Codex is not ready to steer yet. Queued instead.");
      assert.equal((await run(store.getPromptJob(notTaken)))?.state, "pending");
      await release();
      await run(store.setPromptJobDisposition(notTaken, "cancelled"));

      release = await occupy(alasio, conversationId, { steer: (prompt) => Effect.sync(() => steered.push(prompt)).pipe(Effect.as(true)) });
      const taken = await pressSteer("2");
      assert.deepEqual(steered, ["also 2"]);
      assert.deepEqual(calls.answerCallbackQuery.at(-1), ["2", "Steered."]);
      assert.equal(calls.editMessageText.at(-1)?.[2], "Sent as guidance to the active Codex turn.");
      assert.equal((await run(store.getPromptJob(taken)))?.state, "completed");
      await release();
    }, { telegram, workspaceRoot: paths.workspaceRoot, allowedUserIds: "15" });
  });
});
