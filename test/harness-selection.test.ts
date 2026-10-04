import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, realpathSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import type { CallbackQuery, InlineKeyboardButton, Message } from "@grammyjs/types";
import Database from "better-sqlite3";
import { Effect, Exit, Scope } from "effect";

import { ActiveTurns, type RunningTurn } from "../src/harness/active-turns.ts";
import {
  type FreshSessionParams,
  type Harness,
  NO_SERVICE_MOUNTED,
  NO_WORKSPACE_MOUNTED,
  resolveHarnessName,
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
import { migrateSqliteSchema } from "../src/persistence/schema.ts";
import { SqliteStore } from "../src/persistence/store.ts";
import type { SessionSandboxes } from "../src/sandbox/index.ts";
import { handleCallbackQuery } from "../src/telegram/callback-handler.ts";
import type { TextMessageOptions } from "../src/telegram/client.ts";
import { handleMessage } from "../src/telegram/message-handler.ts";
import { listWorkspaceCandidates } from "../src/workspace/policy.ts";
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
 * Each test gets a SQLite store plus a workspace root holding `repo` (a git
 * repository), `plain` (a bare folder), a hidden folder, a file and a symlink
 * that escapes the root.
 */
async function withStore<T>(run: (store: SqliteStore, paths: WorkspacePaths) => T | Promise<T>): Promise<T> {
  const root = realpathSync(mkdtempSync(join(tmpdir(), "alasio-harness-")));
  const workspaceRoot = join(root, "workspaces");
  mkdirSync(join(workspaceRoot, "repo"), { recursive: true });
  execFileSync("git", ["init", "-q", "-b", "main"], { cwd: join(workspaceRoot, "repo") });
  mkdirSync(join(workspaceRoot, "plain"));
  mkdirSync(join(workspaceRoot, ".hidden"));
  writeFileSync(join(workspaceRoot, "notes.txt"), "not a folder");
  symlinkSync(root, join(workspaceRoot, "escape"));
  try {
    const store = new SqliteStore(root);
    try {
      return await run(store, { root, workspaceRoot, repo: join(workspaceRoot, "repo"), plain: join(workspaceRoot, "plain") });
    } finally {
      store.close();
    }
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
  store: SqliteStore,
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
  volumes: { create: () => Effect.die(new Error("no volume is made")), destroy: () => Effect.die(new Error("no volume is destroyed")) },
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
  assert.equal(resolveHarnessName({}, "telegram:1"), null);
  assert.equal(resolveHarnessName({ getActiveHarness: () => CLAUDE_HARNESS }, "telegram:1"), CLAUDE_HARNESS);
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
  await withStore((store) => {
    const conversationId = store.upsertConversation({ chatId: "1", user: { id: 1 } });
    assert.equal(store.getActiveHarness(conversationId), null);
    assert.equal(store.getSessionId(conversationId), undefined);
    assert.throws(() => store.setSessionId(conversationId, "orphan"), /No service is mounted/);
    assert.throws(() => store.enqueuePromptJob({ conversationId, chatId: "1", messageId: "1", prompt: "hi" }), /no service is mounted/);
    assert.throws(() => store.upsertActiveTurn({ conversationId, chatId: "1", messageId: "1", prompt: "hi" }), /no service is mounted/);

    store.setActiveHarness(conversationId, CODEX_HARNESS);
    assert.equal(store.getActiveHarness(conversationId), CODEX_HARNESS);
    assert.deepEqual(store.listConversationsWithSessions(CODEX_HARNESS), []);
    store.setWorkingDirectory(conversationId, "/work/a");
    store.setSessionId(conversationId, "codex-session");
    assert.equal(store.getSessionId(conversationId), "codex-session");

    store.setActiveHarness(conversationId, CLAUDE_HARNESS);
    assert.equal(store.getActiveHarness(conversationId), CLAUDE_HARNESS);
    assert.equal(store.getSessionId(conversationId), undefined);
    store.setSessionId(conversationId, "claude-session");
    assert.equal(store.getSessionId(conversationId), "claude-session");
    assert.equal(store.getHarnessSessionId(conversationId, CODEX_HARNESS), "codex-session");

    store.setActiveHarness(conversationId, CODEX_HARNESS);
    assert.equal(store.getSessionId(conversationId), "codex-session");
    assert.equal(store.getHarnessSessionId(conversationId, CLAUDE_HARNESS), "claude-session");

    assert.deepEqual(
      store.listConversationsWithSessions(CODEX_HARNESS).map((row) => [row.session_id, row.working_directory]),
      [["codex-session", "/work/a"]],
    );
    assert.deepEqual(store.listConversationsWithSessions(CLAUDE_HARNESS), []);
    // The store checks what it is given at run time too.
    assert.throws(() => store.setActiveHarness(conversationId, UNKNOWN_HARNESS as HarnessName), /Unknown harness/);
  });
});

test("switching folders parks both harness session pointers per folder", async () => {
  await withStore((store) => {
    const conversationId = store.upsertConversation({ chatId: "15", user: { id: 15 } });
    assert.equal(store.getWorkingDirectory(conversationId), null);
    store.setActiveHarness(conversationId, CODEX_HARNESS);
    store.setWorkingDirectory(conversationId, "/work/a");
    store.setSessionId(conversationId, "codex-a");
    store.setHarnessSessionId(conversationId, CLAUDE_HARNESS, "claude-a");

    store.setWorkingDirectory(conversationId, "/work/b");
    assert.equal(store.getWorkingDirectory(conversationId), "/work/b");
    assert.equal(store.getSessionId(conversationId), undefined);
    assert.equal(store.getHarnessSessionId(conversationId, CLAUDE_HARNESS), undefined);
    store.setSessionId(conversationId, "codex-b");

    store.setWorkingDirectory(conversationId, "/work/a");
    assert.equal(store.getSessionId(conversationId), "codex-a");
    assert.equal(store.getHarnessSessionId(conversationId, CLAUDE_HARNESS), "claude-a");
    store.setWorkingDirectory(conversationId, "/work/b");
    assert.equal(store.getSessionId(conversationId), "codex-b");
    assert.equal(store.getHarnessSessionId(conversationId, CLAUDE_HARNESS), undefined);
    assert.throws(() => store.setWorkingDirectory(conversationId, ""), /non-empty path/);
    assert.throws(() => store.setWorkingDirectory("telegram:nope", "/work/a"), /Unknown conversation/);
  });
});

test("every session a harness is pointed at is listed with its folder, once", async () => {
  await withStore((store) => {
    const parked = store.upsertConversation({ chatId: "16", user: { id: 16 } });
    store.setActiveHarness(parked, CLAUDE_HARNESS);
    store.setWorkingDirectory(parked, "/work/a");
    store.setSessionId(parked, "claude-a");
    store.setWorkingDirectory(parked, "/work/b");
    store.setSessionId(parked, "claude-b");

    const live = store.upsertConversation({ chatId: "17", user: { id: 17 } });
    store.setActiveHarness(live, CLAUDE_HARNESS);
    store.setWorkingDirectory(live, "/work/c");
    store.upsertActiveTurn({ conversationId: live, chatId: "17", messageId: "1", sessionId: null, prompt: "hi" });
    store.updateActiveTurnSessionId(live, "claude-live");

    const codex = store.upsertConversation({ chatId: "18", user: { id: 18 } });
    store.setActiveHarness(codex, CODEX_HARNESS);
    store.setWorkingDirectory(codex, "/work/d");
    store.setSessionId(codex, "codex-d");

    assert.deepEqual(
      store.listHarnessSessionReferences(CLAUDE_HARNESS).sort((x, y) => x.sessionId.localeCompare(y.sessionId)),
      [
        { sessionId: "claude-a", workingDirectory: "/work/a" },
        { sessionId: "claude-b", workingDirectory: "/work/b" },
        { sessionId: "claude-live", workingDirectory: "/work/c" },
      ],
    );
    assert.deepEqual(store.listHarnessSessionReferences(CODEX_HARNESS), [{ sessionId: "codex-d", workingDirectory: "/work/d" }]);
  });
});

test("callback actions capture the active harness generation", async () => {
  await withStore((store) => {
    const conversationId = store.upsertConversation({ chatId: "2", user: { id: 2 } });
    const neutralId = store.createCallbackAction({ conversationId, kind: "service:use", payload: {} });
    assert.equal(store.consumeCallbackAction(neutralId)?.payload["expectedHarness"], null);
    store.setActiveHarness(conversationId, CODEX_HARNESS);
    const actionId = store.createCallbackAction({ conversationId, kind: "control:current", payload: {} });
    const action = store.consumeCallbackAction(actionId);
    assert.equal(action?.payload["expectedHarness"], CODEX_HARNESS);
  });
});

test("schema migration adds harness columns to an existing v4 database", () => {
  const root = mkdtempSync(join(tmpdir(), "alasio-schema-v4-"));
  try {
    const dbPath = join(root, "alasio.sqlite");
    const legacy = new Database(dbPath);
    legacy.exec(`
      create table conversations (
        id text primary key,
        transport text not null,
        chat_id text not null,
        user_id text,
        username text,
        first_name text,
        last_name text,
        codex_session_id text,
        created_at text not null default (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
        updated_at text not null default (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
        unique (transport, chat_id)
      );
      create table turns (
        id text primary key,
        conversation_id text not null,
        thread_key text not null,
        channel text not null,
        thread_ts text not null,
        session_id text,
        pending_response_id text,
        prompt text,
        state text not null,
        started_at real not null,
        completed_at real
      );
      create table prompt_jobs (
        id text primary key,
        conversation_id text not null,
        chat_id text not null,
        message_id text not null,
        prompt text not null,
        file_paths_json text not null default '[]',
        state text not null,
        priority integer not null default 0,
        attempts integer not null default 0,
        upstream_session_id text,
        upstream_turn_id text,
        upstream_started_at real,
        last_error text,
        created_at real not null,
        started_at real,
        completed_at real,
        unique (conversation_id, message_id)
      );
      insert into conversations (id, transport, chat_id, codex_session_id) values ('telegram:9', 'telegram', '9', 'legacy-session');
    `);
    migrateSqliteSchema(legacy);
    const columns = (table: string) => new Set(legacy.prepare<[], { name: string }>(`pragma table_info(${table})`).all().map((column) => column.name));
    assert.ok(columns("conversations").has("claude_session_id"));
    assert.ok(columns("conversations").has("active_harness"));
    assert.ok(columns("turns").has("harness"));
    assert.ok(columns("prompt_jobs").has("harness"));
    assert.ok(columns("prompt_jobs").has("traceparent"));
    assert.ok(columns("telegram_outbox").has("traceparent"));
    assert.equal(legacy.prepare<[], { value: string }>("select value from bot_state where key = 'schema_version'").get()?.value, "8");
    legacy.close();

    const store = new SqliteStore(root, dbPath, { defaultWorkingDirectory: "/legacy/workspace" });
    assert.equal(store.getActiveHarness("telegram:9"), CODEX_HARNESS);
    assert.equal(store.getSessionId("telegram:9"), "legacy-session");
    assert.equal(store.getWorkingDirectory("telegram:9"), "/legacy/workspace");
    const fresh = store.upsertConversation({ chatId: "10", user: { id: 10 } });
    assert.equal(store.getActiveHarness(fresh), null);
    assert.equal(store.getWorkingDirectory(fresh), "/legacy/workspace");
    store.close();

    const neutral = new SqliteStore(root, dbPath);
    assert.equal(neutral.getWorkingDirectory(neutral.upsertConversation({ chatId: "11", user: { id: 11 } })), null);
    neutral.close();
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("schema migration relaxes the v5 active_harness constraint without losing mounts", () => {
  const root = mkdtempSync(join(tmpdir(), "alasio-schema-v5-"));
  try {
    const dbPath = join(root, "alasio.sqlite");
    const v5 = new Database(dbPath);
    v5.pragma("foreign_keys = ON");
    v5.exec(`
      create table conversations (
        id text primary key,
        transport text not null,
        chat_id text not null,
        user_id text,
        username text,
        first_name text,
        last_name text,
        codex_session_id text,
        claude_session_id text,
        active_harness text not null default 'codex',
        created_at text not null default (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
        updated_at text not null default (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
        unique (transport, chat_id)
      );
      create table messages (
        id text primary key,
        conversation_id text not null references conversations(id) on delete cascade,
        direction text not null,
        kind text not null,
        transport_message_id text,
        text text,
        media_group_id text,
        raw_json text,
        codex_session_id text,
        turn_id text,
        created_at text not null default (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
      );
      insert into conversations (id, transport, chat_id, codex_session_id, claude_session_id, active_harness)
        values ('telegram:11', 'telegram', '11', 'codex-old', 'claude-old', 'claude');
      insert into messages (id, conversation_id, direction, kind, text) values ('m1', 'telegram:11', 'in', 'text', 'kept');
    `);
    migrateSqliteSchema(v5);
    const column = v5.prepare<[], { name: string; notnull: number; dflt_value: string | null }>("pragma table_info(conversations)")
      .all()
      .find((info) => info.name === "active_harness");
    assert.ok(column);
    assert.equal(column.notnull, 0);
    assert.equal(column.dflt_value, null);
    assert.equal(v5.prepare<[], { n: number }>("select count(*) as n from messages").get()?.n, 1);
    v5.close();

    const store = new SqliteStore(root, dbPath);
    assert.equal(store.getActiveHarness("telegram:11"), CLAUDE_HARNESS);
    assert.equal(store.getSessionId("telegram:11"), "claude-old");
    assert.equal(store.getHarnessSessionId("telegram:11", CODEX_HARNESS), "codex-old");
    // No WORKING_DIRECTORY configured: previously mounted rows must choose a folder.
    assert.equal(store.getWorkingDirectory("telegram:11"), null);
    store.close();
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("switching services is refused while a turn is active or prompts are queued", async () => {
  await withStore(async (store, paths) => {
    const conversationId = store.upsertConversation({ chatId: "3", user: { id: 3 } });
    store.setActiveHarness(conversationId, CODEX_HARNESS);
    store.setWorkingDirectory(conversationId, paths.repo);
    await withAlasio(store, async (alasio) => {
      let release = await occupy(alasio, conversationId);
      await assert.rejects(mounts(alasio, (m) => m.switchHarness(conversationId, CLAUDE_HARNESS)), /Codex is currently working/);
      await release();

      const job = store.enqueuePromptJob({ conversationId, chatId: "3", messageId: "10", prompt: "later" });
      assert.equal(job.harness, CODEX_HARNESS);
      await assert.rejects(mounts(alasio, (m) => m.switchHarness(conversationId, CLAUDE_HARNESS)), /Queued prompts are still waiting/);
      store.setPromptJobDisposition(job.id, "cancelled");

      const result = await mounts(alasio, (m) => m.switchHarness(conversationId, CLAUDE_HARNESS));
      assert.deepEqual(result, { switched: true, previous: CODEX_HARNESS, next: CLAUDE_HARNESS, sessionId: null, workingDirectory: paths.repo });
      assert.equal(store.getActiveHarness(conversationId), CLAUDE_HARNESS);
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
    const conversationId = store.upsertConversation({ chatId: "16", user: { id: 16 } });
    store.setActiveHarness(conversationId, CODEX_HARNESS);
    await withAlasio(store, async (alasio) => {
      const switchTo = (target: string) => mounts(alasio, (m) => m.switchWorkspace(conversationId, target));
      const create = (name: string) => mounts(alasio, (m) => m.createWorkspace(conversationId, name));
      const byName = await switchTo("repo");
      assert.deepEqual(byName, { switched: true, previous: null, workingDirectory: paths.repo });
      const byAbsolute = await switchTo(paths.plain);
      assert.equal(byAbsolute.workingDirectory, paths.plain);
      assert.equal(store.getWorkingDirectory(conversationId), paths.plain);

      await assert.rejects(switchTo("missing"), /does not exist under/);
      await assert.rejects(switchTo("../"), /outside the workspace root/);
      await assert.rejects(switchTo("escape"), /outside the workspace root/);
      await assert.rejects(switchTo("/etc"), /outside the workspace root/);
      await assert.rejects(switchTo("notes.txt"), /not a directory/);
      await assert.rejects(switchTo(""), /Folder path is empty/);
      assert.equal(store.getWorkingDirectory(conversationId), paths.plain);

      await assert.rejects(create("../oops"), /Folder names may only use/);
      await assert.rejects(create(".hidden"), /Folder names may only use/);
      await assert.rejects(create("repo"), /already exists/);
      const created = await create("fresh-1");
      assert.deepEqual(created, { switched: true, created: true, previous: paths.plain, workingDirectory: join(paths.workspaceRoot, "fresh-1") });
      assert.equal(store.getWorkingDirectory(conversationId), created.workingDirectory);
      assert.equal(execFileSync("git", ["symbolic-ref", "--short", "HEAD"], { cwd: created.workingDirectory }).toString().trim(), "main");

      const candidates = (await listWorkspaceCandidates(paths.workspaceRoot)).map((candidate) => [candidate.name, candidate.git]);
      assert.deepEqual(candidates, [["fresh-1", true], ["repo", true], ["plain", false]]);
    }, { workspaceRoot: paths.workspaceRoot });
  });
});

test("new sessions and active turns are recorded under the active harness", async () => {
  await withStore(async (store, paths) => {
    const conversationId = store.upsertConversation({ chatId: "4", user: { id: 4 } });
    await withAlasio(store, async (alasio, { claude, codex }) => {
      const startNewSession = () => alasio.runPromise(Effect.flatMap(Turns, (turns) => turns.startNewSession(conversationId)));
      store.setActiveHarness(conversationId, CLAUDE_HARNESS);
      await assert.rejects(startNewSession(), /No folder is mounted/);
      store.setWorkingDirectory(conversationId, paths.repo);
      const sessionId = await startNewSession();
      assert.equal(sessionId, "claude-fresh");
      assert.equal(claude.calls.length, 1);
      assert.equal(claude.calls[0]?.[1].workingDirectory, paths.repo);
      assert.equal(codex.calls.length, 0);
      assert.equal(store.getHarnessSessionId(conversationId, CLAUDE_HARNESS), "claude-fresh");
      assert.equal(store.getHarnessSessionId(conversationId, CODEX_HARNESS), undefined);

      store.upsertActiveTurn({ conversationId, chatId: "4", messageId: "1", sessionId: null, prompt: "hi" });
      const [turn] = store.getActiveTurns();
      assert.equal(turn?.harness, CLAUDE_HARNESS);
      store.updateActiveTurnSessionId(conversationId, "claude-live");
      assert.equal(store.getHarnessSessionId(conversationId, CLAUDE_HARNESS), "claude-live");
      assert.equal(store.getHarnessSessionId(conversationId, CODEX_HARNESS), undefined);
    });
  });
});

test("restart recovery restores the interrupted turn's own harness session", async () => {
  await withStore((store) => {
    const conversationId = store.upsertConversation({ chatId: "5", user: { id: 5 } });
    store.setActiveHarness(conversationId, CLAUDE_HARNESS);
    store.upsertActiveTurn({ conversationId, chatId: "5", messageId: "8", sessionId: "claude-1", prompt: "restart please" });
    store.recordRestartEvent({
      cause: "self_induced",
      thread_key: conversationId,
      channel: "5",
      thread_ts: "8",
      session_id: "claude-1",
      timestamp: 99,
    });
    const [turn] = store.getActiveTurns();
    assert.ok(turn);
    // An empty environment gives the defaults even when the tests themselves run in a
    // release whose names differ.
    const job = store.stageRestartRecovery({ turn, prompt: buildRestartSyntheticText("self_induced", turn.harness, {}) });
    assert.ok(job);
    assert.equal(job.harness, CLAUDE_HARNESS);
    assert.match(job.prompt, /You are Claude, connected through the Kubernetes Deployment alasio in namespace alasio/);
    assert.match(job.prompt, /restart is `kubectl -n alasio rollout restart deployment\/alasio`/);
    assert.match(job.prompt, /not deleting alasio's pod/);
    assert.equal(store.getHarnessSessionId(conversationId, CLAUDE_HARNESS), "claude-1");
    assert.equal(store.getHarnessSessionId(conversationId, CODEX_HARNESS), undefined);
    const operator = buildRestartSyntheticText("operator_induced", undefined, {});
    assert.match(operator, /You are Codex/);
    const named = buildRestartSyntheticText("self_induced", CLAUDE_HARNESS, { ALASIO_DEPLOYMENT: "bot-alasio", ALASIO_NAMESPACE: "bots" });
    assert.match(named, /connected through the Kubernetes Deployment bot-alasio in namespace bots/);
    assert.match(named, /`kubectl -n bots rollout restart deployment\/bot-alasio`/);
  });
});

test("service panel lists both harness mounts and offers the inactive switch", async () => {
  await withStore((store) => {
    const conversationId = store.upsertConversation({ chatId: "6", user: { id: 6 } });
    const neutral = buildServicePanel({ store, conversationId, working: false, notice: CHOOSE_SERVICE_NOTICE });
    assert.match(neutral.text, /Active: none/);
    assert.match(neutral.text, /Nothing runs until a service is chosen/);
    assert.match(neutral.text, /No service is mounted/);
    assert.deepEqual(
      neutral.options.reply_markup.inline_keyboard.flat().map((button) => button.text),
      ["Use Codex", "Use Claude", "Close"],
    );

    store.setActiveHarness(conversationId, CODEX_HARNESS);
    store.setSessionId(conversationId, "codex-abcdef12");
    const panel = buildServicePanel({ store, conversationId, working: true });
    assert.match(panel.text, /Active: Codex/);
    assert.match(panel.text, /Status: working/);
    assert.match(panel.text, /\* Codex: session codex-ab/);
    assert.match(panel.text, /Claude: no mounted session/);
    const buttons = panel.options.reply_markup.inline_keyboard.flat().map((button) => button.text);
    assert.deepEqual(buttons, ["Use Claude", "Close"]);
  });
});

test("/service text command switches and reports the outcome", async () => {
  await withStore(async (store, paths) => {
    const conversationId = store.upsertConversation({ chatId: "7", user: { id: 7 } });
    // With a folder mounted, a newly mounted service needs no folder picker after it.
    store.setWorkingDirectory(conversationId, paths.repo);
    const telegram = recordingTelegram();
    await withAlasio(store, async (alasio) => {
      const command = (target: string) => alasio.runPromise(handleServiceTextCommand({ conversationId, chatId: "7", target }));
      await command("claude");
      assert.equal(store.getActiveHarness(conversationId), CLAUDE_HARNESS);
      assert.match(telegram.calls.sendMessage[0]?.[1] ?? "", /Mounted Claude\. Send a message to start\./);
      assert.match(telegram.calls.sendMessage[0]?.[1] ?? "", /Active: Claude/);

      await command("codex");
      assert.match(telegram.calls.sendMessage[1]?.[1] ?? "", /Switched to Codex\./);

      await command("gemini");
      assert.match(telegram.calls.sendMessage[2]?.[1] ?? "", /Unknown service "gemini"/);
      assert.equal(store.getActiveHarness(conversationId), CODEX_HARNESS);
      assert.equal(telegram.calls.sendMessage.length, 3);
    }, { telegram, workspaceRoot: paths.workspaceRoot });
  });
});

test("service callbacks switch harness and surface refusals in the panel", async () => {
  await withStore(async (store) => {
    const conversationId = store.upsertConversation({ chatId: "8", user: { id: 8 } });
    store.setActiveHarness(conversationId, CODEX_HARNESS);
    const telegram = recordingTelegram();
    await withAlasio(store, async (alasio) => {
      await occupy(alasio, conversationId);
      const actionId = store.createCallbackAction({ conversationId, kind: "service:use", payload: { harness: CLAUDE_HARNESS } });
      const action = store.consumeCallbackAction(actionId);
      assert.ok(action);
      await alasio.runPromise(handleServiceControlCallback({ action, callbackQueryId: "cb-1", chatId: "8", messageId: 5 }));
      assert.match(telegram.calls.answerCallbackQuery[0]?.[1] ?? "", /Codex is currently working/);
      assert.match(telegram.calls.editMessageText[0]?.[2] ?? "", /Active: Codex/);
      assert.match(telegram.calls.editMessageText[0]?.[2] ?? "", /Status: working/);
      assert.match(telegram.calls.editMessageText[0]?.[2] ?? "", /Codex is currently working/);
      assert.equal(store.getActiveHarness(conversationId), CODEX_HARNESS);
    }, { telegram });
  });
});

test("choosing a service from its panel chains into a folder picker that offers a new empty workspace", async () => {
  await withStore(async (store, paths) => {
    const conversationId = store.upsertConversation({ chatId: "10", user: { id: 10 } });
    const actionId = store.createCallbackAction({ conversationId, kind: "service:use", payload: { harness: CLAUDE_HARNESS } });
    const telegram = recordingTelegram();
    await withAlasio(store, async (alasio) => {
      await alasio.runPromise(handleCallbackQuery(buttonPress("cb-10", actionId, 10, 4)));
      assert.equal(store.getActiveHarness(conversationId), CLAUDE_HARNESS);
      const picker = telegram.calls.sendMessage.at(-1);
      assert.match(picker?.[1] ?? "", /Folder: none/);
      assert.ok(inlineKeyboard(picker?.[2]).flat().some((button) => button.text === "New empty workspace…"));
    }, { telegram, workspaceRoot: paths.workspaceRoot, allowedUserIds: "10", sandbox: unusedSandbox });
  });
});

test("callback handler rejects panels created under another harness", async () => {
  await withStore(async (store, paths) => {
    const conversationId = store.upsertConversation({ chatId: "9", user: { id: 9 } });
    store.setActiveHarness(conversationId, CODEX_HARNESS);
    const actionId = store.createCallbackAction({ conversationId, kind: "control:current", payload: {} });
    store.setActiveHarness(conversationId, CLAUDE_HARNESS);
    store.setWorkingDirectory(conversationId, paths.repo);
    const telegram = recordingTelegram();
    await withAlasio(store, async (alasio) => {
      await alasio.runPromise(handleCallbackQuery(buttonPress("cb-9", actionId, 9, 3)));
      assert.deepEqual(telegram.calls.answerCallbackQuery, [["cb-9", "This panel belongs to another service. Open it again."]]);
    }, { telegram, allowedUserIds: "9" });
  });
});

test("prompts sent before a service and folder are chosen only get the pickers and are not queued", async () => {
  await withStore(async (store, paths) => {
    const conversationId = store.upsertConversation({ chatId: "12", user: { id: 12 } });
    const telegram = recordingTelegram();
    const sent = telegram.calls.sendMessage;
    await withAlasio(store, async (alasio, { codex, claude }) => {
      const prompt = (messageId: number, text: string) => alasio.runPromise(processPrompt({ conversationId, chatId: "12", messageId, text, filePaths: [] }));
      await prompt(1, "hello there");
      assert.equal(sent.length, 1);
      assert.match(sent[0]?.[1] ?? "", /Active: none/);
      assert.match(sent[0]?.[1] ?? "", /your message was not queued/);
      assert.deepEqual(
        inlineKeyboard(sent[0]?.[2]).flat().map((button) => button.text),
        ["Use Codex", "Use Claude", "Close"],
      );
      assert.equal(store.hasOpenPromptJobs(conversationId), false);
      assert.equal(store.claimNextPromptJob(conversationId) ?? null, null);
      assert.equal(codex.calls.length, 0);
      assert.equal(claude.calls.length, 0);

      await prompt(2, "/sessions");
      assert.match(sent[1]?.[1] ?? "", /Active: none/);
      await prompt(3, "/stop");
      assert.equal(sent[2]?.[1], "No active query to stop.");

      await prompt(4, "/service claude");
      assert.match(sent[3]?.[1] ?? "", /Mounted Claude/);
      assert.equal(store.getActiveHarness(conversationId), CLAUDE_HARNESS);
      // Service first, then folder: the folder picker follows immediately.
      assert.match(sent[4]?.[1] ?? "", /Workspace\n\nFolder: none/);
      assert.match(sent[4]?.[1] ?? "", /No folder is mounted/);
      assert.deepEqual(
        inlineKeyboard(sent[4]?.[2]).flat().map((button) => button.text),
        ["repo", "· plain", "New folder…", "Refresh", "Close"],
      );

      await prompt(5, "hello again");
      assert.match(sent[5]?.[1] ?? "", /Folder: none/);
      assert.equal(store.hasOpenPromptJobs(conversationId), false);
      await prompt(6, "/sessions new");
      assert.match(sent[6]?.[1] ?? "", /Folder: none/);
      assert.equal(claude.calls.length, 0);

      await prompt(7, "/workspace escape");
      assert.match(sent[7]?.[1] ?? "", /outside the workspace root/);
      assert.equal(store.getWorkingDirectory(conversationId), null);
      await prompt(8, "/workspace repo");
      assert.match(sent[8]?.[1] ?? "", /Mounted repo \(/);
      assert.match(sent[8]?.[1] ?? "", new RegExp(`Folder: ${paths.repo}`));
      assert.equal(inlineKeyboard(sent[8]?.[2])[0]?.[0]?.text, "* repo");
      assert.equal(store.getWorkingDirectory(conversationId), paths.repo);

      await prompt(9, "/sessions new");
      assert.match(sent[9]?.[1] ?? "", /New Claude session mounted: claude-f/);
      assert.equal(claude.calls.length, 1);
      assert.equal(codex.calls.length, 0);

      await prompt(10, "/workspace new made-here");
      assert.match(sent[10]?.[1] ?? "", /Created and mounted made-here/);
      assert.equal(store.getWorkingDirectory(conversationId), join(paths.workspaceRoot, "made-here"));
      assert.equal(store.getSessionId(conversationId), undefined);
      await prompt(11, "/workspace repo");
      assert.equal(store.getSessionId(conversationId), "claude-fresh");
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

      store.setActiveHarness("telegram:13", CODEX_HARNESS);
      await handle(2);
      assert.match(sent[1]?.[1] ?? "", /Folder: none/);

      store.setWorkingDirectory("telegram:13", paths.repo);
      await handle(3);
      assert.equal(sent[2]?.[1], "Alasio is ready.");
    }, { telegram, workspaceRoot: paths.workspaceRoot, allowedUserIds: "13" });
  });
});

test("callbacks other than service and workspace controls are refused while nothing is mounted", async () => {
  await withStore(async (store, paths) => {
    const conversationId = store.upsertConversation({ chatId: "14", user: { id: 14 } });
    const actionId = store.createCallbackAction({ conversationId, kind: "queue", payload: { prompt: "later" } });
    const telegram = recordingTelegram();
    const { calls } = telegram;
    await withAlasio(store, async (alasio) => {
      const press = (id: string, data: string, messageId: number) => alasio.runPromise(handleCallbackQuery(buttonPress(id, data, 14, messageId)));
      await press("cb-14", actionId, 3);
      assert.deepEqual(calls.answerCallbackQuery, [["cb-14", NO_SERVICE_MOUNTED]]);
      assert.equal(calls.editMessageText.length, 0);

      const useId = store.createCallbackAction({ conversationId, kind: "service:use", payload: { harness: CODEX_HARNESS } });
      await press("cb-15", useId, 4);
      assert.equal(store.getActiveHarness(conversationId), CODEX_HARNESS);
      assert.match(calls.answerCallbackQuery[1]?.[1] ?? "", /Mounted Codex/);
      assert.match(calls.editMessageText[0]?.[2] ?? "", /Active: Codex/);
      // Mounting a service chains into the folder picker.
      assert.match(calls.sendMessage[0]?.[1] ?? "", /Folder: none/);

      const queueAgain = store.createCallbackAction({ conversationId, kind: "queue", payload: { prompt: "later" } });
      await press("cb-16", queueAgain, 5);
      assert.deepEqual(calls.answerCallbackQuery[2], ["cb-16", NO_WORKSPACE_MOUNTED]);

      const pick = inlineKeyboard(calls.sendMessage[0]?.[2])[0]?.[0];
      assert.ok(pick && "callback_data" in pick);
      await press("cb-17", pick.callback_data, 6);
      assert.equal(store.getWorkingDirectory(conversationId), paths.repo);
      assert.match(calls.answerCallbackQuery[3]?.[1] ?? "", /Mounted repo/);
      assert.match(calls.editMessageText[1]?.[2] ?? "", new RegExp(`Folder: ${paths.repo}`));
    }, { telegram, workspaceRoot: paths.workspaceRoot, allowedUserIds: "14" });
  });
});

test("Steer says whether the running turn took the message, and queues it when it did not", async () => {
  await withStore(async (store, paths) => {
    const conversationId = store.upsertConversation({ chatId: "15", user: { id: 15 } });
    store.setActiveHarness(conversationId, CODEX_HARNESS);
    store.setWorkingDirectory(conversationId, paths.repo);
    const telegram = recordingTelegram();
    const { calls } = telegram;
    await withAlasio(store, async (alasio) => {
      const steered: string[] = [];
      /** A press of Steer on a concurrent prompt held as a prompt job. */
      const pressSteer = async (id: string) => {
        const job = store.enqueuePromptJob({ conversationId, chatId: "15", messageId: id, prompt: `also ${id}`, state: "awaiting_choice" });
        const steer = store.createCallbackAction({ conversationId, kind: "steer", payload: { jobId: job.id, prompt: job.prompt } });
        await alasio.runPromise(handleCallbackQuery(buttonPress(id, steer, 15, 7)));
        return job.id;
      };

      // A Codex turn not yet started upstream takes no guidance: the message waits for the turn after it.
      let release = await occupy(alasio, conversationId, { steer: () => Effect.succeed(false) });
      const notTaken = await pressSteer("1");
      assert.deepEqual(calls.answerCallbackQuery.at(-1), ["1", "Queued."]);
      assert.equal(calls.editMessageText.at(-1)?.[2], "Codex is not ready to steer yet. Queued instead.");
      assert.equal(store.getPromptJob(notTaken)?.state, "pending");
      await release();
      store.setPromptJobDisposition(notTaken, "cancelled");

      release = await occupy(alasio, conversationId, { steer: (prompt) => Effect.sync(() => steered.push(prompt)).pipe(Effect.as(true)) });
      const taken = await pressSteer("2");
      assert.deepEqual(steered, ["also 2"]);
      assert.deepEqual(calls.answerCallbackQuery.at(-1), ["2", "Steered."]);
      assert.equal(calls.editMessageText.at(-1)?.[2], "Sent as guidance to the active Codex turn.");
      assert.equal(store.getPromptJob(taken)?.state, "completed");
      await release();
    }, { telegram, workspaceRoot: paths.workspaceRoot, allowedUserIds: "15" });
  });
});
