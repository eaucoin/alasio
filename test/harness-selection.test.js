import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, realpathSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import Database from "better-sqlite3";

import { TurnController } from "../src/codex/turn-controller.js";
import { NO_SERVICE_MOUNTED, NO_WORKSPACE_MOUNTED, createHarnessRegistry, resolveHarnessName } from "../src/harness/index.js";
import { CLAUDE_HARNESS, CODEX_HARNESS, getDefaultHarness, harnessDisplayName, normalizeHarnessName } from "../src/harness/names.js";
import { parseCommand } from "../src/operator/command-parser.js";
import { buildRestartSyntheticText } from "../src/operator/restart-prompts.js";
import {
  CHOOSE_SERVICE_NOTICE,
  buildServicePanel,
  handleServiceControlCallback,
  handleServiceTextCommand,
} from "../src/operator/service-control.js";
import { migrateSqliteSchema } from "../src/persistence/schema.js";
import { SqliteStore } from "../src/persistence/store.js";
import { CallbackHandler } from "../src/telegram/callback-handler.js";
import { MessageHandler } from "../src/telegram/message-handler.js";
import { listWorkspaceCandidates } from "../src/workspace/policy.js";

/**
 * Each test gets a SQLite store plus a workspace root holding `repo` (a git
 * repository), `plain` (a bare folder), a hidden folder, a file and a symlink
 * that escapes the root.
 */
async function withStore(run) {
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

function createFakeHarness(name, { sessionId = `${name}-fresh` } = {}) {
  return {
    name,
    displayName: harnessDisplayName(name),
    supportsGoals: name === CODEX_HARNESS,
    supportsWarmup: false,
    supportsSteer: true,
    sessions: {},
    calls: [],
    async startFreshSession(args) {
      this.calls.push(["startFreshSession", args]);
      return sessionId;
    },
    async warmSession() {
      return false;
    },
    async executeTurn() {
      throw new Error("not exercised");
    },
    shutdown() {},
  };
}

function createRegistry(config = {}) {
  const codex = createFakeHarness(CODEX_HARNESS);
  const claude = createFakeHarness(CLAUDE_HARNESS);
  return {
    registry: createHarnessRegistry({ config, overrides: { [CODEX_HARNESS]: codex, [CLAUDE_HARNESS]: claude } }),
    codex,
    claude,
  };
}

function createTurnController(store, registry, client = createClient(), config = {}) {
  return new TurnController({
    config: { workspaceRoot: "/nonexistent-workspace-root", ...config },
    client,
    store,
    outbox: { enqueueText: () => undefined },
    activeQueries: new Map(),
    workflowWaits: new Map(),
    workflowWakeEvents: new Map(),
    isStopping: () => false,
    harnesses: registry,
  });
}

function createClient() {
  const calls = { sendMessage: [], editMessageText: [], answerCallbackQuery: [], deleteMessage: [] };
  return {
    calls,
    async sendMessage(...args) {
      calls.sendMessage.push(args);
      return [{ message_id: 77 }];
    },
    async editMessageText(...args) {
      calls.editMessageText.push(args);
    },
    async answerCallbackQuery(...args) {
      calls.answerCallbackQuery.push(args);
    },
    async deleteMessage(...args) {
      calls.deleteMessage.push(args);
    },
  };
}

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
    assert.throws(() => store.setActiveHarness(conversationId, "gemini"), /Unknown harness/);
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
    assert.equal(store.consumeCallbackAction(neutralId).payload.expectedHarness, null);
    store.setActiveHarness(conversationId, CODEX_HARNESS);
    const actionId = store.createCallbackAction({ conversationId, kind: "control:current", payload: {} });
    const action = store.consumeCallbackAction(actionId);
    assert.equal(action.payload.expectedHarness, CODEX_HARNESS);
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
    const columns = (table) => new Set(legacy.prepare(`pragma table_info(${table})`).all().map((column) => column.name));
    assert.ok(columns("conversations").has("claude_session_id"));
    assert.ok(columns("conversations").has("active_harness"));
    assert.ok(columns("turns").has("harness"));
    assert.ok(columns("prompt_jobs").has("harness"));
    assert.equal(legacy.prepare("select value from bot_state where key = 'schema_version'").get().value, "7");
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
    const column = v5.prepare("pragma table_info(conversations)").all().find((info) => info.name === "active_harness");
    assert.equal(column.notnull, 0);
    assert.equal(column.dflt_value, null);
    assert.equal(v5.prepare("select count(*) as n from messages").get().n, 1);
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
    const activeQueries = new Map();
    const { registry } = createRegistry();
    const turns = new TurnController({
      config: { workspaceRoot: paths.workspaceRoot },
      client: createClient(),
      store,
      outbox: { enqueueText: () => undefined },
      activeQueries,
      workflowWaits: new Map(),
      workflowWakeEvents: new Map(),
      isStopping: () => false,
      harnesses: registry,
    });

    activeQueries.set(conversationId, { abort: async () => undefined, steer: async () => false });
    await assert.rejects(
      () => turns.switchHarness({ conversationId, harness: CLAUDE_HARNESS }),
      /Codex is currently working/,
    );
    activeQueries.delete(conversationId);

    const job = store.enqueuePromptJob({ conversationId, chatId: "3", messageId: "10", prompt: "later" });
    assert.equal(job.harness, CODEX_HARNESS);
    await assert.rejects(
      () => turns.switchHarness({ conversationId, harness: CLAUDE_HARNESS }),
      /Queued prompts are still waiting/,
    );
    store.setPromptJobDisposition(job.id, "cancelled");

    const result = await turns.switchHarness({ conversationId, harness: CLAUDE_HARNESS });
    assert.deepEqual(result, { switched: true, previous: CODEX_HARNESS, next: CLAUDE_HARNESS, sessionId: null, workingDirectory: paths.repo });
    assert.equal(store.getActiveHarness(conversationId), CLAUDE_HARNESS);
    const again = await turns.switchHarness({ conversationId, harness: CLAUDE_HARNESS });
    assert.equal(again.switched, false);
    await assert.rejects(() => turns.switchHarness({ conversationId, harness: "gemini" }), /Unknown service/);

    activeQueries.set(conversationId, { abort: async () => undefined, steer: async () => false });
    await assert.rejects(() => turns.switchWorkspace({ conversationId, target: "plain" }), /Claude is currently working/);
    await assert.rejects(() => turns.createWorkspace({ conversationId, name: "fresh" }), /Claude is currently working/);
    activeQueries.delete(conversationId);
    const same = await turns.switchWorkspace({ conversationId, target: "repo" });
    assert.deepEqual(same, { switched: false, previous: paths.repo, workingDirectory: paths.repo });
  });
});

test("workspace policy keeps every folder under the root", async () => {
  await withStore(async (store, paths) => {
    const conversationId = store.upsertConversation({ chatId: "16", user: { id: 16 } });
    store.setActiveHarness(conversationId, CODEX_HARNESS);
    const { registry } = createRegistry();
    const turns = createTurnController(store, registry, createClient(), { workspaceRoot: paths.workspaceRoot });

    const byName = await turns.switchWorkspace({ conversationId, target: "repo" });
    assert.deepEqual(byName, { switched: true, previous: null, workingDirectory: paths.repo });
    const byAbsolute = await turns.switchWorkspace({ conversationId, target: paths.plain });
    assert.equal(byAbsolute.workingDirectory, paths.plain);
    assert.equal(store.getWorkingDirectory(conversationId), paths.plain);

    await assert.rejects(() => turns.switchWorkspace({ conversationId, target: "missing" }), /does not exist under/);
    await assert.rejects(() => turns.switchWorkspace({ conversationId, target: "../" }), /outside the workspace root/);
    await assert.rejects(() => turns.switchWorkspace({ conversationId, target: "escape" }), /outside the workspace root/);
    await assert.rejects(() => turns.switchWorkspace({ conversationId, target: "/etc" }), /outside the workspace root/);
    await assert.rejects(() => turns.switchWorkspace({ conversationId, target: "notes.txt" }), /not a directory/);
    await assert.rejects(() => turns.switchWorkspace({ conversationId, target: "" }), /Folder path is empty/);
    assert.equal(store.getWorkingDirectory(conversationId), paths.plain);

    await assert.rejects(() => turns.createWorkspace({ conversationId, name: "../oops" }), /Folder names may only use/);
    await assert.rejects(() => turns.createWorkspace({ conversationId, name: ".hidden" }), /Folder names may only use/);
    await assert.rejects(() => turns.createWorkspace({ conversationId, name: "repo" }), /already exists/);
    const created = await turns.createWorkspace({ conversationId, name: "fresh-1" });
    assert.deepEqual(created, { switched: true, created: true, previous: paths.plain, workingDirectory: join(paths.workspaceRoot, "fresh-1") });
    assert.equal(store.getWorkingDirectory(conversationId), created.workingDirectory);
    assert.equal(execFileSync("git", ["symbolic-ref", "--short", "HEAD"], { cwd: created.workingDirectory }).toString().trim(), "main");

    const candidates = (await listWorkspaceCandidates(paths.workspaceRoot)).map((candidate) => [candidate.name, candidate.git]);
    assert.deepEqual(candidates, [["fresh-1", true], ["repo", true], ["plain", false]]);
  });
});

test("new sessions and active turns are recorded under the active harness", async () => {
  await withStore(async (store, paths) => {
    const conversationId = store.upsertConversation({ chatId: "4", user: { id: 4 } });
    const { registry, claude, codex } = createRegistry();
    const turns = createTurnController(store, registry);
    store.setActiveHarness(conversationId, CLAUDE_HARNESS);
    await assert.rejects(() => turns.startNewSession({ conversationId }), /No folder is mounted/);
    store.setWorkingDirectory(conversationId, paths.repo);
    const sessionId = await turns.startNewSession({ conversationId });
    assert.equal(sessionId, "claude-fresh");
    assert.equal(claude.calls.length, 1);
    assert.equal(claude.calls[0][1].workingDirectory, paths.repo);
    assert.equal(codex.calls.length, 0);
    assert.equal(store.getHarnessSessionId(conversationId, CLAUDE_HARNESS), "claude-fresh");
    assert.equal(store.getHarnessSessionId(conversationId, CODEX_HARNESS), undefined);

    store.upsertActiveTurn({ conversationId, chatId: "4", messageId: "1", sessionId: null, prompt: "hi" });
    const [turn] = store.getActiveTurns();
    assert.equal(turn.harness, CLAUDE_HARNESS);
    store.updateActiveTurnSessionId(conversationId, "claude-live");
    assert.equal(store.getHarnessSessionId(conversationId, CLAUDE_HARNESS), "claude-live");
    assert.equal(store.getHarnessSessionId(conversationId, CODEX_HARNESS), undefined);
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
    const job = store.stageRestartRecovery({ turn, prompt: buildRestartSyntheticText("self_induced", turn.harness) });
    assert.equal(job.harness, CLAUDE_HARNESS);
    assert.match(job.prompt, /You are Claude, connected through `alasio.service`/);
    assert.equal(store.getHarnessSessionId(conversationId, CLAUDE_HARNESS), "claude-1");
    assert.equal(store.getHarnessSessionId(conversationId, CODEX_HARNESS), undefined);
    assert.match(buildRestartSyntheticText("operator_induced"), /You are Codex/);
    assert.match(buildRestartSyntheticText("operator_induced"), /connected through `alasio\.service`/);
    assert.match(buildRestartSyntheticText("operator_induced"), /bots\/alasio\/restart-alasio-operator\.sh`; from that directory use `\.\/restart-alasio-operator\.sh`/);
    const standaloneEnv = { ALASIO_SERVICE_UNIT: "alasio-standalone.service", ALASIO_RESTART_WRAPPER: "/home/operator/alasio/restart-alasio-standalone.sh" };
    const standalone = buildRestartSyntheticText("self_induced", CLAUDE_HARNESS, standaloneEnv);
    assert.match(standalone, /You are Claude, connected through `alasio-standalone\.service`/);
    assert.match(standalone, /`\/home\/operator\/alasio\/restart-alasio-standalone\.sh`; from that directory use `\.\/restart-alasio-standalone\.sh`/);
    assert.match(standalone, /not raw `sudo systemctl restart alasio-standalone\.service`/);
  });
});

test("service panel lists both harness mounts and offers the inactive switch", async () => {
  await withStore((store) => {
    const conversationId = store.upsertConversation({ chatId: "6", user: { id: 6 } });
    const neutral = buildServicePanel({ store, activeQueries: new Map(), conversationId, notice: CHOOSE_SERVICE_NOTICE });
    assert.match(neutral.text, /Active: none/);
    assert.match(neutral.text, /Nothing runs until a service is chosen/);
    assert.match(neutral.text, /No service is mounted/);
    assert.deepEqual(
      neutral.options.reply_markup.inline_keyboard.flat().map((button) => button.text),
      ["Use Codex", "Use Claude", "Close"],
    );

    store.setActiveHarness(conversationId, CODEX_HARNESS);
    store.setSessionId(conversationId, "codex-abcdef12");
    const panel = buildServicePanel({ store, activeQueries: new Map(), conversationId });
    assert.match(panel.text, /Active: Codex/);
    assert.match(panel.text, /\* Codex: session codex-ab/);
    assert.match(panel.text, /Claude: no mounted session/);
    const buttons = panel.options.reply_markup.inline_keyboard.flat().map((button) => button.text);
    assert.deepEqual(buttons, ["Use Claude", "Close"]);
  });
});

test("/service text command switches and reports the outcome", async () => {
  await withStore(async (store) => {
    const conversationId = store.upsertConversation({ chatId: "7", user: { id: 7 } });
    const client = createClient();
    const switches = [];
    const switchHarness = async (args) => {
      switches.push(args);
      const previous = store.getActiveHarness(args.conversationId);
      store.setActiveHarness(args.conversationId, args.harness);
      return { switched: true, previous, next: args.harness, sessionId: null };
    };
    await handleServiceTextCommand({
      client,
      store,
      activeQueries: new Map(),
      conversationId,
      chatId: "7",
      target: "claude",
      switchHarness,
    });
    assert.deepEqual(switches, [{ conversationId, harness: CLAUDE_HARNESS }]);
    assert.match(client.calls.sendMessage[0][1], /Mounted Claude\. Send a message to start\./);
    assert.match(client.calls.sendMessage[0][1], /Active: Claude/);

    await handleServiceTextCommand({
      client,
      store,
      activeQueries: new Map(),
      conversationId,
      chatId: "7",
      target: "codex",
      switchHarness,
    });
    assert.match(client.calls.sendMessage[1][1], /Switched to Codex\./);

    await handleServiceTextCommand({
      client,
      store,
      activeQueries: new Map(),
      conversationId,
      chatId: "7",
      target: "gemini",
      switchHarness: async () => {
        throw new Error("should not be called");
      },
    });
    assert.match(client.calls.sendMessage[2][1], /Unknown service "gemini"/);
  });
});

test("service callbacks switch harness and surface refusals in the panel", async () => {
  await withStore(async (store) => {
    const conversationId = store.upsertConversation({ chatId: "8", user: { id: 8 } });
    store.setActiveHarness(conversationId, CODEX_HARNESS);
    const client = createClient();
    const actionId = store.createCallbackAction({ conversationId, kind: "service:use", payload: { harness: CLAUDE_HARNESS } });
    const action = store.consumeCallbackAction(actionId);
    await handleServiceControlCallback({
      client,
      store,
      activeQueries: new Map(),
      action,
      switchHarness: async () => {
        throw new Error("Codex is currently working. Stop the active turn before switching services.");
      },
      callbackQueryId: "cb-1",
      chatId: "8",
      messageId: 5,
    });
    assert.match(client.calls.answerCallbackQuery[0][1], /Codex is currently working/);
    assert.match(client.calls.editMessageText[0][2], /Active: Codex/);
    assert.match(client.calls.editMessageText[0][2], /Codex is currently working/);
  });
});

test("callback handler rejects panels created under another harness", async () => {
  await withStore(async (store) => {
    const conversationId = store.upsertConversation({ chatId: "9", user: { id: 9 } });
    store.setActiveHarness(conversationId, CODEX_HARNESS);
    const actionId = store.createCallbackAction({ conversationId, kind: "control:current", payload: {} });
    store.setActiveHarness(conversationId, CLAUDE_HARNESS);
    const client = createClient();
    const handler = new CallbackHandler({
      authorizer: { isAuthorizedCallbackQuery: () => true },
      client,
      config: { workingDirectory: "/tmp" },
      store,
      turns: {
        harnessFor: () => createFakeHarness(CLAUDE_HARNESS),
        switchHarness: async () => ({ switched: false }),
      },
      activeQueries: new Map(),
    });
    await handler.handle({
      id: "cb-9",
      data: actionId,
      from: { id: 9 },
      message: { chat: { id: 9 }, message_id: 3 },
    });
    assert.deepEqual(client.calls.answerCallbackQuery, [["cb-9", "This panel belongs to another service. Open it again."]]);
  });
});

test("prompts sent before a service and folder are chosen only get the pickers and are not queued", async () => {
  await withStore(async (store, paths) => {
    const conversationId = store.upsertConversation({ chatId: "12", user: { id: 12 } });
    const client = createClient();
    const { registry, codex, claude } = createRegistry();
    const turns = createTurnController(store, registry, client, { workspaceRoot: paths.workspaceRoot });

    await turns.processPrompt({ conversationId, chatId: "12", messageId: "1", text: "hello there", filePaths: [] });
    assert.equal(client.calls.sendMessage.length, 1);
    assert.match(client.calls.sendMessage[0][1], /Active: none/);
    assert.match(client.calls.sendMessage[0][1], /your message was not queued/);
    assert.deepEqual(
      client.calls.sendMessage[0][2].reply_markup.inline_keyboard.flat().map((button) => button.text),
      ["Use Codex", "Use Claude", "Close"],
    );
    assert.equal(store.hasOpenPromptJobs(conversationId), false);
    assert.equal(store.claimNextPromptJob(conversationId) ?? null, null);
    assert.equal(codex.calls.length, 0);
    assert.equal(claude.calls.length, 0);

    await turns.processPrompt({ conversationId, chatId: "12", messageId: "2", text: "/sessions", filePaths: [] });
    assert.match(client.calls.sendMessage[1][1], /Active: none/);
    await turns.processPrompt({ conversationId, chatId: "12", messageId: "3", text: "/stop", filePaths: [] });
    assert.equal(client.calls.sendMessage[2][1], "No active query to stop.");

    await turns.processPrompt({ conversationId, chatId: "12", messageId: "4", text: "/service claude", filePaths: [] });
    assert.match(client.calls.sendMessage[3][1], /Mounted Claude/);
    assert.equal(store.getActiveHarness(conversationId), CLAUDE_HARNESS);
    // Service first, then folder: the folder picker follows immediately.
    assert.match(client.calls.sendMessage[4][1], /Workspace\n\nFolder: none/);
    assert.match(client.calls.sendMessage[4][1], /No folder is mounted/);
    assert.deepEqual(
      client.calls.sendMessage[4][2].reply_markup.inline_keyboard.flat().map((button) => button.text),
      ["repo", "· plain", "New folder…", "Refresh", "Close"],
    );

    await turns.processPrompt({ conversationId, chatId: "12", messageId: "5", text: "hello again", filePaths: [] });
    assert.match(client.calls.sendMessage[5][1], /Folder: none/);
    assert.equal(store.hasOpenPromptJobs(conversationId), false);
    await turns.processPrompt({ conversationId, chatId: "12", messageId: "6", text: "/sessions new", filePaths: [] });
    assert.match(client.calls.sendMessage[6][1], /Folder: none/);
    assert.equal(claude.calls.length, 0);

    await turns.processPrompt({ conversationId, chatId: "12", messageId: "7", text: "/workspace escape", filePaths: [] });
    assert.match(client.calls.sendMessage[7][1], /outside the workspace root/);
    assert.equal(store.getWorkingDirectory(conversationId), null);
    await turns.processPrompt({ conversationId, chatId: "12", messageId: "8", text: "/workspace repo", filePaths: [] });
    assert.match(client.calls.sendMessage[8][1], /Mounted repo \(/);
    assert.match(client.calls.sendMessage[8][1], new RegExp(`Folder: ${paths.repo}`));
    assert.equal(client.calls.sendMessage[8][2].reply_markup.inline_keyboard[0][0].text, "* repo");
    assert.equal(store.getWorkingDirectory(conversationId), paths.repo);

    await turns.processPrompt({ conversationId, chatId: "12", messageId: "9", text: "/sessions new", filePaths: [] });
    assert.match(client.calls.sendMessage[9][1], /New Claude session mounted: claude-f/);
    assert.equal(claude.calls.length, 1);
    assert.equal(codex.calls.length, 0);

    await turns.processPrompt({ conversationId, chatId: "12", messageId: "10", text: "/workspace new made-here", filePaths: [] });
    assert.match(client.calls.sendMessage[10][1], /Created and mounted made-here/);
    assert.equal(store.getWorkingDirectory(conversationId), join(paths.workspaceRoot, "made-here"));
    assert.equal(store.getSessionId(conversationId), undefined);
    await turns.processPrompt({ conversationId, chatId: "12", messageId: "11", text: "/workspace repo", filePaths: [] });
    assert.equal(store.getSessionId(conversationId), "claude-fresh");
  });
});

test("/start offers the pickers until a service and folder are mounted", async () => {
  await withStore(async (store, paths) => {
    const client = createClient();
    const { registry } = createRegistry();
    const turns = createTurnController(store, registry, client, { workspaceRoot: paths.workspaceRoot });
    const handler = new MessageHandler({
      authorizer: { isAuthorizedMessage: () => true },
      client,
      store,
      turns,
      mediaGroups: { buffer() {} },
      log: { error() {} },
    });
    const message = { message_id: 1, chat: { id: 13, type: "private" }, from: { id: 13 }, text: "/start" };
    await handler.handle(message, 1);
    assert.match(client.calls.sendMessage[0][1], /Active: none/);

    store.setActiveHarness("telegram:13", CODEX_HARNESS);
    await handler.handle({ ...message, message_id: 2 }, 2);
    assert.match(client.calls.sendMessage[1][1], /Folder: none/);

    store.setWorkingDirectory("telegram:13", paths.repo);
    await handler.handle({ ...message, message_id: 3 }, 3);
    assert.equal(client.calls.sendMessage[2][1], "Alasio is ready.");
  });
});

test("callbacks other than service and workspace controls are refused while nothing is mounted", async () => {
  await withStore(async (store, paths) => {
    const conversationId = store.upsertConversation({ chatId: "14", user: { id: 14 } });
    const actionId = store.createCallbackAction({ conversationId, kind: "queue", payload: { prompt: "later" } });
    const client = createClient();
    const { registry } = createRegistry();
    const turns = createTurnController(store, registry, client, { workspaceRoot: paths.workspaceRoot });
    const handler = new CallbackHandler({
      authorizer: { isAuthorizedCallbackQuery: () => true },
      client,
      config: { workspaceRoot: paths.workspaceRoot },
      store,
      turns,
      activeQueries: new Map(),
    });
    await handler.handle({ id: "cb-14", data: actionId, from: { id: 14 }, message: { chat: { id: 14 }, message_id: 3 } });
    assert.deepEqual(client.calls.answerCallbackQuery, [["cb-14", NO_SERVICE_MOUNTED]]);
    assert.equal(client.calls.editMessageText.length, 0);

    const useId = store.createCallbackAction({ conversationId, kind: "service:use", payload: { harness: CODEX_HARNESS } });
    await handler.handle({ id: "cb-15", data: useId, from: { id: 14 }, message: { chat: { id: 14 }, message_id: 4 } });
    assert.equal(store.getActiveHarness(conversationId), CODEX_HARNESS);
    assert.match(client.calls.answerCallbackQuery[1][1], /Mounted Codex/);
    assert.match(client.calls.editMessageText[0][2], /Active: Codex/);
    // Mounting a service chains into the folder picker.
    assert.match(client.calls.sendMessage[0][1], /Folder: none/);

    const queueAgain = store.createCallbackAction({ conversationId, kind: "queue", payload: { prompt: "later" } });
    await handler.handle({ id: "cb-16", data: queueAgain, from: { id: 14 }, message: { chat: { id: 14 }, message_id: 5 } });
    assert.deepEqual(client.calls.answerCallbackQuery[2], ["cb-16", NO_WORKSPACE_MOUNTED]);

    const pickId = client.calls.sendMessage[0][2].reply_markup.inline_keyboard[0][0].callback_data;
    await handler.handle({ id: "cb-17", data: pickId, from: { id: 14 }, message: { chat: { id: 14 }, message_id: 6 } });
    assert.equal(store.getWorkingDirectory(conversationId), paths.repo);
    assert.match(client.calls.answerCallbackQuery[3][1], /Mounted repo/);
    assert.match(client.calls.editMessageText[1][2], new RegExp(`Folder: ${paths.repo}`));
  });
});
