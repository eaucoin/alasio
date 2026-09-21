import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import Database from "better-sqlite3";

import { TurnController } from "../src/codex/turn-controller.js";
import { createHarnessRegistry, resolveHarnessName } from "../src/harness/index.js";
import { CLAUDE_HARNESS, CODEX_HARNESS, harnessDisplayName, normalizeHarnessName } from "../src/harness/names.js";
import { parseCommand } from "../src/operator/command-parser.js";
import { buildRestartSyntheticText } from "../src/operator/restart-prompts.js";
import {
  buildServicePanel,
  handleServiceControlCallback,
  handleServiceTextCommand,
} from "../src/operator/service-control.js";
import { migrateSqliteSchema } from "../src/persistence/schema.js";
import { SqliteStore } from "../src/persistence/store.js";
import { CallbackHandler } from "../src/telegram/callback-handler.js";

async function withStore(run) {
  const root = mkdtempSync(join(tmpdir(), "alasio-harness-"));
  try {
    const store = new SqliteStore(root);
    try {
      return await run(store, root);
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

function createRegistry(config = { workingDirectory: "/tmp" }) {
  const codex = createFakeHarness(CODEX_HARNESS);
  const claude = createFakeHarness(CLAUDE_HARNESS);
  return {
    registry: createHarnessRegistry({ config, overrides: { [CODEX_HARNESS]: codex, [CLAUDE_HARNESS]: claude } }),
    codex,
    claude,
  };
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
  assert.equal(harnessDisplayName(CLAUDE_HARNESS), "Claude Code");
  assert.equal(resolveHarnessName({}, "telegram:1"), CODEX_HARNESS);
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
    assert.equal(store.getActiveHarness(conversationId), CODEX_HARNESS);
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

    assert.deepEqual(store.listConversationsWithSessions(CODEX_HARNESS).map((row) => row.session_id), ["codex-session"]);
    assert.deepEqual(store.listConversationsWithSessions(CLAUDE_HARNESS), []);
    assert.throws(() => store.setActiveHarness(conversationId, "gemini"), /Unknown harness/);
  });
});

test("callback actions capture the active harness generation", async () => {
  await withStore((store) => {
    const conversationId = store.upsertConversation({ chatId: "2", user: { id: 2 } });
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
    assert.equal(legacy.prepare("select value from bot_state where key = 'schema_version'").get().value, "5");
    legacy.close();

    const store = new SqliteStore(root, dbPath);
    assert.equal(store.getActiveHarness("telegram:9"), CODEX_HARNESS);
    assert.equal(store.getSessionId("telegram:9"), "legacy-session");
    store.close();
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("switching services is refused while a turn is active or prompts are queued", async () => {
  await withStore(async (store) => {
    const conversationId = store.upsertConversation({ chatId: "3", user: { id: 3 } });
    const activeQueries = new Map();
    const { registry } = createRegistry();
    const turns = new TurnController({
      config: { workingDirectory: "/tmp" },
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
    assert.deepEqual(result, { switched: true, previous: CODEX_HARNESS, next: CLAUDE_HARNESS, sessionId: null });
    assert.equal(store.getActiveHarness(conversationId), CLAUDE_HARNESS);
    const again = await turns.switchHarness({ conversationId, harness: CLAUDE_HARNESS });
    assert.equal(again.switched, false);
    await assert.rejects(() => turns.switchHarness({ conversationId, harness: "gemini" }), /Unknown service/);
  });
});

test("new sessions and active turns are recorded under the active harness", async () => {
  await withStore(async (store) => {
    const conversationId = store.upsertConversation({ chatId: "4", user: { id: 4 } });
    const { registry, claude, codex } = createRegistry();
    const turns = new TurnController({
      config: { workingDirectory: "/tmp" },
      client: createClient(),
      store,
      outbox: { enqueueText: () => undefined },
      activeQueries: new Map(),
      workflowWaits: new Map(),
      workflowWakeEvents: new Map(),
      isStopping: () => false,
      harnesses: registry,
    });
    store.setActiveHarness(conversationId, CLAUDE_HARNESS);
    const sessionId = await turns.startNewSession({ conversationId });
    assert.equal(sessionId, "claude-fresh");
    assert.equal(claude.calls.length, 1);
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
    assert.match(job.prompt, /You are Claude Code, connected through `alasio.service`/);
    assert.equal(store.getHarnessSessionId(conversationId, CLAUDE_HARNESS), "claude-1");
    assert.equal(store.getHarnessSessionId(conversationId, CODEX_HARNESS), undefined);
    assert.match(buildRestartSyntheticText("operator_induced"), /You are Codex/);
  });
});

test("service panel lists both harness mounts and offers the inactive switch", async () => {
  await withStore((store) => {
    const conversationId = store.upsertConversation({ chatId: "6", user: { id: 6 } });
    store.setSessionId(conversationId, "codex-abcdef12");
    const panel = buildServicePanel({ store, activeQueries: new Map(), conversationId });
    assert.match(panel.text, /Active: Codex/);
    assert.match(panel.text, /\* Codex: session codex-ab/);
    assert.match(panel.text, /Claude Code: no mounted session/);
    const buttons = panel.options.reply_markup.inline_keyboard.flat().map((button) => button.text);
    assert.deepEqual(buttons, ["Use Claude Code", "Close"]);
  });
});

test("/service text command switches and reports the outcome", async () => {
  await withStore(async (store) => {
    const conversationId = store.upsertConversation({ chatId: "7", user: { id: 7 } });
    const client = createClient();
    const switches = [];
    await handleServiceTextCommand({
      client,
      store,
      activeQueries: new Map(),
      conversationId,
      chatId: "7",
      target: "claude",
      switchHarness: async (args) => {
        switches.push(args);
        store.setActiveHarness(args.conversationId, args.harness);
        return { switched: true, previous: CODEX_HARNESS, next: args.harness, sessionId: null };
      },
    });
    assert.deepEqual(switches, [{ conversationId, harness: CLAUDE_HARNESS }]);
    assert.match(client.calls.sendMessage[0][1], /Switched to Claude Code/);
    assert.match(client.calls.sendMessage[0][1], /Active: Claude Code/);

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
    assert.match(client.calls.sendMessage[1][1], /Unknown service "gemini"/);
  });
});

test("service callbacks switch harness and surface refusals in the panel", async () => {
  await withStore(async (store) => {
    const conversationId = store.upsertConversation({ chatId: "8", user: { id: 8 } });
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
