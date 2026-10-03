// @ts-nocheck
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { resolveCodexModelChoice, ALASIO_CODEX_MODEL, ALASIO_CODEX_REASONING_EFFORT } from "../src/codex/model.ts";
import { getClaudeEffort, getClaudeModel, ALASIO_CLAUDE_MODEL } from "../src/harness/claude/model.ts";
import { buildClaudeQueryOptions } from "../src/harness/claude/runtime.ts";
import { CLAUDE_HARNESS, CODEX_HARNESS } from "../src/harness/names.ts";
import { parseCommand } from "../src/operator/command-parser.ts";
import { buildModelPanel, handleModelControlCallback, isModelControlAction } from "../src/operator/model-control.ts";
import { SqliteStore } from "../src/persistence/store.ts";

async function withStore(run) {
  const root = mkdtempSync(join(tmpdir(), "alasio-model-"));
  const store = new SqliteStore(root);
  try {
    store.upsertConversation({ chatId: 42, user: { id: 1 } });
    store.setActiveHarness("telegram:42", CLAUDE_HARNESS);
    return await run(store, "telegram:42");
  } finally {
    store.close();
    rmSync(root, { recursive: true, force: true });
  }
}

function fakeClient() {
  const calls = [];
  return {
    calls,
    async sendMessage(chatId, text, options) { calls.push({ op: "send", text, options }); },
    async editMessageText(chatId, messageId, text, options) { calls.push({ op: "edit", text, options }); },
    async answerCallbackQuery(id, text) { calls.push({ op: "answer", text }); },
  };
}

const claudeHarness = {
  name: CLAUDE_HARNESS,
  displayName: "Claude",
  async listModels() {
    return [
      { id: "opus[1m]", label: "Opus (1M context)", efforts: ["low", "medium", "high", "xhigh", "max"], defaultEffort: null },
      { id: "haiku", label: "Haiku", efforts: [], defaultEffort: null },
    ];
  },
  defaultModelChoice: () => ({ model: ALASIO_CLAUDE_MODEL, effort: "high" }),
};

test("/model parses to the model command", () => {
  assert.deepEqual(parseCommand("/model"), { type: "model" });
  assert.deepEqual(parseCommand("/model@alasio_bot"), { type: "model" });
});

test("a model choice is stored per conversation and per harness", async () => {
  await withStore((store, id) => {
    assert.equal(store.getModelChoice(id, CLAUDE_HARNESS), null);
    store.setModelChoice(id, CLAUDE_HARNESS, { model: "sonnet", effort: "medium" });
    store.setModelChoice(id, CODEX_HARNESS, { model: "gpt-6-astra", effort: "ultra" });
    assert.deepEqual(store.getModelChoice(id, CLAUDE_HARNESS), { model: "sonnet", effort: "medium" });
    assert.deepEqual(store.getModelChoice(id, CODEX_HARNESS), { model: "gpt-6-astra", effort: "ultra" });
    store.clearModelChoice(id, CLAUDE_HARNESS);
    assert.equal(store.getModelChoice(id, CLAUDE_HARNESS), null);
    assert.deepEqual(store.getModelChoice(id, CODEX_HARNESS), { model: "gpt-6-astra", effort: "ultra" });
  });
});

test("the Claude pin is the 1M-context variant, and a choice overrides it for the turn", () => {
  assert.equal(ALASIO_CLAUDE_MODEL, "claude-opus-5-5[1m]");
  assert.equal(getClaudeModel({}), ALASIO_CLAUDE_MODEL);
  assert.equal(getClaudeModel({}, { model: "sonnet", effort: "low" }), "sonnet");
  assert.equal(getClaudeEffort({}, { model: "sonnet", effort: "low" }), "low");
  assert.equal(getClaudeEffort({}, { model: "haiku", effort: null }), null, "a model without effort gets none");
  const options = buildClaudeQueryOptions({
    workingDirectory: "/w",
    claudeEnv: {},
    mcpServers: {},
    resumeSession: "abc",
    resumeExists: true,
    controller: new AbortController(),
    hooks: {},
    env: {},
    modelChoice: { model: "claude-fable-5-1[1m]", effort: "max" },
  });
  assert.equal(options.model, "claude-fable-5-1[1m]");
  assert.equal(options.effort, "max");
});

test("a Codex choice replaces the pinned model and effort for the turn", () => {
  assert.deepEqual(resolveCodexModelChoice(null), { model: ALASIO_CODEX_MODEL, effort: ALASIO_CODEX_REASONING_EFFORT });
  assert.deepEqual(resolveCodexModelChoice({ model: "gpt-6-astra", effort: "ultra" }), { model: "gpt-6-astra", effort: "ultra" });
});

test("/model walks model then effort, using the chosen model's own effort levels", async () => {
  await withStore(async (store, id) => {
    const panel = await buildModelPanel({ store, harness: claudeHarness, conversationId: id });
    assert.match(panel.text, /Current: claude-opus-5-5\[1m\] at high effort \(default\)/);
    const opusButton = panel.options.reply_markup.inline_keyboard.flat().find((b) => b.text === "Opus (1M context)");
    const pick = store.consumeCallbackAction(opusButton.callback_data);
    assert.ok(isModelControlAction(pick.kind));

    const client = fakeClient();
    await handleModelControlCallback({ client, store, action: pick, callbackQueryId: "q1", chatId: 42, messageId: 7 });
    const effortPanel = client.calls.find((call) => call.op === "edit");
    const effortTexts = effortPanel.options.reply_markup.inline_keyboard.flat().map((b) => b.text);
    assert.deepEqual(effortTexts.filter((t) => t !== "Close"), ["low", "medium", "high", "xhigh", "max"]);
    assert.equal(store.getModelChoice(id, CLAUDE_HARNESS), null, "nothing is stored until an effort is chosen");

    const maxButton = effortPanel.options.reply_markup.inline_keyboard.flat().find((b) => b.text === "max");
    await handleModelControlCallback({ client, store, action: store.consumeCallbackAction(maxButton.callback_data), callbackQueryId: "q2", chatId: 42, messageId: 7 });
    assert.deepEqual(store.getModelChoice(id, CLAUDE_HARNESS), { model: "opus[1m]", effort: "max" });
  });
});

test("a model without effort control is chosen in one step", async () => {
  await withStore(async (store, id) => {
    const panel = await buildModelPanel({ store, harness: claudeHarness, conversationId: id });
    const haiku = panel.options.reply_markup.inline_keyboard.flat().find((b) => b.text === "Haiku");
    const client = fakeClient();
    await handleModelControlCallback({ client, store, action: store.consumeCallbackAction(haiku.callback_data), callbackQueryId: "q", chatId: 42, messageId: 7 });
    assert.deepEqual(store.getModelChoice(id, CLAUDE_HARNESS), { model: "haiku", effort: null });
  });
});
