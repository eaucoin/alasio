import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import type { InlineKeyboardButton, InlineKeyboardMarkup } from "@grammyjs/types";
import { Effect, Layer } from "effect";

import { resolveCodexModelChoice, ALASIO_CODEX_MODEL, ALASIO_CODEX_REASONING_EFFORT } from "../src/codex/model.ts";
import { getClaudeEffort, getClaudeModel, ALASIO_CLAUDE_MODEL } from "../src/harness/claude/model.ts";
import { buildClaudeQueryOptions } from "../src/harness/claude/runtime.ts";
import { CLAUDE_HARNESS, CODEX_HARNESS } from "../src/harness/names.ts";
import { parseCommand } from "../src/operator/command-parser.ts";
import { buildModelPanel, handleModelControlCallback, isModelControlAction, type ModelControlHarness } from "../src/operator/model-control.ts";
import type { CallbackAction } from "../src/persistence/callback-repository.ts";
import { SqliteStore, Store } from "../src/persistence/store.ts";
import type { TelegramClient } from "../src/telegram/client.ts";
import { recordingTelegram } from "./support/telegram-calls.ts";

async function withStore<T>(run: (store: SqliteStore, id: string) => T | Promise<T>): Promise<T> {
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

/** Runs a /model effect on `store` and a Telegram client recording into `telegram`. */
function run<A, E>(effect: Effect.Effect<A, E, Store | TelegramClient>, store: SqliteStore, telegram = recordingTelegram()): Promise<A> {
  return Effect.runPromise(effect.pipe(Effect.provide(Layer.merge(Layer.succeed(Store, store), telegram.layer))));
}

/** The buttons of a panel's keyboard, row after row. */
function buttonsOf(markup: InlineKeyboardMarkup | undefined): InlineKeyboardButton[] {
  return markup?.inline_keyboard.flat() ?? [];
}

/** The callback data of the panel button labelled `text`. */
function callbackData(markup: InlineKeyboardMarkup | undefined, text: string): string {
  const button = buttonsOf(markup).find((candidate) => candidate.text === text);
  assert.ok(button && "callback_data" in button, `a ${text} button`);
  return button.callback_data;
}

/** The action a pressed button carries, which the store hands out once. */
function consume(store: SqliteStore, data: string): CallbackAction {
  const action = store.consumeCallbackAction(data);
  assert.ok(action, "the button's action is stored");
  return action;
}

const claudeHarness: ModelControlHarness = {
  name: CLAUDE_HARNESS,
  displayName: "Claude",
  listModels: () =>
    Effect.succeed([
      {
        id: "opus[1m]",
        label: "Opus (1M context)",
        description: "",
        resolvedModel: "opus[1m]",
        efforts: ["low", "medium", "high", "xhigh", "max"],
        defaultEffort: null,
        isDefault: false,
      },
      { id: "haiku", label: "Haiku", description: "", resolvedModel: "haiku", efforts: [], defaultEffort: null, isDefault: false },
    ]),
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
    const panel = await run(buildModelPanel({ harness: claudeHarness, conversationId: id }), store);
    assert.match(panel.text, /Current: claude-opus-5-5\[1m\] at high effort \(default\)/);
    const pick = consume(store, callbackData(panel.options.reply_markup, "Opus (1M context)"));
    assert.ok(isModelControlAction(pick.kind));

    const telegram = recordingTelegram();
    await run(handleModelControlCallback({ action: pick, callbackQueryId: "q1", chatId: 42, messageId: 7 }), store, telegram);
    const [effortPanel] = telegram.calls.editMessageText;
    const effortTexts = buttonsOf(effortPanel?.[3]?.reply_markup).map((b) => b.text);
    assert.deepEqual(effortTexts.filter((t) => t !== "Close"), ["low", "medium", "high", "xhigh", "max"]);
    assert.equal(store.getModelChoice(id, CLAUDE_HARNESS), null, "nothing is stored until an effort is chosen");

    const max = callbackData(effortPanel?.[3]?.reply_markup, "max");
    await run(handleModelControlCallback({ action: consume(store, max), callbackQueryId: "q2", chatId: 42, messageId: 7 }), store, telegram);
    assert.deepEqual(store.getModelChoice(id, CLAUDE_HARNESS), { model: "opus[1m]", effort: "max" });
  });
});

test("a model without effort control is chosen in one step", async () => {
  await withStore(async (store, id) => {
    const panel = await run(buildModelPanel({ harness: claudeHarness, conversationId: id }), store);
    const haiku = callbackData(panel.options.reply_markup, "Haiku");
    await run(handleModelControlCallback({ action: consume(store, haiku), callbackQueryId: "q", chatId: 42, messageId: 7 }), store);
    assert.deepEqual(store.getModelChoice(id, CLAUDE_HARNESS), { model: "haiku", effort: null });
  });
});
