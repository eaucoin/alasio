import assert from "node:assert/strict";
import { test } from "node:test";

import type { InlineKeyboardMarkup } from "@grammyjs/types";

import { noActiveTurns } from "../src/harness/active-turns.ts";
import {
  type CreateSessionWorkspace,
  type WorkspaceControlCallback,
  type WorkspaceControlStore,
  buildWorkspacePanel,
  handleWorkspaceControlCallback,
} from "../src/operator/workspace-control.ts";
import type { Client } from "../src/telegram/client.ts";

// A store whose only jobs here are minting callback ids and reporting the mounted folder.
function fakeStore(): WorkspaceControlStore {
  return {
    createCallbackAction: ({ kind, payload }) => `${kind}:${JSON.stringify(payload ?? {})}`,
    getWorkingDirectory: () => null,
  };
}

// Buttons in a panel/edit, flattened to their labels.
const labels = (markup: InlineKeyboardMarkup) => markup.inline_keyboard.flat().map((b) => b.text);

test("the workspace panel offers New empty workspace only when session filesystems are enabled", async () => {
  const store = fakeStore();
  const off = await buildWorkspacePanel({ store, conversationId: "c1", workspaceRoot: "/root", sandboxEnabled: false });
  assert.ok(!labels(off.options.reply_markup).includes("New empty workspace…"));
  const on = await buildWorkspacePanel({ store, conversationId: "c1", workspaceRoot: "/root", sandboxEnabled: true });
  assert.ok(labels(on.options.reply_markup).includes("New empty workspace…"));
});

function fakeClient() {
  const calls: {
    answerCallbackQuery: Parameters<Client["answerCallbackQuery"]>[];
    editMessageText: Parameters<Client["editMessageText"]>[];
  } = { answerCallbackQuery: [], editMessageText: [] };
  const client: WorkspaceControlCallback["client"] = {
    answerCallbackQuery: async (...a) => {
      calls.answerCallbackQuery.push(a);
      return true;
    },
    editMessageText: async (...a) => {
      calls.editMessageText.push(a);
      return true;
    },
    deleteMessage: () => assert.fail("deleteMessage"),
    sendMessage: () => assert.fail("sendMessage"),
  };
  return { calls, client };
}

test("choosing New empty workspace shows the internet dialog, then creates with the chosen mode", async () => {
  const store = fakeStore();
  const { calls, client } = fakeClient();
  const created: Parameters<CreateSessionWorkspace>[0][] = [];
  const createSessionWorkspace: CreateSessionWorkspace = async (args) => {
    created.push(args);
    return { created: true, switched: true, previous: null, workingDirectory: "sessionfs:fs-x" };
  };
  const switchWorkspace = () => assert.fail("no folder is switched to here");

  // Step 1: the "sessionfs" action edits the panel to a two-option internet dialog.
  await handleWorkspaceControlCallback({
    client, store, activeTurns: noActiveTurns,
    action: { id: "a1", kind: "workspace:sessionfs", conversationId: "c1", payload: {} },
    workspaceRoot: "/root", sandboxEnabled: true, switchWorkspace,
    createSessionWorkspace,
    callbackQueryId: "cb1", chatId: 1, messageId: 2,
  });
  const dialog = calls.editMessageText.at(-1)?.[3]?.reply_markup;
  assert.ok(dialog);
  assert.deepEqual(labels(dialog).sort(), ["Back", "Full internet", "No internet"]);

  // Step 2: the "sessionfs_create" action with net=full creates the session filesystem.
  await handleWorkspaceControlCallback({
    client, store, activeTurns: noActiveTurns,
    action: { id: "a2", kind: "workspace:sessionfs_create", conversationId: "c1", payload: { net: "full" } },
    workspaceRoot: "/root", sandboxEnabled: true, switchWorkspace,
    createSessionWorkspace,
    callbackQueryId: "cb2", chatId: 1, messageId: 2,
  });
  assert.deepEqual(created, [{ conversationId: "c1", netMode: "full" }]);

  // Without the creator wired, it declines rather than throwing.
  await handleWorkspaceControlCallback({
    client, store, activeTurns: noActiveTurns,
    action: { id: "a3", kind: "workspace:sessionfs_create", conversationId: "c1", payload: { net: "none" } },
    workspaceRoot: "/root", sandboxEnabled: true, switchWorkspace, createSessionWorkspace: null,
    callbackQueryId: "cb3", chatId: 1, messageId: 2,
  });
  assert.match(calls.answerCallbackQuery.at(-1)?.[1] ?? "", /not enabled/);
});
