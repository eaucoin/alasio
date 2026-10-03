// @ts-nocheck
import assert from "node:assert/strict";
import { test } from "node:test";

import { buildWorkspacePanel, handleWorkspaceControlCallback } from "../src/operator/workspace-control.ts";

// A store whose only jobs here are minting callback ids and reporting the mounted folder.
function fakeStore() {
  return {
    createCallbackAction: ({ kind, payload }) => `${kind}:${JSON.stringify(payload ?? {})}`,
    getWorkingDirectory: () => null,
  };
}

// Buttons in a panel/edit, flattened to their labels.
const labels = (markup) => (markup.reply_markup.inline_keyboard ?? []).flat().map((b) => b.text);

test("the workspace panel offers New empty workspace only when session filesystems are enabled", async () => {
  const store = fakeStore();
  const off = await buildWorkspacePanel({ store, conversationId: "c1", workspaceRoot: "/root", sandboxEnabled: false });
  assert.ok(!labels(off.options).includes("New empty workspace…"));
  const on = await buildWorkspacePanel({ store, conversationId: "c1", workspaceRoot: "/root", sandboxEnabled: true });
  assert.ok(labels(on.options).includes("New empty workspace…"));
});

function fakeClient() {
  const calls = { answerCallbackQuery: [], editMessageText: [] };
  return {
    calls,
    answerCallbackQuery: async (...a) => calls.answerCallbackQuery.push(a),
    editMessageText: async (...a) => calls.editMessageText.push(a),
  };
}

test("choosing New empty workspace shows the internet dialog, then creates with the chosen mode", async () => {
  const store = fakeStore();
  const client = fakeClient();
  const created = [];

  // Step 1: the "sessionfs" action edits the panel to a two-option internet dialog.
  await handleWorkspaceControlCallback({
    client, store, activeQueries: new Map(),
    action: { kind: "workspace:sessionfs", conversationId: "c1", payload: {} },
    workspaceRoot: "/root", sandboxEnabled: true,
    createSessionWorkspace: async (args) => { created.push(args); return { created: true, switched: true, workingDirectory: "sessionfs:fs-x" }; },
    callbackQueryId: "cb1", chatId: 1, messageId: 2,
  });
  const dialog = client.calls.editMessageText.at(-1)[3];
  assert.deepEqual(labels(dialog).sort(), ["Back", "Full internet", "No internet"]);

  // Step 2: the "sessionfs_create" action with net=full creates the session filesystem.
  await handleWorkspaceControlCallback({
    client, store, activeQueries: new Map(),
    action: { kind: "workspace:sessionfs_create", conversationId: "c1", payload: { net: "full" } },
    workspaceRoot: "/root", sandboxEnabled: true,
    createSessionWorkspace: async (args) => { created.push(args); return { created: true, switched: true, workingDirectory: "sessionfs:fs-x" }; },
    callbackQueryId: "cb2", chatId: 1, messageId: 2,
  });
  assert.deepEqual(created, [{ conversationId: "c1", netMode: "full" }]);

  // Without the creator wired, it declines rather than throwing.
  await handleWorkspaceControlCallback({
    client, store, activeQueries: new Map(),
    action: { kind: "workspace:sessionfs_create", conversationId: "c1", payload: { net: "none" } },
    workspaceRoot: "/root", sandboxEnabled: true, createSessionWorkspace: null,
    callbackQueryId: "cb3", chatId: 1, messageId: 2,
  });
  assert.match(client.calls.answerCallbackQuery.at(-1)[1], /not enabled/);
});
