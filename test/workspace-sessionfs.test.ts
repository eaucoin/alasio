import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import type { InlineKeyboardMarkup } from "@grammyjs/types";
import { Effect } from "effect";

import { type WorkspaceControlStore, buildWorkspacePanel, handleWorkspaceControlCallback } from "../src/operator/workspace-control.ts";
import { SqliteStore } from "../src/persistence/store.ts";
import type { NetMode, SessionSandboxes } from "../src/sandbox/index.ts";
import { parseWorkspace } from "../src/workspace/kind.ts";
import { type RecordingTelegram, recordingTelegram } from "./support/telegram-calls.ts";
import { type TestAlasio, withServices } from "./support/turns.ts";

// A store whose only jobs here are minting callback ids and reporting the mounted folder.
function fakeStore(): WorkspaceControlStore {
  return {
    createCallbackAction: ({ kind, payload }) => `${kind}:${JSON.stringify(payload ?? {})}`,
    getWorkingDirectory: () => null,
  };
}

// Buttons in a panel/edit, flattened to their labels.
const labels = (markup: InlineKeyboardMarkup) => markup.inline_keyboard.flat().map((b) => b.text);

test("the workspace panel offers New empty workspace only when session filesystems are enabled", () => {
  const store = fakeStore();
  const panel = (sandboxEnabled: boolean) =>
    buildWorkspacePanel({ store, conversationId: "c1", workspaceRoot: "/root", listing: { candidates: [] }, working: false, sandboxEnabled });
  assert.ok(!labels(panel(false).options.reply_markup).includes("New empty workspace…"));
  assert.ok(labels(panel(true).options.reply_markup).includes("New empty workspace…"));
});

/** The conversation of chat 1, on Codex. */
const CONVERSATION = "telegram:1";

/** The workspace controls' services over a conversation on Codex, with `sandbox` offering session filesystems when given. */
async function withWorkspaceControls(
  sandbox: SessionSandboxes["Service"] | undefined,
  use: (alasio: TestAlasio, telegram: RecordingTelegram, store: SqliteStore) => Promise<void>,
): Promise<void> {
  const root = mkdtempSync(join(tmpdir(), "alasio-workspace-sessionfs-"));
  const store = new SqliteStore(root);
  try {
    store.setActiveHarness(store.upsertConversation({ chatId: "1", user: { id: 1 } }), "codex");
    const telegram = recordingTelegram();
    await withServices({ store, telegram: telegram.layer, workspaceRoot: root, sandbox }, (alasio) => use(alasio, telegram, store));
  } finally {
    store.close();
    rmSync(root, { recursive: true, force: true });
  }
}

/** Session filesystems that make the volumes asked for, recording each. */
function volumesMade(created: (readonly [string, NetMode | undefined])[]): SessionSandboxes["Service"] {
  return {
    volumes: {
      create: (volumeId, netMode = "none") => Effect.sync(() => {
        created.push([volumeId, netMode]);
        return { volumeId, netMode };
      }),
      fork: () => Effect.die(new Error("no volume is forked")),
      destroy: () => Effect.die(new Error("no volume is destroyed")),
    },
    harnessDirectory: () => assert.fail("harnessDirectory"),
    ensureSession: () => Effect.die(new Error("no session is ensured")),
    readFile: () => Effect.die(new Error("no file is read")),
  };
}

/** A press of the workspace panel's button `kind`, carrying `payload`. */
const press = (kind: string, payload: Record<string, string> = {}) =>
  handleWorkspaceControlCallback({
    action: { id: "a1", kind: `workspace:${kind}`, conversationId: CONVERSATION, payload },
    callbackQueryId: `cb-${kind}`,
    chatId: 1,
    messageId: 2,
  });

test("choosing New empty workspace shows the internet dialog, then creates with the chosen mode", async () => {
  const created: (readonly [string, NetMode | undefined])[] = [];
  await withWorkspaceControls(volumesMade(created), async (alasio, { calls }, store) => {
    // Step 1: the "sessionfs" action edits the panel to a two-option internet dialog.
    await alasio.runPromise(press("sessionfs"));
    const dialog = calls.editMessageText.at(-1)?.[3]?.reply_markup;
    assert.ok(dialog);
    assert.deepEqual(labels(dialog).sort(), ["Back", "Full internet", "No internet"]);

    // Step 2: the "sessionfs_create" action with net=full creates the session filesystem and mounts it.
    await alasio.runPromise(press("sessionfs_create", { net: "full" }));
    assert.equal(created.length, 1);
    assert.equal(created[0]?.[1], "full");
    const workspace = parseWorkspace(store.getWorkingDirectory(CONVERSATION) ?? "");
    assert.equal(workspace?.kind === "sessionfs" ? workspace.volumeId : null, created[0]?.[0]);
    assert.match(calls.answerCallbackQuery.at(-1)?.[1] ?? "", /Created and mounted/);
  });
});

test("without session filesystems, a new empty workspace is declined", async () => {
  await withWorkspaceControls(undefined, async (alasio, { calls }, store) => {
    await alasio.runPromise(press("sessionfs_create", { net: "none" }));
    assert.deepEqual(calls.answerCallbackQuery.at(-1), ["cb-sessionfs_create", "Session filesystems are not enabled."]);
    assert.equal(store.getWorkingDirectory(CONVERSATION), null);
  });
});
