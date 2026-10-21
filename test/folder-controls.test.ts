/**
 * The operator's controls of a folder conversation's bayma, the sandbox gateway's folder
 * instance (src/mcp/bayma.ts): the buttons of the notice a failed bind sends, and the
 * workspace panel's, which bind it again and restart it.
 */
import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import type { CallbackQuery } from "@grammyjs/types";
import { Effect } from "effect";

import { FOLDER_RETRY, HostBayma } from "../src/mcp/bayma.ts";
import { takeCallbackQuery } from "../src/telegram/callback-handler.ts";
import { buildWorkspacePanel, handleWorkspaceControlCallback, handleWorkspaceTextCommand } from "../src/operator/workspace-control.ts";
import type { FolderScope } from "../src/sandbox/gateway-client.ts";
import { pooledStore } from "./support/pools.ts";
import { run } from "./support/store.ts";
import { recordingTelegram } from "./support/telegram-calls.ts";
import { withServices } from "./support/turns.ts";

const CONVERSATION = "telegram:1";

/** Folder instances that record what they are asked, in `asked`. */
function recordingHostBayma(asked: string[]): HostBayma["Service"] {
  const said = (verb: string) => ({ harness, threadKey }: FolderScope) => Effect.sync(() => (asked.push(`${verb} ${harness} ${threadKey}`), `${verb} of ${harness}`));
  return HostBayma.of({ ensure: () => Effect.die(new Error("no turn runs")), retry: said("retry"), restart: said("restart") });
}

/** A press of the workspace panel's button `kind`, carrying `payload`. */
const press = (kind: string, payload: Record<string, string> = {}) =>
  handleWorkspaceControlCallback({ action: { id: "a1", kind: `workspace:${kind}`, conversationId: CONVERSATION, payload }, callbackQueryId: `cb-${kind}`, chatId: 1, messageId: 2 });

test("the workspace panel offers to restart a mounted folder's instance where folder instances run, and nowhere else", () => {
  const labels = (current: string | null, folderInstances: boolean, branch: string | null = null) =>
    buildWorkspacePanel({ current, workspaceRoot: "/root", listing: { candidates: [] }, working: false, sandboxEnabled: true, folderInstances, branch }).keyboard.flat().map(({ text }) => text);
  assert.ok(labels("/root/repo", true).includes("Restart folder instance"));
  for (const [current, folderInstances, branch] of [["/root/repo", false, null], ["sessionfs:fs-abc123", true, null], [null, true, null], ["/root/repo", true, "try"]] as const) {
    assert.ok(!labels(current, folderInstances, branch).includes("Restart folder instance"), `${current} ${folderInstances} ${branch}`);
  }
});

test("a notice's Retry now and Restart folder instance act on its harness's instance, the panel's on each harness's, and are answered as they did", async () => {
  const root = mkdtempSync(join(tmpdir(), "alasio-folder-controls-"));
  const store = await pooledStore();
  try {
    mkdirSync(join(root, "repo"));
    await run(store.setActiveHarness(await run(store.upsertConversation({ chatId: "1", user: { id: 1 } })), "claude"));
    const telegram = recordingTelegram();
    const asked: string[] = [];
    await withServices({ store, telegram: telegram.layer, workspaceRoot: root, hostBayma: recordingHostBayma(asked) }, async (alasio) => {
      await alasio.runPromise(press("folder_retry", { harness: "claude" }));
      assert.deepEqual(telegram.calls.answerCallbackQuery.at(-1), ["cb-folder_retry", "Starting its bayma…"]);
      assert.equal(telegram.calls.sendMessage.at(-1)?.[1], "retry of claude");
      await alasio.runPromise(press("folder_restart", { harness: "codex" }));
      assert.equal(telegram.calls.sendMessage.at(-1)?.[1], "restart of codex");
      // The panel's, of a folder mounted: each harness's instance.
      await alasio.runPromise(handleWorkspaceTextCommand({ conversationId: CONVERSATION, chatId: 1, args: "repo" }));
      const panel = telegram.calls.sendMessage.at(-1)?.[2] as { reply_markup: { inline_keyboard: { text: string }[][] } } | undefined;
      assert.ok(panel?.reply_markup.inline_keyboard.flat().some(({ text }) => text === "Restart folder instance"));
      await alasio.runPromise(press("folder_restart"));
      assert.equal(telegram.calls.sendMessage.at(-1)?.[1], "Claude: restart of claude\nCodex: restart of codex");
    });
    assert.deepEqual(asked, ["retry claude telegram:1", "restart codex telegram:1", "restart claude telegram:1", "restart codex telegram:1"]);
    // Without folder instances, a press is answered so.
    const without = recordingTelegram();
    await withServices({ store, telegram: without.layer, workspaceRoot: root }, async (alasio) => {
      await alasio.runPromise(press("folder_retry", { harness: "claude" }));
      assert.deepEqual(without.calls.answerCallbackQuery.at(-1), ["cb-folder_retry", "This deployment runs no folder instances."]);
    });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("a notice's button acts though the conversation's session and service changed since it was sent, as a panel's does not", async () => {
  const store = await pooledStore();
  await run(store.setActiveHarness(await run(store.upsertConversation({ chatId: "1", user: { id: 1 } })), "claude"));
  await run(store.setSessionId(CONVERSATION, "session-before"));
  const [retry = "", refresh = ""] = await run(store.createCallbackActions(CONVERSATION, [{ kind: FOLDER_RETRY, payload: { harness: "claude" } }, { kind: "workspace:refresh" }]));
  // The turn it was sent before starts a session of its own; the operator switches service.
  await run(store.setSessionId(CONVERSATION, "session-after"));
  await run(store.setActiveHarness(CONVERSATION, "codex"));
  const pressOf = (id: string, data: string): CallbackQuery => ({ id, chat_instance: "instance-1", data, from: { id: 1, is_bot: false, first_name: "Operator" }, message: { message_id: 2, date: 0, chat: { id: 1, type: "private", first_name: "Operator" } } });
  const telegram = recordingTelegram();
  const asked: string[] = [];
  await withServices({ store, telegram: telegram.layer, allowedUserIds: "1", hostBayma: recordingHostBayma(asked) }, async (alasio) => {
    await alasio.runPromise(Effect.flatten(takeCallbackQuery(pressOf("cb-retry", retry))));
    await alasio.runPromise(Effect.flatten(takeCallbackQuery(pressOf("cb-refresh", refresh))));
  });
  assert.deepEqual(asked, ["retry claude telegram:1"]);
  assert.deepEqual(telegram.calls.answerCallbackQuery, [["cb-retry", "Starting its bayma…"], ["cb-refresh", "This panel is stale. Open it again."]]);
});
