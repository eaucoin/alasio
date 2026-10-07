import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import type { InlineKeyboardMarkup } from "@grammyjs/types";
import { Effect } from "effect";

import { ActiveTurns } from "../src/harness/active-turns.ts";
import { Mounts } from "../src/operator/mounts.ts";
import { buildWorkspacePanel, handleWorkspaceControlCallback, handleWorkspaceTextCommand } from "../src/operator/workspace-control.ts";
import type { Store } from "../src/persistence/store.ts";
import { type NetMode, SessionForkError, type SessionSandboxes } from "../src/sandbox/index.ts";
import { parseWorkspace } from "../src/workspace/kind.ts";
import { run, testStore } from "./support/store.ts";
import { type RecordingTelegram, recordingTelegram } from "./support/telegram-calls.ts";
import { type TestAlasio, withServices } from "./support/turns.ts";
import { eventually } from "./support/wait.ts";

// Buttons in a panel/edit, flattened to their labels.
const labels = (markup: InlineKeyboardMarkup) => markup.inline_keyboard.flat().map((b) => b.text);

test("the workspace panel offers New empty workspace only when session filesystems are enabled", () => {
  const panel = (sandboxEnabled: boolean) =>
    buildWorkspacePanel({ current: null, workspaceRoot: "/root", listing: { candidates: [] }, working: false, sandboxEnabled }).keyboard.flat().map((button) => button.text);
  assert.ok(!panel(false).includes("New empty workspace…"));
  assert.ok(panel(true).includes("New empty workspace…"));
});

test("the workspace panel lists the session workspaces made by what they are, and offers to fork the one mounted, saying what a fork is", () => {
  const sessionWorkspaces = [
    { volumeId: "fs-def456", netMode: "none", forkedFrom: "fs-abc123", madeAt: new Date(2) },
    { volumeId: "fs-abc123", netMode: "full", forkedFrom: null, madeAt: new Date(1) },
  ] as const;
  const panel = (current: string | null) =>
    buildWorkspacePanel({ current, workspaceRoot: "/root", listing: { candidates: [{ name: "repo", path: "/root/repo", git: true }] }, working: false, sandboxEnabled: true, sessionWorkspaces });
  const forked = panel("sessionfs:fs-def456");
  assert.match(forked.text, /^Session workspace: fs-def456, no internet, fork of fs-abc123$/mu);
  assert.match(forked.text, /^Fork clones this session workspace/mu);
  assert.deepEqual(forked.keyboard.flat().map(({ text, payload }) => [text, payload?.["path"]]), [
    ["repo", "/root/repo"],
    ["* fs-def456 (fork of fs-abc123)", "sessionfs:fs-def456"],
    ["fs-abc123", "sessionfs:fs-abc123"],
    ["Fork this workspace", undefined],
    ["New empty workspace…", undefined],
    ["New folder…", undefined],
    ["Refresh", undefined],
    ["Close", undefined],
  ]);
  const folder = panel("/root/repo");
  assert.match(folder.text, /^Folder: \/root\/repo$/mu);
  assert.doesNotMatch(folder.text, /Fork clones/u);
  assert.ok(!folder.keyboard.flat().some(({ text }) => text === "Fork this workspace"));
});

/** The conversation of chat 1, on Codex. */
const CONVERSATION = "telegram:1";

/** The workspace controls' services over a conversation on Codex, with `sandbox` offering session filesystems when given. */
async function withWorkspaceControls(
  sandbox: SessionSandboxes["Service"] | undefined,
  use: (alasio: TestAlasio, telegram: RecordingTelegram, store: Store["Service"], root: string) => Promise<void>,
): Promise<void> {
  const root = mkdtempSync(join(tmpdir(), "alasio-workspace-sessionfs-"));
  const store = await testStore();
  try {
    await run(store.setActiveHarness(await run(store.upsertConversation({ chatId: "1", user: { id: 1 } })), "codex"));
    const telegram = recordingTelegram();
    await withServices({ store, telegram: telegram.layer, workspaceRoot: root, sandbox }, (alasio) => use(alasio, telegram, store, root));
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}

/** What the fake session filesystems were asked: volumes made, forked and destroyed. */
interface VolumeCalls {
  readonly created: (readonly [string, NetMode | undefined])[];
  readonly forked: (readonly [string, string])[];
  readonly destroyed: string[];
}

/**
 * Session filesystems that make, fork and destroy the volumes asked for, recording each;
 * a fork fails when `forkFails`, and lasts until `forking` ends; the forks' Sandboxes are `forks`.
 */
function fakeVolumes({ forkFails = false, forking = Effect.void, forks = [] }: {
  readonly forkFails?: boolean;
  readonly forking?: Effect.Effect<void>;
  readonly forks?: readonly string[];
} = {}): SessionSandboxes["Service"] & VolumeCalls {
  const calls: VolumeCalls = { created: [], forked: [], destroyed: [] };
  return {
    ...calls,
    volumes: {
      create: (volumeId, netMode = "none") => Effect.sync(() => {
        calls.created.push([volumeId, netMode]);
        return { volumeId, netMode };
      }),
      fork: (sourceVolumeId, volumeId) =>
        Effect.suspend(() => {
          calls.forked.push([sourceVolumeId, volumeId]);
          return Effect.andThen(forking, forkFails ? Effect.fail(new SessionForkError({ message: "the clone failed" })) : Effect.succeed({ volumeId, netMode: "full" as const }));
        }),
      destroy: (volumeId) => Effect.sync(() => void calls.destroyed.push(volumeId)),
      forks: Effect.succeed(forks),
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

/** The volume of the session workspace the conversation has mounted, if it has one. */
const mountedVolume = async (store: Store["Service"]) => {
  const workspace = parseWorkspace((await run(store.getMount(CONVERSATION))).workingDirectory);
  return workspace?.kind === "sessionfs" ? workspace.volumeId : null;
};

test("choosing New empty workspace shows the internet dialog, then creates with the chosen mode, recorded as made", async () => {
  const sandbox = fakeVolumes();
  await withWorkspaceControls(sandbox, async (alasio, { calls }, store) => {
    // Step 1: the "sessionfs" action edits the panel to a two-option internet dialog.
    await alasio.runPromise(press("sessionfs"));
    const dialog = calls.editMessageText.at(-1)?.[3]?.reply_markup;
    assert.ok(dialog);
    assert.deepEqual(labels(dialog).sort(), ["Back", "Full internet", "No internet"]);

    // Step 2: the "sessionfs_create" action with net=full creates the session filesystem and mounts it.
    await alasio.runPromise(press("sessionfs_create", { net: "full" }));
    assert.equal(sandbox.created.length, 1);
    const [[volumeId, netMode] = []] = sandbox.created;
    assert.equal(netMode, "full");
    assert.equal(await mountedVolume(store), volumeId);
    assert.equal(calls.answerCallbackQuery.at(-1)?.[1], `Created and mounted session workspace ${volumeId} (sessionfs:${volumeId}). Send a message to start.`);
    assert.deepEqual((await run(store.listSessionWorkspaces)).map(({ volumeId, netMode, forkedFrom, madeAt }) => [volumeId, netMode, forkedFrom, madeAt instanceof Date]), [
      [volumeId, "full", null, true],
    ]);
  });
});

test("without session filesystems, a new empty workspace is declined", async () => {
  await withWorkspaceControls(undefined, async (alasio, { calls }, store) => {
    await alasio.runPromise(press("sessionfs_create", { net: "none" }));
    assert.deepEqual(calls.answerCallbackQuery.at(-1), ["cb-sessionfs_create", "Session filesystems are not enabled."]);
    assert.equal((await run(store.getMount(CONVERSATION))).workingDirectory, null);
  });
});

test("Fork clones the session workspace mounted and switches the conversation to the fork, with a fresh session, the source's kept to switch back to", async () => {
  const sandbox = fakeVolumes();
  await withWorkspaceControls(sandbox, async (alasio, { calls }, store) => {
    await alasio.runPromise(press("sessionfs_create", { net: "full" }));
    const source = await mountedVolume(store);
    assert.ok(source);
    await run(store.setSessionId(CONVERSATION, "thread-of-source"));

    await alasio.runPromise(press("fork"));
    const [[forkedFrom, fork] = []] = sandbox.forked;
    assert.equal(forkedFrom, source);
    assert.equal(await mountedVolume(store), fork);
    assert.equal((await run(store.getMount(CONVERSATION))).sessionId, null);
    assert.deepEqual(calls.answerCallbackQuery.at(-1), ["cb-fork", "Forking this workspace…"]);
    const told = `Forked session workspace ${source} into session workspace ${fork} and switched to it, with a fresh session. ${source} is left as it was.`;
    assert.equal(calls.sendMessage.at(-1)?.[1], told);
    const panel = calls.editMessageText.at(-1);
    assert.match(panel?.[2] ?? "", new RegExp(`^Session workspace: ${fork}, full internet, fork of ${source}$`, "mu"));
    assert.deepEqual((await run(store.listSessionWorkspaces)).map(({ volumeId, forkedFrom }) => [volumeId, forkedFrom]), [[fork, source], [source, null]]);

    // The source is the operator's to switch back to, with its session.
    await alasio.runPromise(press("use", { path: `sessionfs:${source}` }));
    assert.equal(await mountedVolume(store), source);
    assert.equal((await run(store.getMount(CONVERSATION))).sessionId, "thread-of-source");
  });
});

test("a folder is not forked, nor a workspace while prompts wait in its conversation", async () => {
  const sandbox = fakeVolumes();
  await withWorkspaceControls(sandbox, async (alasio, { calls }, store, root) => {
    await alasio.runPromise(handleWorkspaceTextCommand({ conversationId: CONVERSATION, chatId: 1, args: "new repo" }));
    await alasio.runPromise(press("fork"));
    assert.equal(calls.sendMessage.at(-1)?.[1], "Only a session workspace can be forked: a folder's files are the host's, which are not copied on write.");
    assert.equal((await run(store.getMount(CONVERSATION))).workingDirectory, join(root, "repo"));

    await alasio.runPromise(press("sessionfs_create", { net: "none" }));
    const source = await mountedVolume(store);
    await run(store.enqueuePromptJob({ conversationId: CONVERSATION, chatId: 1, messageId: 10, prompt: "queued" }));
    await alasio.runPromise(press("fork"));
    assert.equal(
      calls.sendMessage.at(-1)?.[1],
      "Queued prompts are still waiting for the current service. Let them finish or discard them before forking its workspace.",
    );
    assert.deepEqual(sandbox.forked, []);
    assert.equal(await mountedVolume(store), source);
    assert.equal((await run(store.listSessionWorkspaces)).length, 1);
  });
});

test("a fork that fails is not recorded, leaves its source mounted, and says why", async () => {
  const sandbox = fakeVolumes({ forkFails: true });
  await withWorkspaceControls(sandbox, async (alasio, { calls }, store) => {
    await alasio.runPromise(press("sessionfs_create", { net: "none" }));
    const source = await mountedVolume(store);
    await alasio.runPromise(press("fork"));
    const [[, fork] = []] = sandbox.forked;
    assert.equal(calls.sendMessage.at(-1)?.[1], "the clone failed");
    assert.deepEqual(sandbox.destroyed, [fork]);
    assert.equal(await mountedVolume(store), source);
    assert.deepEqual((await run(store.listSessionWorkspaces)).map(({ volumeId }) => volumeId), [source]);
  });
});

test("only a session workspace alasio made is switched to", async () => {
  await withWorkspaceControls(fakeVolumes(), async (alasio, { calls }, store) => {
    await alasio.runPromise(handleWorkspaceTextCommand({ conversationId: CONVERSATION, chatId: 1, args: "sessionfs:fs-unknown" }));
    assert.match(calls.sendMessage.at(-1)?.[1] ?? "", /^There is no session workspace fs-unknown\.$/mu);
    assert.equal((await run(store.getMount(CONVERSATION))).workingDirectory, null);
  });
});

test("as alasio starts, the session workspaces a crash left unmade go: those recorded unmade, and forks no record made", async () => {
  const sandbox = fakeVolumes({ forks: ["fs-made", "fs-unmade", "fs-orphan"] });
  await withWorkspaceControls(sandbox, async (alasio, _telegram, store) => {
    await run(store.recordSessionWorkspace({ volumeId: "fs-made", forkedFrom: "fs-source" }));
    await run(store.markSessionWorkspaceMade("fs-made", "none"));
    await run(store.recordSessionWorkspace({ volumeId: "fs-unmade", forkedFrom: "fs-source" }));
    await alasio.runPromise(Effect.flatMap(Mounts, (mounts) => mounts.reconcileSessionWorkspaces));
    assert.deepEqual(sandbox.destroyed, ["fs-unmade", "fs-orphan"]);
    assert.deepEqual((await run(store.listSessionWorkspaces)).map(({ volumeId }) => volumeId), ["fs-made"]);
  });
});

test("Fork holds every conversation on the workspace busy while it clones, and is refused while a turn runs in one", async () => {
  const forking = Promise.withResolvers<void>();
  const forkEnds = Promise.withResolvers<void>();
  // Forks last until the test ends them once it holds them.
  let held = false;
  const sandbox = fakeVolumes({ forking: Effect.suspend(() => (held ? Effect.andThen(Effect.sync(() => forking.resolve()), Effect.promise(() => forkEnds.promise)) : Effect.void)) });
  await withWorkspaceControls(sandbox, async (alasio, { calls }, store) => {
    await alasio.runPromise(press("sessionfs_create", { net: "none" }));
    const source = await mountedVolume(store);
    const other = await run(store.upsertConversation({ chatId: "2", user: { id: 2 } }));
    await run(store.setWorkingDirectory(other, `sessionfs:${source}`));
    const busy = (conversationId: string) => alasio.runPromise(Effect.flatMap(ActiveTurns, (active) => active.isBusy(conversationId)));

    // A turn in another conversation on the workspace, which the fork would suspend.
    const turnEnds = Promise.withResolvers<void>();
    const turn = alasio.runPromise(Effect.scoped(Effect.gen(function*() {
      yield* (yield* ActiveTurns).register(other, { stop: () => Effect.void, steer: () => Effect.succeed(false), cliInitiated: false });
      yield* Effect.promise(() => turnEnds.promise);
    })));
    await eventually("the other conversation's turn to run", async () => (await busy(other)) || undefined);
    await alasio.runPromise(press("fork"));
    assert.equal(calls.sendMessage.at(-1)?.[1], "A turn runs on this session workspace now, which a fork would suspend. Fork it once that turn ends.");
    assert.deepEqual(sandbox.forked, []);
    turnEnds.resolve();
    await turn;

    // While it clones, no turn starts in either.
    held = true;
    const pressed = alasio.runPromise(press("fork"));
    await forking.promise;
    try {
      assert.deepEqual([await busy(CONVERSATION), await busy(other)], [true, true]);
    } finally {
      forkEnds.resolve();
    }
    await pressed;
    assert.deepEqual([await busy(CONVERSATION), await busy(other)], [false, false]);
    assert.equal(sandbox.forked.length, 1);
  });
});
