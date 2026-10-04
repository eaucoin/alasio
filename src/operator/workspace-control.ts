import type { InlineKeyboardButton } from "@grammyjs/types";
import { Effect } from "effect";

import type { ConversationChat } from "../codex/turns.ts";
import { ActiveTurns } from "../harness/active-turns.ts";
import { type MountStore, resolveWorkingDirectory } from "../harness/index.ts";
import type { CallbackPayload } from "../persistence/callback-repository.ts";
import { type SqliteStore, Store } from "../persistence/store.ts";
import { TelegramClient, type TelegramError } from "../telegram/client.ts";
import {
  MAX_LISTED_WORKSPACES,
  type WorkspaceCandidate,
  listWorkspaceCandidates,
  workspaceLabel,
} from "../workspace/policy.ts";
import { Mounts, type WorkspaceChange, type WorkspaceChangeError } from "./mounts.ts";
import { type ControlCallback, type ControlPanel, closePanel, editPanel, panelOptions, sendPanel } from "./panel.ts";
import { truncateText } from "./text.ts";

/** The store's mounts and callback actions, as the workspace panel reads them. */
export type WorkspaceControlStore = MountStore & Pick<SqliteStore, "createCallbackAction">;

/** What the workspace controls run on. */
export type WorkspaceControlServices = Store | TelegramClient | ActiveTurns | Mounts;

const WORKSPACE_KIND_PREFIX = "workspace:";

const CHOOSE_WORKSPACE_NOTICE = "No folder is mounted. Choose a folder to work in, or create one; your message was not queued.";

function workspaceKind(kind: string): string {
  return `${WORKSPACE_KIND_PREFIX}${kind}`;
}

export function isWorkspaceControlAction(kind: unknown): boolean {
  return typeof kind === "string" && kind.startsWith(WORKSPACE_KIND_PREFIX);
}

function createButton(
  store: WorkspaceControlStore,
  conversationId: string,
  text: string,
  kind: string,
  payload: CallbackPayload = {},
): InlineKeyboardButton.CallbackButton {
  return {
    text,
    callback_data: store.createCallbackAction({
      conversationId,
      kind: workspaceKind(kind),
      payload,
    }),
  };
}

function pairs<T>(items: readonly T[]): T[][] {
  const rows = [];
  for (let index = 0; index < items.length; index += 2) {
    rows.push(items.slice(index, index + 2));
  }
  return rows;
}

/** The folders under the workspace root, or why they could not be listed. */
export type WorkspaceListing =
  | { readonly candidates: readonly WorkspaceCandidate[] }
  | { readonly error: string };

export interface WorkspacePanelRequest {
  readonly store: WorkspaceControlStore;
  readonly conversationId: string;
  readonly workspaceRoot: string;
  readonly listing: WorkspaceListing;
  /** Whether a turn runs in the conversation. */
  readonly working: boolean;
  readonly notice?: string | undefined;
  /** Whether to offer a new session filesystem: the deployment has a sandbox. */
  readonly sandboxEnabled?: boolean | undefined;
}

/**
 * Telegram-native folder picker. Lists top-level folders under the workspace
 * root (git repositories first) as buttons; anything beyond the button cap is
 * still reachable with `/workspace <name>`.
 */
export function buildWorkspacePanel({ store, conversationId, workspaceRoot, listing, working, notice = "", sandboxEnabled = false }: WorkspacePanelRequest): ControlPanel {
  const current = resolveWorkingDirectory(store, conversationId);
  const candidates = "candidates" in listing ? listing.candidates : [];
  const shown = candidates.slice(0, MAX_LISTED_WORKSPACES);
  const lines = [
    "Workspace",
    "",
    `Folder: ${current ?? "none"}`,
    `Status: ${working ? "working" : "idle"}`,
    `Root: ${workspaceRoot}`,
    "",
    "Sessions belong to one service and one folder. Switching folders parks the current sessions and restores the ones from the chosen folder.",
    "",
    "Type /workspace <name> to mount a folder under the root, or /workspace new <name> to create a git-initialized one.",
  ];
  if ("error" in listing) {
    lines.push("", `Could not list folders: ${truncateText(listing.error, 200)}`);
  } else if (candidates.length > shown.length) {
    lines.push("", `${candidates.length - shown.length} more folders are not shown; mount them by name.`);
  }
  if (notice) {
    lines.push("", truncateText(notice, 300));
  }
  const keyboard = pairs(shown.map((candidate) => createButton(
    store,
    conversationId,
    `${candidate.path === current ? "* " : ""}${candidate.git ? "" : "· "}${candidate.name}`,
    "use",
    { path: candidate.path },
  )));
  if (sandboxEnabled) {
    keyboard.push([createButton(store, conversationId, "New empty workspace…", "sessionfs")]);
  }
  keyboard.push([
    createButton(store, conversationId, "New folder…", "new"),
    createButton(store, conversationId, "Refresh", "refresh"),
    createButton(store, conversationId, "Close", "close"),
  ]);
  return {
    text: lines.join("\n"),
    options: panelOptions({ inline_keyboard: keyboard }),
  };
}

/** The folders under `workspaceRoot`, as the panel lists them. */
const listWorkspaces = (workspaceRoot: string): Effect.Effect<WorkspaceListing> =>
  Effect.tryPromise(() => listWorkspaceCandidates(workspaceRoot)).pipe(
    Effect.match({
      onFailure: ({ cause }) => ({ error: cause instanceof Error ? cause.message : String(cause) }),
      onSuccess: (candidates) => ({ candidates }),
    }),
  );

/** The conversation's workspace panel as it stands, with `notice` under it. */
const workspacePanel = Effect.fnUntraced(function*(conversationId: string, notice = ""): Effect.fn.Return<ControlPanel, never, Store | ActiveTurns | Mounts> {
  const mounts = yield* Mounts;
  const working = yield* Effect.flatMap(ActiveTurns, (activeTurns) => activeTurns.isBusy(conversationId));
  return buildWorkspacePanel({
    store: yield* Store,
    conversationId,
    workspaceRoot: mounts.workspaceRoot,
    listing: yield* listWorkspaces(mounts.workspaceRoot),
    working,
    notice,
    sandboxEnabled: mounts.sessionFilesystems,
  });
});

/**
 * Reply used whenever a prompt or control arrives before a folder is mounted.
 */
export const sendChooseWorkspacePanel = ({ conversationId, chatId }: ConversationChat): Effect.Effect<void, TelegramError, WorkspaceControlServices> =>
  Effect.flatMap(workspacePanel(conversationId, CHOOSE_WORKSPACE_NOTICE), (panel) => sendPanel(chatId, panel));

function describeOutcome(result: WorkspaceChange): string {
  if (result.created) {
    return `Created and mounted ${workspaceLabel(result.workingDirectory)} (${result.workingDirectory}). Send a message to start.`;
  }
  if (!result.switched) {
    return `${workspaceLabel(result.workingDirectory)} is already mounted.`;
  }
  return result.previous
    ? `Switched to ${workspaceLabel(result.workingDirectory)} (${result.workingDirectory}).`
    : `Mounted ${workspaceLabel(result.workingDirectory)} (${result.workingDirectory}). Send a message to start.`;
}

/** What to tell the operator of a change of folder: what it did, or why it did not. */
const applyWorkspaceChange = <R>(change: Effect.Effect<WorkspaceChange, WorkspaceChangeError, R>): Effect.Effect<string, never, R> =>
  Effect.match(change, { onFailure: (error) => error.message, onSuccess: describeOutcome });

/** What /workspace was asked to do: show the panel, mount a folder, or create one. */
type WorkspaceArgs =
  | { readonly action: "panel" }
  | { readonly action: "create"; readonly name: string }
  | { readonly action: "use"; readonly target: string };

function parseWorkspaceArgs(args: string | null | undefined): WorkspaceArgs {
  const trimmed = String(args ?? "").trim();
  if (!trimmed) {
    return { action: "panel" };
  }
  const create = /^new\s+(\S+)\s*$/i.exec(trimmed);
  if (create) {
    // The group is not optional, so every match fills it and the `?? ""` never applies.
    return { action: "create", name: create[1] ?? "" };
  }
  return { action: "use", target: trimmed };
}

export interface WorkspaceTextCommand extends ConversationChat {
  /** What followed /workspace. */
  readonly args: string;
}

export const handleWorkspaceTextCommand = Effect.fnUntraced(function*({ conversationId, chatId, args }: WorkspaceTextCommand): Effect.fn.Return<
  void,
  TelegramError,
  WorkspaceControlServices
> {
  const mounts = yield* Mounts;
  const parsed = parseWorkspaceArgs(args);
  let notice = "";
  if (parsed.action === "use") {
    notice = yield* applyWorkspaceChange(mounts.switchWorkspace(conversationId, parsed.target));
  } else if (parsed.action === "create") {
    notice = yield* applyWorkspaceChange(mounts.createWorkspace(conversationId, parsed.name));
  }
  yield* sendPanel(chatId, yield* workspacePanel(conversationId, notice));
});

export const handleWorkspaceControlCallback = Effect.fnUntraced(function*({ action, callbackQueryId, chatId, messageId }: ControlCallback): Effect.fn.Return<
  void,
  TelegramError,
  WorkspaceControlServices
> {
  const store = yield* Store;
  const client = yield* TelegramClient;
  const mounts = yield* Mounts;
  const kind = action.kind.slice(WORKSPACE_KIND_PREFIX.length);
  const payload = action.payload ?? {};
  const { conversationId } = action;
  if (kind === "sessionfs") {
    // Offer the internet choice before creating the empty workspace.
    yield* client.answerCallbackQuery(callbackQueryId, "Choose internet access.");
    yield* editPanel(chatId, messageId, {
      text: [
        "New empty workspace",
        "",
        "An isolated, empty filesystem in a sandbox that sees nothing of the host.",
        "Choose its internet access:",
        "",
        "· No internet — only the model is reachable.",
        "· Full internet — the public internet is reachable (never the host or other sessions).",
      ].join("\n"),
      options: panelOptions({ inline_keyboard: [[
        createButton(store, conversationId, "No internet", "sessionfs_create", { net: "none" }),
        createButton(store, conversationId, "Full internet", "sessionfs_create", { net: "full" }),
      ], [createButton(store, conversationId, "Back", "refresh")]] }),
    });
    return;
  }
  if (kind === "sessionfs_create") {
    if (!mounts.sessionFilesystems) {
      yield* client.answerCallbackQuery(callbackQueryId, "Session filesystems are not enabled.");
      return;
    }
    const notice = yield* applyWorkspaceChange(mounts.createSessionWorkspace(conversationId, payload["net"] === "full" ? "full" : "none"));
    yield* client.answerCallbackQuery(callbackQueryId, truncateText(notice, 180));
    yield* editPanel(chatId, messageId, yield* workspacePanel(conversationId, notice));
    return;
  }
  if (kind === "close") {
    return yield* closePanel({ callbackQueryId, chatId, messageId });
  }
  if (kind === "new") {
    yield* client.answerCallbackQuery(callbackQueryId, "Send /workspace new <name>");
    yield* client.sendMessage(chatId, "Send /workspace new <name> to create a git-initialized folder under the workspace root.");
    return;
  }
  if (kind === "refresh") {
    yield* client.answerCallbackQuery(callbackQueryId, "Refreshed.");
    yield* editPanel(chatId, messageId, yield* workspacePanel(conversationId));
    return;
  }
  if (kind === "use") {
    const path = payload["path"];
    if (typeof path !== "string" || !path) {
      yield* client.answerCallbackQuery(callbackQueryId, "Unknown folder.");
      return;
    }
    const notice = yield* applyWorkspaceChange(mounts.switchWorkspace(conversationId, path));
    yield* client.answerCallbackQuery(callbackQueryId, truncateText(notice, 180));
    yield* editPanel(chatId, messageId, yield* workspacePanel(conversationId, notice));
    return;
  }
  yield* client.answerCallbackQuery(callbackQueryId, "Unknown action.");
});
