import type { InlineKeyboardButton } from "@grammyjs/types";
import type { ActiveTurnsFacade } from "../harness/active-turns.ts";
import { type MountStore, resolveWorkingDirectory } from "../harness/index.ts";
import type { CallbackPayload } from "../persistence/callback-repository.ts";
import type { SqliteStore } from "../persistence/store.ts";
import type { NetMode } from "../sandbox/index.ts";
import type { ChatId, Client } from "../telegram/client.ts";
import {
  MAX_LISTED_WORKSPACES,
  type WorkspaceCandidate,
  WorkspaceError,
  listWorkspaceCandidates,
  workspaceLabel,
} from "../workspace/policy.ts";
import type { ControlCallback, ControlPanel } from "./session-control.ts";
import { truncateText } from "./text.ts";

/** The outcome of mounting a folder on a conversation, or of creating one and mounting it. */
export interface WorkspaceChange {
  /** Whether the folder changed; false when it was already the mounted one. */
  readonly switched: boolean;
  /** Set when the folder was newly created. */
  readonly created?: boolean | undefined;
  readonly previous: string | null;
  readonly workingDirectory: string;
}

/** Mounts the folder `target` names under the workspace root, or throws why it cannot. */
export type SwitchWorkspace = (request: { readonly conversationId: string; readonly target: string }) => Promise<WorkspaceChange>;

/** Creates a git-initialized folder `name` under the workspace root and mounts it, or throws why it cannot. */
export type CreateWorkspace = (request: { readonly conversationId: string; readonly name: string }) => Promise<WorkspaceChange>;

/** Creates an empty session filesystem with internet access `netMode` and mounts it, or throws why it cannot. */
export type CreateSessionWorkspace = (request: { readonly conversationId: string; readonly netMode: NetMode }) => Promise<WorkspaceChange>;

/** The store's mounts and callback actions, as the workspace panel reads them. */
export type WorkspaceControlStore = MountStore & Pick<SqliteStore, "createCallbackAction">;

const WORKSPACE_KIND_PREFIX = "workspace:";

export const CHOOSE_WORKSPACE_NOTICE = "No folder is mounted. Choose a folder to work in, or create one; your message was not queued.";

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

export interface WorkspacePanelRequest {
  readonly store: WorkspaceControlStore;
  readonly activeTurns?: ActiveTurnsFacade | null | undefined;
  readonly conversationId: string;
  readonly workspaceRoot: string;
  readonly notice?: string | undefined;
  /** Whether to offer a new session filesystem: the deployment has a sandbox. */
  readonly sandboxEnabled?: boolean | undefined;
}

/**
 * Telegram-native folder picker. Lists top-level folders under the workspace
 * root (git repositories first) as buttons; anything beyond the button cap is
 * still reachable with `/workspace <name>`.
 */
export async function buildWorkspacePanel({ store, activeTurns, conversationId, workspaceRoot, notice = "", sandboxEnabled = false }: WorkspacePanelRequest): Promise<ControlPanel> {
  const current = resolveWorkingDirectory(store, conversationId);
  const working = activeTurns?.isBusy(conversationId) ?? false;
  let candidates: WorkspaceCandidate[] = [];
  let listingError = null;
  try {
    candidates = await listWorkspaceCandidates(workspaceRoot);
  } catch (error) {
    listingError = error instanceof Error ? error.message : String(error);
  }
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
  if (listingError) {
    lines.push("", `Could not list folders: ${truncateText(listingError, 200)}`);
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
    options: { format: "plain", reply_markup: { inline_keyboard: keyboard } },
  };
}

export interface SendWorkspacePanelRequest extends Omit<WorkspacePanelRequest, "notice"> {
  readonly client: Pick<Client, "sendMessage">;
  readonly chatId: ChatId;
}

/**
 * Reply used whenever a prompt or control arrives before a folder is mounted.
 */
export async function sendChooseWorkspacePanel({ client, store, activeTurns, conversationId, chatId, workspaceRoot, sandboxEnabled = false }: SendWorkspacePanelRequest): Promise<void> {
  const panel = await buildWorkspacePanel({ store, activeTurns, conversationId, workspaceRoot, notice: CHOOSE_WORKSPACE_NOTICE, sandboxEnabled });
  await client.sendMessage(chatId, panel.text, panel.options);
}

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

async function applyWorkspaceChange(run: () => Promise<WorkspaceChange>): Promise<string> {
  try {
    return describeOutcome(await run());
  } catch (error) {
    if (error instanceof WorkspaceError) {
      return error.message;
    }
    return error instanceof Error ? error.message : String(error);
  }
}

/** What /workspace was asked to do: show the panel, mount a folder, or create one. */
export type WorkspaceArgs =
  | { readonly action: "panel" }
  | { readonly action: "create"; readonly name: string }
  | { readonly action: "use"; readonly target: string };

export function parseWorkspaceArgs(args: string | null | undefined): WorkspaceArgs {
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

export interface WorkspaceTextCommand extends SendWorkspacePanelRequest {
  /** What followed /workspace. */
  readonly args: string;
  readonly switchWorkspace: SwitchWorkspace;
  readonly createWorkspace: CreateWorkspace;
}

export async function handleWorkspaceTextCommand({
  client,
  store,
  activeTurns,
  conversationId,
  chatId,
  args,
  workspaceRoot,
  switchWorkspace,
  createWorkspace,
  sandboxEnabled = false,
}: WorkspaceTextCommand): Promise<void> {
  const parsed = parseWorkspaceArgs(args);
  let notice = "";
  if (parsed.action === "use") {
    notice = await applyWorkspaceChange(() => switchWorkspace({ conversationId, target: parsed.target }));
  } else if (parsed.action === "create") {
    notice = await applyWorkspaceChange(() => createWorkspace({ conversationId, name: parsed.name }));
  }
  const panel = await buildWorkspacePanel({ store, activeTurns, conversationId, workspaceRoot, notice, sandboxEnabled });
  await client.sendMessage(chatId, panel.text, panel.options);
}

async function editPanel(client: Pick<Client, "editMessageText">, chatId: ChatId, messageId: number, panel: ControlPanel): Promise<void> {
  try {
    await client.editMessageText(chatId, messageId, panel.text, panel.options);
  } catch (error) {
    if (!String(error).includes("message is not modified")) {
      throw error;
    }
  }
}

export interface WorkspaceControlCallback extends ControlCallback {
  readonly client: Pick<Client, "answerCallbackQuery" | "editMessageText" | "deleteMessage" | "sendMessage">;
  readonly store: WorkspaceControlStore;
  readonly activeTurns?: ActiveTurnsFacade | null | undefined;
  readonly workspaceRoot: string;
  readonly switchWorkspace: SwitchWorkspace;
  /** Present when the deployment offers session filesystems. */
  readonly createSessionWorkspace?: CreateSessionWorkspace | null | undefined;
  readonly sandboxEnabled?: boolean | undefined;
}

export async function handleWorkspaceControlCallback({
  client,
  store,
  activeTurns,
  action,
  workspaceRoot,
  switchWorkspace,
  createSessionWorkspace = null,
  sandboxEnabled = false,
  callbackQueryId,
  chatId,
  messageId,
}: WorkspaceControlCallback): Promise<void> {
  const kind = action.kind.slice(WORKSPACE_KIND_PREFIX.length);
  const payload = action.payload ?? {};
  const conversationId = action.conversationId;
  const panel = (notice: string) => buildWorkspacePanel({ store, activeTurns, conversationId, workspaceRoot, notice, sandboxEnabled });
  if (kind === "sessionfs") {
    // Offer the internet choice before creating the empty workspace.
    await client.answerCallbackQuery(callbackQueryId, "Choose internet access.");
    await editPanel(client, chatId, messageId, {
      text: [
        "New empty workspace",
        "",
        "An isolated, empty filesystem in a sandbox that sees nothing of the host.",
        "Choose its internet access:",
        "",
        "· No internet — only the model is reachable.",
        "· Full internet — the public internet is reachable (never the host or other sessions).",
      ].join("\n"),
      options: { format: "plain", reply_markup: { inline_keyboard: [[
        createButton(store, conversationId, "No internet", "sessionfs_create", { net: "none" }),
        createButton(store, conversationId, "Full internet", "sessionfs_create", { net: "full" }),
      ], [createButton(store, conversationId, "Back", "refresh")]] } },
    });
    return;
  }
  if (kind === "sessionfs_create") {
    if (!createSessionWorkspace) {
      await client.answerCallbackQuery(callbackQueryId, "Session filesystems are not enabled.");
      return;
    }
    const notice = await applyWorkspaceChange(() => createSessionWorkspace({ conversationId, netMode: payload["net"] === "full" ? "full" : "none" }));
    await client.answerCallbackQuery(callbackQueryId, truncateText(notice, 180));
    await editPanel(client, chatId, messageId, await panel(notice));
    return;
  }
  if (kind === "close") {
    await client.answerCallbackQuery(callbackQueryId, "Closed.");
    try {
      await client.deleteMessage(chatId, messageId);
    } catch {
      await client.editMessageText(chatId, messageId, "Closed.", { format: "plain" });
    }
    return;
  }
  if (kind === "new") {
    await client.answerCallbackQuery(callbackQueryId, "Send /workspace new <name>");
    await client.sendMessage(chatId, "Send /workspace new <name> to create a git-initialized folder under the workspace root.");
    return;
  }
  if (kind === "refresh") {
    await client.answerCallbackQuery(callbackQueryId, "Refreshed.");
    await editPanel(client, chatId, messageId, await panel(""));
    return;
  }
  if (kind === "use") {
    const path = payload["path"];
    if (typeof path !== "string" || !path) {
      await client.answerCallbackQuery(callbackQueryId, "Unknown folder.");
      return;
    }
    const notice = await applyWorkspaceChange(() => switchWorkspace({ conversationId, target: path }));
    await client.answerCallbackQuery(callbackQueryId, truncateText(notice, 180));
    await editPanel(client, chatId, messageId, await panel(notice));
    return;
  }
  await client.answerCallbackQuery(callbackQueryId, "Unknown action.");
}
