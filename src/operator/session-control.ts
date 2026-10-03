import type { InlineKeyboardButton, InlineKeyboardMarkup } from "@grammyjs/types";
import type { ListedSession } from "../harness/claude/sessions.ts";
import { type ActiveTurnsFacade, noActiveTurns } from "../harness/active-turns.ts";
import type { HarnessFacade } from "../harness/index.ts";
import type { CallbackAction, CallbackPayload } from "../persistence/callback-repository.ts";
import type { SqliteStore } from "../persistence/store.ts";
import { SESSIONS_PER_PAGE } from "../shared/runtime-constants.ts";
import type { ChatId, Client, TextMessageOptions } from "../telegram/client.ts";
import { truncateText } from "./text.ts";

/** How an operator panel is sent or edited: as plain text, under its inline keyboard. */
export interface ControlPanelOptions extends TextMessageOptions {
  readonly format: "plain";
  readonly reply_markup: InlineKeyboardMarkup;
}

/** An operator panel: a message and the buttons under it. */
export interface ControlPanel {
  readonly text: string;
  readonly options: ControlPanelOptions;
}

/** A pressed panel button, as the callback handler hands it to a control. */
export interface ControlCallback {
  readonly action: CallbackAction;
  readonly callbackQueryId: string;
  readonly chatId: ChatId;
  readonly messageId: number;
}

/** Starts and mounts a new session for the conversation; resolves to its id. */
export type StartNewSession = (request: { readonly conversationId: string }) => Promise<string>;

/** The store's sessions and callback actions, as the session panels read and change them. */
export type SessionControlStore = Pick<SqliteStore, "getSessionId" | "setSessionId" | "getSessionTokens" | "createCallbackAction">;

/** The mounted harness, as the session panels list, rewind, and resume its sessions. */
export type SessionControlHarness = Pick<HarnessFacade, "displayName" | "sessions">;

const CONTROL_KIND_PREFIX = "control:";

function controlKind(kind: string): string {
  return `${CONTROL_KIND_PREFIX}${kind}`;
}

export function isSessionControlAction(kind: unknown): boolean {
  return typeof kind === "string" && kind.startsWith(CONTROL_KIND_PREFIX);
}

// A page arrives from a button's payload, which has been through JSON, so any value
// is read leniently.
function normalizePage(page: unknown, totalPages: number): number {
  const parsed = Number.parseInt(String(page ?? 1), 10);
  const safePage = Number.isFinite(parsed) && parsed > 0 ? parsed : 1;
  return Math.min(Math.max(safePage, 1), totalPages);
}

function shortSessionId(sessionId: string | null | undefined): string {
  return sessionId ? sessionId.slice(0, 8) : "-";
}

function createButton(
  store: Pick<SqliteStore, "createCallbackAction">,
  conversationId: string,
  text: string,
  kind: string,
  payload: CallbackPayload = {},
): InlineKeyboardButton.CallbackButton {
  return {
    text,
    callback_data: store.createCallbackAction({
      conversationId,
      kind: controlKind(kind),
      payload,
    }),
  };
}

function buildPanelOptions(replyMarkup: InlineKeyboardMarkup): ControlPanelOptions {
  return {
    format: "plain",
    reply_markup: replyMarkup,
  };
}

function closeRow(store: Pick<SqliteStore, "createCallbackAction">, conversationId: string): InlineKeyboardButton.CallbackButton[] {
  return [createButton(store, conversationId, "Close", "close")];
}

function stringField(payload: CallbackPayload, key: string): string | undefined {
  const value = payload[key];
  return typeof value === "string" ? value : undefined;
}

function numberField(payload: CallbackPayload, key: string): number | undefined {
  const value = payload[key];
  return typeof value === "number" ? value : undefined;
}

function describeSession(session: ListedSession, mountedSessionId: string | undefined): string {
  const marker = session.uuid === mountedSessionId ? "* " : "";
  return `${marker}${session.timestamp || "-"} - ${session.label || shortSessionId(session.uuid)}`;
}

async function buildMountedSummary(store: SessionControlStore, harness: SessionControlHarness, conversationId: string): Promise<string[]> {
  const sessionId = store.getSessionId(conversationId);
  if (!sessionId) {
    return ["Mounted", `No mounted ${harness.displayName} session.`];
  }
  const lastMessage = await harness.sessions.getSessionLastMessage(sessionId);
  const tokens = store.getSessionTokens(sessionId);
  return [
    "Mounted",
    `${shortSessionId(sessionId)}${tokens ? ` - cache read ${tokens} tokens` : ""}`,
    lastMessage ? truncateText(lastMessage, 260) : "No assistant message found yet.",
  ];
}

export interface SessionsPanelRequest {
  readonly store: SessionControlStore;
  readonly harness: SessionControlHarness;
  readonly conversationId: string;
  /** The page to show; normalized into range, so a button's payload is passed as it is. */
  readonly page?: unknown;
}

export async function buildSessionsPanel({ store, harness, conversationId, page = 1 }: SessionsPanelRequest): Promise<ControlPanel> {
  const mountedSessionId = store.getSessionId(conversationId);
  const totalPages = await harness.sessions.getTotalSessionPages();
  const safePage = normalizePage(page, totalPages);
  const sessions = await harness.sessions.listSessions(safePage);
  const startNumber = (safePage - 1) * SESSIONS_PER_PAGE + 1;
  const lines = [
    `Sessions (${harness.displayName})`,
    "",
    ...(await buildMountedSummary(store, harness, conversationId)),
    "",
    `Recent sessions (page ${safePage}/${totalPages})`,
  ];
  if (sessions.length === 0) {
    lines.push("No sessions found.");
  } else {
    for (const [index, session] of sessions.entries()) {
      lines.push(`${startNumber + index}. ${describeSession(session, mountedSessionId)}`);
    }
  }

  const keyboard = [
    [
      createButton(store, conversationId, "Current Session", "current"),
      createButton(store, conversationId, "New Session", "new"),
    ],
  ];
  for (const [index, session] of sessions.entries()) {
    keyboard.push([
      createButton(
        store,
        conversationId,
        `${startNumber + index}. ${truncateText(describeSession(session, mountedSessionId), 52)}`,
        "preview",
        { sessionId: session.uuid, page: safePage },
      ),
    ]);
  }
  if (totalPages > 1) {
    keyboard.push([
      createButton(store, conversationId, "Prev", "sessions", { page: safePage - 1 }),
      createButton(store, conversationId, "Next", "sessions", { page: safePage + 1 }),
    ]);
  }
  keyboard.push(closeRow(store, conversationId));

  return {
    text: lines.join("\n"),
    options: buildPanelOptions({ inline_keyboard: keyboard }),
  };
}

export interface CurrentSessionPanelRequest {
  readonly store: SessionControlStore;
  readonly harness: SessionControlHarness;
  readonly activeTurns: ActiveTurnsFacade;
  readonly conversationId: string;
}

export async function buildCurrentSessionPanel({ store, harness, activeTurns, conversationId }: CurrentSessionPanelRequest): Promise<ControlPanel> {
  const sessionId = store.getSessionId(conversationId);
  if (!sessionId) {
    return {
      text: [
        `Current Session (${harness.displayName})`,
        "",
        `No ${harness.displayName} session is mounted.`,
        "Start a new session or open Sessions to mount an existing one.",
      ].join("\n"),
      options: buildPanelOptions({
        inline_keyboard: [
          [createButton(store, conversationId, "Sessions", "sessions", { page: 1 })],
          [createButton(store, conversationId, "New Session", "new")],
          closeRow(store, conversationId),
        ],
      }),
    };
  }
  const lastMessage = await harness.sessions.getSessionLastMessage(sessionId);
  const tokens = store.getSessionTokens(sessionId);
  const active = activeTurns.isBusy(conversationId);
  const lines = [
    `Current Session (${harness.displayName})`,
    "",
    `Session: ${shortSessionId(sessionId)}`,
    `Status: ${active ? "working" : "idle"}`,
  ];
  if (tokens) {
    lines.push(`Cache read: ${tokens} tokens`);
  }
  lines.push("", lastMessage ? truncateText(lastMessage, 520) : "No assistant message found yet.");

  const keyboard = [];
  if (active) {
    keyboard.push([createButton(store, conversationId, "Stop Turn", "stop")]);
  }
  keyboard.push([
    createButton(store, conversationId, "Rewind", "rewind", { page: 1 }),
    createButton(store, conversationId, "Sessions", "sessions", { page: 1 }),
  ]);
  keyboard.push([createButton(store, conversationId, "New Session", "new")]);
  keyboard.push(closeRow(store, conversationId));

  return {
    text: lines.join("\n"),
    options: buildPanelOptions({ inline_keyboard: keyboard }),
  };
}

async function buildSessionPreviewPanel({ store, harness, conversationId, sessionId, page = 1 }: {
  readonly store: SessionControlStore;
  readonly harness: SessionControlHarness;
  readonly conversationId: string;
  readonly sessionId: string;
  readonly page?: unknown;
}): Promise<ControlPanel> {
  const lastMessage = await harness.sessions.getSessionLastMessage(sessionId);
  const mountedSessionId = store.getSessionId(conversationId);
  const lines = [
    "Session",
    "",
    `Session: ${shortSessionId(sessionId)}`,
    sessionId === mountedSessionId ? "Mounted: yes" : "Mounted: no",
    "",
    lastMessage ? truncateText(lastMessage, 900) : "No assistant message found yet.",
  ];
  return {
    text: lines.join("\n"),
    options: buildPanelOptions({
      inline_keyboard: [
        [createButton(store, conversationId, "Mount This Session", "mount", { sessionId })],
        [
          createButton(store, conversationId, "Rewind", "rewind", { page: 1, sessionId }),
          createButton(store, conversationId, "Back", "sessions", { page }),
        ],
        closeRow(store, conversationId),
      ],
    }),
  };
}

async function buildRewindPanel({ store, harness, activeTurns, conversationId, page = 1, sessionId = null }: {
  readonly store: SessionControlStore;
  readonly harness: SessionControlHarness;
  readonly activeTurns: ActiveTurnsFacade;
  readonly conversationId: string;
  readonly page?: unknown;
  /** The session to rewind; the mounted one when left out. */
  readonly sessionId?: string | null | undefined;
}): Promise<ControlPanel> {
  const targetSessionId = sessionId ?? store.getSessionId(conversationId);
  if (!targetSessionId) {
    return await buildCurrentSessionPanel({ store, harness, activeTurns, conversationId });
  }
  const totalPages = await harness.sessions.getTotalRewindPages(targetSessionId);
  const safePage = normalizePage(page, totalPages);
  const messages = await harness.sessions.listSessionMessages(targetSessionId);
  const start = (safePage - 1) * SESSIONS_PER_PAGE;
  const pageMessages = messages.slice(start, start + SESSIONS_PER_PAGE);
  const lines = [
    "Rewind",
    "",
    `Session: ${shortSessionId(targetSessionId)}`,
    `Page ${safePage}/${totalPages}`,
  ];
  if (pageMessages.length === 0) {
    lines.push("", "No rewind points found.");
  } else {
    lines.push("");
    for (const message of pageMessages) {
      lines.push(`${message.index}. ${truncateText(message.text, 92)}`);
    }
  }

  const keyboard = pageMessages.map((message) => [
    createButton(store, conversationId, `Before ${message.index}`, "rewind_preview", {
      sessionId: targetSessionId,
      index: message.index,
      page: safePage,
    }),
  ]);
  if (totalPages > 1) {
    keyboard.push([
      createButton(store, conversationId, "Prev", "rewind", { sessionId: targetSessionId, page: safePage - 1 }),
      createButton(store, conversationId, "Next", "rewind", { sessionId: targetSessionId, page: safePage + 1 }),
    ]);
  }
  keyboard.push([createButton(store, conversationId, "Back", "current")]);
  keyboard.push(closeRow(store, conversationId));

  return {
    text: lines.join("\n"),
    options: buildPanelOptions({ inline_keyboard: keyboard }),
  };
}

async function buildRewindPreviewPanel({ store, harness, conversationId, sessionId, index, page = 1 }: {
  readonly store: SessionControlStore;
  readonly harness: SessionControlHarness;
  readonly conversationId: string;
  readonly sessionId: string;
  readonly index: number | undefined;
  readonly page?: unknown;
}): Promise<ControlPanel> {
  const messages = await harness.sessions.listSessionMessages(sessionId);
  const target = messages.find((message) => message.index === index);
  if (!target) {
    return await buildRewindPanel({ store, harness, activeTurns: noActiveTurns, conversationId, sessionId, page });
  }
  return {
    text: [
      "Rewind Preview",
      "",
      `Fork before message ${target.index}:`,
      "",
      truncateText(target.text, 1000),
    ].join("\n"),
    options: buildPanelOptions({
      inline_keyboard: [
        [createButton(store, conversationId, "Fork And Mount Here", "rewind_fork", { sessionId, index })],
        [createButton(store, conversationId, "Back", "rewind", { sessionId, page })],
        closeRow(store, conversationId),
      ],
    }),
  };
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

export interface SendSessionsPanelRequest extends SessionsPanelRequest {
  readonly client: Pick<Client, "sendMessage">;
  readonly chatId: ChatId;
}

export async function sendSessionsPanel({ client, store, harness, conversationId, chatId, page = 1 }: SendSessionsPanelRequest): Promise<void> {
  const panel = await buildSessionsPanel({ store, harness, conversationId, page });
  await client.sendMessage(chatId, panel.text, panel.options);
}

export interface SendCurrentSessionPanelRequest extends CurrentSessionPanelRequest {
  readonly client: Pick<Client, "sendMessage">;
  readonly chatId: ChatId;
}

export async function sendCurrentSessionPanel({ client, store, harness, activeTurns, conversationId, chatId }: SendCurrentSessionPanelRequest): Promise<void> {
  const panel = await buildCurrentSessionPanel({ store, harness, activeTurns, conversationId });
  await client.sendMessage(chatId, panel.text, panel.options);
}

export interface SessionControlCallback extends ControlCallback {
  readonly client: Pick<Client, "answerCallbackQuery" | "editMessageText" | "deleteMessage">;
  readonly store: SessionControlStore;
  readonly harness: SessionControlHarness;
  readonly activeTurns: ActiveTurnsFacade;
  readonly startNewSession: StartNewSession;
}

export async function handleSessionControlCallback({
  client,
  store,
  harness,
  activeTurns,
  action,
  startNewSession,
  callbackQueryId,
  chatId,
  messageId,
}: SessionControlCallback): Promise<void> {
  const kind = action.kind.slice(CONTROL_KIND_PREFIX.length);
  const payload = action.payload ?? {};
  // Every button of these panels that names a session carries it as a string.
  const payloadSessionId = stringField(payload, "sessionId");
  let panel: ControlPanel | null = null;
  let notice = "";

  if (kind === "sessions") {
    panel = await buildSessionsPanel({ store, harness, conversationId: action.conversationId, page: payload["page"] });
  } else if (kind === "current") {
    panel = await buildCurrentSessionPanel({ store, harness, activeTurns, conversationId: action.conversationId });
  } else if (kind === "new") {
    if (activeTurns.isBusy(action.conversationId)) {
      notice = `${harness.displayName} is currently working.`;
      panel = await buildCurrentSessionPanel({ store, harness, activeTurns, conversationId: action.conversationId });
    } else {
      const sessionId = await startNewSession({ conversationId: action.conversationId });
      notice = `New session mounted: ${shortSessionId(sessionId)}.`;
      panel = await buildCurrentSessionPanel({ store, harness, activeTurns, conversationId: action.conversationId });
    }
  } else if (kind === "preview" && payloadSessionId !== undefined) {
    panel = await buildSessionPreviewPanel({
      store,
      harness,
      conversationId: action.conversationId,
      sessionId: payloadSessionId,
      page: payload["page"],
    });
  } else if (kind === "mount" && payloadSessionId !== undefined) {
    store.setSessionId(action.conversationId, payloadSessionId);
    notice = "Mounted.";
    panel = await buildCurrentSessionPanel({ store, harness, activeTurns, conversationId: action.conversationId });
  } else if (kind === "rewind") {
    panel = await buildRewindPanel({
      store,
      harness,
      activeTurns,
      conversationId: action.conversationId,
      sessionId: payloadSessionId,
      page: payload["page"],
    });
  } else if (kind === "rewind_preview" && payloadSessionId !== undefined) {
    panel = await buildRewindPreviewPanel({
      store,
      harness,
      conversationId: action.conversationId,
      sessionId: payloadSessionId,
      index: numberField(payload, "index"),
      page: payload["page"],
    });
  } else if (kind === "rewind_fork" && payloadSessionId !== undefined) {
    const index = numberField(payload, "index");
    const messages = await harness.sessions.listSessionMessages(payloadSessionId);
    const target = messages.find((message) => message.index === index);
    const forkedId = target
      ? await harness.sessions.createForkedSession(payloadSessionId, target.uuid, { threadKey: action.conversationId })
      : null;
    if (forkedId) {
      store.setSessionId(action.conversationId, forkedId);
      notice = "Fork mounted.";
      panel = await buildCurrentSessionPanel({ store, harness, activeTurns, conversationId: action.conversationId });
    } else {
      notice = "Failed to fork.";
      panel = await buildRewindPanel({
        store,
        harness,
        activeTurns,
        conversationId: action.conversationId,
        sessionId: payloadSessionId,
        page: 1,
      });
    }
  } else if (kind === "stop") {
    const interrupted = await activeTurns.stop(action.conversationId);
    notice = interrupted ? "Interrupted." : "No active turn.";
    panel = await buildCurrentSessionPanel({ store, harness, activeTurns, conversationId: action.conversationId });
  } else if (kind === "close") {
    await client.answerCallbackQuery(callbackQueryId, "Closed.");
    try {
      await client.deleteMessage(chatId, messageId);
    } catch {
      await client.editMessageText(chatId, messageId, "Closed.", { format: "plain" });
    }
    return;
  }

  if (!panel) {
    await client.answerCallbackQuery(callbackQueryId, "Unknown action.");
    return;
  }
  await client.answerCallbackQuery(callbackQueryId, notice);
  await editPanel(client, chatId, messageId, panel);
}
