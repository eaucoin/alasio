import type { InlineKeyboardButton } from "@grammyjs/types";
import { Effect } from "effect";

import { type ConversationBusy, Turns } from "../codex/turns.ts";
import type { ListedSession } from "../harness/claude/sessions.ts";
import { ActiveTurns } from "../harness/active-turns.ts";
import type { Harness, HarnessError, HarnessUnavailable, NoServiceMounted } from "../harness/index.ts";
import type { CallbackPayload } from "../persistence/callback-repository.ts";
import { type SqliteStore, Store } from "../persistence/store.ts";
import { SESSIONS_PER_PAGE } from "../shared/runtime-constants.ts";
import { type ChatId, TelegramClient, type TelegramError } from "../telegram/client.ts";
import { type ControlCallback, type ControlPanel, closePanel, editPanel, panelOptions, sendPanel } from "./panel.ts";
import { truncateText } from "./text.ts";

/** The mounted harness, as the session panels list, rewind, and resume its sessions. */
export type SessionControlHarness = Pick<Harness, "displayName" | "sessions">;

/** How starting a new session from a panel fails. */
export type NewSessionError = NoServiceMounted | HarnessUnavailable | ConversationBusy | HarnessError;

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

const buildMountedSummary = Effect.fnUntraced(function*(harness: SessionControlHarness, conversationId: string): Effect.fn.Return<string[], HarnessError, Store> {
  const store = yield* Store;
  const sessionId = store.getSessionId(conversationId);
  if (!sessionId) {
    return ["Mounted", `No mounted ${harness.displayName} session.`];
  }
  const lastMessage = yield* harness.sessions.getSessionLastMessage(sessionId);
  const tokens = store.getSessionTokens(sessionId);
  return [
    "Mounted",
    `${shortSessionId(sessionId)}${tokens ? ` - cache read ${tokens} tokens` : ""}`,
    lastMessage ? truncateText(lastMessage, 260) : "No assistant message found yet.",
  ];
});

export interface SessionsPanelRequest {
  readonly harness: SessionControlHarness;
  readonly conversationId: string;
  /** The page to show; normalized into range, so a button's payload is passed as it is. */
  readonly page?: unknown;
}

const buildSessionsPanel = Effect.fnUntraced(function*({ harness, conversationId, page = 1 }: SessionsPanelRequest): Effect.fn.Return<ControlPanel, HarnessError, Store> {
  const store = yield* Store;
  const mountedSessionId = store.getSessionId(conversationId);
  const totalPages = yield* harness.sessions.getTotalSessionPages();
  const safePage = normalizePage(page, totalPages);
  const sessions = yield* harness.sessions.listSessions(safePage);
  const startNumber = (safePage - 1) * SESSIONS_PER_PAGE + 1;
  const lines = [
    `Sessions (${harness.displayName})`,
    "",
    ...(yield* buildMountedSummary(harness, conversationId)),
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
    options: panelOptions({ inline_keyboard: keyboard }),
  };
});

export interface CurrentSessionPanelRequest {
  readonly harness: SessionControlHarness;
  readonly conversationId: string;
}

const buildCurrentSessionPanel = Effect.fnUntraced(function*({ harness, conversationId }: CurrentSessionPanelRequest): Effect.fn.Return<ControlPanel, HarnessError, Store | ActiveTurns> {
  const store = yield* Store;
  const sessionId = store.getSessionId(conversationId);
  if (!sessionId) {
    return {
      text: [
        `Current Session (${harness.displayName})`,
        "",
        `No ${harness.displayName} session is mounted.`,
        "Start a new session or open Sessions to mount an existing one.",
      ].join("\n"),
      options: panelOptions({
        inline_keyboard: [
          [createButton(store, conversationId, "Sessions", "sessions", { page: 1 })],
          [createButton(store, conversationId, "New Session", "new")],
          closeRow(store, conversationId),
        ],
      }),
    };
  }
  const lastMessage = yield* harness.sessions.getSessionLastMessage(sessionId);
  const tokens = store.getSessionTokens(sessionId);
  const active = yield* Effect.flatMap(ActiveTurns, (activeTurns) => activeTurns.isBusy(conversationId));
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
    options: panelOptions({ inline_keyboard: keyboard }),
  };
});

const buildSessionPreviewPanel = Effect.fnUntraced(function*({ harness, conversationId, sessionId, page = 1 }: {
  readonly harness: SessionControlHarness;
  readonly conversationId: string;
  readonly sessionId: string;
  readonly page?: unknown;
}): Effect.fn.Return<ControlPanel, HarnessError, Store> {
  const store = yield* Store;
  const lastMessage = yield* harness.sessions.getSessionLastMessage(sessionId);
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
    options: panelOptions({
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
});

const buildRewindPanel = Effect.fnUntraced(function*({ harness, conversationId, page = 1, sessionId = null }: {
  readonly harness: SessionControlHarness;
  readonly conversationId: string;
  readonly page?: unknown;
  /** The session to rewind; the mounted one when left out. */
  readonly sessionId?: string | null | undefined;
}): Effect.fn.Return<ControlPanel, HarnessError, Store | ActiveTurns> {
  const store = yield* Store;
  const targetSessionId = sessionId ?? store.getSessionId(conversationId);
  if (!targetSessionId) {
    return yield* buildCurrentSessionPanel({ harness, conversationId });
  }
  const totalPages = yield* harness.sessions.getTotalRewindPages(targetSessionId);
  const safePage = normalizePage(page, totalPages);
  const messages = yield* harness.sessions.listSessionMessages(targetSessionId);
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
    options: panelOptions({ inline_keyboard: keyboard }),
  };
});

const buildRewindPreviewPanel = Effect.fnUntraced(function*({ harness, conversationId, sessionId, index, page = 1 }: {
  readonly harness: SessionControlHarness;
  readonly conversationId: string;
  readonly sessionId: string;
  readonly index: number | undefined;
  readonly page?: unknown;
}): Effect.fn.Return<ControlPanel, HarnessError, Store | ActiveTurns> {
  const store = yield* Store;
  const messages = yield* harness.sessions.listSessionMessages(sessionId);
  const target = messages.find((message) => message.index === index);
  if (!target) {
    return yield* buildRewindPanel({ harness, conversationId, sessionId, page });
  }
  return {
    text: [
      "Rewind Preview",
      "",
      `Fork before message ${target.index}:`,
      "",
      truncateText(target.text, 1000),
    ].join("\n"),
    options: panelOptions({
      inline_keyboard: [
        [createButton(store, conversationId, "Fork And Mount Here", "rewind_fork", { sessionId, index })],
        [createButton(store, conversationId, "Back", "rewind", { sessionId, page })],
        closeRow(store, conversationId),
      ],
    }),
  };
});

export interface SendSessionsPanelRequest extends SessionsPanelRequest {
  readonly chatId: ChatId;
}

export const sendSessionsPanel = Effect.fnUntraced(function*({ chatId, ...request }: SendSessionsPanelRequest): Effect.fn.Return<void, HarnessError | TelegramError, Store | TelegramClient> {
  yield* sendPanel(chatId, yield* buildSessionsPanel(request));
});

export interface SendCurrentSessionPanelRequest extends CurrentSessionPanelRequest {
  readonly chatId: ChatId;
}

export const sendCurrentSessionPanel = Effect.fnUntraced(function*({ chatId, ...request }: SendCurrentSessionPanelRequest): Effect.fn.Return<
  void,
  HarnessError | TelegramError,
  Store | ActiveTurns | TelegramClient
> {
  yield* sendPanel(chatId, yield* buildCurrentSessionPanel(request));
});

export interface SessionControlCallback extends ControlCallback {
  readonly harness: SessionControlHarness;
}

export const handleSessionControlCallback = Effect.fnUntraced(function*({
  harness,
  action,
  callbackQueryId,
  chatId,
  messageId,
}: SessionControlCallback): Effect.fn.Return<void, NewSessionError | TelegramError, Store | ActiveTurns | Turns | TelegramClient> {
  const store = yield* Store;
  const activeTurns = yield* ActiveTurns;
  const client = yield* TelegramClient;
  const { conversationId } = action;
  const kind = action.kind.slice(CONTROL_KIND_PREFIX.length);
  const payload = action.payload ?? {};
  // Every button of these panels that names a session carries it as a string.
  const payloadSessionId = stringField(payload, "sessionId");
  const current = buildCurrentSessionPanel({ harness, conversationId });
  let panel: ControlPanel | null = null;
  let notice = "";

  if (kind === "sessions") {
    panel = yield* buildSessionsPanel({ harness, conversationId, page: payload["page"] });
  } else if (kind === "current") {
    panel = yield* current;
  } else if (kind === "new") {
    if (yield* activeTurns.isBusy(conversationId)) {
      notice = `${harness.displayName} is currently working.`;
    } else {
      const sessionId = yield* Effect.flatMap(Turns, (turns) => turns.startNewSession(conversationId));
      notice = `New session mounted: ${shortSessionId(sessionId)}.`;
    }
    panel = yield* current;
  } else if (kind === "preview" && payloadSessionId !== undefined) {
    panel = yield* buildSessionPreviewPanel({ harness, conversationId, sessionId: payloadSessionId, page: payload["page"] });
  } else if (kind === "mount" && payloadSessionId !== undefined) {
    store.setSessionId(conversationId, payloadSessionId);
    notice = "Mounted.";
    panel = yield* current;
  } else if (kind === "rewind") {
    panel = yield* buildRewindPanel({ harness, conversationId, sessionId: payloadSessionId, page: payload["page"] });
  } else if (kind === "rewind_preview" && payloadSessionId !== undefined) {
    panel = yield* buildRewindPreviewPanel({
      harness,
      conversationId,
      sessionId: payloadSessionId,
      index: numberField(payload, "index"),
      page: payload["page"],
    });
  } else if (kind === "rewind_fork" && payloadSessionId !== undefined) {
    const index = numberField(payload, "index");
    const messages = yield* harness.sessions.listSessionMessages(payloadSessionId);
    const target = messages.find((message) => message.index === index);
    const forkedId = target ? yield* harness.sessions.createForkedSession(payloadSessionId, target.uuid, { threadKey: conversationId }) : null;
    if (forkedId) {
      store.setSessionId(conversationId, forkedId);
      notice = "Fork mounted.";
      panel = yield* current;
    } else {
      notice = "Failed to fork.";
      panel = yield* buildRewindPanel({ harness, conversationId, sessionId: payloadSessionId, page: 1 });
    }
  } else if (kind === "stop") {
    const interrupted = yield* activeTurns.stop(conversationId, "interrupt");
    notice = interrupted ? "Interrupted." : "No active turn.";
    panel = yield* current;
  } else if (kind === "close") {
    return yield* closePanel({ callbackQueryId, chatId, messageId });
  }

  if (!panel) {
    yield* client.answerCallbackQuery(callbackQueryId, "Unknown action.");
    return;
  }
  yield* client.answerCallbackQuery(callbackQueryId, notice);
  yield* editPanel(chatId, messageId, panel);
});
