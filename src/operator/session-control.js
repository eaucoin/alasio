import { interruptCodexTurn } from "../codex/runtime.js";
import { SESSIONS_PER_PAGE } from "../shared/runtime-constants.js";
import { createForkedSession } from "../sessions/forking.js";
import {
  getSessionLastMessage,
  getTotalRewindPages,
  getTotalSessionPages,
  listSessionMessages,
  listSessions,
} from "../sessions/index.js";
import { truncateText } from "./text.js";

const CONTROL_KIND_PREFIX = "control:";

function controlKind(kind) {
  return `${CONTROL_KIND_PREFIX}${kind}`;
}

export function isSessionControlAction(kind) {
  return typeof kind === "string" && kind.startsWith(CONTROL_KIND_PREFIX);
}

function normalizePage(page, totalPages) {
  const parsed = Number.parseInt(String(page ?? 1), 10);
  const safePage = Number.isFinite(parsed) && parsed > 0 ? parsed : 1;
  return Math.min(Math.max(safePage, 1), totalPages);
}

function shortSessionId(sessionId) {
  return sessionId ? sessionId.slice(0, 8) : "-";
}

function createButton(store, conversationId, text, kind, payload = {}) {
  return {
    text,
    callback_data: store.createCallbackAction({
      conversationId,
      kind: controlKind(kind),
      payload,
    }),
  };
}

function buildPanelOptions(replyMarkup) {
  return {
    format: "plain",
    reply_markup: replyMarkup,
  };
}

function closeRow(store, conversationId) {
  return [createButton(store, conversationId, "Close", "close")];
}

function describeSession(session, mountedSessionId) {
  const marker = session.uuid === mountedSessionId ? "* " : "";
  return `${marker}${session.timestamp || "-"} - ${session.label || shortSessionId(session.uuid)}`;
}

function buildMountedSummary(store, conversationId) {
  const sessionId = store.getSessionId(conversationId);
  if (!sessionId) {
    return ["Mounted", "No mounted Codex session."];
  }
  const lastMessage = getSessionLastMessage(sessionId);
  const tokens = store.getSessionTokens(sessionId);
  return [
    "Mounted",
    `${shortSessionId(sessionId)}${tokens ? ` - cache read ${tokens} tokens` : ""}`,
    lastMessage ? truncateText(lastMessage, 260) : "No assistant message found yet.",
  ];
}

export function buildSessionsPanel({ store, conversationId, page = 1 }) {
  const mountedSessionId = store.getSessionId(conversationId);
  const totalPages = getTotalSessionPages();
  const safePage = normalizePage(page, totalPages);
  const sessions = listSessions(safePage);
  const startNumber = (safePage - 1) * SESSIONS_PER_PAGE + 1;
  const lines = [
    "Sessions",
    "",
    ...buildMountedSummary(store, conversationId),
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

export function buildCurrentSessionPanel({ store, activeQueries, conversationId }) {
  const sessionId = store.getSessionId(conversationId);
  if (!sessionId) {
    return {
      text: [
        "Current Session",
        "",
        "No Codex session is mounted.",
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
  const lastMessage = getSessionLastMessage(sessionId);
  const tokens = store.getSessionTokens(sessionId);
  const active = activeQueries.has(conversationId);
  const lines = [
    "Current Session",
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

function buildSessionPreviewPanel({ store, conversationId, sessionId, page = 1 }) {
  const lastMessage = getSessionLastMessage(sessionId);
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

function buildRewindPanel({ store, activeQueries, conversationId, page = 1, sessionId = null }) {
  const targetSessionId = sessionId ?? store.getSessionId(conversationId);
  if (!targetSessionId) {
    return buildCurrentSessionPanel({ store, activeQueries, conversationId });
  }
  const totalPages = getTotalRewindPages(targetSessionId);
  const safePage = normalizePage(page, totalPages);
  const messages = listSessionMessages(targetSessionId);
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

function buildRewindPreviewPanel({ store, conversationId, sessionId, index, page = 1 }) {
  const messages = listSessionMessages(sessionId);
  const target = messages.find((message) => message.index === index);
  if (!target) {
    return buildRewindPanel({ store, activeQueries: new Map(), conversationId, sessionId, page });
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

async function editPanel(client, chatId, messageId, panel) {
  try {
    await client.editMessageText(chatId, messageId, panel.text, panel.options);
  } catch (error) {
    if (!String(error).includes("message is not modified")) {
      throw error;
    }
  }
}

export async function sendSessionsPanel({ client, store, conversationId, chatId, page = 1 }) {
  const panel = buildSessionsPanel({ store, conversationId, page });
  await client.sendMessage(chatId, panel.text, panel.options);
}

export async function sendCurrentSessionPanel({ client, store, activeQueries, conversationId, chatId }) {
  const panel = buildCurrentSessionPanel({ store, activeQueries, conversationId });
  await client.sendMessage(chatId, panel.text, panel.options);
}

export async function handleSessionControlCallback({
  client,
  store,
  activeQueries,
  action,
  startNewSession,
  callbackQueryId,
  chatId,
  messageId,
}) {
  const kind = action.kind.slice(CONTROL_KIND_PREFIX.length);
  const payload = action.payload ?? {};
  let panel = null;
  let notice = "";

  if (kind === "sessions") {
    panel = buildSessionsPanel({ store, conversationId: action.conversationId, page: payload.page });
  } else if (kind === "current") {
    panel = buildCurrentSessionPanel({ store, activeQueries, conversationId: action.conversationId });
  } else if (kind === "new") {
    if (activeQueries.has(action.conversationId)) {
      notice = "Codex is currently working.";
      panel = buildCurrentSessionPanel({ store, activeQueries, conversationId: action.conversationId });
    } else {
      const sessionId = await startNewSession({ conversationId: action.conversationId });
      notice = `New session mounted: ${shortSessionId(sessionId)}.`;
      panel = buildCurrentSessionPanel({ store, activeQueries, conversationId: action.conversationId });
    }
  } else if (kind === "preview") {
    panel = buildSessionPreviewPanel({
      store,
      conversationId: action.conversationId,
      sessionId: payload.sessionId,
      page: payload.page,
    });
  } else if (kind === "mount") {
    store.setSessionId(action.conversationId, payload.sessionId);
    notice = "Mounted.";
    panel = buildCurrentSessionPanel({ store, activeQueries, conversationId: action.conversationId });
  } else if (kind === "rewind") {
    panel = buildRewindPanel({
      store,
      activeQueries,
      conversationId: action.conversationId,
      sessionId: payload.sessionId,
      page: payload.page,
    });
  } else if (kind === "rewind_preview") {
    panel = buildRewindPreviewPanel({
      store,
      conversationId: action.conversationId,
      sessionId: payload.sessionId,
      index: payload.index,
      page: payload.page,
    });
  } else if (kind === "rewind_fork") {
    const messages = listSessionMessages(payload.sessionId);
    const target = messages.find((message) => message.index === payload.index);
    const forkedId = target ? createForkedSession(payload.sessionId, target.uuid) : null;
    if (forkedId) {
      store.setSessionId(action.conversationId, forkedId);
      notice = "Fork mounted.";
      panel = buildCurrentSessionPanel({ store, activeQueries, conversationId: action.conversationId });
    } else {
      notice = "Failed to fork.";
      panel = buildRewindPanel({
        store,
        activeQueries,
        conversationId: action.conversationId,
        sessionId: payload.sessionId,
        page: 1,
      });
    }
  } else if (kind === "stop") {
    const interrupted = await interruptCodexTurn(activeQueries, action.conversationId);
    notice = interrupted ? "Interrupted." : "No active turn.";
    panel = buildCurrentSessionPanel({ store, activeQueries, conversationId: action.conversationId });
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
