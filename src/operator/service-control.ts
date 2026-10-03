// @ts-nocheck
import {
  CLAUDE_HARNESS,
  CODEX_HARNESS,
  HARNESS_NAMES,
  harnessDisplayName,
  normalizeHarnessName,
  resolveHarnessName,
} from "../harness/index.ts";
import { truncateText } from "./text.ts";

const SERVICE_KIND_PREFIX = "service:";

export const CHOOSE_SERVICE_NOTICE = "No service is mounted. Choose Codex or Claude to start; your message was not queued.";

function serviceKind(kind) {
  return `${SERVICE_KIND_PREFIX}${kind}`;
}

export function isServiceControlAction(kind) {
  return typeof kind === "string" && kind.startsWith(SERVICE_KIND_PREFIX);
}

function shortSessionId(sessionId) {
  return sessionId ? sessionId.slice(0, 8) : "-";
}

function createButton(store, conversationId, text, kind, payload = {}) {
  return {
    text,
    callback_data: store.createCallbackAction({
      conversationId,
      kind: serviceKind(kind),
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

function mountedLine(store, conversationId, harness) {
  const sessionId = store.getHarnessSessionId?.(conversationId, harness) ?? null;
  return `${harnessDisplayName(harness)}: ${sessionId ? `session ${shortSessionId(sessionId)}` : "no mounted session"}`;
}

export function buildServicePanel({ store, activeQueries, conversationId, notice = "" }) {
  const active = resolveHarnessName(store, conversationId);
  const working = activeQueries?.has?.(conversationId) ?? false;
  const lines = [
    "Service",
    "",
    `Active: ${active ? harnessDisplayName(active) : "none"}`,
    `Status: ${working ? "working" : "idle"}`,
    "",
    "Mounted sessions",
    ...HARNESS_NAMES.map((harness) => `${harness === active ? "* " : "  "}${mountedLine(store, conversationId, harness)}`),
    "",
    active
      ? "Sessions belong to one service. Switching parks the current session and resumes the other service's own session."
      : "Nothing runs until a service is chosen. Each service keeps its own sessions and uses its own local login.",
  ];
  if (notice) {
    lines.push("", truncateText(notice, 300));
  }
  const switchRow = HARNESS_NAMES
    .filter((harness) => harness !== active)
    .map((harness) => createButton(store, conversationId, `Use ${harnessDisplayName(harness)}`, "use", { harness }));
  const keyboard = [];
  if (switchRow.length > 0) {
    keyboard.push(switchRow);
  }
  keyboard.push([createButton(store, conversationId, "Close", "close")]);
  return {
    text: lines.join("\n"),
    options: buildPanelOptions({ inline_keyboard: keyboard }),
  };
}

/**
 * Reply used whenever a prompt or control arrives before any service is mounted.
 * It is the only thing alasio says in that state.
 */
export async function sendChooseServicePanel({ client, store, activeQueries, conversationId, chatId }) {
  const panel = buildServicePanel({ store, activeQueries, conversationId, notice: CHOOSE_SERVICE_NOTICE });
  await client.sendMessage(chatId, panel.text, panel.options);
}

function describeSwitch(result, harness) {
  if (!result.switched) {
    return `${harnessDisplayName(harness)} is already active.`;
  }
  return result.previous ? `Switched to ${harnessDisplayName(harness)}.` : `Mounted ${harnessDisplayName(harness)}. Send a message to start.`;
}

export function resolveServiceTarget(target) {
  if (!target) {
    return null;
  }
  const normalized = normalizeHarnessName(target, null);
  if (normalized === CODEX_HARNESS || normalized === CLAUDE_HARNESS) {
    return normalized;
  }
  return null;
}

export async function handleServiceTextCommand({ client, store, activeQueries, conversationId, chatId, target, switchHarness, onMounted = null }) {
  if (target) {
    const harness = resolveServiceTarget(target);
    if (!harness) {
      await client.sendMessage(chatId, `Unknown service "${target}". Use /service codex or /service claude.`);
      return;
    }
    let notice;
    let mounted = false;
    try {
      const result = await switchHarness({ conversationId, harness });
      notice = describeSwitch(result, harness);
      mounted = result.switched;
    } catch (error) {
      notice = error instanceof Error ? error.message : String(error);
    }
    const panel = buildServicePanel({ store, activeQueries, conversationId, notice });
    await client.sendMessage(chatId, panel.text, panel.options);
    if (mounted && onMounted) {
      await onMounted();
    }
    return;
  }
  const panel = buildServicePanel({ store, activeQueries, conversationId });
  await client.sendMessage(chatId, panel.text, panel.options);
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

export async function handleServiceControlCallback({
  client,
  store,
  activeQueries,
  action,
  switchHarness,
  callbackQueryId,
  chatId,
  messageId,
  onMounted = null,
}) {
  const kind = action.kind.slice(SERVICE_KIND_PREFIX.length);
  const payload = action.payload ?? {};
  if (kind === "close") {
    await client.answerCallbackQuery(callbackQueryId, "Closed.");
    try {
      await client.deleteMessage(chatId, messageId);
    } catch {
      await client.editMessageText(chatId, messageId, "Closed.", { format: "plain" });
    }
    return;
  }
  if (kind === "use") {
    const harness = resolveServiceTarget(payload.harness);
    if (!harness) {
      await client.answerCallbackQuery(callbackQueryId, "Unknown service.");
      return;
    }
    let notice;
    let mounted = false;
    try {
      const result = await switchHarness({ conversationId: action.conversationId, harness });
      notice = describeSwitch(result, harness);
      mounted = result.switched;
    } catch (error) {
      notice = error instanceof Error ? error.message : String(error);
    }
    await client.answerCallbackQuery(callbackQueryId, truncateText(notice, 180));
    await editPanel(client, chatId, messageId, buildServicePanel({
      store,
      activeQueries,
      conversationId: action.conversationId,
      notice,
    }));
    if (mounted && onMounted) {
      await onMounted();
    }
    return;
  }
  await client.answerCallbackQuery(callbackQueryId, "Unknown action.");
}
