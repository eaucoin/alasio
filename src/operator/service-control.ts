import type { InlineKeyboardButton, InlineKeyboardMarkup } from "@grammyjs/types";
import type { ActiveTurnsFacade } from "../harness/active-turns.ts";
import {
  CLAUDE_HARNESS,
  CODEX_HARNESS,
  HARNESS_NAMES,
  type MountStore,
  harnessDisplayName,
  normalizeHarnessName,
  resolveHarnessName,
} from "../harness/index.ts";
import type { HarnessName } from "../harness/names.ts";
import type { CallbackPayload } from "../persistence/callback-repository.ts";
import type { SqliteStore } from "../persistence/store.ts";
import type { ChatId, Client } from "../telegram/client.ts";
import type { ControlCallback, ControlPanel, ControlPanelOptions } from "./session-control.ts";
import { truncateText } from "./text.ts";

/** The outcome of mounting a service on a conversation. */
export interface HarnessSwitch {
  /** Whether the service changed; false when it was already the active one. */
  readonly switched: boolean;
  readonly previous: HarnessName | null;
  readonly next: HarnessName;
  readonly sessionId: string | null;
  readonly workingDirectory?: string | null | undefined;
}

/** Mounts `harness` on the conversation, or throws why it cannot be switched now. */
export type SwitchHarness = (request: { readonly conversationId: string; readonly harness: HarnessName }) => Promise<HarnessSwitch>;

/** The store's mounts, sessions, and callback actions, as the service panel reads them. */
export type ServiceControlStore = MountStore
  & Pick<SqliteStore, "createCallbackAction">
  & Partial<Pick<SqliteStore, "getHarnessSessionId">>;

const SERVICE_KIND_PREFIX = "service:";

export const CHOOSE_SERVICE_NOTICE = "No service is mounted. Choose Codex or Claude to start; your message was not queued.";

function serviceKind(kind: string): string {
  return `${SERVICE_KIND_PREFIX}${kind}`;
}

export function isServiceControlAction(kind: unknown): boolean {
  return typeof kind === "string" && kind.startsWith(SERVICE_KIND_PREFIX);
}

function shortSessionId(sessionId: string | null): string {
  return sessionId ? sessionId.slice(0, 8) : "-";
}

function createButton(
  store: ServiceControlStore,
  conversationId: string,
  text: string,
  kind: string,
  payload: CallbackPayload = {},
): InlineKeyboardButton.CallbackButton {
  return {
    text,
    callback_data: store.createCallbackAction({
      conversationId,
      kind: serviceKind(kind),
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

function mountedLine(store: ServiceControlStore, conversationId: string, harness: HarnessName): string {
  const sessionId = store.getHarnessSessionId?.(conversationId, harness) ?? null;
  return `${harnessDisplayName(harness)}: ${sessionId ? `session ${shortSessionId(sessionId)}` : "no mounted session"}`;
}

export interface ServicePanelRequest {
  readonly store: ServiceControlStore;
  readonly activeTurns?: ActiveTurnsFacade | null | undefined;
  readonly conversationId: string;
  readonly notice?: string | undefined;
}

export function buildServicePanel({ store, activeTurns, conversationId, notice = "" }: ServicePanelRequest): ControlPanel {
  const active = resolveHarnessName(store, conversationId);
  const working = activeTurns?.isBusy(conversationId) ?? false;
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
  const keyboard: InlineKeyboardButton.CallbackButton[][] = [];
  if (switchRow.length > 0) {
    keyboard.push(switchRow);
  }
  keyboard.push([createButton(store, conversationId, "Close", "close")]);
  return {
    text: lines.join("\n"),
    options: buildPanelOptions({ inline_keyboard: keyboard }),
  };
}

export interface SendServicePanelRequest {
  readonly client: Pick<Client, "sendMessage">;
  readonly store: ServiceControlStore;
  readonly activeTurns?: ActiveTurnsFacade | null | undefined;
  readonly conversationId: string;
  readonly chatId: ChatId;
}

/**
 * Reply used whenever a prompt or control arrives before any service is mounted.
 * It is the only thing alasio says in that state.
 */
export async function sendChooseServicePanel({ client, store, activeTurns, conversationId, chatId }: SendServicePanelRequest): Promise<void> {
  const panel = buildServicePanel({ store, activeTurns, conversationId, notice: CHOOSE_SERVICE_NOTICE });
  await client.sendMessage(chatId, panel.text, panel.options);
}

function describeSwitch(result: HarnessSwitch, harness: HarnessName): string {
  if (!result.switched) {
    return `${harnessDisplayName(harness)} is already active.`;
  }
  return result.previous ? `Switched to ${harnessDisplayName(harness)}.` : `Mounted ${harnessDisplayName(harness)}. Send a message to start.`;
}

/** The service a /service argument or a button's payload names, or null when it names none. */
export function resolveServiceTarget(target: unknown): HarnessName | null {
  if (!target) {
    return null;
  }
  const normalized = normalizeHarnessName(target, null);
  if (normalized === CODEX_HARNESS || normalized === CLAUDE_HARNESS) {
    return normalized;
  }
  return null;
}

export interface ServiceTextCommand extends SendServicePanelRequest {
  /** The service named after /service; empty for the panel alone. */
  readonly target: string;
  readonly switchHarness: SwitchHarness;
  /** Called once a service has been newly mounted. */
  readonly onMounted?: (() => Promise<void>) | null | undefined;
}

export async function handleServiceTextCommand({ client, store, activeTurns, conversationId, chatId, target, switchHarness, onMounted = null }: ServiceTextCommand): Promise<void> {
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
    const panel = buildServicePanel({ store, activeTurns, conversationId, notice });
    await client.sendMessage(chatId, panel.text, panel.options);
    if (mounted && onMounted) {
      await onMounted();
    }
    return;
  }
  const panel = buildServicePanel({ store, activeTurns, conversationId });
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

export interface ServiceControlCallback extends ControlCallback {
  readonly client: Pick<Client, "answerCallbackQuery" | "editMessageText" | "deleteMessage">;
  readonly store: ServiceControlStore;
  readonly activeTurns?: ActiveTurnsFacade | null | undefined;
  readonly switchHarness: SwitchHarness;
  /** Called once a service has been newly mounted. */
  readonly onMounted?: (() => Promise<void>) | null | undefined;
}

export async function handleServiceControlCallback({
  client,
  store,
  activeTurns,
  action,
  switchHarness,
  callbackQueryId,
  chatId,
  messageId,
  onMounted = null,
}: ServiceControlCallback): Promise<void> {
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
    const harness = resolveServiceTarget(payload["harness"]);
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
      activeTurns,
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
