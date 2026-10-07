import { Effect } from "effect";

import type { ConversationChat } from "../codex/turns.ts";
import { ActiveTurns } from "../harness/active-turns.ts";
import { CLAUDE_HARNESS, CODEX_HARNESS, HARNESS_NAMES, harnessDisplayName, normalizeHarnessName } from "../harness/index.ts";
import type { HarnessName } from "../harness/names.ts";
import type { CallbackPayload } from "../persistence/callback-repository.ts";
import { type Conversation, harnessSessionOf, mountOf } from "../persistence/conversation-repository.ts";
import type { StoreError } from "../persistence/sql.ts";
import { Store } from "../persistence/store.ts";
import { TelegramClient, type TelegramError } from "../telegram/client.ts";
import { type HarnessSwitch, Mounts } from "./mounts.ts";
import { type ButtonDraft, type ControlCallback, type ControlPanel, closePanel, editPanel, keepPanel, type PanelDraft, sendPanel } from "./panel.ts";
import { truncateText } from "./text.ts";
import { sendChooseWorkspacePanel } from "./workspace-control.ts";

/** What the service controls run on. */
export type ServiceControlServices = Store | TelegramClient | ActiveTurns | Mounts;

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

function button(text: string, kind: string, payload: CallbackPayload = {}): ButtonDraft {
  return { text, kind: serviceKind(kind), payload };
}

function mountedLine(conversation: Conversation | null, harness: HarnessName): string {
  const sessionId = harnessSessionOf(conversation, harness);
  return `${harnessDisplayName(harness)}: ${sessionId ? `session ${shortSessionId(sessionId)}` : "no mounted session"}`;
}

export interface ServicePanelRequest {
  /** The conversation, as the store keeps it; null before it is. */
  readonly conversation: Conversation | null;
  /** Whether a turn runs in the conversation. */
  readonly working: boolean;
  readonly notice?: string | undefined;
}

export function buildServicePanel({ conversation, working, notice = "" }: ServicePanelRequest): PanelDraft {
  const active = mountOf(conversation).harness;
  const lines = [
    "Service",
    "",
    `Active: ${active ? harnessDisplayName(active) : "none"}`,
    `Status: ${working ? "working" : "idle"}`,
    "",
    "Mounted sessions",
    ...HARNESS_NAMES.map((harness) => `${harness === active ? "* " : "  "}${mountedLine(conversation, harness)}`),
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
    .map((harness) => button(`Use ${harnessDisplayName(harness)}`, "use", { harness }));
  return {
    text: lines.join("\n"),
    keyboard: [...(switchRow.length > 0 ? [switchRow] : []), [button("Close", "close")]],
  };
}

/** The conversation's service panel as it stands, with `notice` under it. */
const servicePanel = Effect.fnUntraced(function*(conversationId: string, notice?: string): Effect.fn.Return<ControlPanel, StoreError, Store | ActiveTurns> {
  const working = yield* Effect.flatMap(ActiveTurns, (activeTurns) => activeTurns.isBusy(conversationId));
  const conversation = yield* Effect.flatMap(Store, (store) => store.getConversation(conversationId));
  return yield* keepPanel(conversationId, buildServicePanel({ conversation, working, notice }));
});

/**
 * Reply used whenever a prompt or control arrives before any service is mounted.
 * It is the only thing alasio says in that state.
 */
export const sendChooseServicePanel = ({ conversationId, chatId }: ConversationChat): Effect.Effect<void, TelegramError | StoreError, Store | TelegramClient | ActiveTurns> =>
  Effect.flatMap(servicePanel(conversationId, CHOOSE_SERVICE_NOTICE), (panel) => sendPanel(chatId, panel));

function describeSwitch(result: HarnessSwitch, harness: HarnessName): string {
  if (!result.switched) {
    return `${harnessDisplayName(harness)} is already active.`;
  }
  return result.previous ? `Switched to ${harnessDisplayName(harness)}.` : `Mounted ${harnessDisplayName(harness)}. Send a message to start.`;
}

/** The service a /service argument or a button's payload names, or null when it names none. */
function resolveServiceTarget(target: unknown): HarnessName | null {
  if (!target) {
    return null;
  }
  const normalized = normalizeHarnessName(target, null);
  if (normalized === CODEX_HARNESS || normalized === CLAUDE_HARNESS) {
    return normalized;
  }
  return null;
}

/** Mounts `harness`: what to tell the operator, and whether it was newly mounted. */
const switchTo = (conversationId: string, harness: HarnessName): Effect.Effect<{ readonly notice: string; readonly mounted: boolean }, StoreError, Mounts> =>
  Effect.flatMap(Mounts, (mounts) => mounts.switchHarness(conversationId, harness)).pipe(
    Effect.map((result) => ({ notice: describeSwitch(result, harness), mounted: result.switched })),
    Effect.catchTag("MountRefused", (refusal) => Effect.succeed({ notice: refusal.message, mounted: false })),
  );

/** Service first, then folder: once a service is newly mounted, the folder picker follows when no folder is. */
const chainIntoFolderPicker = Effect.fnUntraced(function*(conversation: ConversationChat): Effect.fn.Return<void, TelegramError | StoreError, ServiceControlServices> {
  if (!(yield* Effect.flatMap(Store, (store) => store.getMount(conversation.conversationId))).workingDirectory) {
    yield* sendChooseWorkspacePanel(conversation);
  }
});

export interface ServiceTextCommand extends ConversationChat {
  /** The service named after /service; empty for the panel alone. */
  readonly target: string;
}

export const handleServiceTextCommand = Effect.fnUntraced(function*({ conversationId, chatId, target }: ServiceTextCommand): Effect.fn.Return<
  void,
  TelegramError | StoreError,
  ServiceControlServices
> {
  if (!target) {
    return yield* sendPanel(chatId, yield* servicePanel(conversationId));
  }
  const harness = resolveServiceTarget(target);
  if (!harness) {
    yield* Effect.flatMap(TelegramClient, (client) => client.sendMessage(chatId, `Unknown service "${target}". Use /service codex or /service claude.`));
    return;
  }
  const { notice, mounted } = yield* switchTo(conversationId, harness);
  yield* sendPanel(chatId, yield* servicePanel(conversationId, notice));
  if (mounted) {
    yield* chainIntoFolderPicker({ conversationId, chatId });
  }
});

export const handleServiceControlCallback = Effect.fnUntraced(function*({ action, callbackQueryId, chatId, messageId }: ControlCallback): Effect.fn.Return<
  void,
  TelegramError | StoreError,
  ServiceControlServices
> {
  const client = yield* TelegramClient;
  const { conversationId } = action;
  const kind = action.kind.slice(SERVICE_KIND_PREFIX.length);
  const payload = action.payload ?? {};
  if (kind === "close") {
    return yield* closePanel({ callbackQueryId, chatId, messageId });
  }
  if (kind === "use") {
    const harness = resolveServiceTarget(payload["harness"]);
    if (!harness) {
      yield* client.answerCallbackQuery(callbackQueryId, "Unknown service.");
      return;
    }
    const { notice, mounted } = yield* switchTo(conversationId, harness);
    yield* client.answerCallbackQuery(callbackQueryId, truncateText(notice, 180));
    yield* editPanel(chatId, messageId, yield* servicePanel(conversationId, notice));
    if (mounted) {
      yield* chainIntoFolderPicker({ conversationId, chatId });
    }
    return;
  }
  yield* client.answerCallbackQuery(callbackQueryId, "Unknown action.");
});
