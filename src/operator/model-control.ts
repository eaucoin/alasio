import type { InlineKeyboardButton, InlineKeyboardMarkup } from "@grammyjs/types";
import { type HarnessFacade, harnessDisplayName, isHarnessName } from "../harness/index.ts";
import type { HarnessName } from "../harness/names.ts";
import type { CallbackPayload } from "../persistence/callback-repository.ts";
import type { ModelChoice } from "../persistence/conversation-repository.ts";
import type { SqliteStore } from "../persistence/store.ts";
import type { ChatId, Client } from "../telegram/client.ts";
import type { ControlCallback, ControlPanel, ControlPanelOptions } from "./session-control.ts";
import { truncateText } from "./text.ts";

// /model: choose a model for the mounted service, then an effort that model
// supports. The two services have different catalogues and effort scales, so
// both steps are built from what the service itself reports rather than a
// fixed list. The choice is stored per conversation and per service and takes
// effect on the next turn.

/** The store's model choices and callback actions, as /model reads and changes them. */
export type ModelControlStore = Pick<SqliteStore, "createCallbackAction" | "setModelChoice" | "clearModelChoice">
  & Partial<Pick<SqliteStore, "getModelChoice">>;

/** The mounted harness, as /model lists its models. */
export type ModelControlHarness = Pick<HarnessFacade, "name" | "displayName" | "listModels" | "defaultModelChoice">;

/** What a model's button carries to the effort step. */
interface ModelPick {
  readonly model: string;
  readonly label: string;
  readonly efforts: readonly string[];
  readonly defaultEffort: string | null;
}

const MODEL_KIND_PREFIX = "model:";
const BUTTONS_PER_ROW = 2;

function modelKind(kind: string): string {
  return `${MODEL_KIND_PREFIX}${kind}`;
}

export function isModelControlAction(kind: unknown): boolean {
  return typeof kind === "string" && kind.startsWith(MODEL_KIND_PREFIX);
}

function button(
  store: Pick<SqliteStore, "createCallbackAction">,
  conversationId: string,
  text: string,
  kind: string,
  payload: CallbackPayload,
): InlineKeyboardButton.CallbackButton {
  return {
    text,
    callback_data: store.createCallbackAction({ conversationId, kind: modelKind(kind), payload }),
  };
}

function rows<T>(buttons: readonly T[], perRow = BUTTONS_PER_ROW): T[][] {
  const result = [];
  for (let index = 0; index < buttons.length; index += perRow) {
    result.push(buttons.slice(index, index + perRow));
  }
  return result;
}

function describeChoice(choice: ModelChoice | null): string {
  if (!choice?.model) {
    return "none";
  }
  return choice.effort ? `${choice.model} at ${choice.effort} effort` : choice.model;
}

function panelOptions(keyboard: InlineKeyboardMarkup["inline_keyboard"]): ControlPanelOptions {
  return { format: "plain", reply_markup: { inline_keyboard: keyboard } };
}

export interface ModelPanelRequest {
  readonly store: ModelControlStore;
  readonly harness: ModelControlHarness;
  readonly conversationId: string;
}

/** Step one: every model the mounted service offers. */
export async function buildModelPanel({ store, harness, conversationId }: ModelPanelRequest): Promise<ControlPanel> {
  const models = await harness.listModels();
  const chosen = store.getModelChoice?.(conversationId, harness.name) ?? null;
  const current = chosen ?? harness.defaultModelChoice();
  const lines = [
    `Model for ${harness.displayName}`,
    "",
    `Current: ${describeChoice(current)}${chosen ? "" : " (default)"}`,
    "",
    "Choose a model, then an effort. It applies from the next turn.",
  ];
  const modelButtons = models.map((model) =>
    button(store, conversationId, model.label, "pick", {
      harness: harness.name,
      model: model.id,
      label: model.label,
      efforts: model.efforts,
      defaultEffort: model.defaultEffort,
    }),
  );
  const keyboard = rows(modelButtons);
  const footer = [];
  if (chosen) {
    footer.push(button(store, conversationId, "Use default", "reset", { harness: harness.name }));
  }
  footer.push(button(store, conversationId, "Close", "close", {}));
  keyboard.push(footer);
  return { text: lines.join("\n"), options: panelOptions(keyboard) };
}

export interface SendModelPanelRequest extends Omit<ModelPanelRequest, "harness"> {
  readonly client: Pick<Client, "sendMessage">;
  /** The mounted harness; null when none is. */
  readonly harness: ModelControlHarness | null;
  readonly chatId: ChatId;
}

export async function sendModelPanel({ client, store, harness, conversationId, chatId }: SendModelPanelRequest): Promise<void> {
  if (!harness?.listModels) {
    await client.sendMessage(chatId, "No service is mounted. Choose one with /service first.");
    return;
  }
  let panel;
  try {
    panel = await buildModelPanel({ store, harness, conversationId });
  } catch (error) {
    const detail = error instanceof Error ? error.message : String(error);
    await client.sendMessage(chatId, truncateText(`Could not list ${harness.displayName} models: ${detail}`, 400));
    return;
  }
  await client.sendMessage(chatId, panel.text, panel.options);
}

/** Step two: the efforts the chosen model supports. */
function buildEffortPanel({ store, conversationId, harnessName, payload }: {
  readonly store: Pick<SqliteStore, "createCallbackAction">;
  readonly conversationId: string;
  readonly harnessName: HarnessName;
  readonly payload: ModelPick;
}): ControlPanel {
  const lines = [
    `${payload.label} (${payload.model})`,
    "",
    "Choose an effort.",
  ];
  const effortButtons = payload.efforts.map((effort) =>
    button(
      store,
      conversationId,
      effort === payload.defaultEffort ? `${effort} (default)` : effort,
      "effort",
      { harness: harnessName, model: payload.model, label: payload.label, effort },
    ),
  );
  const keyboard = rows(effortButtons, 3);
  keyboard.push([button(store, conversationId, "Close", "close", {})]);
  return { text: lines.join("\n"), options: panelOptions(keyboard) };
}

function confirmation(harnessName: HarnessName, label: string, model: string, effort: string | null | undefined): string {
  const at = effort ? ` at ${effort} effort` : "";
  return `${harnessDisplayName(harnessName)} will use ${label} (${model})${at} from the next turn.`;
}

function stringField(payload: CallbackPayload, key: string): string | undefined {
  const value = payload[key];
  return typeof value === "string" ? value : undefined;
}

export interface ModelControlCallback extends ControlCallback {
  readonly client: Pick<Client, "answerCallbackQuery" | "editMessageText">;
  readonly store: ModelControlStore;
}

export async function handleModelControlCallback({ client, store, action, callbackQueryId, chatId, messageId }: ModelControlCallback): Promise<void> {
  const kind = action.kind.slice(MODEL_KIND_PREFIX.length);
  const payload = action.payload ?? {};
  // The store also stamps expectedHarness, which the callback handler uses to
  // reject a panel left open across a /service switch.
  const harnessName = payload["harness"];
  if (kind === "close") {
    await client.answerCallbackQuery(callbackQueryId, "Closed.");
    await client.editMessageText(chatId, messageId, "Closed.", { format: "plain" }).catch(() => undefined);
    return;
  }
  // Every other button of these panels names its harness, and a model's names the model.
  const model = stringField(payload, "model");
  const label = stringField(payload, "label");
  if (kind === "reset" && isHarnessName(harnessName)) {
    store.clearModelChoice(action.conversationId, harnessName);
    const text = `${harnessDisplayName(harnessName)} is back on its default model from the next turn.`;
    await client.answerCallbackQuery(callbackQueryId, "Reset.");
    await client.editMessageText(chatId, messageId, text, { format: "plain" });
    return;
  }
  if (kind === "pick" && isHarnessName(harnessName) && model !== undefined && label !== undefined) {
    const efforts = payload["efforts"];
    if (!Array.isArray(efforts) || efforts.length === 0) {
      // A model without effort control is chosen outright.
      store.setModelChoice(action.conversationId, harnessName, { model, effort: null });
      await client.answerCallbackQuery(callbackQueryId, "Model set.");
      await client.editMessageText(chatId, messageId, confirmation(harnessName, label, model, null), { format: "plain" });
      return;
    }
    const pick: ModelPick = {
      model,
      label,
      efforts: efforts.filter((effort): effort is string => typeof effort === "string"),
      defaultEffort: stringField(payload, "defaultEffort") ?? null,
    };
    const panel = buildEffortPanel({ store, conversationId: action.conversationId, harnessName, payload: pick });
    await client.answerCallbackQuery(callbackQueryId, "Now choose an effort.");
    await client.editMessageText(chatId, messageId, panel.text, panel.options);
    return;
  }
  if (kind === "effort" && isHarnessName(harnessName) && model !== undefined && label !== undefined) {
    const effort = stringField(payload, "effort");
    store.setModelChoice(action.conversationId, harnessName, { model, effort });
    await client.answerCallbackQuery(callbackQueryId, "Model set.");
    await client.editMessageText(chatId, messageId, confirmation(harnessName, label, model, effort), { format: "plain" });
    return;
  }
  await client.answerCallbackQuery(callbackQueryId, "Unknown action.");
}
