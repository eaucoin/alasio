import { Effect } from "effect";

import { type Harness, type HarnessError, harnessDisplayName, isHarnessName } from "../harness/index.ts";
import type { HarnessName } from "../harness/names.ts";
import type { CallbackPayload } from "../persistence/callback-repository.ts";
import type { ModelChoice } from "../persistence/conversation-repository.ts";
import type { StoreError } from "../persistence/sql.ts";
import { Store } from "../persistence/store.ts";
import { type ChatId, TelegramClient, type TelegramError } from "../telegram/client.ts";
import { type ButtonDraft, type ControlCallback, type ControlPanel, keepPanel, type PanelDraft, sendPanel } from "./panel.ts";
import { truncateText } from "./text.ts";

// /model: choose a model for the mounted service, then an effort that model
// supports. The two services have different catalogues and effort scales, so
// both steps are built from what the service itself reports rather than a
// fixed list. The choice is stored per conversation and per service and takes
// effect on the next turn.

/** The mounted harness, as /model lists its models. */
export type ModelControlHarness = Pick<Harness, "name" | "displayName" | "listModels" | "defaultModelChoice">;

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

function button(text: string, kind: string, payload: CallbackPayload): ButtonDraft {
  return { text, kind: modelKind(kind), payload };
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

export interface ModelPanelRequest {
  readonly harness: ModelControlHarness;
  readonly conversationId: string;
}

/** Step one: every model the mounted service offers. */
export const buildModelPanel = Effect.fnUntraced(function*({ harness, conversationId }: ModelPanelRequest): Effect.fn.Return<ControlPanel, HarnessError | StoreError, Store> {
  const store = yield* Store;
  const models = yield* harness.listModels();
  const chosen = yield* store.getModelChoice(conversationId, harness.name);
  const current = chosen ?? harness.defaultModelChoice();
  const lines = [
    `Model for ${harness.displayName}`,
    "",
    `Current: ${describeChoice(current)}${chosen ? "" : " (default)"}`,
    "",
    "Choose a model, then an effort. It applies from the next turn.",
  ];
  const modelButtons = models.map((model) =>
    button(model.label, "pick", {
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
    footer.push(button("Use default", "reset", { harness: harness.name }));
  }
  footer.push(button("Close", "close", {}));
  keyboard.push(footer);
  return yield* keepPanel(conversationId, { text: lines.join("\n"), keyboard });
});

export interface SendModelPanelRequest extends Omit<ModelPanelRequest, "harness"> {
  /** The mounted harness; null when none is. */
  readonly harness: ModelControlHarness | null;
  readonly chatId: ChatId;
}

export const sendModelPanel = Effect.fnUntraced(function*({ harness, conversationId, chatId }: SendModelPanelRequest): Effect.fn.Return<void, TelegramError | StoreError, Store | TelegramClient> {
  const client = yield* TelegramClient;
  if (!harness) {
    yield* client.sendMessage(chatId, "No service is mounted. Choose one with /service first.");
    return;
  }
  yield* buildModelPanel({ harness, conversationId }).pipe(
    Effect.matchEffect({
      // The models could not be listed, which the operator is told; alasio's store failing is not that.
      onFailure: (error): Effect.Effect<unknown, TelegramError | StoreError> =>
        error._tag === "StoreError"
          ? Effect.fail(error)
          : client.sendMessage(chatId, truncateText(`Could not list ${harness.displayName} models: ${error.message}`, 400)),
      onSuccess: (panel) => sendPanel(chatId, panel),
    }),
  );
});

/** Step two: the efforts the chosen model supports. */
function buildEffortPanel({ harnessName, payload }: {
  readonly harnessName: HarnessName;
  readonly payload: ModelPick;
}): PanelDraft {
  const lines = [
    `${payload.label} (${payload.model})`,
    "",
    "Choose an effort.",
  ];
  const effortButtons = payload.efforts.map((effort) =>
    button(
      effort === payload.defaultEffort ? `${effort} (default)` : effort,
      "effort",
      { harness: harnessName, model: payload.model, label: payload.label, effort },
    ),
  );
  const keyboard = rows(effortButtons, 3);
  keyboard.push([button("Close", "close", {})]);
  return { text: lines.join("\n"), keyboard };
}

function confirmation(harnessName: HarnessName, label: string, model: string, effort: string | null | undefined): string {
  const at = effort ? ` at ${effort} effort` : "";
  return `${harnessDisplayName(harnessName)} will use ${label} (${model})${at} from the next turn.`;
}

function stringField(payload: CallbackPayload, key: string): string | undefined {
  const value = payload[key];
  return typeof value === "string" ? value : undefined;
}

export const handleModelControlCallback = Effect.fnUntraced(function*({ action, callbackQueryId, chatId, messageId }: ControlCallback): Effect.fn.Return<
  void,
  TelegramError | StoreError,
  Store | TelegramClient
> {
  const store = yield* Store;
  const client = yield* TelegramClient;
  const kind = action.kind.slice(MODEL_KIND_PREFIX.length);
  const payload = action.payload ?? {};
  // The store also stamps expectedHarness, which the callback handler uses to
  // reject a panel left open across a /service switch.
  const harnessName = payload["harness"];
  if (kind === "close") {
    yield* client.answerCallbackQuery(callbackQueryId, "Closed.");
    yield* client.editMessageText(chatId, messageId, "Closed.", { format: "plain" }).pipe(Effect.ignore);
    return;
  }
  // Every other button of these panels names its harness, and a model's names the model.
  const model = stringField(payload, "model");
  const label = stringField(payload, "label");
  if (kind === "reset" && isHarnessName(harnessName)) {
    yield* store.clearModelChoice(action.conversationId, harnessName);
    const text = `${harnessDisplayName(harnessName)} is back on its default model from the next turn.`;
    yield* client.answerCallbackQuery(callbackQueryId, "Reset.");
    yield* client.editMessageText(chatId, messageId, text, { format: "plain" });
    return;
  }
  if (kind === "pick" && isHarnessName(harnessName) && model !== undefined && label !== undefined) {
    const efforts = payload["efforts"];
    if (!Array.isArray(efforts) || efforts.length === 0) {
      // A model without effort control is chosen outright.
      yield* store.setModelChoice(action.conversationId, harnessName, { model, effort: null });
      yield* client.answerCallbackQuery(callbackQueryId, "Model set.");
      yield* client.editMessageText(chatId, messageId, confirmation(harnessName, label, model, null), { format: "plain" });
      return;
    }
    const pick: ModelPick = {
      model,
      label,
      efforts: efforts.filter((effort): effort is string => typeof effort === "string"),
      defaultEffort: stringField(payload, "defaultEffort") ?? null,
    };
    const panel = yield* keepPanel(action.conversationId, buildEffortPanel({ harnessName, payload: pick }));
    yield* client.answerCallbackQuery(callbackQueryId, "Now choose an effort.");
    yield* client.editMessageText(chatId, messageId, panel.text, panel.options);
    return;
  }
  if (kind === "effort" && isHarnessName(harnessName) && model !== undefined && label !== undefined) {
    const effort = stringField(payload, "effort");
    yield* store.setModelChoice(action.conversationId, harnessName, { model, effort });
    yield* client.answerCallbackQuery(callbackQueryId, "Model set.");
    yield* client.editMessageText(chatId, messageId, confirmation(harnessName, label, model, effort), { format: "plain" });
    return;
  }
  yield* client.answerCallbackQuery(callbackQueryId, "Unknown action.");
});
