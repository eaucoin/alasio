import { harnessDisplayName } from "../harness/index.js";
import { truncateText } from "./text.js";

// /model: choose a model for the mounted service, then an effort that model
// supports. The two services have different catalogues and effort scales, so
// both steps are built from what the service itself reports rather than a
// fixed list. The choice is stored per conversation and per service and takes
// effect on the next turn.

const MODEL_KIND_PREFIX = "model:";
const BUTTONS_PER_ROW = 2;

function modelKind(kind) {
  return `${MODEL_KIND_PREFIX}${kind}`;
}

export function isModelControlAction(kind) {
  return typeof kind === "string" && kind.startsWith(MODEL_KIND_PREFIX);
}

function button(store, conversationId, text, kind, payload) {
  return {
    text,
    callback_data: store.createCallbackAction({ conversationId, kind: modelKind(kind), payload }),
  };
}

function rows(buttons, perRow = BUTTONS_PER_ROW) {
  const result = [];
  for (let index = 0; index < buttons.length; index += perRow) {
    result.push(buttons.slice(index, index + perRow));
  }
  return result;
}

function describeChoice(choice) {
  if (!choice?.model) {
    return "none";
  }
  return choice.effort ? `${choice.model} at ${choice.effort} effort` : choice.model;
}

function panelOptions(keyboard) {
  return { format: "plain", reply_markup: { inline_keyboard: keyboard } };
}

/** Step one: every model the mounted service offers. */
export async function buildModelPanel({ store, harness, conversationId }) {
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

export async function sendModelPanel({ client, store, harness, conversationId, chatId }) {
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
function buildEffortPanel({ store, conversationId, harnessName, payload }) {
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

function confirmation(harnessName, label, model, effort) {
  const at = effort ? ` at ${effort} effort` : "";
  return `${harnessDisplayName(harnessName)} will use ${label} (${model})${at} from the next turn.`;
}

export async function handleModelControlCallback({ client, store, action, callbackQueryId, chatId, messageId }) {
  const kind = action.kind.slice(MODEL_KIND_PREFIX.length);
  const payload = action.payload ?? {};
  // The store also stamps expectedHarness, which the callback handler uses to
  // reject a panel left open across a /service switch.
  const harnessName = payload.harness;
  if (kind === "close") {
    await client.answerCallbackQuery(callbackQueryId, "Closed.");
    await client.editMessageText(chatId, messageId, "Closed.", { format: "plain" }).catch(() => undefined);
    return;
  }
  if (kind === "reset") {
    store.clearModelChoice(action.conversationId, harnessName);
    const text = `${harnessDisplayName(harnessName)} is back on its default model from the next turn.`;
    await client.answerCallbackQuery(callbackQueryId, "Reset.");
    await client.editMessageText(chatId, messageId, text, { format: "plain" });
    return;
  }
  if (kind === "pick") {
    if (!Array.isArray(payload.efforts) || payload.efforts.length === 0) {
      // A model without effort control is chosen outright.
      store.setModelChoice(action.conversationId, harnessName, { model: payload.model, effort: null });
      await client.answerCallbackQuery(callbackQueryId, "Model set.");
      await client.editMessageText(chatId, messageId, confirmation(harnessName, payload.label, payload.model, null), { format: "plain" });
      return;
    }
    const panel = buildEffortPanel({ store, conversationId: action.conversationId, harnessName, payload });
    await client.answerCallbackQuery(callbackQueryId, "Now choose an effort.");
    await client.editMessageText(chatId, messageId, panel.text, panel.options);
    return;
  }
  if (kind === "effort") {
    store.setModelChoice(action.conversationId, harnessName, { model: payload.model, effort: payload.effort });
    await client.answerCallbackQuery(callbackQueryId, "Model set.");
    await client.editMessageText(chatId, messageId, confirmation(harnessName, payload.label, payload.model, payload.effort), { format: "plain" });
    return;
  }
  await client.answerCallbackQuery(callbackQueryId, "Unknown action.");
}
