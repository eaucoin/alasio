/**
 * The operator's panels: a message and the buttons under it, sent, edited as a button
 * changes what they show, and closed.
 */
import type { InlineKeyboardMarkup } from "@grammyjs/types";
import { Effect } from "effect";

import type { CallbackAction, NewCallbackAction } from "../persistence/callback-repository.ts";
import type { StoreError } from "../persistence/sql.ts";
import { Store } from "../persistence/store.ts";
import { type ChatId, TelegramClient, type TelegramError, type TextMessageOptions } from "../telegram/client.ts";

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

/** A panel's button before its action is kept: what it says, and the action pressing it takes. */
export interface ButtonDraft extends NewCallbackAction {
  readonly text: string;
}

/** An operator panel before its buttons' actions are kept: its text, and its buttons, row by row. */
export interface PanelDraft {
  readonly text: string;
  readonly keyboard: readonly (readonly ButtonDraft[])[];
}

/** The panel `draft` is of, its buttons' actions kept for the conversation, all in one statement. */
export const keepPanel = (conversationId: string, { text, keyboard }: PanelDraft): Effect.Effect<ControlPanel, StoreError, Store> =>
  Effect.gen(function*() {
    const ids = yield* Effect.flatMap(Store, (store) => store.createCallbackActions(conversationId, keyboard.flat()));
    let next = 0;
    return {
      text,
      options: panelOptions({ inline_keyboard: keyboard.map((row) => row.map((button) => ({ text: button.text, callback_data: ids[next++]! }))) }),
    };
  });

/** A pressed panel button, as the callback handler hands it to a control. */
export interface ControlCallback {
  readonly action: CallbackAction;
  readonly callbackQueryId: string;
  readonly chatId: ChatId;
  readonly messageId: number;
}

/** A panel's options: plain text under `replyMarkup`. */
export function panelOptions(replyMarkup: InlineKeyboardMarkup): ControlPanelOptions {
  return { format: "plain", reply_markup: replyMarkup };
}

/** Sends `panel` to the chat. */
export const sendPanel = (chatId: ChatId, panel: ControlPanel): Effect.Effect<void, TelegramError, TelegramClient> =>
  Effect.flatMap(TelegramClient, (client) => client.sendMessage(chatId, panel.text, panel.options)).pipe(Effect.asVoid);

/** Whether Telegram refused an edit because it would change nothing. */
function isNotModified(error: TelegramError): boolean {
  return error._tag === "TelegramApiError" && error.description?.includes("message is not modified") === true;
}

/** Edits the panel under `messageId` to show `panel`; one already showing it stays as it is. */
export const editPanel = (chatId: ChatId, messageId: number, panel: ControlPanel): Effect.Effect<void, TelegramError, TelegramClient> =>
  Effect.flatMap(TelegramClient, (client) => client.editMessageText(chatId, messageId, panel.text, panel.options)).pipe(
    Effect.asVoid,
    Effect.catchIf(isNotModified, () => Effect.void),
  );

/** Closes the panel a Close button was pressed on: deleted, or, where it cannot be, edited to say so. */
export const closePanel = Effect.fnUntraced(function*(
  { callbackQueryId, chatId, messageId }: Omit<ControlCallback, "action">,
): Effect.fn.Return<void, TelegramError, TelegramClient> {
  const client = yield* TelegramClient;
  yield* client.answerCallbackQuery(callbackQueryId, "Closed.");
  yield* client.deleteMessage(chatId, messageId).pipe(
    Effect.catch(() => client.editMessageText(chatId, messageId, "Closed.", { format: "plain" })),
  );
});
