import type { CallbackQuery, Chat, Message, User } from "@grammyjs/types";
import { Context, Effect, Layer } from "effect";

import type { StoreError } from "../persistence/sql.ts";

/** Who an update came from, and where. */
export interface PrivateUpdate {
  readonly chat: Chat | undefined;
  readonly from: User | undefined;
}

function parseAllowedUserIds(raw: string): ReadonlySet<string> {
  return new Set(
    String(raw ?? "")
      .split(",")
      .map((value) => value.trim())
      .filter(Boolean),
  );
}

/**
 * Who may use alasio: the Telegram users `allowedUserIds` names (comma-separated), each in
 * their private chat with the bot, and no one else; with none named, no one. The webhook
 * intake applies the same rule before an update reaches alasio (webhook/src/updates.ts):
 * this is the second check.
 */
export class Authorizer extends Context.Service<Authorizer, {
  readonly isAuthorizedMessage: (message: Message) => Effect.Effect<boolean, StoreError>;
  readonly isAuthorizedCallbackQuery: (callbackQuery: CallbackQuery) => Effect.Effect<boolean, StoreError>;
}>()("alasio/telegram/Authorizer") {
  static readonly layer = (allowedUserIds: string): Layer.Layer<Authorizer> =>
    Layer.sync(Authorizer, () => {
      const allowed = parseAllowedUserIds(allowedUserIds);

      const isAuthorizedPrivateUpdate = ({ chat, from }: PrivateUpdate): boolean => {
        if (chat?.type !== "private") return false;
        const userId = from?.id == null ? "" : String(from.id);
        const chatId = chat.id == null ? "" : String(chat.id);
        return userId !== "" && chatId === userId && allowed.has(userId);
      };

      return Authorizer.of({
        isAuthorizedMessage: (message) => Effect.succeed(isAuthorizedPrivateUpdate({ chat: message.chat, from: message.from })),
        isAuthorizedCallbackQuery: (callbackQuery) => Effect.succeed(isAuthorizedPrivateUpdate({ chat: callbackQuery.message?.chat, from: callbackQuery.from })),
      });
    });
}
