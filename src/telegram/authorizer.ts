import type { CallbackQuery, Chat, Message, User } from "@grammyjs/types";
import { Context, Effect, Layer } from "effect";

import type { StoreError } from "../persistence/sql.ts";
import { Store } from "../persistence/store.ts";
import { withLogScope } from "../shared/log.ts";

/** Who an update came from and where, and whether it may claim an empty allowlist. */
export interface PrivateUpdate {
  readonly chat: Chat | undefined;
  readonly from: User | undefined;
  readonly allowBootstrap: boolean;
}

/** The state key of the operator an empty allowlist bootstrapped. */
const BOOTSTRAP_USER = "telegram_bootstrap_user_id";

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
 * their private chat with the bot. With none named, the first user to message the bot
 * privately becomes its operator, kept in the store; a button press claims nothing.
 */
export class Authorizer extends Context.Service<Authorizer, {
  readonly isAuthorizedMessage: (message: Message) => Effect.Effect<boolean, StoreError>;
  readonly isAuthorizedCallbackQuery: (callbackQuery: CallbackQuery) => Effect.Effect<boolean, StoreError>;
}>()("alasio/telegram/Authorizer") {
  static readonly layer = (allowedUserIds: string): Layer.Layer<Authorizer, never, Store> =>
    Layer.effect(Authorizer, Effect.map(Store, (store) => {
      const allowed = parseAllowedUserIds(allowedUserIds);

      const isAuthorizedPrivateUpdate = Effect.fnUntraced(function*({ chat, from, allowBootstrap }: PrivateUpdate): Effect.fn.Return<boolean, StoreError> {
        if (chat?.type !== "private") {
          return false;
        }
        const userId = from?.id == null ? "" : String(from.id);
        const chatId = chat.id == null ? "" : String(chat.id);
        if (!userId || chatId !== userId) {
          return false;
        }
        if (allowed.size > 0) {
          return allowed.has(userId);
        }
        const operator = yield* store.getState(BOOTSTRAP_USER);
        if (operator !== null || !allowBootstrap) {
          return operator === userId;
        }
        // The first user to ask becomes the operator, however many ask at once.
        const claimed = yield* store.claimState(BOOTSTRAP_USER, userId);
        if (claimed === userId) {
          yield* Effect.logInfo(`Bootstrapped Telegram operator user id ${userId}`).pipe(withLogScope("telegram-app"));
        }
        return claimed === userId;
      });

      return Authorizer.of({
        isAuthorizedMessage: (message) => isAuthorizedPrivateUpdate({ chat: message.chat, from: message.from, allowBootstrap: true }),
        isAuthorizedCallbackQuery: (callbackQuery) =>
          isAuthorizedPrivateUpdate({ chat: callbackQuery.message?.chat, from: callbackQuery.from, allowBootstrap: false }),
      });
    }));
}
