import type { CallbackQuery, Chat, Message, User } from "@grammyjs/types";
import { Context, Effect, Layer } from "effect";

import { Store } from "../persistence/store.ts";
import { withLogScope } from "../shared/log.ts";

/** Who an update came from and where, and whether it may claim an empty allowlist. */
export interface PrivateUpdate {
  readonly chat: Chat | undefined;
  readonly from: User | undefined;
  readonly allowBootstrap: boolean;
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
 * their private chat with the bot. With none named, the first user to message the bot
 * privately becomes its operator, kept in the store; a button press claims nothing.
 */
export class Authorizer extends Context.Service<Authorizer, {
  readonly isAuthorizedMessage: (message: Message) => Effect.Effect<boolean>;
  readonly isAuthorizedCallbackQuery: (callbackQuery: CallbackQuery) => Effect.Effect<boolean>;
}>()("alasio/telegram/Authorizer") {
  static readonly layer = (allowedUserIds: string): Layer.Layer<Authorizer, never, Store> =>
    Layer.effect(Authorizer, Effect.map(Store, (store) => {
      const allowed = parseAllowedUserIds(allowedUserIds);

      const isAuthorizedPrivateUpdate = ({ chat, from, allowBootstrap }: PrivateUpdate): Effect.Effect<boolean> =>
        Effect.suspend(() => {
          if (chat?.type !== "private") {
            return Effect.succeed(false);
          }
          const userId = from?.id == null ? "" : String(from.id);
          const chatId = chat.id == null ? "" : String(chat.id);
          if (!userId || chatId !== userId) {
            return Effect.succeed(false);
          }
          if (allowed.size > 0) {
            return Effect.succeed(allowed.has(userId));
          }
          const bootstrapUserId = store.getState("telegram_bootstrap_user_id");
          if (bootstrapUserId) {
            return Effect.succeed(bootstrapUserId === userId);
          }
          if (!allowBootstrap) {
            return Effect.succeed(false);
          }
          store.setState("telegram_bootstrap_user_id", userId);
          return Effect.as(Effect.logInfo(`Bootstrapped Telegram operator user id ${userId}`).pipe(withLogScope("telegram-app")), true);
        });

      return Authorizer.of({
        isAuthorizedMessage: (message) => isAuthorizedPrivateUpdate({ chat: message.chat, from: message.from, allowBootstrap: true }),
        isAuthorizedCallbackQuery: (callbackQuery) =>
          isAuthorizedPrivateUpdate({ chat: callbackQuery.message?.chat, from: callbackQuery.from, allowBootstrap: false }),
      });
    }));
}
