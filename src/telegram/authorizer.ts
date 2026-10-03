import type { CallbackQuery, Chat, Message, User } from "@grammyjs/types";

/** The bot state the Authorizer keeps the bootstrapped operator in (SqliteStore). */
interface OperatorStateStore {
  getState(key: string): string | null;
  setState(key: string, value: string): void;
}

interface AuthorizerLog {
  info(message: string): void;
}

export interface AuthorizerOptions {
  /** Comma-separated Telegram user ids; empty lets the first private user bootstrap as the operator. */
  allowedUserIds: string;
  store: OperatorStateStore;
  log: AuthorizerLog;
}

/** Who an update came from and where, and whether it may claim an empty allowlist. */
export interface PrivateUpdate {
  chat: Chat | undefined;
  from: User | undefined;
  allowBootstrap: boolean;
}

function parseAllowedUserIds(raw: string) {
  return new Set(
    String(raw ?? "")
      .split(",")
      .map((value) => value.trim())
      .filter(Boolean),
  );
}

export class Authorizer {
  private readonly allowedUserIds: Set<string>;
  private readonly store: OperatorStateStore;
  private readonly log: AuthorizerLog;

  constructor({ allowedUserIds, store, log }: AuthorizerOptions) {
    this.allowedUserIds = parseAllowedUserIds(allowedUserIds);
    this.store = store;
    this.log = log;
  }

  isAuthorizedMessage(message: Message): boolean {
    return this.isAuthorizedPrivateUpdate({
      chat: message.chat,
      from: message.from,
      allowBootstrap: true,
    });
  }

  isAuthorizedCallbackQuery(callbackQuery: CallbackQuery): boolean {
    return this.isAuthorizedPrivateUpdate({
      chat: callbackQuery.message?.chat,
      from: callbackQuery.from,
      allowBootstrap: false,
    });
  }

  isAuthorizedPrivateUpdate({ chat, from, allowBootstrap }: PrivateUpdate): boolean {
    if (chat?.type !== "private") {
      return false;
    }
    const userId = from?.id == null ? "" : String(from.id);
    const chatId = chat.id == null ? "" : String(chat.id);
    if (!userId || chatId !== userId) {
      return false;
    }
    if (this.allowedUserIds.size > 0) {
      return this.allowedUserIds.has(userId);
    }
    const bootstrapUserId = this.store.getState("telegram_bootstrap_user_id");
    if (bootstrapUserId) {
      return bootstrapUserId === userId;
    }
    if (!allowBootstrap) {
      return false;
    }
    this.store.setState("telegram_bootstrap_user_id", userId);
    this.log.info(`Bootstrapped Telegram operator user id ${userId}`);
    return true;
  }
}
