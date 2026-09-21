function parseAllowedUserIds(raw) {
  return new Set(
    String(raw ?? "")
      .split(",")
      .map((value) => value.trim())
      .filter(Boolean),
  );
}

export class Authorizer {
  constructor({ allowedUserIds, store, log }) {
    this.allowedUserIds = parseAllowedUserIds(allowedUserIds);
    this.store = store;
    this.log = log;
  }

  isAuthorizedMessage(message) {
    return this.isAuthorizedPrivateUpdate({
      chat: message.chat,
      from: message.from,
      allowBootstrap: true,
    });
  }

  isAuthorizedCallbackQuery(callbackQuery) {
    return this.isAuthorizedPrivateUpdate({
      chat: callbackQuery.message?.chat,
      from: callbackQuery.from,
      allowBootstrap: false,
    });
  }

  isAuthorizedPrivateUpdate({ chat, from, allowBootstrap }) {
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
