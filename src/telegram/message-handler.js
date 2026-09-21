import { getMessageFiles, getMessageText } from "./message.js";

export class MessageHandler {
  constructor({ authorizer, client, store, turns, mediaGroups, log }) {
    this.authorizer = authorizer;
    this.client = client;
    this.store = store;
    this.turns = turns;
    this.mediaGroups = mediaGroups;
    this.log = log;
  }

  upsertConversationFromMessage(message) {
    const chatId = message.chat.id;
    return this.store.upsertConversation({ chatId, user: message.from });
  }

  async handle(message, updateId) {
    if (!this.authorizer.isAuthorizedMessage(message)) {
      if (message.chat?.type === "private") {
        await this.client.sendMessage(message.chat.id, "This bot is not authorized for this Telegram user.");
      }
      return;
    }

    const conversationId = this.upsertConversationFromMessage(message);
    const text = getMessageText(message);
    const files = getMessageFiles(message);
    const messageId = this.store.insertMessage({
      conversationId,
      direction: "in",
      kind: files.length > 0 ? "file" : "text",
      transportMessageId: message.message_id,
      text,
      mediaGroupId: message.media_group_id ?? null,
      raw: message,
      sessionId: this.store.getSessionId(conversationId) ?? null,
      turnId: null,
    });

    if (/^\/start(?:@\w+)?(?:\s|$)/i.test(text)) {
      if (this.turns.harnessFor?.(conversationId)) {
        await this.client.sendMessage(message.chat.id, "Alasio is ready.");
      } else {
        await this.turns.sendChooseServicePanel({ conversationId, chatId: message.chat.id });
      }
      return;
    }

    const localPaths = [];
    for (const file of files) {
      const downloaded = await this.client.downloadTelegramFile(file, file.file_name ?? `telegram-${message.message_id}`);
      this.store.insertFile({
        conversationId,
        messageId,
        file,
        localPath: downloaded.localPath,
        sha256: downloaded.sha256,
      });
      localPaths.push(downloaded.localPath);
    }

    if (!text && localPaths.length === 0) {
      return;
    }

    if (message.media_group_id) {
      this.mediaGroups.buffer({
        mediaGroupId: message.media_group_id,
        conversationId,
        updateId,
        chatId: message.chat.id,
      });
      return;
    }

    this.turns.processPrompt({
      conversationId,
      chatId: message.chat.id,
      messageId: message.message_id,
      text,
      filePaths: localPaths,
    }).catch((error) => {
      this.log.error(`Failed to process prompt for ${conversationId}: ${error}`);
      const agentName = this.turns.harnessLabel?.(conversationId) ?? "The agent";
      this.client.sendMessage(message.chat.id, `${agentName} hit an error: ${error instanceof Error ? error.message : String(error)}`).catch(() => undefined);
    });
  }
}
