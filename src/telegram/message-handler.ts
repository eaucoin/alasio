import type { Message } from "@grammyjs/types";
import type { SqliteStore } from "../persistence/store.ts";
import type { Logger } from "../shared/log.ts";
import type { Authorizer } from "./authorizer.ts";
import type { Client } from "./client.ts";
import type { MediaGroupBuffer } from "./media-group-buffer.ts";
import { type IncomingFile, getMessageFiles, getMessageText } from "./message.ts";

/** Bot API getFile refuses anything larger than this, regardless of plan. */
export const TELEGRAM_BOT_FILE_LIMIT_BYTES = 20 * 1024 * 1024;

// What the handler calls on the turn controller (codex/turn-controller.ts), narrowed to
// what it uses until that module exports its own types.
interface IncomingPrompt {
  conversationId: string;
  chatId: number;
  messageId: number;
  text: string;
  filePaths: string[];
}

interface MessageTurns {
  /** Sends the next step of the conversation's setup, if it has one left; whether it did. */
  sendNextSetupStep(target: { conversationId: string; chatId: number }): Promise<boolean>;
  processPrompt(prompt: IncomingPrompt): Promise<void>;
  harnessLabel?(conversationId: string): string;
}

/** The store's conversations, messages, and files, as the handler records them. */
type MessageStore = Pick<SqliteStore, "upsertConversation" | "insertMessage" | "getSessionId" | "insertFile">;

export interface MessageHandlerOptions {
  authorizer: Pick<Authorizer, "isAuthorizedMessage">;
  client: Pick<Client, "sendMessage" | "downloadTelegramFile">;
  store: MessageStore;
  turns: MessageTurns;
  mediaGroups: Pick<MediaGroupBuffer, "buffer">;
  log: Logger;
}

function describeDownloadFailure(file: IncomingFile, error: unknown): string {
  const name = file.file_name ?? file.kind ?? "file";
  const message = error instanceof Error ? error.message : String(error);
  if ((file.file_size ?? 0) > TELEGRAM_BOT_FILE_LIMIT_BYTES || /file is too big/i.test(message)) {
    const size = file.file_size ? ` (${(file.file_size / (1024 * 1024)).toFixed(1)} MB)` : "";
    return `Could not fetch ${name}${size}: Telegram only lets bots download files up to 20 MB. Put it in the mounted folder yourself, or send a link or a smaller file.`;
  }
  return `Could not fetch ${name}: ${message}`;
}

export class MessageHandler {
  private readonly authorizer: Pick<Authorizer, "isAuthorizedMessage">;
  private readonly client: Pick<Client, "sendMessage" | "downloadTelegramFile">;
  private readonly store: MessageStore;
  private readonly turns: MessageTurns;
  private readonly mediaGroups: Pick<MediaGroupBuffer, "buffer">;
  private readonly log: Logger;

  constructor({ authorizer, client, store, turns, mediaGroups, log }: MessageHandlerOptions) {
    this.authorizer = authorizer;
    this.client = client;
    this.store = store;
    this.turns = turns;
    this.mediaGroups = mediaGroups;
    this.log = log;
  }

  upsertConversationFromMessage(message: Message): string {
    const chatId = message.chat.id;
    return this.store.upsertConversation({ chatId, user: message.from });
  }

  async handle(message: Message, updateId: number): Promise<void> {
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
      if (!(await this.turns.sendNextSetupStep({ conversationId, chatId: message.chat.id }))) {
        await this.client.sendMessage(message.chat.id, "Alasio is ready.");
      }
      return;
    }

    const localPaths: string[] = [];
    const failures: string[] = [];
    for (const file of files) {
      // A file the Bot API refuses must not poison the update: report it and carry on
      // with whatever else the message carried.
      if ((file.file_size ?? 0) > TELEGRAM_BOT_FILE_LIMIT_BYTES) {
        failures.push(describeDownloadFailure(file, new Error("file is too big")));
        continue;
      }
      try {
        const downloaded = await this.client.downloadTelegramFile(file, file.file_name ?? `telegram-${message.message_id}`);
        this.store.insertFile({
          conversationId,
          messageId,
          file,
          localPath: downloaded.localPath,
          sha256: downloaded.sha256,
        });
        localPaths.push(downloaded.localPath);
      } catch (error) {
        this.log.warn(`Download failed for ${file.file_id} in ${conversationId}: ${error instanceof Error ? error.message : String(error)}`);
        failures.push(describeDownloadFailure(file, error));
      }
    }
    if (failures.length > 0) {
      await this.client.sendMessage(message.chat.id, failures.join("\n\n"));
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
    }).catch((error: unknown) => {
      this.log.error(`Failed to process prompt for ${conversationId}: ${error}`);
      const agentName = this.turns.harnessLabel?.(conversationId) ?? "The agent";
      this.client.sendMessage(message.chat.id, `${agentName} hit an error: ${error instanceof Error ? error.message : String(error)}`).catch(() => undefined);
    });
  }
}
