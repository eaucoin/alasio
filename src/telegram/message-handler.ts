import type { Message } from "@grammyjs/types";
import { Effect } from "effect";

import { harnessLabelOf } from "../harness/index.ts";
import type { OperatorServices } from "../operator/command-handler.ts";
import { type IncomingPrompt, processPrompt, sendNextSetupStep } from "../operator/prompts.ts";
import type { StoreError } from "../persistence/sql.ts";
import { Store } from "../persistence/store.ts";
import { Authorizer } from "./authorizer.ts";
import { TelegramClient, type TelegramError } from "./client.ts";
import { type ReceivedFile, ReceivedFiles } from "./files.ts";
import { MediaGroups } from "./media-group-buffer.ts";
import { type IncomingFile, getMessageFiles, getMessageText } from "./message.ts";

/** Bot API getFile refuses anything larger than this, regardless of plan. */
export const TELEGRAM_BOT_FILE_LIMIT_BYTES = 20 * 1024 * 1024;

/** What a message is handled with. */
export type MessageServices = Authorizer | MediaGroups | OperatorServices;

function describeDownloadFailure(file: IncomingFile, message: string): string {
  const name = file.file_name ?? file.kind ?? "file";
  if ((file.file_size ?? 0) > TELEGRAM_BOT_FILE_LIMIT_BYTES || /file is too big/i.test(message)) {
    const size = file.file_size ? ` (${(file.file_size / (1024 * 1024)).toFixed(1)} MB)` : "";
    return `Could not fetch ${name}${size}: Telegram only lets bots download files up to 20 MB. Put it in the mounted folder yourself, or send a link or a smaller file.`;
  }
  return `Could not fetch ${name}: ${message}`;
}

/**
 * Records a message from Telegram and keeps its files: the prompt it carries, for
 * processIncomingPrompt, or null when it carries none to process now (it was not the
 * operator's, it was /start, it carried nothing, or it is part of an album, which is
 * handled as one prompt once all of it has arrived).
 */
export const handleMessage = Effect.fnUntraced(function*(message: Message, updateId: number): Effect.fn.Return<
  IncomingPrompt | null,
  TelegramError | StoreError,
  MessageServices
> {
  const store = yield* Store;
  const client = yield* TelegramClient;
  const authorizer = yield* Authorizer;
  const receivedFiles = yield* ReceivedFiles;
  if (!(yield* authorizer.isAuthorizedMessage(message))) {
    if (message.chat?.type === "private") {
      yield* client.sendMessage(message.chat.id, "This bot is not authorized for this Telegram user.");
    }
    return null;
  }

  const conversationId = yield* store.upsertConversation({ chatId: message.chat.id, user: message.from });
  const text = getMessageText(message);
  const files = getMessageFiles(message);
  const messageId = yield* store.insertMessage({
    conversationId,
    direction: "in",
    kind: files.length > 0 ? "file" : "text",
    transportMessageId: message.message_id,
    text,
    mediaGroupId: message.media_group_id ?? null,
    raw: message,
    sessionId: (yield* store.getMount(conversationId)).sessionId,
    turnId: null,
  });

  if (/^\/start(?:@\w+)?(?:\s|$)/i.test(text)) {
    if (!(yield* sendNextSetupStep({ conversationId, chatId: message.chat.id }))) {
      yield* client.sendMessage(message.chat.id, "Alasio is ready.");
    }
    return null;
  }

  const received: ReceivedFile[] = [];
  const failures: string[] = [];
  for (const file of files) {
    // A file the Bot API refuses must not poison the update: report it and carry on
    // with whatever else the message carried.
    if ((file.file_size ?? 0) > TELEGRAM_BOT_FILE_LIMIT_BYTES) {
      failures.push(describeDownloadFailure(file, "file is too big"));
      continue;
    }
    const failed = (error: { readonly message: string }) =>
      Effect.logWarning(`Download failed for ${file.file_id} in ${conversationId}: ${error.message}`).pipe(
        Effect.andThen(Effect.sync(() => failures.push(describeDownloadFailure(file, error.message)))),
      );
    yield* client.downloadTelegramFile(file, file.file_name ?? `telegram-${message.message_id}`).pipe(
      Effect.matchEffect({
        onFailure: failed,
        // Kept in the store, or the update fails; then written for the agent to read.
        onSuccess: ({ name, content, sha256 }) =>
          receivedFiles.keep({ conversationId, messageId, file, name, content, sha256 }).pipe(
            Effect.flatMap((kept) => Effect.sync(() => received.push(kept))),
            Effect.catchTag("ReceivedFileError", failed),
          ),
      }),
    );
  }
  if (failures.length > 0) {
    yield* client.sendMessage(message.chat.id, failures.join("\n\n"));
  }

  if (!text && received.length === 0) {
    return null;
  }

  const mediaGroupId = message.media_group_id;
  if (mediaGroupId) {
    yield* Effect.flatMap(MediaGroups, (mediaGroups) => mediaGroups.buffer({ mediaGroupId, conversationId, updateId, chatId: message.chat.id }));
    return null;
  }

  return { conversationId, chatId: message.chat.id, messageId: message.message_id, text, files: received };
});

/** Processes a message's prompt, telling the operator when that fails. */
export const processIncomingPrompt = (prompt: IncomingPrompt): Effect.Effect<void, never, OperatorServices> => {
  const failed = Effect.fnUntraced(function*(error: unknown) {
    yield* Effect.logError(`Failed to process prompt for ${prompt.conversationId}: ${error}`);
    yield* Effect.gen(function*() {
      const agentName = harnessLabelOf(yield* Effect.flatMap(Store, (store) => store.getMount(prompt.conversationId)));
      const client = yield* TelegramClient;
      yield* client.sendMessage(prompt.chatId, `${agentName} hit an error: ${error instanceof Error ? error.message : String(error)}`);
    }).pipe(Effect.ignore);
  });
  return processPrompt(prompt).pipe(Effect.catch(failed), Effect.catchDefect(failed));
};
