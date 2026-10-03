/**
 * A TelegramClient for a unit test, recording the calls alasio makes with what it passes
 * them; a call the test does not expect fails as Telegram would refuse it.
 */
import type { Message } from "@grammyjs/types";
import { Effect, Layer } from "effect";

import { TelegramClient, TelegramTransportError } from "../../src/telegram/client.ts";

type Service = TelegramClient["Service"];

/** The calls recorded, by method, each as the arguments it was made with. */
export interface TelegramCalls {
  readonly sendMessage: Parameters<Service["sendMessage"]>[];
  readonly editMessageText: Parameters<Service["editMessageText"]>[];
  readonly answerCallbackQuery: Parameters<Service["answerCallbackQuery"]>[];
  readonly deleteMessage: Parameters<Service["deleteMessage"]>[];
  readonly downloadTelegramFile: Parameters<Service["downloadTelegramFile"]>[];
}

/** The recording client, its calls, and it as a layer. */
export interface RecordingTelegram {
  readonly client: Service;
  readonly calls: TelegramCalls;
  readonly layer: Layer.Layer<TelegramClient>;
}

/** A call the test's client was not given, failing as a refused call does. */
const unused = (method: string) => Effect.fail(TelegramTransportError.of(method, new Error(`the test's Telegram client makes no ${method} call`)));

/** A message as Telegram answers a sendMessage with it. */
export function sentMessage(chatId: string | number, messageId = 77): Message {
  return { message_id: messageId, date: 0, chat: { id: Number(chatId), type: "private", first_name: "Operator" } };
}

/**
 * A client recording sendMessage, editMessageText, answerCallbackQuery, deleteMessage and
 * downloadTelegramFile, answering as Telegram does when each succeeds, or as `answers`
 * says; the rest it does not make.
 */
export function recordingTelegram(answers: Partial<Pick<Service, keyof TelegramCalls>> = {}): RecordingTelegram {
  const calls: TelegramCalls = { sendMessage: [], editMessageText: [], answerCallbackQuery: [], deleteMessage: [], downloadTelegramFile: [] };
  const client = TelegramClient.of({
    call: (method) => unused(method),
    callMultipart: (method) => unused(method),
    getMe: unused("getMe"),
    deleteWebhook: () => unused("deleteWebhook"),
    setMyCommands: () => unused("setMyCommands"),
    setChatMenuButton: () => unused("setChatMenuButton"),
    getUpdates: () => unused("getUpdates"),
    sendMessage: (...args) =>
      Effect.suspend(() => {
        calls.sendMessage.push(args);
        return answers.sendMessage?.(...args) ?? Effect.succeed([sentMessage(args[0])]);
      }),
    editMessageText: (...args) =>
      Effect.suspend(() => {
        calls.editMessageText.push(args);
        return answers.editMessageText?.(...args) ?? Effect.succeed(true);
      }),
    deleteMessage: (...args) =>
      Effect.suspend(() => {
        calls.deleteMessage.push(args);
        return answers.deleteMessage?.(...args) ?? Effect.succeed(true);
      }),
    answerCallbackQuery: (...args) =>
      Effect.suspend(() => {
        calls.answerCallbackQuery.push(args);
        return answers.answerCallbackQuery?.(...args) ?? Effect.succeed(true);
      }),
    getFile: () => unused("getFile"),
    downloadTelegramFile: (...args) =>
      Effect.suspend(() => {
        calls.downloadTelegramFile.push(args);
        return answers.downloadTelegramFile?.(...args) ?? unused("downloadTelegramFile");
      }),
    sendDocument: () => unused("sendDocument"),
  });
  return { client, calls, layer: Layer.succeed(TelegramClient, client) };
}
