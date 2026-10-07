import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import type { Message } from "@grammyjs/types";
import { Array as Arr, Effect, Fiber, Layer, Logger } from "effect";

import { Store } from "../src/persistence/store.ts";
import { TelegramTransportError } from "../src/telegram/client.ts";
import { ReceivedFiles } from "../src/telegram/files.ts";
import { handleMessage, TELEGRAM_BOT_FILE_LIMIT_BYTES } from "../src/telegram/message-handler.ts";
import { pollUpdates } from "../src/telegram/update-poller.ts";
import { botApiLayer, paramsOf } from "./support/bot-api.ts";
import { run, testStore } from "./support/store.ts";
import { type RecordingTelegram, recordingTelegram } from "./support/telegram-calls.ts";
import { withServices } from "./support/turns.ts";

const CHAT = { id: 5, type: "private", first_name: "Operator" } as const;
const OPERATOR = { id: 5, is_bot: false, first_name: "Operator" };

/** Handles `message` from the operator, as update `updateId`, on `store` and `telegram`: the prompt it carries. */
function handleOperatorMessage(store: Store["Service"], telegram: RecordingTelegram, message: Message, updateId: number) {
  return withServices({ store, telegram: telegram.layer, allowedUserIds: String(OPERATOR.id) }, (alasio) =>
    alasio.runPromise(handleMessage(message, updateId)));
}

test("oversized documents are reported instead of failing the update", async () => {
  const store = await testStore();
  const telegram = recordingTelegram();
  const prompt = await handleOperatorMessage(store, telegram, {
    message_id: 10,
    date: 0,
    chat: CHAT,
    from: OPERATOR,
    caption: "look at this",
    document: { file_id: "big", file_unique_id: "big", file_name: "video.mov", file_size: TELEGRAM_BOT_FILE_LIMIT_BYTES + 1 },
  }, 1);
  assert.deepEqual(telegram.calls.downloadTelegramFile, []);
  assert.match(telegram.calls.sendMessage[0]?.[1] ?? "", /Could not fetch video\.mov \(20\.0 MB\): Telegram only lets bots download files up to 20 MB/);
  // The caption still reaches the harness without the file.
  assert.deepEqual(prompt, { conversationId: "telegram:5", chatId: 5, messageId: 10, text: "look at this", files: [] });
});

test("Bot API download refusals are reported and a file-only message ends there", async () => {
  const store = await testStore();
  const telegram = recordingTelegram({
    downloadTelegramFile: () =>
      Effect.fail(TelegramTransportError.of("getFile", new Error('Telegram getFile failed: HTTP 400 {"description":"Bad Request: file is too big"}'))),
  });
  const prompt = await handleOperatorMessage(store, telegram, {
    message_id: 11,
    date: 0,
    chat: CHAT,
    from: OPERATOR,
    document: { file_id: "unknown-size", file_unique_id: "unknown-size", file_name: "dump.bin" },
  }, 2);
  assert.deepEqual(telegram.calls.downloadTelegramFile.map(([file]) => file.file_id), ["unknown-size"]);
  assert.match(telegram.calls.sendMessage[0]?.[1] ?? "", /Could not fetch dump\.bin: Telegram only lets bots download files up to 20 MB/);
  assert.equal(prompt, null);
});

test("a file a message carries is kept in the store, and written where its prompt names it, again once it is gone", async () => {
  const store = await testStore();
  const stateDir = mkdtempSync(join(tmpdir(), "alasio-ingress-"));
  const content = Buffer.from("a photo");
  const telegram = recordingTelegram({
    downloadTelegramFile: () =>
      Effect.succeed({ name: "photo.jpg", content, sha256: createHash("sha256").update(content).digest("hex"), remote: { file_id: "photo", file_unique_id: "photo" } }),
  });
  try {
    await withServices({ store, telegram: telegram.layer, allowedUserIds: String(OPERATOR.id), stateDir }, async (alasio) => {
      const prompt = await alasio.runPromise(handleMessage({
        message_id: 12,
        date: 0,
        chat: CHAT,
        from: OPERATOR,
        photo: [{ file_id: "photo", file_unique_id: "photo", width: 1, height: 1 }],
      }, 3));
      const [file] = prompt?.files ?? [];
      assert.ok(file);
      assert.deepEqual(readFileSync(file.path), content);
      // alasio's state directory does not outlast its pod.
      rmSync(join(stateDir, "telegram-files"), { recursive: true });
      await alasio.runPromise(Effect.flatMap(ReceivedFiles, (files) => files.materialize([file.id])));
      assert.deepEqual(readFileSync(file.path), content);
    });
  } finally {
    rmSync(stateDir, { recursive: true, force: true });
  }
});

test("the poller advances past an update whose processing throws", async () => {
  const store = await testStore();
  const seen: number[] = [];
  const errors: string[] = [];
  const offsets: (number | undefined)[] = [];
  const { promise: polledAgain, resolve: pollAgain } = Promise.withResolvers<void>();
  const telegram = botApiLayer(async (call) => {
    offsets.push(paramsOf(call, "getUpdates").offset);
    if (offsets.length === 1) return [{ update_id: 100 }, { update_id: 101 }];
    pollAgain();
    return await new Promise<never>(() => {}); // a long poll that lasts until polling stops
  });
  const logged = Logger.layer([Logger.make(({ message }) => errors.push(Arr.ensure(message).join(" ")))]);
  const poller = Effect.runFork(pollUpdates((update) =>
    Effect.suspend(() => {
      seen.push(update.update_id);
      return update.update_id === 100 ? Effect.die(new Error("poison")) : Effect.void;
    })
  ).pipe(Effect.provide([telegram, Layer.succeed(Store, store), logged])));
  await polledAgain;
  await Effect.runPromise(Fiber.interrupt(poller));
  assert.deepEqual(offsets, [undefined, 102]);
  assert.deepEqual(seen, [100, 101]);
  assert.equal(await run(store.getTelegramOffset), 102);
  assert.equal(errors.length, 1);
  assert.match(errors[0] ?? "", /Skipping update 100 after processing failure: Error: poison/);
});
