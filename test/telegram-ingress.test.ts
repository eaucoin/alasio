import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import type { Message } from "@grammyjs/types";
import { Array as Arr, Effect, Fiber, Layer, Logger } from "effect";

import { StoreError } from "../src/persistence/sql.ts";
import { Store } from "../src/persistence/store.ts";
import { TelegramTransportError } from "../src/telegram/client.ts";
import { ReceivedFiles } from "../src/telegram/files.ts";
import { handleMessage, TELEGRAM_BOT_FILE_LIMIT_BYTES } from "../src/telegram/message-handler.ts";
import { consumeUpdates, TelegramUpdates, type UpdateFeed } from "../src/telegram/update-consumer.ts";
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

test("updates are taken from the topic in turn, the group told of each once it is handled, past one whose handling throws", async () => {
  const seen: number[] = [];
  const committed: number[] = [];
  const errors: string[] = [];
  const records = [{ update_id: 100 }, { update_id: 101 }, null].map((update, index) => ({
    update,
    position: `telegram.main/0/${index}`,
    commit: async () => {
      committed.push(index);
    },
  }));
  const { promise: drained, resolve: drain } = Promise.withResolvers<void>();
  const feed: UpdateFeed = {
    next: async () => records.shift() ?? (drain(), await new Promise<never>(() => {})), // a wait that lasts until consuming stops
    close: async () => {},
  };
  const logged = Logger.layer([Logger.make(({ message }) => errors.push(Arr.ensure(message).join(" ")))]);
  const consumer = Effect.runFork(consumeUpdates((update) =>
    Effect.suspend(() => {
      seen.push(update.update_id);
      return update.update_id === 100 ? Effect.die(new Error("poison")) : Effect.void;
    })
  ).pipe(Effect.provide([Layer.succeed(TelegramUpdates, TelegramUpdates.of({ join: Effect.succeed(feed) })), logged])));
  await drained;
  await Effect.runPromise(Fiber.interrupt(consumer));
  assert.deepEqual(seen, [100, 101]);
  assert.deepEqual(committed, [0, 1, 2]);
  assert.equal(errors.length, 2);
  assert.match(errors[0] ?? "", /Skipping update 100 after processing failure: Error: poison/);
  assert.match(errors[1] ?? "", /Skipping telegram\.main\/0\/2, which is no update of Telegram's/);
});

test("a member of the group that fails is left, and the group joined again", async () => {
  let joins = 0;
  const { promise: rejoined, resolve: rejoin } = Promise.withResolvers<void>();
  const consumer = Effect.runFork(consumeUpdates(() => Effect.void).pipe(Effect.provide([
    Layer.succeed(TelegramUpdates, TelegramUpdates.of({
      join: Effect.suspend(() => {
        joins += 1;
        if (joins === 2) rejoin();
        return Effect.succeed({ next: async () => { throw new Error("the broker went away"); }, close: async () => {} } satisfies UpdateFeed);
      }),
    })),
    Logger.layer([]),
  ])));
  await rejoined;
  await Effect.runPromise(Fiber.interrupt(consumer));
  assert.equal(joins, 2);
});

test("an update the store could not keep is not told to the group, but handed over again once it is joined again, and handled", async () => {
  const topic = [{ update_id: 200 }, { update_id: 201 }];
  // Each update handed over, with how many the group had been told of by then.
  const seen: [number, number][] = [];
  const committed: number[] = [];
  let joins = 0;
  let storeDown = true;
  const { promise: drained, resolve: drain } = Promise.withResolvers<void>();
  const consumer = Effect.runFork(consumeUpdates((update) =>
    Effect.suspend(() => {
      seen.push([update.update_id, committed.length]);
      if (!storeDown) return Effect.void;
      storeDown = false;
      return Effect.fail(new StoreError({ cause: new Error("the store is down") }));
    })
  ).pipe(Effect.provide([
    Layer.succeed(TelegramUpdates, TelegramUpdates.of({
      // Each member reads from where the group was last told, as the log's does.
      join: Effect.sync(() => {
        joins += 1;
        let at = committed.length;
        return {
          next: async () => {
            const update = topic[at];
            if (!update) return (drain(), await new Promise<never>(() => {})); // a wait that lasts until consuming stops
            const index = at++;
            return { update, position: `telegram.main/0/${index}`, commit: async () => { committed.push(index); } };
          },
          close: async () => {},
        } satisfies UpdateFeed;
      }),
    })),
    Logger.layer([]),
  ])));
  await drained;
  await Effect.runPromise(Fiber.interrupt(consumer));
  assert.equal(joins, 2);
  assert.deepEqual(seen, [[200, 0], [200, 0], [201, 1]]);
  assert.deepEqual(committed, [0, 1]);
});

test("an update kept and processed before is known as such, and one kept but not processed is not", async () => {
  const store = await testStore();
  const update = { update_id: 77, message: { message_id: 1, date: 0, chat: CHAT, from: OPERATOR, text: "hi" } } as const;
  assert.equal(await run(store.recordTelegramUpdate(update)), true);
  assert.equal(await run(store.recordTelegramUpdate(update)), true);
  await run(store.markTelegramUpdateProcessed(77));
  assert.equal(await run(store.recordTelegramUpdate(update)), false);
});
