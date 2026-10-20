import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";

import type { Message } from "@grammyjs/types";
import { Array as Arr, Effect, Fiber, Layer, Logger } from "effect";
import type pg from "pg";

import { StoreError } from "../src/persistence/sql.ts";
import type { SessionSandboxes } from "../src/sandbox/index.ts";
import { Store } from "../src/persistence/store.ts";
import { NeonTelegramContentRepository } from "../src/persistence/telegram-content-repository.ts";
import { TelegramTransportError } from "../src/telegram/client.ts";
import { ReceivedFiles } from "../src/telegram/files.ts";
import { handleMessage, TELEGRAM_BOT_FILE_LIMIT_BYTES } from "../src/telegram/message-handler.ts";
import { consumeUpdates, TelegramUpdates, type UpdateFeed } from "../src/telegram/update-consumer.ts";
import { newSchema, run, testPool, testStore } from "./support/store.ts";
import { type RecordingTelegram, recordingTelegram } from "./support/telegram-calls.ts";
import { withServices } from "./support/turns.ts";

/** A message handled as an update's handling handles it, its marks the store's own. */
const handled = (message: Message, updateId: number) => Effect.flatMap(Store, (store) => handleMessage(message, updateId, store));


const CHAT = { id: 5, type: "private", first_name: "Operator" } as const;
const OPERATOR = { id: 5, is_bot: false, first_name: "Operator" };

/** Handles `message` from the operator, as update `updateId`, on `store` and `telegram`: the prompt it carries. */
function handleOperatorMessage(store: Store["Service"], telegram: RecordingTelegram, message: Message, updateId: number) {
  return withServices({ store, telegram: telegram.layer, allowedUserIds: String(OPERATOR.id) }, (alasio) =>
    alasio.runPromise(handled(message, updateId)));
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
      const prompt = await alasio.runPromise(handled({
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
      await alasio.runPromise(Effect.flatMap(ReceivedFiles, (files) => files.materialize([file.id], null)));
      assert.deepEqual(readFileSync(file.path), content);
    });
  } finally {
    rmSync(stateDir, { recursive: true, force: true });
  }
});

test("a file sent to a conversation on a session workspace is written into its session, under its home, through the session filesystems, and again before its prompt runs; none under alasio's state directory", async () => {
  const store = await testStore();
  const stateDir = mkdtempSync(join(tmpdir(), "alasio-ingress-"));
  const content = Buffer.from("a photo");
  const written: string[] = [];
  const unused = () => Effect.die("only a file is written");
  const sandbox: SessionSandboxes["Service"] = {
    volumes: { create: unused, fork: unused, destroy: unused, list: Effect.die("no workspace is listed") },
    harnessDirectory: () => assert.fail("no harness runs"),
    ensureSession: unused,
    readFile: unused,
    writeFile: (volumeId, path, bytes) => Effect.sync(() => void written.push(`${volumeId} ${path} ${Buffer.from(bytes).toString()}`)),
  };
  const telegram = recordingTelegram({
    downloadTelegramFile: () =>
      Effect.succeed({ name: "photo.jpg", content, sha256: createHash("sha256").update(content).digest("hex"), remote: { file_id: "photo", file_unique_id: "photo" } }),
  });
  try {
    const conversationId = await run(store.upsertConversation({ chatId: CHAT.id, user: OPERATOR }));
    await run(store.setWorkingDirectory(conversationId, "sessionfs:fs-abc123"));
    await withServices({ store, telegram: telegram.layer, allowedUserIds: String(OPERATOR.id), stateDir, sandbox }, async (alasio) => {
      const prompt = await alasio.runPromise(handled({ message_id: 12, date: 0, chat: CHAT, from: OPERATOR, photo: [{ file_id: "photo", file_unique_id: "photo", width: 1, height: 1 }] }, 3));
      const [file] = prompt?.files ?? [];
      assert.ok(file);
      // Where the session's agent reads it, which its prompt names.
      assert.equal(file.path, `/home/agent/.alasio/received/${file.id}/photo.jpg`);
      assert.deepEqual(written, [`fs-abc123 ${file.path} a photo`]);
      assert.equal(existsSync(join(stateDir, "telegram-files")), false);
      // Written again as its prompt runs, as alasio cannot see what the session holds.
      await alasio.runPromise(Effect.flatMap(ReceivedFiles, (files) => files.materialize([file.id], { kind: "sessionfs", volumeId: "fs-abc123" })));
      assert.equal(written.length, 2);
    });
  } finally {
    rmSync(stateDir, { recursive: true, force: true });
  }
});


test("a message handled again, as an update whose handling lock was lost is, is kept once, its file once, and its prompt names the file kept", async () => {
  const schema = newSchema();
  const store = await testStore({ schema });
  const stateDir = mkdtempSync(join(tmpdir(), "alasio-ingress-"));
  const content = Buffer.from("a photo");
  const telegram = recordingTelegram({
    downloadTelegramFile: () =>
      Effect.succeed({ name: "photo.jpg", content, sha256: createHash("sha256").update(content).digest("hex"), remote: { file_id: "photo", file_unique_id: "photo" } }),
  });
  const message: Message = { message_id: 14, date: 0, chat: CHAT, from: OPERATOR, caption: "look", photo: [{ file_id: "photo", file_unique_id: "photo", width: 1, height: 1 }] };
  try {
    await withServices({ store, telegram: telegram.layer, allowedUserIds: String(OPERATOR.id), stateDir }, async (alasio) => {
      const first = await alasio.runPromise(handled(message, 4));
      const again = await alasio.runPromise(handled(message, 4));
      assert.deepEqual(again?.files.map(({ id }) => id), first?.files.map(({ id }) => id));
    });
    const count = async (table: string) => Number((await (await testPool()).query<{ n: string }>(`select count(*) as n from ${schema}.${table}`)).rows[0]?.n);
    assert.deepEqual([await count("messages"), await count("files")], [1, 1]);
  } finally {
    rmSync(stateDir, { recursive: true, force: true });
  }
});

test("a message two alasios keep at once, as a rebalance mid-handling has them, is kept once, the second given the first's", async () => {
  const schema = newSchema();
  await testStore({ schema });
  const pool = await testPool();
  const conversationId = "telegram:42";
  await pool.query(`insert into ${schema}.conversations (id, transport, chat_id) values ($1, 'telegram', '42')`, [conversationId]);
  /** The repository on `client`, as a store's statements run on it. */
  const on = (client: pg.ClientBase | pg.Pool) =>
    new NeonTelegramContentRepository({ query: (text, values = []) => Effect.promise(async () => (await client.query(text, [...values])).rows) }, schema);
  const message = { conversationId, direction: "in" as const, kind: "text" as const, transportMessageId: 9, text: "hi", raw: { message_id: 9, date: 1_000, chat: CHAT, text: "hi" } };
  // The first keeps it in a transaction not yet committed, as its handling goes on.
  const first = await pool.connect();
  try {
    await first.query("begin");
    const kept = await Effect.runPromise(on(first).insertMessage(message));
    const second = Effect.runPromise(on(pool).insertMessage(message));
    await sleep(200);
    await first.query("commit");
    assert.equal(await second, kept);
  } finally {
    first.release();
  }
  assert.equal(Number((await pool.query<{ n: string }>(`select count(*) as n from ${schema}.messages`)).rows[0]?.n), 1);
});

test("a message whose id one sent earlier had, as a branch's bot's has one its parent's state holds, is kept as a message of its own", async () => {
  const schema = newSchema();
  const store = await testStore({ schema });
  const stateDir = mkdtempSync(join(tmpdir(), "alasio-ingress-"));
  try {
    await withServices({ store, allowedUserIds: String(OPERATOR.id), stateDir }, async (alasio) => {
      // The parent's, sent to its own bot before the branch was made; the branch's bot numbers its chat's messages from 1 again.
      await alasio.runPromise(handled({ message_id: 1, date: 1_000, chat: CHAT, from: OPERATOR, text: "hello" }, 1));
      await alasio.runPromise(handled({ message_id: 1, date: 2_000, chat: CHAT, from: OPERATOR, text: "again" }, 1));
    });
    const { rows } = await (await testPool()).query<{ text: string }>(`select text from ${schema}.messages order by created_at`);
    assert.deepEqual(rows.map(({ text }) => text), ["hello", "again"]);
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
