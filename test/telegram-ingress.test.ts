import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import type { File, Message } from "@grammyjs/types";

import { SqliteStore } from "../src/persistence/store.ts";
import type { Client, DownloadedFile, GetUpdatesOptions } from "../src/telegram/client.ts";
import { type IncomingPrompt, MessageHandler, type MessageHandlerOptions, TELEGRAM_BOT_FILE_LIMIT_BYTES } from "../src/telegram/message-handler.ts";
import { UpdatePoller } from "../src/telegram/update-poller.ts";

const CHAT = { id: 5, type: "private", first_name: "Operator" } as const;
const OPERATOR = { id: 5, is_bot: false, first_name: "Operator" };

interface FakeClientOptions {
  readonly downloadError?: Error | null;
}

function createClient({ downloadError = null }: FakeClientOptions = {}) {
  const calls: { sendMessage: Parameters<Client["sendMessage"]>[]; downloads: string[] } = { sendMessage: [], downloads: [] };
  return {
    calls,
    async sendMessage(...args: Parameters<Client["sendMessage"]>): Promise<Message[]> {
      calls.sendMessage.push(args);
      return [{ message_id: 1, date: 0, chat: CHAT }];
    },
    async downloadTelegramFile(file: Pick<File, "file_id">): Promise<DownloadedFile> {
      calls.downloads.push(file.file_id);
      if (downloadError) {
        throw downloadError;
      }
      return { localPath: `/tmp/${file.file_id}`, sha256: "abc", remote: { file_id: file.file_id, file_unique_id: file.file_id } };
    },
  };
}

function createHandler(store: SqliteStore, client: MessageHandlerOptions["client"], prompts: IncomingPrompt[]) {
  return new MessageHandler({
    authorizer: { isAuthorizedMessage: () => true },
    client,
    store,
    turns: {
      async processPrompt(args) {
        prompts.push(args);
      },
      async sendNextSetupStep() {
        return false;
      },
    },
    mediaGroups: { buffer() {} },
    log: { warn() {}, error() {} },
  });
}

async function withStore(run: (store: SqliteStore) => Promise<void>) {
  const root = mkdtempSync(join(tmpdir(), "alasio-ingress-"));
  const store = new SqliteStore(root);
  try {
    await run(store);
  } finally {
    store.close();
    rmSync(root, { recursive: true, force: true });
  }
}

test("oversized documents are reported instead of failing the update", async () => {
  await withStore(async (store) => {
    const prompts: IncomingPrompt[] = [];
    const client = createClient();
    const handler = createHandler(store, client, prompts);
    await handler.handle({
      message_id: 10,
      date: 0,
      chat: CHAT,
      from: OPERATOR,
      caption: "look at this",
      document: { file_id: "big", file_unique_id: "big", file_name: "video.mov", file_size: TELEGRAM_BOT_FILE_LIMIT_BYTES + 1 },
    }, 1);
    assert.deepEqual(client.calls.downloads, []);
    assert.match(client.calls.sendMessage[0]?.[1] ?? "", /Could not fetch video\.mov \(20\.0 MB\): Telegram only lets bots download files up to 20 MB/);
    // The caption still reaches the harness without the file.
    assert.equal(prompts.length, 1);
    assert.deepEqual(prompts[0]?.filePaths, []);
    assert.equal(prompts[0]?.text, "look at this");
  });
});

test("Bot API download refusals are reported and a file-only message ends there", async () => {
  await withStore(async (store) => {
    const prompts: IncomingPrompt[] = [];
    const client = createClient({ downloadError: new Error('Telegram getFile failed: HTTP 400 {"description":"Bad Request: file is too big"}') });
    const handler = createHandler(store, client, prompts);
    await handler.handle({
      message_id: 11,
      date: 0,
      chat: CHAT,
      from: OPERATOR,
      document: { file_id: "unknown-size", file_unique_id: "unknown-size", file_name: "dump.bin" },
    }, 2);
    assert.deepEqual(client.calls.downloads, ["unknown-size"]);
    assert.match(client.calls.sendMessage[0]?.[1] ?? "",/Could not fetch dump\.bin: Telegram only lets bots download files up to 20 MB/);
    assert.equal(prompts.length, 0);
  });
});

test("the poller advances past an update whose processing throws", async () => {
  await withStore(async (store) => {
    const seen: number[] = [];
    const errors: string[] = [];
    let batch = 0;
    const poller = new UpdatePoller({
      client: {
        async getUpdates({ offset }: GetUpdatesOptions) {
          batch += 1;
          if (batch === 1) {
            assert.equal(offset, undefined);
            return [{ update_id: 100 }, { update_id: 101 }];
          }
          if (batch === 2) {
            assert.equal(offset, 102);
            void poller.stop();
          }
          return [];
        },
      },
      store,
      processUpdate: async (update) => {
        seen.push(update.update_id);
        if (update.update_id === 100) {
          throw new Error("poison");
        }
      },
      log: { error: (message) => errors.push(message) },
    });
    await poller.start();
    assert.deepEqual(seen, [100, 101]);
    assert.equal(store.getTelegramOffset(), 102);
    assert.equal(errors.length, 1);
    assert.match(errors[0] ?? "",/Skipping update 100 after processing failure: Error: poison/);
  });
});
