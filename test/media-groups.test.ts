/**
 * Albums as every alasio on the store handles them (src/telegram/media-group-buffer.ts, the
 * store's claimMediaGroup): claimed under their chat's handling lock, once, only once their
 * last message has been quiet for a moment, and left waiting where the one handling them
 * stops before it is through; due by their last message, not their first; their prompts run
 * once their chat's lock is let go of, their files written by their turn's host, its chat told
 * where they could not be; and a message that comes once its album was handled told of in its
 * chat.
 */
import assert from "node:assert/strict";
import { tmpdir } from "node:os";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";

import { Deferred, Duration, Effect, Exit, Fiber } from "effect";
import pg from "pg";

import { StoreError } from "../src/persistence/sql.ts";
import type { Harness } from "../src/harness/index.ts";
import { CODEX_HARNESS, harnessDisplayName } from "../src/harness/names.ts";
import { Store } from "../src/persistence/store.ts";
import { LATE_ALBUM_PART, MediaGroups } from "../src/telegram/media-group-buffer.ts";
import { POOLED_SCHEMA, pooledStore, poolOf } from "./support/pools.ts";
import { newSchema, run, testDatabaseUrl, testPool, testStore } from "./support/store.ts";
import { recordingOutbox, turnsStub, withServices } from "./support/turns.ts";

const CHAT = 42;
const QUIET_MS = 1500;

/** A store whose chat has an album of `messages` messages, kept `agoMs` ago, the album waiting. */
async function albumWaiting(messages: number, agoMs: number, schema = newSchema()): Promise<{ readonly store: Store["Service"]; readonly schema: string; readonly conversationId: string }> {
  const store = await testStore({ schema });
  const conversationId = await run(store.upsertConversation({ chatId: CHAT, user: { id: CHAT } }));
  for (let n = 1; n <= messages; n += 1) {
    const messageId = await run(store.insertMessage({ conversationId, direction: "in", kind: "file", transportMessageId: n, mediaGroupId: "album", text: n === 1 ? "caption" : null }));
    const content = Buffer.from(`photo ${n}`);
    await run(store.insertFile({ conversationId, messageId, file: { kind: "photo", file_id: `photo-${n}`, file_unique_id: `photo-${n}`, width: 1, height: 1, file_name: `photo-${n}.jpg`, mime_type: "image/jpeg" }, name: `photo-${n}.jpg`, content, sha256: `sha-${n}` }));
  }
  await run(store.upsertMediaGroup({ mediaGroupId: "album", conversationId, updateId: 1 }));
  await kept(schema, agoMs);
  return { store, schema, conversationId };
}

/** Has the album's messages, and the album, kept `agoMs` ago. */
async function kept(schema: string, agoMs: number): Promise<void> {
  const pool = await testPool();
  await pool.query(`update ${schema}.messages set created_at = now() - make_interval(secs => $1) where media_group_id = 'album'`, [agoMs / 1000]);
  await pool.query(`update ${schema}.media_groups set created_at = now() - make_interval(secs => $1) where id = 'album'`, [agoMs / 1000]);
}

const claim = (store: Store["Service"]) => store.withHandlingLock(CHAT, (marks) => marks.claimMediaGroup("album", QUIET_MS));

test("an album is claimed once of two alasios that would handle it at once, under its chat's handling lock", async () => {
  const { store, schema } = await albumWaiting(2, 10_000);
  const other = await testStore({ schema });
  const claims = await Effect.runPromise(Effect.all([claim(store), claim(other)], { concurrency: "unbounded" }));
  assert.deepEqual(claims.toSorted(), [false, true]);
  assert.equal(await run(claim(store)), false, "handled already");
});

test("an album whose last message was kept within its moment is not claimed, and is once it has been quiet for it", async () => {
  const { store, schema } = await albumWaiting(2, 10_000);
  // Another message of it, kept just now, as by another alasio its chat's updates went to.
  await (await testPool()).query(`update ${schema}.messages set created_at = now() where media_group_id = 'album' and transport_message_id = '2'`);
  assert.equal(await run(claim(store)), false);
  assert.deepEqual((await run(store.getPendingMediaGroupsDue(QUIET_MS))).map(({ id }) => id), [], "not due by its first message alone");
  await kept(schema, 2_000);
  assert.deepEqual((await run(store.getPendingMediaGroupsDue(QUIET_MS))).map(({ id }) => id), ["album"]);
  assert.equal(await run(claim(store)), true);
  assert.deepEqual(await run(store.getPendingMediaGroupsDue(QUIET_MS)), []);
});

test("an album whose handling stops before it is through is left waiting, its claim rolled back with its chat's lock, and is due again", async () => {
  const { store } = await albumWaiting(2, 10_000);
  const claimed = Effect.runSync(Deferred.make<void>());
  const handling = Effect.runFork(store.withHandlingLock(CHAT, (marks) =>
    marks.claimMediaGroup("album", QUIET_MS).pipe(
      Effect.tap(() => Deferred.succeed(claimed, undefined)),
      // Its files written, or its prompt queued, as the alasio handling it stops.
      Effect.andThen(Effect.never),
    )
  ));
  await Effect.runPromise(Deferred.await(claimed));
  await Effect.runPromise(Fiber.interrupt(handling));
  assert.ok(Exit.isFailure(await Effect.runPromise(Fiber.await(handling))));
  assert.deepEqual((await run(store.getPendingMediaGroupsDue(QUIET_MS))).map(({ id }) => id), ["album"]);
  assert.equal(await run(claim(store)), true);
});

/** The prompts alasio's turns were given, as a stand-in for them records each, holding each until `release` resolves, where it is given. */
function promptsTaken(release?: Promise<void>) {
  const prompts: string[] = [];
  const turns = turnsStub({
    submit: ({ prompt }) => Effect.promise(async () => {
      prompts.push(prompt);
      await release;
      return { kind: "queued" as const, jobId: `job-${prompts.length}` } as never;
    }),
  });
  return { prompts, turns };
}

/** The texts the outbox of the store in `schema` holds, in order. */
async function queued(schema: string): Promise<string[]> {
  return (await (await testPool()).query<{ text: string }>(`select text from ${schema}.telegram_outbox order by seq`)).rows.map(({ text }) => text);
}

/** Has the conversation of the album's chat work on Codex in a session, as one set up does. */
async function setUp(store: Store["Service"], conversationId: string): Promise<void> {
  await run(store.setActiveHarness(conversationId, "codex"));
  await run(store.setWorkingDirectory(conversationId, "sessionfs:fs-abc123"));
}

/** Codex, its turns recorded in `ran` as they run, none of which a test expects to run; nothing else of it used. */
function refusingHarness(ran: string[]): Harness {
  const unused = (name: string) => () => assert.fail(`Codex here has no ${name}`);
  return {
    name: CODEX_HARNESS,
    displayName: harnessDisplayName(CODEX_HARNESS),
    supportsGoals: false,
    supportsWarmup: false,
    sessions: {
      listSessions: unused("listSessions"),
      getTotalSessionPages: unused("getTotalSessionPages"),
      getSessionByNumber: unused("getSessionByNumber"),
      getSessionLastMessage: unused("getSessionLastMessage"),
      listSessionMessages: unused("listSessionMessages"),
      getTotalRewindPages: unused("getTotalRewindPages"),
      createForkedSession: unused("createForkedSession"),
    },
    startFreshSession: unused("startFreshSession"),
    warmSession: () => Effect.succeed(false),
    runTurn: (params) => Effect.andThen(Effect.sync(() => void ran.push(params.prompt)), Effect.die(new Error("its turn ran"))),
    listModels: unused("listModels"),
    defaultModelChoice: unused("defaultModelChoice"),
  };
}

const flushDue = Effect.flatMap(MediaGroups, (groups) => groups.flushDue);

/** Waits until `done` says so, failing past 10 s. */
async function until(what: string, done: () => boolean | Promise<boolean>): Promise<void> {
  for (const deadline = Date.now() + 10_000; !(await done()); await sleep(20)) {
    if (Date.now() > deadline) assert.fail(what);
  }
}

test("an album two alasios both look for as it comes due is one prompt, of its caption and every file, run by one of them", async () => {
  const { store, schema, conversationId } = await albumWaiting(2, 10_000);
  await setUp(store, conversationId);
  const other = await testStore({ schema });
  const { prompts, turns } = promptsTaken();
  await withServices({ store, turns, workspaceRoot: tmpdir() }, async (one) => {
    await withServices({ store: other, turns, workspaceRoot: tmpdir() }, async (another) => {
      await Promise.all([one.runPromise(flushDue), another.runPromise(flushDue)]);
      await until("the album's prompt runs", () => prompts.length > 0);
      await sleep(200);
    });
  });
  assert.equal(prompts.length, 1);
  assert.match(prompts[0] ?? "", /^caption[^]*photo-1\.jpg[^]*photo-2\.jpg/u);
  assert.deepEqual(await run(store.getPendingMediaGroupsDue(QUIET_MS)), []);
});

test("an album's prompt runs once its chat's handling lock is let go of, so that a prompt that takes a while holds back none of its chat's updates", async () => {
  const { store, conversationId } = await albumWaiting(2, 10_000);
  await setUp(store, conversationId);
  const release = Promise.withResolvers<void>();
  const { prompts, turns } = promptsTaken(release.promise);
  try {
    await withServices({ store, turns, workspaceRoot: tmpdir() }, async (alasio) => {
      await alasio.runPromise(flushDue);
      await until("the album's prompt runs", () => prompts.length > 0);
      // The prompt still runs, and the chat's next update takes its lock at once.
      const took = await Effect.runPromise(store.withHandlingLock(CHAT, () => Effect.void).pipe(Effect.timeoutOption(Duration.seconds(2))));
      assert.ok(took._tag === "Some", "the chat's handling lock is held as the album's prompt runs");
      release.resolve();
    });
  } finally {
    release.resolve();
  }
});

test("an album whose files its turn's host cannot write where its harness reads them runs no turn, its chat told which files and why", async () => {
  const store = await pooledStore();
  const conversationId = await run(store.upsertConversation({ chatId: CHAT, user: { id: CHAT } }));
  await setUp(store, conversationId);
  for (let n = 1; n <= 2; n += 1) {
    const messageId = await run(store.insertMessage({ conversationId, direction: "in", kind: "file", transportMessageId: n, mediaGroupId: "album", text: n === 1 ? "caption" : null }));
    await run(store.insertFile({ conversationId, messageId, file: { kind: "photo", file_id: `photo-${n}`, file_unique_id: `photo-${n}`, width: 1, height: 1, file_name: `photo-${n}.jpg`, mime_type: "image/jpeg" }, name: `photo-${n}.jpg`, content: Buffer.from(`photo ${n}`), sha256: `sha-${n}` }));
  }
  await run(store.upsertMediaGroup({ mediaGroupId: "album", conversationId, updateId: 1 }));
  await poolOf(store).query(`update ${POOLED_SCHEMA}.messages set created_at = now() - interval '10 seconds' where media_group_id = 'album'`);
  await poolOf(store).query(`update ${POOLED_SCHEMA}.media_groups set created_at = now() - interval '10 seconds' where id = 'album'`);
  const outbox = recordingOutbox();
  const ran: string[] = [];
  // The session's files reached through the sandbox gateway, whose front writes none of them here: so its host cannot write the album's files into it.
  await withServices({ store, outbox: outbox.layer, harnesses: { [CODEX_HARNESS]: refusingHarness(ran) }, workspaceRoot: tmpdir() }, async (alasio) => {
    await alasio.runPromise(flushDue);
    await until("its chat told", () => outbox.queued.texts.some(({ text }) => text.startsWith("The files this prompt carried")));
  });
  assert.deepEqual(ran, [], "its turn ran its harness");
  const [told] = outbox.queued.texts.filter(({ text }) => text.startsWith("The files this prompt carried"));
  assert.match(told?.text ?? "", /^The files this prompt carried could not be written where Codex reads them, so its prompt was not run: writing \/home\/agent\/\.alasio\/received\/[0-9a-f-]+\/photo-1\.jpg into session fs-abc123 failed: .+\. Send it again\.$/u);
});

test("a message of an album that comes once the album was handled is told of in its chat, not dropped without a word, and starts no wait of its own", async () => {
  const { store, schema, conversationId } = await albumWaiting(2, 10_000);
  assert.equal(await run(claim(store)), true);
  const { prompts, turns } = promptsTaken();
  await withServices({ store, turns, workspaceRoot: tmpdir() }, async (alasio) => {
    await alasio.runPromise(Effect.flatMap(MediaGroups, (groups) => Effect.flatMap(Store, (store) => groups.buffer({ mediaGroupId: "album", conversationId, updateId: 3, chatId: CHAT }, store))));
    await sleep(QUIET_MS + 500);
  });
  assert.deepEqual(prompts, []);
  assert.deepEqual(await queued(schema), [LATE_ALBUM_PART]);
});

test("an album its store fails to handle once its chat's lock is had, however often, is not given up on, and is handled once the store answers", { timeout: 60_000 }, async () => {
  const { store, schema, conversationId } = await albumWaiting(2, 10_000);
  await setUp(store, conversationId);
  // The store failing as the album's messages are read under the lock, look after look.
  const LOOKS = 7;
  let failing = LOOKS;
  const failingStore: Store["Service"] = {
    ...store,
    getMediaGroupMessages: (mediaGroupId) => (failing-- > 0 ? Effect.fail(new StoreError({ cause: new Error("Neon is away") })) : store.getMediaGroupMessages(mediaGroupId)),
  };
  const { prompts, turns } = promptsTaken();
  await withServices({ store: failingStore, turns, workspaceRoot: tmpdir() }, async (alasio) => {
    for (let look = 0; look < LOOKS; look += 1) await alasio.runPromise(flushDue);
    assert.deepEqual(prompts, []);
    assert.deepEqual((await run(store.getPendingMediaGroupsDue(QUIET_MS))).map(({ id }) => id), ["album"], "still waiting");
    await alasio.runPromise(flushDue);
    await until("the album's prompt runs", () => prompts.length > 0);
  });
  assert.equal(prompts.length, 1);
  assert.deepEqual(await queued(schema), [], "its chat told nothing of its files");
});

test("albums in more chats than the pool has connections, slow to read under their chats' locks, hold at most two of its connections, so the store still answers", { timeout: 60_000 }, async () => {
  const schema = newSchema();
  // A pool of three, fewer than the chats: a flush for each at once, each holding its chat's lock, would starve the rest.
  const pool = new pg.Pool({ connectionString: await testDatabaseUrl(), max: 3 });
  try {
    const store = await Effect.runPromise(Effect.provide(Store, Store.layer({ pool, schema })));
    const chats = [201, 202, 203, 204, 205, 206];
    const conversations = new Map<number, string>();
    for (const chat of chats) {
      const conversationId = await run(store.upsertConversation({ chatId: chat, user: { id: chat } }));
      conversations.set(chat, conversationId);
      await setUp(store, conversationId);
      const messageId = await run(store.insertMessage({ conversationId, direction: "in", kind: "file", transportMessageId: 1, mediaGroupId: `album-${chat}`, text: "caption" }));
      await run(store.insertFile({ conversationId, messageId, file: { kind: "photo", file_id: `photo-${chat}`, file_unique_id: `photo-${chat}`, width: 1, height: 1, file_name: `photo-${chat}.jpg`, mime_type: "image/jpeg" }, name: `photo-${chat}.jpg`, content: Buffer.from("photo"), sha256: `sha-${chat}` }));
      await run(store.upsertMediaGroup({ mediaGroupId: `album-${chat}`, conversationId, updateId: chat }));
    }
    await (await testPool()).query(`update ${schema}.messages set created_at = now() - interval '10 seconds'`);
    const { prompts, turns } = promptsTaken();
    const reading = Effect.runSync(Deferred.make<void>());
    let inFlight = 0;
    let most = 0;
    // Each album's files read slowly, its chat's lock held meanwhile.
    const slow: Store["Service"] = {
      ...store,
      getFilesForMessages: (messageIds) =>
        Effect.acquireUseRelease(
          Effect.sync(() => void (most = Math.max(most, ++inFlight))),
          () => Effect.andThen(Deferred.await(reading), store.getFilesForMessages(messageIds)),
          () => Effect.sync(() => void inFlight--),
        ),
    };
    await withServices({ store: slow, turns, workspaceRoot: tmpdir() }, async (alasio) => {
      for (const chat of chats) {
        await alasio.runPromise(Effect.flatMap(MediaGroups, (groups) => groups.buffer({ mediaGroupId: `album-${chat}`, conversationId: conversations.get(chat) ?? "", updateId: chat, chatId: chat }, store)));
      }
      await until("two albums' files being read", () => inFlight === 2);
      await sleep(500);
      // The store answers, its pool not taken by the albums waiting for their turn.
      assert.equal(await Effect.runPromise(store.getPendingOutboxCount.pipe(Effect.timeout("2 seconds"))), 0);
      assert.equal(most, 2);
      await Effect.runPromise(Deferred.succeed(reading, undefined));
      await until("every album's prompt runs", () => prompts.length === chats.length);
    });
    assert.equal(most, 2);
  } finally {
    await pool.end();
  }
});
