/**
 * Albums as every alasio on the store handles them (src/telegram/media-group-buffer.ts, the
 * store's claimMediaGroup): claimed under their chat's handling lock, once, only once their
 * last message has been quiet for a moment, and left waiting where the one handling them
 * stops before it is through, or its files could not be written, until it has failed as
 * often as alasio tries; due by their last message, not their first; their prompts run once
 * their chat's lock is let go of; and a message that comes once its album was handled told
 * of in its chat.
 */
import assert from "node:assert/strict";
import { tmpdir } from "node:os";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";

import { Deferred, Duration, Effect, Exit, Fiber } from "effect";

import type { Store } from "../src/persistence/store.ts";
import { SessionFileError, SessionSandboxes } from "../src/sandbox/index.ts";
import { LATE_ALBUM_PART, MEDIA_GROUP_TRIES, MediaGroups } from "../src/telegram/media-group-buffer.ts";
import { newSchema, run, testPool, testStore } from "./support/store.ts";
import { turnsStub, withServices } from "./support/turns.ts";

const CHAT = 42;
const QUIET_MS = 1500;

/** A store whose chat has an album of `messages` messages, kept `agoMs` ago, the album waiting. */
async function albumWaiting(messages: number, agoMs: number): Promise<{ readonly store: Store["Service"]; readonly schema: string; readonly conversationId: string }> {
  const schema = newSchema();
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

/** Session filesystems whose file writes fail while `failing()` says so, the files written recorded. */
function sessionWrites(failing: () => boolean) {
  const written: string[] = [];
  const sandbox = SessionSandboxes.of({
    volumes: { create: () => Effect.die("no session is made"), fork: () => Effect.die("no session is forked"), destroy: () => Effect.die("no session is deleted"), list: Effect.succeed([]) },
    harnessDirectory: () => tmpdir(),
    ensureSession: () => Effect.die("no session is brought up"),
    readFile: () => Effect.die("no file is read"),
    writeFile: (_volumeId, path) => (failing() ? Effect.fail(new SessionFileError({ message: "the session does not answer" })) : Effect.sync(() => void written.push(path))),
  });
  return { written, sandbox };
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
  const { written, sandbox } = sessionWrites(() => false);
  await withServices({ store, turns, sandbox, workspaceRoot: tmpdir() }, async (one) => {
    await withServices({ store: other, turns, sandbox, workspaceRoot: tmpdir() }, async (another) => {
      await Promise.all([one.runPromise(flushDue), another.runPromise(flushDue)]);
      await until("the album's prompt runs", () => prompts.length > 0);
      await sleep(200);
    });
  });
  assert.equal(prompts.length, 1);
  assert.match(prompts[0] ?? "", /^caption[^]*photo-1\.jpg[^]*photo-2\.jpg/u);
  assert.equal(written.length, 2);
  assert.deepEqual(await run(store.getPendingMediaGroupsDue(QUIET_MS)), []);
});

test("an album's prompt runs once its chat's handling lock is let go of, so that a prompt that takes a while holds back none of its chat's updates", async () => {
  const { store, conversationId } = await albumWaiting(2, 10_000);
  await setUp(store, conversationId);
  const release = Promise.withResolvers<void>();
  const { prompts, turns } = promptsTaken(release.promise);
  const { sandbox } = sessionWrites(() => false);
  try {
    await withServices({ store, turns, sandbox, workspaceRoot: tmpdir() }, async (alasio) => {
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

test("an album whose files cannot be written is left waiting, and handled by the next look once they can be", async () => {
  const { store, conversationId } = await albumWaiting(2, 10_000);
  await setUp(store, conversationId);
  let failing = true;
  const { prompts, turns } = promptsTaken();
  const { written, sandbox } = sessionWrites(() => failing);
  await withServices({ store, turns, sandbox, workspaceRoot: tmpdir() }, async (alasio) => {
    await alasio.runPromise(flushDue);
    assert.deepEqual((await run(store.getPendingMediaGroupsDue(QUIET_MS))).map(({ id }) => id), ["album"]);
    assert.deepEqual(prompts, []);
    failing = false;
    await alasio.runPromise(flushDue);
    await until("the album's prompt runs", () => prompts.length > 0);
  });
  assert.equal(prompts.length, 1);
  assert.equal(written.length, 2);
  assert.deepEqual(await run(store.getPendingMediaGroupsDue(QUIET_MS)), []);
});

test("an album whose files fail as often as alasio tries is given up on, its chat told once, and its prompt never run", async () => {
  const { store, schema, conversationId } = await albumWaiting(2, 10_000);
  await setUp(store, conversationId);
  const { prompts, turns } = promptsTaken();
  const { sandbox } = sessionWrites(() => true);
  await withServices({ store, turns, sandbox, workspaceRoot: tmpdir() }, async (alasio) => {
    for (let look = 1; look < MEDIA_GROUP_TRIES; look += 1) await alasio.runPromise(flushDue);
    assert.deepEqual(await queued(schema), [], "told before it is given up on");
    assert.deepEqual((await run(store.getPendingMediaGroupsDue(QUIET_MS))).map(({ id }) => id), ["album"]);
    await alasio.runPromise(flushDue);
    assert.deepEqual(await run(store.getPendingMediaGroupsDue(QUIET_MS)), []);
    await alasio.runPromise(flushDue);
  });
  assert.deepEqual(prompts, []);
  assert.deepEqual(await queued(schema), [
    `An album's files could not be written for its prompt, ${MEDIA_GROUP_TRIES} times, so its prompt was not run: the session does not answer. Send it again.`,
  ]);
});

test("a message of an album that comes once the album was handled is told of in its chat, not dropped without a word, and starts no wait of its own", async () => {
  const { store, schema, conversationId } = await albumWaiting(2, 10_000);
  assert.equal(await run(claim(store)), true);
  const { prompts, turns } = promptsTaken();
  await withServices({ store, turns, workspaceRoot: tmpdir() }, async (alasio) => {
    await alasio.runPromise(Effect.flatMap(MediaGroups, (groups) => groups.buffer({ mediaGroupId: "album", conversationId, updateId: 3, chatId: CHAT })));
    await sleep(QUIET_MS + 500);
  });
  assert.deepEqual(prompts, []);
  assert.deepEqual(await queued(schema), [LATE_ALBUM_PART]);
});
