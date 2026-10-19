/**
 * Albums as every alasio on the store handles them (src/telegram/media-group-buffer.ts, the
 * store's claimMediaGroup): claimed under their chat's handling lock, once, only once their
 * last message has been quiet for a moment, and left waiting where the one handling them
 * stops before it is through; and due by their last message, not their first.
 */
import assert from "node:assert/strict";
import { test } from "node:test";

import { Deferred, Effect, Exit, Fiber } from "effect";

import type { Store } from "../src/persistence/store.ts";
import { newSchema, run, testPool, testStore } from "./support/store.ts";

const CHAT = 42;
const QUIET_MS = 1500;

/** A store whose chat has an album of `messages` messages, kept `agoMs` ago, the album waiting. */
async function albumWaiting(messages: number, agoMs: number): Promise<{ readonly store: Store["Service"]; readonly schema: string }> {
  const schema = newSchema();
  const store = await testStore({ schema });
  const conversationId = await run(store.upsertConversation({ chatId: CHAT, user: { id: CHAT } }));
  for (let n = 1; n <= messages; n += 1) {
    await run(store.insertMessage({ conversationId, direction: "in", kind: "file", transportMessageId: n, mediaGroupId: "album", text: n === 1 ? "caption" : null }));
  }
  await run(store.upsertMediaGroup({ mediaGroupId: "album", conversationId, updateId: 1 }));
  await kept(schema, agoMs);
  return { store, schema };
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
