/** What alasio's pod tells `alasio status` of its Codex login (src/codex/login-state.ts), as its store keeps it. */
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { test } from "node:test";

import { Effect } from "effect";

import { Store } from "../src/persistence/store.ts";
import { pooledDatabase } from "./support/pools.ts";
import { run } from "./support/store.ts";

test("alasio's pod says whether its store keeps a Codex login, and when its refresh was refused, until another is kept", async () => {
  const database = await pooledDatabase();
  const store = await Effect.runPromise(Effect.provide(Store, Store.layer({ pool: database.pool })));
  const said = () => JSON.parse(execFileSync("node", ["src/codex/login-state.ts"], { env: { ...process.env, ALASIO_DATABASE_URL: database.url, ALASIO_DATABASE_URL_FILE: "" }, encoding: "utf8" })) as unknown;
  assert.deepEqual(said(), { kept: false, refusedAt: null });
  await run(store.setCodexLogin('{"tokens":{}}'));
  assert.deepEqual(said(), { kept: true, refusedAt: null });
  await run(store.withCodexLoginLock((login) => Effect.flatMap(login.get, (kept) => (kept ? login.refuse(kept.savedAt) : Effect.void))));
  const refused = said() as { readonly kept: boolean; readonly refusedAt: string | null };
  assert.equal(refused.kept, true);
  assert.ok(refused.refusedAt !== null && !Number.isNaN(Date.parse(refused.refusedAt)), String(refused.refusedAt));
  await run(store.setCodexLogin('{"tokens":{"again":true}}'));
  assert.deepEqual(said(), { kept: true, refusedAt: null });
  // Refused in its turn: the refusal of the one before, which says nothing now, gone.
  await run(store.withCodexLoginLock((login) => Effect.flatMap(login.get, (kept) => (kept ? login.refuse(kept.savedAt) : Effect.void))));
  assert.notEqual((said() as { readonly refusedAt: string | null }).refusedAt, null);
  const { rows } = await database.pool.query<{ count: string }>("select count(*) from state.codex_login_refusal");
  assert.equal(rows[0]?.count, "1");
});

test("a refusal of a login another was kept in place of since, as `alasio login codex` keeps one taking no lock, refuses neither", async () => {
  const database = await pooledDatabase();
  const store = await Effect.runPromise(Effect.provide(Store, Store.layer({ pool: database.pool })));
  await run(store.setCodexLogin('{"tokens":{"spent":true}}'));
  await run(store.withCodexLoginLock((login) =>
    Effect.gen(function*() {
      const kept = yield* login.get;
      yield* store.setCodexLogin('{"tokens":{"new":true}}');
      if (kept) yield* login.refuse(kept.savedAt);
    })
  ));
  const record = await run(store.getCodexLoginRecord);
  assert.deepEqual([record?.auth, record?.refusedAt], ['{"tokens":{"new":true}}', null]);
  const { rows } = await database.pool.query<{ count: string }>("select count(*) from state.codex_login_refusal");
  assert.equal(rows[0]?.count, "0");
});

test("a store whose Codex login was kept before refusals had a table, as one upgraded is, has its login read and refused as any", async () => {
  const database = await pooledDatabase();
  await database.pool.query("create schema if not exists state");
  await database.pool.query("create table state.codex_login (one boolean primary key default true check (one), auth text not null, updated_at timestamptz not null default now())");
  await database.pool.query("insert into state.codex_login (auth) values ('{\"tokens\":{}}')");
  const store = await Effect.runPromise(Effect.provide(Store, Store.layer({ pool: database.pool })));
  const kept = await run(store.getCodexLoginRecord);
  assert.deepEqual([kept?.auth, kept?.refusedAt], ['{"tokens":{}}', null]);
  await run(store.withCodexLoginLock((login) => Effect.flatMap(login.get, (record) => (record ? login.refuse(record.savedAt) : Effect.void))));
  assert.ok((await run(store.getCodexLoginRecord))?.refusedAt);
});
