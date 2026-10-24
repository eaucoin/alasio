/**
 * The Codex login relay's role (src/neon/codex-login.ts, src/persistence/codex-login-grants.ts),
 * as alasio makes it in its Neon: a store of that role keeps the login as the relay does, under
 * the store's lock of it, and the role reaches nothing else of the state. A test file of its own,
 * as roles are a server's: here alasio makes its role as Neon's alasio is made, a role that may
 * make roles and databases, and is no superuser.
 */
import assert from "node:assert/strict";
import { test } from "node:test";

import { ConfigProvider, Context, Effect, Layer } from "effect";
import pg from "pg";

import { CODEX_LOGIN_ROLE, HARNESS_ROLE } from "../neon/control/secrets.ts";
import { scramVerifier } from "../neon/control/scram.ts";
import { Neon } from "../src/neon/connect.ts";
import { LakeWake } from "../src/neon/lake-wake.ts";
import { Store } from "../src/persistence/store.ts";
import { startPostgres } from "./support/postgres.ts";

test("a store of the Codex login relay's role keeps the login as the relay does, under its lock, and the role reaches nothing else of the state", { timeout: 60_000 }, async () => {
  const database = await startPostgres();
  /** `database`'s server, as `user`, in alasio's database. */
  const as = (user: string, password: string) => {
    const url = new URL(database.url);
    Object.assign(url, { username: user, password, pathname: "/alasio" });
    return url.toString();
  };
  const relayPool = new pg.Pool({ connectionString: as(CODEX_LOGIN_ROLE, "codex-login-password") });
  try {
    const admin = new pg.Client({ connectionString: database.url });
    await admin.connect();
    await admin.query("create role alasio login password 'alasio-password' createrole createdb");
    await admin.query("create database alasio owner alasio");
    await admin.end();
    const env = {
      ALASIO_DATABASE_URL: as("alasio", "alasio-password"),
      ALASIO_LAKE_VERIFIER: scramVerifier("lake-password"),
      ALASIO_HARNESS_VERIFIER: scramVerifier("harness-password"),
      ALASIO_CODEX_LOGIN_VERIFIER: scramVerifier("codex-login-password"),
    };
    await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
      const neon = yield* Neon.make;
      // alasio's store, which makes the state and grants the role the login.
      const alasio = Context.get(yield* Layer.build(Store.layer({ pool: neon.pool, harnessRole: HARNESS_ROLE, codexLoginRole: CODEX_LOGIN_ROLE })), Store);
      yield* alasio.upsertConversation({ chatId: "41", user: { id: 41 } });
      yield* alasio.setCodexLogin("the login alasio kept");
      // The relay's store, of the state alasio made: it makes none of it.
      const relay = Context.get(yield* Layer.build(Store.layer({ pool: relayPool, makesSchema: false })), Store);
      assert.equal(yield* relay.getCodexLogin, "the login alasio kept");
      // Refreshed under the lock, as the relay refreshes it, and its refusal recorded.
      yield* relay.withCodexLoginLock((login) => Effect.flatMap(login.save("the login the relay refreshed"), (savedAt) => login.refuse(savedAt)));
      const kept = yield* alasio.getCodexLoginRecord;
      assert.equal(kept?.auth, "the login the relay refreshed");
      assert.notEqual(kept?.refusedAt, null);
    }).pipe(
      Effect.provideService(LakeWake, LakeWake.none),
      Effect.provide(ConfigProvider.layer(ConfigProvider.fromEnv({ env }))),
    )));

    // Nothing else of the state: the conversations, the turns, Telegram's updates, the outbox, the grants, nor the schema.
    for (
      const statement of [
        "select * from state.conversations",
        "select * from state.turns",
        "select * from state.telegram_updates",
        "insert into state.telegram_outbox (id, chat_id, kind, text, options) values ('forged', '41', 'text', 'forged', '{}')",
        "select * from state.harness_grants",
        "select * from state.session_workspaces",
        "create table state.forged (id int)",
      ]
    ) {
      await assert.rejects(relayPool.query(statement), /permission denied/u, statement);
    }
    const { rows: roles } = await relayPool.query("select r.rolname from pg_auth_members m join pg_roles r on r.oid = m.roleid where m.member = (select oid from pg_roles where rolname = $1)", [CODEX_LOGIN_ROLE]);
    assert.deepEqual(roles, [], "a member of no other role");
  } finally {
    await relayPool.end();
    await database.stop();
  }
});
