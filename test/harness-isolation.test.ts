/**
 * One harness pool pod's reach of alasio's Neon (src/persistence/harness-grants.ts,
 * src/harness/grant.ts, src/neon/granted-pool.ts): every pod shares the harness host's role, and
 * its grant alone holds it to its own conversation's rows, of the state and of the transcripts'
 * and rollouts' stores; another conversation's it neither writes nor sees, and a session another
 * conversation has it never claims, but as alasio gives it one. A test file of its own, as roles are a server's:
 * here alasio makes its roles as Neon's alasio is made, a role that may make roles and
 * databases, and is no superuser.
 */
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";

import { ConfigProvider, Context, Effect, Exit, Layer, Scope } from "effect";
import pg from "pg";

import { HARNESS_ROLE, LAKE_ROLE } from "../neon/control/secrets.ts";
import { scramVerifier } from "../neon/control/scram.ts";
import { NeonRolloutStore } from "../src/codex/rollouts/store.ts";
import { NeonSessionStore } from "../src/harness/claude/session-store.ts";
import { GRANT_SETTING, grantDigest, grantKey, harnessGrant } from "../src/harness/grant.ts";
import { CODEX_HARNESS } from "../src/harness/names.ts";
import { Neon } from "../src/neon/connect.ts";
import { GrantedPool } from "../src/neon/granted-pool.ts";
import { LakeWake } from "../src/neon/lake-wake.ts";
import { type Store as StoreService, Store } from "../src/persistence/store.ts";
import { startPostgres } from "./support/postgres.ts";

/** Runs `statements` on a connection to `url`. */
async function run(url: string, ...statements: string[]): Promise<void> {
  const client = new pg.Client({ connectionString: url });
  await client.connect();
  try {
    for (const statement of statements) await client.query(statement);
  } finally {
    await client.end();
  }
}

/** What a conversation of the test has, each of its kind of row's key. */
interface Kept {
  readonly id: string;
  /** Its Codex session, its Claude session, and a session only a row of its workspaces names. */
  readonly codex: string;
  readonly claude: string;
  readonly elsewhere: string;
  readonly response: string;
  readonly rollout: string;
}

/** The key of each table's rows the role reaches, the conversation's or one of its sessions', as `kept` has it. */
const KEYS: ReadonlyArray<readonly [table: string, column: string, of: (kept: Kept) => string]> = [
  ["state.conversations", "id", (kept) => kept.id],
  ["state.turns", "conversation_id", (kept) => kept.id],
  ["state.responses", "thread_key", (kept) => kept.id],
  ["state.response_blocks", "response_id", (kept) => kept.response],
  ["state.session_usage", "session_id", (kept) => kept.codex],
  ["state.files", "conversation_id", (kept) => kept.id],
  ["state.conversation_holds", "conversation_id", (kept) => kept.id],
  ["state.prompt_jobs", "conversation_id", (kept) => kept.id],
  ["claude_sessions.entries", "session_id", (kept) => kept.claude],
  ["claude_sessions.summaries", "session_id", (kept) => kept.claude],
  ["codex_sessions.rollouts", "thread_id", (kept) => kept.codex],
  ["codex_sessions.rollout_chunks", "name", (kept) => kept.rollout],
];

/** Each table's keys `pool` sees, in KEYS's order. */
const seen = async (pool: pg.Pool): Promise<string[][]> =>
  Promise.all(KEYS.map(async ([table, column]) => (await pool.query<{ key: string }>(`select ${column} as key from ${table} order by 1`)).rows.map(({ key }) => key)));

/** How many rows `statement` touched, as `pool`. */
const touched = async (pool: pg.Pool, statement: string, values: readonly unknown[]): Promise<number> => (await pool.query(statement, [...values])).rowCount ?? 0;

/** The writes the role may make, of `kept`'s rows, with the values each takes. */
const inserts = (kept: Kept, suffix: string): ReadonlyArray<readonly [statement: string, values: readonly unknown[]]> => [
  ["insert into state.turns (id, conversation_id, thread_key, channel, thread_ts, harness, state, started_at) values ($1, $2, $2, '1', '1', 'codex', 'active', now())", [`turn-${suffix}`, kept.id]],
  ["insert into state.responses (id, chat_id, message_id, thread_key) values ($1, '1', '1', $2)", [`response-${suffix}`, kept.id]],
  ["insert into state.response_blocks (response_id, block) values ($1, '{}')", [kept.response]],
  ["insert into state.session_usage (session_id, cache_read_input_tokens) values ($1, 1) on conflict (session_id) do update set cache_read_input_tokens = 2", [kept.codex]],
  ["insert into claude_sessions.entries (project_key, session_id, entry, mtime) values ('p', $1, '{}', 1)", [kept.claude]],
  ["insert into claude_sessions.summaries (project_key, session_id, mtime, data) values ($2, $1, 1, '{}')", [kept.claude, `p-${suffix}`]],
  ["insert into codex_sessions.rollouts (name, path, thread_id, rollout_id, size, head_digest, modified_ms) values ($1, 'r', $2, $2, 0, 'd', 1)", [`rollout-${suffix}`, kept.codex]],
  ["insert into codex_sessions.rollout_chunks (name, start, bytes) values ($1, 100, 'x')", [kept.rollout]],
];

/** The updates and deletes the role may make, of `kept`'s rows. */
const changes = (kept: Kept): ReadonlyArray<readonly [statement: string, values: readonly unknown[]]> => [
  ["update state.conversations set updated_at = now() where id = $1", [kept.id]],
  ["update state.turns set status = 'working' where conversation_id = $1", [kept.id]],
  ["update state.responses set completed = completed where thread_key = $1", [kept.id]],
  ["update state.session_usage set updated_at = now() where session_id = $1", [kept.codex]],
  ["update state.prompt_jobs set upstream_turn_id = 'turn' where conversation_id = $1", [kept.id]],
  ["update claude_sessions.entries set mtime = 2 where session_id = $1", [kept.claude]],
  ["update claude_sessions.summaries set mtime = 2 where session_id = $1", [kept.claude]],
  ["update codex_sessions.rollouts set path = 'moved' where thread_id = $1", [kept.codex]],
  ["update codex_sessions.rollout_chunks set bytes = 'y' where name = $1", [kept.rollout]],
  ["delete from codex_sessions.rollout_chunks where name = $1", [kept.rollout]],
  ["delete from codex_sessions.rollouts where thread_id = $1", [kept.codex]],
  ["delete from claude_sessions.entries where session_id = $1", [kept.claude]],
  ["delete from claude_sessions.summaries where session_id = $1", [kept.claude]],
];

/** A conversation with a row of each kind the role reaches, as alasio writes them, its sessions named `name`. */
const keep = (alasio: StoreService["Service"], owner: pg.Pool, sessions: NeonSessionStore, rollouts: NeonRolloutStore, chatId: string, name: string) =>
  Effect.gen(function*() {
    const id = yield* alasio.upsertConversation({ chatId, user: { id: Number(chatId) } });
    yield* alasio.setActiveHarness(id, CODEX_HARNESS);
    yield* alasio.setWorkingDirectory(id, "/work");
    const kept: Kept = { id, codex: `${name}-codex`, claude: `${name}-claude`, elsewhere: `${name}-elsewhere`, response: "", rollout: `${name}-rollout` };
    yield* alasio.setSessionId(id, kept.codex);
    const messageId = yield* alasio.insertMessage({ conversationId: id, direction: "in", kind: "file", transportMessageId: 10, text: "go" });
    const content = Buffer.from("a report");
    yield* alasio.insertFile({
      conversationId: id,
      messageId,
      file: { kind: "document", file_id: `f-${name}`, file_unique_id: `u-${name}`, file_name: "report.txt" },
      name: "report.txt",
      content,
      sha256: createHash("sha256").update(content).digest("hex"),
    });
    yield* alasio.enqueuePromptJob({ conversationId: id, chatId, messageId: "10", prompt: "go" });
    const epoch = yield* alasio.claimTurn({ conversationId: id, chatId, messageId: "10", sessionId: kept.codex, harness: CODEX_HARNESS, prompt: "go" });
    const turn = alasio.turnPersistence(id, epoch);
    const response = yield* turn.createPendingResponse(chatId, "10", kept.codex);
    yield* turn.appendBlocksToPending(response, [{ type: "text", content: "Fixed." }]);
    yield* turn.updateSessionUsage(kept.codex, { cacheReadInputTokens: 12 });
    assert.ok(yield* alasio.holdConversations([id], "forking", 60_000));
    yield* Effect.promise(async () => {
      await owner.query("update state.conversations set claude_session_id = $2 where id = $1", [id, kept.claude]);
      await owner.query("insert into state.workspace_sessions (conversation_id, harness, working_directory, session_id) values ($1, 'claude', '/before', $2)", [id, kept.elsewhere]);
      await sessions.append({ projectKey: "p", sessionId: kept.claude }, [{ type: "user", uuid: `${name}-entry`, timestamp: "2026-10-21T00:00:00Z" }]);
      await rollouts.save(
        { name: kept.rollout, path: "sessions/r.jsonl", threadId: kept.codex, rolloutId: kept.codex, historyBase: null, size: 3, headDigest: "d", modifiedMs: 1 },
        { start: 0, bytes: Buffer.from("abc") },
      );
    });
    return { ...kept, response };
  });

/** Two pool pods' reach of alasio's Neon: the database's server, as each role reaches it, alasio's store, two conversations kept as KEYS says, and a pod's pool granted each. */
interface GrantedPods {
  /** `database`'s server, as `user`, in alasio's database. */
  readonly as: (user: string, password: string) => string;
  readonly owner: pg.Pool;
  readonly harness: { readonly a: GrantedPool; readonly b: GrantedPool };
  readonly ungranted: pg.Pool;
  readonly lake: pg.Pool;
  readonly alasio: StoreService["Service"];
  readonly a: Kept;
  readonly b: Kept;
  readonly key: Buffer;
  readonly grantA: string;
}

/** Runs `use` on two pool pods granted their conversations, as GrantedPods says, and takes it all down after. */
async function withGrantedPods(use: (pods: GrantedPods) => Promise<void>): Promise<void> {
  const database = await startPostgres();
  const as = (user: string, password: string) => {
    const url = new URL(database.url);
    Object.assign(url, { username: user, password, pathname: "/alasio" });
    return url.toString();
  };
  const owner = new pg.Pool({ connectionString: as("alasio", "alasio-password") });
  const harness = { a: new GrantedPool({ connectionString: as(HARNESS_ROLE, "harness-password") }), b: new GrantedPool({ connectionString: as(HARNESS_ROLE, "harness-password") }) };
  const ungranted = new pg.Pool({ connectionString: as(HARNESS_ROLE, "harness-password") });
  const lake = new pg.Pool({ connectionString: as(LAKE_ROLE, "lake-password") });
  // alasio's Neon and store, open for the test's length.
  const scope = Effect.runSync(Scope.make());
  try {
    await run(database.url, "create role alasio login password 'alasio-password' createrole createdb", "create database alasio owner alasio");
    const env = { ALASIO_DATABASE_URL: as("alasio", "alasio-password"), ALASIO_LAKE_VERIFIER: scramVerifier("lake-password"), ALASIO_HARNESS_VERIFIER: scramVerifier("harness-password"), ALASIO_LAKE_ENABLED: "1" };
    const key = grantKey(Buffer.from("the environment's key"));
    const { alasio, a, b, grantA } = await Effect.runPromise(Scope.provide(Effect.gen(function*() {
      const neon = yield* Neon.make;
      const alasio = Context.get(yield* Layer.build(Store.layer({ pool: neon.pool, harnessRole: HARNESS_ROLE })), Store);
      const a = yield* keep(alasio, owner, neon.sessionStore, neon.rollouts, "41", "a");
      const b = yield* keep(alasio, owner, neon.sessionStore, neon.rollouts, "42", "b");
      const grantA = harnessGrant(key, a.id, "binding-a");
      const grantB = harnessGrant(key, b.id, "binding-b");
      yield* alasio.issueHarnessGrant(a.id, "binding-a", grantDigest(grantA));
      yield* alasio.issueHarnessGrant(b.id, "binding-b", grantDigest(grantB));
      harness.a.grant(grantA);
      harness.b.grant(grantB);
      return { alasio, a, b, grantA };
    }).pipe(Effect.provideService(LakeWake, LakeWake.none), Effect.provide(ConfigProvider.layer(ConfigProvider.fromEnv({ env })))), scope));
    await use({ as, owner, harness, ungranted, lake, alasio, a, b, key, grantA });
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
    await Promise.all([owner.end(), harness.a.end(), harness.b.end(), ungranted.end(), lake.end()]);
    await database.stop();
  }
}

test("a pool pod's grant holds the harness role to its own conversation's rows: another's it neither writes nor sees, and a session another has it never claims, but as alasio gives it one", { timeout: 60_000 }, async () => {
  await withGrantedPods(async ({ as, owner, harness, ungranted, lake, alasio, a, b, key, grantA }) => {
    // Its own conversation's rows alone, of each table it reaches; alasio, and the lake, see both.
    assert.deepEqual(await seen(harness.a), KEYS.map(([, , of]) => [of(a)]));
    assert.deepEqual(await seen(harness.b), KEYS.map(([, , of]) => [of(b)]));
    assert.deepEqual(await seen(owner), KEYS.map(([, , of]) => [of(a), of(b)].sort()));
    assert.deepEqual((await lake.query<{ session_id: string }>("select session_id from claude_sessions.entries order by 1")).rows.map(({ session_id }) => session_id), [a.claude, b.claude]);

    // Another's rows it writes none of: an insert is refused, an update or delete touches nothing; its own it writes.
    for (const [statement, values] of inserts(b, "forged")) await assert.rejects(harness.a.query(statement, [...values]), /row-level security/u, statement);
    for (const [statement, values] of changes(b)) assert.equal(await touched(harness.a, statement, values), 0, statement);
    for (const [statement, values] of inserts(a, "own")) assert.equal(await touched(harness.a, statement, values), 1, statement);
    for (const [statement, values] of changes(a)) assert.ok(await touched(harness.a, statement, values) >= 1, statement);
    assert.deepEqual(await seen(owner).then((tables) => tables.map((keys) => keys.filter((kept) => KEYS.some(([, , of]) => of(b) === kept)))), await seen(harness.b));

    // A session another conversation's rows name, a workspace's alone among them, it never claims; a fresh one it claims first, which no other may after.
    for (const session of [b.codex, b.claude, b.elsewhere]) {
      await assert.rejects(harness.a.query("insert into claude_sessions.entries (project_key, session_id, entry, mtime) values ('p', $1, '{}', 1)", [session]), /row-level security/u, session);
      await assert.rejects(harness.a.query("update state.conversations set claude_session_id = $2 where id = $1", [a.id, session]), /row-level security/u, session);
    }
    await new NeonSessionStore(harness.a).append({ projectKey: "p", sessionId: "fresh" }, [{ type: "user", uuid: "fresh-entry", timestamp: "2026-10-21T00:00:00Z" }]);
    assert.deepEqual((await owner.query("select conversation_id from state.conversation_sessions where session_id = 'fresh'")).rows, [{ conversation_id: a.id }]);
    assert.equal((await new NeonSessionStore(harness.a).load({ projectKey: "p", sessionId: "fresh" }))?.length, 1);
    assert.equal(await new NeonSessionStore(harness.b).load({ projectKey: "p", sessionId: "fresh" }), null);
    await assert.rejects(new NeonSessionStore(harness.b).append({ projectKey: "p", sessionId: "fresh" }, [{ type: "user", uuid: "forged-entry", timestamp: "2026-10-21T00:00:00Z" }]), /row-level security/u);
    // Given it by alasio's operator, as a session of the folder another conversation mounts is resumed from its panel, it is that one's too, and still the first's.
    await Effect.runPromise(alasio.giveSession(b.id, "fresh"));
    await owner.query("update state.conversations set claude_session_id = 'fresh' where id = $1", [b.id]);
    await new NeonSessionStore(harness.b).append({ projectKey: "p", sessionId: "fresh" }, [{ type: "user", uuid: "resumed-entry", timestamp: "2026-10-21T00:00:01Z" }]);
    assert.equal((await new NeonSessionStore(harness.b).load({ projectKey: "p", sessionId: "fresh" }))?.length, 2);
    assert.equal((await new NeonSessionStore(harness.a).load({ projectKey: "p", sessionId: "fresh" }))?.length, 2);

    // A session the stores keep and no row names, as one of a harness's no conversation mounts, no pod claims, whatever its id.
    await new NeonSessionStore(owner).append({ projectKey: "p", sessionId: "unnamed" }, [{ type: "user", uuid: "unnamed-entry", timestamp: "2026-10-21T00:00:00Z" }]);
    await assert.rejects(new NeonSessionStore(harness.a).append({ projectKey: "p", sessionId: "unnamed" }, [{ type: "user", uuid: "claimed-entry", timestamp: "2026-10-21T00:00:01Z" }]), /row-level security/u);
    assert.equal(await new NeonSessionStore(harness.a).load({ projectKey: "p", sessionId: "unnamed" }), null);
    assert.deepEqual((await owner.query("select conversation_id from state.conversation_sessions where session_id = 'unnamed'")).rows, []);
    // A fork alasio's operator gives a conversation, as one resumed from a folder's panel, is that one's with the thread its history starts in, which its pod writes back.
    const rollouts = new NeonRolloutStore(owner);
    for (const [threadId, historyBase] of [["panel-parent", null], ["panel-fork", "panel-parent"]] as const) {
      await rollouts.save({ name: `rollout-${threadId}.jsonl`, path: `sessions/rollout-${threadId}.jsonl`, threadId, rolloutId: threadId, historyBase, size: 1, headDigest: "d", modifiedMs: 1 }, { start: 0, bytes: Buffer.from("x") });
    }
    await Effect.runPromise(alasio.mountGivenSession(b.id, "panel-fork"));
    assert.deepEqual((await owner.query("select session_id from state.conversation_sessions where conversation_id = $1 and session_id like 'panel-%' order by 1", [b.id])).rows, [{ session_id: "panel-fork" }, { session_id: "panel-parent" }]);
    assert.deepEqual((await new NeonRolloutStore(harness.b).lineage(["panel-fork"])).map(({ name }) => name), ["rollout-panel-fork.jsonl", "rollout-panel-parent.jsonl"]);
    assert.deepEqual(await new NeonRolloutStore(harness.a).lineage(["panel-fork"]), []);

    // Neither the grants nor the sessions' conversations are the role's to read, or its to write.
    for (const statement of ["select * from state.harness_grants", "select * from state.conversation_sessions", "insert into state.conversation_sessions (session_id, conversation_id) values ('taken', 'telegram:41')"]) {
      await assert.rejects(harness.a.query(statement), /permission denied/u, statement);
    }

    // No setting, a grant no binding holds, and a grant released, see nothing.
    const none = KEYS.map(() => []);
    assert.deepEqual(await seen(ungranted), none);
    const forged = new GrantedPool({ connectionString: as(HARNESS_ROLE, "harness-password") });
    forged.grant(harnessGrant(grantKey(Buffer.from("another key")), a.id, "binding-a"));
    try {
      assert.deepEqual(await seen(forged), none);
    } finally {
      await forged.end();
    }
    // Its conversation's harness bound anew in another pool, as a switch of its harness or its workspace binds it, the pod still bound drains as its own rows' still, as the new one reaches them.
    const switched = new GrantedPool({ connectionString: as(HARNESS_ROLE, "harness-password") });
    try {
      const before = await seen(harness.a);
      const grantSwitched = harnessGrant(key, a.id, "binding-other");
      await Effect.runPromise(alasio.issueHarnessGrant(a.id, "binding-other", grantDigest(grantSwitched)));
      switched.grant(grantSwitched);
      assert.deepEqual(await seen(harness.a), before, "the pod still bound reaches nothing once its conversation is bound anew");
      assert.deepEqual(await seen(switched), before);
      await new NeonSessionStore(harness.a).append({ projectKey: "p", sessionId: a.claude }, [{ type: "user", uuid: "drained-entry", timestamp: "2026-10-21T00:00:02Z" }]);
      // The new binding let go of, the one before is kept, as of a binding of its own.
      await Effect.runPromise(alasio.withEventHandled("released-a", "harness.released", (writes) => writes.revokeHarnessGrant(a.id, "binding-other")));
      assert.deepEqual(await seen(switched), none);
      assert.notDeepEqual(await seen(harness.a), none, "a release of another binding of the conversation's revokes nothing");
    } finally {
      await switched.end();
    }
    await Effect.runPromise(alasio.withEventHandled("released-a-again", "harness.released", (writes) => writes.revokeHarnessGrant(a.id, "binding-a")));
    assert.deepEqual(await seen(harness.a), none);
    assert.notDeepEqual(await seen(harness.b), none);
    // Its grant issued again, as alasio binds the conversation anew, it reaches its rows again.
    await Effect.runPromise(alasio.issueHarnessGrant(a.id, "binding-a", grantDigest(grantA)));
    assert.notDeepEqual(await seen(harness.a), none);
  });
});

test("a pod's writes of its turn and of its response at once, each naming its conversation's session, never wait on each other's lock of it", { timeout: 60_000 }, async () => {
  await withGrantedPods(async ({ harness, a }) => {
    // A write of its response, fenced on its turn's row, as its host's are (src/persistence/response-repository.ts): the row held for share.
    const responding = await harness.a.connect();
    const turning = await harness.a.connect();
    try {
      await responding.query("begin");
      await responding.query("select 1 from state.turns where conversation_id = $1 for share", [a.id]);
      // Its turn's row written meanwhile, which waits for the fence to let go.
      const turned = turning.query("update state.turns set status = 'working' where conversation_id = $1", [a.id]);
      await sleep(300);
      // The response written, as its session is its conversation's already: no wait on the turn's writer, and so no deadlock.
      assert.equal((await responding.query("update state.responses set completed = completed where thread_key = $1", [a.id])).rowCount, 1);
      await responding.query("commit");
      assert.equal((await turned).rowCount, 1);
    } finally {
      responding.release();
      turning.release();
    }
  });
});

test("a pod's turn given a session no conversation has yet, as its response is given it at once, takes its turn's row before the session's lock, as the response's write does, so neither waits on the other", { timeout: 60_000 }, async () => {
  await withGrantedPods(async ({ harness, a }) => {
    const store = await Effect.runPromise(Effect.provide(Store, Store.layer({ pool: harness.a, makesSchema: false })));
    const { epoch } = (await harness.a.query<{ epoch: string }>("select epoch from state.turns where id = $1", [a.id])).rows[0]!;
    const responding = await harness.a.connect();
    try {
      // The response given the session, fenced on its turn's row (src/persistence/response-repository.ts updatePendingSessionId), its transaction not yet through.
      await responding.query("begin");
      await responding.query("select 1 from state.turns where id = $1 for share", [a.id]);
      // The turn given it meanwhile, as its host records the session its harness started (src/persistence/turn-repository.ts updateSessionIdAt).
      const turned = Effect.runPromise(store.turnPersistence(a.id, Number(epoch)).updateActiveTurnSessionId("new-session"));
      await sleep(300);
      assert.equal((await responding.query("update state.responses set session_id = 'new-session' where id = $1", [a.response])).rowCount, 1);
      await responding.query("commit");
      await turned;
    } finally {
      responding.release();
    }
    assert.deepEqual((await harness.a.query("select session_id from state.turns where id = $1", [a.id])).rows, [{ session_id: "new-session" }]);
  });
});

test("a pod's connections carry the grant last given: one reused never carries a grant given before", { timeout: 60_000 }, async () => {
  const database = await startPostgres();
  const pool = new GrantedPool({ connectionString: database.url, max: 1 });
  /** The grant the connection handed on carries, and its server process. */
  const carried = async () => (await pool.query<{ grant: string; pid: number }>("select current_setting($1, true) as grant, pg_backend_pid() as pid", [GRANT_SETTING])).rows[0];
  try {
    const before = await carried();
    assert.equal(before?.grant, "", "none before any is given");
    pool.grant("first");
    const first = await carried();
    assert.equal(first?.grant, "first");
    assert.notEqual(first?.pid, before?.pid, "the connection made before the grant is not reused");
    const reused = await carried();
    assert.deepEqual(reused, first, "one made with the grant is");
    // A connection held as a grant is given goes on as it was until handed back; none handed on after carries the one before.
    const held = await pool.connect();
    pool.grant("second");
    assert.equal((await held.query<{ grant: string }>("select current_setting($1) as grant", [GRANT_SETTING])).rows[0]?.grant, "first");
    held.release();
    const second = await carried();
    assert.equal(second?.grant, "second");
    assert.notEqual(second?.pid, reused?.pid);
    // Through a transaction's connection as through a statement's.
    const client = await pool.connect();
    try {
      assert.equal((await client.query<{ grant: string }>("select current_setting($1) as grant", [GRANT_SETTING])).rows[0]?.grant, "second");
    } finally {
      client.release();
    }
  } finally {
    await pool.end();
    await database.stop();
  }
});

test("a session another conversation has is never its pod's for knowing its id: neither by a rollout of its own naming it as its history, nor by alasio told of it, but as alasio's operator shares it", { timeout: 60_000 }, async () => {
  await withGrantedPods(async ({ owner, harness, alasio, a, b }) => {
    const sessionsOf = async (conversation: string) =>
      (await owner.query<{ session_id: string }>("select session_id from state.conversation_sessions where conversation_id = $1 order by 1", [conversation])).rows.map(({ session_id }) => session_id);
    const before = await sessionsOf(a.id);
    // A thread of its own whose rollout says its history starts in the other's, then named as its turn's session, as by its harness and by alasio.
    assert.equal((await harness.a.query("insert into codex_sessions.rollouts (name, path, thread_id, rollout_id, history_base, size, head_digest, modified_ms) values ('rollout-mine', 'r', 'mine', 'mine', $1, 0, 'd', 1)", [b.codex])).rowCount, 1);
    assert.equal((await harness.a.query("update state.turns set session_id = 'mine' where conversation_id = $1", [a.id])).rowCount, 1);
    await Effect.runPromise(alasio.setSessionId(a.id, "mine"));
    assert.deepEqual(await sessionsOf(a.id), [...before, "mine"].sort());
    assert.deepEqual((await new NeonRolloutStore(harness.a).lineage(["mine"])).map(({ name }) => name), ["rollout-mine"]);
    assert.deepEqual((await harness.a.query("select name from codex_sessions.rollout_chunks where name = $1", [b.rollout])).rows, []);
    // The other's session, as a pod answers alasio with it, of a turn, a fresh session or a turn attached to again: refused.
    const { epoch } = (await owner.query<{ epoch: string }>("select epoch from state.turns where id = $1", [a.id])).rows[0]!;
    for (const session of [b.codex, b.claude]) {
      const refused = /the session \S+ is not the conversation telegram:41's/u;
      await assert.rejects(Effect.runPromise(alasio.setSessionId(a.id, session)), (error: { readonly cause?: { readonly message?: string } }) => refused.test(error.cause?.message ?? ""), session);
      await assert.rejects(Effect.runPromise(alasio.turnPersistence(a.id, Number(epoch)).updateActiveTurnSessionId(session)), (error: { readonly cause?: { readonly message?: string } }) => refused.test(error.cause?.message ?? ""), session);
    }
    assert.deepEqual(await sessionsOf(a.id), [...before, "mine"].sort());
    assert.equal((await Effect.runPromise(alasio.getMount(a.id))).sessionId, "mine");
    // Shared by alasio's operator, as of the folder's panel: the other's session is its too, and what its harness kept of it its pod's to read.
    await Effect.runPromise(alasio.mountGivenSession(a.id, b.codex));
    assert.ok((await sessionsOf(a.id)).includes(b.codex));
    assert.deepEqual((await harness.a.query("select name from codex_sessions.rollout_chunks where name = $1", [b.rollout])).rows, [{ name: b.rollout }]);
  });
});
