import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, describe, test } from "node:test";

import { forkSession, getSessionMessages, importSessionToStore, listSessions } from "@anthropic-ai/claude-agent-sdk";
import pg from "pg";

import { NeonSessionStore, docOf, mirrorOnly } from "../src/harness/claude/session-store.js";
import { dockerAvailable, startPostgres } from "./support/postgres.js";
import { E, KEY, expectEntries, sessionStoreConformance } from "./support/session-store-conformance.js";

const skip = !dockerAvailable() && "needs Docker for a throwaway Postgres";

let database;
let pool;
let schemas = 0;

/** A store on its own schema: every case starts empty. */
async function makeStore() {
  const store = new NeonSessionStore(pool, { schema: `test_${process.pid}_${++schemas}` });
  await store.ensureSchema();
  return store;
}

before(async () => {
  if (skip) return;
  database = await startPostgres();
  pool = new pg.Pool({ connectionString: database.url, max: 4 });
});

after(async () => {
  await pool?.end();
  await database?.stop();
});

sessionStoreConformance(makeStore, { skip });

test("an entry's doc replaces only what jsonb cannot hold, in keys and values alike", () => {
  const half = "😀".slice(0, 1);
  assert.deepEqual(
    docOf({ type: "user", [`k\u0000${half}`]: ["a\u0000b", `x${half}`, "whole 😀", String.raw`writes \u0000`, 7, null, true] }),
    { type: "user", "k\ufffd\ufffd": ["a\ufffdb", "x\ufffd", "whole 😀", String.raw`writes \u0000`, 7, null, true] },
  );
});

describe("alasio's session store", { skip }, () => {
  test("an entry re-delivered after a retried append is kept once; entries without a uuid are kept as appended", async () => {
    const store = await makeStore();
    await store.append(KEY, [E("user", { uuid: "u1" }), E("summary")]);
    await store.append(KEY, [E("user", { uuid: "u1" }), E("assistant", { uuid: "a1" }), E("summary")]);
    expectEntries(await store.load(KEY), [
      E("user", { uuid: "u1" }),
      E("summary"),
      E("assistant", { uuid: "a1" }),
      E("summary"),
    ]);
  });

  test("an entry is kept exactly as written, NUL characters and key order included", async () => {
    const store = await makeStore();
    const entry = { z: 1, type: "user", uuid: "u1", message: { content: "binary\u0000output" }, a: 2 };
    await store.append(KEY, [entry]);
    const [loaded] = await store.load(KEY);
    assert.equal(JSON.stringify(loaded), JSON.stringify(entry));
    const [summary] = await store.listSessionSummaries("proj");
    assert.equal(summary.sessionId, "sess");
  });

  test("each entry has a jsonb doc for SQL, which leaves the entry exactly as written", async () => {
    const store = await makeStore();
    const schema = `test_${process.pid}_${schemas}`;
    const nul = { z: 1, type: "user", uuid: "u1", out: "a\u0000b" };
    const halfEmoji = { type: "user", uuid: "u2", out: `cut ${"😀".slice(0, 1)}` };
    const whole = { type: "user", uuid: "u3", out: "whole 😀" };
    await store.append(KEY, [nul, halfEmoji, whole]);

    const loaded = await store.load(KEY);
    assert.deepEqual(loaded.map((entry) => JSON.stringify(entry)), [nul, halfEmoji, whole].map((entry) => JSON.stringify(entry)));
    const { rows } = await pool.query(`select doc->>'type' as type, doc->>'out' as out from ${schema}.entries order by seq`);
    assert.deepEqual(rows, [
      { type: "user", out: "a\ufffdb" },
      { type: "user", out: "cut \ufffd" },
      { type: "user", out: "whole 😀" },
    ]);
  });

  test("an entry that only writes about those escapes gets a doc with its text unchanged", async () => {
    const store = await makeStore();
    const schema = `test_${process.pid}_${schemas}`;
    // The text \u0000 and \ud83d, and a backslash just before a real NUL: in
    // the entry's JSON, each escape is preceded by an escaped backslash.
    const writing = { type: "assistant", uuid: "w1", out: String.raw`jsonb rejects \u0000 and a lone \ud83d` };
    const backslashNul = { type: "assistant", uuid: "w2", out: "a\\\u0000b" };
    await store.append(KEY, [writing, backslashNul]);
    const { rows } = await pool.query(`select doc->>'out' as out from ${schema}.entries order by seq`);
    assert.deepEqual(rows, [{ out: writing.out }, { out: "a\\\ufffdb" }]);
  });

  test("a doc Postgres will not take as jsonb becomes null rather than an error", async () => {
    await makeStore();
    const schema = `test_${process.pid}_${schemas}`;
    // Valid JSON, but past the range of jsonb's numbers.
    const { rows } = await pool.query(`select ${schema}.as_doc($1) as doc`, ['{"n":1e1000000}']);
    assert.deepEqual(rows, [{ doc: null }]);
  });

  test("rows stored before doc existed get theirs when the schema is ensured", async () => {
    const store = await makeStore();
    const schema = `test_${process.pid}_${schemas}`;
    await store.append(KEY, [E("user", { uuid: "u1" })]);
    await pool.query(`alter table ${schema}.entries drop column doc`);
    await store.ensureSchema();
    const { rows } = await pool.query(`select doc->>'uuid' as uuid from ${schema}.entries`);
    assert.deepEqual(rows, [{ uuid: "u1" }]);
  });

  test("a batch beyond one insert's parameter limit keeps its order", async () => {
    const store = await makeStore();
    const entries = Array.from({ length: 12_345 }, (_, n) => E("x", { uuid: `u${n}`, n }));
    await store.append(KEY, entries);
    const loaded = await store.load(KEY);
    assert.equal(loaded.length, entries.length);
    assert.ok(loaded.every((entry, n) => entry.n === n));
  });

  test("summaries fold what is new, and go with a deleted session", async () => {
    const store = await makeStore();
    await store.append(KEY, [E("user", { uuid: "u1" })]);
    await store.append({ ...KEY, subpath: "subagents/a" }, [E("user", { uuid: "s1" })]);
    const [summary] = await store.listSessionSummaries("proj");
    assert.equal(summary.sessionId, "sess");
    assert.ok(summary.mtime > 1e12);
    await store.delete(KEY);
    assert.deepEqual(await store.listSessionSummaries("proj"), []);
  });

  test("finds a session's project, the uuids it holds, and its entries without one", async () => {
    const store = await makeStore();
    await store.append(KEY, [E("user", { uuid: "u1" }), E("summary")]);
    assert.equal(await store.projectKeyOf("sess"), "proj");
    assert.equal(await store.projectKeyOf("missing"), null);
    assert.deepEqual([...(await store.uuidsOf(KEY))], ["u1"]);
    expectEntries(await store.uuidlessEntriesOf(KEY), [E("summary")]);
  });

  test("a query's view mirrors every write but offers nothing to resume from", async () => {
    const store = await makeStore();
    const view = mirrorOnly(store);
    await view.append(KEY, [E("a")]);
    expectEntries(await store.load(KEY), [E("a")]);
    assert.equal(await view.load(KEY), null);
  });
});

describe("the SDK's session helpers on the store", { skip }, () => {
  let home;
  let previousHome;

  before(() => {
    home = mkdtempSync(join(tmpdir(), "alasio-claude-home-"));
    previousHome = process.env.CLAUDE_CONFIG_DIR;
    process.env.CLAUDE_CONFIG_DIR = home;
  });

  after(() => {
    if (previousHome === undefined) delete process.env.CLAUDE_CONFIG_DIR;
    else process.env.CLAUDE_CONFIG_DIR = previousHome;
    rmSync(home, { recursive: true, force: true });
  });

  test("a transcript imported into the store is listed, read, and forked from it", async () => {
    const store = await makeStore();
    const dir = join(home, "work");
    mkdirSync(dir, { recursive: true });
    const sessionId = randomUUID();
    const projectDir = join(home, "projects", dir.replace(/[^a-zA-Z0-9]/gu, "-"));
    mkdirSync(projectDir, { recursive: true });
    const base = { sessionId, cwd: dir, version: "2.1.280", isSidechain: false, userType: "external" };
    const lines = [
      { ...base, type: "user", uuid: "11111111-1111-4111-8111-111111111111", parentUuid: null, timestamp: "2026-09-27T00:00:00.000Z", message: { role: "user", content: "remember the number 7" } },
      { ...base, type: "assistant", uuid: "22222222-2222-4222-8222-222222222222", parentUuid: "11111111-1111-4111-8111-111111111111", timestamp: "2026-09-27T00:00:01.000Z", message: { id: "msg_1", type: "message", role: "assistant", model: "claude", content: [{ type: "text", text: "7, noted" }], stop_reason: "end_turn", usage: { input_tokens: 1, output_tokens: 1 } } },
      { ...base, type: "user", uuid: "33333333-3333-4333-8333-333333333333", parentUuid: "22222222-2222-4222-8222-222222222222", timestamp: "2026-09-27T00:00:02.000Z", message: { role: "user", content: "and 8" } },
    ];
    writeFileSync(join(projectDir, `${sessionId}.jsonl`), lines.map((line) => JSON.stringify(line)).join("\n") + "\n");

    await importSessionToStore(sessionId, store, { dir });
    rmSync(join(projectDir, `${sessionId}.jsonl`));

    const listed = await listSessions({ dir, sessionStore: store });
    assert.deepEqual(listed.map((s) => s.sessionId), [sessionId]);
    assert.equal(listed[0].firstPrompt, "remember the number 7");

    const messages = await getSessionMessages(sessionId, { dir, sessionStore: store });
    assert.deepEqual(messages.map((m) => m.uuid), lines.map((line) => line.uuid));

    const forked = await forkSession(sessionId, { dir, sessionStore: store, upToMessageId: lines[1].uuid });
    const forkedMessages = await getSessionMessages(forked.sessionId, { dir, sessionStore: store });
    assert.equal(forkedMessages.length, 2);
    assert.notEqual(forked.sessionId, sessionId);
  });
});
