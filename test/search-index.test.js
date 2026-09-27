import assert from "node:assert/strict";
import { after, before, describe, test } from "node:test";

import pg from "pg";

import { NeonSessionStore } from "../src/harness/claude/session-store.js";
import { embedBatch } from "../src/harness/claude/search/embedder.js";
import { startTranscriptSearch } from "../src/harness/claude/search/index.js";
import { SETTLE_MS, collectOrphans, indexBatch, settle } from "../src/harness/claude/search/indexer.js";
import { ensureSearchSchema } from "../src/harness/claude/search/schema.js";
import { dockerAvailable, startPostgres } from "./support/postgres.js";

const skip = !dockerAvailable() && "needs Docker for a throwaway Postgres";

let database;
let pool;
let schemas = 0;

before(async () => {
  if (skip) return;
  database = await startPostgres();
  pool = new pg.Pool({ connectionString: database.url, max: 6 });
});

after(async () => {
  await pool?.end();
  await database?.stop();
});

/** A store and its search on a schema of their own. */
async function makeSearch() {
  const schema = `search_${process.pid}_${++schemas}`;
  const store = new NeonSessionStore(pool, { schema });
  await store.ensureSchema();
  await ensureSearchSchema(pool, schema);
  const q = async (text, values) => (await pool.query(text.replaceAll("S.", `${schema}.`), values)).rows;
  const indexAll = async () => {
    let total = 0;
    for (let read; (read = await indexBatch(pool, schema)) > 0; ) total += read;
    return total;
  };
  return { schema, store, q, indexAll };
}

const prompt = (uuid, text, at = "2026-09-27T01:00:00Z") => ({ type: "user", uuid, timestamp: at, message: { role: "user", content: text } });
const answer = (uuid, text, at = "2026-09-27T01:00:01Z") => ({ type: "assistant", uuid, timestamp: at, message: { content: [{ type: "text", text }] } });
const output = (uuid, text, at = "2026-09-27T01:00:02Z") => ({ type: "user", uuid, timestamp: at, message: { content: [{ type: "tool_result", tool_use_id: "toolu_1", content: text }] } });
const key = (sessionId) => ({ projectKey: "-work", sessionId });

describe("indexing the store's entries", { skip }, () => {
  test("each distinct text is one passage, with an occurrence wherever it is written", async () => {
    const { schema, store, q, indexAll } = await makeSearch();
    await store.append(key("a"), [prompt("u1", "where are the transcripts kept?"), answer("a1", "In Neon.")]);
    await store.append(key("b"), [prompt("u2", "where are the transcripts kept?"), { type: "cost-state", totalCostUSD: 1 }]);

    assert.equal(await indexAll(), 4);
    assert.deepEqual(await q("select text from S.passages order by id"), [{ text: "where are the transcripts kept?" }, { text: "In Neon." }]);
    assert.deepEqual(
      await q("select o.session_id, o.kind, p.text from S.occurrences o join S.passages p on p.id = o.passage_id order by o.entry_seq"),
      [
        { session_id: "a", kind: "user.text", text: "where are the transcripts kept?" },
        { session_id: "a", kind: "assistant.text", text: "In Neon." },
        { session_id: "b", kind: "user.text", text: "where are the transcripts kept?" },
      ],
    );
    assert.deepEqual(await q("select count(*)::int as n from S.indexed"), [{ n: 4 }]);
    assert.equal(await indexBatch(pool, schema), 0, "nothing is read twice");
  });

  test("a batch reads only so many entries, and the next goes on from there", async () => {
    const { schema, store, q } = await makeSearch();
    await store.append(key("a"), [prompt("u1", "one"), prompt("u2", "two"), prompt("u3", "three")]);
    assert.equal(await indexBatch(pool, schema, { limit: 2 }), 2);
    assert.equal(await indexBatch(pool, schema, { limit: 2 }), 1);
    assert.equal(await indexBatch(pool, schema, { limit: 2 }), 0);
    assert.deepEqual((await q("select text from S.passages order by id")).map((row) => row.text), ["one", "two", "three"]);
  });

  test("an entry holding a NUL is indexed with U+FFFD in its place", async () => {
    const { store, q, indexAll } = await makeSearch();
    await store.append(key("a"), [output("t1", "font-path\u0000/usr/share/fonts")]);
    await indexAll();
    assert.deepEqual(await q("select text from S.passages"), [{ text: "font-path\ufffd/usr/share/fonts" }]);
  });

  test("the settled mark moves past old, indexed entries and stops at the first not indexed", async () => {
    const { schema, store, q, indexAll } = await makeSearch();
    await store.append(key("a"), [prompt("u1", "one"), prompt("u2", "two")]);
    await indexAll();
    await store.append(key("a"), [prompt("u3", "three")]);
    const [first, second, third] = (await q("select seq from S.entries order by seq")).map((row) => Number(row.seq));
    const settled = async () => Number((await q("select value from S.search_state where name = 'settled_seq'"))[0]?.value ?? 0);

    await settle(pool, schema);
    assert.equal(await settled(), 0, "no entry is old enough yet");

    const later = Date.now() + SETTLE_MS + 1000;
    await settle(pool, schema, { now: later });
    assert.equal(await settled(), second, "stops before the entry not yet indexed");
    assert.ok(first < second && second < third);

    await indexAll();
    await settle(pool, schema, { now: later });
    assert.equal(await settled(), third);
  });

  test("a session the SDK deletes takes its occurrences, and passages no entry holds are dropped", async () => {
    const { schema, store, q, indexAll } = await makeSearch();
    await store.append(key("a"), [prompt("u1", "shared text"), prompt("u2", "only in a")]);
    await store.append(key("b"), [prompt("u3", "shared text")]);
    await indexAll();

    await store.delete(key("a"));
    assert.deepEqual(await q("select session_id from S.occurrences"), [{ session_id: "b" }]);
    assert.equal(await collectOrphans(pool, schema), 1);
    assert.deepEqual(await q("select text from S.passages"), [{ text: "shared text" }]);
  });
});

describe("searching", { skip }, () => {
  test("finds passages by their words, stemmed for prose, and by trigrams for typos and substrings", async () => {
    const { store, q, indexAll } = await makeSearch();
    await store.append(key("a"), [
      answer("a1", "Safekeepers rebuild a lost disk from their peers."),
      { type: "assistant", uuid: "a2", timestamp: "2026-09-27T01:00:03Z", message: { content: [{ type: "tool_use", id: "toolu_1", name: "Bash", input: { command: "docker compose --project-name alasio-neon up --wait" } }] } },
      answer("a3", "Bananas are yellow."),
    ]);
    await indexAll();
    const top = async (query) => (await q("select kind, snippet from S.search($1)", [query]))[0];

    assert.deepEqual(await top("safekeeper rebuilding disks"), { kind: "assistant.text", snippet: "<b>Safekeepers</b> <b>rebuild</b> a lost <b>disk</b> from their peers" });
    assert.equal((await top("projct-name")).kind, "assistant.tool_use");
    assert.equal((await top("alasio-ne")).kind, "assistant.tool_use");
    assert.deepEqual(await q("select * from S.search('xylophone')"), []);
  });

  test("filters by kind, session, and time, and reports a repeated text once with how often it occurs", async () => {
    const { store, q, indexAll } = await makeSearch();
    await store.append(key("a"), [prompt("u1", "restart the neon stack", "2026-09-26T10:00:00Z"), output("t1", "restart the neon stack", "2026-09-26T10:00:05Z")]);
    await store.append(key("b"), [prompt("u2", "restart the neon stack", "2026-09-27T10:00:00Z")]);
    await indexAll();
    const search = (args) => q("select kind, session_id, occurrences from S.search('restart neon'" + args + ")");

    assert.deepEqual(await search(""), [{ kind: "user.text", session_id: "b", occurrences: "3" }]);
    assert.deepEqual(await search(", only_kinds => array['user.tool_result']"), [{ kind: "user.tool_result", session_id: "a", occurrences: "1" }]);
    assert.deepEqual(await search(", only_sessions => array['a']"), [{ kind: "user.text", session_id: "a", occurrences: "2" }]);
    assert.deepEqual(await search(", until => '2026-09-27'"), [{ kind: "user.text", session_id: "a", occurrences: "2" }]);
    assert.deepEqual(await search(", since => '2026-09-27'"), [{ kind: "user.text", session_id: "b", occurrences: "1" }]);
  });

  test("the conversation outranks tool output and the harness's own text", async () => {
    const { store, q, indexAll } = await makeSearch();
    await store.append(key("a"), [
      output("t1", "checkpoint written to disk"),
      { type: "attachment", uuid: "x1", timestamp: "2026-09-27T01:00:04Z", attachment: { content: "the checkpoint reminder" } },
      answer("a1", "The checkpoint is taken as bayma stops."),
    ]);
    await indexAll();
    assert.deepEqual((await q("select kind from S.search('checkpoint')")).map((row) => row.kind), ["assistant.text", "user.tool_result", "attachment"]);
  });
});

describe("embedding", { skip }, () => {
  test("without pgrag's model nothing is embedded or marked, and search goes on by words", async () => {
    const { schema, store, q, indexAll } = await makeSearch();
    await store.append(key("a"), [answer("a1", "Embeddings wait for the model.")]);
    await indexAll();
    assert.equal(await embedBatch(pool, schema), null);
    assert.deepEqual(await q("select count(*)::int as n from S.embeddings"), [{ n: 0 }]);
    assert.equal((await q("select kind from S.search('embeddings')"))[0].kind, "assistant.text");
  });
});

describe("the search loops", { skip }, () => {
  test("index what is appended while they run, and stop when closed", async () => {
    const { schema, store, q } = await makeSearch();
    const search = startTranscriptSearch({ pool, schema });
    try {
      await store.append(key("a"), [prompt("u1", "appended while indexing runs")]);
      const deadline = Date.now() + 15_000;
      let found = [];
      while (found.length === 0 && Date.now() < deadline) {
        found = await q("select kind from S.search('appended while indexing')");
        if (found.length === 0) await new Promise((resolve) => setTimeout(resolve, 200));
      }
      assert.deepEqual(found, [{ kind: "user.text" }]);
    } finally {
      const closing = Date.now();
      await search.close();
      assert.ok(Date.now() - closing < 5_000, "close does not wait out a loop's sleep");
    }
  });
});
