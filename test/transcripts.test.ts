import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, beforeEach, describe, test } from "node:test";

import type { SessionStoreEntry } from "@anthropic-ai/claude-agent-sdk";
import { Effect } from "effect";
import pg from "pg";

import { NeonSessionStore } from "../src/harness/claude/session-store.ts";
import { adoptTranscripts, ensureLocalTranscript } from "../src/harness/claude/transcripts.ts";
import { dockerAvailable, startPostgres, type TestPostgres } from "./support/postgres.ts";

const skip = !dockerAvailable() && "needs Docker for a throwaway Postgres";

// Set by the first hook unless every test that uses them is skipped.
let database: TestPostgres | undefined;
let pool: pg.Pool;
// Each case's Claude home, made before it.
let home: string;
let previousHome: string | undefined;
let schemas = 0;

before(async () => {
  if (skip) return;
  database = await startPostgres();
  pool = new pg.Pool({ connectionString: database.url, max: 4 });
});

after(async () => {
  await pool?.end();
  await database?.stop();
  if (home) rmSync(home, { recursive: true, force: true });
  if (previousHome === undefined) delete process.env["CLAUDE_CONFIG_DIR"];
  else process.env["CLAUDE_CONFIG_DIR"] = previousHome;
});

// A fresh Claude home per case, where the SDK and alasio both look for it.
beforeEach(() => {
  if (home) rmSync(home, { recursive: true, force: true });
  else previousHome = process.env["CLAUDE_CONFIG_DIR"];
  home = mkdtempSync(join(tmpdir(), "alasio-claude-home-"));
  process.env["CLAUDE_CONFIG_DIR"] = home;
});

async function makeStore(): Promise<NeonSessionStore> {
  const store = new NeonSessionStore(pool, { schema: `transcripts_${process.pid}_${++schemas}` });
  await store.ensureSchema();
  return store;
}

const readLines = (path: string): SessionStoreEntry[] => readFileSync(path, "utf8").trim().split("\n").map((line) => JSON.parse(line));
const message = (uuid: string, text: string): SessionStoreEntry => ({ type: "user", uuid, message: { role: "user", content: text } });

/** A local transcript as Claude Code writes one, in a project directory named like the SDK's. */
function writeLocal(
  dir: string,
  sessionId: string,
  entries: readonly SessionStoreEntry[],
  subagents: Readonly<Record<string, readonly SessionStoreEntry[]>> = {},
) {
  const projectKey = dir.replace(/[^a-zA-Z0-9]/gu, "-");
  const projectDir = join(home, "projects", projectKey);
  mkdirSync(projectDir, { recursive: true });
  writeFileSync(join(projectDir, `${sessionId}.jsonl`), entries.map((e) => JSON.stringify(e)).join("\n") + "\n");
  for (const [name, lines] of Object.entries(subagents)) {
    mkdirSync(join(projectDir, sessionId, "subagents"), { recursive: true });
    writeFileSync(join(projectDir, sessionId, "subagents", `${name}.jsonl`), lines.map((e) => JSON.stringify(e)).join("\n") + "\n");
  }
  return { projectKey, main: join(projectDir, `${sessionId}.jsonl`) };
}

describe("claude transcripts against the store", { skip }, () => {
  test("a transcript missing locally is written back whole, subagents and their metadata beside it", async () => {
    const store = await makeStore();
    const sessionId = randomUUID();
    const key = { projectKey: "-work", sessionId };
    await store.append(key, [message("u1", "one"), message("u2", "two")]);
    await store.append({ ...key, subpath: "subagents/agent-a" }, [
      message("s1", "sub"),
      { type: "agent_metadata", agentType: "Explore", description: "look around" },
    ]);

    assert.equal(await Effect.runPromise(ensureLocalTranscript(store, sessionId)), true);

    const main = join(home, "projects", "-work", `${sessionId}.jsonl`);
    assert.deepEqual(readLines(main).map((e) => e.uuid), ["u1", "u2"]);
    const sub = join(home, "projects", "-work", sessionId, "subagents", "agent-a");
    assert.deepEqual(readLines(`${sub}.jsonl`).map((e) => e.uuid), ["s1"]);
    assert.deepEqual(JSON.parse(readFileSync(`${sub}.meta.json`, "utf8")), { agentType: "Explore", description: "look around" });
  });

  test("a transcript present locally is left as it is", async () => {
    const store = await makeStore();
    const sessionId = randomUUID();
    const { projectKey, main } = writeLocal("/work", sessionId, [message("u1", "local")]);
    await store.append({ projectKey, sessionId }, [message("u1", "local"), message("u2", "only in the store")]);
    assert.equal(await Effect.runPromise(ensureLocalTranscript(store, sessionId)), true);
    assert.deepEqual(readLines(main).map((e) => e.uuid), ["u1"]);
  });

  test("a session the store has never held is not found there", async () => {
    const store = await makeStore();
    assert.equal(await Effect.runPromise(ensureLocalTranscript(store, randomUUID())), false);
  });

  test("adoption imports a session whole, then only adds what the mirror dropped", async () => {
    const store = await makeStore();
    const dir = join(home, "work");
    const sessionId = randomUUID();
    const first = [message("11111111-1111-4111-8111-111111111111", "one"), { type: "summary", summary: "s" }];
    const { projectKey, main } = writeLocal(dir, sessionId, first, {
      "agent-a": [message("22222222-2222-4222-8222-222222222222", "sub")],
    });

    await Effect.runPromise(adoptTranscripts({ store, sessions: [{ sessionId, workingDirectory: dir }] }));
    const key = { projectKey, sessionId };
    assert.equal((await store.load(key))?.length, 2);
    assert.equal((await store.load({ ...key, subpath: "subagents/agent-a" }))?.length, 1);

    // The mirror missed two entries, and the metadata Claude Code writes as
    // it exits, outside the mirror; the summary already stored is not copied again.
    const later = [
      ...first,
      message("33333333-3333-4333-8333-333333333333", "two"),
      message("44444444-4444-4444-8444-444444444444", "three"),
      { type: "cost-state", totalCostUSD: 1 },
    ];
    writeFileSync(main, later.map((e) => JSON.stringify(e)).join("\n") + "\n");
    await Effect.runPromise(adoptTranscripts({ store, sessions: [{ sessionId, workingDirectory: dir }] }));
    await Effect.runPromise(adoptTranscripts({ store, sessions: [{ sessionId, workingDirectory: dir }] }));
    assert.deepEqual(
      (await store.load(key))?.map((e) => e.uuid ?? e.type),
      ["11111111-1111-4111-8111-111111111111", "summary", "33333333-3333-4333-8333-333333333333", "44444444-4444-4444-8444-444444444444", "cost-state"],
    );
  });

  test("a session with no local transcript is left to the store", async () => {
    const store = await makeStore();
    await Effect.runPromise(adoptTranscripts({ store, sessions: [{ sessionId: randomUUID(), workingDirectory: "/nowhere" }] }));
    assert.equal(existsSync(join(home, "projects")), false);
  });
});
