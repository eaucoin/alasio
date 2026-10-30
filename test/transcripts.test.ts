import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import { closeSync, existsSync, mkdirSync, mkdtempSync, openSync, readFileSync, rmSync, statSync, writeFileSync, writeSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, beforeEach, describe, test } from "node:test";
import { promisify } from "node:util";

import type { SessionStoreEntry } from "@anthropic-ai/claude-agent-sdk";
import { Effect } from "effect";
import pg from "pg";

import { NeonSessionStore } from "../src/harness/claude/session-store.ts";
import { adoptTranscripts, ensureLocalTranscript } from "../src/harness/claude/transcripts.ts";
import { closedBeforeStop, startPostgres, type TestPostgres } from "./support/postgres.ts";

// Set by the first hook.
let database: TestPostgres | undefined;
let pool: pg.Pool;
// Each case's Claude home, made before it.
let home: string;
let previousHome: string | undefined;
let schemas = 0;

before(async () => {
  database = await startPostgres();
  pool = new pg.Pool({ connectionString: database.url, max: 4 });
  // Ended before the server stops, as the test file's tests end, which would terminate its idle connections first.
  closedBeforeStop(() => pool.end());
});

after(async () => {
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

async function makeStore(schema = `transcripts_${process.pid}_${++schemas}`): Promise<NeonSessionStore> {
  const store = new NeonSessionStore(pool, { schema });
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

describe("claude transcripts against the store", () => {
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

  test("a transcript several times the size of the heap is reconciled a line at a time, holding what it matches by and a batch alone", async () => {
    const schema = `transcripts_${process.pid}_${++schemas}`;
    const store = await makeStore(schema);
    const dir = join(home, "work");
    const sessionId = randomUUID();
    const { projectKey, main } = writeLocal(dir, sessionId, []);
    const key = { projectKey, sessionId };
    // Turns of 64 KiB, as a tool's output makes them, each stored in brief, as matching by
    // uuid reads no more of them, with the metadata Claude Code writes again on each.
    const turn = (n: number) => message(`00000000-0000-4000-8000-${String(n).padStart(12, "0")}`, `${n} `.padEnd(64 * 1024, "x"));
    const prompt = (n: number) => ({ type: "last-prompt", lastPrompt: `prompt ${n}`, sessionId });
    const turns = Array.from({ length: 4096 }, (_, n) => n);
    await store.append(key, turns.flatMap((n) => [{ ...turn(n), message: { role: "user", content: "brief" } }, prompt(n)]));
    const file = openSync(main, "w");
    for (const n of turns) writeSync(file, `${JSON.stringify(turn(n))}\n${JSON.stringify(prompt(n))}\n`);
    // And what the mirror dropped.
    writeSync(file, `${JSON.stringify(message("99999999-9999-4999-8999-999999999999", "dropped"))}\n${JSON.stringify({ type: "cost-state", totalCostUSD: 1 })}\n`);
    closeSync(file);
    assert.ok(statSync(main).size > 256 * 1024 ** 2);

    // The reconcile in a process of its own, as the harness runs it, its heap limited to a quarter of the transcript.
    const script = [
      'import { Effect } from "effect";',
      'import pg from "pg";',
      `import { NeonSessionStore } from ${JSON.stringify(new URL("../src/harness/claude/session-store.ts", import.meta.url).href)};`,
      `import { adoptTranscripts } from ${JSON.stringify(new URL("../src/harness/claude/transcripts.ts", import.meta.url).href)};`,
      `const pool = new pg.Pool({ connectionString: ${JSON.stringify(database?.url)} });`,
      `const store = new NeonSessionStore(pool, { schema: ${JSON.stringify(schema)} });`,
      `await Effect.runPromise(adoptTranscripts({ store, sessions: [{ sessionId: ${JSON.stringify(sessionId)}, workingDirectory: ${JSON.stringify(dir)} }] }));`,
      "await pool.end();",
    ].join("\n");
    await promisify(execFile)(process.execPath, ["--max-old-space-size=64", "--input-type=module", "-e", script], { cwd: new URL("..", import.meta.url).pathname, env: { ...process.env, CLAUDE_CONFIG_DIR: home } });

    const stored = (await store.load(key)) ?? [];
    assert.equal(stored.length, 2 * turns.length + 2);
    assert.deepEqual(stored.slice(-2).map((e) => e.uuid ?? e.type), ["99999999-9999-4999-8999-999999999999", "cost-state"]);
  });

  test("a session with no local transcript is left to the store", async () => {
    const store = await makeStore();
    await Effect.runPromise(adoptTranscripts({ store, sessions: [{ sessionId: randomUUID(), workingDirectory: "/nowhere" }] }));
    assert.equal(existsSync(join(home, "projects")), false);
  });
});
