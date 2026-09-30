/**
 * Codex's sessions end to end, against the real app-server binary alasio
 * runs: turns answered by a local stand-in for the Responses API, so no login
 * and no model; the session panels' api over them; rewind; and the rollout
 * store keeping, and writing back after they are lost, the files of a thread
 * whose history starts in another's.
 */
import assert from "node:assert/strict";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, describe, test } from "node:test";
import { zstdDecompressSync } from "node:zlib";

import pg from "pg";

import { AppServerClient } from "../src/codex/app-server/client.js";
import { buildCodexEnv } from "../src/codex/env.js";
import { listRolloutFiles } from "../src/codex/rollouts/files.js";
import { startCodexRollouts } from "../src/codex/rollouts/index.js";
import { restoreRollouts } from "../src/codex/rollouts/restore.js";
import { NeonRolloutStore } from "../src/codex/rollouts/store.js";
import { createCodexSessionApi } from "../src/codex/sessions.js";
import { dockerAvailable, startPostgres } from "./support/postgres.js";

const CODEX_BIN = new URL("../node_modules/.bin/codex", import.meta.url).pathname;
const skip = !existsSync(CODEX_BIN)
  ? "needs the Codex binary alasio installs"
  : !dockerAvailable() && "needs Docker for a throwaway Postgres";

/** Server-sent events as the Responses API streams them. */
function sse(events) {
  return events.map((event) => `event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`).join("");
}

/** A stand-in for the Responses API that answers the nth request "answer n", and keeps what each asked. */
async function startResponses() {
  const requests = [];
  const server = createServer((request, response) => {
    const body = [];
    request.on("data", (chunk) => body.push(chunk));
    request.on("end", () => {
      if (request.method !== "POST" || !request.url.endsWith("/responses")) {
        response.writeHead(404).end();
        return;
      }
      const raw = Buffer.concat(body);
      const text = request.headers["content-encoding"] === "zstd" ? zstdDecompressSync(raw).toString() : raw.toString();
      requests.push(text);
      const id = `resp_${requests.length}`;
      response.writeHead(200, { "content-type": "text/event-stream" });
      response.end(sse([
        { type: "response.created", response: { id } },
        {
          type: "response.output_item.done",
          item: { type: "message", role: "assistant", id: `msg_${requests.length}`, content: [{ type: "output_text", text: `answer ${requests.length}` }] },
        },
        {
          type: "response.completed",
          response: { id, usage: { input_tokens: 0, input_tokens_details: null, output_tokens: 0, output_tokens_details: null, total_tokens: 0 } },
        },
      ]));
    });
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  return { url: `http://127.0.0.1:${server.address().port}/v1`, requests, close: () => new Promise((resolve) => server.close(resolve)) };
}

/** A Codex home of its own, whose only provider is the stand-in. */
function makeCodexHome(root, name, providerUrl) {
  const home = join(root, name);
  mkdirSync(home, { recursive: true });
  writeFileSync(join(home, "config.toml"), `
model_provider = "stand_in"

[model_providers.stand_in]
name = "stand-in"
base_url = "${providerUrl}"
wire_api = "responses"
request_max_retries = 0
stream_max_retries = 0
`);
  return home;
}

test("sessions are labelled by name, else first prompt; turns without a prompt are no rewind point; an unreadable thread has none", async () => {
  const turn = (id, prompt, answer) => ({
    id,
    startedAt: 1_790_000_000,
    items: [
      ...(prompt === null ? [] : [{ type: "userMessage", content: [{ type: "image", url: "x" }, { type: "text", text: prompt }] }]),
      ...(answer ? [{ type: "agentMessage", text: answer }] : []),
    ],
  });
  const client = {
    listThreads: async () => [
      { id: "named", name: "Named   thread", preview: "ignored", updatedAt: 1_790_000_000 },
      { id: "prompted", name: null, preview: "a first prompt that is well over forty characters long", updatedAt: 0 },
      { id: "empty-thread-id", name: null, preview: "", updatedAt: 1_790_000_000 },
    ],
    listTurns: async ({ threadId }) => {
      if (threadId === "gone") throw new Error("thread not found");
      return [turn("t3", null, "continued on its own"), turn("t2", "second", null), turn("t1", "first", "answered")];
    },
  };
  const sessions = createCodexSessionApi({ workingDirectory: "/work", listingScope: async () => ({ cwd: "/work", codexEnv: {}, client }) });
  assert.deepEqual(await sessions.listSessions(1), [
    { uuid: "named", timestamp: "2026-09-21", label: "Named thread" },
    { uuid: "prompted", timestamp: "-", label: "a first prompt that is well over fort..." },
    { uuid: "empty-thread-id", timestamp: "2026-09-21", label: "empty-thread-id" },
  ]);
  assert.deepEqual(
    (await sessions.listSessionMessages("thread")).map(({ index, text, uuid }) => ({ index, text, uuid })),
    [{ index: -1, text: "second", uuid: "t2" }, { index: -2, text: "first", uuid: "t1" }],
  );
  assert.equal(await sessions.getSessionLastMessage("thread"), "continued on its own");
  assert.deepEqual(await sessions.listSessionMessages("gone"), []);
  assert.equal(await sessions.getSessionLastMessage("gone"), null);
});

describe("Codex sessions through the app-server", { skip }, () => {
  let root;
  let responses;
  let database;
  let pool;
  let client;
  let rollouts;
  const savedCodexHome = process.env.CODEX_HOME;

  before(async () => {
    root = mkdtempSync(join(tmpdir(), "alasio-codex-sessions-"));
    responses = await startResponses();
    database = await startPostgres();
    pool = new pg.Pool({ connectionString: database.url, max: 4 });
  });

  after(async () => {
    await rollouts?.close();
    client?.stop();
    await pool?.end();
    await database?.stop();
    await responses?.close();
    if (savedCodexHome === undefined) delete process.env.CODEX_HOME;
    else process.env.CODEX_HOME = savedCodexHome;
    rmSync(root, { recursive: true, force: true });
  });

  /** One turn on a thread, run to its end. */
  async function runTurn(threadId, prompt, cwd) {
    const turnId = await client.startTurn({ threadId, prompt, cwd, env: buildCodexEnv(), config: {} });
    for await (const event of client.eventsForTurn(threadId, turnId)) {
      if (event.type === "turn.failed") throw new Error(event.error.message);
    }
  }

  test("sessions list, rewind forks, and a lost thread comes back whole from the store", async () => {
    const workingDirectory = join(root, "work");
    mkdirSync(workingDirectory);
    process.env.CODEX_HOME = makeCodexHome(root, "codex-home", responses.url);
    client = new AppServerClient();
    const forks = [];
    const sessions = createCodexSessionApi({
      workingDirectory,
      listingScope: async () => ({ cwd: workingDirectory, codexEnv: buildCodexEnv(), client }),
      fork: async ({ sessionId, beforeTurnId, threadKey }) => {
        forks.push(threadKey);
        return await client.forkThread({ threadId: sessionId, beforeTurnId, threadKey, cwd: workingDirectory, env: buildCodexEnv(), config: {} });
      },
    });

    // Mirrored from the start, as alasio does.
    const home = process.env.CODEX_HOME;
    const store = new NeonRolloutStore(pool, { schema: "codex_sessions_e2e" });
    await store.ensureSchema();
    rollouts = startCodexRollouts({ store, home });
    /** Whether the store holds exactly what a thread's files hold now. */
    const heldExactly = async (id) => {
      const files = listRolloutFiles(home).filter((file) => file.name.includes(id));
      for (const file of files) {
        if (!(await store.read(file.name)).equals(readFileSync(join(home, file.path)))) return false;
      }
      return files.length > 0;
    };

    const threadId = await client.startThread({ threadKey: "conversation-1", cwd: workingDirectory, env: buildCodexEnv(), config: {} });
    for (const prompt of ["remember the heron", "and now the crane"]) {
      await runTurn(threadId, prompt, workingDirectory);
      // What Codex wrote by the turn's end is all in the store once a flush returns.
      await rollouts.flush(threadId);
      assert.ok(await heldExactly(threadId));
    }

    assert.equal(await sessions.getTotalSessionPages(), 1);
    const [listed] = await sessions.listSessions(1);
    assert.equal(listed.uuid, threadId);
    assert.equal(listed.label, "remember the heron");
    assert.match(listed.timestamp, /^\d{4}-\d{2}-\d{2}$/);
    assert.equal(await sessions.getSessionByNumber(1), threadId);
    assert.equal(await sessions.getSessionByNumber(2), null);
    assert.equal(await sessions.getSessionLastMessage(threadId), "answer 2");
    const messages = await sessions.listSessionMessages(threadId);
    assert.deepEqual(messages.map(({ index, text }) => ({ index, text })), [
      { index: -1, text: "and now the crane" },
      { index: -2, text: "remember the heron" },
    ]);
    assert.equal(await sessions.getTotalRewindPages(threadId), 1);

    // Rewind to before the second message: a new thread with the first turn's history only.
    const forkedId = await sessions.createForkedSession(threadId, messages[0].uuid, { threadKey: "conversation-1" });
    assert.ok(forkedId && forkedId !== threadId);
    assert.deepEqual(forks, ["conversation-1"]);
    assert.equal(await sessions.createForkedSession(threadId, "no-such-turn", { threadKey: "conversation-1" }), null);

    // The fork is mirrored too, unasked: the watcher reports its file.
    const deadline = Date.now() + 3_000;
    while (!(await heldExactly(forkedId)) && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 20));
    assert.ok(await heldExactly(forkedId));
    const files = listRolloutFiles(home);
    assert.equal(files.length, 2);
    const originals = new Map(files.map((file) => [file.path, { bytes: readFileSync(join(home, file.path)), modifiedMs: file.modifiedMs }]));
    await rollouts.close();

    // Lose everything: the files, and every index Codex made from them.
    client.stop();
    process.env.CODEX_HOME = makeCodexHome(root, "new-codex-home", responses.url);
    const written = await restoreRollouts({ store, threadIds: [forkedId], home: process.env.CODEX_HOME });
    // The fork's history starts in the first thread's file, so both come back.
    assert.deepEqual(written.sort(), [...originals.keys()].sort());
    for (const [path, original] of originals) {
      assert.deepEqual(readFileSync(join(process.env.CODEX_HOME, path)), original.bytes);
      assert.equal(Math.round(statSync(join(process.env.CODEX_HOME, path)).mtimeMs), Math.round(original.modifiedMs));
    }

    // Codex resumes the fork again, and its next turn carries the first
    // turn's history and not the rewound one. The client starts a new
    // app-server, on the new Codex home, as it does after any stop.
    await client.ensureThread({ threadId: forkedId, threadKey: "conversation-1", cwd: workingDirectory, env: buildCodexEnv(), config: {} });
    await runTurn(forkedId, "which bird?", workingDirectory);
    const lastRequest = responses.requests.at(-1);
    assert.match(lastRequest, /remember the heron/);
    assert.match(lastRequest, /answer 1/);
    assert.match(lastRequest, /which bird\?/);
    assert.doesNotMatch(lastRequest, /and now the crane/);
    assert.equal(await sessions.getSessionLastMessage(forkedId), `answer ${responses.requests.length}`);
    // Codex lists a thread once it holds a prompt of its own: both, now.
    assert.deepEqual(
      (await sessions.listSessions(1)).map(({ uuid }) => uuid).sort(),
      [threadId, forkedId].sort(),
    );
  });
});
