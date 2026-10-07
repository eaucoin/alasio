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

import { Effect, Exit, Logger, Scope, Stream } from "effect";
import pg from "pg";

import { type AppServer, makeAppServer } from "../src/codex/app-server/client.ts";
import { AppServerRequestFailed } from "../src/codex/app-server/rpc-client.ts";
import { buildCodexEnv } from "../src/codex/env.ts";
import { listRolloutFiles } from "../src/codex/rollouts/files.ts";
import { makeCodexRollouts } from "../src/codex/rollouts/index.ts";
import { restoreRollouts } from "../src/codex/rollouts/restore.ts";
import { NeonRolloutStore } from "../src/codex/rollouts/store.ts";
import { type SessionListingScope, createCodexSessionApi } from "../src/codex/sessions.ts";
import type { CodexThreadConfig } from "../src/codex/thread-config.ts";
import { agentMessage, codexThread, codexTurn, userMessage } from "./support/codex-protocol.ts";
import { type TestPostgres, startPostgres } from "./support/postgres.ts";

const CODEX_BIN = new URL("../node_modules/.bin/codex", import.meta.url).pathname;
const skip = !existsSync(CODEX_BIN) && "needs the Codex binary alasio installs";

/** An event the Responses API streams: its type, and what it carries. */
interface ResponsesEvent {
  readonly type: string;
  readonly [field: string]: unknown;
}

/** The stand-in for the Responses API: its URL, the request bodies it was sent, and how to stop it. */
interface ResponsesStandIn {
  readonly url: string;
  readonly requests: readonly string[];
  close(): Promise<void>;
}

/** Runs the session panels' effects, their log lines dropped. */
const run = <A, E>(effect: Effect.Effect<A, E>): Promise<A> => Effect.runPromise(effect.pipe(Effect.provideService(Logger.CurrentLoggers, new Set())));

/** No config beyond what alasio itself sets on a thread. */
const NO_CONFIG: CodexThreadConfig = { developer_instructions: "", mcp_servers: {} };

/** Server-sent events as the Responses API streams them. */
function sse(events: readonly ResponsesEvent[]): string {
  return events.map((event) => `event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`).join("");
}

/** A stand-in for the Responses API that answers the nth request "answer n", and keeps what each asked. */
async function startResponses(): Promise<ResponsesStandIn> {
  const requests: string[] = [];
  const server = createServer((request, response) => {
    const body: Buffer[] = [];
    request.on("data", (chunk: Buffer) => body.push(chunk));
    request.on("end", () => {
      if (request.method !== "POST" || !request.url?.endsWith("/responses")) {
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
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  assert.ok(address !== null && typeof address === "object", "a TCP server's address");
  return { url: `http://127.0.0.1:${address.port}/v1`, requests, close: () => new Promise((resolve) => server.close(() => resolve())) };
}

/** A Codex home of its own, whose only provider is the stand-in. */
function makeCodexHome(root: string, name: string, providerUrl: string): string {
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
  const turn = (id: string, prompt: string | null, answer: string | null) => codexTurn(id, {
    startedAt: 1_790_000_000,
    items: [
      ...(prompt === null ? [] : [userMessage(`${id}-prompt`, [{ type: "image", url: "x" }, { type: "text", text: prompt, text_elements: [] }])]),
      ...(answer ? [agentMessage(`${id}-answer`, answer)] : []),
    ],
  });
  const appServer: SessionListingScope["appServer"] = {
    listThreads: () => Effect.succeed([
      codexThread("named", { name: "Named   thread", preview: "ignored", updatedAt: 1_790_000_000 }),
      codexThread("prompted", { name: null, preview: "a first prompt that is well over forty characters long", updatedAt: 0 }),
      codexThread("empty-thread-id", { name: null, preview: "", updatedAt: 1_790_000_000 }),
    ]),
    listTurns: ({ threadId }) =>
      threadId === "gone"
        ? Effect.fail(new AppServerRequestFailed({ error: { message: "thread not found" } }))
        : Effect.succeed([turn("t3", null, "continued on its own"), turn("t2", "second", null), turn("t1", "first", "answered")]),
  };
  const sessions = createCodexSessionApi({
    listingScope: Effect.succeed({ cwd: "/work", codexEnv: {}, appServer }),
    fork: () => Effect.sync(() => assert.fail("nothing is forked")),
  });
  assert.deepEqual(await run(sessions.listSessions(1)), [
    { uuid: "named", timestamp: "2026-09-21", label: "Named thread" },
    { uuid: "prompted", timestamp: "-", label: "a first prompt that is well over fort..." },
    { uuid: "empty-thread-id", timestamp: "2026-09-21", label: "empty-thread-id" },
  ]);
  assert.deepEqual(
    (await run(sessions.listSessionMessages("thread"))).map(({ index, text, uuid }) => ({ index, text, uuid })),
    [{ index: -1, text: "second", uuid: "t2" }, { index: -2, text: "first", uuid: "t1" }],
  );
  assert.equal(await run(sessions.getSessionLastMessage("thread")), "continued on its own");
  assert.deepEqual(await run(sessions.listSessionMessages("gone")), []);
  assert.equal(await run(sessions.getSessionLastMessage("gone")), null);
});

describe("Codex sessions through the app-server", { skip }, () => {
  let root: string | undefined;
  let responses: ResponsesStandIn | undefined;
  let database: TestPostgres | undefined;
  let pool: pg.Pool | undefined;
  /** The scope the app-server runs in, as alasio runs it. */
  let running: Scope.Closeable | undefined;
  /** The scope the rollouts are kept in, as alasio keeps them while it runs. */
  let mirroring: Scope.Closeable | undefined;
  const savedCodexHome = process.env["CODEX_HOME"];

  before(async () => {
    root = mkdtempSync(join(tmpdir(), "alasio-codex-sessions-"));
    responses = await startResponses();
    database = await startPostgres();
    pool = new pg.Pool({ connectionString: database.url, max: 4 });
  });

  after(async () => {
    if (mirroring) await Effect.runPromise(Scope.close(mirroring, Exit.void));
    if (running) await Effect.runPromise(Scope.close(running, Exit.void));
    await pool?.end();
    await database?.stop();
    await responses?.close();
    if (savedCodexHome === undefined) delete process.env["CODEX_HOME"];
    else process.env["CODEX_HOME"] = savedCodexHome;
    if (root) rmSync(root, { recursive: true, force: true });
  });

  /** One turn on a thread of conversation-1, the conversation every thread here is for, run to its end. */
  async function runTurn(client: AppServer, threadId: string, prompt: string, cwd: string): Promise<void> {
    await run(Effect.gen(function*() {
      const turnId = yield* client.startTurn({ threadId, threadKey: "conversation-1", prompt, cwd, env: buildCodexEnv(), config: NO_CONFIG });
      yield* Stream.runForEach(client.eventsForTurn(threadId, turnId), (event) =>
        event.type === "turn.failed" ? Effect.sync(() => assert.fail(event.error.message)) : Effect.void);
    }));
  }

  test("sessions list, rewind forks, and a lost thread comes back whole from the store", async () => {
    assert.ok(root && responses && pool, "set up before the tests");
    const workingDirectory = join(root, "work");
    mkdirSync(workingDirectory);
    const home = makeCodexHome(root, "codex-home", responses.url);
    process.env["CODEX_HOME"] = home;
    running = Effect.runSync(Scope.make());
    const client = await run(makeAppServer().pipe(Scope.provide(running)));
    const forks: string[] = [];
    const sessions = createCodexSessionApi({
      listingScope: Effect.sync(() => ({ cwd: workingDirectory, codexEnv: buildCodexEnv(), appServer: client })),
      fork: ({ sessionId, beforeTurnId, threadKey }) => {
        forks.push(threadKey);
        return client.forkThread({ threadId: sessionId, beforeTurnId, threadKey, cwd: workingDirectory, env: buildCodexEnv(), config: NO_CONFIG });
      },
    });

    // Mirrored from the start, as alasio does.
    const store = new NeonRolloutStore(pool, { schema: "codex_sessions_e2e" });
    await store.ensureSchema();
    const scope = Effect.runSync(Scope.make());
    mirroring = scope;
    const rollouts = await Effect.runPromise(makeCodexRollouts({ store, home }).pipe(Scope.provide(scope)));
    /** Whether the store holds exactly what a thread's files hold now. */
    const heldExactly = async (id: string) => {
      const files = listRolloutFiles(home).filter((file) => file.name.includes(id));
      for (const file of files) {
        if (!(await store.read(file.name)).equals(readFileSync(join(home, file.path)))) return false;
      }
      return files.length > 0;
    };

    const threadId = await run(client.startThread({ threadKey: "conversation-1", cwd: workingDirectory, env: buildCodexEnv(), config: NO_CONFIG }));
    for (const prompt of ["remember the heron", "and now the crane"]) {
      await runTurn(client, threadId, prompt, workingDirectory);
      // What Codex wrote by the turn's end is all in the store once a flush returns.
      await Effect.runPromise(rollouts.flush(threadId));
      assert.ok(await heldExactly(threadId));
    }

    assert.equal(await run(sessions.getTotalSessionPages()), 1);
    const [listed] = await run(sessions.listSessions(1));
    assert.ok(listed);
    assert.equal(listed.uuid, threadId);
    assert.equal(listed.label, "remember the heron");
    assert.match(listed.timestamp, /^\d{4}-\d{2}-\d{2}$/);
    assert.equal(await run(sessions.getSessionByNumber(1)), threadId);
    assert.equal(await run(sessions.getSessionByNumber(2)), null);
    assert.equal(await run(sessions.getSessionLastMessage(threadId)), "answer 2");
    const messages = await run(sessions.listSessionMessages(threadId));
    assert.deepEqual(messages.map(({ index, text }) => ({ index, text })), [
      { index: -1, text: "and now the crane" },
      { index: -2, text: "remember the heron" },
    ]);
    assert.equal(await run(sessions.getTotalRewindPages(threadId)), 1);

    // Rewind to before the second message: a new thread with the first turn's history only.
    const [latest] = messages;
    assert.ok(latest);
    const forkedId = await run(sessions.createForkedSession(threadId, latest.uuid, { threadKey: "conversation-1" }));
    assert.ok(forkedId && forkedId !== threadId);
    assert.deepEqual(forks, ["conversation-1"]);
    assert.equal(await run(sessions.createForkedSession(threadId, "no-such-turn", { threadKey: "conversation-1" })), null);

    // The fork is mirrored too, unasked: the watcher reports its file.
    const deadline = Date.now() + 3_000;
    while (!(await heldExactly(forkedId)) && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 20));
    assert.ok(await heldExactly(forkedId));
    const files = listRolloutFiles(home);
    assert.equal(files.length, 2);
    const originals = new Map(files.map((file) => [file.path, { bytes: readFileSync(join(home, file.path)), modifiedMs: file.modifiedMs }]));
    await Effect.runPromise(Scope.close(scope, Exit.void));

    // Lose everything: the files, and every index Codex made from them.
    await run(client.stop);
    const newHome = makeCodexHome(root, "new-codex-home", responses.url);
    process.env["CODEX_HOME"] = newHome;
    const written = await Effect.runPromise(restoreRollouts({ store, threadIds: [forkedId], home: newHome }));
    // The fork's history starts in the first thread's file, so both come back.
    assert.deepEqual(written.sort(), [...originals.keys()].sort());
    for (const [path, original] of originals) {
      assert.deepEqual(readFileSync(join(newHome, path)), original.bytes);
      assert.equal(Math.round(statSync(join(newHome, path)).mtimeMs), Math.round(original.modifiedMs));
    }

    // Codex resumes the fork again, and its next turn carries the first
    // turn's history and not the rewound one. The client starts a new
    // app-server, on the new Codex home, as it does after any stop.
    await run(client.ensureThread({ threadId: forkedId, threadKey: "conversation-1", cwd: workingDirectory, env: buildCodexEnv(), config: NO_CONFIG }));
    await runTurn(client, forkedId, "which bird?", workingDirectory);
    const lastRequest = responses.requests.at(-1) ?? "";
    assert.match(lastRequest, /remember the heron/);
    assert.match(lastRequest, /answer 1/);
    assert.match(lastRequest, /which bird\?/);
    assert.doesNotMatch(lastRequest, /and now the crane/);
    assert.equal(await run(sessions.getSessionLastMessage(forkedId)), `answer ${responses.requests.length}`);
    // Codex lists a thread once it holds a prompt of its own: both, now.
    assert.deepEqual(
      (await run(sessions.listSessions(1))).map(({ uuid }) => uuid).sort(),
      [threadId, forkedId].sort(),
    );
  });
});
