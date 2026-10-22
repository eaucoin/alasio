/**
 * The Codex threads the operator's session panels list, read from the rollouts kept in Neon
 * (src/codex/rollouts/listing.ts) and through Codex's own app-server, on one home the pinned
 * Codex wrote: threads of two directories, one with no prompt, one resumed by a new app-server,
 * and a fork. The panels say the same of each, whichever they read.
 */
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, utimesSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { zstdCompressSync } from "node:zlib";
import { test } from "node:test";

import { Effect, Exit, Logger, Scope, Stream } from "effect";
import pg from "pg";

import { type AppServer, makeAppServer } from "../src/codex/app-server/client.ts";
import { buildCodexEnv } from "../src/codex/env.ts";
import { listRolloutFiles } from "../src/codex/rollouts/files.ts";
import { makeCodexRollouts } from "../src/codex/rollouts/index.ts";
import { rolloutListing } from "../src/codex/rollouts/listing.ts";
import { NeonRolloutStore } from "../src/codex/rollouts/store.ts";
import { createCodexSessionApi, type SessionListingScope } from "../src/codex/sessions.ts";
import type { CodexThreadConfig } from "../src/codex/thread-config.ts";
import type { HarnessSessions } from "../src/harness/index.ts";
import { makeCodexHome, startResponses } from "./support/codex-responses.ts";
import { startPostgres } from "./support/postgres.ts";

const CODEX_BIN = new URL("../node_modules/.bin/codex", import.meta.url).pathname;
const skip = !existsSync(CODEX_BIN) && "needs the Codex binary alasio installs";

/** Runs an effect, its log lines dropped. */
const run = <A, E>(effect: Effect.Effect<A, E>): Promise<A> => Effect.runPromise(effect.pipe(Effect.provideService(Logger.CurrentLoggers, new Set())));

/** No config beyond what alasio itself sets on a thread. */
const NO_CONFIG: CodexThreadConfig = { developer_instructions: "", mcp_servers: {} };


/** Archives the thread `threadId` as the operator archives one, through an app-server of the pinned Codex's, asked as alasio asks none (thread/archive). */
async function archive(threadId: string, cwd: string): Promise<void> {
  const child = spawn(CODEX_BIN, ["app-server", "--listen", "stdio://"], { cwd, env: { ...process.env, ...buildCodexEnv() }, stdio: ["pipe", "pipe", "inherit"] });
  try {
    const answers = createInterface({ input: child.stdout });
    const answered = new Map<number, (message: { readonly result?: unknown; readonly error?: { readonly message: string } }) => void>();
    answers.on("line", (line) => {
      const message = JSON.parse(line) as { readonly id?: number; readonly result?: unknown; readonly error?: { readonly message: string } };
      if (typeof message.id === "number") answered.get(message.id)?.(message);
    });
    const ask = (id: number, method: string, params: object) =>
      new Promise<unknown>((resolve, reject) => {
        answered.set(id, (message) => (message.error ? reject(new Error(`${method}: ${message.error.message}`)) : resolve(message.result)));
        child.stdin.write(`${JSON.stringify({ id, method, params })}\n`);
      });
    await ask(1, "initialize", { clientInfo: { name: "alasio_test", title: "alasio's tests", version: "1.0.0" }, capabilities: { experimentalApi: true } });
    child.stdin.write(`${JSON.stringify({ method: "initialized" })}\n`);
    await ask(2, "thread/archive", { threadId });
  } finally {
    child.kill("SIGTERM");
    await once(child, "exit");
  }
}

/** All the panels say of `cwd`'s threads, each thread's last message, rewind points and their pages among it. */
async function panelsOf(sessions: HarnessSessions): Promise<unknown> {
  const pages = await run(sessions.getTotalSessionPages());
  const listed = await run(sessions.listSessions(1));
  const threads = [];
  for (const { uuid } of listed) {
    threads.push({
      uuid,
      last: await run(sessions.getSessionLastMessage(uuid)),
      messages: await run(sessions.listSessionMessages(uuid)),
      rewindPages: await run(sessions.getTotalRewindPages(uuid)),
    });
  }
  const numbered = [];
  for (let number = 1; number <= listed.length + 1; number += 1) numbered.push(await run(sessions.getSessionByNumber(number)));
  return { pages, listed, numbered, threads };
}

test("the session panels say of Codex's threads, read from the rollouts kept in Neon, what Codex's own app-server says of them", { skip, timeout: 120_000 }, async () => {
  const root = mkdtempSync(join(tmpdir(), "alasio-rollout-listing-"));
  const responses = await startResponses();
  const database = await startPostgres();
  const pool = new pg.Pool({ connectionString: database.url, max: 4 });
  const savedCodexHome = process.env["CODEX_HOME"];
  const scopes: Scope.Closeable[] = [];
  try {
    const [here, elsewhere] = ["here", "elsewhere"].map((name) => join(root, name));
    for (const directory of [here!, elsewhere!]) mkdirSync(directory);
    const home = makeCodexHome(root, "codex-home", responses.url);
    process.env["CODEX_HOME"] = home;
    const store = new NeonRolloutStore(pool, { schema: "codex_listing" });
    await store.ensureSchema();
    const mirroring = Effect.runSync(Scope.make());
    scopes.push(mirroring);
    const rollouts = await Effect.runPromise(makeCodexRollouts({ store, home }).pipe(Scope.provide(mirroring)));
    /** An app-server on the home, as alasio starts one. */
    const appServer = async () => {
      const running = Effect.runSync(Scope.make());
      scopes.push(running);
      return run(makeAppServer().pipe(Scope.provide(running)));
    };
    let client: AppServer = await appServer();
    const turn = (threadId: string, prompt: string, cwd: string) =>
      run(Effect.gen(function*() {
        const turnId = yield* client.startTurn({ threadId, threadKey: "c1", prompt, cwd, env: buildCodexEnv(), config: NO_CONFIG });
        yield* Stream.runForEach(client.eventsForTurn(threadId, turnId), (event) => (event.type === "turn.failed" ? Effect.sync(() => assert.fail(event.error.message)) : Effect.void));
      }));
    const thread = (cwd: string) => run(client.startThread({ threadKey: "c1", cwd, env: buildCodexEnv(), config: NO_CONFIG }));
    /** A second between threads' last changes, so their order is the same whoever sorts them. */
    const apart = () => new Promise((resolve) => setTimeout(resolve, 1_100));
    /**
     * Gives the threads `[made first, made after]` files that last changed within one second, the
     * one made first 400 ms after the other, in the second its last turn ended: on the home, which
     * Codex lists by, and as Neon keeps them, as the mirror keeps a file's time only as its bytes
     * change. Set, not waited for: two turns one after the other end within a second only on a
     * machine not loaded. Each a second or more from the threads before and after, as `apart` is.
     */
    const tie = async ([before, after]: readonly string[]) => {
      const fileOf = (threadId: string) => {
        const [file] = listRolloutFiles(home).filter(({ name }) => name.includes(threadId));
        assert.ok(file, `no rollout of ${threadId}`);
        return file;
      };
      const second = Math.floor(fileOf(before!).modifiedMs / 1_000) * 1_000;
      for (const [threadId, ms] of [[after!, second + 100], [before!, second + 500]] as const) {
        const file = fileOf(threadId);
        utimesSync(join(home, file.path), new Date(ms), new Date(ms));
        const { rowCount } = await pool.query("update codex_listing.rollouts set modified_ms = $2 where name = $1", [file.name, ms]);
        assert.equal(rowCount, 1, `Neon keeps no rollout ${file.name}`);
      }
    };

    const first = await thread(here!);
    await turn(first, "remember the heron", here!);
    await turn(first, "and now the crane", here!);
    await apart();
    const other = await thread(elsewhere!);
    await turn(other, "a thread of another folder", elsewhere!);
    await apart();
    // One with no prompt of its own, which Codex does not list.
    await thread(here!);
    // A fork before the crane, with a turn of its own.
    const [crane] = await run(client.listTurns({ threadId: first, env: buildCodexEnv(), cwd: here! }));
    assert.ok(crane);
    const fork = await run(client.forkThread({ threadId: first, beforeTurnId: crane.id, threadKey: "c1", cwd: here!, env: buildCodexEnv(), config: NO_CONFIG }));
    await turn(fork, "which bird?", here!);
    await apart();
    // One kept in Neon before its first prompt was written, its first chunk holding none of it: its prompt the next chunk's.
    const late = await thread(here!);
    await Effect.runPromise(rollouts.flush(late));
    await turn(late, "a prompt past its first chunk", here!);
    await apart();
    // Two whose files last changed within one second, the one made first changed last, as two turns ending at once leave them.
    const tied = [await thread(here!), await thread(here!)];
    await turn(tied[1]!, "a tie, made after", here!);
    await turn(tied[0]!, "a tie, made before", here!);
    await Effect.runPromise(rollouts.flush(tied[1]!));
    await Effect.runPromise(rollouts.flush(tied[0]!));
    await tie(tied);
    await apart();
    // One archived, as the operator archives a thread, which Codex lists no more.
    const archived = await thread(here!);
    await turn(archived, "a thread archived", here!);
    await run(client.stop);
    await archive(archived, here!);
    await apart();
    // The first resumed by an app-server started anew, as a pod or an alasio started again resumes it.
    client = await appServer();
    await run(client.ensureThread({ threadId: first, threadKey: "c1", cwd: here!, env: buildCodexEnv(), config: NO_CONFIG }));
    await turn(first, "and then the stork", here!);
    for (const threadId of [first, other, fork, late, ...tied, archived]) await Effect.runPromise(rollouts.flush(threadId!));
    // The other folder's thread's file compressed once kept, as Codex's compression of old rollouts leaves it: kept as it was before.
    for (const file of listRolloutFiles(home).filter(({ name }) => name.includes(other))) {
      writeFileSync(join(home, `${file.path}.zst`), zstdCompressSync(readFileSync(join(home, file.path))));
      rmSync(join(home, file.path));
    }

    const codexOf = (cwd: string): SessionListingScope => ({ cwd, codexEnv: buildCodexEnv(), appServer: client });
    const neonOf = (cwd: string): SessionListingScope => ({ cwd, codexEnv: buildCodexEnv(), appServer: rolloutListing(store) });
    const noFork = () => Effect.sync(() => assert.fail("nothing is forked"));
    for (const cwd of [here!, elsewhere!]) {
      const codex = await panelsOf(createCodexSessionApi({ listingScope: Effect.succeed(codexOf(cwd)), fork: noFork }));
      const neon = await panelsOf(createCodexSessionApi({ listingScope: Effect.succeed(neonOf(cwd)), fork: noFork }));
      assert.deepEqual(neon, codex, cwd);
    }
    // What was compared holds the cases: the fork listed with its own turn and the first's before the fork, the first with its resumed turn.
    const listed = (await panelsOf(createCodexSessionApi({ listingScope: Effect.succeed(neonOf(here!)), fork: noFork }))) as { readonly threads: readonly { readonly uuid: string; readonly messages: readonly { readonly text: string }[] }[] };
    assert.deepEqual(new Set(listed.threads.map(({ uuid }) => uuid)), new Set([first, fork, late, ...tied]), "the archived thread is listed, or another is not");
    const messagesOf = (threadId: string) => listed.threads.find(({ uuid }) => uuid === threadId)?.messages.map(({ text }) => text);
    assert.deepEqual([messagesOf(first), messagesOf(fork), messagesOf(late)], [
      ["and then the stork", "and now the crane", "remember the heron"],
      ["which bird?", "remember the heron"],
      ["a prompt past its first chunk"],
    ]);
    // Of the two that changed within one second, the one that changed last first, as Codex orders them.
    const order = listed.threads.map(({ uuid }) => uuid);
    assert.ok(order.indexOf(tied[0]!) < order.indexOf(tied[1]!), "the tied threads in the order they were made");
    // The compressed one listed still, of its folder.
    const elsewhereListed = (await panelsOf(createCodexSessionApi({ listingScope: Effect.succeed(neonOf(elsewhere!)), fork: noFork }))) as { readonly threads: readonly { readonly uuid: string }[] };
    assert.deepEqual(elsewhereListed.threads.map(({ uuid }) => uuid), [other]);
    // The late thread's file kept in more chunks than one, its first holding no prompt.
    assert.ok((await pool.query("select count(*)::int as n from codex_listing.rollout_chunks c join codex_listing.rollouts r using (name) where r.thread_id = $1", [late])).rows[0]?.n > 1);
  } finally {
    for (const scope of scopes.reverse()) await Effect.runPromise(Scope.close(scope, Exit.void));
    await pool.end();
    await database.stop();
    await responses.close();
    if (savedCodexHome === undefined) delete process.env["CODEX_HOME"];
    else process.env["CODEX_HOME"] = savedCodexHome;
    rmSync(root, { recursive: true, force: true });
  }
});
