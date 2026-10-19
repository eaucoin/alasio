/**
 * `alasio harness` (src/harness/program.ts, src/harness/serve.ts): one conversation's
 * harness host, run apart from alasio as the harness host's role. As it binds, it writes
 * back the Codex rollouts the conversation's session needs from Neon; then it serves the
 * host over HTTP, as src/harness/wire.ts says, to the bearer of its token alone. A test
 * file of its own, as the role is the server's.
 */
import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { ConfigProvider, Context, Deferred, Effect, Exit, Layer, Scope } from "effect";
import pg from "pg";

import { HARNESS_ROLE } from "../neon/control/secrets.ts";
import { scramVerifier } from "../neon/control/scram.ts";
import { makeCodexRollouts } from "../src/codex/rollouts/index.ts";
import { type Harness, type TurnResult } from "../src/harness/index.ts";
import { CODEX_HARNESS, harnessDisplayName } from "../src/harness/names.ts";
import { startHarness } from "../src/harness/program.ts";
import { HOST_PATHS } from "../src/harness/wire.ts";
import { Neon } from "../src/neon/connect.ts";
import { LakeWake } from "../src/neon/lake-wake.ts";
import { Store, TurnFenced } from "../src/persistence/store.ts";
import { startPostgres } from "./support/postgres.ts";

/** A thread of Codex's, and where its rollout file is under a Codex home. */
const THREAD = "01a0cadd-b753-7d42-84a0-15a98e372686";
const ROLLOUT = join("sessions/2026/09/28", `rollout-2026-09-28T18-47-10-${THREAD}.jsonl`);
const ROLLOUT_TEXT = `${JSON.stringify({ type: "session_meta", payload: { id: THREAD } })}\n{"type":"turn"}\n`;

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

/** A turn's harness held midway: told it got there, and let go on. */
interface Held {
  readonly reached: Deferred.Deferred<void>;
  readonly release: Deferred.Deferred<void>;
}

/**
 * A Codex harness whose turn writes its response, held midway where `holding` says, and
 * tells of what it does on its own after it, as a harness that runs on between prompts.
 */
function answeringHarness(holding: () => Held | null): Harness {
  const unused = (name: string) => () => assert.fail(`the answering harness has no ${name}`);
  return {
    name: CODEX_HARNESS,
    displayName: harnessDisplayName(CODEX_HARNESS),
    supportsGoals: false,
    supportsWarmup: false,
    sessions: {
      listSessions: unused("listSessions"),
      getTotalSessionPages: unused("getTotalSessionPages"),
      getSessionByNumber: unused("getSessionByNumber"),
      getSessionLastMessage: unused("getSessionLastMessage"),
      listSessionMessages: unused("listSessionMessages"),
      getTotalRewindPages: unused("getTotalRewindPages"),
      createForkedSession: unused("createForkedSession"),
    },
    startFreshSession: () => Effect.succeed("thread-fresh"),
    warmSession: () => Effect.succeed(false),
    runTurn: (params) =>
      Effect.gen(function*() {
        const pendingResponseId = yield* params.persistence.createPendingResponse(params.chatId, params.messageId, THREAD);
        const held = holding();
        if (held) yield* Deferred.succeed(held.reached, undefined).pipe(Effect.andThen(Deferred.await(held.release)));
        yield* params.persistence.appendBlocksToPending(pendingResponseId, [{ type: "text", content: `Answered ${params.prompt}.` }]);
        yield* params.persistence.markPendingResponseComplete(pendingResponseId);
        yield* params.onBackgroundResponse ?? Effect.void;
        yield* params.onIdle ?? Effect.void;
        const result: TurnResult = { blockSequence: [{ type: "text", content: `Answered ${params.prompt}.` }], sessionId: THREAD, pendingResponseId, interrupted: false, responseCompleted: true };
        return result;
      }),
    listModels: unused("listModels"),
    defaultModelChoice: unused("defaultModelChoice"),
  };
}

/** The lines an answer that lasts has given so far, read as they come. */
function linesOf(response: Response): { readonly next: () => Promise<unknown> } {
  assert.ok(response.body);
  const reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
  let buffered = "";
  return {
    next: async () => {
      for (;;) {
        const end = buffered.indexOf("\n");
        if (end >= 0) {
          const line = buffered.slice(0, end);
          buffered = buffered.slice(end + 1);
          const said: unknown = JSON.parse(line);
          // What says only that the answer lasts is skipped, as the host's callers skip it.
          if (JSON.stringify(said) === JSON.stringify({ alive: true })) continue;
          return said;
        }
        const { value, done } = await reader.read();
        if (done) throw new Error("the answer ended");
        buffered += value;
      }
    },
  };
}

test("alasio harness binds its conversation, writing back the rollouts its Codex session needs, and serves its host to the bearer of its token", { timeout: 120_000 }, async () => {
  const database = await startPostgres();
  /** `database`'s server, as `user`, in alasio's database. */
  const as = (user: string, password: string) => {
    const url = new URL(database.url);
    Object.assign(url, { username: user, password, pathname: "/alasio" });
    return url.toString();
  };
  const dir = mkdtempSync(join(tmpdir(), "alasio-harness-program-"));
  const [earlier, home, stateDir, folder] = ["earlier", "home", "state", "folder"].map((name) => join(dir, name));
  for (const made of [earlier, home, stateDir, folder]) mkdirSync(made!, { recursive: true });
  process.env["CODEX_HOME"] = home;
  const neonEnv = (url: string) => ConfigProvider.layer(ConfigProvider.fromEnv({ env: { ALASIO_DATABASE_URL: url, ALASIO_LAKE_VERIFIER: scramVerifier("lake-password"), ALASIO_HARNESS_VERIFIER: scramVerifier("harness-password") } }));
  const scope = Effect.runSync(Scope.make());
  /** `effect`, in the test's scope, which closes as the test ends. */
  const inScope = <A, E>(effect: Effect.Effect<A, E, Scope.Scope>) => Effect.runPromise(Scope.provide(effect, scope));
  try {
    await run(database.url, "create role alasio login password 'alasio-password' createrole createdb", "create database alasio owner alasio");
    // alasio: its state made, the conversation mounted on a folder with Codex, its session one an earlier instance's home kept the rollout of.
    const { alasio, conversationId } = await inScope(Effect.gen(function*() {
      const neon = yield* Effect.provide(Neon.make, neonEnv(as("alasio", "alasio-password")));
      const alasio = Context.get(yield* Layer.build(Store.layer({ pool: neon.pool, harnessRole: HARNESS_ROLE })), Store);
      const conversationId = yield* alasio.upsertConversation({ chatId: "41", user: { id: 41 } });
      yield* alasio.setActiveHarness(conversationId, CODEX_HARNESS);
      yield* alasio.setWorkingDirectory(conversationId, folder!);
      yield* alasio.setSessionId(conversationId, THREAD);
      mkdirSync(join(earlier!, "sessions/2026/09/28"), { recursive: true });
      writeFileSync(join(earlier!, ROLLOUT), ROLLOUT_TEXT);
      yield* Effect.scoped(Effect.flatMap(makeCodexRollouts({ store: neon.rollouts, home: earlier! }), (kept) => kept.flush(THREAD)));
      return { alasio, conversationId };
    }).pipe(Effect.provideService(LakeWake, LakeWake.none)));

    let held: Held | null = null;
    const { port } = await inScope(startHarness(
      { conversationId, port: 0, token: "harness-token", stateDir: stateDir!, branch: null, lakeWake: null },
      { overrides: { [CODEX_HARNESS]: answeringHarness(() => held) } },
    ).pipe(Effect.provide(neonEnv(as(HARNESS_ROLE, "harness-password")))));
    // Bound: the conversation's session's rollout written back, whole, where Codex reads it.
    assert.equal(readFileSync(join(home!, ROLLOUT), "utf8"), ROLLOUT_TEXT);

    const call = (path: string, body: object | null, { token = "harness-token", signal }: { readonly token?: string; readonly signal?: AbortSignal } = {}) =>
      fetch(`http://127.0.0.1:${port}${path}`, { method: body === null ? "GET" : "POST", headers: { authorization: `Bearer ${token}`, "content-type": "application/json" }, ...(body === null ? {} : { body: JSON.stringify(body) }), ...(signal ? { signal } : {}) });
    const answered = async (path: string, body: object, options?: Parameters<typeof call>[2]) => {
      const response = await call(path, body, options);
      return { status: response.status, body: await response.json() as Readonly<Record<string, unknown>> };
    };

    // Its token's bearer alone, of its conversation alone.
    assert.equal((await answered(HOST_PATHS.status, { conversationId }, { token: "forged" })).status, 401);
    assert.deepEqual(await answered(HOST_PATHS.status, { conversationId: "telegram:42" }), { status: 404, body: { error: { _tag: "OtherConversation", message: `this host serves ${conversationId}, not telegram:42` } } });
    assert.deepEqual([(await answered(HOST_PATHS.status, { conversationId })).body["busy"], (await answered(HOST_PATHS.status, { conversationId })).body["held"]], [false, false]);

    // Held by one holder at a time, until it releases the hold: let go of by the time it is answered.
    const reservation = linesOf(await call(HOST_PATHS.reserve, { conversationId }));
    const hold = await reservation.next() as { readonly held: boolean; readonly id: string };
    assert.equal(hold.held, true);
    assert.equal((await answered(HOST_PATHS.status, { conversationId })).body["busy"], true);
    assert.deepEqual(await linesOf(await call(HOST_PATHS.reserve, { conversationId })).next(), { held: false, id: null });
    assert.deepEqual((await answered(HOST_PATHS.release, { conversationId, id: hold.id })).body, { released: true });
    assert.equal((await answered(HOST_PATHS.status, { conversationId })).body["busy"], false);
    assert.deepEqual((await answered(HOST_PATHS.release, { conversationId, id: hold.id })).body, { released: false });
    // Or until its holder lets go of the connection, as a holder that is gone does.
    const holding = new AbortController();
    assert.equal((await linesOf(await call(HOST_PATHS.reserve, { conversationId }, { signal: holding.signal })).next() as { readonly held: boolean }).held, true);
    holding.abort();
    for (let waited = 0; (await answered(HOST_PATHS.status, { conversationId })).body["busy"]; waited += 20) {
      assert.ok(waited < 5_000, "the reservation outlasted its holder's connection");
      await new Promise((resolve) => setTimeout(resolve, 20));
    }

    // A turn, claimed by alasio, run to its end; what its harness does on its own after it told as the host's events.
    // Listened to until the host stops, as alasio listens, which its stop ends rather than waits on.
    const events = linesOf(await call(HOST_PATHS.events, null));
    assert.deepEqual(await events.next(), { listening: true });
    const start = (prompt: string, epoch: number) => ({
      conversationId,
      chatId: "41",
      messageId: prompt,
      harness: CODEX_HARNESS,
      workingDirectory: folder!,
      prompt,
      fileIds: [],
      resumeSession: THREAD,
      modelChoice: null,
      epoch,
      jobId: null,
      attachedTurn: null,
    });
    const claim = (messageId: string) => Effect.runPromise(alasio.claimTurn({ conversationId, chatId: "41", messageId, sessionId: THREAD, harness: CODEX_HARNESS, prompt: messageId }));
    const first = await claim("first");
    const turn = await linesOf(await call(HOST_PATHS.turns, start("first", first))).next() as { readonly answer: Readonly<Record<string, unknown>> };
    assert.deepEqual([turn.answer["blockSequence"], turn.answer["sessionId"], turn.answer["responseCompleted"]], [[{ type: "text", content: "Answered first." }], THREAD, true]);
    assert.deepEqual([await events.next(), await events.next()], [{ kind: "background-response", conversationId }, { kind: "idle", conversationId }]);
    // One of a claim since overtaken fails as the store says, whole.
    const second = await claim("second");
    const fenced = new TurnFenced({ threadKey: conversationId, epoch: first });
    assert.deepEqual(await linesOf(await call(HOST_PATHS.turns, start("late", first))).next(), { error: { _tag: "TurnFenced", message: fenced.message, threadKey: conversationId, epoch: first } });

    // A turn its caller lets go of, as alasio does when it stops, goes on to its end.
    const holdingTurn: Held = { reached: Effect.runSync(Deferred.make<void>()), release: Effect.runSync(Deferred.make<void>()) };
    held = holdingTurn;
    const leaving = new AbortController();
    const left = (await call(HOST_PATHS.turns, start("second", second), { signal: leaving.signal })).text().catch((error: unknown) => error);
    await Effect.runPromise(Deferred.await(holdingTurn.reached));
    leaving.abort();
    assert.equal((await left as Error).name, "AbortError");
    // A call after it, once the host has seen its caller go: the turn is let go on only then.
    await answered(HOST_PATHS.status, { conversationId });
    Effect.runSync(Deferred.succeed(holdingTurn.release, undefined));
    for (let waited = 0; ; waited += 20) {
      const completed = await Effect.runPromise(alasio.getCompletedResponsesPendingDelivery);
      if (completed.some(({ blocks }) => JSON.stringify(blocks) === JSON.stringify([{ type: "text", content: "Answered second." }]))) break;
      assert.ok(waited < 5_000, "the turn its caller let go of did not go on to its end");
      await new Promise((resolve) => setTimeout(resolve, 20));
    }

    // No turn runs to steer or stop now; a fresh session, of the conversation's harness.
    assert.deepEqual((await answered(HOST_PATHS.steer, { conversationId, prompt: "also", fileIds: [] })).body, { taken: null });
    assert.deepEqual((await answered(HOST_PATHS.interrupt, { conversationId, reason: "interrupt" })).body, { interrupted: false });
    assert.deepEqual((await answered(HOST_PATHS.freshSession, { conversationId, harness: CODEX_HARNESS, workingDirectory: folder })).body, { sessionId: "thread-fresh" });
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
    await database.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});
