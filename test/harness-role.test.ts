/**
 * The harness host's role (src/neon/harness.ts, src/persistence/harness-grants.ts), as
 * alasio makes it in its Neon: a turn run through the host on a store of that role writes
 * all a turn writes, and the role may do nothing of the control plane's. A test file of
 * its own, as roles are a server's: here alasio makes its role as Neon's alasio is made, a
 * role that may make roles and databases, and is no superuser.
 */
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { ConfigProvider, Context, Effect, Layer } from "effect";
import pg from "pg";

import { HARNESS_ROLE } from "../neon/control/secrets.ts";
import { scramVerifier } from "../neon/control/scram.ts";
import { CodexAppServer } from "../src/codex/app-server/client.ts";
import { ActiveTurns } from "../src/harness/active-turns.ts";
import { HarnessHost } from "../src/harness/host.ts";
import { type Harness, Harnesses, type OwnTurns, type TurnResult } from "../src/harness/index.ts";
import { grantDigest, grantKey, harnessGrant } from "../src/harness/grant.ts";
import { CODEX_HARNESS, harnessDisplayName } from "../src/harness/names.ts";
import { Neon } from "../src/neon/connect.ts";
import { GrantedPool } from "../src/neon/granted-pool.ts";
import { LakeWake } from "../src/neon/lake-wake.ts";
import { Store } from "../src/persistence/store.ts";
import { ReceivedFiles } from "../src/telegram/files.ts";
import { startPostgres } from "./support/postgres.ts";
import { inProcessEvents } from "./support/host-events.ts";

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

/** A Codex harness whose turn writes everything a turn writes through its persistence, and hands on how it claims a turn of its own. */
function writingHarness(own: OwnTurns[]): Harness {
  const unused = (name: string) => () => assert.fail(`the writing harness has no ${name}`);
  return {
    name: CODEX_HARNESS,
    displayName: harnessDisplayName(CODEX_HARNESS),
    supportsGoals: false,
    sessions: {
      listSessions: unused("listSessions"),
      getTotalSessionPages: unused("getTotalSessionPages"),
      getSessionByNumber: unused("getSessionByNumber"),
      getSessionLastMessage: unused("getSessionLastMessage"),
      listSessionMessages: unused("listSessionMessages"),
      getTotalRewindPages: unused("getTotalRewindPages"),
      createForkedSession: unused("createForkedSession"),
    },
    startFreshSession: unused("startFreshSession"),
    runTurn: (params) =>
      Effect.gen(function*() {
        const turn = params.persistence;
        own.push(params.ownTurns);
        yield* params.onPromptDispatched ?? Effect.void;
        yield* params.onTransportStarted?.({ sessionId: "thread-1", turnId: "turn-1" }) ?? Effect.void;
        const pendingResponseId = yield* turn.createPendingResponse(params.chatId, params.messageId, null);
        yield* turn.updateActiveTurnPendingResponseId(pendingResponseId);
        yield* turn.updatePendingSessionId(pendingResponseId, "thread-1");
        yield* turn.updateActiveTurnSessionId("thread-1");
        yield* turn.appendBlocksToPending(pendingResponseId, [{ type: "text", content: "Fixed." }]);
        yield* turn.updateSessionUsage("thread-1", { cacheReadInputTokens: 12 });
        yield* turn.setWaiting("workflow 7");
        yield* turn.setWaiting(null);
        yield* turn.recordRestartEvent({ cause: "self_induced", channel: params.chatId, thread_ts: params.messageId, session_id: "thread-1", command: "kubectl rollout restart deployment/alasio" });
        yield* turn.markPendingResponseComplete(pendingResponseId);
        yield* params.onTransportCompleted?.({ sessionId: "thread-1", turnId: "turn-1" }) ?? Effect.void;
        const result: TurnResult = { blockSequence: [], sessionId: "thread-1", pendingResponseId, interrupted: false, responseCompleted: true };
        return result;
      }).pipe(Effect.orDie),
    listModels: unused("listModels"),
    defaultModelChoice: unused("defaultModelChoice"),
  };
}

test("a turn run through the harness host as its own role writes all a turn writes, and the role may do nothing of the control plane's", { timeout: 60_000 }, async () => {
  const database = await startPostgres();
  /** `database`'s server, as `user`, in alasio's database. */
  const as = (user: string, password: string) => {
    const url = new URL(database.url);
    Object.assign(url, { username: user, password, pathname: "/alasio" });
    return url.toString();
  };
  // As a pool's pod reaches it: by the grant of its conversation's binding, which alasio keeps.
  const harnessPool = new GrantedPool({ connectionString: as(HARNESS_ROLE, "harness-password") });
  // Where the host writes the files a prompt names, as its harness reads them.
  const stateDir = mkdtempSync(join(tmpdir(), "alasio-harness-role-"));
  try {
    await run(database.url, "create role alasio login password 'alasio-password' createrole createdb", "create database alasio owner alasio");
    const env = { ALASIO_DATABASE_URL: as("alasio", "alasio-password"), ALASIO_LAKE_VERIFIER: scramVerifier("lake-password"), ALASIO_HARNESS_VERIFIER: scramVerifier("harness-password") };
    const own: OwnTurns[] = [];
    await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
      const neon = yield* Neon.make;
      // alasio's store, which makes the state and grants the role its part.
      const alasio = Context.get(yield* Layer.build(Store.layer({ pool: neon.pool, harnessRole: HARNESS_ROLE })), Store);
      const conversationId = yield* alasio.upsertConversation({ chatId: "41", user: { id: 41 } });
      const grant = harnessGrant(grantKey(Buffer.from("the environment's key")), conversationId, "binding-1");
      yield* alasio.issueHarnessGrant(conversationId, "binding-1", grantDigest(grant));
      harnessPool.grant(grant);
      yield* alasio.setActiveHarness(conversationId, CODEX_HARNESS);
      yield* alasio.setWorkingDirectory(conversationId, "/work");
      const messageId = yield* alasio.insertMessage({ conversationId, direction: "in", kind: "file", transportMessageId: 10, text: "go" });
      const content = Buffer.from("a report");
      const fileId = yield* alasio.insertFile({
        conversationId,
        messageId,
        file: { kind: "document", file_id: "f", file_unique_id: "u", file_name: "report.txt" },
        name: "report.txt",
        content,
        sha256: createHash("sha256").update(content).digest("hex"),
      });
      const job = yield* alasio.enqueuePromptJob({ conversationId, chatId: "41", messageId: "10", prompt: "go" });
      yield* alasio.claimNextPromptJob(conversationId);
      const epoch = yield* alasio.claimTurn({ conversationId, chatId: "41", messageId: "10", sessionId: null, harness: CODEX_HARNESS, prompt: "go" });

      // The host, on a store of the harness's role, of the state alasio made: it makes none of it.
      const services = yield* Layer.build(HarnessHost.layer.pipe(
        Layer.provideMerge(Harnesses.layer({ overrides: { [CODEX_HARNESS]: writingHarness(own) } })),
        Layer.provideMerge(Layer.mergeAll(inProcessEvents, ActiveTurns.layer, CodexAppServer.layer, ReceivedFiles.layer({ stateDir }))),
        Layer.provideMerge(Store.layer({ pool: harnessPool, makesSchema: false })),
      ));
      const harness = { host: Context.get(services, HarnessHost), store: Context.get(services, Store) };
      const result = yield* harness.host.startTurn({
        conversationId,
        chatId: "41",
        messageId: "10",
        harness: CODEX_HARNESS,
        workingDirectory: "/work",
        prompt: "go",
        fileIds: [fileId],
        resumeSession: null,
        modelChoice: null,
        epoch,
        jobId: job.id,
        attachedTurn: null,
      });
      assert.deepEqual((yield* alasio.getCompletedResponsesPendingDelivery).map(({ id, blocks }) => ({ id, blocks })), [{ id: result.pendingResponseId, blocks: [{ type: "text", content: "Fixed." }] }]);
      assert.equal((yield* alasio.getConversation(conversationId))?.codex_session_id, "thread-1");
      const progress = yield* alasio.getPromptJob(job.id);
      assert.ok(progress?.upstream_dispatched_at && progress.upstream_started_at && progress.upstream_completed_at);
      assert.equal((yield* alasio.getRestartEvent(conversationId))?.cause, "self_induced");
      // A turn of its harness's own after it, claimed and ended by the host.
      yield* alasio.endTurn(conversationId, epoch, "completed");
      const [ownTurns] = own;
      assert.ok(ownTurns);
      const background = yield* ownTurns.claim("11", "thread-1");
      yield* background.persistence.createPendingResponse("41", "11", "thread-1");
      yield* background.end("completed");
      assert.equal((yield* alasio.getActiveTurn(conversationId)), null);
      // What it says of the conversation it reads as the role too: its mount, and the holds that keep it busy.
      const holder = yield* alasio.holdConversations([conversationId], "forking session fs-a into fs-b", 60_000);
      assert.ok(holder);
      assert.deepEqual([(yield* harness.host.status(conversationId)).busy, (yield* harness.host.status(conversationId)).held], [true, true]);
      yield* alasio.releaseHolds(holder);
      // The file the prompt named, written where its harness reads it.
      assert.equal(readFileSync(join(stateDir, "telegram-files", fileId, "report.txt"), "utf8"), "a report");
    }).pipe(
      Effect.provideService(LakeWake, LakeWake.none),
      Effect.provide(ConfigProvider.layer(ConfigProvider.fromEnv({ env }))),
    )));

    // Nothing of the control plane's: Telegram's updates, the buttons, the outbox, what a
    // conversation mounts, the queue, the Codex login, making holds, and the schema itself.
    for (
      const statement of [
        "select * from state.telegram_updates",
        "select * from state.callback_actions",
        "insert into state.telegram_outbox (id, chat_id, kind, text, options) values ('forged', '41', 'text', 'forged', '{}')",
        "update state.conversations set working_directory = '/elsewhere'",
        "delete from state.conversations",
        "update state.prompt_jobs set state = 'pending'",
        "delete from state.turns",
        "select * from state.session_workspaces",
        "select * from state.codex_login",
        "insert into state.conversation_holds (conversation_id, reason) values ('telegram:41', 'held')",
        "create table state.forged (id int)",
      ]
    ) {
      await assert.rejects(harnessPool.query(statement), /permission denied/u, statement);
    }
    const { rows: roles } = await harnessPool.query("select r.rolname from pg_auth_members m join pg_roles r on r.oid = m.roleid where m.member = (select oid from pg_roles where rolname = $1)", [HARNESS_ROLE]);
    assert.deepEqual(roles, [], "a member of no other role");
  } finally {
    await harnessPool.end();
    await database.stop();
    rmSync(stateDir, { recursive: true, force: true });
  }
});
