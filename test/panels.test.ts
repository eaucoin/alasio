/**
 * What the operator's panels reach of a conversation's harness, alasio running none
 * (src/harness/panels.ts): the sessions of the conversation's workspace alone, read from where
 * Neon keeps them, as the panels showed them when harnesses ran in alasio's process; and a
 * rewind's fork, and a thread's goals, of the conversation's own pod.
 */
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, test } from "node:test";

import { Effect, Layer, Logger, ManagedRuntime, Option } from "effect";
import pg from "pg";

import { CodexAppServer } from "../src/codex/app-server/client.ts";
import { NeonRolloutStore, SESSION_FS_SCHEMA } from "../src/codex/rollouts/store.ts";
import { NeonSessionStore } from "../src/harness/claude/session-store.ts";
import { type ConversationHarness, HarnessHost, type HarnessPanels } from "../src/harness/host.ts";
import type { HarnessGoals } from "../src/harness/index.ts";
import { CLAUDE_HARNESS, CODEX_HARNESS, type HarnessName } from "../src/harness/names.ts";
import { Panels, sessionFsDirectory } from "../src/harness/panels.ts";
import { sessionFsWorkspace } from "../src/workspace/kind.ts";
import { startPostgres, type TestPostgres } from "./support/postgres.ts";

const STATE_DIR = "/state";
const [HERE, THERE] = ["/work/here", "/work/there"];
const [VOLUME, OTHER_VOLUME] = ["fs-0123456789abcdef", "fs-fedcba9876543210"];

// Set by the first hook.
let database: TestPostgres | undefined;
let pool: pg.Pool;
let claudeHome: string;
let savedClaudeHome: string | undefined;

before(async () => {
  database = await startPostgres();
  pool = new pg.Pool({ connectionString: database.url, max: 4 });
  // The SDK's session helpers look for local transcripts here first: none, so the store is read.
  savedClaudeHome = process.env["CLAUDE_CONFIG_DIR"];
  claudeHome = mkdtempSync(join(tmpdir(), "alasio-panels-claude-"));
  process.env["CLAUDE_CONFIG_DIR"] = claudeHome;
});

after(async () => {
  if (savedClaudeHome === undefined) delete process.env["CLAUDE_CONFIG_DIR"];
  else process.env["CLAUDE_CONFIG_DIR"] = savedClaudeHome;
  rmSync(claudeHome, { recursive: true, force: true });
  await pool?.end();
  await database?.stop();
});

/** A Codex thread's rollout as Codex writes one: begun in `cwd`, its one turn the prompt `prompt`. */
async function keepThread(store: NeonRolloutStore, { id, cwd, prompt }: { readonly id: string; readonly cwd: string; readonly prompt: string }): Promise<void> {
  const lines = [
    { timestamp: "2026-10-21T00:00:00.000Z", ordinal: 0, type: "session_meta", payload: { id, cwd, source: "cli" } },
    { timestamp: "2026-10-21T00:00:01.000Z", ordinal: 1, type: "event_msg", payload: { type: "task_started", turn_id: `${id}-turn`, started_at: 1 } },
    { timestamp: "2026-10-21T00:00:01.000Z", ordinal: 2, type: "event_msg", payload: { type: "item_completed", turn_id: `${id}-turn`, item: { type: "UserMessage", id: `${id}-prompt`, content: [{ type: "text", text: prompt }] } } },
  ];
  const bytes = Buffer.from(lines.map((line) => JSON.stringify(line)).join("\n") + "\n");
  const name = `rollout-2026-10-21T00-00-00-${id}.jsonl`;
  await store.save(
    { name, path: `sessions/2026/10/21/${name}`, threadId: id, rolloutId: id, historyBase: null, size: bytes.length, headDigest: id, modifiedMs: Date.parse("2026-10-21T00:00:02.000Z") },
    { start: 0, bytes },
  );
}

/** A Claude Code session's transcript as Claude Code keeps it of `dir`'s project: its one prompt `prompt`. */
async function keepTranscript(store: NeonSessionStore, { sessionId, dir, prompt }: { readonly sessionId: string; readonly dir: string; readonly prompt: string }): Promise<void> {
  const base = { sessionId, cwd: dir, version: "2.1.280", isSidechain: false, userType: "external" };
  await store.append({ projectKey: dir.replace(/[^a-zA-Z0-9]/gu, "-"), sessionId }, [
    { ...base, type: "user", uuid: `${sessionId.slice(0, -12)}111111111111`, parentUuid: null, timestamp: "2026-10-21T00:00:00.000Z", message: { role: "user", content: prompt } },
  ]);
}

/** What the panels asked of the conversation's host: each fork, and each goal read. */
interface Asked {
  readonly forks: unknown[];
  readonly goals: unknown[];
}

/** The host the panels reach, which they ask only for forks and goals: what they asked recorded in `asked`. */
function recordingHost(asked: Asked): HarnessHost["Service"] {
  const goals = (of: ConversationHarness): HarnessGoals => ({
    read: ({ threadId }) => Effect.sync(() => (asked.goals.push({ ...of, threadId }), null)),
    set: () => Effect.die(new Error("the panels set no goal here")),
    clear: () => Effect.die(new Error("the panels clear no goal here")),
    waitForTurnId: () => Effect.die(new Error("the panels wait on no goal here")),
  });
  const used: Pick<HarnessHost["Service"], "forkSession" | "goals"> = {
    forkSession: (params) => Effect.sync(() => (asked.forks.push(params), "forked-thread")),
    goals,
  };
  return new Proxy(used, {
    get: (target, key) => (key in target ? target[key as keyof typeof target] : assert.fail(`the panels asked the host for ${String(key)}`)),
  }) as HarnessHost["Service"];
}

/** The panels over a store of each kind, seeded with threads and transcripts of more workspaces than one, and what they asked of the host. */
async function seededPanels(schema: string) {
  const codexRollouts = new NeonRolloutStore(pool, { schema: `${schema}_codex` });
  const sessionFsCodexRollouts = new NeonRolloutStore(pool, { schema: `${schema}_${SESSION_FS_SCHEMA}` });
  const sessionStore = new NeonSessionStore(pool, { schema: `${schema}_claude` });
  for (const store of [codexRollouts, sessionFsCodexRollouts, sessionStore]) await store.ensureSchema();
  await keepThread(codexRollouts, { id: "11111111-0000-4000-8000-000000000001", cwd: HERE, prompt: "here" });
  await keepThread(codexRollouts, { id: "11111111-0000-4000-8000-000000000002", cwd: THERE, prompt: "there" });
  // Of the session filesystems' store, one of them begun in the folder's directory too: the folder's panels read the folder's store alone.
  await keepThread(sessionFsCodexRollouts, { id: "11111111-0000-4000-8000-000000000003", cwd: sessionFsDirectory(STATE_DIR, VOLUME), prompt: "in the volume" });
  await keepThread(sessionFsCodexRollouts, { id: "11111111-0000-4000-8000-000000000004", cwd: sessionFsDirectory(STATE_DIR, OTHER_VOLUME), prompt: "in another volume" });
  await keepThread(sessionFsCodexRollouts, { id: "11111111-0000-4000-8000-000000000005", cwd: HERE, prompt: "in the volumes' store" });
  await keepTranscript(sessionStore, { sessionId: "22222222-0000-4000-8000-000000000001", dir: HERE, prompt: "here" });
  await keepTranscript(sessionStore, { sessionId: "22222222-0000-4000-8000-000000000002", dir: THERE, prompt: "there" });
  const asked: Asked = { forks: [], goals: [] };
  const panels = ManagedRuntime.make(
    Panels.layer({ sessionStore, codexRollouts, sessionFsCodexRollouts, stateDir: STATE_DIR }).pipe(
      Layer.provide(Layer.mergeAll(Layer.succeed(HarnessHost, recordingHost(asked)), CodexAppServer.layer)),
      Layer.provideMerge(Logger.layer([])),
    ),
  );
  /** The panels of conversation `conversationId`'s harness `harness` in `workingDirectory`. */
  const of = (conversationId: string, harness: HarnessName, workingDirectory: string): Promise<HarnessPanels> =>
    panels.runPromise(Effect.flatMap(Panels, (each) => each.forMount(conversationId, { harness, workingDirectory, sessionId: null })).pipe(Effect.map(Option.getOrThrow)));
  return { panels, of, asked };
}

const listed = (panels: HarnessPanels): Promise<string[]> => Effect.runPromise(Effect.map(panels.sessions.listSessions(1), (sessions) => sessions.map(({ uuid }) => uuid)));

test("a conversation's panels list the sessions of its own workspace alone, of the store that keeps that workspace's", async () => {
  const { panels, of } = await seededPanels("scoped");
  try {
    assert.deepEqual(await listed(await of("conversation-1", CODEX_HARNESS, HERE)), ["11111111-0000-4000-8000-000000000001"]);
    assert.deepEqual(await listed(await of("conversation-2", CODEX_HARNESS, THERE)), ["11111111-0000-4000-8000-000000000002"]);
    assert.deepEqual(await listed(await of("conversation-3", CODEX_HARNESS, sessionFsWorkspace(VOLUME))), ["11111111-0000-4000-8000-000000000003"]);
    assert.deepEqual(await listed(await of("conversation-1", CLAUDE_HARNESS, HERE)), ["22222222-0000-4000-8000-000000000001"]);
    assert.deepEqual(await listed(await of("conversation-2", CLAUDE_HARNESS, THERE)), ["22222222-0000-4000-8000-000000000002"]);
  } finally {
    await panels.dispose();
  }
});

test("a rewind's fork of a Codex thread, and the thread's goals, are asked of the conversation's own host", async () => {
  const { panels, of, asked } = await seededPanels("hosted");
  try {
    const codex = await of("conversation-1", CODEX_HARNESS, HERE);
    const forked = await Effect.runPromise(codex.sessions.createForkedSession("11111111-0000-4000-8000-000000000001", "11111111-0000-4000-8000-000000000001-turn", { threadKey: "conversation-1" }));
    await Effect.runPromise(codex.goals!.read({ threadId: "11111111-0000-4000-8000-000000000001" }));
    assert.equal(forked, "forked-thread");
    const conversation = { conversationId: "conversation-1", harness: CODEX_HARNESS, workingDirectory: HERE };
    assert.deepEqual(asked, {
      forks: [{ ...conversation, sessionId: "11111111-0000-4000-8000-000000000001", beforeUuid: "11111111-0000-4000-8000-000000000001-turn" }],
      goals: [{ ...conversation, threadId: "11111111-0000-4000-8000-000000000001" }],
    });
  } finally {
    await panels.dispose();
  }
});
