/**
 * Claude Code's local transcripts, kept whole against the Neon store.
 *
 * Claude Code works from JSONL files under its home
 * (`<CLAUDE_CONFIG_DIR>/projects/<projectKey>/<sessionId>.jsonl`, subagents
 * under `<sessionId>/subagents/`); the SDK mirrors every write to the store,
 * which is the durable copy. Two things keep the two in step:
 *
 * - `ensureLocalTranscript` writes a transcript back from the store before a
 *   resume, if the local file is gone.
 * - `adoptTranscripts` runs at startup: a session the store has never seen is
 *   imported whole; one it has is reconciled, adding any entry the
 *   best-effort mirror dropped or never saw, matched so nothing is duplicated.
 */
import { importSessionToStore, type SessionKey, type SessionStoreEntry } from "@anthropic-ai/claude-agent-sdk";
import { existsSync, mkdirSync, readdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import { Effect, Schema } from "effect";

import { withLogScope } from "../../shared/log.ts";
import type { NeonSessionStore } from "./session-store.ts";

const LOG_SCOPE = "claude-transcripts";

/** A transcript could not be written back from the store. */
export class TranscriptRestoreError extends Schema.TaggedError<TranscriptRestoreError>()("TranscriptRestoreError", {
  cause: Schema.Defect(),
}) {
  override get message(): string {
    return this.cause instanceof Error ? this.cause.message : String(this.cause);
  }
}

/** Claude Code's transcripts could not be brought into the store. */
export class TranscriptAdoptionError extends Schema.TaggedError<TranscriptAdoptionError>()("TranscriptAdoptionError", {
  cause: Schema.Defect(),
}) {
  override get message(): string {
    return this.cause instanceof Error ? this.cause.message : String(this.cause);
  }
}

/** A Claude session alasio points at, and the directory it runs in. */
export interface AdoptedSession {
  readonly sessionId: string;
  readonly workingDirectory: string;
}

/** The sessions adoptTranscripts brings into the store. */
export interface AdoptTranscriptsOptions {
  readonly store: NeonSessionStore;
  readonly sessions: readonly AdoptedSession[];
}

/** One of a session's local transcripts: the main one (no subpath) or a subagent's. */
interface LocalTranscript {
  readonly subpath: string | undefined;
  readonly entries: readonly SessionStoreEntry[];
}

/**
 * Claude Code's home, as the SDK resolves it: from this process's
 * environment, which the SDK's own session helpers read too.
 */
function claudeHome(): string {
  return process.env["CLAUDE_CONFIG_DIR"]?.trim() || join(process.env["HOME"] || homedir(), ".claude");
}

function transcriptPath(home: string, projectKey: string, sessionId: string, subpath?: string): string {
  return subpath
    ? join(home, "projects", projectKey, sessionId, `${subpath}.jsonl`)
    : join(home, "projects", projectKey, `${sessionId}.jsonl`);
}

function writeAtomically(path: string, content: string): void {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(`${path}.alasio-tmp`, content, { mode: 0o600 });
  renameSync(`${path}.alasio-tmp`, path);
}

const jsonl = (entries: readonly SessionStoreEntry[]): string => entries.map((entry) => JSON.stringify(entry)).join("\n") + "\n";

/** A session's subagent transcripts and then its main one, as the store holds them, written here. */
async function writeBack(store: Pick<NeonSessionStore, "listSubkeys" | "load">, home: string, projectKey: string, sessionId: string): Promise<void> {
  const key = { projectKey, sessionId };
  for (const subpath of await store.listSubkeys(key)) {
    const entries = (await store.load({ ...key, subpath })) ?? [];
    const metadata = entries.filter((entry) => entry.type === "agent_metadata");
    const transcript = entries.filter((entry) => entry.type !== "agent_metadata");
    const path = transcriptPath(home, projectKey, sessionId, subpath);
    if (transcript.length > 0) writeAtomically(path, jsonl(transcript));
    const latest = metadata.at(-1);
    if (latest !== undefined) {
      const { type: _type, ...rest } = latest;
      writeAtomically(path.replace(/\.jsonl$/u, ".meta.json"), JSON.stringify(rest));
    }
  }
  // The main transcript last: its presence is what marks the copy complete.
  writeAtomically(transcriptPath(home, projectKey, sessionId), jsonl((await store.load(key)) ?? []));
}

/** What the store answers, or the work of writing a transcript back, failing as it failed. */
const restoring = <A>(work: () => Promise<A>): Effect.Effect<A, TranscriptRestoreError> =>
  Effect.tryPromise({ try: work, catch: (cause) => new TranscriptRestoreError({ cause }) });

/**
 * Writes a session's transcript back from the store if its local copy is
 * missing, as the SDK itself lays out a transcript it restores: the main
 * JSONL, each subagent's JSONL, and a subagent's metadata beside it.
 * Succeeds with whether the transcript is present locally afterwards.
 */
export const ensureLocalTranscript = Effect.fnUntraced(function*(
  store: Pick<NeonSessionStore, "projectKeyOf" | "listSubkeys" | "load">,
  sessionId: string,
): Effect.fn.Return<boolean, TranscriptRestoreError> {
  const projectKey = yield* restoring(() => store.projectKeyOf(sessionId));
  if (!projectKey) return false;
  const home = claudeHome();
  if (existsSync(transcriptPath(home, projectKey, sessionId))) return true;
  yield* restoring(() => writeBack(store, home, projectKey, sessionId));
  yield* Effect.logInfo(`wrote transcript ${sessionId} back from the store`);
  return true;
}, withLogScope(LOG_SCOPE));

/** A local transcript's entries, each line as Claude Code wrote it. */
function readJsonl(path: string): SessionStoreEntry[] {
  if (!existsSync(path)) return [];
  const entries: SessionStoreEntry[] = [];
  for (const line of readFileSync(path, "utf8").split("\n")) {
    if (!line.trim()) continue;
    try {
      entries.push(JSON.parse(line));
    } catch {
      // A torn last line from a crash: Claude Code ignores it too.
    }
  }
  return entries;
}

/** Where a session's local transcript lives: its project directory, if any. */
function findProject(home: string, sessionId: string): string | null {
  const projects = join(home, "projects");
  if (!existsSync(projects)) return null;
  for (const projectKey of readdirSync(projects)) {
    if (existsSync(join(projects, projectKey, `${sessionId}.jsonl`))) return projectKey;
  }
  return null;
}

/** The local transcripts of a session, main and subagents, by subpath. */
function localTranscripts(home: string, projectKey: string, sessionId: string): LocalTranscript[] {
  const transcripts: LocalTranscript[] = [{ subpath: undefined, entries: readJsonl(transcriptPath(home, projectKey, sessionId)) }];
  const subagents = join(home, "projects", projectKey, sessionId, "subagents");
  if (existsSync(subagents)) {
    for (const name of readdirSync(subagents, { recursive: true, encoding: "utf8" })) {
      if (!name.endsWith(".jsonl")) continue;
      const subpath = `subagents/${name.slice(0, -".jsonl".length)}`;
      transcripts.push({ subpath, entries: readJsonl(join(subagents, name)) });
    }
  }
  return transcripts;
}

/**
 * Adds whatever local entries the store lacks. Returns how many. Entries with
 * a uuid are matched by it; those without, such as the metadata Claude Code
 * writes as it exits, outside the mirror, by their JSON, counted, so an entry
 * written twice is kept twice and one already stored is not added again.
 */
async function reconcile(store: NeonSessionStore, home: string, projectKey: string, sessionId: string): Promise<number> {
  let added = 0;
  for (const { subpath, entries } of localTranscripts(home, projectKey, sessionId)) {
    const key: SessionKey = { projectKey, sessionId, ...(subpath ? { subpath } : {}) };
    const storedUuids = await store.uuidsOf(key);
    const storedUuidless = new Map<string, number>();
    for (const entry of await store.uuidlessEntriesOf(key)) {
      const text = JSON.stringify(entry);
      storedUuidless.set(text, (storedUuidless.get(text) ?? 0) + 1);
    }
    const missing = entries.filter((entry) => {
      if (typeof entry.uuid === "string") return !storedUuids.has(entry.uuid);
      const text = JSON.stringify(entry);
      const stored = storedUuidless.get(text) ?? 0;
      if (stored > 0) {
        storedUuidless.set(text, stored - 1);
        return false;
      }
      return true;
    });
    if (missing.length > 0) {
      await store.append(key, missing);
      added += missing.length;
    }
  }
  return added;
}

/** What adopting a transcript asks of the store, failing as it failed. */
const adopting = <A>(work: () => Promise<A>): Effect.Effect<A, TranscriptAdoptionError> =>
  Effect.tryPromise({ try: work, catch: (cause) => new TranscriptAdoptionError({ cause }) });

/**
 * Brings every session alasio points at into the store: `sessions` is
 * `[{ sessionId, workingDirectory }]`. Idempotent; run before serving. A session
 * that cannot be adopted is logged and left; failing to look for sessions here fails.
 */
export const adoptTranscripts = Effect.fnUntraced(function*({ store, sessions }: AdoptTranscriptsOptions): Effect.fn.Return<void, TranscriptAdoptionError> {
  const home = claudeHome();
  for (const { sessionId, workingDirectory } of sessions) {
    const projectKey = yield* Effect.try({ try: () => findProject(home, sessionId), catch: (cause) => new TranscriptAdoptionError({ cause }) });
    if (!projectKey) continue;
    yield* Effect.gen(function*() {
      if (!(yield* adopting(() => store.projectKeyOf(sessionId)))) {
        yield* adopting(() => importSessionToStore(sessionId, store, { dir: workingDirectory, includeSubagents: true }));
        yield* Effect.logInfo(`adopted transcript ${sessionId} into the store`);
        return;
      }
      const added = yield* adopting(() => reconcile(store, home, projectKey, sessionId));
      if (added > 0) yield* Effect.logInfo(`reconciled transcript ${sessionId}: ${added} missing entries added`);
    }).pipe(
      Effect.catch((error) => Effect.logError(`could not adopt transcript ${sessionId}: ${error.message}`)),
    );
  }
}, withLogScope(LOG_SCOPE));
