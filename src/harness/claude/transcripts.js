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
import { importSessionToStore } from "@anthropic-ai/claude-agent-sdk";
import { existsSync, mkdirSync, readdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join } from "node:path";

import { createLogger } from "../../shared/log.js";

const log = createLogger("claude-transcripts");

/**
 * Claude Code's home, as the SDK resolves it: from this process's
 * environment, which the SDK's own session helpers read too.
 */
export function claudeHome() {
  return process.env.CLAUDE_CONFIG_DIR?.trim() || join(process.env.HOME || homedir(), ".claude");
}

function transcriptPath(home, projectKey, sessionId, subpath) {
  return subpath
    ? join(home, "projects", projectKey, sessionId, `${subpath}.jsonl`)
    : join(home, "projects", projectKey, `${sessionId}.jsonl`);
}

function writeAtomically(path, content) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(`${path}.alasio-tmp`, content, { mode: 0o600 });
  renameSync(`${path}.alasio-tmp`, path);
}

const jsonl = (entries) => entries.map((entry) => JSON.stringify(entry)).join("\n") + "\n";

/**
 * Writes a session's transcript back from the store if its local copy is
 * missing, as the SDK itself lays out a transcript it restores: the main
 * JSONL, each subagent's JSONL, and a subagent's metadata beside it.
 * Returns whether the transcript is present locally afterwards.
 */
export async function ensureLocalTranscript(store, sessionId) {
  const projectKey = await store.projectKeyOf(sessionId);
  if (!projectKey) return false;
  const home = claudeHome();
  const main = transcriptPath(home, projectKey, sessionId);
  if (existsSync(main)) return true;

  const key = { projectKey, sessionId };
  for (const subpath of await store.listSubkeys(key)) {
    const entries = (await store.load({ ...key, subpath })) ?? [];
    const metadata = entries.filter((entry) => entry.type === "agent_metadata");
    const transcript = entries.filter((entry) => entry.type !== "agent_metadata");
    const path = transcriptPath(home, projectKey, sessionId, subpath);
    if (transcript.length > 0) writeAtomically(path, jsonl(transcript));
    if (metadata.length > 0) {
      const { type: _type, ...rest } = metadata.at(-1);
      writeAtomically(path.replace(/\.jsonl$/u, ".meta.json"), JSON.stringify(rest));
    }
  }
  // The main transcript last: its presence is what marks the copy complete.
  writeAtomically(main, jsonl((await store.load(key)) ?? []));
  log.info(`wrote transcript ${sessionId} back from the store`);
  return true;
}

function readJsonl(path) {
  if (!existsSync(path)) return [];
  const entries = [];
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
function findProject(home, sessionId) {
  const projects = join(home, "projects");
  if (!existsSync(projects)) return null;
  for (const projectKey of readdirSync(projects)) {
    if (existsSync(join(projects, projectKey, `${sessionId}.jsonl`))) return projectKey;
  }
  return null;
}

/** The local transcripts of a session, main and subagents, by subpath. */
function localTranscripts(home, projectKey, sessionId) {
  const transcripts = [{ subpath: undefined, entries: readJsonl(transcriptPath(home, projectKey, sessionId)) }];
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
async function reconcile(store, home, projectKey, sessionId) {
  let added = 0;
  for (const { subpath, entries } of localTranscripts(home, projectKey, sessionId)) {
    const key = { projectKey, sessionId, ...(subpath ? { subpath } : {}) };
    const storedUuids = await store.uuidsOf(key);
    const storedUuidless = new Map();
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

/**
 * Brings every session alasio points at into the store: `sessions` is
 * `[{ sessionId, workingDirectory }]`. Idempotent; run before serving.
 */
export async function adoptTranscripts({ store, sessions }) {
  const home = claudeHome();
  for (const { sessionId, workingDirectory } of sessions) {
    const projectKey = findProject(home, sessionId);
    if (!projectKey) continue;
    try {
      if (!(await store.projectKeyOf(sessionId))) {
        await importSessionToStore(sessionId, store, { dir: workingDirectory, includeSubagents: true });
        log.info(`adopted transcript ${sessionId} into the store`);
        continue;
      }
      const added = await reconcile(store, home, projectKey, sessionId);
      if (added > 0) log.info(`reconciled transcript ${sessionId}: ${added} missing entries added`);
    } catch (error) {
      log.error(`could not adopt transcript ${sessionId}: ${error instanceof Error ? error.message : String(error)}`);
    }
  }
}
