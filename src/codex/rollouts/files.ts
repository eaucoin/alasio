/**
 * The rollout files under `$CODEX_HOME`, found by Codex's own naming:
 * `rollout-<timestamp>-<thread id>.jsonl` under `sessions/YYYY/MM/DD/`, or
 * `archived_sessions/` once archived, with `_<rollout id>` after the thread
 * id for a reverted thread's newer file, and `.zst` appended when Codex's
 * optional compression has compressed it.
 */
import { readdirSync, statSync } from "node:fs";
import { basename, join } from "node:path";

export const ROLLOUT_DIRS: readonly string[] = ["sessions", "archived_sessions"];
const UUID = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";
const ROLLOUT_NAME = new RegExp(`^rollout-\\d{4}-\\d{2}-\\d{2}T\\d{2}-\\d{2}-\\d{2}-(${UUID})(?:_(${UUID}))?\\.jsonl$`, "iu");
const COMPRESSED = ".zst";

/** The ids a rollout file's name carries: its thread's, and its own, which is the thread's but for a reverted thread's newer file. */
export interface RolloutName {
  readonly threadId: string;
  readonly rolloutId: string;
}

/** A rollout file under the Codex home, as listRolloutFiles gives it. */
export interface RolloutFile {
  /** The file's name, without the compression suffix. */
  readonly name: string;
  /** Where the file is, relative to the Codex home. */
  readonly path: string;
  readonly size: number;
  readonly modifiedMs: number;
  readonly compressed: boolean;
}

/** Whether `error` is a file system error for a path that is not there. */
export function isNotFound(error: unknown): boolean {
  return typeof error === "object" && error !== null && "code" in error && error.code === "ENOENT";
}

/** A rollout file name's thread id and rollout id, or null for any other name. */
export function parseRolloutName(name: string): RolloutName | null {
  const [, threadId, rolloutId] = ROLLOUT_NAME.exec(name) ?? [];
  return threadId === undefined ? null : { threadId, rolloutId: rolloutId ?? threadId };
}

/**
 * Every rollout file here: `{ name, path, size, modifiedMs, compressed }`,
 * `path` relative to `home`, and `name` without the compression suffix, so a
 * file keeps its name compressed.
 */
export function listRolloutFiles(home: string): RolloutFile[] {
  const files: RolloutFile[] = [];
  for (const dir of ROLLOUT_DIRS) {
    let entries: string[];
    try {
      entries = readdirSync(join(home, dir), { recursive: true, encoding: "utf8" });
    } catch (error) {
      if (isNotFound(error)) continue;
      throw error;
    }
    for (const entry of entries) {
      const file = rolloutFile(home, join(dir, entry));
      if (file) files.push(file);
    }
  }
  return files;
}

/** The rollout file at `path` under `home`, as listRolloutFiles gives it, or null if there is none. */
export function rolloutFile(home: string, path: string): RolloutFile | null {
  const compressed = path.endsWith(COMPRESSED);
  const name = basename(compressed ? path.slice(0, -COMPRESSED.length) : path);
  if (!parseRolloutName(name)) return null;
  try {
    const stat = statSync(join(home, path));
    return stat.isFile() ? { name, path, size: stat.size, modifiedMs: stat.mtimeMs, compressed } : null;
  } catch (error) {
    // Moved or removed since it was named.
    if (isNotFound(error)) return null;
    throw error;
  }
}
