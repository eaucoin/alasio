/**
 * The rollout files under `$CODEX_HOME`, found by Codex's own naming:
 * `rollout-<timestamp>-<thread id>.jsonl` under `sessions/YYYY/MM/DD/`, or
 * `archived_sessions/` once archived, with `_<rollout id>` after the thread
 * id for a reverted thread's newer file, and `.zst` appended when Codex's
 * optional compression has compressed it.
 */
import { readdirSync, statSync } from "node:fs";
import { basename, join } from "node:path";

export const ROLLOUT_DIRS = ["sessions", "archived_sessions"];
const UUID = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";
const ROLLOUT_NAME = new RegExp(`^rollout-\\d{4}-\\d{2}-\\d{2}T\\d{2}-\\d{2}-\\d{2}-(${UUID})(?:_(${UUID}))?\\.jsonl$`, "iu");
const COMPRESSED = ".zst";

/** A rollout file name's thread id and rollout id, or null for any other name. */
export function parseRolloutName(name) {
  const match = ROLLOUT_NAME.exec(name);
  return match ? { threadId: match[1], rolloutId: match[2] ?? match[1] } : null;
}

/**
 * Every rollout file here: `{ name, path, size, modifiedMs, compressed }`,
 * `path` relative to `home`, and `name` without the compression suffix, so a
 * file keeps its name compressed.
 */
export function listRolloutFiles(home) {
  const files = [];
  for (const dir of ROLLOUT_DIRS) {
    let entries;
    try {
      entries = readdirSync(join(home, dir), { recursive: true, encoding: "utf8" });
    } catch (error) {
      if (error.code === "ENOENT") continue;
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
export function rolloutFile(home, path) {
  const compressed = path.endsWith(COMPRESSED);
  const name = basename(compressed ? path.slice(0, -COMPRESSED.length) : path);
  if (!parseRolloutName(name)) return null;
  try {
    const stat = statSync(join(home, path));
    return stat.isFile() ? { name, path, size: stat.size, modifiedMs: stat.mtimeMs, compressed } : null;
  } catch (error) {
    // Moved or removed since it was named.
    if (error.code === "ENOENT") return null;
    throw error;
  }
}
