/**
 * The rollout files under `$CODEX_HOME`, found by Codex's own naming:
 * `rollout-<timestamp>-<thread id>.jsonl` under `sessions/YYYY/MM/DD/`, or
 * `archived_sessions/` once archived, with `_<rollout id>` after the thread
 * id for a reverted thread's newer file, and `.zst` appended when Codex's
 * optional compression has compressed it.
 */
import { readdirSync, statSync } from "node:fs";
import { basename, join } from "node:path";

const ROLLOUT_DIRS = ["sessions", "archived_sessions"];
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
      const compressed = entry.endsWith(COMPRESSED);
      const name = basename(compressed ? entry.slice(0, -COMPRESSED.length) : entry);
      if (!parseRolloutName(name)) continue;
      const path = join(dir, entry);
      try {
        const stat = statSync(join(home, path));
        if (stat.isFile()) files.push({ name, path, size: stat.size, modifiedMs: stat.mtimeMs, compressed });
      } catch (error) {
        // Moved or removed since the directory was read.
        if (error.code !== "ENOENT") throw error;
      }
    }
  }
  return files;
}
