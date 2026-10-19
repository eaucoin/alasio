/**
 * The operator's Codex configuration, as alasio gives it a harness pool's pod's folder
 * conversations' Codex each turn (../harness/pooled-host.ts): what of their Codex home configures
 * Codex, its config.toml, AGENTS.md, prompts and skills, by path under it, as text; never their
 * login (auth.json), nor what Codex keeps of its own (sessions, logs, its state's databases).
 * Each file of MAX_FILE_BYTES at most, and MAX_TOTAL_BYTES in all, what is past them left out.
 */
import { lstat, readdir, readFile } from "node:fs/promises";
import { join, relative } from "node:path";

import { Effect } from "effect";

/** The files and directories of a Codex home that configure Codex. */
const CONFIGURING = ["config.toml", "AGENTS.md", "prompts", "skills"];
export const MAX_FILE_BYTES = 256 * 1024;
export const MAX_TOTAL_BYTES = 2 * 1024 ** 2;

/** The operator's Codex configuration in `home`, as the module says: none of what is not there. */
export const codexConfiguration = (home: string): Effect.Effect<Readonly<Record<string, string>>> =>
  Effect.promise(async () => {
    const files: Record<string, string> = {};
    let total = 0;
    const visit = async (path: string): Promise<void> => {
      const stats = await lstat(path).catch(() => null);
      if (!stats || stats.isSymbolicLink()) return;
      if (stats.isDirectory()) {
        for (const name of (await readdir(path).catch(() => [] as string[])).sort()) await visit(join(path, name));
        return;
      }
      if (!stats.isFile() || stats.size > MAX_FILE_BYTES || total + stats.size > MAX_TOTAL_BYTES) return;
      const text = await readFile(path, "utf8").catch(() => null);
      if (text === null) return;
      files[relative(home, path)] = text;
      total += stats.size;
    };
    for (const name of CONFIGURING) await visit(join(home, name));
    return files;
  });
