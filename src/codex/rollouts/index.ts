// @ts-nocheck
/**
 * Codex's rollout files, kept in alasio's Neon: a mirror that copies each
 * change to them as it is written, and restore, which writes back what a
 * thread alasio points at needs (restore.ts).
 *
 * The kernel reports each write to the rollout directories, and the file is
 * mirrored within milliseconds. A check every half minute mirrors anything
 * a report missed: changes made while alasio was down, a directory made after
 * its watch could start, or a failed watch, which it starts again. `flush`
 * mirrors one thread now, for a turn to wait on before its reply is final.
 *
 * Every write to the store goes through one queue, so two changes to a
 * file are never mirrored at once. The mirror is not on Codex's path: Codex
 * writes its files as ever, and if the mirror fails, the next check catches
 * up on everything.
 */
import { watch } from "node:fs";
import { join } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";

import { createLogger } from "../../shared/log.ts";
import { listRolloutFiles, parseRolloutName, rolloutFile, ROLLOUT_DIRS } from "./files.ts";
import { mirrorRollout } from "./mirror.ts";
import { restoreRollouts } from "./restore.ts";

const log = createLogger("codex-rollouts");

/** How often every rollout file is checked. */
const CHECK_MS = 30_000;
/** How long the check waits after an error. */
const RETRY_MS = 60_000;
/** How long a change may take to be reported before the check counts it as missed. */
const REPORT_GRACE_MS = 5_000;
/** How long a flush may take before its turn goes on without it. */
const FLUSH_TIMEOUT_MS = 5_000;
/** The key of the Codex home's own watcher, beside the rollout directories'. */
const HOME = ".";

const errorText = (error) => (error instanceof Error ? error.message : String(error));

/**
 * Starts keeping the rollouts under `home` in `store`. Returns
 * `{ restore(threadIds), flush(threadId), close() }`.
 */
export function startCodexRollouts({ store, home }) {
  const controller = new AbortController();
  const { signal } = controller;
  const watchers = new Map();
  // Paths the watchers reported that are not yet mirrored.
  const reported = new Set();
  let queue = Promise.resolve();
  let known = null;
  let drainQueued = false;
  let failing = false;

  /** Runs `work` once everything queued before it is done. */
  function serially(work) {
    const run = queue.then(async () => {
      try {
        known ??= new Map((await store.list()).map(({ name, ...kept }) => [name, kept]));
        const result = await work();
        if (failing) log.info("mirroring is running again");
        failing = false;
        return result;
      } catch (error) {
        // What the store keeps may have changed with the failure: read it again.
        known = null;
        throw error;
      }
    });
    queue = run.catch(() => {});
    return run;
  }

  function failed(error) {
    if (!failing) log.warn(`mirroring failed, and the next check retries it: ${errorText(error)}`);
    failing = true;
  }

  async function drain() {
    drainQueued = false;
    for (const path of reported) {
      reported.delete(path);
      const file = rolloutFile(home, path);
      if (file) await mirrorRollout({ store, home, known, file });
    }
  }

  function report(dir, filename) {
    if (filename) reported.add(join(dir, filename));
    if (drainQueued) return;
    drainQueued = true;
    // A report without a file name, which the kernel may give, is a check.
    serially(filename ? drain : check).catch((error) => {
      drainQueued = false;
      failed(error);
    });
  }

  /** Starts a watcher kept under `key`, which the next check starts again if it fails. Returns whether it started. */
  function startWatcher(key, path, options, onChange) {
    try {
      const watcher = watch(path, options, onChange);
      watcher.on("error", (error) => {
        log.warn(`watching ${path} failed, and the next check watches it again: ${errorText(error)}`);
        watcher.close();
        watchers.delete(key);
      });
      watchers.set(key, watcher);
      return true;
    } catch (error) {
      if (error.code !== "ENOENT") log.warn(`could not watch ${path}: ${errorText(error)}`);
      return false;
    }
  }

  /**
   * Watches each rollout directory, and while one is missing, the Codex home
   * for it to be made, as Codex does with a new home's first thread.
   */
  function watchDirs() {
    const missing = ROLLOUT_DIRS.filter(
      (dir) => !watchers.has(dir) && !startWatcher(dir, join(home, dir), { recursive: true }, (_event, filename) => report(dir, filename)),
    );
    if (missing.length === 0) {
      watchers.get(HOME)?.close();
      watchers.delete(HOME);
    } else if (!watchers.has(HOME)) {
      startWatcher(HOME, home, {}, (_event, filename) => {
        if (!missing.includes(filename)) return;
        watchDirs();
        report(filename, null);
      });
    }
  }

  /** Mirrors every file that changed. Returns how many, and how many of those no report had covered. */
  async function check() {
    drainQueued = false;
    reported.clear();
    let mirrored = 0;
    let missed = 0;
    for (const file of listRolloutFiles(home)) {
      if (!(await mirrorRollout({ store, home, known, file }))) continue;
      mirrored += 1;
      if (Date.now() - file.modifiedMs > REPORT_GRACE_MS) missed += 1;
    }
    return { mirrored, missed };
  }

  const running = (async () => {
    let catchingUp = true;
    while (!signal.aborted) {
      watchDirs();
      let delay = CHECK_MS;
      try {
        const { mirrored, missed } = await serially(check);
        if (catchingUp) log.info(`mirrored ${mirrored} rollout file(s) changed since the store last saw them`);
        else if (missed > 0) log.warn(`the check mirrored ${missed} change(s) no watch reported`);
        catchingUp = false;
      } catch (error) {
        failed(error);
        catchingUp = true;
        delay = RETRY_MS;
      }
      await sleep(delay, undefined, { signal }).catch(() => {});
    }
  })();

  return {
    /** Writes back from the store what these threads need and this machine lacks. Returns the paths written. */
    async restore(threadIds) {
      return await restoreRollouts({ store, threadIds, home });
    },

    /** Mirrors a thread's files now. Rejects if that fails or takes longer than FLUSH_TIMEOUT_MS. */
    async flush(threadId) {
      const flushed = serially(async () => {
        for (const file of listRolloutFiles(home)) {
          if (parseRolloutName(file.name).threadId === threadId) await mirrorRollout({ store, home, known, file });
        }
      });
      const timeout = new AbortController();
      try {
        await Promise.race([
          flushed,
          sleep(FLUSH_TIMEOUT_MS, undefined, { signal: timeout.signal }).then(() => {
            throw new Error(`not mirrored within ${FLUSH_TIMEOUT_MS / 1000}s`);
          }),
        ]);
      } finally {
        timeout.abort();
      }
    },

    async close() {
      controller.abort();
      for (const watcher of watchers.values()) watcher.close();
      watchers.clear();
      await running;
      await queue;
    },
  };
}
