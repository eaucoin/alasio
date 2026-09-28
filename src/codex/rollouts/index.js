/**
 * Codex's rollout files, kept in alasio's Neon: a mirror that copies every
 * change to them within seconds, and restore, which writes back what a
 * thread alasio points at needs (restore.js).
 *
 * The mirror is not on Codex's path: Codex writes its files as ever, and if
 * the mirror falls behind or fails, it catches up when it can.
 */
import { setTimeout as sleep } from "node:timers/promises";

import { createLogger } from "../../shared/log.js";
import { mirrorRollouts } from "./mirror.js";

const log = createLogger("rollout-mirror");

/** How long the mirror waits between passes. */
const INTERVAL_MS = 2_000;
/** How long it waits after an error. */
const RETRY_MS = 60_000;

/** Starts mirroring the rollouts under `home` into `store`. Returns `{ close }`. */
export function startRolloutMirror({ store, home }) {
  const controller = new AbortController();
  const { signal } = controller;
  let known = null;

  async function pass() {
    const catchingUp = known === null;
    known ??= new Map((await store.list()).map(({ name, ...kept }) => [name, kept]));
    const mirrored = await mirrorRollouts({ store, home, known });
    if (catchingUp) log.info(`mirrored ${mirrored} rollout file(s) changed since the store last saw them`);
  }

  // An error is logged once, until a pass succeeds again, and waited out.
  const running = (async () => {
    let failing = false;
    while (!signal.aborted) {
      let delay = INTERVAL_MS;
      try {
        await pass();
        if (failing) log.info("mirroring is running again");
        failing = false;
      } catch (error) {
        if (!failing) log.warn(`mirroring failed, and retries every ${RETRY_MS / 1000}s: ${error instanceof Error ? error.message : String(error)}`);
        failing = true;
        // What the store keeps may have changed with the failure: read it again.
        known = null;
        delay = RETRY_MS;
      }
      await sleep(delay, undefined, { signal }).catch(() => {});
    }
  })();

  return {
    async close() {
      controller.abort();
      await running;
    },
  };
}
