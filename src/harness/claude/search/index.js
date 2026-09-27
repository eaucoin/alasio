/**
 * Transcript search: keeps the session store's entries searchable, from
 * beside the store. While alasio runs, the indexer reads new entries into
 * passages within seconds of their append, and on a first start works
 * through every entry already stored.
 *
 * It is not on the SDK's path: an append never waits on it, and if it falls
 * behind or fails, it catches up when it can. Search itself is SQL,
 * claude_sessions.search() (schema.js), for any client to call.
 */
import { setTimeout as sleep } from "node:timers/promises";

import { createLogger } from "../../../shared/log.js";
import { DEFAULT_SCHEMA } from "../session-store.js";
import { collectOrphans, indexBatch, settle } from "./indexer.js";
import { ensureSearchSchema } from "./schema.js";

const log = createLogger("transcript-search");

/** How long the indexer, with nothing to read, waits before looking again. */
const IDLE_MS = 2_000;
/** How long it waits after an error. */
const RETRY_MS = 60_000;
/** How often passages no entry holds are dropped. */
const ORPHANS_EVERY_MS = 60 * 60 * 1000;

/** Starts transcript search on the store's pool. Returns `{ close }`. */
export function startTranscriptSearch({ pool, schema = DEFAULT_SCHEMA }) {
  const controller = new AbortController();
  const { signal } = controller;
  let ready = false;
  let caughtUp = false;
  let orphansAt = 0;

  /** One pass: a batch of entries, or when there are none, upkeep. Returns the delay before the next. */
  async function step() {
    if (!ready) {
      await ensureSearchSchema(pool, schema);
      ready = true;
    }
    if ((await indexBatch(pool, schema)) > 0) return 0;
    if (!caughtUp) log.info("every stored entry is indexed");
    caughtUp = true;
    await settle(pool, schema);
    if (Date.now() - orphansAt > ORPHANS_EVERY_MS) {
      const dropped = await collectOrphans(pool, schema);
      if (dropped > 0) log.info(`dropped ${dropped} passage(s) no entry holds any more`);
      orphansAt = Date.now();
    }
    return IDLE_MS;
  }

  // An error is logged once, until indexing succeeds again, and waited out.
  const running = (async () => {
    let failing = false;
    while (!signal.aborted) {
      let delay;
      try {
        delay = await step();
        if (failing) log.info("indexing is running again");
        failing = false;
      } catch (error) {
        if (!failing) log.warn(`indexing failed, and retries every ${RETRY_MS / 1000}s: ${error instanceof Error ? error.message : String(error)}`);
        failing = true;
        delay = RETRY_MS;
      }
      if (delay > 0) await sleep(delay, undefined, { signal }).catch(() => {});
    }
  })();

  return {
    async close() {
      controller.abort();
      await running;
    },
  };
}
