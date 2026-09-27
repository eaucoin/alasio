/**
 * Transcript search: keeps the session store's entries searchable, from
 * beside the store. Two loops run while alasio does:
 *
 * - the indexer reads new entries into passages within seconds of their
 *   append, and on a first start works through every entry already stored;
 * - the embedder embeds passages as the model allows, using at most half the
 *   time, so the compute's CPU stays free for everything else.
 *
 * Neither is on the SDK's path: an append never waits on them, and if either
 * falls behind or fails, it catches up when it can. Search itself is SQL,
 * claude_sessions.search() (schema.js), for any client to call.
 */
import { setTimeout as sleep } from "node:timers/promises";

import { createLogger } from "../../../shared/log.js";
import { DEFAULT_SCHEMA } from "../session-store.js";
import { embedBatch } from "./embedder.js";
import { collectOrphans, indexBatch, settle } from "./indexer.js";
import { ensureSearchSchema } from "./schema.js";

const log = createLogger("transcript-search");

/** How long a loop with nothing to do waits before looking again. */
const IDLE_MS = 2_000;
/** How long a loop waits after an error, and the embedder while the model is away. */
const RETRY_MS = 60_000;
/** How often passages no entry holds are dropped. */
const ORPHANS_EVERY_MS = 60 * 60 * 1000;

/**
 * Runs `step` until `signal` aborts. `step` returns a delay before it runs
 * again: 0 to go straight on. An error is logged once until the step
 * succeeds again, and waited out.
 */
async function loop(name, step, signal) {
  let failing = false;
  while (!signal.aborted) {
    let delay;
    try {
      delay = await step();
      if (failing) log.info(`${name} is running again`);
      failing = false;
    } catch (error) {
      if (!failing) log.warn(`${name} failed, and retries every ${RETRY_MS / 1000}s: ${error instanceof Error ? error.message : String(error)}`);
      failing = true;
      delay = RETRY_MS;
    }
    if (delay > 0) await sleep(delay, undefined, { signal }).catch(() => {});
  }
}

/** Starts transcript search on the store's pool. Returns `{ close }`. */
export function startTranscriptSearch({ pool, schema = DEFAULT_SCHEMA }) {
  const controller = new AbortController();
  const { signal } = controller;
  let ready = null;
  const ensureReady = () => (ready ??= ensureSearchSchema(pool, schema).catch((error) => {
    ready = null;
    throw error;
  }));

  let orphansAt = 0;
  let caughtUp = false;
  const indexer = loop(
    "indexing",
    async () => {
      await ensureReady();
      const read = await indexBatch(pool, schema);
      if (read > 0) return 0;
      if (!caughtUp) log.info("every stored entry is indexed");
      caughtUp = true;
      await settle(pool, schema);
      if (Date.now() - orphansAt > ORPHANS_EVERY_MS) {
        const dropped = await collectOrphans(pool, schema);
        if (dropped > 0) log.info(`dropped ${dropped} passage(s) no entry holds any more`);
        orphansAt = Date.now();
      }
      return IDLE_MS;
    },
    signal,
  );

  let modelAway = false;
  const embedder = loop(
    "embedding",
    async () => {
      await ensureReady();
      const started = Date.now();
      const handled = await embedBatch(pool, schema);
      if (handled === null) {
        if (!modelAway) log.warn(`pgrag's embedding model is unavailable; search runs on words until it is, looking again every ${RETRY_MS / 1000}s`);
        modelAway = true;
        return RETRY_MS;
      }
      if (modelAway) log.info("pgrag's embedding model is available");
      modelAway = false;
      // As long again as the batch took: at most half the time embedding.
      return handled > 0 ? Date.now() - started : IDLE_MS;
    },
    signal,
  );

  return {
    async close() {
      controller.abort();
      await Promise.all([indexer, embedder]);
    },
  };
}
