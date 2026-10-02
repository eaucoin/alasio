/**
 * alasio's Neon: the connection alasio keeps to the database its deployment runs (the Helm
 * chart's Neon, or one of the operator's), and what alasio keeps in it.
 *
 * It makes the analytics lake's role and catalog database either way (./lake.js), and
 * with the lake on (ALASIO_LAKE_ENABLED) grants the lake its reads; with it off, it
 * revokes them.
 */
import { readFileSync } from "node:fs";
import { setTimeout as sleep } from "node:timers/promises";

import pg from "pg";

import { NeonRolloutStore } from "../codex/rollouts/store.js";
import { NeonSessionStore } from "../harness/claude/session-store.js";
import { createLogger } from "../shared/log.js";
import { inSpan } from "../telemetry/index.js";
import { ensureLakeRole, lakeEnabled, syncLakeReads } from "./lake.js";

const log = createLogger("neon");

/**
 * Connects to a Neon that is already up and makes what alasio keeps in it: the session
 * and rollout stores' schemas, and the lake's role and reads.
 */
async function openNeon({ databaseUrl, lakePassword, lake }) {
  const pool = new pg.Pool({
    connectionString: databaseUrl,
    max: 8,
    idleTimeoutMillis: 30_000,
    connectionTimeoutMillis: 30_000,
  });
  // An idle connection dies with the compute when it restarts; the pool
  // replaces it on the next checkout.
  pool.on("error", (error) => log.warn(`idle database connection lost: ${error.message}`));
  try {
    const store = new NeonSessionStore(pool);
    await store.ensureSchema();
    const rollouts = new NeonRolloutStore(pool);
    await rollouts.ensureSchema();
    await ensureLakeRole(pool, lakePassword);
    await syncLakeReads(pool, lake);
    return { pool, store, rollouts };
  } catch (error) {
    await pool.end().catch(() => {});
    throw error;
  }
}

/** How long alasio waits for a deployment's Neon to answer as it starts. */
const CONNECT_TIMEOUT_MS = 600_000;

function fileOrValue(env, key) {
  const file = env[`${key}_FILE`]?.trim();
  if (file) return readFileSync(file, "utf8").trim();
  const value = env[key]?.trim();
  if (!value) throw new Error(`${key} or ${key}_FILE must be set`);
  return value;
}

/**
 * Connects to the Neon the deployment provides, from `ALASIO_DATABASE_URL` and
 * `ALASIO_LAKE_PASSWORD` or their `_FILE` forms. The stack starts beside alasio, so it
 * is waited for, up to ten minutes, retrying while it does not answer. Returns `{ pool,
 * store, rollouts, lake, close }`: `store` keeps Claude Code's transcripts, `rollouts`
 * Codex's rollout files, and `lake` says whether the analytics lake runs.
 */
export async function connectNeon({ env = process.env, lake = lakeEnabled(env), timeoutMs = CONNECT_TIMEOUT_MS } = {}) {
  const databaseUrl = fileOrValue(env, "ALASIO_DATABASE_URL");
  const lakePassword = fileOrValue(env, "ALASIO_LAKE_PASSWORD");
  const deadline = Date.now() + timeoutMs;
  const { pool, store, rollouts } = await inSpan("alasio.neon.connect", { attributes: { "alasio.neon.lake": lake } }, async () => {
    for (let attempt = 1; ; attempt += 1) {
      try {
        return await openNeon({ databaseUrl, lakePassword, lake });
      } catch (error) {
        if (Date.now() + 5000 > deadline) throw error;
        if (attempt === 1 || attempt % 12 === 0) log.info(`waiting for Neon: ${error.message}`);
        await sleep(5000);
      }
    }
  });
  log.info("connected to Neon");
  return { pool, store, rollouts, lake, close: () => pool.end() };
}
