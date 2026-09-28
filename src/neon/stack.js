/**
 * alasio's Neon: the stack neon/compose.yml describes, brought up before alasio
 * serves, and the connection alasio keeps to it.
 *
 * `startNeon` renders the stack's configuration (neon/control/setup.js),
 * brings the compose project up, idempotently, leaving running services
 * alone and removing any no longer in it, and opens a pool to the compute.
 * Docker keeps the services running across crashes and reboots; this makes
 * sure they are up, as each alasio start does.
 */
import { execFile } from "node:child_process";
import { readFileSync } from "node:fs";
import { promisify } from "node:util";

import pg from "pg";

import { COMPOSE_FILE, DEFAULT_COMPUTE_PORT, setupNeon } from "../../neon/control/setup.js";
import { NeonRolloutStore } from "../codex/rollouts/store.js";
import { NeonSessionStore } from "../harness/claude/session-store.js";
import { createLogger } from "../shared/log.js";

const log = createLogger("neon");
const run = promisify(execFile);

export const NEON_PROJECT = "alasio-neon";

/** How long the whole stack may take to come up healthy, from nothing. */
const STACK_UP_TIMEOUT_SECONDS = 600;

/** The docker compose invocation for the stack under `stateDir`. */
export function composeCommand(layout, project = NEON_PROJECT) {
  return ["compose", "--project-name", project, "--file", COMPOSE_FILE, "--env-file", layout.composeEnv];
}

/** Pulls every image the stack runs, so its first start waits on none. */
export async function pullNeon({ stateDir, project = NEON_PROJECT }) {
  const layout = setupNeon(stateDir);
  await run("docker", [...composeCommand(layout, project), "pull", "--quiet"], { maxBuffer: 16 * 1024 * 1024 });
}

/**
 * Brings the stack up and connects to it. Returns `{ pool, store, rollouts, close }`:
 * `store` keeps Claude Code's transcripts, `rollouts` Codex's rollout files.
 * `project` and `computePort` exist for tests, which run their own stack.
 */
export async function startNeon({ stateDir, project = NEON_PROJECT, computePort = DEFAULT_COMPUTE_PORT }) {
  const layout = setupNeon(stateDir, { computePort });
  log.info(`bringing up ${project}`);
  // --remove-orphans: a service no longer in compose.yml goes with the next start.
  await run("docker", [...composeCommand(layout, project), "up", "--detach", "--wait", "--remove-orphans", "--wait-timeout", String(STACK_UP_TIMEOUT_SECONDS)], {
    maxBuffer: 16 * 1024 * 1024,
  });
  const pool = new pg.Pool({
    connectionString: readFileSync(layout.databaseUrlFile, "utf8").trim(),
    max: 8,
    idleTimeoutMillis: 30_000,
    connectionTimeoutMillis: 30_000,
  });
  // An idle connection dies with the compute when it restarts; the pool
  // replaces it on the next checkout.
  pool.on("error", (error) => log.warn(`idle database connection lost: ${error.message}`));
  const store = new NeonSessionStore(pool);
  await store.ensureSchema();
  const rollouts = new NeonRolloutStore(pool);
  await rollouts.ensureSchema();
  log.info(`${project} is up`);
  return {
    pool,
    store,
    rollouts,
    close: () => pool.end(),
  };
}
