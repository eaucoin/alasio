/**
 * alasio's Neon: the stack neon/compose.yml describes, brought up before alasio
 * serves, and the connection alasio keeps to it.
 *
 * `startNeon` renders the stack's configuration (neon/control/setup.js),
 * brings the compose project up, idempotently, leaving running services
 * alone and removing any no longer in it, and opens a pool to the compute.
 * Docker keeps the services running across crashes and reboots; this makes
 * sure they are up, as each alasio start does.
 *
 * It makes the analytics lake's role and catalog database either way (./lake.js).
 * With the lake on (ALASIO_LAKE_ENABLED) it also builds the lake's image where it is
 * missing and grants the lake its reads; with it off, it stops the lake and revokes
 * them.
 */
import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { promisify } from "node:util";

import pg from "pg";

import { COMPOSE_FILE, DEFAULT_BRIDGE, DEFAULT_COMPUTE_PORT, lakeImage, setupNeon } from "../../neon/control/setup.js";
import { NeonRolloutStore } from "../codex/rollouts/store.js";
import { NeonSessionStore } from "../harness/claude/session-store.js";
import { createLogger } from "../shared/log.js";
import { inSpan, parseKeyValueList, signalHeaders } from "../telemetry/index.js";
import { ensureLakeRole, lakeEnabled, syncLakeReads } from "./lake.js";

const log = createLogger("neon");
const run = promisify(execFile);

export const NEON_PROJECT = "alasio-neon";

/**
 * The bridge a project's network gets: the stable name for alasio's own stack, and one
 * derived from the project for any other (a test stack), since two bridges cannot share
 * a name and Linux caps interface names at fifteen characters.
 */
export function neonBridgeName(project) {
  return project === NEON_PROJECT ? DEFAULT_BRIDGE : `qn${createHash("sha256").update(project).digest("hex").slice(0, 12)}`;
}

/** How long the whole stack may take to come up healthy, from nothing. */
const STACK_UP_TIMEOUT_SECONDS = 600;

/** The docker compose invocation for the stack under `stateDir`. */
export function composeCommand(layout, project = NEON_PROJECT) {
  return ["compose", "--project-name", project, "--file", COMPOSE_FILE, "--env-file", layout.composeEnv];
}

/**
 * Where the stack's telemetry goes: `ALASIO_NEON_OTLP_ENDPOINT`, an OTLP/HTTP endpoint
 * as the stack's own network reaches it (alasio's endpoint is as this machine reaches
 * it), with the headers alasio's metrics go with; null for nowhere.
 */
export function neonTelemetry(env = process.env) {
  const endpoint = env.ALASIO_NEON_OTLP_ENDPOINT?.trim();
  return endpoint ? { endpoint, headers: parseKeyValueList(signalHeaders(env, "metrics")) } : null;
}

/** Builds the lake's image unless this revision of it is already built. */
async function buildLakeImage(layout, project) {
  const image = lakeImage();
  const built = await run("docker", ["image", "inspect", image]).then(() => true, () => false);
  if (built) return;
  log.info(`building ${image}`);
  await run("docker", [...composeCommand(layout, project), "build", "lake"], { maxBuffer: 64 * 1024 * 1024 });
}

/**
 * Removes the lake's images this revision supersedes, once the lake runs the new one;
 * one another stack's lake still runs is left.
 */
async function pruneLakeImages() {
  const current = lakeImage();
  const { stdout } = await run("docker", ["image", "ls", "--format", "{{.Repository}}:{{.Tag}}", current.split(":")[0]]);
  for (const image of stdout.split("\n").filter((name) => name && name !== current)) {
    await run("docker", ["image", "rm", image]).catch(() => {});
  }
}

/**
 * Pulls every image the stack runs, so its first start waits on none, and builds the
 * lake's when it is on.
 */
export async function pullNeon({ stateDir, project = NEON_PROJECT, lake = lakeEnabled() }) {
  const layout = setupNeon(stateDir, { bridgeName: neonBridgeName(project), otlp: neonTelemetry(), lake });
  await run("docker", [...composeCommand(layout, project), "pull", "--quiet", "--ignore-buildable"], { maxBuffer: 16 * 1024 * 1024 });
  if (lake) await buildLakeImage(layout, project);
}

/**
 * Brings the stack up and connects to it. Returns `{ pool, store, rollouts, lake, close }`:
 * `store` keeps Claude Code's transcripts, `rollouts` Codex's rollout files, and `lake`
 * says whether the analytics lake runs. `project` and `computePort` exist for tests,
 * which run their own stack.
 */
export async function startNeon({ stateDir, project = NEON_PROJECT, computePort = DEFAULT_COMPUTE_PORT, lake = lakeEnabled() }) {
  const otlp = neonTelemetry();
  const layout = setupNeon(stateDir, { computePort, bridgeName: neonBridgeName(project), otlp, lake });
  log.info(`bringing up ${project}`);
  const { pool, store, rollouts } = await inSpan("alasio.neon.start", { attributes: { "alasio.neon.project": project, "alasio.neon.lake": lake } }, async () => {
    if (lake) await buildLakeImage(layout, project);
    // --remove-orphans: a service no longer in compose.yml goes with the next start.
    await run("docker", [...composeCommand(layout, project), "up", "--detach", "--wait", "--remove-orphans", "--wait-timeout", String(STACK_UP_TIMEOUT_SECONDS)], {
      maxBuffer: 16 * 1024 * 1024,
    });
    if (!otlp) {
      // A service of a profile no longer on is left running by `up`; telemetry turned
      // off stops its collector here.
      await run("docker", [...composeCommand(layout, project), "--profile", "telemetry", "rm", "--stop", "--force", "otel-collector"]);
    }
    if (lake) {
      await pruneLakeImages();
    } else {
      // Likewise the lake, turned off; its catalog and files stay, for when it is on again.
      await run("docker", [...composeCommand(layout, project), "--profile", "lake", "rm", "--stop", "--force", "lake", "lake-init"]);
    }
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
    await ensureLakeRole(pool, readFileSync(layout.lakePasswordFile, "utf8"));
    await syncLakeReads(pool, lake);
    return { pool, store, rollouts };
  });
  log.info(`${project} is up`);
  return {
    pool,
    store,
    rollouts,
    lake,
    close: () => pool.end(),
  };
}
