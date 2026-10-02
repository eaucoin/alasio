/**
 * The lake service: the stack's container that keeps the analytics lake loaded from
 * alasio's Neon (see ../README.md). One loads at a time, which a Postgres advisory
 * lock on the catalog makes sure of; DuckDB keeps nothing of its own, so the
 * container is replaceable at any moment, and the loader opens its connections again
 * whenever they fail, as when the compute restarts.
 *
 * It serves /healthz (the compose healthcheck) and /metrics (Prometheus, for the
 * stack's telemetry collector) on LAKE_HTTP_PORT from the moment it starts, and logs a
 * JSON line per event.
 */
import { createServer } from "node:http";
import { setTimeout as sleep } from "node:timers/promises";

import pg from "pg";

import { loadConfig } from "./config.js";
import { openLake } from "./lake.js";
import { startLoader } from "./loader.js";
import { createMetrics } from "./metrics.js";
import { prepareLake } from "./sync.js";

/** Names the one loader's advisory lock, in the catalog database. */
const LOCK = "alasio.lake.loader";
const LOCK_RETRY_MS = 10_000;

function log(message, fields = {}) {
  console.log(JSON.stringify({ time: new Date().toISOString(), message, ...fields }));
}

/**
 * Takes the loader's lock, waiting while another loader holds it, so two never load
 * the same entries. Resolves `{ client, lost() }`: the connection holding it, and
 * whether that connection, and so the lock, has since been lost.
 */
async function takeLock(catalog, signal) {
  const client = new pg.Client({ ...catalog, connectionTimeoutMillis: 30_000 });
  let lost = false;
  client.on("error", (error) => {
    lost = true;
    log("lost the loader lock's connection", { error: error.message });
  });
  client.on("end", () => {
    lost = true;
  });
  try {
    await client.connect();
    for (let attempt = 0; ; attempt += 1) {
      const { rows } = await client.query("select pg_try_advisory_lock(hashtext($1)) as held", [LOCK]);
      if (rows[0].held) return { client, lost: () => lost };
      if (attempt % 6 === 0) log("another loader holds the lock; waiting");
      await sleep(LOCK_RETRY_MS, undefined, { signal });
    }
  } catch (error) {
    await client.end().catch(() => {});
    throw error;
  }
}

const config = loadConfig();

/** Opens the lake for loading, under the lock, its model made ready. */
async function open(signal) {
  const lock = await takeLock(config.catalog, signal);
  let lake;
  try {
    lake = await openLake(config);
    const rebuilt = await prepareLake(lake.db);
    log(rebuilt ? "lake built, to be loaded from the source" : "lake open");
  } catch (error) {
    lake?.close();
    await lock.client.end().catch(() => {});
    throw error;
  }
  return {
    db: lake.db,
    lost: lock.lost,
    async close() {
      lake.close();
      await lock.client.end().catch(() => {});
    },
  };
}

const metrics = createMetrics();
const loader = startLoader({
  open,
  metrics,
  log,
  intervalMs: config.intervalMs,
  maintenanceIntervalMs: config.maintenanceIntervalMs,
});

const server = createServer((request, response) => {
  if (request.method === "GET" && request.url === "/healthz") {
    const { ok, detail } = loader.health();
    response.writeHead(ok ? 200 : 503, { "content-type": "text/plain" }).end(`${detail}\n`);
    return;
  }
  if (request.method === "GET" && request.url === "/metrics") {
    response.writeHead(200, { "content-type": "text/plain; version=0.0.4" }).end(metrics.render());
    return;
  }
  response.writeHead(404).end();
});
server.listen(config.httpPort, "0.0.0.0", () => log("listening", { port: config.httpPort }));

// As PID 1 in its container, Node would otherwise ignore docker stop's SIGTERM. A
// load under way finishes first; being transactional, one cut short by the kill
// that follows the grace period loses nothing either.
process.on("SIGTERM", async () => {
  log("stopping");
  server.close();
  await loader.stop();
  process.exit(0);
});
