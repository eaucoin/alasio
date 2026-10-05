/**
 * The lake service: the stack's container that keeps the analytics lake loaded from
 * alasio's Neon (model.ts). One loads at a time, which a Postgres advisory
 * lock on the catalog makes sure of; DuckDB keeps nothing of its own, so the
 * container is replaceable at any moment, and the loader opens its connections again
 * whenever they fail, as when the compute restarts.
 *
 * It serves /healthz (its pod's liveness: loads are not failing for long), /readyz
 * (its readiness: a load has succeeded, so the lake exists to be queried) and /metrics
 * (Prometheus, for the stack's telemetry collector) on LAKE_HTTP_PORT from the moment
 * it starts, and logs a JSON line per event.
 */
import { createServer } from "node:http";
import { setTimeout as sleep } from "node:timers/promises";

import pg from "pg";

import { type DatabaseConfig, loadConfig } from "./config.ts";
import { type Lake, openLake } from "./lake.ts";
import { type LoaderLake, startLoader } from "./loader.ts";
import { createMetrics } from "./metrics.ts";
import { prepareLake } from "./sync.ts";

/** Names the one loader's advisory lock, in the catalog database. */
const LOCK = "alasio.lake.loader";
const LOCK_RETRY_MS = 10_000;

/** The loader's lock: the connection holding it, and whether that has since been lost. */
interface LoaderLock {
  client: pg.Client;
  lost: () => boolean;
}

function log(message: string, fields: Record<string, unknown> = {}): void {
  console.log(JSON.stringify({ time: new Date().toISOString(), message, ...fields }));
}

/**
 * Takes the loader's lock, waiting while another loader holds it, so two never load
 * the same entries. Resolves `{ client, lost() }`: the connection holding it, and
 * whether that connection, and so the lock, has since been lost.
 */
async function takeLock(catalog: DatabaseConfig, signal: AbortSignal): Promise<LoaderLock> {
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
      const { rows } = await client.query<{ held: boolean }>("select pg_try_advisory_lock(hashtext($1)) as held", [LOCK]);
      // A select of a function is one row.
      if (rows[0]!.held) return { client, lost: () => lost };
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
async function open(signal: AbortSignal): Promise<LoaderLake> {
  const lock = await takeLock(config.catalog, signal);
  let lake: Lake | undefined;
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
  if (request.method === "GET" && (request.url === "/healthz" || request.url === "/readyz")) {
    const { ok, ready, detail } = loader.health();
    const well = request.url === "/healthz" ? ok : ok && ready;
    response.writeHead(well ? 200 : 503, { "content-type": "text/plain" }).end(`${detail}\n`);
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
