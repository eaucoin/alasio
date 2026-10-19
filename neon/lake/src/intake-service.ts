/**
 * The telemetry intake's service: a replica of `lake-intake` (cli/src/manifests/lake.ts),
 * which KEDA scales on the lag of its consumer group, consuming the log's telemetry
 * topics into the lake (./intake.ts). Its writes take turns with every other writer's of
 * the lake under its write lock (./lake.ts). It serves /healthz (its loop is not wedged, or what it is wedged on),
 * /readyz (it holds the lake and its member of the group) and /metrics (Prometheus, for
 * the stack's collector) on LAKE_HTTP_PORT; logs a JSON line per event, and exports each
 * as telemetry too (./telemetry.ts).
 */
import { createServer } from "node:http";
import { hostname } from "node:os";

import pg from "pg";

import { loadIntakeConfig } from "./config.ts";
import { DUCKDB_MEMORY, type IntakeLake, startIntake, TOPICS } from "./intake.ts";
import { CONNECT_TIMEOUT_MS, type Lake, openLake, writeLock } from "./lake.ts";
import { joinLog } from "./log.ts";
import { createMetrics, sampleMemory } from "./metrics.ts";
import { ensureOtel } from "./otel.ts";
import { grantReads } from "./reader.ts";
import { startTelemetry } from "./telemetry.ts";

/** The consumer group every replica is a member of. */
export const GROUP = "lake-intake";
const telemetry = startTelemetry();

function log(message: string, fields: Record<string, unknown> = {}): void {
  console.log(JSON.stringify({ time: new Date().toISOString(), message, ...fields }));
  telemetry?.emit(message, fields);
}

const config = loadIntakeConfig();
writeLock(config.catalog);

const opened: Record<"intake", Lake | null> = { intake: null };

/**
 * Opens the lake for the intake: the telemetry's schema made ready, and the catalog,
 * which the first open of all makes, readable by the query endpoint's reader.
 */
async function open(): Promise<IntakeLake> {
  const lake = await openLake(config, { memoryLimit: DUCKDB_MEMORY });
  const catalog = new pg.Client({ ...config.catalog, connectionTimeoutMillis: CONNECT_TIMEOUT_MS });
  try {
    await ensureOtel(lake.db);
    await catalog.connect();
    await grantReads(catalog);
  } catch (error) {
    lake.close();
    throw error;
  } finally {
    await catalog.end().catch(() => {});
  }
  opened.intake = lake;
  return {
    db: lake.db,
    close() {
      opened.intake = null;
      lake.close();
    },
  };
}

const metrics = createMetrics();
const intake = startIntake({ open, join: () => joinLog(config.log, GROUP, Object.keys(TOPICS), hostname()), metrics, log, logRetentionMs: config.logRetentionMs });

const server = createServer((request, response) => {
  if (request.method === "GET" && request.url === "/healthz") {
    const { ok, detail } = intake.health();
    response.writeHead(ok ? 200 : 503, { "content-type": "text/plain" }).end(`intake: ${detail}\n`);
    return;
  }
  if (request.method === "GET" && request.url === "/readyz") {
    const ready = intake.ready();
    response.writeHead(ready ? 200 : 503, { "content-type": "text/plain" }).end(`intake: ${ready ? "consuming" : "opening the lake and joining the group"}\n`);
    return;
  }
  if (request.method === "GET" && request.url === "/metrics") {
    sampleMemory(metrics, opened).then(
      () => response.writeHead(200, { "content-type": "text/plain; version=0.0.4" }).end(metrics.render()),
      (error: unknown) => response.writeHead(500, { "content-type": "text/plain" }).end(`${error instanceof Error ? error.message : String(error)}\n`),
    );
    return;
  }
  response.writeHead(404).end();
});
server.listen(config.httpPort, "0.0.0.0", () => log("listening", { port: config.httpPort }));

// As PID 1 in its container, Node would otherwise ignore SIGTERM. A batch being written
// finishes first; one being gathered is left to be read again, by this replica's successor
// or another member of the group.
process.on("SIGTERM", async () => {
  log("stopping");
  server.close();
  await intake.stop();
  await telemetry?.shutdown();
  process.exit(0);
});
