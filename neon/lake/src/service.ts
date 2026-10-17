/**
 * The loader's service: a replica of `lake-loader` (cli/src/manifests/lake.ts), which
 * keeps the analytics lake loaded from alasio's Neon (model.ts) while alasio says it wrote
 * transcripts, and sleeps otherwise (loader.ts). One load runs at a time, its or a
 * reconciliation's, which a Postgres advisory lock on the catalog makes sure of, taken for
 * each, and its writes take turns with every other writer's of the lake under its write
 * lock (lake.ts); DuckDB keeps nothing of its own, so
 * the container is replaceable at any moment, and the loader opens its connections again
 * whenever they fail, as when the compute restarts.
 *
 * It serves /healthz (its liveness: loads are not failing for long), /readyz (it holds the
 * lake open, and its member of the group) and /metrics (Prometheus, for the
 * stack's telemetry collector) on LAKE_HTTP_PORT. It logs a JSON line per event, and
 * exports each as telemetry too (telemetry.ts). The lake is maintained by maintain.ts, and
 * read through the query endpoint (endpoint.ts), each a process of its own.
 */
import { createServer } from "node:http";
import { hostname } from "node:os";

import { loadConfig } from "./config.ts";
import { type Lake, writeLock } from "./lake.ts";
import { type LoaderLake, openLoaderLake, startLoader } from "./loader.ts";
import { joinLog } from "./log.ts";
import { createMetrics, sampleMemory } from "./metrics.ts";
import { startTelemetry } from "./telemetry.ts";

const telemetry = startTelemetry();

function log(message: string, fields: Record<string, unknown> = {}): void {
  console.log(JSON.stringify({ time: new Date().toISOString(), message, ...fields }));
  telemetry?.emit(message, fields);
}

const config = loadConfig();
writeLock(config.catalog);

/** The lake as the loader has it open now, whose DuckDB's memory /metrics reports. */
const opened: Record<"loader", Lake | null> = { loader: null };

/** The lake opened for loading (loader.ts's openLoaderLake), whose DuckDB /metrics reports while it is open. */
async function open(signal: AbortSignal): Promise<LoaderLake> {
  const held = await openLoaderLake(config, log, signal);
  opened.loader = held.lake;
  return {
    ...held,
    async close() {
      opened.loader = null;
      await held.close();
    },
  };
}

const metrics = createMetrics();
const loader = startLoader({ open, join: () => joinLog(config.log, config.wake.group, [config.wake.topic], hostname()), metrics, log });

const server = createServer((request, response) => {
  if (request.method === "GET" && (request.url === "/healthz" || request.url === "/readyz")) {
    const { ok, detail } = loader.health();
    const well = request.url === "/healthz" ? ok : ok && loader.ready();
    response.writeHead(well ? 200 : 503, { "content-type": "text/plain" }).end(`loader: ${detail}\n`);
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

// As PID 1 in its container, Node would otherwise ignore docker stop's SIGTERM. A
// load under way finishes first; being transactional, one cut short by the kill that
// follows the grace period loses nothing either.
process.on("SIGTERM", async () => {
  log("stopping");
  server.close();
  await loader.stop();
  await telemetry?.shutdown();
  process.exit(0);
});
