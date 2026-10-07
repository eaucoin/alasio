/**
 * The lake's query endpoint: the lake as its reader reads it (./reader.ts), over HTTP,
 * for Grafana's dashboards and alert rules and for `alasio lake` (./query.ts). It runs
 * on the lake's image in a container of its own beside the lake service's, so a heavy
 * query is bounded by its own DuckDB's memory limit and its own container's, away from
 * the loader and the intake.
 *
 *   POST /query    a query in DuckDB's SQL, the request's body, with
 *                  `Authorization: Bearer <LAKE_QUERY_TOKEN>`: 200 with its rows, a JSON
 *                  array of objects; 400 with why it was refused or failed; 401 without
 *                  the token; 503 while the lake cannot be opened
 *   GET  /healthz  200 while it serves: its liveness, and its readiness, so its pod is
 *                  ready for the intake whether or not the lake can be read
 *
 * It opens the lake at the first query that finds it closed, and keeps it open: DuckDB
 * connects to the catalog anew by itself when the compute drops its connections, as it
 * does as it restarts. Queries run one at a time, on its one connection.
 */
import { timingSafeEqual } from "node:crypto";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";

import { loadEndpointConfig } from "./config.ts";
import { type Lake, queue } from "./lake.ts";
import type { Log } from "./loader.ts";
import { answer, openReader, QueryRefused } from "./reader.ts";
import { startTelemetry } from "./telemetry.ts";

/** The most a query's text may be. */
const MAX_QUERY_BYTES = 1024 * 1024;

export interface EndpointOptions {
  /** Opens the lake as its reader. */
  open: () => Promise<Lake>;
  /** The bearer token a query must carry. */
  token: string;
  log: Log;
}

export interface Endpoint {
  /** Answers an HTTP request. */
  handle(request: IncomingMessage, response: ServerResponse): void;
  /** Closes the lake, once a query under way has ended. */
  close(): Promise<void>;
}

const errorText = (error: unknown) => (error instanceof Error ? error.message : String(error));

/** Whether `header` is `Bearer <token>`, compared in constant time. */
function carries(header: string | undefined, token: string): boolean {
  const given = Buffer.from(header ?? "");
  const expected = Buffer.from(`Bearer ${token}`);
  return given.length === expected.length && timingSafeEqual(given, expected);
}

/** A request's body, as text, or null past `limit` bytes. */
async function readText(request: IncomingMessage, limit: number): Promise<string | null> {
  const chunks: Buffer[] = [];
  let size = 0;
  // With no encoding set, a request reads as Buffers.
  for await (const chunk of request as AsyncIterable<Buffer>) {
    size += chunk.length;
    if (size <= limit) chunks.push(chunk);
  }
  return size > limit ? null : Buffer.concat(chunks).toString("utf8");
}

export function startEndpoint({ open, token, log }: EndpointOptions): Endpoint {
  const reading = queue();
  let lake: Lake | null = null;

  /** The rows of `sql`, as an HTTP status and body. */
  const query = (sql: string) =>
    reading(async (): Promise<[number, string]> => {
      if (!lake) {
        try {
          lake = await open();
          log("query endpoint opened the lake");
        } catch (error) {
          log("query endpoint could not open the lake", { error: errorText(error) });
          return [503, `the lake cannot be read now: ${errorText(error)}`];
        }
      }
      try {
        return [200, JSON.stringify(await answer(lake.db, sql))];
      } catch (error) {
        return [error instanceof QueryRefused ? 400 : 500, errorText(error)];
      }
    });

  async function respond(request: IncomingMessage, response: ServerResponse): Promise<void> {
    if (request.method === "GET" && request.url === "/healthz") {
      response.writeHead(200, { "content-type": "text/plain" }).end("serving\n");
      return;
    }
    if (request.method !== "POST" || request.url !== "/query") {
      response.writeHead(404).end();
      return;
    }
    if (!carries(request.headers.authorization, token)) {
      response.writeHead(401, { "content-type": "text/plain" }).end("a query carries the endpoint's bearer token\n");
      return;
    }
    const sql = await readText(request, MAX_QUERY_BYTES);
    if (sql === null) {
      response.writeHead(413).end();
      return;
    }
    const [status, body] = await query(sql);
    response.writeHead(status, { "content-type": status === 200 ? "application/json" : "text/plain" }).end(status === 200 ? body : `${body}\n`);
  }

  return {
    handle(request, response) {
      respond(request, response).catch((error: unknown) => {
        log("query endpoint failed a request", { error: errorText(error) });
        if (!response.headersSent) response.writeHead(500);
        response.end();
      });
    },
    close: () =>
      reading(async () => {
        lake?.close();
        lake = null;
      }),
  };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const config = loadEndpointConfig();
  const telemetry = startTelemetry();
  const log: Log = (message, fields = {}) => {
    console.log(JSON.stringify({ time: new Date().toISOString(), message, ...fields }));
    telemetry?.emit(message, fields);
  };
  const endpoint = startEndpoint({ open: () => openReader(config), token: config.token, log });
  const server = createServer(endpoint.handle);
  server.listen(config.port, "0.0.0.0", () => log("taking queries", { port: config.port }));
  // As PID 1 in its container, Node would otherwise ignore docker stop's SIGTERM.
  process.on("SIGTERM", async () => {
    log("stopping");
    server.close();
    await endpoint.close();
    await telemetry?.shutdown();
    process.exit(0);
  });
}
