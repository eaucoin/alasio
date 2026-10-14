/**
 * The lake's query endpoint: the lake as its reader reads it (./reader.ts), over HTTP,
 * for Grafana's dashboards and alert rules and for `alasio lake` (./query.ts). It runs
 * on the lake's image in a container of its own beside the lake service's, so a heavy
 * query is bounded by its own DuckDB's memory limit and its own container's, away from
 * the loader.
 *
 *   POST /query    a query in DuckDB's SQL, the request's body, with
 *                  `Authorization: Bearer <LAKE_QUERY_TOKEN>`: 200 with its rows, a JSON
 *                  array of objects; 400 with why it was refused or failed; 401 without
 *                  the token; 503 while the lake cannot be opened. An error is said in
 *                  the body, and in the status line's reason as well, on one line: that
 *                  is all Grafana's Infinity shows of an error, as it reads no body of a
 *                  status of 400 or more; 503 too where it is not answered within
 *                  answerWithinMs, as a query before it holds the lake
 *   GET  /healthz  200 while it serves: its liveness, and its readiness, so its pod is
 *                  ready for the loader whether or not the lake can be read; 503 once
 *                  a query has held the lake past wedgedMs, so its container is started
 *                  again
 *
 * It opens the lake at the first query that finds it closed, and keeps it open: DuckDB
 * connects to the catalog anew by itself when the compute drops its connections, as it
 * does as it restarts. Queries run one at a time, on its one connection. A query is
 * bounded by the reader's QUERY_TIMEOUT_MS, which interrupts DuckDB, and by its role's
 * limits in the catalog's database (src/neon/lake.ts's ensureLakeReaderRole); but a
 * statement DuckDB has sent Postgres it cannot interrupt, so the endpoint bounds its own
 * answer too, and says when its one connection is held for good.
 */
import { timingSafeEqual } from "node:crypto";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";

import { loadEndpointConfig } from "./config.ts";
import { type Lake, queue } from "./lake.ts";
import type { Log } from "./loader.ts";
import { answer, openReader, QUERY_TIMEOUT_MS, QueryRefused } from "./reader.ts";
import { startTelemetry } from "./telemetry.ts";

/** The most a query's text may be. */
const MAX_QUERY_BYTES = 1024 * 1024;

/** How long a query is answered within, whatever holds the lake: past the reader's own bound. */
export const ANSWER_WITHIN_MS = QUERY_TIMEOUT_MS + 15_000;

/** How long a query may hold the lake before the endpoint is not well. */
const WEDGED_MS = 2 * ANSWER_WITHIN_MS;

export interface EndpointOptions {
  /** Opens the lake as its reader. */
  open: () => Promise<Lake>;
  /** The bearer token a query must carry. */
  token: string;
  log: Log;
  /** How long a query is answered within: ANSWER_WITHIN_MS unless given. */
  answerWithinMs?: number;
  /** How long a query may hold the lake before /healthz fails: WEDGED_MS unless given. */
  wedgedMs?: number;
}

export interface Endpoint {
  /** Answers an HTTP request. */
  handle(request: IncomingMessage, response: ServerResponse): void;
  /** Closes the lake, once a query under way has ended. */
  close(): Promise<void>;
}

const errorText = (error: unknown) => (error instanceof Error ? error.message : String(error));

/**
 * Answers `status` with `message`: in the body, and as the status line's reason,
 * which may hold printable ASCII alone, on one line, and is kept short.
 */
function refuse(response: ServerResponse, status: number, message: string): void {
  const reason = message.replace(/[^\x20-\x7e]+/gu, " ").trim().slice(0, 1000);
  response.writeHead(status, reason, { "content-type": "text/plain" }).end(`${message}\n`);
}

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

export function startEndpoint({ open, token, log, answerWithinMs = ANSWER_WITHIN_MS, wedgedMs = WEDGED_MS }: EndpointOptions): Endpoint {
  const reading = queue();
  let lake: Lake | null = null;
  /** The query that holds the lake, and since when. */
  let holding: { readonly sql: string; readonly since: number } | null = null;
  let saidWedged = false;

  /** What holds the lake, where it has held it past `ms`: for a message. */
  const heldPast = (ms: number) => {
    const held = holding;
    if (!held || Date.now() - held.since < ms) return null;
    return `a query has held the lake for ${Math.round((Date.now() - held.since) / 1000)} s: ${held.sql.slice(0, 200)}`;
  };

  /** The rows of `sql` on the lake, opening it where it is closed, as an HTTP status and body. */
  async function rows(sql: string): Promise<[number, string]> {
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
  }

  /**
   * The rows of `sql`, as an HTTP status and body, once the queries before it have had the
   * lake: 503 where they are not had within answerWithinMs, and then not read at all where
   * the lake was not the query's yet.
   */
  async function query(sql: string): Promise<[number, string]> {
    let givenUp = false;
    let timer: NodeJS.Timeout | undefined;
    const late = new Promise<[number, string]>((resolve) => {
      timer = setTimeout(() => {
        givenUp = true;
        const held = heldPast(0);
        resolve([503, `the lake answered nothing within ${Math.round(answerWithinMs / 1000)} s${held ? `, as ${held}` : ""}`]);
      }, answerWithinMs);
    });
    const read = reading(async (): Promise<[number, string]> => {
      if (givenUp) return [503, "given up"];
      holding = { sql, since: Date.now() };
      try {
        return await rows(sql);
      } finally {
        holding = null;
        saidWedged = false;
      }
    });
    try {
      return await Promise.race([read, late]);
    } finally {
      clearTimeout(timer);
    }
  }

  async function respond(request: IncomingMessage, response: ServerResponse): Promise<void> {
    if (request.method === "GET" && request.url === "/healthz") {
      const wedged = heldPast(wedgedMs);
      if (wedged) {
        if (!saidWedged) log("query endpoint wedged", { held: wedged });
        saidWedged = true;
        refuse(response, 503, wedged);
        return;
      }
      response.writeHead(200, { "content-type": "text/plain" }).end("serving\n");
      return;
    }
    if (request.method !== "POST" || request.url !== "/query") {
      response.writeHead(404).end();
      return;
    }
    if (!carries(request.headers.authorization, token)) {
      refuse(response, 401, "a query carries the endpoint's bearer token");
      return;
    }
    const sql = await readText(request, MAX_QUERY_BYTES);
    if (sql === null) {
      response.writeHead(413).end();
      return;
    }
    const [status, body] = await query(sql);
    if (status === 200) response.writeHead(200, { "content-type": "application/json" }).end(body);
    else refuse(response, status, body);
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
