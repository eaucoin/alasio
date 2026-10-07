/**
 * The telemetry intake: OTLP over HTTP, binary protobuf or JSON, gzipped or not, which
 * the stack's collector exports everything it receives to (cli/src/manifests/
 * collector.ts). Each request is decoded (./otlp.ts) into the `otel` schema's rows
 * (./otel.ts) and written in one transaction; the collector batches, so the intake
 * keeps nothing back. It is answered as OTLP's exporters expect: 200 once written, 400
 * for a request that does not decode, which is not sent again, and 503 while the lake
 * cannot be written, which is.
 *
 * The intake holds the lake open on a DuckDB of its own, apart from the loader's, from
 * its start: it opens it until that succeeds, and opens it anew whenever a write fails,
 * so it rides out the compute restarting as the loader does, and is ready whatever
 * becomes of the loader. Its requests are written one at a time, on its one
 * connection, and now and then what it wrote inlined in the catalog is flushed to
 * Parquet.
 */
import type { IncomingMessage, ServerResponse } from "node:http";
import { promisify } from "node:util";
import { gunzip } from "node:zlib";

import type { DuckDBConnection } from "@duckdb/node-api";

import { queue } from "./lake.ts";
import type { Log } from "./loader.ts";
import type { Metrics } from "./metrics.ts";
import { flushTelemetry, telemetryRows, writeTelemetry } from "./otel.ts";
import { decodeRequest, type Encoding, MalformedRequest, SIGNALS } from "./otlp.ts";

const gunzipAsync = promisify(gunzip);

/** The lake as the intake holds it, the `otel` schema ready. */
export interface IntakeLake {
  db: DuckDBConnection;
  close(): void;
}

export interface IntakeOptions {
  open: () => Promise<IntakeLake>;
  metrics: Pick<Metrics, "add">;
  log: Log;
  /** How soon an open that failed is tried again. */
  retryMs?: number;
  /** How often what is inlined is flushed to Parquet. */
  flushIntervalMs?: number;
}

export interface Intake {
  /** Answers an HTTP request. */
  handle(request: IncomingMessage, response: ServerResponse): void;
  /** Whether the lake is open to be written. */
  ready(): boolean;
  /** Ends the loop, once a write under way has ended, and closes the lake. */
  stop(): Promise<void>;
}

/** The most a request's body may be, sent or unzipped: well past the collector's batches. */
export const MAX_BODY_BYTES = 16 * 1024 * 1024;
const RETRY_MS = 10_000;
/** How long the catalog holds the telemetry inlined in it, at most. */
const FLUSH_INTERVAL_MS = 3_600_000;

const MEDIA_TYPES: Readonly<Record<string, Encoding>> = { "application/x-protobuf": "protobuf", "application/json": "json" };

const errorText = (error: unknown) => (error instanceof Error ? error.message : String(error));

/** A request's body, read whole, or null past `limit`. */
async function readBody(request: IncomingMessage, limit: number): Promise<Buffer | null> {
  const chunks: Buffer[] = [];
  let size = 0;
  // With no encoding set, a request reads as Buffers.
  for await (const chunk of request as AsyncIterable<Buffer>) {
    size += chunk.length;
    if (size <= limit) chunks.push(chunk);
  }
  return size > limit ? null : Buffer.concat(chunks);
}

/** A gzipped body, unzipped; one that does not unzip, or unzips past MAX_BODY_BYTES, is malformed. */
async function unzip(body: Buffer): Promise<Buffer> {
  try {
    return await gunzipAsync(body, { maxOutputLength: MAX_BODY_BYTES });
  } catch (error) {
    throw new MalformedRequest(`the body does not unzip: ${errorText(error)}`);
  }
}

/** Starts the intake: it opens the lake now, and keeps it open until stopped. */
export function startIntake({ open, metrics, log, retryMs = RETRY_MS, flushIntervalMs = FLUSH_INTERVAL_MS }: IntakeOptions): Intake {
  const stopped = new AbortController();
  const writing = queue();
  let lake: IntakeLake | null = null;
  let wake: (() => void) | null = null;

  const sleep = (ms: number) =>
    new Promise<void>((resolve) => {
      const timer = setTimeout(resolve, ms);
      wake = () => {
        clearTimeout(timer);
        resolve();
      };
    });

  /** Closes the lake after `error`, so the loop opens it anew. */
  function drop(error: unknown) {
    if (!lake) return;
    log("telemetry intake lost the lake", { error: errorText(error) });
    lake.close();
    lake = null;
    wake?.();
  }

  // Opens the lake whenever it is not open, and flushes it while it is.
  const done = (async () => {
    while (!stopped.signal.aborted) {
      if (!lake) {
        try {
          lake = await open();
          log("telemetry intake open");
        } catch (error) {
          log("telemetry intake could not open the lake", { error: errorText(error) });
          await sleep(retryMs);
          continue;
        }
      }
      await sleep(flushIntervalMs);
      const held = lake;
      if (stopped.signal.aborted || !held || held !== lake) continue;
      await writing(() => flushTelemetry(held.db)).catch(drop);
    }
    await writing(async () => lake?.close());
    lake = null;
  })();

  async function take(signal: (typeof SIGNALS)[number], encoding: Encoding, body: Buffer): Promise<number> {
    let rows;
    try {
      rows = telemetryRows(signal, decodeRequest(signal, encoding, body));
    } catch (error) {
      metrics.add("lake_telemetry_requests_total", { signal, outcome: "malformed" });
      throw error;
    }
    return writing(async () => {
      const held = lake;
      if (!held) return 503;
      try {
        const written = await writeTelemetry(held.db, rows);
        for (const [table, count] of Object.entries(written)) metrics.add("lake_telemetry_rows_total", { table: `otel.${table}` }, count);
        metrics.add("lake_telemetry_requests_total", { signal, outcome: "written" });
        return 200;
      } catch (error) {
        metrics.add("lake_telemetry_requests_total", { signal, outcome: "failed" });
        drop(error);
        return 503;
      }
    });
  }

  async function answer(request: IncomingMessage, response: ServerResponse): Promise<void> {
    const signal = SIGNALS.find((name) => request.url === `/v1/${name}`);
    if (request.method !== "POST" || !signal) {
      response.writeHead(404).end();
      return;
    }
    const mediaType = (request.headers["content-type"] ?? "").split(";")[0]?.trim() ?? "";
    const encoding = MEDIA_TYPES[mediaType];
    const contentEncoding = request.headers["content-encoding"] ?? "identity";
    if (!encoding || (contentEncoding !== "gzip" && contentEncoding !== "identity")) {
      response.writeHead(415).end();
      return;
    }
    const sent = await readBody(request, MAX_BODY_BYTES);
    if (!sent) {
      response.writeHead(413).end();
      return;
    }
    let status: number;
    try {
      status = await take(signal, encoding, contentEncoding === "gzip" ? await unzip(sent) : sent);
    } catch (error) {
      if (!(error instanceof MalformedRequest)) throw error;
      response.writeHead(400, { "content-type": "text/plain" }).end(`${error.message}\n`);
      return;
    }
    if (status !== 200) {
      response.writeHead(status).end();
      return;
    }
    // A full success, in the request's own encoding: an empty message, or an empty object.
    response.writeHead(200, { "content-type": mediaType }).end(encoding === "json" ? "{}" : "");
  }

  return {
    handle(request, response) {
      answer(request, response).catch((error: unknown) => {
        log("telemetry intake failed a request", { error: errorText(error) });
        if (!response.headersSent) response.writeHead(500);
        response.end();
      });
    },
    ready: () => lake !== null,
    async stop() {
      stopped.abort();
      wake?.();
      await done;
    },
  };
}
