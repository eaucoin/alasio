/**
 * A stand-in OTLP/HTTP endpoint for alasio's end-to-end tests: it accepts every export
 * and remembers, per request, its signal, its encoding and whether its bytes name a
 * given value, so a test can see what reached the deployment's telemetry backend and
 * how it was stamped.
 *
 *   POST /v1/{traces,metrics,logs}        an export; answered as a full success
 *   GET  /control/exports?contains=<s>    the exports so far, each with whether it
 *                                         contains <s>
 *
 * Dependency-free: `node otlp-sink.ts [port]`.
 */
import { createServer } from "node:http";
import { gunzipSync } from "node:zlib";
import type { Signal } from "../../src/telemetry/config.ts";

/** An export the sink received. */
interface ReceivedExport {
  readonly signal: Signal;
  readonly type: string | undefined;
  readonly bytes: Buffer;
  readonly at: number;
}

/** An export as /control/exports lists it: whether it contains the asked-for value, when one was. */
export interface ListedExport {
  readonly signal: Signal;
  readonly type: string | undefined;
  readonly size: number;
  readonly at: number;
  readonly contains?: boolean;
}

const PORT = Number(process.argv[2] ?? 4318);
const exports: ReceivedExport[] = [];

createServer(async (request, response) => {
  // A server's requests always carry their URL.
  const url = new URL(request.url!, "http://sink");
  const chunks: Buffer[] = [];
  // With no encoding set, a request reads as Buffers.
  for await (const chunk of request as AsyncIterable<Buffer>) chunks.push(chunk);
  let body = Buffer.concat(chunks);
  if (request.headers["content-encoding"] === "gzip") body = gunzipSync(body);
  // The pattern names a Signal alone.
  const signal = /^\/v1\/(traces|metrics|logs)$/u.exec(url.pathname)?.[1] as Signal | undefined;
  if (signal && request.method === "POST") {
    exports.push({ signal, type: request.headers["content-type"], bytes: body, at: Date.now() });
    // A full success in the request's encoding: an empty message, or an empty object.
    const json = String(request.headers["content-type"]).startsWith("application/json");
    response.writeHead(200, { "content-type": json ? "application/json" : "application/x-protobuf" }).end(json ? "{}" : "");
    return;
  }
  if (url.pathname === "/control/exports") {
    const needle = url.searchParams.get("contains");
    const listed = exports.map(({ signal, type, bytes, at }): ListedExport => ({
      signal, type, size: bytes.length, at, ...(needle ? { contains: bytes.includes(needle) } : {}),
    }));
    response.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify(listed));
    return;
  }
  response.writeHead(404).end();
}).listen(PORT, "0.0.0.0", () => console.log(`otlp sink on ${PORT}`));
