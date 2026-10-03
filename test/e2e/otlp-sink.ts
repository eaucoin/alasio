// @ts-nocheck
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

const PORT = Number(process.argv[2] ?? 4318);
const exports = [];

createServer(async (request, response) => {
  const url = new URL(request.url, "http://sink");
  const chunks = [];
  for await (const chunk of request) chunks.push(chunk);
  let body = Buffer.concat(chunks);
  if (request.headers["content-encoding"] === "gzip") body = gunzipSync(body);
  const signal = /^\/v1\/(traces|metrics|logs)$/u.exec(url.pathname)?.[1];
  if (signal && request.method === "POST") {
    exports.push({ signal, type: request.headers["content-type"], bytes: body, at: Date.now() });
    // A full success in the request's encoding: an empty message, or an empty object.
    const json = String(request.headers["content-type"]).startsWith("application/json");
    response.writeHead(200, { "content-type": json ? "application/json" : "application/x-protobuf" }).end(json ? "{}" : "");
    return;
  }
  if (url.pathname === "/control/exports") {
    const needle = url.searchParams.get("contains");
    const listed = exports.map(({ signal, type, bytes, at }) => ({
      signal, type, size: bytes.length, at, ...(needle ? { contains: bytes.includes(needle) } : {}),
    }));
    response.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify(listed));
    return;
  }
  response.writeHead(404).end();
}).listen(PORT, "0.0.0.0", () => console.log(`otlp sink on ${PORT}`));
