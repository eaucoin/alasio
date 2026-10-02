/**
 * A session's telemetry drain, which the session host runs beside bayma inside the
 * sandbox when alasio exports telemetry (sandbox/session-host/entrypoint.sh). bayma
 * exports OTLP over HTTP to it on the sandbox's own loopback, and it holds each request,
 * within a bound, until alasio reads them through `agent-connect`
 * (src/sandbox/telemetry.js). alasio then stamps them with their origin and exports them
 * where it exports its own. So nothing in the sandbox is given a route, an endpoint, or
 * a credential, and what bayma records while alasio is away waits here instead of being
 * lost.
 *
 * Everything in the sandbox is the agent's, this included: the agent can send to it,
 * read from it, or replace it, so alasio treats whatever it reads here as untrusted.
 *
 * The reader is sent frames, each a 6-byte head (the body's length as a big-endian u32,
 * the frame's kind, the body's encoding) and the body:
 *   kind 0, 1, 2  an OTLP request for traces, metrics, or logs; encoding 0 is binary
 *                 protobuf and 1 is JSON
 *   kind 3        how many requests were dropped since the last such frame (a u32)
 *   kind 4        the first frame of every read: the frame protocol's version (a u8)
 * A new reader replaces the last, and a frame written to a reader that then goes away
 * is lost with it.
 *
 *   node telemetry-drain.mjs [otlp-port [read-port [max-held-bytes]]]
 */
import { createServer as createHttpServer } from "node:http";
import { createServer } from "node:net";

const SIGNALS = ["traces", "metrics", "logs"];
const ENCODINGS = new Map([["application/x-protobuf", 0], ["application/json", 1]]);
const DROPPED = 3;
const HELLO = 4;
const VERSION = 1;
const MAX_REQUEST_BYTES = 4 * 1024 * 1024;

const [otlpPort = 4318, readPort = 7291, maxHeldBytes = 16 * 1024 * 1024] = process.argv.slice(2).map(Number);

const held = [];
let heldBytes = 0;
let dropped = 0;
let reader = null;

function frame(kind, encoding, body) {
  const head = Buffer.alloc(6);
  head.writeUInt32BE(body.length, 0);
  head[4] = kind;
  head[5] = encoding;
  return Buffer.concat([head, body]);
}

/** Holds a frame for the reader, dropping the oldest held beyond the bound. */
function hold(data) {
  held.push(data);
  heldBytes += data.length;
  while (heldBytes > maxHeldBytes) {
    heldBytes -= held.shift().length;
    dropped += 1;
  }
  flush();
}

/** Writes what is held to the reader, as far as it takes it without buffering. */
function flush() {
  if (!reader || reader.writableNeedDrain) return;
  if (dropped > 0) {
    const count = Buffer.alloc(4);
    count.writeUInt32BE(Math.min(dropped, 0xffffffff), 0);
    dropped = 0;
    if (!reader.write(frame(DROPPED, 0, count))) return;
  }
  while (held.length > 0) {
    const next = held.shift();
    heldBytes -= next.length;
    if (!reader.write(next)) return;
  }
}

function answer(res, status, type = "text/plain", body = "") {
  res.writeHead(status, { "content-type": type, "content-length": Buffer.byteLength(body) }).end(body);
}

const receiver = createHttpServer((req, res) => {
  const signal = req.method === "POST" && req.url.startsWith("/v1/") ? SIGNALS.indexOf(req.url.slice(4)) : -1;
  if (signal < 0) return answer(res, 404);
  const type = (req.headers["content-type"] ?? "").split(";")[0].trim().toLowerCase();
  const encoding = ENCODINGS.get(type);
  if (encoding === undefined || (req.headers["content-encoding"] ?? "identity") !== "identity") {
    return answer(res, 415);
  }
  // A request over the bound is read to its end and discarded, then refused: refused
  // before then, its sender would mostly see the connection reset, not the refusal.
  const chunks = [];
  let size = 0;
  req.on("data", (chunk) => {
    size += chunk.length;
    if (size <= MAX_REQUEST_BYTES) chunks.push(chunk);
    else chunks.length = 0;
  });
  req.on("end", () => {
    if (size > MAX_REQUEST_BYTES) {
      res.setHeader("connection", "close");
      return answer(res, 413);
    }
    hold(frame(signal, encoding, Buffer.concat(chunks)));
    // An empty Export*ServiceResponse: full success.
    answer(res, 200, type, encoding === 1 ? "{}" : "");
  });
  req.on("error", () => {});
});

const readers = createServer((socket) => {
  reader?.destroy();
  reader = socket;
  socket.on("drain", flush);
  socket.on("error", () => {});
  socket.on("close", () => {
    if (reader === socket) reader = null;
  });
  socket.resume();
  socket.write(frame(HELLO, 0, Buffer.from([VERSION])));
  flush();
});

function listen(server, port) {
  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, "127.0.0.1", () => resolve(server.address().port));
  });
}

try {
  const ports = await Promise.all([listen(receiver, otlpPort), listen(readers, readPort)]);
  console.log(`telemetry-drain listening ${ports.join(" ")}`);
} catch (error) {
  console.error(`telemetry-drain: ${error.message}`);
  process.exit(1);
}
