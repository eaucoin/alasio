import assert from "node:assert/strict";
import { createServer, type IncomingHttpHeaders, type OutgoingHttpHeaders, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { after, before, test } from "node:test";
import { gunzipSync } from "node:zlib";

import { createOtlpForwarder, retryAfterMs } from "../src/telemetry/forward.ts";

// The forwarder sends what alasio relays as OpenTelemetry's exporters send alasio's own:
// to each signal's endpoint, with its headers and compression, retrying what OTLP says
// to retry. A stand-in OTLP server answers each request with the next queued status.

/** A request as the server received it. */
interface ReceivedRequest {
  readonly url: string | undefined;
  readonly headers: IncomingHttpHeaders;
  readonly body: Buffer;
}

/** An answer the server is to give: a status, and any headers. */
type QueuedStatus = readonly [status: number, headers?: OutgoingHttpHeaders];

let server: Server;
let base: string;
const received: ReceivedRequest[] = [];
const statuses: QueuedStatus[] = [];

before(async () => {
  server = createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on("data", (chunk: Buffer) => chunks.push(chunk));
    req.on("end", () => {
      received.push({ url: req.url, headers: req.headers, body: Buffer.concat(chunks) });
      const [status, headers = {}] = statuses.shift() ?? [200];
      res.writeHead(status, headers).end();
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  // Listening on a TCP port, the server's address is an AddressInfo.
  base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
});
after(() => new Promise((resolve) => server.close(resolve)));

function fresh() {
  received.length = 0;
  statuses.length = 0;
}

test("each signal goes to its endpoint with its headers and compression, in the encoding it arrived in", async () => {
  fresh();
  const forwarder = createOtlpForwarder({
    OTEL_EXPORTER_OTLP_ENDPOINT: base,
    OTEL_EXPORTER_OTLP_HEADERS: "authorization=Bearer%20secret,x-tenant=a",
    OTEL_EXPORTER_OTLP_LOGS_ENDPOINT: `${base}/custom/logs`,
    OTEL_EXPORTER_OTLP_LOGS_PROTOCOL: "http/json",
    OTEL_EXPORTER_OTLP_METRICS_COMPRESSION: "gzip",
  });
  try {
    assert.deepEqual(forwarder.protocols, { traces: "http/protobuf", metrics: "http/protobuf", logs: "http/json" });
    assert.deepEqual(await forwarder.export("traces", "protobuf", Buffer.from([1, 2, 3])), { ok: true });
    assert.deepEqual(await forwarder.export("metrics", "protobuf", Buffer.from([4, 5])), { ok: true });
    assert.deepEqual(await forwarder.export("logs", "json", Buffer.from("{}")), { ok: true });
  } finally {
    forwarder.close();
  }
  const [traces, metrics, logs] = received;
  assert.ok(traces && metrics && logs);
  assert.equal(traces.url, "/v1/traces");
  assert.equal(traces.headers["content-type"], "application/x-protobuf");
  assert.equal(traces.headers.authorization, "Bearer secret");
  assert.equal(traces.headers["x-tenant"], "a");
  assert.deepEqual([...traces.body], [1, 2, 3]);
  assert.equal(metrics.url, "/v1/metrics");
  assert.equal(metrics.headers["content-encoding"], "gzip");
  assert.deepEqual([...gunzipSync(metrics.body)], [4, 5]);
  assert.equal(logs.url, "/custom/logs");
  assert.equal(logs.headers["content-type"], "application/json");
  assert.equal(logs.body.toString(), "{}");
});

test("a retryable answer is retried after the server's Retry-After, and any other failure is not", async () => {
  fresh();
  const forwarder = createOtlpForwarder({ OTEL_EXPORTER_OTLP_ENDPOINT: base });
  try {
    statuses.push([503, { "retry-after": "0" }], [429, { "retry-after": "0" }]);
    assert.deepEqual(await forwarder.export("traces", "protobuf", Buffer.from([1])), { ok: true });
    assert.equal(received.length, 3);

    fresh();
    statuses.push([400]);
    assert.deepEqual(await forwarder.export("traces", "protobuf", Buffer.from([1])), { ok: false, error: "HTTP 400", retryable: false, retryAfterMs: null });
    assert.equal(received.length, 1);
  } finally {
    forwarder.close();
  }
});

test("retries stop at the signal's timeout", async () => {
  fresh();
  const forwarder = createOtlpForwarder({ OTEL_EXPORTER_OTLP_ENDPOINT: base, OTEL_EXPORTER_OTLP_TRACES_TIMEOUT: "300" });
  try {
    statuses.push([503, { "retry-after": "1" }]);
    const startedAt = Date.now();
    assert.deepEqual(await forwarder.export("traces", "protobuf", Buffer.from([1])), { ok: false, error: "HTTP 503", retryable: true, retryAfterMs: 1000 });
    assert.ok(Date.now() - startedAt < 1000);
    assert.equal(received.length, 1);
  } finally {
    forwarder.close();
  }
});

test("retries stop within what an export is given, where less than the signal's timeout, and a server not answering is one to send again", async () => {
  fresh();
  const forwarder = createOtlpForwarder({ OTEL_EXPORTER_OTLP_ENDPOINT: base });
  try {
    statuses.push([503]);
    const startedAt = Date.now();
    assert.deepEqual(await forwarder.export("traces", "protobuf", Buffer.from([1]), { withinMs: 300 }), { ok: false, error: "HTTP 503", retryable: true, retryAfterMs: null });
    assert.ok(Date.now() - startedAt < 1000);
    assert.equal(received.length, 1);
  } finally {
    forwarder.close();
  }
  // A port nothing listens on, as a collector that is down.
  const closed = createServer();
  await new Promise<void>((resolve) => closed.listen(0, "127.0.0.1", resolve));
  const port = (closed.address() as AddressInfo).port;
  await new Promise((resolve) => closed.close(resolve));
  const down = createOtlpForwarder({ OTEL_EXPORTER_OTLP_ENDPOINT: `http://127.0.0.1:${port}` });
  try {
    const result = await down.export("traces", "protobuf", Buffer.from([1]), { withinMs: 300 });
    assert.deepEqual(result.ok ? null : [result.retryable, result.retryAfterMs], [true, null]);
  } finally {
    down.close();
  }
});

test("an export aborted ends the request it is sending, and any wait to send it again", async () => {
  // A collector that never answers, saying when a request it was sent goes away.
  let closed = 0;
  const silent = createServer((request) => void request.on("close", () => void (closed += 1)));
  await new Promise<void>((resolve) => silent.listen(0, "127.0.0.1", resolve));
  const forwarder = createOtlpForwarder({ OTEL_EXPORTER_OTLP_ENDPOINT: `http://127.0.0.1:${(silent.address() as AddressInfo).port}` });
  try {
    const sending = new AbortController();
    setTimeout(() => sending.abort(), 100);
    const startedAt = Date.now();
    assert.equal((await forwarder.export("traces", "protobuf", Buffer.from([1]), { signal: sending.signal })).ok, false);
    assert.ok(Date.now() - startedAt < 1000);
    for (const deadline = Date.now() + 2000; closed === 0 && Date.now() < deadline;) await new Promise((resolve) => setTimeout(resolve, 10));
    assert.equal(closed, 1);
  } finally {
    forwarder.close();
    silent.closeAllConnections();
    await new Promise((resolve) => silent.close(resolve));
  }
  fresh();
  const waiting = createOtlpForwarder({ OTEL_EXPORTER_OTLP_ENDPOINT: base });
  try {
    statuses.push([503, { "retry-after": "5" }]);
    const sending = new AbortController();
    setTimeout(() => sending.abort(), 100);
    const startedAt = Date.now();
    assert.equal((await waiting.export("traces", "protobuf", Buffer.from([1]), { signal: sending.signal })).ok, false);
    assert.ok(Date.now() - startedAt < 1000);
    assert.equal(received.length, 1);
  } finally {
    waiting.close();
  }
});

test("a signal alasio does not export is not forwarded", async () => {
  const forwarder = createOtlpForwarder({ OTEL_EXPORTER_OTLP_ENDPOINT: base, OTEL_METRICS_EXPORTER: "none" });
  try {
    assert.deepEqual(forwarder.protocols, { traces: "http/protobuf", logs: "http/protobuf" });
    assert.equal((await forwarder.export("metrics", "protobuf", Buffer.from([1]))).ok, false);
  } finally {
    forwarder.close();
  }
});

test("Retry-After is read as seconds or as a date", () => {
  assert.equal(retryAfterMs("2"), 2000);
  assert.equal(retryAfterMs(new Date(10_000).toUTCString(), 4000), 6000);
  assert.equal(retryAfterMs("soon"), null);
  assert.equal(retryAfterMs(undefined), null);
});
