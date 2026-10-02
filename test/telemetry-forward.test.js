import assert from "node:assert/strict";
import { createServer } from "node:http";
import { after, before, test } from "node:test";
import { gunzipSync } from "node:zlib";

import { createOtlpForwarder, retryAfterMs } from "../src/telemetry/forward.js";

// The forwarder sends what alasio relays as OpenTelemetry's exporters send alasio's own:
// to each signal's endpoint, with its headers and compression, retrying what OTLP says
// to retry. A stand-in OTLP server answers each request with the next queued status.

let server;
let base;
const received = [];
const statuses = [];

before(async () => {
  server = createServer((req, res) => {
    const chunks = [];
    req.on("data", (chunk) => chunks.push(chunk));
    req.on("end", () => {
      received.push({ url: req.url, headers: req.headers, body: Buffer.concat(chunks) });
      const [status, headers = {}] = statuses.shift() ?? [200];
      res.writeHead(status, headers).end();
    });
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  base = `http://127.0.0.1:${server.address().port}`;
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
    assert.deepEqual(await forwarder.export("traces", "protobuf", Buffer.from([1])), { ok: false, error: "HTTP 400" });
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
    assert.deepEqual(await forwarder.export("traces", "protobuf", Buffer.from([1])), { ok: false, error: "HTTP 503" });
    assert.ok(Date.now() - startedAt < 1000);
    assert.equal(received.length, 1);
  } finally {
    forwarder.close();
  }
});

test("a signal alasio exports over gRPC, or not at all, is not forwarded", async () => {
  const warnings = [];
  const forwarder = createOtlpForwarder({
    OTEL_EXPORTER_OTLP_ENDPOINT: base,
    OTEL_EXPORTER_OTLP_TRACES_PROTOCOL: "grpc",
    OTEL_METRICS_EXPORTER: "none",
  }, { warn: (message) => warnings.push(message) });
  try {
    assert.deepEqual(forwarder.protocols, { logs: "http/protobuf" });
    assert.equal((await forwarder.export("traces", "protobuf", Buffer.from([1]))).ok, false);
    assert.match(warnings.join("\n"), /traces relayed from session sandboxes are not exported: alasio exports traces over grpc/);
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
