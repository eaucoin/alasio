import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { EventEmitter, once } from "node:events";
import { createServer } from "node:http";
import { connect as connectTcp } from "node:net";
import { PassThrough } from "node:stream";
import { after, before, test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";

import { metrics } from "@opentelemetry/api";
import { ProtobufTraceSerializer } from "@opentelemetry/otlp-transformer";
import { resourceFromAttributes } from "@opentelemetry/resources";
import { MeterProvider, MetricReader } from "@opentelemetry/sdk-metrics";
import { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor } from "@opentelemetry/sdk-trace-base";

// bayma inside a sandbox exports to the telemetry drain beside it, and alasio relays the
// drain where it exports its own. These tests run the real drain as a process, read it
// through a child that pipes stdio to its read port as agent-connect does, and relay it
// to a stand-in OTLP server. alasio's meter is registered first, so its counts are seen.
class CollectingReader extends MetricReader {
  async onForceFlush() {}
  async onShutdown() {}
}
const metricReader = new CollectingReader();
metrics.setGlobalMeterProvider(new MeterProvider({ readers: [metricReader] }));

const { createSandbox } = await import("../src/sandbox/index.js");
const { createFrameReader, sandboxBaymaTelemetryEnv, sandboxResource, startTelemetryRelay } = await import("../src/sandbox/telemetry.js");
const { createOtlpForwarder } = await import("../src/telemetry/forward.js");

const DRAIN = new URL("../sandbox/agent/telemetry-drain.mjs", import.meta.url).pathname;
const PIPE = 'const s=require("node:net").connect(Number(process.argv[1]),"127.0.0.1");process.stdin.pipe(s);s.pipe(process.stdout);s.on("error",()=>process.exit(1));s.on("close",()=>process.exit(0));';
const STAMP = { "service.name": "bayma", "alasio.volume.id": "fs-abc123" };

const drains = [];
after(() => { for (const drain of drains) drain.kill(); });

/** A drain on free ports (or those given), once it listens: `{ process, otlp, read }`. */
async function startDrain({ otlp = 0, read = 0, maxHeld } = {}) {
  const child = spawn(process.execPath, [DRAIN, String(otlp), String(read), ...(maxHeld ? [String(maxHeld)] : [])], {
    stdio: ["ignore", "pipe", "inherit"],
  });
  drains.push(child);
  const [line] = await once(child.stdout, "data");
  const [, otlpPort, readPort] = /listening (\d+) (\d+)/.exec(line.toString());
  return { process: child, otlp: Number(otlpPort), read: Number(readPort) };
}

/** A POST to the drain: the response's status and body. */
async function post(drain, path, body, headers = { "content-type": "application/x-protobuf" }) {
  const response = await fetch(`http://127.0.0.1:${drain.otlp}${path}`, { method: "POST", headers, body });
  return { status: response.status, body: await response.text() };
}

/** Reads the drain's read port until `count` frames have arrived. */
async function readFrames(drain, count) {
  const socket = connectTcp(drain.read, "127.0.0.1");
  const frames = [];
  const reader = createFrameReader((frame) => frames.push({ ...frame, body: Buffer.from(frame.body) }));
  socket.on("data", (chunk) => reader.push(chunk));
  const deadline = Date.now() + 5000;
  while (frames.length < count && Date.now() < deadline) await sleep(10);
  return { socket, frames };
}

/** A traces request as bayma's SDK makes it, from a resource claiming to be another session. */
function tracesRequest(name) {
  const spans = new InMemorySpanExporter();
  const resource = resourceFromAttributes({ "service.name": "bayma", "alasio.volume.id": "fs-other" });
  new BasicTracerProvider({ resource, spanProcessors: [new SimpleSpanProcessor(spans)] }).getTracer("bayma").startSpan(name).end();
  return Buffer.from(ProtobufTraceSerializer.serializeRequest(spans.getFinishedSpans()));
}

async function relayCounts() {
  const { resourceMetrics } = await metricReader.collect();
  const points = resourceMetrics.scopeMetrics.flatMap((scope) => scope.metrics)
    .filter((metric) => metric.descriptor.name === "alasio.sandbox.telemetry.requests")
    .flatMap((metric) => metric.dataPoints);
  return Object.fromEntries(points.map(({ attributes, value }) =>
    [[attributes.outcome, attributes.signal, attributes.reason].filter(Boolean).join(" "), value]));
}

// The stand-in OTLP server alasio exports to.
let collector;
let collectorUrl;
const collected = [];
before(async () => {
  collector = createServer((req, res) => {
    const chunks = [];
    req.on("data", (chunk) => chunks.push(chunk));
    req.on("end", () => {
      collected.push({ url: req.url, headers: req.headers, body: Buffer.concat(chunks) });
      res.writeHead(200).end();
    });
  });
  await new Promise((resolve) => collector.listen(0, "127.0.0.1", resolve));
  collectorUrl = `http://127.0.0.1:${collector.address().port}`;
});
after(() => new Promise((resolve) => collector.close(resolve)));

async function collectedCount(count) {
  const deadline = Date.now() + 5000;
  while (collected.length < count && Date.now() < deadline) await sleep(10);
  return collected.length;
}

test("the drain holds what bayma sends until it is read, and greets each reader first", async () => {
  const drain = await startDrain();
  assert.deepEqual(await post(drain, "/v1/traces", Buffer.from([1, 2])), { status: 200, body: "" });
  assert.deepEqual(await post(drain, "/v1/logs", "{}", { "content-type": "application/json; charset=utf-8" }), { status: 200, body: "{}" });
  const { socket, frames } = await readFrames(drain, 3);
  socket.destroy();
  assert.deepEqual(frames.map(({ kind, encoding, body }) => [kind, encoding, body.toString("hex")]), [
    [4, 0, "01"],
    [0, 0, "0102"],
    [2, 1, Buffer.from("{}").toString("hex")],
  ]);
});

test("the drain takes only OTLP over HTTP, uncompressed and within its size bound", async () => {
  const drain = await startDrain();
  assert.equal((await fetch(`http://127.0.0.1:${drain.otlp}/v1/traces`)).status, 404);
  assert.equal((await post(drain, "/v1/profiles", "x")).status, 404);
  assert.equal((await post(drain, "/v1/traces", "x", { "content-type": "text/plain" })).status, 415);
  assert.equal((await post(drain, "/v1/traces", "x", { "content-type": "application/x-protobuf", "content-encoding": "gzip" })).status, 415);
  assert.equal((await post(drain, "/v1/traces", Buffer.alloc(4 * 1024 * 1024 + 1))).status, 413);
  const { socket, frames } = await readFrames(drain, 1);
  await sleep(100);
  socket.destroy();
  assert.deepEqual(frames.map((frame) => frame.kind), [4]); // nothing but the greeting
});

test("the drain drops the oldest beyond its bound and says how many", async () => {
  // Each 50-byte request is a 56-byte frame, and two do not fit in 100 bytes.
  const drain = await startDrain({ maxHeld: 100 });
  for (const byte of [1, 2, 3]) await post(drain, "/v1/traces", Buffer.alloc(50, byte));
  const { socket, frames } = await readFrames(drain, 3);
  socket.destroy();
  assert.deepEqual(frames.map(({ kind, body }) => [kind, kind === 3 ? body.readUInt32BE(0) : body[0]]), [[4, 1], [3, 2], [0, 3]]);
});

test("a new reader replaces the last", async () => {
  const drain = await startDrain();
  const first = await readFrames(drain, 1);
  const closed = once(first.socket, "close");
  const second = await readFrames(drain, 1);
  await closed;
  await post(drain, "/v1/metrics", Buffer.from([9]));
  const deadline = Date.now() + 5000;
  while (second.frames.length < 2 && Date.now() < deadline) await sleep(10);
  second.socket.destroy();
  assert.deepEqual(second.frames.map((frame) => frame.kind), [4, 1]);
});

test("the relay exports what bayma sent, stamped, drops what does not parse, and reads on", async () => {
  collected.length = 0;
  const drain = await startDrain();
  const forwarder = createOtlpForwarder({ OTEL_EXPORTER_OTLP_ENDPOINT: collectorUrl, OTEL_EXPORTER_OTLP_HEADERS: "authorization=Bearer%20secret" });
  const before = await relayCounts();
  const relay = startTelemetryRelay({
    volumeId: "fs-abc123",
    connect: () => spawn(process.execPath, ["-e", PIPE, String(drain.read)], { stdio: ["pipe", "pipe", "ignore"] }),
    isRunning: async () => true,
    forwarder,
    stamp: STAMP,
  });
  try {
    await post(drain, "/v1/traces", tracesRequest("tools/call exec"));
    await post(drain, "/v1/traces", Buffer.from([0x0b])); // not a request
    await post(drain, "/v1/traces", tracesRequest("bayma.exec"));
    assert.equal(await collectedCount(2), 2);
    await sleep(100);
    assert.equal(collected.length, 2);
    for (const [index, name] of ["tools/call exec", "bayma.exec"].entries()) {
      const { url, headers, body } = collected[index];
      assert.equal(url, "/v1/traces");
      assert.equal(headers.authorization, "Bearer secret");
      assert.ok(body.includes(name));
      assert.ok(body.includes("fs-abc123"));
      assert.ok(!body.includes("fs-other"));
    }
    const counts = await relayCounts();
    assert.equal((counts["exported traces"] ?? 0) - (before["exported traces"] ?? 0), 2);
    assert.equal((counts["dropped traces malformed"] ?? 0) - (before["dropped traces malformed"] ?? 0), 1);
  } finally {
    await relay.close();
    forwarder.close();
  }
});

test("the relay relays a frame that arrives on its own, after the greeting", async () => {
  // The drain's output, as a stand-in connection hands it over: the greeting alone first.
  const stdout = new PassThrough();
  const child = Object.assign(new EventEmitter(), { stdout, stdin: null, kill: () => stdout.end() });
  stdout.on("end", () => child.emit("close"));
  const exported = [];
  const relay = startTelemetryRelay({
    volumeId: "fs-abc123",
    connect: () => child,
    isRunning: async () => false,
    forwarder: { protocols: { traces: "http/protobuf" }, export: async (...args) => { exported.push(args); return { ok: true }; } },
    stamp: STAMP,
  });
  try {
    stdout.write(Buffer.from([0, 0, 0, 1, 4, 0, 1]));
    await sleep(20);
    stdout.write(Buffer.from([0, 0, 0, 0, 0, 0])); // an empty traces request
    const deadline = Date.now() + 2000;
    while (exported.length === 0 && Date.now() < deadline) await sleep(10);
    assert.deepEqual(exported.map(([signal, encoding]) => [signal, encoding]), [["traces", "protobuf"]]);
  } finally {
    await relay.close();
  }
});

test("the relay reads a drain that comes back, and ends when its session host stops", async () => {
  collected.length = 0;
  let drain = await startDrain();
  const ports = { otlp: drain.otlp, read: drain.read };
  const forwarder = createOtlpForwarder({ OTEL_EXPORTER_OTLP_ENDPOINT: collectorUrl });
  let running = true;
  let ended = false;
  const relay = startTelemetryRelay({
    volumeId: "fs-abc123",
    connect: () => spawn(process.execPath, ["-e", PIPE, String(ports.read)], { stdio: ["pipe", "pipe", "ignore"] }),
    isRunning: async () => running,
    forwarder,
    stamp: STAMP,
    onEnd: () => { ended = true; },
    // Long enough for the drain's restart to come within the relay's five tries.
    retryDelay: () => 200,
  });
  try {
    await post(drain, "/v1/traces", tracesRequest("before"));
    assert.equal(await collectedCount(1), 1);
    drain.process.kill();
    await once(drain.process, "exit");
    drain = await startDrain(ports);
    await post(drain, "/v1/traces", tracesRequest("after"));
    assert.equal(await collectedCount(2), 2);
    assert.ok(collected[1].body.includes("after"));

    running = false;
    drain.process.kill();
    const deadline = Date.now() + 5000;
    while (!ended && Date.now() < deadline) await sleep(10);
    assert.equal(ended, true);
  } finally {
    await relay.close();
    forwarder.close();
  }
});

test("the relay gives up on a session host without a drain until it is next used", async () => {
  let connections = 0;
  let ended = false;
  const relay = startTelemetryRelay({
    volumeId: "fs-abc123",
    connect: () => {
      connections += 1;
      return spawn(process.execPath, ["-e", "process.exit(1)"], { stdio: ["pipe", "pipe", "ignore"] });
    },
    isRunning: async () => true,
    forwarder: { protocols: {}, export: async () => ({ ok: true }) },
    stamp: STAMP,
    onEnd: () => { ended = true; },
    retryDelay: () => 1,
  });
  const deadline = Date.now() + 5000;
  while (!ended && Date.now() < deadline) await sleep(10);
  await relay.close();
  assert.equal(ended, true);
  assert.equal(connections, 5);
});

test("bayma in a sandbox exports each signal alasio exports over HTTP, in its protocol, to the drain", () => {
  assert.deepEqual(sandboxBaymaTelemetryEnv({}), {});
  assert.deepEqual(sandboxBaymaTelemetryEnv({
    OTEL_EXPORTER_OTLP_ENDPOINT: "https://collector.example:4318",
    OTEL_EXPORTER_OTLP_HEADERS: "authorization=secret",
    OTEL_EXPORTER_OTLP_TRACES_PROTOCOL: "grpc",
    OTEL_EXPORTER_OTLP_LOGS_PROTOCOL: "http/json",
  }), {
    OTEL_EXPORTER_OTLP_ENDPOINT: "http://127.0.0.1:4318",
    OTEL_TRACES_EXPORTER: "none",
    OTEL_METRICS_EXPORTER: "otlp",
    OTEL_EXPORTER_OTLP_METRICS_PROTOCOL: "http/protobuf",
    OTEL_LOGS_EXPORTER: "otlp",
    OTEL_EXPORTER_OTLP_LOGS_PROTOCOL: "http/json",
  });
  assert.deepEqual(sandboxResource("fs-abc123", { OTEL_RESOURCE_ATTRIBUTES: "service.name=alasio,deployment.environment.name=prod%20eu" }), {
    "deployment.environment.name": "prod eu",
    "service.name": "bayma",
    "alasio.volume.id": "fs-abc123",
  });
});

/** A sandbox over a fake Docker, with the standard variables `env`. */
function fakeSandbox(env) {
  const calls = [];
  const relays = [];
  const running = new Set();
  const docker = {
    cli: async (args) => {
      calls.push(args);
      if (args[0] === "run") running.add(args[args.indexOf("--name") + 1]);
      return { stdout: args[0] === "logs" ? "session-host ready 1200\n" : "", stderr: "" };
    },
    isRunning: async (name) => running.has(name),
    spawn: (args) => ({ args }),
  };
  const volumes = new Map([["fs-abc123", { id: "fs-abc123", netMode: "none", formatted: true }]]);
  let forwarderClosed = false;
  const sandbox = createSandbox({
    config: {
      metadata: { url: "redis://valkey:6379", databases: 16, passwordFile: "/run/meta" },
      s3: { endpoint: "http://seaweedfs:8333", bucket: "sessions", accessKey: "k", secretKey: "s" },
      host: { network: "alasio-neon_default", agentImage: "alasio/agent", sessionHostImage: "alasio/session-host", memoryMb: 1, cpus: 1, pidsLimit: 1, cacheMb: 1, hostPublicIp: null },
    },
    store: { sessionVolumes: { getVolume: (id) => volumes.get(id) ?? null, setVolumeFormatted: () => {} } },
    stateDir: "/nonexistent",
    env,
    docker,
    startForward: async () => ({ url: "http://127.0.0.1:40000/mcp", headers: {}, close: async () => {} }),
    createForwarder: async () => ({ protocols: {}, close: () => { forwarderClosed = true; } }),
    startRelay: (options) => {
      const relay = { options, closed: false, close: async () => { relay.closed = true; } };
      relays.push(relay);
      return relay;
    },
  });
  sandbox.volumes.mountEnv = () => ({ JFS_META: "redis://valkey:6379/1" });
  return { sandbox, calls, relays, forwarderClosed: () => forwarderClosed };
}

test("a session host is told bayma's telemetry settings, and its telemetry relayed, only when alasio exports", async () => {
  const exporting = fakeSandbox({ OTEL_EXPORTER_OTLP_ENDPOINT: "http://collector:4318", OTEL_RESOURCE_ATTRIBUTES: "deployment.environment.name=prod" });
  await Promise.all([exporting.sandbox.ensureSession("fs-abc123"), exporting.sandbox.ensureSession("fs-abc123")]);
  const setting = exporting.calls.find((args) => args[0] === "run").find((arg) => arg.startsWith("SANDBOX_TELEMETRY="));
  assert.deepEqual(JSON.parse(setting.slice("SANDBOX_TELEMETRY=".length)), {
    OTEL_EXPORTER_OTLP_ENDPOINT: "http://127.0.0.1:4318",
    OTEL_TRACES_EXPORTER: "otlp",
    OTEL_EXPORTER_OTLP_TRACES_PROTOCOL: "http/protobuf",
    OTEL_METRICS_EXPORTER: "otlp",
    OTEL_EXPORTER_OTLP_METRICS_PROTOCOL: "http/protobuf",
    OTEL_LOGS_EXPORTER: "otlp",
    OTEL_EXPORTER_OTLP_LOGS_PROTOCOL: "http/protobuf",
  });
  await sleep(0);
  assert.equal(exporting.relays.length, 1); // one relay for callers that ask together
  const [{ options }] = exporting.relays;
  assert.deepEqual(options.connect().args, ["exec", "-i", "alasio-session-fs-abc123", "agent-connect", "7291"]);
  assert.deepEqual(options.stamp, { "deployment.environment.name": "prod", "service.name": "bayma", "alasio.volume.id": "fs-abc123" });
  assert.equal(await options.isRunning(), true);
  // A relay that ends is started again the next time the session is used.
  options.onEnd();
  await exporting.sandbox.ensureSession("fs-abc123");
  await sleep(0);
  assert.equal(exporting.relays.length, 2);
  await exporting.sandbox.close();
  assert.deepEqual(exporting.relays.map((relay) => relay.closed), [false, true]);
  assert.equal(exporting.forwarderClosed(), true);

  const silent = fakeSandbox({});
  await silent.sandbox.ensureSession("fs-abc123");
  await sleep(0);
  assert.ok(!silent.calls.find((args) => args[0] === "run").some((arg) => arg.startsWith("SANDBOX_TELEMETRY=")));
  assert.equal(silent.relays.length, 0);
});
