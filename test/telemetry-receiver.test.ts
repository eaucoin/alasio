/**
 * The sandbox service's receiver of sessions' telemetry (src/sandbox/telemetry-receiver.ts),
 * as src/receiver/receiver.ts serves it: a session's OTLP taken with its token alone,
 * stamped, held within its limits, and answered only once the collector has taken it.
 */
import assert from "node:assert/strict";
import { createServer, request as httpRequest } from "node:http";
import type { AddressInfo } from "node:net";
import { test } from "node:test";
import { gzipSync } from "node:zlib";

import type { Attributes } from "@opentelemetry/api";
import { type ExportResult, ExportResultCode } from "@opentelemetry/core";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-proto";
import { ProtobufTraceSerializer } from "@opentelemetry/otlp-transformer";
import { resourceFromAttributes } from "@opentelemetry/resources";
import { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor } from "@opentelemetry/sdk-trace-base";
import { Effect } from "effect";
import { TestClock } from "effect/testing";

import { makeRateLimiter, serveTelemetryReceiver } from "../src/sandbox/telemetry-receiver.ts";
import type { Signal } from "../src/telemetry/config.ts";
import { createOtlpForwarder, type ForwardResult, type OtlpEncoding, type OtlpForwarder } from "../src/telemetry/forward.ts";

/** The session `volumeId`, as a receiver's authentication says it, stamped as a sandbox's is. */
const stamped = (volumeId: string) => ({ session: volumeId, stamp: { "service.name": "bayma", "alasio.volume.id": volumeId } });

test("a session's byte budget refills over time and asks an exporter to wait when spent", async () => {
  await Effect.runPromise(Effect.gen(function*() {
    const limiter = yield* makeRateLimiter({ rate: 100, burst: 300 });
    assert.equal(yield* limiter.take("a", 250), 0);
    assert.equal(yield* limiter.take("a", 100), 1);
    assert.equal(yield* limiter.take("b", 300), 0);
    yield* TestClock.adjust("1 second");
    assert.equal(yield* limiter.take("a", 100), 0);
  }).pipe(Effect.provide(TestClock.layer())));
});

function traceRequest(resource: Attributes) {
  const exporter = new InMemorySpanExporter();
  const provider = new BasicTracerProvider({ resource: resourceFromAttributes(resource), spanProcessors: [new SimpleSpanProcessor(exporter)] });
  provider.getTracer("t").startSpan("work").end();
  const request = ProtobufTraceSerializer.serializeRequest(exporter.getFinishedSpans());
  assert.ok(request);
  return Buffer.from(request);
}

/** What the receiver's `post` sends: a bearer token (or none), a body, its content type, and whether it is gzipped. */
interface ReceiverPost {
  readonly token?: string | null;
  readonly body?: Buffer;
  readonly type?: string;
  readonly gzip?: boolean;
}

test("the receiver takes a session's OTLP with its token only, stamped with what alasio knows", async () => {
  const exported: { signal: Signal; encoding: OtlpEncoding; body: Buffer }[] = [];
  const forwarder = {
    protocols: { traces: "http/protobuf" },
    async export(signal: Signal, encoding: OtlpEncoding, body: Buffer): Promise<ForwardResult> {
      exported.push({ signal, encoding, body });
      return { ok: true };
    },
  } satisfies Pick<OtlpForwarder, "protocols" | "export">;
  await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    const receiver = yield* serveTelemetryReceiver({
      port: 0,
      host: "127.0.0.1",
      forwarder,
      authenticate: (token) => Effect.succeed(token === "fs-abc123.good" ? stamped("fs-abc123") : null),
      limiter: yield* makeRateLimiter({ rate: 1, burst: 100_000 }),
    });
    const url = (path: string) => `http://127.0.0.1:${receiver.port}${path}`;
    const post = (path: string, { token = "fs-abc123.good", body = traceRequest({ "service.name": "liar", "alasio.volume.id": "fs-other" }), type = "application/x-protobuf", gzip = false }: ReceiverPost = {}) => fetch(url(path), {
      method: "POST",
      headers: { "content-type": type, ...(token ? { authorization: `Bearer ${token}` } : {}), ...(gzip ? { "content-encoding": "gzip" } : {}) },
      body: gzip ? gzipSync(body) : body,
    });
    yield* Effect.promise(async () => {
      assert.equal((await post("/v1/traces", { token: null })).status, 401);
      assert.equal((await post("/v1/traces", { token: "fs-abc123.bad" })).status, 401);
      assert.equal((await post("/v1/spans")).status, 404);
      assert.equal((await post("/v1/traces", { type: "text/plain" })).status, 415);
      assert.equal((await post("/v1/traces", { type: "constructor" })).status, 415);
      assert.equal((await post("/v1/traces", { body: Buffer.from([0x0a, 0xff]) })).status, 400);
      assert.equal((await post("/v1/logs")).status, 200);
      assert.equal(exported.length, 0);

      const response = await post("/v1/traces", { gzip: true });
      assert.equal(response.status, 200);
      // A full success, as the exporter decodes it: an empty protobuf message.
      assert.equal(response.headers.get("content-type"), "application/x-protobuf");
      assert.equal((await response.arrayBuffer()).byteLength, 0);
      const json = await post("/v1/logs", { type: "application/json; charset=utf-8", body: Buffer.from("{}") });
      assert.equal(await json.text(), "{}");
      assert.equal(exported.length, 1);
      const [stamped] = exported;
      assert.ok(stamped);
      const text = stamped.body.toString("latin1");
      assert.match(text, /fs-abc123/u);
      assert.doesNotMatch(text, /liar|fs-other/u);

      const flood = await post("/v1/traces", { body: Buffer.alloc(200_000) });
      assert.equal(flood.status, 429);
      assert.ok(Number(flood.headers.get("retry-after")) > 0);
      const huge = await post("/v1/traces", { body: Buffer.alloc(5 * 1024 * 1024) });
      assert.equal(huge.status, 413);
    });
  })));
});

/** Spans named `name`, ended, as a tracer gives them to its exporter. */
function spansNamed(name: string) {
  const exporter = new InMemorySpanExporter();
  const provider = new BasicTracerProvider({ resource: resourceFromAttributes({ "service.name": "bayma" }), spanProcessors: [new SimpleSpanProcessor(exporter)] });
  provider.getTracer("t").startSpan(name).end();
  return exporter.getFinishedSpans();
}

/**
 * A receiver of sessions' telemetry, each session's token `<its volume id>.good`, exporting
 * through `forwarder`, holding at most what `limits` says; and a POST of a trace request to
 * it, of session fs-abc123's unless another's token is given.
 */
const receiving = (forwarder: Pick<OtlpForwarder, "protocols" | "export">, limits: { maxHeldBytes?: number; maxSessionHeldBytes?: number } = {}) =>
  Effect.gen(function*() {
    const receiver = yield* serveTelemetryReceiver({
      port: 0,
      host: "127.0.0.1",
      forwarder,
      authenticate: (token) => Effect.succeed(token.endsWith(".good") ? stamped(token.slice(0, -".good".length)) : null),
      limiter: yield* makeRateLimiter({ rate: 1, burst: 100_000_000 }),
      ...limits,
    });
    const url = `http://127.0.0.1:${receiver.port}/v1/traces`;
    const post = (body = traceRequest({ "service.name": "bayma" }), { token = "fs-abc123.good", signal }: { token?: string; signal?: AbortSignal } = {}) =>
      fetch(url, { method: "POST", headers: { "content-type": "application/x-protobuf", authorization: `Bearer ${token}` }, body, ...(signal ? { signal } : {}) });
    return { url, post, receiver };
  });

/** A trace request of some 100 kB, most of it a span's, which stamping keeps. */
function paddedRequest(): Buffer<ArrayBuffer> {
  const spans = new InMemorySpanExporter();
  const provider = new BasicTracerProvider({ resource: resourceFromAttributes({ "service.name": "bayma" }), spanProcessors: [new SimpleSpanProcessor(spans)] });
  provider.getTracer("t").startSpan("work", { attributes: { padding: "x".repeat(100_000) } }).end();
  const request = ProtobufTraceSerializer.serializeRequest(spans.getFinishedSpans());
  assert.ok(request);
  return Buffer.from(request);
}

/** Waits until `holds`, within five seconds, failing with `what` where it does not. */
async function until(holds: () => boolean, what: string): Promise<void> {
  for (const deadline = Date.now() + 5000; !holds();) {
    assert.ok(Date.now() < deadline, what);
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

test("the receiver answers a request taken only once the collector has taken it: 503 with a Retry-After while it does not, which the exporter sends again after, and 400 where it refuses it", async () => {
  const results: ForwardResult[] = [
    { ok: false, error: "HTTP 503", retryable: true, retryAfterMs: null },
    { ok: false, error: "HTTP 429", retryable: true, retryAfterMs: 2500 },
    { ok: false, error: "connect ECONNREFUSED", retryable: true, retryAfterMs: null },
    { ok: false, error: "HTTP 400", retryable: false, retryAfterMs: null },
    { ok: true },
  ];
  const within: (number | undefined)[] = [];
  const forwarder = {
    protocols: { traces: "http/protobuf" },
    async export(_signal: Signal, _encoding: OtlpEncoding, _body: Buffer, options?: { withinMs?: number }): Promise<ForwardResult> {
      within.push(options?.withinMs);
      return results.shift() ?? { ok: true };
    },
  } satisfies Pick<OtlpForwarder, "protocols" | "export">;
  await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    const { post } = yield* receiving(forwarder);
    yield* Effect.promise(async () => {
      const answers = [];
      for (let n = 0; n < 5; n += 1) {
        const response = await post();
        answers.push([response.status, response.headers.get("retry-after")]);
      }
      assert.deepEqual(answers, [[503, "1"], [503, "3"], [503, "1"], [400, null], [200, null]]);
      // Each forwarded within what leaves the exporter, waiting for the answer, time to send it again.
      assert.ok(within.every((ms) => ms !== undefined && ms < 10_000), JSON.stringify(within));
    });
  })));
});

test("the receiver holds at most so many bytes of the requests it is answering at once, answering 503 past them, and gives them back as each is answered", async () => {
  const body = paddedRequest();
  let release = () => {};
  const held = new Promise<void>((resolve) => void (release = resolve));
  let exports = 0;
  const forwarder = {
    protocols: { traces: "http/protobuf" },
    async export(): Promise<ForwardResult> {
      exports += 1;
      // The first held until released, as by a collector slow to answer.
      if (exports === 1) await held;
      return { ok: true };
    },
  } satisfies Pick<OtlpForwarder, "protocols" | "export">;
  await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    // One request held, read and stamped, but not the next read beside it.
    const { post } = yield* receiving(forwarder, { maxHeldBytes: 2.5 * body.length, maxSessionHeldBytes: 2.5 * body.length });
    yield* Effect.promise(async () => {
      const first = post(body);
      try {
        await until(() => exports > 0, "the first request never reached the collector");
        const second = await post(body);
        assert.deepEqual([second.status, second.headers.get("retry-after")], [503, "1"]);
      } finally {
        release();
      }
      assert.equal((await first).status, 200);
      assert.equal((await post(body)).status, 200);
      // One larger than all it may hold, alone, though not than a request may be.
      assert.equal((await post(Buffer.alloc(3 * body.length))).status, 503);
      assert.equal(exports, 2);
    });
  })));
});

test("a request whose sender goes away, as its body arrives or as it is forwarded, ends its forward, and gives back what it held once the forward has ended", async () => {
  const body = paddedRequest();
  const forwards: { aborted: boolean; ended: boolean }[] = [];
  const forwarder = {
    protocols: { traces: "http/protobuf" },
    export(_signal: Signal, _encoding: OtlpEncoding, _body: Buffer, options?: { signal?: AbortSignal }): Promise<ForwardResult> {
      const forward = { aborted: false, ended: false };
      forwards.push(forward);
      // The first held until it is aborted, then ending a moment after, as a request being sent does; any after it taken at once.
      if (forwards.length > 1) return Promise.resolve({ ok: true });
      return new Promise((resolve) => {
        options?.signal?.addEventListener("abort", () => {
          forward.aborted = true;
          setTimeout(() => {
            forward.ended = true;
            resolve({ ok: false, error: "aborted", retryable: true, retryAfterMs: null });
          }, 300);
        });
      });
    },
  } satisfies Pick<OtlpForwarder, "protocols" | "export">;
  await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    // A request read and stamped needs all it may hold.
    const all = 2.2 * body.length;
    const { url, post, receiver } = yield* receiving(forwarder, { maxHeldBytes: all, maxSessionHeldBytes: all });
    yield* Effect.promise(async () => {
      // Its sender gone with half its body sent.
      const { hostname, port, pathname } = new URL(url);
      const partial = httpRequest({ hostname, port, path: pathname, method: "POST", headers: { "content-type": "application/x-protobuf", authorization: "Bearer fs-abc123.good", "content-length": body.length } });
      partial.on("error", () => {});
      partial.write(body.subarray(0, body.length / 2));
      await until(() => receiver.held().bytes > 0, "the half sent was never held");
      partial.destroy();
      await until(() => receiver.held().bytes === 0, "the half sent was never given back");
      assert.deepEqual([...receiver.held().sessions], []);

      // Its sender gone as it is forwarded: the forward ended, and what it held given back only once it has.
      const sender = new AbortController();
      const forwarded = post(body, { signal: sender.signal }).catch(() => null);
      await until(() => forwards.length === 1, "the request was never forwarded");
      const holding = receiver.held().bytes;
      assert.ok(holding > body.length, String(holding));
      sender.abort();
      await forwarded;
      await until(() => forwards[0]?.aborted === true, "the forward was never aborted");
      assert.deepEqual([forwards[0]?.ended, receiver.held().bytes], [false, holding]);
      await until(() => receiver.held().bytes === 0, "what the forward held was never given back");
      assert.equal(forwards[0]?.ended, true);

      // All it may hold given back: a request that needs all of it is taken.
      assert.equal((await post(body)).status, 200);
      assert.deepEqual([receiver.held().bytes, forwards.length], [0, 2]);
    });
  })));
});

test("one session's requests hold at most a share of what the receiver may hold, so they leave room for another session's", async () => {
  const body = paddedRequest();
  let release = () => {};
  const held = new Promise<void>((resolve) => void (release = resolve));
  let exports = 0;
  const forwarder = {
    protocols: { traces: "http/protobuf" },
    async export(): Promise<ForwardResult> {
      exports += 1;
      // The first held until released, as by a collector slow to answer.
      if (exports === 1) await held;
      return { ok: true };
    },
  } satisfies Pick<OtlpForwarder, "protocols" | "export">;
  await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    // A session's share, a quarter, holds one request read and stamped, not a second read beside it.
    const { post, receiver } = yield* receiving(forwarder, { maxHeldBytes: 10 * body.length });
    yield* Effect.promise(async () => {
      const first = post(body);
      try {
        await until(() => exports > 0, "the first request never reached the collector");
        const again = await post(body);
        assert.deepEqual([again.status, again.headers.get("retry-after")], [503, "1"]);
        assert.deepEqual([...receiver.held().sessions.keys()], ["fs-abc123"]);
        assert.equal((await post(body, { token: "fs-def456.good" })).status, 200);
      } finally {
        release();
      }
      assert.equal((await first).status, 200);
      assert.equal(receiver.held().bytes, 0);
    });
  })));
});

test("a session's telemetry the collector does not take, down longer than the forwarder's retries, is never answered as taken to bayma's exporter, and is once the collector takes it", async () => {
  // The collector's port, nothing listening on it until it comes up.
  const probe = createServer();
  await new Promise<void>((resolve) => probe.listen(0, "127.0.0.1", resolve));
  const port = (probe.address() as AddressInfo).port;
  await new Promise((resolve) => probe.close(resolve));
  const taken: Buffer[] = [];
  const collector = createServer((request, response) => {
    const chunks: Buffer[] = [];
    request.on("data", (chunk: Buffer) => chunks.push(chunk));
    request.on("end", () => {
      taken.push(Buffer.concat(chunks));
      response.writeHead(200, { "content-type": "application/x-protobuf" }).end();
    });
  });
  const forwarder = createOtlpForwarder({ OTEL_EXPORTER_OTLP_ENDPOINT: `http://127.0.0.1:${port}` });
  try {
    await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
      const { url } = yield* receiving(forwarder);
      // As bayma exports, with its exporter's own retries and timeout.
      const exporter = new OTLPTraceExporter({ url, headers: { authorization: "Bearer fs-abc123.good" } });
      const exported = (name: string) => new Promise<ExportResult>((resolve) => exporter.export(spansNamed(name), resolve));
      yield* Effect.promise(async () => {
        const down = await exported("while-down");
        assert.equal(down.code, ExportResultCode.FAILED);
        assert.equal(taken.length, 0);
        await new Promise<void>((resolve) => collector.listen(port, "127.0.0.1", resolve));
        const up = await exported("once-up");
        assert.equal(up.code, ExportResultCode.SUCCESS);
        assert.deepEqual(taken.map((body) => [/once-up/u.test(body.toString("latin1")), /while-down/u.test(body.toString("latin1"))]), [[true, false]]);
      });
      yield* Effect.promise(() => exporter.shutdown());
    })));
  } finally {
    forwarder.close();
    await new Promise((resolve) => collector.close(resolve));
  }
});
