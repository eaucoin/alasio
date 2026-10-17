import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { createServer, type IncomingMessage, request as httpRequest } from "node:http";
import type { AddressInfo } from "node:net";
import { test } from "node:test";

import { Effect, type Scope } from "effect";

import { sigV4Authorization } from "../src/node-agent/s3.ts";
import { keyOf, makeStoreProxy, SKEW_MS, type StoreProxy, type StoreToken } from "../src/node-agent/store-proxy.ts";
import { type FakeS3, startFakeS3 } from "./support/fake-s3.ts";

// The snapshots bucket through the node agent, as a restore's checkpoint gofer reads it
// (src/node-agent/store-proxy.ts). Its clients here sign as alasio's own S3 client signs
// (src/node-agent/s3.ts), which the proxy checks with the official signer: two of them.

const CREDENTIALS = { accessKeyId: "AKIAAGENT", secretAccessKey: "the agent's secret" };
const PREFIX = "main/fs-abc/layers/";
const KEY = `${PREFIX}${"a".repeat(64)}/pages.img`;
const EMPTY = createHash("sha256").update("").digest("hex");

/** `effect` with a proxy of a fake bucket holding KEY, run to its end. */
function withProxy<A>(effect: (context: { readonly proxy: StoreProxy; readonly s3: FakeS3 }) => Effect.Effect<A, unknown, Scope.Scope>): Promise<A> {
  return Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    const s3 = yield* Effect.acquireRelease(
      Effect.promise(() => startFakeS3({ bucket: "snapshots", region: "us-east-1", ...CREDENTIALS, heads: true })),
      (made) => Effect.promise(() => made.close()),
    );
    s3.objects.set(KEY, Buffer.from(Array.from({ length: 4096 }, (_, index) => index % 251)));
    const proxy = yield* makeStoreProxy({ store: { endpoint: s3.endpoint, region: "us-east-1", bucket: "snapshots", ...CREDENTIALS }, host: "127.0.0.1", port: 0 });
    return yield* effect({ proxy, s3 });
  })));
}

/** What a GET of `path` through `endpoint` came to, signed with `token` as `how` says. */
interface Asked {
  readonly status: number;
  readonly headers: IncomingMessage["headers"];
  readonly body: Buffer;
}

interface Signing {
  readonly token: StoreToken;
  readonly method?: string;
  readonly range?: string;
  readonly region?: string;
  readonly service?: string;
  readonly at?: Date;
  /** The headers signed, of those sent; all unless said. */
  readonly signs?: (name: string) => boolean;
  readonly extra?: Readonly<Record<string, string>>;
  readonly query?: Readonly<Record<string, string>>;
  readonly authorization?: string;
}

const amzDate = (at: Date) => at.toISOString().replace(/[-:]|\.\d{3}/gu, "");

function ask(endpoint: string, path: string, signing: Signing | null): Promise<Asked> {
  const url = new URL(endpoint);
  const headers: Record<string, string> = { host: url.host, "x-amz-date": amzDate(signing?.at ?? new Date()), "x-amz-content-sha256": EMPTY, ...signing?.extra };
  if (signing?.range !== undefined) headers["range"] = signing.range;
  const query = signing?.query ?? {};
  const search = new URLSearchParams(query).toString();
  if (signing) {
    const signed = Object.fromEntries(Object.entries(headers).filter(([name]) => signing.signs?.(name) ?? true));
    headers["authorization"] = signing.authorization ?? sigV4Authorization(
      { method: signing.method ?? "GET", path, query, headers: signed, payloadHash: EMPTY },
      { region: signing.region ?? "us-east-1", service: signing.service ?? "s3" },
      signing.token,
    );
  }
  return new Promise((resolve, reject) => {
    const asked = httpRequest({ host: url.hostname, port: url.port, method: signing?.method ?? "GET", path: search === "" ? path : `${path}?${search}`, headers }, (response) => {
      const chunks: Buffer[] = [];
      response.on("data", (chunk: Buffer) => chunks.push(chunk));
      response.on("end", () => resolve({ status: response.statusCode ?? 0, headers: response.headers, body: Buffer.concat(chunks) }));
    });
    asked.on("error", reject);
    asked.end();
  });
}

const refusal = (asked: Asked) => `${asked.status} ${/<Code>([^<]+)<\/Code>/u.exec(asked.body.toString())?.[1] ?? ""}`;

test("a restore's token reads its objects through the proxy, whole or in ranges, as the bucket gives them, the bucket asked as the agent", () =>
  withProxy(({ proxy, s3 }) =>
    Effect.gen(function*() {
      const token = proxy.mint("main/fs-abc", PREFIX);
      assert.notEqual(token.accessKeyId, CREDENTIALS.accessKeyId);
      assert.equal(Buffer.from(token.secretAccessKey, "base64url").length, 32);
      const whole = yield* Effect.promise(() => ask(proxy.endpoint, `/snapshots/${KEY}`, { token }));
      assert.equal(whole.status, 200);
      assert.deepEqual(whole.body, s3.objects.get(KEY));
      const ranged = yield* Effect.promise(() => ask(proxy.endpoint, `/snapshots/${KEY}`, { token, range: "bytes=10-19" }));
      assert.deepEqual([ranged.status, ranged.headers["content-range"], ranged.body], [206, "bytes 10-19/4096", s3.objects.get(KEY)?.subarray(10, 20)]);
      // As the AWS SDKs ask it, the operation named in the query.
      assert.equal((yield* Effect.promise(() => ask(proxy.endpoint, `/snapshots/${KEY}`, { token, query: { "x-id": "GetObject" } }))).status, 200);
      // Signed by the agent, which the fake bucket checks of every request.
      assert.deepEqual(s3.requests.filter((asked) => asked.startsWith("GET ")), [`GET ${KEY}`, `GET ${KEY} bytes=10-19`, `GET ${KEY}`]);
      // Its errors as it gives them: an object it has none of, a failure of its own.
      const missing = yield* Effect.promise(() => ask(proxy.endpoint, `/snapshots/${PREFIX}missing`, { token }));
      assert.equal(refusal(missing), "404 NoSuchKey");
      s3.refuse = (method) => (method === "GET" ? 503 : null);
      assert.equal((yield* Effect.promise(() => ask(proxy.endpoint, `/snapshots/${KEY}`, { token }))).status, 503);
    })
  ));

test("the proxy takes a GET alone, signed by a restore's token as the official signer signs it, for the bucket's region and S3, of now, signing its host, date, payload, range and Amazon's headers", () =>
  withProxy(({ proxy, s3 }) =>
    Effect.gen(function*() {
      const token = proxy.mint("main/fs-abc", PREFIX);
      const path = `/snapshots/${KEY}`;
      const refused = (signing: Signing | null, at = path) => Effect.promise(() => ask(proxy.endpoint, at, signing)).pipe(Effect.map(refusal));
      assert.equal(yield* refused(null), "403 AccessDenied");
      assert.equal(yield* refused({ token, authorization: "AWS4-HMAC-SHA256 Credential=x" }), "403 AccessDenied");
      assert.equal(yield* refused({ token: { accessKeyId: "ALASIOUNKNOWN", secretAccessKey: token.secretAccessKey } }), "403 AccessDenied");
      assert.equal(yield* refused({ token: { ...token, secretAccessKey: "another secret" } }), "403 AccessDenied");
      const said = (signing: Signing) => Effect.promise(() => ask(proxy.endpoint, path, signing)).pipe(Effect.map((asked) => /<Message>([^<]+)<\/Message>/u.exec(asked.body.toString())?.[1] ?? ""));
      assert.equal(yield* said({ token, region: "eu-west-1" }), "the request is signed for eu-west-1&#39;s s3, not us-east-1&#39;s s3");
      assert.equal(yield* said({ token, service: "s3-object-lambda" }), "the request is signed for us-east-1&#39;s s3-object-lambda, not us-east-1&#39;s s3");
      assert.equal(yield* refused({ token, at: new Date(Date.now() - SKEW_MS - 60_000) }), "403 AccessDenied");
      assert.equal(yield* refused({ token, at: new Date(Date.now() + SKEW_MS + 60_000) }), "403 AccessDenied");
      for (const unsigned of ["host", "x-amz-date", "x-amz-content-sha256", "range", "x-amz-meta-anything"]) {
        assert.equal(yield* refused({ token, range: "bytes=0-9", extra: { "x-amz-meta-anything": "1" }, signs: (name) => name !== unsigned }), "403 AccessDenied", unsigned);
      }
      // Signed in its query, as a presigned URL is.
      assert.equal(yield* said({ token, query: { "X-Amz-Signature": "0".repeat(64) } }), "a request signed in its query is not taken");
      // Asking what a read does not.
      assert.equal(yield* refused({ token, query: { "uploads": "" } }), "400 InvalidRequest");
      for (const method of ["PUT", "HEAD", "DELETE", "POST"]) {
        const asked = yield* Effect.promise(() => ask(proxy.endpoint, path, { token, method }));
        assert.equal(asked.status, 405, method);
      }
      // Signed right, near enough to now: taken.
      assert.equal(yield* refused({ token, at: new Date(Date.now() - SKEW_MS + 60_000) }), "200 ");
      assert.equal(s3.requests.filter((asked) => asked.startsWith("GET ")).length, 1);
    })
  ));

test("a token reads the objects under its restore's prefix alone, in the snapshots bucket, by no path that leaves it, until it is revoked or minted anew", () =>
  withProxy(({ proxy, s3 }) =>
    Effect.gen(function*() {
      s3.objects.set("main/fs-other/layers/b/pages.img", Buffer.from("another's"));
      const token = proxy.mint("main/fs-abc", PREFIX);
      const status = (path: string, signing: Signing = { token }) => Effect.promise(() => ask(proxy.endpoint, path, signing)).pipe(Effect.map(refusal));
      assert.equal(yield* status("/snapshots/main/fs-other/layers/b/pages.img"), "403 AccessDenied");
      assert.equal(yield* status(`/other-bucket/${KEY}`), "403 AccessDenied");
      for (const path of [
        "/snapshots/main/fs-abc/layers/../../fs-other/layers/b/pages.img",
        "/snapshots/main/fs-abc/layers/%2e%2e/%2e%2e/fs-other/layers/b/pages.img",
        "/snapshots/main/fs-abc/layers/%252e%252e/x",
        "/snapshots/main/fs-abc/layers/a%2F..%2F..%2Ffs-other",
        "/snapshots/main/fs-abc/layers/a%5C..%5Cfs-other",
        "/snapshots/main/fs-abc/layers/./b",
        "/snapshots/main/fs-abc/layers//b",
        "/snapshots/main/fs-abc/layers/%00",
      ]) {
        assert.equal(yield* status(path), "403 AccessDenied", path);
      }
      assert.equal(s3.requests.filter((asked) => asked.startsWith("GET ")).length, 0);
      // Revoked, or as it lapses, it reads nothing; minted anew, the one before reads nothing.
      assert.equal(yield* status(`/snapshots/${KEY}`), "200 ");
      const again = proxy.mint("main/fs-abc", PREFIX);
      assert.equal(yield* status(`/snapshots/${KEY}`), "403 AccessDenied");
      assert.equal(yield* status(`/snapshots/${KEY}`, { token: again }), "200 ");
      proxy.revokeIn("main/fs-abc", 60_000);
      assert.equal(yield* status(`/snapshots/${KEY}`, { token: again }), "200 ");
      proxy.revokeIn("main/fs-abc", 0);
      assert.equal(yield* status(`/snapshots/${KEY}`, { token: again }), "403 AccessDenied");
      const last = proxy.mint("main/fs-abc", PREFIX);
      proxy.revoke("main/fs-abc");
      assert.equal(yield* status(`/snapshots/${KEY}`, { token: last }), "403 AccessDenied");
    })
  ));

test("a path is a key of the bucket only as it names it, segment by segment, once decoded", () => {
  assert.equal(keyOf("/snapshots/main/fs-abc/layers/a%20b/pages.img", "snapshots"), "main/fs-abc/layers/a b/pages.img");
  for (const path of ["snapshots/x", "/snapshots", "/snapshots/", "/other/x", "/snapshots/a/../b", "/snapshots/a/%2E%2E/b", "/snapshots/a%2fb", "/snapshots/%E0%A4%A", "/snapshots/a%25b"]) {
    assert.equal(keyOf(path, "snapshots"), null, path);
  }
});

/** A bucket of one object, of `bytes` bytes, sent as fast as its reader reads, which says when a request of it ends before its answer does. */
async function streamingBucket(bytes: number) {
  const ended: string[] = [];
  const chunk = Buffer.alloc(1 << 20, 7);
  const server = createServer((request, response) => {
    let left = bytes;
    response.writeHead(200, { "content-length": bytes });
    response.once("close", () => {
      if (left > 0) ended.push(`ended with ${left} bytes left`);
    });
    const pump = () => {
      while (left > 0) {
        const size = Math.min(left, chunk.length);
        left -= size;
        if (!response.write(size === chunk.length ? chunk : chunk.subarray(0, size))) return void response.once("drain", pump);
      }
      response.end();
    };
    request.resume();
    pump();
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  return { endpoint: `http://127.0.0.1:${(server.address() as AddressInfo).port}`, ended, close: () => new Promise<void>((resolve) => server.close(() => resolve())) };
}

/** `effect` with a proxy of a bucket `streamingBucket` makes. */
function withStreamingProxy<A>(bytes: number, effect: (context: { readonly proxy: StoreProxy; readonly ended: readonly string[] }) => Effect.Effect<A, unknown, Scope.Scope>): Promise<A> {
  return Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    const bucket = yield* Effect.acquireRelease(Effect.promise(() => streamingBucket(bytes)), (made) => Effect.promise(() => made.close()));
    const proxy = yield* makeStoreProxy({ store: { endpoint: bucket.endpoint, region: "us-east-1", bucket: "snapshots", ...CREDENTIALS }, host: "127.0.0.1", port: 0 });
    return yield* effect({ proxy, ended: bucket.ended });
  })));
}

test("a read its client ends ends the bucket's too", () =>
  withStreamingProxy(1 << 30, ({ proxy, ended }) =>
    Effect.gen(function*() {
      const token = proxy.mint("main/fs-abc", PREFIX);
      const url = new URL(proxy.endpoint);
      const headers: Record<string, string> = { host: url.host, "x-amz-date": amzDate(new Date()), "x-amz-content-sha256": EMPTY };
      headers["authorization"] = sigV4Authorization({ method: "GET", path: `/snapshots/${KEY}`, query: {}, headers: { ...headers }, payloadHash: EMPTY }, { region: "us-east-1", service: "s3" }, token);
      yield* Effect.promise(() =>
        new Promise<void>((resolve) => {
          const asked = httpRequest({ host: url.hostname, port: url.port, path: `/snapshots/${KEY}`, headers }, (response) => {
            response.once("data", () => {
              asked.destroy();
              resolve();
            });
          });
          asked.on("error", () => undefined);
          asked.end();
        })
      );
      yield* Effect.promise(async () => {
        for (let wait = 0; ended.length === 0 && wait < 100; wait++) await new Promise((resolve) => setTimeout(resolve, 20));
      });
      assert.equal(ended.length, 1);
      assert.match(ended[0] ?? "", /^ended with \d+ bytes left$/u);
    })
  ));

test("a GiB read through the proxy is streamed, the agent's memory not growing with it", async () => {
  const bucket = await streamingBucket(1 << 30);
  // The proxy in a process of its own, as the agent runs it, its memory its own.
  const proxied = spawn(process.execPath, ["--input-type=module", "-e", `
    import { Effect } from "effect";
    import { makeStoreProxy } from ${JSON.stringify(new URL("../src/node-agent/store-proxy.ts", import.meta.url).href)};
    await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
      const proxy = yield* makeStoreProxy({ store: { endpoint: ${JSON.stringify(bucket.endpoint)}, region: "us-east-1", bucket: "snapshots", accessKeyId: "a", secretAccessKey: "s" }, host: "127.0.0.1", port: 0 });
      process.stdout.write(JSON.stringify({ endpoint: proxy.endpoint, token: proxy.mint("main/fs-abc", ${JSON.stringify(PREFIX)}) }) + "\\n");
      yield* Effect.never;
    })));
  `], { cwd: new URL("..", import.meta.url).pathname, stdio: ["ignore", "pipe", "inherit"] });
  try {
    const { endpoint, token } = await new Promise<{ endpoint: string; token: StoreToken }>((resolve) => proxied.stdout.once("data", (line: Buffer) => resolve(JSON.parse(line.toString()))));
    const memory = (field: "VmRSS" | "VmHWM") => Number(new RegExp(`^${field}:\\s+(\\d+) kB$`, "mu").exec(readFileSync(`/proc/${proxied.pid}/status`, "utf8"))?.[1]) * 1024;
    const before = memory("VmRSS");
    const read = await new Promise<number>((resolve, reject) => {
      const url = new URL(endpoint);
      const headers: Record<string, string> = { host: url.host, "x-amz-date": amzDate(new Date()), "x-amz-content-sha256": EMPTY };
      headers["authorization"] = sigV4Authorization({ method: "GET", path: `/snapshots/${KEY}`, query: {}, headers: { ...headers }, payloadHash: EMPTY }, { region: "us-east-1", service: "s3" }, token);
      const asked = httpRequest({ host: url.hostname, port: url.port, path: `/snapshots/${KEY}`, headers }, (response) => {
        let bytes = 0;
        response.on("data", (chunk: Buffer) => (bytes += chunk.length));
        response.on("end", () => resolve(bytes));
      });
      asked.on("error", reject);
      asked.end();
    });
    assert.equal(read, 1 << 30);
    // What it held of the GiB at most, beside what it held before: what a stream of it at the
    // speed of the loopback leaves for its collector, a fraction of the GiB and of the agent's
    // limit (512 MiB), as no answer is held whole.
    const grown = memory("VmHWM") - before;
    assert.ok(grown < 128 * 1024 ** 2, `the proxy's process grew by ${Math.round(grown / 1024 ** 2)} MiB as it streamed a GiB`);
  } finally {
    proxied.kill();
    await bucket.close();
  }
});
