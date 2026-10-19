import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync, mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { createServer, type IncomingMessage, request as httpRequest } from "node:http";
import type { AddressInfo } from "node:net";
import { join } from "node:path";
import { test } from "node:test";

import { Effect, Fiber, Logger, type Scope } from "effect";

import { sigV4Authorization } from "../src/node-agent/s3.ts";
import { answerStart, keyOf, makeStoreProxy, SKEW_MS, type StoreProxy, type StoreProxyOptions, type StoreToken, withRange } from "../src/node-agent/store-proxy.ts";
import { type FakeS3, startFakeS3 } from "./support/fake-s3.ts";

// The snapshots bucket through the node agent, as a restore's checkpoint gofer reads it
// (src/node-agent/store-proxy.ts). Its clients here sign as alasio's own S3 client signs
// (src/node-agent/s3.ts), which the proxy checks with the official signer: two of them.

const CREDENTIALS = { accessKeyId: "AKIAAGENT", secretAccessKey: "the agent's secret" };
const PREFIX = "main/fs-abc/layers/";
const KEY = `${PREFIX}${"a".repeat(64)}/pages.img`;
const EMPTY = createHash("sha256").update("").digest("hex");

/** How long a proxy's tokens and quiet last, unless told: as the proxy has them; and its socket and tokens file, if any. */
type Times = Pick<StoreProxyOptions, "preparedForMs" | "quietMs" | "socket" | "tokensFile">;

/** `effect` with a proxy of a fake bucket holding KEY, of `times`, run to its end; what it logs, put in `logged`. */
function withProxy<A>(effect: (context: { readonly proxy: StoreProxy; readonly s3: FakeS3 }) => Effect.Effect<A, unknown, Scope.Scope>, times: Times = {}, logged: string[] = []): Promise<A> {
  return Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    const s3 = yield* Effect.acquireRelease(
      Effect.promise(() => startFakeS3({ bucket: "snapshots", region: "us-east-1", ...CREDENTIALS })),
      (made) => Effect.promise(() => made.close()),
    );
    s3.objects.set(KEY, Buffer.from(Array.from({ length: 4096 }, (_, index) => index % 251)));
    const proxy = yield* makeStoreProxy({ store: { endpoint: s3.endpoint, region: "us-east-1", bucket: "snapshots", ...CREDENTIALS }, host: "127.0.0.1", port: 0, ...times });
    return yield* effect({ proxy, s3 });
  })).pipe(Effect.provide(Logger.layer([Logger.make(({ message }) => void logged.push(String(message)))]))));
}

/** The token `proxy` mints for `holder`, of PREFIX, reached by `transport`, which it must. */
function minted(proxy: StoreProxy, holder = "main/fs-abc", transport: "socket" | "tcp" = "tcp"): StoreToken {
  const token = proxy.mint(holder, PREFIX, transport);
  assert.ok(token, `the proxy minted no token for ${holder}`);
  return token;
}

const sleep = (ms: number) => Effect.promise(() => new Promise((resolve) => setTimeout(resolve, ms)));

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

/** What a GET of `path` through `endpoint`, or through the socket at `socketPath` naming `endpoint`, came to, signed with `token` as `signing` says. */
function ask(endpoint: string, path: string, signing: Signing | null, socketPath?: string): Promise<Asked> {
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
    const asked = httpRequest({ ...(socketPath === undefined ? { host: url.hostname, port: url.port } : { socketPath }), method: signing?.method ?? "GET", path: search === "" ? path : `${path}?${search}`, headers }, (response) => {
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
      const token = minted(proxy);
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
      const token = minted(proxy);
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
      const token = minted(proxy);
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
      // Revoked, it reads nothing; minted anew, the one before reads nothing.
      assert.equal(yield* status(`/snapshots/${KEY}`), "200 ");
      const again = minted(proxy);
      assert.equal(yield* status(`/snapshots/${KEY}`), "403 AccessDenied");
      assert.equal(yield* status(`/snapshots/${KEY}`, { token: again }), "200 ");
      const last = minted(proxy);
      proxy.revoke("main/fs-abc");
      assert.equal(yield* status(`/snapshots/${KEY}`, { token: last }), "403 AccessDenied");
    })
  ));

test("a token lasts as long as a restore takes to its resume, and from its resume as long as its background reads may take, whatever came before", () =>
  withProxy(({ proxy }) =>
    Effect.gen(function*() {
      const status = (token: StoreToken) => Effect.promise(() => ask(proxy.endpoint, `/snapshots/${KEY}`, { token })).pipe(Effect.map(refusal));
      // Never resumed, it lapses as a prepare's would.
      const prepared = minted(proxy);
      assert.equal(yield* status(prepared), "200 ");
      yield* sleep(400);
      assert.equal(yield* status(prepared), "403 AccessDenied");
      // Resumed, it lasts the background's time from then, beyond its prepare's, and no longer.
      const resumed = minted(proxy);
      proxy.resumed("main/fs-abc", 1000);
      yield* sleep(400);
      assert.equal(yield* status(resumed), "200 ");
      proxy.resumed("main/fs-abc", 100);
      yield* sleep(200);
      assert.equal(yield* status(resumed), "403 AccessDenied");
      // Of a holder of none, nothing.
      proxy.resumed("main/fs-none", 1000);
      assert.equal(proxy.reading(), 0);
    }), { preparedForMs: 300 }));

test("a proxy that drains mints no token, and ends once no restore reads through it: none prepared and not yet resumed, none whose read is under way, none resumed whose reads have been quiet for less than its quiet", () =>
  withProxy(({ proxy, s3 }) =>
    Effect.gen(function*() {
      const prepared = minted(proxy, "main/fs-a");
      const resumed = minted(proxy, "main/fs-b");
      proxy.resumed("main/fs-b", 60_000);
      // Read slowly: under way for a second.
      s3.getDelayMs = 1000;
      const reading = yield* Effect.forkChild(Effect.promise(() => ask(proxy.endpoint, `/snapshots/${KEY}`, { token: resumed })));
      const drained = yield* Effect.forkChild(proxy.drained);
      yield* Effect.yieldNow;
      assert.equal(proxy.mint("main/fs-c", PREFIX), null);
      // Past the quiet: the read is under way still, and the prepared one is not resumed.
      yield* sleep(500);
      assert.equal(proxy.reading(), 2);
      assert.equal(drained.pollUnsafe(), undefined);
      // Resumed, the prepared one is quiet once its quiet has passed; the other, once its read ended and its quiet passed after.
      proxy.resumed("main/fs-a", 60_000);
      assert.equal((yield* Fiber.join(reading)).status, 200);
      yield* sleep(100);
      assert.equal(proxy.reading(), 1);
      assert.equal(drained.pollUnsafe(), undefined);
      yield* Fiber.join(drained).pipe(Effect.timeout("3 seconds"));
      assert.equal(proxy.reading(), 0);
      // Revoked, a token is read through no more, drained or not.
      assert.equal(refusal(yield* Effect.promise(() => ask(proxy.endpoint, `/snapshots/${KEY}`, { token: prepared }))), "200 ");
      proxy.revoke("main/fs-a");
      assert.equal(refusal(yield* Effect.promise(() => ask(proxy.endpoint, `/snapshots/${KEY}`, { token: prepared }))), "403 AccessDenied");
    }), { quietMs: 300 }));

/** A directory of the test's, in /tmp, short enough for a socket's path, whatever TMPDIR is, removed after `body`. */
async function withDirectory<A>(body: (directory: string) => Promise<A>): Promise<A> {
  const directory = mkdtempSync(join("/tmp", "alasio-proxy-"));
  try {
    return await body(directory);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

/** `effect`, and what it logged, each line with its level. */
function logging<A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<{ readonly value: A; readonly logged: readonly string[] }, E, R> {
  const logged: string[] = [];
  const logger = Logger.make(({ logLevel, message }) => logged.push(`${logLevel} ${Array.isArray(message) ? message.join(" ") : String(message)}`));
  return effect.pipe(Effect.map((value) => ({ value, logged })), Effect.provide(Logger.layer([logger])));
}

test("the proxy serves a restore's token through its socket too, only root's, by the path alone, whatever host its requests name, and removes the socket as it ends", () =>
  withDirectory(async (directory) => {
    const socket = join(directory, "node-agent", "store.sock");
    await withProxy(({ proxy, s3 }) =>
      Effect.gen(function*() {
        assert.equal(proxy.socket, socket);
        assert.equal((statSync(socket).mode & 0o777).toString(8), "600");
        assert.equal((statSync(join(directory, "node-agent")).mode & 0o777).toString(8), "700");
        const token = minted(proxy, "main/fs-abc", "socket");
        const asked = yield* Effect.promise(() => ask("http://alasio-node-agent", `/snapshots/${KEY}`, { token, range: "bytes=10-19" }, socket));
        assert.deepEqual([asked.status, asked.body], [206, s3.objects.get(KEY)?.subarray(10, 20)]);
        // As it refuses over TCP, it refuses there.
        assert.equal(refusal(yield* Effect.promise(() => ask("http://alasio-node-agent", `/snapshots/${KEY}`, null, socket))), "403 AccessDenied");
      }), { socket });
    assert.ok(!existsSync(socket));
    // A socket an agent before left, killed, is no obstacle to the next.
    writeFileSync(socket, "");
    await withProxy(() => Effect.sync(() => assert.equal((statSync(socket).mode & 0o777).toString(8), "600")), { socket });
  }));

test("a proxy that starts again takes back from its tokens file the tokens of the restores before it, but those ended or revoked; one whose file cannot be read takes none, says so, and starts", () =>
  withDirectory(async (directory) => {
    const tokensFile = join(directory, "node-agent", "tokens.json");
    const status = (proxy: StoreProxy, token: StoreToken) => Effect.promise(() => ask(proxy.endpoint, `/snapshots/${KEY}`, { token })).pipe(Effect.map(refusal));
    const [resumed, prepared, ended, revoked] = await withProxy(({ proxy }) =>
      Effect.gen(function*() {
        const tokens = [minted(proxy, "main/fs-a", "socket"), minted(proxy, "main/fs-b", "socket"), minted(proxy, "main/fs-c"), minted(proxy, "main/fs-d", "socket")] as const;
        proxy.resumed("main/fs-a", 60_000);
        proxy.resumed("main/fs-c", 1);
        proxy.revoke("main/fs-d");
        assert.equal((statSync(tokensFile).mode & 0o777).toString(8), "600");
        return tokens;
      }), { tokensFile });
    await withProxy(({ proxy }) =>
      Effect.gen(function*() {
        // Kept again as it mints one, those ended no more among them.
        minted(proxy, "main/fs-e", "socket");
        const kept = JSON.parse(readFileSync(tokensFile, "utf8")) as { tokens: { holder: string }[] };
        assert.deepEqual(kept.tokens.map(({ holder }) => holder).sort(), ["main/fs-a", "main/fs-b", "main/fs-e"]);
        assert.equal(yield* status(proxy, resumed), "200 ");
        assert.equal(yield* status(proxy, prepared), "200 ");
        assert.equal(yield* status(proxy, ended), "403 AccessDenied");
        assert.equal(yield* status(proxy, revoked), "403 AccessDenied");
        // Its holder's still: minted anew, the one before reads nothing.
        minted(proxy, "main/fs-a", "socket");
        assert.equal(yield* status(proxy, resumed), "403 AccessDenied");

      }), { tokensFile });
    // A file that is not one, as a crash mid-write would not leave, but a disk might.
    for (const corrupt of ["{ not json", JSON.stringify({ schemaVersion: 2, tokens: [] }), JSON.stringify({ schemaVersion: 1, tokens: [{ accessKeyId: prepared.accessKeyId }] })]) {
      writeFileSync(tokensFile, corrupt);
      const { value: read, logged } = await Effect.runPromise(logging(Effect.scoped(Effect.gen(function*() {
        const proxy = yield* makeStoreProxy({ store: { endpoint: "http://127.0.0.1:1", region: "us-east-1", bucket: "snapshots", ...CREDENTIALS }, host: "127.0.0.1", port: 0, tokensFile });
        return yield* status(proxy, prepared);
      }))));
      assert.equal(read, "403 AccessDenied");
      assert.ok(logged.some((line) => line.startsWith("Error ") && line.includes(`cannot read back the tokens of the restores reading the snapshots bucket through it, ${tokensFile}`)), logged.join("\n"));
    }
  }));

test("a proxy that drains waits on no restore through its socket, which waits for the agent that comes back, however many bytes of its pages it has yet to be served; only on those over TCP, until they have been", () =>
  withProxy(({ proxy }) =>
    Effect.gen(function*() {
      const socket = minted(proxy, "main/fs-socket", "socket");
      const tcp = minted(proxy, "main/fs-tcp");
      assert.equal(proxy.reading(), 1);
      // Both resumed, each told the 4096 bytes of pages it reads, served none, and quiet for longer than their quiet.
      for (const [holder, token] of [["main/fs-socket", socket], ["main/fs-tcp", tcp]] as const) {
        proxy.resumed(holder, 60_000);
        proxy.expect(token.accessKeyId, 4096);
      }
      yield* sleep(300);
      assert.equal(proxy.reading(), 1);
      // Over TCP, served them all: done.
      assert.equal(refusal(yield* Effect.promise(() => ask(proxy.endpoint, `/snapshots/${KEY}`, { token: tcp, range: "bytes=0-4095" }))), "206 ");
      yield* sleep(50);
      assert.equal(proxy.reading(), 0);
      yield* proxy.drained.pipe(Effect.timeout("2 seconds"));
    }), { quietMs: 100 }));

const SERVED_ALL = "the restore of main/fs-abc has been served all 4096 bytes of its pages through the node agent";

test("a resumed restore told the bytes of pages its image holds reads until it has been served them all, each once, however long its reads are apart, as the proxy logs once", () => {
  const logged: string[] = [];
  return withProxy(({ proxy, s3 }) =>
    Effect.gen(function*() {
      s3.objects.set(`${PREFIX}${"a".repeat(64)}/checkpoint.img`, Buffer.alloc(100, 1));
      const token = minted(proxy);
      const read = (range?: string, key = KEY) => Effect.promise(() => ask(proxy.endpoint, `/snapshots/${key}`, { token, ...(range === undefined ? {} : { range }) })).pipe(Effect.map(refusal));
      proxy.resumed("main/fs-abc", 60_000);
      proxy.expect(token.accessKeyId, 4096);
      // Its first half read, then nothing for longer than its quiet: it reads on, as its loader paces it.
      assert.equal(yield* read("bytes=0-2047"), "206 ");
      yield* sleep(300);
      assert.equal(proxy.reading(), 1);
      // The same bytes again, as a read made again after a failure, and another file's: counted once, and not at all.
      assert.equal(yield* read("bytes=1024-2047"), "206 ");
      assert.equal(yield* read(undefined, `${PREFIX}${"a".repeat(64)}/checkpoint.img`), "200 ");
      yield* sleep(300);
      assert.equal(proxy.reading(), 1);
      assert.deepEqual(logged.filter((line) => line.includes("has been served")), []);
      // The rest: done.
      assert.equal(yield* read("bytes=2048-4095"), "206 ");
      yield* sleep(50);
      assert.equal(proxy.reading(), 0);
      // Read again, and told again: logged once.
      assert.equal(yield* read("bytes=0-4095"), "206 ");
      proxy.expect(token.accessKeyId, 4096);
      yield* sleep(50);
      assert.deepEqual(logged.filter((line) => line.includes("has been served")), [SERVED_ALL]);
      yield* proxy.drained.pipe(Effect.timeout("2 seconds"));
    }), { quietMs: 100 }, logged);
});

test("a resumed restore served its pages before the proxy is told how many bytes they are is done once it is told, as it logs", () => {
  const logged: string[] = [];
  return withProxy(({ proxy }) =>
    Effect.gen(function*() {
      const token = minted(proxy);
      proxy.resumed("main/fs-abc", 60_000);
      proxy.expect(token.accessKeyId, Number.POSITIVE_INFINITY);
      assert.equal(refusal(yield* Effect.promise(() => ask(proxy.endpoint, `/snapshots/${KEY}`, { token }))), "200 ");
      yield* sleep(50);
      assert.equal(proxy.reading(), 1);
      proxy.expect(token.accessKeyId, 4096);
      assert.equal(proxy.reading(), 0);
      yield* sleep(50);
      assert.deepEqual(logged.filter((line) => line.includes("has been served")), [SERVED_ALL]);
    }), { quietMs: 100 }, logged);
});

test("a resumed restore whose bytes of pages are not known yet reads however quiet, and once told none, as its quiet says", () =>
  withProxy(({ proxy }) =>
    Effect.gen(function*() {
      const token = minted(proxy);
      proxy.resumed("main/fs-abc", 60_000);
      proxy.expect(token.accessKeyId, Number.POSITIVE_INFINITY);
      yield* sleep(300);
      assert.equal(proxy.reading(), 1);
      proxy.expect(token.accessKeyId, null);
      assert.equal(proxy.reading(), 0);
      // Of a token there is not, nothing.
      proxy.expect("ALASIONONE", 1);
      assert.equal(proxy.reading(), 0);
    }), { quietMs: 100 }));

test("what the proxy is told of a restore's bytes is of the token it names, never of a later token of the same workspace", () =>
  withProxy(({ proxy }) =>
    Effect.gen(function*() {
      const before = minted(proxy);
      const after = minted(proxy);
      proxy.resumed("main/fs-abc", 60_000);
      // A slow inspect of the restore before tells its bytes once the next one is prepared: not the next one's.
      proxy.expect(before.accessKeyId, 4096);
      yield* sleep(300);
      assert.equal(proxy.reading(), 0);
      proxy.expect(after.accessKeyId, 4096);
      assert.equal(proxy.reading(), 1);
    }), { quietMs: 100 }));

/** A GET of `path` through `endpoint`, signed with `token`, of `range`, its client ending it once `cutAt` bytes of its body came: how many did. */
function askCut(endpoint: string, path: string, token: StoreToken, range: string, cutAt: number): Promise<number> {
  const url = new URL(endpoint);
  const headers: Record<string, string> = { host: url.host, "x-amz-date": amzDate(new Date()), "x-amz-content-sha256": EMPTY, range };
  headers["authorization"] = sigV4Authorization({ method: "GET", path, query: {}, headers: { ...headers }, payloadHash: EMPTY }, { region: "us-east-1", service: "s3" }, token);
  return new Promise((resolve, reject) => {
    const asked = httpRequest({ host: url.hostname, port: url.port, path, headers }, (response) => {
      let came = 0;
      response.on("data", (chunk: Buffer) => {
        came += chunk.length;
        if (came >= cutAt) {
          asked.destroy();
          resolve(came);
        }
      });
      response.on("end", () => resolve(came));
    });
    asked.on("error", (error) => (asked.destroyed ? undefined : reject(error)));
    asked.end();
  });
}

test("an answer of pages cut short counts what it gave, as the gofer keeps it and asks for the rest alone", () =>
  withProxy(({ proxy, s3 }) =>
    Effect.gen(function*() {
      const size = 16 * 1024 * 1024;
      s3.objects.set(KEY, Buffer.alloc(size, 3));
      const token = minted(proxy);
      proxy.resumed("main/fs-abc", 60_000);
      proxy.expect(token.accessKeyId, size);
      // Cut once a MiB came, as a connection lost mid-answer is.
      const came = yield* Effect.promise(() => askCut(proxy.endpoint, `/snapshots/${KEY}`, token, `bytes=0-${size - 1}`, 1024 * 1024));
      assert.ok(came >= 1024 * 1024 && came < size, `${came} bytes came`);
      yield* sleep(100);
      assert.equal(proxy.reading(), 1);
      // The rest, from where it was cut: done.
      const rest = yield* Effect.promise(() => ask(proxy.endpoint, `/snapshots/${KEY}`, { token, range: `bytes=${came}-${size - 1}` }));
      assert.equal(rest.body.length, size - came);
      yield* sleep(50);
      assert.equal(proxy.reading(), 0);
    }), { quietMs: 100 }));

test("pages are counted from where the bucket's answer says they begin, a range it answers whole from 0, whatever the request asked", () =>
  withProxy(({ proxy }) =>
    Effect.gen(function*() {
      const token = minted(proxy);
      proxy.resumed("main/fs-abc", 60_000);
      proxy.expect(token.accessKeyId, 4096);
      // A range the bucket does not take, which it answers with the whole object.
      const asked = yield* Effect.promise(() => ask(proxy.endpoint, `/snapshots/${KEY}`, { token, range: "bytes=-100" }));
      assert.equal(asked.status, 200);
      yield* sleep(50);
      assert.equal(proxy.reading(), 0);
    }), { quietMs: 100 }));

test("an answer's body begins at 0 of a whole object, at its range's first byte of a part, and nowhere the proxy counts where its range does not say", () => {
  assert.equal(answerStart(200, undefined), 0);
  assert.equal(answerStart(200, "bytes 5-9/10"), 0);
  assert.equal(answerStart(206, "bytes 1024-2047/4096"), 1024);
  assert.equal(answerStart(206, "bytes 1024-2047/*"), 1024);
  assert.equal(answerStart(206, undefined), null);
  assert.equal(answerStart(206, "bytes */4096"), null);
  assert.equal(answerStart(206, "items 0-1/2"), null);
  assert.equal(answerStart(416, "bytes */4096"), null);
  assert.equal(answerStart(500, undefined), null);
});

test("byte ranges are kept sorted and apart, those that meet or overlap merged", () => {
  assert.deepEqual(withRange([], 0, 10), [[0, 10]]);
  assert.deepEqual(withRange([[0, 10]], 10, 20), [[0, 20]]);
  assert.deepEqual(withRange([[0, 10], [30, 40]], 5, 35), [[0, 40]]);
  assert.deepEqual(withRange([[30, 40]], 0, 10), [[0, 10], [30, 40]]);
  assert.deepEqual(withRange([[0, 10]], 5, 5), [[0, 10]]);
});

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
      const token = minted(proxy);
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
