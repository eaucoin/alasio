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
import { keyOf, makeStoreProxy, SKEW_MS, type StoreProxy, type StoreProxyOptions, type StoreToken } from "../src/node-agent/store-proxy.ts";
import { type FakeS3, startFakeS3 } from "./support/fake-s3.ts";

// The snapshots bucket through the node agent, as a restore's checkpoint gofer reads it
// (src/node-agent/store-proxy.ts). Its clients here sign as alasio's own S3 client signs
// (src/node-agent/s3.ts), which the proxy checks with the official signer: two of them.

const CREDENTIALS = { accessKeyId: "AKIAAGENT", secretAccessKey: "the agent's secret" };
const PREFIX = "main/fs-abc/layers/";
const KEY = `${PREFIX}${"a".repeat(64)}/pages.img`;
const EMPTY = createHash("sha256").update("").digest("hex");

/** How long a proxy's tokens and quiet last, and how often it keeps its tokens, unless told: as the proxy has them; and its socket and tokens file, if any. */
type Times = Pick<StoreProxyOptions, "preparedForMs" | "quietMs" | "askedWindowMs" | "resumedCeilingMs" | "keptEveryMs" | "socket" | "tokensFile">;

/** `effect` with a proxy of a fake bucket holding KEY, of `times`, run to its end. */
function withProxy<A>(effect: (context: { readonly proxy: StoreProxy; readonly s3: FakeS3 }) => Effect.Effect<A, unknown, Scope.Scope>, times: Times = {}): Promise<A> {
  return Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    const s3 = yield* Effect.acquireRelease(
      Effect.promise(() => startFakeS3({ bucket: "snapshots", region: "us-east-1", ...CREDENTIALS })),
      (made) => Effect.promise(() => made.close()),
    );
    s3.objects.set(KEY, Buffer.from(Array.from({ length: 4096 }, (_, index) => index % 251)));
    const proxy = yield* makeStoreProxy({ store: { endpoint: s3.endpoint, region: "us-east-1", bucket: "snapshots", ...CREDENTIALS }, host: "127.0.0.1", port: 0, ...times });
    return yield* effect({ proxy, s3 });
  })));
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

test("a resumed token lasts while its restore keeps asking, through a bucket away for ten times its window, failing or not answering; once it asks no more, a window later it ends", () =>
  withProxy(({ proxy, s3 }) =>
    Effect.gen(function*() {
      const status = (token: StoreToken, through = proxy) => Effect.promise(() => ask(through.endpoint, `/snapshots/${KEY}`, { token })).pipe(Effect.map(refusal));
      const token = minted(proxy);
      proxy.resumed("main/fs-abc", 100);
      // The bucket failing every read for ten windows, its restore asking again every 50 ms,
      // as its gofer does: each answered as the bucket answers, the token lasting.
      s3.refuse = (method) => (method === "GET" ? 503 : null);
      for (const startedAt = Date.now(); Date.now() - startedAt < 2000;) {
        assert.equal(yield* status(token), "503 InternalError");
        yield* sleep(50);
      }
      // Back, it reads.
      s3.refuse = () => null;
      assert.equal(yield* status(token), "200 ");
      // Asked no more, it ends a window after its last request.
      yield* sleep(100);
      assert.equal(yield* status(token), "200 ");
      yield* sleep(300);
      assert.equal(yield* status(token), "403 AccessDenied");
      // A bucket that does not answer at all, likewise.
      yield* Effect.scoped(Effect.gen(function*() {
        const away = yield* makeStoreProxy({ store: { endpoint: "http://127.0.0.1:1", region: "us-east-1", bucket: "snapshots", ...CREDENTIALS }, host: "127.0.0.1", port: 0, askedWindowMs: 200 });
        const unanswered = away.mint("main/fs-abc", PREFIX);
        assert.ok(unanswered);
        away.resumed("main/fs-abc", 100);
        for (const startedAt = Date.now(); Date.now() - startedAt < 2000;) {
          assert.equal(yield* status(unanswered, away), "502 BadGateway");
          yield* sleep(50);
        }
        yield* sleep(300);
        assert.equal(yield* status(unanswered, away), "403 AccessDenied");
      }));
      // Never resumed, its requests lengthen it not: it ends as a prepare's does.
      const prepared = minted(proxy, "main/fs-prepared");
      for (let asked = 0; asked < 4; asked++) {
        yield* status(prepared);
        yield* sleep(100);
      }
      assert.equal(yield* status(prepared), "403 AccessDenied");
    }), { askedWindowMs: 200, preparedForMs: 300 }));

test("a resumed token ends at its ceiling from its resume, however its restore keeps asking", () =>
  withProxy(({ proxy }) =>
    Effect.gen(function*() {
      const token = minted(proxy);
      proxy.resumed("main/fs-abc", 100);
      const startedAt = Date.now();
      let refused = 0;
      while (refused === 0 && Date.now() - startedAt < 3000) {
        const said = refusal(yield* Effect.promise(() => ask(proxy.endpoint, `/snapshots/${KEY}`, { token })));
        if (said !== "200 ") refused = Date.now() - startedAt;
        yield* sleep(50);
      }
      assert.ok(refused >= 1000 && refused < 1400, `refused ${refused} ms after its resume`);
    }), { askedWindowMs: 200, resumedCeilingMs: 1000 }));

test("a proxy that starts again takes back the time its tokens' requests lengthened, kept at most every so often", () =>
  withDirectory(async (directory) => {
    const tokensFile = join(directory, "node-agent", "tokens.json");
    const status = (proxy: StoreProxy, token: StoreToken) => Effect.promise(() => ask(proxy.endpoint, `/snapshots/${KEY}`, { token })).pipe(Effect.map(refusal));
    const until = () => (JSON.parse(readFileSync(tokensFile, "utf8")) as { tokens: { until: number }[] }).tokens[0]?.until ?? 0;
    const token = await withProxy(({ proxy }) =>
      Effect.gen(function*() {
        const token = minted(proxy, "main/fs-a", "socket");
        proxy.resumed("main/fs-a", 100);
        const resumedUntil = until();
        // Asked of, its lengthened time kept.
        assert.equal(yield* status(proxy, token), "200 ");
        const asked = until();
        assert.ok(asked >= resumedUntil + 50_000, `${asked} kept, ${resumedUntil} on its resume`);
        return token;
      }), { tokensFile, askedWindowMs: 60_000, keptEveryMs: 0 });
    // Past its resume's time, taken back, it reads, as its requests lengthened it.
    await new Promise((resolve) => setTimeout(resolve, 300));
    await withProxy(({ proxy }) => Effect.gen(function*() {
      assert.equal(yield* status(proxy, token), "200 ");
    }), { tokensFile });
    // Written at most every keptEveryMs: a request within it lengthens the token, not the file.
    await withProxy(({ proxy }) =>
      Effect.gen(function*() {
        const again = minted(proxy, "main/fs-b", "socket");
        proxy.resumed("main/fs-b", 100);
        const written = readFileSync(tokensFile, "utf8");
        assert.equal(yield* status(proxy, again), "200 ");
        assert.equal(readFileSync(tokensFile, "utf8"), written);
        yield* sleep(300);
        assert.equal(yield* status(proxy, again), "200 ");
      }), { tokensFile, askedWindowMs: 60_000, keptEveryMs: 60_000 });
  }));

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

test("a proxy that starts again takes back from its tokens file the tokens of the restores before it, a resumed one whose window ended while it was away among them, but those revoked; one whose file cannot be read takes none, says so, and starts", () =>
  withDirectory(async (directory) => {
    const tokensFile = join(directory, "node-agent", "tokens.json");
    const status = (proxy: StoreProxy, token: StoreToken) => Effect.promise(() => ask(proxy.endpoint, `/snapshots/${KEY}`, { token })).pipe(Effect.map(refusal));
    const [resumed, prepared, lapsed, revoked] = await withProxy(({ proxy }) =>
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
        // Kept again as it mints one, the one revoked no more among them.
        minted(proxy, "main/fs-e", "socket");
        const kept = JSON.parse(readFileSync(tokensFile, "utf8")) as { tokens: { holder: string }[] };
        assert.deepEqual(kept.tokens.map(({ holder }) => holder).sort(), ["main/fs-a", "main/fs-b", "main/fs-c", "main/fs-e"]);
        assert.equal(yield* status(proxy, resumed), "200 ");
        assert.equal(yield* status(proxy, prepared), "200 ");
        // Its window ended while no agent ran, which its restore could not ask of.
        assert.equal(yield* status(proxy, lapsed), "200 ");
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

test("a resumed token read back by a proxy that starts again lasts a window from its start, however long it was away, a window later ends once its restore asks no more, and past its ceiling is not taken back", () =>
  withDirectory(async (directory) => {
    const tokensFile = join(directory, "node-agent", "tokens.json");
    const status = (proxy: StoreProxy, token: StoreToken) => Effect.promise(() => ask(proxy.endpoint, `/snapshots/${KEY}`, { token })).pipe(Effect.map(refusal));
    const windows = { tokensFile, askedWindowMs: 200, keptEveryMs: 0 };
    const [away, capped] = await withProxy(({ proxy }) =>
      Effect.gen(function*() {
        const away = minted(proxy, "main/fs-a", "socket");
        const capped = minted(proxy, "main/fs-b", "socket");
        proxy.resumed("main/fs-a", 100);
        proxy.resumed("main/fs-b", 100);
        assert.equal(yield* status(proxy, away), "200 ");
        return [away, capped] as const;
      }), { ...windows, resumedCeilingMs: 60_000 });
    // Away for ten times its window, as an agent down for twenty minutes is for two.
    await new Promise((resolve) => setTimeout(resolve, 2000));
    await withProxy(({ proxy }) =>
      Effect.gen(function*() {
        assert.equal(yield* status(proxy, away), "200 ");
        // Asked of no more: a window later, it ends.
        yield* sleep(400);
        assert.equal(yield* status(proxy, away), "403 AccessDenied");
      }), { ...windows, resumedCeilingMs: 60_000 });
    // Past its ceiling, as an agent away longer than a restore's ceiling: refused.
    const file = JSON.parse(readFileSync(tokensFile, "utf8")) as { tokens: { holder: string; ceiling: number }[] };
    for (const token of file.tokens) if (token.holder === "main/fs-b") token.ceiling = Date.now() - 1;
    writeFileSync(tokensFile, JSON.stringify(file));
    await withProxy(({ proxy }) =>
      Effect.gen(function*() {
        assert.equal(yield* status(proxy, capped), "403 AccessDenied");
      }), windows);
  }));

test("a proxy that drains waits on no restore through its socket, which waits for the agent that comes back, only on those over TCP", () =>
  withProxy(({ proxy }) =>
    Effect.gen(function*() {
      minted(proxy, "main/fs-socket", "socket");
      const tcp = minted(proxy, "main/fs-tcp");
      assert.equal(proxy.reading(), 1);
      proxy.revoke("main/fs-tcp");
      assert.equal(proxy.reading(), 0);
      yield* proxy.drained.pipe(Effect.timeout("2 seconds"));
      assert.ok(tcp);
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
