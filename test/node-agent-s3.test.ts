/**
 * The node agent's S3 client (src/node-agent/s3.ts): its signatures, against AWS's
 * Signature Version 4 test suite, and its requests, against a stand-in bucket that
 * checks them.
 */
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { Effect, Exit } from "effect";

import { makeS3Bucket, PRESIGNED_MAX_SECONDS, presignedUrl, queryAuthSignature, sigV4Authorization } from "../src/node-agent/s3.ts";
import { startFakeS3 } from "./support/fake-s3.ts";

/** The credentials and scope of AWS's test suite (aws-sig-v4-test-suite, as botocore keeps it in tests/unit/auth/aws4_testsuite). */
const SUITE_CREDENTIALS = { accessKeyId: "AKIDEXAMPLE", secretAccessKey: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY" };
const SUITE_SCOPE = { region: "us-east-1", service: "service" };
const EMPTY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
const SUITE_HEADERS = { host: "example.amazonaws.com", "x-amz-date": "20150830T123600Z" };

test("requests are signed as AWS's Signature Version 4 test suite signs them", () => {
  // get-vanilla
  assert.equal(
    sigV4Authorization({ method: "GET", path: "/", query: {}, headers: SUITE_HEADERS, payloadHash: EMPTY_SHA256 }, SUITE_SCOPE, SUITE_CREDENTIALS),
    "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31",
  );
  // get-vanilla-query-order-key-case: the query's parameters in order.
  assert.equal(
    sigV4Authorization({ method: "GET", path: "/", query: { Param2: "value2", Param1: "value1" }, headers: SUITE_HEADERS, payloadHash: EMPTY_SHA256 }, SUITE_SCOPE, SUITE_CREDENTIALS),
    "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=b97d918cfa904a5beff61c982a1b6f458b799221646efd99d3219ec94cdf2500",
  );
  // post-x-www-form-urlencoded: a payload's hash, and a header beyond the two.
  assert.equal(
    sigV4Authorization(
      {
        method: "POST",
        path: "/",
        query: {},
        headers: { "content-type": "application/x-www-form-urlencoded", ...SUITE_HEADERS },
        payloadHash: createHash("sha256").update("Param1=value1").digest("hex"),
      },
      SUITE_SCOPE,
      SUITE_CREDENTIALS,
    ),
    "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=ff11897932ad3f4e8b18135d722051e5ac45fc38421b1da7b9d196a0fe09473a",
  );
});

test("a request signed in its query is signed as AWS's example of a presigned URL is", () => {
  // Amazon S3's "Authenticating Requests: Using Query Parameters", its GET of examplebucket's test.txt.
  assert.equal(
    queryAuthSignature(
      {
        method: "GET",
        path: "/test.txt",
        query: {
          "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
          "X-Amz-Credential": "AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request",
          "X-Amz-Date": "20130524T000000Z",
          "X-Amz-Expires": "86400",
          "X-Amz-SignedHeaders": "host",
        },
        host: "examplebucket.s3.amazonaws.com",
      },
      { region: "us-east-1", service: "s3" },
      { accessKeyId: "AKIAIOSFODNN7EXAMPLE", secretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" },
    ),
    "aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404",
  );
});

const CREDENTIALS = { accessKeyId: "snapshots0123", secretAccessKey: "a secret" };

/** The status of `method` of `url`, with `headers`, and the body it was answered with. */
const fetched = async (url: string, method = "GET", headers: Record<string, string> = {}): Promise<{ readonly status: number; readonly body: Buffer }> => {
  const response = await fetch(url, { method, headers });
  return { status: response.status, body: Buffer.from(await response.arrayBuffer()) };
};

test("a presigned URL of an object grants its method of that object alone, of a range and as of an ETag too, until it expires, and no request that sends a header of the store's it does not sign", async () => {
  const s3 = await startFakeS3({ bucket: "snapshots", region: "us-east-1", ...CREDENTIALS });
  try {
    const object = Buffer.from("0123456789abcdef".repeat(256));
    s3.objects.set("main/fs-abc/layers/d1/pages.img", object);
    s3.objects.set("main/fs-abc/layers/d2/pages.img", object);
    const signer = { region: "us-east-1", ...CREDENTIALS };
    const url = presignedUrl(s3.endpoint, "snapshots", "main/fs-abc/layers/d1/pages.img", "GET", signer, new Date(), 60);
    // It signs nothing of the agent's but its host: no credential's secret in it.
    assert.ok(!url.includes(encodeURIComponent(CREDENTIALS.secretAccessKey)) && !url.includes(CREDENTIALS.secretAccessKey));
    assert.deepEqual(await fetched(url), { status: 200, body: object });
    assert.deepEqual(await fetched(url, "GET", { range: "bytes=16-31" }), { status: 206, body: object.subarray(16, 32) });
    const etag = `"${createHash("md5").update(object).digest("hex")}"`;
    assert.equal((await fetched(url, "GET", { "if-match": etag, range: "bytes=0-15" })).status, 206);
    assert.equal((await fetched(url, "GET", { "if-match": '"0123"' })).status, 412);
    // Another object, another method, a header of the store's: refused.
    assert.equal((await fetched(url.replace("/d1/", "/d2/"))).status, 403);
    assert.equal((await fetched(url, "PUT")).status, 403);
    assert.equal((await fetched(url, "HEAD")).status, 403);
    assert.equal((await fetched(presignedUrl(s3.endpoint, "snapshots", "main/fs-abc/layers/d1/pages.img", "HEAD", signer, new Date(), 60), "HEAD")).status, 200);
    assert.equal((await fetched(url, "GET", { "x-amz-content-sha256": "UNSIGNED-PAYLOAD" })).status, 403);
    // Signed by another key, or past its expiry: refused.
    assert.equal((await fetched(presignedUrl(s3.endpoint, "snapshots", "main/fs-abc/layers/d1/pages.img", "GET", { ...signer, secretAccessKey: "another" }, new Date(), 60))).status, 403);
    assert.equal((await fetched(presignedUrl(s3.endpoint, "snapshots", "main/fs-abc/layers/d1/pages.img", "GET", signer, new Date(Date.now() - 61_000), 60))).status, 403);
    assert.equal((await fetched(presignedUrl(s3.endpoint, "snapshots", "main/fs-abc/layers/d1/pages.img", "GET", signer, new Date(Date.now() - 59_000), 60))).status, 200);
    // S3 takes none past 7 days.
    assert.throws(() => presignedUrl(s3.endpoint, "snapshots", "k", "GET", signer, new Date(), PRESIGNED_MAX_SECONDS + 1), RangeError);
    assert.throws(() => presignedUrl(s3.endpoint, "snapshots", "k", "GET", signer, new Date(), 0), RangeError);
  } finally {
    await s3.close();
  }
});

test("the bucket takes files and JSON, gives them back, says what it holds, copies within itself, lists a prefix across pages, and deletes, as of an ETag too, every request signed", async () => {
  const s3 = await startFakeS3({ bucket: "snapshots", region: "us-east-1", ...CREDENTIALS, pageSize: 2 });
  const directory = mkdtempSync(join(tmpdir(), "alasio-node-agent-s3-"));
  try {
    const bucket = makeS3Bucket({ endpoint: s3.endpoint, region: "us-east-1", bucket: "snapshots", ...CREDENTIALS });
    const source = join(directory, "pages.img");
    const pages = Buffer.alloc(300_000, "page");
    writeFileSync(source, pages);
    const run = <A, E>(effect: Effect.Effect<A, E>) => Effect.runPromise(effect);

    // A key of characters a path encodes, streamed up unsigned and down with its hash.
    const etag = await run(bucket.putFile("main/fs-abc/s1/a page+1.img", source));
    assert.deepEqual(s3.objects.get("main/fs-abc/s1/a page+1.img"), pages);
    // What the bucket holds of it is what its upload was answered with.
    assert.deepEqual(await run(bucket.head("main/fs-abc/s1/a page+1.img")), { bytes: pages.length, etag });
    assert.equal(await run(bucket.head("main/fs-abc/s1/none")), null);
    const received = await run(bucket.getFile("main/fs-abc/s1/a page+1.img", join(directory, "copy.img"), pages.length));
    assert.deepEqual(received, { bytes: pages.length, sha256: createHash("sha256").update(pages).digest("hex") });
    assert.deepEqual(readFileSync(join(directory, "copy.img")), pages);

    // Copied in the bucket, its source named as its path is, the request signed with it.
    await run(bucket.copy("main/fs-abc/s1/a page+1.img", "main/fs-new/s1/a page+1.img", pages.length));
    assert.deepEqual(s3.objects.get("main/fs-new/s1/a page+1.img"), pages);
    assert.equal(s3.requests.at(-1), "PUT main/fs-new/s1/a page+1.img from main/fs-abc/s1/a page+1.img");
    const uncopied = await Effect.runPromiseExit(bucket.copy("main/fs-abc/s9/none", "main/fs-new/s9/none", 10));
    assert.ok(Exit.isFailure(uncopied));
    assert.match(String(uncopied.cause), /the bucket's copy of main\/fs-abc\/s9\/none to main\/fs-new\/s9\/none failed: 404 NoSuchKey/u);
    assert.equal(s3.objects.has("main/fs-new/s9/none"), false);

    await run(bucket.putJson("main/fs-abc/current.json", { snapshot: "s1" }));
    assert.deepEqual(await run(bucket.getJson("main/fs-abc/current.json")), { snapshot: "s1" });
    assert.equal(await run(bucket.getJson("main/fs-none/current.json")), null);

    for (const name of ["s1/b", "s1/c", "s2/a"]) await run(bucket.putJson(`main/fs-abc/${name}`, {}));
    await run(bucket.putJson("main/fs-abcd/current.json", {}));
    // Five keys under the prefix, two to a page, each with when it was written.
    s3.writtenAt.set("main/fs-abc/s1/b", Date.parse("2026-10-01T12:00:00.000Z"));
    const listed = await run(bucket.list("main/fs-abc/"));
    assert.deepEqual(listed.map(({ key }) => key), [
      "main/fs-abc/current.json",
      "main/fs-abc/s1/a page+1.img",
      "main/fs-abc/s1/b",
      "main/fs-abc/s1/c",
      "main/fs-abc/s2/a",
    ]);
    assert.deepEqual(listed.map(({ key, writtenAt }) => [key, writtenAt]), listed.map(({ key }) => [key, s3.writtenAt.get(key)]));
    assert.equal(listed[2]?.writtenAt, Date.parse("2026-10-01T12:00:00.000Z"));
    assert.equal(s3.requests.filter((request) => request.startsWith("GET ?")).length, 3);

    await run(bucket.remove("main/fs-abc/s2/a"));
    await run(bucket.remove("main/fs-abc/s2/a"));
    assert.equal(s3.objects.has("main/fs-abc/s2/a"), false);

    // Removed as of the ETag it was read with alone: not once written over since, nor once gone.
    const read = await run(bucket.getTaggedJson("main/fs-abc/current.json"));
    assert.deepEqual(read?.value, { snapshot: "s1" });
    assert.equal(await run(bucket.getTaggedJson("main/fs-none/current.json")), null);
    await run(bucket.putJson("main/fs-abc/current.json", { snapshot: "s2" }));
    assert.equal(await run(bucket.removeIf("main/fs-abc/current.json", read?.etag ?? "")), false);
    assert.deepEqual(await run(bucket.getJson("main/fs-abc/current.json")), { snapshot: "s2" });
    const again = await run(bucket.getTaggedJson("main/fs-abc/current.json"));
    assert.equal(await run(bucket.removeIf("main/fs-abc/current.json", again?.etag ?? "")), true);
    assert.equal(s3.objects.has("main/fs-abc/current.json"), false);
    assert.equal(await run(bucket.removeIf("main/fs-abc/current.json", again?.etag ?? "")), false);
    // Refused as one written at once (409), it is left as it is, to be read again.
    await run(bucket.putJson("main/fs-abc/current.json", { snapshot: "s3" }));
    const contended = await run(bucket.getTaggedJson("main/fs-abc/current.json"));
    s3.refuse = (method) => (method === "DELETE" ? 409 : null);
    assert.equal(await run(bucket.removeIf("main/fs-abc/current.json", contended?.etag ?? "")), false);
    s3.refuse = () => null;
    assert.deepEqual(await run(bucket.getJson("main/fs-abc/current.json")), { snapshot: "s3" });
    // Of a store that answers a get with no ETag, JSON is read all the same; only what is to be removed as read is not.
    s3.getsTagged = false;
    assert.deepEqual(await run(bucket.getJson("main/fs-abc/current.json")), { snapshot: "s3" });
    const untagged = await Effect.runPromiseExit(bucket.getTaggedJson("main/fs-abc/current.json"));
    assert.ok(Exit.isFailure(untagged));
    assert.match(String(untagged.cause), /the bucket's read of main\/fs-abc\/current\.json failed: it was answered with no ETag/u);
    s3.getsTagged = true;

    const missing = await Effect.runPromiseExit(bucket.getFile("main/fs-abc/s9/none", join(directory, "none"), 10));
    assert.ok(Exit.isFailure(missing));
    assert.match(String(missing.cause), /the bucket's download of main\/fs-abc\/s9\/none failed: 404 NoSuchKey The specified key does not exist\./u);

    const stranger = makeS3Bucket({ endpoint: s3.endpoint, region: "us-east-1", bucket: "snapshots", ...CREDENTIALS, secretAccessKey: "another" });
    const refused = await Effect.runPromiseExit(stranger.getJson("main/fs-abc/current.json"));
    assert.ok(Exit.isFailure(refused));
    assert.match(String(refused.cause), /the bucket's read of main\/fs-abc\/current\.json failed: 403 SignatureDoesNotMatch/u);
  } finally {
    await s3.close();
    rmSync(directory, { recursive: true, force: true });
  }
});

test("JSON is put as of the ETag the bucket holds it by, or of none, and refused otherwise; a store is proved to honour such conditions, and one that ignores them refused, the object it proved them with gone", async () => {
  const s3 = await startFakeS3({ bucket: "snapshots", region: "us-east-1", ...CREDENTIALS });
  try {
    const bucket = makeS3Bucket({ endpoint: s3.endpoint, region: "us-east-1", bucket: "snapshots", ...CREDENTIALS });
    const run = <A, E>(effect: Effect.Effect<A, E>) => Effect.runPromise(effect);
    const key = "main/fs-abc/current.json";
    // Of none, where there is none, and not again.
    assert.equal(await run(bucket.putJsonIf(key, { snapshot: "s1" }, null)), true);
    assert.equal(await run(bucket.putJsonIf(key, { snapshot: "s0" }, null)), false);
    const read = await run(bucket.getTaggedJson(key));
    assert.deepEqual(read?.value, { snapshot: "s1" });
    // As of the ETag it was read with, and not once written over since, nor once gone.
    await run(bucket.putJson(key, { snapshot: "s2" }));
    assert.equal(await run(bucket.putJsonIf(key, { snapshot: "s3" }, read?.etag ?? "")), false);
    assert.deepEqual(await run(bucket.getJson(key)), { snapshot: "s2" });
    const again = await run(bucket.getTaggedJson(key));
    assert.equal(await run(bucket.putJsonIf(key, { snapshot: "s3" }, again?.etag ?? "")), true);
    assert.deepEqual(await run(bucket.getJson(key)), { snapshot: "s3" });
    await run(bucket.remove(key));
    assert.equal(await run(bucket.putJsonIf(key, { snapshot: "s4" }, again?.etag ?? "")), false);
    assert.equal(s3.objects.has(key), false);
    // A write of it at once (409) is a refusal, to read again.
    s3.refuse = (method) => (method === "PUT" ? 409 : null);
    assert.equal(await run(bucket.putJsonIf(key, { snapshot: "s4" }, null)), false);
    s3.refuse = () => null;

    // What a probe stopped as it probed left, of over an hour ago, goes; one of another agent's probing now stays.
    await run(bucket.putJson(".alasio/probes/left.json", { probe: 1 }));
    await run(bucket.putJson(".alasio/probes/probing.json", { probe: 1 }));
    s3.writtenAt.set(".alasio/probes/left.json", Date.now() - 61 * 60_000);
    await run(bucket.checkConditions);
    const probes = () => [...s3.objects.keys()].filter((name) => name.startsWith(".alasio/probes/"));
    assert.deepEqual(probes(), [".alasio/probes/probing.json"]);
    await run(bucket.remove(".alasio/probes/probing.json"));
    assert.ok(s3.requests.some((asked) => asked.startsWith("PUT .alasio/probes/")));
    // A store that refuses every conditional PUT, or says it implements none (501), is refused too.
    for (const [status, said] of [[412, /a PUT as of the ETag it holds \(If-Match\), which it refuses/u], [501, /a PUT as of an ETag \(If-Match\), which it says it does not implement/u]] as const) {
      let puts = 0;
      s3.refuse = (method, key) => (method === "PUT" && key.startsWith(".alasio/probes/") && ++puts > 1 ? status : null);
      const unconditional = await Effect.runPromiseExit(bucket.checkConditions);
      assert.ok(Exit.isFailure(unconditional));
      assert.match(String(unconditional.cause), said);
      s3.refuse = () => null;
      assert.deepEqual(probes(), []);
    }
    s3.honoursConditions = false;
    const refused = await Effect.runPromiseExit(bucket.checkConditions);
    assert.ok(Exit.isFailure(refused));
    assert.match(String(refused.cause), /StoreUnconditional[\s\S]*a PUT as of an ETag it does not hold \(If-Match\)/u);
    assert.deepEqual(probes(), []);
  } finally {
    await s3.close();
  }
});

test("a file larger than a part is put in parts, some at a time, and got in ranges; one larger than a copy is copied in parts; an upload that fails is aborted", async () => {
  const s3 = await startFakeS3({ bucket: "snapshots", region: "us-east-1", ...CREDENTIALS });
  const directory = mkdtempSync(join(tmpdir(), "alasio-node-agent-s3-"));
  try {
    const bucket = makeS3Bucket({ endpoint: s3.endpoint, region: "us-east-1", bucket: "snapshots", ...CREDENTIALS, partBytes: 1000, parallel: 2, syncBytes: 300, copyBytes: 1500 });
    const run = <A, E>(effect: Effect.Effect<A, E>) => Effect.runPromise(effect);
    const pages = Buffer.from(Array.from({ length: 3500 }, (_, index) => index % 251));
    const source = join(directory, "pages.img");
    writeFileSync(source, pages);
    const key = "main/fs-abc/s1/pages.img";

    // Four parts, the last the rest, put together in order: the bucket's ETag of the whole,
    // which is not its MD5, as it says it of the object.
    const etag = await run(bucket.putFile(key, source));
    assert.deepEqual(s3.objects.get(key), pages);
    assert.deepEqual(s3.requests.filter((asked) => asked.startsWith(`PUT ${key} part`)).toSorted(), [1, 2, 3, 4].map((part) => `PUT ${key} part ${part}`));
    assert.deepEqual([s3.requests[0], s3.requests.at(-1)], [`POST ${key}?uploads`, `POST ${key} complete`]);
    assert.match(etag, /^"[0-9a-f]{32}-4"$/u);
    assert.deepEqual(await run(bucket.head(key)), { bytes: pages.length, etag });
    // A file of a part or less is put whole.
    writeFileSync(join(directory, "small.img"), pages.subarray(0, 1000));
    await run(bucket.putFile("main/fs-abc/s1/small.img", join(directory, "small.img")));
    assert.equal(s3.requests.at(-1), "PUT main/fs-abc/s1/small.img");

    // Got in ranges of a part, each where it goes.
    s3.requests.length = 0;
    const received = await run(bucket.getFile(key, join(directory, "copy.img"), pages.length));
    assert.deepEqual(received, { bytes: pages.length, sha256: createHash("sha256").update(pages).digest("hex") });
    assert.deepEqual(readFileSync(join(directory, "copy.img")), pages);
    assert.deepEqual(s3.requests.toSorted(), ["bytes=0-999", "bytes=1000-1999", "bytes=2000-2999", "bytes=3000-3499"].map((range) => `GET ${key} ${range}`));
    // One shorter than its manifest says fails as it is got.
    s3.objects.set(key, pages.subarray(0, 2500));
    const short = await Effect.runPromiseExit(bucket.getFile(key, join(directory, "short.img"), pages.length));
    assert.ok(Exit.isFailure(short));
    assert.match(String(short.cause), /the bucket's download of main\/fs-abc\/s1\/pages\.img failed: (416 InvalidRange|its range from byte 2000 was answered 206 with 500 bytes, not 1000)/u);
    s3.objects.set(key, pages);

    // Copied in parts of the most copied at once.
    s3.requests.length = 0;
    await run(bucket.copy(key, "main/fs-new/s1/pages.img", pages.length));
    assert.deepEqual(s3.objects.get("main/fs-new/s1/pages.img"), pages);
    assert.deepEqual(s3.requests.filter((asked) => asked.includes(" part ")).toSorted(), [1, 2, 3].map((part) => `PUT main/fs-new/s1/pages.img part ${part} from ${key}`));

    // An upload one of whose parts fails is aborted, leaving no object and no upload.
    s3.refuse = (method, refused) => (method === "PUT" && refused === "main/fs-abc/s2/pages.img" && s3.requests.at(-1)?.endsWith("part 3") ? 503 : null);
    const failed = await Effect.runPromiseExit(bucket.putFile("main/fs-abc/s2/pages.img", source));
    assert.ok(Exit.isFailure(failed));
    assert.match(String(failed.cause), /the bucket's upload of main\/fs-abc\/s2\/pages\.img failed: 503 InternalError/u);
    assert.ok(s3.requests.includes("DELETE main/fs-abc/s2/pages.img abort"));
    assert.equal(s3.objects.has("main/fs-abc/s2/pages.img"), false);
    assert.equal(s3.uploads.size, 0);
  } finally {
    await s3.close();
    rmSync(directory, { recursive: true, force: true });
  }
});

test("a file put once it is ready sends each part, or the whole of one put whole, only once it is, told the byte the part ends before", async () => {
  const s3 = await startFakeS3({ bucket: "snapshots", region: "us-east-1", ...CREDENTIALS });
  const directory = mkdtempSync(join(tmpdir(), "alasio-node-agent-s3-"));
  try {
    const bucket = makeS3Bucket({ endpoint: s3.endpoint, region: "us-east-1", bucket: "snapshots", ...CREDENTIALS, partBytes: 1000, parallel: 2 });
    const pages = Buffer.from(Array.from({ length: 3500 }, (_, index) => index % 251));
    const source = join(directory, "pages.img");
    writeFileSync(source, pages);
    writeFileSync(join(directory, "small.img"), pages.subarray(0, 1000));
    /** Readiness asked for, each let go by the test in turn. */
    const asked: (() => void)[] = [];
    /** The byte each part asking ends before, in the order they asked. */
    const ends: number[] = [];
    const ready = (end: number) => new Promise<void>((resolve) => (ends.push(end), asked.push(resolve)));
    const settled = () => new Promise((resolve) => setTimeout(resolve, 100));
    const parts = (key: string) => s3.requests.filter((request) => request.startsWith(`PUT ${key}`)).length;

    const putting = Effect.runPromise(bucket.putFile("main/fs-abc/s1/pages.img", source, ready));
    await settled();
    // Begun, as its first two parts ask whether they may go, but none sent.
    assert.equal(asked.length, 2);
    assert.equal(parts("main/fs-abc/s1/pages.img"), 0);
    asked.shift()?.();
    await settled();
    assert.equal(parts("main/fs-abc/s1/pages.img"), 1);
    // Each part after it waits on readiness too.
    while (asked.length > 0) {
      asked.shift()?.();
      await settled();
    }
    await putting;
    assert.deepEqual(s3.objects.get("main/fs-abc/s1/pages.img"), pages);
    assert.equal(parts("main/fs-abc/s1/pages.img"), 4);
    assert.deepEqual(ends.splice(0), [1000, 2000, 3000, 3500]);

    const whole = Effect.runPromise(bucket.putFile("main/fs-abc/s1/small.img", join(directory, "small.img"), ready));
    await settled();
    assert.equal(parts("main/fs-abc/s1/small.img"), 0);
    asked.shift()?.();
    await whole;
    assert.equal(parts("main/fs-abc/s1/small.img"), 1);
    assert.deepEqual(ends, [1000]);
  } finally {
    await s3.close();
    rmSync(directory, { recursive: true, force: true });
  }
});

test("the multipart uploads begun under a prefix and left, across pages of their listing, are aborted, and those elsewhere left be", async () => {
  const s3 = await startFakeS3({ bucket: "snapshots", region: "us-east-1", ...CREDENTIALS });
  try {
    const bucket = makeS3Bucket({ endpoint: s3.endpoint, region: "us-east-1", bucket: "snapshots", ...CREDENTIALS });
    for (const [id, key] of [["upload-1", "main/fs-abc/layers/d1/pages.img"], ["upload-2", "main/fs-abc/layers/d1/pages.img"], ["upload-3", "main/fs-abc/layers/d1/checkpoint.img"], ["upload-4", "main/fs-def/layers/d1/pages.img"]] as const) {
      s3.uploads.set(id, { key, parts: new Map() });
    }
    // The stand-in lists two a page.
    assert.equal(await Effect.runPromise(bucket.abortUploads("main/fs-abc/layers/d1/")), 3);
    assert.deepEqual([...s3.uploads.keys()], ["upload-4"]);
    assert.equal(s3.requests.filter((asked) => asked.endsWith(" abort")).length, 3);
    assert.equal(await Effect.runPromise(bucket.abortUploads("main/fs-abc/layers/d1/")), 0);
  } finally {
    await s3.close();
  }
});

test("a versioned bucket's versions and delete markers under a prefix are listed across pages, in order, and versions are deleted for good, a thousand a request, saying those the bucket did not delete, one gone already gone; a request that fails fails the deletion", async () => {
  const s3 = await startFakeS3({ bucket: "neon", region: "us-east-1", ...CREDENTIALS });
  try {
    const bucket = makeS3Bucket({ endpoint: s3.endpoint, region: "us-east-1", bucket: "neon", ...CREDENTIALS });
    const at = Date.parse("2026-11-20T12:00:00.000Z");
    const listed = [
      { key: "pageserver/a&b", versionId: "v3", writtenAt: at, bytes: 0, latest: true, deleteMarker: true },
      { key: "pageserver/a&b", versionId: "v2", writtenAt: at - 1000, bytes: 7, latest: false, deleteMarker: false },
      { key: "pageserver/a&b", versionId: "v1", writtenAt: at - 2000, bytes: 5, latest: false, deleteMarker: false },
      { key: "pageserver/c", versionId: "v1", writtenAt: at - 3000, bytes: 9, latest: true, deleteMarker: false },
      { key: "safekeeper/d", versionId: "v1", writtenAt: at - 4000, bytes: 16, latest: true, deleteMarker: false },
    ];
    s3.versions.push(...listed);
    // The stand-in lists two a page.
    assert.deepEqual(await Effect.runPromise(bucket.listVersions("pageserver/")), listed.slice(0, 4));
    assert.deepEqual(s3.requests.filter((asked) => asked.startsWith("GET ?")).length, 2);
    assert.deepEqual(await Effect.runPromise(bucket.listVersions("")), listed);

    // In the order given, one gone already among them.
    assert.deepEqual(await Effect.runPromise(bucket.removeVersions([{ key: "pageserver/a&b", versionId: "v1" }, { key: "pageserver/a&b", versionId: "v2" }, { key: "pageserver/gone", versionId: "v9" }, { key: "pageserver/a&b", versionId: "v3" }])), []);
    assert.deepEqual(s3.versions.map(({ key }) => key), ["pageserver/c", "safekeeper/d"]);
    assert.equal(s3.requests.filter((asked) => asked === "POST ?delete=").length, 1);

    // A thousand a request.
    const many = Array.from({ length: 1001 }, (_, index) => ({ key: `pageserver/m${index}`, versionId: "v1", writtenAt: at, bytes: 0, latest: true, deleteMarker: true }));
    s3.versions.push(...many);
    assert.deepEqual(await Effect.runPromise(bucket.removeVersions(many)), []);
    assert.equal(s3.requests.filter((asked) => asked === "POST ?delete=").length, 3);
    assert.equal(s3.versions.length, 2);

    // Gone already, as S3 may say it, is deleted; refused otherwise, it is said, and the versions after it in the request
    // deleted, as S3 goes on past one it does not delete.
    s3.versions.push({ key: "pageserver/e", versionId: "v1", writtenAt: at, bytes: 1, latest: true, deleteMarker: false });
    s3.refuseVersion = (key) => (key === "pageserver/c" ? "NoSuchVersion" : key === "safekeeper/d" ? "AccessDenied" : null);
    assert.deepEqual(await Effect.runPromise(bucket.removeVersions([{ key: "pageserver/c", versionId: "v1" }, { key: "safekeeper/d", versionId: "v1" }, { key: "pageserver/e", versionId: "v1" }])), [
      { key: "safekeeper/d", versionId: "v1", code: "AccessDenied", message: "refused, as the test asks" },
    ]);
    assert.deepEqual(s3.versions.map(({ key }) => key), ["pageserver/c", "safekeeper/d"]);
    // A request that fails fails the deletion.
    s3.refuse = (method) => (method === "POST" ? 503 : null);
    const failed = await Effect.runPromiseExit(bucket.removeVersions([{ key: "pageserver/c", versionId: "v1" }]));
    assert.ok(Exit.isFailure(failed));
    assert.match(String(failed.cause), /the bucket's deletion of versions of pageserver\/c failed: 503/u);
  } finally {
    await s3.close();
  }
});

test("a listing of versions cut short that names no key or version to go on from, or that is no listing, fails, as does a deletion answered with no result of one, or with an error of no version, so that nothing is taken of either", async () => {
  const s3 = await startFakeS3({ bucket: "neon", region: "us-east-1", ...CREDENTIALS });
  try {
    const bucket = makeS3Bucket({ endpoint: s3.endpoint, region: "us-east-1", bucket: "neon", ...CREDENTIALS });
    const at = Date.parse("2026-11-20T12:00:00.000Z");
    s3.versions.push(...["a", "b", "c"].map((key) => ({ key: `pageserver/${key}`, versionId: "v1", writtenAt: at, bytes: 1, latest: true, deleteMarker: false })));
    const failure = async <A>(effect: Effect.Effect<A, unknown>) => {
      const exit = await Effect.runPromiseExit(effect);
      assert.ok(Exit.isFailure(exit));
      return String(exit.cause);
    };
    // Cut short, the two a page, naming no version, or an empty key, to go on from.
    s3.answers = (asked, body) => (/^GET \?.*\bversions=/u.test(asked) ? body.replace(/<NextVersionIdMarker>[^<]*<\/NextVersionIdMarker>/u, "") : body);
    assert.match(await failure(bucket.listVersions("")), /its listing went on past a page that names no key and version to go on from \(NextKeyMarker "pageserver\/b", NextVersionIdMarker ""\)/u);
    s3.answers = (asked, body) => (/^GET \?.*\bversions=/u.test(asked) ? body.replace(/<NextKeyMarker>[^<]*<\/NextKeyMarker>/u, "<NextKeyMarker></NextKeyMarker>") : body);
    assert.match(await failure(bucket.listVersions("")), /names no key and version to go on from/u);
    s3.answers = (asked, body) => (/^GET \?.*\bversions=/u.test(asked) ? "<html>a proxy's page</html>" : body);
    assert.match(await failure(bucket.listVersions("")), /it answered no listing of versions: <html>/u);
    s3.answers = (_asked, body) => body;
    assert.equal((await Effect.runPromise(bucket.listVersions(""))).length, 3);

    // A deletion answered with no result of one, or with an error that names no version.
    s3.answers = (asked, body) => (asked.startsWith("POST ?delete") ? "<html>a proxy's page</html>" : body);
    assert.match(await failure(bucket.removeVersions([{ key: "pageserver/a", versionId: "v1" }])), /it answered no result of a deletion: <html>/u);
    s3.answers = (asked, body) => (asked.startsWith("POST ?delete") ? "<DeleteResult><Error><Code>InternalError</Code><Message>it failed</Message></Error></DeleteResult>" : body);
    assert.match(await failure(bucket.removeVersions([{ key: "pageserver/b", versionId: "v1" }])), /it answered an error of no version: InternalError it failed/u);
  } finally {
    await s3.close();
  }
});

test("a versioned bucket of more versions than a page of its listing holds, a thousand, is listed whole across pages, a key's versions across a page's end among them", async () => {
  const s3 = await startFakeS3({ bucket: "neon", region: "us-east-1", ...CREDENTIALS, pageSize: 1000 });
  try {
    const bucket = makeS3Bucket({ endpoint: s3.endpoint, region: "us-east-1", bucket: "neon", ...CREDENTIALS });
    const at = Date.parse("2026-11-20T12:00:00.000Z");
    // 2500 versions: keys of one version each, but for one of four versions, the 998th to the 1001st listed, across the first
    // page's end, and in its order, its latest a delete marker.
    const one = (index: number) => ({ key: `pageserver/k${String(index).padStart(5, "0")}`, versionId: "v1", writtenAt: at - index, bytes: index, latest: true, deleteMarker: false });
    const spanning = Array.from({ length: 4 }, (_, index) => ({ key: "pageserver/k00997-span", versionId: `s${4 - index}`, writtenAt: at - index * 1000, bytes: index === 0 ? 0 : 10, latest: index === 0, deleteMarker: index === 0 }));
    const listed = [...Array.from({ length: 997 }, (_, index) => one(index)), ...spanning, ...Array.from({ length: 1499 }, (_, index) => one(index + 997))];
    s3.versions.push(...listed);
    assert.equal(listed.length, 2500);
    const got = await Effect.runPromise(bucket.listVersions(""));
    assert.deepEqual(got, listed);
    assert.deepEqual(got.slice(997, 1001).map(({ versionId }) => versionId), ["s4", "s3", "s2", "s1"]);
    assert.equal(s3.requests.filter((asked) => asked.startsWith("GET ?")).length, 3);
    assert.deepEqual(await Effect.runPromise(bucket.removeVersions(got)), []);
    assert.equal(s3.requests.filter((asked) => asked === "POST ?delete=").length, 3);
    assert.deepEqual(s3.versions, []);
  } finally {
    await s3.close();
  }
});

test("the bucket's gets go `parallel` at a time, every one of its files together, each written synced to the disk", async () => {
  const s3 = await startFakeS3({ bucket: "snapshots", region: "us-east-1", ...CREDENTIALS });
  const directory = mkdtempSync(join(tmpdir(), "alasio-node-agent-s3-"));
  try {
    const bucket = makeS3Bucket({ endpoint: s3.endpoint, region: "us-east-1", bucket: "snapshots", ...CREDENTIALS, partBytes: 1000, parallel: 2, syncBytes: 300 });
    const pages = Buffer.from(Array.from({ length: 3500 }, (_, index) => index % 251));
    const small = pages.subarray(0, 700);
    s3.objects.set("main/fs-a/s1/pages.img", pages);
    s3.objects.set("main/fs-b/s1/pages.img", pages);
    s3.objects.set("main/fs-c/s1/small.img", small);
    s3.getDelayMs = 30;
    // Two snapshots' images in ranges, and a small file whole, at once.
    const got = await Promise.all([
      Effect.runPromise(bucket.getFile("main/fs-a/s1/pages.img", join(directory, "a.img"), pages.length)),
      Effect.runPromise(bucket.getFile("main/fs-b/s1/pages.img", join(directory, "b.img"), pages.length)),
      Effect.runPromise(bucket.getFile("main/fs-c/s1/small.img", join(directory, "c.img"), small.length)),
    ]);
    assert.equal(s3.mostGetting(), 2);
    const sha256 = (bytes: Buffer) => createHash("sha256").update(bytes).digest("hex");
    assert.deepEqual(got, [{ bytes: pages.length, sha256: sha256(pages) }, { bytes: pages.length, sha256: sha256(pages) }, { bytes: small.length, sha256: sha256(small) }]);
    assert.deepEqual([readFileSync(join(directory, "a.img")), readFileSync(join(directory, "b.img")), readFileSync(join(directory, "c.img"))], [pages, pages, small]);
  } finally {
    await s3.close();
    rmSync(directory, { recursive: true, force: true });
  }
});

test("a get holds the bucket to the size its file is said to be, a JSON object to a manifest's, and one range that fails stops the rest and lets go of the budget", async () => {
  const s3 = await startFakeS3({ bucket: "snapshots", region: "us-east-1", ...CREDENTIALS });
  const directory = mkdtempSync(join(tmpdir(), "alasio-node-agent-s3-"));
  try {
    const bucket = makeS3Bucket({ endpoint: s3.endpoint, region: "us-east-1", bucket: "snapshots", ...CREDENTIALS, partBytes: 1000, parallel: 2, syncBytes: 300 });
    const pages = Buffer.from(Array.from({ length: 3500 }, (_, index) => index % 251));
    const failure = async <A>(effect: Effect.Effect<A, unknown>) => {
      const exit = await Effect.runPromiseExit(effect);
      assert.ok(Exit.isFailure(exit));
      return String(exit.cause);
    };

    // An object larger than its manifest says, got whole: nothing past what it is said to be is written.
    s3.objects.set("main/fs-a/s1/small.img", pages.subarray(0, 900));
    assert.match(await failure(bucket.getFile("main/fs-a/s1/small.img", join(directory, "small.img"), 600)), /it holds more than the 600 bytes it is said to/u);
    assert.ok(readFileSync(join(directory, "small.img")).length <= 600);

    // A JSON object larger than any manifest is refused, not read whole.
    s3.objects.set("main/fs-a/current.json", Buffer.from(JSON.stringify({ padding: "x".repeat(2 * 1024 * 1024) })));
    assert.match(await failure(bucket.getJson("main/fs-a/current.json")), /the bucket's read of main\/fs-a\/current\.json failed: it holds more than 1048576 bytes/u);

    // The second range fails at once while the first is slow: the first is aborted, no other starts, and the budget is free.
    s3.objects.set("main/fs-b/s1/pages.img", pages);
    s3.getDelayMs = 5000;
    s3.refuse = (method, _key, asked) => (method === "GET" && asked.endsWith("bytes=1000-1999") ? 503 : null);
    s3.requests.length = 0;
    const started = Date.now();
    assert.match(await failure(bucket.getFile("main/fs-b/s1/pages.img", join(directory, "pages.img"), pages.length)), /503 InternalError/u);
    assert.ok(Date.now() - started < 2500, `the failure took ${Date.now() - started} ms, as long as the slow range`);
    assert.deepEqual(s3.requests.toSorted(), ["GET main/fs-b/s1/pages.img bytes=0-999", "GET main/fs-b/s1/pages.img bytes=1000-1999"]);
    s3.getDelayMs = 0;
    s3.refuse = () => null;
    s3.objects.set("main/fs-c/s1/a.img", pages.subarray(0, 500));
    s3.objects.set("main/fs-c/s1/b.img", pages.subarray(0, 500));
    const got = await Promise.all(["a", "b"].map((name) => Effect.runPromise(bucket.getFile(`main/fs-c/s1/${name}.img`, join(directory, `${name}.img`), 500))));
    assert.deepEqual(got.map(({ bytes }) => bytes), [500, 500]);
  } finally {
    await s3.close();
    rmSync(directory, { recursive: true, force: true });
  }
});
