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

import { makeS3Bucket, sigV4Authorization } from "../src/node-agent/s3.ts";
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

const CREDENTIALS = { accessKeyId: "snapshots0123", secretAccessKey: "a secret" };

test("the bucket takes files and JSON, gives them back, says what it holds, copies within itself, lists a prefix across pages, and deletes, every request signed", async () => {
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
