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

test("the bucket takes files and JSON, gives them back, lists a prefix across pages, and deletes, every request signed", async () => {
  const s3 = await startFakeS3({ bucket: "snapshots", region: "us-east-1", ...CREDENTIALS, pageSize: 2 });
  const directory = mkdtempSync(join(tmpdir(), "alasio-node-agent-s3-"));
  try {
    const bucket = makeS3Bucket({ endpoint: s3.endpoint, region: "us-east-1", bucket: "snapshots", ...CREDENTIALS });
    const source = join(directory, "pages.img");
    const pages = Buffer.alloc(300_000, "page");
    writeFileSync(source, pages);
    const run = <A, E>(effect: Effect.Effect<A, E>) => Effect.runPromise(effect);

    // A key of characters a path encodes, streamed up unsigned and down with its hash.
    await run(bucket.putFile("main/fs-abc/s1/a page+1.img", source));
    assert.deepEqual(s3.objects.get("main/fs-abc/s1/a page+1.img"), pages);
    const received = await run(bucket.getFile("main/fs-abc/s1/a page+1.img", join(directory, "copy.img")));
    assert.deepEqual(received, { bytes: pages.length, sha256: createHash("sha256").update(pages).digest("hex") });
    assert.deepEqual(readFileSync(join(directory, "copy.img")), pages);

    await run(bucket.putJson("main/fs-abc/current.json", { snapshot: "s1" }));
    assert.deepEqual(await run(bucket.getJson("main/fs-abc/current.json")), { snapshot: "s1" });
    assert.equal(await run(bucket.getJson("main/fs-none/current.json")), null);

    for (const name of ["s1/b", "s1/c", "s2/a"]) await run(bucket.putJson(`main/fs-abc/${name}`, {}));
    await run(bucket.putJson("main/fs-abcd/current.json", {}));
    // Five keys under the prefix, two to a page.
    assert.deepEqual(await run(bucket.list("main/fs-abc/")), [
      "main/fs-abc/current.json",
      "main/fs-abc/s1/a page+1.img",
      "main/fs-abc/s1/b",
      "main/fs-abc/s1/c",
      "main/fs-abc/s2/a",
    ]);
    assert.equal(s3.requests.filter((request) => request.startsWith("GET ?")).length, 3);

    await run(bucket.remove("main/fs-abc/s2/a"));
    await run(bucket.remove("main/fs-abc/s2/a"));
    assert.equal(s3.objects.has("main/fs-abc/s2/a"), false);

    const missing = await Effect.runPromiseExit(bucket.getFile("main/fs-abc/s9/none", join(directory, "none")));
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
