/**
 * The URLs the node agent gives a restore's checkpoint gofer to read the bucket by
 * (src/node-agent/store-urls.ts): at the store's endpoint as the node reaches it.
 */
import assert from "node:assert/strict";
import { test } from "node:test";

import { Effect, Exit } from "effect";

import { isServiceName, makeStoreUrls, SIGNED_BEFORE_MS } from "../src/node-agent/store-urls.ts";
import { startFakeS3 } from "./support/fake-s3.ts";

const CREDENTIALS = { accessKeyId: "snapshots0123", secretAccessKey: "a secret" };

test("a Service's name of the cluster's is one the node does not resolve; an operator's store's is not", () => {
  for (const name of ["alasio-seaweedfs.alasio.svc", "alasio-seaweedfs.alasio.svc.cluster.local"]) assert.ok(isServiceName(name), name);
  for (const name of ["s3.example.com", "s3.svcs.example.com", "127.0.0.1", "minio.lan"]) assert.ok(!isServiceName(name), name);
});

test("a URL at a Service's endpoint is signed for the address the agent resolves its name to, as the node reaches it, resolved once in a while; one at an operator's store's endpoint as it is, resolved by no one but the node; each dated a little before the agent's clock", async () => {
  const s3 = await startFakeS3({ bucket: "snapshots", region: "us-east-1", ...CREDENTIALS });
  try {
    s3.objects.set("main/fs-abc/layers/d1/pages.img", Buffer.from("pages"));
    const port = new URL(s3.endpoint).port;
    const resolved: string[] = [];
    const urlOf = makeStoreUrls({
      store: { endpoint: `http://alasio-seaweedfs.alasio.svc:${port}`, region: "us-east-1", bucket: "snapshots", ...CREDENTIALS },
      resolve: async (hostname) => (resolved.push(hostname), "127.0.0.1"),
    });
    const first = await Effect.runPromise(urlOf("main/fs-abc/layers/d1/pages.img", 60));
    const second = await Effect.runPromise(urlOf("main/fs-abc/layers/d1/pages_meta.img", 60));
    assert.equal(new URL(first).host, `127.0.0.1:${port}`);
    assert.equal(new URL(second).host, `127.0.0.1:${port}`);
    assert.deepEqual(resolved, ["alasio-seaweedfs.alasio.svc"]);
    // Signed for the address it is reached at: the store takes it.
    const got = await fetch(first);
    assert.equal(got.status, 200);
    assert.equal(Buffer.from(await got.arrayBuffer()).toString(), "pages");

    // An address of IPv6, in brackets.
    const six = makeStoreUrls({ store: { endpoint: "http://alasio-seaweedfs.alasio.svc:8333", region: "us-east-1", bucket: "snapshots", ...CREDENTIALS }, resolve: async () => "fd00:43::10" });
    assert.equal(new URL(await Effect.runPromise(six("k", 60))).host, "[fd00:43::10]:8333");

    // An operator's store: as it is, its name resolved by the node.
    const external = makeStoreUrls({ store: { endpoint: "https://s3.example.com", region: "eu-west-1", bucket: "snapshots", ...CREDENTIALS }, resolve: async (hostname) => assert.fail(`resolved ${hostname}`) });
    const url = new URL(await Effect.runPromise(external("main/fs-abc/layers/d1/pages.img", 60)));
    assert.equal(url.origin, "https://s3.example.com");
    assert.match(url.searchParams.get("X-Amz-Credential") ?? "", /^snapshots0123\/\d{8}\/eu-west-1\/s3\/aws4_request$/u);

    // Dated SIGNED_BEFORE_MS before the agent's clock, expiring as asked all the same: a store whose clock runs behind takes it.
    const clock = Date.UTC(2026, 10, 25, 12, 0, 0);
    const dated = new URL(await Effect.runPromise(makeStoreUrls({ store: { endpoint: "https://s3.example.com", region: "eu-west-1", bucket: "snapshots", ...CREDENTIALS }, now: () => clock })("k", 60)));
    assert.equal(dated.searchParams.get("X-Amz-Date"), "20261125T115930Z");
    assert.equal(dated.searchParams.get("X-Amz-Expires"), String(60 + SIGNED_BEFORE_MS / 1000));
    s3.clockBehindMs = 20_000;
    const behind = await fetch(await Effect.runPromise(urlOf("main/fs-abc/layers/d1/pages.img", 60)));
    assert.equal(behind.status, 200);
    s3.clockBehindMs = 0;

    // A name that does not resolve fails the signing, said so.
    const lost = makeStoreUrls({ store: { endpoint: "http://gone.alasio.svc:8333", region: "us-east-1", bucket: "snapshots", ...CREDENTIALS }, resolve: async () => Promise.reject(new Error("ENOTFOUND gone.alasio.svc")) });
    const exit = await Effect.runPromise(Effect.exit(lost("k", 60)));
    assert.ok(Exit.isFailure(exit));
    assert.match(String(exit.cause), /the store's Service did not resolve: ENOTFOUND/u);
  } finally {
    await s3.close();
  }
});
