import assert from "node:assert/strict";
import { test } from "node:test";

import { Result } from "effect";

import { exitCodeOf, mergePatch, patchBody } from "../src/kube/client.ts";

test("a merge patch makes one object into another, removing what the other lacks and giving arrays whole", () => {
  const from = { a: 1, b: { c: 2, d: 3 }, e: [1, 2], f: "same", g: { h: 1 } };
  const to = { a: 1, b: { c: 2, x: 4 }, e: [1], f: "same", g: { h: 1 }, i: { j: true }, k: undefined };
  const patch = mergePatch(from, to);
  assert.deepEqual(patch, { b: { d: null, x: 4 }, e: [1], i: { j: true } });
  assert.deepEqual(mergePatch(to, to), {});
  assert.deepEqual(mergePatch({ a: 1 }, { a: null }), { a: null });
  assert.equal(mergePatch(1, 2), 2);
});

test("a patch is sent addressed to its object, beside what it changes in its metadata", () => {
  const pod = ["v1", "Pod", "alasio-sessions", "sandboxes-none-7d9f8-x2v5q"] as const;
  assert.deepEqual(patchBody(...pod, { spec: { activeDeadlineSeconds: 60 } }), {
    apiVersion: "v1",
    kind: "Pod",
    metadata: { namespace: "alasio-sessions", name: "sandboxes-none-7d9f8-x2v5q" },
    spec: { activeDeadlineSeconds: 60 },
  });
  assert.deepEqual(patchBody(...pod, { metadata: { annotations: { a: "1" }, name: "other" }, spec: {} }), {
    apiVersion: "v1",
    kind: "Pod",
    metadata: { annotations: { a: "1" }, namespace: "alasio-sessions", name: "sandboxes-none-7d9f8-x2v5q" },
    spec: {},
  });
});

test("an exec's exit code is read from its status", () => {
  assert.deepEqual(exitCodeOf({ status: "Success" }), Result.succeed(0));
  assert.deepEqual(exitCodeOf({ status: "Failure", reason: "NonZeroExitCode", details: { causes: [{ reason: "ExitCode", message: "3" }] } }), Result.succeed(3));
  const failed = exitCodeOf({ status: "Failure", message: "container not found" });
  assert.ok(Result.isFailure(failed));
  assert.equal(failed.failure.message, "exec failed: container not found");
});
