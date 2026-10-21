import assert from "node:assert/strict";
import { setTimeout as sleep } from "node:timers/promises";
import { test } from "node:test";

import { KubeConfig } from "@kubernetes/client-node";
import { Effect, Exit, Result, Stream } from "effect";

import { exitCodeOf, makeKubeClient, mergePatch, patchBody } from "../src/kube/client.ts";
import { startKubeApiServer } from "./support/kube-apiserver.ts";

/** How long the silent watch below may hear nothing: far less than the minute between the API server's bookmarks. */
const SILENCE_MS = 1_000;

/** A client of a real API server, made with `options`, and a namespace of its own made for the test. */
async function watching(namespace: string, options?: Parameters<typeof makeKubeClient>[1]) {
  const kubeConfig = new KubeConfig();
  kubeConfig.loadFromFile((await startKubeApiServer()).kubeconfig);
  const kube = makeKubeClient(kubeConfig, options);
  await Effect.runPromise(kube.create({ apiVersion: "v1", kind: "Namespace", metadata: { name: namespace } }));
  return kube;
}

/** The test's Leases, as they change, from a watch run until `stopped` aborts: what each says it saw (its type, and the Lease's beat), and how the watch ended. */
function watchBeats(kube: ReturnType<typeof makeKubeClient>, namespace: string, stopped: AbortSignal) {
  const seen: string[] = [];
  const ended = Effect.runPromiseExit(
    Stream.runForEach(kube.watch("coordination.k8s.io/v1", "leases", namespace, "alasio.dev/test=watch"), ({ type, object }) =>
      Effect.sync(() => void seen.push(`${type} ${object.metadata?.annotations?.["beat"] ?? ""}`.trimEnd()))),
    { signal: stopped },
  );
  return { seen, ended };
}

test("a watch of something quiet for longer than client-node's 30 s request timeout lives on, and hears a change after it", async () => {
  const namespace = "watch-quiet";
  const kube = await watching(namespace);
  await Effect.runPromise(kube.create({ apiVersion: "coordination.k8s.io/v1", kind: "Lease", metadata: { name: "beat", namespace, labels: { "alasio.dev/test": "watch" }, annotations: { beat: "0" } }, spec: {} }));
  const stopping = new AbortController();
  const { seen, ended } = watchBeats(kube, namespace, stopping.signal);
  let over = false;
  void ended.then(() => { over = true; });
  await sleep(31_000);
  await Effect.runPromise(kube.patch("coordination.k8s.io/v1", "Lease", namespace, "beat", { metadata: { annotations: { beat: "1" } } }));
  for (const deadline = Date.now() + 5_000; !seen.includes("MODIFIED 1");) {
    assert.ok(!over && Date.now() < deadline, `the watch ended, or missed the change, having seen ${seen.join(", ")}`);
    await sleep(20);
  }
  assert.deepEqual(seen.slice(0, 2), ["ADDED 0", "BOOKMARK"]);
  stopping.abort();
  assert.ok(Exit.hasInterrupts(await ended));
});

test("a watch that hears nothing for its silence, not even a bookmark, fails, its connection taken for lost", async () => {
  const namespace = "watch-silent";
  const kube = await watching(namespace, { watchSilenceMs: SILENCE_MS });
  const began = Date.now();
  const { seen, ended } = watchBeats(kube, namespace, new AbortController().signal);
  const exit = await ended;
  const took = Date.now() - began;
  assert.deepEqual(seen, ["BOOKMARK"]);
  assert.ok(Exit.isFailure(exit) && !Exit.hasInterrupts(exit), `the watch ended ${Exit.isSuccess(exit) ? "as the API server ended it" : "interrupted"}`);
  assert.ok(took >= SILENCE_MS && took < 10_000, `the watch failed ${took} ms after it began`);
});

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
