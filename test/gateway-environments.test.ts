import assert from "node:assert/strict";
import { before, test } from "node:test";

import { Context, Effect, Exit, Layer, Scope } from "effect";

import { branchEnvironmentsOf, makeEnvironments } from "../src/gateway/environments.ts";
import { KubeClient } from "../src/kube/client.ts";
import { startKubeApiServer } from "./support/kube-apiserver.ts";

// The environments of sessions the sandbox service finds, by their namespaces on a real API server.

before(async () => {
  process.env["KUBECONFIG"] = (await startKubeApiServer()).kubeconfig;
});

const MAIN = { name: "main", namespace: "alasio-sessions" };

/** A namespace of the branch `branch`, by its name, branched from main. */
const branchNamespace = (name: string, branch: string) => ({ apiVersion: "v1", kind: "Namespace", metadata: { name, labels: { "alasio.dev/branch": branch, "alasio.dev/branch-parent": "main" } } });

test("a branch environment is its sessions' namespace, named for the branch its label names, its parent the one its other label names; its alasio's, one named for no branch, and one being deleted, are none", () => {
  assert.deepEqual(
    branchEnvironmentsOf([
      branchNamespace("alasio-branch-try-sessions", "try"),
      { metadata: { name: "alasio-branch-orphan-sessions", labels: { "alasio.dev/branch": "orphan" } } },
      branchNamespace("alasio-branch-try", "try"),
      branchNamespace("alasio-branch-other-sessions", "try"),
      { metadata: { name: "alasio-branch-bare-sessions" } },
      { metadata: { ...branchNamespace("alasio-branch-gone-sessions", "gone").metadata, deletionTimestamp: "2026-10-17T00:00:00Z" } },
    ]),
    [{ name: "try", namespace: "alasio-branch-try-sessions", parent: "main" }, { name: "orphan", namespace: "alasio-branch-orphan-sessions", parent: null }],
  );
});

test("the environments are main's and each branch's found, a branch made found as soon as it is asked for, one deleted let go of at the next find, and asks made at once asking the API server at most twice: once, and once more for those made as it was asked", async () => {
  const scope = await Effect.runPromise(Scope.make());
  const client = await Effect.runPromise(Layer.buildWithScope(Layer.fresh(KubeClient.layer), scope));
  const kube = Context.get(client, KubeClient);
  let lists = 0;
  const counted = KubeClient.of({ ...kube, list: (...args) => Effect.suspend(() => ((lists += 1), kube.list(...args))) });
  const create = (object: object) => Effect.runPromise(kube.create(object));
  try {
    await create(branchNamespace("alasio-branch-first-sessions", "first"));
    const environments = await Effect.runPromise(Effect.provideService(makeEnvironments({ main: MAIN, refreshMs: 60_000 }), KubeClient, counted).pipe(Scope.provide(scope)));
    assert.deepEqual(await Effect.runPromise(environments.current), [{ ...MAIN, parent: null }, { name: "first", namespace: "alasio-branch-first-sessions", parent: "main" }]);
    // Made since: found as it is asked for, the asks made at once asking twice at most.
    await create(branchNamespace("alasio-branch-try-sessions", "try"));
    const asked = lists;
    const asks = Array.from({ length: 10 }, (_, n) => (n % 2 === 0 ? environments.named("try") : environments.ofNamespace("alasio-branch-try-sessions")));
    const found = await Effect.runPromise(Effect.all(asks, { concurrency: "unbounded" }));
    assert.deepEqual(found, Array(10).fill({ name: "try", namespace: "alasio-branch-try-sessions", parent: "main" }));
    assert.ok(lists - asked <= 2, `asked ${lists - asked} times`);
    // One there is not: none, once asked anew.
    assert.equal(await Effect.runPromise(environments.named("never")), null);
    // Deleted, its namespace being deleted (as an API server with no controllers leaves it): let go of at the next find.
    await Effect.runPromise(kube.remove("v1", "Namespace", "", "alasio-branch-first-sessions"));
    assert.equal(await Effect.runPromise(environments.named("never")), null);
    assert.deepEqual(await Effect.runPromise(environments.current), [{ ...MAIN, parent: null }, { name: "try", namespace: "alasio-branch-try-sessions", parent: "main" }]);
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
  }
});
