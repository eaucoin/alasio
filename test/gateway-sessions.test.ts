import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { once } from "node:events";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { before, test } from "node:test";

import type { V1Deployment, V1Pod } from "@kubernetes/client-node";
import { Effect, Layer } from "effect";

import { Binder } from "../src/binder/binder.ts";
import { freeLabels, type Pool, readLease, touchLease } from "../src/binder/bindings.ts";
import { verifyToken } from "../src/binder/tokens.ts";
import { makeSessions, type SessionsOptions } from "../src/gateway/sessions.ts";
import { KubeClient } from "../src/kube/client.ts";
import type { CheckpointRequest, Manifest } from "../src/node-agent/agent.ts";
import { type AttachRequest, NodeAgentError, type NodeAgents } from "../src/sandbox/node-agents.ts";
import { sessionKey } from "../src/sandbox/names.ts";
import { startKubeApiServer } from "./support/kube-apiserver.ts";

// The gateway's session workspaces on a real API server, each pool's pods at the loopback's
// address, where a stand-in for bayma answers /status as the test says, and the node
// agents a stand-in recording what they are asked.

const bindingKey = randomBytes(32);

const withKube = <A, E>(use: (kube: KubeClient["Service"]) => Effect.Effect<A, E>): Effect.Effect<A, E, KubeClient> => Effect.flatMap(KubeClient, use);
const run = <A, E>(effect: Effect.Effect<A, E, KubeClient>): Promise<A> => Effect.runPromise(Effect.provide(effect, Layer.fresh(KubeClient.layer)));

before(async () => {
  process.env["KUBECONFIG"] = (await startKubeApiServer()).kubeconfig;
  // The RuntimeClass gVisor pods name, as the API server admits no pod of one there is not.
  await run(withKube((kube) => kube.create({ apiVersion: "node.k8s.io/v1", kind: "RuntimeClass", metadata: { name: "gvisor" }, handler: "runsc" } as never)));
});

let namespaces = 0;

/** A pool pod's bayma, as its template runs it, with the state directory its hook is told. */
const BAYMA_SPEC = {
  containers: [{ name: "bayma", image: "agent", command: ["/usr/bin/tini", "-s", "--", "node", "/opt/bayma/bayma.js"], args: ["mcp-http", "--state-dir", "/home/agent/.bayma"] }],
};

/** A stand-in for bayma's /status, answering what `status` says now, and the bearers it was asked with. */
async function bayma(): Promise<{ port: number; status: { execsRunning: number; execsQueued: number; idleSinceMs: number | null }; asked: string[]; close: () => void }> {
  const asked: string[] = [];
  const stand = { status: { execsRunning: 0, execsQueued: 0, idleSinceMs: 0 as number | null } };
  const server = createServer((incoming, outgoing) => {
    asked.push(String(incoming.headers.authorization));
    outgoing.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify(stand.status));
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  return { port: (server.address() as AddressInfo).port, get status() { return stand.status; }, set status(value) { stand.status = value; }, asked, close: () => server.close() };
}

/**
 * Node agents that record what they are asked, in order; a checkpoint does what `checkpoint`
 * says to the pod it saves: ends it saved, fails ending it, or fails leaving it running.
 */
function agents(checkpoint: "saves" | "fails, ending it" | "fails" = "saves"): { agents: NodeAgents; attached: (AttachRequest & { node: string })[]; detached: string[]; asked: string[]; saved: CheckpointRequest[] } {
  const attached: (AttachRequest & { node: string })[] = [];
  const detached: string[] = [];
  const asked: string[] = [];
  const saved: CheckpointRequest[] = [];
  const unused = () => Effect.die("not asked");
  const end = (request: CheckpointRequest) =>
    run(withKube((kube) => kube.patchPodStatus(request.namespace, request.pod, { status: { containerStatuses: [{ name: "bayma", image: "agent", imageID: "", ready: false, restartCount: 0, containerID: `containerd://${request.containerId}`, state: { terminated: { exitCode: 0 } } }] } })));
  return {
    attached,
    detached,
    asked,
    saved,
    agents: {
      nodes: Effect.succeed(["node-a"]),
      prepare: unused, current: unused, copy: unused, forget: unused, resume: unused, holdings: unused, workspaces: unused, discard: unused,
      readFile: unused, writeFile: unused,
      checkpoint: (node, request) =>
        Effect.gen(function*() {
          asked.push(`checkpoint ${node} ${request.pod}`);
          saved.push(request);
          if (checkpoint !== "fails") yield* Effect.promise(() => end(request));
          if (checkpoint !== "saves") return yield* new NodeAgentError({ node, call: "snapshot", status: 500, reason: "runsc checkpoint failed" });
          return { snapshot: "s-1" } as unknown as Manifest;
        }),
      attach: (node, request) => Effect.sync(() => void attached.push({ node, ...request })),
      detach: (node, pod, containerIds) =>
        Effect.sync(() => {
          asked.push(`detach ${node} ${pod}`);
          detached.push(`${node} ${pod} ${containerIds.join(",")}`);
        }),
    },
  };
}

/** An environment of the test's own, its pools' namespace, with `count` free pods of the `none` pool on node-a, serving at the loopback. */
async function environment(count: number, { gvisor = false } = {}): Promise<{ name: string; namespace: string; none: Pool }> {
  const namespace = `sessions-${(namespaces += 1)}`;
  const none: Pool = { name: "sandboxes-none", namespace };
  await run(withKube((kube) =>
    Effect.gen(function*() {
      yield* kube.create({ apiVersion: "v1", kind: "Namespace", metadata: { name: namespace } });
      // The pool's template, as its snapshots record it.
      const template = { metadata: { labels: { ...freeLabels(none) } }, spec: { ...BAYMA_SPEC, ...(gvisor ? { runtimeClassName: "gvisor" } : {}) } };
      yield* kube.create<V1Deployment>({ apiVersion: "apps/v1", kind: "Deployment", metadata: { name: none.name, namespace }, spec: { replicas: 0, selector: { matchLabels: { ...freeLabels(none) } }, template } });
      for (let n = 0; n < count; n += 1) {
        const name = `none-${n}`;
        yield* kube.create<V1Pod>({ apiVersion: "v1", kind: "Pod", metadata: { name, namespace, labels: { ...freeLabels(none) } }, spec: { nodeName: "node-a", ...template.spec } });
        yield* kube.patchPodStatus(namespace, name, {
          status: { phase: "Running", podIP: "127.0.0.1", conditions: [{ type: "Ready", status: "True" }], containerStatuses: [{ name: "bayma", image: "agent", imageID: "", ready: true, restartCount: 0, containerID: `containerd://c-${n}`, state: { running: {} } }] },
        });
      }
    })
  ));
  return { name: `env-${namespaces}`, namespace, none };
}

const options = (env: { name: string; namespace: string }, podPort: number): SessionsOptions => ({
  environments: [env],
  pools: { none: "sandboxes-none", full: "sandboxes-full" },
  releaseAfterMs: 60_000,
  podPort,
  bindingKey,
});

/** The gateway's sessions and its Binder, on clients of their own. */
const sessionsOf = (opts: SessionsOptions, nodeAgents: NodeAgents) =>
  Effect.runPromise(Effect.provide(Effect.gen(function*() {
    const binder = yield* Binder;
    return { binder, sessions: yield* makeSessions(opts, nodeAgents, binder) };
  }), Layer.provideMerge(Binder.layer({ waitMs: 5_000 }), Layer.fresh(KubeClient.layer))));

test("a session's workspace is placed in its environment's pool of its internet, and readied by its node's agent attaching it with its binding's token", async () => {
  const env = await environment(1);
  const recorded = agents();
  const { binder, sessions } = await sessionsOf(options(env, 7290), recorded.agents);
  const key = sessionKey("fs-abc123");
  for (const [said, why] of [
    [{ environment: env.name, mode: "none" }, null],
    [{ environment: "elsewhere", mode: "none" }, "there is no environment elsewhere of sessions"],
    [{ environment: env.name, mode: "some" }, "a session's internet is none or full, not some"],
  ] as const) {
    const placed = await Effect.runPromise(Effect.result(sessions.place(key, said)));
    assert.equal(placed._tag === "Failure" ? placed.failure.message : null, why);
  }
  assert.equal((await Effect.runPromise(Effect.flip(sessions.place("folder:claude:conversation", { environment: env.name, mode: "none" })))).message, "folder:claude:conversation is no session workspace's");
  const placement = await Effect.runPromise(sessions.place(key, { environment: env.name, mode: "none" }));
  assert.deepEqual(placement.pool, env.none);
  const bound = await Effect.runPromise(binder.bind(placement.pool, key, placement));
  const [attach] = recorded.attached;
  assert.deepEqual([attach?.node, attach?.environment, attach?.workspace, attach?.pod], ["node-a", env.namespace, "fs-abc123", bound.pod]);
  assert.deepEqual(verifyToken(bindingKey, attach?.token ?? "", Date.now()), { key, binding: bound.binding });
});

test("a bound pod whose bayma runs an exec is kept, and its bayma asked again only once its poll comes round, with its binding's token", async () => {
  const env = await environment(2);
  const stand = await bayma();
  const recorded = agents();
  try {
    const { binder, sessions } = await sessionsOf(options(env, stand.port), recorded.agents);
    const key = sessionKey("fs-abc123");
    const placement = await Effect.runPromise(sessions.place(key, { environment: env.name, mode: "none" }));
    const bound = await Effect.runPromise(binder.bind(placement.pool, key, placement));
    // Its bayma busy: not released, and asked no more until its poll comes round.
    stand.status = { execsRunning: 1, execsQueued: 0, idleSinceMs: null };
    assert.deepEqual(await Effect.runPromise(Effect.provide(sessions.sweep, Layer.fresh(KubeClient.layer))), []);
    assert.deepEqual(await Effect.runPromise(Effect.provide(sessions.sweep, Layer.fresh(KubeClient.layer))), []);
    assert.equal(stand.asked.length, 1);
    assert.deepEqual(verifyToken(bindingKey, stand.asked[0]?.slice("Bearer ".length) ?? "", Date.now()), { key, binding: bound.binding });
    assert.ok(await run(readLease(env.none, key)));
  } finally {
    stand.close();
  }
});

test("a bound pod whose bayma is idle is kept while a request of it came within the wait, and once none did, released: under a runtime that saves nothing, its pod's deletion ends it, its workspace detached once its containers stopped, then its binding gone", async () => {
  const env = await environment(1);
  const stand = await bayma();
  const recorded = agents();
  try {
    const { binder, sessions } = await sessionsOf({ ...options(env, stand.port), releaseAfterMs: 2_000, idlePollMs: 500 }, recorded.agents);
    const key = sessionKey("fs-abc123");
    const placement = await Effect.runPromise(sessions.place(key, { environment: env.name, mode: "none" }));
    const bound = await Effect.runPromise(binder.bind(placement.pool, key, placement));
    stand.status = { execsRunning: 0, execsQueued: 0, idleSinceMs: Date.now() - 600_000 };
    await run(touchLease(env.none, key, Date.now()));
    assert.deepEqual(await Effect.runPromise(Effect.provide(sessions.sweep, Layer.fresh(KubeClient.layer))), []);
    // Its wait past its last request, as kubelet stops its containers once its deletion is asked.
    await new Promise((resolve) => setTimeout(resolve, 2_100));
    const stopping = (async () => {
      for (;;) {
        const pod = await run(withKube((kube) => kube.read("v1", "Pod", env.namespace, bound.pod))) as V1Pod | null;
        if (pod?.metadata?.deletionTimestamp) {
          await run(withKube((kube) => kube.patchPodStatus(env.namespace, bound.pod, { status: { containerStatuses: [{ name: "bayma", image: "agent", imageID: "", ready: false, restartCount: 0, containerID: "containerd://c-0", state: { terminated: { exitCode: 143 } } }] } })));
          return;
        }
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
    })();
    const released = await Effect.runPromise(Effect.provide(sessions.sweep, Layer.fresh(KubeClient.layer)));
    await stopping;
    assert.deepEqual(released, [key]);
    assert.deepEqual(recorded.detached, [`node-a ${bound.pod} c-0`]);
    assert.equal(await run(readLease(env.none, key)), null);
  } finally {
    stand.close();
  }
});

test("a bound gVisor pod idle for its wait is saved whole by its node's agent first, which ends it, then detached, then deleted with its binding", async () => {
  const env = await environment(1, { gvisor: true });
  const stand = await bayma();
  const recorded = agents();
  try {
    const { binder, sessions } = await sessionsOf({ ...options(env, stand.port), releaseAfterMs: 0 }, recorded.agents);
    const key = sessionKey("fs-abc123");
    const placement = await Effect.runPromise(sessions.place(key, { environment: env.name, mode: "none" }));
    const bound = await Effect.runPromise(binder.bind(placement.pool, key, placement));
    stand.status = { execsRunning: 0, execsQueued: 0, idleSinceMs: Date.now() - 600_000 };
    assert.deepEqual(await Effect.runPromise(sessions.sweep), [key]);
    assert.deepEqual(recorded.asked, [`checkpoint node-a ${bound.pod}`, `detach node-a ${bound.pod}`]);
    const [saved] = recorded.saved;
    assert.deepEqual([saved?.environment, saved?.workspace, saved?.namespace, saved?.containerId], [env.namespace, "fs-abc123", env.namespace, "c-0"]);
    assert.deepEqual(saved?.hook, ["node", "/opt/bayma/bayma.js", "sandbox-hook", "--state-dir", "/home/agent/.bayma"]);
    assert.equal((saved?.podTemplate as { spec?: { runtimeClassName?: string } }).spec?.runtimeClassName, "gvisor");
    assert.equal(await run(readLease(env.none, key)), null);
  } finally {
    stand.close();
  }
});

test("a release whose save fails leaving the sandbox running keeps the binding, and one whose save fails ending it goes on, as bayma's checkpoints keep what ran", async () => {
  for (const [outcome, kept] of [["fails", true], ["fails, ending it", false]] as const) {
    const env = await environment(1, { gvisor: true });
    const stand = await bayma();
    const recorded = agents(outcome);
    try {
      const { binder, sessions } = await sessionsOf({ ...options(env, stand.port), releaseAfterMs: 0 }, recorded.agents);
      const key = sessionKey("fs-abc123");
      const placement = await Effect.runPromise(sessions.place(key, { environment: env.name, mode: "none" }));
      const bound = await Effect.runPromise(binder.bind(placement.pool, key, placement));
      stand.status = { execsRunning: 0, execsQueued: 0, idleSinceMs: Date.now() - 600_000 };
      assert.deepEqual(await Effect.runPromise(sessions.sweep), kept ? [] : [key], outcome);
      const lease = await run(readLease(env.none, key));
      if (kept) {
        assert.deepEqual([lease?.spec?.holderIdentity, lease?.metadata?.annotations?.["alasio.dev/releasing"]], [bound.pod, undefined], outcome);
        assert.deepEqual(recorded.detached, [], outcome);
      } else {
        assert.equal(lease, null, outcome);
        assert.deepEqual(recorded.asked, [`checkpoint node-a ${bound.pod}`, `detach node-a ${bound.pod}`], outcome);
      }
    } finally {
      stand.close();
    }
  }
});
