import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { lookup } from "node:dns/promises";
import { once } from "node:events";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { before, test } from "node:test";

import type { V1Deployment, V1Pod, V1PodSpec, V1PodTemplateSpec } from "@kubernetes/client-node";
import { Effect, Layer } from "effect";
import { TestClock } from "effect/testing";

import { added } from "./support/metrics.ts";
import { Binder } from "../src/binder/binder.ts";
import { markReleasing, readLease, releaseLease, takeLease, touchLease } from "../src/binder/bindings.ts";
import { freeLabels, type Pool } from "../src/binder/names.ts";
import { verifyToken } from "../src/binder/tokens.ts";
import type { Environments, SessionsEnvironment } from "../src/gateway/environments.ts";
import type { EventInput } from "../src/events/events.ts";
import { RELEASES } from "../src/gateway/releases.ts";
import { ATTACH_DURATION, DETACH_DURATION, makeSessions, type SessionsOptions } from "../src/gateway/sessions.ts";
import { KubeClient } from "../src/kube/client.ts";
import type { CheckpointRequest, Manifest } from "../src/node-agent/agent.ts";
import { DEFER_FOR_MS } from "../src/gateway/snapshots.ts";
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
  containers: [{
    name: "bayma",
    image: "agent",
    command: ["/usr/bin/tini", "-s", "--", "node", "/opt/bayma/bayma.js"],
    args: ["mcp-http", "--state-dir", "/home/agent/.bayma"],
    env: [{ name: "POD_NAME", valueFrom: { fieldRef: { fieldPath: "metadata.name" } } }],
    // Its own directory on its node, as a pool's template mounts it.
    volumeMounts: [{ name: "attach", mountPath: "/attach", subPathExpr: "$(POD_NAME)", mountPropagation: "HostToContainer" }],
  }],
  volumes: [{ name: "attach", hostPath: { path: "/var/lib/alasio/attach", type: "DirectoryOrCreate" } }],
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
function agents(checkpoint: "saves" | "fails, ending it" | "fails" | "unsaveable" = "saves"): { agents: NodeAgents; attached: (AttachRequest & { node: string })[]; detached: string[]; asked: string[]; saved: CheckpointRequest[] } {
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
      // No workspace of theirs has a snapshot to restore, nor a record of a release, unless a test gives it one.
      current: () => Effect.succeed(null),
      released: () => Effect.succeed(null),
      // What is attached on its node is reachable, unless a test says otherwise.
      attachments: () => Effect.succeed([]),
      prepare: unused, copy: unused, forget: unused, resume: unused, holdings: unused, workspaces: unused, discard: unused, spend: unused, collect: unused, abandon: unused,
      readFile: unused, writeFile: unused, createWorkspace: unused, cloneWorkspace: unused, removeWorkspace: unused, listWorkspaces: unused, listEnvironments: unused(), quota: unused, sync: unused,
      checkpoint: (node, request) =>
        Effect.gen(function*() {
          asked.push(`checkpoint ${node} ${request.pod}${request.live ? " live" : ""}`);
          saved.push(request);
          // Taken live, its sandbox runs on, at bayma's generation 1.
          if (request.live) return { snapshot: "s-live", release: "r1", live: { stateDir: request.live.stateDir, generation: 1, running: [] } } as unknown as Manifest;
          if (checkpoint === "unsaveable") return yield* new NodeAgentError({ node, call: "snapshot", status: 410, reason: "its restore from the bucket failed to read some of its pages" });
          if (checkpoint !== "fails") yield* Effect.promise(() => end(request));
          if (checkpoint !== "saves") return yield* new NodeAgentError({ node, call: "snapshot", status: 500, reason: "runsc checkpoint failed" });
          return { snapshot: "s-1" } as unknown as Manifest;
        }),
      attach: (node, request) => Effect.sync(() => void attached.push({ node, ...request })),
      setReleased: (ref, into) => Effect.sync(() => void asked.push(`record ${ref.environment}/${ref.workspace} released into ${into?.snapshot ?? "none"}`)),
      // Its bayma, told to go on with its stop, ends.
      stop: (node, request) =>
        Effect.sync(() => void asked.push(`stop ${node} ${request.pod} ${request.hook.join(" ")}`)).pipe(
          Effect.andThen(Effect.promise(() => end({ namespace: request.namespace, pod: request.pod, containerId: request.containerId } as CheckpointRequest))),
        ),
      detach: (node, namespace, pod, containerIds) =>
        Effect.sync(() => {
          asked.push(`detach ${node} ${namespace}/${pod}`);
          detached.push(`${node} ${pod} ${containerIds.join(",")}`);
        }),
    },
  };
}

/** An environment of the test's own, its pools' namespace, with `count` free pods of the `none` pool on node-a, serving at the loopback. */
async function environment(count: number, { gvisor = false, holdsStop = false } = {}): Promise<SessionsEnvironment & { none: Pool; template: V1PodTemplateSpec }> {
  const namespace = `sessions-${(namespaces += 1)}`;
  const none: Pool = { name: "sandboxes-none", namespace };
  let template: V1PodTemplateSpec = {};
  await run(withKube((kube) =>
    Effect.gen(function*() {
      yield* kube.create({ apiVersion: "v1", kind: "Namespace", metadata: { name: namespace } });
      // The pool's template, as its snapshots record it.
      // Its bayma holding its stop for a save, where it is told to, as a pool's of snapshotted sessions does.
      const spec = holdsStop
        ? { terminationGracePeriodSeconds: 90, volumes: BAYMA_SPEC.volumes, containers: BAYMA_SPEC.containers.map((container) => ({ ...container, args: [...container.args, "--stop-wait-ms", "60000"] })) }
        : BAYMA_SPEC;
      template = { metadata: { labels: { ...freeLabels(none) }, annotations: { "alasio.dev/session-template": "t-current" } }, spec: { ...spec, ...(gvisor ? { runtimeClassName: "gvisor" } : {}) } };
      yield* kube.create<V1Deployment>({ apiVersion: "apps/v1", kind: "Deployment", metadata: { name: none.name, namespace }, spec: { replicas: 0, selector: { matchLabels: { ...freeLabels(none) } }, template } });
      for (let n = 0; n < count; n += 1) {
        const name = `none-${n}`;
        yield* kube.create<V1Pod>({ apiVersion: "v1", kind: "Pod", metadata: { name, namespace, labels: { ...freeLabels(none) } }, spec: { nodeName: "node-a", ...template.spec } as V1PodSpec });
        yield* kube.patchPodStatus(namespace, name, {
          status: { phase: "Running", podIP: "127.0.0.1", conditions: [{ type: "Ready", status: "True" }], containerStatuses: [{ name: "bayma", image: "agent", imageID: "", ready: true, restartCount: 0, containerID: `containerd://c-${n}`, state: { running: {} } }] },
        });
      }
    })
  ));
  return { name: `env-${namespaces}`, namespace, parent: null, none, template };
}

/** The environments `list`, as found: those alone. */
const found = (list: readonly SessionsEnvironment[]): Environments => ({
  current: Effect.succeed(list),
  named: (name) => Effect.succeed(list.find((each) => each.name === name) ?? null),
  ofNamespace: (namespace) => Effect.succeed(list.find((each) => each.namespace === namespace) ?? null),
});

/** What the sessions told of, as each environment's alasio reads it. */
const told: EventInput[] = [];

/** The gateway's sessions' options, of the environment `env`, main, whose sessions are snapshotted under the release `r1`, onto its pool's template. */
const options = (env: SessionsEnvironment & { template?: V1PodTemplateSpec }, podPort: number): SessionsOptions => ({
  environments: found([env]),
  mainNamespace: env.namespace,
  snapshots: {
    liveEverySeconds: 600,
    releases: [{ release: "r1", runtimeClassName: "gvisor-r1", gofer: { transport: "socket", pagesRetry: true } }],
    templates: { none: { current: { ...env.template }, previous: null }, full: { current: { ...env.template }, previous: null } },
  },
  pools: { none: "sandboxes-none", full: "sandboxes-full" },
  releaseAfterMs: 60_000,
  podPort,
  quotaBytes: 2 * 1024 ** 3,
  bindingKey,
  events: { tell: (event) => Effect.sync(() => void told.push(event)) },
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
  const telemetry = { host: "localhost", port: 4318, env: { OTEL_TRACES_EXPORTER: "otlp" } };
  const { binder, sessions } = await sessionsOf({ ...options(env, 7290), telemetry }, recorded.agents);
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
  assert.deepEqual(verifyToken(bindingKey, attach?.token ?? "", Date.now()), { key, binding: bound.binding, namespace: env.namespace });
  // Its bayma exports to the receiver by its address, as a session with no internet resolves no name.
  assert.deepEqual(attach?.env, { OTEL_TRACES_EXPORTER: "otlp", OTEL_EXPORTER_OTLP_ENDPOINT: `http://${(await lookup("localhost")).address}:4318` });
});

test("a pod whose readying failed, its workspace not attached, is detached once its binder deleted it and its containers stopped, its directory on its node with it", async () => {
  const env = await environment(2);
  const recorded = agents();
  let fails = true;
  const failing: NodeAgents = {
    ...recorded.agents,
    attach: (node, request) => (fails ? ((fails = false), Effect.fail(new NodeAgentError({ node, call: "attach", status: 503, reason: "workspace storage is not mounted" }))) : recorded.agents.attach(node, request)),
  };
  const { binder, sessions } = await sessionsOf(options(env, 7290), failing);
  const key = sessionKey("fs-unready");
  const placement = await Effect.runPromise(sessions.place(key, { environment: env.name, mode: "none" }));
  const failed = await Effect.runPromise(Effect.result(binder.bind(placement.pool, key, placement)));
  assert.equal(failed._tag, "Failure");
  // Deleted by its binder, it ends, as kubelet would end it.
  const [name] = ((await run(withKube((kube) => kube.list("v1", "Pod", env.namespace, "")))) as V1Pod[]).filter(({ metadata }) => metadata?.deletionTimestamp).map(({ metadata }) => metadata?.name ?? "");
  assert.ok(name, "no pod was deleted");
  await run(withKube((kube) => kube.patchPodStatus(env.namespace, name, { status: { containerStatuses: [{ name: "bayma", image: "agent", imageID: "", ready: false, restartCount: 0, containerID: "containerd://c-x", state: { terminated: { exitCode: 0 } } }] } })));
  for (const deadline = Date.now() + 10_000; !recorded.asked.includes(`detach node-a ${env.namespace}/${name}`); await new Promise((resolve) => setTimeout(resolve, 100))) assert.ok(Date.now() < deadline, recorded.asked.join("\n"));
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
    assert.deepEqual(verifyToken(bindingKey, stand.asked[0]?.slice("Bearer ".length) ?? "", Date.now()), { key, binding: bound.binding, namespace: env.namespace });
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
    let released: readonly string[] = [];
    const counted = await added(RELEASES, async () => (released = await Effect.runPromise(Effect.provide(sessions.sweep, Layer.fresh(KubeClient.layer)))));
    await stopping;
    assert.deepEqual(released, [key]);
    assert.deepEqual(counted, { [`k8s.namespace.name=${env.namespace},alasio.gateway.release.reason=idle`]: 1 });
    assert.deepEqual(recorded.detached, [`node-a ${bound.pod} c-0`]);
    assert.equal(await run(readLease(env.none, key)), null);
    // Told of, to its environment's alasio.
    assert.deepEqual(told.filter((event) => event.subject.key === key && event.environment === env.name), [
      { kind: "workspace.released", environment: env.name, source: "sandbox-gateway", subject: { kind: "workspace", key }, binding: bound.binding, reason: "idle" },
    ]);
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
    // Its release recorded with the workspace, for its next bind, before its workspace is detached.
    assert.deepEqual(recorded.asked, [`checkpoint node-a ${bound.pod}`, `record ${env.namespace}/fs-abc123 released into s-1`, `detach node-a ${env.namespace}/${bound.pod}`]);
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
        assert.deepEqual(recorded.asked, [`checkpoint node-a ${bound.pod}`, `record ${env.namespace}/fs-abc123 released into none`, `detach node-a ${env.namespace}/${bound.pod}`], outcome);
      }
    } finally {
      stand.close();
    }
  }
});

test("a binding let go as its pod went while it was bound is told of, to its environment's alasio, as lost", async () => {
  const env = { name: "try", namespace: "alasio-branch-try-sessions", parent: "main" };
  const { sessions } = await sessionsOf(options(env, 7290), agents().agents);
  const key = sessionKey("fs-lost");
  const lease = { apiVersion: "coordination.k8s.io/v1", kind: "Lease", metadata: { uid: "uid-lost", namespace: env.namespace, annotations: { "alasio.dev/binding-key": key } }, spec: { holderIdentity: "sandboxes-none-0" } };
  await Effect.runPromise(sessions.lost({ name: "sandboxes-none", namespace: env.namespace }, lease));
  assert.deepEqual(told.filter((event) => event.subject.key === key), [
    { kind: "workspace.lost", environment: "try", source: "sandbox-gateway", subject: { kind: "workspace", key }, binding: "uid-lost", pod: "sandboxes-none-0", reason: "its pod went while it was bound" },
  ]);
});

/** Makes the namespace `name`, as a branch's sessions' is made, where it is not there. */
const namespaced = (name: string) => run(withKube((kube) => kube.create({ apiVersion: "v1", kind: "Namespace", metadata: { name } })).pipe(Effect.ignore));

/**
 * Node agents that keep workspaces as a map of their files, by environment and name, and their
 * current snapshots and records of what they were released into, as maps of theirs, recording
 * each call.
 */
function workspaceAgents(): {
  agents: NodeAgents;
  calls: string[];
  files: Map<string, Map<string, Uint8Array>>;
  snapshots: Map<string, { snapshot: string; release: string }>;
  records: Map<string, { snapshot: string; release: string }>;
  cached: Map<string, readonly string[]>;
} {
  const calls: string[] = [];
  const files = new Map<string, Map<string, Uint8Array>>();
  const snapshots = new Map<string, { snapshot: string; release: string }>();
  const records = new Map<string, { snapshot: string; release: string }>();
  // The images node-a caches of each workspace.
  const cached = new Map<string, readonly string[]>();
  const name = ({ environment, workspace }: { environment: string; workspace: string }) => `${environment}/${workspace}`;
  const missing = (call: string) => new NodeAgentError({ node: "node-a", call, status: 404, reason: "there is no such workspace" });
  const base = agents().agents;
  const ofName = (each: string) => ({ environment: each.split("/")[0] ?? "", workspace: each.split("/")[1] ?? "" });
  return {
    calls,
    files,
    snapshots,
    records,
    cached,
    agents: {
      ...base,
      current: (ref) => Effect.sync(() => (snapshots.has(name(ref)) ? ({ ...snapshots.get(name(ref)) } as unknown as Manifest) : null)),
      released: (ref) => Effect.sync(() => records.get(name(ref)) ?? null),
      setReleased: (ref, into) => Effect.sync(() => (calls.push(`record ${name(ref)} released into ${into?.snapshot ?? "none"}`), void (into ? records.set(name(ref), into) : records.delete(name(ref))))),
      copy: (from, to) =>
        Effect.suspend(() => {
          calls.push(`copy ${name(from)} ${name(to)}`);
          const source = snapshots.get(name(from));
          if (!source) return Effect.fail(missing("copy"));
          const copied = { snapshot: `${source.snapshot}-copy`, release: source.release };
          snapshots.set(name(to), copied);
          return Effect.succeed(copied as unknown as Manifest);
        }),
      forget: (ref) => Effect.sync(() => (calls.push(`forget ${name(ref)}`), void snapshots.delete(name(ref)))),
      discard: (node, ref, keep) => Effect.sync(() => (calls.push(`discard ${node} ${name(ref)} keeping ${keep.join(",") || "none"}`), cached.set(name(ref), (cached.get(name(ref)) ?? []).filter((digest) => keep.includes(digest))), true)),
      workspaces: (environment) => Effect.sync(() => [...snapshots.keys()].filter((each) => each.startsWith(`${environment}/`)).map((each) => ofName(each).workspace)),
      collect: (ref) => Effect.sync(() => (calls.push(`collect ${name(ref)}`), [`${snapshots.get(name(ref))?.snapshot}-image`])),
      holdings: (node) => Effect.sync(() => ({ node, releases: [], cached: [...cached].map(([each, images]) => ({ ...ofName(each), images, bytes: 1, usedAt: "2026-10-17T12:00:00Z" })) })),
      createWorkspace: (ref, quotaBytes) => Effect.sync(() => (calls.push(`create ${name(ref)} ${quotaBytes}`), void files.set(name(ref), files.get(name(ref)) ?? new Map()))),
      removeWorkspace: (ref) => Effect.sync(() => (calls.push(`remove ${name(ref)}`), void files.delete(name(ref)))),
      sync: (node, request) => Effect.sync(() => void calls.push(`sync ${node} ${request.namespace}/${request.pod} ${request.containerId}`)),
      cloneWorkspace: (from, to, quotaBytes) =>
        Effect.suspend(() => {
          calls.push(`clone ${name(from)} ${name(to)} ${quotaBytes}`);
          const source = files.get(name(from));
          return source ? Effect.sync(() => void files.set(name(to), new Map(source))) : Effect.fail(missing("clone"));
        }),
      listWorkspaces: (environment) => Effect.sync(() => [...files.keys()].filter((each) => each.startsWith(`${environment}/`)).map((each) => each.slice(environment.length + 1))),
      listEnvironments: Effect.sync(() => [...new Set([...files.keys()].map((each) => ofName(each).environment))].sort()),
      quota: (ref) => Effect.sync(() => (files.has(name(ref)) ? { capacityBytes: 2 * 1024 ** 3, usedBytes: 5 } : null)),
      readFile: (ref, path) => {
        const file = files.get(name(ref))?.get(path);
        return file ? Effect.succeed(file) : Effect.fail(missing("read"));
      },
      writeFile: (ref, path, bytes) => {
        const workspace = files.get(name(ref));
        return workspace ? Effect.sync(() => void workspace.set(path, Uint8Array.from(bytes))) : Effect.fail(missing("write"));
      },
    },
  };
}

test("a caller makes, writes, reads, measures, lists and deletes its own environment's workspace through the gateway alone, one bound not deleted", async () => {
  const env = await environment(1);
  const recorded = workspaceAgents();
  const { binder, sessions } = await sessionsOf({ ...options(env, 7290), environments: found([env, { name: "other", namespace: "other-sessions", parent: null }]) }, recorded.agents);
  const key = sessionKey("fs-own123");
  const claims = { key, expires: Date.now() + 60_000, options: { environment: env.name, mode: "none" } };
  const ask = (method: string, path: string, body: unknown = "", of = claims) => {
    const [route = "", search = ""] = path.split("?", 2);
    const bytes = typeof body === "string" ? Buffer.from(body) : Buffer.from(JSON.stringify(body));
    return Effect.runPromise(sessions.operate(of, { method, path: route, query: new URLSearchParams(search), body: bytes }));
  };
  assert.deepEqual(await ask("POST", "/workspace"), { status: 204 });
  assert.deepEqual(await ask("PUT", "/workspace/files?path=workspace/note.txt", "hello"), { status: 204 });
  const read = await ask("GET", "/workspace/files?path=workspace/note.txt");
  assert.deepEqual([read.status, Buffer.from(read.bytes ?? []).toString()], [200, "hello"]);
  assert.equal((await ask("GET", "/workspace/files?path=workspace/none.txt")).status, 404);
  assert.equal((await ask("GET", "/workspace/files?path=workspace/note.txt&max=many")).status, 400);
  assert.deepEqual(await ask("GET", "/workspace/quota"), { status: 200, json: { capacityBytes: 2 * 1024 ** 3, usedBytes: 5 } });
  assert.deepEqual(await ask("GET", "/workspace/all"), { status: 200, json: { workspaces: ["fs-own123"] } });
  // Made in its own environment's namespace, of the gateway's quota; another's lists none of it.
  assert.deepEqual(recorded.calls, [`create ${env.namespace}/fs-own123 ${2 * 1024 ** 3}`]);
  assert.deepEqual(await ask("GET", "/workspace/all", "", { ...claims, options: { environment: "other", mode: "none" } }), { status: 200, json: { workspaces: [] } });
  // Bound, it is not deleted; released, it is.
  const placement = await Effect.runPromise(sessions.place(key, claims.options));
  await Effect.runPromise(binder.bind(placement.pool, key, placement));
  assert.equal((await ask("DELETE", "/workspace")).status, 409);
  await Effect.runPromise(binder.release(placement.pool, key));
  // Being bound, as by another binder whose bind is under way, it is not deleted either.
  const under = await run(takeLease(placement.pool, key, "another-binder"));
  assert.equal((await ask("DELETE", "/workspace")).status, 409);
  await run(releaseLease(under.lease));
  // Released into a snapshot, held by node-a's cache: deleted, its snapshots forgotten with it everywhere, under its Lease, let go after.
  recorded.snapshots.set(`${env.namespace}/fs-own123`, { snapshot: "s-1", release: "r1" });
  recorded.cached.set(`${env.namespace}/fs-own123`, ["s-1-image"]);
  recorded.calls.length = 0;
  assert.deepEqual(await ask("DELETE", "/workspace"), { status: 204 });
  assert.equal(recorded.files.has(`${env.namespace}/fs-own123`), false);
  assert.deepEqual(recorded.calls, [`remove ${env.namespace}/fs-own123`, `forget ${env.namespace}/fs-own123`, `discard node-a ${env.namespace}/fs-own123 keeping none`]);
  assert.equal(recorded.snapshots.size, 0);
  assert.equal(await run(readLease(placement.pool, key)), null);
  // An environment there is not, a key of no workspace, and a route there is not.
  assert.equal((await ask("POST", "/workspace", "", { ...claims, options: { environment: "nowhere", mode: "none" } })).status, 403);
  assert.equal((await ask("POST", "/workspace", "", { ...claims, key: "folder:claude:conversation" })).status, 403);
  assert.equal((await ask("PATCH", "/workspace")).status, 404);
});

test("main's token reads, of every environment workspace storage holds, by each gVisor release, the snapshots its workspaces may be restored from; another's is refused, and what cannot be read fails it whole", async () => {
  const main: SessionsEnvironment = { name: "main", namespace: "releases-main-sessions", parent: null };
  const one = { name: "one", namespace: "alasio-branch-one-sessions", parent: "main" };
  const recorded = workspaceAgents();
  await namespaced(main.namespace);
  const { sessions } = await sessionsOf({ ...options(main, 7290), environments: found([main, one]) }, recorded.agents);
  for (const each of ["releases-main-sessions/fs-moved01", "releases-main-sessions/fs-same001", "releases-main-sessions/fs-none001", "alasio-branch-one-sessions/fs-branch1", "gone-sessions/fs-left001"]) recorded.files.set(each, new Map());
  // Its current snapshot taken under another release than the one it was released into; both are restored from.
  recorded.snapshots.set("releases-main-sessions/fs-moved01", { snapshot: "s-2", release: "r2" });
  recorded.records.set("releases-main-sessions/fs-moved01", { snapshot: "s-1", release: "r1" });
  recorded.snapshots.set("releases-main-sessions/fs-same001", { snapshot: "s-3", release: "r2" });
  recorded.records.set("releases-main-sessions/fs-same001", { snapshot: "s-3", release: "r2" });
  recorded.snapshots.set("alasio-branch-one-sessions/fs-branch1", { snapshot: "s-4", release: "r3" });
  // Of an environment found no more, its workspace still held; and in the bucket alone, of a workspace gone, which restores nothing.
  recorded.records.set("gone-sessions/fs-left001", { snapshot: "s-5", release: "r1" });
  recorded.snapshots.set("releases-main-sessions/fs-gone001", { snapshot: "s-6", release: "r4" });
  const ask = (environment: string) =>
    Effect.runPromise(sessions.operate({ key: "snapshots", expires: Date.now() + 60_000, options: { environment } }, { method: "GET", path: "/snapshots/releases", query: new URLSearchParams(), body: new Uint8Array() }));
  assert.deepEqual(await ask("main"), {
    status: 200,
    json: {
      releases: {
        r1: ["gone-sessions/fs-left001/s-5", "releases-main-sessions/fs-moved01/s-1"],
        r2: ["releases-main-sessions/fs-moved01/s-2", "releases-main-sessions/fs-same001/s-3"],
        r3: ["alasio-branch-one-sessions/fs-branch1/s-4"],
      },
    },
  });
  assert.equal((await ask("one")).status, 403);
  const failing = await sessionsOf({ ...options(main, 7290), environments: found([main]) }, {
    ...recorded.agents,
    released: () => Effect.fail(new NodeAgentError({ node: "node-a", call: "read a record", status: 503, reason: "workspace storage is not mounted" })),
  });
  const failed = await Effect.runPromise(failing.sessions.operate({ key: "snapshots", expires: Date.now() + 60_000, options: { environment: "main" } }, { method: "GET", path: "/snapshots/releases", query: new URLSearchParams(), body: new Uint8Array() }));
  assert.equal(failed.status, 502);
});

test("main forks its workspace into a branch of its own, as the branch records main its parent, and a branch into itself, but never into main nor into a sibling", async () => {
  const main: SessionsEnvironment = { name: "main", namespace: "fork-main-sessions", parent: null };
  const one = { name: "one", namespace: "alasio-branch-one-sessions", parent: "main" };
  const two = { name: "two", namespace: "alasio-branch-two-sessions", parent: "main" };
  const recorded = workspaceAgents();
  for (const env of [main, one, two]) await namespaced(env.namespace);
  const { sessions } = await sessionsOf({ ...options(main, 7290), environments: found([main, one, two]) }, recorded.agents);
  for (const env of [main, one]) recorded.files.set(`${env.namespace}/fs-shared1`, new Map([["workspace/a", Buffer.from(env.name)]]));
  const fork = (from: SessionsEnvironment, to: string, workspace = "fs-shared1") =>
    Effect.runPromise(sessions.operate(
      { key: sessionKey("fs-shared1"), expires: Date.now() + 60_000, options: { environment: from.name, mode: "none" } },
      { method: "POST", path: "/workspace/forks", query: new URLSearchParams(), body: Buffer.from(JSON.stringify({ environment: to, workspace })) },
    ));
  assert.deepEqual(await fork(main, "one"), { status: 204 });
  assert.equal(Buffer.from(recorded.files.get(`${one.namespace}/fs-shared1`)?.get("workspace/a") ?? []).toString(), "main");
  assert.deepEqual(await fork(main, "main", "fs-copy01"), { status: 204 });
  assert.deepEqual(await fork(one, "one", "fs-copy02"), { status: 204 });
  for (const [from, to] of [[one, "main"], [one, "two"], [main, "nowhere"]] as const) {
    const refused = await fork(from, to, "fs-stolen1");
    assert.equal(refused.status, 403, `${from.name} into ${to}`);
  }
  assert.equal((await fork(main, "one", "Not A Name")).status, 400);
  assert.deepEqual(recorded.calls, [
    `clone ${main.namespace}/fs-shared1 ${one.namespace}/fs-shared1 ${2 * 1024 ** 3}`,
    `clone ${main.namespace}/fs-shared1 ${main.namespace}/fs-copy01 ${2 * 1024 ** 3}`,
    `clone ${one.namespace}/fs-shared1 ${one.namespace}/fs-copy02 ${2 * 1024 ** 3}`,
  ]);
});

test("a fork of main's workspace released into a snapshot into main is given a copy of it, recorded released into it, under both Leases; one of a workspace bound, or into a branch, a clone of its files alone; and none into a fork bound", async () => {
  const env = await environment(1);
  const branch = { name: "forked", namespace: "fork-branch-sessions", parent: env.name };
  const recorded = workspaceAgents();
  await namespaced(branch.namespace);
  const { binder, sessions } = await sessionsOf({ ...options(env, 7290), environments: found([env, branch]) }, recorded.agents);
  const source = `${env.namespace}/fs-source1`;
  recorded.files.set(source, new Map([["workspace/a", Buffer.from("a")]]));
  recorded.snapshots.set(source, { snapshot: "s-1", release: "r1" });
  recorded.records.set(source, { snapshot: "s-1", release: "r1" });
  const claims = { key: sessionKey("fs-source1"), expires: Date.now() + 60_000, options: { environment: env.name, mode: "none" } };
  const fork = (environment: string, workspace: string) =>
    Effect.runPromise(sessions.operate(claims, { method: "POST", path: "/workspace/forks", query: new URLSearchParams(), body: Buffer.from(JSON.stringify({ environment, workspace })) }));
  assert.deepEqual(await fork(env.name, "fs-fork0001"), { status: 204 });
  assert.deepEqual(recorded.calls, [
    `clone ${source} ${env.namespace}/fs-fork0001 ${2 * 1024 ** 3}`,
    `copy ${source} ${env.namespace}/fs-fork0001`,
    `record ${env.namespace}/fs-fork0001 released into s-1-copy`,
  ]);
  for (const workspace of ["fs-source1", "fs-fork0001"]) assert.equal(await run(readLease(env.none, sessionKey(workspace))), null, `${workspace}'s Lease is let go`);
  // Into a branch, whose sessions are not snapshotted: its files alone.
  recorded.calls.length = 0;
  assert.deepEqual(await fork(branch.name, "fs-fork0002"), { status: 204 });
  assert.deepEqual(recorded.calls, [`clone ${source} ${branch.namespace}/fs-fork0002 ${2 * 1024 ** 3}`]);
  // Of one that moved on from what it was released into: its files alone.
  recorded.snapshots.set(source, { snapshot: "s-2", release: "r1" });
  recorded.calls.length = 0;
  assert.deepEqual(await fork(env.name, "fs-fork0003"), { status: 204 });
  assert.deepEqual(recorded.calls, [`clone ${source} ${env.namespace}/fs-fork0003 ${2 * 1024 ** 3}`]);
  // Of one bound: its files alone, as its sandbox runs on from its snapshot, once its sandbox has written them through to its
  // node's mount; bound here from bayma's checkpoints, its record then as it was.
  recorded.records.delete(source);
  const placement = await Effect.runPromise(sessions.place(claims.key, claims.options));
  await Effect.runPromise(binder.bind(placement.pool, claims.key, placement));
  const boundPod = (await Effect.runPromise(binder.bound(placement.pool, claims.key)))?.pod;
  recorded.snapshots.set(source, { snapshot: "s-1", release: "r1" });
  recorded.records.set(source, { snapshot: "s-1", release: "r1" });
  recorded.calls.length = 0;
  assert.deepEqual(await fork(env.name, "fs-fork0004"), { status: 204 });
  assert.deepEqual(recorded.calls, [`sync node-a ${env.namespace}/${boundPod} c-0`, `clone ${source} ${env.namespace}/fs-fork0004 ${2 * 1024 ** 3}`]);
  await Effect.runPromise(binder.release(placement.pool, claims.key));
  // Into one bound, or being bound: none.
  const under = await run(takeLease(env.none, sessionKey("fs-fork0005"), "another-binder"));
  recorded.calls.length = 0;
  assert.equal((await fork(env.name, "fs-fork0005")).status, 409);
  assert.deepEqual(recorded.calls, []);
  await run(releaseLease(under.lease));
});

test("what of main's snapshots no workspace has a use for is collected: the snapshots of one gone forgotten, and what its node caches that none needs discarded", async () => {
  const env = await environment(1);
  const recorded = workspaceAgents();
  const { sessions } = await sessionsOf(options(env, 7290), recorded.agents);
  recorded.files.set(`${env.namespace}/fs-kept0001`, new Map());
  recorded.snapshots.set(`${env.namespace}/fs-kept0001`, { snapshot: "s-1", release: "r1" });
  recorded.snapshots.set(`${env.namespace}/fs-gone0001`, { snapshot: "s-2", release: "r1" });
  recorded.cached.set(`${env.namespace}/fs-kept0001`, ["s-1-image", "s-0-image"]);
  recorded.cached.set(`${env.namespace}/fs-gone0001`, ["s-2-image"]);
  recorded.cached.set("other-sessions/fs-other001", ["s-3-image"]);
  await Effect.runPromise(sessions.collect);
  assert.deepEqual(recorded.calls, [
    `collect ${env.namespace}/fs-kept0001`,
    `forget ${env.namespace}/fs-gone0001`,
    `discard node-a ${env.namespace}/fs-kept0001 keeping s-1-image`,
    `discard node-a ${env.namespace}/fs-gone0001 keeping none`,
  ]);
  assert.deepEqual([...recorded.snapshots.keys()], [`${env.namespace}/fs-kept0001`]);
});

test("each attach and detach is timed, and each binding released counted by why: asked by its caller, or lost as its pod went", async () => {
  const env = await environment(1);
  const recorded = agents();
  const { binder, sessions } = await sessionsOf(options(env, 7290), recorded.agents);
  const key = sessionKey("fs-timed1");
  const placement = await Effect.runPromise(sessions.place(key, { environment: env.name, mode: "none" }));
  const of = `k8s.namespace.name=${env.namespace}`;
  assert.deepEqual(await added(ATTACH_DURATION, () => Effect.runPromise(binder.bind(placement.pool, key, placement))), { [of]: 1 });
  // Released as its caller asks, as the binder's front runs its placement's release; its pod's deletion, under no gVisor, ends it.
  const bound = await Effect.runPromise(binder.bound(placement.pool, key));
  const stopping = (async () => {
    for (;;) {
      const pod = await run(withKube((kube) => kube.read("v1", "Pod", env.namespace, bound?.pod ?? ""))) as V1Pod | null;
      if (!pod || pod.metadata?.deletionTimestamp) {
        if (pod) await run(withKube((kube) => kube.patchPodStatus(env.namespace, pod.metadata?.name ?? "", { status: { containerStatuses: [{ name: "bayma", image: "agent", imageID: "", ready: false, restartCount: 0, containerID: "containerd://c-0", state: { terminated: { exitCode: 143 } } }] } })));
        return;
      }
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  })();
  const detached = await added(DETACH_DURATION, async () => {
    const asked = await added(RELEASES, async () => {
      assert.equal(await Effect.runPromise(binder.release(placement.pool, key, placement.release)), "released");
    });
    assert.deepEqual(asked, { [`${of},alasio.gateway.release.reason=asked`]: 1 });
  });
  await stopping;
  assert.deepEqual(detached, { [of]: 1 });
  const lease = { apiVersion: "coordination.k8s.io/v1", kind: "Lease", metadata: { uid: "uid-gone", namespace: env.namespace, annotations: { "alasio.dev/binding-key": key } }, spec: { holderIdentity: "none-0" } };
  assert.deepEqual(await added(RELEASES, () => Effect.runPromise(sessions.lost(env.none, lease))), { [`${of},alasio.gateway.release.reason=lost`]: 1 });
});

test("a release whose sandbox can be saved no more has its bayma go on with the stop it holds, its node's agent running its hook, and is released into no snapshot", async () => {
  const env = await environment(1, { gvisor: true, holdsStop: true });
  const stand = await bayma();
  const recorded = agents("unsaveable");
  try {
    const { binder, sessions } = await sessionsOf({ ...options(env, stand.port), releaseAfterMs: 0 }, recorded.agents);
    const key = sessionKey("fs-abc123");
    const placement = await Effect.runPromise(sessions.place(key, { environment: env.name, mode: "none" }));
    const bound = await Effect.runPromise(binder.bind(placement.pool, key, placement));
    stand.status = { execsRunning: 0, execsQueued: 0, idleSinceMs: Date.now() - 600_000 };
    const counted = await added("alasio.sandbox.releases", async () => assert.deepEqual(await Effect.runPromise(sessions.sweep), [key]));
    assert.deepEqual(counted, { "alasio.sandbox.release.outcome=unsaveable": 1 });
    assert.deepEqual(recorded.asked, [
      `checkpoint node-a ${bound.pod}`,
      `stop node-a ${bound.pod} node /opt/bayma/bayma.js sandbox-hook stop --state-dir /home/agent/.bayma`,
      `record ${env.namespace}/fs-abc123 released into none`,
      `detach node-a ${env.namespace}/${bound.pod}`,
    ]);
    assert.equal(await run(readLease(env.none, key)), null);
  } finally {
    stand.close();
  }
});

/** Its snapshot `snapshot` of the workspace `workspace` of `env`, taken under `r1` on its pool's template. */
const manifestOf = (env: SessionsEnvironment & { template: V1PodTemplateSpec }, workspace: string, snapshot: string): Manifest => ({
  environment: env.namespace,
  workspace,
  snapshot,
  release: "r1",
  cpuFeatures: ["sse"],
  podTemplate: { ...env.template },
  layers: [{ digest: `sha256:${"a".repeat(64)}`, files: [] }],
  bytes: 0,
  takenAt: "2026-10-17T12:00:00Z",
  pauseMs: 0,
  uploadMs: 0,
});

/** Node agents as `agents` makes them, which hold `current` of the workspace, and its record of `released`; their prepare does as `prepare` says. */
function restoringAgents(current: Manifest | null, released: { snapshot: string; release: string } | null, { prepare = "prepares" as "prepares" | "fails" } = {}) {
  const base = agents();
  const state = { current, released };
  const restoring: NodeAgents = {
    ...base.agents,
    released: () => Effect.sync(() => state.released),
    setReleased: (ref, into) => Effect.sync(() => (base.asked.push(`record ${ref.environment}/${ref.workspace} released into ${into?.snapshot ?? "none"}`), void (state.released = into))),
    current: () => Effect.sync(() => state.current),
    holdings: (node) => Effect.succeed({ node, releases: [{ release: "r1", cpuFeatures: ["sse", "avx"] }], cached: [] }),
    prepare: (node, _workspace, gofer) =>
      prepare === "fails"
        ? Effect.fail(new NodeAgentError({ node, call: "prepare", status: 502, reason: "the snapshots bucket failed" }))
        : Effect.sync(() => (base.asked.push(`prepare ${node}${gofer ? ` ${gofer.transport}${gofer.pagesRetry ? " retrying pages" : ""}` : ""}`), { manifest: state.current as Manifest, imagePath: "/var/cache/alasio/image", source: "cache" as const })),
    resume: (node, request) => Effect.sync(() => void base.asked.push(`resume ${node} ${request.pod} ${request.snapshot}`)),
    spend: (_ref, snapshot) => Effect.sync(() => (base.asked.push(`spend ${snapshot}`), void (state.current = null))),
    forget: (ref) => Effect.sync(() => (base.asked.push(`forget ${ref.workspace}`), void (state.current = null))),
  };
  return { ...base, state, agents: restoring };
}

test("a workspace released into its snapshot is restored from it as it is next bound, by a pod of its own of the pool's template, its workspace attached into its directory, made for it, before the pod is made, paused until its node's agent resumes it, then spent", async () => {
  const env = await environment(1, { gvisor: true });
  await run(withKube((kube) => kube.create({ apiVersion: "node.k8s.io/v1", kind: "RuntimeClass", metadata: { name: "gvisor-r1" }, handler: "runsc-r1" } as never)).pipe(Effect.ignore));
  const snapshot = "20261017T120000000Z-0000000a";
  const recorded = restoringAgents(manifestOf(env, "fs-restored", snapshot), { snapshot, release: "r1" });
  const { binder, sessions } = await sessionsOf(options(env, 7290), recorded.agents);
  const key = sessionKey("fs-restored");
  const placement = await Effect.runPromise(sessions.place(key, { environment: env.name, mode: "none" }));
  const counted = await added("alasio.sandbox.restores", async () => {
    const binding = Effect.runPromise(binder.bind(placement.pool, key, placement));
    // Its pod runs, as kubelet would run it, then, once resumed, is ready.
    const restored = await (async () => {
      for (;;) {
        const [pod] = (await run(withKube((kube) => kube.list("v1", "Pod", env.namespace, "alasio.dev/binding")))) as V1Pod[];
        if (pod?.metadata?.name?.startsWith("sandboxes-none-restore-")) return pod;
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
    })();
    const name = restored.metadata?.name ?? "";
    // Never started again, as its container's start restores it from its snapshot.
    assert.equal(restored.spec?.restartPolicy, "Never");
    // Its workspace attached into its directory on the node it is restored on, made for it, before the pod was made.
    assert.deepEqual(recorded.attached.map(({ node, pod, made }) => ({ node, pod, made })), [{ node: "node-a", pod: name, made: true }]);
    assert.deepEqual(recorded.asked, [`prepare node-a socket retrying pages`]);
    await run(withKube((kube) => kube.patchPodStatus(env.namespace, name, { status: { phase: "Running", podIP: "127.0.0.1", podIPs: [{ ip: "127.0.0.1" }], containerStatuses: [{ name: "bayma", image: "agent", imageID: "", ready: false, restartCount: 0, containerID: "containerd://c-restored", state: { running: {} } }] } })));
    for (;;) {
      const pod = (await run(withKube((kube) => kube.read("v1", "Pod", env.namespace, name)))) as V1Pod;
      if (pod.status?.conditions?.some(({ type, status }) => type === "alasio.dev/resumed" && status === "True")) break;
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    await run(withKube((kube) => kube.patchPodStatus(env.namespace, name, { status: { conditions: [{ type: "Ready", status: "True" }] } })));
    const bound = await binding;
    assert.equal(bound.pod, name);
    // Of the pool's template, its sandbox restored paused from the image its node's agent prepared, under the release that took it, on that node.
    assert.deepEqual([restored.metadata?.annotations?.["dev.gvisor.internal.restore.host-image-path"], restored.metadata?.annotations?.["dev.gvisor.internal.restore.paused"]], ["/var/cache/alasio/image", "true"]);
    assert.equal(restored.spec?.runtimeClassName, "gvisor-r1");
    const plain = (value: unknown) => JSON.parse(JSON.stringify(value)) as unknown;
    // Its own directory named by its volume's path, not a subpath, so its workspace, attached first, is bound into its container with it.
    assert.deepEqual(plain(restored.spec?.volumes?.find(({ name }) => name === "attach")), { name: "attach", hostPath: { path: `/var/lib/alasio/attach/${env.namespace}/${name}`, type: "Directory" } });
    assert.deepEqual(plain(restored.spec?.containers[0]?.volumeMounts), [{ name: "attach", mountPath: "/attach", mountPropagation: "HostToContainer" }]);
    assert.equal(restored.spec?.initContainers, undefined);
    assert.deepEqual(plain(restored.spec?.readinessGates), [{ conditionType: "alasio.dev/resumed" }]);
    assert.deepEqual(plain(restored.spec?.affinity?.nodeAffinity?.requiredDuringSchedulingIgnoredDuringExecution?.nodeSelectorTerms), [{ matchFields: [{ key: "metadata.name", operator: "In", values: ["node-a"] }] }]);
    // Bound from the first, and no pool's free pod, which its ReplicaSet would take for its own.
    assert.equal(restored.metadata?.labels?.["alasio.dev/free"], undefined);
    assert.equal(restored.metadata?.annotations?.["alasio.dev/binding-key"], key);
    // Its checkpoint gofer as the release that took it restores best.
    assert.deepEqual(recorded.asked, [`prepare node-a socket retrying pages`, `resume node-a ${name} ${snapshot}`, `spend ${snapshot}`, `record ${env.namespace}/fs-restored released into none`]);
    assert.equal(recorded.attached[0]?.pod, name);
  });
  assert.deepEqual(counted, { "alasio.sandbox.restore.outcome=restored": 1 });
  // A free pod of the pool's claimed by none.
  assert.equal(((await run(withKube((kube) => kube.list("v1", "Pod", env.namespace, "alasio.dev/free=true")))) as V1Pod[]).length, 1);
});

/**
 * A restore of `workspace` whose bayma fails to start as `status` says, of a pool of two free
 * pods: what its bind came to, what the agents were asked, what was abandoned, and what was counted.
 */
async function failedRestore(workspace: string, status: Record<string, unknown>) {
  const env = await environment(2, { gvisor: true });
  await run(withKube((kube) => kube.create({ apiVersion: "node.k8s.io/v1", kind: "RuntimeClass", metadata: { name: "gvisor-r1" }, handler: "runsc-r1" } as never)).pipe(Effect.ignore));
  const snapshot = "20261017T120000000Z-0000000c";
  const recorded = restoringAgents(manifestOf(env, workspace, snapshot), { snapshot, release: "r1" });
  const abandoned: string[] = [];
  const agents: NodeAgents = { ...recorded.agents, abandon: (node, request) => Effect.sync(() => (abandoned.push(`${node} ${request.pod}`), [])) };
  const { binder, sessions } = await sessionsOf(options(env, 7290), agents);
  const key = sessionKey(workspace);
  const placement = await Effect.runPromise(sessions.place(key, { environment: env.name, mode: "none" }));
  let name = "";
  const counted = await added("alasio.sandbox.restores", async () => {
    const binding = Effect.runPromise(Effect.result(binder.bind(placement.pool, key, placement)));
    const restored = await (async () => {
      for (;;) {
        const [pod] = (await run(withKube((kube) => kube.list("v1", "Pod", env.namespace, "alasio.dev/binding")))) as V1Pod[];
        if (pod?.metadata?.name?.startsWith("sandboxes-none-restore-")) return pod;
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
    })();
    name = restored.metadata?.name ?? "";
    assert.deepEqual(recorded.attached.map(({ pod, made }) => ({ pod, made })), [{ pod: name, made: true }]);
    await run(withKube((kube) => kube.patchPodStatus(env.namespace, name, { status: { phase: "Running", containerStatuses: [{ name: "bayma", image: "agent", imageID: "", ready: false, ...status }] } })));
    return binding;
  });
  // What was attached for it detached once its pod, which its binder deletes, has gone.
  for (const deadline = Date.now() + 10_000; !recorded.asked.includes(`detach node-a ${env.namespace}/${name}`); await new Promise((resolve) => setTimeout(resolve, 100))) {
    assert.ok(Date.now() < deadline, `what was attached for ${name} was not detached: ${recorded.asked.join(", ")}`);
  }
  return { counted, recorded, abandoned, name, bindAgain: () => Effect.runPromise(Effect.result(binder.bind(placement.pool, key, placement))) };
}

test("a restore runsc refuses, its kernel failing to restore the sandbox, falls back at once, abandoned, its snapshot forgotten, the workspace bound to a free pod", async () => {
  const { counted, recorded, abandoned, name, bindAgain } = await failedRestore("fs-refused", {
    restartCount: 1,
    state: { waiting: { reason: "CrashLoopBackOff" } },
    lastState: { terminated: { exitCode: 128, reason: "StartError", message: "failed to start containerd task \"bayma\": OCI runtime restore failed: vfs.CompleteRestore() failed" } },
  });
  assert.deepEqual(counted, { "alasio.sandbox.restore.outcome=fallback,alasio.sandbox.restore.fallback=restore": 1 });
  assert.deepEqual(abandoned, [`node-a ${name}`]);
  assert.ok(recorded.asked.includes("forget fs-refused"), recorded.asked.join("\n"));
  const bound = await bindAgain();
  assert.ok(bound._tag === "Success" && /^none-/u.test(bound.success.pod));
});

test("a restore whose pod cannot start otherwise, as a node's hiccup fails it, is deferred, abandoned, its snapshot kept for the next bind to restore", async () => {
  const { counted, recorded, abandoned, name } = await failedRestore("fs-hiccup", {
    restartCount: 0,
    state: { waiting: { reason: "CreateContainerError", message: "context deadline exceeded" } },
  });
  assert.deepEqual(counted, { "alasio.sandbox.restore.outcome=deferred,alasio.sandbox.restore.fallback=restore": 1 });
  assert.deepEqual(abandoned, [`node-a ${name}`]);
  assert.ok(!recorded.asked.some((asked) => asked.startsWith("forget")), recorded.asked.join("\n"));
  // Its snapshot, and its record of it, kept, for its next bind to restore, within DEFER_FOR_MS.
  assert.notEqual(recorded.state.current, null);
  assert.notEqual(recorded.state.released, null);
});

test("a workspace whose record names a snapshot it holds no more, or another than its current one, is bound to a free pod, its snapshot and its record forgotten", async () => {
  for (const [current, into, why] of [
    [null, "20261017T120000000Z-0000000a", "a record of a snapshot gone"],
    ["20261017T130000000Z-0000000b", "20261017T120000000Z-0000000a", "a record of a snapshot moved on from"],
    ["20261017T130000000Z-0000000b", null, "a snapshot no release recorded"],
  ] as const) {
    const env = await environment(1, { gvisor: true });
    const recorded = restoringAgents(current ? manifestOf(env, "fs-stale", current) : null, into ? { snapshot: into, release: "r1" } : null);
    const { binder, sessions } = await sessionsOf(options(env, 7290), recorded.agents);
    const key = sessionKey("fs-stale");
    const placement = await Effect.runPromise(sessions.place(key, { environment: env.name, mode: "none" }));
    const counted = await added("alasio.sandbox.restores", async () => assert.equal((await Effect.runPromise(binder.bind(placement.pool, key, placement))).pod, "none-0", why));
    assert.deepEqual(counted, { "alasio.sandbox.restore.outcome=fallback,alasio.sandbox.restore.fallback=stale": 1 }, why);
    assert.deepEqual([recorded.state.current, recorded.state.released], [null, null], why);
    assert.deepEqual(recorded.asked, [`forget fs-stale`, `record ${env.namespace}/fs-stale released into none`], why);
  }
});

test("a restore whose snapshot cannot be prepared now is deferred, the bind refused to be tried again, its snapshot kept; and one deferred for long enough falls back", async () => {
  const env = await environment(1, { gvisor: true });
  const snapshot = "20261017T120000000Z-0000000a";
  const recorded = restoringAgents(manifestOf(env, "fs-deferred", snapshot), { snapshot, release: "r1" }, { prepare: "fails" });
  const { binder, sessions } = await sessionsOf(options(env, 7290), recorded.agents);
  const key = sessionKey("fs-deferred");
  const placement = await Effect.runPromise(sessions.place(key, { environment: env.name, mode: "none" }));
  // Its deferrals timed by a clock of the test's, from its first.
  const later = await added("alasio.sandbox.restores", () =>
    Effect.runPromise(Effect.gen(function*() {
      const refused = yield* Effect.flip(binder.bind(placement.pool, key, placement));
      assert.match(refused.message, /deferred, to be tried again: .*the snapshots bucket failed/u);
      assert.equal(recorded.state.current?.snapshot, snapshot);
      assert.equal(yield* Effect.promise(() => run(readLease(env.none, key))), null);
      // Deferred for long enough: bound to a free pod, its snapshot forgotten.
      yield* TestClock.adjust(DEFER_FOR_MS);
      return yield* binder.bind(placement.pool, key, placement);
    }).pipe(Effect.provide(TestClock.layer()))));
  assert.deepEqual(later, {
    "alasio.sandbox.restore.outcome=deferred,alasio.sandbox.restore.fallback=prepare": 1,
    "alasio.sandbox.restore.outcome=fallback,alasio.sandbox.restore.fallback=prepare": 1,
  });
  assert.equal(recorded.state.current, null);
  assert.equal((await run(readLease(env.none, key)))?.spec?.holderIdentity, "none-0");
});

test("a bound pod ended by other than the gateway, as a drain ends one, is saved as its bayma holds its stop, its release recorded, its workspace detached once its containers stopped, and its binding let go", async () => {
  const env = await environment(1, { gvisor: true, holdsStop: true });
  const recorded = agents();
  const { binder, sessions } = await sessionsOf(options(env, 7290), recorded.agents);
  const key = sessionKey("fs-drained");
  const placement = await Effect.runPromise(sessions.place(key, { environment: env.name, mode: "none" }));
  const bound = await Effect.runPromise(binder.bind(placement.pool, key, placement));
  // Deleted as a drain deletes it: with its grace, kubelet to end it.
  await run(withKube((kube) => kube.remove("v1", "Pod", env.namespace, bound.pod)));
  const counted = await added("alasio.sandbox.stop_releases", async () => {
    assert.deepEqual(await Effect.runPromise(Effect.provide(sessions.drain, Layer.fresh(KubeClient.layer))), [key]);
    // Begun once, however often it is looked at meanwhile.
    assert.deepEqual(await Effect.runPromise(Effect.provide(sessions.drain, Layer.fresh(KubeClient.layer))), []);
    for (const deadline = Date.now() + 10_000; (await run(readLease(env.none, key))) !== null; await new Promise((resolve) => setTimeout(resolve, 100))) {
      assert.ok(Date.now() < deadline, "its binding was not let go");
    }
  });
  assert.deepEqual(counted, { "alasio.sandbox.stop_release.outcome=snapshotted": 1 });
  assert.deepEqual(recorded.asked, [`checkpoint node-a ${bound.pod}`, `record ${env.namespace}/fs-drained released into s-1`, `detach node-a ${env.namespace}/${bound.pod}`]);
  // Told once its release is through, which lets its binding go first.
  const toldReleased = () => told.some((event) => event.kind === "workspace.released" && event.subject.key === key && event.reason === "deleted" && event.binding === bound.binding);
  for (const deadline = Date.now() + 10_000; !toldReleased(); await new Promise((resolve) => setTimeout(resolve, 50))) {
    assert.ok(Date.now() < deadline, "its release was not told");
  }
});

test("a bound pod ending whose binding another releases, as a broken one's is, is begun released once, however long its pod takes to go", async () => {
  const env = await environment(1, { gvisor: true, holdsStop: true });
  const recorded = agents();
  const { binder, sessions } = await sessionsOf(options(env, 7290), recorded.agents);
  const key = sessionKey("fs-elsewhere");
  const placement = await Effect.runPromise(sessions.place(key, { environment: env.name, mode: "none" }));
  const bound = await Effect.runPromise(binder.bind(placement.pool, key, placement));
  const lease = await run(readLease(env.none, key));
  assert.ok(lease);
  await run(markReleasing(lease, "another-binder"));
  await run(withKube((kube) => kube.remove("v1", "Pod", env.namespace, bound.pod)));
  const drain = () => Effect.runPromise(Effect.provide(sessions.drain, Layer.fresh(KubeClient.layer)));
  assert.deepEqual(await drain(), [key]);
  // Its release one of none, as another's runs.
  await new Promise((resolve) => setTimeout(resolve, 1_000));
  assert.deepEqual(await drain(), []);
  assert.deepEqual(recorded.asked, []);
});

/** A stand-in for bayma's /status that says its state generation too, as bayma 0.14 does. */
async function baymaOfGeneration(): Promise<{ port: number; status: { execsRunning: number; execsQueued: number; idleSinceMs: number | null; generation: number }; close: () => void }> {
  const stand = { status: { execsRunning: 0, execsQueued: 0, idleSinceMs: 0 as number | null, generation: 1 } };
  const server = createServer((_incoming, outgoing) => void outgoing.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify(stand.status)));
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  return { port: (server.address() as AddressInfo).port, get status() { return stand.status; }, set status(value) { stand.status = value; }, close: () => server.close() };
}

test("a bound pod whose execs run is saved live every liveEvery, its sandbox running on, and its snapshot spent once bayma's generation moves on from it", async () => {
  const env = await environment(1, { gvisor: true });
  const stand = await baymaOfGeneration();
  const recorded = agents();
  const spent: string[] = [];
  try {
    const opts = options(env, stand.port);
    const { binder, sessions } = await sessionsOf({ ...opts, snapshots: { ...opts.snapshots!, liveEverySeconds: 1 } }, { ...recorded.agents, spend: (_ref, snapshot) => Effect.sync(() => void spent.push(snapshot)) });
    const key = sessionKey("fs-busy");
    const placement = await Effect.runPromise(sessions.place(key, { environment: env.name, mode: "none" }));
    const bound = await Effect.runPromise(binder.bind(placement.pool, key, placement));
    stand.status = { execsRunning: 1, execsQueued: 0, idleSinceMs: null, generation: 1 };
    const look = () => Effect.runPromise(Effect.provide(sessions.live, Layer.fresh(KubeClient.layer)));
    const counted = await added("alasio.sandbox.live_checkpoints", async () => {
      // Its execs begun: not saved live before liveEvery has passed since.
      await look();
      assert.deepEqual(recorded.asked, [] as string[]);
      await new Promise((resolve) => setTimeout(resolve, 1_100));
      await look();
      for (const deadline = Date.now() + 5_000; !recorded.asked.includes(`checkpoint node-a ${bound.pod} live`); await new Promise((resolve) => setTimeout(resolve, 50))) assert.ok(Date.now() < deadline);
      assert.deepEqual(recorded.saved.at(-1)?.live, { stateDir: "/home/agent/.bayma" });
      // bayma moved on from the generation it caught: spent.
      stand.status = { execsRunning: 0, execsQueued: 0, idleSinceMs: Date.now(), generation: 2 };
      await look();
    });
    assert.deepEqual(spent, ["s-live"]);
    assert.deepEqual(counted, { "alasio.sandbox.live_checkpoint.outcome=taken": 1, "alasio.sandbox.live_checkpoint.outcome=spent": 1 });
  } finally {
    stand.close();
  }
});

test("a bound pod whose bayma ended in it, or was started again, is released as broken, into the snapshot taken of it live; and one whose pod went, as lost, into it too, recorded while its Lease holds", async () => {
  const env = await environment(2, { gvisor: true });
  const recorded = agents();
  // Saved live once it is bound.
  let saved = false;
  const live = { ...recorded.agents, current: () => Effect.succeed(saved ? ({ snapshot: "s-live", release: "r1", live: { stateDir: "/home/agent/.bayma", generation: 1, running: [] } } as unknown as Manifest) : null) };
  const { binder, sessions } = await sessionsOf(options(env, 7290), live);
  const key = sessionKey("fs-broken");
  const placement = await Effect.runPromise(sessions.place(key, { environment: env.name, mode: "none" }));
  const bound = await Effect.runPromise(binder.bind(placement.pool, key, placement));
  saved = true;
  // Started again in its pod, as kubelet starts a container that ended.
  await run(withKube((kube) => kube.patchPodStatus(env.namespace, bound.pod, { status: { containerStatuses: [{ name: "bayma", image: "agent", imageID: "", ready: false, restartCount: 1, containerID: "containerd://c-0", state: { waiting: { reason: "CrashLoopBackOff" } } }] } })));
  // Its deletion ends it, as kubelet would.
  const ending = (async () => {
    for (;;) {
      const pod = (await run(withKube((kube) => kube.read("v1", "Pod", env.namespace, bound.pod)))) as V1Pod | null;
      if (pod?.metadata?.deletionTimestamp) {
        await run(withKube((kube) => kube.patchPodStatus(env.namespace, bound.pod, { status: { containerStatuses: [{ name: "bayma", image: "agent", imageID: "", ready: false, restartCount: 1, containerID: "containerd://c-0", state: { terminated: { exitCode: 1 } } }] } })));
        return;
      }
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  })();
  await Effect.runPromise(Effect.provide(sessions.live, Layer.fresh(KubeClient.layer)));
  await ending;
  for (const deadline = Date.now() + 10_000; (await run(readLease(env.none, key))) !== null; await new Promise((resolve) => setTimeout(resolve, 100))) assert.ok(Date.now() < deadline, "its binding was not let go");
  assert.deepEqual(recorded.asked, [`record ${env.namespace}/fs-broken released into s-live`, `detach node-a ${env.namespace}/${bound.pod}`]);
  assert.ok(told.some((event) => event.kind === "workspace.released" && event.subject.key === key && event.reason === "broken"));
  // One whose pod went: released into it as its Lease is let go.
  const lease = { apiVersion: "coordination.k8s.io/v1", kind: "Lease", metadata: { uid: "uid-gone", namespace: env.namespace, annotations: { "alasio.dev/binding-key": sessionKey("fs-gone") } }, spec: { holderIdentity: "none-9" } };
  await Effect.runPromise(sessions.recordLost(env.none, lease));
  assert.equal(recorded.asked.at(-1), `record ${env.namespace}/fs-gone released into s-live`);
});

test("a bound pod whose workspace its node can reach no more, as its node's mount of workspace storage ended under it, is released as broken, into the snapshot taken of it live; one whose workspace is reachable is kept", async () => {
  const env = await environment(2, { gvisor: true });
  const recorded = agents();
  let saved = false;
  let unreachable = "";
  const live: NodeAgents = {
    ...recorded.agents,
    current: () => Effect.succeed(saved ? ({ snapshot: "s-live", release: "r1", live: { stateDir: "/home/agent/.bayma", generation: 1, running: [] } } as unknown as Manifest) : null),
    attachments: (node) => Effect.sync(() => (recorded.asked.push(`attachments ${node}`), [{ namespace: env.namespace, pod: unreachable, reachable: false }])),
  };
  const { binder, sessions } = await sessionsOf(options(env, 7291), live);
  const key = sessionKey("fs-unreachable");
  const kept = sessionKey("fs-reachable");
  const placement = await Effect.runPromise(sessions.place(key, { environment: env.name, mode: "none" }));
  const bound = await Effect.runPromise(binder.bind(placement.pool, key, placement));
  const other = await Effect.runPromise(binder.bind(placement.pool, kept, await Effect.runPromise(sessions.place(kept, { environment: env.name, mode: "none" }))));
  saved = true;
  unreachable = bound.pod;
  // Its deletion ends it, as kubelet would.
  const ending = (async () => {
    for (;;) {
      const pod = (await run(withKube((kube) => kube.read("v1", "Pod", env.namespace, bound.pod)))) as V1Pod | null;
      if (pod?.metadata?.deletionTimestamp) {
        await run(withKube((kube) => kube.patchPodStatus(env.namespace, bound.pod, { status: { containerStatuses: [{ name: "bayma", image: "agent", imageID: "", ready: false, restartCount: 0, containerID: "containerd://c-0", state: { terminated: { exitCode: 1 } } }] } })));
        return;
      }
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  })();
  await Effect.runPromise(Effect.provide(sessions.live, Layer.fresh(KubeClient.layer)));
  await ending;
  // Its binding let go, then its release told, once its Lease is gone.
  const toldBroken = () => told.some((event) => event.kind === "workspace.released" && event.subject.key === key && event.reason === "broken");
  for (const deadline = Date.now() + 10_000; (await run(readLease(env.none, key))) !== null || !toldBroken(); await new Promise((resolve) => setTimeout(resolve, 100))) {
    assert.ok(Date.now() < deadline, "its binding was not let go as broken");
  }
  // Its node was asked once, for both.
  assert.deepEqual(recorded.asked, ["attachments node-a", `record ${env.namespace}/fs-unreachable released into s-live`, `detach node-a ${env.namespace}/${bound.pod}`]);
  assert.notEqual(await run(readLease(env.none, kept)), null);
  assert.ok(!told.some((event) => event.kind === "workspace.released" && event.subject.key === kept));
  assert.notEqual(other.pod, bound.pod);
});
