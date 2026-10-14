import assert from "node:assert/strict";
import { test } from "node:test";

import type { KubernetesObject, V1Pod, V1PodTemplateSpec } from "@kubernetes/client-node";
import { Effect, Fiber, Layer } from "effect";
import { FetchHttpClient } from "effect/http";
import { TestClock } from "effect/testing";

// First: alasio's modules create their metrics as they load.
import { added } from "./support/metrics.ts";
import { KubeApiError, KubeClient } from "../src/kube/client.ts";
import type { SessionsProfile } from "../src/kube/config.ts";
import { type Sandbox, type Sandboxes, type Saved, SNAPSHOT_ANNOTATION, type StoredSandbox } from "../src/kube/sandboxes.ts";
import type { CheckpointRequest, Manifest } from "../src/node-agent/agent.ts";
import { sessionSandboxManifest } from "../src/sandbox/index.ts";
import { type Holdings, NodeAgentError, type NodeAgents } from "../src/sandbox/node-agents.ts";
import { CPU_FEATURES_ANNOTATION, RESUMED_CONDITION, SESSION_TEMPLATE_ANNOTATION } from "../src/kube/sandbox-shape.ts";
import { cpuBaseline, hookOf, makeSessionSnapshots, sessionTemplateHash, withCpuBaseline } from "../src/sandbox/snapshots.ts";

const CURRENT = "20260921.0-eaucoin.4";
const PREVIOUS = "20260921.0-eaucoin.3";

const snapshots = {
  releaseAfterSeconds: 300,
  releases: [{ release: CURRENT, runtimeClassName: "gvisor-20260921-0-eaucoin-4" }, { release: PREVIOUS, runtimeClassName: "gvisor-20260921-0-eaucoin-3" }],
  agents: { namespace: "alasio", selector: "app.kubernetes.io/component=node-agent", port: 8484 },
};

const profile = (image = "agent@sha256:1"): SessionsProfile => ({
  namespace: "alasio-sessions",
  port: 7290,
  workspaceDir: "/workspace",
  podTemplate: {
    spec: {
      runtimeClassName: "gvisor",
      containers: [{
        name: "bayma",
        image,
        command: ["/usr/bin/tini", "-s", "--", "node", "/opt/bayma/bayma.js"],
        args: ["mcp-http", "--port", "7290", "--state-dir", "/home/agent/.bayma"],
      }],
    },
  },
  snapshots,
});

/** The Sandbox of session `name` alasio makes from `of`. */
const sessionOf = (of: SessionsProfile, name = "fs-abc123"): Sandbox => sessionSandboxManifest({ volumeId: name, netMode: "none", profile: of, telemetry: null });

/** `sandbox` as the API server holds it, suspended, released into the snapshot `s-1` (or `bound`), or running. */
const stored = (sandbox: Sandbox, operatingMode: "Running" | "Suspended" = "Suspended", bound: string | null = "s-1"): StoredSandbox => ({
  ...sandbox,
  metadata: {
    ...sandbox.metadata,
    uid: "uid-1",
    ...(operatingMode === "Suspended" && bound !== null ? { annotations: { ...sandbox.metadata.annotations, [SNAPSHOT_ANNOTATION]: bound } } : {}),
  },
  spec: { ...sandbox.spec, operatingMode },
});

/** A manifest of a snapshot of `podTemplate`, taken under `release`, saved with `cpuFeatures`. */
const manifestOf = (podTemplate: V1PodTemplateSpec, release = CURRENT, cpuFeatures: readonly string[] = ["sse", "avx"]): Manifest => ({
  environment: "alasio-sessions",
  workspace: "fs-abc123",
  snapshot: "s-1",
  release,
  image: "sha256:image",
  cpuFeatures,
  podTemplate: podTemplate as Readonly<Record<string, unknown>>,
  files: [],
  bytes: 1024,
  takenAt: "2026-10-12T20:00:00.000Z",
  pauseMs: 80,
  uploadMs: 120,
});

/** Node agents in memory: the current snapshot, each node's holdings, and how a prepare goes on each; what they were asked. */
function fakeAgents({ current = null, holdings = [], prepare = () => null, read, resumed = null, held = [] }: {
  /** The workspaces the bucket holds anything of. */
  readonly held?: readonly string[];
  readonly current?: Manifest | null;
  readonly holdings?: readonly Holdings[];
  readonly prepare?: (node: string) => string | null;
  readonly read?: NodeAgentError;
  /** Why the agent does not resume a restored sandbox, or null when it does. */
  readonly resumed?: string | null;
} = {}) {
  const asked: string[] = [];
  const checkpoints: (readonly [string, CheckpointRequest])[] = [];
  const agents: NodeAgents = {
    nodes: Effect.succeed(holdings.map(({ node }) => node)),
    checkpoint: (node, request) =>
      Effect.sync(() => {
        checkpoints.push([node, request]);
        return manifestOf(request.podTemplate as V1PodTemplateSpec, request.release);
      }),
    prepare: (node) =>
      Effect.suspend(() => {
        asked.push(`prepare on ${node}`);
        const refused = prepare(node);
        return refused === null
          ? Effect.succeed({ manifest: current as Manifest, imagePath: `/var/lib/alasio/snapshots/alasio-sessions/fs-abc123/s-1` })
          : Effect.fail(new NodeAgentError({ node, call: "prepare alasio-sessions/fs-abc123's snapshot", status: 409, reason: refused }));
      }),
    current: () => (read ? Effect.fail(read) : Effect.succeed(current)),
    copy: () => Effect.die("nothing is copied"),
    forget: (workspace) => Effect.sync(() => void asked.push(`forget ${workspace.environment}/${workspace.workspace}`)),
    resume: (node, request) =>
      Effect.suspend(() => {
        asked.push(`resume on ${node} ${request.podIPs.join(",")} ${request.containerIds.length}`);
        return resumed === null ? Effect.void : Effect.fail(new NodeAgentError({ node, call: "resume alasio-sessions/fs-abc123", status: 504, reason: resumed }));
      }),
    holdings: (node) => Effect.succeed(holdings.find((holding) => holding.node === node) as Holdings),
    workspaces: () => Effect.succeed(held),
    discard: (node, workspace, keep) =>
      Effect.sync(() => {
        asked.push(`discard ${workspace.environment}/${workspace.workspace} on ${node} but ${keep}`);
        return [];
      }),
  };
  return { agents, asked, checkpoints };
}

/** A node holding `releases`, each with the features `cpuFeatures`, and caching `cached` snapshots. */
const node = (name: string, releases: readonly string[], cpuFeatures: readonly string[] = ["sse", "avx", "avx2"], cached: readonly string[] = []): Holdings => ({
  node: name,
  releases: releases.map((release) => ({ release, cpuFeatures })),
  cached: cached.map((snapshot) => ({ environment: "alasio-sessions", workspace: "fs-abc123", snapshot, bytes: 1024, usedAt: "2026-10-12T20:00:00.000Z" })),
});

/** A KubeClient of ConfigMaps and Pods in memory, which records what it writes. */
function fakeKube(objects: Map<string, KubernetesObject> = new Map()) {
  const writes: string[] = [];
  const key = (kind: string, namespace: string, name: string) => `${kind}/${namespace}/${name}`;
  const service = KubeClient.of({
    read: (_apiVersion, kind, namespace, name) => Effect.sync(() => structuredClone(objects.get(key(kind, namespace, name)) ?? null)),
    list: (_apiVersion, kind, namespace) => Effect.sync(() => [...objects.values()].filter((object) => object.kind === kind && object.metadata?.namespace === namespace)),
    create: (object) =>
      Effect.sync(() => {
        writes.push(`create ${object.kind}`);
        objects.set(key(object.kind ?? "", object.metadata?.namespace ?? "", object.metadata?.name ?? ""), structuredClone(object));
        return object;
      }),
    replace: (object) =>
      Effect.sync(() => {
        writes.push(`replace ${object.kind}`);
        objects.set(key(object.kind ?? "", object.metadata?.namespace ?? "", object.metadata?.name ?? ""), structuredClone(object));
        return object;
      }),
    patch: () => Effect.fail(new KubeApiError({ status: 500, cause: new Error("nothing is patched") })),
    patchPodStatus: (namespace, name, patch) =>
      Effect.sync(() => {
        writes.push(`patch Pod/${name}'s status ${JSON.stringify(patch)}`);
        const pod = objects.get(key("Pod", namespace, name)) as V1Pod | undefined;
        const conditions = (patch as V1Pod).status?.conditions ?? [];
        if (pod) pod.status = { ...pod.status, conditions: [...(pod.status?.conditions ?? []).filter(({ type }) => !conditions.some((set) => set.type === type)), ...conditions] };
      }),
    remove: () => Effect.fail(new KubeApiError({ status: 500, cause: new Error("nothing is removed") })),
    exec: () => Effect.die("nothing is run"),
  });
  return { objects, writes, layer: Layer.succeed(KubeClient, service) };
}

/** `effect` run on `kube`, its bayma's status answered by `status`. */
function run<A, E>(kube: ReturnType<typeof fakeKube>, effect: Effect.Effect<A, E, KubeClient>, status: () => unknown = () => ({ execsRunning: 0, execsQueued: 0, idleSinceMs: 0 })): Promise<A> {
  const fetch = (async () => new Response(JSON.stringify(status()), { status: 200 })) as unknown as typeof globalThis.fetch;
  return Effect.runPromise(effect.pipe(Effect.provide(Layer.merge(kube.layer, Layer.succeed(FetchHttpClient.Fetch, fetch)))));
}

const templates = (current: SessionsProfile, previous: SessionsProfile | null = null) => ({ current: sessionTemplateHash(current), previous: previous && sessionTemplateHash(previous) });

test("bayma's hook is bayma as its container runs it, told the state directory it serves", () => {
  const [bayma] = profile().podTemplate.spec?.containers ?? [];
  assert.ok(bayma);
  assert.deepEqual(hookOf(bayma), ["node", "/opt/bayma/bayma.js", "sandbox-hook", "--state-dir", "/home/agent/.bayma"]);
  assert.deepEqual(hookOf({ name: "bayma", command: ["bayma"], args: ["mcp-http", "--state-dir", "/state"] }), ["bayma", "sandbox-hook", "--state-dir", "/state"]);
  assert.equal(hookOf({ name: "bayma", command: ["bayma"], args: ["mcp-http"] }), null);
});

test("a session's pod template records the profile's template it is made from", () => {
  const made = sessionOf(profile());
  assert.equal(made.spec.podTemplate.metadata?.annotations?.[SESSION_TEMPLATE_ANNOTATION], sessionTemplateHash(profile()));
  assert.notEqual(sessionTemplateHash(profile()), sessionTemplateHash(profile("agent@sha256:2")));
});

test("sessions are held to the CPU features every node has under the current release, which their template records", async () => {
  const { agents } = fakeAgents({ holdings: [node("node-a", [CURRENT, PREVIOUS], ["sse", "avx", "avx2"]), { ...node("node-b", [CURRENT], ["sse", "avx2", "fma"]) }] });
  const baseline = await Effect.runPromise(cpuBaseline(agents, CURRENT));
  assert.deepEqual(baseline, ["sse", "avx2"]);
  const held = withCpuBaseline(profile(), baseline ?? []);
  assert.equal(held.podTemplate.metadata?.annotations?.[CPU_FEATURES_ANNOTATION], "sse,avx2");
  assert.notEqual(sessionTemplateHash(held), sessionTemplateHash(profile()));
  assert.equal(sessionOf(held).spec.podTemplate.metadata?.annotations?.[CPU_FEATURES_ANNOTATION], "sse,avx2");
  // With no agent to tell, none.
  const none = await Effect.runPromise(Effect.gen(function*() {
    const telling = yield* Effect.forkChild(cpuBaseline(fakeAgents().agents, CURRENT));
    while (telling.pollUnsafe() === undefined) yield* TestClock.adjust("2 seconds");
    return yield* Fiber.join(telling);
  }).pipe(Effect.provide(TestClock.layer())));
  assert.equal(none, null);
});

test("a session with a snapshot on the current template is restored on a node that can, the one holding it first, into what it is now", async () => {
  const desired = sessionOf(profile());
  const manifest = manifestOf(desired.spec.podTemplate);
  const { agents, asked } = fakeAgents({
    current: manifest,
    holdings: [node("node-a", [CURRENT], ["sse"]), node("node-b", [PREVIOUS]), node("node-c", [CURRENT]), node("node-d", [CURRENT], undefined, ["s-1"])],
    prepare: (on) => (on === "node-d" ? "its image does not match its manifest" : null),
  });
  const plan = await run(fakeKube(), Effect.gen(function*() {
    const snapshotted = yield* makeSessionSnapshots({ profile: profile(), snapshots, agents, templates: templates(profile()) });
    return yield* snapshotted.restore(stored(desired), desired);
  }));
  assert.ok(plan);
  // node-a lacks avx, node-b the release; node-d holds it, and fails.
  assert.deepEqual(asked, ["prepare on node-d", "prepare on node-c"]);
  assert.deepEqual(plan.kept, desired.spec.podTemplate);
  assert.equal(plan.podTemplate.metadata?.annotations?.["dev.gvisor.internal.restore.host-image-path"], "/var/lib/alasio/snapshots/alasio-sessions/fs-abc123/s-1");
  assert.equal(plan.podTemplate.spec?.runtimeClassName, "gvisor-20260921-0-eaucoin-4");
  assert.deepEqual(plan.podTemplate.spec?.affinity?.nodeAffinity?.requiredDuringSchedulingIgnoredDuringExecution, {
    nodeSelectorTerms: [{ matchFields: [{ key: "metadata.name", operator: "In", values: ["node-c"] }] }],
  });
  assert.deepEqual(plan.podTemplate.spec?.containers, desired.spec.podTemplate.spec?.containers);
  // Restored, the session moves on from it, counted, with how long it took to come back.
  const timed = await added("alasio.sandbox.restore.duration", async () => {
    assert.deepEqual(await added("alasio.sandbox.restores", () => Effect.runPromise(plan.restored)), { "alasio.sandbox.restore.outcome=restored": 1 });
  });
  assert.deepEqual(timed, { "": 1 });
  assert.deepEqual(asked.slice(2), ["forget alasio-sessions/fs-abc123"]);
});

test("a restored pod is let run once its containers run, by its node's agent, then made ready; one that ends, or that the agent does not resume, is refused", async () => {
  const desired = sessionOf(profile());
  const restoredPod = (state: "running" | "terminated" | "waiting"): V1Pod => ({
    apiVersion: "v1",
    kind: "Pod",
    metadata: { name: "fs-abc123", namespace: "alasio-sessions" },
    spec: { nodeName: "node-a", containers: [] },
    status: {
      podIPs: [{ ip: "10.42.0.7" }],
      containerStatuses: [{
        name: "bayma",
        containerID: `containerd://${"b".repeat(64)}`,
        image: "",
        imageID: "",
        ready: false,
        restartCount: 0,
        state: state === "running" ? { running: {} } : state === "terminated" ? { terminated: { exitCode: 128 } } : { waiting: { reason: "ContainerCreating" } },
      }],
    },
  });
  const resume = async (pod: V1Pod, resumed: string | null = null) => {
    const kube = fakeKube(new Map<string, KubernetesObject>([["Pod/alasio-sessions/fs-abc123", pod]]));
    const { agents, asked } = fakeAgents({ current: manifestOf(desired.spec.podTemplate), holdings: [node("node-a", [CURRENT])], resumed });
    const outcome = await run(kube, Effect.gen(function*() {
      const snapshotted = yield* makeSessionSnapshots({ profile: profile(), snapshots, agents, templates: templates(profile()) });
      const plan = yield* snapshotted.restore(stored(desired), desired);
      assert.ok(plan);
      assert.equal(plan.podTemplate.metadata?.annotations?.["dev.gvisor.internal.restore.paused"], "true");
      assert.deepEqual(plan.podTemplate.spec?.readinessGates, [{ conditionType: RESUMED_CONDITION }]);
      return yield* plan.resume.pipe(Effect.as("resumed"), Effect.catch((refused) => Effect.succeed(refused.message)));
    }));
    return { outcome, asked, writes: kube.writes };
  };
  const ran = await resume(restoredPod("running"));
  assert.equal(ran.outcome, "resumed");
  assert.deepEqual(ran.asked.slice(1), [`resume on node-a 10.42.0.7 1`]);
  assert.match(ran.writes.at(-1) ?? "", /^patch Pod\/fs-abc123's status .*"type":"alasio.dev\/resumed","status":"True"/u);
  assert.equal((await resume(restoredPod("terminated"))).outcome, "its pod ended before it ran (terminated)");
  const unconfined = await resume(restoredPod("running"), "kube-router has not programmed 10.42.0.7 within 30s");
  assert.match(unconfined.outcome, /kube-router has not programmed 10\.42\.0\.7 within 30s$/u);
  assert.ok(!unconfined.writes.some((write) => write.startsWith("patch")));
});

test("a snapshot on the template before the current one is restored into that one, and a session restored so stays on it while it runs", async () => {
  const older = profile();
  const newer = profile("agent@sha256:2");
  const recorded = sessionOf(older).spec.podTemplate;
  const desired = sessionOf(newer);
  const { agents } = fakeAgents({ current: manifestOf(recorded, PREVIOUS), holdings: [node("node-a", [CURRENT, PREVIOUS])] });
  await run(fakeKube(), Effect.gen(function*() {
    const snapshotted = yield* makeSessionSnapshots({ profile: newer, snapshots, agents, templates: templates(newer, older) });
    const plan = yield* snapshotted.restore(stored(desired), desired);
    assert.ok(plan);
    assert.deepEqual(plan.kept, recorded);
    assert.equal(plan.podTemplate.spec?.containers[0]?.image, "agent@sha256:1");
    assert.equal(plan.podTemplate.spec?.runtimeClassName, "gvisor-20260921-0-eaucoin-3");
    assert.equal(snapshotted.keeps(stored(sessionOf(older), "Running")), true);
    assert.equal(snapshotted.keeps(stored(desired, "Running")), false);
  }));
});

test("a fork's snapshot, its source's, restores only onto the current template, its source's own naming the source", async () => {
  const older = profile();
  const newer = profile("agent@sha256:2");
  const desired = sessionOf(newer);
  const { agents, asked } = fakeAgents({ current: manifestOf(sessionOf(older, "fs-source").spec.podTemplate, PREVIOUS), holdings: [node("node-a", [CURRENT, PREVIOUS])] });
  const plan = await run(fakeKube(), Effect.gen(function*() {
    const snapshotted = yield* makeSessionSnapshots({ profile: newer, snapshots, agents, templates: templates(newer, older) });
    return yield* snapshotted.restore(stored(desired), desired);
  }));
  assert.equal(plan, null);
  assert.deepEqual(asked, ["forget alasio-sessions/fs-abc123"]);
});

test("a session without a snapshot is resumed as it is, and one whose snapshot cannot restore is too, its snapshot forgotten, saying why", async () => {
  const desired = sessionOf(profile());
  const restore = async (agents: NodeAgents, of = profile(), kept = templates(profile())) =>
    run(fakeKube(), Effect.gen(function*() {
      const snapshotted = yield* makeSessionSnapshots({ profile: of, snapshots, agents, templates: kept });
      return yield* snapshotted.restore(stored(desired), desired);
    }));
  const none = fakeAgents();
  assert.equal(await restore(none.agents), null);
  assert.deepEqual(none.asked, []);
  const cases = [
    // Taken under a release no longer kept.
    fakeAgents({ current: manifestOf(desired.spec.podTemplate, "20260801.0-eaucoin.1"), holdings: [node("node-a", [CURRENT])] }),
    // On a template neither current nor the one before.
    fakeAgents({ current: manifestOf(sessionOf(profile("agent@sha256:0")).spec.podTemplate), holdings: [node("node-a", [CURRENT])] }),
    // On no node with every CPU feature it was saved with.
    fakeAgents({ current: manifestOf(desired.spec.podTemplate, CURRENT, ["avx512f"]), holdings: [node("node-a", [CURRENT])] }),
    // Prepared on no node.
    fakeAgents({ current: manifestOf(desired.spec.podTemplate), holdings: [node("node-a", [CURRENT])], prepare: () => "the bucket failed" }),
    // Not read.
    fakeAgents({ read: new NodeAgentError({ node: "any node", call: "be reached", reason: "no agent is ready" }) }),
  ];
  const fallbacks = await added("alasio.sandbox.restores", async () => {
    for (const { agents, asked } of cases) {
      assert.equal(await restore(agents), null);
      assert.equal(asked.at(-1), "forget alasio-sessions/fs-abc123");
    }
  });
  assert.deepEqual(
    fallbacks,
    Object.fromEntries(["release", "template", "node", "prepare", "unreadable"].map((why) => [`alasio.sandbox.restore.outcome=fallback,alasio.sandbox.restore.fallback=${why}`, 1])),
  );
});

test("a session is restored only from the snapshot its Sandbox was released into: one it moved on from is stale, and forgotten", async () => {
  const desired = sessionOf(profile());
  const restore = (bound: string | null) => {
    const { agents, asked } = fakeAgents({ current: manifestOf(desired.spec.podTemplate), holdings: [node("node-a", [CURRENT])] });
    return run(fakeKube(), Effect.gen(function*() {
      const snapshotted = yield* makeSessionSnapshots({ profile: profile(), snapshots, agents, templates: templates(profile()) });
      return { plan: yield* snapshotted.restore(stored(desired, "Suspended", bound), desired), asked };
    }));
  };
  assert.ok((await restore("s-1")).plan);
  for (const bound of [null, "s-0"]) {
    const fallbacks = await added("alasio.sandbox.restores", async () => {
      const { plan, asked } = await restore(bound);
      assert.equal(plan, null);
      assert.deepEqual(asked, ["forget alasio-sessions/fs-abc123"]);
    });
    assert.deepEqual(fallbacks, { "alasio.sandbox.restore.outcome=fallback,alasio.sandbox.restore.fallback=stale": 1 });
  }
  // A Sandbox left on a restore's template, by a restore alasio stopped in the middle of, is never kept on it.
  const snapshotted = await run(fakeKube(), makeSessionSnapshots({ profile: profile(), snapshots, agents: fakeAgents().agents, templates: templates(profile(), profile("agent@sha256:0")) }));
  const older = stored(sessionOf(profile("agent@sha256:0")), "Running");
  assert.equal(snapshotted.keeps(older), true);
  older.spec.podTemplate.metadata = { ...older.spec.podTemplate.metadata, annotations: { ...older.spec.podTemplate.metadata?.annotations, "dev.gvisor.internal.restore.host-image-path": "/var/lib/alasio/snapshots/x" } };
  assert.equal(snapshotted.keeps(older), false);
});

test("what no Sandbox has a use for is collected: snapshots of sessions that are gone, and images nodes cache but the one each Sandbox is released into", async () => {
  const kept = stored(sessionOf(profile(), "fs-kept"), "Suspended", "s-2");
  const restored = stored(sessionOf(profile(), "fs-restored"), "Running");
  const kube = fakeKube(new Map<string, KubernetesObject>([["Sandbox/alasio-sessions/fs-kept", kept], ["Sandbox/alasio-sessions/fs-restored", restored]]));
  const cachedOn = (name: string, entries: readonly (readonly [string, string])[]): Holdings => ({
    ...node(name, [CURRENT]),
    cached: entries.map(([workspace, snapshot]) => ({ environment: "alasio-sessions", workspace, snapshot, bytes: 1024, usedAt: "2026-10-12T20:00:00.000Z" })),
  });
  const { agents, asked } = fakeAgents({
    held: ["fs-gone", "fs-kept", "fs-restored"],
    holdings: [cachedOn("node-a", [["fs-kept", "s-2"], ["fs-restored", "s-1"]]), cachedOn("node-b", [["fs-kept", "s-1"], ["fs-gone", "s-9"]])],
  });
  await run(kube, Effect.scoped(Effect.gen(function*() {
    const snapshotted = yield* makeSessionSnapshots({ profile: profile(), snapshots, agents, templates: templates(profile()) });
    yield* snapshotted.watch({ token: () => Effect.succeed(null) } as unknown as Sandboxes, () => null);
    yield* Effect.sleep("50 millis");
  })));
  assert.deepEqual(asked, [
    "forget alasio-sessions/fs-gone",
    "discard alasio-sessions/fs-restored on node-a but null",
    "discard alasio-sessions/fs-kept on node-b but s-2",
    "discard alasio-sessions/fs-gone on node-b but null",
  ]);
  // A session deleted takes its snapshot with it, from the bucket and every node's cache.
  const deleted = fakeAgents({ holdings: [node("node-a", [CURRENT]), node("node-b", [CURRENT])] });
  await run(fakeKube(), Effect.gen(function*() {
    const snapshotted = yield* makeSessionSnapshots({ profile: profile(), snapshots, agents: deleted.agents, templates: templates(profile()) });
    yield* snapshotted.forget("fs-abc123");
  }));
  assert.deepEqual(deleted.asked, ["forget alasio-sessions/fs-abc123", "discard alasio-sessions/fs-abc123 on node-a but null", "discard alasio-sessions/fs-abc123 on node-b but null"]);
});

test("where one release is kept, a snapshot is restored under it alone, and one taken under the release before falls back", async () => {
  const desired = sessionOf(profile());
  const one = { ...snapshots, releases: [{ release: CURRENT, runtimeClassName: "gvisor-20260921-0-eaucoin-4" }] };
  const restore = (agents: NodeAgents) =>
    run(fakeKube(), Effect.gen(function*() {
      const snapshotted = yield* makeSessionSnapshots({ profile: profile(), snapshots: one, agents, templates: templates(profile()) });
      return yield* snapshotted.restore(stored(desired), desired);
    }));
  const plan = await restore(fakeAgents({ current: manifestOf(desired.spec.podTemplate), holdings: [node("node-a", [CURRENT])] }).agents);
  assert.equal(plan?.podTemplate.spec?.runtimeClassName, "gvisor-20260921-0-eaucoin-4");
  const before = fakeAgents({ current: manifestOf(desired.spec.podTemplate, PREVIOUS), holdings: [node("node-a", [CURRENT, PREVIOUS])] });
  const fallbacks = await added("alasio.sandbox.restores", async () => assert.equal(await restore(before.agents), null));
  assert.deepEqual(fallbacks, { "alasio.sandbox.restore.outcome=fallback,alasio.sandbox.restore.fallback=release": 1 });
  assert.equal(before.asked.at(-1), "forget alasio-sessions/fs-abc123");
});

test("an idle session is released, its sandbox snapshotted by its node's agent, addressed to bayma's container, with bayma's hook and its template", async () => {
  const sandbox = stored(sessionOf(profile()), "Running");
  sandbox.status = { serviceFQDN: "fs-abc123.alasio-sessions.svc.cluster.local", conditions: [{ type: "Ready", status: "True", reason: "Ready", message: "", lastTransitionTime: new Date(0), observedGeneration: 0 }] };
  const pod: V1Pod = {
    apiVersion: "v1",
    kind: "Pod",
    metadata: { name: "fs-abc123", namespace: "alasio-sessions" },
    spec: { nodeName: "node-a", runtimeClassName: "gvisor", containers: [] },
    status: { containerStatuses: [{ name: "bayma", containerID: `containerd://${"a".repeat(64)}`, image: "", imageID: "", ready: true, restartCount: 0 }] },
  };
  const kube = fakeKube(new Map<string, KubernetesObject>([["Sandbox/alasio-sessions/fs-abc123", sandbox], ["Pod/alasio-sessions/fs-abc123", pod]]));
  const { agents, checkpoints } = fakeAgents({ holdings: [node("node-a", [CURRENT])] });
  const saved: string[] = [];
  // What happens once the release holds the session, before it saves it: a turn beginning, as one may.
  let meanwhile = () => {};
  const sandboxes = {
    token: () => Effect.succeed("fs-abc123.token"),
    release: (name: string, save: (sandbox: StoredSandbox) => Effect.Effect<Saved, unknown>) =>
      Effect.sync(() => meanwhile()).pipe(
        Effect.andThen(save(sandbox)),
        Effect.map((outcome) => {
          saved.push(`${name}: ${JSON.stringify(outcome)}`);
          return outcome !== "keep";
        }),
      ),
  } as unknown as Sandboxes;
  const look = (lastTurn: number | null, status: unknown) => {
    let turned = lastTurn;
    return run(kube, Effect.scoped(Effect.gen(function*() {
      const snapshotted = yield* makeSessionSnapshots({ profile: profile(), snapshots, agents, templates: templates(profile()) });
      meanwhile = () => {
        if (beginsMeanwhile) turned = Infinity;
      };
      yield* snapshotted.watch(sandboxes, () => turned);
      yield* Effect.sleep("50 millis");
    })), () => status);
  };
  let beginsMeanwhile = false;
  // Busy, or turned on lately: not released.
  await look(null, { execsRunning: 1, execsQueued: 0, idleSinceMs: null });
  await look(Infinity, { execsRunning: 0, execsQueued: 0, idleSinceMs: 0 });
  await look(Date.now(), { execsRunning: 0, execsQueued: 0, idleSinceMs: 0 });
  await look(null, { execsRunning: 0, execsQueued: 0, idleSinceMs: Date.now() });
  assert.deepEqual(saved, []);
  // A turn that begins once the release was decided on, before it holds the session, keeps it running.
  beginsMeanwhile = true;
  await look(Date.now() - 301_000, { execsRunning: 0, execsQueued: 0, idleSinceMs: Date.now() - 301_000 });
  assert.deepEqual(saved, ['fs-abc123: "keep"']);
  assert.deepEqual(checkpoints, []);
  saved.length = 0;
  beginsMeanwhile = false;
  // Idle for five minutes, with no turn since.
  const released = await added("alasio.sandbox.releases", () => look(Date.now() - 301_000, { execsRunning: 0, execsQueued: 0, idleSinceMs: Date.now() - 301_000 }));
  assert.deepEqual(saved, ['fs-abc123: {"snapshot":"s-1"}']);
  assert.deepEqual(released, { "alasio.sandbox.release.outcome=snapshotted": 1 });
  assert.deepEqual(checkpoints, [["node-a", {
    environment: "alasio-sessions",
    workspace: "fs-abc123",
    release: CURRENT,
    containerId: "a".repeat(64),
    hook: ["node", "/opt/bayma/bayma.js", "sandbox-hook", "--state-dir", "/home/agent/.bayma"],
    podTemplate: sandbox.spec.podTemplate,
  }]]);
});
