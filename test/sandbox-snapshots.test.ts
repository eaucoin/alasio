import assert from "node:assert/strict";
import { test } from "node:test";

import type { KubernetesObject, V1LabelSelector, V1Pod, V1PodTemplateSpec } from "@kubernetes/client-node";
import { Deferred, Effect, Fiber, Layer } from "effect";
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
import { hookOf, makeSessionSnapshots, sessionTemplateHash, stopHookOf } from "../src/sandbox/snapshots.ts";

const CURRENT = "20260921.0-eaucoin.4";
const PREVIOUS = "20260921.0-eaucoin.3";

const snapshots = {
  releaseAfterSeconds: 300,
  liveEverySeconds: 600,
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

/** The uid of the pod of session fs-abc123, as the API server gives it. */
const POD_UID = "7c9e6679-7425-40de-944b-e07fc1f90ae7";

/** The digest of the image of the snapshots of the tests'. */
const IMAGE = "a".repeat(64);

/** A manifest of a snapshot of `podTemplate`, taken under `release`, saved with `cpuFeatures`. */
const manifestOf = (podTemplate: V1PodTemplateSpec, release = CURRENT, cpuFeatures: readonly string[] = ["sse", "avx"]): Manifest => ({
  environment: "alasio-sessions",
  workspace: "fs-abc123",
  snapshot: "s-1",
  release,
  cpuFeatures,
  podTemplate: podTemplate as Readonly<Record<string, unknown>>,
  layers: [{ digest: IMAGE, files: [] }],
  bytes: 0,
  takenAt: "2026-10-12T20:00:00.000Z",
  pauseMs: 80,
  uploadMs: 120,
});

/** Node agents in memory: the current snapshot, each node's holdings, and how a prepare goes on each; what they were asked. */
function fakeAgents({ current = null, holdings = [], prepare = () => null, read, resumed = null, resumedStatus = 504, abandoned = null, held = [], kept = () => [], generation = () => 0 }: {
  /** The workspaces the bucket holds anything of. */
  readonly held?: readonly string[];
  /** The images a collection keeps of a workspace. */
  readonly kept?: (workspace: string) => readonly string[];
  readonly current?: Manifest | null;
  readonly holdings?: readonly Holdings[];
  /** Why a prepare on the node fails, and with which status (409 unless said), or null when it does not. */
  readonly prepare?: (node: string) => string | { readonly status: number; readonly reason: string } | null;
  readonly read?: NodeAgentError;
  /** Why the agent does not resume a restored sandbox, or null when it does, and with which status. */
  readonly resumed?: string | null;
  readonly resumedStatus?: number;
  /** bayma's state generation, which a snapshot taken live catches. */
  readonly generation?: () => number;
  /** Why the agent does not abandon a refused restore, or null when it does. */
  readonly abandoned?: string | null;
} = {}) {
  const asked: string[] = [];
  const checkpoints: (readonly [string, CheckpointRequest])[] = [];
  const agents: NodeAgents = {
    nodes: Effect.succeed(holdings.map(({ node }) => node)),
    checkpoint: (node, request) =>
      Effect.sync(() => {
        checkpoints.push([node, request]);
        const manifest = manifestOf(request.podTemplate as V1PodTemplateSpec);
        return request.live ? { ...manifest, live: { stateDir: request.live.stateDir, generation: generation(), running: [] } } : manifest;
      }),
    prepare: (node) =>
      Effect.suspend(() => {
        asked.push(`prepare on ${node}`);
        const refused = prepare(node);
        const { status, reason } = typeof refused === "string" ? { status: 409, reason: refused } : refused ?? { status: 0, reason: "" };
        return refused === null
          ? Effect.succeed({ manifest: current as Manifest, imagePath: `/var/lib/alasio/snapshots/alasio-sessions/fs-abc123/s-1` })
          : Effect.fail(new NodeAgentError({ node, call: "prepare alasio-sessions/fs-abc123's snapshot", status, reason }));
      }),
    current: () => (read ? Effect.fail(read) : Effect.succeed(current)),
    copy: () => Effect.die("nothing is copied"),
    forget: (workspace) => Effect.sync(() => void asked.push(`forget ${workspace.environment}/${workspace.workspace}`)),
    spend: (workspace, snapshot) => Effect.sync(() => void asked.push(`spend ${workspace.environment}/${workspace.workspace} ${snapshot}`)),
    collect: (workspace) =>
      Effect.sync(() => {
        asked.push(`collect ${workspace.environment}/${workspace.workspace}`);
        return kept(workspace.workspace);
      }),
    resume: (node, request) =>
      Effect.suspend(() => {
        asked.push(`resume on ${node} from ${request.snapshot} ${request.podIPs.join(",")} ${request.containerIds.length}${request.policies.length > 0 ? ` through ${request.policies.join(",")}` : ""}`);
        return resumed === null ? Effect.void : Effect.fail(new NodeAgentError({ node, call: "resume alasio-sessions/fs-abc123", status: resumedStatus, reason: resumed }));
      }),
    abandon: (node, request) =>
      Effect.suspend(() => {
        asked.push(`abandon on ${node} ${request.environment}/${request.workspace} in ${request.namespace}/${request.pod} of ${request.uid}`);
        return abandoned === null
          ? Effect.succeed([{ containerId: "b".repeat(64), pid: 4321 }])
          : Effect.fail(new NodeAgentError({ node, call: "abandon alasio-sessions/fs-abc123's restore", status: 500, reason: abandoned }));
      }),
    holdings: (node) => Effect.succeed(holdings.find((holding) => holding.node === node) as Holdings),
    workspaces: () => Effect.succeed(held),
    discard: (node, workspace, keep) =>
      Effect.sync(() => {
        asked.push(`discard ${workspace.environment}/${workspace.workspace} on ${node} but ${keep.join(",")}`);
        return true;
      }),
  };
  return { agents, asked, checkpoints };
}

/** A node holding `releases`, each with the features `cpuFeatures`, and caching the images `cached` of the session's. */
const node = (name: string, releases: readonly string[], cpuFeatures: readonly string[] = ["sse", "avx", "avx2"], cached: readonly string[] = []): Holdings => ({
  node: name,
  releases: releases.map((release) => ({ release, cpuFeatures })),
  cached: cached.length === 0 ? [] : [{ environment: "alasio-sessions", workspace: "fs-abc123", images: cached, bytes: 1024, usedAt: "2026-10-12T20:00:00.000Z" }],
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
function run<A, E>(kube: ReturnType<typeof fakeKube>, effect: Effect.Effect<A, E, KubeClient>, status: () => unknown = () => ({ execsRunning: 0, execsQueued: 0, idleSinceMs: 0, generation: 0 })): Promise<A> {
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

test("the CPU baseline alasio up holds sessions to is part of their template, as its hash records it", () => {
  const given = profile();
  const held: SessionsProfile = {
    ...given,
    podTemplate: { ...given.podTemplate, metadata: { ...given.podTemplate.metadata, annotations: { ...given.podTemplate.metadata?.annotations, [CPU_FEATURES_ANNOTATION]: "avx2,sse" } } },
  };
  assert.notEqual(sessionTemplateHash(held), sessionTemplateHash(given));
  assert.equal(sessionOf(held).spec.podTemplate.metadata?.annotations?.[CPU_FEATURES_ANNOTATION], "avx2,sse");
});

test("a session with a snapshot on the current template is restored on a node that can, the one holding it first, into what it is now", async () => {
  const desired = sessionOf(profile());
  const manifest = manifestOf(desired.spec.podTemplate);
  const { agents, asked } = fakeAgents({
    current: manifest,
    holdings: [node("node-a", [CURRENT], ["sse"]), node("node-b", [PREVIOUS]), node("node-c", [CURRENT]), node("node-d", [CURRENT], undefined, [IMAGE])],
    prepare: (on) => (on === "node-d" ? "its image does not match its manifest" : null),
  });
  const plan = await run(fakeKube(), Effect.gen(function*() {
    const snapshotted = yield* makeSessionSnapshots({ profile: profile(), snapshots, agents, templates: templates(profile()) });
    return yield* snapshotted.restore(stored(desired), desired);
  }));
  assert.ok(plan && "podTemplate" in plan);
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
  // Its images go on as the layers of the next.
  assert.deepEqual(asked.slice(2), ["spend alasio-sessions/fs-abc123 s-1"]);
});

test("a restored pod is let run once its containers run, by its node's agent, then made ready; one that ends, or that the agent does not resume, is refused", async () => {
  const desired = sessionOf(profile());
  const restoredPod = (state: "running" | "terminated" | "waiting"): V1Pod => ({
    apiVersion: "v1",
    kind: "Pod",
    metadata: { name: "fs-abc123", namespace: "alasio-sessions", labels: { "alasio.dev/workload": "session", "alasio.dev/net-mode": "none" } },
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
  const resume = async (pod: V1Pod, resumed: string | null = null, prepared: Manifest | null = null, resumedStatus = 504) => {
    // The policies of its namespace, of which those that select it: all of its pods', its workload's, and none of full mode's or of another namespace.
    const policy = (name: string, podSelector: V1LabelSelector, namespace = "alasio-sessions"): [string, KubernetesObject] => [
      `NetworkPolicy/${namespace}/${name}`,
      { apiVersion: "networking.k8s.io/v1", kind: "NetworkPolicy", metadata: { name, namespace }, spec: { podSelector } } as KubernetesObject,
    ];
    const kube = fakeKube(new Map<string, KubernetesObject>([
      ["Pod/alasio-sessions/fs-abc123", pod],
      policy("default-deny", {}),
      policy("sessions", { matchLabels: { "alasio.dev/workload": "session" } }),
      policy("full-egress", { matchLabels: { "alasio.dev/workload": "session", "alasio.dev/net-mode": "full" } }),
      policy("not-folders", { matchExpressions: [{ key: "alasio.dev/workload", operator: "NotIn", values: ["folder"] }] }),
      policy("elsewhere", {}, "alasio"),
    ]));
    const base = fakeAgents({ current: manifestOf(desired.spec.podTemplate), holdings: [node("node-a", [CURRENT])], resumed, resumedStatus });
    let ran: boolean | undefined;
    const { asked } = base;
    const agents: NodeAgents = prepared === null ? base.agents : {
      ...base.agents,
      prepare: (on) => Effect.sync(() => (asked.push(`prepare on ${on}`), { manifest: prepared, imagePath: "/var/lib/alasio/snapshots/alasio-sessions/fs-abc123/s-2" })),
    };
    const outcome = await run(kube, Effect.gen(function*() {
      const snapshotted = yield* makeSessionSnapshots({ profile: profile(), snapshots, agents, templates: templates(profile()) });
      const plan = yield* snapshotted.restore(stored(desired), desired);
      assert.ok(plan && "podTemplate" in plan);
      assert.equal(plan.podTemplate.metadata?.annotations?.["dev.gvisor.internal.restore.paused"], "true");
      assert.deepEqual(plan.podTemplate.spec?.readinessGates, [{ conditionType: RESUMED_CONDITION }]);
      return yield* plan.resume.pipe(Effect.as("resumed"), Effect.catch((refused) => Effect.sync(() => ((ran = refused.resumed), refused.message))));
    }));
    return { outcome, asked, writes: kube.writes, ran };
  };
  const ran = await resume(restoredPod("running"));
  assert.equal(ran.outcome, "resumed");
  assert.deepEqual(ran.asked.slice(1), [`resume on node-a from s-1 10.42.0.7 1 through default-deny,sessions,not-folders`]);
  assert.match(ran.writes.at(-1) ?? "", /^patch Pod\/fs-abc123's status .*"type":"alasio.dev\/resumed","status":"True"/u);
  // A compaction of it, committed between alasio's read and the agent's prepare: the sandbox runs from what was prepared, which the resume names.
  const compacted: Manifest = { ...manifestOf(desired.spec.podTemplate), snapshot: "s-2", compactedFrom: "s-1" };
  const raced = await resume(restoredPod("running"), null, compacted);
  assert.equal(raced.outcome, "resumed");
  assert.deepEqual(raced.asked.slice(1), [`resume on node-a from s-2 10.42.0.7 1 through default-deny,sessions,not-folders`]);
  assert.equal((await resume(restoredPod("terminated"))).outcome, "its pod ended before it ran (terminated)");
  const unconfined = await resume(restoredPod("running"), "kube-router has not programmed 10.42.0.7 within 30s");
  assert.match(unconfined.outcome, /kube-router has not programmed 10\.42\.0\.7 within 30s$/u);
  assert.ok(!unconfined.writes.some((write) => write.startsWith("patch")));
  assert.equal(unconfined.ran, false);
  // Left paused where the bucket fails to give the agent the snapshot's manifest, which may pass, and where it is refused; run, maybe, where a command failed.
  for (const [status, ran] of [[502, false], [410, false], [500, true]] as const) {
    assert.equal((await resume(restoredPod("running"), "it failed", null, status)).ran, ran, String(status));
  }
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
    assert.ok(plan && "podTemplate" in plan);
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
    // Prepared on no node: none can restore it, or its image is not what it says.
    fakeAgents({ current: manifestOf(desired.spec.podTemplate), holdings: [node("node-a", [CURRENT]), node("node-b", [CURRENT])], prepare: (on) => (on === "node-a" ? "its CPU lacks avx" : { status: 422, reason: "its image is corrupt" }) }),
    // Not read: what the bucket holds is no snapshot.
    fakeAgents({ read: new NodeAgentError({ node: "node-a", call: "read alasio-sessions/fs-abc123's snapshot", status: 422, reason: "its current.json is not a manifest" }) }),
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

test("a restore that fails for what may pass is deferred, its snapshot kept, as is one whose sandbox never ran, until restores have been for 15 minutes", async () => {
  const desired = sessionOf(profile());
  const manifest = manifestOf(desired.spec.podTemplate);
  let reading: NodeAgentError | undefined = new NodeAgentError({ node: "node-a", call: "read alasio-sessions/fs-abc123's snapshot", status: 502, reason: "the bucket answered 503" });
  let preparing: { readonly status: number; readonly reason: string } | null = null;
  const base = fakeAgents({ current: manifest, holdings: [node("node-a", [CURRENT])] });
  const agents: NodeAgents = {
    ...base.agents,
    current: (workspace) => (reading ? Effect.fail(reading) : base.agents.current(workspace)),
    prepare: (on, workspace) => (preparing ? Effect.fail(new NodeAgentError({ node: on, call: "prepare", ...preparing })) : base.agents.prepare(on, workspace)),
  };
  const counted = await added("alasio.sandbox.restores", () =>
    run(fakeKube(), Effect.gen(function*() {
      const snapshotted = yield* makeSessionSnapshots({ profile: profile(), snapshots, agents, templates: templates(profile()) });
      const restore = snapshotted.restore(stored(desired), desired);
      // Its bucket fails, then its node's agent as it prepares it: deferred, its snapshot kept.
      assert.deepEqual(yield* restore, { deferred: "the node agent of node-a could not read alasio-sessions/fs-abc123's snapshot: the bucket answered 503" });
      reading = undefined;
      preparing = { status: 502, reason: "the bucket answered 503" };
      assert.deepEqual(yield* restore, { deferred: "the node agent of node-a could not prepare: the bucket answered 503" });
      preparing = null;
      // Prepared, its pod does not come back, its sandbox never having run: deferred; having run, it falls back.
      const plan = yield* restore;
      assert.ok(plan && "podTemplate" in plan);
      assert.deepEqual(plan.snapshot, { snapshot: "s-1", release: CURRENT });
      assert.equal(yield* plan.failed({ why: "its pod did not run within 3 minutes", resumed: false }), "deferred");
      assert.deepEqual(base.asked.filter((each) => each.startsWith("forget")), []);
      assert.equal(yield* plan.failed({ why: "Sandbox did not become ready", resumed: true }), "fallen back");
      assert.deepEqual(base.asked.filter((each) => each.startsWith("forget")), ["forget alasio-sessions/fs-abc123"]);
      // Deferred for 15 minutes from the first, it falls back.
      reading = new NodeAgentError({ node: "node-a", call: "read alasio-sessions/fs-abc123's snapshot", reason: "connect ECONNREFUSED" });
      assert.ok(yield* restore.pipe(Effect.map((came) => came !== null && "deferred" in came)));
      yield* TestClock.adjust("15 minutes");
      assert.equal(yield* restore, null);
    }).pipe(Effect.provide(TestClock.layer())))
  );
  assert.deepEqual(counted, {
    "alasio.sandbox.restore.outcome=deferred,alasio.sandbox.restore.fallback=unreadable": 2,
    "alasio.sandbox.restore.outcome=deferred,alasio.sandbox.restore.fallback=prepare": 1,
    "alasio.sandbox.restore.outcome=deferred,alasio.sandbox.restore.fallback=restore": 1,
    "alasio.sandbox.restore.outcome=fallback,alasio.sandbox.restore.fallback=restore": 1,
    "alasio.sandbox.restore.outcome=fallback,alasio.sandbox.restore.fallback=unreadable": 1,
  });
});

test("a restore refused before its sandbox resumed is abandoned on its node, its pod named by its uid, before it is deferred, whether or not the agent can; one refused after is not, nor one whose pod is gone", async () => {
  const desired = sessionOf(profile());
  const refuse = (resumed: boolean, abandoned: string | null = null, objects = runningOnNodeA()) => {
    const { agents, asked } = fakeAgents({ current: manifestOf(desired.spec.podTemplate), holdings: [node("node-a", [CURRENT])], abandoned });
    return run(fakeKube(objects), Effect.gen(function*() {
      const snapshotted = yield* makeSessionSnapshots({ profile: profile(), snapshots, agents, templates: templates(profile()) });
      const plan = yield* snapshotted.restore(stored(desired), desired);
      assert.ok(plan && "podTemplate" in plan);
      const came = yield* plan.failed({ why: "its pod did not run within 3 minutes", resumed });
      return { came, asked: asked.slice(1) };
    }));
  };
  const abandoned = `abandon on node-a alasio-sessions/fs-abc123 in alasio-sessions/fs-abc123 of ${POD_UID}`;
  assert.deepEqual(await refuse(false), { came: "deferred", asked: [abandoned] });
  assert.deepEqual(await refuse(false, "runsc list of the node's failed"), { came: "deferred", asked: [abandoned] });
  assert.deepEqual(await refuse(true), { came: "fallen back", asked: ["forget alasio-sessions/fs-abc123"] });
  // Its pod gone, torn down, there is nothing of it to abandon.
  assert.deepEqual(await refuse(false, null, new Map()), { came: "deferred", asked: [] });
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
  // One its node's agent compacted from that one since is the same snapshot, restored, and spent, as itself.
  const compacted = fakeAgents({ current: { ...manifestOf(desired.spec.podTemplate), snapshot: "s-2", compactedFrom: "s-1" }, holdings: [node("node-a", [CURRENT])] });
  const fromCompacted = await run(fakeKube(), Effect.gen(function*() {
    const snapshotted = yield* makeSessionSnapshots({ profile: profile(), snapshots, agents: compacted.agents, templates: templates(profile()) });
    const plan = yield* snapshotted.restore(stored(desired, "Suspended", "s-1"), desired);
    assert.ok(plan && "podTemplate" in plan);
    yield* plan.restored;
    return plan.snapshot;
  }));
  assert.deepEqual(fromCompacted, { snapshot: "s-2", release: CURRENT });
  assert.deepEqual(compacted.asked.slice(1), ["spend alasio-sessions/fs-abc123 s-2"]);
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

test("what no Sandbox has a use for is collected: snapshots of sessions that are gone, what of each session's no snapshot of it needs, and the images nodes cache that none needs", async () => {
  const kept = stored(sessionOf(profile(), "fs-kept"), "Suspended", "s-2");
  const restored = stored(sessionOf(profile(), "fs-restored"), "Running");
  const kube = fakeKube(new Map<string, KubernetesObject>([["Sandbox/alasio-sessions/fs-kept", kept], ["Sandbox/alasio-sessions/fs-restored", restored]]));
  const [K1, K2, R0, R1, G] = ["1".repeat(64), "2".repeat(64), "3".repeat(64), "4".repeat(64), "5".repeat(64)] as const;
  const cachedOn = (name: string, entries: readonly (readonly [string, readonly string[]])[]): Holdings => ({
    ...node(name, [CURRENT]),
    cached: entries.map(([workspace, images]) => ({ environment: "alasio-sessions", workspace, images, bytes: 1024, usedAt: "2026-10-12T20:00:00.000Z" })),
  });
  const { agents, asked } = fakeAgents({
    held: ["fs-gone", "fs-kept", "fs-restored"],
    kept: (workspace) => (workspace === "fs-kept" ? [K2] : [R1]),
    holdings: [cachedOn("node-a", [["fs-kept", [K2]], ["fs-restored", [R0, R1]]]), cachedOn("node-b", [["fs-kept", [K1]], ["fs-gone", [G]]])],
  });
  await run(kube, Effect.scoped(Effect.gen(function*() {
    const snapshotted = yield* makeSessionSnapshots({ profile: profile(), snapshots, agents, templates: templates(profile()) });
    yield* snapshotted.watch({ token: () => Effect.succeed(null) } as unknown as Sandboxes, () => null);
    yield* Effect.sleep("50 millis");
  })));
  assert.deepEqual(asked, [
    "forget alasio-sessions/fs-gone",
    "collect alasio-sessions/fs-kept",
    "collect alasio-sessions/fs-restored",
    `discard alasio-sessions/fs-restored on node-a but ${R1}`,
    `discard alasio-sessions/fs-kept on node-b but ${K2}`,
    "discard alasio-sessions/fs-gone on node-b but ",
  ]);
  // A session deleted takes its snapshots with it, from the bucket and every node's cache.
  const deleted = fakeAgents({ holdings: [node("node-a", [CURRENT]), node("node-b", [CURRENT])] });
  await run(fakeKube(), Effect.gen(function*() {
    const snapshotted = yield* makeSessionSnapshots({ profile: profile(), snapshots, agents: deleted.agents, templates: templates(profile()) });
    yield* snapshotted.forget("fs-abc123");
  }));
  assert.deepEqual(deleted.asked, ["forget alasio-sessions/fs-abc123", "discard alasio-sessions/fs-abc123 on node-a but ", "discard alasio-sessions/fs-abc123 on node-b but "]);
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
  assert.ok(plan && "podTemplate" in plan);
  assert.equal(plan.podTemplate.spec?.runtimeClassName, "gvisor-20260921-0-eaucoin-4");
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
  await look(null, { execsRunning: 1, execsQueued: 0, idleSinceMs: null, generation: 0 });
  await look(Infinity, { execsRunning: 0, execsQueued: 0, idleSinceMs: 0, generation: 0 });
  await look(Date.now(), { execsRunning: 0, execsQueued: 0, idleSinceMs: 0, generation: 0 });
  await look(null, { execsRunning: 0, execsQueued: 0, idleSinceMs: Date.now(), generation: 0 });
  assert.deepEqual(saved, []);
  // A turn that begins once the release was decided on, before it holds the session, keeps it running.
  beginsMeanwhile = true;
  await look(Date.now() - 301_000, { execsRunning: 0, execsQueued: 0, idleSinceMs: Date.now() - 301_000, generation: 0 });
  assert.deepEqual(saved, ['fs-abc123: "keep"']);
  assert.deepEqual(checkpoints, []);
  saved.length = 0;
  beginsMeanwhile = false;
  // Idle for five minutes, with no turn since.
  const released = await added("alasio.sandbox.releases", () => look(Date.now() - 301_000, { execsRunning: 0, execsQueued: 0, idleSinceMs: Date.now() - 301_000, generation: 0 }));
  assert.deepEqual(saved, [`fs-abc123: {"saved":{"snapshot":"s-1","release":"${CURRENT}"}}`]);
  assert.deepEqual(released, { "alasio.sandbox.release.outcome=snapshotted": 1 });
  assert.deepEqual(checkpoints, [["node-a", {
    environment: "alasio-sessions",
    workspace: "fs-abc123",
    namespace: "alasio-sessions",
    pod: "fs-abc123",
    containerId: "a".repeat(64),
    hook: ["node", "/opt/bayma/bayma.js", "sandbox-hook", "--state-dir", "/home/agent/.bayma"],
    podTemplate: sandbox.spec.podTemplate,
  }]]);
});

test("a node's idle sessions are released two at a time, the others left for a later look", async () => {
  const objects = new Map<string, KubernetesObject>();
  for (const [name, on] of [["fs-a1", "node-a"], ["fs-a2", "node-a"], ["fs-a3", "node-a"], ["fs-b1", "node-b"]] as const) {
    const sandbox = stored(sessionOf(profile(), name), "Running");
    sandbox.status = { serviceFQDN: `${name}.alasio-sessions.svc.cluster.local`, conditions: [{ type: "Ready", status: "True", reason: "Ready", message: "", lastTransitionTime: new Date(0), observedGeneration: 0 }] };
    objects.set(`Sandbox/alasio-sessions/${name}`, sandbox);
    objects.set(`Pod/alasio-sessions/${name}`, { apiVersion: "v1", kind: "Pod", metadata: { name, namespace: "alasio-sessions" }, spec: { nodeName: on, containers: [] } } as V1Pod);
  }
  const { agents } = fakeAgents({ holdings: [node("node-a", [CURRENT]), node("node-b", [CURRENT])] });
  const releasing: string[] = [];
  // Each release holds on, as a checkpoint of a large sandbox does.
  const sandboxes = {
    token: () => Effect.succeed("token"),
    release: (name: string) => Effect.sync(() => releasing.push(name)).pipe(Effect.andThen(Effect.never)),
  } as unknown as Sandboxes;
  await run(fakeKube(objects), Effect.scoped(Effect.gen(function*() {
    const snapshotted = yield* makeSessionSnapshots({ profile: profile(), snapshots, agents, templates: templates(profile()) });
    yield* snapshotted.watch(sandboxes, () => null);
    yield* Effect.sleep("50 millis");
  })), () => ({ execsRunning: 0, execsQueued: 0, idleSinceMs: 0, generation: 0 }));
  assert.deepEqual(releasing.toSorted(), ["fs-a1", "fs-a2", "fs-b1"]);
});

test("a release whose checkpoint fails is counted failed while its sandbox runs on, and lost once it ended it; one that fails beside its save, an error", async () => {
  const objects = new Map<string, KubernetesObject>();
  for (const [name, ended] of [["fs-runs", false], ["fs-ended", true], ["fs-error", false]] as const) {
    const sandbox = stored(sessionOf(profile(), name), "Running");
    sandbox.status = { serviceFQDN: `${name}.alasio-sessions.svc.cluster.local`, conditions: [{ type: "Ready", status: "True", reason: "Ready", message: "", lastTransitionTime: new Date(0), observedGeneration: 0 }] };
    objects.set(`Sandbox/alasio-sessions/${name}`, sandbox);
    objects.set(`Pod/alasio-sessions/${name}`, {
      apiVersion: "v1",
      kind: "Pod",
      metadata: { name, namespace: "alasio-sessions" },
      spec: { nodeName: name === "fs-runs" ? "node-a" : "node-b", runtimeClassName: "gvisor", containers: [] },
      status: { containerStatuses: [{ name: "bayma", containerID: `containerd://${"a".repeat(64)}`, image: "", imageID: "", ready: !ended, restartCount: 0, state: ended ? { terminated: { exitCode: 0 } } : { running: {} } }] },
    } as V1Pod);
  }
  const base = fakeAgents({ holdings: [node("node-a", [CURRENT]), node("node-b", [CURRENT])] });
  const agents: NodeAgents = { ...base.agents, checkpoint: (on) => Effect.fail(new NodeAgentError({ node: on, call: "snapshot", status: 500, reason: "runsc checkpoint failed" })) };
  const saved: string[] = [];
  const sandboxes = {
    token: () => Effect.succeed("token"),
    release: (name: string, save: (sandbox: StoredSandbox) => Effect.Effect<Saved, unknown>) =>
      name === "fs-error"
        ? Effect.fail(new KubeApiError({ status: 409, cause: new Error("the Sandbox changed") }))
        : save(objects.get(`Sandbox/alasio-sessions/${name}`) as StoredSandbox).pipe(Effect.map((outcome) => (saved.push(`${name}: ${JSON.stringify(outcome)}`), outcome !== "keep"))),
  } as unknown as Sandboxes;
  const counted = await added("alasio.sandbox.releases", () =>
    run(fakeKube(objects), Effect.scoped(Effect.gen(function*() {
      const snapshotted = yield* makeSessionSnapshots({ profile: profile(), snapshots, agents, templates: templates(profile()) });
      yield* snapshotted.watch(sandboxes, () => null);
      yield* Effect.sleep("50 millis");
    })), () => ({ execsRunning: 0, execsQueued: 0, idleSinceMs: 0, generation: 0 })));
  assert.deepEqual(saved.toSorted(), ['fs-ended: {"saved":null}', 'fs-runs: "keep"']);
  assert.deepEqual(counted, {
    "alasio.sandbox.release.outcome=failed": 1,
    "alasio.sandbox.release.outcome=lost": 1,
    "alasio.sandbox.release.outcome=error": 1,
  });
});

/** Session fs-abc123 running on node-a, its Sandbox and its pod, as the API server holds them. */
const runningOnNodeA = (): Map<string, KubernetesObject> => {
  const sandbox = stored(sessionOf(profile()), "Running");
  sandbox.status = { serviceFQDN: "fs-abc123.alasio-sessions.svc.cluster.local", conditions: [{ type: "Ready", status: "True", reason: "Ready", message: "", lastTransitionTime: new Date(0), observedGeneration: 0 }] };
  const pod: V1Pod = {
    apiVersion: "v1",
    kind: "Pod",
    metadata: { name: "fs-abc123", namespace: "alasio-sessions", uid: POD_UID },
    spec: { nodeName: "node-a", runtimeClassName: "gvisor", containers: [] },
    status: { containerStatuses: [{ name: "bayma", containerID: `containerd://${"a".repeat(64)}`, image: "", imageID: "", ready: true, restartCount: 0 }] },
  };
  return new Map<string, KubernetesObject>([["Sandbox/alasio-sessions/fs-abc123", sandbox], ["Pod/alasio-sessions/fs-abc123", pod]]);
};

test("a running session is snapshotted live every liveEvery while its execs run, and the snapshot spent once bayma's generation moves on from it", async () => {
  const status = { execsRunning: 1, execsQueued: 0, idleSinceMs: null as number | null, generation: 5 };
  const { agents, asked, checkpoints } = fakeAgents({ holdings: [node("node-a", [CURRENT])], generation: () => status.generation });
  const sandboxes = { token: () => Effect.succeed("token"), release: () => Effect.die("no session is idle") } as unknown as Sandboxes;
  // The looks due by now, each one's fetch of bayma's status answered.
  const settle = TestClock.withLive(Effect.sleep("20 millis"));
  const pass = (minutes: number) =>
    Effect.gen(function*() {
      for (let look = 0; look < minutes * 4; look++) {
        yield* TestClock.adjust("15 seconds");
        yield* settle;
      }
    });
  const counted = await added("alasio.sandbox.live_checkpoints", () =>
    run(fakeKube(runningOnNodeA()), Effect.scoped(Effect.gen(function*() {
      const snapshotted = yield* makeSessionSnapshots({ profile: profile(), snapshots, agents, templates: templates(profile()) });
      yield* snapshotted.watch(sandboxes, () => null);
      yield* settle;
      // Its execs have run for less than liveEvery.
      yield* pass(9);
      assert.equal(checkpoints.length, 0);
      // Ten minutes in: saved live, bayma's state directory named, the sandbox running on.
      yield* pass(1);
      assert.equal(checkpoints.length, 1);
      assert.deepEqual(checkpoints[0]?.[1].live, { stateDir: "/home/agent/.bayma" });
      // And every ten minutes after, while they run.
      yield* pass(9);
      assert.equal(checkpoints.length, 1);
      yield* pass(1);
      assert.equal(checkpoints.length, 2);
      assert.deepEqual(asked, []);
      // bayma moves on: the snapshot holds the session no more, and is spent.
      status.generation = 6;
      yield* pass(0.25);
      assert.deepEqual(asked, ["spend alasio-sessions/fs-abc123 s-1"]);
      // Its execs end: none is saved live again.
      status.execsRunning = 0;
      yield* pass(20);
      assert.equal(checkpoints.length, 2);
      assert.deepEqual(asked, ["spend alasio-sessions/fs-abc123 s-1"]);
    }).pipe(Effect.provide(TestClock.layer()))), () => status)
  );
  assert.deepEqual(counted, { "alasio.sandbox.live_checkpoint.outcome=taken": 2, "alasio.sandbox.live_checkpoint.outcome=spent": 1 });
});

test("a session's snapshot taken live is the one its lost pod restarts into, and none other is", async () => {
  const desired = sessionOf(profile());
  const liveOf = (agents: NodeAgents) =>
    run(fakeKube(), Effect.gen(function*() {
      const snapshotted = yield* makeSessionSnapshots({ profile: profile(), snapshots, agents, templates: templates(profile()) });
      return yield* snapshotted.live("fs-abc123");
    }));
  const taken: Manifest = { ...manifestOf(desired.spec.podTemplate), live: { stateDir: "/home/agent/.bayma", generation: 5, running: [] } };
  assert.deepEqual(await liveOf(fakeAgents({ current: taken }).agents), { snapshot: "s-1", release: CURRENT });
  // A release's, taken as the session stopped, is restored as it is resumed, not as it is lost.
  assert.equal(await liveOf(fakeAgents({ current: manifestOf(desired.spec.podTemplate) }).agents), null);
  assert.equal(await liveOf(fakeAgents().agents), null);
  // One that cannot be read restarts it into none, as its restore would not find it either.
  const unread = new NodeAgentError({ node: "node-a", call: "read alasio-sessions/fs-abc123's snapshot", status: 502, reason: "the bucket answered 503" });
  assert.equal(await liveOf(fakeAgents({ read: unread }).agents), null);
});

test("a restore bayma refuses, its state generation moved on from the snapshot taken live, is stale: abandoned on its node, and fallen back from at once", async () => {
  const desired = sessionOf(profile());
  const objects = runningOnNodeA();
  const pod = objects.get("Pod/alasio-sessions/fs-abc123") as V1Pod;
  pod.status = { ...pod.status, podIPs: [{ ip: "10.42.0.7" }], containerStatuses: [{ ...pod.status!.containerStatuses![0]!, ready: false, state: { running: {} } }] };
  const { agents, asked } = fakeAgents({
    current: manifestOf(desired.spec.podTemplate),
    holdings: [node("node-a", [CURRENT])],
    resumed: "bayma's state generation is 6, where the snapshot caught 5",
    resumedStatus: 410,
  });
  let came: string | undefined;
  const counted = await added("alasio.sandbox.restores", () =>
    run(fakeKube(objects), Effect.gen(function*() {
      const snapshotted = yield* makeSessionSnapshots({ profile: profile(), snapshots, agents, templates: templates(profile()) });
      const plan = yield* snapshotted.restore(stored(desired), desired);
      assert.ok(plan && "podTemplate" in plan);
      const refused = yield* plan.resume.pipe(Effect.flip);
      assert.deepEqual({ resumed: refused.resumed, stale: refused.stale }, { resumed: false, stale: true });
      // As sandboxes.ts hands the refusal on.
      came = yield* plan.failed({ why: refused.message, resumed: refused.resumed, stale: true });
    }))
  );
  assert.equal(came, "fallen back");
  assert.deepEqual(asked.slice(1), [
    "resume on node-a from s-1 10.42.0.7 1",
    `abandon on node-a alasio-sessions/fs-abc123 in alasio-sessions/fs-abc123 of ${POD_UID}`,
    "forget alasio-sessions/fs-abc123",
  ]);
  assert.deepEqual(counted, { "alasio.sandbox.restore.outcome=fallback,alasio.sandbox.restore.fallback=stale": 1 });
});

/**
 * The pod of session `name` on `on`, ending, deleted at `deletion`, as an eviction or a
 * deletion through the API server leaves one; its bayma holding its stop, unless `held` is false.
 */
const endingPod = (name: string, deletion: Date, held = true, on = "node-a"): V1Pod => ({
  apiVersion: "v1",
  kind: "Pod",
  metadata: { name, namespace: "alasio-sessions", deletionTimestamp: deletion },
  spec: {
    nodeName: on,
    runtimeClassName: "gvisor",
    containers: [{ name: "bayma", args: ["mcp-http", "--state-dir", "/home/agent/.bayma", ...(held ? ["--stop-wait-ms", "60000"] : [])] }],
  },
  status: { containerStatuses: [{ name: "bayma", containerID: `containerd://${"a".repeat(64)}`, image: "", imageID: "", ready: true, restartCount: 0 }] },
});

test("the watch releases a running session whose pod is ending though alasio did not end it, its bayma holding its stop; not one alasio ends, one being deleted, nor one whose bayma holds none", async () => {
  const look = async (change: (sandbox: StoredSandbox) => void, held = true) => {
    const objects = runningOnNodeA();
    objects.set("Pod/alasio-sessions/fs-abc123", endingPod("fs-abc123", new Date(Date.now() + 90_000), held));
    change(objects.get("Sandbox/alasio-sessions/fs-abc123") as StoredSandbox);
    const { agents } = fakeAgents({ holdings: [node("node-a", [CURRENT])] });
    const asked: string[] = [];
    const sandboxes = {
      token: () => Effect.succeed("token"),
      release: () => Effect.die("no session is idle"),
      releaseStopping: (name: string) => Effect.sync(() => (asked.push(name), true)),
    } as unknown as Sandboxes;
    await run(fakeKube(objects), Effect.scoped(Effect.gen(function*() {
      const snapshotted = yield* makeSessionSnapshots({ profile: profile(), snapshots, agents, templates: templates(profile()) });
      yield* snapshotted.watch(sandboxes, () => null);
      yield* Effect.sleep("50 millis");
    })), () => ({ execsRunning: 1, execsQueued: 0, idleSinceMs: null, generation: 0 }));
    return asked;
  };
  assert.deepEqual(await look(() => undefined), ["fs-abc123"]);
  // Suspended, as alasio suspends a Sandbox before it ends its pod: alasio's own stop, snapshotted already or not to be.
  assert.deepEqual(await look((sandbox) => void (sandbox.spec = { ...sandbox.spec, operatingMode: "Suspended" })), []);
  // Being deleted, its pod with it.
  assert.deepEqual(await look((sandbox) => void (sandbox.metadata = { ...sandbox.metadata, deletionTimestamp: new Date(0) })), []);
  // Made before its bayma was told to hold its stop: it stops as it is told to.
  assert.deepEqual(await look(() => undefined, false), []);
});

test("a session whose pod is ending is snapshotted as a release's while its bayma holds its stop, for as long as the agent's call takes; and not where its bayma holds none, where its turn comes once it holds it no more, or where its snapshot fails, which no release counts", async () => {
  const sandbox = stored(sessionOf(profile()), "Suspended", null);
  // Deleted at 90 s, its bayma holds its stop until 60 s, then stops in 30.
  const saved = async (pod: V1Pod, { checkpoint, at = 0 }: { checkpoint?: NodeAgents["checkpoint"]; at?: number } = {}) => {
    const objects = runningOnNodeA();
    objects.set("Pod/alasio-sessions/fs-abc123", pod);
    const base = fakeAgents({ holdings: [node("node-a", [CURRENT])] });
    const agents: NodeAgents = checkpoint ? { ...base.agents, checkpoint } : base.agents;
    let snapshot: unknown;
    let counted: Readonly<Record<string, number>> = {};
    const releases = await added("alasio.sandbox.releases", async () => {
      counted = await added("alasio.sandbox.stop_releases", () =>
        run(fakeKube(objects), Effect.gen(function*() {
          const snapshotted = yield* makeSessionSnapshots({ profile: profile(), snapshots, agents, templates: templates(profile()) });
          yield* TestClock.setTime(at);
          // Slow as it may be: the clock runs on two minutes, past its bayma's stop, as it saves.
          const saving = yield* snapshotted.saveStopping(sandbox, pod).pipe(Effect.forkChild);
          yield* TestClock.withLive(Effect.sleep("20 millis"));
          yield* TestClock.adjust("2 minutes");
          snapshot = yield* Fiber.join(saving);
        }).pipe(Effect.provide(TestClock.layer()))));
    });
    assert.deepEqual(releases, {});
    return { snapshot, counted, checkpoints: base.checkpoints };
  };
  const deletedAt = new Date(90_000);
  const slow: NodeAgents["checkpoint"] = (_node, request) => Effect.sleep("100 seconds").pipe(Effect.as(manifestOf(request.podTemplate as V1PodTemplateSpec)));
  const snapshotted = await saved(endingPod("fs-abc123", deletedAt), { checkpoint: slow });
  assert.deepEqual(snapshotted.snapshot, { snapshot: "s-1", release: CURRENT });
  assert.deepEqual(snapshotted.counted, { "alasio.sandbox.stop_release.outcome=snapshotted": 1 });
  // Its bayma holds no stop.
  const unheld = await saved(endingPod("fs-abc123", deletedAt, false));
  assert.equal(unheld.snapshot, null);
  assert.deepEqual(unheld.checkpoints, []);
  assert.deepEqual(unheld.counted, { "alasio.sandbox.stop_release.outcome=unheld": 1 });
  // Its turn comes at 60 s, as its bayma holds its stop no more.
  const late = await saved(endingPod("fs-abc123", deletedAt), { at: 60_000 });
  assert.equal(late.snapshot, null);
  assert.deepEqual(late.checkpoints, []);
  assert.deepEqual(late.counted, { "alasio.sandbox.stop_release.outcome=late": 1 });
  // Its snapshot fails, its sandbox running on until its bayma stops: none, and no release failed, running on.
  const failing: NodeAgents["checkpoint"] = (on) => Effect.fail(new NodeAgentError({ node: on, call: "snapshot", status: 500, reason: "runsc checkpoint failed" }));
  const failed = await saved(endingPod("fs-abc123", deletedAt), { checkpoint: failing });
  assert.equal(failed.snapshot, null);
  assert.deepEqual(failed.counted, { "alasio.sandbox.stop_release.outcome=unsnapshotted": 1 });
});

test("a node snapshots two of its sessions whose pods are ending at a time, the others waiting their turn, and another node its own beside them", async () => {
  const names = [["fs-a1", "node-a"], ["fs-a2", "node-a"], ["fs-a3", "node-a"], ["fs-b1", "node-b"]] as const;
  const objects = new Map<string, KubernetesObject>(names.map(([name, on]) => [`Pod/alasio-sessions/${name}`, endingPod(name, new Date(Date.now() + 90_000), true, on)]));
  const base = fakeAgents({ holdings: [node("node-a", [CURRENT]), node("node-b", [CURRENT])] });
  const saving: string[] = [];
  const done = await Effect.runPromise(Deferred.make<void>());
  const agents: NodeAgents = {
    ...base.agents,
    checkpoint: (on, request) => Effect.sync(() => saving.push(`${request.workspace} on ${on}`)).pipe(Effect.andThen(Deferred.await(done)), Effect.andThen(base.agents.checkpoint(on, request))),
  };
  await run(fakeKube(objects), Effect.scoped(Effect.gen(function*() {
    const snapshotted = yield* makeSessionSnapshots({ profile: profile(), snapshots, agents, templates: templates(profile()) });
    const fibers = yield* Effect.forEach(names, ([name]) =>
      snapshotted.saveStopping(stored(sessionOf(profile(), name), "Suspended", null), objects.get(`Pod/alasio-sessions/${name}`) as V1Pod).pipe(Effect.forkScoped));
    yield* Effect.sleep("50 millis");
    assert.deepEqual(saving.toSorted(), ["fs-a1 on node-a", "fs-a2 on node-a", "fs-b1 on node-b"]);
    // Theirs done, the third of node-a's has its turn.
    yield* Deferred.succeed(done, undefined);
    yield* Fiber.joinAll(fibers);
    assert.deepEqual(saving.toSorted(), ["fs-a1 on node-a", "fs-a2 on node-a", "fs-a3 on node-a", "fs-b1 on node-b"]);
  })));
});

test("bayma's hook is told to go on with its held stop as the hook is told its other modes", () => {
  const [bayma] = profile().podTemplate.spec?.containers ?? [];
  assert.ok(bayma);
  assert.deepEqual(stopHookOf(bayma), ["node", "/opt/bayma/bayma.js", "sandbox-hook", "stop", "--state-dir", "/home/agent/.bayma"]);
  assert.equal(stopHookOf({ name: "bayma", command: ["bayma"], args: ["mcp-http"] }), null);
});
