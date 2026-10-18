import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createServer, request as httpRequest } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { gzipSync } from "node:zlib";

import type { KubernetesObject, V1Condition, V1Container, V1ObjectMeta } from "@kubernetes/client-node";
import type { Attributes } from "@opentelemetry/api";
import { type ExportResult, ExportResultCode } from "@opentelemetry/core";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-proto";
import { ProtobufTraceSerializer } from "@opentelemetry/otlp-transformer";
import { resourceFromAttributes } from "@opentelemetry/resources";
import { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor } from "@opentelemetry/sdk-trace-base";
import { ConfigProvider, Effect, Exit, Fiber, Layer, Option, Result, Schema, type Scope, Stream } from "effect";
import { FetchHttpClient } from "effect/http";
import { TestClock } from "effect/testing";

import { type ExecResult, exitCodeOf, KubeApiError, KubeClient, KubeExecError, mergePatch, patchBody } from "../src/kube/client.ts";
import { decodeKubeTemplates, type KubeTemplates, loadKubeTemplates, type SessionsProfile } from "../src/kube/config.ts";
import { EGRESS_GATE_SCRIPT } from "../src/kube/sandbox-shape.ts";
import {
  makeSandboxes,
  newToken,
  podEnded,
  POD_TEMPLATE_ANNOTATION,
  podTemplateHash,
  type SandboxSpec,
  type SandboxStatus,
  sandboxManifest,
  sandboxReady,
  tokenSecretManifest,
} from "../src/kube/sandboxes.ts";
import { SessionToken } from "../src/sandbox/names.ts";
import { makeRateLimiter, serveTelemetryReceiver } from "../src/sandbox/telemetry-receiver.ts";
import { sameToken } from "../src/shared/tokens.ts";
import type { Signal } from "../src/telemetry/config.ts";
import { createOtlpForwarder, type ForwardResult, type OtlpEncoding, type OtlpForwarder } from "../src/telemetry/forward.ts";

/** The session `volumeId`, as a receiver's authentication says it, stamped as a sandbox's is. */
const stamped = (volumeId: string) => ({ session: volumeId, stamp: { "service.name": "bayma", "alasio.volume.id": volumeId } });

const profile = (extra: Partial<SessionsProfile> = {}): SessionsProfile => ({
  namespace: "alasio-sessions",
  port: 7290,
  workspaceDir: "/workspace",
  podTemplate: {
    metadata: { labels: { team: "a" } },
    spec: {
      containers: [{ name: "bayma", image: "agent@sha256:1", args: ["mcp-http", "--port", "7290"], securityContext: { runAsUser: 1000 } }],
      initContainers: [{ name: "prepare", image: "agent@sha256:1" }],
      volumes: [{ name: "tmp", emptyDir: {} }],
    },
  },
  volumeClaimTemplates: [{ metadata: { name: "data" }, spec: { resources: { requests: { storage: "1Gi" } } } }],
  ...extra,
});

/** A Sandbox, Secret or Pod as the fake API server keeps it, with the uid and generation it gave it. */
interface KeptObject extends KubernetesObject {
  metadata: V1ObjectMeta & { uid: string; generation: number };
  spec?: Partial<SandboxSpec> & { nodeName?: string; containers?: V1Container[] };
  status?: SandboxStatus;
  data?: Record<string, string>;
  stringData?: Record<string, string>;
}

/** A merge patch of a Sandbox's annotations and spec, the only kind alasio makes. */
interface SandboxPatch {
  readonly metadata?: { readonly annotations?: Readonly<Record<string, string | undefined>> };
  readonly spec?: Partial<SandboxSpec>;
}

/** `target` with the JSON merge patch `patch` applied (RFC 7386), as the API server applies one. */
function applyMergePatch(target: unknown, patch: unknown): unknown {
  if (typeof patch !== "object" || patch === null || Array.isArray(patch)) return patch;
  const result: Record<string, unknown> = typeof target === "object" && target !== null && !Array.isArray(target) ? { ...target } : {};
  for (const [key, value] of Object.entries(patch)) {
    if (value === null) delete result[key];
    else result[key] = applyMergePatch(result[key], value);
  }
  return result;
}

type ExecArgs = Parameters<KubeClient["Service"]["exec"]>;

/** A call the fake API server was made, with what identifies it. */
type KubeCall =
  | readonly [verb: "read" | "create" | "remove", kind: string | undefined, name: string | undefined]
  | readonly [verb: "list", kind: string, labelSelector: string]
  | readonly [verb: "patch", kind: string, name: string, patch: SandboxPatch]
  | readonly [verb: "exec", ...ExecArgs];

interface FakeKubeOptions {
  /** What a command run in a container comes to: its result, or how the exec failed. */
  readonly onExec?: (...args: ExecArgs) => ExecResult | KubeExecError;
  /** How deleting the object of `kind` and `name` fails, if it does. */
  readonly onRemove?: (kind: string, name: string) => KubeApiError | undefined;
}

/** The Ready condition agent-sandbox's controller sets once it has seen `generation`. */
const readyCondition = (observedGeneration: number): V1Condition => ({
  type: "Ready",
  status: "True",
  observedGeneration,
  reason: "Ready",
  message: "",
  lastTransitionTime: new Date(0),
});

/** The API's refusal `status`, as the client fails with it. */
const refusal = (status: number, message: string) => new KubeApiError({ status, cause: new Error(message) });

/**
 * An API server of Sandboxes, Secrets and Pods in memory, as a KubeClient. `ready(name)`
 * marks a Sandbox's pod ready, as agent-sandbox's controller would; `exec` answers with
 * `onExec`, and `remove` fails as `onRemove` says.
 */
function fakeKube({ onExec = () => ({ exitCode: 0, stdout: Buffer.alloc(0), stderr: "" }), onRemove = () => undefined }: FakeKubeOptions = {}) {
  const objects = new Map<string, KeptObject>();
  const key = (kind: string | undefined, namespace: string | undefined, name: string | undefined) => `${kind}/${namespace}/${name}`;
  let uid = 0;
  const calls: KubeCall[] = [];
  const read = (kind: string, namespace: string, name: string): KeptObject | null => {
    const object = objects.get(key(kind, namespace, name));
    if (!object) return null;
    const copy = structuredClone(object);
    if (kind === "Secret" && copy.stringData) {
      copy.data = Object.fromEntries(Object.entries(copy.stringData).map(([k, v]) => [k, Buffer.from(v).toString("base64")]));
      delete copy.stringData;
    }
    return copy;
  };
  const create = <T extends KubernetesObject>(object: T): Effect.Effect<T, KubeApiError> =>
    Effect.suspend(() => {
      // As the API server names an object by its generateName.
      const name = object.metadata?.name ?? `${object.metadata?.generateName}${uid + 1}`;
      calls.push(["create", object.kind, name]);
      const k = key(object.kind, object.metadata?.namespace, name);
      if (objects.has(k)) return Effect.fail(refusal(409, "exists"));
      const stored = { ...structuredClone(object), metadata: { ...structuredClone(object.metadata), name, uid: `uid-${++uid}`, generation: 1 } };
      objects.set(k, stored);
      return Effect.succeed(structuredClone(stored));
    });
  const patch = (kind: string, namespace: string, name: string, change: SandboxPatch): Effect.Effect<KeptObject, KubeApiError> =>
    Effect.suspend(() => {
      calls.push(["patch", kind, name, change]);
      const object = objects.get(key(kind, namespace, name));
      if (!object) return Effect.fail(refusal(404, "absent"));
      const { metadata, spec } = applyMergePatch(object, structuredClone(change)) as KeptObject;
      object.metadata = metadata;
      object.spec = spec ?? {};
      // As the API server does: a change of spec is a new generation.
      if (change.spec) object.metadata.generation += 1;
      return Effect.succeed(structuredClone(object));
    });
  const service = KubeClient.of({
    read: (_apiVersion, kind, namespace, name) => Effect.sync(() => {
      calls.push(["read", kind, name]);
      return read(kind, namespace, name);
    }),
    create,
    list: (_apiVersion, kind, namespace, labelSelector) => Effect.sync(() => {
      calls.push(["list", kind, labelSelector]);
      const wanted = labelSelector.split(",").map((term) => term.split("="));
      return [...objects.values()]
        .filter(({ kind: kept, metadata }) => kept === kind && metadata.namespace === namespace && wanted.every(([label, value]) => metadata.labels?.[label ?? ""] === value))
        .map((object) => structuredClone(object));
    }),
    replace: () => Effect.die("alasio replaces no object"),
    patch: (_apiVersion, kind, namespace, name, change) => patch(kind, namespace, name, change),
    patchPodStatus: () => Effect.die("no pod's status is patched"),
    remove: (_apiVersion, kind, namespace, name) => Effect.suspend(() => {
      calls.push(["remove", kind, name]);
      const refused = onRemove(kind, name);
      if (refused) return Effect.fail(refused);
      objects.delete(key(kind, namespace, name));
      for (const [k, object] of objects) {
        if (object.metadata.ownerReferences?.some((owner) => owner.name === name && owner.kind === kind)) objects.delete(k);
      }
      return Effect.void;
    }),
    watch: () => Stream.die("nothing is watched"),
    exec: (...args) => Effect.suspend(() => {
      calls.push(["exec", ...args]);
      const result = onExec(...args);
      return result instanceof KubeExecError ? Effect.fail(result) : Effect.succeed(result);
    }),
  });
  return {
    objects,
    calls,
    layer: Layer.succeed(KubeClient, service),
    /** What the API server holds, read as alasio would, outside the calls it records. */
    peek: read,
    create: (object: KubernetesObject) => Effect.runSync(create(object)),
    patch: (kind: string, namespace: string, name: string, change: SandboxPatch) => Effect.runSync(patch(kind, namespace, name, change)),
    ready(namespace: string, name: string) {
      const sandbox = objects.get(key("Sandbox", namespace, name));
      assert.ok(sandbox, `Sandbox ${namespace}/${name} exists`);
      sandbox.status = {
        serviceFQDN: `${name}.${namespace}.svc.cluster.local`,
        conditions: [readyCondition(sandbox.metadata.generation)],
      };
    },
    /**
     * Acts on the Sandbox every few milliseconds as agent-sandbox's controller does: ends
     * the pod of a suspended one, saying so, and starts a running one's from its current
     * pod template, on the node `node-a`, recorded in `pods`, unless `fails` gives a
     * reason that pod ends at once with; a change of a running one's spec leaves its pod
     * as it is. Stops when `stop()` is called.
     */
    control(namespace: string, name: string, fails: (sandbox: KeptObject) => string | null = () => null) {
      const pods: string[] = [];
      const timer = setInterval(() => {
        const sandbox = objects.get(key("Sandbox", namespace, name));
        if (!sandbox) return;
        const generation = sandbox.metadata.generation;
        if (sandbox.spec?.operatingMode === "Suspended") {
          objects.delete(key("Pod", namespace, name));
          sandbox.status = { conditions: [{ ...readyCondition(generation), type: "Suspended", reason: "SandboxSuspendedPodTerminated" }] };
        } else if (!sandboxReady(sandbox) && !podEnded(sandbox) && objects.has(key("Pod", namespace, name))) {
          this.ready(namespace, name);
        } else if (!sandboxReady(sandbox) && !podEnded(sandbox)) {
          // As agent-sandbox, which replaces no pod that has ended.
          pods.push(String(sandbox.spec?.podTemplate?.spec?.containers[0]?.image));
          objects.set(key("Pod", namespace, name), {
            apiVersion: "v1",
            kind: "Pod",
            metadata: { name, namespace, uid: `uid-${++uid}`, generation: 1 },
            spec: { containers: structuredClone(sandbox.spec?.podTemplate?.spec?.containers ?? []), nodeName: "node-a" },
          });
          const ended = fails(sandbox);
          if (ended === null) this.ready(namespace, name);
          else sandbox.status = { conditions: [{ ...readyCondition(generation), status: "False", reason: ended, message: "" }] };
        }
      }, 5);
      return { pods, stop: () => clearInterval(timer) };
    },
  };
}

type FakeKube = ReturnType<typeof fakeKube>;

/** bayma as seen over HTTP: 401 without the token, 400 with it (no MCP session). */
function fakeBayma(kube: FakeKube, namespace: string): typeof fetch {
  return async (url, init) => {
    const name = new URL(url instanceof Request ? url.url : url).hostname.split(".")[0];
    const encoded = kube.peek("Secret", namespace, `${name}-bayma-token`)?.data?.["token"];
    assert.ok(encoded, `${name} has a token Secret`);
    const token = Buffer.from(encoded, "base64").toString();
    return new Response(null, { status: new Headers(init?.headers).get("Authorization") === `Bearer ${token}` ? 400 : 401 });
  };
}

/** `effect` run against `kube`, reaching bayma with `fetch`, in a scope of its own. */
function onKube<A, E>(kube: FakeKube, fetch: typeof globalThis.fetch, effect: Effect.Effect<A, E, KubeClient | Scope.Scope>): Promise<A> {
  return Effect.runPromise(Effect.scoped(effect).pipe(Effect.provide(Layer.merge(kube.layer, Layer.succeed(FetchHttpClient.Fetch, fetch)))));
}

/** Waits `ms` of real time, as the fake controller takes to mark a Sandbox ready. */
const after = (ms: number, then: () => void) => setTimeout(then, ms);

test("the deployment's templates are checked as alasio starts", async () => {
  const load = (value: unknown): Result.Result<KubeTemplates, { readonly message: string }> => decodeKubeTemplates("/t.json", JSON.stringify(value));
  const failure = (value: unknown) => Result.match(load(value), { onSuccess: () => assert.fail("decoded"), onFailure: (error) => error.message });
  const sessions = Result.getOrThrow(load({ sessions: profile() }));
  assert.equal(sessions.host, null);
  assert.equal(sessions.sessions?.podTemplate.spec?.containers[0]?.image, "agent@sha256:1");
  // A namespace YAML reads as a number is that name.
  assert.equal(Result.getOrThrow(load({ sessions: { ...profile(), namespace: 123 } })).sessions?.namespace, "123");
  assert.equal(
    Result.match(decodeKubeTemplates("/t.json", "{"), { onSuccess: () => "", onFailure: (error) => error.message }),
    "ALASIO_KUBE_TEMPLATES /t.json is not readable JSON: Expected property name or '}' in JSON at position 1 (line 1 column 2)",
  );
  assert.equal(failure({ sessions: { ...profile(), namespace: "Bad_NS" } }), "ALASIO_KUBE_TEMPLATES sessions.namespace must be a namespace name");
  assert.equal(failure({ sessions: { ...profile(), port: 0 } }), "ALASIO_KUBE_TEMPLATES sessions.port must be a port number");
  assert.equal(
    failure({ host: { ...profile(), stateRoot: "/s", podTemplate: { spec: { containers: [{ name: "x" }] } } } }),
    'ALASIO_KUBE_TEMPLATES host.podTemplate.spec.containers must include one named "bayma"',
  );
  assert.equal(
    failure({ sessions: { ...profile(), podTemplate: { spec: { containers: { name: "bayma" } } } } }),
    'ALASIO_KUBE_TEMPLATES sessions.podTemplate.spec.containers must include one named "bayma"',
  );
  assert.equal(failure({ sessions: { ...profile(), workspaceDir: undefined } }), "ALASIO_KUBE_TEMPLATES sessions.workspaceDir must be a path");
  assert.equal(failure({ host: profile() }), "ALASIO_KUBE_TEMPLATES host.stateRoot must be a path");
  assert.equal(failure({ sessions: 5 }), "ALASIO_KUBE_TEMPLATES sessions must be an object");
  const clone = { claimTemplate: "data", job: { kind: "Job" } };
  assert.deepEqual(Result.getOrThrow(load({ sessions: { ...profile(), clone } })).sessions?.clone, clone);
  const shortName = "ALASIO_KUBE_TEMPLATES sessions.clone.claimTemplate must be a claim template's name, a DNS label of at most 4 characters";
  assert.equal(failure({ sessions: { ...profile(), clone: { ...clone, claimTemplate: "" } } }), shortName);
  // Its claims, `<template>-<volume id>`, would be longer than a DNS label.
  assert.equal(failure({ sessions: { ...profile(), clone: { ...clone, claimTemplate: "volume" } } }), shortName);
  assert.equal(failure({ sessions: { ...profile(), clone: { claimTemplate: "data" } } }), "ALASIO_KUBE_TEMPLATES sessions.clone.job must be an object");

  const loading = (env: Record<string, string>) =>
    Effect.runPromiseExit(loadKubeTemplates.pipe(Effect.provide(ConfigProvider.layer(ConfigProvider.fromEnv({ env })))));
  const unset = await loading({});
  assert.ok(Exit.isFailure(unset));
  assert.equal(Option.getOrThrow(Exit.findErrorOption(unset)).message, "ALASIO_KUBE_TEMPLATES is not set: alasio runs where alasio up installs it");
  const directory = mkdtempSync(join(tmpdir(), "alasio-templates-"));
  try {
    const path = join(directory, "templates.json");
    writeFileSync(path, JSON.stringify({ sessions: profile() }));
    const loaded = await loading({ ALASIO_KUBE_TEMPLATES: path });
    assert.ok(Exit.isSuccess(loaded));
    assert.equal(loaded.value.sessions?.namespace, "alasio-sessions");
    const missing = await loading({ ALASIO_KUBE_TEMPLATES: join(directory, "missing.json") });
    assert.ok(Exit.isFailure(missing));
    assert.match(Option.getOrThrow(Exit.findErrorOption(missing)).message, /^ALASIO_KUBE_TEMPLATES .*missing\.json is not readable JSON: ENOENT/u);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("a token names its Sandbox and is compared whole", () => {
  const token = newToken("fs-abc123", () => "secret");
  assert.equal(token, "fs-abc123.secret");
  const session = (presented: unknown) => Option.map(Schema.decodeUnknownOption(SessionToken)(presented), ([volumeId]) => volumeId);
  assert.deepEqual(session(token), Option.some("fs-abc123"));
  assert.deepEqual(session("nodot"), Option.none());
  assert.deepEqual(session("Bad.secret"), Option.none());
  assert.deepEqual(session(undefined), Option.none());
  assert.equal(sameToken(token, "fs-abc123.secret"), true);
  assert.equal(sameToken(token, "fs-abc123.secreT"), false);
  assert.equal(sameToken(token, "fs-abc123.secret2"), false);
});

test("every Sandbox serves bayma with its token, from a Secret it owns", () => {
  const sandbox = sandboxManifest({ name: "fs-abc123", namespace: "alasio-sessions", template: profile(), labels: { extra: "1" } });
  assert.equal(sandbox.spec.operatingMode, "Running");
  assert.equal(sandbox.spec.service, true);
  assert.deepEqual(sandbox.metadata.labels, { "app.kubernetes.io/managed-by": "alasio", "alasio.dev/sandbox": "fs-abc123", extra: "1" });
  assert.deepEqual(sandbox.spec.podTemplate.metadata?.labels, { team: "a", ...sandbox.metadata.labels });
  const podSpec = sandbox.spec.podTemplate.spec;
  assert.ok(podSpec);
  const [bayma] = podSpec.containers;
  assert.ok(bayma);
  assert.deepEqual(bayma.args, ["mcp-http", "--port", "7290", "--token-file", "/run/alasio/bayma/token"]);
  assert.deepEqual(bayma.volumeMounts, [{ name: "alasio-bayma-token", mountPath: "/run/alasio/bayma", readOnly: true }]);
  assert.deepEqual(podSpec.volumes?.at(-1), {
    name: "alasio-bayma-token",
    secret: { secretName: "fs-abc123-bayma-token", defaultMode: 0o440 },
  });
  assert.equal(sandbox.spec.volumeClaimTemplates?.[0]?.metadata?.name, "data");
  assert.equal(sandboxManifest({ name: "fs-abc123", namespace: "alasio-sessions", template: profile(), operatingMode: "Suspended" }).spec.operatingMode, "Suspended");
  assert.throws(
    () => sandboxManifest({ name: "x", namespace: "n", template: { podTemplate: { spec: { containers: [{ name: "other" }] } } } }),
    /no container named "bayma"/,
  );

  const secret = tokenSecretManifest({ metadata: { name: "fs-abc123", namespace: "alasio-sessions", uid: "u1" } }, "t");
  assert.deepEqual(secret.metadata?.ownerReferences, [{
    apiVersion: "agents.x-k8s.io/v1beta1", kind: "Sandbox", name: "fs-abc123", uid: "u1", controller: true, blockOwnerDeletion: true,
  }]);
  assert.deepEqual(secret.stringData, { token: "t" });
});

test("a Sandbox is ready only once its controller has seen its current spec", () => {
  const sandbox = (generation: number, observed: number) => ({
    metadata: { generation },
    status: { conditions: [readyCondition(observed)] },
  });
  assert.equal(sandboxReady(sandbox(2, 2)), true);
  assert.equal(sandboxReady(sandbox(3, 2)), false);
  assert.equal(sandboxReady({ metadata: { generation: 1 } }), false);
});

test("ensure makes the Sandbox and its token, waits for bayma to answer, and is shared by callers asking at once", async () => {
  const kube = fakeKube();
  const fetches: Parameters<typeof fetch>[0][] = [];
  const bayma = fakeBayma(kube, "alasio-sessions");
  let answered = 0;
  // bayma's first answer is the connection refused while its policy applies.
  const fetchBayma: typeof fetch = async (url, options) => {
    fetches.push(url);
    if (answered++ === 0) throw new Error("ECONNREFUSED");
    return await bayma(url, options);
  };
  const manifest = () => sandboxManifest({ name: "fs-abc123", namespace: "alasio-sessions", template: profile() });
  const { a, b, token } = await onKube(kube, fetchBayma, Effect.gen(function*() {
    const sandboxes = yield* makeSandboxes({ namespace: "alasio-sessions", port: 7290, poll: "5 millis" });
    after(20, () => kube.ready("alasio-sessions", "fs-abc123"));
    const [a, b] = yield* Effect.all([sandboxes.ensure("fs-abc123", manifest), sandboxes.ensure("fs-abc123", manifest)], { concurrency: "unbounded" });
    return { a, b, token: yield* sandboxes.token("fs-abc123") };
  }));
  assert.deepEqual(a, b);
  assert.equal(a.url, "http://fs-abc123.alasio-sessions.svc.cluster.local:7290/mcp");
  assert.match(token ?? "", /^fs-abc123\.[\w-]{43}$/u);
  assert.deepEqual(a.headers, { Authorization: `Bearer ${token}` });
  assert.equal(kube.calls.filter(([verb, kind]) => verb === "create" && kind === "Sandbox").length, 1);
  assert.equal(kube.calls.filter(([verb, kind]) => verb === "create" && kind === "Secret").length, 1);
  assert.equal(fetches.length, 2);
});

test("ensure resumes a suspended Sandbox and gives one left without a token its Secret", async () => {
  const kube = fakeKube();
  const manifest = () => sandboxManifest({ name: "fs-abc123", namespace: "alasio-sessions", template: profile() });
  const created = kube.create(manifest());
  kube.patch("Sandbox", "alasio-sessions", "fs-abc123", { spec: { operatingMode: "Suspended" } });
  kube.ready("alasio-sessions", "fs-abc123");
  kube.calls.length = 0;
  await onKube(kube, fakeBayma(kube, "alasio-sessions"), Effect.gen(function*() {
    const sandboxes = yield* makeSandboxes({ namespace: "alasio-sessions", port: 7290, poll: "5 millis" });
    // The resume raises the generation, so the earlier readiness no longer counts.
    after(20, () => kube.ready("alasio-sessions", "fs-abc123"));
    yield* sandboxes.ensure("fs-abc123", manifest);
  }));
  assert.equal(kube.peek("Sandbox", "alasio-sessions", "fs-abc123")?.spec?.operatingMode, "Running");
  assert.equal(kube.peek("Secret", "alasio-sessions", "fs-abc123-bayma-token")?.metadata.ownerReferences?.[0]?.uid, created.metadata?.uid);
  // Made from its current template, it is resumed and nothing else.
  assert.deepEqual(kube.calls.filter(([verb]) => verb === "patch" || verb === "create").map(([verb, kind, , change]) => [verb, kind, change]), [
    ["create", "Secret", undefined],
    ["patch", "Sandbox", { spec: { operatingMode: "Running" } }],
  ]);
});

test("suspend returns once the Sandbox's pod has ended, and leaves one that is gone as it is", async () => {
  const kube = fakeKube();
  const manifest = () => sandboxManifest({ name: "fs-abc123", namespace: "alasio-sessions", template: profile() });
  const controller = kube.control("alasio-sessions", "fs-abc123");
  await onKube(kube, fakeBayma(kube, "alasio-sessions"), Effect.gen(function*() {
    const sandboxes = yield* makeSandboxes({ namespace: "alasio-sessions", port: 7290, poll: "5 millis" });
    yield* sandboxes.ensure("fs-abc123", manifest);
    assert.ok(kube.peek("Pod", "alasio-sessions", "fs-abc123"));
    yield* sandboxes.suspend("fs-abc123");
    assert.equal(kube.peek("Pod", "alasio-sessions", "fs-abc123"), null);
    yield* sandboxes.suspend("fs-gone");
  })).finally(controller.stop);
  assert.equal(kube.peek("Sandbox", "alasio-sessions", "fs-abc123")?.spec?.operatingMode, "Suspended");
});

test("a Sandbox records the pod template it runs, the same for the same template however it is written", () => {
  const hashOf = (template: SessionsProfile) =>
    sandboxManifest({ name: "fs-abc123", namespace: "alasio-sessions", template }).metadata.annotations?.[POD_TEMPLATE_ANNOTATION];
  const recorded = hashOf(profile());
  assert.match(recorded ?? "", /^[0-9a-f]{64}$/u);
  assert.equal(recorded, podTemplateHash(sandboxManifest({ name: "fs-abc123", namespace: "alasio-sessions", template: profile() }).spec.podTemplate));
  // The same template with its keys in another order.
  const podTemplate = profile().podTemplate;
  assert.equal(hashOf(profile({ podTemplate: Object.fromEntries(Object.entries(podTemplate).reverse()) })), recorded);
  assert.notEqual(hashOf(profile({ podTemplate: { ...profile().podTemplate, spec: { containers: [{ name: "bayma", image: "agent@sha256:2" }] } } })), recorded);
  // What the caller adds to the Sandbox's own metadata is beside it.
  assert.deepEqual(
    sandboxManifest({ name: "fs-abc123", namespace: "alasio-sessions", template: profile(), annotations: { "alasio.dev/conversation": "telegram:1" } }).metadata.annotations,
    { "alasio.dev/conversation": "telegram:1", [POD_TEMPLATE_ANNOTATION]: recorded },
  );
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
  const sandbox = ["agents.x-k8s.io/v1beta1", "Sandbox", "alasio-sessions", "fs-abc123"] as const;
  assert.deepEqual(patchBody(...sandbox, { spec: { operatingMode: "Running" } }), {
    apiVersion: "agents.x-k8s.io/v1beta1",
    kind: "Sandbox",
    metadata: { namespace: "alasio-sessions", name: "fs-abc123" },
    spec: { operatingMode: "Running" },
  });
  assert.deepEqual(patchBody(...sandbox, { metadata: { annotations: { a: "1" }, name: "other" }, spec: {} }), {
    apiVersion: "agents.x-k8s.io/v1beta1",
    kind: "Sandbox",
    metadata: { annotations: { a: "1" }, namespace: "alasio-sessions", name: "fs-abc123" },
    spec: {},
  });
});

test("ensure moves a Sandbox made from another pod template onto alasio's, ending its pod and keeping its volumes", async () => {
  const kube = fakeKube();
  const older = sandboxManifest({ name: "fs-abc123", namespace: "alasio-sessions", template: profile() });
  const created = kube.create(older);
  // Its pod of the older template runs.
  kube.ready("alasio-sessions", "fs-abc123");
  // A newer bayma, and a label alasio no longer sets.
  const template = profile({
    podTemplate: {
      metadata: {},
      spec: { ...profile().podTemplate.spec, containers: [{ name: "bayma", image: "agent@sha256:2", args: ["mcp-http", "--port", "7290"] }] },
    },
  });
  const current = sandboxManifest({ name: "fs-abc123", namespace: "alasio-sessions", template, annotations: { "alasio.dev/conversation": "telegram:1" } });
  const controller = kube.control("alasio-sessions", "fs-abc123");
  const patches = await onKube(kube, fakeBayma(kube, "alasio-sessions"), Effect.gen(function*() {
    const sandboxes = yield* makeSandboxes({ namespace: "alasio-sessions", port: 7290, poll: "5 millis" });
    yield* sandboxes.ensure("fs-abc123", () => current);
    const patches = kube.calls.filter(([verb]) => verb === "patch").map(([, , , change]) => change);
    // Brought up again, it is on alasio's template, and left as it is.
    kube.calls.length = 0;
    yield* sandboxes.ensure("fs-abc123", () => current);
    assert.deepEqual(kube.calls.filter(([verb]) => verb === "patch"), []);
    return patches;
  })).finally(controller.stop);
  // Suspended with the current template, which ends the older pod, then resumed: one pod, of the current template.
  assert.deepEqual(patches, [
    {
      metadata: { annotations: { [POD_TEMPLATE_ANNOTATION]: current.metadata.annotations?.[POD_TEMPLATE_ANNOTATION] } },
      spec: { operatingMode: "Suspended", podTemplate: mergePatch(older.spec.podTemplate, current.spec.podTemplate) },
    },
    { spec: { operatingMode: "Running" } },
  ]);
  assert.deepEqual(controller.pods, ["agent@sha256:2"]);
  const moved = kube.peek("Sandbox", "alasio-sessions", "fs-abc123");
  assert.ok(moved);
  assert.deepEqual(moved.spec?.podTemplate, current.spec.podTemplate);
  assert.equal(moved.metadata.annotations?.[POD_TEMPLATE_ANNOTATION], current.metadata.annotations?.[POD_TEMPLATE_ANNOTATION]);
  assert.equal(moved.spec?.operatingMode, "Running");
  assert.deepEqual(moved.spec?.volumeClaimTemplates, older.spec.volumeClaimTemplates);
  assert.equal(moved.metadata.uid, created.metadata?.uid, "the Sandbox, and so its volumes, are the same");
  assert.equal(kube.peek("Secret", "alasio-sessions", "fs-abc123-bayma-token")?.metadata.ownerReferences?.[0]?.uid, created.metadata?.uid);
});

test("ensure restarts a Sandbox whose pod has ended, which agent-sandbox never replaces, keeping its volumes", async () => {
  const kube = fakeKube();
  const manifest = () => sandboxManifest({ name: "fs-abc123", namespace: "alasio-sessions", template: profile() });
  const created = kube.create(manifest());
  // Evicted: the controller says so, for the Sandbox's current spec, and leaves it.
  const sandbox = kube.objects.get("Sandbox/alasio-sessions/fs-abc123");
  assert.ok(sandbox);
  sandbox.status = { conditions: [{ ...readyCondition(sandbox.metadata.generation), status: "False", reason: "PodFailed", message: "Pod failed" }] };
  assert.equal(podEnded(sandbox), true);
  kube.calls.length = 0;
  const controller = kube.control("alasio-sessions", "fs-abc123");
  await onKube(kube, fakeBayma(kube, "alasio-sessions"), Effect.gen(function*() {
    const sandboxes = yield* makeSandboxes({ namespace: "alasio-sessions", port: 7290, poll: "5 millis" });
    yield* sandboxes.ensure("fs-abc123", manifest);
  })).finally(controller.stop);
  assert.deepEqual(kube.calls.filter(([verb]) => verb === "patch").map(([, , , change]) => change), [
    { spec: { operatingMode: "Suspended" } },
    { spec: { operatingMode: "Running" } },
  ]);
  assert.equal(controller.pods.length, 1);
  assert.equal(kube.peek("Sandbox", "alasio-sessions", "fs-abc123")?.metadata.uid, created.metadata?.uid);
});

test("ensure gives up on a Sandbox that does not become ready in five minutes, saying why", async () => {
  const kube = fakeKube();
  const error = await onKube(kube, fetch, Effect.gen(function*() {
    const sandboxes = yield* makeSandboxes({ namespace: "ns", port: 7290 });
    const ensuring = yield* Effect.forkChild(Effect.flip(sandboxes.ensure("fs-abc123", () => sandboxManifest({ name: "fs-abc123", namespace: "ns", template: profile() }))));
    let waited = 0;
    while (ensuring.pollUnsafe() === undefined) {
      yield* TestClock.adjust("500 millis");
      waited += 500;
    }
    assert.ok(waited >= 300_000 && waited <= 301_000, `gave up after ${waited} ms`);
    return yield* Fiber.join(ensuring);
  }).pipe(Effect.provide(TestClock.layer())));
  assert.equal(error.message, "Sandbox ns/fs-abc123 did not become ready within 300s");
});

/** The egress gate against an API server on `port` of this machine, given 1.5s: its exit code and stderr. */
function runGate(port: number) {
  return new Promise<{ code: number | null; stderr: string }>((resolve) => {
    const child = spawn(process.execPath, ["-e", EGRESS_GATE_SCRIPT.replace("120000", "1500")], {
      env: { KUBERNETES_SERVICE_HOST: "127.0.0.1", KUBERNETES_SERVICE_PORT: String(port) },
    });
    let stderr = "";
    child.stderr.on("data", (chunk) => { stderr += chunk; });
    child.on("exit", (code) => resolve({ code, stderr }));
  });
}

test("the egress gate passes once the API server is unreachable, and fails a pod whose egress stays open", async () => {
  // Nothing listens on port 9 here: connections are refused, as egress policy refuses them.
  assert.equal((await runGate(9)).code, 0);
  const server = createServer().listen(0, "127.0.0.1");
  await once(server, "listening");
  try {
    // Listening on a TCP port, the server's address is an AddressInfo.
    const open = await runGate((server.address() as AddressInfo).port);
    assert.equal(open.code, 1);
    assert.match(open.stderr, /egress is not confined/);
  } finally {
    server.close();
  }
});

test("an exec's exit code is read from its status", () => {
  assert.deepEqual(exitCodeOf({ status: "Success" }), Result.succeed(0));
  assert.deepEqual(exitCodeOf({ status: "Failure", reason: "NonZeroExitCode", details: { causes: [{ reason: "ExitCode", message: "3" }] } }), Result.succeed(3));
  const failed = exitCodeOf({ status: "Failure", message: "container not found" });
  assert.ok(Result.isFailure(failed));
  assert.equal(failed.failure.message, "exec failed: container not found");
});

test("a session's byte budget refills over time and asks an exporter to wait when spent", async () => {
  await Effect.runPromise(Effect.gen(function*() {
    const limiter = yield* makeRateLimiter({ rate: 100, burst: 300 });
    assert.equal(yield* limiter.take("a", 250), 0);
    assert.equal(yield* limiter.take("a", 100), 1);
    assert.equal(yield* limiter.take("b", 300), 0);
    yield* TestClock.adjust("1 second");
    assert.equal(yield* limiter.take("a", 100), 0);
  }).pipe(Effect.provide(TestClock.layer())));
});

function traceRequest(resource: Attributes) {
  const exporter = new InMemorySpanExporter();
  const provider = new BasicTracerProvider({ resource: resourceFromAttributes(resource), spanProcessors: [new SimpleSpanProcessor(exporter)] });
  provider.getTracer("t").startSpan("work").end();
  const request = ProtobufTraceSerializer.serializeRequest(exporter.getFinishedSpans());
  assert.ok(request);
  return Buffer.from(request);
}

/** What the receiver's `post` sends: a bearer token (or none), a body, its content type, and whether it is gzipped. */
interface ReceiverPost {
  readonly token?: string | null;
  readonly body?: Buffer;
  readonly type?: string;
  readonly gzip?: boolean;
}

test("the receiver takes a session's OTLP with its token only, stamped with what alasio knows", async () => {
  const exported: { signal: Signal; encoding: OtlpEncoding; body: Buffer }[] = [];
  const forwarder = {
    protocols: { traces: "http/protobuf" },
    async export(signal: Signal, encoding: OtlpEncoding, body: Buffer): Promise<ForwardResult> {
      exported.push({ signal, encoding, body });
      return { ok: true };
    },
  } satisfies Pick<OtlpForwarder, "protocols" | "export">;
  await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    const receiver = yield* serveTelemetryReceiver({
      port: 0,
      host: "127.0.0.1",
      forwarder,
      authenticate: (token) => Effect.succeed(token === "fs-abc123.good" ? stamped("fs-abc123") : null),
      limiter: yield* makeRateLimiter({ rate: 1, burst: 100_000 }),
    });
    const url = (path: string) => `http://127.0.0.1:${receiver.port}${path}`;
    const post = (path: string, { token = "fs-abc123.good", body = traceRequest({ "service.name": "liar", "alasio.volume.id": "fs-other" }), type = "application/x-protobuf", gzip = false }: ReceiverPost = {}) => fetch(url(path), {
      method: "POST",
      headers: { "content-type": type, ...(token ? { authorization: `Bearer ${token}` } : {}), ...(gzip ? { "content-encoding": "gzip" } : {}) },
      body: gzip ? gzipSync(body) : body,
    });
    yield* Effect.promise(async () => {
      assert.equal((await post("/v1/traces", { token: null })).status, 401);
      assert.equal((await post("/v1/traces", { token: "fs-abc123.bad" })).status, 401);
      assert.equal((await post("/v1/spans")).status, 404);
      assert.equal((await post("/v1/traces", { type: "text/plain" })).status, 415);
      assert.equal((await post("/v1/traces", { type: "constructor" })).status, 415);
      assert.equal((await post("/v1/traces", { body: Buffer.from([0x0a, 0xff]) })).status, 400);
      assert.equal((await post("/v1/logs")).status, 200);
      assert.equal(exported.length, 0);

      const response = await post("/v1/traces", { gzip: true });
      assert.equal(response.status, 200);
      // A full success, as the exporter decodes it: an empty protobuf message.
      assert.equal(response.headers.get("content-type"), "application/x-protobuf");
      assert.equal((await response.arrayBuffer()).byteLength, 0);
      const json = await post("/v1/logs", { type: "application/json; charset=utf-8", body: Buffer.from("{}") });
      assert.equal(await json.text(), "{}");
      assert.equal(exported.length, 1);
      const [stamped] = exported;
      assert.ok(stamped);
      const text = stamped.body.toString("latin1");
      assert.match(text, /fs-abc123/u);
      assert.doesNotMatch(text, /liar|fs-other/u);

      const flood = await post("/v1/traces", { body: Buffer.alloc(200_000) });
      assert.equal(flood.status, 429);
      assert.ok(Number(flood.headers.get("retry-after")) > 0);
      const huge = await post("/v1/traces", { body: Buffer.alloc(5 * 1024 * 1024) });
      assert.equal(huge.status, 413);
    });
  })));
});

/** Spans named `name`, ended, as a tracer gives them to its exporter. */
function spansNamed(name: string) {
  const exporter = new InMemorySpanExporter();
  const provider = new BasicTracerProvider({ resource: resourceFromAttributes({ "service.name": "bayma" }), spanProcessors: [new SimpleSpanProcessor(exporter)] });
  provider.getTracer("t").startSpan(name).end();
  return exporter.getFinishedSpans();
}

/**
 * A receiver of sessions' telemetry, each session's token `<its volume id>.good`, exporting
 * through `forwarder`, holding at most what `limits` says; and a POST of a trace request to
 * it, of session fs-abc123's unless another's token is given.
 */
const receiving = (forwarder: Pick<OtlpForwarder, "protocols" | "export">, limits: { maxHeldBytes?: number; maxSessionHeldBytes?: number } = {}) =>
  Effect.gen(function*() {
    const receiver = yield* serveTelemetryReceiver({
      port: 0,
      host: "127.0.0.1",
      forwarder,
      authenticate: (token) => Effect.succeed(token.endsWith(".good") ? stamped(token.slice(0, -".good".length)) : null),
      limiter: yield* makeRateLimiter({ rate: 1, burst: 100_000_000 }),
      ...limits,
    });
    const url = `http://127.0.0.1:${receiver.port}/v1/traces`;
    const post = (body = traceRequest({ "service.name": "bayma" }), { token = "fs-abc123.good", signal }: { token?: string; signal?: AbortSignal } = {}) =>
      fetch(url, { method: "POST", headers: { "content-type": "application/x-protobuf", authorization: `Bearer ${token}` }, body, ...(signal ? { signal } : {}) });
    return { url, post, receiver };
  });

/** A trace request of some 100 kB, most of it a span's, which stamping keeps. */
function paddedRequest(): Buffer<ArrayBuffer> {
  const spans = new InMemorySpanExporter();
  const provider = new BasicTracerProvider({ resource: resourceFromAttributes({ "service.name": "bayma" }), spanProcessors: [new SimpleSpanProcessor(spans)] });
  provider.getTracer("t").startSpan("work", { attributes: { padding: "x".repeat(100_000) } }).end();
  const request = ProtobufTraceSerializer.serializeRequest(spans.getFinishedSpans());
  assert.ok(request);
  return Buffer.from(request);
}

/** Waits until `holds`, within five seconds, failing with `what` where it does not. */
async function until(holds: () => boolean, what: string): Promise<void> {
  for (const deadline = Date.now() + 5000; !holds();) {
    assert.ok(Date.now() < deadline, what);
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

test("the receiver answers a request taken only once the collector has taken it: 503 with a Retry-After while it does not, which the exporter sends again after, and 400 where it refuses it", async () => {
  const results: ForwardResult[] = [
    { ok: false, error: "HTTP 503", retryable: true, retryAfterMs: null },
    { ok: false, error: "HTTP 429", retryable: true, retryAfterMs: 2500 },
    { ok: false, error: "connect ECONNREFUSED", retryable: true, retryAfterMs: null },
    { ok: false, error: "HTTP 400", retryable: false, retryAfterMs: null },
    { ok: true },
  ];
  const within: (number | undefined)[] = [];
  const forwarder = {
    protocols: { traces: "http/protobuf" },
    async export(_signal: Signal, _encoding: OtlpEncoding, _body: Buffer, options?: { withinMs?: number }): Promise<ForwardResult> {
      within.push(options?.withinMs);
      return results.shift() ?? { ok: true };
    },
  } satisfies Pick<OtlpForwarder, "protocols" | "export">;
  await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    const { post } = yield* receiving(forwarder);
    yield* Effect.promise(async () => {
      const answers = [];
      for (let n = 0; n < 5; n += 1) {
        const response = await post();
        answers.push([response.status, response.headers.get("retry-after")]);
      }
      assert.deepEqual(answers, [[503, "1"], [503, "3"], [503, "1"], [400, null], [200, null]]);
      // Each forwarded within what leaves the exporter, waiting for the answer, time to send it again.
      assert.ok(within.every((ms) => ms !== undefined && ms < 10_000), JSON.stringify(within));
    });
  })));
});

test("the receiver holds at most so many bytes of the requests it is answering at once, answering 503 past them, and gives them back as each is answered", async () => {
  const body = paddedRequest();
  let release = () => {};
  const held = new Promise<void>((resolve) => void (release = resolve));
  let exports = 0;
  const forwarder = {
    protocols: { traces: "http/protobuf" },
    async export(): Promise<ForwardResult> {
      exports += 1;
      // The first held until released, as by a collector slow to answer.
      if (exports === 1) await held;
      return { ok: true };
    },
  } satisfies Pick<OtlpForwarder, "protocols" | "export">;
  await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    // One request held, read and stamped, but not the next read beside it.
    const { post } = yield* receiving(forwarder, { maxHeldBytes: 2.5 * body.length, maxSessionHeldBytes: 2.5 * body.length });
    yield* Effect.promise(async () => {
      const first = post(body);
      try {
        await until(() => exports > 0, "the first request never reached the collector");
        const second = await post(body);
        assert.deepEqual([second.status, second.headers.get("retry-after")], [503, "1"]);
      } finally {
        release();
      }
      assert.equal((await first).status, 200);
      assert.equal((await post(body)).status, 200);
      // One larger than all it may hold, alone, though not than a request may be.
      assert.equal((await post(Buffer.alloc(3 * body.length))).status, 503);
      assert.equal(exports, 2);
    });
  })));
});

test("a request whose sender goes away, as its body arrives or as it is forwarded, ends its forward, and gives back what it held once the forward has ended", async () => {
  const body = paddedRequest();
  const forwards: { aborted: boolean; ended: boolean }[] = [];
  const forwarder = {
    protocols: { traces: "http/protobuf" },
    export(_signal: Signal, _encoding: OtlpEncoding, _body: Buffer, options?: { signal?: AbortSignal }): Promise<ForwardResult> {
      const forward = { aborted: false, ended: false };
      forwards.push(forward);
      // The first held until it is aborted, then ending a moment after, as a request being sent does; any after it taken at once.
      if (forwards.length > 1) return Promise.resolve({ ok: true });
      return new Promise((resolve) => {
        options?.signal?.addEventListener("abort", () => {
          forward.aborted = true;
          setTimeout(() => {
            forward.ended = true;
            resolve({ ok: false, error: "aborted", retryable: true, retryAfterMs: null });
          }, 300);
        });
      });
    },
  } satisfies Pick<OtlpForwarder, "protocols" | "export">;
  await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    // A request read and stamped needs all it may hold.
    const all = 2.2 * body.length;
    const { url, post, receiver } = yield* receiving(forwarder, { maxHeldBytes: all, maxSessionHeldBytes: all });
    yield* Effect.promise(async () => {
      // Its sender gone with half its body sent.
      const { hostname, port, pathname } = new URL(url);
      const partial = httpRequest({ hostname, port, path: pathname, method: "POST", headers: { "content-type": "application/x-protobuf", authorization: "Bearer fs-abc123.good", "content-length": body.length } });
      partial.on("error", () => {});
      partial.write(body.subarray(0, body.length / 2));
      await until(() => receiver.held().bytes > 0, "the half sent was never held");
      partial.destroy();
      await until(() => receiver.held().bytes === 0, "the half sent was never given back");
      assert.deepEqual([...receiver.held().sessions], []);

      // Its sender gone as it is forwarded: the forward ended, and what it held given back only once it has.
      const sender = new AbortController();
      const forwarded = post(body, { signal: sender.signal }).catch(() => null);
      await until(() => forwards.length === 1, "the request was never forwarded");
      const holding = receiver.held().bytes;
      assert.ok(holding > body.length, String(holding));
      sender.abort();
      await forwarded;
      await until(() => forwards[0]?.aborted === true, "the forward was never aborted");
      assert.deepEqual([forwards[0]?.ended, receiver.held().bytes], [false, holding]);
      await until(() => receiver.held().bytes === 0, "what the forward held was never given back");
      assert.equal(forwards[0]?.ended, true);

      // All it may hold given back: a request that needs all of it is taken.
      assert.equal((await post(body)).status, 200);
      assert.deepEqual([receiver.held().bytes, forwards.length], [0, 2]);
    });
  })));
});

test("one session's requests hold at most a share of what the receiver may hold, so they leave room for another session's", async () => {
  const body = paddedRequest();
  let release = () => {};
  const held = new Promise<void>((resolve) => void (release = resolve));
  let exports = 0;
  const forwarder = {
    protocols: { traces: "http/protobuf" },
    async export(): Promise<ForwardResult> {
      exports += 1;
      // The first held until released, as by a collector slow to answer.
      if (exports === 1) await held;
      return { ok: true };
    },
  } satisfies Pick<OtlpForwarder, "protocols" | "export">;
  await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    // A session's share, a quarter, holds one request read and stamped, not a second read beside it.
    const { post, receiver } = yield* receiving(forwarder, { maxHeldBytes: 10 * body.length });
    yield* Effect.promise(async () => {
      const first = post(body);
      try {
        await until(() => exports > 0, "the first request never reached the collector");
        const again = await post(body);
        assert.deepEqual([again.status, again.headers.get("retry-after")], [503, "1"]);
        assert.deepEqual([...receiver.held().sessions.keys()], ["fs-abc123"]);
        assert.equal((await post(body, { token: "fs-def456.good" })).status, 200);
      } finally {
        release();
      }
      assert.equal((await first).status, 200);
      assert.equal(receiver.held().bytes, 0);
    });
  })));
});

test("a session's telemetry the collector does not take, down longer than the forwarder's retries, is never answered as taken to bayma's exporter, and is once the collector takes it", async () => {
  // The collector's port, nothing listening on it until it comes up.
  const probe = createServer();
  await new Promise<void>((resolve) => probe.listen(0, "127.0.0.1", resolve));
  const port = (probe.address() as AddressInfo).port;
  await new Promise((resolve) => probe.close(resolve));
  const taken: Buffer[] = [];
  const collector = createServer((request, response) => {
    const chunks: Buffer[] = [];
    request.on("data", (chunk: Buffer) => chunks.push(chunk));
    request.on("end", () => {
      taken.push(Buffer.concat(chunks));
      response.writeHead(200, { "content-type": "application/x-protobuf" }).end();
    });
  });
  const forwarder = createOtlpForwarder({ OTEL_EXPORTER_OTLP_ENDPOINT: `http://127.0.0.1:${port}` });
  try {
    await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
      const { url } = yield* receiving(forwarder);
      // As bayma exports, with its exporter's own retries and timeout.
      const exporter = new OTLPTraceExporter({ url, headers: { authorization: "Bearer fs-abc123.good" } });
      const exported = (name: string) => new Promise<ExportResult>((resolve) => exporter.export(spansNamed(name), resolve));
      yield* Effect.promise(async () => {
        const down = await exported("while-down");
        assert.equal(down.code, ExportResultCode.FAILED);
        assert.equal(taken.length, 0);
        await new Promise<void>((resolve) => collector.listen(port, "127.0.0.1", resolve));
        const up = await exported("once-up");
        assert.equal(up.code, ExportResultCode.SUCCESS);
        assert.deepEqual(taken.map((body) => [/once-up/u.test(body.toString("latin1")), /while-down/u.test(body.toString("latin1"))]), [[true, false]]);
      });
      yield* Effect.promise(() => exporter.shutdown());
    })));
  } finally {
    forwarder.close();
    await new Promise((resolve) => collector.close(resolve));
  }
});
