/**
 * The node agent (src/node-agent/): its checkpoints, committed to a stand-in bucket and
 * forgotten when they fail, its copies, its prepared snapshots, its cache, its resumes of
 * restored sandboxes once kube-router's rules for their pods are in force, its abandons of
 * restores alasio refused, killing processes the test starts as runsc would, its API, as
 * alasio's client (src/sandbox/node-agents.ts) calls it too, and its process, with runsc a
 * stand-in of its own (./support/fake-runsc.ts) and the node's commands answered from
 * fixtures (./support/kube-router-rules.ts).
 */
import assert from "node:assert/strict";
import { type ChildProcess, execFileSync, spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { request as httpRequest } from "node:http";
import { closeSync, constants, type PathLike, existsSync, mkdirSync, mkdtempSync, openSync, readdirSync, readFileSync, readlinkSync, renameSync, rmSync, statSync, symlinkSync, truncateSync, utimesSync, writeFileSync, writeSync } from "node:fs";
import { createRequire, syncBuiltinESMExports } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import type { V1Pod } from "@kubernetes/client-node";
import { Cause, ConfigProvider, Effect, Exit, Fiber, Layer, Option, Schedule, Scope, Stream } from "effect";
import { TestClock } from "effect/testing";

// First: alasio's modules create their metrics as they load.
import { added, collected } from "./support/metrics.ts";
import { KubeClient } from "../src/kube/client.ts";
import { SNAPSHOT_ID } from "../src/kube/sandbox-shape.ts";
import {
  type AbandonRequest,
  type CheckpointRequest,
  COLLECT_GRACE_MS,
  compactionDue,
  type Killed,
  type Layer as Image,
  makeNodeAgent,
  type Manifest,
  MAX_CHAIN_DEPTH,
  memoryLimitOf,
  type NodeAgent,
  PoliciesNotInForce,
  type ResumeRequest,
  SnapshotOutlived,
  STATE_FILE_BYTES,
  STATE_READ_TIMEOUT_MS,
} from "../src/node-agent/agent.ts";
import { makeSnapshotCache, RESERVE_BYTES, type SnapshotCache, type Spared } from "../src/node-agent/cache.ts";
import { type HostCommands, type HostRun, makeHostCommands, NODE_PATH } from "../src/node-agent/host.ts";
import { egressGap } from "../src/node-agent/kube-router.ts";
import { makeRunsc, type Runsc, runscFlags } from "../src/node-agent/runsc.ts";
import { makeS3Bucket, sigV4Authorization } from "../src/node-agent/s3.ts";
import { makeStoreProxy, type StoreProxy } from "../src/node-agent/store-proxy.ts";
import { NODE_PINS } from "../cli/src/cluster/node.ts";
import { NODE_AGENT_PORT, restoresFromBucket, STORE_PROXY_PORT } from "../src/node-agent/names.ts";
import { makeOperations } from "../src/node-agent/operations.ts";
import { serveNodeAgent } from "../src/node-agent/server.ts";
import type { Workspaces } from "../src/node-agent/workspaces.ts";
import { workspacesNode } from "./support/node-workspaces.ts";
import { makeNodeAgents } from "../src/sandbox/node-agents.ts";
import { type FakeReleases, fakeReleases, POD_UID } from "./support/fake-runsc.ts";
import { type FakeS3, startFakeS3 } from "./support/fake-s3.ts";
import { clusterDns, filterTable, sessionPod } from "./support/kube-router-rules.ts";

/** node:fs as CommonJS has it, whose exports node:fs's ESM ones are synced to (syncBuiltinESMExports). */
const builtinFs = createRequire(import.meta.url)("node:fs") as typeof import("node:fs");

const RELEASE = "20260921.0-eaucoin.8";
const CONTAINER = "0123456789abcdef".repeat(4);
const OTHER_CONTAINER = "fedcba9876543210".repeat(4);
const HOOK = ["node", "/opt/bayma/bayma.js", "sandbox-hook", "--state-dir", "/home/agent/.bayma"];
const POD_TEMPLATE = { metadata: { labels: { "alasio.dev/sandbox": "fs-abc" } }, spec: { containers: [{ name: "bayma", image: "bayma@sha256:00" }] } };
const RUNSC_ROOT = "/run/containerd/runsc/k8s.io";
const CREDENTIALS = { accessKeyId: "snapshots0123", secretAccessKey: "a secret" };

const request = (workspace: string, containerId = CONTAINER): CheckpointRequest => ({
  environment: "main",
  workspace,
  namespace: SESSIONS,
  pod: POD,
  containerId,
  hook: HOOK,
  podTemplate: POD_TEMPLATE,
});

/** bayma's state directory, on the volume mounted at /home/agent, where a live checkpoint saves it. */
const STATE_DIR = "/home/agent/.bayma";

/** A request to checkpoint the workspace's sandbox live. */
const live = (workspace: string, containerId = CONTAINER): CheckpointRequest => ({ ...request(workspace, containerId), live: { stateDir: STATE_DIR } });

/** What bayma's hook says a save caught, in last-save.json. */
const CAUGHT = { generation: 5, running: [{ sessionId: "sess_1", execId: "exec_1" }] };

const SESSIONS = "alasio-sessions";
const POD = "fs-abc123";

const sha256 = (data: string | Buffer): string => createHash("sha256").update(data).digest("hex");

const POD_IP = "10.42.0.15";
const POD_IPV6 = "fd00:42::f";

/** The network policies that select a session's pod, as sessionPod's rules run it through them. */
const SESSION_POLICIES = ["default-deny", "bayma-from-alasio", "telemetry-to-alasio"];

/** A request to resume fs-abc's sandbox, restored from `snapshot`. */
const resumeRequest = (snapshot: string, podIPs: readonly string[] = [POD_IP], containerIds: readonly string[] = [CONTAINER, OTHER_CONTAINER]): ResumeRequest => ({
  environment: "main",
  workspace: "fs-abc",
  namespace: SESSIONS,
  pod: POD,
  podIPs,
  containerIds,
  policies: SESSION_POLICIES,
  snapshot,
});

/** The id of the snapshot of fs-abc `released` puts the manifest of in the bucket. */
const RELEASED = "20261013T000000000Z-0000000a";
/** How long a resumed restore of the test's may read the bucket through an agent. */
const BACKGROUND_READ_MS = 30 * 60_000;

/**
 * Puts in the bucket the manifest of a snapshot of fs-abc a release took, RELEASED, as one
 * the sandbox a resume names was restored from: its id.
 */
const released = (s3: FakeS3): string => {
  const manifest: Manifest = {
    environment: "main",
    workspace: "fs-abc",
    snapshot: RELEASED,
    release: RELEASE,
    cpuFeatures: [],
    podTemplate: POD_TEMPLATE,
    layers: [{ digest: "a".repeat(64), files: [] }],
    bytes: 0,
    takenAt: "2026-10-13T00:00:00.000Z",
    pauseMs: 0,
    uploadMs: 0,
  };
  s3.objects.set(`main/fs-abc/manifests/${RELEASED}.json`, Buffer.from(JSON.stringify(manifest)));
  return RELEASED;
};

/** What conntrack says where it deleted no connection, as it exits 1. */
const NO_CONNECTIONS: HostRun = { exitCode: 1, stdout: "", stderr: "conntrack v1.4.8 (conntrack-tools): 0 flow entries have been deleted.\n" };

/**
 * Commands run on a node of the test's, answered as the node would: its filter tables
 * (kube-router's rules for the pod POD at POD_IP and POD_IPV6 in them unless the test
 * sets others), conntrack deleting no connection, and what `answer` gives in place of
 * either; what was run, in order.
 */
interface FakeHost {
  readonly host: HostCommands;
  readonly ran: string[][];
  ipv4: string;
  ipv6: string;
  answer: (command: readonly string[]) => HostRun | null;
}

function fakeHost(): FakeHost {
  const fake: FakeHost = {
    ran: [],
    ipv4: filterTable([clusterDns("10.42.0.3"), sessionPod(POD, POD_IP)]),
    ipv6: filterTable([clusterDns("fd00:42::3"), sessionPod(POD, POD_IPV6)], { family: 6 }),
    answer: () => null,
    host: {
      run: (command) =>
        Effect.sync(() => {
          fake.ran.push([...command]);
          const [program] = command;
          return fake.answer(command) ??
            (program === "conntrack" ? NO_CONNECTIONS : { exitCode: 0, stdout: program === "ip6tables-save" ? fake.ipv6 : fake.ipv4, stderr: "" });
        }),
    },
  };
  return fake;
}

/**
 * A node: its releases, its cache, the commands run on it, and its agent on the shared
 * bucket, which sees the node's filesystem as the test's own; and another agent of it, as
 * one started again.
 */
interface Node {
  readonly releases: FakeReleases;
  readonly cacheDir: string;
  readonly cache: SnapshotCache;
  readonly host: FakeHost;
  /** The agent's proxy of the bucket, which its node's restores read through. */
  readonly storeProxy: StoreProxy;
  readonly agent: NodeAgent;
  readonly restarted: Effect.Effect<NodeAgent, never, Scope.Scope>;
}

/** What a node of the test's has: its releases, its cache's budget, and the runsc its agent runs, the node's as `runsc` changes it. */
interface NodeOptions {
  readonly releases?: readonly string[];
  readonly budget?: number;
  readonly runsc?: (runsc: Runsc, releases: FakeReleases) => Runsc;
  /** Whether its agent's proxy listens on a socket too, as on a node. */
  readonly socket?: boolean;
  /** How long its proxy takes a resumed restore told no bytes of pages to be done once its reads are quiet. */
  readonly quietMs?: number;
  /** How long its runsc may take to describe an image in the bucket. */
  readonly inspectTimeoutMs?: number;
}

/** A bucket and a directory of the test's, and nodes made on them, removed after `body`. */
async function withBucket(body: (context: { readonly s3: FakeS3; readonly node: (name: string, options?: NodeOptions) => Effect.Effect<Node, never, Scope.Scope> }) => Effect.Effect<void, unknown, Scope.Scope>): Promise<void> {
  const root = mkdtempSync(join(tmpdir(), "alasio-node-agent-"));
  const s3 = await startFakeS3({ bucket: "snapshots", region: "us-east-1", ...CREDENTIALS });
  const bucket = makeS3Bucket({ endpoint: s3.endpoint, region: "us-east-1", bucket: "snapshots", ...CREDENTIALS });
  const node = (name: string, { releases: installed = [RELEASE], budget = 1024 ** 3, runsc: changed = (runsc) => runsc, socket = false, quietMs, inspectTimeoutMs }: NodeOptions = {}) =>
    Effect.gen(function*() {
      const releases = fakeReleases(join(root, name), installed);
      const cacheDir = join(root, name, "cache");
      const cache = makeSnapshotCache({ root: cacheDir, budget, graceMs: 0 });
      const runsc = changed(makeRunsc({ releasesDir: releases.releasesDir, containerdDir: releases.containerdDir, root: RUNSC_ROOT, procRoot: releases.procRoot }), releases);
      // The pod's containers run under the node's first release.
      const [running] = installed;
      if (running !== undefined) for (const id of [CONTAINER, OTHER_CONTAINER]) releases.runs(id, running, SESSIONS, POD);
      const host = fakeHost();
      // Its socket in /tmp, short enough for a socket's path, whatever TMPDIR is.
      const sockets = socket ? mkdtempSync(join("/tmp", "alasio-agent-")) : null;
      if (sockets !== null) yield* Effect.addFinalizer(() => Effect.sync(() => rmSync(sockets, { recursive: true, force: true })));
      const storeProxy = yield* makeStoreProxy({ store: { endpoint: s3.endpoint, region: "us-east-1", bucket: "snapshots", ...CREDENTIALS }, host: "127.0.0.1", port: 0, ...(sockets === null ? {} : { socket: join(sockets, "store.sock") }), ...(quietMs === undefined ? {} : { quietMs }) }).pipe(Effect.orDie);
      const restarted = makeNodeAgent({ node: name, nodeRoot: "/", runsc, cache, bucket, storeProxy, host: host.host, restoresFromBucketSince: NODE_PINS.gvisor.restoresFromBucketSince, backgroundReadMs: BACKGROUND_READ_MS, ...(inspectTimeoutMs === undefined ? {} : { inspectTimeoutMs }) });
      return { releases, cacheDir, cache, host, storeProxy, agent: yield* restarted, restarted };
    });
  try {
    await Effect.runPromise(Effect.scoped(body({ s3, node })));
  } finally {
    await s3.close();
    rmSync(root, { recursive: true, force: true });
  }
}

/** Whether the cache of a node holds anything of a workspace. */
const cachesAny = (node: Node, workspace: string): boolean => existsSync(join(node.cacheDir, "main", workspace));

/** The images the cache of a node holds of a workspace, by digest, whole or not. */
const imagesOf = (node: Node, workspace: string): string[] => {
  const directory = join(node.cacheDir, "main", workspace, "layers");
  return existsSync(directory) ? readdirSync(directory).sort() : [];
};

/** Resolves once the node's cache holds every image of `manifest`, stamped as checked, as the agent fetches them after a restore from the bucket began. */
const fetched = (node: Node, manifest: Manifest) =>
  Effect.sync(() => {
    const directory = join(node.cacheDir, manifest.environment, manifest.workspace);
    const held = existsSync(join(directory, "layers")) ? readdirSync(join(directory, "layers")) : [];
    return manifest.layers.every(({ digest }) => held.includes(digest) && existsSync(join(directory, "stamps", `${digest}.json`)));
  }).pipe(
    Effect.repeat({ schedule: Schedule.spaced("10 millis"), until: Boolean }),
    Effect.timeout("10 seconds"),
  );

/** What the checkpoint gofer's options file of a snapshot prepared from the bucket holds, and its mode. */
const goferOptionsOf = (imagePath: string) => ({
  files: readdirSync(imagePath),
  mode: (statSync(join(imagePath, "s3_opts.json")).mode & 0o777).toString(8),
  options: JSON.parse(readFileSync(join(imagePath, "s3_opts.json"), "utf8")) as Record<string, unknown>,
});

/** The files of an image the stand-in's runsc writes, in order. */
const IMAGE_FILES = ["checkpoint.img", "pages.img", "pages_meta.img"];

const md5 = (data: Buffer): string => createHash("md5").update(data).digest("hex");

/** The files of the image in `directory`, as a manifest says them once the stand-in bucket holds them. */
const filesOf = (directory: string) =>
  IMAGE_FILES.map((path) => {
    const data = readFileSync(join(directory, path));
    return { path, bytes: data.length, sha256: sha256(data), etag: `"${md5(data)}"` };
  });

/**
 * What `effect` comes to, and the files under `directory` the process opened to read as a
 * stream as it ran, as the agent reads an image's files to hash them: node:fs's
 * createReadStream, which the agent's modules import, wrapped meanwhile.
 */
const readsUnder = <A, E>(directory: string, effect: Effect.Effect<A, E>): Effect.Effect<string[], E> =>
  Effect.acquireUseRelease(
    Effect.sync(() => {
      const opened: string[] = [];
      const original = builtinFs.createReadStream;
      builtinFs.createReadStream = ((path: PathLike, ...rest: never[]) => {
        if (String(path).startsWith(`${directory}/`)) opened.push(String(path));
        return original(path, ...rest);
      }) as typeof original;
      syncBuiltinESMExports();
      return { opened, original };
    }),
    ({ opened }) => effect.pipe(Effect.as(opened)),
    ({ original }) =>
      Effect.sync(() => {
        builtinFs.createReadStream = original;
        syncBuiltinESMExports();
      }),
  );

/** The bucket's keys under a prefix. */
const keysOf = (s3: FakeS3, prefix: string): string[] => [...s3.objects.keys()].filter((key) => key.startsWith(prefix)).sort();

/** The message of what an exit failed with. */
const failure = (exit: Exit.Exit<unknown, { readonly message: string }>): string => {
  assert.ok(Exit.isFailure(exit));
  const error = exit.cause.reasons.find((reason) => reason._tag === "Fail");
  assert.ok(error && error._tag === "Fail");
  return error.error.message;
};

test("a runtime's options file gives runsc its shim's flags", () => {
  assert.deepEqual(
    runscFlags([
      "restore_direct = true",
      "# runsc's flags",
      "[runsc_config]",
      '  systemd-cgroup = "true"',
      '  "restore-validate-files" = "all" # every file',
      "",
      "[other]",
      '  debug = "true"',
    ].join("\n")),
    ["--systemd-cgroup=true", "--restore-validate-files=all"],
  );
  assert.throws(() => runscFlags("[runsc_config]\n  debug = true\n"), /\[runsc_config\] holds what is not a string flag: debug = true/u);
});

test("a command is run in a container by its release's runsc's exec, its failure said", async () => {
  const root = mkdtempSync(join(tmpdir(), "alasio-runsc-exec-"));
  try {
    const releases = fakeReleases(root, [RELEASE]);
    const runsc = makeRunsc({ releasesDir: releases.releasesDir, containerdDir: releases.containerdDir, root: RUNSC_ROOT, procRoot: releases.procRoot });
    await Effect.runPromise(runsc.exec(RELEASE, "container-0", ["bayma", "sandbox-hook", "stop"], 10_000));
    assert.deepEqual(releases.calls(RELEASE).at(-1)?.filter((arg) => !arg.startsWith("--")), ["exec", "container-0", "bayma", "sandbox-hook", "stop"]);
    writeFileSync(join(releases.dirOf(RELEASE), "fail-exec"), "");
    const failed = await Effect.runPromise(Effect.flip(runsc.exec(RELEASE, "container-0", ["bayma", "sandbox-hook", "stop"], 10_000)));
    assert.match(failed.message, /the hook exited 1/u);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("a checkpoint saves the sandbox with its release's runsc, uploads its image under its digest, checks the bucket holds it as put, then writes its manifest and commits it", () =>
  withBucket(({ s3, node }) =>
    Effect.gen(function*() {
      const here = yield* node("node-1");
      const first = yield* here.agent.checkpoint(request("fs-abc"));
      const staged = join(here.cacheDir, "main", "fs-abc", "layers", `.${first.snapshot}`);
      assert.deepEqual(here.releases.calls(RELEASE), [
        [
          `--root=${RUNSC_ROOT}`,
          "--systemd-cgroup=true",
          "checkpoint",
          `--image-path=${staged}`,
          "--compression=none",
          "--save-restore-exec-argv=node /opt/bayma/bayma.js sandbox-hook --state-dir /home/agent/.bayma",
          "--save-restore-exec-timeout=30s",
          CONTAINER,
        ],
        [`--root=${RUNSC_ROOT}`, "--systemd-cgroup=true", "image", "inspect", "--json", staged],
        [`--root=${RUNSC_ROOT}`, "--systemd-cgroup=true", "image", "layers", staged],
      ]);
      const [image] = first.layers;
      assert.ok(image);
      const directory = join(here.cacheDir, "main", "fs-abc", "layers", image.digest);
      // Its digest is its pages metadata's, as runsc's is.
      assert.equal(image.digest, sha256(readFileSync(join(directory, "pages_meta.img"))));
      const { pauseMs, uploadMs, takenAt, snapshot, ...rest } = first;
      assert.match(snapshot, new RegExp(`^${SNAPSHOT_ID}$`, "u"));
      assert.ok(pauseMs >= 0 && uploadMs >= 0 && Date.parse(takenAt) <= Date.now());
      const files = filesOf(directory);
      assert.deepEqual(rest, {
        environment: "main",
        workspace: "fs-abc",
        release: RELEASE,
        cpuFeatures: ["sse", "avx"],
        podTemplate: POD_TEMPLATE,
        layers: [{ digest: image.digest, files }],
        bytes: files.reduce((sum, { bytes }) => sum + bytes, 0),
      });
      const prefix = `main/fs-abc/layers/${image.digest}`;
      // Its image's files, each then looked at, then its manifest, then the commit.
      assert.deepEqual(s3.requests, [
        ...IMAGE_FILES.map((path) => `PUT ${prefix}/${path}`),
        ...IMAGE_FILES.map((path) => `HEAD ${prefix}/${path}`),
        `PUT main/fs-abc/manifests/${snapshot}.json`,
        "PUT main/fs-abc/current.json",
      ]);
      assert.deepEqual(JSON.parse(s3.objects.get("main/fs-abc/current.json")?.toString() ?? ""), first);
      assert.deepEqual(JSON.parse(s3.objects.get(`main/fs-abc/manifests/${snapshot}.json`)?.toString() ?? ""), first);
      assert.deepEqual(yield* here.agent.current({ environment: "main", workspace: "fs-abc" }), first);
      // The cache holds its image, and its manifest beside it.
      assert.deepEqual(imagesOf(here, "fs-abc"), [image.digest]);
      assert.deepEqual(JSON.parse(readFileSync(join(here.cacheDir, "main", "fs-abc", "manifest.json"), "utf8")), first);

      // Another workspace's snapshot is its own; the next of a sandbox restored from none is whole too, and current.
      const other = yield* here.agent.checkpoint(request("fs-other"));
      const second = yield* here.agent.checkpoint(request("fs-abc", OTHER_CONTAINER));
      assert.equal(second.layers.length, 1);
      assert.notEqual(second.layers[0]?.digest, image.digest);
      assert.deepEqual(yield* here.agent.current({ environment: "main", workspace: "fs-abc" }), second);
      assert.deepEqual(imagesOf(here, "fs-abc"), [image.digest, second.layers[0]?.digest].sort());
      assert.deepEqual(yield* here.agent.current({ environment: "main", workspace: "fs-other" }), other);
    })
  ));

test("the checkpoint of a sandbox restored from a snapshot, which tracks its writes, is a delta of that snapshot's image, which it refers to; whole otherwise", () =>
  withBucket(({ s3, node }) =>
    Effect.gen(function*() {
      const workspace = { environment: "main", workspace: "fs-abc" };
      const taker = yield* node("node-1");
      const base = yield* taker.agent.checkpoint(request("fs-abc"));
      const [baseImage] = base.layers;
      assert.ok(baseImage);
      // Restored on another node, its sandbox resumed there.
      const there = yield* node("node-2");
      const restored = () =>
        Effect.gen(function*() {
          const { manifest } = yield* there.agent.prepare(workspace);
          yield* there.agent.spend(workspace, manifest.snapshot);
          yield* there.agent.resume(resumeRequest(manifest.snapshot));
        });
      yield* restored();
      // Read from the bucket as it was restored, its image fetched into the cache meanwhile.
      yield* fetched(there, base);
      const asked = s3.requests.length;
      const delta = yield* there.agent.checkpoint(request("fs-abc"));
      const saved = there.releases.calls(RELEASE).findLast((args) => args.includes("checkpoint"));
      assert.ok(saved?.includes(`--parent-image-path=${join(there.cacheDir, "main", "fs-abc", "layers", baseImage.digest)}`));
      const [own, ...layers] = delta.layers;
      assert.ok(own);
      assert.deepEqual(layers, base.layers);
      assert.equal(delta.bytes, own.files.reduce((sum, { bytes }) => sum + bytes, 0) + base.bytes);
      // Only its own image goes up; the one it refers to is looked at, as it was put.
      assert.deepEqual(s3.requests.slice(asked), [
        ...IMAGE_FILES.map((path) => `PUT main/fs-abc/layers/${own.digest}/${path}`),
        ...IMAGE_FILES.map((path) => `HEAD main/fs-abc/layers/${own.digest}/${path}`),
        ...IMAGE_FILES.map((path) => `HEAD main/fs-abc/layers/${baseImage.digest}/${path}`),
        `PUT main/fs-abc/manifests/${delta.snapshot}.json`,
        "PUT main/fs-abc/current.json",
      ]);
      // Restored on a third node, it is read from the bucket, its own image and its layers each under its digest.
      const third = yield* node("node-3");
      const remote = yield* third.agent.prepare(workspace);
      assert.equal(remote.source, "bucket");
      const { files, mode, options } = goferOptionsOf(remote.imagePath);
      assert.deepEqual([files, mode], [["s3_opts.json"], "600"]);
      // Through the node agent's proxy of the bucket, by a token of the restore's own: none of the bucket's credentials.
      const { credentials, ...where } = options as { credentials: { access_key_id: string; secret_access_key: string } };
      assert.deepEqual(where, {
        endpoint: third.storeProxy.endpoint,
        region: "us-east-1",
        bucket: "snapshots",
        object_prefix: `main/fs-abc/layers/${own.digest}/`,
        layers_prefix: "main/fs-abc/layers/",
        addressing: "path",
      });
      assert.match(credentials.access_key_id, /^ALASIO[0-9A-F]{20}$/u);
      assert.equal(Buffer.from(credentials.secret_access_key, "base64url").length, 32);
      assert.ok(!JSON.stringify(options).includes(CREDENTIALS.secretAccessKey) && !JSON.stringify(options).includes(CREDENTIALS.accessKeyId));
      // Restored there again once its images are fetched, from the cache: both images, its own, and the one it refers to where runsc finds it.
      yield* fetched(third, delta);
      const prepared = yield* third.agent.prepare(workspace);
      assert.equal(prepared.source, "cache");
      assert.deepEqual(readdirSync(prepared.imagePath).sort(), [...IMAGE_FILES, "layers"].sort());
      assert.equal(readlinkSync(join(prepared.imagePath, "layers", baseImage.digest)), join("..", "..", "layers", baseImage.digest));
      assert.deepEqual(filesOf(join(prepared.imagePath, "layers", baseImage.digest)), baseImage.files);
      assert.deepEqual(filesOf(prepared.imagePath), own.files);

      // A sandbox that does not track its writes, as one started before its runtime's options said to, is saved whole.
      there.releases.runs(CONTAINER, RELEASE, SESSIONS, POD, false);
      yield* restored();
      assert.equal((yield* there.agent.checkpoint(request("fs-abc"))).layers.length, 1);
      // So is one whose base this node's cache no longer holds.
      there.releases.runs(CONTAINER, RELEASE, SESSIONS, POD);
      yield* restored();
      rmSync(join(there.cacheDir, "main", "fs-abc", "layers"), { recursive: true });
      assert.equal((yield* there.agent.checkpoint(request("fs-abc"))).layers.length, 1);
      // And one of a sandbox restored from none.
      assert.equal((yield* there.agent.checkpoint(request("fs-abc", OTHER_CONTAINER))).layers.length, 1);
    })
  ));

test("a cached image that is not what its manifest says is read again from the bucket, it alone, the snapshot's others kept as they are", () =>
  withBucket(({ s3, node }) =>
    Effect.gen(function*() {
      const workspace = { environment: "main", workspace: "fs-abc" };
      const taker = yield* node("node-1");
      const base = yield* taker.agent.checkpoint(request("fs-abc"));
      const [baseImage] = base.layers;
      assert.ok(baseImage);
      // Restored on another node, its next snapshot there a delta of it: two images, both in that node's cache.
      const there = yield* node("node-2");
      const { manifest } = yield* there.agent.prepare(workspace);
      yield* there.agent.spend(workspace, manifest.snapshot);
      yield* there.agent.resume(resumeRequest(manifest.snapshot));
      // A delta once its base is fetched into the cache, as a checkpoint waits on no download.
      yield* fetched(there, base);
      const delta = yield* there.agent.checkpoint(request("fs-abc"));
      const [own] = delta.layers;
      assert.ok(own);
      assert.deepEqual(delta.layers.slice(1), base.layers);
      yield* fetched(there, delta);
      // Its base, looked at after its own image, changed on the disk: read from the bucket as
      // the restore goes, and the base alone fetched into the cache again.
      writeFileSync(join(there.cacheDir, "main", "fs-abc", "layers", baseImage.digest, "pages_meta.img"), "changed on the disk");
      const asked = s3.requests.length;
      const prepared = yield* there.agent.prepare(workspace);
      assert.deepEqual([prepared.manifest, prepared.source], [delta, "bucket"]);
      yield* fetched(there, delta);
      assert.deepEqual(s3.requests.slice(asked).filter((each) => each.startsWith("GET main/fs-abc/layers/")), baseImage.files.map(({ path }) => `GET main/fs-abc/layers/${baseImage.digest}/${path}`));
      const layers = join(there.cacheDir, "main", "fs-abc", "layers");
      assert.deepEqual([filesOf(join(layers, own.digest)), filesOf(join(layers, baseImage.digest))], [own.files, baseImage.files]);
      // The delta was of the base it was restored from.
      assert.deepEqual(manifest, base);
    })
  ));

test("a prepared snapshot of a release before the bucket's is verified by runsc, its images read again from the bucket where those the cache held fail it, and refused where those fail it too", () =>
  withBucket(({ s3, node }) =>
    Effect.gen(function*() {
      const workspace = { environment: "main", workspace: "fs-abc" };
      // Fetched into the cache first, as a release before restoresFromBucketSince is.
      const OLDER = "20260921.0-eaucoin.6";
      const taker = yield* node("node-1", { releases: [OLDER] });
      const base = yield* taker.agent.checkpoint(request("fs-abc"));
      const [baseImage] = base.layers;
      assert.ok(baseImage);
      const there = yield* node("node-2", { releases: [OLDER] });
      const { manifest } = yield* there.agent.prepare(workspace);
      yield* there.agent.spend(workspace, manifest.snapshot);
      yield* there.agent.resume(resumeRequest(manifest.snapshot));
      const delta = yield* there.agent.checkpoint(request("fs-abc"));
      const [own] = delta.layers;
      assert.ok(own);
      const verified = () => there.releases.calls(OLDER).filter((args) => args.includes("verify"));
      // Prepared from the cache, verified there.
      const prepared = yield* there.agent.prepare(workspace);
      assert.deepEqual(verified().at(-1)?.slice(-4), ["image", "verify", "--host", prepared.imagePath]);
      // Its base's pages metadata changed in place as its stamp does not see, its size and times kept, as a disk that rots changes it.
      const meta = join(there.cacheDir, "main", "fs-abc", "layers", baseImage.digest, "pages_meta.img");
      const written = readFileSync(meta, "utf8");
      writeFileSync(meta, written.replace(/"state":"(.)/u, (_, first: string) => `"state":"${first === "0" ? "1" : "0"}`));
      yield* there.cache.stamp("main", "fs-abc", baseImage.digest, baseImage.files);
      const asked = s3.requests.length;
      assert.deepEqual(yield* there.agent.prepare(workspace), { ...prepared, source: "bucket" });
      const downloaded = s3.requests.slice(asked).filter((each) => each.startsWith("GET main/fs-abc/layers/"));
      assert.deepEqual(downloaded.toSorted(), [own, baseImage].flatMap(({ digest, files }) => files.map(({ path }) => `GET main/fs-abc/layers/${digest}/${path}`)).toSorted());
      assert.equal(readFileSync(meta, "utf8"), written);
      // What runsc does not verify, read from the bucket, is no snapshot to restore, and goes from the cache.
      writeFileSync(join(there.releases.dirOf(OLDER), "verify-status"), "3 Image of digest mismatch");
      assert.equal(failure(yield* Effect.exit(there.agent.prepare(workspace))), "the snapshot of main/fs-abc is corrupt: runsc image verify exited 3: Image of digest mismatch");
      assert.equal(cachesAny(there, "fs-abc"), false);
    })
  ));

/** An image of `bytes` bytes, in one file, as a manifest says it. */
const imageOf = (digit: string, bytes: number): Image => ({ digest: digit.repeat(64), files: [{ path: "pages.img", bytes, sha256: "0".repeat(64), etag: '"0"' }] });

test("a snapshot is compacted once its deltas together are as large as its base, one is more than half of it, or it reads from more than 16 images", () => {
  const base = imageOf("a", 4096);
  assert.equal(compactionDue([base]), null);
  assert.equal(compactionDue([imageOf("b", 512), base, imageOf("c", 512)]), null);
  // Its base is its largest image, wherever runsc lists it.
  assert.equal(compactionDue([imageOf("b", 2048), imageOf("c", 2048), base]), "its deltas are 4096 bytes together, as large as its base's 4096");
  assert.equal(compactionDue([base, imageOf("b", 2049)]), "a delta of 2049 bytes is more than half its base's 4096");
  assert.equal(compactionDue([base, imageOf("b", 2048)]), null);
  const deep = [base, ...Array.from({ length: MAX_CHAIN_DEPTH }, (_, index) => imageOf(index.toString(16), 1))];
  assert.equal(compactionDue(deep.slice(0, MAX_CHAIN_DEPTH)), null);
  assert.equal(compactionDue(deep), "it reads from 17 images, more than 16");
  // With an image to be added to it, as a delta about to be taken would be: it counts to its depth alone.
  assert.equal(compactionDue(deep.slice(0, MAX_CHAIN_DEPTH - 1), 1), null);
  assert.equal(compactionDue(deep.slice(0, MAX_CHAIN_DEPTH), 1), "it reads from 17 images, more than 16");
  assert.equal(compactionDue([base], 1), null);
  assert.equal(compactionDue([base, imageOf("b", 2048), imageOf("c", 2048)], 1), "its deltas are 4096 bytes together, as large as its base's 4096");
});

test("a snapshot taken live is taken whole where a delta of its parent would make a chain due to be compacted, so the chain of a sandbox saved live as long as it runs is bounded", () =>
  withBucket(({ node }) =>
    Effect.gen(function*() {
      const volume = mkdtempSync(join(tmpdir(), "alasio-volume-"));
      yield* Effect.addFinalizer(() => Effect.sync(() => rmSync(volume, { recursive: true, force: true })));
      mkdirSync(join(volume, ".bayma"));
      writeFileSync(join(volume, ".bayma", "last-save.json"), JSON.stringify({ schemaVersion: 1, ...CAUGHT }));
      const here = yield* node("node-1");
      here.releases.runs(CONTAINER, RELEASE, SESSIONS, POD, true, [{ destination: "/home/agent", source: volume }]);
      // Saved live every so often as its exec runs on, each save's delta of 512 bytes, its base 4096.
      const taken: Manifest[] = [];
      for (let count = 0; count < 12; count++) taken.push(yield* here.agent.checkpoint(live("fs-abc")));
      const cut = taken.findIndex(({ layers }, index) => index > 0 && layers.length === 1);
      assert.ok(cut > 1, `no snapshot was taken whole: ${taken.map(({ layers }) => layers.length).join(", ")}`);
      // Each a delta of the one before, until a delta of the last would make a chain due, as the one cut is.
      for (const [index, { layers }] of taken.slice(0, cut).entries()) assert.deepEqual(layers.slice(1), taken[index - 1]?.layers ?? []);
      assert.equal(compactionDue(taken[cut - 2]?.layers ?? [], 1), null);
      assert.notEqual(compactionDue(taken[cut - 1]?.layers ?? [], 1), null);
      // The one after is a delta of the one taken whole.
      assert.deepEqual(taken[cut + 1]?.layers.slice(1), taken[cut]?.layers);
      assert.ok(taken.every(({ layers }) => layers.length <= MAX_CHAIN_DEPTH));
    })
  ));

/** The workspace's current snapshot, once it is one compacted from `from`. */
const compactedFrom = (agent: NodeAgent, from: string) =>
  agent.current({ environment: "main", workspace: "fs-abc" }).pipe(
    Effect.repeat({ schedule: Schedule.spaced("10 millis"), until: (manifest) => manifest?.compactedFrom === from }),
    Effect.timeout("10 seconds"),
  );

test("a release's snapshot whose chain is due is compacted into one image, at the node's lowest priority, which becomes current and the next delta's base", () =>
  added("alasio.node_agent.compactions", () =>
    withBucket(({ s3, node }) =>
      Effect.gen(function*() {
        const workspace = { environment: "main", workspace: "fs-abc" };
        const taker = yield* node("node-1");
        yield* taker.agent.checkpoint(request("fs-abc"));
        const there = yield* node("node-2");
        const restored = () =>
          Effect.gen(function*() {
            const { manifest } = yield* there.agent.prepare(workspace);
            yield* there.agent.spend(workspace, manifest.snapshot);
            yield* there.agent.resume(resumeRequest(manifest.snapshot));
            yield* fetched(there, manifest);
          });
        yield* restored();
        // The sandbox wrote over as much as its base holds.
        writeFileSync(join(there.releases.dirOf(RELEASE), "delta-pages-bytes"), "4096");
        const delta = yield* there.agent.checkpoint(request("fs-abc"));
        assert.equal(delta.layers.length, 2);
        const compacted = yield* compactedFrom(there.agent, delta.snapshot);
        assert.ok(compacted);
        const [own] = delta.layers;
        assert.ok(own);
        const layersDir = join(there.cacheDir, "main", "fs-abc", "layers");
        const staged = join(layersDir, `.${compacted.snapshot}`);
        assert.deepEqual(there.releases.calls(RELEASE).find((args) => args.includes("compact")), [
          `--root=${RUNSC_ROOT}`,
          "--systemd-cgroup=true",
          "image",
          "compact",
          `--output=${staged}`,
          `--layer-path=${layersDir}`,
          "--working-set-first",
          join(layersDir, own.digest),
        ]);
        assert.equal(readFileSync(join(there.releases.dirOf(RELEASE), "compact-priority"), "utf8"), "19");
        // One image, of the delta's memory and its base's, in place of both; the same sandbox at the same moment.
        const [image] = compacted.layers;
        assert.ok(image);
        assert.equal(compacted.layers.length, 1);
        assert.equal(image.files.find(({ path }) => path === "pages.img")?.bytes, 8192);
        assert.ok(compacted.snapshot > delta.snapshot);
        const { snapshot: _snapshot, layers: _layers, bytes, uploadMs: _uploadMs, compactedFrom: from, ...same } = compacted;
        const { snapshot: _from, layers: _chain, bytes: _bytes, uploadMs: _taken, ...was } = delta;
        assert.deepEqual(same, was);
        assert.equal(from, delta.snapshot);
        assert.equal(bytes, image.files.reduce((sum, file) => sum + file.bytes, 0));
        assert.deepEqual(filesOf(join(layersDir, image.digest)), image.files);
        // Committed as a checkpoint commits: the delta's manifest kept beside it, which a restore that read it reads within the grace.
        assert.deepEqual(JSON.parse(s3.objects.get(`main/fs-abc/manifests/${compacted.snapshot}.json`)?.toString() ?? ""), compacted);
        assert.ok(s3.objects.has(`main/fs-abc/manifests/${delta.snapshot}.json`));
        assert.deepEqual(JSON.parse(readFileSync(join(there.cacheDir, "main", "fs-abc", "manifest.json"), "utf8")), compacted);
        assert.deepEqual(readdirSync(layersDir).filter((name) => name.startsWith(".")), []);
        // Restored elsewhere, it reads one image; the next delta of the sandbox restored is of that image.
        const third = yield* node("node-3");
        const prepared = yield* third.agent.prepare(workspace);
        assert.equal(goferOptionsOf(prepared.imagePath).options["object_prefix"], `main/fs-abc/layers/${image.digest}/`);
        writeFileSync(join(there.releases.dirOf(RELEASE), "delta-pages-bytes"), "512");
        yield* restored();
        const next = yield* there.agent.checkpoint(request("fs-abc"));
        assert.deepEqual(next.layers.slice(1).map(({ digest }) => digest), [image.digest]);
        assert.ok(there.releases.calls(RELEASE).findLast((args) => args.includes("checkpoint"))?.includes(`--parent-image-path=${join(layersDir, image.digest)}`));
        // Small, it is not compacted.
        yield* Effect.sleep("200 millis");
        assert.deepEqual(yield* there.agent.current(workspace), next);
      })
    )
  ).then((counted) => assert.deepEqual(counted, { "alasio.node_agent.compaction.outcome=compacted": 1 })));

test("a spend of a snapshot spends the one compacted from it, which a compaction committed as its restore ran, and a collection keeps what it holds while the compacted one is the latest", () =>
  withBucket(({ s3, node }) =>
    Effect.gen(function*() {
      const workspace = { environment: "main", workspace: "fs-abc" };
      const taker = yield* node("node-1");
      yield* taker.agent.checkpoint(request("fs-abc"));
      const there = yield* node("node-2");
      const { manifest } = yield* there.agent.prepare(workspace);
      yield* there.agent.spend(workspace, manifest.snapshot);
      yield* there.agent.resume(resumeRequest(manifest.snapshot));
      // Its images in the node's cache, so that its checkpoint is a delta of them, whose chain is due.
      yield* fetched(there, manifest);
      writeFileSync(join(there.releases.dirOf(RELEASE), "delta-pages-bytes"), "4096");
      const delta = yield* there.agent.checkpoint(request("fs-abc"));
      assert.equal(delta.layers.length, 2);
      // A restore reads the delta as current, and prepares it; meanwhile its compaction commits.
      const third = yield* node("node-3");
      const restoring = yield* third.agent.prepare(workspace);
      assert.equal(restoring.manifest.snapshot, delta.snapshot);
      const compacted = yield* compactedFrom(there.agent, delta.snapshot);
      assert.ok(compacted);
      // The restore's spend of the delta spends the snapshot compacted from it, the same, which a later restore would read stale.
      yield* third.agent.spend(workspace, delta.snapshot);
      assert.equal(yield* there.agent.current(workspace), null);
      // Its sandbox runs on from the delta, which its next delta refers to: kept by a collection however long ago it was superseded.
      const longAgo = Date.now() - COLLECT_GRACE_MS - 1000;
      for (const key of keysOf(s3, "main/fs-abc/")) s3.writtenAt.set(key, longAgo);
      const kept = yield* there.agent.collect(workspace);
      assert.deepEqual(kept, [...new Set([...delta.layers, ...compacted.layers].map(({ digest }) => digest))].sort());
      for (const { digest, files } of delta.layers) for (const { path } of files) assert.ok(s3.objects.has(`main/fs-abc/layers/${digest}/${path}`), `${digest}/${path} was collected`);
    })
  ));

test("a compaction the workspace moves on from meanwhile commits nothing, what it reads no collection discards meanwhile, one that fails changes nothing, and a live snapshot is never compacted", () =>
  added("alasio.node_agent.compactions", () =>
    withBucket(({ node }) =>
      Effect.gen(function*() {
        const workspace = { environment: "main", workspace: "fs-abc" };
        const here = yield* node("node-1");
        const runscDir = here.releases.dirOf(RELEASE);
        const chained = (take: CheckpointRequest = request("fs-abc")) =>
          Effect.gen(function*() {
            writeFileSync(join(runscDir, "delta-pages-bytes"), "512");
            yield* here.agent.checkpoint(request("fs-abc"));
            const { manifest } = yield* here.agent.prepare(workspace);
            yield* here.agent.spend(workspace, manifest.snapshot);
            yield* here.agent.resume(resumeRequest(manifest.snapshot));
            writeFileSync(join(runscDir, "delta-pages-bytes"), "4096");
            return yield* here.agent.checkpoint(take);
          });
        const layersDir = join(here.cacheDir, "main", "fs-abc", "layers");
        const compactions = () => here.releases.calls(RELEASE).filter((args) => args.includes("compact")).length;
        // Restored, and so spent, as it compacts: its compacted image commits nothing.
        writeFileSync(join(runscDir, "hold-compact"), "");
        const moved = yield* chained();
        yield* Effect.sync(compactions).pipe(Effect.repeat({ schedule: Schedule.spaced("10 millis"), until: (count) => count === 1 }));
        // What it reads and writes beside the workspace's lock is discarded by no collection meanwhile.
        const reading = imagesOf(here, "fs-abc");
        assert.equal(yield* here.agent.discard(workspace, []), false);
        assert.deepEqual(imagesOf(here, "fs-abc"), reading);
        yield* here.agent.spend(workspace, moved.snapshot);
        rmSync(join(runscDir, "hold-compact"));
        yield* Effect.sync(() => readdirSync(layersDir).some((name) => name.startsWith("."))).pipe(Effect.repeat({ schedule: Schedule.spaced("10 millis"), until: (staging) => !staging }));
        yield* Effect.sleep("200 millis");
        assert.equal(yield* here.agent.current(workspace), null);
        // One that fails leaves the snapshot as it was committed, and nothing it staged.
        writeFileSync(join(runscDir, "fail-compact"), "");
        const failed = yield* chained();
        yield* Effect.sync(compactions).pipe(Effect.repeat({ schedule: Schedule.spaced("10 millis"), until: (count) => count === 2 }));
        yield* Effect.sleep("200 millis");
        assert.deepEqual(yield* here.agent.current(workspace), failed);
        assert.deepEqual(readdirSync(layersDir).filter((name) => name.startsWith(".")), []);
        rmSync(join(runscDir, "fail-compact"));
        // Taken live, the sandbox running on from it, it is not compacted, however large its delta.
        const volume = mkdtempSync(join(tmpdir(), "alasio-volume-"));
        yield* Effect.addFinalizer(() => Effect.sync(() => rmSync(volume, { recursive: true, force: true })));
        mkdirSync(join(volume, ".bayma"));
        writeFileSync(join(volume, ".bayma", "last-save.json"), JSON.stringify({ schemaVersion: 1, ...CAUGHT }));
        here.releases.runs(CONTAINER, RELEASE, SESSIONS, POD, true, [{ destination: "/home/agent", source: volume }]);
        yield* here.agent.checkpoint(live("fs-abc"));
        const second = yield* here.agent.checkpoint(live("fs-abc"));
        assert.equal(second.layers.length, 2);
        yield* Effect.sleep("200 millis");
        assert.equal(compactions(), 2);
        assert.deepEqual(yield* here.agent.current(workspace), second);
      })
    )
  ).then((counted) => assert.deepEqual(counted, { "alasio.node_agent.compaction.outcome=superseded": 1, "alasio.node_agent.compaction.outcome=RunscError": 1 })));

test("a snapshot is committed only once each image it refers to is in the bucket as it was put, put again from the cache where it is not", () =>
  withBucket(({ s3, node }) =>
    Effect.gen(function*() {
      const workspace = { environment: "main", workspace: "fs-abc" };
      const taker = yield* node("node-1");
      const there = yield* node("node-2");
      const restored = () =>
        Effect.gen(function*() {
          const { manifest } = yield* there.agent.prepare(workspace);
          yield* there.agent.resume(resumeRequest(manifest.snapshot));
          yield* fetched(there, manifest);
        });
      // Lost, or another of the same size there, as its ETag says: put again from this node's cache.
      for (const lose of [(key: string) => s3.objects.delete(key), (key: string) => s3.objects.set(key, Buffer.alloc(4096, "other"))]) {
        const base = yield* taker.agent.checkpoint(request("fs-abc"));
        const pagesKey = `main/fs-abc/layers/${base.layers[0]?.digest}/pages.img`;
        yield* restored();
        lose(pagesKey);
        const asked = s3.requests.length;
        const delta = yield* there.agent.checkpoint(request("fs-abc"));
        assert.ok(s3.requests.slice(asked).includes(`PUT ${pagesKey}`));
        const unsent = ({ files }: Image) => files.map(({ path, bytes, sha256 }) => ({ path, bytes, sha256 }));
        assert.deepEqual(delta.layers.slice(1).map(unsent), base.layers.map(unsent));
        assert.equal(delta.layers[1]?.files.find(({ path }) => path === "pages.img")?.etag, `"${md5(readFileSync(join(there.cacheDir, "main", "fs-abc", "layers", base.layers[0]?.digest ?? "", "pages.img")))}"`);
        assert.deepEqual(yield* there.agent.current(workspace), delta);
      }

      // Lost, and not in this node's cache either: no snapshot of it can be committed, and the one it had is forgotten.
      const latest = yield* there.agent.current(workspace);
      assert.ok(latest);
      yield* restored();
      const [latestImage] = latest.layers;
      assert.ok(latestImage);
      s3.objects.delete(`main/fs-abc/layers/${latestImage.digest}/pages.img`);
      writeFileSync(join(there.cacheDir, "main", "fs-abc", "layers", latestImage.digest, "pages.img"), "changed on the disk");
      assert.equal(
        failure(yield* Effect.exit(there.agent.checkpoint(request("fs-abc")))),
        `checkpointing main/fs-abc failed: the snapshot of main/fs-abc is corrupt: its image ${latestImage.digest} is not in the bucket as it was put, and this node's cache holds other files of it; its snapshot is forgotten, as the workspace has moved on since it was taken`,
      );
      assert.equal(yield* there.agent.current(workspace), null);
    })
  ));

test("a checkpoint that fails forgets the workspace's snapshot, which the workspace has moved on from, and says so", () =>
  withBucket(({ s3, node }) =>
    Effect.gen(function*() {
      const here = yield* node("node-1");
      yield* here.agent.checkpoint(request("fs-abc"));

      // runsc fails.
      writeFileSync(join(here.releases.dirOf(RELEASE), "fail-checkpoint"), "");
      assert.equal(
        failure(yield* Effect.exit(here.agent.checkpoint(request("fs-abc")))),
        `checkpointing main/fs-abc failed: runsc checkpoint of ${RELEASE} failed: checkpoint failed: the sandbox is gone; its snapshot is forgotten, as the workspace has moved on since it was taken`,
      );
      assert.deepEqual(keysOf(s3, "main/fs-abc/"), []);
      assert.equal(cachesAny(here, "fs-abc"), false);
      assert.equal(yield* here.agent.current({ environment: "main", workspace: "fs-abc" }), null);
      rmSync(join(here.releases.dirOf(RELEASE), "fail-checkpoint"));

      // The commit fails, after the image went up: it goes too.
      yield* here.agent.checkpoint(request("fs-abc"));
      s3.refuse = (method, key) => (method === "PUT" && key === "main/fs-abc/current.json" ? 500 : null);
      assert.match(
        failure(yield* Effect.exit(here.agent.checkpoint(request("fs-abc", OTHER_CONTAINER)))),
        /^checkpointing main\/fs-abc failed: the bucket's upload of main\/fs-abc\/current\.json failed: 500 InternalError refused, as the test asks; its snapshot is forgotten, as the workspace has moved on since it was taken$/u,
      );
      assert.deepEqual(keysOf(s3, "main/fs-abc/"), []);
      assert.equal(cachesAny(here, "fs-abc"), false);
      s3.refuse = () => null;

      // An image saved by another release than the sandbox's is no snapshot of it.
      yield* here.agent.checkpoint(request("fs-abc"));
      writeFileSync(join(here.releases.dirOf(RELEASE), "saved-by"), "release-20260801.0-eaucoin.1");
      assert.equal(
        failure(yield* Effect.exit(here.agent.checkpoint(request("fs-abc")))),
        `checkpointing main/fs-abc failed: the snapshot of main/fs-abc is corrupt: its image says runsc release-20260801.0-eaucoin.1 saved it, not release-${RELEASE}; its snapshot is forgotten, as the workspace has moved on since it was taken`,
      );
      assert.deepEqual(keysOf(s3, "main/fs-abc/"), []);
      rmSync(join(here.releases.dirOf(RELEASE), "saved-by"));

      // A checkpoint stopped before it is done forgets it too; meanwhile what else is asked
      // of the workspace waits, and what is asked of another does not.
      yield* here.agent.checkpoint(request("fs-abc"));
      writeFileSync(join(here.releases.dirOf(RELEASE), "hang-checkpoint"), "");
      const runs = here.releases.calls(RELEASE).length;
      const stopped = yield* Effect.forkChild(here.agent.checkpoint(request("fs-abc")));
      while (here.releases.calls(RELEASE).length === runs) yield* Effect.sleep("20 millis");
      const waiting = yield* Effect.forkChild(here.agent.prepare({ environment: "main", workspace: "fs-abc" }));
      yield* here.agent.forget({ environment: "main", workspace: "fs-other" });
      yield* Effect.sleep("100 millis");
      assert.equal(waiting.pollUnsafe(), undefined);
      yield* Fiber.interrupt(stopped);
      assert.equal(failure(yield* Effect.exit(Fiber.join(waiting))), "main/fs-abc has no snapshot");
      assert.deepEqual(keysOf(s3, "main/fs-abc/"), []);
      assert.equal(cachesAny(here, "fs-abc"), false);
      rmSync(join(here.releases.dirOf(RELEASE), "hang-checkpoint"));
      const other = yield* here.agent.checkpoint(request("fs-other"));

      // The bucket holds the image otherwise than it was put, as an upload that failed silently would leave it.
      s3.refuse = (method, key) => (method === "HEAD" && key.endsWith("/pages.img") ? 404 : null);
      assert.match(
        failure(yield* Effect.exit(here.agent.checkpoint(request("fs-abc")))),
        /^checkpointing main\/fs-abc failed: the snapshot of main\/fs-abc is corrupt: its image [0-9a-f]{64} is not in the bucket as it was put; its snapshot is forgotten/u,
      );
      assert.deepEqual(keysOf(s3, "main/fs-abc/"), []);
      s3.refuse = () => null;

      // Forgetting fails too: said as well.
      s3.refuse = (method, key) => (key.startsWith("main/fs-other/") && method !== "GET" ? 503 : null);
      writeFileSync(join(here.releases.dirOf(RELEASE), "fail-checkpoint"), "");
      assert.match(
        failure(yield* Effect.exit(here.agent.checkpoint(request("fs-other")))),
        /: checkpoint failed: the sandbox is gone; forgetting its snapshot, as the workspace has moved on since it was taken, failed too: the bucket's deletion of main\/fs-other\/current\.json failed: 503 InternalError/u,
      );
      // The node's copy is gone, and the bucket's is as the deletion left it.
      assert.equal(cachesAny(here, "fs-other"), false);
      assert.equal(keysOf(s3, "main/fs-other/").length, 5);
      assert.equal((yield* here.agent.current({ environment: "main", workspace: "fs-other" }))?.snapshot, other.snapshot);
    })
  ));

test("a checkpoint taken live leaves its sandbox running, records what bayma said it caught, is the base of its next, and changes no snapshot where it fails", () =>
  withBucket(({ s3, node }) =>
    Effect.gen(function*() {
      const workspace = { environment: "main", workspace: "fs-abc" };
      const volume = mkdtempSync(join(tmpdir(), "alasio-volume-"));
      yield* Effect.addFinalizer(() => Effect.sync(() => rmSync(volume, { recursive: true, force: true })));
      const stateDir = join(volume, ".bayma");
      mkdirSync(stateDir);
      const lastSave = join(stateDir, "last-save.json");
      writeFileSync(lastSave, JSON.stringify({ schemaVersion: 1, ...CAUGHT }));
      const here = yield* node("node-1");
      here.releases.runs(CONTAINER, RELEASE, SESSIONS, POD, true, [{ destination: "/home/agent", source: volume }]);

      const first = yield* here.agent.checkpoint(live("fs-abc"));
      const saved = here.releases.calls(RELEASE).findLast((args) => args.includes("checkpoint"));
      const LIVE_FLAGS = ["--leave-running", "--precopy=auto", "--precopy-throttle=on", "--direct"];
      assert.deepEqual(saved?.filter((arg) => LIVE_FLAGS.includes(arg)), LIVE_FLAGS);
      assert.deepEqual(first.live, { stateDir: STATE_DIR, ...CAUGHT });
      assert.deepEqual(yield* here.agent.current(workspace), first);
      // Its sandbox runs on from the image it was saved to, so the next is a delta of it.
      const second = yield* here.agent.checkpoint(live("fs-abc"));
      assert.deepEqual(second.layers.slice(1), first.layers);

      // runsc fails: the snapshot it had holds the sandbox still, which ran on.
      writeFileSync(join(here.releases.dirOf(RELEASE), "fail-checkpoint"), "");
      assert.equal(
        failure(yield* Effect.exit(here.agent.checkpoint(live("fs-abc")))),
        `checkpointing main/fs-abc failed: runsc checkpoint of ${RELEASE} failed: checkpoint failed: the sandbox is gone; its snapshot is kept, as a live checkpoint that fails changes nothing`,
      );
      rmSync(join(here.releases.dirOf(RELEASE), "fail-checkpoint"));
      assert.deepEqual(yield* here.agent.current(workspace), second);
      // What runsc saved of the writes since is gone with it, so the next is whole.
      const whole = yield* here.agent.checkpoint(live("fs-abc"));
      assert.equal(whole.layers.length, 1);

      // What bayma said the save caught cannot be told: no snapshot of it.
      for (const said of ["{", JSON.stringify({ schemaVersion: 2, ...CAUGHT }), JSON.stringify({ schemaVersion: 1, ...CAUGHT, more: true })]) {
        writeFileSync(lastSave, said);
        assert.match(failure(yield* Effect.exit(here.agent.checkpoint(live("fs-abc")))), /: what its live save caught cannot be told: .*\/home\/agent\/\.bayma\/last-save\.json is none of bayma's: .*; its snapshot is kept/u);
        assert.deepEqual(yield* here.agent.current(workspace), whole);
      }
      writeFileSync(lastSave, JSON.stringify({ schemaVersion: 1, ...CAUGHT }));

      // The commit fails, after runsc saved it: the snapshot it had is kept, what it staged goes, and the next is whole.
      s3.refuse = (method, key) => (method === "PUT" && key === "main/fs-abc/current.json" ? 500 : null);
      assert.match(failure(yield* Effect.exit(here.agent.checkpoint(live("fs-abc")))), /; its snapshot is kept, as a live checkpoint that fails changes nothing$/u);
      s3.refuse = () => null;
      assert.deepEqual(yield* here.agent.current(workspace), whole);
      assert.deepEqual(readdirSync(join(here.cacheDir, "main", "fs-abc", "layers")).filter((name) => name.startsWith(".")), []);
      assert.equal((yield* here.agent.checkpoint(live("fs-abc"))).layers.length, 1);
    })
  ));

test("a sandbox restored from a snapshot taken live resumes only while bayma's state generation is the one it caught, read on its volume as untrusted", () =>
  withBucket(({ s3, node }) =>
    Effect.gen(function*() {
      const workspace = { environment: "main", workspace: "fs-abc" };
      const outside = mkdtempSync(join(tmpdir(), "alasio-volume-"));
      yield* Effect.addFinalizer(() => Effect.sync(() => rmSync(outside, { recursive: true, force: true })));
      // The volume mounted at /home/agent; beside it, outside it, what the sandbox may link to,
      // a generation that matches: one read there would let the sandbox resume.
      const volume = join(outside, "volume");
      const elsewhere = join(outside, "elsewhere");
      const matching = JSON.stringify({ schemaVersion: 1, generation: CAUGHT.generation });
      mkdirSync(elsewhere, { recursive: true });
      writeFileSync(join(elsewhere, "generation.json"), matching);
      const stateDir = join(volume, ".bayma");
      const file = join(stateDir, "generation.json");
      const reset = () => {
        rmSync(stateDir, { recursive: true, force: true });
        mkdirSync(stateDir, { recursive: true });
      };
      const generation = (written: string | Buffer) => {
        reset();
        writeFileSync(file, written);
      };
      const mounted = [{ destination: "/home/agent", source: volume }];

      const taker = yield* node("node-1");
      taker.releases.runs(CONTAINER, RELEASE, SESSIONS, POD, true, mounted);
      reset();
      writeFileSync(join(stateDir, "last-save.json"), JSON.stringify({ schemaVersion: 1, ...CAUGHT }));
      const snapshot = yield* taker.agent.checkpoint(live("fs-abc"));
      const there = yield* node("node-2");
      for (const id of [CONTAINER, OTHER_CONTAINER]) there.releases.runs(id, RELEASE, SESSIONS, POD, true, id === CONTAINER ? mounted : []);
      yield* there.agent.prepare(workspace);
      const resumes = () => there.releases.calls(RELEASE).filter((args) => args.includes("resume")).length;
      const refused = (agent: NodeAgent, why: RegExp) =>
        Effect.gen(function*() {
          const before = resumes();
          const said = failure(yield* Effect.exit(agent.resume(resumeRequest(snapshot.snapshot)).pipe(Effect.timeout("5 seconds"))));
          assert.match(said, new RegExp(`^the snapshot ${snapshot.snapshot} of main/fs-abc, taken live, holds the session no more: ${why.source}; its sandbox is left paused$`, "u"));
          assert.equal(resumes(), before);
        });

      // Nothing its clients were told of happened since the save, as bayma's padding leaves it: it resumes.
      generation(matching.padEnd(127) + "\n");
      yield* there.agent.resume(resumeRequest(snapshot.snapshot));
      assert.equal(resumes(), 2);

      // The generation moved on, or there is none.
      generation(JSON.stringify({ schemaVersion: 1, generation: CAUGHT.generation + 1 }));
      yield* refused(there.agent, /bayma's state generation is 6, and was 5 as it was saved/u);
      reset();
      yield* refused(there.agent, /.*\/volume\/\.bayma\/generation\.json cannot be opened: there is none/u);

      // A link at any name of its path, to what is outside the volume, or to the node's own files, is not followed.
      const linked = /.*\/volume\/\.bayma(\/generation\.json)? cannot be opened: it is a symbolic link, which is not followed, or no directory/u;
      reset();
      symlinkSync(join(elsewhere, "generation.json"), file);
      yield* refused(there.agent, linked);
      reset();
      symlinkSync("/etc/shadow", file);
      yield* refused(there.agent, linked);
      rmSync(stateDir, { recursive: true, force: true });
      symlinkSync(elsewhere, stateDir);
      yield* refused(there.agent, linked);

      // A FIFO is never opened, so not waited on, as a writer waiting for its reader shows,
      // which the agent's opening it would let write; a directory is no file either.
      reset();
      execFileSync("mkfifo", [file]);
      const wrote = join(outside, "wrote");
      const writer = spawn(process.execPath, ["-e", 'const fs = require("fs"); process.stdout.write("opening\\n"); fs.openSync(process.argv[1], "w"); fs.writeFileSync(process.argv[2], "")', file, wrote], { stdio: ["ignore", "pipe", "ignore"] });
      yield* Effect.addFinalizer(() => Effect.sync(() => writer.kill("SIGKILL")));
      yield* Effect.promise(() => new Promise((resolve) => writer.stdout.once("data", resolve)));
      // Waiting in its open by now.
      yield* Effect.sleep("100 millis");
      yield* refused(there.agent, /.*\/generation\.json is not a regular file/u);
      yield* Effect.sleep("300 millis");
      assert.equal(existsSync(wrote), false, "the agent opened the FIFO");
      writer.kill("SIGKILL");
      reset();
      mkdirSync(file);
      yield* refused(there.agent, /.*\/generation\.json is not a regular file/u);

      // One larger than the agent reads is not read.
      generation("");
      truncateSync(file, STATE_FILE_BYTES + 1);
      yield* refused(there.agent, new RegExp(`.*/generation\\.json is ${STATE_FILE_BYTES + 1} bytes, more than the ${STATE_FILE_BYTES} read of it`, "u"));

      // One that is not bayma's generation, as it says it, of a schema it knows, is none.
      for (const malformed of [
        "{",
        JSON.stringify({ schemaVersion: 2, generation: CAUGHT.generation }),
        JSON.stringify({ schemaVersion: 1, generation: -1 }),
        JSON.stringify({ schemaVersion: 1, generation: CAUGHT.generation, more: true }),
        Buffer.from([0x7b, 0xff, 0x7d]),
      ]) {
        generation(malformed);
        yield* refused(there.agent, /\/home\/agent\/\.bayma\/generation\.json is none of bayma's: [^\n]+/u);
      }

      // One not read within its time, as a volume that hangs would leave it, is unread: here the
      // container's spec, which the agent reads first, made a FIFO no one writes yet.
      generation(matching);
      const spec = join(there.releases.bundleOf(CONTAINER), "config.json");
      const specified = readFileSync(spec);
      rmSync(spec);
      execFileSync("mkfifo", [spec]);
      const resumed = resumes();
      const slow = failure(yield* Effect.exit(there.agent.resume(resumeRequest(snapshot.snapshot)).pipe(Effect.timeout(STATE_READ_TIMEOUT_MS + 10_000))));
      // What waits on it is given it, and ends.
      const writing = openSync(spec, constants.O_WRONLY | constants.O_NONBLOCK);
      writeSync(writing, specified);
      closeSync(writing);
      rmSync(spec);
      writeFileSync(spec, specified);
      assert.equal(slow, `the snapshot ${snapshot.snapshot} of main/fs-abc, taken live, holds the session no more: ${STATE_DIR}/generation.json was not read within ${STATE_READ_TIMEOUT_MS / 1000} s; its sandbox is left paused`);
      assert.equal(resumes(), resumed);

      // No volume of its pod holds bayma's state directory.
      there.releases.runs(CONTAINER, RELEASE, SESSIONS, POD, true, []);
      yield* refused(there.agent, /no volume of its pod holds \/home\/agent\/\.bayma\/generation\.json/u);
      there.releases.runs(CONTAINER, RELEASE, SESSIONS, POD, true, mounted);

      // An agent started since the prepare tells the snapshot from the manifest its cache keeps beside it.
      const again = yield* there.restarted;
      generation(JSON.stringify({ schemaVersion: 1, generation: 9 }));
      yield* refused(again, /bayma's state generation is 9, and was 5 as it was saved/u);
      generation(matching);
      yield* again.resume(resumeRequest(snapshot.snapshot));
      // One whose cache keeps no manifest of it reads the bucket's, and one neither holds is not let run either.
      const unkept = yield* there.restarted;
      rmSync(join(there.cacheDir, "main", "fs-abc", "manifest.json"));
      generation(JSON.stringify({ schemaVersion: 1, generation: 9 }));
      yield* refused(unkept, /bayma's state generation is 9, and was 5 as it was saved/u);
      generation(matching);
      // One the bucket fails to give is not let run either, but as the bucket's failure, which may pass, not as one refused.
      const manifestKey = `main/fs-abc/manifests/${snapshot.snapshot}.json`;
      s3.refuse = (method, key) => (method === "GET" && key === manifestKey ? 403 : null);
      const before = resumes();
      const unread = yield* Effect.exit(unkept.resume(resumeRequest(snapshot.snapshot)));
      s3.refuse = () => null;
      assert.ok(Exit.isFailure(unread) && Cause.findErrorOption(unread.cause).pipe(Option.map(({ _tag }) => _tag), Option.getOrUndefined) === "S3Error", failure(unread));
      assert.equal(resumes(), before);
      s3.objects.delete(manifestKey);
      yield* refused(unkept, /its manifest cannot be read, so whether it may run cannot be told: neither this node nor the bucket holds it/u);
    })
  ));

/** The token of the gofer's options in `imagePath`, as the agent gave it. */
const tokenIn = (imagePath: string) => {
  const { credentials } = goferOptionsOf(imagePath).options as { credentials: { access_key_id: string; secret_access_key: string } };
  return { accessKeyId: credentials.access_key_id, secretAccessKey: credentials.secret_access_key };
};

/** The status of a GET of `key` through `node`'s proxy of the bucket, signed with `token`. */
const throughProxy = (node: Node, token: { readonly accessKeyId: string; readonly secretAccessKey: string }, key: string): Effect.Effect<number> =>
  Effect.promise(() =>
    new Promise<number>((resolve, reject) => {
      const url = new URL(node.storeProxy.endpoint);
      const empty = createHash("sha256").update("").digest("hex");
      const headers: Record<string, string> = { host: url.host, "x-amz-date": new Date().toISOString().replace(/[-:]|\.\d{3}/gu, ""), "x-amz-content-sha256": empty };
      headers["authorization"] = sigV4Authorization({ method: "GET", path: `/snapshots/${key}`, query: {}, headers: { ...headers }, payloadHash: empty }, { region: "us-east-1", service: "s3" }, token);
      const asked = httpRequest({ host: url.hostname, port: url.port, path: `/snapshots/${key}`, headers }, (response) => {
        response.resume();
        resolve(response.statusCode ?? 0);
      });
      asked.on("error", reject);
      asked.end();
    })
  );

test("a restore from the bucket reads its own images through the agent by a token of its own, which its sandbox's resume lets read on in the background, and its abandon, its next prepare and its next checkpoint revoke", () =>
  withBucket(({ node }) =>
    Effect.gen(function*() {
      const workspace = { environment: "main", workspace: "fs-abc" };
      const taker = yield* node("node-1");
      const manifest = yield* taker.agent.checkpoint(request("fs-abc"));
      const [image] = manifest.layers;
      assert.ok(image);
      const own = `main/fs-abc/layers/${image.digest}/pages.img`;
      const others = (yield* taker.agent.checkpoint(request("fs-other", OTHER_CONTAINER))).layers[0]?.digest;
      const other = yield* node("node-2");
      /** Prepared from the bucket, as a node whose cache holds none of it does, once what a prepare before fetched is cleared: the token it was given. */
      let fetching: Manifest | null = null;
      /** The gofer's options file of the last prepared, which holds its token. */
      let options = "";
      const preparedRemote = Effect.gen(function*() {
        if (fetching) yield* fetched(other, fetching);
        yield* other.cache.clear("main", "fs-abc");
        const prepared = yield* other.agent.prepare(workspace);
        assert.equal(prepared.source, "bucket");
        fetching = prepared.manifest;
        options = join(prepared.imagePath, "s3_opts.json");
        return tokenIn(prepared.imagePath);
      });
      const first = yield* preparedRemote;
      assert.equal(yield* throughProxy(other, first, own), 200);
      // The workspace's images alone.
      assert.equal(yield* throughProxy(other, first, `main/fs-other/layers/${others}/pages.img`), 403);
      // Prepared again: a token of its own, the one before revoked.
      const second = yield* preparedRemote;
      assert.deepEqual([yield* throughProxy(other, first, own), yield* throughProxy(other, second, own)], [403, 200]);
      // Abandoned: revoked, and its options, which its gofer read as it began, gone from the node.
      assert.ok(existsSync(options));
      yield* other.agent.abandon({ ...workspace, namespace: SESSIONS, pod: POD, uid: POD_UID });
      assert.equal(yield* throughProxy(other, second, own), 403);
      assert.ok(!existsSync(options));
      // Resumed, it reads on, as its restore reads in the background what its sandbox did not touch first, its options gone.
      const third = yield* preparedRemote;
      assert.ok(existsSync(options));
      yield* other.agent.resume(resumeRequest(manifest.snapshot));
      assert.equal(yield* throughProxy(other, third, own), 200);
      assert.ok(!existsSync(options));
      // Its sandbox checkpointed live, all of its memory read: revoked.
      const volume = mkdtempSync(join(tmpdir(), "alasio-volume-"));
      yield* Effect.addFinalizer(() => Effect.sync(() => rmSync(volume, { recursive: true, force: true })));
      mkdirSync(join(volume, ".bayma"));
      writeFileSync(join(volume, ".bayma", "last-save.json"), JSON.stringify({ schemaVersion: 1, ...CAUGHT }));
      writeFileSync(join(volume, ".bayma", "generation.json"), JSON.stringify({ schemaVersion: 1, generation: CAUGHT.generation }));
      other.releases.runs(CONTAINER, RELEASE, SESSIONS, POD, true, [{ destination: "/home/agent", source: volume }]);
      yield* other.agent.checkpoint(live("fs-abc"));
      assert.equal(yield* throughProxy(other, third, own), 403);
      // And at its release, its sandbox ending.
      const fourth = yield* preparedRemote;
      yield* other.agent.resume(resumeRequest((yield* other.agent.current(workspace))?.snapshot ?? ""));
      assert.equal(yield* throughProxy(other, fourth, `main/fs-abc/layers/${(yield* other.agent.current(workspace))?.layers[0]?.digest}/pages.img`), 200);
      yield* other.agent.checkpoint(request("fs-abc"));
      assert.equal(yield* throughProxy(other, fourth, own), 403);
      // A prepare from the cache, as of one now held, revokes the one before it too.
      const fifth = yield* preparedRemote;
      yield* fetched(other, (yield* other.agent.current(workspace))!);
      assert.equal((yield* other.agent.prepare(workspace)).source, "cache");
      assert.equal(yield* throughProxy(other, fifth, own), 403);
    })
  ));

test("a restore that asks for its checkpoint gofer through the agent's socket, reading again pages a read of failed, is given options that say so, its token one the agent's drain does not wait on; one that asks for none reads over TCP as before", () =>
  withBucket(({ node }) =>
    Effect.gen(function*() {
      const workspace = { environment: "main", workspace: "fs-abc" };
      const here = yield* node("node-1");
      const manifest = yield* here.agent.checkpoint(request("fs-abc"));
      const [own] = manifest.layers;
      assert.ok(own);
      const there = yield* node("node-2", { socket: true });
      const where = (options: Record<string, unknown>) => {
        const { credentials: _, ...rest } = options;
        return rest;
      };
      const through = yield* there.agent.prepare({ ...workspace, gofer: { transport: "socket", pagesRetry: true } });
      assert.equal(through.source, "bucket");
      assert.deepEqual(where(goferOptionsOf(through.imagePath).options), {
        endpoint: "http://alasio-node-agent",
        socket: there.storeProxy.socket,
        region: "us-east-1",
        bucket: "snapshots",
        object_prefix: `main/fs-abc/layers/${own.digest}/`,
        layers_prefix: "main/fs-abc/layers/",
        addressing: "path",
        pages_retry: "while-running",
      });
      // Through the socket, its restore waits for the agent that comes back: the drain does not wait on it.
      assert.equal(there.storeProxy.reading(), 0);
      // Over TCP, as a release before the first that restores through the socket asks, or none.
      for (const gofer of [{ transport: "tcp", pagesRetry: false } as const, undefined]) {
        const over = yield* there.agent.prepare({ ...workspace, ...(gofer === undefined ? {} : { gofer }) });
        assert.deepEqual(where(goferOptionsOf(over.imagePath).options), {
          endpoint: there.storeProxy.endpoint,
          region: "us-east-1",
          bucket: "snapshots",
          object_prefix: `main/fs-abc/layers/${own.digest}/`,
          layers_prefix: "main/fs-abc/layers/",
          addressing: "path",
        });
        assert.equal(there.storeProxy.reading(), 1);
      }
      // Asked for the socket of an agent that serves none, it reads over TCP, said so.
      const plain = yield* node("node-3");
      const asked = yield* plain.agent.prepare({ ...workspace, gofer: { transport: "socket", pagesRetry: true } });
      assert.deepEqual(where(goferOptionsOf(asked.imagePath).options), {
        endpoint: plain.storeProxy.endpoint,
        region: "us-east-1",
        bucket: "snapshots",
        object_prefix: `main/fs-abc/layers/${own.digest}/`,
        layers_prefix: "main/fs-abc/layers/",
        addressing: "path",
        pages_retry: "while-running",
      });
    })
  ));

test("a restore from the bucket is told the bytes of pages its image holds, of every layer, as runsc says, and its agent's stop waits for them all, however quiet its reads; told none, or not in time, its quiet", () =>
  withBucket(({ s3, node }) =>
    Effect.gen(function*() {
      const workspace = { environment: "main", workspace: "fs-abc" };
      const taker = yield* node("node-1");
      const manifest = yield* taker.agent.checkpoint(request("fs-abc"));
      const own = `main/fs-abc/layers/${manifest.layers[0]?.digest}/pages.img`;
      // Its image's pages, 4096 bytes, and a layer's beside them, the rest of what runsc says
      // of the image (./support/runsc-image-inspect.json): 45056 bytes in all.
      const layer = `main/fs-abc/layers/${"b".repeat(64)}/pages.img`;
      s3.objects.set(layer, Buffer.alloc(45056 - 4096, 7));
      const there = yield* node("node-2", { quietMs: 100, inspectTimeoutMs: 2000 });
      const said = (name: string, on: boolean) => {
        const path = join(there.releases.releasesDir, RELEASE, name);
        if (on) writeFileSync(path, "");
        else rmSync(path, { force: true });
      };
      const restored = Effect.gen(function*() {
        yield* there.cache.clear("main", "fs-abc");
        const prepared = yield* there.agent.prepare(workspace);
        assert.equal(prepared.source, "bucket");
        const token = tokenIn(prepared.imagePath);
        yield* there.agent.resume(resumeRequest(manifest.snapshot));
        return token;
      });
      // Told its 45056 bytes: reading, its reads however far apart, until it was served them all.
      said("remote-inspect", true);
      const token = yield* restored;
      yield* Effect.sleep("400 millis");
      assert.equal(there.storeProxy.reading(), 1);
      assert.equal(yield* throughProxy(there, token, own), 200);
      yield* Effect.sleep("400 millis");
      assert.equal(there.storeProxy.reading(), 1);
      assert.equal(yield* throughProxy(there, token, layer), 200);
      yield* Effect.sleep("50 millis");
      assert.equal(there.storeProxy.reading(), 0);
      // Told none, as runsc fails to say: done once its quiet has passed.
      said("remote-inspect", false);
      yield* restored;
      yield* Effect.sleep("400 millis");
      assert.equal(there.storeProxy.reading(), 0);
      // Not told in time, as runsc hangs: the same, its runsc killed.
      said("hang-inspect", true);
      yield* restored;
      yield* Effect.sleep("300 millis");
      assert.equal(there.storeProxy.reading(), 1);
      yield* Effect.sleep("2000 millis");
      assert.equal(there.storeProxy.reading(), 0);
      const inspecting = execFileSync("ps", ["-eo", "args"], { encoding: "utf8" }).split("\n").filter((line) => line.includes(join(there.releases.releasesDir, RELEASE)) && line.includes("inspect"));
      assert.deepEqual(inspecting, []);
      said("hang-inspect", false);
    })
  ));

test("an agent that stops prepares no restore from the bucket, which would read through it after it stopped, its node's next agent preparing it; one from its cache it does", () =>
  withBucket(({ node }) =>
    Effect.gen(function*() {
      const workspace = { environment: "main", workspace: "fs-abc" };
      const here = yield* node("node-1");
      const manifest = yield* here.agent.checkpoint(request("fs-abc"));
      const other = yield* node("node-2");
      // Its cache holds the snapshot once a restore from the bucket fetched it, the next prepared from there.
      assert.equal((yield* other.agent.prepare(workspace)).source, "bucket");
      yield* fetched(other, manifest);
      assert.equal((yield* other.agent.prepare(workspace)).source, "cache");
      // Nothing reads through its proxy, that restore's token revoked: drained at once.
      yield* other.storeProxy.drained.pipe(Effect.timeout("5 seconds"));
      assert.equal((yield* other.agent.prepare(workspace)).source, "cache");
      yield* other.cache.clear("main", "fs-abc");
      assert.equal(failure(yield* other.agent.prepare(workspace).pipe(Effect.exit)), "the node agent is stopping, so it prepares no restore of main/fs-abc from the bucket: its next one does");
      assert.ok(!cachesAny(other, "fs-abc"));
      assert.equal(other.storeProxy.reading(), 0);
    })
  ));

test("a sandbox restored from the bucket, checkpointed as its pod stops while its images are fetched into the cache, waits on no download: each is downloaded outside the workspace's lock, taken to commit it alone", () =>
  withBucket(({ s3, node }) =>
    Effect.gen(function*() {
      const workspace = { environment: "main", workspace: "fs-abc" };
      const taker = yield* node("node-1");
      const base = yield* taker.agent.checkpoint(request("fs-abc"));
      const [image] = base.layers;
      assert.ok(image);
      const there = yield* node("node-2");
      // Each get of the bucket slow, as one of a large image is.
      s3.getDelayMs = 1500;
      const { manifest } = yield* there.agent.prepare(workspace);
      yield* there.agent.resume(resumeRequest(manifest.snapshot));
      // Its image is fetched still, a get of each of its files taking 1.5 s.
      assert.ok(!existsSync(join(there.cacheDir, "main", "fs-abc", "layers", image.digest)));
      const startedAt = Date.now();
      const released = yield* there.agent.checkpoint(request("fs-abc"));
      const took = Date.now() - startedAt;
      assert.ok(took < 1000, `the checkpoint waited ${took} ms`);
      assert.ok(!existsSync(join(there.cacheDir, "main", "fs-abc", "layers", image.digest)), "the image was fetched before the checkpoint ended");
      // Whole, as the cache held no base for it; and the image fetched after all.
      assert.equal(released.layers.length, 1);
      yield* fetched(there, base);
    })
  ));

test("only a release that restores straight from the bucket is given the bucket: a snapshot of one before is fetched into the cache first, and restored from there", () =>
  withBucket(({ s3, node }) =>
    Effect.gen(function*() {
      // As cluster/node/pins.json has it: from the fork's .7 on, which the installation gives the agent.
      assert.equal(NODE_PINS.gvisor.restoresFromBucketSince, "20260921.0-eaucoin.7");
      assert.deepEqual(
        ["20260921.0-eaucoin.6", "20260921.0-eaucoin.7", "20260921.0-eaucoin.10", "20260921.1-eaucoin.1", "20261020.0-eaucoin.1", "20260801.0-eaucoin.9", "20260921.0"].map((release) => restoresFromBucket(release, "20260921.0-eaucoin.7")),
        [false, true, true, true, true, false, false],
      );
      // Where none is given, none does.
      assert.equal(restoresFromBucket("20260921.0-eaucoin.8", ""), false);
      const OLDER = "20260921.0-eaucoin.6";
      const workspace = { environment: "main", workspace: "fs-abc" };
      const taker = yield* node("node-1", { releases: [OLDER] });
      const manifest = yield* taker.agent.checkpoint(request("fs-abc"));
      assert.equal(manifest.release, OLDER);
      const [image] = manifest.layers;
      assert.ok(image);
      const other = yield* node("node-2", { releases: [OLDER] });
      const prepared = yield* other.agent.prepare(workspace);
      // Read from the bucket, into the cache, which it is restored from: no gofer's options, its images there already.
      assert.deepEqual(prepared, { manifest, imagePath: join(other.cacheDir, "main", "fs-abc", manifest.snapshot), source: "bucket" });
      assert.deepEqual(filesOf(prepared.imagePath), image.files);
      assert.ok(!readdirSync(prepared.imagePath).includes("s3_opts.json"));
      assert.deepEqual(imagesOf(other, "fs-abc"), [image.digest]);
      assert.deepEqual(s3.requests.filter((asked) => asked.startsWith("GET main/fs-abc/layers/")).length, image.files.length);
      // A file it downloads that is not what the manifest says fails it, and leaves nothing of it.
      s3.objects.set(`main/fs-abc/layers/${image.digest}/pages_meta.img`, Buffer.from("something else"));
      const third = yield* node("node-3", { releases: [OLDER] });
      const expected = image.files.find(({ path }) => path === "pages_meta.img");
      assert.equal(
        failure(yield* Effect.exit(third.agent.prepare(workspace))),
        `the snapshot of main/fs-abc is corrupt: its image ${image.digest}'s file pages_meta.img is 14 bytes of SHA-256 ${sha256("something else")}, where its manifest says ${expected?.bytes} bytes of ${expected?.sha256}`,
      );
      assert.equal(cachesAny(third, "fs-abc"), false);
    })
  ));

test("a snapshot is prepared from the node's cache where it holds it whole and its runsc restores it, and otherwise from the bucket, its images fetched meanwhile", () =>
  withBucket(({ s3, node }) =>
    Effect.gen(function*() {
      const workspace = { environment: "main", workspace: "fs-abc" };
      const taker = yield* node("node-1");
      assert.equal(failure(yield* Effect.exit(taker.agent.prepare(workspace))), "main/fs-abc has no snapshot");
      const manifest = yield* taker.agent.checkpoint(request("fs-abc"));
      const [image] = manifest.layers;
      assert.ok(image);
      const files = image.files.map(({ path }) => `GET main/fs-abc/layers/${image.digest}/${path}`);
      const downloads = () => s3.requests.filter((asked) => asked.startsWith("GET main/fs-abc/layers/"));

      // The node that took it has it, which its runsc restores, as it says.
      const linked = join(taker.cacheDir, "main", "fs-abc", manifest.snapshot);
      assert.deepEqual(yield* taker.agent.prepare(workspace), { manifest, imagePath: linked, source: "cache" });
      assert.deepEqual(taker.releases.calls(RELEASE).at(-1), [`--root=${RUNSC_ROOT}`, "--systemd-cgroup=true", "image", "verify", "--host", linked]);
      assert.deepEqual(downloads(), []);

      // Another node reads it from the bucket, each of its images there as it was put, as the restore goes; and fetches it meanwhile, once.
      const other = yield* node("node-2");
      const asked = s3.requests.length;
      const remote = yield* other.agent.prepare(workspace);
      assert.deepEqual(remote, { manifest, imagePath: join(other.cacheDir, "main", "fs-abc", manifest.snapshot), source: "bucket" });
      assert.deepEqual(goferOptionsOf(remote.imagePath).files, ["s3_opts.json"]);
      assert.deepEqual(s3.requests.slice(asked, asked + 1 + IMAGE_FILES.length), ["GET main/fs-abc/current.json", ...IMAGE_FILES.map((path) => `HEAD main/fs-abc/layers/${image.digest}/${path}`)]);
      yield* fetched(other, manifest);
      assert.deepEqual(downloads(), files);
      assert.deepEqual(JSON.parse(readFileSync(join(other.cacheDir, "main", "fs-abc", "manifest.json"), "utf8")), manifest);
      // Prepared again from the cache, verified, none of the image's files read, as its stamp says each is as it was checked.
      const cachedImage = join(other.cacheDir, "main", "fs-abc", "layers", image.digest);
      const sources: string[] = [];
      assert.deepEqual(yield* readsUnder(cachedImage, other.agent.prepare(workspace).pipe(Effect.tap(({ source }) => Effect.sync(() => sources.push(source))))), []);
      assert.deepEqual(sources, ["cache"]);
      assert.deepEqual(downloads(), files);
      const times = (count: number) => Array.from({ length: count }, () => files).flat();
      /** Prepared from the bucket again, as a cached copy that is not as it was checked is, and fetched again: the downloads then. */
      const fromTheBucket = (prepared: Effect.Effect<{ readonly source: string }, unknown>) =>
        Effect.gen(function*() {
          assert.equal((yield* prepared).source, "bucket");
          yield* fetched(other, manifest);
          return downloads();
        });
      // A cached copy that is no longer what its manifest says, as a disk may change it: one of
      // other bytes, one whose modification time has changed, and one without its stamp.
      writeFileSync(join(cachedImage, "pages_meta.img"), "changed on the disk");
      assert.deepEqual(yield* fromTheBucket(other.agent.prepare(workspace)), times(2));
      const later = new Date(Date.now() + 60_000);
      utimesSync(join(cachedImage, "pages.img"), later, later);
      assert.deepEqual(yield* fromTheBucket(other.agent.prepare(workspace)), times(3));
      rmSync(join(other.cacheDir, "main", "fs-abc", "stamps", `${image.digest}.json`));
      assert.deepEqual(yield* fromTheBucket(other.agent.prepare(workspace)), times(4));
      assert.deepEqual(filesOf(join(other.cacheDir, "main", "fs-abc", "layers", image.digest)), image.files);
      // Stamped at a whole second, which the test can set a file's modification time back to exactly:
      // one whose size has changed since, its modification time set back, or another file in its place, is read again too.
      const pagesPath = join(cachedImage, "pages.img");
      const stampedAt = new Date(1_790_000_000_000);
      const restamped = (changed: () => void) =>
        Effect.gen(function*() {
          utimesSync(pagesPath, stampedAt, stampedAt);
          yield* other.cache.stamp("main", "fs-abc", image.digest, image.files);
          changed();
          utimesSync(pagesPath, stampedAt, stampedAt);
          return yield* other.agent.prepare(workspace);
        });
      const pages = readFileSync(pagesPath);
      assert.deepEqual(yield* fromTheBucket(restamped(() => writeFileSync(pagesPath, Buffer.concat([pages, Buffer.from("more")])))), times(5));
      // Another file in its place, made before the one it replaces is gone, so of another inode.
      assert.deepEqual(yield* fromTheBucket(restamped(() => {
        writeFileSync(`${pagesPath}.new`, pages);
        renameSync(`${pagesPath}.new`, pagesPath);
      })), times(6));
      // As it was stamped, it is taken as checked.
      assert.equal((yield* restamped(() => {})).source, "cache");
      assert.deepEqual(downloads(), times(6));
      // So is one its runsc finds no restorable image of, malformed (3) or missing a layer (4).
      for (const status of ["3 Image of digest mismatch", "4 layer not found"]) {
        writeFileSync(join(other.releases.dirOf(RELEASE), "verify-status"), status);
        assert.equal((yield* other.agent.prepare(workspace)).source, "bucket");
        rmSync(join(other.releases.dirOf(RELEASE), "verify-status"));
        yield* fetched(other, manifest);
      }
      // One its runsc cannot restore on this node (5) is restored on none of its.
      writeFileSync(join(other.releases.dirOf(RELEASE), "verify-status"), "5 Not restorable here: saved by runsc \"release-a\", this is runsc \"release-b\"");
      assert.equal(
        failure(yield* Effect.exit(other.agent.prepare(workspace))),
        `this node cannot restore the snapshot ${manifest.snapshot} of main/fs-abc: this node's runsc does not restore it: Not restorable here: saved by runsc "release-a", this is runsc "release-b"`,
      );
      rmSync(join(other.releases.dirOf(RELEASE), "verify-status"));

      // A newer snapshot is fetched beside the older one's images, which a later collection discards.
      const newer = yield* taker.agent.checkpoint(request("fs-abc", OTHER_CONTAINER));
      yield* other.agent.prepare(workspace);
      yield* fetched(other, newer);
      assert.deepEqual(imagesOf(other, "fs-abc"), [image.digest, newer.layers[0]?.digest].sort());

      // An image not in the bucket as it was put fails it, and leaves nothing of it.
      const [newerImage] = newer.layers;
      assert.ok(newerImage);
      s3.objects.set(`main/fs-abc/layers/${newerImage.digest}/pages_meta.img`, Buffer.from("something else"));
      const third = yield* node("node-3");
      assert.equal(failure(yield* Effect.exit(third.agent.prepare(workspace))), `the snapshot of main/fs-abc is corrupt: its image ${newerImage.digest} is not in the bucket as it was put`);
      assert.equal(cachesAny(third, "fs-abc"), false);

      // A node without its release, or whose CPU lacks a feature it was saved with, cannot restore it.
      const older = yield* node("node-4", { releases: ["20260801.0-eaucoin.1"] });
      assert.equal(
        failure(yield* Effect.exit(older.agent.prepare(workspace))),
        `this node cannot restore the snapshot ${newer.snapshot} of main/fs-abc: its gVisor ${RELEASE} is not installed here`,
      );
      const narrower = yield* node("node-5");
      writeFileSync(join(narrower.releases.dirOf(RELEASE), "cpu-features"), "sse,avx2");
      assert.equal(
        failure(yield* Effect.exit(narrower.agent.prepare(workspace))),
        `this node cannot restore the snapshot ${newer.snapshot} of main/fs-abc: this node's CPU lacks features it was saved with: avx`,
      );
    })
  ));

test("what no snapshot needs any more is collected, but what a restore or a commit may still read, and what the latest snapshot holds", () =>
  withBucket(({ s3, node }) =>
    Effect.gen(function*() {
      const workspace = { environment: "main", workspace: "fs-abc" };
      const here = yield* node("node-1");
      const first = yield* here.agent.checkpoint(request("fs-abc"));
      const digestOf = ({ layers: [image] }: Manifest) => image?.digest ?? "";
      const imageKeys = (digest: string) => IMAGE_FILES.map((path) => `main/fs-abc/layers/${digest}/${path}`);

      // A restore on another node reads the snapshot current as it begins; meanwhile a newer one is
      // committed, and what no snapshot needs collected. The first was superseded just now, so its
      // image is kept: the restore reads it whole.
      s3.getDelayMs = 100;
      const reader = yield* node("node-2");
      const restoring = yield* Effect.forkChild(reader.agent.prepare(workspace));
      yield* Effect.sync(() => s3.requests.some((asked) => asked.startsWith(`GET main/fs-abc/layers/${digestOf(first)}/`))).pipe(
        Effect.repeat({ schedule: Schedule.spaced("10 millis"), until: Boolean }),
      );
      const second = yield* here.agent.checkpoint(request("fs-abc", OTHER_CONTAINER));
      assert.deepEqual(yield* here.agent.collect(workspace), [digestOf(first), digestOf(second)].sort());
      assert.deepEqual((yield* Fiber.join(restoring)).manifest, first);
      s3.getDelayMs = 0;

      // Superseded for longer than the grace, the first goes, its image and its manifest; the latest stays, however old.
      const longAgo = Date.now() - COLLECT_GRACE_MS - 1000;
      for (const key of keysOf(s3, "main/fs-abc/")) s3.writtenAt.set(key, longAgo);
      assert.deepEqual(yield* here.agent.collect(workspace), [digestOf(second)]);
      const latest = [...imageKeys(digestOf(second)), `main/fs-abc/manifests/${second.snapshot}.json`, "main/fs-abc/current.json"].sort();
      assert.deepEqual(keysOf(s3, "main/fs-abc/"), latest);

      // Spent, as its sandbox was restored from it, it is kept still: the sandbox's next snapshot refers to it.
      // Spending another, as one that a newer superseded, spends nothing.
      yield* here.agent.spend(workspace, first.snapshot);
      assert.deepEqual(yield* here.agent.current(workspace), second);
      yield* here.agent.spend(workspace, second.snapshot);
      assert.equal(yield* here.agent.current(workspace), null);
      assert.deepEqual(yield* here.agent.collect(workspace), [digestOf(second)]);
      assert.deepEqual(keysOf(s3, "main/fs-abc/"), latest.filter((key) => key !== "main/fs-abc/current.json"));

      // An image whose manifest is not yet written, as a commit writes them, is kept within the grace.
      const committing = `main/fs-abc/layers/${"f".repeat(64)}/pages.img`;
      s3.objects.set(committing, Buffer.alloc(10));
      s3.writtenAt.set(committing, Date.now());
      yield* here.agent.collect(workspace);
      assert.ok(s3.objects.has(committing));
      s3.writtenAt.set(committing, longAgo);
      yield* here.agent.collect(workspace);
      assert.equal(s3.objects.has(committing), false);

      // A node discards the images its cache holds that are no longer kept, once no restore there may read them.
      assert.deepEqual(imagesOf(here, "fs-abc"), [digestOf(first), digestOf(second)].sort());
      assert.equal(yield* here.agent.discard(workspace, [digestOf(second)]), true);
      assert.deepEqual(imagesOf(here, "fs-abc"), [digestOf(second)]);
      assert.deepEqual(readdirSync(join(here.cacheDir, "main", "fs-abc", "stamps")), [`${digestOf(second)}.json`]);
    })
  ));

test("a copy gives a workspace another's snapshot, its images copied in the bucket, checked, then its manifest, then its commit", () =>
  withBucket(({ s3, node }) =>
    Effect.gen(function*() {
      const here = yield* node("node-1");
      const from = { environment: "main", workspace: "fs-abc" };
      const fork = { environment: "br-1", workspace: "fs-fork" };
      assert.equal(failure(yield* Effect.exit(here.agent.copy(from, fork))), "main/fs-abc has no snapshot");
      assert.deepEqual(keysOf(s3, "br-1/"), []);

      const source = yield* here.agent.checkpoint(request("fs-abc"));
      const [image] = source.layers;
      assert.ok(image);
      const asked = s3.requests.length;
      const copied = yield* here.agent.copy(from, fork);
      assert.deepEqual(copied, { ...source, environment: "br-1", workspace: "fs-fork" });
      const prefix = `br-1/fs-fork/layers/${image.digest}`;
      assert.deepEqual(s3.requests.slice(asked).filter((request) => !request.startsWith("GET")), [
        ...image.files.flatMap(({ path }) => [`PUT ${prefix}/${path} from main/fs-abc/layers/${image.digest}/${path}`, `HEAD ${prefix}/${path}`]),
        `PUT br-1/fs-fork/manifests/${source.snapshot}.json`,
        "PUT br-1/fs-fork/current.json",
      ]);
      assert.deepEqual(JSON.parse(s3.objects.get(`br-1/fs-fork/manifests/${source.snapshot}.json`)?.toString() ?? ""), copied);
      assert.deepEqual(yield* here.agent.current(fork), copied);
      assert.deepEqual(yield* here.agent.current(from), source);
      // Another node prepares it from the bucket, at the fork's own prefix, and fetches each file of it, what the source's is.
      const other = yield* node("node-2");
      const prepared = yield* other.agent.prepare(fork);
      assert.deepEqual(prepared, { manifest: copied, imagePath: join(other.cacheDir, "br-1", "fs-fork", source.snapshot), source: "bucket" });
      assert.equal(goferOptionsOf(prepared.imagePath).options["object_prefix"], `${prefix}/`);
      yield* fetched(other, copied);
      assert.deepEqual(filesOf(join(other.cacheDir, "br-1", "fs-fork", "layers", image.digest)), image.files);

      // A workspace's own snapshot is replaced, and what this node's cache held of it goes.
      const own = { environment: "main", workspace: "fs-own" };
      yield* here.agent.checkpoint(request("fs-own"));
      const newer = yield* here.agent.checkpoint(request("fs-abc", OTHER_CONTAINER));
      assert.equal((yield* here.agent.copy(from, own)).snapshot, newer.snapshot);
      assert.equal((yield* here.agent.current(own))?.snapshot, newer.snapshot);
      assert.equal(cachesAny(here, "fs-own"), false);

      // A workspace's snapshot is its own already.
      assert.deepEqual(yield* here.agent.copy(from, from), newer);

      // Copies each way between two workspaces, both waiting as a checkpoint of one runs,
      // hold them in turn, never one each.
      writeFileSync(join(here.releases.dirOf(RELEASE), "hang-checkpoint"), "");
      const runs = here.releases.calls(RELEASE).length;
      const checkpointing = yield* Effect.forkChild(here.agent.checkpoint(request("fs-abc")));
      while (here.releases.calls(RELEASE).length === runs) yield* Effect.sleep("20 millis");
      const there = yield* Effect.forkChild(Effect.exit(here.agent.copy(from, own)));
      const back = yield* Effect.forkChild(here.agent.copy(own, from));
      yield* Effect.sleep("100 millis");
      yield* Fiber.interrupt(checkpointing);
      rmSync(join(here.releases.dirOf(RELEASE), "hang-checkpoint"));
      // The checkpoint stopped forgot the snapshot of fs-abc, which fs-own's then replaced.
      assert.equal(failure(yield* Fiber.join(there).pipe(Effect.timeout("5 seconds"))), "main/fs-abc has no snapshot");
      assert.equal((yield* Fiber.join(back).pipe(Effect.timeout("5 seconds"))).snapshot, newer.snapshot);
    })
  ));

test("a copy that fails as it writes leaves the workspace without a snapshot, rather than part of one or the one it had", () =>
  withBucket(({ s3, node }) =>
    Effect.gen(function*() {
      const here = yield* node("node-1");
      const from = { environment: "main", workspace: "fs-abc" };
      const own = { environment: "main", workspace: "fs-own" };
      const source = yield* here.agent.checkpoint(request("fs-abc"));
      const [image] = source.layers;
      assert.ok(image);

      // The commit fails, after the images were copied.
      yield* here.agent.checkpoint(request("fs-own"));
      s3.refuse = (method, key) => (method === "PUT" && key === "main/fs-own/current.json" ? 500 : null);
      assert.match(failure(yield* Effect.exit(here.agent.copy(from, own))), /^the bucket's upload of main\/fs-own\/current\.json failed: 500 InternalError/u);
      assert.deepEqual(keysOf(s3, "main/fs-own/"), []);
      assert.equal(cachesAny(here, "fs-own"), false);
      assert.equal(yield* here.agent.current(own), null);

      // A file's copy fails.
      s3.refuse = (method, key) => (method === "PUT" && key.endsWith("/pages_meta.img") ? 500 : null);
      assert.match(
        failure(yield* Effect.exit(here.agent.copy(from, own))),
        new RegExp(`^the bucket's copy of main/fs-abc/layers/${image.digest}/pages_meta\\.img to main/fs-own/layers/${image.digest}/pages_meta\\.img failed: 500`, "u"),
      );
      assert.deepEqual(keysOf(s3, "main/fs-own/"), []);

      // Forgetting fails too: the copy's failure is said, and the forgetting's logged.
      s3.refuse = (method, key) => (key.startsWith("main/fs-own/") && method !== "GET" ? 503 : null);
      assert.match(failure(yield* Effect.exit(here.agent.copy(from, own))), /^the bucket's copy of .* failed: 503/u);
      s3.refuse = () => null;
      // The source keeps its snapshot throughout.
      assert.deepEqual(yield* here.agent.current(from), source);
    })
  ));

test("the agent counts what it does, and tells, as metrics are exported, the releases it keeps and the snapshots its cache holds of each", () =>
  withBucket(({ node }) =>
    Effect.gen(function*() {
      const older = "20260801.0-eaucoin.1";
      const here = yield* node("node-1", { releases: [RELEASE, older] });
      const counted = (name: string, effect: Effect.Effect<unknown, unknown>) => Effect.promise(() => added(name, () => Effect.runPromise(effect)));
      const both = Effect.all([here.agent.checkpoint(request("fs-abc")), here.agent.checkpoint(request("fs-other"))]);
      assert.deepEqual(yield* counted("alasio.node_agent.checkpoints", both), { "alasio.node_agent.checkpoint.outcome=taken": 2 });
      assert.deepEqual(yield* counted("alasio.node_agent.prepares", here.agent.prepare({ environment: "main", workspace: "fs-abc" })), {
        "alasio.node_agent.prepare.outcome=prepared,alasio.node_agent.prepare.source=cache": 1,
      });
      assert.deepEqual(yield* counted("alasio.node_agent.prepares", Effect.exit(here.agent.prepare({ environment: "main", workspace: "fs-none" }))), {
        "alasio.node_agent.prepare.outcome=NoSnapshot": 1,
      });
      const releases = yield* Effect.promise(() => collected("alasio.node_agent.releases"));
      assert.deepEqual(releases, [{ attributes: "", value: 2 }]);
      const cached = yield* Effect.promise(() => collected("alasio.node_agent.cache.snapshots"));
      assert.deepEqual(cached.toSorted((a, b) => a.attributes.localeCompare(b.attributes)), [
        { attributes: `alasio.gvisor.release=${older}`, value: 0 },
        { attributes: `alasio.gvisor.release=${RELEASE}`, value: 2 },
      ]);
      // A checkpoint that fails is counted by what failed.
      writeFileSync(join(here.releases.dirOf(RELEASE), "fail-checkpoint"), "");
      assert.deepEqual(yield* counted("alasio.node_agent.checkpoints", Effect.exit(here.agent.checkpoint(request("fs-abc")))), { "alasio.node_agent.checkpoint.outcome=RunscError": 1 });
    })
  ));

test("checkpoints and prepares keep the node's cache within its budget, the least recently used workspace evicted", () =>
  withBucket(({ node }) =>
    Effect.gen(function*() {
      // Room for one workspace's snapshot, of about 6 KB with its stamps and manifest, and not two.
      const taker = yield* node("node-1", { budget: 9000 });
      yield* taker.agent.checkpoint(request("fs-abc"));
      yield* Effect.sleep("20 millis");
      yield* taker.agent.checkpoint(request("fs-other"));
      assert.deepEqual([cachesAny(taker, "fs-abc"), cachesAny(taker, "fs-other")], [false, true]);
      // Read from the bucket, as their images are fetched.
      const other = yield* node("node-2", { budget: 9000 });
      yield* fetched(other, (yield* other.agent.prepare({ environment: "main", workspace: "fs-other" })).manifest);
      yield* Effect.sleep("20 millis");
      yield* fetched(other, (yield* other.agent.prepare({ environment: "main", workspace: "fs-abc" })).manifest);
      yield* Effect.sync(() => cachesAny(other, "fs-other")).pipe(Effect.repeat({ schedule: Schedule.spaced("10 millis"), until: (held) => !held }), Effect.timeout("5 seconds"));
      assert.deepEqual([cachesAny(other, "fs-abc"), cachesAny(other, "fs-other")], [true, false]);
    })
  ));

/** What spares no workspace, as the cache is told where nothing of the agent's runs. */
const sparesNone: Spared = () => false;

/** What spares the workspace `name`, as the cache is told where the agent writes to it. */
const spares = (name: string): Spared => ({ workspace }) => workspace === name;

test("a checkpoint's images, the one it stages and the parent it saves a delta of, are never evicted as another workspace's checkpoint makes room meanwhile", () =>
  withBucket(({ node }) =>
    Effect.gen(function*() {
      // Room for about one workspace's snapshot of 4 KB; each workspace's sandbox under a release of its own, so one's checkpoint can be held as the other's runs.
      const other = "20260921.0-eaucoin.4";
      const here = yield* node("node-1", { releases: [RELEASE, other], budget: 6000 });
      here.releases.runs(OTHER_CONTAINER, other, SESSIONS, POD);
      const workspace = { environment: "main", workspace: "fs-abc" };
      // fs-abc's sandbox restored from its snapshot, so its next is a delta of the image the cache holds.
      yield* here.agent.checkpoint(request("fs-abc"));
      const { manifest } = yield* here.agent.prepare(workspace);
      yield* here.agent.spend(workspace, manifest.snapshot);
      yield* here.agent.resume(resumeRequest(manifest.snapshot, [POD_IP], [CONTAINER]));
      const held = join(here.releases.dirOf(RELEASE), "hold-checkpoint");
      writeFileSync(held, "");
      const checkpoints = () => here.releases.calls(RELEASE).filter((args) => args.includes("checkpoint")).length;
      const before = checkpoints();
      const delta = yield* Effect.forkChild(here.agent.checkpoint(request("fs-abc")));
      yield* Effect.sync(checkpoints).pipe(Effect.repeat({ schedule: Schedule.spaced("10 millis"), until: (count) => count > before }));
      // Meanwhile fs-other's checkpoint commits, the cache over its budget, and the agent's eviction runs.
      yield* Effect.sleep("20 millis");
      const taken = yield* here.agent.checkpoint(request("fs-other", OTHER_CONTAINER));
      assert.ok(cachesAny(here, "fs-abc"), "fs-abc was evicted as its checkpoint ran");
      rmSync(held);
      const saved = yield* Fiber.join(delta);
      // Neither lost what it wrote: fs-abc's is a delta of the image its sandbox was restored from.
      assert.deepEqual(saved.layers.slice(1), manifest.layers);
      assert.deepEqual(yield* here.agent.current(workspace), saved);
      assert.deepEqual(yield* here.agent.current({ environment: "main", workspace: "fs-other" }), taken);
    })
  ));

const S1 = "20261013T000000000Z-00000001";
const [D1, D2] = ["1".repeat(64), "2".repeat(64)];

/** Puts an image of 1000 bytes, `digest`, in the cache, of `workspace`, used `minutesAgo`. */
const cacheImage = (cache: SnapshotCache, workspace: string, digest: string, minutesAgo: number) =>
  Effect.gen(function*() {
    const directory = yield* cache.stage("main", workspace, S1);
    writeFileSync(join(directory, "pages.img"), Buffer.alloc(1000));
    yield* cache.commit("main", workspace, S1, digest);
    const at = (Date.now() - minutesAgo * 60_000) / 1000;
    utimesSync(cache.workspace("main", workspace), at, at);
  });

test("the node's cache keeps within its budget, evicting the least recently used workspace first, but none used within its grace", async () => {
  const root = mkdtempSync(join(tmpdir(), "alasio-node-cache-"));
  try {
    const names = (cached: readonly { readonly workspace: string }[]) => cached.map(({ workspace }) => workspace).sort();
    await Effect.runPromise(Effect.gen(function*() {
      const cache = makeSnapshotCache({ root, budget: 2500, graceMs: 5 * 60_000 });
      yield* cacheImage(cache, "fs-old", D1, 60);
      yield* cacheImage(cache, "fs-older", D1, 90);
      yield* cacheImage(cache, "fs-new", D1, 1);
      // An image being written counts for nothing, and is never evicted.
      mkdirSync(join(root, "main", "fs-new", "layers", `.${S1}`));
      writeFileSync(join(root, "main", "fs-new", "layers", `.${S1}`, "pages.img"), Buffer.alloc(5000));
      assert.equal((yield* cache.list).reduce((sum, { bytes }) => sum + bytes, 0), 3000);
      assert.deepEqual((yield* cache.list).map(({ images }) => images), [[D1], [D1], [D1]]);
      assert.deepEqual(names(yield* cache.evict(sparesNone)), ["fs-older"]);
      assert.deepEqual(names(yield* cache.list), ["fs-new", "fs-old"]);
      // Prepared just now, the older is kept, and the newer one goes.
      const prepared = yield* cache.prepare("main", "fs-old", S1, [D1]);
      assert.deepEqual(readdirSync(prepared).sort(), ["layers", "pages.img"]);
      const tighter = makeSnapshotCache({ root, budget: 500, graceMs: 5 * 60_000 });
      assert.deepEqual(names(yield* tighter.evict(sparesNone)), []);
      const longAgo = (Date.now() - 10 * 60_000) / 1000;
      utimesSync(cache.workspace("main", "fs-new"), longAgo, longAgo);
      assert.deepEqual(names(yield* tighter.evict(sparesNone)), ["fs-new"]);
      assert.deepEqual(names(yield* tighter.list), ["fs-old"]);
      // An image prepared is counted once, however many links it has.
      assert.equal((yield* tighter.list)[0]?.bytes, 1000);
      // Discarded, but what was used within its grace, which a restore may be reading.
      yield* cacheImage(tighter, "fs-old", D2, 0);
      assert.equal(yield* tighter.discard("main", "fs-old", [D2]), false);
      utimesSync(cache.workspace("main", "fs-old"), longAgo, longAgo);
      assert.equal(yield* tighter.discard("main", "fs-old", [D2]), true);
      assert.deepEqual((yield* tighter.list).map(({ images }) => images), [[D2]]);
      assert.equal(existsSync(prepared), false);
      // What a stopped agent left half written goes as the next starts: an image, and a stamp.
      mkdirSync(join(root, "main", "fs-old", "layers", `.${S1}`));
      mkdirSync(join(root, "main", "fs-old", "stamps"), { recursive: true });
      writeFileSync(join(root, "main", "fs-old", "stamps", `.${D2}.json`), "{");
      yield* tighter.clean;
      assert.equal(existsSync(join(root, "main", "fs-old", "layers", `.${S1}`)), false);
      assert.equal(existsSync(join(root, "main", "fs-old", "stamps", `.${D2}.json`)), false);
      // An image staged uses its workspace, which a checkpoint writing it does; one the cache is told to spare is kept, whenever it was used.
      const none = makeSnapshotCache({ root, budget: 0, graceMs: 5 * 60_000 });
      utimesSync(cache.workspace("main", "fs-old"), longAgo, longAgo);
      yield* none.stage("main", "fs-old", S1);
      assert.deepEqual(names(yield* none.evict(sparesNone)), []);
      utimesSync(cache.workspace("main", "fs-old"), longAgo, longAgo);
      assert.deepEqual(names(yield* none.evict(spares("fs-old"))), []);
      assert.deepEqual(names(yield* none.evict(sparesNone)), ["fs-old"]);
    }));
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("the node's cache lists what it holds as the agent moves and removes it meanwhile, which counts for nothing", async () => {
  const root = mkdtempSync(join(tmpdir(), "alasio-cache-churn-"));
  try {
    const cache = makeSnapshotCache({ root, budget: 1024 ** 4, graceMs: 0 });
    const workspace = join(root, "main", "fs-abc");
    const image = (index: number) => join(workspace, "layers", sha256(Buffer.from(String(index))));
    const write = (index: number) => {
      mkdirSync(image(index), { recursive: true });
      writeFileSync(join(image(index), "pages.img"), "pages");
    };
    for (let index = 0; index < 64; index += 1) write(index);
    // Each image removed and written again, and now and then the workspace whole, as the agent does while it lists.
    let churning = true;
    const churn = (async () => {
      for (let round = 0; churning; round += 1) {
        for (let index = 0; index < 64; index += 1) {
          const whole = index % 16 === 0;
          rmSync(whole ? workspace : image(index), { recursive: true, force: true });
          // The workspace gone a while, as an eviction leaves it.
          if (whole) await new Promise((resolve) => setImmediate(resolve));
          write(index);
          await new Promise((resolve) => setImmediate(resolve));
        }
      }
    })();
    try {
      for (let listing = 0; listing < 200; listing += 1) {
        for (const { images, bytes } of await Effect.runPromise(cache.list)) assert.ok(images.length <= 64 && bytes <= 64 * 5, `${bytes} bytes of ${images.length} images`);
      }
    } finally {
      churning = false;
      await churn;
    }
    // At rest, each image written again, it holds each, and its bytes.
    for (let index = 0; index < 64; index += 1) write(index);
    assert.deepEqual((await Effect.runPromise(cache.list)).map(({ images, bytes }) => [images.length, bytes]), [[64, 64 * 5]]);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("a checkpoint makes room for as much as its pod may hold in memory, as its containers' limits say", () => {
  const limits = (...memory: (string | undefined)[]) => ({ spec: { containers: memory.map((each) => ({ resources: each === undefined ? {} : { limits: { memory: each } } })) } });
  assert.equal(memoryLimitOf(limits("3Gi")), 3 * 1024 ** 3);
  assert.equal(memoryLimitOf(limits("512Mi", "1.5G", undefined)), 512 * 1024 ** 2 + 1.5e9);
  assert.equal(memoryLimitOf(limits("1000", "lots")), 1000);
  assert.equal(memoryLimitOf({}), 0);
});

test("the node's cache makes room on its disk for what it writes, its reserve kept free, evicting as it would over its budget, or refuses", async () => {
  const root = mkdtempSync(join(tmpdir(), "alasio-node-room-"));
  try {
    // A disk of the reserve and 3500 bytes, less what the cache holds, in files of 1000 bytes.
    const held = () => readdirSync(root, { recursive: true, withFileTypes: true }).filter((entry) => entry.isFile()).length * 1000;
    const cache = makeSnapshotCache({ root, budget: 1024 ** 3, graceMs: 5 * 60_000, free: () => Promise.resolve(RESERVE_BYTES + 3500 - held()) });
    const names = (cached: readonly { readonly workspace: string }[]) => cached.map(({ workspace }) => workspace).sort();
    await Effect.runPromise(Effect.gen(function*() {
      yield* cacheImage(cache, "fs-a", D1, 60);
      yield* cacheImage(cache, "fs-b", D1, 90);
      yield* cacheImage(cache, "fs-c", D1, 1);
      // 500 bytes free besides the reserve: room for 400 as it is; for 1200, the least recently used goes, but not one used within the grace.
      assert.deepEqual(names(yield* cache.room(400, spares("fs-d"))), []);
      assert.deepEqual(names(yield* cache.room(1200, spares("fs-d"))), ["fs-b"]);
      // Nor one of the workspace written to.
      const refused = yield* Effect.flip(cache.room(2000, spares("fs-a")));
      assert.equal(refused.message, `the node's snapshot cache failed: its disk has ${RESERVE_BYTES + 1500} bytes free, and 2000 more would leave less than the ${RESERVE_BYTES} it keeps free`);
      assert.deepEqual(names(yield* cache.list), ["fs-a", "fs-c"]);
      assert.deepEqual(names(yield* cache.room(2000, spares("fs-d"))), ["fs-a"]);
    }));
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("a pod's address has kube-router's rules once a jump of what it sends, commented as the pod's, reaches a chain running it through a policy and rejecting the rest", () => {
  const address = { namespace: SESSIONS, pod: POD, ip: POD_IP, policies: SESSION_POLICIES };
  const gap = (table: string, of = address) => egressGap(table, of);
  const noJump = `KUBE-ROUTER-FORWARD has no jump of what it sends to the chain of ${SESSIONS}/${POD}`;
  const programmed = filterTable([clusterDns("10.42.0.3"), sessionPod("fs-other", "10.42.0.9"), sessionPod(POD, POD_IP)]);
  assert.equal(gap(programmed), null);
  // Matched by what the rules say, whatever a sync named the chains.
  assert.equal(gap(filterTable([sessionPod(POD, POD_IP)], { sync: "1791844099000000000" })), null);

  // Before a sync wrote them, or another pod's at the address, as a pod that had it before.
  assert.equal(gap(filterTable([clusterDns("10.42.0.3"), sessionPod("fs-other", "10.42.0.9")])), noJump);
  assert.equal(gap(filterTable([sessionPod("fs-old999", POD_IP)])), noJump);
  assert.equal(gap(filterTable([sessionPod("fs-abc1234", POD_IP)])), noJump);
  assert.equal(gap(filterTable([sessionPod(POD, POD_IP, "alasio-sessions-br-1")])), noJump);
  assert.equal(gap(programmed, { ...address, ip: "10.42.0.1" }), noJump);

  // A chain that runs what the pod sends through no policy of its own lets it all through.
  const chain = /KUBE-POD-FW-[A-Z2-7]{16}/u;
  const unselected = gap(filterTable([{ namespace: SESSIONS, pod: POD, ip: POD_IP, policies: [] }]), { ...address, policies: [] });
  assert.match(unselected ?? "", new RegExp(`^the chain of ${SESSIONS}/${POD}, ${chain.source}, runs what it sends through no network policy$`, "u"));
  const ingressOnly = { ...address, policies: ["bayma-from-alasio"] };
  assert.match(gap(filterTable([{ namespace: SESSIONS, pod: POD, ip: POD_IP, policies: [{ name: "bayma-from-alasio", type: "ingress" }] }]), ingressOnly) ?? "", /runs what it sends through no network policy$/u);
  assert.equal(gap(filterTable([{ namespace: SESSIONS, pod: POD, ip: POD_IP, policies: [{ name: "telemetry-to-alasio", type: "egress" }] }]), { ...address, policies: ["telemetry-to-alasio"] }), null);

  // The rules of a pod of its name before it, at its address, run through the policies that selected that one: until a sync, none of this one's.
  const before = filterTable([{ ...sessionPod(POD, POD_IP), policies: [...sessionPod(POD, POD_IP).policies, { name: "internet-from-full", type: "egress" }] }]);
  assert.match(
    gap(before) ?? "",
    new RegExp(`^the chain of ${SESSIONS}/${POD}, ${chain.source}, runs it through the network policies bayma-from-alasio, default-deny, internet-from-full, telemetry-to-alasio, not bayma-from-alasio, default-deny, telemetry-to-alasio, as a pod of its name before it was$`, "u"),
  );
  assert.match(gap(programmed, { ...address, policies: [...SESSION_POLICIES, "internet-from-full"] }) ?? "", /, as a pod of its name before it was$/u);

  // A jump commented as a policy's, to a chain that is none.
  const unpoliced = programmed.replace(/(nw policy [a-z-]+" -j )KUBE-NWPLCY-[A-Z2-7]{16}/gu, (_jump, kept: string) => `${kept}ACCEPT`);
  assert.match(gap(unpoliced) ?? "", /runs what it sends through no network policy$/u);

  // One that does not reject what no policy let through: without the rule, or with another.
  const rejection = `--comment "rule to REJECT traffic destined for POD name:${POD} namespace: ${SESSIONS}" -m mark ! --mark 0x10000/0x10000 -j REJECT`;
  const rejecting = (instead: string | null) =>
    gap(programmed.split("\n").flatMap((line) => (line.includes(rejection) ? (instead === null ? [] : [line.replace(rejection, instead)]) : [line])).join("\n"));
  const unrejected = /, does not reject what its network policies do not let through$/u;
  assert.match(rejecting(null) ?? "", unrejected);
  assert.match(rejecting(rejection.replace(" ! --mark", " --mark")) ?? "", unrejected);
  assert.match(rejecting(rejection.replace("0x10000/0x10000", "0x20000/0x20000")) ?? "", unrejected);
  assert.match(rejecting(rejection.replace("namespace: alasio-sessions", "namespace: alasio")) ?? "", unrejected);
  assert.equal(rejecting(rejection), null);

  // An IPv6 address, in its family's table, as a /128.
  const v6 = filterTable([sessionPod(POD, POD_IPV6)], { family: 6 });
  assert.match(v6, /-A KUBE-ROUTER-FORWARD -s fd00:42::f\/128 -m comment --comment "rule to jump traffic from POD name:fs-abc123 namespace: alasio-sessions to chain KUBE-POD-FW-/u);
  assert.equal(gap(v6, { ...address, ip: POD_IPV6 }), null);
  assert.equal(gap(programmed, { ...address, ip: POD_IPV6 }), noJump);
});

test("a resume waits for the pod's rules on every address, forgets the connections tracked from them, then resumes each container", () =>
  withBucket(({ s3, node }) =>
    Effect.gen(function*() {
      const here = yield* node("node-1");
      const snapshot = released(s3);
      const { host } = here;
      const programmed = host.ipv4;
      host.ipv4 = filterTable([clusterDns("10.42.0.3"), sessionPod("fs-old999", POD_IP)]);
      const resuming = yield* Effect.forkChild(here.agent.resume(resumeRequest(snapshot, [POD_IP, POD_IPV6])));
      yield* TestClock.adjust("1 second");
      // Looked for every 250 ms, its sandbox untouched meanwhile.
      assert.equal(resuming.pollUnsafe(), undefined);
      assert.deepEqual(host.ran.slice(0, 2), [["iptables-save", "-t", "filter"], ["ip6tables-save", "-t", "filter"]]);
      assert.equal(host.ran.length, 10);
      assert.deepEqual(here.releases.calls(RELEASE), []);

      host.ipv4 = programmed;
      host.answer = (command) => (command[0] === "conntrack" && command.includes(POD_IPV6) ? { exitCode: 0, stdout: "", stderr: "conntrack v1.4.8 (conntrack-tools): 2 flow entries have been deleted.\n" } : null);
      yield* TestClock.adjust("250 millis");
      yield* Fiber.join(resuming);
      assert.deepEqual(host.ran.slice(10), [
        ["iptables-save", "-t", "filter"],
        ["ip6tables-save", "-t", "filter"],
        ["conntrack", "-D", "-s", POD_IP],
        ["conntrack", "-D", "-s", POD_IPV6, "-f", "ipv6"],
      ]);
      assert.deepEqual(here.releases.calls(RELEASE), [
        [`--root=${RUNSC_ROOT}`, "--systemd-cgroup=true", "resume", CONTAINER],
        [`--root=${RUNSC_ROOT}`, "--systemd-cgroup=true", "resume", OTHER_CONTAINER],
      ]);
    }).pipe(Effect.provide(TestClock.layer()))
  ));

test("a resume whose pod's rules do not come within its wait, or whose commands fail, leaves the sandbox paused, saying why", () =>
  withBucket(({ s3, node }) =>
    Effect.gen(function*() {
      const here = yield* node("node-1");
      const snapshot = released(s3);
      const { host } = here;
      const programmed = host.ipv6;

      // Its IPv6 address's rules do not come.
      host.ipv6 = filterTable([sessionPod("fs-old999", POD_IPV6)], { family: 6 });
      const waiting = yield* Effect.forkChild(here.agent.resume(resumeRequest(snapshot, [POD_IP, POD_IPV6])));
      yield* TestClock.adjust("29 seconds");
      assert.equal(waiting.pollUnsafe(), undefined);
      yield* TestClock.adjust("1 second");
      assert.equal(
        failure(yield* Effect.exit(Fiber.join(waiting))),
        `the network policies of ${SESSIONS}/${POD} are not in force on this node after 30 s, its sandbox left paused: for ${POD_IPV6}, KUBE-ROUTER-FORWARD has no jump of what it sends to the chain of ${SESSIONS}/${POD}`,
      );
      assert.equal(host.ran.filter(([program]) => program === "conntrack").length, 0);
      assert.deepEqual(here.releases.calls(RELEASE), []);
      host.ipv6 = programmed;

      // The node has no ip6tables-save, as nsenter says.
      host.answer = ([program]) => (program === "ip6tables-save" ? { exitCode: 127, stdout: "", stderr: "nsenter: failed to execute ip6tables-save: No such file or directory\n" } : null);
      assert.equal(
        failure(yield* Effect.exit(here.agent.resume(resumeRequest(snapshot, [POD_IPV6])))),
        "ip6tables-save -t filter on the node failed: nsenter: failed to execute ip6tables-save: No such file or directory",
      );

      // conntrack fails, as it does deleting none.
      host.answer = ([program]) => (program === "conntrack" ? { exitCode: 1, stdout: "", stderr: "conntrack v1.4.8 (conntrack-tools): Operation failed: Operation not permitted\n" } : null);
      assert.equal(
        failure(yield* Effect.exit(here.agent.resume(resumeRequest(snapshot)))),
        `conntrack -D -s ${POD_IP} on the node failed: conntrack v1.4.8 (conntrack-tools): Operation failed: Operation not permitted`,
      );
      assert.deepEqual(here.releases.calls(RELEASE), []);
      host.answer = () => null;

      // runsc fails.
      writeFileSync(join(here.releases.dirOf(RELEASE), "fail-resume"), "");
      assert.equal(failure(yield* Effect.exit(here.agent.resume(resumeRequest(snapshot)))), `runsc resume of ${RELEASE} failed: resume failed: the container is gone`);
    }).pipe(Effect.provide(TestClock.layer()))
  ));

test("commands run on the node enter its mounts and network from its PID 1, with k3s's binaries on their PATH, and say how they exited", async () => {
  const root = mkdtempSync(join(tmpdir(), "alasio-node-host-"));
  try {
    const nsenter = join(root, "nsenter");
    writeFileSync(nsenter, '#!/bin/sh\necho "$*"\necho "PATH=$PATH"\nshift 5\nif [ "$1" = fail ]; then echo "it failed" >&2; exit 4; fi\n', { mode: 0o755 });
    const host = makeHostCommands(nsenter);
    assert.deepEqual(await Effect.runPromise(host.run(["iptables-save", "-t", "filter"])), {
      exitCode: 0,
      stdout: `--target 1 --mount --net -- iptables-save -t filter\nPATH=${NODE_PATH}\n`,
      stderr: "",
    });
    assert.equal(NODE_PATH, "/var/lib/rancher/k3s/data/current/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/var/lib/rancher/k3s/data/current/bin/aux:/bin/aux");
    assert.deepEqual(await Effect.runPromise(host.run(["fail"])), { exitCode: 4, stdout: `--target 1 --mount --net -- fail\nPATH=${NODE_PATH}\n`, stderr: "it failed\n" });
    const missing = await Effect.runPromise(Effect.flip(makeHostCommands(join(root, "none")).run(["conntrack", "-L"])));
    assert.match(missing.message, /^conntrack -L on the node failed: .*ENOENT/u);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

/** Workspaces no test of snapshots reaches. */
const unusedWorkspaces: Workspaces = {
  create: () => Effect.die("no workspace is made"),
  clone: () => Effect.die("no workspace is cloned"),
  remove: () => Effect.die("no workspace is deleted"),
  list: () => Effect.die("no workspace is listed"),
  environments: Effect.die("no environment is listed"),
  quota: () => Effect.die("no workspace's quota is read"),
  attachments: Effect.die("no attachment is looked at"),
  attach: () => Effect.die("no workspace is attached"),
  detach: () => Effect.die("no workspace is detached"),
  read: () => Effect.die("no workspace's file is read"),
  write: () => Effect.die("no workspace's file is written"),
  stop: () => Effect.die("no bayma is told to stop"),
  sync: () => Effect.die("no sandbox is told to write its files through"),
  released: () => Effect.die("no workspace's release is read"),
  setReleased: () => Effect.die("no workspace's release is recorded"),
};

test("the agent's API takes alasio's token alone, and answers each route as it says", () =>
  withBucket(({ s3, node }) =>
    Effect.gen(function*() {
      const here = yield* node("node-1");
      const { port } = yield* serveNodeAgent({ port: 0, host: "127.0.0.1", token: "the token", agent: here.agent, workspaces: unusedWorkspaces });
      const call = (method: string, path: string, body?: unknown, token = "the token") =>
        Effect.promise(async () => {
          const response = await fetch(`http://127.0.0.1:${port}${path}`, {
            method,
            headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
            ...(body === undefined ? {} : { body: typeof body === "string" ? body : JSON.stringify(body) }),
          });
          const text = await response.text();
          return { status: response.status, body: text ? (JSON.parse(text) as unknown) : null };
        });
      const error = (status: number, message: string | RegExp) => (answered: { readonly status: number; readonly body: unknown }) => {
        assert.equal(answered.status, status);
        const said = (answered.body as { readonly error: string }).error;
        if (typeof message === "string") assert.equal(said, message);
        else assert.match(said, message);
      };
      /** How the `operation` (checkpoints, prepare) of main's `workspace` stands once it has ended, asked until it has. */
      const ended = (operation: string, workspace: string) =>
        call("GET", `/${operation}/main/${workspace}`).pipe(
          Effect.repeat({ schedule: Schedule.spaced("20 millis"), while: ({ status, body }) => status === 200 && (body as { readonly state: string }).state === "running" }),
        );
      /** Starts the `operation` of `request`, which the agent answers it has, and how it ended. */
      const run = (operation: string, request: { readonly environment: string; readonly workspace: string; readonly [field: string]: unknown }) =>
        Effect.gen(function*() {
          assert.deepEqual(yield* call("POST", `/${operation === "checkpoints" ? "checkpoints" : "prepare"}`, request), { status: 202, body: { state: "running" } });
          return yield* ended(operation, request.workspace);
        });
      /** That `answered` says an operation failed with `status`, as `message` says. */
      const failed = (status: number, message: string | RegExp) => (answered: { readonly status: number; readonly body: unknown }) => {
        assert.equal(answered.status, 200);
        const { state, ...rest } = answered.body as { readonly state: string; readonly status: number; readonly error: string };
        assert.equal(state, "failed");
        error(status, message)({ status: rest.status, body: rest });
      };

      const health = yield* Effect.promise(() => fetch(`http://127.0.0.1:${port}/healthz`));
      assert.equal(health.status, 200);
      error(401, "the request bears no token of alasio's")(yield* call("GET", "/holdings", undefined, "another"));
      error(401, "the request bears no token of alasio's")(yield* Effect.promise(async () => {
        const response = await fetch(`http://127.0.0.1:${port}/holdings`);
        return { status: response.status, body: await response.json() };
      }));

      error(400, /hook/u)(yield* call("POST", "/checkpoints", { ...request("fs-abc"), hook: ["bayma sandbox-hook"] }));
      error(400, /containerId/u)(yield* call("POST", "/checkpoints", { ...request("fs-abc"), containerId: "short" }));
      error(400, "the body is not JSON")(yield* call("POST", "/checkpoints", "{"));
      const taken = yield* run("checkpoints", request("fs-abc"));
      assert.equal((taken.body as { readonly state: string }).state, "done");
      const manifest = (taken.body as { readonly value: Manifest }).value;
      assert.equal(manifest.workspace, "fs-abc");
      error(404, "the agent keeps no such operation of the workspace's")(yield* call("GET", "/checkpoints/main/fs-none"));
      error(400, /workspace/u)(yield* call("GET", "/checkpoints/main/fs_abc"));

      assert.deepEqual(yield* call("GET", "/snapshots/main/fs-abc"), { status: 200, body: manifest });
      error(404, "the workspace has no snapshot")(yield* call("GET", "/snapshots/main/fs-none"));
      error(400, /environment/u)(yield* call("GET", "/snapshots/Main/fs-abc"));

      const fork = { environment: "main", workspace: "fs-fork" };
      assert.deepEqual(yield* call("POST", "/copies", { from: { environment: "main", workspace: "fs-abc" }, to: fork }), {
        status: 200,
        body: { ...manifest, workspace: "fs-fork" },
      });
      error(404, "main/fs-none has no snapshot")(yield* call("POST", "/copies", { from: { environment: "main", workspace: "fs-none" }, to: fork }));
      error(400, /to/u)(yield* call("POST", "/copies", { from: { environment: "main", workspace: "fs-abc" }, to: { environment: "main" } }));
      error(400, /workspace/u)(yield* call("POST", "/copies", { from: { environment: "main", workspace: "../fs-abc" }, to: fork }));

      assert.deepEqual(yield* run("prepare", { environment: "main", workspace: "fs-abc" }), {
        status: 200,
        body: { state: "done", value: { manifest, imagePath: join(here.cacheDir, "main", "fs-abc", manifest.snapshot), source: "cache" } },
      });
      failed(404, "main/fs-none has no snapshot")(yield* run("prepare", { environment: "main", workspace: "fs-none" }));
      // How its checkpoint gofer reads the bucket, as alasio asks it, or a bad request.
      assert.equal((yield* run("prepare", { environment: "main", workspace: "fs-abc", gofer: { transport: "socket", pagesRetry: true } })).status, 200);
      error(400, /transport/u)(yield* call("POST", "/prepare", { environment: "main", workspace: "fs-abc", gofer: { transport: "udp", pagesRetry: true } }));
      error(400, /pagesRetry/u)(yield* call("POST", "/prepare", { environment: "main", workspace: "fs-abc", gofer: { transport: "socket" } }));

      const holdings = yield* call("GET", "/holdings");
      assert.equal(holdings.status, 200);
      const { cached, ...held } = holdings.body as { readonly cached: readonly { readonly usedAt: string }[] };
      assert.deepEqual(held, { node: "node-1", releases: [{ release: RELEASE, cpuFeatures: ["sse", "avx", "avx2"] }] });
      assert.deepEqual(cached.map(({ usedAt, ...rest }) => (assert.ok(Date.parse(usedAt) <= Date.now()), rest)), [
        // Its image, its manifest beside it, and its image's stamp.
        { environment: "main", workspace: "fs-abc", images: [manifest.layers[0]?.digest], bytes: manifest.bytes + JSON.stringify(manifest).length + statSync(join(here.cacheDir, "main", "fs-abc", "stamps", `${manifest.layers[0]?.digest}.json`)).size },
      ]);

      // Snapshots it cannot restore, or that are not what they say, and a bucket that fails.
      s3.objects.set("main/fs-abc/current.json", Buffer.from(JSON.stringify({ ...manifest, release: "20260801.0-eaucoin.1" })));
      failed(409, /its gVisor 20260801\.0-eaucoin\.1 is not installed here/u)(yield* run("prepare", { environment: "main", workspace: "fs-abc" }));
      s3.objects.set("main/fs-abc/current.json", Buffer.from("{}"));
      error(422, /^the snapshot of main\/fs-abc is corrupt: its current\.json is not a manifest/u)(yield* call("GET", "/snapshots/main/fs-abc"));
      s3.refuse = () => 503;
      failed(502, /^the bucket's read of main\/fs-abc\/current\.json failed: 503/u)(yield* run("prepare", { environment: "main", workspace: "fs-abc" }));
      error(502, /^the bucket's read of main\/fs-abc\/current\.json failed: 503/u)(yield* call("POST", "/copies", { from: { environment: "main", workspace: "fs-abc" }, to: fork }));
      s3.refuse = () => null;

      // The workspaces the bucket holds anything of; a node's cache discards what it is told, but the images kept.
      assert.deepEqual(yield* call("GET", "/snapshots/main"), { status: 200, body: { workspaces: ["fs-abc", "fs-fork"] } });
      error(400, /./u)(yield* call("GET", "/snapshots/Main"));
      const [image] = manifest.layers;
      assert.ok(image);
      error(400, /is no image's digest/u)(yield* call("DELETE", `/cache/main/fs-abc?keep=..%2F..%2Fx`));
      assert.deepEqual(yield* call("DELETE", `/cache/main/fs-abc?keep=${image.digest}`), { status: 200, body: { discarded: true } });
      assert.deepEqual(imagesOf(here, "fs-abc"), [image.digest]);
      assert.deepEqual(yield* call("DELETE", "/cache/main/fs-abc?keep="), { status: 200, body: { discarded: true } });
      assert.deepEqual(imagesOf(here, "fs-abc"), []);
      error(400, /workspace/u)(yield* call("DELETE", "/cache/main/fs_abc?keep="));

      // What no snapshot needs is collected: the images kept, those of the current snapshot; none while it cannot be read.
      error(422, /its current\.json is not a manifest/u)(yield* call("POST", "/collect", { environment: "main", workspace: "fs-abc" }));
      assert.equal(keysOf(s3, `main/fs-abc/layers/${image.digest}/`).length, 3);
      s3.objects.set("main/fs-abc/current.json", Buffer.from(JSON.stringify(manifest)));
      assert.deepEqual(yield* call("POST", "/collect", { environment: "main", workspace: "fs-abc" }), { status: 200, body: { kept: [image.digest] } });
      error(400, /workspace/u)(yield* call("POST", "/collect", { environment: "main" }));
      // Spent, it is current no more, but its images are kept; spending another spends nothing.
      assert.deepEqual(yield* call("DELETE", "/snapshots/main/fs-abc/current?snapshot=20261013T000000000Z-00000001"), { status: 204, body: null });
      assert.equal((yield* call("GET", "/snapshots/main/fs-abc")).status, 200);
      assert.equal((yield* call("DELETE", "/snapshots/main/fs-abc/current")).status, 404);
      assert.deepEqual(yield* call("DELETE", `/snapshots/main/fs-abc/current?snapshot=${manifest.snapshot}`), { status: 204, body: null });
      error(404, "the workspace has no snapshot")(yield* call("GET", "/snapshots/main/fs-abc"));
      assert.equal(keysOf(s3, `main/fs-abc/layers/${image.digest}/`).length, 3);

      assert.deepEqual(yield* call("DELETE", "/snapshots/main/fs-abc"), { status: 204, body: null });
      assert.deepEqual(keysOf(s3, "main/fs-abc/"), []);
      assert.equal(cachesAny(here, "fs-abc"), false);

      // An agent that stops prepares none from the bucket: its node's next agent does.
      yield* here.agent.checkpoint(request("fs-other", OTHER_CONTAINER));
      yield* here.cache.clear("main", "fs-other");
      yield* here.storeProxy.drained;
      failed(503, "the node agent is stopping, so it prepares no restore of main/fs-other from the bucket: its next one does")(yield* run("prepare", { environment: "main", workspace: "fs-other" }));

      error(400, /podIPs/u)(yield* call("POST", "/resume", { ...resumeRequest(RELEASED), podIPs: ["10.42.0.300"] }));
      error(400, /podIPs/u)(yield* call("POST", "/resume", { ...resumeRequest(RELEASED), podIPs: [] }));
      error(400, /containerIds/u)(yield* call("POST", "/resume", { ...resumeRequest(RELEASED), containerIds: ["short"] }));
      error(400, /pod/u)(yield* call("POST", "/resume", { ...resumeRequest(RELEASED), pod: "fs_abc123" }));
      assert.deepEqual(yield* call("POST", "/resume", resumeRequest(released(s3))), { status: 204, body: null });
      assert.deepEqual(here.releases.calls(RELEASE).slice(-2).map((args) => args.slice(-2)), [["resume", CONTAINER], ["resume", OTHER_CONTAINER]]);

      // One of a sandbox whose restore failed to read some of its pages, which runsc saves no more: unsaveable.
      writeFileSync(join(here.releases.dirOf(RELEASE), "fail-checkpoint"), "checkpoint failed: saving kernel: previous async page loading failed: GetObject: EIO");
      failed(410, /^the sandbox of main\/fs-abc can be saved no more, its restore having failed to read some of its pages: .*previous async page loading failed.*; its snapshot is forgotten/u)(yield* run("checkpoints", request("fs-abc")));
      failed(410, /^the sandbox of main\/fs-other can be saved no more, its restore having failed to read some of its pages: .*; its snapshot is kept, as a live checkpoint that fails changes nothing$/u)(yield* run("checkpoints", live("fs-other")));
      writeFileSync(join(here.releases.dirOf(RELEASE), "fail-checkpoint"), "");
      failed(500, /^checkpointing main\/fs-abc failed: .*its snapshot is forgotten/u)(yield* run("checkpoints", request("fs-abc")));
      // Its snapshot forgotten, the manifest of the one a resume names cannot be read: refused.
      error(410, /its manifest cannot be read, so whether it may run cannot be told/u)(yield* call("POST", "/resume", resumeRequest(RELEASED)));
      writeFileSync(join(here.releases.dirOf(RELEASE), "fail-resume"), "");
      error(500, `runsc resume of ${RELEASE} failed: resume failed: the container is gone`)(yield* call("POST", "/resume", resumeRequest(released(s3))));
      error(404, "there is no GET /snapshots")(yield* call("GET", "/snapshots"));
    })
  ));

/** alasio's client of the agents, of which there is `agent`, of node-1, served until the scope closes. */
const agentsOf = (agent: NodeAgent, workspaces: Workspaces = unusedWorkspaces) =>
  Effect.gen(function*() {
    const { port } = yield* serveNodeAgent({ port: 0, host: "127.0.0.1", token: "the token", agent, workspaces });
    const pod: V1Pod = {
      metadata: { name: "node-agent-1", namespace: "alasio" },
      spec: { nodeName: "node-1", containers: [] },
      status: { podIP: "127.0.0.1", conditions: [{ type: "Ready", status: "True" }] },
    };
    const unreachable = () => Effect.die("only pods are listed");
    const kube = KubeClient.of({ read: unreachable, list: () => Effect.succeed([pod]), create: unreachable, replace: unreachable, patch: unreachable, patchPodStatus: unreachable, remove: unreachable, exec: unreachable, watch: () => Stream.die("nothing is watched") });
    return yield* makeNodeAgents({ namespace: "alasio", selector: "app.kubernetes.io/component=node-agent", port }).pipe(
      Effect.provide(Layer.merge(Layer.succeed(KubeClient, kube), ConfigProvider.layer(ConfigProvider.fromEnv({ env: { ALASIO_NODE_AGENT_TOKEN: "the token" } })))),
    );
  });

test("alasio has a snapshot copied by any ready agent, and is answered its manifest, or why not", () =>
  withBucket(({ node }) =>
    Effect.gen(function*() {
      const here = yield* node("node-1");
      const agents = yield* agentsOf(here.agent);
      const from = { environment: "main", workspace: "fs-abc" };
      const fork = { environment: "main", workspace: "fs-fork" };

      const refused = yield* Effect.flip(agents.copy(from, fork));
      assert.equal(refused.message, "the node agent of node-1 could not copy main/fs-abc's snapshot to main/fs-fork: main/fs-abc has no snapshot");
      assert.equal(refused._tag === "NodeAgentError" && refused.status, 404);
      const source = yield* here.agent.checkpoint(request("fs-abc"));
      assert.deepEqual(yield* agents.copy(from, fork), { ...source, workspace: "fs-fork" });
    })
  ));

test("a checkpoint alasio stops waiting for goes on, one asked for meanwhile is that one, and alasio is answered how it ended, or why not", () =>
  withBucket(({ node }) =>
    Effect.gen(function*() {
      const here = yield* node("node-1");
      const agents = yield* agentsOf(here.agent);
      const held = join(here.releases.dirOf(RELEASE), "hold-checkpoint");
      const checkpoints = () => here.releases.calls(RELEASE).filter((args) => args.includes("checkpoint")).length;
      writeFileSync(held, "");
      // alasio asks, and gives up waiting, as it would restarting: runsc goes on.
      const asked = yield* Effect.forkChild(agents.checkpoint("node-1", request("fs-abc")));
      yield* Effect.sync(checkpoints).pipe(Effect.repeat({ schedule: Schedule.spaced("10 millis"), until: (count) => count === 1 }));
      yield* Fiber.interrupt(asked);
      // Asked again meanwhile, it is the same checkpoint, which alasio is answered with once it ends.
      const again = yield* Effect.forkChild(agents.checkpoint("node-1", request("fs-abc")));
      rmSync(held);
      const manifest = yield* Fiber.join(again);
      assert.equal(checkpoints(), 1);
      assert.deepEqual(yield* here.agent.current({ environment: "main", workspace: "fs-abc" }), manifest);
      // One that fails is answered with why, and the status the agent gives it.
      writeFileSync(join(here.releases.dirOf(RELEASE), "fail-checkpoint"), "");
      const refused = yield* Effect.flip(agents.checkpoint("node-1", request("fs-abc")));
      assert.match(refused.message, /^the node agent of node-1 could not snapshot main\/fs-abc: checkpointing main\/fs-abc failed: .*its snapshot is forgotten/u);
      assert.equal(refused._tag === "NodeAgentError" && refused.status, 500);
      // A prepare of a workspace without a snapshot is refused as the agent refuses it.
      const unprepared = yield* Effect.flip(agents.prepare("node-1", { environment: "main", workspace: "fs-none" }));
      assert.equal(unprepared.message, "the node agent of node-1 could not prepare main/fs-none's snapshot: main/fs-none has no snapshot");
      assert.equal(unprepared._tag === "NodeAgentError" && unprepared.status, 404);
    })
  ));

test("alasio has a restored sandbox resumed by its node's agent, or is told why not", () =>
  withBucket(({ s3, node }) =>
    Effect.gen(function*() {
      const here = yield* node("node-1");
      const snapshot = released(s3);
      yield* (yield* agentsOf(here.agent)).resume("node-1", resumeRequest(snapshot));
      assert.deepEqual(here.releases.calls(RELEASE).map((args) => args.slice(-2)), [["resume", CONTAINER], ["resume", OTHER_CONTAINER]]);

      // An agent whose wait ended answers 504.
      const late = new PoliciesNotInForce({ namespace: SESSIONS, pod: POD, ip: POD_IP, reason: "KUBE-ROUTER-FORWARD has no jump of what it sends" });
      const refused = yield* Effect.flip((yield* agentsOf({ ...here.agent, resume: () => Effect.fail(late) })).resume("node-1", resumeRequest(snapshot)));
      assert.equal(refused.message, `the node agent of node-1 could not resume main/fs-abc's sandbox: ${late.message}`);
      assert.equal(refused._tag === "NodeAgentError" && refused.status, 504);

      // One restored from a snapshot that holds the session no more answers 410.
      const outlived = new SnapshotOutlived({ environment: "main", workspace: "fs-abc", snapshot: "20261013T000000000Z-00000001", reason: "bayma's state generation is 6, and was 5 as it was saved" });
      const gone = yield* Effect.flip((yield* agentsOf({ ...here.agent, resume: () => Effect.fail(outlived) })).resume("node-1", resumeRequest(snapshot)));
      assert.equal(gone._tag === "NodeAgentError" && gone.status, 410);
    })
  ));

const abandonRequest: AbandonRequest = { environment: "main", workspace: "fs-abc", namespace: SESSIONS, pod: POD, uid: POD_UID };

/** A process of the test's on `node`, started as runsc would start it, named `argv0`, and the signal that ends it, once one does; killed as the scope closes. */
const runscProcess = (node: Node, argv0: string, args: readonly string[]) =>
  Effect.acquireRelease(
    Effect.sync(() => {
      const started = node.releases.starts(argv0, args);
      const ended = new Promise<NodeJS.Signals | null>((resolve) => started.once("exit", (_code, signal) => resolve(signal)));
      return { pid: started.pid ?? 0, ended, running: () => started.exitCode === null && started.signalCode === null, started };
    }),
    ({ started }) => Effect.sync(() => started.kill("SIGKILL")),
  );

/** The arguments runsc starts the sandbox of the container `id` with on `node`, under `root`, its bundle `bundle`. */
const sandboxArgs = (node: Node, id: string, root = RUNSC_ROOT, bundle = node.releases.bundleOf(id)) => [`--root=${root}`, "--dirty-tracking=wp", "boot", `--bundle=${bundle}`, "--controller-fd=3", id];

/** The arguments runsc starts the gofer of the container `id` with on `node`, which name no id but its bundle's. */
const goferArgs = (node: Node, id: string) => [`--root=${RUNSC_ROOT}`, "--dirty-tracking=wp", "gofer", "--bundle", node.releases.bundleOf(id), "--gofer-mount-confs=lisafs:self"];

test("an abandon kills what runsc runs for the pod's containers, their sandbox, their gofers and runsc's commands, and says what it killed", () =>
  withBucket(({ node }) =>
    Effect.gen(function*() {
      const here = yield* node("node-1");
      // The pod's first container's sandbox runs them all; each has its gofer; a restore of it waits on them.
      const sandbox = yield* runscProcess(here, "runsc-sandbox", sandboxArgs(here, CONTAINER));
      const gofer = yield* runscProcess(here, "runsc-gofer", goferArgs(here, CONTAINER));
      const otherGofer = yield* runscProcess(here, "runsc-gofer", goferArgs(here, OTHER_CONTAINER));
      const restoring = yield* runscProcess(here, "runsc", [`--root=${RUNSC_ROOT}`, "restore", "--image-path=/var/lib/alasio/snapshots/main/fs-abc/s-1", "--bundle", here.releases.bundleOf(CONTAINER), CONTAINER]);
      const byPid = (killed: readonly Killed[]) => killed.toSorted((a, b) => a.pid - b.pid);
      // Each as the node's /proc says it, running as runsc would, its command line copied there
      // once the kernel has set it (FakeReleases.starts). An abandon scanning a real one before
      // then finds no runsc's: one just exec'd, as a process of the container's forks and execs
      // one, which the scan after its kills finds (ABANDON_SCANS).
      for (const each of [sandbox, gofer, otherGofer, restoring]) {
        const said = readFileSync(join(here.releases.procRoot, String(each.pid), "cmdline"), "utf8").split("\0");
        assert.ok(each.running() && said[0]?.startsWith("runsc"), `${each.pid}: ${said.join(" ")}`);
      }
      let killed: readonly Killed[] = [];
      const counted = yield* Effect.promise(() =>
        added("alasio.node_agent.abandons", async () => {
          killed = await Effect.runPromise(here.agent.abandon(abandonRequest));
        })
      );
      assert.deepEqual(counted, { "alasio.node_agent.abandon.outcome=abandoned": 1 });
      assert.deepEqual(byPid(killed), byPid([
        { containerId: CONTAINER, pid: sandbox.pid },
        { containerId: CONTAINER, pid: gofer.pid },
        { containerId: OTHER_CONTAINER, pid: otherGofer.pid },
        { containerId: CONTAINER, pid: restoring.pid },
      ]));
      for (const each of [sandbox, gofer, otherGofer, restoring]) assert.equal(yield* Effect.promise(() => each.ended), "SIGKILL");

      // Asked by alasio, of a pod whose processes are gone, and then of one whose gofer runs on.
      const agents = yield* agentsOf(here.agent);
      assert.deepEqual(yield* agents.abandon("node-1", abandonRequest), []);
      const left = yield* runscProcess(here, "runsc-gofer", goferArgs(here, OTHER_CONTAINER));
      assert.deepEqual(yield* agents.abandon("node-1", abandonRequest), [{ containerId: OTHER_CONTAINER, pid: left.pid }]);
      assert.equal(yield* Effect.promise(() => left.ended), "SIGKILL");
      const refused = yield* Effect.flip(agents.abandon("node-1", { ...abandonRequest, pod: "fs_abc123" }));
      assert.equal(refused._tag === "NodeAgentError" && refused.status, 400);
      const unnamed = yield* Effect.flip(agents.abandon("node-1", { ...abandonRequest, uid: "pod-1" }));
      assert.equal(unnamed._tag === "NodeAgentError" && unnamed.status, 400);
    })
  ));

test("an abandon kills what a process it found forks meanwhile, which a scan after its kills finds, scanning until one kills nothing new", () =>
  withBucket(({ node }) =>
    Effect.gen(function*() {
      // As the agent's first scan returns, the restore it finds forks its gofer, which the
      // scan did not see.
      let fork = false;
      let child: ChildProcess | undefined;
      const scans: number[] = [];
      const here = yield* node("node-1", {
        runsc: (runsc, releases) => ({
          ...runsc,
          processes: (containers) =>
            runsc.processes(containers).pipe(
              Effect.tap((found) =>
                Effect.sync(() => {
                  scans.push(found.length);
                  if (!fork) return;
                  fork = false;
                  child = releases.starts("runsc-gofer", goferArgs(here, CONTAINER));
                })
              ),
            ),
        }),
      });
      yield* Effect.addFinalizer(() => Effect.sync(() => child?.kill("SIGKILL")));
      const restoring = yield* runscProcess(here, "runsc", [`--root=${RUNSC_ROOT}`, "restore", "--image-path=/var/lib/alasio/snapshots/main/fs-abc/s-1", "--bundle", here.releases.bundleOf(CONTAINER), CONTAINER]);
      fork = true;
      const killed = yield* here.agent.abandon(abandonRequest);
      const gofer = child;
      assert.ok(gofer?.pid, "no gofer was forked");
      const ended = new Promise<NodeJS.Signals | null>((resolve) => {
        if (gofer.exitCode !== null || gofer.signalCode !== null) resolve(gofer.signalCode);
        else gofer.once("exit", (_code, signal) => resolve(signal));
      });
      assert.deepEqual(killed, [{ containerId: CONTAINER, pid: restoring.pid }, { containerId: CONTAINER, pid: gofer.pid }]);
      assert.equal(yield* Effect.promise(() => restoring.ended), "SIGKILL");
      assert.equal(yield* Effect.promise(() => ended), "SIGKILL");
      // The first scan found the restore, the second its gofer, and the third, killing nothing new, ended them.
      assert.equal(scans.length, 3);
    })
  ));

test("an abandon kills no process of another pod's container, of a pod of its name made since, of no container's, of another root or bundle, or whose start time has changed since it was found", () =>
  withBucket(({ node }) =>
    Effect.gen(function*() {
      // The node gives the pid of the process `restarting` names to another as the agent finds them.
      let restarting = 0;
      const here = yield* node("node-1", {
        runsc: (runsc, releases) => ({ ...runsc, processes: (containers) => runsc.processes(containers).pipe(Effect.tap(() => Effect.sync(() => releases.restarted(restarting)))) }),
      });
      const OTHER_POD_CONTAINER = "0f".repeat(32);
      const UNKNOWN = "c".repeat(64);
      here.releases.runs(OTHER_POD_CONTAINER, RELEASE, SESSIONS, "fs-other");
      // Of a pod of its namespace and name, but another uid, as one made since is.
      const RECREATED_CONTAINER = "1e".repeat(32);
      here.releases.runs(RECREATED_CONTAINER, RELEASE, SESSIONS, POD, true, [], "0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d");
      const spared = [
        yield* runscProcess(here, "runsc-sandbox", sandboxArgs(here, OTHER_POD_CONTAINER)),
        yield* runscProcess(here, "runsc-sandbox", sandboxArgs(here, RECREATED_CONTAINER)),
        yield* runscProcess(here, "runsc-sandbox", sandboxArgs(here, UNKNOWN)),
        yield* runscProcess(here, "runsc-sandbox", sandboxArgs(here, CONTAINER, "/run/containerd/runsc/other")),
        yield* runscProcess(here, "runsc-sandbox", sandboxArgs(here, CONTAINER, RUNSC_ROOT, here.releases.bundleOf(UNKNOWN))),
        // Its bundle given another flag than --bundle.
        yield* runscProcess(here, "runsc", [`--root=${RUNSC_ROOT}`, `--log=${here.releases.bundleOf(CONTAINER)}`, "events", CONTAINER]),
      ];
      const reused = yield* runscProcess(here, "runsc-gofer", goferArgs(here, OTHER_CONTAINER));
      restarting = reused.pid;
      const gofer = yield* runscProcess(here, "runsc-gofer", goferArgs(here, CONTAINER));
      assert.deepEqual(yield* here.agent.abandon(abandonRequest), [{ containerId: CONTAINER, pid: gofer.pid }]);
      assert.equal(yield* Effect.promise(() => gofer.ended), "SIGKILL");
      // Each would have been killed by now.
      yield* Effect.sleep("200 millis");
      for (const each of [...spared, reused]) assert.ok(each.running(), `${each.pid} runs on`);
    })
  ));

test("as the agent stops, an operation it runs is let end before what it runs in is closed", () =>
  Effect.gen(function*() {
    const scope = yield* Scope.make();
    const operations = yield* makeOperations<string, never>(scope, () => ({ status: 500, error: "failed" }));
    const done: string[] = [];
    yield* operations.start("main/fs-abc", Effect.sleep("100 millis").pipe(Effect.andThen(Effect.sync(() => done.push("saved"))), Effect.as("saved")));
    yield* Scope.close(scope, Exit.void);
    assert.deepEqual(done, ["saved"]);
  }).pipe(Effect.runPromise));

test("the agent runs as its own process from its environment, and stops when told", async () => {
  const s3 = await startFakeS3({ bucket: "snapshots", region: "us-east-1", ...CREDENTIALS });
  // Its socket and tokens in /tmp, as the node's /run is not the test's to write.
  const runDir = mkdtempSync(join("/tmp", "alasio-agent-"));
  const env = {
    ALASIO_NODE_AGENT_RUN_DIR: runDir,
    PATH: process.env["PATH"],
    NODE_NAME: "node-1",
    POD_IP: "127.0.0.1",
    ALASIO_NODE_AGENT_TOKEN: "the token",
    ALASIO_BACKGROUND_READ_SECONDS: "600",
    S3_ENDPOINT: s3.endpoint,
    S3_REGION: "us-east-1",
    S3_BUCKET: "snapshots",
    AWS_ACCESS_KEY_ID: CREDENTIALS.accessKeyId,
    AWS_SECRET_ACCESS_KEY: CREDENTIALS.secretAccessKey,
  };
  const start = (environment: NodeJS.ProcessEnv) => {
    const agent = spawn(process.execPath, [join(import.meta.dirname, "../src/node-agent/main.ts")], { env: environment, stdio: ["ignore", "pipe", "pipe"] });
    let output = "";
    agent.stdout.on("data", (chunk: Buffer) => (output += chunk.toString()));
    agent.stderr.on("data", (chunk: Buffer) => (output += chunk.toString()));
    const exited = new Promise<number | null>((resolve) => agent.once("exit", resolve));
    return { agent, exited, output: () => output };
  };
  try {
    const { NODE_NAME: _node, ...unnamed } = env;
    const refused = start(unnamed);
    assert.equal(await refused.exited, 1);
    assert.match(refused.output(), /NODE_NAME/u);

    const running = start(env);
    const holdings = await (async () => {
      for (;;) {
        const response = await fetch(`http://127.0.0.1:${NODE_AGENT_PORT}/holdings`, { headers: { authorization: "Bearer the token" } }).catch(() => null);
        if (response) return response.json();
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
    })();
    assert.deepEqual(holdings, { node: "node-1", releases: [], cached: [] });
    // And the bucket, to its node, on its pod's address: refusing what no restore's token signs.
    const unsigned = await fetch(`http://127.0.0.1:${STORE_PROXY_PORT}/snapshots/main/fs-abc/current.json`);
    assert.equal(unsigned.status, 403);
    assert.match(await unsigned.text(), /<Code>AccessDenied<\/Code>/u);
    // And on its socket, only root's.
    assert.equal((statSync(join(runDir, "store.sock")).mode & 0o777).toString(8), "600");
    running.agent.kill("SIGTERM");
    assert.equal(await running.exited, 0);
    assert.match(running.output(), /\[node-agent\] the node agent of node-1 serves on port 8484, and the snapshots bucket to its node on 127\.0\.0\.1:8485, its cache within 5368709120 bytes/u);
    assert.ok(!existsSync(join(runDir, "store.sock")));
  } finally {
    await s3.close();
    rmSync(runDir, { recursive: true, force: true });
  }
});

test("a manifest whose names, images' digests or files' paths are not the agent's, whose size is not its images', or which names another workspace, is no snapshot, and nothing is written outside the cache", () =>
  withBucket(({ s3, node }) =>
    Effect.gen(function*() {
      const here = yield* node("node-1");
      const taken = yield* here.agent.checkpoint(request("fs-abc"));
      const outside = join(here.cacheDir, "..", "escaped");
      const corrupted = (manifest: Record<string, unknown>) =>
        Effect.gen(function*() {
          s3.objects.set("main/fs-abc/current.json", Buffer.from(JSON.stringify(manifest)));
          rmSync(join(here.cacheDir, "main", "fs-abc"), { recursive: true, force: true });
          return failure(yield* Effect.exit(here.agent.prepare({ environment: "main", workspace: "fs-abc" })));
        });
      const [image] = taken.layers;
      assert.ok(image);
      const [file] = image.files;
      for (const manifest of [
        { ...taken, layers: [{ ...image, files: [{ ...file, path: "../../escaped" }] }] },
        { ...taken, layers: [{ ...image, files: [{ ...file, path: "/etc/escaped" }] }] },
        { ...taken, layers: [{ ...image, digest: "../../escaped" }] },
        { ...taken, layers: [] },
        { ...taken, snapshot: "../../escaped" },
        { ...taken, environment: "../escaped" },
        // Smaller than its images, so the disk would be made too little room for them.
        { ...taken, bytes: 1 },
      ]) {
        assert.match(yield* corrupted(manifest), /is not a manifest/u);
      }
      assert.match(yield* corrupted({ ...taken, workspace: "fs-other" }), /names main\/fs-other/u);
      assert.equal(existsSync(outside), false);
      assert.equal(existsSync("/etc/escaped"), false);
    })
  ));

test("the node's cache builds no path but of the names the agent gives", () => {
  const cache = makeSnapshotCache({ root: "/var/lib/alasio/snapshots", budget: 1, graceMs: 0 });
  assert.equal(cache.image("main", "fs-abc", D1), `/var/lib/alasio/snapshots/main/fs-abc/layers/${D1}`);
  for (const [environment, workspace, digest] of [["..", "fs-abc", D1], ["main", "../x", D1], ["main", "fs-abc", "../../x"], ["main", "fs-abc", ".x"]] as const) {
    assert.throws(() => cache.image(environment, workspace, digest), /names no/u);
  }
  assert.throws(() => cache.stage("main", "fs-abc", "../../x"), /names no/u);
});

test("a checkpoint is taken under the release that runs the container, whatever runs its pod's RuntimeClass now, and of no container but the named pod's", () =>
  withBucket(({ node }) =>
    Effect.gen(function*() {
      const older = "20260801.0-eaucoin.1";
      const here = yield* node("node-1", { releases: [RELEASE, older] });
      // Started before the release was bumped, the pod's sandbox runs the release before.
      here.releases.runs(CONTAINER, older, SESSIONS, POD);
      const taken = yield* here.agent.checkpoint(request("fs-abc"));
      assert.equal(taken.release, older);
      assert.equal(here.releases.calls(older).filter(([, , command]) => command === "checkpoint").length, 1);
      assert.deepEqual(here.releases.calls(RELEASE).filter(([, , command]) => command === "checkpoint"), []);
      // Another pod's container is refused, as is one no release runs.
      here.releases.runs(OTHER_CONTAINER, RELEASE, SESSIONS, "fs-other");
      assert.match(failure(yield* Effect.exit(here.agent.checkpoint(request("fs-abc", OTHER_CONTAINER)))), /is not of the pod alasio-sessions\/fs-abc123, but of alasio-sessions\/fs-other/u);
      assert.match(failure(yield* Effect.exit(here.agent.resume(resumeRequest(RELEASED)))), /is not of the pod alasio-sessions\/fs-abc123/u);
      assert.match(failure(yield* Effect.exit(here.agent.checkpoint(request("fs-abc", "c".repeat(64))))), /container does not exist/u);
    })
  ));

test("the agent's API attaches, detaches, reads and writes a workspace's files as its workspaces say, refusing as they refuse", () =>
  withBucket(({ node }) =>
    Effect.gen(function*() {
      const here = yield* node("node-1");
      const at = workspacesNode();
      yield* Effect.addFinalizer(() => Effect.sync(at.done));
      at.files("fs-abc123");
      at.pod("pool-0");
      const { port } = yield* serveNodeAgent({ port: 0, host: "127.0.0.1", token: "the token", agent: here.agent, workspaces: at.workspaces });
      const call = (method: string, path: string, body?: string) =>
        Effect.promise(() => fetch(`http://127.0.0.1:${port}${path}`, { method, headers: { authorization: "Bearer the token" }, ...(body === undefined ? {} : { body }) }));
      const json = (value: unknown) => JSON.stringify(value);
      assert.equal((yield* call("POST", "/attach", json({ environment: "main", workspace: "fs-abc123", pod: "pool-0", token: "binding", env: {} }))).status, 204);
      assert.equal((yield* call("POST", "/attach", json({ environment: "main", workspace: "fs-abc123", pod: "pool-0", token: "binding", env: {} }))).status, 409);
      assert.equal((yield* call("POST", "/attach", json({ environment: "main", workspace: "fs-abc123" }))).status, 400);
      assert.equal((yield* call("PUT", "/files/main/fs-abc123?path=home/.alasio/received/1/note.txt", "hello")).status, 204);
      const read = yield* call("GET", "/files/main/fs-abc123?path=home/.alasio/received/1/note.txt&max=100");
      assert.deepEqual([read.status, yield* Effect.promise(() => read.text())], [200, "hello"]);
      assert.equal((yield* call("GET", "/files/main/fs-abc123?path=home/.alasio/received/1/note.txt&max=2")).status, 413);
      assert.equal((yield* call("GET", "/files/main/fs-abc123?path=../../etc/passwd")).status, 400);
      at.running.add("container-0");
      assert.equal((yield* call("POST", "/detach", json({ pod: "pool-0", containerIds: ["container-0"] }))).status, 409);
      at.running.delete("container-0");
      assert.equal((yield* call("POST", "/detach", json({ pod: "pool-0", containerIds: ["container-0"] }))).status, 204);
      assert.deepEqual(at.mounts, []);
    })
  ));

test("alasio attaches and detaches a workspace through the node's agent, and reads and writes its files through any, the bytes as they are", () =>
  withBucket(({ node }) =>
    Effect.gen(function*() {
      const here = yield* node("node-1");
      const at = workspacesNode();
      yield* Effect.addFinalizer(() => Effect.sync(at.done));
      at.files("fs-abc123");
      at.pod("pool-0");
      const agents = yield* agentsOf(here.agent, at.workspaces);
      const ref = { environment: "main", workspace: "fs-abc123" };
      yield* agents.attach("node-1", { ...ref, pod: "pool-0", token: "binding", env: {} });
      const again = yield* Effect.flip(agents.attach("node-1", { ...ref, pod: "pool-0", token: "binding", env: {} }));
      assert.equal(again._tag === "NodeAgentError" && again.status, 409);
      // What is attached on it, through its node's agent, reachable until its mount is not.
      assert.deepEqual(yield* agents.attachments("node-1"), [{ pod: "pool-0", reachable: true }]);
      at.unreachable.add("pool-0");
      assert.deepEqual(yield* agents.attachments("node-1"), [{ pod: "pool-0", reachable: false }]);
      at.unreachable.delete("pool-0");
      const bytes = Uint8Array.from([0, 255, 10, 13, 128]);
      yield* agents.writeFile(ref, "home/.alasio/received/1/blob", bytes);
      assert.deepEqual(yield* agents.readFile(ref, "home/.alasio/received/1/blob", 100), bytes);
      const tooLarge = yield* Effect.flip(agents.readFile(ref, "home/.alasio/received/1/blob", 2));
      assert.equal(tooLarge._tag === "NodeAgentError" && tooLarge.status, 413);
      at.running.add("container-0");
      // Its bayma told to go on with its stop, through its node's agent; another pod's bayma not.
      at.annotations.set("container-0", { "io.kubernetes.cri.sandbox-namespace": "alasio-sessions", "io.kubernetes.cri.sandbox-name": "pool-0", "io.kubernetes.cri.container-name": "bayma" });
      const hook = ["bayma", "sandbox-hook", "stop", "--state-dir", "/home/agent/.bayma"];
      yield* agents.stop("node-1", { namespace: "alasio-sessions", pod: "pool-0", containerId: "container-0", hook });
      assert.deepEqual(at.execs, [`r container-0 ${hook.join(" ")}`]);
      const notItsPod = yield* Effect.flip(agents.stop("node-1", { namespace: "alasio-sessions", pod: "pool-1", containerId: "container-0", hook }));
      assert.equal(notItsPod._tag === "NodeAgentError" && notItsPod.status, 403);
      const running = yield* Effect.flip(agents.detach("node-1", "pool-0", ["container-0"]));
      assert.equal(running._tag === "NodeAgentError" && running.status, 409);
      at.running.delete("container-0");
      yield* agents.detach("node-1", "pool-0", ["container-0"]);
      assert.deepEqual(at.mounts, []);
      // A workspace made through any agent, its quota read back; one that has none, none.
      const made = { environment: "main", workspace: "fs-made" };
      yield* agents.createWorkspace(made, 5 * 1024 ** 3);
      assert.deepEqual(yield* agents.quota(made), { capacityBytes: 5 * 1024 ** 3, usedBytes: 12 * 1024 ** 2 });
      const refusedMade = yield* Effect.flip(agents.createWorkspace({ environment: "main", workspace: "../etc" }, 1024));
      assert.equal(refusedMade._tag === "NodeAgentError" && refusedMade.status, 400);
      // Cloned, and deleted, through any agent.
      const fork = { environment: "main", workspace: "fs-fork" };
      yield* agents.cloneWorkspace(made, fork, 1024 ** 3);
      assert.deepEqual((yield* agents.quota(fork))?.capacityBytes, 1024 ** 3);
      assert.deepEqual(yield* agents.listWorkspaces("main"), ["fs-abc123", "fs-fork", "fs-made"]);
      // The environments it holds workspaces of, through any agent.
      assert.deepEqual(yield* agents.listEnvironments, ["main"]);
      yield* agents.removeWorkspace(fork);
      assert.equal(yield* agents.quota(fork), null);
      // The snapshot one was released into, recorded and read back through any agent, and cleared.
      const into = { snapshot: "20261017T120000000Z-0000000a", release: "20260921.0-eaucoin.8" };
      assert.equal(yield* agents.released(made), null);
      yield* agents.setReleased(made, into);
      assert.deepEqual(yield* agents.released(made), into);
      yield* agents.setReleased(made, null);
      assert.equal(yield* agents.released(made), null);
      const unrecorded = yield* Effect.flip(agents.setReleased(fork, into));
      assert.equal(unrecorded._tag === "NodeAgentError" && unrecorded.status, 404);
    })
  ));
