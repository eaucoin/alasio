/**
 * The node agent (src/node-agent/): its checkpoints, committed to a stand-in bucket and
 * forgotten when they fail, its copies, its prepared snapshots, its cache, its resumes of
 * restored sandboxes once kube-router's rules for their pods are in force, its API, as
 * alasio's client (src/sandbox/node-agents.ts) calls it too, and its process, with runsc a
 * stand-in of its own (./support/fake-runsc.ts) and the node's commands answered from
 * fixtures (./support/kube-router-rules.ts).
 */
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, readlinkSync, rmSync, utimesSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import type { V1Pod } from "@kubernetes/client-node";
import { ConfigProvider, Effect, Exit, Fiber, Layer, Schedule, Scope } from "effect";
import { TestClock } from "effect/testing";

// First: alasio's modules create their metrics as they load.
import { added, collected } from "./support/metrics.ts";
import { KubeClient } from "../src/kube/client.ts";
import { SNAPSHOT_ID } from "../src/kube/sandbox-shape.ts";
import { type CheckpointRequest, COLLECT_GRACE_MS, type Layer as Image, makeNodeAgent, type Manifest, memoryLimitOf, type NodeAgent, PoliciesNotInForce, type ResumeRequest } from "../src/node-agent/agent.ts";
import { makeSnapshotCache, RESERVE_BYTES, type SnapshotCache } from "../src/node-agent/cache.ts";
import { type HostCommands, type HostRun, makeHostCommands, NODE_PATH } from "../src/node-agent/host.ts";
import { egressGap } from "../src/node-agent/kube-router.ts";
import { makeRunsc, runscFlags } from "../src/node-agent/runsc.ts";
import { makeS3Bucket } from "../src/node-agent/s3.ts";
import { NODE_AGENT_PORT } from "../src/node-agent/names.ts";
import { makeOperations } from "../src/node-agent/operations.ts";
import { serveNodeAgent } from "../src/node-agent/server.ts";
import { makeNodeAgents } from "../src/sandbox/node-agents.ts";
import { type FakeReleases, fakeReleases } from "./support/fake-runsc.ts";
import { type FakeS3, startFakeS3 } from "./support/fake-s3.ts";
import { clusterDns, filterTable, sessionPod } from "./support/kube-router-rules.ts";

const RELEASE = "20260921.0-eaucoin.3";
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

const SESSIONS = "alasio-sessions";
const POD = "fs-abc123";

const sha256 = (data: string | Buffer): string => createHash("sha256").update(data).digest("hex");

const POD_IP = "10.42.0.15";
const POD_IPV6 = "fd00:42::f";

/** The network policies that select a session's pod, as sessionPod's rules run it through them. */
const SESSION_POLICIES = ["default-deny", "bayma-from-alasio", "telemetry-to-alasio"];

const resumeRequest = (podIPs: readonly string[] = [POD_IP], containerIds: readonly string[] = [CONTAINER, OTHER_CONTAINER]): ResumeRequest => ({
  environment: "main",
  workspace: "fs-abc",
  namespace: SESSIONS,
  pod: POD,
  podIPs,
  containerIds,
  policies: SESSION_POLICIES,
});

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

/** A node: its releases, its cache, the commands run on it, and its agent on the shared bucket. */
interface Node {
  readonly releases: FakeReleases;
  readonly cacheDir: string;
  readonly cache: SnapshotCache;
  readonly host: FakeHost;
  readonly agent: NodeAgent;
}

/** What a node of the test's has: its releases, and its cache's budget. */
interface NodeOptions {
  readonly releases?: readonly string[];
  readonly budget?: number;
}

/** A bucket and a directory of the test's, and nodes made on them, removed after `body`. */
async function withBucket(body: (context: { readonly s3: FakeS3; readonly node: (name: string, options?: NodeOptions) => Effect.Effect<Node, never, Scope.Scope> }) => Effect.Effect<void, unknown, Scope.Scope>): Promise<void> {
  const root = mkdtempSync(join(tmpdir(), "alasio-node-agent-"));
  const s3 = await startFakeS3({ bucket: "snapshots", region: "us-east-1", ...CREDENTIALS });
  const bucket = makeS3Bucket({ endpoint: s3.endpoint, region: "us-east-1", bucket: "snapshots", ...CREDENTIALS });
  const node = (name: string, { releases: installed = [RELEASE], budget = 1024 ** 3 }: NodeOptions = {}) =>
    Effect.gen(function*() {
      const releases = fakeReleases(join(root, name), installed);
      const cacheDir = join(root, name, "cache");
      const cache = makeSnapshotCache({ root: cacheDir, budget, graceMs: 0 });
      const runsc = makeRunsc({ releasesDir: releases.releasesDir, containerdDir: releases.containerdDir, root: RUNSC_ROOT, procRoot: releases.procRoot });
      // The pod's containers run under the node's first release.
      const [running] = installed;
      if (running !== undefined) for (const id of [CONTAINER, OTHER_CONTAINER]) releases.runs(id, running, SESSIONS, POD);
      const host = fakeHost();
      return { releases, cacheDir, cache, host, agent: yield* makeNodeAgent({ node: name, runsc, cache, bucket, host: host.host }) };
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

/** The files of an image the stand-in's runsc writes, in order. */
const IMAGE_FILES = ["checkpoint.img", "pages.img", "pages_meta.img"];

const md5 = (data: Buffer): string => createHash("md5").update(data).digest("hex");

/** The files of the image in `directory`, as a manifest says them once the stand-in bucket holds them. */
const filesOf = (directory: string) =>
  IMAGE_FILES.map((path) => {
    const data = readFileSync(join(directory, path));
    return { path, bytes: data.length, sha256: sha256(data), etag: `"${md5(data)}"` };
  });

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
          yield* there.agent.prepare(workspace);
          yield* there.agent.spend(workspace);
          yield* there.agent.resume(resumeRequest());
        });
      yield* restored();
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
      // Restored on a third node, it is prepared from both images: its own, and the one it refers to where runsc finds it.
      const third = yield* node("node-3");
      const prepared = yield* third.agent.prepare(workspace);
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
      yield* there.agent.spend(workspace);
      yield* there.agent.resume(resumeRequest());
      const delta = yield* there.agent.checkpoint(request("fs-abc"));
      const [own] = delta.layers;
      assert.ok(own);
      assert.deepEqual(delta.layers.slice(1), base.layers);
      // Its base, looked at after its own image, changed on the disk.
      writeFileSync(join(there.cacheDir, "main", "fs-abc", "layers", baseImage.digest, "pages_meta.img"), "changed on the disk");
      const asked = s3.requests.length;
      const prepared = yield* there.agent.prepare(workspace);
      assert.deepEqual(prepared.manifest, delta);
      assert.deepEqual(s3.requests.slice(asked).filter((each) => each.startsWith("GET main/fs-abc/layers/")), baseImage.files.map(({ path }) => `GET main/fs-abc/layers/${baseImage.digest}/${path}`));
      assert.deepEqual(filesOf(prepared.imagePath), own.files);
      assert.deepEqual(filesOf(join(prepared.imagePath, "layers", baseImage.digest)), baseImage.files);
      // The delta was of the base it was restored from.
      assert.deepEqual(manifest, base);
    })
  ));

test("a snapshot is committed only once each image it refers to is in the bucket as it was put, put again from the cache where it is not", () =>
  withBucket(({ s3, node }) =>
    Effect.gen(function*() {
      const workspace = { environment: "main", workspace: "fs-abc" };
      const taker = yield* node("node-1");
      const there = yield* node("node-2");
      const restored = () =>
        Effect.gen(function*() {
          yield* there.agent.prepare(workspace);
          yield* there.agent.resume(resumeRequest());
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

test("a snapshot is prepared from the node's cache, or from the bucket, checked file by file, and only where it restores", () =>
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

      // The node that took it has it.
      assert.deepEqual(yield* taker.agent.prepare(workspace), { manifest, imagePath: join(taker.cacheDir, "main", "fs-abc", manifest.snapshot) });
      assert.deepEqual(downloads(), []);

      // Another node downloads it, once.
      const other = yield* node("node-2");
      const prepared = yield* other.agent.prepare(workspace);
      assert.deepEqual(prepared, { manifest, imagePath: join(other.cacheDir, "main", "fs-abc", manifest.snapshot) });
      assert.deepEqual(downloads(), files);
      assert.deepEqual(filesOf(prepared.imagePath), image.files);
      assert.deepEqual(JSON.parse(readFileSync(join(other.cacheDir, "main", "fs-abc", "manifest.json"), "utf8")), manifest);
      yield* other.agent.prepare(workspace);
      assert.deepEqual(downloads(), files);
      // A cached copy that is no longer what its manifest says, as a disk may change it, is read again from the bucket.
      writeFileSync(join(other.cacheDir, "main", "fs-abc", "layers", image.digest, "pages_meta.img"), "changed on the disk");
      assert.deepEqual(yield* other.agent.prepare(workspace), prepared);
      assert.deepEqual(downloads(), [...files, ...files]);
      assert.deepEqual(filesOf(prepared.imagePath), image.files);

      // A newer snapshot is prepared beside the older one's images, which a later collection discards.
      const newer = yield* taker.agent.checkpoint(request("fs-abc", OTHER_CONTAINER));
      yield* other.agent.prepare(workspace);
      assert.deepEqual(imagesOf(other, "fs-abc"), [image.digest, newer.layers[0]?.digest].sort());

      // A file that is not what the manifest says fails it, and leaves nothing of it.
      const [newerImage] = newer.layers;
      assert.ok(newerImage);
      s3.objects.set(`main/fs-abc/layers/${newerImage.digest}/pages_meta.img`, Buffer.from("something else"));
      const third = yield* node("node-3");
      const expected = newerImage.files.find(({ path }) => path === "pages_meta.img");
      assert.equal(
        failure(yield* Effect.exit(third.agent.prepare(workspace))),
        `the snapshot of main/fs-abc is corrupt: its image ${newerImage.digest}'s file pages_meta.img is 14 bytes of SHA-256 ${sha256("something else")}, where its manifest says ${expected?.bytes} bytes of ${expected?.sha256}`,
      );
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
      yield* here.agent.spend(workspace);
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
      // Another node prepares it from the bucket, each file what the source's is.
      const other = yield* node("node-2");
      assert.deepEqual(yield* other.agent.prepare(fork), { manifest: copied, imagePath: join(other.cacheDir, "br-1", "fs-fork", source.snapshot) });

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
      // Room for one workspace's snapshot of about 4 KB.
      const taker = yield* node("node-1", { budget: 6000 });
      yield* taker.agent.checkpoint(request("fs-abc"));
      yield* Effect.sleep("20 millis");
      yield* taker.agent.checkpoint(request("fs-other"));
      assert.deepEqual([cachesAny(taker, "fs-abc"), cachesAny(taker, "fs-other")], [false, true]);
      const other = yield* node("node-2", { budget: 6000 });
      yield* other.agent.prepare({ environment: "main", workspace: "fs-other" });
      yield* Effect.sleep("20 millis");
      yield* other.agent.prepare({ environment: "main", workspace: "fs-abc" });
      assert.deepEqual([cachesAny(other, "fs-abc"), cachesAny(other, "fs-other")], [true, false]);
    })
  ));

const S1 = "20261013T000000Z-00000001";
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
      assert.deepEqual(names(yield* cache.evict), ["fs-older"]);
      assert.deepEqual(names(yield* cache.list), ["fs-new", "fs-old"]);
      // Prepared just now, the older is kept, and the newer one goes.
      const prepared = yield* cache.prepare("main", "fs-old", S1, [D1]);
      assert.deepEqual(readdirSync(prepared).sort(), ["layers", "pages.img"]);
      const tighter = makeSnapshotCache({ root, budget: 500, graceMs: 5 * 60_000 });
      assert.deepEqual(names(yield* tighter.evict), []);
      const longAgo = (Date.now() - 10 * 60_000) / 1000;
      utimesSync(cache.workspace("main", "fs-new"), longAgo, longAgo);
      assert.deepEqual(names(yield* tighter.evict), ["fs-new"]);
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
      // What a stopped agent left half written goes as the next starts.
      mkdirSync(join(root, "main", "fs-old", "layers", `.${S1}`));
      yield* tighter.clean;
      assert.equal(existsSync(join(root, "main", "fs-old", "layers", `.${S1}`)), false);
    }));
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
      assert.deepEqual(names(yield* cache.room(400, { environment: "main", workspace: "fs-d" })), []);
      assert.deepEqual(names(yield* cache.room(1200, { environment: "main", workspace: "fs-d" })), ["fs-b"]);
      // Nor one of the workspace written to.
      const refused = yield* Effect.flip(cache.room(2000, { environment: "main", workspace: "fs-a" }));
      assert.equal(refused.message, `the node's snapshot cache failed: its disk has ${RESERVE_BYTES + 1500} bytes free, and 2000 more would leave less than the ${RESERVE_BYTES} it keeps free`);
      assert.deepEqual(names(yield* cache.list), ["fs-a", "fs-c"]);
      assert.deepEqual(names(yield* cache.room(2000, { environment: "main", workspace: "fs-d" })), ["fs-a"]);
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
  withBucket(({ node }) =>
    Effect.gen(function*() {
      const here = yield* node("node-1");
      const { host } = here;
      const programmed = host.ipv4;
      host.ipv4 = filterTable([clusterDns("10.42.0.3"), sessionPod("fs-old999", POD_IP)]);
      const resuming = yield* Effect.forkChild(here.agent.resume(resumeRequest([POD_IP, POD_IPV6])));
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
  withBucket(({ node }) =>
    Effect.gen(function*() {
      const here = yield* node("node-1");
      const { host } = here;
      const programmed = host.ipv6;

      // Its IPv6 address's rules do not come.
      host.ipv6 = filterTable([sessionPod("fs-old999", POD_IPV6)], { family: 6 });
      const waiting = yield* Effect.forkChild(here.agent.resume(resumeRequest([POD_IP, POD_IPV6])));
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
        failure(yield* Effect.exit(here.agent.resume(resumeRequest([POD_IPV6])))),
        "ip6tables-save -t filter on the node failed: nsenter: failed to execute ip6tables-save: No such file or directory",
      );

      // conntrack fails, as it does deleting none.
      host.answer = ([program]) => (program === "conntrack" ? { exitCode: 1, stdout: "", stderr: "conntrack v1.4.8 (conntrack-tools): Operation failed: Operation not permitted\n" } : null);
      assert.equal(
        failure(yield* Effect.exit(here.agent.resume(resumeRequest()))),
        `conntrack -D -s ${POD_IP} on the node failed: conntrack v1.4.8 (conntrack-tools): Operation failed: Operation not permitted`,
      );
      assert.deepEqual(here.releases.calls(RELEASE), []);
      host.answer = () => null;

      // runsc fails.
      writeFileSync(join(here.releases.dirOf(RELEASE), "fail-resume"), "");
      assert.equal(failure(yield* Effect.exit(here.agent.resume(resumeRequest()))), `runsc resume of ${RELEASE} failed: resume failed: the container is gone`);
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

test("the agent's API takes alasio's token alone, and answers each route as it says", () =>
  withBucket(({ s3, node }) =>
    Effect.gen(function*() {
      const here = yield* node("node-1");
      const { port } = yield* serveNodeAgent({ port: 0, host: "127.0.0.1", token: "the token", agent: here.agent });
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
      const run = (operation: string, request: { readonly environment: string; readonly workspace: string }) =>
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
        body: { state: "done", value: { manifest, imagePath: join(here.cacheDir, "main", "fs-abc", manifest.snapshot) } },
      });
      failed(404, "main/fs-none has no snapshot")(yield* run("prepare", { environment: "main", workspace: "fs-none" }));

      const holdings = yield* call("GET", "/holdings");
      assert.equal(holdings.status, 200);
      const { cached, ...held } = holdings.body as { readonly cached: readonly { readonly usedAt: string }[] };
      assert.deepEqual(held, { node: "node-1", releases: [{ release: RELEASE, cpuFeatures: ["sse", "avx", "avx2"] }] });
      assert.deepEqual(cached.map(({ usedAt, ...rest }) => (assert.ok(Date.parse(usedAt) <= Date.now()), rest)), [
        { environment: "main", workspace: "fs-abc", images: [manifest.layers[0]?.digest], bytes: manifest.bytes + JSON.stringify(manifest).length },
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
      // Spent, it is current no more, but its images are kept.
      assert.deepEqual(yield* call("DELETE", "/snapshots/main/fs-abc/current"), { status: 204, body: null });
      error(404, "the workspace has no snapshot")(yield* call("GET", "/snapshots/main/fs-abc"));
      assert.equal(keysOf(s3, `main/fs-abc/layers/${image.digest}/`).length, 3);

      assert.deepEqual(yield* call("DELETE", "/snapshots/main/fs-abc"), { status: 204, body: null });
      assert.deepEqual(keysOf(s3, "main/fs-abc/"), []);
      assert.equal(cachesAny(here, "fs-abc"), false);

      error(400, /podIPs/u)(yield* call("POST", "/resume", { ...resumeRequest(), podIPs: ["10.42.0.300"] }));
      error(400, /podIPs/u)(yield* call("POST", "/resume", { ...resumeRequest(), podIPs: [] }));
      error(400, /containerIds/u)(yield* call("POST", "/resume", { ...resumeRequest(), containerIds: ["short"] }));
      error(400, /pod/u)(yield* call("POST", "/resume", { ...resumeRequest(), pod: "fs_abc123" }));
      assert.deepEqual(yield* call("POST", "/resume", resumeRequest()), { status: 204, body: null });
      assert.deepEqual(here.releases.calls(RELEASE).slice(-2).map((args) => args.slice(-2)), [["resume", CONTAINER], ["resume", OTHER_CONTAINER]]);

      writeFileSync(join(here.releases.dirOf(RELEASE), "fail-checkpoint"), "");
      failed(500, /^checkpointing main\/fs-abc failed: .*its snapshot is forgotten/u)(yield* run("checkpoints", request("fs-abc")));
      writeFileSync(join(here.releases.dirOf(RELEASE), "fail-resume"), "");
      error(500, `runsc resume of ${RELEASE} failed: resume failed: the container is gone`)(yield* call("POST", "/resume", resumeRequest()));
      error(404, "there is no GET /snapshots")(yield* call("GET", "/snapshots"));
    })
  ));

/** alasio's client of the agents, of which there is `agent`, of node-1, served until the scope closes. */
const agentsOf = (agent: NodeAgent) =>
  Effect.gen(function*() {
    const { port } = yield* serveNodeAgent({ port: 0, host: "127.0.0.1", token: "the token", agent });
    const pod: V1Pod = {
      metadata: { name: "node-agent-1", namespace: "alasio" },
      spec: { nodeName: "node-1", containers: [] },
      status: { podIP: "127.0.0.1", conditions: [{ type: "Ready", status: "True" }] },
    };
    const unreachable = () => Effect.die("only pods are listed");
    const kube = KubeClient.of({ read: unreachable, list: () => Effect.succeed([pod]), create: unreachable, replace: unreachable, patch: unreachable, patchPodStatus: unreachable, remove: unreachable, exec: unreachable });
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
  withBucket(({ node }) =>
    Effect.gen(function*() {
      const here = yield* node("node-1");
      yield* (yield* agentsOf(here.agent)).resume("node-1", resumeRequest());
      assert.deepEqual(here.releases.calls(RELEASE).map((args) => args.slice(-2)), [["resume", CONTAINER], ["resume", OTHER_CONTAINER]]);

      // An agent whose wait ended answers 504.
      const late = new PoliciesNotInForce({ namespace: SESSIONS, pod: POD, ip: POD_IP, reason: "KUBE-ROUTER-FORWARD has no jump of what it sends" });
      const refused = yield* Effect.flip((yield* agentsOf({ ...here.agent, resume: () => Effect.fail(late) })).resume("node-1", resumeRequest()));
      assert.equal(refused.message, `the node agent of node-1 could not resume main/fs-abc's sandbox: ${late.message}`);
      assert.equal(refused._tag === "NodeAgentError" && refused.status, 504);
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
  const env = {
    PATH: process.env["PATH"],
    NODE_NAME: "node-1",
    ALASIO_NODE_AGENT_TOKEN: "the token",
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
    running.agent.kill("SIGTERM");
    assert.equal(await running.exited, 0);
    assert.match(running.output(), /\[node-agent\] the node agent of node-1 serves on port 8484, its cache within 5368709120 bytes/u);
  } finally {
    await s3.close();
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
      assert.match(failure(yield* Effect.exit(here.agent.resume(resumeRequest()))), /is not of the pod alasio-sessions\/fs-abc123/u);
      assert.match(failure(yield* Effect.exit(here.agent.checkpoint(request("fs-abc", "c".repeat(64))))), /container does not exist/u);
    })
  ));
