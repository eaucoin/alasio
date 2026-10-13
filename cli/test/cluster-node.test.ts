/**
 * This machine made a k3s node (cli/src/cluster/node.ts), as root: the files that set k3s
 * and its containerd, gVisor's releases installed from their archives and removed, what
 * k3s's own scripts do, and what is downloaded checked against its pin again as root.
 */
import assert from "node:assert/strict";
import { mkdtempSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test, type TestContext } from "node:test";

import { NodeServices } from "@effect/platform-node";
import { Effect, Layer, Logger } from "effect";

import { containerdFiles } from "../src/cluster/k3s.ts";
import { Machine } from "../src/cluster/machine.ts";
import { type Artifact, ConfigureNode, downloads, InstallGvisor, InstallStargz, nodeFiles, type NodeStep, performNodeStep, RemoveGvisor } from "../src/cluster/node.ts";
import { STARGZ_ROOT, STARGZ_SOCKET, stargzConfig } from "../src/cluster/stargz.ts";
import { FakeMachine } from "./support/fake-machine.ts";
import { type FakeReleases, fakeReleases } from "./support/fake-releases.ts";

const CONTAINERD = "/var/lib/rancher/k3s/agent/etc/containerd";
const STORAGE = "/home/op/.local/share/alasio/storage";

/** A machine of the test's, and a way to do a step on it as root. */
function rig(t: TestContext) {
  const directory = mkdtempSync(join(tmpdir(), "alasio-node-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const machine = new FakeMachine(join(directory, "machine"));
  const perform = (step: NodeStep) =>
    Effect.runPromise(
      performNodeStep(step).pipe(
        Effect.match({ onFailure: (error) => error.message, onSuccess: () => null }),
        Effect.provide(Layer.mergeAll(Layer.succeed(Machine, { platform: "linux", arch: "x64", root: machine.root }), machine.layer, NodeServices.layer)),
        Effect.provide(Logger.layer([])),
      ),
    );
  return { directory, machine, perform };
}

test("a server's files: k3s's config, without traefik, its volumes where the config says, kubelet's thresholds, no snapshotter of its own; registries only when given; each gVisor release a runtime, where systemd runs the machine; stargz-snapshotter's daemon's", () => {
  const files = nodeFiles({ role: "server", storagePath: STORAGE, gvisor: ["20990101.0"] });
  assert.deepEqual([...files.keys()], [
    "/etc/rancher/k3s/config.yaml",
    `${CONTAINERD}/config-v3.toml.tmpl`,
    `${CONTAINERD}/runsc-20990101-0.toml`,
    "/etc/containerd-stargz-grpc/config.toml",
    "/etc/systemd/system/stargz-snapshotter.service",
    "/etc/systemd/system/k3s.service.d/alasio-stargz.conf",
  ]);
  const [comment, k3s] = (files.get("/etc/rancher/k3s/config.yaml") ?? "").split("\n");
  assert.match(comment ?? "", /^# /u);
  assert.deepEqual(JSON.parse(k3s ?? ""), {
    "disable": ["traefik"],
    "cluster-cidr": "10.42.0.0/16",
    "service-cidr": "10.43.0.0/16",
    "default-local-storage-path": STORAGE,
    "kubelet-arg": [
      "kube-reserved=cpu=250m,memory=1Gi",
      "system-reserved=cpu=250m,memory=512Mi",
      "eviction-hard=imagefs.available<5%,nodefs.available<5%,memory.available<500Mi",
      "eviction-minimum-reclaim=imagefs.available=1%,nodefs.available=1%",
      "image-gc-high-threshold=98",
      "image-gc-low-threshold=95",
    ],
    "snapshotter": "",
  });
  const runtimes = containerdFiles(["20990101.0"], { systemd: true });
  for (const [path, content] of runtimes) assert.equal(files.get(path), content);
  assert.equal(files.get("/etc/containerd-stargz-grpc/config.toml"), stargzConfig());
  // The daemon, which k3s's service wants started before it, stops and restarts with k3s, its FUSE manager keeping its mounts.
  const unit = files.get("/etc/systemd/system/stargz-snapshotter.service") ?? "";
  assert.match(unit, /^PartOf=k3s\.service$/mu);
  assert.match(unit, /^KillMode=process$/mu);
  assert.match(unit, new RegExp(`^ExecStart=/usr/local/bin/containerd-stargz-grpc --address=${STARGZ_SOCKET} --config=/etc/containerd-stargz-grpc/config\\.toml --root=${STARGZ_ROOT}$`, "mu"));
  assert.match(files.get("/etc/systemd/system/k3s.service.d/alasio-stargz.conf") ?? "", /^\[Unit\]\nWants=stargz-snapshotter\.service\nAfter=stargz-snapshotter\.service\n$/mu);
  const registries = { mirrors: { "docker.io": { endpoint: ["https://mirror.lan"] } }, configs: {} };
  assert.equal(nodeFiles({ role: "server", storagePath: "/srv", registries, gvisor: [] }).get("/etc/rancher/k3s/registries.yaml"), `{"mirrors":{"docker.io":{"endpoint":["https://mirror.lan"]}},"configs":{}}\n`);
});

test("configuring the node removes runsc's options for releases it no longer runs, and leaves containerd's own configuration", async (t) => {
  const { machine, perform } = rig(t);
  machine.write(`${CONTAINERD}/config.toml`, "# containerd's, which k3s writes from the template\n");
  machine.write(`${CONTAINERD}/runsc.toml`, "[runsc_config]\n");
  machine.write(`${CONTAINERD}/runsc-20980101-0.toml`, "[runsc_config]\n");
  assert.equal(await perform(ConfigureNode.make({ config: { role: "server", storagePath: STORAGE, gvisor: ["20990101.0"] } })), null);
  assert.deepEqual(["config.toml", "config-v3.toml.tmpl", "runsc.toml", "runsc-20980101-0.toml", "runsc-20990101-0.toml"].filter((name) => machine.has(`${CONTAINERD}/${name}`)), [
    "config.toml",
    "config-v3.toml.tmpl",
    "runsc-20990101-0.toml",
  ]);
});

test("stargz-snapshotter's daemon keeps its mounts with its FUSE manager, and reads layers through the mirrors it can reach as they are", () => {
  const plain = stargzConfig();
  assert.match(plain, /^\[fuse_manager\]\nenable = true\n/mu);
  assert.doesNotMatch(plain, /resolver/u);
  const mirrored = stargzConfig({
    mirrors: {
      "docker.io": { endpoint: ["https://mirror.lan", "http://cache.lan:5000", "https://proxy.lan/docker"] },
      "ghcr.io": { endpoint: ["https://ghcr.mirror.lan"], rewrite: { "^eaucoin/(.*)": "mirror/$1" } },
      "*": { endpoint: ["https://any.lan"] },
    },
  });
  assert.equal(mirrored.slice(plain.length), [
    "",
    '[[resolver.host."docker.io".mirrors]]',
    'host = "mirror.lan"',
    "",
    '[[resolver.host."docker.io".mirrors]]',
    'host = "cache.lan:5000"',
    "insecure = true",
    "",
  ].join("\n"));
});

/** The archive of gVisor's `release`, downloaded into `directory`, as the operator's command leaves it for root. */
function downloaded(directory: string, release = "20990101.0", releases: FakeReleases = fakeReleases({ gvisor: [release] })): Artifact {
  const download = downloads(releases.pins).gvisor.get(release);
  assert.ok(download);
  const path = join(directory, `gvisor-${release}.tar.zstd`);
  writeFileSync(path, releases.served.get(download.url) ?? "");
  return { path, ...download };
}

test("each gVisor release is installed in its own directory as its archive has it, runsc, its shim and gvisor-bin/, and nothing else of it", async (t) => {
  const { directory, machine, perform } = rig(t);
  assert.equal(await perform(InstallGvisor.make({ release: "20990101.0", archive: downloaded(directory) })), null);
  const installed = "/usr/local/lib/alasio/gvisor/20990101.0";
  assert.equal(machine.read(`${installed}/runsc`), "runsc 20990101.0\n");
  assert.equal(machine.read(`${installed}/containerd-shim-runsc-v1`), "containerd-shim-runsc-v1 20990101.0\n");
  assert.equal(machine.read(`${installed}/gvisor-bin/gvisor_sentry`), "gvisor_sentry 20990101.0\n");
  assert.equal(statSync(machine.at(`${installed}/runsc`)).mode & 0o777, 0o755);
  assert.equal(machine.has(`${installed}/README`), false);
  assert.equal(machine.has("/usr/local/bin/runsc"), false);
  assert.deepEqual(JSON.parse(machine.read("/etc/rancher/k3s/alasio.json") ?? ""), { gvisor: ["20990101.0"] });

  assert.equal(await perform(InstallGvisor.make({ release: "21000101.0", archive: downloaded(directory, "21000101.0") })), null);
  assert.equal(machine.read("/usr/local/lib/alasio/gvisor/21000101.0/runsc"), "runsc 21000101.0\n");
  assert.equal(machine.read(`${installed}/runsc`), "runsc 20990101.0\n");
  assert.deepEqual(JSON.parse(machine.read("/etc/rancher/k3s/alasio.json") ?? ""), { gvisor: ["20990101.0", "21000101.0"] });
});

test("a gVisor release is removed with its processes stopped, and the others kept", async (t) => {
  const { directory, machine, perform } = rig(t);
  for (const release of ["20990101.0", "21000101.0"]) await perform(InstallGvisor.make({ release, archive: downloaded(directory, release) }));
  assert.equal(await perform(RemoveGvisor.make({ release: "20990101.0" })), null);
  const removed = "/usr/local/lib/alasio/gvisor/20990101.0";
  assert.deepEqual(machine.killed, [[`${removed}/runsc`, `${removed}/containerd-shim-runsc-v1`, `${removed}/gvisor-bin/gvisor_sentry`]]);
  assert.equal(machine.has(removed), false);
  assert.equal(machine.read("/usr/local/lib/alasio/gvisor/21000101.0/runsc"), "runsc 21000101.0\n");
  assert.deepEqual(JSON.parse(machine.read("/etc/rancher/k3s/alasio.json") ?? ""), { gvisor: ["21000101.0"] });
});

test("stargz-snapshotter's release is installed as its archive has it, its daemon and its FUSE manager, and nothing else of it", async (t) => {
  const { directory, machine, perform } = rig(t);
  const releases = fakeReleases();
  const download = downloads(releases.pins).stargz;
  const path = join(directory, "stargz-snapshotter.tar.gz");
  writeFileSync(path, releases.served.get(download.url) ?? "");
  assert.equal(await perform(InstallStargz.make({ version: "v9.9.0", archive: { path, ...download } })), null);
  assert.equal(machine.read("/usr/local/bin/containerd-stargz-grpc"), "containerd-stargz-grpc v9.9.0\n");
  assert.equal(machine.read("/usr/local/bin/stargz-fuse-manager"), "stargz-fuse-manager v9.9.0\n");
  assert.equal(machine.has("/usr/local/bin/ctr-remote"), false);
  assert.deepEqual(JSON.parse(machine.read("/etc/rancher/k3s/alasio.json") ?? ""), { stargz: "v9.9.0" });
});

test("an archive changed since it was downloaded is refused as root, and nothing of it installed", async (t) => {
  const { directory, machine, perform } = rig(t);
  const archive = downloaded(directory);
  writeFileSync(archive.path, "changed");
  assert.match(await perform(InstallGvisor.make({ release: "20990101.0", archive })) ?? "", /^https:\/\/github\.com\/eaucoin\/gvisor\/.* is not what alasio pins: its sha512 is /u);
  assert.equal(machine.has("/usr/local/lib/alasio"), false);
  assert.equal(machine.has("/etc/rancher/k3s/alasio.json"), false);
});
