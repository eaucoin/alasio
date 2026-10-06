/**
 * This machine made a k3s node (cli/src/cluster/node.ts), as root: the files that set k3s
 * and its containerd, gVisor's release installed from its archive, what k3s's own scripts
 * do, and what is downloaded checked against its pin again as root.
 */
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test, type TestContext } from "node:test";

import { NodeServices } from "@effect/platform-node";
import { Effect, Layer, Logger } from "effect";

import { Machine } from "../src/cluster/machine.ts";
import { type Artifact, downloads, InstallGvisor, nodeFiles, type NodeStep, performNodeStep, RUNSC_TOML } from "../src/cluster/node.ts";
import { tarEntries } from "../src/cluster/tar.ts";
import { FakeMachine } from "./support/fake-machine.ts";
import { fakeReleases, tar } from "./support/fake-releases.ts";

const TEMPLATE = readFileSync(new URL("../../cluster/node/config-v3.toml.tmpl", import.meta.url), "utf8");

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

test("a server's files: k3s's config, without traefik, its volumes where the config says, kubelet's thresholds; registries only when given; the template; runsc's options", () => {
  const files = nodeFiles({ role: "server", storagePath: "/home/op/.local/share/alasio/storage" }, TEMPLATE);
  assert.deepEqual([...files.keys()], ["/etc/rancher/k3s/config.yaml", "/var/lib/rancher/k3s/agent/etc/containerd/config-v3.toml.tmpl", RUNSC_TOML]);
  const [comment, k3s] = (files.get("/etc/rancher/k3s/config.yaml") ?? "").split("\n");
  assert.match(comment ?? "", /^# /u);
  assert.deepEqual(JSON.parse(k3s ?? ""), {
    "disable": ["traefik"],
    "cluster-cidr": "10.42.0.0/16",
    "service-cidr": "10.43.0.0/16",
    "default-local-storage-path": "/home/op/.local/share/alasio/storage",
    "kubelet-arg": [
      "eviction-hard=imagefs.available<5%,nodefs.available<5%",
      "eviction-minimum-reclaim=imagefs.available=1%,nodefs.available=1%",
      "image-gc-high-threshold=98",
      "image-gc-low-threshold=95",
    ],
  });
  assert.match(files.get(RUNSC_TOML) ?? "", /\[runsc_config\]\n {2}systemd-cgroup = "true"\n$/u);
  const registries = { mirrors: { "docker.io": { endpoint: ["https://mirror.lan"] } }, configs: {} };
  assert.equal(nodeFiles({ role: "server", storagePath: "/srv", registries }, TEMPLATE).get("/etc/rancher/k3s/registries.yaml"), `{"mirrors":{"docker.io":{"endpoint":["https://mirror.lan"]}},"configs":{}}\n`);
});

test("the containerd template, the node image's, gives runsc the options a node writes where systemd's cgroups are kubelet's, and only there", () => {
  assert.match(TEMPLATE, new RegExp(`\\{\\{- if \\.SystemdCgroup \\}\\}[^]*ConfigPath = "${RUNSC_TOML}"\\n\\{\\{- end \\}\\}\\n$`, "u"));
  assert.match(TEMPLATE, /^\{\{ template "base" \. \}\}\n\n\[plugins\.'io\.containerd\.cri\.v1\.runtime'\.containerd\.runtimes\.runsc\]\n {2}runtime_type = "io\.containerd\.runsc\.v1"\n/u);
});

/** gVisor's release archive, downloaded into `directory`, as the operator's command leaves it for root. */
function downloaded(directory: string, releases = fakeReleases()): Artifact {
  const download = downloads(releases.pins).gvisor;
  const path = join(directory, "gvisor.tar.zstd");
  writeFileSync(path, releases.served.get(download.url) ?? "");
  return { path, ...download };
}

test("gVisor's release is installed as its archive has it, runsc, its shim and gvisor-bin/, and nothing else of it", async (t) => {
  const { directory, machine, perform } = rig(t);
  assert.equal(await perform(InstallGvisor.make({ release: "20990101.0", archive: downloaded(directory) })), null);
  assert.equal(machine.read("/usr/local/bin/runsc"), "runsc 20990101.0\n");
  assert.equal(machine.read("/usr/local/bin/containerd-shim-runsc-v1"), "containerd-shim-runsc-v1 20990101.0\n");
  assert.equal(machine.read("/usr/local/bin/gvisor-bin/gvisor_sentry"), "gvisor_sentry 20990101.0\n");
  assert.equal(statSync(machine.at("/usr/local/bin/runsc")).mode & 0o777, 0o755);
  assert.equal(machine.has("/usr/local/bin/README"), false);
  assert.deepEqual(JSON.parse(machine.read("/etc/rancher/k3s/alasio.json") ?? ""), { gvisor: "20990101.0" });
});

test("an archive changed since it was downloaded is refused as root, and nothing of it installed", async (t) => {
  const { directory, machine, perform } = rig(t);
  const archive = downloaded(directory);
  writeFileSync(archive.path, "changed");
  assert.match(await perform(InstallGvisor.make({ release: "20990101.0", archive })) ?? "", /^https:\/\/storage\.googleapis\.com\/.* is not what alasio pins: its sha512 is /u);
  assert.equal(machine.has("/usr/local/bin/runsc"), false);
  assert.equal(machine.has("/etc/rancher/k3s/alasio.json"), false);
});

test("tarEntries reads files and directories, a POSIX header's prefix as its name's start, and passes over other entries", () => {
  const archive = tar([{ name: "gvisor-bin/" }, { name: "gvisor-bin/runsc-metric-server", content: Buffer.from("metrics") }]);
  const linked = Buffer.from(archive);
  // A symbolic link, which is passed over.
  linked[512 + 156] = 0x32;
  assert.deepEqual(tarEntries(archive).map(({ name, type, content }) => [name, type, content.toString()]), [["gvisor-bin/", "directory", ""], ["gvisor-bin/runsc-metric-server", "file", "metrics"]]);
  assert.deepEqual(tarEntries(linked).map(({ name }) => name), ["gvisor-bin/"]);
  const prefixed = tar([{ name: "runsc", content: Buffer.from("runsc") }]);
  prefixed.write("release", 345, "latin1");
  assert.equal(tarEntries(prefixed)[0]?.name, "release/runsc");
});
