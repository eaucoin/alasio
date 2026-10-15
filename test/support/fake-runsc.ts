/**
 * A stand-in for a node's gVisor releases: a directory of releases, each a runsc that is
 * a script, and containerd's directory of their runtimes' options files. The script
 * records each run's arguments, one JSON line each, in `calls.jsonl` beside it, and
 * answers what the node agent asks runsc as the fork's runsc does
 * (runsc/cmd/checkpoint.go, resume.go, image/inspect.go, cpu_features.go):
 *
 * - `checkpoint --image-path=DIR [--parent-image-path=PARENT] ... ID`: writes an image of
 *   ID into DIR, of runsc's three files: its state file, its pages, and its pages metadata,
 *   which here names the images it holds pages of besides its own, its layers: none, or
 *   PARENT's digest and PARENT's layers, failing as runsc does without PARENT's pages
 *   metadata. Or it fails, saying so, while a file `fail-checkpoint` is beside it, or takes a
 *   minute while one `hang-checkpoint` is, or waits while one `hold-checkpoint` is;
 * - `image inspect --json DIR`: describes the image, as saved by the release's runsc, whose
 *   version is `release-RELEASE` (or what a file `saved-by` beside it says), with the CPU
 *   features of `image-cpu-features`, its digest that of its pages metadata, as runsc's;
 * - `image layers DIR`: its layers' digests, a line each;
 * - `resume ID`: says nothing; or fails, saying so, while a file `fail-resume` is beside it;
 * - `cpu-features`: the node's, from `cpu-features`, comma-separated;
 * - `state ID`: the container's sandbox's process and its pod's annotations, as
 *   `containers.json` beside the releases says (FakeReleases.runs), or fails for one it
 *   does not hold; that process's program, in the stand-in's /proc, is the runsc of the
 *   release that runs it, and its arguments runsc's flags, which track its writes or not;
 * - `list --format=json`: each container `containers.json` holds, as `state` says it, or
 *   null for none, as runsc writes an empty list.
 *
 * The stand-in's /proc holds besides, while they run, the processes the test starts as
 * runsc would (FakeReleases.starts), which are real, and idle until killed: their
 * arguments and their stat, as /proc says them, which the test may make say another start
 * time (FakeReleases.restarted), as a process the node gave the pid since would have.
 */
import { type ChildProcess, spawn } from "node:child_process";
import { chmodSync, existsSync, mkdirSync, readFileSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { join } from "node:path";

import { gvisorHandler } from "../../src/node-agent/names.ts";

const SCRIPT = `#!/usr/bin/env node
const { appendFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } = require("node:fs");
const { createHash } = require("node:crypto");
const { basename, join } = require("node:path");
const here = __dirname;
const args = process.argv.slice(2);
appendFileSync(join(here, "calls.jsonl"), JSON.stringify(args) + "\\n");
const read = (name, otherwise) => (existsSync(join(here, name)) ? readFileSync(join(here, name), "utf8").trim() : otherwise);
const rest = args.filter((arg) => !arg.startsWith("--root=") && !arg.startsWith("--systemd-cgroup="));
const flag = (name) => rest.find((arg) => arg.startsWith("--" + name + "="))?.slice(name.length + 3);
const sha256 = (data) => createHash("sha256").update(data).digest("hex");
const containers = () => (existsSync(join(here, "..", "containers.json")) ? JSON.parse(readFileSync(join(here, "..", "containers.json"), "utf8")) : {});
const stateOf = (id, { pid, bundle, annotations }) => ({ ociVersion: "1.0.2", id, status: "running", pid, bundle, annotations });
const checkpoint = () => {
  const image = flag("image-path");
  const id = rest[rest.length - 1];
  const parent = flag("parent-image-path");
  let layers = [];
  if (parent !== undefined) {
    if (!existsSync(join(parent, "pages_meta.img"))) { process.stderr.write("checkpoint failed: the parent image " + parent + " has no pages metadata\\n"); process.exit(128); }
    const parentMeta = readFileSync(join(parent, "pages_meta.img"));
    layers = [sha256(parentMeta), ...JSON.parse(parentMeta).layers];
  }
  mkdirSync(image, { recursive: true });
  // Each save its own state, as each sandbox's moment is.
  const state = "state of " + id + " at " + process.hrtime.bigint();
  writeFileSync(join(image, "checkpoint.img"), state);
  writeFileSync(join(image, "pages_meta.img"), JSON.stringify({ state: sha256(state), layers }));
  writeFileSync(join(image, "pages.img"), Buffer.alloc(4096, id));
};
if (rest[0] === "checkpoint") {
  if (existsSync(join(here, "fail-checkpoint"))) { process.stderr.write("checkpoint failed: the sandbox is gone\\n"); process.exit(128); }
  if (existsSync(join(here, "hang-checkpoint"))) { setTimeout(() => {}, 60000); return; }
  if (existsSync(join(here, "hold-checkpoint"))) {
    const held = setInterval(() => { if (!existsSync(join(here, "hold-checkpoint"))) { clearInterval(held); checkpoint(); } }, 20);
    return;
  }
  checkpoint();
} else if (rest[0] === "image" && rest[1] === "inspect") {
  const image = rest[rest.length - 1];
  const meta = readFileSync(join(image, "pages_meta.img"));
  process.stdout.write(JSON.stringify({
    digest: sha256(meta),
    format: { major: 2, minor: 0 },
    state: { size: 10, metadata: { runsc_version: read("saved-by", "release-" + basename(here)), cpu_features: read("image-cpu-features", "sse,avx"), platform: "systrap" } },
    layers: [{ pages_size: 4096, path: join(image, "pages.img") }, ...JSON.parse(meta).layers.map((digest) => ({ digest, pages_size: 4096, path: join(image, "layers", digest, "pages.img") }))],
    memory_files: [],
  }, null, 2) + "\\n");
} else if (rest[0] === "image" && rest[1] === "layers") {
  const image = rest[rest.length - 1];
  for (const digest of JSON.parse(readFileSync(join(image, "pages_meta.img"), "utf8")).layers) process.stdout.write(digest + "\\n");
} else if (rest[0] === "resume") {
  if (existsSync(join(here, "fail-resume"))) { process.stderr.write("resume failed: the container is gone\\n"); process.exit(128); }
} else if (rest[0] === "state") {
  const running = containers()[rest[rest.length - 1]];
  if (!running) { process.stderr.write("container does not exist\\n"); process.exit(1); }
  process.stdout.write(JSON.stringify(stateOf(rest[rest.length - 1], running)) + "\\n");
} else if (rest[0] === "list" && flag("format") === "json") {
  const listed = Object.entries(containers()).map(([id, running]) => stateOf(id, running));
  process.stdout.write(JSON.stringify(listed.length === 0 ? null : listed) + "\\n");
} else if (rest[0] === "cpu-features") {
  process.stdout.write(read("cpu-features", "sse,avx,avx2") + "\\n");
} else {
  process.stderr.write("unknown command " + rest.join(" ") + "\\n");
  process.exit(2);
}
`;

/** A mount of an OCI spec: where it is in the container, and what it mounts there, on the node. */
export interface OciMount {
  readonly destination: string;
  readonly source: string;
}

/** A node's releases and options files, at `root`. */
export interface FakeReleases {
  readonly releasesDir: string;
  readonly containerdDir: string;
  /** The directory of a release's runsc, where its files of the stand-in go. */
  readonly dirOf: (release: string) => string;
  /** The arguments of each run of a release's runsc, in order, but its reads of containers' state and its lists of them. */
  readonly calls: (release: string) => string[][];
  /** The node's processes, as the agent reads them: each container's sandbox's, of its release's runsc, and those the test starts. */
  readonly procRoot: string;
  /** The bundle of a container that runs. */
  readonly bundleOf: (containerId: string) => string;
  /** Starts a process that idles, named `argv0`, of the arguments `args`, which the node's /proc shows while it runs. The test kills it. */
  readonly starts: (argv0: string, args: readonly string[]) => ChildProcess;
  /** Makes the node's /proc show the process `pid` the test started with a start time a tick later. */
  readonly restarted: (pid: number) => void;
  /**
   * Makes the container run under `release`, in the pod `namespace`/`pod`, as runsc's state
   * says, its sandbox tracking its writes (`--dirty-tracking=wp`) unless `tracksWrites` is
   * false, and its spec, in its bundle, mounting `mounts`.
   */
  readonly runs: (containerId: string, release: string, namespace: string, pod: string, tracksWrites?: boolean, mounts?: readonly OciMount[]) => void;
}

/** Installs `releases` under `root`, each with an options file giving runsc systemd's cgroups. */
export function fakeReleases(root: string, releases: readonly string[]): FakeReleases {
  const releasesDir = join(root, "gvisor");
  const containerdDir = join(root, "containerd");
  mkdirSync(containerdDir, { recursive: true });
  const dirOf = (release: string) => join(releasesDir, release);
  for (const release of releases) {
    mkdirSync(dirOf(release), { recursive: true });
    writeFileSync(join(dirOf(release), "runsc"), SCRIPT);
    chmodSync(join(dirOf(release), "runsc"), 0o755);
    writeFileSync(join(containerdDir, `${gvisorHandler(release)}.toml`), 'restore_direct = true\n\n[runsc_config]\n  systemd-cgroup = "true"\n');
  }
  const procRoot = join(root, "proc");
  const containers = join(releasesDir, "containers.json");
  const bundleOf = (containerId: string) => join(root, "bundles", containerId);
  let pid = 4000;
  const runs = (containerId: string, release: string, namespace: string, pod: string, tracksWrites = true, mounts: readonly OciMount[] = []) => {
    const held = existsSync(containers) ? JSON.parse(readFileSync(containers, "utf8")) as Record<string, unknown> : {};
    pid += 1;
    mkdirSync(join(procRoot, String(pid)), { recursive: true });
    symlinkSync(join(dirOf(release), "runsc"), join(procRoot, String(pid), "exe"));
    // As runsc starts a sandbox: its flags, then its command.
    const argv = ["runsc-sandbox", "--root=/run/containerd/runsc/k8s.io", `--dirty-tracking=${tracksWrites ? "wp" : "off"}`, "boot", containerId];
    writeFileSync(join(procRoot, String(pid), "cmdline"), `${argv.join("\0")}\0`);
    const bundle = bundleOf(containerId);
    mkdirSync(bundle, { recursive: true });
    writeFileSync(join(bundle, "config.json"), JSON.stringify({ ociVersion: "1.2.0", mounts: mounts.map((mount) => ({ ...mount, type: "bind", options: ["rbind"] })) }));
    held[containerId] = { pid, bundle, annotations: { "io.kubernetes.cri.sandbox-namespace": namespace, "io.kubernetes.cri.sandbox-name": pod } };
    writeFileSync(containers, JSON.stringify(held));
  };
  const starts = (argv0: string, args: readonly string[]) => {
    // Its arguments after node's own, which end at "--".
    const started = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)", "--", ...args], { argv0, stdio: "ignore" });
    const { pid: id } = started;
    if (id === undefined) throw new Error(`${argv0} did not start`);
    mkdirSync(join(procRoot, String(id)), { recursive: true });
    for (const file of ["cmdline", "stat"]) writeFileSync(join(procRoot, String(id), file), readFileSync(join("/proc", String(id), file)));
    // Gone from the node's /proc as it ends, as its pid may be given another's.
    started.once("exit", () => rmSync(join(procRoot, String(id)), { recursive: true, force: true }));
    return started;
  };
  const restarted = (id: number) => {
    const file = join(procRoot, String(id), "stat");
    const stat = readFileSync(file, "utf8");
    // Its fields past its name, the third first: its start time is the 22nd.
    const named = stat.lastIndexOf(")") + 1;
    const fields = stat.slice(named + 1).split(" ");
    fields[22 - 3] = String(BigInt(fields[22 - 3] ?? "0") + 1n);
    writeFileSync(file, `${stat.slice(0, named)} ${fields.join(" ")}`);
  };
  return {
    releasesDir,
    containerdDir,
    procRoot,
    runs,
    dirOf,
    bundleOf,
    starts,
    restarted,
    calls: (release) => {
      const log = join(dirOf(release), "calls.jsonl");
      const runs = existsSync(log) ? readFileSync(log, "utf8").split("\n").filter(Boolean).map((line) => JSON.parse(line) as string[]) : [];
      return runs.filter((args) => !args.includes("state") && !args.includes("list"));
    },
  };
}
