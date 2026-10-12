/**
 * A stand-in for a node's gVisor releases: a directory of releases, each a runsc that is
 * a script, and containerd's directory of their runtimes' options files. The script
 * records each run's arguments, one JSON line each, in `calls.jsonl` beside it, and
 * answers what the node agent asks runsc as the fork's runsc does
 * (runsc/cmd/checkpoint.go, resume.go, image/inspect.go, cpu_features.go):
 *
 * - `checkpoint --image-path=DIR ... ID`: writes an image of three files into DIR, made
 *   from ID; or fails, saying so, while a file `fail-checkpoint` is beside it, or takes a
 *   minute while one `hang-checkpoint` is;
 * - `image inspect --json DIR`: describes the image, as saved by the
 *   release's runsc, whose version is `release-RELEASE` (or what a file `saved-by` beside it
 *   says), with the CPU features of `image-cpu-features`;
 * - `resume ID`: says nothing; or fails, saying so, while a file `fail-resume` is beside it;
 * - `cpu-features`: the node's, from `cpu-features`, comma-separated.
 */
import { chmodSync, existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
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
if (rest[0] === "checkpoint") {
  if (existsSync(join(here, "fail-checkpoint"))) { process.stderr.write("checkpoint failed: the sandbox is gone\\n"); process.exit(128); }
  if (existsSync(join(here, "hang-checkpoint"))) { setTimeout(() => {}, 60000); return; }
  const image = rest.find((arg) => arg.startsWith("--image-path=")).slice("--image-path=".length);
  const id = rest[rest.length - 1];
  mkdirSync(join(image, "layers"), { recursive: true });
  writeFileSync(join(image, "checkpoint.img"), "state of " + id);
  writeFileSync(join(image, "pages_meta.img"), "meta of " + id);
  writeFileSync(join(image, "layers", "pages.img"), Buffer.alloc(4096, id));
} else if (rest[0] === "image" && rest[1] === "inspect") {
  const image = rest[rest.length - 1];
  const digest = createHash("sha256").update(readFileSync(join(image, "checkpoint.img"))).digest("hex");
  process.stdout.write(JSON.stringify({
    digest: "sha256:" + digest,
    format: { major: 1, minor: 0 },
    state: { size: 10, metadata: { runsc_version: read("saved-by", "release-" + basename(here)), cpu_features: read("image-cpu-features", "sse,avx"), platform: "systrap" } },
    layers: [{ pages_size: 4096, path: join(image, "layers", "pages.img") }],
    memory_files: [],
  }, null, 2) + "\\n");
} else if (rest[0] === "resume") {
  if (existsSync(join(here, "fail-resume"))) { process.stderr.write("resume failed: the container is gone\\n"); process.exit(128); }
} else if (rest[0] === "cpu-features") {
  process.stdout.write(read("cpu-features", "sse,avx,avx2") + "\\n");
} else {
  process.stderr.write("unknown command " + rest.join(" ") + "\\n");
  process.exit(2);
}
`;

/** A node's releases and options files, at `root`. */
export interface FakeReleases {
  readonly releasesDir: string;
  readonly containerdDir: string;
  /** The directory of a release's runsc, where its files of the stand-in go. */
  readonly dirOf: (release: string) => string;
  /** The arguments of each run of a release's runsc, in order. */
  readonly calls: (release: string) => string[][];
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
  return {
    releasesDir,
    containerdDir,
    dirOf,
    calls: (release) => {
      const log = join(dirOf(release), "calls.jsonl");
      return existsSync(log) ? readFileSync(log, "utf8").split("\n").filter(Boolean).map((line) => JSON.parse(line) as string[]) : [];
    },
  };
}
