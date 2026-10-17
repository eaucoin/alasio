/**
 * The script of the node's mount of workspace storage (cli/src/manifests/sandbox-gateway.ts),
 * run by sh with stand-ins for juicefs, for the node's mount table and for entering the node's
 * mount namespace: its juicefs copied to the node and run there; idle once its mount is handed
 * over, a failure once it ends any other way, ready only while its own mount serves the node's
 * mount point; and a mount left by a juicefs gone unmounted before it mounts.
 */
import assert from "node:assert/strict";
import { type ChildProcess, spawn, spawnSync } from "node:child_process";
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test, type TestContext } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";

import { HANDED_OVER, type MountPaths, workspaceMountReady, workspaceMountScript } from "../src/manifests/sandbox-gateway.ts";

/** What juicefs's stand-in does: serve until it is ended, hand its mount over, or fail. */
type Juicefs = "serves" | "hands over" | "fails";

interface Mount {
  readonly paths: MountPaths;
  readonly root: string;
  readonly child: ChildProcess;
  readonly out: () => string;
  readonly ended: Promise<number | null>;
  readonly ready: () => boolean;
  /** What was run in the node's namespace, a command a line. */
  readonly inNode: () => string[];
}

/**
 * The mount's script, its juicefs doing as `juicefs` says, its mount the node's mount table's
 * line while it serves; where `left`, the table holds a mount of a juicefs gone at the root
 * already, which reads as nothing until it is unmounted.
 */
function mount(t: TestContext, juicefs: Juicefs, { left = false } = {}): Mount {
  const directory = mkdtempSync(join(tmpdir(), "alasio-workspace-mount-"));
  const root = join(directory, "workspaces");
  const mountinfo = join(directory, "mountinfo");
  const ran = join(directory, "in-node");
  const dead = join(directory, "dead");
  const line = `36 1 0:50 / ${root} rw,relatime shared:9 - fuse.juicefs JuiceFS:workspaces rw`;
  writeFileSync(mountinfo, left ? `${line}\n` : "");
  if (left) writeFileSync(dead, "");
  const what: Record<Juicefs, string> = {
    serves: `echo '${line}' >> ${mountinfo}; echo mounted; trap 'exit 0' TERM; while :; do sleep 0.1; done`,
    "hands over": `echo 'juicefs[7] <INFO>: ${HANDED_OVER} [launchMount@mount_unix.go:1057]'; exit 1`,
    fails: "echo 'juicefs[7] <FATAL>: the metadata engine refused us'; exit 3",
  };
  const image = join(directory, "image-juicefs");
  writeFileSync(image, `#!/bin/sh\n${what[juicefs]}\n`);
  // The node's namespace, the test's own: each command logged, a dead mount's ls failing, its umount taking it out of the table.
  const inNode = join(directory, "nsenter");
  writeFileSync(inNode, [
    "#!/bin/sh",
    `echo "$*" >> ${ran}`,
    `if [ "$1" = ls ] && [ -e ${dead} ]; then exit 2; fi`,
    `if [ "$1" = umount ]; then rm -f ${dead}; : > ${mountinfo}; exit 0; fi`,
    'exec "$@"',
  ].join("\n"));
  for (const file of [image, inNode]) chmodSync(file, 0o755);
  const paths: MountPaths = { run: join(directory, "run"), bin: join(directory, "bin"), state: join(directory, "state"), cache: join(directory, "cache"), juicefs: image, mountinfo, inNode };
  mkdirSync(paths.run);
  const env = { ...process.env, META_URL: "redis://valkey/1" };
  // A group of its own, so whatever it started is ended with it.
  const child = spawn("sh", ["-c", workspaceMountScript(root, 1024, paths)], { env, stdio: ["ignore", "pipe", "pipe"], detached: true });
  let out = "";
  child.stdout?.on("data", (chunk: Buffer) => (out += chunk.toString()));
  child.stderr?.on("data", (chunk: Buffer) => (out += chunk.toString()));
  const ended = new Promise<number | null>((resolve) => child.on("exit", (code) => resolve(code)));
  t.after(() => {
    try {
      process.kill(-(child.pid ?? 0), "SIGKILL");
    } catch {
      // Ended already, all of it.
    }
    rmSync(directory, { recursive: true, force: true });
  });
  return {
    paths,
    root,
    child,
    out: () => out,
    ended,
    ready: () => spawnSync("sh", ["-c", workspaceMountReady(root, paths)], { env }).status === 0,
    inNode: () => (existsSync(ran) ? readFileSync(ran, "utf8").trim().split("\n") : []),
  };
}

/** Waits until `condition` holds, for a few seconds at most. */
async function until(condition: () => boolean): Promise<void> {
  for (let n = 0; n < 100 && !condition(); n += 1) await sleep(50);
  assert.ok(condition());
}

test("a mount that serves runs its image's juicefs, copied to the node under its digest, in the node's namespace, is ready while the node's table holds its mount, and, asked to end, ends its mount with it, a failure", async (t) => {
  const serving = mount(t, "serves");
  await until(() => serving.ready());
  await until(() => serving.out().includes("mounted"));
  const [copied] = readdirSync(serving.paths.bin);
  assert.match(copied ?? "", /^juicefs-[0-9a-f]{16}$/u);
  assert.equal(readFileSync(join(serving.paths.bin, copied ?? ""), "utf8"), readFileSync(serving.paths.juicefs, "utf8"));
  assert.deepEqual(serving.inNode().at(-1)?.split(" ").slice(0, 2), [join(serving.paths.bin, copied ?? ""), "mount"]);
  assert.ok(serving.inNode().at(-1)?.includes(`--cache-dir ${serving.paths.cache}`));
  serving.child.kill("SIGTERM");
  assert.notEqual(await serving.ended, 0);
  assert.equal(existsSync(join(serving.paths.run, "handed-over")), false);
});

test("a mount that hands its session over idles, not ready, until its pod is deleted", async (t) => {
  const handed = mount(t, "hands over");
  await until(() => existsSync(join(handed.paths.run, "handed-over")));
  assert.equal(handed.ready(), false);
  await sleep(500);
  assert.equal(handed.child.exitCode, null);
  assert.match(handed.out(), /handed over to the pod that replaces this one/u);
  handed.child.kill("SIGTERM");
  assert.equal(await handed.ended, 0);
});

test("a mount that ends without handing its session over fails, so kubelet starts it again, and is never idle", async (t) => {
  const failed = mount(t, "fails");
  assert.equal(await failed.ended, 3);
  assert.equal(existsSync(join(failed.paths.run, "handed-over")), false);
  assert.match(failed.out(), /the metadata engine refused us/u);
  assert.match(failed.out(), /juicefs mount ended \(3\) without handing its mount over/u);
});

test("a mount left at the root by a juicefs gone, as a pod killed with no successor leaves one, is unmounted before the node is mounted afresh", async (t) => {
  const serving = mount(t, "serves", { left: true });
  await until(() => serving.ready());
  const ran = serving.inNode();
  assert.ok(ran.indexOf(`umount -l ${serving.root}`) >= 0 && ran.indexOf(`umount -l ${serving.root}`) < ran.findIndex((command) => command.includes(" mount ")), ran.join("\n"));
  assert.match(serving.out(), /unmounted the node's mount its juicefs left/u);
  // One that serves is left be.
  const whole = mount(t, "serves");
  await until(() => whole.ready());
  assert.ok(!whole.inNode().some((command) => command.startsWith("umount")));
});

test("a mount is not ready while the node's table holds no mount of JuiceFS's at the root, as when its juicefs serves none", async (t) => {
  const serving = mount(t, "serves");
  await until(() => serving.ready());
  writeFileSync(serving.paths.mountinfo, `37 1 0:51 / ${serving.root} rw - overlay overlay rw\n`);
  assert.equal(serving.ready(), false);
});
