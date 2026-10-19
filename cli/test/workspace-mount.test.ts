/**
 * The script of the node's mount of workspace storage (cli/src/manifests/sandbox-gateway.ts),
 * run by sh with stand-ins for juicefs, for the node's mount table and for entering the node's
 * mount namespace: its juicefs copied to the node and run there; idle once its mount is handed
 * over, a failure once it ends any other way, ready only while its own mount serves the node's
 * mount point; and a mount left by a juicefs gone unmounted before it mounts. And the Services
 * it reaches by address, as up reads them, which roll it as they are made anew.
 */
import assert from "node:assert/strict";
import { type ChildProcess, spawn, spawnSync } from "node:child_process";
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test, type TestContext } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";

import { Effect } from "effect";

import { readMountServices } from "../src/install.ts";
import { KubeApi } from "../src/kube/api.ts";
import { HANDED_OVER, type MountPaths, SERVING, workspaceMountReady, workspaceMountScript } from "../src/manifests/sandbox-gateway.ts";
import { serveFakeKube } from "./support/fake-kube.ts";

/** What juicefs's stand-in does: serve until it is ended, take a mount over once told to and serve it, hand its mount over, or fail. */
type Juicefs = "serves" | "takes over" | "hands over" | "fails";

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
function mount(t: TestContext, juicefs: Juicefs, { left = false, env: given = {} as Record<string, string> } = {}): Mount & { readonly takeOver: () => void } {
  const directory = mkdtempSync(join(tmpdir(), "alasio-workspace-mount-"));
  const root = join(directory, "workspaces");
  const mountinfo = join(directory, "mountinfo");
  const ran = join(directory, "in-node");
  const dead = join(directory, "dead");
  const line = `36 1 0:50 / ${root} rw,relatime shared:9 - fuse.juicefs JuiceFS:workspaces rw`;
  // The mount of the pod a mount that takes over replaces, served still.
  writeFileSync(mountinfo, left || juicefs === "takes over" ? `${line}\n` : "");
  if (left) writeFileSync(dead, "");
  // Served, it reads as a directory.
  if (juicefs === "takes over") mkdirSync(root);
  const told = join(directory, "take-over");
  const serving = `echo 'juicefs[7] <INFO>: OK, workspaces ${SERVING} "${root}" [checkMountpoint@mount_unix.go:327]'`;
  const what: Record<Juicefs, string> = {
    serves: `printf %s "$META_PASSWORD" > ${join(directory, "meta-password")}; echo '${line}' >> ${mountinfo}; echo mounted; ${serving}; trap 'exit 0' TERM; while :; do sleep 0.1; done`,
    "takes over": `trap 'exit 0' TERM; while [ ! -e ${told} ]; do sleep 0.05; done; ${serving}; while :; do sleep 0.1; done`,
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
  // The pod's DNS: the cluster's two Services, and nothing else.
  const resolve = join(directory, "getent");
  writeFileSync(resolve, [
    "#!/bin/sh",
    'case "$2" in',
    "  alasio-valkey.alasio.svc.cluster.local) echo '10.43.0.7       alasio-valkey.alasio.svc.cluster.local';;",
    "  alasio-seaweedfs.alasio.svc.cluster.local) echo '10.43.0.8       alasio-seaweedfs.alasio.svc.cluster.local';;",
    "  *) exit 2;;",
    "esac",
  ].join("\n"));
  for (const file of [image, inNode, resolve]) chmodSync(file, 0o755);
  const paths: MountPaths = { run: join(directory, "run"), bin: join(directory, "bin"), state: join(directory, "state"), cache: join(directory, "cache"), juicefs: image, mountinfo, inNode, resolve: `${resolve} hosts` };
  mkdirSync(paths.run);
  const env = { ...process.env, META_URL: "redis://:a%40secret@alasio-valkey.alasio.svc.cluster.local:6379/1", BUCKET: "http://alasio-seaweedfs.alasio.svc.cluster.local:8333/workspaces", ...given };
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
    takeOver: () => writeFileSync(told, ""),
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
  // The cluster's names, which the node's namespace resolves none of, given as their addresses, by the pod's DNS.
  assert.ok(serving.inNode().at(-1)?.includes("--bucket http://10.43.0.8:8333/workspaces"), serving.inNode().at(-1));
  // The metadata engine's password in META_PASSWORD, not on the command line.
  assert.ok(serving.inNode().at(-1)?.endsWith(`redis://10.43.0.7:6379/1 ${serving.root}`), serving.inNode().at(-1));
  assert.ok(!serving.inNode().some((command) => command.includes("secret")), serving.inNode().join("\n"));
  assert.equal(readFileSync(join(serving.paths.run, "..", "meta-password"), "utf8"), "a@secret");
  serving.child.kill("SIGTERM");
  assert.notEqual(await serving.ended, 0);
  assert.equal(existsSync(join(serving.paths.run, "handed-over")), false);
});

test("a mount whose juicefs runs is not ready while the node's table holds the mount of the pod it replaces, before its juicefs says it serves the mount point itself, as that pod is deleted, its mount with it, once it is", async (t) => {
  const taking = mount(t, "takes over");
  await until(() => existsSync(join(taking.paths.run, "juicefs.pid")));
  await sleep(500);
  assert.equal(taking.ready(), false);
  taking.takeOver();
  await until(() => taking.ready());
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
  // As its output comes, which may be after its mount is ready.
  await until(() => serving.out().includes("unmounted the node's mount its juicefs left"));
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

test("a mount whose metadata engine or bucket is no Service of the cluster's is given them as they are, and one whose Service's name resolves to nothing mounts nothing, a failure", async (t) => {
  const outside = mount(t, "serves", { env: { META_URL: "rediss://:pw@valkey.example.com:6380/1", BUCKET: "https://s3.example.com/workspaces" } });
  await until(() => outside.ready());
  assert.ok(outside.inNode().at(-1)?.includes("--bucket https://s3.example.com/workspaces "), outside.inNode().at(-1));
  assert.ok(outside.inNode().at(-1)?.includes(" rediss://valkey.example.com:6380/1 "), outside.inNode().at(-1));
  const unresolved = mount(t, "serves", { env: { META_URL: "redis://:pw@alasio-gone.alasio.svc.cluster.local:6379/1" } });
  assert.equal(await unresolved.ended, 1);
  assert.match(unresolved.out(), /alasio-gone\.alasio\.svc\.cluster\.local resolves to no address/u);
  assert.deepEqual(unresolved.inNode(), []);
});

test("up reads the UIDs of the Services the mount reaches by address, none of one not there", async (t) => {
  const kube = await serveFakeKube();
  const directory = mkdtempSync(join(tmpdir(), "alasio-mount-services-"));
  t.after(async () => {
    await kube.close();
    rmSync(directory, { recursive: true, force: true });
  });
  const kubeconfig = join(directory, "kubeconfig");
  writeFileSync(kubeconfig, kube.kubeconfig);
  kube.put("services", { apiVersion: "v1", kind: "Service", metadata: { name: "alasio-valkey", namespace: "alasio", uid: "uid-valkey" }, spec: {} });
  const read = await Effect.runPromise(Effect.provide(readMountServices(["alasio-valkey", "alasio-seaweedfs"]), KubeApi.layer({ path: kubeconfig })));
  assert.deepEqual(read, { "alasio-valkey": "uid-valkey", "alasio-seaweedfs": null });
});
