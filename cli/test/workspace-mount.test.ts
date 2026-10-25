/**
 * The script of the node's mount of workspace storage (cli/src/manifests/sandbox-gateway.ts),
 * run by sh with stand-ins for juicefs, for the node's mount table and for entering the node's
 * mount namespace: its juicefs copied to the node and run there; idle once its mount is handed
 * over, a failure once it ends any other way, ready only while its own mount serves the node's
 * mount point; and a mount left by a juicefs gone unmounted before it mounts, one that serves
 * though it fails its reads taken over. And the Services
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
type Juicefs = "serves" | "takes over" | "hands over" | "fails" | "never serves" | "dies on quit";

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
 * already, which the kernel answers as not connected until it is unmounted; where
 * `unreachable`, the mount it takes over fails its reads, as one whose metadata engine is down;
 * where `unreadable`, its image's juicefs cannot be read, as one of a layer whose read failed;
 * where `hung`, the table holds a mount at the root already that answers nothing, as one whose
 * juicefs is stopped or hangs does, until it is unmounted; where `slow`, the mount it takes over
 * answers what needs its metadata engine, a listing, in no less than a minute, as one whose
 * metadata engine is slow does, and what it holds in memory, its .config, at once; where `held`,
 * the table holds the mount of the pod it replaces, which answers its .config, as one its
 * juicefs takes over does; where `foreign`, it holds, on top of any other at the root, a mount of
 * the same minor that is no FUSE connection's: a block device's FUSE file system; where `takes`, its juicefs, as it starts, takes the node's mount's
 * session, which no process serves then, so the mount answers nothing; where `fusectl`, the
 * node has fusectl's mount. `serveWaitSeconds` is how long its juicefs has to serve.
 */
function mount(t: TestContext, juicefs: Juicefs, { left = false, unreachable = false, unreadable = false, hung = false, slow = false, held = false, foreign = false, takes = false, fusectl = false, serveWaitSeconds = 180, env: given = {} as Record<string, string> } = {}): Mount & { readonly takeOver: () => void; readonly aborted: () => string } {
  const directory = mkdtempSync(join(tmpdir(), "alasio-workspace-mount-"));
  const root = join(directory, "workspaces");
  const mountinfo = join(directory, "mountinfo");
  const ran = join(directory, "in-node");
  const dead = join(directory, "dead");
  const failing = join(directory, "failing");
  const hanging = join(directory, "hanging");
  const slowed = join(directory, "slow");
  const line = `36 1 0:50 / ${root} rw,relatime shared:9 - fuse.juicefs JuiceFS:workspaces rw`;
  // The mount of the pod a mount that takes over replaces, served still.
  writeFileSync(mountinfo, `${left || hung || held || juicefs === "takes over" ? `${line}\n` : ""}${foreign ? `37 1 0:50 / ${root} rw - fuseblk /dev/sdb1 rw\n` : ""}`);
  // The node's FUSE connections, fusectl's: the mount's, of its device's minor, aborted as 1 is written to its abort.
  const fuse = join(directory, "fuse-connections");
  mkdirSync(join(fuse, "50"), { recursive: true });
  writeFileSync(join(fuse, "50", "abort"), "");
  if (fusectl) writeFileSync(mountinfo, `${readFileSync(mountinfo, "utf8")}24 22 0:21 / ${fuse} rw - fusectl fusectl rw\n`);
  const taking = takes ? `require("node:fs").writeFileSync(${JSON.stringify(hanging)}, ""); ` : "";
  if (hung) writeFileSync(hanging, "");
  if (left) writeFileSync(dead, "");
  if (unreachable) writeFileSync(failing, "");
  if (slow) writeFileSync(slowed, "");
  // Served, it reads as a directory.
  if (juicefs === "takes over") mkdirSync(root);
  const told = join(directory, "take-over");
  const serving = `echo 'juicefs[7] <INFO>: OK, workspaces ${SERVING} "${root}" [checkMountpoint@mount_unix.go:327]'`;
  const what: Record<Juicefs, string> = {
    serves: `printf %s "$META_PASSWORD" > ${join(directory, "meta-password")}; echo '${line}' >> ${mountinfo}; echo mounted; ${serving}; trap 'exit 0' TERM; while :; do sleep 0.1; done`,
    "takes over": `trap 'exit 0' TERM; while [ ! -e ${told} ]; do sleep 0.05; done; ${serving}; while :; do sleep 0.1; done`,
    "hands over": `echo 'juicefs[7] <INFO>: ${HANDED_OVER} [launchMount@mount_unix.go:1057]'; exit 1`,
    fails: "echo 'juicefs[7] <FATAL>: the metadata engine refused us'; exit 3",
    // As one held up on a read of a mount that answers nothing: it says nothing more, and ends on KILL alone.
    // Node, not sh, which an asynchronous list's QUIT is ignored in, past any trap. As one held up where no signal but
    // KILL ends it, it says its goroutines on QUIT, as Go's runtime does, and so does the mount process it runs, and
    // goes on; or, as Go's runtime, says them and ends.
    "never serves": `exec node -e '${taking}console.log("juicefs[7] <INFO>: Data use s3://bucket/workspaces/"); require("node:child_process").spawn(process.execPath, ["-e", "process.on(\\"SIGQUIT\\", () => console.log(\\"goroutine 9 [mount process]:\\")); setInterval(() => {}, 100)"], { stdio: "inherit" }); process.on("SIGTERM", () => console.log("TERM heard")); process.on("SIGQUIT", () => console.log("goroutine 1 [chan receive]:")); setInterval(() => {}, 100)'`,
    "dies on quit": `exec node -e '${taking}console.log("juicefs[7] <INFO>: Data use s3://bucket/workspaces/"); process.on("SIGTERM", () => {}); process.on("SIGQUIT", () => { console.log("goroutine 1 [chan receive]:"); process.exit(2); }); setInterval(() => {}, 100)'`,
  };
  const image = join(directory, "image-juicefs");
  writeFileSync(image, `#!/bin/sh\n${what[juicefs]}\n`);
  // The node's namespace, the test's own: each command logged, as the kernel and juicefs answer:
  // a dead mount not connected, a hung one nothing; one whose metadata engine is down failing
  // its listing, one whose metadata engine is slow listing in a minute; and, of each that
  // serves, its .config read at once; its umount taking it out of the table.
  const inNode = join(directory, "nsenter");
  writeFileSync(inNode, [
    "#!/bin/sh",
    `echo "$*" >> ${ran}`,
    `if [ "$1" = ls ] || [ "$1" = cat ]; then`,
    `  if [ -e ${dead} ]; then echo "$1: '$2': Transport endpoint is not connected" >&2; exit 2; fi`,
    // Hung, as a read a juicefs took but never answers is, which no wait outlasts: KILLed, it leaves behind what holds
    // its output open, out of its group's reach, as the read itself stays where no signal ends it.
    `  if [ -e ${hanging} ]; then setsid sleep 60 & wait; fi`,
    "fi",
    `if [ "$1" = cat ] && [ "$2" = ${root}/.config ]; then echo '{"Meta": {}}'; exit 0; fi`,
    `if [ "$1" = ls ] && [ -e ${failing} ]; then echo "ls: reading directory '$2': Input/output error" >&2; exit 2; fi`,
    `if [ "$1" = ls ] && [ -e ${slowed} ]; then sleep 60; fi`,
    `if [ "$1" = umount ]; then rm -f ${dead} ${hanging}; : > ${mountinfo}; exit 0; fi`,
    `if [ "$1" = mount ]; then exit 0; fi`,
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
  const paths: MountPaths = { run: join(directory, "run"), bin: join(directory, "bin"), state: join(directory, "state"), cache: join(directory, "cache"), juicefs: unreadable ? join(directory, "unread-juicefs") : image, mountinfo, inNode, resolve: `${resolve} hosts`, fuseConnections: fuse };
  mkdirSync(paths.run);
  const env = { ...process.env, META_URL: "redis://:a%40secret@alasio-valkey.alasio.svc.cluster.local:6379/1", BUCKET: "http://alasio-seaweedfs.alasio.svc.cluster.local:8333/workspaces", ...given };
  // A group of its own, so whatever it started is ended with it.
  const child = spawn("sh", ["-c", workspaceMountScript(root, 1024, paths, serveWaitSeconds)], { env, stdio: ["ignore", "pipe", "pipe"], detached: true });
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
    aborted: () => readFileSync(join(fuse, "50", "abort"), "utf8"),
    ready: () => spawnSync("sh", ["-c", workspaceMountReady(root, paths)], { env }).status === 0,
    inNode: () => (existsSync(ran) ? readFileSync(ran, "utf8").trim().split("\n") : []),
  };
}

/** Waits until `condition` holds, for `tries` looks 50 ms apart, a few seconds unless given. */
async function until(condition: () => boolean, tries = 100): Promise<void> {
  for (let n = 0; n < tries && !condition(); n += 1) await sleep(50);
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

test("a mount that hands its session over idles, ready, until its pod is deleted, as Kubernetes deletes it at once otherwise, its successor not ready yet", async (t) => {
  const handed = mount(t, "hands over");
  await until(() => existsSync(join(handed.paths.run, "handed-over")));
  assert.equal(handed.ready(), true);
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

test("a mount whose image's juicefs cannot be read, as one of a layer whose read failed, fails before it runs any juicefs, saying so", async (t) => {
  const unread = mount(t, "serves", { unreadable: true });
  assert.equal(await unread.ended, 1);
  assert.match(unread.out(), /could not read .*unread-juicefs, so the mount is not started/u);
  assert.deepEqual(unread.inNode(), []);
  assert.deepEqual(readdirSync(unread.paths.bin), []);
});

test("a mount whose juicefs serves nothing within its wait, as one held up taking over a mount that answers nothing, has it ended, KILLed where TERM does not end it, and fails, so kubelet starts it again", { timeout: 30_000 }, async (t) => {
  const stuck = mount(t, "never serves", { serveWaitSeconds: 2 });
  const started = Date.now();
  assert.equal(await stuck.ended, 137);
  assert.ok(Date.now() - started < 15_000, `ended ${Date.now() - started} ms on`);
  assert.match(stuck.out(), /juicefs served nothing within 2 s of starting, so it is ended, and the mount started again to mount afresh/u);
  assert.match(stuck.out(), /juicefs mount ended \(137\) without handing its mount over/u);
  assert.equal(stuck.ready(), false);
});

test("a mount whose juicefs serves nothing within its wait, holding the node's mount's session, which no process serves, says its goroutines' waits and those of the process it runs, has the mount's FUSE connection aborted before it is ended, as what holds it up may be a request on that session, which no signal ends, and the dead mount unmounted", { timeout: 60_000 }, async (t) => {
  const stuck = mount(t, "never serves", { held: true, takes: true, serveWaitSeconds: 2 });
  assert.equal(await stuck.ended, 137);
  assert.equal(stuck.aborted(), "1\n");
  assert.match(stuck.out(), /goroutine 9 \[mount process\]:/u);
  assert.match(stuck.out(), /goroutine 1 \[chan receive\]:[^]*aborted the FUSE connection 50 of the node's mount, so what waits on it fails[^]*TERM heard[^]*unmounted the node's mount, which no juicefs served/u);
  assert.equal(readFileSync(stuck.paths.mountinfo, "utf8"), "");
  // fusectl's mount made, the node having none.
  assert.ok(stuck.inNode().includes(`mount -t fusectl none ${stuck.paths.fuseConnections}`), stuck.inNode().join("\n"));
  // A juicefs that ends on QUIT, as Go's runtime does, has its mount aborted and unmounted all the same; and a node with fusectl's mount has none made.
  const ending = mount(t, "dies on quit", { held: true, takes: true, fusectl: true, serveWaitSeconds: 2 });
  assert.equal(await ending.ended, 2);
  assert.equal(ending.aborted(), "1\n");
  assert.match(ending.out(), /unmounted the node's mount, which no juicefs served/u);
  assert.ok(!ending.inNode().some((command) => command.startsWith("mount -t fusectl")), ending.inNode().join("\n"));
});

test("a mount whose juicefs serves nothing within its wait, the node's mount it would take over served still, as before it asks for the session, is ended alone, that mount neither aborted nor unmounted, for the next to take over", { timeout: 30_000 }, async (t) => {
  const stuck = mount(t, "never serves", { held: true, serveWaitSeconds: 2 });
  assert.equal(await stuck.ended, 137);
  assert.equal(stuck.aborted(), "");
  assert.ok(!stuck.inNode().some((command) => command.startsWith("umount")), stuck.inNode().join("\n"));
  assert.notEqual(readFileSync(stuck.paths.mountinfo, "utf8"), "");
});

test("a mount whose juicefs serves nothing within its wait aborts no connection where the root holds no mount, or, topmost, one of no FUSE connection's", { timeout: 30_000 }, async (t) => {
  for (const none of [mount(t, "never serves", { takes: true, serveWaitSeconds: 2 }), mount(t, "never serves", { foreign: true, takes: true, serveWaitSeconds: 2 }), mount(t, "never serves", { held: true, foreign: true, takes: true, serveWaitSeconds: 2 })]) {
    assert.equal(await none.ended, 137);
    assert.equal(none.aborted(), "");
    assert.doesNotMatch(none.out(), /aborted the FUSE connection/u);
  }
});

test("a mount at the root that answers nothing, as one whose juicefs is stopped or hangs, not even its .config, is unmounted within 5 s before the node is mounted afresh", async (t) => {
  const serving = mount(t, "serves", { hung: true });
  await until(() => serving.ready(), 200);
  const ran = serving.inNode();
  assert.ok(ran.indexOf(`umount -l ${serving.root}`) >= 0 && ran.indexOf(`umount -l ${serving.root}`) < ran.findIndex((command) => command.includes(" mount --no-bgjob ")), ran.join("\n"));
  await until(() => serving.out().includes("unmounted the node's mount, whose juicefs answered nothing in 5 s, not even its .config"));
  // Its FUSE connection aborted first, so what waits on it, unanswerable, fails rather than stays.
  assert.equal(serving.aborted(), "1\n");
  const aborting = ran.findIndex((command) => command.startsWith("sh -c") && command.includes("abort"));
  assert.ok(aborting >= 0 && aborting < ran.indexOf(`umount -l ${serving.root}`), ran.join("\n"));
});

test("a mount whose metadata engine is slow, as it is where `up` rolls the mount, its listing taking a minute, but which answers its .config, is taken over, not unmounted", async (t) => {
  const taking = mount(t, "takes over", { slow: true });
  await until(() => existsSync(join(taking.paths.run, "juicefs.pid")), 200);
  taking.takeOver();
  await until(() => taking.ready());
  assert.ok(!taking.inNode().some((command) => command.startsWith("umount")), taking.inNode().join("\n"));
  assert.doesNotMatch(taking.out(), /unmounted the node's mount/u);
});

test("a mount left at the root by a juicefs gone, as a pod killed with no successor leaves one, is unmounted before the node is mounted afresh", async (t) => {
  const serving = mount(t, "serves", { left: true });
  await until(() => serving.ready());
  const ran = serving.inNode();
  assert.ok(ran.indexOf(`umount -l ${serving.root}`) >= 0 && ran.indexOf(`umount -l ${serving.root}`) < ran.findIndex((command) => command.includes(" mount --no-bgjob ")), ran.join("\n"));
  // As its output comes, which may be after its mount is ready.
  await until(() => serving.out().includes("unmounted the node's mount its juicefs left"));
  // One that serves is left be.
  const whole = mount(t, "serves");
  await until(() => whole.ready());
  assert.ok(!whole.inNode().some((command) => command.startsWith("umount")));
});

test("a mount whose juicefs serves but fails its reads, as its metadata engine restarts, is taken over, not unmounted", async (t) => {
  const taking = mount(t, "takes over", { unreachable: true });
  await until(() => existsSync(join(taking.paths.run, "juicefs.pid")));
  taking.takeOver();
  await until(() => taking.ready());
  assert.ok(!taking.inNode().some((command) => command.startsWith("umount")), taking.inNode().join("\n"));
  assert.doesNotMatch(taking.out(), /unmounted the node's mount/u);
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
