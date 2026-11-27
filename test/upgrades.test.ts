/**
 * The cut-over of an installation of the release before (upgrades/from-10.0.27.sh), in how it
 * runs, without a cluster: its checks and refusals, its point of no return, the session
 * image's layers mounted anew once it has upgraded, and how a run again goes on from where
 * one before stopped. The end-to-end run's upgrade shard runs it on an installation of that
 * release (test/e2e/alasio.test.ts).
 */
import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { once } from "node:events";
import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { delimiter, join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

import { FUSE_MANAGER_SOCKET, FUSE_MANAGER_STORE, STARGZ_CONFIG, STARGZ_ROOT, STARGZ_SOCKET, stargzConfig } from "../cli/src/cluster/stargz.ts";

const SCRIPT = fileURLToPath(new URL("../upgrades/from-10.0.27.sh", import.meta.url));

/** The PATH the cut-over runs with: `bin`'s, and the system's. */
const pathOf = (bin: string) => [bin, "/usr/bin", "/bin"].join(delimiter);

/**
 * A machine for the cut-over: a kubectl that reaches its cluster and lists the namespaces
 * `namespaces` names, `<name>\t<branch label>` each, and deletes a pod, its containers ended
 * with it; an UPGRADE and an UPGRADE_CHECK, each saying it ran, UPGRADE failing while
 * `upgradeFails` is there; and its root, NODE_ROOT, holding no stargz-snapshotter of alasio's
 * until `stargz` makes one.
 */
function machine() {
  const root = mkdtempSync(join(tmpdir(), "alasio-cut-over-"));
  const bin = join(root, "bin");
  const ran = join(root, "ran");
  const namespaces = join(root, "namespaces");
  const upgradeFails = join(root, "upgrade-fails");
  const config = join(root, "config.json");
  const node = join(root, "node");
  /** What NODE_ROOT has mounted; each container's pod, `<id> <namespace> <name>` a line; and the pods deleted. */
  const mountinfo = join(node, "proc", "self", "mountinfo");
  const pods = join(root, "pods");
  const deleted = join(root, "deleted");
  mkdirSync(bin);
  mkdirSync(join(node, "proc", "self"), { recursive: true });
  writeFileSync(mountinfo, "");
  writeFileSync(pods, "");
  writeFileSync(namespaces, "");
  writeFileSync(config, JSON.stringify({ target: { host: {} }, install: { host: { enabled: true } } }), { mode: 0o600 });
  writeFileSync(join(bin, "kubectl"), `#!/bin/sh
case "$*" in
  "get --raw=/readyz") exit 0 ;;
  "get namespace -l alasio.dev/branch -o jsonpath="*) cat '${namespaces}' ;;
  "-n "*" delete pod "*" --wait=false --ignore-not-found")
    echo "$2/$5" >>'${deleted}'
    # Its containers ended and removed, their roots unmounted.
    awk -v ns="$2" -v pod="$5" '$2 == ns && $3 == pod { print "/k8s.io/" $1 "/rootfs " }' '${pods}' >'${root}/ended'
    grep -v -F -f '${root}/ended' '${mountinfo}' >'${mountinfo}.left' || true
    mv '${mountinfo}.left' '${mountinfo}' ;;
  *) echo "kubectl $* is not the cut-over's" >&2; exit 2 ;;
esac
`, { mode: 0o755 });
  const out = join(root, "out");
  /** The cut-over, failing at `failAt` if given: what it came to. */
  const cutOver = (failAt?: string) => {
    const run = spawnSync("bash", [SCRIPT], {
      encoding: "utf8",
      env: {
        PATH: pathOf(bin),
        OUT: out,
        CONFIG: config,
        UPGRADE: `echo upgrade >>'${ran}' && [ ! -e '${upgradeFails}' ]`,
        UPGRADE_CHECK: `echo check >>'${ran}'`,
        NODE_ROOT: node,
        ...(failAt === undefined ? {} : { FAIL_AT: failAt }),
      },
    });
    return { code: run.status, said: run.stderr };
  };
  return {
    cutOver,
    /** What UPGRADE and UPGRADE_CHECK ran, in order, since the last time it was asked. */
    ran: () => {
      const read = existsSync(ran) ? readFileSync(ran, "utf8").split("\n").filter(Boolean) : [];
      rmSync(ran, { force: true });
      return read;
    },
    /** The cut-over's markers, of what it recorded. */
    marks: () => (existsSync(out) ? readdirSync(out).filter((name) => ["point-of-no-return", "upgraded"].includes(name)).sort() : []),
    namespaces: (listed: string) => writeFileSync(namespaces, listed),
    /** Makes NODE_ROOT a node of alasio's, its stargz-snapshotter's daemon running with the configuration `passthrough` says. */
    stargz: (passthrough: boolean) => stargzNode({ root, bin, node, pods, passthrough }),
    /** The pods kubectl deleted, `<namespace>/<name>` each. */
    deleted: () => (existsSync(deleted) ? readFileSync(deleted, "utf8").split("\n").filter(Boolean) : []),
    /** The cut-over, its process's own, beside what the test runs meanwhile, ended past a minute: what it came to, and what it said. */
    cutOverAsync: async () => {
      // Its steps traced apart from what it says, on a descriptor of their own.
      const child = spawn("bash", ["-x", SCRIPT], {
        env: { PATH: pathOf(bin), OUT: out, CONFIG: config, UPGRADE: "true", UPGRADE_CHECK: "true", NODE_ROOT: node, BASH_XTRACEFD: "3" },
        stdio: ["ignore", "ignore", "pipe", "pipe"],
      });
      let said = "";
      let traced = "";
      child.stderr?.on("data", (chunk: Buffer) => (said += chunk.toString()));
      child.stdio[3]?.on("data", (chunk: Buffer) => (traced += chunk.toString()));
      const closed = once(child, "close").then(() => true);
      const ended = await Promise.race([closed, new Promise<false>((resolve) => setTimeout(() => resolve(false), 60_000))]);
      const exited = child.exitCode;
      if (!ended) child.kill("SIGKILL");
      assert.ok(ended, `the cut-over ${exited === null ? "ran on" : `exited with ${exited}, its output held open,`} past a minute, its last steps:\n${traced.split("\n").slice(-30).join("\n")}\nhaving said:\n${said}`);
      return { code: child.exitCode, said };
    },
    upgradeFails: (fails: boolean) => (fails ? writeFileSync(upgradeFails, "") : rmSync(upgradeFails, { force: true })),
    remove: () => rmSync(root, { recursive: true, force: true }),
  };
}

test("the cut-over checks, upgrades past its point of no return, and says it is through", () => {
  const at = machine();
  try {
    const run = at.cutOver();
    assert.equal(run.code, 0, run.said);
    assert.deepEqual(at.ran(), ["check", "upgrade"]);
    assert.deepEqual(at.marks(), ["point-of-no-return", "upgraded"]);
    assert.match(run.said, /cutting over from 10\.0\.27[^\n]*\n[^\n]* checking that no branch environment exists[^\n]*\n[^\n]* checking that the upgrade can run[^\n]*\n[^\n]* past the point of no return[^\n]*\n[^\n]* upgrading, from the cut-over begun at [^\n]*\n[^\n]* no stargz-snapshotter daemon of alasio's on this machine serves the session image through FUSE passthrough: no layer to mount anew\n[^\n]* upgraded at \S+\n[^\n]* through\n$/u);
  } finally {
    at.remove();
  }
});

test("the cut-over refuses a branch environment before it changes anything, naming each to delete, and no namespace that is no branch's own", () => {
  const at = machine();
  try {
    at.namespaces("alasio-branch-one\tone\nalasio-branch-two\ttwo\nalasio-sessions\tone\nother\t\n");
    const refused = at.cutOver();
    assert.notEqual(refused.code, 0);
    assert.match(refused.said, /FAILED: alasio upgrades main alone, and the branch environments 10\.0\.27 made [^\n]*\(alasio branch delete one, alasio branch delete two\)/u);
    assert.deepEqual([at.ran(), at.marks()], [[], []]);
    // A namespace labelled for a branch that is not that branch's own is no branch environment.
    at.namespaces("alasio-sessions\tone\n");
    assert.equal(at.cutOver().code, 0);
  } finally {
    at.remove();
  }
});

test("the cut-over failing before its point of no return leaves none, and a run again starts over", () => {
  const at = machine();
  try {
    const failed = at.cutOver("before-the-point-of-no-return");
    assert.notEqual(failed.code, 0);
    assert.match(failed.said, /checking that the upgrade can run[^\n]*\n[^\n]* FAILED: a failure injected before the point of no return\n$/u);
    assert.deepEqual([at.ran(), at.marks()], [["check"], []]);
    const again = at.cutOver();
    assert.equal(again.code, 0, again.said);
    assert.match(again.said, /checking that no branch environment exists/u);
    assert.deepEqual([at.ran(), at.marks()], [["check", "upgrade"], ["point-of-no-return", "upgraded"]]);
  } finally {
    at.remove();
  }
});

test("the cut-over failing past its point of no return, or in its upgrade, goes on with the upgrade when run again, checking nothing again", () => {
  const at = machine();
  try {
    const failed = at.cutOver("past-the-point-of-no-return");
    assert.notEqual(failed.code, 0);
    assert.match(failed.said, /FAILED: a failure injected past the point of no return, before the upgrade\n$/u);
    assert.deepEqual([at.ran(), at.marks()], [["check"], ["point-of-no-return"]]);
    at.upgradeFails(true);
    const broken = at.cutOver();
    assert.notEqual(broken.code, 0);
    assert.match(broken.said, /FAILED: the upgrade failed \(upgrade\.log\): the cut-over run again goes on with it\n$/u);
    assert.deepEqual([at.ran(), at.marks()], [["upgrade"], ["point-of-no-return"]]);
    at.upgradeFails(false);
    const again = at.cutOver();
    assert.equal(again.code, 0, again.said);
    assert.doesNotMatch(again.said, /checking that/u);
    assert.deepEqual([at.ran(), at.marks()], [["upgrade"], ["point-of-no-return", "upgraded"]]);
  } finally {
    at.remove();
  }
});

test("the cut-over failing once its upgrade is through goes on with what follows the upgrade alone when run again", () => {
  const at = machine();
  try {
    const failed = at.cutOver("after-the-upgrade");
    assert.notEqual(failed.code, 0);
    assert.match(failed.said, /FAILED: a failure injected once the upgrade was through\n$/u);
    assert.deepEqual([at.ran(), at.marks()], [["check", "upgrade"], ["point-of-no-return", "upgraded"]]);
    const again = at.cutOver();
    assert.equal(again.code, 0, again.said);
    assert.doesNotMatch(again.said, /checking that|upgrading, from the cut-over/u);
    assert.match(again.said, /^[^\n]* no stargz-snapshotter daemon of alasio's on this machine serves[^\n]*\n[^\n]* upgraded at \S+\n[^\n]* through\n$/u);
    assert.deepEqual(at.ran(), []);
  } finally {
    at.remove();
  }
});

test("the cut-over refuses to run without what it needs, or a cluster to reach, before it changes anything", () => {
  const root = mkdtempSync(join(tmpdir(), "alasio-cut-over-"));
  try {
    const run = spawnSync("bash", [SCRIPT], { encoding: "utf8", env: { PATH: "/usr/bin:/bin" } });
    assert.notEqual(run.status, 0);
    assert.match(run.stderr, /UPGRADE: the upgrade of the release/u);
    const unchecked = spawnSync("bash", [SCRIPT], { encoding: "utf8", env: { PATH: "/usr/bin:/bin", UPGRADE: "true" } });
    assert.notEqual(unchecked.status, 0);
    assert.match(unchecked.stderr, /UPGRADE_CHECK: a command proving UPGRADE can run/u);
    const needs = { PATH: pathOf(join(root, "bin")), OUT: join(root, "out"), UPGRADE: "true", UPGRADE_CHECK: "true" };
    const unconfigured = spawnSync("bash", [SCRIPT], { encoding: "utf8", env: needs });
    assert.notEqual(unconfigured.status, 0);
    assert.match(unconfigured.stderr, /CONFIG: the file of the configuration/u);
    // A kubectl that reaches no cluster.
    mkdirSync(join(root, "bin"));
    writeFileSync(join(root, "bin", "kubectl"), "#!/bin/sh\nexit 1\n", { mode: 0o755 });
    const unread = spawnSync("bash", [SCRIPT], { encoding: "utf8", env: { ...needs, CONFIG: join(root, "none.json") } });
    assert.notEqual(unread.status, 0);
    assert.match(unread.stderr, /FAILED: CONFIG \S+none\.json cannot be read/u);
    writeFileSync(join(root, "config.json"), "{}");
    const unreached = spawnSync("bash", [SCRIPT], { encoding: "utf8", env: { ...needs, CONFIG: join(root, "config.json") } });
    assert.notEqual(unreached.status, 0);
    assert.match(unreached.stderr, /FAILED: kubectl does not reach the cluster/u);
    assert.ok(!existsSync(join(root, "out", "point-of-no-return")));
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

/** Where stargz-snapshotter's daemon mounts the layer of the snapshot `id`. */
const layer = (id: number) => `${STARGZ_ROOT}/snapshotter/snapshots/${id}/fs`;
/** A container's id, 64 hex digits of `digit`. */
const containerId = (digit: string) => digit.repeat(64);
/** The containers of the node stargzNode makes: a session's bound pod's and a free pod's, on the daemon's layers; a pod's of an image pulled whole. */
const ON_LAYERS = { bound: containerId("a"), free: containerId("c") };
const PULLED_WHOLE = containerId("b");

/**
 * NODE_ROOT made a node of alasio's: the daemon's configuration, with FUSE passthrough as
 * `passthrough` says; its FUSE manager, a process of the test's own, by its executable and its
 * start; what is mounted: two layers of the daemon's, workspace storage's FUSE mount, and three
 * containers' roots, ON_LAYERS on those layers and PULLED_WHOLE on a snapshot of an image pulled
 * whole; and a systemctl that stops and starts the daemon, which starts a manager of its own as
 * it starts and makes its socket, failing while `startFails` is there; a umount that says what it
 * is asked; and a k3s crictl that says each container's pod.
 */
function stargzNode({ root, bin, node, pods, passthrough }: { root: string; bin: string; node: string; pods: string; passthrough: boolean }) {
  const at = (path: string) => join(node, path);
  mkdirSync(at("/etc/containerd-stargz-grpc"), { recursive: true });
  writeFileSync(at(STARGZ_CONFIG), stargzConfig(undefined, passthrough));
  mkdirSync(at(STARGZ_ROOT), { recursive: true });
  mkdirSync(at("/run/containerd-stargz-grpc"), { recursive: true });
  writeFileSync(at(FUSE_MANAGER_SOCKET), "");
  writeFileSync(at(FUSE_MANAGER_STORE), "");
  writeFileSync(at(STARGZ_SOCKET), "");
  const manager = spawn("sleep", ["600"], { stdio: "ignore" });
  /** A process of NODE_ROOT's, running `exe`, started at `ticks`. */
  const processOf = (pid: number, exe: string, ticks: number) => {
    mkdirSync(at(`/proc/${pid}`), { recursive: true });
    symlinkSync(exe, at(`/proc/${pid}/exe`));
    writeFileSync(at(`/proc/${pid}/stat`), `${pid} (${exe.slice(exe.lastIndexOf("/") + 1, exe.lastIndexOf("/") + 16)}) S 1 1 1 0 -1 0 0 0 0 0 0 0 0 0 20 0 1 0 ${ticks} 0 0\n`);
  };
  processOf(manager.pid ?? 0, "/usr/local/bin/stargz-fuse-manager", 4242);
  processOf(1, "/usr/lib/systemd/systemd", 1);
  const rootfs = (id: string, lower: string) =>
    `50 25 0:50 / /run/k3s/containerd/io.containerd.runtime.v2.task/k8s.io/${id}/rootfs rw,relatime - overlay overlay rw,lowerdir=${lower},upperdir=${layer(90)}/u,workdir=${layer(90)}/w`;
  writeFileSync(at("/proc/self/mountinfo"), [
    `36 25 0:31 / ${layer(12)} ro,nodev,relatime shared:1 - fuse.rawBridge stargz ro,user_id=0,group_id=0`,
    `37 25 0:32 / ${layer(123)} ro,nodev,relatime shared:2 - fuse.rawBridge stargz ro,user_id=0,group_id=0`,
    "38 25 0:33 / /var/lib/alasio/workspaces rw,relatime shared:3 - fuse.juicefs JuiceFS:alasio rw,user_id=0",
    rootfs(ON_LAYERS.bound, `${layer(123)}:${layer(12)}`),
    rootfs(PULLED_WHOLE, layer(7)),
    rootfs(ON_LAYERS.free, layer(12)),
    "",
  ].join("\n"));
  writeFileSync(pods, `${ON_LAYERS.bound} alasio-sessions sandboxes-none-bound\n${PULLED_WHOLE} alasio alasio-gateway-0\n${ON_LAYERS.free} alasio-sessions sandboxes-none-free\n`);
  const calls = join(root, "calls");
  const startFails = join(root, "start-fails");
  const state = join(root, "daemon");
  writeFileSync(state, "active");
  writeFileSync(join(bin, "systemctl"), `#!/bin/sh
echo "systemctl $*" >>'${calls}'
case "$*" in
  "stop stargz-snapshotter") echo inactive >'${state}' ;;
  "start stargz-snapshotter")
    [ ! -e '${startFails}' ] || exit 1
    echo active >'${state}'
    mkdir -p '${at("/proc/4343")}' && ln -sfn /usr/local/bin/stargz-fuse-manager '${at("/proc/4343/exe")}'
    '${process.execPath}' -e 'require("net").createServer().listen(process.argv[1], () => process.exit(0))' '${at(STARGZ_SOCKET)}' ;;
  "is-active --quiet stargz-snapshotter") [ "$(cat '${state}')" = active ] ;;
  *) exit 2 ;;
esac
`, { mode: 0o755 });
  writeFileSync(join(bin, "umount"), `#!/bin/sh\necho "umount $*" >>'${calls}'\n`, { mode: 0o755 });
  writeFileSync(join(bin, "k3s"), `#!/bin/sh
case "$*" in
  "crictl inspect --output go-template --template "*) for id; do :; done; awk -v id="$id" '$1 == id { print $2, $3 }' '${pods}' ;;
  *) exit 2 ;;
esac
`, { mode: 0o755 });
  return {
    manager,
    at,
    /** What systemctl and umount were asked, in order, since the last time it was asked. */
    calls: () => {
      const read = existsSync(calls) ? readFileSync(calls, "utf8").split("\n").filter(Boolean) : [];
      rmSync(calls, { force: true });
      return read;
    },
    startFails: (fails: boolean) => (fails ? writeFileSync(startFails, "") : rmSync(startFails, { force: true })),
  };
}

/** Whether `child` ends within `ms`. */
const endsWithin = (child: ReturnType<typeof spawn>, ms: number) =>
  child.exitCode !== null || child.signalCode !== null
    ? Promise.resolve(true)
    : Promise.race([once(child, "exit").then(() => true), new Promise<boolean>((resolve) => setTimeout(() => resolve(false), ms))]);

test("the cut-over mounts the session image's layers anew under FUSE passthrough: the daemon stopped, its manager's layers unmounted lazily, the daemon started on a manager of its own, the pods of the containers on the layers before deleted, and the manager before killed once none runs", async () => {
  const at = machine();
  const node = at.stargz(true);
  try {
    const run = await at.cutOverAsync();
    assert.equal(run.code, 0, run.said);
    assert.deepEqual(node.calls(), [
      "systemctl stop stargz-snapshotter",
      // The daemon's layers alone, workspace storage's mount kept, the deepest first.
      `umount -l ${layer(123)}`,
      `umount -l ${layer(12)}`,
      "systemctl is-active --quiet stargz-snapshotter",
      "systemctl start stargz-snapshotter",
    ]);
    // The manager's socket and its record removed, so the daemon started a manager of its own, whose socket is the daemon's to make.
    assert.deepEqual([existsSync(node.at(FUSE_MANAGER_SOCKET)), existsSync(node.at(FUSE_MANAGER_STORE))], [false, false]);
    // The pods of the containers on the layers before, and none other.
    assert.deepEqual(at.deleted(), ["alasio-sessions/sandboxes-none-bound", "alasio-sessions/sandboxes-none-free"]);
    assert.equal(await endsWithin(node.manager, 5_000), true, "the FUSE manager before still runs");
    assert.match(
      run.said,
      new RegExp(
        [
          `stopping stargz-snapshotter's daemon, its FUSE manager ${node.manager.pid} serving its layers on \\(remount\\.log\\)`,
          "unmounted the 2 layers it served, lazily, which 2 containers read on, the manager serving them still",
          "starting stargz-snapshotter's daemon, which starts a FUSE manager of its own and mounts each layer again through it, under FUSE passthrough",
          "stargz-snapshotter's daemon serves again, [0-9.]+ s after this run of the step began, each layer mounted anew through its FUSE manager 4343",
          "deleting the 2 pods of the 2 containers on the layers before, as a drain does: a session's bound pod saved and released, the others made again \\(remount\\.log\\)",
          "the containers on the layers before ended, 0 s after their pods were deleted",
          `killed the FUSE manager before, ${node.manager.pid}, with the layers it served, which nothing reads`,
          "the session image's layers mounted anew, under FUSE passthrough, [0-9.]+ s after this run of the step began",
          "upgraded at \\S+",
          "through",
        ].map((line) => `[^\\n]* ${line}\\n`).join("") + "$",
        "u",
      ),
    );
    // Run again, it mounts nothing anew.
    const again = at.cutOver();
    assert.equal(again.code, 0, again.said);
    assert.deepEqual(node.calls(), []);
    assert.match(again.said, /^[^\n]* the session image's layers mounted anew at \S+\n[^\n]* upgraded at \S+\n[^\n]* through\n$/u);
  } finally {
    node.manager.kill();
    at.remove();
  }
});

test("the cut-over mounts nothing anew where the daemon's configuration sets no FUSE passthrough", () => {
  const at = machine();
  const node = at.stargz(false);
  try {
    const run = at.cutOver();
    assert.equal(run.code, 0, run.said);
    assert.deepEqual([node.calls(), at.deleted()], [[], []]);
    assert.match(run.said, /no stargz-snapshotter daemon of alasio's on this machine serves the session image through FUSE passthrough: no layer to mount anew\n/u);
  } finally {
    node.manager.kill();
    at.remove();
  }
});

test("the cut-over whose daemon does not start again goes on with it when run again, its layers unmounted and its containers found once", async () => {
  const at = machine();
  const node = at.stargz(true);
  try {
    node.startFails(true);
    const failed = await at.cutOverAsync();
    assert.notEqual(failed.code, 0);
    assert.match(failed.said, /FAILED: stargz-snapshotter's daemon could not be started \(remount\.log\): the cut-over run again goes on with it\n$/u);
    assert.deepEqual(at.deleted(), []);
    assert.equal(await endsWithin(node.manager, 500), false, "the FUSE manager before was killed with its containers running on");
    node.calls();
    node.startFails(false);
    const again = await at.cutOverAsync();
    assert.equal(again.code, 0, again.said);
    // Neither stopped nor unmounted again: started, the containers found before deleted, the manager before killed.
    assert.deepEqual(node.calls(), ["systemctl is-active --quiet stargz-snapshotter", "systemctl start stargz-snapshotter"]);
    assert.deepEqual(at.deleted(), ["alasio-sessions/sandboxes-none-bound", "alasio-sessions/sandboxes-none-free"]);
    assert.equal(await endsWithin(node.manager, 5_000), true, "the FUSE manager before still runs");
  } finally {
    node.manager.kill();
    at.remove();
  }
});
