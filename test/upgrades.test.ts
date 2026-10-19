/**
 * The cut-over of an installation of the release before (upgrades/from-8.0.0.sh), in how it
 * runs, without a cluster: its checks and refusals, its point of no return, and how a run
 * again goes on from where one before stopped. The end-to-end run's upgrade shard runs it on
 * an installation of that release (test/e2e/alasio.test.ts).
 */
import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { once } from "node:events";
import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, statSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

import { testDatabaseUrl, testPool, testStore } from "./support/store.ts";

const SCRIPT = fileURLToPath(new URL("../upgrades/from-8.0.0.sh", import.meta.url));
/** Where the jq the cut-over needs is, on this machine. */
const JQ = dirname(spawnSync("sh", ["-c", "command -v jq"], { encoding: "utf8" }).stdout.trim() || "/usr/bin/jq");
/** The repository, where alasio's pod runs what the cut-over runs in it, as alasio's image is. */
const REPOSITORY = fileURLToPath(new URL("..", import.meta.url));

/**
 * alasio's database as 8.0.0 keeps it, in its schema, `state`: this release's schema, but the
 * indexes that keep a Telegram message, and a file it carried, once, which 8.0.0 had not.
 */
const pool = await testPool();
await testStore({ schema: "state" });
await pool.query("drop index state.messages_telegram_once; drop index state.files_telegram_once");
/** Makes the forks' holds as 8.0.0's table keeps them: without their holder and expiry. */
const holdsOf800 = () =>
  pool.query(`drop table state.conversation_holds;
    create table state.conversation_holds (
      conversation_id text primary key references state.conversations (id) on delete cascade,
      reason text not null,
      held_at timestamptz not null default now()
    )`);
await holdsOf800();
const DATABASE_URL = await testDatabaseUrl();

/** The log's StatefulSet, its claims labelled with `version` if given, as the cluster has it. */
const logStatefulSet = (version?: string) => ({
  apiVersion: "apps/v1",
  kind: "StatefulSet",
  metadata: { name: "alasio-log", namespace: "alasio", uid: "uid-of-8.0.0", resourceVersion: "41", generation: 2 },
  spec: { volumeClaimTemplates: [{ metadata: { name: "data", labels: { "app.kubernetes.io/component": "log", ...(version === undefined ? {} : { "app.kubernetes.io/version": version }) } } }] },
  status: { replicas: 1 },
});

/**
 * A machine for the cut-over: a kubectl that reaches its cluster, lists the namespaces
 * `namespaces` names, `<name>\t<branch label>` each, and keeps the log's StatefulSet in
 * `statefulSet`, getting, deleting and creating it; an UPGRADE that makes the log's
 * StatefulSet this release's, with no version on its claims, and an UPGRADE_CHECK, each saying
 * it ran, UPGRADE failing while `upgradeFails` is there; a node, in `node`, whose containerd
 * and stargz-snapshotter's daemon hold images, one of the daemon's snapshots mounted in
 * another, and whose systemctl, umount and k3s-killall.sh say what they were asked, umount
 * unmounting, its systemctl failing to start k3s while `startFails` is there. What the
 * cut-over runs in alasio's pod runs here, on the test's database, as 8.0.0's alasio reaches
 * its own.
 */
function machine() {
  const root = mkdtempSync(join(tmpdir(), "alasio-cut-over-"));
  const bin = join(root, "bin");
  const ran = join(root, "ran");
  const namespaces = join(root, "namespaces");
  const upgradeFails = join(root, "upgrade-fails");
  const statefulSet = join(root, "statefulset.json");
  const calls = join(root, "calls");
  const crd = join(root, "crd");
  const folders = join(root, "folders.json");
  const sandboxes = join(root, "sandboxes.tsv");
  const config = join(root, "config.json");
  const reboot = join(root, "reboot-required");
  const sessionSandboxes = join(root, "session-sandboxes");
  const sessionClaims = join(root, "session-claims");
  writeFileSync(sessionSandboxes, "");
  writeFileSync(sessionClaims, "");
  const node = join(root, "node");
  const host = join(root, "host");
  const startFails = join(root, "start-fails");
  for (const held of ["var/lib/rancher/k3s/agent/containerd/snapshots/1", "var/lib/containerd-stargz-grpc/snapshotter/snapshots/1/fs", "run/containerd-stargz-grpc", "proc/self", "usr/local/bin"]) {
    mkdirSync(join(node, held), { recursive: true });
  }
  writeFileSync(join(node, "var/lib/rancher/k3s/agent/containerd/snapshots/1/layer"), "unpacked by overlayfs");
  writeFileSync(join(node, "var/lib/containerd-stargz-grpc/snapshotter/snapshots/1/fs/layer"), "fetched lazily");
  writeFileSync(join(node, "run/containerd-stargz-grpc/fuse-manager.sock"), "");
  const daemonSnapshot = join(node, "var/lib/containerd-stargz-grpc/snapshotter/snapshots/1/fs");
  writeFileSync(join(node, "proc/self/mounts"), `proc /proc proc rw 0 0\nstargz ${daemonSnapshot} fuse.rawBridge rw 0 0\nstargz ${daemonSnapshot}/inner fuse.rawBridge rw 0 0\n`);
  writeFileSync(join(node, "usr/local/bin/k3s-killall.sh"), `#!/bin/sh\necho k3s-killall.sh >>'${host}'\n`, { mode: 0o755 });
  const databaseUrl = join(root, "database-url");
  writeFileSync(namespaces, "");
  writeFileSync(folders, JSON.stringify({ items: [] }));
  writeFileSync(sandboxes, "");
  writeFileSync(config, JSON.stringify({ target: { host: {} }, install: { host: { enabled: true } } }), { mode: 0o600 });
  writeFileSync(statefulSet, JSON.stringify(logStatefulSet("8.0.0")));
  writeFileSync(databaseUrl, DATABASE_URL);
  spawnSync("mkdir", [bin]);
  writeFileSync(join(bin, "kubectl"), `#!/bin/sh
printf '%s' "$*" | tr '\\n' ' ' >>'${calls}' && echo >>'${calls}'
case "$*" in
  "get --raw=/readyz") exit 0 ;;
  "get namespace -l alasio.dev/branch -o jsonpath="*) cat '${namespaces}' ;;
  "-n alasio get statefulset alasio-log --ignore-not-found -o json") [ ! -e '${statefulSet}' ] || cat '${statefulSet}' ;;
  "-n alasio get statefulset alasio-log --ignore-not-found -o name") [ ! -e '${statefulSet}' ] || echo statefulset.apps/alasio-log ;;
  "-n alasio get statefulset alasio-log -o json") cat '${statefulSet}' ;;
  "-n alasio delete statefulset alasio-log --cascade=orphan --wait=true") rm '${statefulSet}' ;;
  "-n alasio create -f -") cat >'${statefulSet}' ;;
  "get crd sandboxes.agents.x-k8s.io --ignore-not-found -o name") [ ! -e '${crd}' ] || echo customresourcedefinition.apiextensions.k8s.io/sandboxes.agents.x-k8s.io ;;
  "-n alasio-sessions get sandboxes.agents.x-k8s.io -o name") cat '${sessionSandboxes}' ;;
  "-n alasio-sessions get pvc -o name") cat '${sessionClaims}' ;;
  "-n alasio get pod -l "*" -o name") ;;
  "get pod -A -l alasio.dev/workload=folder -o json") cat '${folders}' ;;
  "get sandboxes.agents.x-k8s.io -A -o jsonpath="*) cat '${sandboxes}' ;;
  "delete crd sandboxes.agents.x-k8s.io --ignore-not-found --wait=true") rm -f '${crd}' ;;
  "-n alasio scale "*|"-n "*" delete pod "*|"-n "*" patch sandboxes.agents.x-k8s.io "*|"-n "*" delete sandboxes.agents.x-k8s.io "*) ;;
  "-n alasio delete "*" --ignore-not-found"|"delete clusterrole,"*" --ignore-not-found"|"-n alasio-host delete "*" --ignore-not-found"|"delete validatingadmissionpolicy,"*" --ignore-not-found") ;;
  "-n alasio exec -i deploy/alasio -c alasio -- node --input-type=module -e "*)
    shift 11
    # Elsewhere than alasio's modules, as a folder workspace's pod is; its image's, here.
    cd '${root}' && ALASIO_DATABASE_URL_FILE='${databaseUrl}' exec '${process.execPath}' --input-type=module -e "$(printf '%s' "$1" | sed 's#/opt/alasio/#${REPOSITORY}#g')" ;;
  *) echo "kubectl $* is not the cut-over's" >&2; exit 2 ;;
esac
`, { mode: 0o755 });
  writeFileSync(join(bin, "systemctl"), `#!/bin/sh
echo "systemctl $*" >>'${host}'
[ "$*" != "start k3s" ] || [ ! -e '${startFails}' ]
`, { mode: 0o755 });
  // Unmounting it of the node's mount table, as the kernel does.
  const mounts = join(node, "proc/self/mounts");
  writeFileSync(join(bin, "umount"), `#!/bin/sh
echo "umount $*" >>'${host}'
awk -v point="$2" '$2 != point' '${mounts}' >'${mounts}.next' && mv '${mounts}.next' '${mounts}'
`, { mode: 0o755 });
  const out = join(root, "out");
  /** The cut-over, failing at `failAt` if given: what it came to. */
  const cutOver = (failAt?: string) => {
    const run = spawnSync("bash", [SCRIPT], {
      encoding: "utf8",
      env: {
        PATH: `${bin}:${JQ}:/usr/bin:/bin`,
        OUT: out,
        CONFIG: config,
        REBOOT_REQUIRED: reboot,
        NODE_ROOT: node,
        UPGRADE: `echo upgrade >>'${ran}' && [ ! -e '${upgradeFails}' ] && printf '%s' '${JSON.stringify(logStatefulSet())}' >'${statefulSet}'`,
        // Saying whether the configuration it checks says how to run agent-sandbox.
        UPGRADE_CHECK: `if jq -e '.install | has("agentSandbox")' '${config}' >/dev/null; then echo check of agentSandbox >>'${ran}'; else echo check >>'${ran}'; fi`,
        ...(failAt === undefined ? {} : { FAIL_AT: failAt }),
      },
    });
    return { code: run.status, said: run.stderr };
  };
  return {
    cutOver,
    /** What UPGRADE and UPGRADE_CHECK ran, in order, since the last time it was asked. */
    ran: () => {
      const lines = existsSync(ran) ? readFileSync(ran, "utf8").split("\n").filter(Boolean) : [];
      rmSync(ran, { force: true });
      return lines;
    },
    /** What the cut-over recorded in `name`. */
    recorded: (name: string) => readFileSync(join(out, name), "utf8"),
    /** The cut-over's markers, of what it recorded. */
    marks: () => (existsSync(out) ? readdirSync(out).filter((name) => ["point-of-no-return", "upgraded"].includes(name)).sort() : []),
    /** The cut-over's markers of the move of the node's images. */
    imageMarks: () => (existsSync(out) ? readdirSync(out).filter((name) => name.startsWith("images-")).sort() : []),
    /** What the node's systemctl, umount and k3s-killall.sh were asked, in order, since the last time it was asked. */
    host: () => {
      const lines = existsSync(host) ? readFileSync(host, "utf8").split("\n").filter(Boolean) : [];
      rmSync(host, { force: true });
      return lines;
    },
    startFails: (fails: boolean) => (fails ? writeFileSync(startFails, "") : rmSync(startFails, { force: true })),
    node,
    daemonSnapshot,
    namespaces: (listed: string) => writeFileSync(namespaces, listed),
    /** The log's StatefulSet, as the cluster has it, or null where it has none. */
    statefulSet: () => (existsSync(statefulSet) ? JSON.parse(readFileSync(statefulSet, "utf8")) as ReturnType<typeof logStatefulSet> : null),
    /** Gives the cluster the log's StatefulSet, its claims of `version` if given. */
    hasStatefulSet: (version?: string) => writeFileSync(statefulSet, JSON.stringify(logStatefulSet(version))),
    /** What the cut-over kept of the log's StatefulSet, in its record. */
    kept: () => (existsSync(join(out, "log-statefulset.json")) ? JSON.parse(readFileSync(join(out, "log-statefulset.json"), "utf8")) as unknown : null),
    upgradeFails: (fails: boolean) => (fails ? writeFileSync(upgradeFails, "") : rmSync(upgradeFails, { force: true })),
    /** The kubectl commands the cut-over ran, in order, since the last time it was asked, but for what reads alone and what it runs on alasio's database (on_database), which the database's own tests read. */
    calls: () => {
      const lines = existsSync(calls) ? readFileSync(calls, "utf8").split("\n").filter((line) => line && !/^(-n \S+ )?get /u.test(line) && !line.startsWith("-n alasio exec -i deploy/alasio ")) : [];
      rmSync(calls, { force: true });
      return lines;
    },
    /** Gives the cluster 8.0.0's agent-sandbox: its CRD, the folders' bayma pods, `<namespace> <pod> <state dir>` each, and its Sandboxes, `<namespace> <name>` each. */
    hasAgentSandbox: (pods: readonly (readonly [string, string, string])[], listed: readonly (readonly [string, string])[]) => {
      writeFileSync(crd, "");
      writeFileSync(folders, JSON.stringify({
        items: pods.map(([namespace, name, state]) => ({ metadata: { namespace, name }, spec: { containers: [{ name: "bayma", args: ["mcp-http", "--state-dir", state, "--token-file", "/run/alasio/bayma/token"] }] } })),
      }));
      writeFileSync(sandboxes, listed.map((each) => `${each.join("\t")}\n`).join(""));
    },
    /** Whether the cluster has the Sandboxes' CRD still. */
    hasCrd: () => existsSync(crd),
    /** The configuration's file, and what it holds. */
    config,
    configured: () => JSON.parse(readFileSync(config, "utf8")) as { install: Record<string, unknown> },
    /** Gives the sessions' namespace the release before's session workspaces: Sandboxes and claims, by name. */
    hasSessionWorkspaces: (sandboxes: readonly string[], claims: readonly string[]) => {
      writeFileSync(sessionSandboxes, sandboxes.map((name) => `sandbox.agents.x-k8s.io/${name}\n`).join(""));
      writeFileSync(sessionClaims, claims.map((name) => `persistentvolumeclaim/${name}\n`).join(""));
    },
    /** Leaves this machine a reboot pending, or none. */
    rebootPending: (pending: boolean) => (pending ? writeFileSync(reboot, "") : rmSync(reboot, { force: true })),
    root,
    remove: () => rmSync(root, { recursive: true, force: true }),
  };
}

test("the cut-over checks, upgrades past its point of no return, and says it is through", () => {
  const at = machine();
  try {
    const run = at.cutOver();
    assert.equal(run.code, 0, run.said);
    assert.deepEqual(at.ran(), ["check", "upgrade", "check", "upgrade"]);
    assert.deepEqual(at.marks(), ["point-of-no-return", "upgraded"]);
    assert.match(run.said, /checking that no branch environment exists[^]*checking that the upgrade can run[^]*past the point of no return[^]*upgrading, from the cut-over begun at [^]*stopping k3s, to remove the node's images[^]*applying the upgrade again[^]*upgraded at \S+\n[^\n]* removing what of 8\.0\.0's this release has no use for[^\n]*\n[^\n]* through\n$/u);
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
    assert.match(refused.said, /FAILED: alasio upgrades main alone[^\n]*\(alasio branch delete one, alasio branch delete two\)/u);
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
    assert.match(failed.said, /FAILED: a failure injected before the point of no return\n/u);
    assert.deepEqual([at.ran(), at.marks()], [["check"], []]);
    assert.equal(at.cutOver().code, 0);
    assert.deepEqual([at.ran(), at.marks()], [["check", "upgrade", "check", "upgrade"], ["point-of-no-return", "upgraded"]]);
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
    assert.deepEqual([at.ran(), at.marks()], [["upgrade", "check", "upgrade"], ["point-of-no-return", "upgraded"]]);
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
    assert.deepEqual([at.ran(), at.marks()], [["check", "upgrade", "check", "upgrade"], ["point-of-no-return", "upgraded"]]);
    at.host();
    const again = at.cutOver();
    assert.equal(again.code, 0, again.said);
    assert.doesNotMatch(again.said, /checking that|upgrading, from the cut-over|stopping k3s|applying the upgrade again/u);
    assert.match(again.said, /upgraded at \S+\n[^\n]* removing what of 8\.0\.0's this release has no use for[^\n]*\n[^\n]* through\n$/u);
    assert.deepEqual([at.ran(), at.host()], [[], []]);
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
    // A kubectl that reaches no cluster.
    spawnSync("mkdir", [join(root, "bin")]);
    writeFileSync(join(root, "bin", "kubectl"), "#!/bin/sh\nexit 1\n", { mode: 0o755 });
    const needs = { PATH: `${join(root, "bin")}:${JQ}:/usr/bin:/bin`, OUT: join(root, "out"), UPGRADE: "true", UPGRADE_CHECK: "true" };
    const unconfigured = spawnSync("bash", [SCRIPT], { encoding: "utf8", env: needs });
    assert.notEqual(unconfigured.status, 0);
    assert.match(unconfigured.stderr, /CONFIG: the file of the configuration/u);
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

test("the cut-over deletes 8.0.0's log StatefulSet, whose claims carry its version, its pod and claim kept, for the upgrade to make anew, and leaves one of this release's", () => {
  const at = machine();
  try {
    const run = at.cutOver();
    assert.equal(run.code, 0, run.said);
    assert.match(run.said, /deleting the log's StatefulSet, its pod and claim kept, for the upgrade to make it anew without 8\.0\.0 on its claims[^]*checking that the upgrade can run/u);
    assert.deepEqual(at.kept(), logStatefulSet("8.0.0"));
    // Made anew by the upgrade, as this release's.
    assert.deepEqual(at.statefulSet(), logStatefulSet());
    // One whose claims carry no version, or none at all, is left as it is.
    const again = machine();
    try {
      again.hasStatefulSet();
      const left = again.cutOver();
      assert.equal(left.code, 0, left.said);
      assert.doesNotMatch(left.said, /deleting the log's StatefulSet/u);
      assert.equal(again.kept(), null);
    } finally {
      again.remove();
    }
  } finally {
    at.remove();
  }
});

test("the cut-over failing before its point of no return makes the log's StatefulSet again as 8.0.0 had it, which takes its pod back, and a run again deletes it anew", () => {
  const at = machine();
  try {
    const failed = at.cutOver("before-the-point-of-no-return");
    assert.notEqual(failed.code, 0);
    assert.match(failed.said, /deleting the log's StatefulSet[^]*FAILED: a failure injected before the point of no return\n[^\n]* making the log's StatefulSet again, as 8\.0\.0 had it\n$/u);
    // As 8.0.0 had it, but what the cluster gives an object it makes.
    const { metadata: { uid: _uid, resourceVersion: _version, generation: _generation, ...metadata }, status: _status, ...made } = logStatefulSet("8.0.0");
    assert.deepEqual(at.statefulSet(), { ...made, metadata });
    const again = at.cutOver();
    assert.equal(again.code, 0, again.said);
    assert.match(again.said, /deleting the log's StatefulSet/u);
    assert.deepEqual(at.statefulSet(), logStatefulSet());
  } finally {
    at.remove();
  }
});

test("the cut-over removes install.agentSandbox from the configuration before the check reads it, the file's mode kept, and a failure before its point of no return puts it back", () => {
  const at = machine();
  try {
    const install = { host: { enabled: true }, agentSandbox: { resources: { requests: { cpu: "10m" } } } };
    writeFileSync(at.config, JSON.stringify({ target: { host: {} }, install }), { mode: 0o600 });
    const failed = at.cutOver("before-the-point-of-no-return");
    assert.notEqual(failed.code, 0);
    assert.match(failed.said, /removing install\.agentSandbox from [^\n]*config\.json, as this release runs no agent-sandbox[^]*checking that the upgrade can run[^]*putting install\.agentSandbox back in /u);
    assert.deepEqual(at.ran(), ["check"]);
    assert.deepEqual(at.configured().install, install);
    const run = at.cutOver();
    assert.equal(run.code, 0, run.said);
    assert.deepEqual(at.ran(), ["check", "upgrade", "check", "upgrade"]);
    assert.deepEqual(at.configured(), { target: { host: {} }, install: { host: { enabled: true } } });
    assert.equal(statSync(at.config).mode & 0o777, 0o600);
    // One without it is left as it is.
    const again = machine();
    try {
      const left = again.cutOver();
      assert.equal(left.code, 0, left.said);
      assert.doesNotMatch(left.said, /agentSandbox/u);
      assert.deepEqual(again.configured(), { target: { host: {} }, install: { host: { enabled: true } } });
    } finally {
      again.remove();
    }
  } finally {
    at.remove();
  }
});

test("the cut-over refuses a machine with a reboot pending before it changes anything", () => {
  const at = machine();
  try {
    at.rebootPending(true);
    const refused = at.cutOver();
    assert.notEqual(refused.code, 0);
    assert.match(refused.said, /FAILED: this machine has a reboot pending [^\n]*: reboot it first, then cut over/u);
    assert.deepEqual([at.ran(), at.marks()], [[], []]);
    assert.deepEqual(at.statefulSet(), logStatefulSet("8.0.0"));
  } finally {
    at.remove();
  }
});

test("past its point of no return, before the upgrade, the cut-over stops agent-sandbox's controller and alasio, then each folder's bayma with ten minutes to save its REPLs into its state directory, which it leaves, then deletes every Sandbox", () => {
  const at = machine();
  try {
    const state = join(at.root, "home", ".alasio", "bayma", "claude", "telegram-42");
    mkdirSync(join(state, "snapshots", "sess_a"), { recursive: true });
    mkdirSync(join(state, "snapshots", "sess_b"));
    at.hasAgentSandbox([["alasio-host", "bayma-0123", state]], [["alasio-host", "bayma-0123"], ["alasio-sessions", "fs-abc123"]]);
    const run = at.cutOver();
    assert.equal(run.code, 0, run.said);
    const calls = at.calls();
    const upgraded = calls.indexOf("delete crd sandboxes.agents.x-k8s.io --ignore-not-found --wait=true");
    assert.deepEqual(calls.slice(0, 8), [
      "-n alasio delete statefulset alasio-log --cascade=orphan --wait=true",
      "-n alasio scale deployment alasio-agent-sandbox-controller --replicas=0",
      "-n alasio scale deployment alasio --replicas=0",
      "-n alasio-host delete pod bayma-0123 --grace-period=600 --wait=true --timeout=11m",
      `-n alasio-host patch sandboxes.agents.x-k8s.io bayma-0123 --type=merge -p {"metadata":{"finalizers":null}}`,
      "-n alasio-host delete sandboxes.agents.x-k8s.io bayma-0123 --ignore-not-found --wait=true",
      `-n alasio-sessions patch sandboxes.agents.x-k8s.io fs-abc123 --type=merge -p {"metadata":{"finalizers":null}}`,
      "-n alasio-sessions delete sandboxes.agents.x-k8s.io fs-abc123 --ignore-not-found --wait=true",
    ]);
    assert.equal(upgraded, 8);
    assert.match(run.said, /stopping the folder's bayma alasio-host\/bayma-0123, given 10m to save its REPL sessions whole into \S+telegram-42, which stays[^]*saved the sessions sess_a sess_b whole[^]*deleting every Sandbox[^]*upgrading, from the cut-over/u);
    assert.ok(existsSync(join(state, "snapshots", "sess_a")));
    // Once its Sandboxes and CRD are gone, a run again does none of it.
    const after = machine();
    try {
      const plain = after.cutOver();
      assert.equal(plain.code, 0, plain.said);
      assert.doesNotMatch(plain.said, /agent-sandbox's controller|folder's bayma|every Sandbox/u);
    } finally {
      after.remove();
    }
  } finally {
    at.remove();
  }
});

test("a folder's bayma that saved no session whole is said to bring each back from its checkpoint", () => {
  const at = machine();
  try {
    at.hasAgentSandbox([["alasio-host", "bayma-4567", join(at.root, "nothing-saved")]], [["alasio-host", "bayma-4567"]]);
    const run = at.cutOver();
    assert.equal(run.code, 0, run.said);
    assert.match(run.said, /the folder's bayma alasio-host\/bayma-4567 saved no session whole: each comes back from its checkpoint/u);
  } finally {
    at.remove();
  }
});

test("once upgraded, the cut-over deletes what of 8.0.0's this release has no use for, the Sandboxes' CRD last of agent-sandbox's, each idempotently, as a run again does it again", () => {
  const at = machine();
  try {
    at.hasAgentSandbox([], []);
    const failed = at.cutOver("after-the-upgrade");
    assert.notEqual(failed.code, 0);
    at.calls();
    assert.ok(at.hasCrd(), "nothing deleted before what follows the upgrade");
    const removed = [
      "delete crd sandboxes.agents.x-k8s.io --ignore-not-found --wait=true",
      "-n alasio delete deployment,serviceaccount,role,rolebinding alasio-agent-sandbox-controller --ignore-not-found",
      "delete clusterrole,clusterrolebinding alasio-agent-sandbox-controller --ignore-not-found",
      "-n alasio delete configmap alasio-sandbox-templates --ignore-not-found",
      "-n alasio-host delete role,rolebinding alasio --ignore-not-found",
      "-n alasio-host delete networkpolicy bayma-from-alasio --ignore-not-found",
      "delete validatingadmissionpolicy,validatingadmissionpolicybinding alasio-sandboxes alasio-secrets --ignore-not-found",
    ];
    const run = at.cutOver();
    assert.equal(run.code, 0, run.said);
    assert.deepEqual(at.calls(), removed);
    assert.ok(!at.hasCrd());
    const again = at.cutOver();
    assert.equal(again.code, 0, again.said);
    assert.deepEqual(at.calls(), removed);
  } finally {
    at.remove();
  }
});

test("the cut-over refuses session workspaces of 8.0.0's before it changes anything, naming each with how to delete it, and goes on once they are deleted", () => {
  const at = machine();
  try {
    at.hasAgentSandbox([], []);
    at.hasSessionWorkspaces(["fs-abc123"], ["workspace-fs-abc123"]);
    const refused = at.cutOver();
    assert.notEqual(refused.code, 0);
    assert.match(refused.said, /FAILED: alasio-sessions holds session workspaces of 8\.0\.0's, which this release runs no session on and moves nothing of: delete each first, its files with it \(kubectl -n alasio-sessions delete sandbox\.agents\.x-k8s\.io\/fs-abc123; kubectl -n alasio-sessions delete persistentvolumeclaim\/workspace-fs-abc123\), and run the cut-over again\n$/u);
    assert.deepEqual([at.ran(), at.marks(), at.calls()], [[], [], []]);
    assert.deepEqual(at.statefulSet(), logStatefulSet("8.0.0"));
    at.hasSessionWorkspaces([], []);
    const again = at.cutOver();
    assert.equal(again.code, 0, again.said);
  } finally {
    at.remove();
  }
});

test("once its upgrade has run, the cut-over moves the node's images onto stargz-snapshotter's daemon: k3s stopped, gVisor's processes and the daemon's FUSE manager killed, the daemon's mounts unmounted, deepest first, containerd's state and the daemon's removed, k3s started, and the upgrade applied again once the release accepts the installation", async () => {
  const at = machine();
  // Processes of the node's, each by its executable, as /proc says it.
  const running = (executable: string) => {
    const child = spawn("sleep", ["300"], { stdio: "ignore" });
    mkdirSync(join(at.node, "proc", String(child.pid)), { recursive: true });
    symlinkSync(executable, join(at.node, "proc", String(child.pid), "exe"));
    return child;
  };
  const sandbox = running(join(at.node, "usr/local/lib/alasio/gvisor/20990101.0/runsc"));
  const manager = running(join(at.node, "usr/local/bin/stargz-fuse-manager"));
  const other = running("/usr/bin/sleep");
  try {
    const run = at.cutOver();
    assert.equal(run.code, 0, run.said);
    assert.match(run.said, /upgrading, from the cut-over[^]*stopping k3s, to remove the node's images, unpacked by overlayfs under 8\.0\.0: \S+ of containerd's state\n[^]*starting k3s, which pulls every image again through stargz-snapshotter's daemon\n[^]*applying the upgrade again, which waits until alasio runs on the images pulled anew\n[^]*upgraded at /u);
    assert.deepEqual(at.host(), [
      "systemctl stop k3s",
      "k3s-killall.sh",
      `umount --lazy ${at.daemonSnapshot}/inner`,
      `umount --lazy ${at.daemonSnapshot}`,
      "systemctl start k3s",
    ]);
    // The check the move waits for, between the upgrade and its applying again.
    assert.deepEqual(at.ran(), ["check", "upgrade", "check", "upgrade"]);
    for (const removed of ["var/lib/rancher/k3s/agent/containerd", "var/lib/containerd-stargz-grpc", "run/containerd-stargz-grpc"]) {
      assert.ok(!existsSync(join(at.node, removed)), `${removed} is left`);
    }
    assert.deepEqual(at.imageMarks(), ["images-moved"]);
    assert.deepEqual((await Promise.all([once(sandbox, "exit"), once(manager, "exit")])).map(([, signal]) => signal), ["SIGKILL", "SIGKILL"]);
    assert.equal(other.exitCode ?? other.signalCode, null);
  } finally {
    for (const child of [sandbox, manager, other]) child.kill("SIGKILL");
    at.remove();
  }
});

test("a cut-over that stopped in the move of the node's images ends it first when run again, k3s stopped, then applies the upgrade again alone", () => {
  const at = machine();
  try {
    at.startFails(true);
    const stopped = at.cutOver();
    assert.notEqual(stopped.code, 0);
    assert.deepEqual([at.ran(), at.marks(), at.imageMarks()], [["check", "upgrade"], ["point-of-no-return"], ["images-moving"]]);
    assert.deepEqual(at.host().slice(-1), ["systemctl start k3s"]);
    at.startFails(false);
    const again = at.cutOver();
    assert.equal(again.code, 0, again.said);
    assert.match(again.said, /^[^\n]* stopping k3s, to remove the node's images, unpacked by overlayfs under 8\.0\.0: none of containerd's state\n[^]*starting k3s[^]*applying the upgrade again[^]*through\n$/u);
    assert.doesNotMatch(again.said, /checking that|upgrading, from the cut-over/u);
    assert.deepEqual(at.host(), ["systemctl stop k3s", "k3s-killall.sh", "systemctl start k3s"]);
    assert.deepEqual([at.ran(), at.marks(), at.imageMarks()], [["check", "upgrade"], ["point-of-no-return", "upgraded"], ["images-moved"]]);
  } finally {
    at.remove();
  }
});

/** The conversation 8.0.0's alasio kept the messages of these tests in. */
const CONVERSATION = "telegram:7";

/**
 * Keeps a Telegram message in the conversation as 8.0.0's alasio did, by its id `id`: Telegram's
 * `messageId`, sent at `date`, saying `text`, kept `at`.
 */
async function kept(id: string, { messageId = 10, date = 1_700_000_000, text = "hello", at }: { readonly messageId?: number; readonly date?: number; readonly text?: string; readonly at: string }): Promise<void> {
  await pool.query("insert into state.conversations (id, transport, chat_id) values ($1, 'telegram', '7') on conflict do nothing", [CONVERSATION]);
  await pool.query(
    "insert into state.messages (id, conversation_id, direction, kind, transport_message_id, text, raw, created_at) values ($1, $2, 'inbound', 'text', $3, $4, $5, $6)",
    [id, CONVERSATION, String(messageId), text, JSON.stringify({ message_id: messageId, date, text }), at],
  );
}

/** Keeps a file, by its id `id`, as the message `messageId` carried it, Telegram's `uniqueId`. */
async function carried(id: string, messageId: string, uniqueId: string): Promise<void> {
  await pool.query(
    "insert into state.files (id, message_id, conversation_id, telegram_file_id, telegram_file_unique_id, name, file_size, sha256, content, raw) values ($1, $2, $3, 'file-id', $4, 'photo.jpg', 1, 'sha', '\\x00', '{}')",
    [id, messageId, CONVERSATION, uniqueId],
  );
}

/** The messages kept, by their ids; and the files, by theirs, with the message each is of. */
const messages = async () => (await pool.query<{ id: string }>("select id from state.messages order by id")).rows.map(({ id }) => id);
const files = async () => (await pool.query<{ id: string; message_id: string | null }>("select id, message_id from state.files order by id")).rows.map(({ id, message_id }) => [id, message_id]);

/** Forgets what a test kept, its conversation, its messages and their files with it. */
const forget = () => pool.query("delete from state.conversations");

test("the cut-over says which Telegram messages 8.0.0 kept twice before it changes anything, and past its point of no return removes their exact copies, the first kept kept, its files given it, so this release's schema is made", async () => {
  const at = machine();
  try {
    await kept("first", { at: "2026-10-01T00:00:00Z" });
    await kept("second", { at: "2026-10-01T00:00:01Z" });
    await carried("photo", "second", "unique-photo");
    await kept("other", { messageId: 11, at: "2026-10-01T00:00:00Z" });
    await kept("other-again", { messageId: 11, at: "2026-10-01T00:00:01Z" });
    await carried("document", "other", "unique-document");
    await carried("document-again", "other-again", "unique-document");
    // Telegram's id again, sent at another time, as a branch's bot gives its own: another message.
    await kept("later", { date: 1_700_000_100, at: "2026-10-01T00:00:02Z" });
    const failed = at.cutOver("before-the-point-of-no-return");
    assert.notEqual(failed.code, 0);
    assert.match(failed.said, /finding the Telegram messages[^\n]*\n[^\n]* 2 exact copies of Telegram messages kept twice, removed past the point of no return\n/u);
    assert.equal(at.recorded("telegram-duplicates.tsv"), `message\t${CONVERSATION}\tinbound\t10\t1700000000\t2\t1\nmessage\t${CONVERSATION}\tinbound\t11\t1700000000\t2\t1\n`);
    // Nothing is removed before the point of no return.
    assert.deepEqual(await messages(), ["first", "later", "other", "other-again", "second"]);
    const run = at.cutOver();
    assert.equal(run.code, 0, run.said);
    assert.match(run.said, /past the point of no return[^\n]*\n[^\n]* removed 2 exact copies of Telegram messages kept twice\n[^]* upgrading, from the cut-over/u);
    assert.deepEqual(await messages(), ["first", "later", "other"]);
    // The copy's file goes to the message kept, which had none; a file the message kept has already is let go of, kept.
    assert.deepEqual(await files(), [["document", "other"], ["document-again", null], ["photo", "first"]]);
    // This release's schema, which keeps each once, is made on what it left.
    await testStore({ schema: "state" });
  } finally {
    at.remove();
    await forget();
    await pool.query("drop index if exists state.messages_telegram_once; drop index if exists state.files_telegram_once");
  }
});

test("the cut-over refuses a Telegram message 8.0.0 kept twice whose copies differ, or a file a message carried kept twice, before it changes anything, saying which", async () => {
  const at = machine();
  try {
    await kept("first", { at: "2026-10-01T00:00:00Z" });
    await kept("edited", { text: "hello again", at: "2026-10-01T00:00:01Z" });
    const refused = at.cutOver();
    assert.notEqual(refused.code, 0);
    assert.match(refused.said, new RegExp(`FAILED: alasio kept these twice[^\\n]*: message 10 of ${CONVERSATION} \\(2 kept, 2 different\\)\\. Put them right by hand`, "u"));
    assert.deepEqual([at.ran(), at.marks(), at.statefulSet(), await messages()], [[], [], logStatefulSet("8.0.0"), ["edited", "first"]]);
    await pool.query("delete from state.messages where id = 'edited'");
    await carried("photo", "first", "unique-photo");
    await carried("photo-again", "first", "unique-photo");
    const again = at.cutOver();
    assert.notEqual(again.code, 0);
    assert.match(again.said, /: file unique-photo of first \(2 kept, 1 different\)\. Put them right by hand/u);
    assert.deepEqual([at.ran(), at.marks(), await files()], [[], [], [["photo", "first"], ["photo-again", "first"]]]);
  } finally {
    at.remove();
    await forget();
  }
});

/** The forks' holds' columns, each with whether it may be null and its default. */
const holdColumns = async () =>
  (await pool.query<{ column_name: string; is_nullable: string; column_default: string | null }>(
    "select column_name, is_nullable, column_default from information_schema.columns where table_schema = 'state' and table_name = 'conversation_holds' order by ordinal_position",
  )).rows.map(({ column_name, is_nullable, column_default }) => [column_name, is_nullable, column_default]);

test("the cut-over gives 8.0.0's forks' holds their holder and expiry past its point of no return, with defaults 8.0.0's holds need, and takes those off once the upgrade is through", async () => {
  await holdsOf800();
  const at = machine();
  try {
    // 8.0.0 holds a conversation, as it does without either.
    await pool.query("insert into state.conversations (id, transport, chat_id) values ('telegram:8', 'telegram', '8') on conflict do nothing");
    await pool.query("insert into state.conversation_holds (conversation_id, reason) values ('telegram:8', 'forking')");
    // Failing once its upgrade is through, the defaults are kept, as 8.0.0's holds, made until the upgrade, needed them.
    const failed = at.cutOver("after-the-upgrade");
    assert.notEqual(failed.code, 0);
    assert.match(failed.said, /past the point of no return[^]*gave the forks' holds their holder and expiry[^]*upgrading, from the cut-over/u);
    const given = await holdColumns();
    assert.ok(given.slice(3).every(([, , fallback]) => fallback !== null), "8.0.0's holds are made with defaults until the upgrade");
    const run = at.cutOver();
    assert.equal(run.code, 0, run.said);
    assert.deepEqual(given.map(([name, nullable]) => [name, nullable]), [["conversation_id", "NO"], ["reason", "NO"], ["held_at", "NO"], ["holder", "NO"], ["expires_at", "NO"]]);
    // 8.0.0's hold, expired as alasio starts.
    assert.equal((await pool.query("select 1 from state.conversation_holds where expires_at > now()")).rows.length, 0);
    assert.match(run.said, /made the forks' holds as this release makes them, without the defaults 8\.0\.0's needed\n[^\n]* upgraded at /u);
    assert.deepEqual((await holdColumns()).slice(3), [["holder", "NO", null], ["expires_at", "NO", null]]);
  } finally {
    at.remove();
    await pool.query("delete from state.conversations");
    await holdsOf800();
  }
});
