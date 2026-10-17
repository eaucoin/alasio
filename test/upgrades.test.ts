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

const SCRIPT = fileURLToPath(new URL("../upgrades/from-8.0.0.sh", import.meta.url));
/** Where the jq the cut-over needs is, on this machine. */
const JQ = dirname(spawnSync("sh", ["-c", "command -v jq"], { encoding: "utf8" }).stdout.trim() || "/usr/bin/jq");

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
 * it ran, UPGRADE failing while `upgradeFails` is there; and a node, in `node`, whose
 * containerd and stargz-snapshotter's daemon hold images, one of the daemon's snapshots mounted
 * in another, and whose systemctl, umount and k3s-killall.sh say what they were asked, umount unmounting, its
 * systemctl failing to start k3s while `startFails` is there.
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
  writeFileSync(namespaces, "");
  writeFileSync(folders, JSON.stringify({ items: [] }));
  writeFileSync(sandboxes, "");
  writeFileSync(config, JSON.stringify({ target: { host: {} }, install: { host: { enabled: true } } }), { mode: 0o600 });
  writeFileSync(statefulSet, JSON.stringify(logStatefulSet("8.0.0")));
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
    /** The kubectl commands the cut-over ran, in order, since the last time it was asked, but for what reads alone. */
    calls: () => {
      const lines = existsSync(calls) ? readFileSync(calls, "utf8").split("\n").filter((line) => line && !/^(-n \S+ )?get /u.test(line)) : [];
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
