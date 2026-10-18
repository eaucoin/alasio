/**
 * The cut-over of an installation of the release before (upgrades/from-8.0.0.sh), in how it
 * runs, without a cluster: its checks and refusals, its point of no return, and how a run
 * again goes on from where one before stopped. The end-to-end run's upgrade shard runs it on
 * an installation of that release (test/e2e/alasio.test.ts).
 */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { existsSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
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
 * it ran, UPGRADE failing while `upgradeFails` is there.
 */
function machine() {
  const root = mkdtempSync(join(tmpdir(), "alasio-cut-over-"));
  const bin = join(root, "bin");
  const ran = join(root, "ran");
  const namespaces = join(root, "namespaces");
  const upgradeFails = join(root, "upgrade-fails");
  const statefulSet = join(root, "statefulset.json");
  writeFileSync(namespaces, "");
  writeFileSync(statefulSet, JSON.stringify(logStatefulSet("8.0.0")));
  spawnSync("mkdir", [bin]);
  writeFileSync(join(bin, "kubectl"), `#!/bin/sh
case "$*" in
  "get --raw=/readyz") exit 0 ;;
  "get namespace -l alasio.dev/branch -o jsonpath="*) cat '${namespaces}' ;;
  "-n alasio get statefulset alasio-log --ignore-not-found -o json") [ ! -e '${statefulSet}' ] || cat '${statefulSet}' ;;
  "-n alasio get statefulset alasio-log --ignore-not-found -o name") [ ! -e '${statefulSet}' ] || echo statefulset.apps/alasio-log ;;
  "-n alasio get statefulset alasio-log -o json") cat '${statefulSet}' ;;
  "-n alasio delete statefulset alasio-log --cascade=orphan --wait=true") rm '${statefulSet}' ;;
  "-n alasio create -f -") cat >'${statefulSet}' ;;
  *) echo "kubectl $* is not the cut-over's" >&2; exit 2 ;;
esac
`, { mode: 0o755 });
  const out = join(root, "out");
  /** The cut-over, failing at `failAt` if given: what it came to. */
  const cutOver = (failAt?: string) => {
    const run = spawnSync("bash", [SCRIPT], {
      encoding: "utf8",
      env: {
        PATH: `${bin}:${JQ}:/usr/bin:/bin`,
        OUT: out,
        UPGRADE: `echo upgrade >>'${ran}' && [ ! -e '${upgradeFails}' ] && printf '%s' '${JSON.stringify(logStatefulSet())}' >'${statefulSet}'`,
        UPGRADE_CHECK: `echo check >>'${ran}'`,
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
    namespaces: (listed: string) => writeFileSync(namespaces, listed),
    /** The log's StatefulSet, as the cluster has it, or null where it has none. */
    statefulSet: () => (existsSync(statefulSet) ? JSON.parse(readFileSync(statefulSet, "utf8")) as ReturnType<typeof logStatefulSet> : null),
    /** Gives the cluster the log's StatefulSet, its claims of `version` if given. */
    hasStatefulSet: (version?: string) => writeFileSync(statefulSet, JSON.stringify(logStatefulSet(version))),
    /** What the cut-over kept of the log's StatefulSet, in its record. */
    kept: () => (existsSync(join(out, "log-statefulset.json")) ? JSON.parse(readFileSync(join(out, "log-statefulset.json"), "utf8")) as unknown : null),
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
    assert.match(run.said, /checking that no branch environment exists[^]*checking that the upgrade can run[^]*past the point of no return[^]*upgrading, from the cut-over begun at [^]*upgraded at \S+\n[^\n]* through\n$/u);
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
    assert.match(again.said, /upgraded at \S+\n[^\n]* through\n$/u);
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
    // A kubectl that reaches no cluster.
    spawnSync("mkdir", [join(root, "bin")]);
    writeFileSync(join(root, "bin", "kubectl"), "#!/bin/sh\nexit 1\n", { mode: 0o755 });
    const unreached = spawnSync("bash", [SCRIPT], { encoding: "utf8", env: { PATH: `${join(root, "bin")}:${JQ}:/usr/bin:/bin`, OUT: join(root, "out"), UPGRADE: "true", UPGRADE_CHECK: "true" } });
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
