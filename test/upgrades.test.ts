/**
 * The cut-over of an installation of the release before (upgrades/from-10.0.26.sh), in how it
 * runs, without a cluster: its checks and refusals, its point of no return, and how a run
 * again goes on from where one before stopped. The end-to-end run's upgrade shard runs it on
 * an installation of that release (test/e2e/alasio.test.ts).
 */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { delimiter, join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

const SCRIPT = fileURLToPath(new URL("../upgrades/from-10.0.26.sh", import.meta.url));

/** The PATH the cut-over runs with: `bin`'s, and the system's. */
const pathOf = (bin: string) => [bin, "/usr/bin", "/bin"].join(delimiter);

/** The bundled object store's parts, by their StatefulSets, as the cut-over stops them. */
const STORE_PARTS = ["alasio-seaweedfs-filer", "alasio-seaweedfs-volume", "alasio-seaweedfs-master", "alasio-seaweedfs-filer-store"];

/**
 * A machine for the cut-over: a kubectl that reaches its cluster and lists the namespaces
 * `namespaces` names, `<name>\t<branch label>` each, and the bundled object store's claims, each
 * a directory of the machine's, whose parts it stops as it is told, saying so in `stopped`; and
 * an UPGRADE and an UPGRADE_CHECK, each saying it ran, UPGRADE failing while `upgradeFails` is
 * there.
 */
function machine() {
  const root = mkdtempSync(join(tmpdir(), "alasio-cut-over-"));
  const bin = join(root, "bin");
  const ran = join(root, "ran");
  const namespaces = join(root, "namespaces");
  const claims = join(root, "claims");
  const volumes = join(root, "volumes");
  const counts = join(root, "counts");
  const stopped = join(root, "stopped");
  const upgradeFails = join(root, "upgrade-fails");
  const config = join(root, "config.json");
  for (const directory of [bin, volumes, counts]) mkdirSync(directory);
  writeFileSync(namespaces, "");
  writeFileSync(claims, "");
  writeFileSync(config, JSON.stringify({ target: { host: {} }, install: { host: { enabled: true } } }), { mode: 0o600 });
  // The claims, '<claim>\t<volume>' a line, each volume's directory volumes/<volume>; each part's count counts/<part>.
  writeFileSync(join(bin, "kubectl"), `#!/bin/sh
case "$*" in
  "get --raw=/readyz") exit 0 ;;
  "get namespace -l alasio.dev/branch -o jsonpath="*) cat '${namespaces}' ;;
  "-n alasio get pvc -o jsonpath="*) cat '${claims}' ;;
  "get pv "*" -o jsonpath="*) printf '%s' '${volumes}'/"$3" ;;
  "-n alasio get statefulset "*" -o jsonpath={.spec.replicas}") cat '${counts}'/"$5" 2>/dev/null || echo 1 ;;
  "-n alasio scale statefulset "*" --replicas=0") echo 0 >'${counts}'/"$5" && echo "$5" >>'${stopped}' ;;
  "-n alasio get pod -l "*" -o name") ;;
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
    /** Gives the cluster the bundled object store's claims, beside another's, each a volume of its own holding `bytes`: the store's. */
    store: (bytes = 1000) => {
      const claimed = ["data-alasio-seaweedfs-master-0", "data-alasio-seaweedfs-volume-0", "data-alasio-seaweedfs-filer-store-0"];
      writeFileSync(claims, [...claimed.map((claim) => `${claim}\tpv-${claim}`), "data-alasio-log-0\tpv-log", ""].join("\n"));
      for (const claim of claimed) {
        mkdirSync(join(volumes, `pv-${claim}`), { recursive: true });
        writeFileSync(join(volumes, `pv-${claim}`, "held"), Buffer.alloc(bytes, claim.length));
      }
      return claimed;
    },
    /** Takes the directory of `claim`'s volume away, as of a volume kept elsewhere than this machine. */
    unkept: (claim: string) => rmSync(join(volumes, `pv-${claim}`), { recursive: true, force: true }),
    /** The parts the cut-over stopped, in order, since the last time it was asked. */
    stopped: () => {
      const read = existsSync(stopped) ? readFileSync(stopped, "utf8").split("\n").filter(Boolean) : [];
      rmSync(stopped, { force: true });
      return read;
    },
    /** The counts the cut-over kept of the object store's parts as it stopped them, a `<part>\t<count>` each. */
    replicas: () => readFileSync(join(out, "object-store-replicas"), "utf8").split("\n").filter(Boolean),
    bin,
    /** What the cut-over copied of `claim`, where it did. */
    copied: (claim: string) => (existsSync(join(out, "object-store", claim, "held")) ? readFileSync(join(out, "object-store", claim, "held")) : null),
    volume: (claim: string) => readFileSync(join(volumes, `pv-${claim}`, "held")),
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
    // Of an installation whose object store is not the bundled one: none of it stopped, nor copied.
    assert.match(run.said, /cutting over from 10\.0\.26[^\n]*\n[^\n]* checking that no branch environment exists[^\n]*\n[^\n]* checking that the upgrade can run[^\n]*\n[^\n]* the object store is not the bundled one: none of it to copy\n[^\n]* past the point of no return[^\n]*\n[^\n]* upgrading, from the cut-over begun at [^\n]*\n[^\n]* upgraded at \S+\n[^\n]* through\n$/u);
    assert.deepEqual(at.stopped(), []);
  } finally {
    at.remove();
  }
});

test("the cut-over stops the bundled object store past its point of no return, writers first, and copies each of its volumes whole before the upgrade, once", () => {
  const at = machine();
  try {
    const claims = at.store(4096);
    const run = at.cutOver();
    assert.equal(run.code, 0, run.said);
    assert.deepEqual(at.ran(), ["check", "upgrade"]);
    assert.deepEqual(at.stopped(), STORE_PARTS);
    for (const claim of claims) assert.deepEqual(at.copied(claim), at.volume(claim), claim);
    // No other claim's volume.
    assert.equal(at.copied("data-alasio-log-0"), null);
    assert.match(run.said, /checking that the upgrade can run[^\n]*\n[^\n]* checking that the object store's volumes can be copied, to \S+\/object-store\n[^\n]* the object store's volumes hold \d+ bytes, which \S+ has room for, \d+ free\n[^\n]* past the point of no return[^\n]*\n[^\n]* stopping the object store[^\n]*\n/u);
    assert.match(run.said, /(copied the object store's data-alasio-seaweedfs-[a-z-]+-0, \d+ bytes, in \d+ s, to \S+\n[^\n]* ){3}the object store's volumes copied[^\n]*\n[^\n]* upgrading, from the cut-over/u);
    // Once: a run again copies and stops nothing.
    const again = at.cutOver();
    assert.equal(again.code, 0, again.said);
    assert.deepEqual(at.stopped(), []);
    assert.doesNotMatch(again.said, /copied/u);
  } finally {
    at.remove();
  }
});

test("the cut-over refuses an object store's volume it cannot copy, or a disk short of room for them, before its point of no return, and stopping nothing", () => {
  const at = machine();
  try {
    at.store();
    at.unkept("data-alasio-seaweedfs-volume-0");
    const unkept = at.cutOver();
    assert.notEqual(unkept.code, 0);
    assert.match(unkept.said, /FAILED: the claim data-alasio-seaweedfs-volume-0 is kept in no directory of this machine/u);
    assert.deepEqual([at.marks(), at.stopped()], [[], []]);
    at.store(1000);
    // A disk with less room than the volumes hold and a tenth more.
    writeFileSync(join(at.bin, "df"), "#!/bin/sh\necho Avail\necho 3200\n", { mode: 0o755 });
    const short = at.cutOver();
    assert.notEqual(short.code, 0);
    assert.match(short.said, /FAILED: \S+ has 3200 bytes free, short of the \d+ the object store's volumes hold and a tenth more/u);
    assert.deepEqual([at.marks(), at.stopped()], [[], []]);
  } finally {
    at.remove();
  }
});

test("the cut-over whose upgrade fails once it has stopped the object store says it is stopped still, and how to stay on the release before", () => {
  const at = machine();
  try {
    at.store();
    at.upgradeFails(true);
    const failed = at.cutOver();
    assert.notEqual(failed.code, 0);
    assert.match(failed.said, /FAILED: the upgrade failed \(upgrade\.log\): the object store stays stopped, every request of it failing, until the cut-over run again gets the upgrade through; to stay on 10\.0\.26 instead, scale each of its parts back to the count \S+\/object-store-replicas keeps/u);
    assert.deepEqual(at.replicas(), STORE_PARTS.map((part) => `${part}\t1`));
    at.upgradeFails(false);
    const again = at.cutOver();
    assert.equal(again.code, 0, again.said);
    assert.deepEqual(at.stopped(), STORE_PARTS);
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
    assert.match(refused.said, /FAILED: alasio upgrades main alone, and the branch environments 10\.0\.26 made [^\n]*\(alasio branch delete one, alasio branch delete two\)/u);
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
    assert.match(failed.said, /checking that the upgrade can run[^\n]*\n[^\n]* the object store is not the bundled one[^\n]*\n[^\n]* FAILED: a failure injected before the point of no return\n$/u);
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
    assert.match(again.said, /^[^\n]* upgraded at \S+\n[^\n]* through\n$/u);
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
