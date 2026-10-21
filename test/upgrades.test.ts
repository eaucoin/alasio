/**
 * The cut-over of an installation of the release before (upgrades/from-9.0.0.sh), in how it
 * runs, without a cluster: its checks and refusals, its point of no return, and how a run
 * again goes on from where one before stopped. The end-to-end run's upgrade shard runs it on
 * an installation of that release (test/e2e/alasio.test.ts).
 */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

const SCRIPT = fileURLToPath(new URL("../upgrades/from-9.0.0.sh", import.meta.url));

/** The sandbox image's digest, as containerd records it. */
const PAUSE = `sha256:${"a".repeat(64)}`;

/**
 * A machine for the cut-over: a kubectl that reaches its cluster, lists the namespaces
 * `namespaces` names, `<name>\t<branch label>` each, and alasio's pod, busy with a turn for
 * as many looks as `busy` says, there until it scales alasio to none and for as many looks
 * after as `lingers` says, every look and the scale said in order in `looked`, and fails to
 * list any while `unlisted` is there; an UPGRADE and an UPGRADE_CHECK, each saying it ran,
 * UPGRADE failing while `upgradeFails` is there; and a node, in `node`, whose containerd
 * pins the sandbox image k3s gives it, and records the images `images` lists, and whose
 * k3s's ctr says what it was asked to fetch, fetching what no registry answers for failing,
 * and what `unfetched` names fetched to no end.
 */
function machine() {
  const root = mkdtempSync(join(tmpdir(), "alasio-cut-over-"));
  const bin = join(root, "bin");
  const ran = join(root, "ran");
  const namespaces = join(root, "namespaces");
  const upgradeFails = join(root, "upgrade-fails");
  const busy = join(root, "busy");
  const lingers = join(root, "lingers");
  const stopped = join(root, "stopped");
  const looked = join(root, "looked");
  const unlisted = join(root, "unlisted");
  const config = join(root, "config.json");
  const node = join(root, "node");
  const fetched = join(root, "fetched");
  // The images containerd records, `<name> <digest> <complete|incomplete>` each, as its check says them, the digests no registry
  // answers for, and those a fetch of leaves lacking still.
  const images = join(root, "images");
  const unreachable = join(root, "unreachable");
  const unfetched = join(root, "unfetched");
  const containerd = join(node, "var/lib/rancher/k3s/agent/etc/containerd");
  for (const made of [bin, containerd, join(node, "usr/local/bin")]) mkdirSync(made, { recursive: true });
  writeFileSync(namespaces, "");
  writeFileSync(busy, "0");
  writeFileSync(lingers, "0");
  writeFileSync(config, JSON.stringify({ target: { host: {} }, install: { host: { enabled: true } } }), { mode: 0o600 });
  writeFileSync(images, `docker.io/rancher/mirrored-pause:3.10.2 ${PAUSE} complete\n`);
  writeFileSync(unreachable, "");
  writeFileSync(unfetched, "");
  // As k3s writes it, from its --pause-image.
  const pins = (sandbox: string) => writeFileSync(join(containerd, "config.toml"), `version = 3\n\n[plugins.'io.containerd.cri.v1.images'.pinned_images]\n  sandbox = "${sandbox}"\n`);
  pins("rancher/mirrored-pause:3.10.2");
  writeFileSync(join(bin, "kubectl"), `#!/bin/sh
case "$*" in
  "get --raw=/readyz") exit 0 ;;
  "get namespace -l alasio.dev/branch -o jsonpath="*) cat '${namespaces}' ;;
  "-n alasio get pod -l app.kubernetes.io/component=alasio,alasio.dev/busy=true -o name")
    [ ! -e '${unlisted}' ] || exit 1
    left=$(cat '${busy}')
    if [ "$left" -gt 0 ]; then echo $((left - 1)) >'${busy}'; echo "busy: running" >>'${looked}'; echo pod/alasio-0; else echo "busy: none" >>'${looked}'; fi ;;
  "-n alasio scale deployment alasio --replicas=0") echo "stop alasio" >>'${ran}'; echo stop >>'${looked}'; touch '${stopped}' ;;
  "-n alasio get pod -l app.kubernetes.io/component=alasio -o name")
    [ ! -e '${unlisted}' ] || exit 1
    left=$(cat '${lingers}')
    if [ ! -e '${stopped}' ] || [ "$left" -gt 0 ]; then
      [ ! -e '${stopped}' ] || echo $((left - 1)) >'${lingers}'
      echo "alasio: running" >>'${looked}'; echo pod/alasio-0
    else echo "alasio: none" >>'${looked}'; fi ;;
  *) echo "kubectl $* is not the cut-over's" >&2; exit 2 ;;
esac
`, { mode: 0o755 });
  // Its check of the images its filter names, and its fetches, which complete each image of the digest fetched.
  writeFileSync(join(node, "usr/local/bin/k3s"), `#!/bin/sh
[ "$1 $2 $3" = "ctr --namespace k8s.io" ] || { echo "k3s $* is not the cut-over's" >&2; exit 2; }
shift 3
case "$1 $2" in
  "images check")
    echo "REF TYPE DIGEST STATUS SIZE UNPACKED"
    awk -v filter="$3" '"name==" $1 == filter { print $1, "application/vnd.oci.image.index.v1+json", $2, ($3 == "complete" ? "complete (2/2)" : "incomplete (1/2)"), "1.0 MiB/1.0 MiB", "true" }' '${images}' ;;
  "content fetch")
    echo "$3 $4 $5" >>'${fetched}'
    digest=\${5##*@}
    ! grep -qx "$digest" '${unreachable}' || { echo "ctr: failed to resolve $5" >&2; exit 1; }
    ! grep -qx "$digest" '${unfetched}' || exit 0
    awk -v digest="$digest" '$2 == digest { $3 = "complete" } { print }' '${images}' >'${images}.next' && mv '${images}.next' '${images}' ;;
  *) echo "k3s ctr $* is not the cut-over's" >&2; exit 2 ;;
esac
`, { mode: 0o755 });
  const out = join(root, "out");
  /** The cut-over, failing at `failAt` if given, its waits bounded by `bounds`: what it came to. */
  const cutOver = (failAt?: string, bounds: Readonly<Record<string, string>> = {}) => {
    const run = spawnSync("bash", [SCRIPT], {
      encoding: "utf8",
      env: {
        PATH: `${bin}:/usr/bin:/bin`,
        OUT: out,
        CONFIG: config,
        NODE_ROOT: node,
        UPGRADE: `echo upgrade >>'${ran}' && [ ! -e '${upgradeFails}' ]`,
        UPGRADE_CHECK: `echo check >>'${ran}'`,
        POLL_SECONDS: "0",
        ...bounds,
        ...(failAt === undefined ? {} : { FAIL_AT: failAt }),
      },
    });
    return { code: run.status, said: run.stderr };
  };
  const lines = (file: string) => {
    const read = existsSync(file) ? readFileSync(file, "utf8").split("\n").filter(Boolean) : [];
    rmSync(file, { force: true });
    return read;
  };
  return {
    cutOver,
    /** What UPGRADE and UPGRADE_CHECK ran, and alasio stopped, in order, since the last time it was asked. */
    ran: () => lines(ran),
    /** The cut-over's markers, of what it recorded. */
    marks: () => (existsSync(out) ? readdirSync(out).filter((name) => ["point-of-no-return", "upgraded"].includes(name)).sort() : []),
    namespaces: (listed: string) => writeFileSync(namespaces, listed),
    upgradeFails: (fails: boolean) => (fails ? writeFileSync(upgradeFails, "") : rmSync(upgradeFails, { force: true })),
    /** Has alasio busy with a turn for the next `looks` looks at it. */
    busy: (looks: number) => writeFileSync(busy, String(looks)),
    /** Has alasio's pod there for `looks` looks once alasio is scaled to none. */
    lingers: (looks: number) => writeFileSync(lingers, String(looks)),
    /** Has kubectl fail to list alasio's pods, or list them again. */
    unlisted: (fails: boolean) => (fails ? writeFileSync(unlisted, "") : rmSync(unlisted, { force: true })),
    /** Each look at alasio's pods, and the scale, in order, since the last time it was asked. */
    looked: () => lines(looked),
    /** Whether the cut-over recorded alasio stopped. */
    alasioStopped: () => existsSync(join(out, "alasio-stopped")),
    /** Has alasio running again, as the upgrade starts it. */
    started: () => rmSync(stopped, { force: true }),
    /** Has containerd pin `sandbox` as the sandbox image, as k3s writes it. */
    pins,
    /** Has containerd record the images `recorded`, `[name, digest, whether it holds all their content]` each. */
    hasImages: (recorded: readonly (readonly [string, string, boolean])[]) =>
      writeFileSync(images, recorded.map(([name, digest, whole]) => `${name} ${digest} ${whole ? "complete" : "incomplete"}\n`).join("")),
    /** The images containerd records, as hasImages gives them. */
    images: () => readFileSync(images, "utf8").split("\n").filter(Boolean).map((line) => {
      const [name = "", digest = "", status] = line.split(" ");
      return [name, digest, status === "complete"] as const;
    }),
    /** What k3s's ctr was asked to fetch, `--hosts-dir <dir> <reference>` each, since the last time it was asked. */
    fetched: () => lines(fetched),
    /** Has no registry answer for the digests `digests`. */
    unreachable: (digests: readonly string[]) => writeFileSync(unreachable, digests.map((digest) => `${digest}\n`).join("")),
    /** Has a fetch of the digests `digests` leave them lacking still, as a registry answering with another's would. */
    unfetched: (digests: readonly string[]) => writeFileSync(unfetched, digests.map((digest) => `${digest}\n`).join("")),
    containerd,
    remove: () => rmSync(root, { recursive: true, force: true }),
  };
}

test("the cut-over checks, upgrades past its point of no return, and says it is through", () => {
  const at = machine();
  try {
    const run = at.cutOver();
    assert.equal(run.code, 0, run.said);
    assert.deepEqual(at.ran(), ["check", "stop alasio", "upgrade"]);
    assert.deepEqual(at.marks(), ["point-of-no-return", "upgraded"]);
    assert.match(run.said, /cutting over from 9\.0\.0[^]*checking that no branch environment exists[^]*checking that the upgrade can run[^]*past the point of no return[^]*waiting for alasio to run no turn[^]*alasio stopped, to be started again by the upgrade\n[^\n]* upgrading, from the cut-over begun at [^\n]*\n[^\n]* upgraded at \S+\n[^\n]* through\n$/u);
  } finally {
    at.remove();
  }
});

test("past its point of no return, the cut-over waits for alasio to run no turn, stops it, and records it stopped once its pod is gone, before it upgrades", () => {
  const at = machine();
  try {
    at.busy(2);
    at.lingers(2);
    const run = at.cutOver();
    assert.equal(run.code, 0, run.said);
    assert.deepEqual(at.ran(), ["check", "stop alasio", "upgrade"]);
    // Stopped after the look that found no turn, and recorded stopped after the look that found no pod.
    assert.deepEqual(at.looked(), ["busy: running", "busy: running", "busy: none", "stop", "alasio: running", "alasio: running", "alasio: none"]);
    assert.ok(at.alasioStopped());
    assert.match(run.said, /past the point of no return[^\n]*\n[^\n]* waiting for alasio to run no turn, then stopping it[^\n]*\n[^\n]* alasio stopped, to be started again by the upgrade\n[^\n]* upgrading/u);
  } finally {
    at.remove();
  }
});

test("the cut-over fails, stopping nothing, where alasio runs a turn on past its bound, and, alasio stopped, recording nothing where its pod outlives its bound", () => {
  const at = machine();
  try {
    at.busy(1_000);
    const busy = at.cutOver(undefined, { TURN_WAIT_SECONDS: "0" });
    assert.notEqual(busy.code, 0);
    assert.match(busy.said, /FAILED: alasio ran a turn on for 0 s: let it end, and run the cut-over again\n$/u);
    assert.deepEqual([at.ran(), at.looked(), at.alasioStopped()], [["check"], ["busy: running"], false]);
    at.busy(0);
    at.lingers(1_000);
    const lingering = at.cutOver(undefined, { STOP_WAIT_SECONDS: "0" });
    assert.notEqual(lingering.code, 0);
    assert.match(lingering.said, /FAILED: alasio did not stop within 0 s\n$/u);
    assert.deepEqual([at.ran(), at.looked(), at.alasioStopped()], [["stop alasio"], ["busy: none", "stop", "alasio: running"], false]);
  } finally {
    at.remove();
  }
});

test("the cut-over fails, stopping nothing, where alasio's pods cannot be listed, rather than take none for none", () => {
  const at = machine();
  try {
    at.unlisted(true);
    const run = at.cutOver();
    assert.notEqual(run.code, 0);
    assert.match(run.said, /FAILED: the pods of app\.kubernetes\.io\/component=alasio,alasio\.dev\/busy=true in alasio could not be listed\n$/u);
    assert.deepEqual([at.ran(), at.alasioStopped()], [["check"], false]);
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
    assert.match(refused.said, /FAILED: alasio upgrades main alone, and the branch environments 9\.0\.0 made [^\n]*\(alasio branch delete one, alasio branch delete two\)/u);
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
    assert.deepEqual([at.ran(), at.marks()], [["check", "stop alasio", "upgrade"], ["point-of-no-return", "upgraded"]]);
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
    assert.match(broken.said, /FAILED: the upgrade failed \(upgrade\.log\): alasio stays stopped until an upgrade gets as far as starting it; the cut-over run again goes on with it\n$/u);
    assert.deepEqual([at.ran(), at.marks()], [["stop alasio", "upgrade"], ["point-of-no-return"]]);
    // Run again, alasio the release's as the upgrade that failed left it, it is stopped no more.
    at.started();
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
    assert.deepEqual([at.ran(), at.marks()], [["check", "stop alasio", "upgrade"], ["point-of-no-return", "upgraded"]]);
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
    const needs = { PATH: `${join(root, "bin")}:/usr/bin:/bin`, OUT: join(root, "out"), UPGRADE: "true", UPGRADE_CHECK: "true" };
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

test("before anything changes, the cut-over fetches what containerd lacks of the sandbox image, by its name as containerd records it and the digest it names, and of no other image", () => {
  const at = machine();
  try {
    // As the box's was: pruned, and pulled again onto its snapshot, its layer's blob never fetched; beside an image pulled lazily.
    const lazy = `sha256:${"b".repeat(64)}`;
    at.hasImages([["docker.io/rancher/mirrored-pause:3.10.2", PAUSE, false], [`ghcr.io/eaucoin/alasio@${lazy}`, lazy, false]]);
    const run = at.cutOver("before-the-point-of-no-return");
    assert.notEqual(run.code, 0);
    assert.match(run.said, /checking that no branch environment exists[^\n]*\n[^\n]* fetching what containerd lacks of the sandbox image docker\.io\/rancher\/mirrored-pause:3\.10\.2, recorded without it[^\n]*\n[^\n]* checking that the upgrade can run/u);
    assert.deepEqual(at.fetched(), [`--hosts-dir ${at.containerd}/certs.d docker.io/rancher/mirrored-pause@${PAUSE}`]);
    assert.deepEqual(at.images(), [["docker.io/rancher/mirrored-pause:3.10.2", PAUSE, true], [`ghcr.io/eaucoin/alasio@${lazy}`, lazy, false]]);
    // Whole, it fetches nothing; nor where none is recorded, the CRI pulling it whole; nor where a run again finds it whole.
    const again = at.cutOver();
    assert.equal(again.code, 0, again.said);
    assert.doesNotMatch(again.said, /fetching/u);
    assert.deepEqual(at.fetched(), []);
    const none = machine();
    try {
      none.hasImages([]);
      const unrecorded = none.cutOver();
      assert.equal(unrecorded.code, 0, unrecorded.said);
      assert.deepEqual(none.fetched(), []);
    } finally {
      none.remove();
    }
  } finally {
    at.remove();
  }
});

test("the sandbox image is fetched by the name containerd records it by, whichever name k3s pins it by", () => {
  for (const [pinned, recorded, fetched] of [
    ["docker.io/rancher/mirrored-pause:3.10.2", "docker.io/rancher/mirrored-pause:3.10.2", "docker.io/rancher/mirrored-pause"],
    ["pause:3.10", "docker.io/library/pause:3.10", "docker.io/library/pause"],
    ["registry.k8s.io/pause:3.10", "registry.k8s.io/pause:3.10", "registry.k8s.io/pause"],
    ["localhost:5000/pause:3.10", "localhost:5000/pause:3.10", "localhost:5000/pause"],
  ] as const) {
    const at = machine();
    try {
      at.pins(pinned);
      at.hasImages([[recorded, PAUSE, false]]);
      const run = at.cutOver();
      assert.equal(run.code, 0, run.said);
      assert.deepEqual(at.fetched(), [`--hosts-dir ${at.containerd}/certs.d ${fetched}@${PAUSE}`]);
    } finally {
      at.remove();
    }
  }
});

test("the cut-over fails before its point of no return where what containerd lacks of the sandbox image cannot be fetched, and a run again fetches it", () => {
  const at = machine();
  try {
    at.hasImages([["docker.io/rancher/mirrored-pause:3.10.2", PAUSE, false]]);
    at.unreachable([PAUSE]);
    const failed = at.cutOver();
    assert.notEqual(failed.code, 0);
    assert.match(failed.said, /FAILED: what containerd lacks of the sandbox image docker\.io\/rancher\/mirrored-pause:3\.10\.2 could not be fetched from its registry\n$/u);
    assert.deepEqual([at.ran(), at.marks()], [[], []]);
    at.unreachable([]);
    const again = at.cutOver();
    assert.equal(again.code, 0, again.said);
    assert.deepEqual(at.images(), [["docker.io/rancher/mirrored-pause:3.10.2", PAUSE, true]]);
    assert.deepEqual([at.ran(), at.marks()], [["check", "stop alasio", "upgrade"], ["point-of-no-return", "upgraded"]]);
  } finally {
    at.remove();
  }
});

test("the cut-over fails before its point of no return where containerd lacks content of the sandbox image still, fetched", () => {
  const at = machine();
  try {
    at.hasImages([["docker.io/rancher/mirrored-pause:3.10.2", PAUSE, false]]);
    at.unfetched([PAUSE]);
    const failed = at.cutOver();
    assert.notEqual(failed.code, 0);
    assert.match(failed.said, /FAILED: containerd lacks content of the sandbox image docker\.io\/rancher\/mirrored-pause:3\.10\.2 still, fetched\n$/u);
    assert.equal(at.fetched().length, 1);
    assert.deepEqual([at.ran(), at.marks()], [[], []]);
  } finally {
    at.remove();
  }
});
