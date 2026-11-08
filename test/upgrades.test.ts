/**
 * The cut-over of an installation of the release before (upgrades/from-10.0.9.sh), in how it
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

const SCRIPT = fileURLToPath(new URL("../upgrades/from-10.0.9.sh", import.meta.url));

/** The PATH the cut-over runs with: `bin`'s, and the system's. */
const pathOf = (bin: string) => [bin, "/usr/bin", "/bin"].join(delimiter);

/**
 * A machine for the cut-over: a kubectl that reaches its cluster, lists the namespaces
 * `namespaces` names, `<name>\t<branch label>` each, says the sandbox gateway runs at a count of
 * one until it scales it to none, and lists the sandbox gateway's pod, there until it
 * scales the gateway to none and for as many looks after as `lingers` says, the scale and every
 * look said in order in `looked`, and fails to list any while `unlisted` is there; and an UPGRADE
 * and an UPGRADE_CHECK, each saying it ran, UPGRADE failing while `upgradeFails` is there, and starting the gateway first
 * while `startsGateway` is.
 */
function machine() {
  const root = mkdtempSync(join(tmpdir(), "alasio-cut-over-"));
  const bin = join(root, "bin");
  const ran = join(root, "ran");
  const namespaces = join(root, "namespaces");
  const upgradeFails = join(root, "upgrade-fails");
  const startsGateway = join(root, "starts-gateway");
  const lingers = join(root, "lingers");
  const stopped = join(root, "stopped");
  const looked = join(root, "looked");
  const unlisted = join(root, "unlisted");
  const config = join(root, "config.json");
  mkdirSync(bin);
  writeFileSync(namespaces, "");
  writeFileSync(lingers, "0");
  writeFileSync(config, JSON.stringify({ target: { host: {} }, install: { host: { enabled: true } } }), { mode: 0o600 });
  writeFileSync(join(bin, "kubectl"), `#!/bin/sh
case "$*" in
  "get --raw=/readyz") exit 0 ;;
  "get namespace -l alasio.dev/branch -o jsonpath="*) cat '${namespaces}' ;;
  "-n alasio get deployment alasio-sandbox-gateway -o jsonpath={.spec.replicas}") if [ -e '${stopped}' ]; then printf 0; else printf 1; fi ;;
  "-n alasio scale deployment alasio-sandbox-gateway --replicas=0") echo "stop the gateway" >>'${ran}'; echo stop >>'${looked}'; touch '${stopped}' ;;
  "-n alasio get pod -l app.kubernetes.io/component=sandbox-gateway -o name")
    [ ! -e '${unlisted}' ] || exit 1
    left=$(cat '${lingers}')
    if [ ! -e '${stopped}' ] || [ "$left" -gt 0 ]; then
      [ ! -e '${stopped}' ] || echo $((left - 1)) >'${lingers}'
      echo "gateway: running" >>'${looked}'; echo pod/alasio-sandbox-gateway-0
    else echo "gateway: none" >>'${looked}'; fi ;;
  *) echo "kubectl $* is not the cut-over's" >&2; exit 2 ;;
esac
`, { mode: 0o755 });
  const out = join(root, "out");
  /** The cut-over, failing at `failAt` if given, its waits bounded by `bounds`: what it came to. */
  const cutOver = (failAt?: string, bounds: Readonly<Record<string, string>> = {}) => {
    const run = spawnSync("bash", [SCRIPT], {
      encoding: "utf8",
      env: {
        PATH: pathOf(bin),
        OUT: out,
        CONFIG: config,
        UPGRADE: `echo upgrade >>'${ran}' && { [ ! -e '${startsGateway}' ] || rm -f '${stopped}'; } && [ ! -e '${upgradeFails}' ]`,
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
    /** What UPGRADE and UPGRADE_CHECK ran, and the gateway's stop, in order, since the last time it was asked. */
    ran: () => lines(ran),
    /** The gateway's stop and each look at its pods, in order, since the last time it was asked. */
    looked: () => lines(looked),
    /** Whether the cut-over recorded the gateway stopped. */
    gatewayStopped: () => existsSync(join(out, "gateway-stopped")),
    /** The count the cut-over recorded the gateway ran at before it stopped it, if it did. */
    gatewayReplicas: () => (existsSync(join(out, "gateway-replicas")) ? readFileSync(join(out, "gateway-replicas"), "utf8") : null),
    lingers: (looks: number) => writeFileSync(lingers, String(looks)),
    unlisted: (fails: boolean) => (fails ? writeFileSync(unlisted, "") : rmSync(unlisted, { force: true })),
    /** The cut-over's markers, of what it recorded. */
    marks: () => (existsSync(out) ? readdirSync(out).filter((name) => ["point-of-no-return", "upgraded"].includes(name)).sort() : []),
    namespaces: (listed: string) => writeFileSync(namespaces, listed),
    upgradeFails: (fails: boolean) => (fails ? writeFileSync(upgradeFails, "") : rmSync(upgradeFails, { force: true })),
    /** Whether UPGRADE starts the sandbox gateway again, as the release's apply does, before it fails where it fails. */
    startsGateway: (starts: boolean) => (starts ? writeFileSync(startsGateway, "") : rmSync(startsGateway, { force: true })),
    remove: () => rmSync(root, { recursive: true, force: true }),
  };
}

test("the cut-over checks, upgrades past its point of no return, and says it is through", () => {
  const at = machine();
  try {
    const run = at.cutOver();
    assert.equal(run.code, 0, run.said);
    assert.deepEqual(at.ran(), ["check", "stop the gateway", "upgrade"]);
    assert.deepEqual(at.marks(), ["point-of-no-return", "upgraded"]);
    assert.match(run.said, /cutting over from 10\.0\.9[^\n]*\n[^\n]* checking that no branch environment exists[^\n]*\n[^\n]* checking that the upgrade can run[^\n]*\n[^\n]* past the point of no return[^\n]*\n[^\n]* stopping the sandbox gateway[^\n]*\n[^\n]* the sandbox gateway stopped, to be started again by the upgrade\n[^\n]* upgrading, from the cut-over begun at [^\n]*\n[^\n]* upgraded at \S+\n[^\n]* through\n$/u);
  } finally {
    at.remove();
  }
});

test("past its point of no return, the cut-over stops the sandbox gateway, and records it stopped once its pod is gone, before it upgrades", () => {
  const at = machine();
  try {
    at.lingers(2);
    const run = at.cutOver();
    assert.equal(run.code, 0, run.said);
    assert.deepEqual(at.ran(), ["check", "stop the gateway", "upgrade"]);
    // Recorded stopped after the look that found no pod.
    assert.deepEqual(at.looked(), ["stop", "gateway: running", "gateway: running", "gateway: none"]);
    assert.ok(at.gatewayStopped());
    // The count it ran at kept, for a way back to the release before.
    assert.equal(at.gatewayReplicas(), "1");
  } finally {
    at.remove();
  }
});

test("the cut-over fails, recording nothing, where the sandbox gateway's pod outlives its bound, or its pods cannot be listed, rather than take none for none", () => {
  const at = machine();
  try {
    at.lingers(1_000);
    const lingering = at.cutOver(undefined, { STOP_WAIT_SECONDS: "0" });
    assert.notEqual(lingering.code, 0);
    assert.match(lingering.said, /FAILED: the sandbox gateway did not stop within 0 s\n$/u);
    assert.deepEqual([at.ran(), at.looked(), at.gatewayStopped()], [["check", "stop the gateway"], ["stop", "gateway: running"], false]);
    at.unlisted(true);
    const unlisted = at.cutOver();
    assert.notEqual(unlisted.code, 0);
    assert.match(unlisted.said, /FAILED: the pods of app\.kubernetes\.io\/component=sandbox-gateway in alasio could not be listed\n$/u);
    assert.deepEqual([at.ran(), at.gatewayStopped()], [["stop the gateway"], false]);
    // Run again once its pods can be listed, and are gone: the count it ran at kept as first read, not the none it runs at now.
    at.unlisted(false);
    at.lingers(0);
    assert.equal(at.cutOver().code, 0);
    assert.equal(at.gatewayReplicas(), "1");
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
    assert.match(refused.said, /FAILED: alasio upgrades main alone, and the branch environments 10\.0\.9 made [^\n]*\(alasio branch delete one, alasio branch delete two\)/u);
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
    assert.deepEqual([at.ran(), at.marks()], [["check", "stop the gateway", "upgrade"], ["point-of-no-return", "upgraded"]]);
  } finally {
    at.remove();
  }
});

test("the cut-over failing past its point of no return, or in its upgrade, goes on with the upgrade when run again, checking nothing again; one failing in its upgrade says the gateway is stopped, and how to go back to the release before", () => {
  const at = machine();
  try {
    const failed = at.cutOver("past-the-point-of-no-return");
    assert.notEqual(failed.code, 0);
    assert.match(failed.said, /FAILED: a failure injected past the point of no return, before the upgrade\n$/u);
    assert.deepEqual([at.ran(), at.marks()], [["check"], ["point-of-no-return"]]);
    at.upgradeFails(true);
    const broken = at.cutOver();
    assert.notEqual(broken.code, 0);
    assert.match(
      broken.said,
      /FAILED: the upgrade failed \(upgrade\.log\): the sandbox gateway stays stopped, every session operation failing, until the cut-over run again gets the upgrade through; to stay on 10\.0\.9 instead, only while no node agent of this release runs \(kubectl -n alasio get pods -l app\.kubernetes\.io\/component=node-agent -L app\.kubernetes\.io\/version\), kubectl -n alasio scale deployment alasio-sandbox-gateway --replicas=1; once one runs, the cut-over run again is the way back\n$/u,
    );
    assert.deepEqual([at.ran(), at.marks()], [["stop the gateway", "upgrade"], ["point-of-no-return"]]);
    // One that fails once it has started the gateway again says so, not that it is stopped.
    at.startsGateway(true);
    const started = at.cutOver();
    assert.notEqual(started.code, 0);
    assert.match(started.said, /FAILED: the upgrade failed \(upgrade\.log\) once it had started the sandbox gateway again, at 1: the cut-over run again goes on with the upgrade\n$/u);
    assert.deepEqual([at.ran(), at.marks()], [["upgrade"], ["point-of-no-return"]]);
    at.startsGateway(false);
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
    assert.deepEqual([at.ran(), at.marks()], [["check", "stop the gateway", "upgrade"], ["point-of-no-return", "upgraded"]]);
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
