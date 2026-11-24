/**
 * The cut-over of an installation of the release before (upgrades/from-10.0.25.sh), in how it
 * runs, without a cluster: its checks and refusals, its point of no return, and how a run
 * again goes on from where one before stopped. The end-to-end run's upgrade shard runs it on
 * an installation of that release (test/e2e/alasio.test.ts).
 */
import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { once } from "node:events";
import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { delimiter, join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

import { NODE_AGENT_PORT, WORKSPACES_ROOT } from "../src/node-agent/names.ts";
import { startFakeS3 } from "./support/fake-s3.ts";

const SCRIPT = fileURLToPath(new URL("../upgrades/from-10.0.25.sh", import.meta.url));
/** The repository, as the node agent's image holds it at /opt/alasio. */
const REPOSITORY = fileURLToPath(new URL("..", import.meta.url)).replace(/\/$/u, "");

/** The PATH the cut-over runs with: `bin`'s, and the system's. */
const pathOf = (bin: string) => [bin, "/usr/bin", "/bin"].join(delimiter);

/** The node agent's pod the stand-in kubectl lists. */
const AGENT_POD = "alasio-node-agent-x1y2z";

/**
 * A machine for the cut-over: a kubectl that reaches its cluster and lists the namespaces
 * `namespaces` names, `<name>\t<branch label>` each, says the node agents rolled out, lists
 * AGENT_POD running, and runs what is run in that pod's node agent as `agentExec` says, which
 * reads what is run on its input: by default nothing, saying it gave no record its manifest; and
 * an UPGRADE and an UPGRADE_CHECK, each saying it ran, UPGRADE failing while `upgradeFails` is
 * there.
 */
function machine() {
  const root = mkdtempSync(join(tmpdir(), "alasio-cut-over-"));
  const bin = join(root, "bin");
  const ran = join(root, "ran");
  const namespaces = join(root, "namespaces");
  const upgradeFails = join(root, "upgrade-fails");
  const config = join(root, "config.json");
  const agentExec = join(root, "agent-exec");
  mkdirSync(bin);
  writeFileSync(agentExec, "cat >/dev/null\necho 'records of releases given their manifests: 0; removed: 0; holding one already: 0; written meanwhile: 0'\n");
  writeFileSync(namespaces, "");
  writeFileSync(config, JSON.stringify({ target: { host: {} }, install: { host: { enabled: true } } }), { mode: 0o600 });
  writeFileSync(join(bin, "kubectl"), `#!/bin/sh
case "$*" in
  "get --raw=/readyz") exit 0 ;;
  "get namespace -l alasio.dev/branch -o jsonpath="*) cat '${namespaces}' ;;
  "-n alasio rollout status daemonset/alasio-node-agent --timeout=10m") exit 0 ;;
  "-n alasio get pods -l app.kubernetes.io/instance=alasio,app.kubernetes.io/component=node-agent --field-selector=status.phase=Running -o jsonpath={.items[0].metadata.name}") printf '%s' '${AGENT_POD}' ;;
  "-n alasio exec -i ${AGENT_POD} -c node-agent -- node --input-type=module - /opt/alasio http://127.0.0.1:${NODE_AGENT_PORT} ${WORKSPACES_ROOT}") . '${agentExec}' ;;
  *) echo "kubectl $* is not the cut-over's" >&2; exit 2 ;;
esac
`, { mode: 0o755 });
  const out = join(root, "out");
  /** The cut-over, failing at `failAt` if given: what it came to. */
  const envOf = (failAt?: string) => ({
    PATH: pathOf(bin),
    OUT: out,
    CONFIG: config,
    UPGRADE: `echo upgrade >>'${ran}' && [ ! -e '${upgradeFails}' ]`,
    UPGRADE_CHECK: `echo check >>'${ran}'`,
    ...(failAt === undefined ? {} : { FAIL_AT: failAt }),
  });
  const cutOver = (failAt?: string) => {
    const run = spawnSync("bash", [SCRIPT], { encoding: "utf8", env: envOf(failAt) });
    return { code: run.status, said: run.stderr };
  };
  /** The cut-over, as cutOver runs it, beside what the test serves meanwhile, as the node agent's pod reaches it. */
  const cutOverAsync = async (failAt?: string) => {
    const child = spawn("bash", [SCRIPT], { env: envOf(failAt), stdio: ["ignore", "ignore", "pipe"] });
    let said = "";
    child.stderr.on("data", (chunk: Buffer) => (said += chunk.toString()));
    const [code] = (await once(child, "close")) as [number | null];
    return { code, said };
  };
  return {
    cutOver,
    cutOverAsync,
    /** What UPGRADE and UPGRADE_CHECK ran, in order, since the last time it was asked. */
    ran: () => {
      const read = existsSync(ran) ? readFileSync(ran, "utf8").split("\n").filter(Boolean) : [];
      rmSync(ran, { force: true });
      return read;
    },
    /** The cut-over's markers, of what it recorded. */
    marks: () => (existsSync(out) ? readdirSync(out).filter((name) => ["point-of-no-return", "upgraded"].includes(name)).sort() : []),
    namespaces: (listed: string) => writeFileSync(namespaces, listed),
    /** Runs what is run in the node agent's pod as the shell commands `commands` say, its input theirs. */
    agentRuns: (commands: string) => writeFileSync(agentExec, commands),
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
    assert.match(run.said, /cutting over from 10\.0\.25[^\n]*\n[^\n]* checking that no branch environment exists[^\n]*\n[^\n]* checking that the upgrade can run[^\n]*\n[^\n]* past the point of no return[^\n]*\n[^\n]* upgrading, from the cut-over begun at [^\n]*\n[^\n]* waiting for this release's node agents[^\n]*\n[^\n]* giving each record of a release its manifest, through the node agent alasio-node-agent-x1y2z \(records\.log\)\n[^\n]* records of releases given their manifests: 0; removed: 0; holding one already: 0; written meanwhile: 0\n[^\n]* upgraded at \S+\n[^\n]* through\n$/u);
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
    assert.match(refused.said, /FAILED: alasio upgrades main alone, and the branch environments 10\.0\.25 made [^\n]*\(alasio branch delete one, alasio branch delete two\)/u);
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
    assert.match(again.said, /^[^\n]* waiting for this release's node agents[^\n]*\n[^\n]* giving each record[^\n]*\n[^\n]* records of releases given[^\n]*\n[^\n]* upgraded at \S+\n[^\n]* through\n$/u);
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

/** The manifest of the snapshot `snapshot` of main's `workspace`, taken under r1, compacted from `compactedFrom` where given. */
const manifestOf = (workspace: string, snapshot: string, compactedFrom?: string) => ({
  environment: "main",
  workspace,
  snapshot,
  release: "r1",
  cpuFeatures: [],
  podTemplate: {},
  layers: [{ digest: "a".repeat(64), files: [] }],
  bytes: 0,
  takenAt: "2026-10-17T12:00:00.000Z",
  pauseMs: 0,
  uploadMs: 0,
  ...(compactedFrom === undefined ? {} : { compactedFrom }),
});

test("once upgraded, the cut-over gives each record of a release its manifest, through a node agent of this release: the workspace's current snapshot where it is the one the record names or one compacted from it, the record removed where it is neither or there is none, one that holds a manifest left as it is; run again, it gives none twice", async () => {
  const at = machine();
  const s3 = await startFakeS3({ bucket: "snapshots", region: "us-east-1", accessKeyId: "AKID", secretAccessKey: "secret" });
  const [S1, S2, C1] = ["20261017T120000000Z-0000000a", "20261017T130000000Z-0000000b", "20261017T123000000Z-0000000c"];
  const done = { snapshot: S1, release: "r1", manifest: manifestOf("fs-done", S1) };
  // Each workspace's record, as its agent keeps it beside its files, on workspace storage, and what was asked of the agent.
  const workspaces = mkdtempSync(join(tmpdir(), "alasio-cut-over-workspaces-"));
  const recordPath = (workspace: string) => join(workspaces, "main", workspace, "released.json");
  const records = {
    set: (workspace: string, record: unknown) => (mkdirSync(join(workspaces, "main", workspace), { recursive: true }), writeFileSync(recordPath(workspace), JSON.stringify(record))),
    delete: (workspace: string) => rmSync(recordPath(workspace), { force: true }),
    all: () => Object.fromEntries(readdirSync(join(workspaces, "main")).filter((each) => existsSync(recordPath(each))).map((each) => [each, JSON.parse(readFileSync(recordPath(each), "utf8")) as unknown])),
  };
  for (const [workspace, record] of [["fs-same", { snapshot: S1, release: "r1", pod: "pool-0" }], ["fs-comp", { snapshot: S1, release: "r1" }], ["fs-moved", { snapshot: S1, release: "r1" }], ["fs-gone", { snapshot: S1, release: "r1" }], ["fs-done", done]] as const) records.set(workspace, record);
  mkdirSync(join(workspaces, "main", "fs-none"), { recursive: true });
  // One that is no record, and one that is a link, as the agent reads neither: removed.
  records.set("fs-corrupt", {});
  writeFileSync(recordPath("fs-corrupt"), "{");
  mkdirSync(join(workspaces, "main", "fs-link"), { recursive: true });
  symlinkSync(recordPath("fs-done"), recordPath("fs-link"));
  // One the gateway writes again, a binding of it ending, as its current snapshot is read: left as the gateway wrote it.
  records.set("fs-raced", { snapshot: S1, release: "r1" });
  const raced = { snapshot: S2, release: "r1", manifest: manifestOf("fs-raced", S2) };
  s3.objects.set("main/fs-raced/current.json", Buffer.from(JSON.stringify(manifestOf("fs-raced", S1))));
  s3.holdGet = (key) => (key === "main/fs-raced/current.json" ? Promise.resolve(records.set("fs-raced", raced)).then(() => (s3.holdGet = () => null, undefined)) : null);
  const asked: string[] = [];
  for (const [workspace, current] of [["fs-same", manifestOf("fs-same", S1)], ["fs-comp", manifestOf("fs-comp", C1, S1)], ["fs-moved", manifestOf("fs-moved", S2)], ["fs-done", manifestOf("fs-done", S1)]] as const) {
    s3.objects.set(`main/${workspace}/current.json`, Buffer.from(JSON.stringify(current)));
  }
  const agent = createServer((request, response) => {
    void (async () => {
      const chunks: Buffer[] = [];
      for await (const chunk of request) chunks.push(chunk as Buffer);
      if (request.headers.authorization !== "Bearer agent-token") return response.writeHead(401).end();
      const path = request.url ?? "";
      asked.push(`${request.method} ${path}`);
      if (path === "/workspaces") return response.writeHead(200).end(JSON.stringify({ environments: ["main"] }));
      if (path === "/workspaces/main") return response.writeHead(200).end(JSON.stringify({ workspaces: ["fs-comp", "fs-corrupt", "fs-done", "fs-gone", "fs-link", "fs-moved", "fs-none", "fs-raced", "fs-same"] }));
      const workspace = /^\/workspaces\/main\/([^/]+)\/released$/u.exec(path)?.[1] ?? "";
      // As this release's agent: a record of the release before, of no manifest, is none it reads.
      if (request.method === "GET") {
        if (!existsSync(recordPath(workspace))) return response.writeHead(404).end();
        const record = JSON.parse(readFileSync(recordPath(workspace), "utf8")) as { readonly manifest?: unknown };
        return record.manifest === undefined ? response.writeHead(500).end("its record of its release is not one") : response.writeHead(200).end(JSON.stringify(record));
      }
      if (request.method === "PUT") records.set(workspace, JSON.parse(Buffer.concat(chunks).toString()));
      if (request.method === "DELETE") records.delete(workspace);
      return response.writeHead(204).end();
    })();
  });
  agent.listen(0, "127.0.0.1");
  await once(agent, "listening");
  const url = `http://127.0.0.1:${(agent.address() as AddressInfo).port}`;
  at.agentRuns([
    `export S3_ENDPOINT='${s3.endpoint}' S3_REGION=us-east-1 S3_BUCKET=snapshots AWS_ACCESS_KEY_ID=AKID AWS_SECRET_ACCESS_KEY=secret ALASIO_NODE_AGENT_TOKEN=agent-token`,
    `exec '${process.execPath}' --input-type=module - '${REPOSITORY}' '${url}' '${workspaces}'`,
  ].join("\n"));
  try {
    const run = await at.cutOverAsync();
    assert.equal(run.code, 0, run.said);
    assert.match(run.said, /records of releases given their manifests: 2; removed: 4; holding one already: 1; written meanwhile: 1\n/u);
    assert.deepEqual(records.all(), {
      "fs-same": { snapshot: S1, release: "r1", pod: "pool-0", manifest: manifestOf("fs-same", S1) },
      "fs-comp": { snapshot: S1, release: "r1", manifest: manifestOf("fs-comp", C1, S1) },
      "fs-done": done,
      "fs-raced": raced,
    });
    // Run again, from what follows the upgrade: nothing given twice.
    asked.length = 0;
    const again = await at.cutOverAsync();
    assert.equal(again.code, 0, again.said);
    assert.match(again.said, /records of releases given their manifests: 0; removed: 0; holding one already: 4; written meanwhile: 0\n/u);
    assert.ok(asked.every((each) => each.startsWith("GET ")), asked.join(", "));
    // One the agent refuses fails the cut-over, which goes on with it when run again.
    records.set("fs-same", { snapshot: S1, release: "r1" });
    agent.removeAllListeners("request");
    agent.on("request", (_request, response) => response.writeHead(503).end("workspace storage is not mounted"));
    const refused = await at.cutOverAsync();
    assert.notEqual(refused.code, 0);
    assert.match(refused.said, /FAILED: the records of releases could not all be given their manifests \(records\.log\)/u);
  } finally {
    agent.close();
    await s3.close();
    rmSync(workspaces, { recursive: true, force: true });
    at.remove();
  }
});
