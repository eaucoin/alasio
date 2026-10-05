/**
 * alasio, end to end, as its operator runs it: installed and started by its command line
 * from its npm package (./harness.ts), driven through the Telegram stand-in as its
 * operator drives it, its sessions' confinement checked from inside them, its own code
 * paths run in its pod with its ServiceAccount, its telemetry looked for where the
 * installation's goes, its workspaces' JuiceFS put through file operations, crashes and a
 * restore (./workspace-storage.ts), its Neon through crashes and losses (neon/test/stack.ts), and
 * removed, with all it keeps, by its command line at the end.
 *
 *   ALASIO_E2E_AGENTS=2 npm run test:e2e
 */
import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { after, afterEach, before, describe, test } from "node:test";

import type { InlineKeyboardButton } from "@grammyjs/types";
import type { V1Pod } from "@kubernetes/client-node";

import { NAMESPACE, RELEASE } from "../../cli/src/manifests/common.ts";
import { neonStack } from "../../neon/test/stack.ts";
import type { NetMode } from "../../src/sandbox/index.ts";
import type { FolderBaymaSeen } from "./folder-bayma.ts";
import {
  AGENTS,
  alasio,
  alasioOk,
  CLUSTER,
  dumpClusterState,
  type Forward,
  HOST_PROFILE,
  HOST_USER,
  inAlasio,
  inAlasioContainer,
  inSession,
  KEEP,
  kube,
  paths,
  ref,
  SESSIONS,
  setUp,
  tcp,
  tearDown,
} from "./harness.ts";
import type { ListedExport } from "./otlp-sink.ts";
import type { RoundtripSeen } from "./session-roundtrip.ts";
import { OTLP, STAND_INS, type StandIn, TELEGRAM } from "./stand-ins.ts";
import type { CallsListing, ControlCallback, ControlMessage, RecordedCall } from "./telegram-stub.ts";
import { workspaceStorage } from "./workspace-storage.ts";

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

// What the cluster was doing is said as something of the run fails, while the cluster is
// as it failed it: before the tests that follow change it, or remove it.
before(async () => {
  try {
    await setUp();
  } catch (error) {
    await dumpClusterState();
    throw error;
  }
}, { timeout: 120 * 60_000 });

afterEach(async (t) => {
  // Node says on a test's context whether it passed, once it has run.
  if ("passed" in t && t.passed === false) await dumpClusterState();
});

after(async () => {
  await tearDown();
}, { timeout: 30 * 60_000 });

/** A local port forwarded to the stand-in's Service, once the stand-in answers there. */
async function forward(standIn: StandIn): Promise<Forward> {
  const forwarded = await kube.forward(STAND_INS, standIn.service, standIn.port);
  for (let attempt = 0; attempt < 60; attempt++) {
    if (await fetch(`${forwarded.base}${standIn.ready}`).then(() => true, () => false)) return forwarded;
    await sleep(500);
  }
  forwarded.close();
  throw new Error(`the ${standIn.name} stand-in did not answer`);
}

/** The operator, through the Telegram stand-in: says things, presses buttons, reads replies. */
function operator(base: string) {
  let seen = 0;
  const post = async (path: string, body: ControlMessage | ControlCallback) => (await fetch(`${base}${path}`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) })).json();
  const calls = async () => {
    // The stand-in answers what it was called with.
    const listed = (await (await fetch(`${base}/control/calls?since=${seen}`)).json()) as CallsListing;
    seen = listed.total;
    return listed.calls;
  };
  const buttons = (call: RecordedCall): InlineKeyboardButton[] => {
    const markup = call.payload.reply_markup;
    return (markup && "inline_keyboard" in markup ? markup.inline_keyboard : []).flat();
  };
  return {
    buttons,
    say: (text: string) => post("/control/message", { text }),
    press: (data: string | undefined) => post("/control/callback", { data }),
    calls,
    /** What alasio sends until `predicate` holds for one, which is returned. */
    async waitFor(predicate: (call: RecordedCall) => boolean, timeoutMs = 300_000): Promise<RecordedCall> {
      const deadline = Date.now() + timeoutMs;
      const got: RecordedCall[] = [];
      while (Date.now() < deadline) {
        got.push(...(await calls()));
        const hit = got.find(predicate);
        if (hit) return hit;
        await sleep(500);
      }
      throw new Error(`alasio sent nothing that matched; it sent ${JSON.stringify(got.map((call) => [call.method, call.payload.text?.slice(0, 120)]))}`);
    },
    /** Presses the button labelled like `label` in what alasio sends next. */
    async choose(label: RegExp) {
      const call = await this.waitFor((candidate) => buttons(candidate).some((button) => label.test(button.text)));
      const button = buttons(call).find((candidate) => label.test(candidate.text));
      await this.press(button && "callback_data" in button ? button.callback_data : undefined);
    },
  };
}

describe("alasio's command line, on the alasio it installed", () => {
  test("status says the cluster's nodes run, and each of alasio's workloads is ready", async () => {
    const said = await alasioOk("status");
    const nodes = [`${CLUSTER}-server-0`, ...Array.from({ length: AGENTS }, (_, index) => `${CLUSTER}-agent-${index}`)];
    assert.match(said, new RegExp(`^cluster ${CLUSTER}, in Docker `, "u"));
    for (const node of nodes) assert.ok(said.includes(`\n  ${node}: running\n`), said);
    assert.ok(said.includes(`  Deployment ${NAMESPACE}/${RELEASE}: ready\n`), said);
    assert.ok(said.includes(`  StatefulSet ${NAMESPACE}/${RELEASE}-neon-pageserver: ready\n`), said);
  });

  test("logs shows what alasio and its components log, and names those it has when asked for another", async () => {
    assert.notEqual((await alasioOk("logs")).trim(), "");
    await alasioOk("logs", "lake", "--since", "1h");
    const unknown = await alasio("logs", "nothing");
    assert.equal(unknown.code, 1);
    assert.match(unknown.stderr, /^alasio: alasio has no component nothing; it has .*\blake\b/mu);
  });

  test("lake answers a query of the analytics lake, in the format asked for", async () => {
    assert.match(await alasioOk("lake", "SELECT 42 AS answer"), /^answer\n-+\n42\n\(1 rows\)$/mu);
    assert.equal((await alasioOk("lake", "--format", "json", "SELECT 42 AS answer")).trim(), '{"answer":42}');
  });
});

let telegram: Forward | undefined;
let sink: Forward | undefined;
let tg: ReturnType<typeof operator>;

/** Creates a session filesystem through /workspace, as the operator does: its volume id. */
async function newSession(net: NetMode) {
  await tg.calls();
  await tg.say("/workspace");
  await tg.choose(/New empty workspace/u);
  await tg.choose(net === "full" ? /Full internet/u : /No internet/u);
  const created = await tg.waitFor((call) => /Created and mounted empty workspace/u.test(call.payload.text ?? ""));
  const volumeId = /fs-[0-9a-f]+/u.exec(created.payload.text ?? "")?.[0];
  assert.ok(volumeId, `alasio named no workspace: ${created.payload.text}`);
  return volumeId;
}

const probe = (target: string) => `fetch(${JSON.stringify(target)},{signal:AbortSignal.timeout(5000)}).then(r=>console.log("open"),e=>console.log("blocked"))`;

const roundtrip = (volumeId: string) => inAlasio<RoundtripSeen>("./session-roundtrip.ts", volumeId);

describe("alasio on Kubernetes", () => {
  let none: string;
  let full: string;

  before(async () => {
    telegram = await forward(TELEGRAM);
    sink = await forward(OTLP);
    tg = operator(telegram.base);
    await tg.waitFor((call) => call.method === "setMyCommands", 300_000).catch(() => {});
  });

  after(() => {
    telegram?.close();
    sink?.close();
  });

  test("a new empty workspace without internet is a session of its own, confined", async () => {
    none = await newSession("none");
    const pod = await kube.get<V1Pod>(ref("Pod", none, SESSIONS));
    if (pod?.spec?.runtimeClassName === "gvisor") assert.match(await inSession(none, 'console.log(require("fs").readFileSync("/proc/version","utf8"))'), /gvisor/u);
    assert.equal((await inSession(none, probe("http://1.1.1.1"))).trim(), "blocked");
    assert.equal((await inSession(none, tcp("process.env.KUBERNETES_SERVICE_HOST", 443))).trim(), "blocked");
    assert.match(await inSession(none, 'require("dns").promises.lookup("example.com").then(()=>console.log("resolved"),()=>console.log("no dns"))'), /no dns/u);
    assert.match(await inSession(none, 'console.log(require("fs").existsSync("/var/run/secrets/kubernetes.io/serviceaccount"))'), /false/u);
    const gate = (await kube.get<V1Pod>(ref("Pod", none, SESSIONS)))?.status?.initContainerStatuses?.find(({ name }) => name === "egress-gate");
    assert.equal(gate?.state?.terminated?.exitCode, 0);
  });

  test("a new workspace with internet reaches the internet and nothing private", async () => {
    full = await newSession("full");
    assert.equal((await inSession(full, probe("https://example.com"))).trim(), "open");
    assert.equal((await inSession(full, tcp("process.env.KUBERNETES_SERVICE_HOST", 443))).trim(), "blocked");
    const other = (await kube.get<V1Pod>(ref("Pod", none, SESSIONS)))?.status?.podIP;
    assert.ok(other, `the session ${none} has no address`);
    assert.equal((await inSession(full, probe(`http://${other}:7290/mcp`))).trim(), "blocked");
    assert.match(await inSession(full, `require("dns").promises.lookup("${none}.${SESSIONS}.svc.cluster.local").then(()=>console.log("resolved"),()=>console.log("unresolved"))`), /unresolved/u);
  });

  test("a session's bayma answers alasio alone, and only with the session's token", async () => {
    const url = `http://${none}.${SESSIONS}.svc.cluster.local:7290/mcp`;
    const token = await kube.secret(SESSIONS, `${none}-bayma-token`, "token");
    const fromAlasio = async (headers: string[]) => (await inAlasioContainer(["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}", "--max-time", "5", ...headers, url])).trim();
    assert.equal(await fromAlasio([]), "401");
    assert.equal(await fromAlasio(["-H", "Authorization: Bearer wrong"]), "401");
    assert.equal(await fromAlasio(["-H", `Authorization: Bearer ${token}`]), "400");
    const stub = await kube.runningPod(STAND_INS, TELEGRAM.name);
    const fromStub = (await kube.execOk(STAND_INS, stub, ["node", "-e", probe(url)], { container: TELEGRAM.container })).trim();
    assert.equal(fromStub, "blocked");
  });

  test("alasio reads a session's files as its agent, a suspended session resumes with them, and one of another pod template moves onto it with them", async () => {
    const seen = await roundtrip(none);
    assert.equal(seen.exec, '"written"');
    assert.match(seen.read ?? "", /^hello from /u);
    assert.equal(seen.missing, "file not found");
    assert.equal(seen.suspendedPod, "gone");
    assert.equal(seen.whileSuspended, "the session is not running");
    assert.equal(seen.afterResume, seen.read);
    assert.equal(seen.execAfterResume, JSON.stringify(seen.read));
    assert.equal(seen.podReplaced, true);
    assert.equal(seen.movedPodLabel, "yes");
    assert.equal(seen.execAfterMove, JSON.stringify(seen.read));
  });

  test("a session's telemetry reaches the installation's backend, stamped with the session", async () => {
    assert.ok(sink, "the OTLP stand-in did not answer");
    const deadline = Date.now() + 120_000;
    let stamped: ListedExport[] = [];
    while (Date.now() < deadline) {
      // The sink answers what it received.
      const listed = (await (await fetch(`${sink.base}/control/exports?contains=${none}`)).json()) as ListedExport[];
      stamped = listed.filter((entry) => entry.contains);
      if (stamped.some((entry) => entry.signal === "traces")) break;
      await sleep(3000);
    }
    assert.ok(stamped.some((entry) => entry.signal === "traces"), "no trace of the session's bayma arrived stamped with it");
  });

  test("the stack's collector sends JuiceFS's metrics and Valkey's to the installation's backend", async () => {
    assert.ok(sink, "the OTLP stand-in did not answer");
    // JuiceFS's are its own Prometheus metrics, scraped; Valkey's, the collector's redis receiver's.
    const names = ["juicefs_", "redis.memory.used"];
    const deadline = Date.now() + 180_000;
    let missing = names;
    while (Date.now() < deadline) {
      const arrived = await Promise.all(missing.map(async (name) => {
        // The sink answers what it received.
        const listed = (await (await fetch(`${sink?.base}/control/exports?contains=${encodeURIComponent(name)}`)).json()) as ListedExport[];
        return listed.some((entry) => entry.signal === "metrics" && entry.contains);
      }));
      missing = missing.filter((_, index) => !arrived[index]);
      if (missing.length === 0) break;
      await sleep(10_000);
    }
    assert.deepEqual(missing, [], "no metric of these names arrived");
  });

  test("a folder conversation's bayma works on the machine as the operator, in their home", { skip: !HOST_PROFILE && "folder workspaces are for a single node" }, async () => {
    const { home } = paths();
    const { url, seen } = await inAlasio<FolderBaymaSeen>("./folder-bayma.ts");
    assert.match(url, /^http:\/\/bayma-[0-9a-f]{20}\.alasio-host\.svc/u);
    assert.deepEqual(seen, { uid: HOST_USER, home });
    // The operator's home is the machine's, shared between alasio and the folder's bayma.
    assert.equal(readFileSync(`${home}/e2e-folder-proof`, "utf8"), `written by ${HOST_USER}`);
    assert.equal(await inAlasioContainer(["cat", `${home}/e2e-folder-proof`]), `written by ${HOST_USER}`);
  });

  test("alasio comes back from alasio restart with its conversation's workspace", async () => {
    assert.equal(await alasioOk("restart"), "alasio restarted; a turn it was running continues.\n");
    await tg.waitFor((call) => call.method === "setMyCommands", 300_000);
    await tg.say("/workspace");
    const panel = await tg.waitFor((call) => /Folder: sessionfs:/u.test(call.payload.text ?? ""));
    assert.match(panel.payload.text ?? "", new RegExp(`sessionfs:${full}`, "u"));
  });
});

workspaceStorage();

neonStack();

describe("alasio uninstall --purge", { skip: KEEP && "the run keeps the cluster" }, () => {
  test("removes alasio and the cluster on this machine, with all they keep, and keeps the config", async () => {
    const { configFile, kubeconfig, storage } = paths();
    assert.equal(await alasioOk("uninstall", "--purge", "--yes"), `alasio and the cluster ${CLUSTER} are removed, with all their data; the config at ${configFile} is kept.\n`);
    assert.ok(!existsSync(storage), `${storage} is left`);
    assert.ok(!existsSync(kubeconfig), `${kubeconfig} is left`);
    assert.ok(existsSync(configFile));
    const status = await alasio("status");
    assert.equal(status.code, 1);
    assert.match(status.stdout, new RegExp(`^cluster ${CLUSTER}, in Docker .*:\n {2}not made\n$`, "u"));
    assert.equal(status.stderr, `alasio: there is no cluster ${CLUSTER} yet: alasio up makes it\n`);
  });
});
