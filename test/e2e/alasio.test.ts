/**
 * alasio, end to end, as its operator runs it: installed and started by its command line
 * from its npm package (./harness.ts), driven through the Telegram stand-in as its
 * operator drives it, its sessions' confinement checked from inside them, its own code
 * paths run in its pod with its ServiceAccount, its telemetry looked for in its lake and
 * where the installation's goes, its workspaces' JuiceFS put through file operations, crashes and a
 * restore (./workspace-storage.ts), its Neon through crashes and losses (neon/test/stack.ts), and
 * removed, with all it keeps, by its command line at the end.
 *
 *   ALASIO_E2E_AGENTS=2 npm run test:e2e
 */
import assert from "node:assert/strict";
import { existsSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, afterEach, before, describe, test } from "node:test";

import type { InlineKeyboardButton } from "@grammyjs/types";
import type { CoreV1Event, KubernetesObject, V1Deployment, V1PersistentVolume, V1Pod } from "@kubernetes/client-node";

import { NAMESPACE, RELEASE } from "../../cli/src/manifests/common.ts";
import { neonStack } from "../../neon/test/stack.ts";
import type { NetMode } from "../../src/sandbox/index.ts";
import { ANSWER } from "./codex-stand-in.ts";
import type { FolderBaymaSeen } from "./folder-bayma.ts";
import {
  AGENTS,
  alasio,
  alasioOk,
  alasioOkWith,
  alasioRunning,
  CLUSTER,
  dumpClusterState,
  type Forward,
  HOST_PROFILE,
  HOST_USER,
  inAlasio,
  inAlasioContainer,
  inSession,
  inShard,
  KEEP,
  kube,
  lakeQuery,
  onNode,
  OTHER_OPERATOR,
  paths,
  ref,
  SESSIONS,
  setUp,
  tcp,
  TARGET,
  tearDown,
} from "./harness.ts";
import type { ListedExport } from "./otlp-sink.ts";
import type { RoundtripSeen } from "./session-roundtrip.ts";
import { OTLP, STAND_INS, type StandIn, TELEGRAM, TELEGRAM_BRANCH, urlOf } from "./stand-ins.ts";
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
    /** Says `text` as the operator, or as `user`, another the bot is for, each in their own chat. */
    say: (text: string, user?: number) => post("/control/message", { text, ...(user === undefined ? {} : { chatId: user, userId: user }) }),
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
    if (TARGET === "host") {
      assert.match(said, /^k3s v\S+ on this machine, with gVisor \S+:\n {2}service k3s: active, enabled\n {2}node \S+: ready\n/u);
    } else {
      assert.match(said, new RegExp(`^cluster ${CLUSTER}, in Docker `, "u"));
      const nodes = [`${CLUSTER}-server-0`, ...Array.from({ length: AGENTS }, (_, index) => `${CLUSTER}-agent-${index}`)];
      for (const node of nodes) assert.ok(said.includes(`\n  ${node}: running\n`), said);
    }
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
  const created = await tg.waitFor((call) => /Created and mounted session workspace/u.test(call.payload.text ?? ""));
  const volumeId = /fs-[0-9a-f]+/u.exec(created.payload.text ?? "")?.[0];
  assert.ok(volumeId, `alasio named no workspace: ${created.payload.text}`);
  return volumeId;
}

const probe = (target: string) => `fetch(${JSON.stringify(target)},{signal:AbortSignal.timeout(5000)}).then(r=>console.log("open"),e=>console.log("blocked"))`;

const roundtrip = (volumeId: string) => inAlasio<RoundtripSeen>("./session-roundtrip.ts", volumeId);

if (inShard("sessions")) {
  describe("alasio on Kubernetes", () => {
    let none: string;
    let full: string;

    before(async () => {
      telegram = await forward(TELEGRAM);
      tg = operator(telegram.base);
      await tg.waitFor((call) => call.method === "setMyCommands", 300_000).catch(() => {});
    });

    after(() => {
      telegram?.close();
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

    test("a folder conversation's bayma works on the machine as the operator, in their home", { skip: !HOST_PROFILE && "folder workspaces are for a single node" }, async () => {
      const { home } = paths();
      const { url, seen } = await inAlasio<FolderBaymaSeen>("./folder-bayma.ts");
      assert.match(url, /^http:\/\/bayma-[0-9a-f]{20}\.alasio-host\.svc/u);
      assert.deepEqual(seen, { uid: HOST_USER, home });
      // The operator's home is the machine's, shared between alasio and the folder's bayma.
      assert.equal(readFileSync(`${home}/e2e-folder-proof`, "utf8"), `written by ${HOST_USER}`);
      assert.equal(await inAlasioContainer(["cat", `${home}/e2e-folder-proof`]), `written by ${HOST_USER}`);
    });

    test("a message runs a turn on Codex, the run's stand-in for it, whose answer reaches the operator, and its spans the lake", async () => {
      await tg.calls();
      await tg.say("/service codex");
      await tg.waitFor((call) => /^Active: Codex$/mu.test(call.payload.text ?? ""));
      await tg.say("hello");
      await tg.waitFor((call) => call.method === "sendRichMessage" && call.payload.rich_message?.markdown === ANSWER);
      await untilInLake("span of Codex's app-server", "select count(*) as n from otel.traces where ServiceName = 'codex-app-server' and SpanName = 'turn/start'");
    });

    test("alasio comes back from alasio restart with its conversation's workspace", async () => {
      assert.equal(await alasioOk("restart"), "alasio restarted; a turn it was running continues.\n");
      await tg.waitFor((call) => call.method === "setMyCommands", 300_000);
      await tg.say("/workspace");
      const panel = await tg.waitFor((call) => /^Session workspace: /mu.test(call.payload.text ?? ""));
      assert.match(panel.payload.text ?? "", new RegExp(`^Session workspace: ${full}, full internet$`, "mu"));
    });

    test("alasio's pod, deleted, is rescheduled on no volume, and its conversation goes on: its mount, the buttons it had sent, and its session", async () => {
      const volumes = (await kube.get<V1Deployment>(ref("Deployment", RELEASE, NAMESPACE)))?.spec?.template.spec?.volumes ?? [];
      assert.deepEqual(volumes.filter((volume) => volume.persistentVolumeClaim), []);
      await tg.say("/workspace");
      const panel = await tg.waitFor((call) => /^Session workspace: /mu.test(call.payload.text ?? ""));
      const refresh = tg.buttons(panel).find((button) => button.text === "Refresh");
      assert.ok(refresh && "callback_data" in refresh, "the panel has a Refresh button");

      const before = await kube.runningPod(NAMESPACE, RELEASE);
      await kube.remove(ref("Pod", before, NAMESPACE));
      await tg.waitFor((call) => call.method === "setMyCommands", 300_000);
      assert.notEqual(await kube.runningPod(NAMESPACE, RELEASE), before);

      // A button sent by the pod that is gone acts in the one that replaced it.
      await tg.press(refresh.callback_data);
      const refreshed = await tg.waitFor((call) => call.method === "editMessageText" && /^Session workspace: /mu.test(call.payload.text ?? ""));
      assert.match(refreshed.payload.text ?? "", new RegExp(`^Session workspace: ${full}, full internet$`, "mu"));

      // The next message goes on in the session the conversation had, which the new pod's
      // Codex, not having run it, is asked to resume.
      await tg.say("again");
      await tg.waitFor((call) => call.method === "sendRichMessage" && call.payload.rich_message?.markdown === ANSWER);
      await untilInLake("resumed thread of Codex's", "select count(*) as n from otel.traces where ServiceName = 'codex-app-server' and SpanName = 'thread/resume'");
    });
  });
}

if (inShard("workspaces")) {
  describe("a session workspace forked from Telegram", () => {
    before(async () => {
      telegram = await forward(TELEGRAM);
      tg = operator(telegram.base);
      await tg.waitFor((call) => call.method === "setMyCommands", 300_000).catch(() => {});
    });

    after(() => {
      telegram?.close();
    });

    test("Fork in the workspace panel switches the conversation to a clone of its session workspace, where a turn runs in a session of its own, the source untouched", async () => {
      const source = await newSession("none");
      const inWorkspace = (volumeId: string, script: string) => kube.execOk(SESSIONS, volumeId, ["sh", "-c", script], { container: "bayma" });
      await inWorkspace(source, "echo from the source > /workspace/kept");
      await tg.say("/service codex");
      await tg.waitFor((call) => /^Active: Codex$/mu.test(call.payload.text ?? ""));
      await tg.say("hello from the source");
      await tg.waitFor((call) => call.method === "sendRichMessage" && call.payload.rich_message?.markdown === ANSWER);

      await tg.calls();
      await tg.say("/workspace");
      await tg.choose(/^Fork this workspace$/u);
      // Said, and then on the panel, which shows the fork mounted.
      const panel = await tg.waitFor((call) => call.method === "editMessageText" && /^Forked session workspace /mu.test(call.payload.text ?? ""));
      const fork = new RegExp(`^Forked session workspace ${source} into session workspace (fs-[0-9a-f]+) and switched to it`, "mu").exec(panel.payload.text ?? "")?.[1];
      assert.ok(fork, panel.payload.text);
      assert.match(panel.payload.text ?? "", new RegExp(`^Session workspace: ${fork}, no internet, fork of ${source}$`, "mu"));
      // The fork waits, suspended, for its first turn; the source runs again.
      assert.equal(await kube.get<V1Pod>(ref("Pod", fork, SESSIONS)), null);
      assert.ok(await kube.get<V1Pod>(ref("Pod", source, SESSIONS)));

      await tg.say("hello from the fork");
      await tg.waitFor((call) => call.method === "sendRichMessage" && call.payload.rich_message?.markdown === ANSWER);
      assert.equal(await inWorkspace(fork, "cat /workspace/kept"), "from the source\n");
      await inWorkspace(fork, "echo from the fork > /workspace/kept");
      assert.equal(await inWorkspace(source, "cat /workspace/kept"), "from the source\n");

      // The source is still listed, and switching back to it restores its session.
      await tg.calls();
      await tg.say("/workspace");
      await tg.choose(new RegExp(`^${source}$`, "u"));
      await tg.waitFor((call) => new RegExp(`^Switched to session workspace ${source} `, "mu").test(call.payload.text ?? ""));
    });
  });

  workspaceStorage();
}

/** How many rows of the lake's `otel` metric tables `condition` selects, as SQL. */
const metricRows = (condition: string) =>
  `select sum(n) as n from (${["gauge", "sum", "histogram", "exponential_histogram", "summary"].map((kind) => `select count(*) as n from otel.metrics_${kind} where ${condition}`).join(" union all ")})`;

/** Waits until `sql`, a count as `n`, counts some of the lake's rows, as the collector sends them within seconds. */
async function untilInLake(what: string, sql: string): Promise<void> {
  const deadline = Date.now() + 240_000;
  let last: unknown;
  while (Date.now() < deadline) {
    last = await lakeQuery(sql).then(([row]) => Number(row?.["n"] ?? 0), (error: unknown) => error);
    if (typeof last === "number" && last > 0) return;
    await sleep(5000);
  }
  assert.fail(`no ${what} arrived in the lake: ${last instanceof Error ? last.message : `${last} rows`}`);
}

/** Waits until the stand-in backend has received an export of `signal` that contains `needle`. */
async function untilExported(signal: string, needle: string, timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    // The sink answers what it received.
    const listed = (await (await fetch(`${sink?.base}/control/exports?contains=${encodeURIComponent(needle)}`)).json()) as ListedExport[];
    if (listed.some((entry) => entry.signal === signal && entry.contains)) return true;
    await sleep(5000);
  }
  return false;
}

if (inShard("telemetry")) {
  describe("alasio's telemetry", () => {
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

    test("alasio's own traces, logs and metrics are in the lake, queryable through alasio lake", async () => {
      await untilInLake("trace of alasio's", "select count(*) as n from otel.traces where ServiceName = 'alasio'");
      await untilInLake("log record of alasio's", "select count(*) as n from otel.logs where ServiceName = 'alasio'");
      await untilInLake("metric of alasio's", metricRows("ServiceName = 'alasio'"));
    });

    test("a session's bayma's telemetry is in the lake, stamped with its session, and reaches the installation's backend too", async () => {
      const volumeId = await newSession("none");
      await untilInLake("trace of the session's bayma", `select count(*) as n from otel.traces where ServiceName = 'bayma' and ResourceAttributes['alasio.volume.id'] = '${volumeId}'`);
      assert.ok(await untilExported("traces", volumeId, 120_000), "no trace of the session's bayma arrived at the backend stamped with it");
    });

    test("the stack's telemetry is in the lake, the compute's and the lake's own among it", async () => {
      // What the collector scrapes is named for its job; the lake exports its events itself.
      await untilInLake("metric of the compute's", metricRows("ServiceName = 'compute'"));
      await untilInLake("log record of the lake's", "select count(*) as n from otel.logs where ServiceName = 'alasio-lake'");
      await untilInLake("metric of JuiceFS's", metricRows("MetricName like 'juicefs_%'"));
      await untilInLake("metric of Valkey's", metricRows("MetricName = 'redis.memory.used'"));
    });

    describe("Grafana", () => {
      let grafana: Awaited<ReturnType<typeof alasioRunning>> | undefined;
      let base = "";
      let authorization = "";
      /** Grafana's API, as its admin. */
      const api = async (path: string, init: RequestInit = {}): Promise<{ status: number; body: unknown }> => {
        const response = await fetch(`${base}${path}`, { ...init, headers: { authorization, "content-type": "application/json", ...init.headers } });
        const text = await response.text();
        return { status: response.status, body: text.startsWith("{") || text.startsWith("[") ? JSON.parse(text) : text };
      };

      before(async () => {
        const password = (await alasioOk("grafana", "--password")).trim();
        authorization = `Basic ${Buffer.from(`admin:${password}`).toString("base64")}`;
        grafana = await alasioRunning("grafana", "--port", "0");
        base = /http:\/\/127\.0\.0\.1:\d+/u.exec(grafana.said)?.[0] ?? "";
        assert.ok(base, grafana.said);
      });

      after(() => {
        grafana?.child.kill();
      });

      test("alasio grafana reaches Grafana, its data source reads the lake, and its dashboards' queries answer with the lake's rows", async (t) => {
        assert.equal((await api("/api/health")).status, 200);
        assert.deepEqual((await api("/api/datasources/uid/lake/health")).body, { message: "Health check successful", status: "OK" });
        const dashboards = (await api("/api/search?type=dash-db")).body as { title: string; folderTitle: string }[];
        assert.deepEqual(dashboards.map(({ folderTitle, title }) => `${folderTitle}/${title}`).sort(), ["alasio/Agents", "alasio/Conversations", "alasio/Stack health"]);
        // The Stack health dashboard's table of services, as its panel queries it.
        const stack: { panels: { title: string; targets?: object[] }[] } = JSON.parse(readFileSync(new URL("../../neon/grafana/dashboards/stack-health.json", import.meta.url), "utf8"));
        const services = stack.panels.find(({ title }) => title === "Services now")?.targets?.[0];
        assert.ok(services);
        let rows = 0;
        for (const deadline = Date.now() + 180_000; rows === 0 && Date.now() < deadline; await sleep(5000)) {
          const answered = (await api("/api/ds/query", { method: "POST", body: JSON.stringify({ from: "now-1h", to: "now", queries: [services] }) })).body as {
            results: { A: { frames?: { data: { values: unknown[][] } }[] } };
          };
          rows = answered.results.A.frames?.[0]?.data.values[0]?.length ?? 0;
        }
        assert.ok(rows > 0, "the panel answered with no service");
        // A query the lake refuses says why, in Grafana.
        const refused = (await api("/api/ds/query", { method: "POST", body: JSON.stringify({ from: "now-1h", to: "now", queries: [{ ...services, url_options: { ...(services as { url_options: object }).url_options, data: "select nothing" } }] }) })).body as {
          results: { A: { error?: string } };
        };
        assert.match(refused.results.A.error ?? "", /Binder Error: Referenced column "nothing" was not found/u);
        // Started once its database was ready for it, on a fresh install: never restarted for want of it.
        const pod = await kube.get<V1Pod>(ref("Pod", await kube.runningPod(NAMESPACE, "alasio-grafana"), NAMESPACE));
        assert.deepEqual(pod?.status?.containerStatuses?.map(({ name, restartCount }) => [name, restartCount]), [["grafana", 0]]);
        const backOffs = (await kube.list<CoreV1Event>("Event", { namespace: NAMESPACE })).filter(({ involvedObject, reason }) => involvedObject.name?.startsWith("alasio-grafana-") && reason === "BackOff");
        assert.deepEqual(backOffs.map(({ message }) => message), []);
        // What Grafana and the lake's query endpoint hold, as their cgroups count it.
        const memory = async (pod: string, container: string) =>
          (await kube.execOk(NAMESPACE, pod, ["cat", "/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/memory.peak"], { container })).trim().split("\n").map((bytes) => `${Math.round(Number(bytes) / 1048576)} MiB`).join(", peak ");
        t.diagnostic(`Grafana: ${await memory(await kube.runningPod(NAMESPACE, "alasio-grafana"), "grafana")}`);
        t.diagnostic(`the lake's query endpoint: ${await memory(await kube.runningPod(NAMESPACE, "alasio-lake"), "query")}`);
        t.diagnostic(`the lake service: ${await memory(await kube.runningPod(NAMESPACE, "alasio-lake"), "lake")}`);
      });

      test("a turn that failed alerts each of the bot's users, through the bot", async () => {
        // A turn that failed, as alasio records one, sent to the stack's collector as alasio sends its spans.
        const now = BigInt(Date.now()) * 1_000_000n;
        const attribute = (key: string, value: string) => ({ key, value: { stringValue: value } });
        const turn = {
          resourceSpans: [{
            resource: { attributes: [attribute("service.name", "alasio")] },
            scopeSpans: [{
              scope: { name: "alasio" },
              spans: [{
                traceId: "4bf92f3577b34da6a3ce929d0e0e4736",
                spanId: "00f067aa0ba902b7",
                name: "alasio.turn",
                kind: 1,
                startTimeUnixNano: String(now - 2_000_000_000n),
                endTimeUnixNano: String(now),
                attributes: [attribute("alasio.conversation.id", "telegram:1001"), attribute("alasio.harness", "codex"), attribute("alasio.turn.outcome", "failed")],
                status: { code: 2, message: "the harness failed" },
              }],
            }],
          }],
        };
        await inAlasioContainer(["curl", "-fsS", "-X", "POST", "-H", "content-type: application/json", "--data-binary", "@-", "http://alasio-collector.alasio.svc:4318/v1/traces"], JSON.stringify(turn));
        // Each user's alert, by the chat it went to, until both users have theirs.
        const alerts = new Map<string, string>();
        await tg.waitFor((call) => {
          if (call.method === "sendMessage" && /Turns are failing/u.test(call.payload.text ?? "")) alerts.set(String(call.payload.chat_id), call.payload.text ?? "");
          return alerts.size === 2;
        }, 420_000);
        assert.deepEqual([...alerts.keys()].sort(), ["1001", String(OTHER_OPERATOR)]);
        for (const text of alerts.values()) assert.match(text, /conversation = telegram:1001/u);
        const rules = (await api("/api/prometheus/grafana/api/v1/rules")).body as { data: { groups: { rules: { name: string; state: string }[] }[] } };
        assert.equal(rules.data.groups.flatMap((group) => group.rules).find(({ name }) => name === "Turns are failing")?.state, "firing");
      });
    });

    test("the stack's collector sends JuiceFS's metrics and Valkey's to the installation's backend too", async () => {
      // JuiceFS's are its own Prometheus metrics, scraped from its clients and from its
      // driver, whose provisioning errors only its controller counts; Valkey's, the
      // collector's redis receiver's.
      const names = ["juicefs_", "juicefs_provision_errors", "redis.memory.used"];
      const arrived = await Promise.all(names.map((name) => untilExported("metrics", name, 180_000)));
      assert.deepEqual(names.filter((_, index) => !arrived[index]), [], "no metric of these names arrived");
    });
  });
}

if (inShard("neon")) neonStack();

/** The branch environment the run makes, and its namespaces. */
const BRANCH = "e2e";
const BRANCH_NAMESPACE = `alasio-branch-${BRANCH}`;
const BRANCH_SESSIONS = `${BRANCH_NAMESPACE}-sessions`;
/** The chat, another user's, that main mounts on a folder. */
const FOLDER_CHAT = OTHER_OPERATOR;

/** Runs `sql` on alasio's database in `namespace` (main's, or a branch's), as the compute's own superuser, in its pod: what psql printed. */
async function onDatabase(namespace: string, sql: string): Promise<string> {
  const pod = await kube.runningPod(namespace, "alasio-neon-compute");
  return (await kube.execOk(namespace, pod, ["psql", "-h", "127.0.0.1", "-p", "55433", "-U", "cloud_admin", "-d", "alasio", "-Atc", sql], { container: "compute" })).trim();
}

/** Calls neon-control's API in its pod with the stack's admin token: what it answered. */
async function neonControl(path: string): Promise<string> {
  const pod = await kube.runningPod(NAMESPACE, "alasio-neon-control");
  const token = await kube.secret(NAMESPACE, "alasio-branches", "control-token");
  return kube.execOk(NAMESPACE, pod, ["curl", "-sS", "-H", "@-", `http://127.0.0.1:8080${path}`], { container: "neon-control", stdin: `authorization: Bearer ${token}\n` });
}

/** Waits until main's lake logs a maintenance pass that `passes` holds for, after those it logged before. */
async function untilMaintained(what: string, passes: (line: string) => boolean): Promise<void> {
  const pod = await kube.runningPod(NAMESPACE, "alasio-lake");
  const seen = (await kube.logs(NAMESPACE, pod, "lake")).split("\n").length;
  const deadline = Date.now() + 240_000;
  while (Date.now() < deadline) {
    const lines = (await kube.logs(NAMESPACE, pod, "lake")).split("\n").slice(seen);
    if (lines.some((line) => line.includes('"message":"maintained"') && passes(line))) return;
    await sleep(5000);
  }
  assert.fail(`main's lake logged no maintenance that ${what}`);
}

if (inShard("branches")) {
  describe("branch environments", () => {
    let branchTelegram: Forward | undefined;
    let branch: ReturnType<typeof operator>;
    let volumeId: string;

    before(async () => {
      telegram = await forward(TELEGRAM);
      branchTelegram = await forward(TELEGRAM_BRANCH);
      tg = operator(telegram.base);
      branch = operator(branchTelegram.base);
      await tg.waitFor((call) => call.method === "setMyCommands", 300_000).catch(() => {});
      // Main's lake loaded and maintained every few seconds, so the run sees its passes.
      const lake = await kube.get<V1Deployment>(ref("Deployment", "alasio-lake", NAMESPACE));
      const containers = lake?.spec?.template.spec?.containers ?? [];
      assert.ok(containers.some(({ name }) => name === "lake"));
      const often = [{ name: "LAKE_INTERVAL_SECONDS", value: "10" }, { name: "LAKE_MAINTENANCE_HOURS", value: "0.002" }];
      // A merge patch replaces the list whole: the query endpoint's container is given back as it was.
      const patched = containers.map((container) => (container.name === "lake" ? { ...container, env: [...(container.env ?? []), ...often] } : container));
      await kube.patch(ref("Deployment", "alasio-lake", NAMESPACE), { spec: { template: { spec: { containers: patched } } } });
      await kube.awaitReady([ref("Deployment", "alasio-lake", NAMESPACE)], "5 minutes");
    });

    after(() => {
      telegram?.close();
      branchTelegram?.close();
    });

    test("main works in a session workspace on Codex, and in a folder in another chat, which the branch inherits", async () => {
      volumeId = await newSession("none");
      await inSession(volumeId, 'require("fs").writeFileSync("/workspace/proof", "written by main")');
      await tg.say("/service codex");
      await tg.waitFor((call) => /^Active: Codex$/mu.test(call.payload.text ?? ""));
      await tg.say("hello");
      await tg.waitFor((call) => call.method === "sendRichMessage" && call.payload.rich_message?.markdown === ANSWER);
      if (HOST_PROFILE) {
        await tg.say("/workspace new e2e-folder", FOLDER_CHAT);
        await tg.waitFor((call) => Number(call.payload.chat_id) === FOLDER_CHAT && /e2e-folder/u.test(call.payload.text ?? ""));
        await tg.say("/service codex", FOLDER_CHAT);
        await tg.waitFor((call) => Number(call.payload.chat_id) === FOLDER_CHAT && /^Active: Codex$/mu.test(call.payload.text ?? ""));
      }
    });

    test("alasio branch create makes the branch, serving its own bot, and main's lake keeps its files while it is", async () => {
      const work = mkdtempSync(join(tmpdir(), "alasio-e2e-branch-"));
      writeFileSync(join(work, "bot-token"), "456:branch", { mode: 0o600 });
      // The branch's alasio talks to its own bot's stand-in, as the command line checks its token with it.
      writeFileSync(join(work, "overrides.json"), JSON.stringify({ alasio: { env: { TELEGRAM_API_ROOT: urlOf(TELEGRAM_BRANCH) } } }));
      const started = Date.now();
      await alasioOkWith({ TELEGRAM_API_ROOT: branchTelegram?.base }, "branch", "create", BRANCH, "--bot-token-file", join(work, "bot-token"), "--overrides", join(work, "overrides.json"));
      console.error(`# the branch ran ${((Date.now() - started) / 1000).toFixed(1)}s after alasio branch create was run`);
      await branch.waitFor((call) => call.method === "setMyCommands", 300_000);
      assert.match(await alasioOk("branch", "list"), new RegExp(`^${BRANCH}: a branch of main, \\d+m old, its alasio ready, `, "mu"));
      await untilMaintained("kept the files for the branch", (line) => line.includes(`"keptFilesFor":["${BRANCH}"]`));
    });

    test("a turn on the branch goes on in main's conversation in a fork of its workspace, which main made, and nothing of main's changes", async () => {
      const messages = await onDatabase(NAMESPACE, "select count(*) from state.messages");
      const mounted = await onDatabase(NAMESPACE, "select working_directory from state.conversations where id = 'telegram:1001'");
      assert.equal(mounted, `sessionfs:${volumeId}`);
      await tg.calls();
      await branch.say("again");
      await branch.waitFor((call) => call.method === "sendRichMessage" && call.payload.rich_message?.markdown === ANSWER);
      // Forked into the branch's own sessions, under the same id, with main's files.
      assert.ok(await kube.get(ref("Sandbox", volumeId, BRANCH_SESSIONS)));
      const inBranch = (code: string) => kube.execOk(BRANCH_SESSIONS, volumeId, ["node", "-e", code], { container: "bayma" });
      assert.equal(await inBranch('process.stdout.write(require("fs").readFileSync("/workspace/proof", "utf8"))'), "written by main");
      await inBranch('require("fs").writeFileSync("/workspace/proof", "written by the branch")');
      assert.equal(await inSession(volumeId, 'process.stdout.write(require("fs").readFileSync("/workspace/proof", "utf8"))'), "written by main");
      // Main's database, and its bot, as they were.
      assert.equal(await onDatabase(NAMESPACE, "select count(*) from state.messages"), messages);
      assert.equal(await onDatabase(NAMESPACE, "select working_directory from state.conversations where id = 'telegram:1001'"), mounted);
      assert.deepEqual((await tg.calls()).filter((call) => call.method !== "getUpdates" && call.method !== "answerCallbackQuery"), []);
      // The branch's database has the turn main's does not.
      assert.ok(Number(await onDatabase(BRANCH_NAMESPACE, "select count(*) from state.messages")) > Number(messages));
    });

    test("the branch's credentials are its own: main's compute takes neither its password nor its compute's token", async () => {
      const passwordOf = async (namespace: string) => decodeURIComponent(new URL(await kube.secret(namespace, "alasio-database", "url")).password);
      const compute = await kube.runningPod(NAMESPACE, "alasio-neon-compute");
      // By the pod's address, as alasio connects, not the loopback compute_ctl trusts.
      const loginWith = async (password: string) => (await kube.execOk(NAMESPACE, compute, [
        "sh",
        "-c",
        'PGPASSWORD="$1" psql -h "$(hostname -i | cut -d" " -f1)" -p 55433 -U alasio -d alasio -Atc "select current_user" 2>/dev/null || echo refused',
        "sh",
        password,
      ])).trim();
      assert.deepEqual([await loginWith(await passwordOf(NAMESPACE)), await loginWith(await passwordOf(BRANCH_NAMESPACE))], ["alasio", "refused"]);
      const control = await kube.runningPod(NAMESPACE, "alasio-neon-control");
      const specStatus = async (id: string, token: string) => (await kube.execOk(NAMESPACE, control, [
        "node",
        "-e",
        'fetch("http://127.0.0.1:8080/compute/api/v2/computes/" + process.argv[1] + "/spec", { headers: { authorization: "Bearer " + process.argv[2] } }).then((r) => process.stdout.write(String(r.status)))',
        "--",
        id,
        token,
      ])).trim();
      const branchToken = await kube.secret(BRANCH_NAMESPACE, "alasio-neon-compute", "NEON_CONTROL_PLANE_TOKEN");
      assert.deepEqual([await specStatus(`branch-${BRANCH}`, branchToken), await specStatus("alasio", branchToken)], ["200", "403"]);
    });

    test("the branch's telemetry is in main's lake, tagged with the branch", async () => {
      await untilInLake("trace of the branch's alasio", `select count(*) as n from otel.traces where ServiceName = 'alasio' and ResourceAttributes['alasio.branch'] = '${BRANCH}'`);
    });

    test("a folder workspace main mounted is refused on the branch, saying why, and so is mounting one", { skip: !HOST_PROFILE && "folder workspaces are for a single node" }, async () => {
      await branch.say("hi", FOLDER_CHAT);
      await branch.waitFor((call) => Number(call.payload.chat_id) === FOLDER_CHAT && /hit an error: This is the branch environment e2e, .* A folder is this machine's own files/u.test(call.payload.text ?? ""));
      await branch.say("/workspace e2e-folder", FOLDER_CHAT);
      await branch.waitFor((call) => Number(call.payload.chat_id) === FOLDER_CHAT && /^This is the branch environment e2e, .* A folder is this machine's own files/mu.test(call.payload.text ?? ""));
    });

    test("the branch's alasio, compute and lake fit the run's cluster, as the metrics server measures them", async () => {
      const deadline = Date.now() + 300_000;
      let measured: readonly (KubernetesObject & { containers?: { usage?: { memory?: string } }[] })[] = [];
      while (Date.now() < deadline && measured.length < 3) {
        measured = await kube.list("PodMetrics", { namespace: BRANCH_NAMESPACE }).catch(() => []);
        if (measured.length < 3) await sleep(10_000);
      }
      const pods = await kube.list<V1Pod>("Pod", { namespace: BRANCH_NAMESPACE });
      for (const { metadata, containers } of measured) {
        const node = pods.find((pod) => pod.metadata?.name === metadata?.name)?.spec?.nodeName;
        console.error(`# measured: ${metadata?.name} on ${node} uses ${containers?.map(({ usage }) => usage?.memory).join(" + ")}`);
      }
      assert.equal(measured.length, 3, "the metrics server measured the branch's three pods");
      if (AGENTS === 0) assert.equal(new Set(pods.map((pod) => pod.spec?.nodeName)).size, 1);
    });

    test("alasio branch delete leaves nothing of the branch, and main's lake maintenance deletes files again", async () => {
      const { branches } = JSON.parse(await neonControl("/branches")) as { branches: { name: string; timelineId: string }[] };
      const timeline = branches.find(({ name }) => name === BRANCH)?.timelineId;
      assert.ok(timeline);
      await alasioOk("branch", "delete", BRANCH);
      assert.equal(await alasioOk("branch", "list"), "alasio has no branch environments.\n");
      for (const namespace of [BRANCH_NAMESPACE, BRANCH_SESSIONS]) assert.equal(await kube.get(ref("Namespace", namespace)), null);
      const volumes = await kube.list<V1PersistentVolume>("PersistentVolume");
      assert.deepEqual(volumes.filter(({ spec }) => spec?.claimRef?.namespace === BRANCH_SESSIONS).map(({ metadata }) => metadata?.name), []);
      const { tenantId } = JSON.parse(await kube.secret(NAMESPACE, "alasio-neon-root", "secrets.json")) as { tenantId: string };
      const controller = await kube.runningPod(NAMESPACE, "alasio-neon-storage-controller");
      const found = await kube.execOk(NAMESPACE, controller, ["sh", "-c", 'curl -sS -o /dev/null -w "%{http_code}" -H "authorization: Bearer $CONTROL_PLANE_JWT_TOKEN" "$0"', `http://127.0.0.1:1234/v1/tenant/${tenantId}/timeline/${timeline}`]);
      assert.equal(found, "404");
      await untilMaintained("deleted files again", (line) => !line.includes("keptFilesFor"));
      // Main goes on as it was.
      await tg.say("and main?");
      await tg.waitFor((call) => call.method === "sendRichMessage" && call.payload.rich_message?.markdown === ANSWER);
    });
  });
}

/** What alasio installs on this machine as the host target, and what its cluster leaves there: none of it is left once it is removed. */
const INSTALLED_HERE = [
  "/usr/local/bin/k3s",
  "/usr/local/bin/k3s-uninstall.sh",
  "/usr/local/bin/runsc",
  "/usr/local/bin/containerd-shim-runsc-v1",
  "/usr/local/bin/gvisor-bin",
  "/etc/systemd/system/k3s.service",
  "/etc/rancher",
  "/var/lib/rancher",
  "/var/lib/kubelet",
  "/var/lib/juicefs",
  "/etc/sysctl.d/60-alasio-inotify.conf",
];

describe("alasio uninstall --purge", { skip: KEEP && "the run keeps the cluster" }, () => {
  test("removes alasio and the cluster on this machine, with all they keep, and keeps the config", async () => {
    const { configFile, kubeconfig, storage } = paths();
    assert.equal(
      await alasioOk("uninstall", "--purge", "--yes"),
      TARGET === "host"
        ? `alasio and k3s on this machine are removed, with gVisor and all their data; the config at ${configFile} is kept.\n`
        : `alasio and the cluster ${CLUSTER} are removed, with all their data; the config at ${configFile} is kept.\n`,
    );
    assert.ok(!existsSync(storage), `${storage} is left`);
    assert.ok(!existsSync(kubeconfig), `${kubeconfig} is left`);
    assert.ok(existsSync(configFile));
    const status = await alasio("status");
    assert.equal(status.code, 1);
    if (TARGET === "host") {
      assert.equal(status.stdout, "k3s on this machine:\n  not installed\n");
      assert.equal(status.stderr, "alasio: there is no k3s on this machine yet: alasio up installs it\n");
      const left = INSTALLED_HERE.filter((path) => existsSync(path));
      assert.deepEqual(left, [], left.length > 0 ? await onNode("", `ls -laR ${left.join(" ")} 2>&1 | head -50`) : "");
      // Nothing of k3s's, its containerd's or gVisor's runs on, by the executables of what runs.
      const running = "for process in /proc/[0-9]*; do readlink \"$process/exe\" 2>/dev/null; done | grep -E '^(/usr/local/bin/(k3s|runsc|containerd-shim-runsc-v1|gvisor-bin/)|/var/lib/rancher/k3s/)' || true";
      assert.equal(await onNode("", running), "");
    } else {
      assert.match(status.stdout, new RegExp(`^cluster ${CLUSTER}, in Docker .*:\n {2}not made\n$`, "u"));
      assert.equal(status.stderr, `alasio: there is no cluster ${CLUSTER} yet: alasio up makes it\n`);
    }
  });
});
