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
import { spawnSync } from "node:child_process";
import { randomBytes, randomUUID } from "node:crypto";
import { existsSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, afterEach, before, describe, test } from "node:test";

import type { InlineKeyboardButton } from "@grammyjs/types";
import type { CoreV1Event, KubernetesObject, V1ConfigMap, V1Deployment, V1Job, V1PersistentVolume, V1Pod, V2HorizontalPodAutoscaler } from "@kubernetes/client-node";

import { memoryBytes } from "../../cli/src/capacity.ts";
import { componentName, NAMESPACE, RELEASE } from "../../cli/src/manifests/common.ts";
import { neonStack } from "../../neon/test/stack.ts";
import type { Sandbox } from "../../src/kube/sandboxes.ts";
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

/** The host profile's namespace, where folder workspaces' bayma run. */
const HOST_NAMESPACE = "alasio-host";

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
    // The object store's copies, as the run gives them, and each volume server's disk, as its metrics say.
    assert.ok(said.includes(AGENTS >= 2 ? "\nthe object store: each object kept twice (replication 001)\n" : "\nthe object store: each object kept once (replication 000)\n"), said);
    const servers = AGENTS >= 2 ? 3 : 1;
    for (let ordinal = 0; ordinal < servers; ordinal += 1) {
      assert.match(said, new RegExp(`\\n {2}${RELEASE}-seaweedfs-volume-${ordinal}: \\S+ \\S+ to fill before it takes no more writes, of \\S+ \\S+ free on its disk`, "u"));
    }
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

    test("alasio reads a session's files as its agent, a suspended session resumes with them, and one recorded of another pod template is moved onto the current one with them", async () => {
      const seen = await roundtrip(none);
      assert.equal(seen.exec, '"written"');
      assert.match(seen.read ?? "", /^hello from /u);
      assert.equal(seen.missing, "file not found");
      assert.equal(seen.suspendedPod, "gone");
      assert.equal(seen.whileSuspended, "the session is not running");
      assert.equal(seen.afterResume, seen.read);
      assert.equal(seen.execAfterResume, JSON.stringify(seen.read));
      assert.equal(seen.podReplaced, true);
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

    test("alasio may make a folder's bayma's Sandbox from its template alone, and no Secret but a Sandbox's token", { skip: !HOST_PROFILE && "folder workspaces are for a single node" }, async () => {
      const made = (await kube.list<Sandbox>("Sandbox", { namespace: HOST_NAMESPACE })).find(({ metadata }) => metadata?.name?.startsWith("bayma-"));
      assert.ok(made?.metadata?.name, "the folder's bayma's Sandbox is made");
      const name = made.metadata.name;
      // As the folder's, under another name and suspended; with `args` after its bayma's.
      const copied = (as: string, args: readonly string[] = []) => {
        const sandbox: Sandbox = JSON.parse(JSON.stringify({ apiVersion: made.apiVersion, kind: made.kind, metadata: { name, namespace: HOST_NAMESPACE }, spec: { ...made.spec, operatingMode: "Suspended" } }).replaceAll(name, as));
        const bayma = sandbox.spec.podTemplate.spec?.containers.find((container) => container.name === "bayma");
        if (bayma) bayma.args = [...(bayma.args ?? []), ...args];
        return sandbox;
      };
      const sandboxes = `/apis/agents.x-k8s.io/v1beta1/namespaces/${HOST_NAMESPACE}/sandboxes`;
      assert.equal(await asAlasio("POST", sandboxes, "application/json", copied("e2e-folder-other", ["--listen", "0.0.0.0:1"])), 422);
      // Its state anywhere but under the template's state root.
      const elsewhere = copied("e2e-folder-elsewhere");
      const args = elsewhere.spec.podTemplate.spec?.containers.find((container) => container.name === "bayma")?.args ?? [];
      const at = args.indexOf("--state-dir") + 1;
      assert.ok(at > 0, "the folder's bayma has a state directory");
      args[at] = "/tmp/claude/e2e";
      assert.equal(await asAlasio("POST", sandboxes, "application/json", elsewhere), 422);
      assert.equal(await asAlasio("POST", sandboxes, "application/json", copied("e2e-folder-made")), 201);
      await kube.remove(ref("Sandbox", "e2e-folder-made", HOST_NAMESPACE));
      // An Opaque Secret named as a token's, holding more than a token; and one holding the token alone, which is made.
      const secrets = `/api/v1/namespaces/${HOST_NAMESPACE}/secrets`;
      const secret = (data: Record<string, string>) => ({ apiVersion: "v1", kind: "Secret", metadata: { name: "e2e-folder-bayma-token" }, type: "Opaque", data });
      assert.equal(await asAlasio("POST", secrets, "application/json", secret({ token: "dG9rZW4=", other: "b3RoZXI=" })), 422);
      assert.equal(await asAlasio("POST", secrets, "application/json", secret({ token: "dG9rZW4=" })), 201);
      await kube.remove(ref("Secret", "e2e-folder-bayma-token", HOST_NAMESPACE));
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

/**
 * Waits until `sql`, a count as `n`, counts some of the lake's rows, as the collector
 * sends them within seconds, and the loader loads transcripts within its interval.
 */
async function untilInLake(what: string, sql: string, timeoutMs = 240_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
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

/** Grafana's API, as its admin, through alasio grafana's port-forward, until it is closed. */
interface GrafanaApi {
  readonly api: (path: string, init?: RequestInit) => Promise<{ status: number; body: unknown }>;
  readonly close: () => void;
}

/** Grafana's API, as its admin, through `alasio grafana`. */
async function grafanaApi(): Promise<GrafanaApi> {
  const password = (await alasioOk("grafana", "--password")).trim();
  const authorization = `Basic ${Buffer.from(`admin:${password}`).toString("base64")}`;
  const grafana = await alasioRunning("grafana", "--port", "0");
  const base = /http:\/\/127\.0\.0\.1:\d+/u.exec(grafana.said)?.[0] ?? "";
  assert.ok(base, grafana.said);
  return {
    api: async (path, init = {}) => {
      const response = await fetch(`${base}${path}`, { ...init, headers: { authorization, "content-type": "application/json", ...init.headers } });
      const text = await response.text();
      return { status: response.status, body: text.startsWith("{") || text.startsWith("[") ? JSON.parse(text) : text };
    },
    close: () => grafana.child.kill(),
  };
}

/** Sends the stack's collector a turn that failed, as alasio records one, as alasio sends its spans. */
async function sendFailedTurn(): Promise<void> {
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
}

/**
 * The alerts that turns are failing alasio sends through the bot until each of its two
 * users has one, and for `more` milliseconds after: their texts, by the chat each went to.
 */
async function untilAlerted(more = 0): Promise<Map<string, string[]>> {
  const alerts = new Map<string, string[]>();
  const collect = async () => {
    for (const call of await tg.calls()) {
      if (call.method !== "sendMessage" || !/Turns are failing/u.test(call.payload.text ?? "")) continue;
      const chat = String(call.payload.chat_id);
      alerts.set(chat, [...(alerts.get(chat) ?? []), call.payload.text ?? ""]);
    }
  };
  for (const deadline = Date.now() + 420_000; alerts.size < 2; await sleep(500)) {
    if (Date.now() > deadline) assert.fail(`not each user was alerted: ${JSON.stringify([...alerts])}`);
    await collect();
  }
  for (const deadline = Date.now() + more; Date.now() < deadline; await sleep(5000)) await collect();
  return alerts;
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

    test("a turn's spans join its transcript in the lake: alasio's turn, Codex's, and the rollout Codex wrote", async () => {
      await tg.calls();
      await tg.say("/service codex");
      await tg.waitFor((call) => /^Active: Codex$/mu.test(call.payload.text ?? ""));
      await tg.say("hello");
      await tg.waitFor((call) => call.method === "sendRichMessage" && call.payload.rich_message?.markdown === ANSWER);
      // alasio's turn names Codex's thread, Codex's span in its trace names Codex's turn, and
      // the thread's rollout, mirrored to Neon and loaded, has that turn, completed.
      await untilInLake(
        "turn joined to its transcript",
        `select count(*) as n from otel.traces turn
          join codex.turns codex on codex.thread_id = turn.SpanAttributes['alasio.session.id']
          join otel.traces app on app.TraceId = turn.TraceId and app.SpanAttributes['turn.id'] = codex.turn_id
          where turn.SpanName = 'alasio.turn' and app.ServiceName = 'codex-app-server' and app.SpanName = 'turn/start' and codex.completed_at is not null`,
        600_000,
      );
    });

    test("the stack's telemetry is in the lake, the compute's and the lake's own among it", async () => {
      // What the collector scrapes is named for its job; the lake exports its events itself.
      await untilInLake("metric of the compute's", metricRows("ServiceName = 'compute'"));
      await untilInLake("log record of the lake's", "select count(*) as n from otel.logs where ServiceName = 'alasio-lake'");
      await untilInLake("metric of JuiceFS's", metricRows("MetricName like 'juicefs_%'"));
      // Each Valkey's, named by its Service.
      for (const valkey of [`${RELEASE}-valkey`, `${RELEASE}-seaweedfs-filer-store`]) {
        await untilInLake(`metric of ${valkey}'s`, metricRows(`MetricName = 'redis.memory.used' and ResourceAttributes['server.address'] = '${valkey}'`));
      }
      // The object store's volume servers', of their disks, labelled with their pods.
      await untilInLake("metric of the object store's volume servers' disks", metricRows("ServiceName = 'seaweedfs' and MetricName = 'SeaweedFS_volumeServer_resource' and Attributes['pod'] like '%-seaweedfs-volume-%'"));
      // The log's, its brokers' among them, and its intake's, how far behind it is.
      await untilInLake("metric of the log's", metricRows("ServiceName = 'log' and MetricName = 'kafka_broker_active_count'"));
      await untilInLake("metric of the lake's intake's", metricRows("ServiceName = 'lake-intake' and MetricName = 'lake_telemetry_lag_records'"));
    });

    describe("Grafana", () => {
      let grafana: GrafanaApi | undefined;
      const api: GrafanaApi["api"] = (path, init) => {
        assert.ok(grafana, "Grafana is reached");
        return grafana.api(path, init);
      };

      before(async () => {
        grafana = await grafanaApi();
      });

      after(() => {
        grafana?.close();
      });

      test("alasio grafana reaches Grafana, its data source reads the lake, and its dashboards' queries answer with the lake's rows", async (t) => {
        assert.equal((await api("/api/health")).status, 200);
        assert.deepEqual((await api("/api/datasources/uid/lake/health")).body, { message: "Health check successful", status: "OK" });
        const dashboards = (await api("/api/search?type=dash-db")).body as { title: string; folderTitle: string }[];
        assert.deepEqual(dashboards.map(({ folderTitle, title }) => `${folderTitle}/${title}`).sort(), ["alasio/Agents", "alasio/Conversations", "alasio/Stack health"]);
        // The Stack health dashboard's tables of services and of the object store's volume servers, as their panels query them.
        const stack: { panels: { title: string; targets?: object[] }[] } = JSON.parse(readFileSync(new URL("../../neon/grafana/dashboards/stack-health.json", import.meta.url), "utf8"));
        // Each as Grafana's browser sends it: it fills in ${__interval_ms} (a minute here, the scrape's), which /api/ds/query
        // takes as sent, the data source expanding ${__from} and ${__to} alone.
        const queryOf = (panel: string): object =>
          JSON.parse(JSON.stringify(stack.panels.find(({ title }) => title === panel)?.targets?.[0] ?? assert.fail(`no panel ${panel}`)).replaceAll("${__interval_ms}", "60000"));
        const services = queryOf("Services now");
        for (const panel of ["Services now", "The object store's volume servers", "The log's answers", "Telemetry waiting in the log"]) {
          let rows = 0;
          let error: string | undefined;
          for (const deadline = Date.now() + 180_000; rows === 0 && Date.now() < deadline; await sleep(5000)) {
            const answered = (await api("/api/ds/query", { method: "POST", body: JSON.stringify({ from: "now-1h", to: "now", queries: [queryOf(panel)] }) })).body as {
              results: { A: { frames?: { data: { values: unknown[][] } }[]; error?: string } };
            };
            rows = answered.results.A.frames?.[0]?.data.values[0]?.length ?? 0;
            error = answered.results.A.error;
          }
          if (rows === 0) {
            // The metrics the panel may read, each name once, with how many of its points are of Produce requests, as the lake has them.
            const names = await lakeQuery(
              "select ServiceName, MetricName, count(*) as n, count(*) filter (where Attributes['type'] = 'Produce') as produce from otel.metrics_gauge where ServiceName in ('log', 'lake-intake') " +
                "and (MetricName like 'kafka_request_time%' or MetricName like 'kafka_stream_wal%' or MetricName like 'lake_telemetry%') and TimeUnix > now()::timestamp - interval 1 hour group by all order by 2",
            ).catch((failure: unknown) => String(failure));
            assert.fail(`${panel} answered with no row${error ? `, and the error ${error}` : ""}; the log's and the intake's metrics it may read: ${JSON.stringify(names)}`);
          }
        }
        // A query the lake refuses says why, in Grafana.
        const refused = (await api("/api/ds/query", { method: "POST", body: JSON.stringify({ from: "now-1h", to: "now", queries: [{ ...services, url_options: { ...(services as { url_options: object }).url_options, data: "select nothing" } }] }) })).body as {
          results: { A: { error?: string } };
        };
        assert.match(refused.results.A.error ?? "", /Binder Error: Referenced column "nothing" was not found/u);
        // Started once its database was ready for it, on a fresh install: never restarted for want of it.
        const pod = await kube.get<V1Pod>(ref("Pod", await kube.runningPod(NAMESPACE, "alasio-grafana"), NAMESPACE));
        assert.deepEqual(pod?.status?.containerStatuses?.map(({ name, restartCount }) => [name, restartCount]), [["grafana", 0]]);
        // A restart's back-off, that is: an image's pull's is the registry's, which a pod's first pull can meet.
        const restarts = (await kube.list<CoreV1Event>("Event", { namespace: NAMESPACE }))
          .filter(({ involvedObject, reason, message }) => involvedObject.name?.startsWith("alasio-grafana-") && reason === "BackOff" && message?.startsWith("Back-off restarting"));
        assert.deepEqual(restarts.map(({ message }) => message), []);
        // What Grafana and the lake's query endpoint hold, as their cgroups count it.
        const memory = async (pod: string, container: string) =>
          (await kube.execOk(NAMESPACE, pod, ["cat", "/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/memory.peak"], { container })).trim().split("\n").map((bytes) => `${Math.round(Number(bytes) / 1048576)} MiB`).join(", peak ");
        t.diagnostic(`Grafana: ${await memory(await kube.runningPod(NAMESPACE, "alasio-grafana"), "grafana")}`);
        t.diagnostic(`the lake's query endpoint: ${await memory(await kube.runningPod(NAMESPACE, "alasio-lake"), "query")}`);
        t.diagnostic(`the lake service: ${await memory(await kube.runningPod(NAMESPACE, "alasio-lake"), "lake")}`);
      });

      test("a turn that failed alerts each of the bot's users, through the bot", async () => {
        await sendFailedTurn();
        const alerts = await untilAlerted();
        assert.deepEqual([...alerts.keys()].sort(), ["1001", String(OTHER_OPERATOR)]);
        for (const [text] of alerts.values()) assert.match(text ?? "", /conversation = telegram:1001/u);
        const rules = (await api("/api/prometheus/grafana/api/v1/rules")).body as { data: { groups: { rules: { name: string; state: string; health: string; lastError?: string }[] }[] } };
        const evaluated = rules.data.groups.flatMap((group) => group.rules);
        assert.equal(evaluated.find(({ name }) => name === "Turns are failing")?.state, "firing");
        // Every rule's query answers: none fails on the lake.
        assert.deepEqual(evaluated.filter(({ health }) => health === "error").map(({ name, lastError }) => `${name}: ${lastError}`), []);
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

    describe("through the log", () => {
      let gateway: Forward | undefined;

      before(async () => {
        gateway = await kube.forward(NAMESPACE, componentName("collector"), 4318);
      });

      after(() => {
        gateway?.close();
      });

      /** Exports the span `seq` of the run `run` to the collector's gateway, as a producer does: whether it was answered as taken. */
      const exported = async (run: string, seq: number): Promise<boolean> => {
        const now = BigInt(Date.now()) * 1_000_000n;
        const body = {
          resourceSpans: [{
            resource: { attributes: [{ key: "service.name", value: { stringValue: "e2e-log" } }] },
            scopeSpans: [{
              spans: [{
                traceId: randomBytes(16).toString("hex"),
                spanId: randomBytes(8).toString("hex"),
                name: "e2e.log",
                kind: 1,
                startTimeUnixNano: String(now),
                endTimeUnixNano: String(now + 1000n),
                attributes: [{ key: "e2e.run", value: { stringValue: run } }, { key: "e2e.seq", value: { intValue: String(seq) } }],
              }],
            }],
          }],
        };
        const answered = await fetch(`${gateway?.base}/v1/traces`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body), signal: AbortSignal.timeout(20_000) })
          .then(({ status }) => status, () => 0);
        return answered === 200;
      };

      /** Exports the run's spans `from` to `to`, `concurrency` at a time: those answered as taken. */
      const exportAll = async (run: string, from: number, to: number, concurrency = 1): Promise<number[]> => {
        const taken: number[] = [];
        let next = from;
        await Promise.all(Array.from({ length: concurrency }, async () => {
          for (let seq = next++; seq < to; seq = next++) if (await exported(run, seq)) taken.push(seq);
        }));
        return taken;
      };

      /** The tail of `container`'s log in the pod `pod`, its lines that match `only` where given, for a failure's message. */
      const tail = async (pod: Promise<string>, container: string, only?: RegExp): Promise<string> => {
        const logged = await pod.then((name) => kube.logs(NAMESPACE, name, container)).catch((error: unknown) => String(error));
        return logged.split("\n").filter((line) => !only || only.test(line)).slice(-30).join("\n");
      };

      /** What the intake, the log and KEDA say, for a failure's message: the intake's records by outcome, and each one's last lines. */
      const logState = async (): Promise<string> => {
        const outcomes = await lakeQuery("select Attributes['topic'] as topic, Attributes['outcome'] as outcome, max(Value) as n from otel.metrics_sum where ServiceName = 'lake-intake' and MetricName like 'lake_telemetry_records%' group by all").catch((error: unknown) => String(error));
        return [
          `the intake's records: ${JSON.stringify(outcomes)}`,
          `the intake's log:\n${await tail(kube.runningPod(NAMESPACE, componentName("lake-intake")), "intake")}`,
          `the log's warnings:\n${await tail(Promise.resolve(`${componentName("log")}-0`), "log", /WARN|ERROR/u)}`,
          `KEDA's operator:\n${await tail(kube.runningPod(NAMESPACE, componentName("keda-operator")), "keda-operator", /kafka|lake-intake/u)}`,
        ].join("\n");
      };

      /**
       * Where the spans exported around `at` are, for a failure's message: the records the
       * log holds of otel.spans from 15 s before it to 45 s after (./log-records.ts), each
       * partition's by offset, the run's marked with their seq; the offsets the intake's
       * group was told and those the lake holds; and the spans of those records the lake
       * lacks, and those the lake has, started in that window, that no record holds: what
       * tells a span the log lost from one the intake skipped, and offsets the log gave
       * twice.
       */
      const whereSpansAre = async (at: number): Promise<string> => {
        const [from, to] = [at - 15_000, at + 45_000];
        const script = readFileSync(join(import.meta.dirname, "log-records.ts"), "utf8");
        const read = await kube.exec(NAMESPACE, `${componentName("log")}-0`, ["sh", "-c", 'cd /opt/alasio && node --input-type=module-typescript - "$@"', "node", String(from), String(to)], { container: "access", stdin: script })
          .then(({ stdout, stderr }) => `${stdout}${stderr}`, (error: unknown) => String(error));
        let log: { partitions: object[]; committed: object[]; records: { partition: number; offset: string; spans: { traceId: string; spanId: string; service?: string; name?: string; seq?: number }[] | string }[] };
        try {
          log = JSON.parse(read.trim().split("\n").at(-1) ?? "");
        } catch {
          return `the log's records could not be read: ${read}`;
        }
        const offsets = await lakeQuery("from otel.intake_offsets order by all").catch((error: unknown) => String(error));
        const lake = await lakeQuery(`select TraceId, SpanId, ServiceName, SpanName from otel.traces where Timestamp >= make_timestamp(${(from - 300_000) * 1000})`).catch(() => []);
        const inLake = new Set(lake.map((row) => `${String(row["TraceId"])}:${String(row["SpanId"])}`));
        const logged = log.records.flatMap((record) => (typeof record.spans === "string" ? [] : record.spans.map((span) => ({ ...span, at: `${record.partition}@${record.offset}` }))));
        const inLog = new Set(logged.map((span) => `${span.traceId}:${span.spanId}`));
        const started = await lakeQuery(`select TraceId, SpanId, ServiceName, SpanName from otel.traces where Timestamp between make_timestamp(${(from + 20_000) * 1000}) and make_timestamp(${(to - 20_000) * 1000})`).catch(() => []);
        const byPartition = new Map<number, string[]>();
        for (const record of log.records) {
          const seqs = typeof record.spans === "string" ? [record.spans] : record.spans.flatMap(({ seq }) => (seq === undefined ? [] : [`seq ${seq}`]));
          byPartition.set(record.partition, [...(byPartition.get(record.partition) ?? []), `${record.offset}${seqs.length > 0 ? ` (${seqs.join(", ")})` : ""}`]);
        }
        return [
          `the log: ${JSON.stringify(log.partitions)}; the intake's group told ${JSON.stringify(log.committed)}; the lake's offsets ${JSON.stringify(offsets)}`,
          ...[...byPartition].map(([partition, records]) => `partition ${partition}'s records from ${new Date(from).toISOString()} to ${new Date(to).toISOString()}: ${records.join(" ")}`),
          `their spans the lake lacks: ${JSON.stringify(logged.filter((span) => !inLake.has(`${span.traceId}:${span.spanId}`)))}`,
          `spans the lake has, started from 5 s after the crash to 25 s after, that no record of the log holds: ${JSON.stringify(started.filter((row) => !inLog.has(`${String(row["TraceId"])}:${String(row["SpanId"])}`)))}`,
          `the gateway's log:\n${await tail(kube.runningPod(NAMESPACE, componentName("collector")), "collector")}`,
        ].join("\n");
      };

      /** Waits until the lake has each of `taken` of the run's spans, and says how many it has of each; `crashed`, where given, is when the log was made to crash. */
      const untilWritten = async (run: string, taken: readonly number[], crashed?: number, timeoutMs = 600_000): Promise<Map<number, number>> => {
        const deadline = Date.now() + timeoutMs;
        for (;;) {
          const rows = await lakeQuery(`select SpanAttributes['e2e.seq'] as seq, count(*) as n from otel.traces where ServiceName = 'e2e-log' and SpanAttributes['e2e.run'] = '${run}' group by 1`).catch(() => []);
          const written = new Map(rows.map((row) => [Number(row["seq"]), Number(row["n"])]));
          if (taken.every((seq) => written.has(seq))) return written;
          if (Date.now() > deadline) {
            assert.fail(`of the ${taken.length} spans the gateway took, the lake lacks ${JSON.stringify(taken.filter((seq) => !written.has(seq)))}\n${crashed === undefined ? "" : `${await whereSpansAre(crashed)}\n`}${await logState()}`);
          }
          await sleep(5000);
        }
      };

      test("a broker's crash loses nothing the gateway answered as taken, which it answers once the log is back", async (t) => {
        for (let round = 1; round <= 20; round += 1) {
        t.diagnostic(`round ${round} at ${new Date().toISOString()}`);
        const run = randomUUID();
        const before = await exportAll(run, 0, 40);
        assert.equal(before.length, 40, "the gateway took every span while the log ran");
        const crashed = Date.now();
        await kube.kill(NAMESPACE, `${componentName("log")}-0`);
        // Each export sent on until one is taken again, as a producer sends what was refused again.
        const during: number[] = [];
        for (let seq = 40, deadline = Date.now() + 600_000; during.length < 20; seq += 1) {
          if (Date.now() > deadline) assert.fail("the gateway took no export again within ten minutes of the log's crash");
          if (await exported(run, seq)) during.push(seq);
        }
        const written = await untilWritten(run, [...before, ...during], crashed);
        assert.deepEqual([...written.values()].filter((n) => n > 1), [], "no span is written twice");
        t.diagnostic(`round ${round}: taken during ${JSON.stringify(during)}`);
        }
      });

      test("the intake's crash, as it gathers or writes a batch, loses nothing and writes nothing twice", async () => {
        const run = randomUUID();
        const first = await exportAll(run, 0, 100, 4);
        // The intake killed as it gathers or writes a batch, whatever it had of the run read again by its successor.
        await sleep(30_000);
        await kube.kill(NAMESPACE, await kube.runningPod(NAMESPACE, componentName("lake-intake")));
        const second = await exportAll(run, 100, 200, 4);
        assert.equal(first.length + second.length, 200, "the gateway took every span");
        const written = await untilWritten(run, [...first, ...second]);
        assert.equal(written.size, 200);
        assert.deepEqual([...written.entries()].filter(([, n]) => n > 1), [], "no span is written twice");
      });

      test("the intake wakes from none on what waits in the log, and scales out on a backlog, to as many as the topics have partitions", async (t) => {
        const scaledObject = ref("ScaledObject", componentName("lake-intake"), NAMESPACE);
        const intake = ref("Deployment", componentName("lake-intake"), NAMESPACE);
        // What KEDA, and the HPA it makes, scale it to.
        const replicas = async () => (await kube.get<V1Deployment>(intake))?.spec?.replicas ?? 0;
        // The lake's write lock, held in a session of the catalog's database as long as the test says, as while the lake cannot be written:
        // the backlog stays in the log, however fast the intake reads it, until KEDA has scaled it out.
        const compute = await kube.runningPod(NAMESPACE, "alasio-neon-compute");
        const catalog = (sql: string, application = "e2e") =>
          kube.exec(NAMESPACE, compute, ["psql", "-h", "127.0.0.1", "-p", "55433", "-U", "cloud_admin", "-d", `dbname=lake application_name=${application}`, "-Atc", sql], { container: "compute" });
        let held: Promise<unknown> | null = null;
        const release = async () => {
          await catalog("select pg_terminate_backend(pid) from pg_stat_activity where application_name = 'e2e-write-lock'");
          await held?.catch(() => {});
        };
        // Held at none, as while nothing waits.
        await kube.patch(scaledObject, { metadata: { annotations: { "autoscaling.keda.sh/paused-replicas": "0" } } });
        try {
          for (const deadline = Date.now() + 300_000; (await kube.get<V1Deployment>(intake))?.status?.replicas; await sleep(2000)) {
            if (Date.now() > deadline) assert.fail("the intake did not stop while paused at none");
          }
          held = catalog("select pg_advisory_lock(hashtext('alasio.lake.write')); select pg_sleep(1200)", "e2e-write-lock");
          for (const deadline = Date.now() + 60_000; ; await sleep(1000)) {
            const granted = await catalog("select count(*) from pg_locks join pg_stat_activity using (pid) where locktype = 'advisory' and granted and application_name = 'e2e-write-lock'");
            if (granted.stdout.trim() === "1") break;
            if (Date.now() > deadline) assert.fail(`the test could not hold the lake's write lock: ${granted.stderr}${granted.stdout}`);
          }
          const run = randomUUID();
          // Some thousands a partition, as after an outage: KEDA gives it a replica a thousand.
          const taken = await exportAll(run, 0, 6000, 16);
          assert.equal(taken.length, 6000);
          await kube.patch(scaledObject, { metadata: { annotations: { "autoscaling.keda.sh/paused-replicas": null } } });
          let most = 0;
          for (const deadline = Date.now() + 300_000; most < 2; await sleep(2000)) {
            if (Date.now() > deadline) {
              const scaled = await kube.get<KubernetesObject & { status?: object }>(scaledObject);
              assert.fail(`the intake ran ${most} replica(s) at most, on a backlog of 6000 records; its ScaledObject: ${JSON.stringify(scaled?.status)}\n${await logState()}`);
            }
            most = Math.max(most, await replicas());
          }
          t.diagnostic(`the intake ran ${most} replicas on the backlog`);
          // The lake writable again, each replica writes what it read, its share once, whichever had it before a rebalance.
          await release();
          const written = await untilWritten(run, taken);
          assert.deepEqual([...written.values()].filter((n) => n > 1), [], "no span is written twice");
        } finally {
          await release();
          await kube.patch(scaledObject, { metadata: { annotations: { "autoscaling.keda.sh/paused-replicas": null } } });
        }
      });
    });
  });
}

if (inShard("neon")) neonStack();

/** The namespace of the scaling suite's own workloads. */
const SCALING = "alasio-e2e-scaling";

/** The pause image, which does nothing, as the suite's own workloads run it. */
const PAUSE = "registry.k8s.io/pause:3.10.1@sha256:278fb9dbcca9518083ad1e11276933a2e96f23de604a3a08cc3c80002767d24c";

/** A pod that does nothing, restricted, labelled `labels`, requesting `requests`, under `runtimeClassName` when given. */
const pausePod = (labels: Record<string, string>, requests: Record<string, string>, runtimeClassName?: string) => ({
  metadata: { labels },
  spec: {
    ...(runtimeClassName ? { runtimeClassName } : {}),
    securityContext: { runAsNonRoot: true, runAsUser: 65535, runAsGroup: 65535, seccompProfile: { type: "RuntimeDefault" } },
    containers: [{
      name: "pause",
      image: PAUSE,
      securityContext: { allowPrivilegeEscalation: false, capabilities: { drop: ["ALL"] }, readOnlyRootFilesystem: true },
      resources: { requests },
    }],
  },
});

/** A Deployment of the suite's of `replicas` pods like `pod`, named `name`. */
const deploymentOf = (name: string, replicas: number, pod: ReturnType<typeof pausePod>) => ({
  apiVersion: "apps/v1",
  kind: "Deployment",
  metadata: { name, namespace: SCALING },
  spec: { replicas, selector: { matchLabels: { app: name } }, template: { ...pod, metadata: { labels: { ...pod.metadata.labels, app: name } } } },
});

/** Waits until `check` gives something, every two seconds for `timeoutMs`: what it gave. */
async function until<A>(what: string, check: () => Promise<A | undefined>, timeoutMs: number): Promise<A> {
  for (const deadline = Date.now() + timeoutMs; Date.now() < deadline; await sleep(2000)) {
    const found = await check();
    if (found !== undefined) return found;
  }
  return assert.fail(`${what}, not within ${timeoutMs / 1000}s`);
}

/** Runs kubectl on the run's cluster with `args`, given `input`: whether it succeeded, what it printed, and all it said. */
function kubectl(args: readonly string[], input?: string): { readonly ok: boolean; readonly printed: string; readonly said: string } {
  const run = spawnSync("kubectl", ["--kubeconfig", paths().kubeconfig, ...args], { encoding: "utf8", ...(input === undefined ? {} : { input }) });
  return { ok: run.status === 0, printed: run.stdout, said: `${run.stdout}${run.stderr}` };
}

/** The HorizontalPodAutoscaler KEDA moves Grafana's replicas by, as its ScaledObject says, or none. */
function kedaHpa(): V2HorizontalPodAutoscaler | null {
  const got = kubectl(["get", "horizontalpodautoscaler", "keda-hpa-alasio-grafana", "--namespace", NAMESPACE, "--output", "json"]);
  if (!got.ok && /NotFound/u.test(got.said)) return null;
  assert.ok(got.ok, got.said);
  return JSON.parse(got.printed);
}

/** Asks the API server `method` of `path`, with `body` as `contentType`, as alasio's ServiceAccount, from its pod: the status it answered. */
async function asAlasio(method: string, path: string, contentType: string, body: unknown): Promise<number> {
  const said = await inAlasioContainer([
    "sh",
    "-c",
    'cd /var/run/secrets/kubernetes.io/serviceaccount && curl -sS -o /dev/null -w "%{http_code}" --cacert ca.crt -H "Authorization: Bearer $(cat token)" ' +
    '-H "Content-Type: $2" -X "$0" --data-binary @- "https://kubernetes.default.svc$1"',
    method,
    path,
    contentType,
  ], JSON.stringify(body));
  return Number(said);
}

/** Whether the pod's label says it runs a turn. */
const labelledBusy = async (namespace: string, name: string) => (await kube.get<V1Pod>(ref("Pod", name, namespace)))?.metadata?.labels?.["alasio.dev/busy"] === "true";

if (inShard("scaling")) {
  describe("how alasio is scaled, sized and drained", () => {
    before(async () => {
      telegram = await forward(TELEGRAM);
      tg = operator(telegram.base);
      await tg.waitFor((call) => call.method === "setMyCommands", 300_000).catch(() => {});
      await kube.apply({ apiVersion: "v1", kind: "Namespace", metadata: { name: SCALING, labels: { "pod-security.kubernetes.io/enforce": "restricted" } } });
    });

    after(() => {
      telegram?.close();
    });

    test("KEDA and VPA run restricted, on pinned images, the external metrics served, within the floor's budget for them", async (t) => {
      const controllers = ["keda-operator", "keda-metrics-apiserver", "keda-admission-webhooks", "vpa-recommender", "vpa-updater", "vpa-admission-controller"];
      for (const controller of controllers) {
        const pod = await kube.get<V1Pod>(ref("Pod", await kube.runningPod(NAMESPACE, componentName(controller)), NAMESPACE));
        assert.equal(pod?.spec?.securityContext?.runAsNonRoot, true, controller);
        assert.equal(pod?.spec?.securityContext?.seccompProfile?.type, "RuntimeDefault", controller);
        assert.deepEqual(
          pod?.spec?.containers.map(({ image, securityContext }) => ({
            pinned: /@sha256:[0-9a-f]{64}$/u.test(image ?? ""),
            drop: securityContext?.capabilities?.drop,
            allowPrivilegeEscalation: securityContext?.allowPrivilegeEscalation,
            readOnlyRootFilesystem: securityContext?.readOnlyRootFilesystem,
          })),
          [{ pinned: true, drop: ["ALL"], allowPrivilegeEscalation: false, readOnlyRootFilesystem: true }],
          controller,
        );
      }
      await until("the external metrics' APIService available", async () => {
        const service = await kube.get<KubernetesObject & { status?: { conditions?: { type: string; status: string }[] } }>(ref("APIService", "v1beta1.external.metrics.k8s.io"));
        return service?.status?.conditions?.some(({ type, status }) => type === "Available" && status === "True") || undefined;
      }, 300_000);
      // What each uses, as the metrics server measures it, against the floor's budget of 0.5 GiB for both.
      const used = await until("every controller measured", async () => {
        const measured = await Promise.all(controllers.map(async (controller) => {
          const [pod] = await kube.list<KubernetesObject & { containers?: { usage?: { memory?: string } }[] }>("PodMetrics", {
            namespace: NAMESPACE,
            labelSelector: `app.kubernetes.io/component=${controller}`,
          });
          return pod?.containers?.reduce((sum, { usage }) => sum + memoryBytes(usage?.memory ?? "0"), 0);
        }));
        return measured.every((bytes) => bytes !== undefined) ? measured as number[] : undefined;
      }, 300_000);
      controllers.forEach((controller, index) => t.diagnostic(`${controller}: ${Math.round((used[index] ?? 0) / 2 ** 20)} MiB`));
      assert.ok(used.reduce((sum, bytes) => sum + bytes, 0) < 0.5 * 2 ** 30, "KEDA and VPA within half a GiB");
    });

    test("KEDA scales a workload from none to one on what it waits on, and back to none once it has waited a while", async () => {
      await kube.apply(deploymentOf("e2e-scaled", 0, pausePod({}, { cpu: "1m", memory: "4Mi" })));
      const scaledObject = {
        apiVersion: "keda.sh/v1alpha1",
        kind: "ScaledObject",
        metadata: { name: "e2e-scaled", namespace: SCALING },
        spec: {
          scaleTargetRef: { name: "e2e-scaled" },
          minReplicaCount: 0,
          maxReplicaCount: 1,
          pollingInterval: 5,
          cooldownPeriod: 15,
          // A pod of this label is what the workload waits on.
          triggers: [{ type: "kubernetes-workload", metadata: { podSelector: "e2e-demand=yes", value: "1" } }],
        },
      };
      await kube.apply(scaledObject);
      const replicas = async () => (await kube.get<V1Deployment>(ref("Deployment", "e2e-scaled", SCALING)))?.status?.readyReplicas ?? 0;
      const demand = pausePod({ "e2e-demand": "yes" }, { cpu: "1m", memory: "4Mi" });
      await kube.apply({ apiVersion: "v1", kind: "Pod", ...demand, metadata: { ...demand.metadata, name: "e2e-demand", namespace: SCALING } });
      await until("the workload woken", async () => ((await replicas()) === 1 ? true : undefined), 180_000);
      await kube.remove(ref("Pod", "e2e-demand", SCALING));
      await until("the workload asleep again", async () => ((await kube.get<V1Deployment>(ref("Deployment", "e2e-scaled", SCALING)))?.spec?.replicas === 0 ? true : undefined), 180_000);
    });

    test("VPA resizes a workload's pod in place, without restarting it, under runc and under gVisor", async (t) => {
      const sized = (name: string, runtimeClassName?: string) => [
        // Requests far under what VPA recommends at the least, so it resizes them at once.
        deploymentOf(name, 1, pausePod({ "app.kubernetes.io/part-of": "alasio" }, { cpu: "1m", memory: "4Mi" }, runtimeClassName)),
        {
          apiVersion: "autoscaling.k8s.io/v1",
          kind: "VerticalPodAutoscaler",
          metadata: { name, namespace: SCALING, labels: { "app.kubernetes.io/part-of": "alasio" } },
          spec: {
            targetRef: { apiVersion: "apps/v1", kind: "Deployment", name },
            updatePolicy: { updateMode: "InPlace", minReplicas: 1 },
            resourcePolicy: { containerPolicies: [{ containerName: "pause", minAllowed: { cpu: "5m", memory: "16Mi" }, controlledValues: "RequestsOnly" }] },
          },
        },
      ];
      for (const object of [...sized("e2e-sized-runc"), ...sized("e2e-sized-gvisor", "gvisor")]) await kube.apply(object);
      await kube.awaitReady([ref("Deployment", "e2e-sized-runc", SCALING), ref("Deployment", "e2e-sized-gvisor", SCALING)], "5 minutes");
      /** The pod of `name`'s Deployment: its uid, its container's restarts, and its memory request. */
      const podOf = async (name: string) => {
        const [pod] = await kube.list<V1Pod>("Pod", { namespace: SCALING, labelSelector: `app=${name}` });
        return {
          uid: pod?.metadata?.uid,
          restarts: pod?.status?.containerStatuses?.[0]?.restartCount,
          memory: pod?.spec?.containers[0]?.resources?.requests?.["memory"],
          resize: JSON.stringify(pod?.status?.conditions?.filter(({ type }) => type.startsWith("PodResize")) ?? []),
        };
      };
      const names = ["e2e-sized-runc", "e2e-sized-gvisor"];
      const befores = await Promise.all(names.map(podOf));
      for (const [index, name] of names.entries()) {
        const before = befores[index] ?? assert.fail(name);
        const resized = await until(`${name}'s pod resized`, async () => {
          const now = await podOf(name);
          return now.uid !== before.uid || now.memory !== before.memory ? now : undefined;
        }, 600_000);
        t.diagnostic(`${name}: requests ${before.memory} to ${resized.memory}, restarts ${resized.restarts}, ${resized.resize}`);
        assert.deepEqual([resized.uid, resized.restarts], [before.uid, 0], `${name}'s pod resized in place, not recreated or restarted`);
        assert.ok(memoryBytes(resized.memory ?? "0") >= 16 * 2 ** 20, resized.memory);
      }
    });

    test("a turn holds alasio's pod and its session's busy until it ends", async () => {
      const volumeId = await newSession("none");
      await tg.say("/service codex");
      await tg.waitFor((call) => /^Active: Codex$/mu.test(call.payload.text ?? ""));
      const alasioPod = await kube.runningPod(NAMESPACE, RELEASE);
      await tg.say("wait 30");
      await until("both pods busy", async () => ((await labelledBusy(NAMESPACE, alasioPod)) && (await labelledBusy(SESSIONS, volumeId)) ? true : undefined), 60_000);
      await tg.waitFor((call) => call.method === "sendRichMessage" && call.payload.rich_message?.markdown === ANSWER, 120_000);
      await until("neither pod busy", async () => (!(await labelledBusy(NAMESPACE, alasioPod)) && !(await labelledBusy(SESSIONS, volumeId)) ? true : undefined), 60_000);
    });

    test("alasio may change no more of a pod than its busy label and annotations, of its own namespace's its own alone, and make no Job, Sandbox or Secret but as it renders them", async () => {
      const volumeId = await newSession("none");
      const grafana = await kube.runningPod(NAMESPACE, componentName("grafana"));
      const imageOf = async (namespace: string, name: string) => (await kube.get<V1Pod>(ref("Pod", name, namespace)))?.spec?.containers[0]?.image;
      const images = [await imageOf(SESSIONS, volumeId), await imageOf(NAMESPACE, grafana)];
      const merged = "application/merge-patch+json";
      const patched = "application/json-patch+json";
      const pod = (namespace: string, name: string) => `/api/v1/namespaces/${namespace}/pods/${name}`;
      // An image the API server lets a pod's container be changed to, but for the policy.
      const otherImage = [{ op: "replace", path: "/spec/containers/0/image", value: "registry.k8s.io/pause:3.10.1" }];
      assert.equal(await asAlasio("PATCH", pod(SESSIONS, volumeId), merged, { metadata: { labels: { "alasio.dev/busy": "true" }, annotations: { "karpenter.sh/do-not-disrupt": "true" } } }), 200);
      assert.equal(await asAlasio("PATCH", pod(SESSIONS, volumeId), merged, { metadata: { labels: { "alasio.dev/busy": null }, annotations: { "karpenter.sh/do-not-disrupt": null } } }), 200);
      assert.equal(await asAlasio("PATCH", pod(SESSIONS, volumeId), patched, otherImage), 422);
      assert.equal(await asAlasio("PATCH", pod(NAMESPACE, grafana), patched, otherImage), 422);
      assert.equal(await asAlasio("PATCH", pod(SESSIONS, volumeId), merged, { metadata: { labels: { "app.kubernetes.io/component": "other" } } }), 422);
      assert.equal(await asAlasio("PATCH", pod(SESSIONS, volumeId), merged, { metadata: { finalizers: ["e2e.alasio.dev/held"] } }), 422);
      assert.equal(await asAlasio("PATCH", pod(SESSIONS, volumeId), merged, { metadata: { ownerReferences: null } }), 422);
      assert.deepEqual([await imageOf(SESSIONS, volumeId), await imageOf(NAMESPACE, grafana)], images);
      // Of its own namespace's pods, its own alone: not the floor's beside it.
      const busyLabel = (busy: boolean) => ({ metadata: { labels: { "alasio.dev/busy": busy ? "true" : null } } });
      const own = await kube.runningPod(NAMESPACE, RELEASE);
      assert.equal(await asAlasio("PATCH", pod(NAMESPACE, grafana), merged, busyLabel(true)), 422);
      assert.equal(await asAlasio("PATCH", pod(NAMESPACE, own), merged, busyLabel(false)), 200);
      // A Sandbox as its session's, but under runc rather than the sessions' runtime; and as it, which is made.
      const session = await kube.get<Sandbox>(ref("Sandbox", volumeId, SESSIONS));
      assert.ok(session);
      const copied = (name: string, runtime: boolean) => {
        const text = JSON.stringify({ apiVersion: session.apiVersion, kind: session.kind, metadata: { name, namespace: SESSIONS }, spec: { ...session.spec, operatingMode: "Suspended" } })
          .replaceAll(volumeId, name);
        const sandbox: Sandbox = JSON.parse(text);
        if (!runtime) delete sandbox.spec.podTemplate.spec?.runtimeClassName;
        return sandbox;
      };
      const sandboxes = `/apis/agents.x-k8s.io/v1beta1/namespaces/${SESSIONS}/sandboxes`;
      assert.equal(await asAlasio("POST", sandboxes, "application/json", copied("e2e-runc", false)), 422);
      assert.equal(await asAlasio("POST", sandboxes, "application/json", copied("e2e-made", true)), 201);
      await kube.remove(ref("Sandbox", "e2e-made", SESSIONS));
      // A Secret that would be a ServiceAccount's token.
      const token = { apiVersion: "v1", kind: "Secret", metadata: { name: "e2e-bayma-token", annotations: { "kubernetes.io/service-account.name": "default" } }, type: "kubernetes.io/service-account-token" };
      assert.equal(await asAlasio("POST", `/api/v1/namespaces/${SESSIONS}/secrets`, "application/json", token), 422);
      // The clone's Job, as the installation gives it alasio, but as KEDA's operator, whose rights reach the whole cluster.
      const templates = await kube.get<V1ConfigMap>(ref("ConfigMap", componentName("sandbox-templates"), NAMESPACE));
      const job: V1Job = JSON.parse(templates?.data?.["templates.json"] ?? "{}").sessions.clone.job;
      const borrowed: V1Job = { ...job, metadata: { ...job.metadata, generateName: "e2e-borrowed-" } };
      if (borrowed.spec?.template.spec) borrowed.spec.template.spec.serviceAccountName = componentName("keda-operator");
      assert.equal(await asAlasio("POST", `/apis/batch/v1/namespaces/${NAMESPACE}/jobs`, "application/json", borrowed), 422);
      const jobs = await kube.list<V1Job>("Job", { namespace: NAMESPACE });
      assert.deepEqual(jobs.filter(({ metadata }) => metadata?.name?.startsWith("e2e-borrowed-")), []);
    });

    test("a session's pod is not evicted while a turn runs in it, and is once the turn ends", async () => {
      const volumeId = await newSession("none");
      await tg.say("/service codex");
      await tg.waitFor((call) => /^Active: Codex$/mu.test(call.payload.text ?? ""));
      await tg.say("wait 30");
      await until("the session busy", async () => ((await labelledBusy(SESSIONS, volumeId)) ? true : undefined), 60_000);
      // An eviction, as a drain or a node autoscaler asks one: what the API server said.
      const evict = () => {
        const eviction = { apiVersion: "policy/v1", kind: "Eviction", metadata: { name: volumeId, namespace: SESSIONS } };
        return kubectl(["create", "--raw", `/api/v1/namespaces/${SESSIONS}/pods/${volumeId}/eviction`, "-f", "-"], JSON.stringify(eviction)).said;
      };
      assert.match(evict(), /disruption budget/u);
      const uid = (await kube.get<V1Pod>(ref("Pod", volumeId, SESSIONS)))?.metadata?.uid;
      assert.ok(uid, "the session's pod is not evicted");
      await tg.waitFor((call) => call.method === "sendRichMessage" && call.payload.rich_message?.markdown === ANSWER, 120_000);
      await until("the session no longer busy", async () => (!(await labelledBusy(SESSIONS, volumeId)) ? true : undefined), 60_000);
      assert.doesNotMatch(evict(), /disruption budget|Error/u);
      // Gone, or going, or made anew by its Sandbox.
      await until("the session's pod evicted", async () => {
        const pod = await kube.get<V1Pod>(ref("Pod", volumeId, SESSIONS));
        return pod === null || pod.metadata?.deletionTimestamp !== undefined || pod.metadata?.uid !== uid ? true : undefined;
      }, 60_000);
    });

    test("a node's drain waits until the turn running in a session there ends, then evicts it", { skip: AGENTS < 2 && "a drain of the cluster's one node stops all of it" }, async () => {
      const volumeId = await newSession("none");
      const node = (await kube.get<V1Pod>(ref("Pod", volumeId, SESSIONS)))?.spec?.nodeName ?? "";
      await tg.say("/service codex");
      await tg.waitFor((call) => /^Active: Codex$/mu.test(call.payload.text ?? ""));
      await tg.say("wait 60");
      await until("the session busy", async () => ((await labelledBusy(SESSIONS, volumeId)) ? true : undefined), 60_000);
      // kubectl drain, of the session's pods alone, as the node's other pods are not this test's.
      let drained = 0;
      const draining = onNode(`${CLUSTER}-server-0`, `kubectl drain ${node} --pod-selector=app.kubernetes.io/component=session --delete-emptydir-data --timeout=300s`).then((said) => {
        drained = Date.now();
        return said;
      });
      try {
        await sleep(20_000);
        assert.equal(drained, 0, "the drain waits while the turn runs");
        assert.ok(await kube.get(ref("Pod", volumeId, SESSIONS)), "the session's pod is not evicted");
        await tg.waitFor((call) => call.method === "sendRichMessage" && call.payload.rich_message?.markdown === ANSWER, 120_000);
        const answered = Date.now();
        assert.match(await draining, /evicted/u);
        assert.ok(drained >= answered, "the session was evicted once its turn had answered");
      } finally {
        await draining.catch(() => undefined);
        await onNode(`${CLUSTER}-server-0`, `kubectl uncordon ${node}`);
      }
    });

    test("two Grafanas, one cluster of them, send each alert once", async () => {
      const grafana = await grafanaApi();
      try {
        // Its Alertmanagers' cluster, as its metrics say it, until both are of it.
        let cluster = "";
        await until("both Grafanas in one cluster", async () => {
          const metrics = String((await grafana.api("/metrics")).body);
          cluster = metrics.split("\n").filter((line) => /alertmanager_cluster_|alertmanager_peer_/u.test(line) && !line.startsWith("#")).join("\n");
          return /alertmanager_cluster_members(\{[^}]*\})? 2$/mu.test(cluster) ? true : undefined;
        }, 300_000).catch(async (error: unknown) => {
          console.error(`# Grafana's Alertmanagers' cluster:\n${cluster}`);
          for (const pod of await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: "app.kubernetes.io/component=grafana" })) {
            const logged = (await kube.logs(NAMESPACE, pod.metadata?.name ?? "", "grafana")).split("\n").filter((line) => /cluster|memberlist|gossip|peer/iu.test(line));
            console.error(`# ${pod.metadata?.name}:\n${logged.slice(-40).join("\n")}`);
          }
          throw error;
        });
        await sendFailedTurn();
        // Each user's, and two minutes more, in which a second Grafana's would have come.
        const alerts = await untilAlerted(120_000);
        assert.deepEqual([...alerts.entries()].map(([chat, texts]) => [chat, texts.length]).sort(), [["1001", 1], [String(OTHER_OPERATOR), 1]]);
      } finally {
        grafana.close();
      }
    });

    describe("Grafana, its scale made a range and fixed again", () => {
      const replicas = async () => (await kube.get<V1Deployment>(ref("Deployment", "alasio-grafana", NAMESPACE)))?.spec?.replicas;
      /** Brings alasio up with Grafana's scale `scaling`: the fewest replicas Grafana was given meanwhile. */
      const upScaled = async (scaling: { min: number; max: number }) => {
        const { configFile } = paths();
        const config = JSON.parse(readFileSync(configFile, "utf8"));
        config.install.grafana.scaling = scaling;
        writeFileSync(configFile, JSON.stringify(config));
        let fewest = Number.POSITIVE_INFINITY;
        let applying = true;
        const watching = (async () => {
          while (applying) {
            fewest = Math.min(fewest, (await replicas()) ?? 0);
            await sleep(200);
          }
        })();
        try {
          await alasioOk("up", "--timeout", "20m");
        } finally {
          applying = false;
          await watching;
        }
        return fewest;
      };

      test("keeps its two replicas as KEDA takes them over, and KEDA scales it on its CPU", async () => {
        assert.equal(await upScaled({ min: 2, max: 4 }), 2);
        assert.equal(await replicas(), 2);
        const scaledObject = await until("Grafana's ScaledObject ready", async () => {
          const found = await kube.get<KubernetesObject & { status?: { conditions?: { type: string; status: string }[] } }>(ref("ScaledObject", "alasio-grafana", NAMESPACE));
          return found?.status?.conditions?.some(({ type, status }) => type === "Ready" && status === "True") ? found : undefined;
        }, 180_000);
        assert.ok(scaledObject);
        const hpa = kedaHpa();
        assert.deepEqual(
          [hpa?.spec?.minReplicas, hpa?.spec?.maxReplicas, hpa?.spec?.metrics?.map(({ type, resource }) => [type, resource?.name, resource?.target.averageUtilization])],
          [2, 4, [["Resource", "cpu", 80]]],
        );
      });

      test("ends at its fixed count, which KEDA's scaler, gone first, does not set back", async () => {
        // Above the range's least, toward which KEDA's scaler, were it still there, would take an idle Grafana.
        await upScaled({ min: 3, max: 3 });
        assert.equal(await kube.get(ref("ScaledObject", "alasio-grafana", NAMESPACE)), null);
        assert.equal(kedaHpa(), null);
        await sleep(30_000);
        assert.equal(await replicas(), 3);
      });
    });
  });
}

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
