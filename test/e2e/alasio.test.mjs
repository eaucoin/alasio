/**
 * alasio on Kubernetes, end to end: an installed release driven through the Telegram
 * stand-in as its operator drives it, its sessions' confinement checked from inside
 * them, its own code paths run in its pod with its ServiceAccount, and its telemetry
 * looked for where the deployment's goes.
 *
 * Runs against a release that test/e2e/run.sh installed: KUBECONFIG names the cluster,
 * ALASIO_E2E_NAMESPACE and ALASIO_E2E_RELEASE the release (alasio and alasio unless set);
 * the stand-ins run in the namespace alasio-test. Needs kubectl.
 */
import assert from "node:assert/strict";
import { execFile, spawn } from "node:child_process";
import { readFileSync } from "node:fs";
import { createServer } from "node:net";
import { after, before, describe, test } from "node:test";
import { promisify } from "node:util";

const run = promisify(execFile);
const NAMESPACE = process.env.ALASIO_E2E_NAMESPACE ?? "alasio";
const RELEASE = process.env.ALASIO_E2E_RELEASE ?? "alasio";
const FULL = RELEASE.includes("alasio") ? RELEASE : `${RELEASE}-alasio`;
const SESSIONS = process.env.ALASIO_E2E_SESSIONS_NAMESPACE ?? "alasio-sessions";
const STUBS = "alasio-test";
const skip = !process.env.KUBECONFIG && "needs a cluster with a release installed: set KUBECONFIG";

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const kubectl = async (namespace, ...args) => (await run("kubectl", ["--namespace", namespace, ...args], { maxBuffer: 64 * 1024 * 1024 })).stdout;

async function freePort() {
  const server = createServer();
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address();
  await new Promise((resolve) => server.close(resolve));
  return port;
}

/** A local port forwarded to a stand-in's Service, once it answers. */
async function forward(service, remote) {
  const port = await freePort();
  const child = spawn("kubectl", ["--namespace", STUBS, "port-forward", `service/${service}`, `${port}:${remote}`], { stdio: "ignore" });
  const base = `http://127.0.0.1:${port}`;
  for (let attempt = 0; attempt < 60; attempt++) {
    if (await fetch(`${base}/control/${service === "telegram" ? "calls" : "exports"}`).then(() => true, () => false)) return { base, stop: () => child.kill() };
    await sleep(500);
  }
  throw new Error(`the ${service} stand-in did not answer`);
}

/** The operator, through the Telegram stand-in: says things, presses buttons, reads replies. */
function operator(base) {
  let seen = 0;
  const post = async (path, body) => (await fetch(`${base}${path}`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) })).json();
  const calls = async () => {
    const listed = await (await fetch(`${base}/control/calls?since=${seen}`)).json();
    seen = listed.total;
    return listed.calls;
  };
  const buttons = (call) => (call?.payload?.reply_markup?.inline_keyboard ?? []).flat();
  return {
    buttons,
    say: (text) => post("/control/message", { text }),
    press: (data) => post("/control/callback", { data }),
    calls,
    /** What alasio sends until `predicate` holds for one, which is returned. */
    async waitFor(predicate, timeoutMs = 300_000) {
      const deadline = Date.now() + timeoutMs;
      const got = [];
      while (Date.now() < deadline) {
        got.push(...(await calls()));
        const hit = got.find(predicate);
        if (hit) return hit;
        await sleep(500);
      }
      throw new Error(`alasio sent nothing that matched; it sent ${JSON.stringify(got.map((call) => [call.method, call.payload.text?.slice(0, 120)]))}`);
    },
    /** Presses the button labelled like `label` in what alasio sends next. */
    async choose(label) {
      const call = await this.waitFor((candidate) => buttons(candidate).some((button) => label.test(button.text)));
      await this.press(buttons(call).find((button) => label.test(button.text)).callback_data);
    },
  };
}

let telegram;
let sink;
let tg;

/** Creates a session filesystem through /workspace, as the operator does: its volume id. */
async function newSession(net) {
  await tg.calls();
  await tg.say("/workspace");
  await tg.choose(/New empty workspace/u);
  await tg.choose(net === "full" ? /Full internet/u : /No internet/u);
  const created = await tg.waitFor((call) => /Created and mounted empty workspace/u.test(call.payload.text ?? ""));
  return /fs-[0-9a-f]+/u.exec(created.payload.text)[0];
}

/** Node run in a session's bayma container: its stdout. */
const inSession = (volumeId, code) => kubectl(SESSIONS, "exec", volumeId, "-c", "bayma", "--", "node", "-e", code);
const probe = (target) => `fetch(${JSON.stringify(target)},{signal:AbortSignal.timeout(5000)}).then(r=>console.log("open"),e=>console.log("blocked"))`;
const tcp = (host, port) => `const s=require("net").connect({host:${host},port:${port},timeout:3000});s.on("connect",()=>{console.log("open");process.exit()});s.on("timeout",()=>{console.log("blocked");process.exit()});s.on("error",()=>{console.log("blocked");process.exit()})`;

/** Runs one of test/e2e's scripts in alasio's pod, with alasio's code and ServiceAccount: its last line, parsed. */
async function inAlasio(script, ...args) {
  const child = spawn("kubectl", ["--namespace", NAMESPACE, "exec", "-i", `deployment/${FULL}`, "-c", "alasio", "--", "sh", "-c", 'cd /opt/alasio && node --input-type=module - "$@"', "node", ...args]);
  child.stdin.end(readFileSync(new URL(script, import.meta.url)));
  let stdout = "";
  child.stdout.on("data", (chunk) => { stdout += chunk; });
  const code = await new Promise((resolve) => child.on("exit", resolve));
  assert.equal(code, 0, stdout);
  return JSON.parse(stdout.trim().split("\n").at(-1));
}

const roundtrip = (volumeId) => inAlasio("./session-roundtrip.mjs", volumeId);

before(async () => {
  if (skip) return;
  await kubectl(NAMESPACE, "rollout", "status", `deployment/${FULL}`, "--timeout=600s");
  telegram = await forward("telegram", 8081);
  sink = await forward("otlp", 4318).catch(() => null);
  tg = operator(telegram.base);
  await tg.waitFor((call) => call.method === "setMyCommands", 300_000).catch(() => {});
});

after(() => {
  telegram?.stop();
  sink?.stop();
});

describe("alasio on Kubernetes", { skip }, () => {
  let none;
  let full;

  test("a new empty workspace without internet is a session of its own, confined", async () => {
    none = await newSession("none");
    const runtime = (await kubectl(SESSIONS, "get", "pod", none, "-o", "jsonpath={.spec.runtimeClassName}")).trim();
    if (runtime === "gvisor") assert.match(await inSession(none, 'console.log(require("fs").readFileSync("/proc/version","utf8"))'), /gvisor/u);
    assert.equal((await inSession(none, probe("http://1.1.1.1"))).trim(), "blocked");
    assert.equal((await inSession(none, tcp("process.env.KUBERNETES_SERVICE_HOST", 443))).trim(), "blocked");
    assert.match(await inSession(none, 'require("dns").promises.lookup("example.com").then(()=>console.log("resolved"),()=>console.log("no dns"))'), /no dns/u);
    assert.match(await inSession(none, 'console.log(require("fs").existsSync("/var/run/secrets/kubernetes.io/serviceaccount"))'), /false/u);
    const gate = await kubectl(SESSIONS, "get", "pod", none, "-o", "jsonpath={.status.initContainerStatuses[?(@.name==\"egress-gate\")].state.terminated.exitCode}");
    assert.equal(gate.trim(), "0");
  });

  test("a new workspace with internet reaches the internet and nothing private", async () => {
    full = await newSession("full");
    assert.equal((await inSession(full, probe("https://example.com"))).trim(), "open");
    assert.equal((await inSession(full, tcp("process.env.KUBERNETES_SERVICE_HOST", 443))).trim(), "blocked");
    const other = (await kubectl(SESSIONS, "get", "pod", none, "-o", "jsonpath={.status.podIP}")).trim();
    assert.equal((await inSession(full, probe(`http://${other}:7290/mcp`))).trim(), "blocked");
    assert.match(await inSession(full, `require("dns").promises.lookup("${none}.${SESSIONS}.svc.cluster.local").then(()=>console.log("resolved"),()=>console.log("unresolved"))`), /unresolved/u);
  });

  test("a session's bayma answers alasio alone, and only with the session's token", async () => {
    const url = `http://${none}.${SESSIONS}.svc.cluster.local:7290/mcp`;
    const token = Buffer.from(await kubectl(SESSIONS, "get", "secret", `${none}-bayma-token`, "-o", "jsonpath={.data.token}"), "base64").toString();
    const fromAlasio = async (headers) => (await kubectl(NAMESPACE, "exec", `deployment/${FULL}`, "-c", "alasio", "--", "curl", "-s", "-o", "/dev/null", "-w", "%{http_code}", "--max-time", "5", ...headers, url)).trim();
    assert.equal(await fromAlasio([]), "401");
    assert.equal(await fromAlasio(["-H", "Authorization: Bearer wrong"]), "401");
    assert.equal(await fromAlasio(["-H", `Authorization: Bearer ${token}`]), "400");
    const fromStub = (await kubectl(STUBS, "exec", "deployment/telegram-stub", "--", "node", "-e", probe(url))).trim();
    assert.equal(fromStub, "blocked");
  });

  test("alasio reads a session's files as its agent, and a suspended session resumes with them", async () => {
    const seen = await roundtrip(none);
    assert.equal(seen.exec, '"written"');
    assert.match(seen.read, /^hello from /u);
    assert.equal(seen.missing, "file not found");
    assert.equal(seen.suspendedPod, "gone");
    assert.equal(seen.whileSuspended, "the session is not running");
    assert.equal(seen.afterResume, seen.read);
    assert.equal(seen.execAfterResume, JSON.stringify(seen.read));
  });

  test("a session's telemetry reaches the deployment's backend, stamped with the session", { skip: !process.env.ALASIO_E2E_TELEMETRY && "the release exports no telemetry" }, async () => {
    const deadline = Date.now() + 120_000;
    let stamped = [];
    while (Date.now() < deadline) {
      stamped = (await (await fetch(`${sink.base}/control/exports?contains=${none}`)).json()).filter((entry) => entry.contains);
      if (stamped.some((entry) => entry.signal === "traces")) break;
      await sleep(3000);
    }
    assert.ok(stamped.some((entry) => entry.signal === "traces"), "no trace of the session's bayma arrived stamped with it");
  });

  test("a folder conversation's bayma works on the machine as the operator, in their home", async () => {
    const { url, seen } = await inAlasio("./folder-bayma.mjs");
    assert.match(url, /^http:\/\/bayma-[0-9a-f]{20}\.alasio-host\.svc/u);
    assert.deepEqual(seen, { uid: 1000, home: "/work" });
    // One node shares its /tmp between alasio and the folder's bayma, as a machine does.
    if (process.env.AGENTS === "0" || !process.env.AGENTS) {
      const proof = await kubectl(NAMESPACE, "exec", `deployment/${FULL}`, "-c", "alasio", "--", "cat", "/work/e2e-folder-proof");
      assert.equal(proof, "written by 1000");
    }
  });

  test("alasio comes back from a rollout restart with its conversation's workspace", async () => {
    await kubectl(NAMESPACE, "rollout", "restart", `deployment/${FULL}`);
    await kubectl(NAMESPACE, "rollout", "status", `deployment/${FULL}`, "--timeout=600s");
    await tg.waitFor((call) => call.method === "setMyCommands", 300_000);
    await tg.say("/workspace");
    const panel = await tg.waitFor((call) => /Folder: sessionfs:/u.test(call.payload.text ?? ""));
    assert.match(panel.payload.text, new RegExp(`sessionfs:${full}`, "u"));
  });
});
