/**
 * The sandbox service as alasio installs it (cli/src/manifests/sandbox-gateway.ts), on the
 * cluster the workspaces shard runs: a workspace made by a node agent with its quota, as a
 * Valkey user of its own that runs no command Valkey calls dangerous; the receiver reaching
 * nothing but the collector and the API server; and workspaces reached through the gateway
 * alone, by tokens of main's own key: one made, bound to a pool's pod and kept whole, every
 * write into it done, through the node's mount of workspace storage handed over to the pod
 * that replaces the one that held it; another made, given a received file, bound, held to its
 * quota, read back of what its session wrote, released, bound again, and deleted.
 *
 * Registered with sandboxService() in the workspaces shard (test/e2e/alasio.test.ts).
 */
import assert from "node:assert/strict";
import { describe, test } from "node:test";

import type { V1DaemonSet, V1Pod } from "@kubernetes/client-node";

import { selectorOf } from "../../cli/src/kube/rollout.ts";
import { componentName, NAMESPACE, RELEASE, selectorLabels } from "../../cli/src/manifests/common.ts";
import { PROXY_PORT } from "../../cli/src/manifests/neon.ts";
import { GATEWAY, GATEWAY_PORT, RECEIVER, WORKSPACE_MOUNT } from "../../cli/src/manifests/sandbox-gateway.ts";
import { VALKEY } from "../../cli/src/manifests/workspace-storage.ts";
import { BINDING_LABEL, environmentKey, keyHash, POOL_LABEL } from "../../src/binder/names.ts";
import { NO_BIND_HEADER } from "../../src/binder/server.ts";
import { mintToken } from "../../src/binder/tokens.ts";
import { NODE_AGENT_PORT, NODE_AGENT_TOKEN_VARIABLE, WORKSPACES_ROOT } from "../../src/node-agent/names.ts";
import { sessionKey } from "../../src/sandbox/names.ts";
import { kube, nodeAgents, ref, SESSIONS, tcp } from "./harness.ts";
import { until } from "./workspace-storage.ts";

/** What a node agent answers `method path` with, `body` as JSON: its status, and what it said. */
async function agentCall(method: string, path: string, body?: unknown): Promise<{ readonly status: number; readonly said: string }> {
  const [agent] = (await nodeAgents()).values();
  assert.ok(agent, "no node agent runs");
  const data = body === undefined ? "" : ` -H 'content-type: application/json' --data '${JSON.stringify(body)}'`;
  const printed = await kube.execOk(NAMESPACE, agent, [
    "sh",
    "-c",
    `curl -sS -w '\\n%{http_code}' -X ${method} -H "authorization: Bearer $${NODE_AGENT_TOKEN_VARIABLE}"${data} http://127.0.0.1:${NODE_AGENT_PORT}${path}`,
  ], { container: "node-agent" });
  const lines = printed.trimEnd().split("\n");
  return { status: Number(lines.at(-1)), said: lines.slice(0, -1).join("\n") };
}

/** A caller's token of the workspace `workspace` of main's sessions, signed by main's own key, as main's alasio signs one. */
async function mainToken(workspace: string): Promise<string> {
  const callerKey = await kube.secret(NAMESPACE, GATEWAY, "caller-key");
  return mintToken(environmentKey(Buffer.from(callerKey), "main"), { key: sessionKey(workspace), expires: Date.now() + 600_000, options: { environment: "main", mode: "none" } });
}

/** What the gateway answers `method path` with, asked as alasio asks it, from its pod, with `token`, `body` sent as it is: its status, and what it said. */
async function gatewayCall(token: string, method: string, path: string, body?: string, { bind = true } = {}): Promise<{ readonly status: number; readonly said: string }> {
  const alasioPod = await kube.runningPod(NAMESPACE, RELEASE);
  const printed = await kube.execOk(NAMESPACE, alasioPod, [
    "sh",
    "-c",
    `curl -sS -w '\n%{http_code}' -X ${method} -H "authorization: Bearer $1"${bind ? "" : ` -H '${NO_BIND_HEADER}: never'`}${body === undefined ? "" : " --data-binary @-"} "http://${GATEWAY}.${NAMESPACE}.svc:${GATEWAY_PORT}$2"`,
    "sh",
    token,
    path,
  ], { container: "alasio", ...(body === undefined ? {} : { stdin: body }) });
  const lines = printed.split("\n");
  return { status: Number(lines.at(-1)), said: lines.slice(0, -1).join("\n") };
}

/** The pod of main's sessions bound to `workspace`, once it is: its name and node. */
async function boundPod(workspace: string): Promise<{ readonly name: string; readonly node: string }> {
  const [bound] = await kube.list<V1Pod>("Pod", { namespace: SESSIONS, labelSelector: `${BINDING_LABEL}=${keyHash(sessionKey(workspace))}` });
  assert.ok(bound?.metadata?.name, `no pod is bound to ${workspace}`);
  return { name: bound.metadata.name, node: bound.spec?.nodeName ?? "" };
}

/** The components of main's sandbox service, in alasio's namespace, which serve every environment. */
const SERVICE_COMPONENTS = ["sandbox-gateway", "receiver", "workspace-mount", "node-agent"] as const;

/**
 * What of main's sandbox service runs, as a branch environment made or deleted must leave it:
 * each pod of its gateway, receiver, nodes' mount and node agents, and of main's pools, by its
 * uid and its containers' restarts, and each of their workloads by its generation.
 */
export async function mainSandboxService(): Promise<string[]> {
  const pods = [
    ...(await Promise.all(SERVICE_COMPONENTS.map((component) => kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf(selectorLabels(component)) })))).flat(),
    ...(await kube.list<V1Pod>("Pod", { namespace: SESSIONS, labelSelector: POOL_LABEL })),
  ];
  assert.ok(pods.some(({ metadata }) => metadata?.labels?.["app.kubernetes.io/component"] === "sandbox-gateway"), "the sandbox gateway does not run");
  const restarts = ({ status }: V1Pod) => (status?.containerStatuses ?? []).reduce((sum, { restartCount }) => sum + restartCount, 0);
  const workloads = [
    ...[GATEWAY, RECEIVER].map((name) => ref("Deployment", name, NAMESPACE)),
    ...[WORKSPACE_MOUNT, componentName("node-agent")].map((name) => ref("DaemonSet", name, NAMESPACE)),
    ...["sandboxes-none", "sandboxes-full"].map((name) => ref("Deployment", name, SESSIONS)),
  ];
  const generations = await Promise.all(workloads.map(async (each) => `${each.kind} ${each.name}: generation ${(await kube.get(each))?.metadata?.generation}`));
  return [...pods.map((pod) => `Pod ${pod.metadata?.namespace}/${pod.metadata?.name} ${pod.metadata?.uid}: ${restarts(pod)} restarts`).sort(), ...generations];
}

/** A token of a workspace of the branch environment `branch`, by its own key: as branch create wrote it, or, once it is deleted, derived as one kept would be. */
async function branchToken(branch: string, workspace: string): Promise<string> {
  const master = Buffer.from(await kube.secret(NAMESPACE, GATEWAY, "caller-key"));
  const written = await kube.secret(`alasio-branch-${branch}`, componentName("sessions-caller"), "caller-key").catch(() => null);
  if (written !== null) assert.deepEqual(Buffer.from(written), environmentKey(master, branch));
  return mintToken(environmentKey(master, branch), { key: sessionKey(workspace), expires: Date.now() + 600_000, options: { environment: branch, mode: "none" } });
}

/** Whether main's gateway serves the branch environment `branch`: its answer to a request that binds nothing, 409 where it does, 403 where it does not. */
export async function servesBranch(branch: string): Promise<{ readonly status: number; readonly said: string }> {
  return gatewayCall(await branchToken(branch, "fs-e2ebranch"), "GET", "/status", undefined, { bind: false });
}

/**
 * Binds a workspace of the branch environment `branch` through main's gateway, by a token of
 * the branch's own key, as its alasio signs one, then releases and deletes it: the namespace of
 * the pod it was bound to.
 */
export async function bindInBranch(branch: string): Promise<{ readonly namespace: string }> {
  const workspace = "fs-e2ebranch";
  const token = await branchToken(branch, workspace);
  const made = await gatewayCall(token, "POST", "/workspace");
  assert.equal(made.status, 204, made.said);
  await until("the gateway binding the branch's workspace", async () => ((await gatewayCall(token, "POST", "/bind")).status === 204 ? true : undefined), 300_000);
  const sessions = `alasio-branch-${branch}-sessions`;
  const [bound] = await kube.list<V1Pod>("Pod", { namespace: sessions, labelSelector: `${BINDING_LABEL}=${keyHash(sessionKey(workspace))}` });
  assert.ok(bound?.metadata?.name, "no pod of the branch's is bound to its workspace");
  assert.equal((await gatewayCall(token, "POST", "/release")).status, 204);
  await until("the gateway deleting the branch's workspace", async () => ((await gatewayCall(token, "DELETE", "/workspace")).status === 204 ? true : undefined), 180_000);
  return { namespace: bound.metadata.namespace ?? "" };
}

/** What the gateway and the node agents logged of `workspace`, for a failure to say. */
async function serviceLogs(workspace: string): Promise<string> {
  const pods = [
    ...(await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf(selectorLabels("sandbox-gateway")) })).map((pod) => [pod, "sandbox-gateway"] as const),
    ...(await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf(selectorLabels("node-agent")) })).map((pod) => [pod, "node-agent"] as const),
  ];
  const said = await Promise.all(pods.map(async ([pod, container]) => {
    const logged = await kube.logs(NAMESPACE, pod.metadata?.name ?? "", container).catch((error: unknown) => String(error));
    return `${pod.metadata?.name}:\n${logged.split("\n").filter((line) => line.includes(workspace)).slice(-20).join("\n")}`;
  }));
  return said.join("\n");
}

/** Makes the workspace `workspace` of the sessions' namespace through a node agent, of `quotaBytes`. */
async function createWorkspace(workspace: string, quotaBytes: number): Promise<void> {
  const made = await agentCall("POST", "/workspaces", { environment: SESSIONS, workspace, quotaBytes });
  assert.equal(made.status, 204, made.said);
}

/** A writer, in a sandbox: a file a step into `directory` for `seconds`, each write's outcome a line of its own, `ok <n>` or `fail <n> <why>`. */
const WRITER = (directory: string, seconds: number) =>
  [
    `mkdir -p ${directory}`,
    `end=$(( $(date +%s) + ${seconds} ))`,
    "i=0",
    'while [ "$(date +%s)" -lt "$end" ]; do',
    "  i=$((i + 1))",
    `  if echo "$i" > ${directory}/$i 2>/tmp/written; then echo "ok $i"; else echo "fail $i $(cat /tmp/written)"; fi`,
    "  sleep 0.05",
    "done",
  ].join("\n");

export function sandboxService(): void {
  describe("the sandbox service", () => {
    test("a node agent makes a workspace with its quota, which it reads back, as a Valkey user of its own that runs no command Valkey calls dangerous", async () => {
      await createWorkspace("fs-e2equota", 2 * 1024 ** 3);
      const read = await agentCall("GET", `/workspaces/${SESSIONS}/fs-e2equota/quota`);
      assert.equal(read.status, 200, read.said);
      const quota = JSON.parse(read.said) as { capacityBytes: number; usedBytes: number };
      assert.deepEqual([quota.capacityBytes, quota.usedBytes >= 0], [2 * 1024 ** 3, true]);
      // Its user may not flush, list every key, read the server's configuration, nor its users.
      // (DEBUG, which the server refuses everyone, says nothing of it.) Quoted, as a shell runs
      // them: `*` a pattern of Valkey's, not of the shell's files.
      for (const command of ["FLUSHDB", "KEYS '*'", "CONFIG GET maxmemory", "ACL LIST"]) {
        const ran = await kube.exec(NAMESPACE, `${VALKEY}-0`, ["sh", "-c", `valkey-cli --no-auth-warning --user quotas --pass "$VALKEY_QUOTAS_PASSWORD" -n 1 ${command}`], { container: "valkey" });
        assert.match(`${ran.stdout}${ran.stderr}`, /NOPERM/u, command);
      }
    });

    test("the receiver reaches nothing but the cluster's DNS, the collector and the API server", async () => {
      const receiver = await kube.runningPod(NAMESPACE, RECEIVER);
      const reach = async (host: string, port: number) => (await kube.execOk(NAMESPACE, receiver, ["node", "-e", tcp(JSON.stringify(host), port)], { container: "receiver" })).trim();
      assert.equal(await reach(`${componentName("collector")}.${NAMESPACE}.svc`, 4318), "open");
      assert.equal(await reach("kubernetes.default.svc", 443), "open");
      for (const [host, port] of [[`${VALKEY}.${NAMESPACE}.svc`, 6379], [`${GATEWAY}.${NAMESPACE}.svc`, GATEWAY_PORT], [`${componentName("neon-proxy")}.${NAMESPACE}.svc`, PROXY_PORT], ["1.1.1.1", 443]] as const) {
        assert.equal(await reach(host, port), "blocked", `${host}:${port}`);
      }
    });

    test("a workspace made and bound to a pool's pod through the gateway keeps every write into it as the node's mount of workspace storage is handed over to the pod that replaces the one that held it", async () => {
      const workspace = "fs-e2etakeover";
      // Made and bound as alasio makes and binds it: a caller's token of the key, signed by main's key, through the gateway alone.
      const token = await mainToken(workspace);
      await until("the gateway making the workspace", async () => ((await gatewayCall(token, "POST", "/workspace")).status === 204 ? true : undefined), 120_000);
      const made = await agentCall("GET", `/workspaces/${SESSIONS}/${workspace}/quota`);
      assert.equal(made.status, 200, made.said);
      await until("the gateway binding the workspace", async () => ((await gatewayCall(token, "POST", "/bind")).status === 204 ? true : undefined), 180_000);
      const { name: pod, node } = await boundPod(workspace);
      // Written into all through the handover, each write's outcome said.
      const writing = kube.exec(SESSIONS, pod, ["sh", "-c", WRITER("/workspace/takeover", 75)], { container: "bayma" });
      await new Promise((resolve) => setTimeout(resolve, 10_000));
      const mountPods = async () =>
        (await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf(selectorLabels("workspace-mount")) })).filter(({ spec }) => spec?.nodeName === node);
      const [before] = await mountPods();
      await kube.patch(ref("DaemonSet", WORKSPACE_MOUNT, NAMESPACE), { spec: { template: { metadata: { annotations: { "alasio.dev/e2e-restartedAt": new Date().toISOString() } } } } });
      await until("the node's mount handed over to a new pod, and the old pod gone", async () => {
        const pods = await mountPods();
        const set = await kube.get<V1DaemonSet>(ref("DaemonSet", WORKSPACE_MOUNT, NAMESPACE));
        const ready = pods.filter(({ metadata, status }) => metadata?.uid !== before?.metadata?.uid && status?.conditions?.some(({ type, status: is }) => type === "Ready" && is === "True"));
        return ready.length === 1 && !pods.some(({ metadata }) => metadata?.uid === before?.metadata?.uid) && (set?.status?.updatedNumberScheduled ?? 0) >= 1 ? true : undefined;
      }, 240_000);
      const written = await writing;
      const outcomes = written.stdout.trim().split("\n");
      const failed = outcomes.filter((line) => line.startsWith("fail"));
      assert.deepEqual(failed, [], `of ${outcomes.length} writes`);
      assert.ok(outcomes.length > 500, `${outcomes.length} writes`);
      const kept = (await kube.execOk(SESSIONS, pod, ["sh", "-c", "ls /workspace/takeover | wc -l"], { container: "bayma" })).trim();
      assert.equal(Number(kept), outcomes.length);
      // The node's mount whole after it: a workspace made since is made in workspace storage, as the node agent sees it, and the node.
      const agent = (await nodeAgents()).get(node) ?? "";
      const seen = async () => {
        const mount = (await kube.exec(NAMESPACE, agent, ["sh", "-c", `stat -f -c 'agent sees %T' ${WORKSPACES_ROOT}; grep -E ' ${WORKSPACES_ROOT}| fuse' /proc/1/mountinfo || echo 'the node mounts nothing there'`], { container: "node-agent" })).stdout.trim();
        // What the mount's pod said of the node's mount, beside juicefs's own.
        const logged = await Promise.all((await mountPods()).map(async ({ metadata }) =>
          `${metadata?.name}: ${(await kube.logs(NAMESPACE, metadata?.name ?? "", "workspace-mount").catch((error: unknown) => String(error))).split("\n").filter((line) => !/juicefs\[\d+\]/u.test(line)).join("\n")}`
        ));
        return [mount, ...logged].join("\n");
      };
      // Within the mount's look, once a second, at whether the node has lost its mount.
      let after = { status: 0, said: "" };
      for (const deadline = Date.now() + 30_000; Date.now() < deadline; await new Promise((resolve) => setTimeout(resolve, 2_000))) {
        after = await gatewayCall(await mainToken("fs-e2eafter"), "POST", "/workspace");
        if (after.status === 204) break;
      }
      assert.equal(after.status, 204, `${after.said}; ${await seen()}`);
    });

    test("a workspace is made, given a received file, bound, held to its quota, read back of what its session wrote, released and deleted through the gateway alone", async () => {
      const workspace = "fs-e2eclaim";
      const token = await mainToken(workspace);
      const inSandbox = async (script: string) => kube.execOk(SESSIONS, (await boundPod(workspace)).name, ["sh", "-c", script], { container: "bayma" });
      await until("the gateway making the workspace", async () => ((await gatewayCall(token, "POST", "/workspace")).status === 204 ? true : undefined), 120_000);
      // A received file, as alasio writes one into the workspace before the turn that names it.
      const received = await gatewayCall(token, "PUT", "/workspace/files?path=workspace/received/note.txt", "a file received on Telegram\n");
      assert.equal(received.status, 204, received.said);
      // Claimed: a pool's pod bound to it, the file in its sandbox.
      await until("the gateway binding the workspace", async () => ((await gatewayCall(token, "POST", "/bind")).status === 204 ? true : undefined), 180_000);
      const claimed = await boundPod(workspace);
      assert.equal(await inSandbox("cat /workspace/received/note.txt"), "a file received on Telegram\n");
      // Held to its quota, the pool's sessions' size, 1Gi in the run, once the node's mount has read it, as JuiceFS's client reads its quotas anew every few seconds.
      const fill = () =>
        inSandbox([
          "cd /workspace",
          'fallocate -l 900M quota-a && echo "took 900M"',
          'fallocate -l 300M quota-b 2>&1 && echo "took 300M more"',
          "rm -f quota-a quota-b",
        ].join("; "));
      let quota = await fill();
      for (const deadline = Date.now() + 60_000; !/quota exceeded/iu.test(quota) && Date.now() < deadline; quota = await fill()) await new Promise((resolve) => setTimeout(resolve, 5_000));
      assert.match(quota, /^took 900M$/mu);
      assert.match(quota, /quota exceeded/iu);
      assert.doesNotMatch(quota, /took 300M more/u);
      // A reply's media, as the session wrote it, read back as alasio reads it to send.
      await inSandbox("mkdir -p /workspace/out && printf 'a chart, drawn' > /workspace/out/reply.txt");
      const reply = await gatewayCall(token, "GET", "/workspace/files?path=workspace/out/reply.txt");
      assert.deepEqual([reply.status, reply.said], [200, "a chart, drawn"]);
      // Released as its caller asks: its pod gone, and its binding, its workspace kept.
      const released = await gatewayCall(token, "POST", "/release");
      assert.equal(released.status, 204, `${released.said}\n${await serviceLogs(workspace)}`);
      await until("the released pod gone", async () => ((await kube.get<V1Pod>(ref("Pod", claimed.name, SESSIONS))) === null ? true : undefined), 180_000);
      assert.deepEqual(await kube.list<V1Pod>("Pod", { namespace: SESSIONS, labelSelector: `${BINDING_LABEL}=${keyHash(sessionKey(workspace))}` }), []);
      assert.equal((await gatewayCall(token, "GET", "/workspace/quota")).status, 200);
      // Bound again, to another pod, with what it held.
      await until("the gateway binding the workspace again", async () => ((await gatewayCall(token, "POST", "/bind")).status === 204 ? true : undefined), 180_000);
      assert.notEqual((await boundPod(workspace)).name, claimed.name);
      assert.equal(await inSandbox("cat /workspace/out/reply.txt"), "a chart, drawn");
      // Deleted once released, and not before.
      assert.equal((await gatewayCall(token, "DELETE", "/workspace")).status, 409);
      assert.equal((await gatewayCall(token, "POST", "/release")).status, 204);
      await until("the gateway deleting the workspace", async () => ((await gatewayCall(token, "DELETE", "/workspace")).status === 204 ? true : undefined), 180_000);
      assert.equal((await gatewayCall(token, "GET", "/workspace/quota")).status, 404);
    });
  });
}
