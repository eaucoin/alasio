/**
 * The sandbox service as alasio installs it (cli/src/manifests/sandbox-gateway.ts), on the
 * cluster the workspaces shard runs: a workspace made by a node agent with its quota, as a
 * Valkey user of its own that runs no command Valkey calls dangerous; the receiver reaching
 * nothing but the collector and the API server; and a workspace made and bound to a pool's pod
 * through the gateway alone, by a token of main's own key, kept whole, every write into it done, through the node's mount of workspace
 * storage handed over to the pod that replaces the one that held it.
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
import { BINDING_LABEL, environmentKey, keyHash } from "../../src/binder/names.ts";
import { mintToken } from "../../src/binder/tokens.ts";
import { NODE_AGENT_PORT, NODE_AGENT_TOKEN_VARIABLE } from "../../src/node-agent/names.ts";
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
      // Its user may not flush, list every key, nor read or change the server's configuration.
      // Quoted, as a shell runs them: `*` a pattern of Valkey's, not of the shell's files.
      for (const command of ["FLUSHDB", "KEYS '*'", "CONFIG GET maxmemory", "DEBUG SLEEP 0"]) {
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
      const key = sessionKey(workspace);
      // Made and bound as alasio makes and binds it: a caller's token of the key, signed by main's key, through the gateway alone.
      const callerKey = await kube.secret(NAMESPACE, GATEWAY, "caller-key");
      const token = mintToken(environmentKey(Buffer.from(callerKey), "main"), { key, expires: Date.now() + 600_000, options: { environment: "main", mode: "none" } });
      const alasioPod = await kube.runningPod(NAMESPACE, RELEASE);
      const gatewayAsk = (path: string) =>
        kube.exec(NAMESPACE, alasioPod, ["sh", "-c", `curl -sS -o /dev/null -w '%{http_code}' -X POST -H 'authorization: Bearer ${token}' http://${GATEWAY}.${NAMESPACE}.svc:${GATEWAY_PORT}${path}`], { container: "alasio" });
      await until("the gateway making the workspace", async () => ((await gatewayAsk("/workspace")).stdout.trim() === "204" ? true : undefined), 120_000);
      const made = await agentCall("GET", `/workspaces/${SESSIONS}/${workspace}/quota`);
      assert.equal(made.status, 200, made.said);
      await until("the gateway binding the workspace", async () => ((await gatewayAsk("/bind")).stdout.trim() === "204" ? true : undefined), 180_000);
      const [bound] = await kube.list<V1Pod>("Pod", { namespace: SESSIONS, labelSelector: `${BINDING_LABEL}=${keyHash(key)}` });
      const pod = bound?.metadata?.name ?? "";
      assert.ok(pod, "no pod is bound to the workspace");
      // Written into all through the handover, each write's outcome said.
      const writing = kube.exec(SESSIONS, pod, ["sh", "-c", WRITER("/workspace/takeover", 75)], { container: "bayma" });
      await new Promise((resolve) => setTimeout(resolve, 10_000));
      const mountPods = async () =>
        (await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf(selectorLabels("workspace-mount")) })).filter(({ spec }) => spec?.nodeName === bound?.spec?.nodeName);
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
    });
  });
}
