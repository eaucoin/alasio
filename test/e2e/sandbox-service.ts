/**
 * The sandbox service as alasio installs it (cli/src/manifests/sandbox-gateway.ts), on the
 * cluster the workspaces shard runs: a workspace made by a node agent with its quota, as a
 * Valkey user of its own that runs no command Valkey calls dangerous; the receiver reaching
 * nothing but the collector and the API server; and workspaces reached through the gateway
 * alone, by tokens of main's own key: one made, bound to a pool's pod and kept whole, every
 * write into it done, through the node's mount of workspace storage handed over to the pod
 * that replaces the one that held it; the node's mount killed with no successor, as a reboot
 * ends it, mounted again by the DaemonSet's restart, a workspace made after it and one bound
 * through it released as broken, then bound again with what it held; one released with a
 * session holding its files open restored whole from its snapshot as it is next bound, the
 * session running on; another made, given a received file, bound, held to its
 * quota, read back of what its session wrote, released, bound again, and deleted.
 *
 * Registered with sandboxService() in the workspaces shard (test/e2e/alasio.test.ts).
 */
import assert from "node:assert/strict";
import { describe, test } from "node:test";

import type { V1DaemonSet, V1Pod } from "@kubernetes/client-node";

import { selectorOf } from "../../cli/src/kube/rollout.ts";
import { componentName, NAMESPACE, selectorLabels } from "../../cli/src/manifests/common.ts";
import { PROXY_PORT } from "../../cli/src/manifests/neon.ts";
import { GATEWAY, GATEWAY_PORT, JUICEFS_STATE, RECEIVER, WORKSPACE_MOUNT } from "../../cli/src/manifests/sandbox-gateway.ts";
import { VALKEY } from "../../cli/src/manifests/workspace-storage.ts";
import { BINDING_LABEL, environmentKey, keyHash, POOL_LABEL } from "../../src/binder/names.ts";
import { mintToken } from "../../src/binder/tokens.ts";
import { NODE_AGENT_PORT, NODE_AGENT_TOKEN_VARIABLE, WORKSPACES_ROOT } from "../../src/node-agent/names.ts";
import { sessionKey } from "../../src/sandbox/names.ts";
import type { ReplAsked, ReplChecked, ReplHeld, ReplWaited } from "./gateway-repl.ts";
import { boundPodOf, gatewayCall, inAlasio, kube, mainToken, nodeAgents, onNode, ref, SESSIONS, tcp } from "./harness.ts";
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

/** Binds `workspace` through the gateway by `token`, as its caller binds it, waiting up to `timeoutMs`; what the gateway last answered, and logged of it, said where it does not. */
async function bindThrough(token: string, workspace: string, what: string, timeoutMs: number): Promise<void> {
  let last = { status: 0, said: "" };
  await until(what, async () => {
    last = await gatewayCall(token, "POST", "/bind");
    return last.status === 200 ? true : undefined;
  }, timeoutMs, async () => `; it last answered ${last.status} ${last.said}\n${await serviceLogs(workspace)}`);
}

/**
 * What `node` holds of each restore's pod's directory as a restore goes, each look that differs
 * from the one before: its entries, its workspace's, the node's mounts of it and of kubelet's
 * subpaths, and where bayma's /attach comes from, as its runtime has it; until stopped.
 */
function watchRestore(node: string): { readonly stop: () => void; readonly said: () => string[] } {
  const said: string[] = [];
  let stopped = false;
  const look = [
    "for d in /var/lib/alasio/attach/*/sandboxes-none-restore-*; do [ -d \"$d\" ] || continue; echo \"== $d\"; ls -la \"$d\" \"$d/ws\" 2>&1 | head -12; done",
    "grep -E 'alasio/attach/[^/]+/sandboxes-none-restore|volume-subpaths/attach' /proc/1/mountinfo | head -12",
    "for c in $(k3s crictl ps -a --name bayma -q 2>/dev/null | head -3); do k3s crictl inspect \"$c\" 2>/dev/null | grep -B1 -A2 '\"containerPath\": \"/attach\"' | head -8; done",
  ].join("; ");
  void (async () => {
    while (!stopped) {
      const now = await onNode(node, look).catch((error: unknown) => String(error));
      if (now.includes("== ") && now !== said.at(-1)) said.push(now);
      await new Promise((resolve) => setTimeout(resolve, 500));
    }
  })();
  return { stop: () => void (stopped = true), said: () => said.slice(-6) };
}

/** The pod of main's sessions bound to `workspace`, once it is: its name and node. */
async function boundPod(workspace: string): Promise<{ readonly name: string; readonly node: string }> {
  // Not one being deleted, as a restore's that failed is, which binds nothing more.
  const bound = await boundPodOf(workspace);
  assert.ok(bound, `no pod is bound to ${workspace}\n${await serviceLogs(workspace)}`);
  return bound;
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
  return mintToken(environmentKey(master, branch), { key: sessionKey(workspace), expires: Date.now() + 9 * 60_000, options: { environment: branch, mode: "none" } });
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
  await until("the gateway binding the branch's workspace", async () => ((await gatewayCall(token, "POST", "/bind")).status === 200 ? true : undefined), 300_000);
  const sessions = `alasio-branch-${branch}-sessions`;
  const [bound] = await kube.list<V1Pod>("Pod", { namespace: sessions, labelSelector: `${BINDING_LABEL}=${keyHash(sessionKey(workspace))}` });
  assert.ok(bound?.metadata?.name, "no pod of the branch's is bound to its workspace");
  assert.equal((await gatewayCall(token, "POST", "/release")).status, 204);
  await until("the gateway deleting the branch's workspace", async () => ((await gatewayCall(token, "DELETE", "/workspace")).status === 204 ? true : undefined), 180_000);
  return { namespace: bound.metadata.namespace ?? "" };
}

/** Binds the workspace `workspace` of the branch environment `branch` through main's gateway, by the branch's own key, as its alasio would: the pod it is bound to. */
export async function bindBranchSession(branch: string, workspace: string): Promise<string> {
  const token = await branchToken(branch, workspace);
  await until(`the gateway binding the branch's ${workspace}`, async () => ((await gatewayCall(token, "POST", "/bind")).status === 200 ? true : undefined), 300_000);
  const [bound] = await kube.list<V1Pod>("Pod", { namespace: `alasio-branch-${branch}-sessions`, labelSelector: `${BINDING_LABEL}=${keyHash(sessionKey(workspace))}` });
  assert.ok(bound?.metadata?.name, `no pod of the branch's is bound to its ${workspace}`);
  return bound.metadata.name;
}

/** What the gateway and the node agents logged of `workspace`, for a failure to say. */
export async function serviceLogs(workspace: string): Promise<string> {
  const pods = [
    ...(await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf(selectorLabels("sandbox-gateway")) })).map((pod) => [pod, "sandbox-gateway"] as const),
    ...(await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf(selectorLabels("node-agent")) })).map((pod) => [pod, "node-agent"] as const),
  ];
  const said = await Promise.all(pods.map(async ([pod, container]) => {
    const logged = await kube.logs(NAMESPACE, pod.metadata?.name ?? "", container).catch((error: unknown) => String(error));
    return `${pod.metadata?.name}:\n${logged.split("\n").filter((line) => line.includes(workspace)).slice(-40).join("\n")}`;
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

    test("the node agents take the sandbox gateway's calls alone: alasio's pod reaches none of them, the gateway's each", async () => {
      const [alasio, gateway] = await Promise.all([kube.runningPod(NAMESPACE, "alasio"), kube.runningPod(NAMESPACE, GATEWAY)]);
      const agents = await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf(selectorLabels("node-agent")) });
      assert.ok(agents.length > 0, "no node agent runs");
      for (const { metadata, status } of agents) {
        assert.ok(status?.podIP, `${metadata?.name} has no address`);
        const from = async (pod: string, container: string) =>
          (await kube.execOk(NAMESPACE, pod, ["node", "-e", tcp(JSON.stringify(status.podIP), NODE_AGENT_PORT)], { container })).trim();
        assert.equal(await from(gateway, "sandbox-gateway"), "open", `the gateway does not reach ${metadata?.name}`);
        assert.equal(await from(alasio, "alasio"), "blocked", `alasio reaches ${metadata?.name}`);
      }
    });

    test("a workspace made and bound to a pool's pod through the gateway keeps every write into it as the node's mount of workspace storage is handed over to the pod that replaces the one that held it", async () => {
      const workspace = "fs-e2etakeover";
      // Made and bound as alasio makes and binds it: a caller's token of the key, signed by main's key, through the gateway alone.
      const token = await mainToken(workspace);
      await until("the gateway making the workspace", async () => ((await gatewayCall(token, "POST", "/workspace")).status === 204 ? true : undefined), 120_000);
      const made = await agentCall("GET", `/workspaces/${SESSIONS}/${workspace}/quota`);
      assert.equal(made.status, 200, made.said);
      await until("the gateway binding the workspace", async () => ((await gatewayCall(token, "POST", "/bind")).status === 200 ? true : undefined), 180_000);
      const { name: pod, node } = await boundPod(workspace);
      // Written into all through the handover, each write's outcome said.
      const writing = kube.exec(SESSIONS, pod, ["sh", "-c", WRITER("/workspace/takeover", 75)], { container: "bayma" });
      await new Promise((resolve) => setTimeout(resolve, 10_000));
      const mountPods = async () =>
        (await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf(selectorLabels("workspace-mount")) })).filter(({ spec }) => spec?.nodeName === node);
      const [before] = await mountPods();
      await kube.patch(ref("DaemonSet", WORKSPACE_MOUNT, NAMESPACE), { spec: { template: { metadata: { annotations: { "alasio.dev/e2e-restartedAt": new Date().toISOString() } } } } });
      // What the old pod logged, and when it turned unready, kept as it goes, as its pod, once deleted, keeps neither.
      let oldLogged = "";
      const oldReadiness: string[] = [];
      await until("the node's mount handed over to a new pod, and the old pod gone", async () => {
        const pods = await mountPods();
        const old = pods.find(({ metadata }) => metadata?.uid === before?.metadata?.uid);
        if (old) {
          oldLogged = await kube.logs(NAMESPACE, old.metadata?.name ?? "", "workspace-mount").catch(() => oldLogged);
          const ready = old.status?.conditions?.find(({ type }) => type === "Ready");
          const said = `${ready?.status} since ${ready?.lastTransitionTime}`;
          if (oldReadiness.at(-1) !== said) oldReadiness.push(said);
        }
        const set = await kube.get<V1DaemonSet>(ref("DaemonSet", WORKSPACE_MOUNT, NAMESPACE));
        const ready = pods.filter(({ metadata, status }) => metadata?.uid !== before?.metadata?.uid && status?.conditions?.some(({ type, status: is }) => type === "Ready" && is === "True"));
        return ready.length === 1 && !old && (set?.status?.updatedNumberScheduled ?? 0) >= 1 ? true : undefined;
      }, 240_000).catch((error: unknown) => {
        console.error(`# the old mount's pod ${before?.metadata?.name}, ready ${oldReadiness.join(", then ")}, logged:\n${oldLogged.split("\n").slice(-80).join("\n")}`);
        throw error;
      });
      const written = await writing;
      const outcomes = written.stdout.trim().split("\n");
      const failed = outcomes.filter((line) => line.startsWith("fail"));
      if (failed.length > 0) {
        // What each mount's pod and juicefs said, and what the node mounts, for the failure to say.
        const mounted = await onNode(node, `grep -E ' ${WORKSPACES_ROOT}| fuse' /proc/1/mountinfo; ls -la ${JUICEFS_STATE}; ps -eo pid,ppid,etime,args | grep '[j]uicefs-'`).catch((error: unknown) => String(error));
        const logged = await Promise.all((await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf(selectorLabels("workspace-mount")) })).map(async ({ metadata, spec }) =>
          `${metadata?.name} on ${spec?.nodeName}:\n${(await kube.logs(NAMESPACE, metadata?.name ?? "", "workspace-mount").catch((error: unknown) => String(error))).split("\n").slice(-60).join("\n")}`
        ));
        console.error(`# the node's mounts:\n${mounted}\n# the mounts' pods:\n${logged.join("\n")}`);
      }
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

    test("a handover whose old mount answers nothing, its juicefs held up, leaves the node's mount served by a new pod within its juicefs's wait and its restart, the old mount's FUSE connection aborted, so a process waiting on it where no signal ends it ends, and the mount unmounted", { timeout: 20 * 60_000 }, async () => {
      const mountPods = async () => kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf(selectorLabels("workspace-mount")) });
      const [before] = await mountPods();
      assert.ok(before?.spec?.nodeName && before.metadata?.name);
      const node = before.spec.nodeName;
      // A process of the node's holding a file of the mount open, as a session's does: once the mount answers nothing,
      // KILLed, its exit's flush of that file waits on a request no process will answer, where no signal ends it, as
      // a takeover's juicefs's did in CI run 40158785689. Started on its own, so it outlives the exec that starts it.
      const holder = (await kube.execOk(NAMESPACE, before.metadata.name, ["sh", "-c", `setsid nsenter -t 1 -m -- sh -c 'exec 3< ${WORKSPACES_ROOT}/.config; exec sleep 3600' > /dev/null 2>&1 < /dev/null & echo $!`], { container: "workspace-mount" })).trim();
      assert.match(holder, /^\d+$/u);
      /** Whether the holder's process is there still, and how its threads are: none, where it is gone. */
      const holding = (pod: string) => kube.execOk(NAMESPACE, pod, ["sh", "-c", `cat /proc/${holder}/task/*/stat 2>/dev/null | sed 's/.*) //' | cut -d ' ' -f 1 | tr '\\n' ' '`], { container: "workspace-mount" }).then((states) => states.trim());
      // Its juicefs held up, as one that answers nothing is: its supervisor and the mount process it runs stopped, the
      // FUSE session theirs still, so the node's mount answers no request, as in CI run 39965151183.
      const stop = [
        "supervisor=$(cat /run/alasio/juicefs.pid)",
        'for proc in /proc/[0-9]*; do [ "$(sed "s/.*) //" "$proc/stat" 2>/dev/null | cut -d " " -f 2)" = "$supervisor" ] && kill -STOP "${proc#/proc/}"; done',
        'kill -STOP "$supervisor"',
      ].join("\n");
      await kube.execOk(NAMESPACE, before.metadata.name, ["sh", "-c", stop], { container: "workspace-mount" });
      // KILLed, the holder stays, waiting uninterruptibly on its flush.
      await kube.execOk(NAMESPACE, before.metadata.name, ["kill", "-KILL", holder], { container: "workspace-mount" });
      await new Promise((resolve) => setTimeout(resolve, 3_000));
      const held = await holding(before.metadata.name);
      assert.match(held, /D/u, `the holder, KILLed, is not waiting uninterruptibly: ${held || "gone"}`);
      await kube.patch(ref("DaemonSet", WORKSPACE_MOUNT, NAMESPACE), { spec: { template: { metadata: { annotations: { "alasio.dev/e2e-restartedAt": new Date().toISOString() } } } } });
      // Served again by a pod of the new template alone: the old deleted once the new is ready.
      await until("the node's mount served by a new pod alone", async () => {
        const pods = (await mountPods()).filter(({ spec }) => spec?.nodeName === node);
        const ready = pods.filter(({ status }) => status?.conditions?.some(({ type, status: is }) => type === "Ready" && is === "True"));
        return ready.length === 1 && pods.every(({ metadata }) => metadata?.uid !== before.metadata?.uid) ? true : undefined;
      }, 12 * 60_000).catch(async (error: unknown) => {
        for (const pod of (await mountPods()).filter(({ spec }) => spec?.nodeName === node)) {
          const logged = await kube.logs(NAMESPACE, pod.metadata?.name ?? "", "workspace-mount").catch(String);
          console.error(`# the mount's pod ${pod.metadata?.name} logged:\n${logged.split("\n").slice(-60).join("\n")}`);
        }
        throw error;
      });
      // And a workspace is made through it, as the node agent sees the node's mount.
      let made = { status: 0, said: "" };
      for (const deadline = Date.now() + 60_000; Date.now() < deadline; await new Promise((resolve) => setTimeout(resolve, 2_000))) {
        made = await gatewayCall(await mainToken("fs-e2eanswersnothing"), "POST", "/workspace");
        if (made.status === 204) break;
      }
      assert.equal(made.status, 204, made.said);
      // The holder ended, its flush failed as the old mount's FUSE connection was aborted before it was unmounted.
      const [now] = (await mountPods()).filter(({ spec, metadata }) => spec?.nodeName === node && !metadata?.deletionTimestamp);
      assert.equal(await holding(now?.metadata?.name ?? ""), "", "the holder, its flush unanswered, is there still");
    });

    test("a node's mount of workspace storage killed with no successor, as a node's reboot ends it, is mounted again by the DaemonSet's restart: a workspace is made after it, and one bound through it is released as broken, then bound again with what it held", async () => {
      const workspace = "fs-e2ereboot";
      const token = await mainToken(workspace);
      await until("the gateway making the workspace", async () => ((await gatewayCall(token, "POST", "/workspace")).status === 204 ? true : undefined), 120_000);
      await until("the gateway binding the workspace", async () => ((await gatewayCall(token, "POST", "/bind")).status === 200 ? true : undefined), 180_000);
      const bound = await boundPod(workspace);
      await kube.execOk(SESSIONS, bound.name, ["sh", "-c", "printf 'kept through it' > /workspace/before && sync"], { container: "bayma" });
      const mountPods = async () =>
        (await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf(selectorLabels("workspace-mount")) })).filter(({ spec }) => spec?.nodeName === bound.node);
      const [before] = await mountPods();
      assert.ok(before?.metadata?.name, "the node runs no mount's pod");
      await kube.kill(NAMESPACE, before.metadata.name);
      await until("the DaemonSet's new pod serving the node's mount", async () =>
        (await mountPods()).some(({ metadata, status }) => metadata?.uid !== before.metadata?.uid && status?.conditions?.some(({ type, status: is }) => type === "Ready" && is === "True")) ? true : undefined, 240_000);
      // A workspace made after it.
      await until("the gateway making a workspace after it", async () => ((await gatewayCall(await mainToken("fs-e2erebooted"), "POST", "/workspace")).status === 204 ? true : undefined), 60_000);
      // The bound one, its workspace unreachable in its pod, released as broken.
      await until("the bound pod released", async () =>
        (await kube.list<V1Pod>("Pod", { namespace: SESSIONS, labelSelector: `${BINDING_LABEL}=${keyHash(sessionKey(workspace))}` })).length === 0 ? true : undefined, 180_000, async () => `\nits node's agent says of what is attached: ${(await agentCall("GET", "/attachments")).said}\n${await serviceLogs(workspace)}`);
      assert.match(await serviceLogs(workspace), /workspace is unreachable in its pod/u);
      await bindThrough(token, workspace, "the gateway binding the workspace again", 180_000);
      const again = await boundPod(workspace);
      assert.notEqual(again.name, bound.name);
      assert.equal(await kube.execOk(SESSIONS, again.name, ["cat", "/workspace/before"], { container: "bayma" }), "kept through it");
      assert.equal((await gatewayCall(token, "POST", "/release")).status, 204);
    });

    test("a node's agent started as the node's mount of workspace storage serves sees the mount made anew once that one's juicefs is gone: a workspace is made after it", async () => {
      const of = (component: string) => async () => await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf(selectorLabels(component)) });
      const [mount] = await of("workspace-mount")();
      const node = mount?.spec?.nodeName ?? "";
      const [agent] = (await of("node-agent")()).filter(({ spec }) => spec?.nodeName === node);
      assert.ok(mount?.metadata?.name && agent?.metadata?.name, "the node runs a mount's pod and an agent");
      const readyAnew = (pods: readonly V1Pod[], before: V1Pod) =>
        pods.some(({ metadata, spec, status }) => spec?.nodeName === node && metadata?.uid !== before.metadata?.uid && status?.conditions?.some(({ type, status: is }) => type === "Ready" && is === "True"));
      const made = (workspace: string) => async () => ((await gatewayCall(await mainToken(workspace), "POST", "/workspace")).status === 204 ? true : undefined);
      // The agent started again while the node's mount serves, as a rollout of it starts one.
      await kube.kill(NAMESPACE, agent.metadata.name);
      await until("the node's agent started again", async () => (readyAnew(await of("node-agent")(), agent) ? true : undefined), 300_000);
      await until("the gateway making a workspace through it", made("fs-e2ebefore"), 120_000);
      // Then the mount's juicefs killed with no successor: the DaemonSet's next pod mounts anew, the agent running on.
      await kube.kill(NAMESPACE, mount.metadata.name);
      await until("the node's mount made anew", async () => (readyAnew(await of("workspace-mount")(), mount) ? true : undefined), 300_000);
      await until("the gateway making a workspace after it", made("fs-e2eanew"), 120_000);
    });

    test("a workspace released with a session holding its files open, a writer mid-write, an SQLite transaction under way and a git index.lock among them, is restored whole from its snapshot as it is next bound, the session running on, its files held", async (t) => {
      const workspace = "fs-e2eheld";
      const token = await mainToken(workspace);
      await until("the gateway making the workspace", async () => ((await gatewayCall(token, "POST", "/workspace")).status === 204 ? true : undefined), 120_000);
      await until("the gateway binding the workspace", async () => ((await gatewayCall(token, "POST", "/bind")).status === 200 ? true : undefined), 180_000);
      const first = await boundPod(workspace);
      const pod = await kube.get<V1Pod>(ref("Pod", first.name, SESSIONS));
      if (pod?.spec?.runtimeClassName === undefined) return t.skip("the pools run under no gVisor here, so nothing is snapshotted");
      // A session of its bayma's, reached through the gateway as alasio reaches it.
      const url = `http://${GATEWAY}.${NAMESPACE}.svc.cluster.local:${GATEWAY_PORT}/mcp`;
      const held = await inAlasio<ReplHeld>("./gateway-repl.ts", "hold", url, token);
      assert.ok(held.step > 5, `its writer at step ${held.step}`);
      // Released mid-write: saved whole into its snapshot.
      const released = await gatewayCall(token, "POST", "/release");
      assert.equal(released.status, 204, `${released.said}\n${await serviceLogs(workspace)}`);
      await until("the released pod gone", async () => ((await kube.get<V1Pod>(ref("Pod", first.name, SESSIONS))) === null ? true : undefined), 180_000);
      // Bound again: restored, by a pod of its own, the session running on from where it was, its files held.
      const seen = watchRestore(first.node);
      await bindThrough(token, workspace, "the gateway binding the workspace again", 300_000).finally(seen.stop);
      const restored = await boundPod(workspace);
      assert.match(restored.name, /^sandboxes-none-restore-/u, `${await serviceLogs(workspace)}\nas its node held its restore's directory:\n${seen.said().join("\n---\n")}`);
      const checked = await inAlasio<ReplChecked>("./gateway-repl.ts", "check", url, token, held.sessionId);
      assert.deepEqual(
        { writing: checked.writing, whole: checked.whole, committed: checked.committed, integrity: checked.integrity, locked: checked.locked },
        { writing: true, whole: true, committed: 2, integrity: "ok", locked: true },
        JSON.stringify(checked),
      );
      assert.ok(checked.step > held.step && checked.lines >= checked.step - 1, JSON.stringify(checked));
      assert.equal((await gatewayCall(token, "POST", "/release")).status, 204);
    });

    test("a workspace bound through its node's agent started again stays bound to its pod, its session running on, its files held, as what is attached outlives the agent", async () => {
      const workspace = "fs-e2eagentagain";
      const token = await mainToken(workspace);
      await until("the gateway making the workspace", async () => ((await gatewayCall(token, "POST", "/workspace")).status === 204 ? true : undefined), 120_000);
      await until("the gateway binding the workspace", async () => ((await gatewayCall(token, "POST", "/bind")).status === 200 ? true : undefined), 180_000);
      const first = await boundPod(workspace);
      const url = `http://${GATEWAY}.${NAMESPACE}.svc.cluster.local:${GATEWAY_PORT}/mcp`;
      const held = await inAlasio<ReplHeld>("./gateway-repl.ts", "hold", url, token);
      const agent = (await nodeAgents()).get(first.node);
      assert.ok(agent, `no node agent runs ready on ${first.node}`);
      await kube.remove(ref("Pod", agent, NAMESPACE));
      await until(`the node agent ${agent} gone`, async () => ((await kube.get(ref("Pod", agent, NAMESPACE))) === null ? true : undefined), 300_000);
      await until(`a node agent ready again on ${first.node}`, async () => ((await nodeAgents()).has(first.node) ? true : undefined), 300_000);
      // Through the gateway's looks at the bound pods meanwhile, and two after it.
      await new Promise((resolve) => setTimeout(resolve, 30_000));
      assert.equal((await boundPod(workspace)).name, first.name, await serviceLogs(workspace));
      const checked = await inAlasio<ReplChecked>("./gateway-repl.ts", "check", url, token, held.sessionId);
      assert.deepEqual(
        { writing: checked.writing, whole: checked.whole, locked: checked.locked, notice: checked.notice },
        { writing: true, whole: true, locked: true, notice: null },
        JSON.stringify(checked),
      );
      assert.ok(checked.step > held.step, JSON.stringify(checked));
      const released = await gatewayCall(token, "POST", "/release");
      assert.equal(released.status, 204, `${released.said}\n${await serviceLogs(workspace)}`);
      await until("the gateway deleting the workspace", async () => ((await gatewayCall(token, "DELETE", "/workspace")).status === 204 ? true : undefined), 180_000);
    });

    test("a call through the gateway under way as it is started again is let end, one made as its pod is deleted is answered or refused at once, never left to hang, and its binding survives it, its session running on", async () => {
      const workspace = "fs-e2egatewayagain";
      const token = await mainToken(workspace);
      await until("the gateway making the workspace", async () => ((await gatewayCall(token, "POST", "/workspace")).status === 204 ? true : undefined), 120_000);
      await until("the gateway binding the workspace", async () => ((await gatewayCall(token, "POST", "/bind")).status === 200 ? true : undefined), 180_000);
      const first = await boundPod(workspace);
      const url = `http://${GATEWAY}.${NAMESPACE}.svc.cluster.local:${GATEWAY_PORT}/mcp`;
      const held = await inAlasio<ReplHeld>("./gateway-repl.ts", "hold", url, token);
      const gateways = () => kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf(selectorLabels("sandbox-gateway")) });
      const before = new Set((await gateways()).map(({ metadata }) => metadata?.uid));
      // A call of 10 s under way through it as its pods are deleted, which it lets end as it
      // stops (src/binder/server.ts STOP_CALLS_MS); and one made as they are, which a gateway
      // stopping, or none, answers or refuses at once, never one gone.
      const waiting = inAlasio<ReplWaited>("./gateway-repl.ts", "wait", url, token, held.sessionId, "10");
      await new Promise((resolve) => setTimeout(resolve, 5_000));
      for (const { metadata } of await gateways()) await kube.remove(ref("Pod", metadata?.name ?? "", NAMESPACE));
      const asked = await inAlasio<ReplAsked>("./gateway-repl.ts", "ask", url, token);
      const waited = await waiting;
      console.log(`# the call made as the gateway's pods were deleted, a call under way: ${JSON.stringify(asked)}`);
      assert.equal(waited.answered, true, `the call under way was cut, not let end: ${waited.failure}`);
      assert.ok(asked.ms < 15_000, `the call made as the gateway's pods were deleted came back only after ${asked.ms} ms: ${JSON.stringify(asked)}`);
      await until("a gateway of its own ready again", async () =>
        (await gateways()).some(({ metadata, status }) => !before.has(metadata?.uid) && status?.conditions?.some(({ type, status: is }) => type === "Ready" && is === "True")) ? true : undefined, 300_000);
      // Its binding as it was, its pod's session running on, reached through the new gateway.
      assert.equal((await boundPod(workspace)).name, first.name, await serviceLogs(workspace));
      const checked = await inAlasio<ReplChecked>("./gateway-repl.ts", "check", url, token, held.sessionId);
      assert.deepEqual({ writing: checked.writing, whole: checked.whole, notice: checked.notice }, { writing: true, whole: true, notice: null }, JSON.stringify(checked));
      // Its pods deleted again, no call under way to hold it, its linger alone there to refuse.
      const again = new Set((await gateways()).map(({ metadata }) => metadata?.uid));
      for (const { metadata } of await gateways()) await kube.remove(ref("Pod", metadata?.name ?? "", NAMESPACE));
      const idleAsked = await inAlasio<ReplAsked>("./gateway-repl.ts", "ask", url, token);
      console.log(`# the call made as the gateway's pods were deleted, no call under way: ${JSON.stringify(idleAsked)}`);
      assert.ok(idleAsked.ms < 15_000, `the call made as the gateway's pods were deleted came back only after ${idleAsked.ms} ms: ${JSON.stringify(idleAsked)}`);
      await until("a gateway of its own ready again", async () =>
        (await gateways()).some(({ metadata, status }) => !again.has(metadata?.uid) && status?.conditions?.some(({ type, status: is }) => type === "Ready" && is === "True")) ? true : undefined, 300_000);
      // Asked again where refused, as its new pod, ready, is not yet where the Service's routes
      // lead, which change a moment after: as alasio's loopback passes a request refused so on
      // again (src/mcp/bayma-proxy.ts).
      const refused = (error: unknown) => /exited with 7: curl: \(7\)/u.test(String(error));
      await until("the gateway releasing the workspace", async () =>
        (await gatewayCall(token, "POST", "/release").catch((error: unknown) => (refused(error) ? null : Promise.reject(error))))?.status === 204 ? true : undefined, 60_000);
      await until("the gateway deleting the workspace", async () => ((await gatewayCall(token, "DELETE", "/workspace")).status === 204 ? true : undefined), 180_000);
    });

    test("a workspace is made, given a received file, bound, held to its quota, read back of what its session wrote, released and deleted through the gateway alone", async () => {
      const workspace = "fs-e2eclaim";
      const token = await mainToken(workspace);
      const inSandbox = async (script: string) => kube.execOk(SESSIONS, (await boundPod(workspace)).name, ["sh", "-c", script], { container: "bayma" });
      await until("the gateway making the workspace", async () => ((await gatewayCall(token, "POST", "/workspace")).status === 204 ? true : undefined), 120_000);
      // A received file, as alasio writes one into the workspace before the turn that names it:
      // asked again while the gateway answers 503, as the node's mount does not hold the
      // workspace's new quota yet (src/sandbox/gateway-client.ts writeFile).
      const received = await until("the gateway writing the received file", async () => {
        const answer = await gatewayCall(token, "PUT", "/workspace/files?path=workspace/received/note.txt", "a file received on Telegram\n");
        if (answer.status !== 503) return answer;
        assert.equal(answer.retryAfter, "2", `a write refused for a moment says when to ask again: ${answer.said}`);
        return undefined;
      }, 60_000);
      assert.equal(received.status, 204, received.said);
      // Claimed: a pool's pod bound to it, the file in its sandbox.
      await until("the gateway binding the workspace", async () => ((await gatewayCall(token, "POST", "/bind")).status === 200 ? true : undefined), 180_000);
      const claimed = await boundPod(workspace);
      const claimedGvisor = (await kube.get<V1Pod>(ref("Pod", claimed.name, SESSIONS)))?.spec?.runtimeClassName !== undefined;
      assert.equal(await inSandbox("cat /workspace/received/note.txt"), "a file received on Telegram\n");
      // Held to its quota, the pool's sessions' size, 1Gi in the run, from its first write: its node's mount held the quota before it was attached.
      const quota = await inSandbox([
        "cd /workspace",
        'fallocate -l 900M quota-a && echo "took 900M"',
        'fallocate -l 300M quota-b 2>&1 && echo "took 300M more"',
        "rm -f quota-a quota-b",
      ].join("; "));
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
      const seen = watchRestore(claimed.node);
      await bindThrough(token, workspace, "the gateway binding the workspace again", 180_000).finally(seen.stop);
      // Restored from the snapshot it was released into, by a pod of its own, where its pool's run under gVisor, and are snapshotted.
      if (claimedGvisor) assert.match((await boundPod(workspace)).name, /^sandboxes-none-restore-/u, `${await serviceLogs(workspace)}\nas its node held its restore's directory:\n${seen.said().join("\n---\n")}`);
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
