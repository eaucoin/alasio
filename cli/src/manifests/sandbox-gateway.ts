/**
 * The sandbox service (src/gateway/, src/binder/): session workspaces' sandboxes as pools
 * of pods already running, each bound to one workspace while it has work, through the
 * gateway.
 *
 * - The gateway: alasio's image, as its own program, the one door to a pool's pod, of main's
 *   sessions and of every branch environment's, which it finds by their namespaces; its
 *   configuration, rendered here; its keys, made by the stack's setup
 *   (neon/control/kube-setup.ts); a Service of it, which alasio binds through and KEDA asks
 *   each pool's counts of, and a headless one its replicas find each other by. In each
 *   environment's sessions' namespace (sandboxPoolObjects) it may bind, claim and delete the
 *   pods of its pools, hold their Leases, and read their Deployments, whose templates a
 *   release's snapshot records; and it finds the node agents.
 * - A pool of each of a session's internets, `none` and `full`, in each environment: a
 *   Deployment in its sessions' namespace of pods of the agent image under the sessions' runtime, each its own directory
 *   on its node (src/node-agent/names.ts ATTACH_ROOT, by `subPathExpr` of its name) at /attach,
 *   where its workspace is attached once it is bound, its bayma started once its binding's
 *   token is beside it (sandbox/agent/start.sh). KEDA keeps `min` free, and makes more, to
 *   `max`, for the binds that wait; a bound pod is out of its Deployment, which replaces it.
 *   A pod that ends is ended: it serves one binding. Its template changes as a whole: the old
 *   free pods go as the new come, as none is ready before it is bound. A bound pod is evicted
 *   by no drain (./disruption.ts boundBudget).
 * - The session telemetry receiver (src/receiver/): alasio's image, as its own program,
 *   watching the pools' Leases of every environment, which bound pods' bayma exports to.
 * - The node's mount of workspace storage, a pod of each node's own, held through upgrades of
 *   the node agent, which attaches workspaces from it: a new one takes its mount over before
 *   the old goes.
 */
import { isDeepStrictEqual } from "node:util";

import type {
  KubernetesObject,
  V1ClusterRole,
  V1ClusterRoleBinding,
  V1ConfigMap,
  V1DaemonSet,
  V1Deployment,
  V1EnvVar,
  V1PodTemplateSpec,
  V1PolicyRule,
  V1Role,
  V1RoleBinding,
  V1Service,
  V1ServiceAccount,
} from "@kubernetes/client-node";
import { Schema } from "effect";

import { freeLabels, type Pool } from "../../../src/binder/names.ts";
import { MAIN_ENVIRONMENT_NAME } from "../../../src/branch/names.ts";
import {
  BINDING_KEY_VARIABLE,
  CALLER_KEY_VARIABLE,
  EVENTS_BROKERS_VARIABLE,
  EVENTS_PASSWORD_FILE_VARIABLE,
  EVENTS_USERNAME_VARIABLE,
  GATEWAY_CONFIG_VARIABLE,
  GatewayConfig as GatewayConfigSchema,
  type GatewayConfig,
  RECEIVER_PORT_VARIABLE,
  SESSIONS_NAMESPACE_VARIABLE,
  templateHash,
} from "../../../src/gateway/names.ts";
import { BAYMA_CONTAINER, BAYMA_STOP_SECONDS, CPU_FEATURES_ANNOTATION, NET_MODE_LABEL, SESSION_TEMPLATE_ANNOTATION, STOP_WAIT_ARG } from "../../../src/kube/sandbox-shape.ts";
import { ATTACH_ROOT, gvisorRuntimeClass, NODE_AGENT_PORT, NODE_AGENT_TOKEN_VARIABLE, WORKSPACES_ROOT } from "../../../src/node-agent/names.ts";
import { memoryBytes } from "../capacity.ts";
import { imageReference } from "../images.ts";
import { READY_ONCE_BOUND_ANNOTATION } from "../kube/rollout.ts";
import { type Carried, type CpuBaseline, type Kept, type PreviousPools, sessionsNamespace, stopWaitSeconds } from "./alasio.ts";
import {
  componentName,
  type Environment,
  given,
  imagePullSecrets,
  labels,
  MAIN_ENVIRONMENT,
  NAMESPACE,
  nodeAgentRuns,
  otelEnv,
  RELEASE,
  replicasOf,
  restrictedContainer,
  restrictedPod,
  selectorLabels,
  sha256,
} from "./common.ts";
import { sandboxPodPolicy } from "./admission.ts";
import { SANDBOX_CONTROLLER_USER } from "./agent-sandbox.ts";
import { durationSeconds, type InstallConfig } from "./config.ts";
import { LOG_BROKERS, logPrincipalSecret } from "./log.ts";
import { boundBudget } from "./disruption.ts";
import { priority } from "./priorities.ts";
import { scaledObject } from "./scalers.ts";
import { verticalPodAutoscaler } from "./sizing.ts";
import { JUICEFS_ADMIN, WORKSPACES_CREDENTIALS } from "./workspace-storage.ts";

/** The gateway's name, its pods' component, and its Service's; the headless Service its replicas find each other by. */
export const GATEWAY = componentName("sandbox-gateway");
export const GATEWAY_COMPONENT = "sandbox-gateway";
const GATEWAY_PEERS = componentName("sandbox-gateway-peers");
/** The port the gateway serves on. */
export const GATEWAY_PORT = 8080;
/** The receiver's name and its pods' component. */
export const RECEIVER = componentName("receiver");
export const RECEIVER_COMPONENT = "receiver";
/** The node's mount of workspace storage, its DaemonSet's name and its pods' component. */
export const WORKSPACE_MOUNT = componentName("workspace-mount");
export const WORKSPACE_MOUNT_COMPONENT = "workspace-mount";
/** The port every pool pod's bayma serves MCP on. */
export const POOL_POD_PORT = 7290;
/** Each pool's name, by its sessions' internet. */
export const POOLS = { none: "sandboxes-none", full: "sandboxes-full" } as const;
/** The component of a pool's pods: a session's. */
export const POOL_COMPONENT = "session";

/** Whether the sandbox service runs: for session workspaces on workspace storage, where the node agents run, which attach them, and Neon's setup makes its keys. */
export function sandboxServiceRuns(config: InstallConfig): boolean {
  return config.workspaceStorage.enabled && nodeAgentRuns(config);
}

/** The pools of `environment`, by its sessions' internet. */
export const poolsOf = (config: InstallConfig, environment: Environment): Record<keyof typeof POOLS, Pool> => {
  const namespace = sessionsNamespace(config, environment);
  return { none: { name: POOLS.none, namespace }, full: { name: POOLS.full, namespace } };
};

/**
 * Each sandbox pool's template before the current one, as the gateway's configuration it
 * replaces, `liveGateway` (its ConfigMap's `config.json`), carried it: the one it rendered, where
 * the template changes, or else the one it carried; none where it rendered none, as where its
 * sessions were not snapshotted. A template is carried for one change of it, and no more, as
 * alasio's sessions' profile is (./alasio.ts carriedFrom); and pinned into the sandbox pods'
 * policy for the restores of the snapshots taken on it, read back from the cluster: the
 * ConfigMap is that trust's anchor, which only the operator, by `alasio up`, writes. Pure, for tests.
 */
export function carriedPools(config: InstallConfig, liveGateway: string | undefined, kept: Kept): PreviousPools {
  const none: PreviousPools = { none: null, full: null };
  if (!sandboxServiceRuns(config) || !poolsSnapshotted(config, kept.cpuBaseline) || liveGateway === undefined) return none;
  const held = Schema.decodeUnknownOption(Schema.fromJsonString(GatewayConfigSchema))(liveGateway);
  if (held._tag === "None" || held.value.snapshots === null) return none;
  const pools = poolsOf(config, MAIN_ENVIRONMENT);
  const carry = (mode: keyof typeof POOLS): V1PodTemplateSpec | null => {
    const { current, previous } = held.value.snapshots?.templates[mode] ?? { current: {}, previous: null };
    const now = JSON.parse(JSON.stringify(poolTemplate(config, pools[mode], mode, kept.cpuBaseline))) as unknown;
    return (isDeepStrictEqual(current, now) ? previous : current) as V1PodTemplateSpec | null;
  };
  return { none: carry("none"), full: carry("full") };
}

/** Whether the sandbox pools' sessions are snapshotted: where the node agents run, which save them, and the nodes' CPU baseline is recorded, which they are held to. */
export const poolsSnapshotted = (config: InstallConfig, cpuBaseline: CpuBaseline): cpuBaseline is readonly string[] => nodeAgentRuns(config) && cpuBaseline !== null;

/**
 * The template of a pool's pods, as the module says: a session's, under its internet `mode`.
 * Where its sessions are snapshotted (poolsSnapshotted), as main's are by the gateway, it holds
 * them to the nodes' CPU baseline `cpuBaseline`, its bayma holds a stop it is not given for the
 * gateway's save (sessions.snapshots.stopWait), and it records its own hash
 * (SESSION_TEMPLATE_ANNOTATION), which a snapshot taken on it names it by. Every environment's
 * pools have the same.
 */
export function poolTemplate(config: InstallConfig, pool: Pool, mode: keyof typeof POOLS, cpuBaseline: CpuBaseline): V1PodTemplateSpec {
  const template = unhashedPoolTemplate(config, pool, mode, cpuBaseline);
  if (!poolsSnapshotted(config, cpuBaseline)) return template;
  return { ...template, metadata: { ...template.metadata, annotations: { ...template.metadata?.annotations, [SESSION_TEMPLATE_ANNOTATION]: templateHash(template) } } };
}

/** A pool's template, as poolTemplate says, but its hash. */
function unhashedPoolTemplate(config: InstallConfig, pool: Pool, mode: keyof typeof POOLS, cpuBaseline: CpuBaseline): V1PodTemplateSpec {
  const { sessions } = config;
  const agent = imageReference(config.images.agent);
  const snapshotted = poolsSnapshotted(config, cpuBaseline);
  return {
    metadata: {
      labels: { "app.kubernetes.io/part-of": "alasio", "app.kubernetes.io/instance": RELEASE, "app.kubernetes.io/component": POOL_COMPONENT, [NET_MODE_LABEL]: mode, ...freeLabels(pool) },
      // Under gVisor, /tmp is a tmpfs of the sandbox's own, by gVisor's mount hints, rather
      // than the emptyDir's directory on the node: a snapshot of the sandbox holds it.
      annotations: {
        "dev.gvisor.spec.mount.tmp.share": "container",
        "dev.gvisor.spec.mount.tmp.type": "tmpfs",
        "dev.gvisor.spec.mount.tmp.options": "rw",
        ...(snapshotted ? { [CPU_FEATURES_ANNOTATION]: cpuBaseline.join(",") } : {}),
      },
    },
    spec: {
      ...(sessions.runtimeClassName ? { runtimeClassName: sessions.runtimeClassName } : {}),
      ...priority("turn"),
      // Ended by other than the gateway, bayma holds its stop for the gateway's save, then stops.
      ...(snapshotted ? { terminationGracePeriodSeconds: stopWaitSeconds(config) + BAYMA_STOP_SECONDS } : {}),
      securityContext: restrictedPod(1000, 1000),
      automountServiceAccountToken: false,
      enableServiceLinks: false,
      // Names resolved as its internet says, as a session's are: by none but itself without one.
      dnsPolicy: "None",
      dnsConfig: { nameservers: mode === "full" ? [...sessions.fullModeNameservers] : ["127.0.0.1"] },
      ...imagePullSecrets(config),
      ...given("nodeSelector", { ...sessions.nodeSelector }),
      ...given("tolerations", [...sessions.tolerations]),
      containers: [{
        name: BAYMA_CONTAINER,
        image: agent,
        imagePullPolicy: config.images.pullPolicy,
        // bayma, and its hook as a release's snapshot runs it, once its workspace is attached.
        command: ["/usr/bin/tini", "-s", "--", "/usr/local/bin/alasio-start", "node", "/opt/bayma/bayma.js"],
        args: [
          "mcp-http",
          "--host",
          "0.0.0.0",
          "--port",
          String(POOL_POD_PORT),
          "--state-dir",
          "/home/agent/.bayma",
          "--token-file",
          "/attach/token",
          "--telemetry-token-file",
          "/attach/token",
          ...(snapshotted ? [STOP_WAIT_ARG, String(stopWaitSeconds(config) * 1000)] : []),
        ],
        workingDir: "/attach",
        env: [
          { name: "POD_NAME", valueFrom: { fieldRef: { fieldPath: "metadata.name" } } },
          { name: "HOME", value: "/home/agent" },
          { name: "USER", value: "agent" },
          { name: "BAYMA_PAYLOAD_DIR", value: "/opt/bayma/payload" },
        ],
        ports: [{ name: "mcp", containerPort: POOL_POD_PORT }],
        readinessProbe: { tcpSocket: { port: "mcp" }, periodSeconds: 2 },
        securityContext: restrictedContainer(),
        resources: sessions.resources,
        volumeMounts: [
          // Its own directory on its node, which its workspace is attached under once it is bound.
          { name: "attach", mountPath: "/attach", subPathExpr: "$(POD_NAME)", mountPropagation: "HostToContainer" },
          { name: "tmp", mountPath: "/tmp" },
        ],
      }],
      volumes: [{ name: "attach", hostPath: { path: ATTACH_ROOT, type: "DirectoryOrCreate" } }, { name: "tmp", emptyDir: { sizeLimit: "2Gi" } }],
    },
  };
}

/** A pool's Deployment, and the ScaledObject that keeps its free pods. */
function poolObjects(config: InstallConfig, pool: Pool, mode: keyof typeof POOLS, cpuBaseline: CpuBaseline): KubernetesObject[] {
  const template = poolTemplate(config, pool, mode, cpuBaseline);
  const deployment: V1Deployment = {
    apiVersion: "apps/v1",
    kind: "Deployment",
    // Ready, as `alasio up` and `alasio status` say it, once its pods run: each is ready only once bound.
    metadata: { name: pool.name, namespace: pool.namespace, labels: labels(POOL_COMPONENT), annotations: { [READY_ONCE_BOUND_ANNOTATION]: "true" } },
    spec: {
      ...replicasOf(config.sessions.pools),
      selector: { matchLabels: { ...freeLabels(pool) } },
      // None is ready before it is bound: the old free pods go as the new come.
      strategy: { type: "Recreate" },
      template,
    },
  };
  // As many free as binds wait for, past `min`: each is claimed by one.
  const waiting = {
    type: "metrics-api",
    metadata: { url: `http://${GATEWAY}.${NAMESPACE}.svc.cluster.local:${GATEWAY_PORT}/counts/${pool.namespace}/${pool.name}`, valueLocation: "waiting", targetValue: "1" },
  };
  // Sized as recommended alone: a sandbox's resources change only as a new pod is made.
  return [deployment, ...scaledObject(deployment, config.sessions.pools, [waiting]), verticalPodAutoscaler(deployment, "Recommended")];
}

/** The gateway's configuration, as src/gateway/gateway.ts reads it. */
function gatewayConfig(config: InstallConfig, carried: Carried): GatewayConfig {
  const pools = poolsOf(config, MAIN_ENVIRONMENT);
  const templates = (mode: keyof typeof POOLS) => ({ current: { ...poolTemplate(config, pools[mode], mode, carried.cpuBaseline) }, previous: carried.previousPools[mode] ? { ...carried.previousPools[mode] } : null });
  return {
    port: GATEWAY_PORT,
    peers: `${GATEWAY_PEERS}.${NAMESPACE}.svc.cluster.local`,
    podPort: POOL_POD_PORT,
    waitSeconds: 120,
    // A duration, as the configuration checks it.
    releaseAfterSeconds: durationSeconds(config.sessions.snapshots.releaseAfter) ?? 0,
    environment: { name: MAIN_ENVIRONMENT_NAME, namespace: sessionsNamespace(config, MAIN_ENVIRONMENT) },
    // A new session's volume's size, as the configuration checks it.
    quotaBytes: memoryBytes(config.sessions.storage.size),
    // As alasio's own sessions were snapshotted (./alasio.ts sessionSnapshots), under the releases the cluster keeps.
    snapshots: poolsSnapshotted(config, carried.cpuBaseline)
      ? {
        liveEverySeconds: durationSeconds(config.sessions.snapshots.liveEvery) ?? 0,
        releases: carried.gvisorReleases.map((release) => ({ release, runtimeClassName: gvisorRuntimeClass(release) })),
        templates: { none: templates("none"), full: templates("full") },
      }
      : null,
    pools: { none: POOLS.none, full: POOLS.full },
    nodeAgents: {
      namespace: NAMESPACE,
      selector: Object.entries(selectorLabels("node-agent")).map(([label, value]) => `${label}=${value}`).join(","),
      port: NODE_AGENT_PORT,
    },
  };
}

/** A Role in `namespace` of `rules`, named for the ServiceAccount `account` of alasio's namespace, and its binding to it. */
function accountRole(account: string, component: string, namespace: string, rules: V1PolicyRule[]): [V1Role, V1RoleBinding] {
  const metadata = { name: account, namespace, labels: labels(component) };
  return [
    { apiVersion: "rbac.authorization.k8s.io/v1", kind: "Role", metadata, rules },
    {
      apiVersion: "rbac.authorization.k8s.io/v1",
      kind: "RoleBinding",
      metadata,
      roleRef: { apiGroup: "rbac.authorization.k8s.io", kind: "Role", name: account },
      subjects: [{ kind: "ServiceAccount", name: account, namespace: NAMESPACE }],
    },
  ];
}

/**
 * What lets the ServiceAccount `account` find the branch environments, by their namespaces
 * (src/gateway/environments.ts): a ClusterRole that reads namespaces, and nothing else of the
 * cluster, and its binding.
 */
function environmentsFinder(account: string, component: string): [V1ClusterRole, V1ClusterRoleBinding] {
  const metadata = { name: account, labels: labels(component) };
  return [
    { apiVersion: "rbac.authorization.k8s.io/v1", kind: "ClusterRole", metadata, rules: [{ apiGroups: [""], resources: ["namespaces"], verbs: ["get", "list", "watch"] }] },
    {
      apiVersion: "rbac.authorization.k8s.io/v1",
      kind: "ClusterRoleBinding",
      metadata,
      roleRef: { apiGroup: "rbac.authorization.k8s.io", kind: "ClusterRole", name: account },
      subjects: [{ kind: "ServiceAccount", name: account, namespace: NAMESPACE }],
    },
  ];
}

/** The ServiceAccount `account` of alasio's namespace. */
const serviceAccount = (account: string, component: string): V1ServiceAccount => ({ apiVersion: "v1", kind: "ServiceAccount", metadata: { name: account, namespace: NAMESPACE, labels: labels(component) } });

/** The user the gateway's ServiceAccount is to the API server, which the sandbox pods' policy names as the one maker of restores' pods. */
export const GATEWAY_USER = `system:serviceaccount:${NAMESPACE}:${GATEWAY}`;

/**
 * The gateway's identity: its ServiceAccount, the node agents it finds, the branch
 * environments it finds, and the nodes it reads whether a restore's pod may be scheduled onto;
 * what it may do in each environment's pools is sandboxPoolObjects'.
 */
function gatewayIdentity(): KubernetesObject[] {
  const nodes = { name: `${GATEWAY}-nodes`, labels: labels(GATEWAY_COMPONENT) };
  const nodesRole: V1ClusterRole = { apiVersion: "rbac.authorization.k8s.io/v1", kind: "ClusterRole", metadata: nodes, rules: [{ apiGroups: [""], resources: ["nodes"], verbs: ["get"] }] };
  const nodesBinding: V1ClusterRoleBinding = {
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "ClusterRoleBinding",
    metadata: nodes,
    roleRef: { apiGroup: "rbac.authorization.k8s.io", kind: "ClusterRole", name: nodes.name },
    subjects: [{ kind: "ServiceAccount", name: GATEWAY, namespace: NAMESPACE }],
  };
  return [
    serviceAccount(GATEWAY, GATEWAY_COMPONENT),
    nodesRole,
    nodesBinding,
    // The node agents, by their pods, which it attaches workspaces through.
    ...accountRole(GATEWAY, GATEWAY_COMPONENT, NAMESPACE, [{ apiGroups: [""], resources: ["pods"], verbs: ["list"] }]),
    ...environmentsFinder(GATEWAY, GATEWAY_COMPONENT),
  ];
}

/** The gateway's Deployment, its configuration's ConfigMap, and its Services. */
function gatewayObjects(config: InstallConfig, carried: Carried): KubernetesObject[] {
  const rendered = JSON.stringify(gatewayConfig(config, carried), null, 2);
  const configMap: V1ConfigMap = { apiVersion: "v1", kind: "ConfigMap", metadata: { name: GATEWAY, namespace: NAMESPACE, labels: labels(GATEWAY_COMPONENT) }, data: { "config.json": rendered } };
  const key = (variable: string, field: string): V1EnvVar => ({ name: variable, valueFrom: { secretKeyRef: { name: GATEWAY, key: field } } });
  const deployment: V1Deployment = {
    apiVersion: "apps/v1",
    kind: "Deployment",
    metadata: { name: GATEWAY, namespace: NAMESPACE, labels: labels(GATEWAY_COMPONENT) },
    spec: {
      replicas: 1,
      selector: { matchLabels: selectorLabels(GATEWAY_COMPONENT) },
      template: {
        metadata: { labels: labels(GATEWAY_COMPONENT), annotations: { "checksum/config": sha256(rendered) } },
        spec: {
          serviceAccountName: GATEWAY,
          ...priority("floor"),
          securityContext: restrictedPod(1000, 1000),
          ...imagePullSecrets(config),
          containers: [{
            name: GATEWAY_COMPONENT,
            image: imageReference(config.images.alasio),
            imagePullPolicy: config.images.pullPolicy,
            command: ["node", "/opt/alasio/src/gateway/main.ts"],
            env: [
              { name: GATEWAY_CONFIG_VARIABLE, value: "/etc/gateway/config.json" },
              key(CALLER_KEY_VARIABLE, "caller-key"),
              key(BINDING_KEY_VARIABLE, "binding-key"),
              { name: NODE_AGENT_TOKEN_VARIABLE, valueFrom: { secretKeyRef: { name: componentName("node-agent"), key: "token" } } },
              { name: "POD_IP", valueFrom: { fieldRef: { fieldPath: "status.podIP" } } },
              // Every environment's events topic, which it writes as its principal on the log (src/events/).
              { name: EVENTS_BROKERS_VARIABLE, value: LOG_BROKERS },
              { name: EVENTS_USERNAME_VARIABLE, valueFrom: { secretKeyRef: { name: logPrincipalSecret("sandbox-gateway"), key: "username" } } },
              { name: EVENTS_PASSWORD_FILE_VARIABLE, value: "/run/alasio/log/password" },
              ...(otelEnv(config).length > 0 ? [{ name: "OTEL_SERVICE_NAME", value: GATEWAY }, ...otelEnv(config)] : []),
            ],
            ports: [{ name: "gateway", containerPort: GATEWAY_PORT }],
            readinessProbe: { httpGet: { path: "/healthz", port: "gateway" }, periodSeconds: 5 },
            securityContext: restrictedContainer(),
            resources: config.sessions.gateway.resources,
            volumeMounts: [{ name: "config", mountPath: "/etc/gateway", readOnly: true }, { name: "log", mountPath: "/run/alasio/log", readOnly: true }],
          }],
          volumes: [{ name: "config", configMap: { name: GATEWAY } }, { name: "log", secret: { secretName: logPrincipalSecret("sandbox-gateway"), items: [{ key: "password", path: "password" }] } }],
        },
      },
    },
  };
  const service = (name: string, headless: boolean): V1Service => ({
    apiVersion: "v1",
    kind: "Service",
    metadata: { name, namespace: NAMESPACE, labels: labels(GATEWAY_COMPONENT) },
    spec: { ...(headless ? { clusterIP: "None" } : {}), selector: selectorLabels(GATEWAY_COMPONENT), ports: [{ name: "gateway", port: GATEWAY_PORT, targetPort: "gateway" }] },
  });
  return [configMap, deployment, verticalPodAutoscaler(deployment, "InPlace"), service(GATEWAY, false), service(GATEWAY_PEERS, true)];
}

/** The receiver: its identity, which finds the branch environments, whose pools' Leases sandboxPoolObjects lets it watch, its Deployment and its Service. */
function receiverObjects(config: InstallConfig): KubernetesObject[] {
  const port = config.telemetry.receiverPort;
  const deployment: V1Deployment = {
    apiVersion: "apps/v1",
    kind: "Deployment",
    metadata: { name: RECEIVER, namespace: NAMESPACE, labels: labels(RECEIVER_COMPONENT) },
    spec: {
      replicas: 1,
      selector: { matchLabels: selectorLabels(RECEIVER_COMPONENT) },
      template: {
        metadata: { labels: labels(RECEIVER_COMPONENT) },
        spec: {
          serviceAccountName: RECEIVER,
          ...priority("floor"),
          securityContext: restrictedPod(1000, 1000),
          ...imagePullSecrets(config),
          containers: [{
            name: RECEIVER_COMPONENT,
            image: imageReference(config.images.alasio),
            imagePullPolicy: config.images.pullPolicy,
            command: ["node", "/opt/alasio/src/receiver/main.ts"],
            env: [
              { name: RECEIVER_PORT_VARIABLE, value: String(port) },
              { name: BINDING_KEY_VARIABLE, valueFrom: { secretKeyRef: { name: GATEWAY, key: "binding-key" } } },
              { name: SESSIONS_NAMESPACE_VARIABLE, value: sessionsNamespace(config, MAIN_ENVIRONMENT) },
              ...(otelEnv(config).length > 0 ? [{ name: "OTEL_SERVICE_NAME", value: RECEIVER }, ...otelEnv(config)] : []),
            ],
            ports: [{ name: "otlp-http", containerPort: port }],
            securityContext: restrictedContainer(),
            resources: config.sessions.receiver.resources,
          }],
        },
      },
    },
  };
  const service: V1Service = {
    apiVersion: "v1",
    kind: "Service",
    metadata: { name: RECEIVER, namespace: NAMESPACE, labels: labels(RECEIVER_COMPONENT) },
    spec: { selector: selectorLabels(RECEIVER_COMPONENT), ports: [{ name: "otlp-http", port, targetPort: "otlp-http" }] },
  };
  return [serviceAccount(RECEIVER, RECEIVER_COMPONENT), ...environmentsFinder(RECEIVER, RECEIVER_COMPONENT), deployment, verticalPodAutoscaler(deployment, "InPlace"), service];
}

/** Where a workspace mount's pod keeps what is its own: its juicefs's log and pid, and whether it has handed its mount over. */
export const MOUNT_RUN = "/run/alasio";
/** Where JuiceFS keeps a mount's open handles on the node, which the mount that takes it over reads: every mount pod's on the node, at /tmp. */
export const JUICEFS_STATE = "/var/lib/alasio/juicefs-state";
/** What JuiceFS's mount says once it has handed its FUSE session over to another mount of the same mount point, and ends. */
export const HANDED_OVER = "transfer FUSE session to others";

/**
 * The script of a workspace mount's pod: JuiceFS's mount of workspace storage at `root`, its
 * log copied out; and, once it ends, idle where it ended by handing its mount over, as it
 * said, to the mount of the pod that replaces it, until its own pod is deleted, or, where it
 * ended any other way, a failure, so kubelet starts it again and it mounts anew. Asked to end
 * while it mounts, it has its mount end too. `run` is MOUNT_RUN but for tests. Handed over, it
 * makes its mounts of `root` its own (private), so the teardown of its container's mounts as it
 * ends takes none of the node's with it, which they would share otherwise (Bidirectional).
 *
 * While its juicefs serves, it keeps the node's mount at `root`: a bind of it of its own, in
 * its own directory, which no other pod's going takes with it, and, where the node has lost
 * it, as it does once the pod that held it before a handover goes (its container's mounts
 * torn down, and with them the node's, which they share), binds that one back at `root`, the
 * node's again, every second it looks. A `root` that cannot be read, as a mount whose
 * session is lost, it leaves be.
 */
export function workspaceMountScript(root: string, cacheSizeMiB: number, run = MOUNT_RUN): string {
  return [
    `run=${run}`,
    `mkdir -p ${root} "$run"`,
    `rm -f "$run/handed-over"`,
    // Made, empty, before the mount starts or the log is followed, which would not find it yet.
    `: > "$run/juicefs.log"`,
    `juicefs mount --no-bgjob --cache-size ${cacheSizeMiB} "$META_URL" ${root} >> "$run/juicefs.log" 2>&1 &`,
    "pid=$!",
    `echo "$pid" > "$run/juicefs.pid"`,
    "keep() {",
    `  kept="$run/kept"; bound=""`,
    `  while kill -0 "$pid" 2>/dev/null; do`,
    `    if type=$(stat -f -c %T ${root} 2>/dev/null); then`,
    `      case "$type" in`,
    `        fuse*) [ -n "$bound" ] || { mkdir -p "$kept" && mount --bind ${root} "$kept" && bound=1 && echo "it keeps a bind of the node's mount of its own"; } ;;`,
    `        *) [ -n "$bound" ] && mount --bind "$kept" ${root} && echo "the node's mount was lost, as the pod that held it went: it is bound again from this pod's own" ;;`,
    "      esac",
    "    fi",
    "    sleep 1",
    "  done",
    "}",
    "keep &",
    "keeper=$!",
    `tail -n +1 -f "$run/juicefs.log" &`,
    "tailer=$!",
    `trap 'kill -TERM "$pid"' TERM INT`,
    `wait "$pid"; code=$?`,
    // A TERM's wait ends as the trap runs; the mount's own end is waited for.
    `while kill -0 "$pid" 2>/dev/null; do wait "$pid"; code=$?; done`,
    "sleep 1",
    `kill "$tailer" "$keeper" 2>/dev/null`,
    `if grep -qF '${HANDED_OVER}' "$run/juicefs.log"; then`,
    `  touch "$run/handed-over"`,
    // Its own mounts of the node's mount kept from the node's: torn down as its container ends, they take the node's with them otherwise.
    `  mount --make-rprivate ${root} && echo "its mounts are its own: its going leaves the node's mount whole"`,
    "  echo 'the mount is handed over to the pod that replaces this one, which is idle until it is deleted'",
    "  trap 'exit 0' TERM INT",
    "  while :; do sleep 3600 & wait $!; done",
    "fi",
    `echo "juicefs mount ended ($code) without handing its mount over" >&2`,
    `[ "$code" -ne 0 ] && exit "$code"`,
    "exit 1",
  ].join("\n");
}

/** The readiness of a workspace mount's pod: its own juicefs runs, it has not handed its mount over, and `root` is FUSE's. */
export function workspaceMountReady(root: string, run = MOUNT_RUN): string {
  return `[ ! -e ${run}/handed-over ] && kill -0 "$(cat ${run}/juicefs.pid)" 2>/dev/null && case "$(stat -f -c %T ${root})" in fuse*) true;; *) false;; esac`;
}

/**
 * The node's mount of workspace storage, at WORKSPACES_ROOT, its pod's own and the node's
 * (Bidirectional), which the node agent attaches workspaces from. A mount held by a pod that
 * goes takes every workspace attached from it with it, so a new pod of it starts beside the
 * old (maxSurge), and its mount of the same mount point takes the old's FUSE session over,
 * which needs them to share the node's PIDs and JuiceFS's state (JUICEFS_STATE); the old pod
 * idles once it has handed its mount over (workspaceMountScript), not ready, and is deleted
 * once the new is ready, which then keeps the node's mount itself, as the old one's going
 * takes the node's with it (workspaceMountScript).
 */
function workspaceMount(config: InstallConfig): V1DaemonSet {
  const credential = (name: string, key: string): V1EnvVar => ({ name, valueFrom: { secretKeyRef: { name: WORKSPACES_CREDENTIALS, key } } });
  return {
    apiVersion: "apps/v1",
    kind: "DaemonSet",
    metadata: { name: WORKSPACE_MOUNT, namespace: NAMESPACE, labels: labels(WORKSPACE_MOUNT_COMPONENT) },
    spec: {
      selector: { matchLabels: selectorLabels(WORKSPACE_MOUNT_COMPONENT) },
      updateStrategy: { type: "RollingUpdate", rollingUpdate: { maxSurge: 1, maxUnavailable: 0 } },
      template: {
        metadata: { labels: { ...labels(WORKSPACE_MOUNT_COMPONENT), ...JUICEFS_ADMIN } },
        spec: {
          ...priority("floor"),
          ...imagePullSecrets(config),
          ...given("nodeSelector", { ...config.sessions.nodeSelector }),
          ...given("tolerations", [...config.sessions.tolerations]),
          automountServiceAccountToken: false,
          // The mount that takes another's over signals it, by its PID on the node.
          hostPID: true,
          containers: [{
            name: WORKSPACE_MOUNT_COMPONENT,
            image: imageReference(config.workspaceStorage.csi.mountImage),
            imagePullPolicy: "IfNotPresent",
            command: ["sh", "-c", workspaceMountScript(WORKSPACES_ROOT, config.workspaceStorage.cacheSizeMiB)],
            env: [credential("META_URL", "metaurl"), credential("ACCESS_KEY", "access-key"), credential("SECRET_KEY", "secret-key")],
            readinessProbe: { exec: { command: ["sh", "-c", workspaceMountReady(WORKSPACES_ROOT)] }, periodSeconds: 2 },
            securityContext: { privileged: true },
            resources: config.workspaceStorage.mount.resources,
            volumeMounts: [
              { name: "workspaces", mountPath: WORKSPACES_ROOT, mountPropagation: "Bidirectional" },
              { name: "state", mountPath: "/tmp" },
              { name: "run", mountPath: MOUNT_RUN },
            ],
          }],
          volumes: [
            { name: "workspaces", hostPath: { path: WORKSPACES_ROOT, type: "DirectoryOrCreate" } },
            { name: "state", hostPath: { path: JUICEFS_STATE, type: "DirectoryOrCreate" } },
            { name: "run", emptyDir: {} },
          ],
        },
      },
    },
  };
}

/**
 * What the sandbox service has in `environment`'s sessions' namespace, main's or a branch
 * environment's (./branch.ts): its pools, the budget of their bound pods, and what the gateway
 * may do there, binding in its pools, and the receiver, watching their Leases.
 */
export function sandboxPoolObjects(config: InstallConfig, environment: Environment, cpuBaseline: CpuBaseline): KubernetesObject[] {
  if (!sandboxServiceRuns(config)) return [];
  const pools = poolsOf(config, environment);
  const namespace = pools.none.namespace;
  return [
    ...accountRole(GATEWAY, GATEWAY_COMPONENT, namespace, [
      // Its pools' pods, claimed by an update of the version read, and deleted as they are released; and a restore's, made, which the sandbox pods' policy holds to its pool's template.
      { apiGroups: [""], resources: ["pods"], verbs: ["get", "list", "watch", "create", "update", "delete"] },
      // A restore's pod made ready once its sandbox is resumed, by its readiness gate's condition.
      { apiGroups: [""], resources: ["pods/status"], verbs: ["patch"] },
      // The network policies that select a restore's pod, which its node's agent waits for in force before it resumes it.
      { apiGroups: ["networking.k8s.io"], resources: ["networkpolicies"], verbs: ["list"] },
      { apiGroups: ["coordination.k8s.io"], resources: ["leases"], verbs: ["get", "list", "watch", "create", "update", "patch", "delete"] },
      // Its pools' templates, which a release's snapshot records.
      { apiGroups: ["apps"], resources: ["deployments"], verbs: ["get"], resourceNames: [POOLS.none, POOLS.full] },
      // What the API server tells of the pods it refused to make, as the sandbox pods' policy refused them (src/gateway/refusals.ts).
      { apiGroups: [""], resources: ["events"], verbs: ["list"] },
    ]),
    ...accountRole(RECEIVER, RECEIVER_COMPONENT, namespace, [{ apiGroups: ["coordination.k8s.io"], resources: ["leases"], verbs: ["list", "watch"] }]),
    ...poolObjects(config, pools.none, "none", cpuBaseline),
    ...poolObjects(config, pools.full, "full", cpuBaseline),
    boundBudget(namespace),
  ];
}

/** The sandbox service's objects: its gateway, its receiver, and the nodes' mount, main's alone, which serve every environment, and main's pools. */
export function sandboxServiceObjects(config: InstallConfig, carried: Carried): KubernetesObject[] {
  if (!sandboxServiceRuns(config)) return [];
  const mount = workspaceMount(config);
  const pools = poolsOf(config, MAIN_ENVIRONMENT);
  return [
    ...gatewayIdentity(),
    ...gatewayObjects(config, carried),
    // Every pod of every environment's sessions held to the pools' templates, as Pod Security cannot.
    ...sandboxPodPolicy([poolTemplate(config, pools.none, "none", carried.cpuBaseline), poolTemplate(config, pools.full, "full", carried.cpuBaseline)], {
      sessionsNamespace: sessionsNamespace(config, MAIN_ENVIRONMENT),
      sandboxController: SANDBOX_CONTROLLER_USER,
      // Main's restores, by the gateway alone, onto each pool's template and the one before it, under the releases kept.
      restores: poolsSnapshotted(config, carried.cpuBaseline)
        ? {
          restorer: GATEWAY_USER,
          templates: (["none", "full"] as const).flatMap((mode) => [poolTemplate(config, pools[mode], mode, carried.cpuBaseline), ...(carried.previousPools[mode] ? [carried.previousPools[mode]] : [])]),
          runtimeClasses: carried.gvisorReleases.map(gvisorRuntimeClass),
        }
        : null,
    }),
    ...receiverObjects(config),
    ...sandboxPoolObjects(config, MAIN_ENVIRONMENT, carried.cpuBaseline),
    mount,
    // Sized as recommended alone, as a pod of it is only replaced, by one that takes its mount over.
    verticalPodAutoscaler(mount, "Recommended"),
  ];
}
