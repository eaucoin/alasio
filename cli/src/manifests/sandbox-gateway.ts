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
 *   on its node (src/node-agent/names.ts ATTACH_ROOT, by `subPathExpr` of its namespace and its name) at /attach,
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
import { createHash } from "node:crypto";
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
  V1Service,
  V1ServiceAccount,
} from "@kubernetes/client-node";
import { Schema } from "effect";

import { freeLabels, type Pool } from "../../../src/binder/names.ts";
import { EVENTS_BROKERS_VARIABLE, EVENTS_PASSWORD_FILE_VARIABLE, EVENTS_USERNAME_VARIABLE } from "../../../src/events/names.ts";
import { MAIN_ENVIRONMENT_NAME } from "../../../src/branch/names.ts";
import {
  BINDING_KEY_VARIABLE,
  CALLER_KEY_VARIABLE,
  GATEWAY_CONFIG_VARIABLE,
  GatewayConfig as GatewayConfigSchema,
  type GatewayConfig,
  FOLDERS_NAMESPACE_VARIABLE,
  RECEIVER_PORT_VARIABLE,
  SESSIONS_NAMESPACE_VARIABLE,
  templateHash,
} from "../../../src/gateway/names.ts";
import { BAYMA_CONTAINER, BAYMA_STOP_SECONDS, CPU_FEATURES_ANNOTATION, NET_MODE_LABEL, SESSION_TEMPLATE_ANNOTATION, STOP_WAIT_ARG } from "../../../src/kube/sandbox-shape.ts";
import { ATTACH_ROOT, goferOf, gvisorRuntimeClass, NODE_AGENT_PORT, NODE_AGENT_TOKEN_VARIABLE, WORKSPACES_ROOT } from "../../../src/node-agent/names.ts";
import { memoryBytes } from "../capacity.ts";
import { NODE_PINS } from "../cluster/node.ts";
import { imageReference } from "../images.ts";
import { READY_ONCE_BOUND_ANNOTATION } from "../kube/rollout.ts";
import { type Carried, type CpuBaseline, type Kept, type PreviousPools, sessionsNamespace, stopWaitSeconds } from "./alasio.ts";
import { folderTemplate, foldersRun } from "./folders.ts";
import { harnessesConfig } from "./harness-pools.ts";
import {
  accountRole,
  componentName,
  type Environment,
  given,
  imagePullSecrets,
  labels,
  MAIN_ENVIRONMENT,
  NAMESPACE,
  otelEnv,
  RELEASE,
  replicasOf,
  restrictedContainer,
  restrictedPod,
  selectorLabels,
  sha256,
} from "./common.ts";
import { resumedConditionPolicy, sandboxPodPolicy } from "./admission.ts";
import { durationSeconds, type InstallConfig } from "./config.ts";
import { LOG_BROKERS, logPrincipalSecret } from "./log.ts";
import { boundBudget } from "./disruption.ts";
import { priority } from "./priorities.ts";
import { scaledObject } from "./scalers.ts";
import { verticalPodAutoscaler } from "./sizing.ts";
import { JUICEFS_ADMIN, VALKEY, VALKEY_ADDRESS, WORKSPACES_CREDENTIALS, workspacesBucketUrl } from "./workspace-storage.ts";

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

/** The pools of `environment`, by its sessions' internet. */
export const poolsOf = (config: InstallConfig, environment: Environment): Record<keyof typeof POOLS, Pool> => {
  const namespace = sessionsNamespace(config, environment);
  return { none: { name: POOLS.none, namespace }, full: { name: POOLS.full, namespace } };
};

/**
 * Each sandbox pool's template before the current one, as the gateway's configuration it
 * replaces, `liveGateway` (its ConfigMap's `config.json`), carried it: the one it rendered, where
 * the template changes, or else the one it carried; none where it rendered none, as where its
 * sessions were not snapshotted. A template is carried for one change of it, and no more
 * (./alasio.ts carriedFrom); and pinned into the sandbox pods'
 * policy for the restores of the snapshots taken on it, read back from the cluster: the
 * ConfigMap is that trust's anchor, which only the operator, by `alasio up`, writes. Pure, for tests.
 */
export function carriedPools(config: InstallConfig, liveGateway: string | undefined, kept: Kept): PreviousPools {
  const none: PreviousPools = { none: null, full: null };
  if (!poolsSnapshotted(kept.cpuBaseline) || liveGateway === undefined) return none;
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

/**
 * The CPU baseline the installation whose gateway's configuration is `liveGateway` (as
 * carriedPools takes it) holds sessions to: what its pools' current template sets, if
 * anything, as only a pool of snapshotted sessions is held to one. Pure, for tests.
 */
export function recordedCpuBaseline(liveGateway: string | undefined): CpuBaseline {
  if (liveGateway === undefined) return null;
  const held = Schema.decodeUnknownOption(Schema.fromJsonString(GatewayConfigSchema))(liveGateway);
  if (held._tag === "None") return null;
  const recorded = (held.value.snapshots?.templates.none.current as V1PodTemplateSpec | undefined)?.metadata?.annotations?.[CPU_FEATURES_ANNOTATION];
  return recorded ? recorded.split(",") : null;
}

/** Whether the sandbox pools' sessions are snapshotted: where the nodes' CPU baseline is recorded, which they are held to. */
export const poolsSnapshotted = (cpuBaseline: CpuBaseline): cpuBaseline is readonly string[] => cpuBaseline !== null;

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
  if (!poolsSnapshotted(cpuBaseline)) return template;
  return { ...template, metadata: { ...template.metadata, annotations: { ...template.metadata?.annotations, [SESSION_TEMPLATE_ANNOTATION]: templateHash(template) } } };
}

/** A pool's template, as poolTemplate says, but its hash. */
function unhashedPoolTemplate(config: InstallConfig, pool: Pool, mode: keyof typeof POOLS, cpuBaseline: CpuBaseline): V1PodTemplateSpec {
  const { sessions } = config;
  const agent = imageReference(config.images.agent);
  const snapshotted = poolsSnapshotted(cpuBaseline);
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
          { name: "POD_NAMESPACE", valueFrom: { fieldRef: { fieldPath: "metadata.namespace" } } },
          { name: "HOME", value: "/home/agent" },
          { name: "USER", value: "agent" },
          { name: "BAYMA_PAYLOAD_DIR", value: "/opt/bayma/payload" },
        ],
        ports: [{ name: "mcp", containerPort: POOL_POD_PORT }],
        readinessProbe: { tcpSocket: { port: "mcp" }, periodSeconds: 2 },
        securityContext: restrictedContainer(),
        resources: sessions.resources,
        volumeMounts: [
          // Its own directory on its node, by its namespace and its name, as pods of two namespaces
          // may share a name, which its workspace is attached under once it is bound.
          { name: "attach", mountPath: "/attach", subPathExpr: "$(POD_NAMESPACE)/$(POD_NAME)", mountPropagation: "HostToContainer" },
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
    // Under the releases the cluster keeps, each restored with a RuntimeClass of its own.
    snapshots: poolsSnapshotted(carried.cpuBaseline)
      ? {
        liveEverySeconds: durationSeconds(config.sessions.snapshots.liveEvery) ?? 0,
        releases: carried.gvisorReleases.map((release) => ({ release, runtimeClassName: gvisorRuntimeClass(release), gofer: goferOf(release, NODE_PINS.gvisor.restoresThroughSocketSince) })),
        templates: { none: templates("none"), full: templates("full") },
      }
      : null,
    pools: { none: POOLS.none, full: POOLS.full },
    nodeAgents: {
      namespace: NAMESPACE,
      selector: Object.entries(selectorLabels("node-agent")).map(([label, value]) => `${label}=${value}`).join(","),
      port: NODE_AGENT_PORT,
    },
    receiver: { host: `${RECEIVER}.${NAMESPACE}.svc.cluster.local`, port: config.telemetry.receiverPort },
    // Folder conversations' instances, of the host profile, where the installation has folder workspaces.
    folders: foldersRun(config)
      ? { namespace: config.host.namespace, template: { ...folderTemplate(config) }, stateRoot: config.host.stateRoot, releaseAfterSeconds: durationSeconds(config.host.releaseAfter) ?? 0 }
      : null,
    // Conversations' harnesses' pools, where they run in pools of their own.
    harnesses: harnessesConfig(config),
  };
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
              ...(foldersRun(config) ? [{ name: FOLDERS_NAMESPACE_VARIABLE, value: config.host.namespace }] : []),
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
/** Where JuiceFS keeps a mount's open handles on the node, which the mount that takes it over reads. */
export const JUICEFS_STATE = "/var/lib/alasio/juicefs-state";
/** The port each node's mount serves JuiceFS's client's metrics on, in its pod's own network, which the collector's scraper reads. */
export const JUICEFS_METRICS_PORT = 9567;
/** Where the node runs JuiceFS's command from: its static release, copied from alasio's image, under its digest. */
export const JUICEFS_BIN = "/var/lib/alasio/bin";
/** Where the node's mount of workspace storage caches its blocks. */
export const JUICEFS_CACHE = "/var/lib/alasio/juicefs-cache";
/** What JuiceFS's mount says once it has handed its FUSE session over to another mount of the same mount point, and ends. */
export const HANDED_OVER = "transfer FUSE session to others";
/**
 * What JuiceFS's mount says once its own process serves the mount point: mounted afresh, or
 * having taken another's FUSE session over, as it waits until the mount point names its process
 * and no longer the one before.
 */
export const SERVING = "is ready at";

/**
 * The Services the nodes' mount of workspace storage reaches by the addresses it resolves them to
 * as it starts (workspaceMountScript): the metadata engine's, Valkey's, and the bundled object
 * store's, where the bucket is its.
 */
export const mountServices = (config: InstallConfig): readonly string[] => [VALKEY, ...(config.objectStore.bundled.enabled ? [componentName("seaweedfs")] : [])];

/** The annotation of the mount's template that changes, rolling it, where what it reaches by address does. */
export const MOUNT_SERVICES_ANNOTATION = "checksum/storage-services";

/**
 * What the mount reaches by address, as a checksum: the UIDs of its Services, `uids`, and the
 * URLs it resolves, so a Service made anew, whose address is another, or another bucket, rolls it.
 */
function mountServicesChecksum(config: InstallConfig, uids: Kept["mountServices"]): string {
  const reached = { metadata: VALKEY_ADDRESS, bucket: workspacesBucketUrl(config), services: Object.fromEntries(mountServices(config).map((name) => [name, uids[name] ?? null])) };
  return createHash("sha256").update(JSON.stringify(reached)).digest("hex").slice(0, 16);
}

/** Where a workspace mount's script finds what it works with: but for tests, the node's and the image's own. */
export interface MountPaths {
  readonly run: string;
  readonly bin: string;
  readonly state: string;
  readonly cache: string;
  /** JuiceFS's command, of alasio's image: its static release, which runs as it is in the node's namespace. */
  readonly juicefs: string;
  /** The node's mount table, as the pod sees it, sharing the node's PIDs. */
  readonly mountinfo: string;
  /** What runs a command in the node's mount namespace. */
  readonly inNode: string;
  /** What resolves a name by the pod's DNS: its address first on the line it prints. */
  readonly resolve: string;
}

export const MOUNT_PATHS: MountPaths = {
  run: MOUNT_RUN,
  bin: JUICEFS_BIN,
  state: JUICEFS_STATE,
  cache: JUICEFS_CACHE,
  juicefs: "/usr/local/bin/juicefs",
  mountinfo: "/proc/1/mountinfo",
  inNode: "nsenter -t 1 -m --",
  resolve: "getent hosts",
};

/**
 * The script of a workspace mount's pod: JuiceFS's mount of workspace storage at `root`, made
 * in the node's own mount namespace, so it is the node's alone, and no container's mounts, torn
 * down as their container ends, take it with them; its log copied out; and, once it ends, idle
 * where it ended by handing its mount over, as it said, to the mount of the pod that replaces it,
 * until its own pod is deleted, or, where it ended any other way, a failure, so kubelet starts it
 * again and it mounts anew. Asked to end while it mounts, it has its mount end too.
 *
 * Its juicefs is JuiceFS's static release, of alasio's image, which runs as it is in the node's
 * namespace: copied to the node, under its digest, never over the one a mount runs. A mount left
 * at `root` whose juicefs is gone, as a pod killed with no successor leaves one, which the kernel
 * answers as not connected, is unmounted first, lazily, so the next mounts afresh.
 *
 * In the node's mount namespace its juicefs reads the node's resolv.conf, which resolves none of
 * the cluster's names, its network the pod's still: so the metadata engine's and the bucket's,
 * where they are a Service's, are resolved here, by the pod's DNS, and it is given their
 * addresses. A Service's address lasts as long as it does: `up` rolls the mount where one is made
 * anew, or the bucket is another (MOUNT_SERVICES_ANNOTATION), and the pod that replaces it reaches
 * the new address.
 *
 * Its juicefs, run in the node's mount namespace, is still this pod's process, of its cgroup: the
 * pod's teardown kills it. So a pod hands its mount over before it is torn down, never after: the
 * pod that replaces it starts beside it (maxSurge), takes its FUSE session over, and only then is
 * it deleted, idle, its juicefs ended already. `paths` are MOUNT_PATHS but for tests.
 */
export function workspaceMountScript(root: string, cacheSizeMiB: number, paths: MountPaths = MOUNT_PATHS): string {
  const { run, bin, state, cache, juicefs, mountinfo, inNode, resolve } = paths;
  return [
    // `url`, its host a Service's name given as its address, by the pod's DNS.
    "resolved() {",
    '  scheme=${1%%://*}; rest=${1#*://}; auth=""',
    '  case $rest in *@*) auth="${rest%@*}@"; rest=${rest##*@};; esac',
    '  host=${rest%%[:/]*}; tail=${rest#"$host"}; address=$host',
    "  case $host in *.svc|*.svc.cluster.local)",
    `    address=$(${resolve} "$host" | awk '{ print $1; exit }')`,
    '    [ -n "$address" ] || { echo "$host resolves to no address" >&2; return 1; };;',
    "  esac",
    `  printf '%s://%s%s%s\n' "$scheme" "$auth" "$address" "$tail"`,
    "}",
    // The metadata engine's password given in META_PASSWORD, which JuiceFS reads, not in its
    // address on the command line, which every process of the node's PIDs could read.
    `META_PASSWORD=$(node -e 'process.stdout.write(decodeURIComponent(new URL(process.env.META_URL).password))') || exit 1`,
    "export META_PASSWORD",
    `meta=$(resolved "$(node -e 'const url = new URL(process.env.META_URL); url.password = ""; process.stdout.write(url.href)')") || exit 1`,
    'bucket=$(resolved "$BUCKET") || exit 1',
    `run=${run}`,
    `mkdir -p "$run" ${bin} ${state} ${cache}`,
    `rm -f "$run/handed-over"`,
    // Made, empty, before the mount starts or the log is followed, which would not find it yet.
    `: > "$run/juicefs.log"`,
    `node_juicefs="${bin}/juicefs-$(sha256sum ${juicefs} | cut -c1-16)"`,
    `if [ ! -x "$node_juicefs" ]; then cp ${juicefs} "$node_juicefs.$$" && chmod 755 "$node_juicefs.$$" && mv "$node_juicefs.$$" "$node_juicefs"; fi`,
    // A mount of a juicefs gone, which its kernel answers as no longer connected: unmounted, so this
    // one mounts afresh. Not one that fails otherwise, as a juicefs that serves does while its
    // metadata engine is unreachable, which this one takes over.
    `if awk -v root=${root} '$5 == root { found = 1 } END { exit !found }' ${mountinfo} && ${inNode} ls ${root} 2>&1 >/dev/null | grep -qi 'not connected'; then`,
    `  ${inNode} umount -l ${root} && echo "unmounted the node's mount its juicefs left, which served nothing"`,
    "fi",
    `${inNode} mkdir -p ${root}`,
    `TMPDIR=${state} ${inNode} "$node_juicefs" mount --no-bgjob --metrics 0.0.0.0:${JUICEFS_METRICS_PORT} --bucket "$bucket" --cache-dir ${cache} --cache-size ${cacheSizeMiB} "$meta" ${root} >> "$run/juicefs.log" 2>&1 &`,
    "pid=$!",
    `echo "$pid" > "$run/juicefs.pid"`,
    `tail -n +1 -f "$run/juicefs.log" &`,
    "tailer=$!",
    `trap 'kill -TERM "$pid"' TERM INT`,
    `wait "$pid"; code=$?`,
    // A TERM's wait ends as the trap runs; the mount's own end is waited for.
    `while kill -0 "$pid" 2>/dev/null; do wait "$pid"; code=$?; done`,
    "sleep 1",
    `kill "$tailer" 2>/dev/null`,
    `if grep -qF '${HANDED_OVER}' "$run/juicefs.log"; then`,
    `  touch "$run/handed-over"`,
    "  echo 'the mount is handed over to the pod that replaces this one, which is idle until it is deleted'",
    "  trap 'exit 0' TERM INT",
    "  while :; do sleep 3600 & wait $!; done",
    "fi",
    `echo "juicefs mount ended ($code) without handing its mount over" >&2`,
    `[ "$code" -ne 0 ] && exit "$code"`,
    "exit 1",
  ].join("\n");
}

/**
 * The readiness of a workspace mount's pod: its own juicefs runs, has said it serves the mount
 * point (SERVING), and has not handed its mount over, and the node's mount table holds JuiceFS's
 * mount at `root`: this pod's juicefs serves the node's mount point. The table alone holds the
 * mount of the pod it replaces as its juicefs starts, before it takes that mount over, which the
 * pod it replaces would be deleted for, its mount with it.
 */
export function workspaceMountReady(root: string, paths: Pick<MountPaths, "run" | "mountinfo"> = MOUNT_PATHS): string {
  const { run, mountinfo } = paths;
  return [
    `[ ! -e ${run}/handed-over ]`,
    `kill -0 "$(cat ${run}/juicefs.pid)" 2>/dev/null`,
    `grep -qF '${SERVING}' ${run}/juicefs.log`,
    `awk -v root=${root} '$5 == root { for (i = 6; i < NF; i += 1) if ($i == "-") { if ($(i + 1) == "fuse.juicefs") found = 1; break } } END { exit !found }' ${mountinfo}`,
  ].join(" && ");
}

/**
 * The node's mount of workspace storage, at WORKSPACES_ROOT, the node's own (workspaceMountScript),
 * which the node agent attaches workspaces from. A mount held by a pod that goes takes every
 * workspace attached from it with it, so a new pod of it starts beside the old (maxSurge), and
 * its mount of the same mount point takes the old's FUSE session over, which needs them to share
 * the node's PIDs and mount namespace; the old pod idles once it has handed its mount over, not
 * ready, and is deleted once the new is ready. It runs alasio's image, for JuiceFS's static
 * release, of the version the mount image is (../../../Dockerfile), privileged, as it enters
 * the node's mount namespace.
 */
function workspaceMount(config: InstallConfig, mountServices: Kept["mountServices"]): V1DaemonSet {
  const credential = (name: string, key: string): V1EnvVar => ({ name, valueFrom: { secretKeyRef: { name: WORKSPACES_CREDENTIALS, key } } });
  const nodePaths = [["bin", JUICEFS_BIN], ["state", JUICEFS_STATE], ["cache", JUICEFS_CACHE]] as const;
  return {
    apiVersion: "apps/v1",
    kind: "DaemonSet",
    metadata: { name: WORKSPACE_MOUNT, namespace: NAMESPACE, labels: labels(WORKSPACE_MOUNT_COMPONENT) },
    spec: {
      selector: { matchLabels: selectorLabels(WORKSPACE_MOUNT_COMPONENT) },
      updateStrategy: { type: "RollingUpdate", rollingUpdate: { maxSurge: 1, maxUnavailable: 0 } },
      template: {
        metadata: {
          labels: { ...labels(WORKSPACE_MOUNT_COMPONENT), ...JUICEFS_ADMIN },
          annotations: { [MOUNT_SERVICES_ANNOTATION]: mountServicesChecksum(config, mountServices) },
        },
        spec: {
          ...priority("floor"),
          ...imagePullSecrets(config),
          ...given("nodeSelector", { ...config.sessions.nodeSelector }),
          ...given("tolerations", [...config.sessions.tolerations]),
          automountServiceAccountToken: false,
          // The mount that takes another's over signals it, by its PID on the node; and it enters the node's mount namespace by PID 1's.
          hostPID: true,
          containers: [{
            name: WORKSPACE_MOUNT_COMPONENT,
            image: imageReference(config.images.alasio),
            imagePullPolicy: config.images.pullPolicy,
            command: ["sh", "-c", workspaceMountScript(WORKSPACES_ROOT, config.workspaceStorage.cacheSizeMiB)],
            env: [credential("META_URL", "metaurl"), credential("BUCKET", "bucket"), credential("ACCESS_KEY", "access-key"), credential("SECRET_KEY", "secret-key")],
            ports: [{ name: "metrics", containerPort: JUICEFS_METRICS_PORT }],
            readinessProbe: { exec: { command: ["sh", "-c", workspaceMountReady(WORKSPACES_ROOT)] }, periodSeconds: 2 },
            securityContext: { privileged: true },
            resources: config.workspaceStorage.mount.resources,
            // The node's directories, where the pod reaches them as the node does.
            volumeMounts: [...nodePaths.map(([name, path]) => ({ name, mountPath: path })), { name: "run", mountPath: MOUNT_RUN }],
          }],
          volumes: [...nodePaths.map(([name, path]) => ({ name, hostPath: { path, type: "DirectoryOrCreate" } })), { name: "run", emptyDir: {} }],
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
  const mount = workspaceMount(config, carried.mountServices);
  const pools = poolsOf(config, MAIN_ENVIRONMENT);
  return [
    ...gatewayIdentity(),
    ...gatewayObjects(config, carried),
    // Every pod of every environment's sessions held to the pools' templates, as Pod Security cannot.
    ...sandboxPodPolicy([poolTemplate(config, pools.none, "none", carried.cpuBaseline), poolTemplate(config, pools.full, "full", carried.cpuBaseline)], {
      sessionsNamespace: sessionsNamespace(config, MAIN_ENVIRONMENT),
      // Main's restores, by the gateway alone, onto each pool's template and the one before it, under the releases kept.
      restores: poolsSnapshotted(carried.cpuBaseline)
        ? {
          restorer: GATEWAY_USER,
          templates: (["none", "full"] as const).flatMap((mode) => [poolTemplate(config, pools[mode], mode, carried.cpuBaseline), ...(carried.previousPools[mode] ? [carried.previousPools[mode]] : [])]),
          runtimeClasses: carried.gvisorReleases.map(gvisorRuntimeClass),
        }
        : null,
    }),
    // Where main's sessions are snapshotted, what the gateway may set of a restore's pod's status.
    ...(poolsSnapshotted(carried.cpuBaseline) ? resumedConditionPolicy(componentName("resumed-condition"), GATEWAY_USER, pools.none.namespace) : []),
    ...receiverObjects(config),
    ...sandboxPoolObjects(config, MAIN_ENVIRONMENT, carried.cpuBaseline),
    mount,
    // Sized as recommended alone, as a pod of it is only replaced, by one that takes its mount over.
    verticalPodAutoscaler(mount, "Recommended"),
  ];
}
