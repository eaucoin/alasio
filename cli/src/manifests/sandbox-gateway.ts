/**
 * The sandbox service (src/gateway/, src/binder/): session workspaces' sandboxes as pools
 * of pods already running, each bound to one workspace while it has work, through the
 * gateway.
 *
 * - The gateway: alasio's image, as its own program, the one door to a pool's pod; its
 *   configuration, which pools it binds in, rendered here; its keys, made by the stack's
 *   setup (neon/control/kube-setup.ts); a Service of it, which alasio binds through and KEDA
 *   asks each pool's counts of, and a headless one its replicas find each other by. It may
 *   bind, claim and delete the pods of its pools' namespace, hold their Leases, and read their
 *   Deployments, whose templates a release's snapshot records; and find the node agents.
 * - A pool of each of a session's internets, `none` and `full`: a Deployment in the sessions'
 *   namespace of pods of the agent image under the sessions' runtime, each its own directory
 *   on its node (src/node-agent/names.ts ATTACH_ROOT, by `subPathExpr` of its name) at /attach,
 *   where its workspace is attached once it is bound, its bayma started once its binding's
 *   token is beside it (sandbox/agent/start.sh). KEDA keeps `min` free, and makes more, to
 *   `max`, for the binds that wait; a bound pod is out of its Deployment, which replaces it.
 *   A pod that ends is ended: it serves one binding. Its template changes as a whole: the old
 *   free pods go as the new come, as none is ready before it is bound.
 * - The session telemetry receiver (src/receiver/): alasio's image, as its own program,
 *   watching the pools' Leases, which bound pods' bayma exports to.
 * - The node's mount of workspace storage, a pod of each node's own, held through upgrades of
 *   the node agent, which attaches workspaces from it: changed only as its pod is deleted.
 */
import type {
  KubernetesObject,
  V1ConfigMap,
  V1DaemonSet,
  V1Deployment,
  V1EnvVar,
  V1PodTemplateSpec,
  V1Role,
  V1RoleBinding,
  V1Service,
  V1ServiceAccount,
} from "@kubernetes/client-node";

import { freeLabels, type Pool } from "../../../src/binder/bindings.ts";
import { BINDING_KEY_VARIABLE, CALLER_KEY_VARIABLE, GATEWAY_CONFIG_VARIABLE, type GatewayConfig } from "../../../src/gateway/gateway.ts";
import { BAYMA_CONTAINER, NET_MODE_LABEL } from "../../../src/kube/sandbox-shape.ts";
import { ATTACH_ROOT, NODE_AGENT_PORT, NODE_AGENT_TOKEN_VARIABLE, WORKSPACES_ROOT } from "../../../src/node-agent/names.ts";
import { BINDING_NAMESPACES_VARIABLE, RECEIVER_PORT_VARIABLE } from "../../../src/receiver/receiver.ts";
import { imageReference } from "../images.ts";
import { sessionsNamespace } from "./alasio.ts";
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
import { durationSeconds, type InstallConfig } from "./config.ts";
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

/** The template of a pool's pods, as the module says: a session's, under its internet `mode`. */
export function poolTemplate(config: InstallConfig, pool: Pool, mode: keyof typeof POOLS): V1PodTemplateSpec {
  const { sessions } = config;
  const agent = imageReference(config.images.agent);
  return {
    metadata: {
      labels: { "app.kubernetes.io/part-of": "alasio", "app.kubernetes.io/instance": RELEASE, "app.kubernetes.io/component": POOL_COMPONENT, [NET_MODE_LABEL]: mode, ...freeLabels(pool) },
      // Under gVisor, /tmp is a tmpfs of the sandbox's own, by gVisor's mount hints, rather
      // than the emptyDir's directory on the node: a snapshot of the sandbox holds it.
      annotations: {
        "dev.gvisor.spec.mount.tmp.share": "container",
        "dev.gvisor.spec.mount.tmp.type": "tmpfs",
        "dev.gvisor.spec.mount.tmp.options": "rw",
      },
    },
    spec: {
      ...(sessions.runtimeClassName ? { runtimeClassName: sessions.runtimeClassName } : {}),
      ...priority("turn"),
      securityContext: restrictedPod(1000, 1000),
      automountServiceAccountToken: false,
      enableServiceLinks: false,
      ...imagePullSecrets(config),
      ...given("nodeSelector", { ...sessions.nodeSelector }),
      ...given("tolerations", [...sessions.tolerations]),
      containers: [{
        name: BAYMA_CONTAINER,
        image: agent,
        imagePullPolicy: config.images.pullPolicy,
        // bayma, and its hook as a release's snapshot runs it, once its workspace is attached.
        command: ["/usr/bin/tini", "-s", "--", "/usr/local/bin/alasio-start", "node", "/opt/bayma/bayma.js"],
        args: ["mcp-http", "--host", "0.0.0.0", "--port", String(POOL_POD_PORT), "--state-dir", "/home/agent/.bayma", "--token-file", "/attach/token", "--telemetry-token-file", "/attach/token"],
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
function poolObjects(config: InstallConfig, pool: Pool, mode: keyof typeof POOLS): KubernetesObject[] {
  const template = poolTemplate(config, pool, mode);
  const deployment: V1Deployment = {
    apiVersion: "apps/v1",
    kind: "Deployment",
    metadata: { name: pool.name, namespace: pool.namespace, labels: labels(POOL_COMPONENT) },
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
function gatewayConfig(config: InstallConfig, environment: Environment): GatewayConfig {
  return {
    port: GATEWAY_PORT,
    peers: `${GATEWAY_PEERS}.${NAMESPACE}.svc.cluster.local`,
    podPort: POOL_POD_PORT,
    waitSeconds: 120,
    // A duration, as the configuration checks it.
    releaseAfterSeconds: durationSeconds(config.sessions.snapshots.releaseAfter) ?? 0,
    environments: [{ name: environment.branch ?? "main", namespace: sessionsNamespace(config, environment) }],
    pools: { none: POOLS.none, full: POOLS.full },
    nodeAgents: {
      namespace: NAMESPACE,
      selector: Object.entries(selectorLabels("node-agent")).map(([label, value]) => `${label}=${value}`).join(","),
      port: NODE_AGENT_PORT,
    },
  };
}

/** The gateway's identity: its ServiceAccount, what it may do in its pools' namespace, and the node agents it finds. */
function gatewayIdentity(config: InstallConfig, environment: Environment): KubernetesObject[] {
  const namespace = sessionsNamespace(config, environment);
  const account: V1ServiceAccount = { apiVersion: "v1", kind: "ServiceAccount", metadata: { name: GATEWAY, namespace: NAMESPACE, labels: labels(GATEWAY_COMPONENT) } };
  const role = (where: string, rules: V1Role["rules"]): V1Role => ({ apiVersion: "rbac.authorization.k8s.io/v1", kind: "Role", metadata: { name: GATEWAY, namespace: where, labels: labels(GATEWAY_COMPONENT) }, rules: rules ?? [] });
  const binding = (where: string): V1RoleBinding => ({
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "RoleBinding",
    metadata: { name: GATEWAY, namespace: where, labels: labels(GATEWAY_COMPONENT) },
    roleRef: { apiGroup: "rbac.authorization.k8s.io", kind: "Role", name: GATEWAY },
    subjects: [{ kind: "ServiceAccount", name: GATEWAY, namespace: NAMESPACE }],
  });
  return [
    account,
    role(namespace, [
      // Its pools' pods, claimed by an update of the version read, and deleted as they are released.
      { apiGroups: [""], resources: ["pods"], verbs: ["get", "list", "watch", "update", "delete"] },
      { apiGroups: ["coordination.k8s.io"], resources: ["leases"], verbs: ["get", "list", "watch", "create", "update", "patch", "delete"] },
      // Its pools' templates, which a release's snapshot records.
      { apiGroups: ["apps"], resources: ["deployments"], verbs: ["get"], resourceNames: [POOLS.none, POOLS.full] },
    ]),
    binding(namespace),
    // The node agents, by their pods, which it attaches workspaces through.
    role(NAMESPACE, [{ apiGroups: [""], resources: ["pods"], verbs: ["list"] }]),
    binding(NAMESPACE),
  ];
}

/** The gateway's Deployment, its configuration's ConfigMap, and its Services. */
function gatewayObjects(config: InstallConfig, environment: Environment): KubernetesObject[] {
  const rendered = JSON.stringify(gatewayConfig(config, environment), null, 2);
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
              ...(otelEnv(config).length > 0 ? [{ name: "OTEL_SERVICE_NAME", value: GATEWAY }, ...otelEnv(config)] : []),
            ],
            ports: [{ name: "gateway", containerPort: GATEWAY_PORT }],
            readinessProbe: { httpGet: { path: "/healthz", port: "gateway" }, periodSeconds: 5 },
            securityContext: restrictedContainer(),
            resources: config.sessions.gateway.resources,
            volumeMounts: [{ name: "config", mountPath: "/etc/gateway", readOnly: true }],
          }],
          volumes: [{ name: "config", configMap: { name: GATEWAY } }],
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

/** The receiver: its identity, which watches the pools' Leases, its Deployment and its Service. */
function receiverObjects(config: InstallConfig, environment: Environment): KubernetesObject[] {
  const namespace = sessionsNamespace(config, environment);
  const port = config.telemetry.receiverPort;
  const account: V1ServiceAccount = { apiVersion: "v1", kind: "ServiceAccount", metadata: { name: RECEIVER, namespace: NAMESPACE, labels: labels(RECEIVER_COMPONENT) } };
  const role: V1Role = {
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "Role",
    metadata: { name: RECEIVER, namespace, labels: labels(RECEIVER_COMPONENT) },
    rules: [{ apiGroups: ["coordination.k8s.io"], resources: ["leases"], verbs: ["list", "watch"] }],
  };
  const binding: V1RoleBinding = {
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "RoleBinding",
    metadata: { name: RECEIVER, namespace, labels: labels(RECEIVER_COMPONENT) },
    roleRef: { apiGroup: "rbac.authorization.k8s.io", kind: "Role", name: RECEIVER },
    subjects: [{ kind: "ServiceAccount", name: RECEIVER, namespace: NAMESPACE }],
  };
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
              { name: BINDING_NAMESPACES_VARIABLE, value: namespace },
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
  return [account, role, binding, deployment, verticalPodAutoscaler(deployment, "InPlace"), service];
}

/**
 * The node's mount of workspace storage, at WORKSPACES_ROOT, its pod's own and the node's
 * (Bidirectional), which the node agent attaches workspaces from: changed only as its pod
 * is deleted (OnDelete), as a mount held by a pod that goes takes every attached workspace
 * with it.
 */
function workspaceMount(config: InstallConfig): V1DaemonSet {
  const credential = (name: string, key: string): V1EnvVar => ({ name, valueFrom: { secretKeyRef: { name: WORKSPACES_CREDENTIALS, key } } });
  return {
    apiVersion: "apps/v1",
    kind: "DaemonSet",
    metadata: { name: WORKSPACE_MOUNT, namespace: NAMESPACE, labels: labels(WORKSPACE_MOUNT_COMPONENT) },
    spec: {
      selector: { matchLabels: selectorLabels(WORKSPACE_MOUNT_COMPONENT) },
      updateStrategy: { type: "OnDelete" },
      template: {
        metadata: { labels: { ...labels(WORKSPACE_MOUNT_COMPONENT), ...JUICEFS_ADMIN } },
        spec: {
          ...priority("floor"),
          ...imagePullSecrets(config),
          ...given("nodeSelector", { ...config.sessions.nodeSelector }),
          ...given("tolerations", [...config.sessions.tolerations]),
          automountServiceAccountToken: false,
          containers: [{
            name: WORKSPACE_MOUNT_COMPONENT,
            image: imageReference(config.workspaceStorage.csi.mountImage),
            imagePullPolicy: "IfNotPresent",
            command: ["sh", "-c", `mkdir -p ${WORKSPACES_ROOT} && exec juicefs mount --no-bgjob --cache-size ${config.workspaceStorage.cacheSizeMiB} "$META_URL" ${WORKSPACES_ROOT}`],
            env: [credential("META_URL", "metaurl"), credential("ACCESS_KEY", "access-key"), credential("SECRET_KEY", "secret-key")],
            securityContext: { privileged: true },
            resources: config.workspaceStorage.mount.resources,
            volumeMounts: [{ name: "workspaces", mountPath: WORKSPACES_ROOT, mountPropagation: "Bidirectional" }],
          }],
          volumes: [{ name: "workspaces", hostPath: { path: WORKSPACES_ROOT, type: "DirectoryOrCreate" } }],
        },
      },
    },
  };
}

/** The sandbox service's objects, of `environment`'s sessions: its gateway, its pools, its receiver, and the nodes' mount. */
export function sandboxServiceObjects(config: InstallConfig, environment: Environment = MAIN_ENVIRONMENT): KubernetesObject[] {
  if (!sandboxServiceRuns(config)) return [];
  const pools = poolsOf(config, environment);
  const mount = workspaceMount(config);
  return [
    ...gatewayIdentity(config, environment),
    ...gatewayObjects(config, environment),
    ...poolObjects(config, pools.none, "none"),
    ...poolObjects(config, pools.full, "full"),
    ...receiverObjects(config, environment),
    mount,
    // Sized as recommended alone: replaced, it takes every workspace attached on its node with it.
    verticalPodAutoscaler(mount, "Recommended"),
  ];
}
