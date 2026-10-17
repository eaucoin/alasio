/**
 * The node agent (src/node-agent/): one pod on every node sessions run on, which takes
 * whole-sandbox snapshots of their sandboxes and prepares them for restores there, kept
 * in the object store's snapshots bucket, and resumes a restored one once its pod's
 * network policies are in force there; and attaches session workspaces into the sandbox
 * pools' pods on its node, from its node's mount of workspace storage (./sandbox-gateway.ts),
 * makes their directories there, and sets their quotas on workspace storage's metadata
 * engine, with JuiceFS's own command, of its image.
 * It runs where sandboxes are snapshotted (common.ts nodeAgentRuns).
 *
 * It is the one component that runs runsc on a node, beside containerd's shims, as they
 * run it, so it runs as the node does: privileged, as root, in the node's PID namespace
 * (runsc finds a sandbox's processes by their PIDs there, and the agent enters the
 * node's mounts and network from its PID 1, to read its packet filter), with the node's
 * paths at the same paths, as runsc and the shims open them, an image's among them: the
 * runsc root and the systemd bus under /run, the gVisor releases and the runtimes'
 * options files, read-only, and the snapshot cache. Pod Security admits it in alasio's
 * namespace, which enforces no level of its own, as Kubernetes admits a privileged pod
 * in a namespace by default; a cluster whose admission defaults otherwise must exempt it.
 *
 * Its bucket and the identity it is reached as are the Secret Neon's setup makes for it
 * (neon/control/kube-setup.ts), taken whole as its environment, and the token alasio
 * presents, the Secret both are given.
 */
import type { KubernetesObject, V1DaemonSet, V1Volume, V1VolumeMount } from "@kubernetes/client-node";

import {
  ATTACH_ROOT,
  CONTAINERD,
  CPU_FEATURES_CONTAINER,
  GVISOR_RELEASES,
  META_URL_VARIABLE,
  NODE_AGENT_PORT,
  NODE_AGENT_TOKEN_VARIABLE,
  OPERATION_LIMIT_SECONDS,
  SNAPSHOT_CACHE_BYTES_VARIABLE,
  SNAPSHOT_CACHE_DIR,
  WORKSPACES_ROOT,
} from "../../../src/node-agent/names.ts";
import { currentGvisorRelease } from "../cluster/node.ts";
import { imageReference } from "../images.ts";
import { componentName, given, imagePullSecrets, labels, NAMESPACE, nodeAgentRuns, otelEnv, selectorLabels } from "./common.ts";
import type { InstallConfig } from "./config.ts";
import { priority } from "./priorities.ts";
import { verticalPodAutoscaler } from "./sizing.ts";
import { WORKSPACES_CREDENTIALS } from "./workspace-storage.ts";

/** The Secret of the snapshots bucket, its endpoint, region and name and the agents' keys to it. */
const SNAPSHOTS_SECRET = componentName("snapshots");
/** The Secret of the token alasio presents to the node agents, under `token`. */
const NODE_AGENT_SECRET = componentName("node-agent");

/** The node's paths the agent has at the same paths: each one's volume, what must be there, and whether the agent only reads it. */
const HOST_PATHS: readonly { readonly name: string; readonly path: string; readonly type: string; readonly readOnly?: true }[] = [
  // The shims' runsc root (/run/containerd/runsc) and, where systemd runs the node, its bus.
  { name: "run", path: "/run", type: "Directory" },
  { name: "gvisor", path: GVISOR_RELEASES, type: "DirectoryOrCreate", readOnly: true },
  { name: "containerd", path: CONTAINERD, type: "DirectoryOrCreate", readOnly: true },
  { name: "snapshots", path: SNAPSHOT_CACHE_DIR, type: "DirectoryOrCreate" },
  // Workspace storage, as the node's mount of it holds it, and the pool pods' directories,
  // which it attaches workspaces into and writes their tokens beside.
  { name: "workspaces", path: WORKSPACES_ROOT, type: "DirectoryOrCreate" },
  { name: "attach", path: ATTACH_ROOT, type: "DirectoryOrCreate" },
];

/** What each suffix of a size multiplies it by, as Kubernetes counts a quantity. */
const SIZE_UNITS: Readonly<Record<string, number>> = { Ki: 2 ** 10, Mi: 2 ** 20, Gi: 2 ** 30, Ti: 2 ** 40, Pi: 2 ** 50, k: 1e3, M: 1e6, G: 1e9, T: 1e12, P: 1e15 };

/** The bytes of a size as the configuration takes one (5Gi, 1.5G, 500000). */
function sizeBytes(size: string): number {
  // A size, as the configuration checks it (config.ts Size).
  const [, amount = "0", unit = ""] = /^([0-9.]+)([A-Za-z]*)$/u.exec(size) ?? [];
  return Math.floor(Number(amount) * (SIZE_UNITS[unit] ?? 1));
}

/** The node agents' DaemonSet. */
function daemonSet(config: InstallConfig): V1DaemonSet {
  const { sessions } = config;
  const volumes: V1Volume[] = HOST_PATHS.map(({ name, path, type }) => ({ name, hostPath: { path, type } }));
  const volumeMounts: V1VolumeMount[] = HOST_PATHS.map(({ name, path, readOnly }) => ({
    name,
    mountPath: path,
    ...(readOnly ? { readOnly } : {}),
    // What is mounted under the node's /run, and its mount of workspace storage, after the agent starts are the agent's too.
    ...(name === "run" || name === "workspaces" ? { mountPropagation: "HostToContainer" } : {}),
  }));
  return {
    apiVersion: "apps/v1",
    kind: "DaemonSet",
    metadata: { name: componentName("node-agent"), namespace: NAMESPACE, labels: labels("node-agent") },
    spec: {
      selector: { matchLabels: selectorLabels("node-agent") },
      // A node's agent replaced once the old one is gone, as two would act on one cache, and
      // one node's at a time, each as long as the old one's operations take to end (below).
      updateStrategy: { type: "RollingUpdate", rollingUpdate: { maxUnavailable: 1, maxSurge: 0 } },
      template: {
        metadata: { labels: labels("node-agent") },
        spec: {
          // A restore waits on it, as a turn does.
          ...priority("floor"),
          // Stopped, it lets the checkpoints and prepares it runs end, rather than end a
          // sandbox unsaved or leave its cache half written (src/node-agent/operations.ts).
          terminationGracePeriodSeconds: OPERATION_LIMIT_SECONDS,
          hostPID: true,
          // It speaks to nothing of the cluster's but alasio, which calls it.
          automountServiceAccountToken: false,
          enableServiceLinks: false,
          ...imagePullSecrets(config),
          ...given("nodeSelector", { ...sessions.nodeSelector }),
          ...given("tolerations", [...sessions.tolerations]),
          // What the node's CPU has under the current release, said in the pod's status
          // before the agent starts, for alasio up to hold sessions to (../cpu-baseline.ts).
          initContainers: [{
            name: CPU_FEATURES_CONTAINER,
            image: imageReference(config.images.alasio),
            imagePullPolicy: config.images.pullPolicy,
            command: ["node", "/opt/alasio/src/node-agent/cpu-features.ts", currentGvisorRelease()],
            securityContext: { privileged: true, runAsUser: 0 },
            resources: sessions.snapshots.nodeAgent.resources,
            volumeMounts: volumeMounts.filter(({ name }) => name !== "snapshots"),
          }],
          containers: [{
            name: "node-agent",
            image: imageReference(config.images.alasio),
            imagePullPolicy: config.images.pullPolicy,
            command: ["node", "/opt/alasio/src/node-agent/main.ts"],
            env: [
              { name: "NODE_NAME", valueFrom: { fieldRef: { fieldPath: "spec.nodeName" } } },
              { name: NODE_AGENT_TOKEN_VARIABLE, valueFrom: { secretKeyRef: { name: NODE_AGENT_SECRET, key: "token" } } },
              { name: SNAPSHOT_CACHE_BYTES_VARIABLE, value: String(sizeBytes(sessions.snapshots.nodeCache)) },
              // Workspace storage's metadata engine, which it sets workspaces' quotas on alone: no key of its object store.
              ...(config.workspaceStorage.enabled ? [{ name: META_URL_VARIABLE, valueFrom: { secretKeyRef: { name: WORKSPACES_CREDENTIALS, key: "metaurl" } } }] : []),
              ...(otelEnv(config).length > 0 ? [{ name: "OTEL_SERVICE_NAME", value: "alasio-node-agent" }, ...otelEnv(config)] : []),
            ],
            envFrom: [{ secretRef: { name: SNAPSHOTS_SECRET } }],
            ports: [{ name: "api", containerPort: NODE_AGENT_PORT }],
            readinessProbe: { httpGet: { path: "/healthz", port: "api" }, periodSeconds: 10 },
            securityContext: { privileged: true, runAsUser: 0 },
            resources: sessions.snapshots.nodeAgent.resources,
            volumeMounts,
          }],
          volumes,
        },
      },
    },
  };
}

/**
 * The node agents, where they run, and their sizing: recommended, not applied in place, as
 * a resize would restart an agent in the middle of a snapshot's upload.
 */
export function nodeAgentObjects(config: InstallConfig): KubernetesObject[] {
  if (!nodeAgentRuns(config)) return [];
  const agents = daemonSet(config);
  return [agents, verticalPodAutoscaler(agents, "Recommended")];
}
