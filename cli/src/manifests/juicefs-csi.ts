/**
 * JuiceFS's CSI driver (juicedata/juicefs-csi-driver v0.33.0) in its default mode, mount
 * pods: the controller, which makes a workspace's volume a directory of the file system,
 * its size the directory's quota; the node service, on every node, which runs a mount pod,
 * the JuiceFS client, for each volume a pod there uses and binds its directory into the
 * pod; and the CSIDriver that names it to kubelet. Mount pods are the node service's to
 * make; it labels them `app.kubernetes.io/name: juicefs-mount`.
 *
 * As upstream's deploy/k8s.yaml at v0.33.0 (sha256
 * ab4860600dc51c586282855aea40aa091b20a468b2b000ff996211fe9a5d93b1) installs it, with what
 * its documentation (docs/en at the same tag) recommends over it:
 *
 * - the controller's own provisioner (`--provisioner=true`, guide/configurations.md
 *   "Advanced PV provisioning"), which Helm installs by default and `pathPattern` needs,
 *   in place of the csi-provisioner sidecar;
 * - mount pods configured in the driver's ConfigMap (guide/configurations.md
 *   "ConfigMap"): their image pinned, as the driver's own default is a nightly build, their
 *   resources without a CPU limit and their cache bounded (guide/resource-optimization.md),
 *   the file system's metadata dumped to its bucket, and a readiness probe on the mount;
 * - mount pods under a non-preempting PriorityClass (resource-optimization.md "Set
 *   non-preempting PriorityClass"), so a new one never evicts a workspace;
 * - one controller, as alasio's other controllers are, and neither the dashboard nor the
 *   snapshotter's role, which alasio does not use.
 */
import type {
  KubernetesObject,
  V1ClusterRole,
  V1ClusterRoleBinding,
  V1ConfigMap,
  V1Container,
  V1CSIDriver,
  V1DaemonSet,
  V1EnvVar,
  V1PolicyRule,
  V1PriorityClass,
  V1Probe,
  V1ServiceAccount,
  V1StatefulSet,
} from "@kubernetes/client-node";

import { imageReference } from "../images.ts";
import { VOLUME_DRIVER_LABEL } from "../kube/apply.ts";
import { goJson, type Labels } from "./common.ts";
import type { InstallConfig } from "./config.ts";

/** The driver's name, which StorageClasses name as their provisioner. */
export const CSI_DRIVER = "csi.juicefs.com";
const VERSION = "v0.33.0";
const CONTROLLER = "juicefs-csi-controller";
const NODE = "juicefs-csi-node";
const CONFIG = "juicefs-csi-driver-config";
const CONTROLLER_ACCOUNT = "juicefs-csi-controller-sa";
const NODE_ACCOUNT = "juicefs-csi-node-sa";
/** The PriorityClass of mount pods. */
const MOUNT_PRIORITY_CLASS = "alasio-juicefs-mount";

/** The labels of the node service's mount pods. */
export const MOUNT_POD_LABELS: Labels = { "app.kubernetes.io/name": "juicefs-mount" };
/** The labels that select the controller's and the node service's pods, upstream's but for its version. */
export const CONTROLLER_POD_LABELS: Labels = { app: CONTROLLER, "app.kubernetes.io/name": "juicefs-csi-driver", "app.kubernetes.io/instance": "juicefs-csi-driver" };
export const NODE_POD_LABELS: Labels = { app: NODE, "app.kubernetes.io/name": "juicefs-csi-driver", "app.kubernetes.io/instance": "juicefs-csi-driver" };

/** The selector of the controller's, the node service's and the mount pods' pods, by their name labels. */
export const JUICEFS_PODS_SELECTOR = `app.kubernetes.io/name in (${MOUNT_POD_LABELS["app.kubernetes.io/name"]}, ${CONTROLLER_POD_LABELS["app.kubernetes.io/name"]})`;
/** Where each of those pods serves its Prometheus metrics (administration/monitoring.md). */
export const JUICEFS_METRICS_PORT = 9567;

/** The label of what the driver needs to delete its volumes' data, which alasio keeps while they remain. */
export const VOLUME_DRIVER: Labels = { [VOLUME_DRIVER_LABEL]: CSI_DRIVER };

/** The labels of the driver's objects: upstream's, at its version, managed by alasio, and needed by its volumes. */
const LABELS: Labels = {
  "app.kubernetes.io/name": "juicefs-csi-driver",
  "app.kubernetes.io/instance": "juicefs-csi-driver",
  "app.kubernetes.io/version": VERSION,
  "app.kubernetes.io/managed-by": "alasio",
  ...VOLUME_DRIVER,
};

/** Where the controller and node service keep mount points and the client's configuration, on the node. */
const MOUNT_PATH = "/var/lib/juicefs/volume";
const CONFIG_PATH = "/var/lib/juicefs/config";
/** The controller's socket, which its sidecars reach it by. */
const CONTROLLER_SOCKET_DIR = "/var/lib/csi/sockets/pluginproxy/";

/**
 * The driver's configuration, its ConfigMap's config.yaml: one patch, for the mount pods
 * of workspace storage's class. Mount options here take precedence over a volume's, and
 * reach mount pods made after a change.
 */
export function driverConfig({ workspaceStorage }: InstallConfig): object {
  const { csi, storageClassName, cacheSizeMiB, backupInterval } = workspaceStorage;
  return {
    enableNodeSelector: false,
    mountPodPatch: [{
      pvcSelector: { matchStorageClassName: storageClassName },
      ceMountImage: imageReference(csi.mountImage),
      resources: csi.mountPod.resources,
      mountOptions: [
        // The read cache, bounded, and keeping a fifth of the node's disk free.
        `cache-size=${cacheSizeMiB}`,
        "free-space-ratio=0.2",
        `backup-meta=${backupInterval}`,
      ],
      // Ready once the mount answers as FUSE's. A mount pod mounts its volume's directory
      // alone (as `subdir`), so that is the mount's root, and nothing is under it by the
      // volume's path; unmounted, the root is the node's own directory.
      readinessProbe: { exec: { command: ["sh", "-c", 'test "$(stat --file-system --format=%T ${MOUNT_POINT})" = fuseblk'] }, initialDelaySeconds: 5, periodSeconds: 10, failureThreshold: 3 },
    }],
  };
}

/** What the controller and node service are told alike: where they are, where mount pods keep their mounts, and the pods' priority. */
function pluginEnv({ workspaceStorage }: InstallConfig): V1EnvVar[] {
  return [
    { name: "NODE_NAME", valueFrom: { fieldRef: { fieldPath: "spec.nodeName" } } },
    { name: "JUICEFS_MOUNT_NAMESPACE", valueFrom: { fieldRef: { fieldPath: "metadata.namespace" } } },
    { name: "POD_NAME", valueFrom: { fieldRef: { fieldPath: "metadata.name" } } },
    { name: "JUICEFS_MOUNT_PATH", value: MOUNT_PATH },
    { name: "JUICEFS_CONFIG_PATH", value: CONFIG_PATH },
    { name: "JUICEFS_MOUNT_PRIORITY_NAME", value: MOUNT_PRIORITY_CLASS },
    { name: "JUICEFS_MOUNT_PREEMPTION_POLICY", value: "Never" },
    ...(workspaceStorage.csi.shareMountPod ? [{ name: "FS_SHARE_MOUNT", value: "true" }] : []),
  ];
}

/** The plugin's liveness, which its liveness-probe sidecar answers on the plugin's healthz port. */
const PLUGIN_PROBE: V1Probe = { httpGet: { path: "/healthz", port: "healthz" }, initialDelaySeconds: 10, periodSeconds: 10, timeoutSeconds: 3, failureThreshold: 5 };

/** The liveness-probe sidecar, for the plugin's socket in `socketVolume`. */
function livenessProbe({ workspaceStorage }: InstallConfig, socketVolume: string): V1Container {
  return {
    name: "liveness-probe",
    image: imageReference(workspaceStorage.csi.livenessProbeImage),
    args: ["--csi-address=/csi/csi.sock", "--health-port=9909"],
    resources: { requests: { cpu: "5m", memory: "16Mi" }, limits: { memory: "64Mi" } },
    volumeMounts: [{ name: socketVolume, mountPath: "/csi" }],
  };
}

/** A rule granting `verbs` on `resources` of `apiGroups`. */
function rule(apiGroups: string[], resources: string[], verbs: string[]): V1PolicyRule {
  return { apiGroups, resources, verbs };
}

/** Who the controller and node service are, and what they may do, as upstream grants it. */
function identities({ workspaceStorage }: InstallConfig): KubernetesObject[] {
  const { namespace } = workspaceStorage.csi;
  const account = (name: string): V1ServiceAccount => ({ apiVersion: "v1", kind: "ServiceAccount", metadata: { name, namespace, labels: LABELS } });
  const role = (name: string, rules: V1PolicyRule[]): V1ClusterRole => ({
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "ClusterRole",
    metadata: { name, labels: LABELS },
    rules,
  });
  const binding = (name: string, roleName: string, accountName: string): V1ClusterRoleBinding => ({
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "ClusterRoleBinding",
    metadata: { name, labels: LABELS },
    subjects: [{ kind: "ServiceAccount", name: accountName, namespace }],
    roleRef: { apiGroup: "rbac.authorization.k8s.io", kind: "ClusterRole", name: roleName },
  });
  const provisioner = "juicefs-external-provisioner-role";
  const nodeService = "juicefs-csi-external-node-service-role";
  return [
    account(CONTROLLER_ACCOUNT),
    account(NODE_ACCOUNT),
    role(provisioner, [
      rule([""], ["persistentvolumes"], ["get", "list", "watch", "create", "delete", "patch"]),
      rule([""], ["persistentvolumeclaims", "persistentvolumeclaims/status"], ["get", "list", "watch", "update", "patch"]),
      rule(["storage.k8s.io"], ["storageclasses"], ["get", "list", "watch"]),
      rule([""], ["events"], ["list", "watch", "create", "update", "patch"]),
      rule(["storage.k8s.io"], ["csinodes"], ["get", "list", "watch"]),
      rule([""], ["nodes"], ["get", "list", "watch"]),
      rule([""], ["secrets"], ["get", "list", "watch", "create", "update", "patch", "delete"]),
      rule([""], ["pods", "pods/log"], ["get", "list", "watch", "create", "update", "patch", "delete"]),
      rule(["batch"], ["jobs"], ["get", "create", "update", "patch", "delete", "list", "watch"]),
      rule([""], ["endpoints"], ["get", "list", "watch", "create", "update", "patch"]),
      rule(["apps"], ["daemonsets"], ["get", "list"]),
      rule(["coordination.k8s.io"], ["leases"], ["get", "watch", "list", "delete", "update", "create"]),
      rule([""], ["configmaps"], ["get", "watch", "list", "delete", "update", "create"]),
    ]),
    role(nodeService, [
      rule([""], ["pods"], ["get", "list", "create", "update", "delete", "patch", "watch"]),
      rule([""], ["pods/log"], ["get"]),
      rule([""], ["secrets"], ["get", "create", "update", "delete", "patch"]),
      rule(["batch"], ["jobs"], ["get", "create", "update", "delete", "patch"]),
      rule([""], ["nodes"], ["get", "list"]),
      rule([""], ["nodes/proxy"], ["*"]),
      rule([""], ["persistentvolumes", "persistentvolumeclaims"], ["get", "list"]),
      rule([""], ["pods/exec"], ["create"]),
      rule([""], ["events"], ["create", "get"]),
      rule(["storage.k8s.io"], ["storageclasses"], ["get", "watch"]),
      rule([""], ["configmaps"], ["get", "update"]),
    ]),
    binding("juicefs-csi-provisioner-binding", provisioner, CONTROLLER_ACCOUNT),
    binding("juicefs-csi-node-service-binding", nodeService, NODE_ACCOUNT),
  ];
}

/** The controller: the plugin, provisioning, and the resizer, which grows a volume's quota as its claim grows. */
function controller(config: InstallConfig): V1StatefulSet {
  const { csi } = config.workspaceStorage;
  return {
    apiVersion: "apps/v1",
    kind: "StatefulSet",
    metadata: { name: CONTROLLER, namespace: csi.namespace, labels: { ...LABELS, "app.kubernetes.io/component": "controller" } },
    spec: {
      serviceName: CONTROLLER,
      replicas: 1,
      selector: { matchLabels: CONTROLLER_POD_LABELS },
      template: {
        metadata: { labels: { ...LABELS, ...CONTROLLER_POD_LABELS } },
        spec: {
          serviceAccountName: CONTROLLER_ACCOUNT,
          priorityClassName: "system-cluster-critical",
          tolerations: [{ key: "CriticalAddonsOnly", operator: "Exists" }],
          containers: [
            {
              name: "juicefs-plugin",
              image: imageReference(csi.image),
              args: ["--endpoint=$(CSI_ENDPOINT)", "--logtostderr", "--nodeid=$(NODE_NAME)", "--leader-election", "--provisioner=true", "--config=/etc/config/config.yaml"],
              env: [{ name: "CSI_ENDPOINT", value: `unix://${CONTROLLER_SOCKET_DIR}csi.sock` }, ...pluginEnv(config)],
              ports: [{ name: "healthz", containerPort: 9909, protocol: "TCP" }],
              livenessProbe: PLUGIN_PROBE,
              // It mounts the file system itself, to make and remove volumes' directories.
              securityContext: { privileged: true, capabilities: { add: ["SYS_ADMIN"] } },
              resources: csi.controller.resources,
              volumeMounts: [
                { name: "socket-dir", mountPath: CONTROLLER_SOCKET_DIR },
                { name: "jfs-dir", mountPath: "/jfs", mountPropagation: "Bidirectional" },
                { name: "jfs-root-dir", mountPath: "/root/.juicefs", mountPropagation: "Bidirectional" },
                { name: "juicefs-config", mountPath: "/etc/config" },
              ],
            },
            {
              name: "csi-resizer",
              image: imageReference(csi.resizerImage),
              args: ["--csi-address=$(ADDRESS)", "--timeout=20s", "--leader-election", "--v=2"],
              env: [{ name: "ADDRESS", value: `${CONTROLLER_SOCKET_DIR}csi.sock` }],
              resources: { requests: { cpu: "5m", memory: "24Mi" }, limits: { memory: "128Mi" } },
              volumeMounts: [{ name: "socket-dir", mountPath: CONTROLLER_SOCKET_DIR }],
            },
            livenessProbe(config, "socket-dir"),
          ],
          volumes: [
            { name: "socket-dir", emptyDir: {} },
            { name: "jfs-dir", hostPath: { path: MOUNT_PATH, type: "DirectoryOrCreate" } },
            { name: "jfs-root-dir", hostPath: { path: CONFIG_PATH, type: "DirectoryOrCreate" } },
            { name: "juicefs-config", configMap: { name: CONFIG } },
          ],
        },
      },
    },
  };
}

/** The node service: the plugin, which makes mount pods and binds their mounts into pods, and the registrar, which names it to kubelet. */
function nodeService(config: InstallConfig): V1DaemonSet {
  const { csi } = config.workspaceStorage;
  const pluginDir = `/var/lib/kubelet/csi-plugins/${CSI_DRIVER}/`;
  return {
    apiVersion: "apps/v1",
    kind: "DaemonSet",
    metadata: { name: NODE, namespace: csi.namespace, labels: { ...LABELS, "app.kubernetes.io/component": "node" } },
    spec: {
      selector: { matchLabels: NODE_POD_LABELS },
      template: {
        metadata: { labels: { ...LABELS, ...NODE_POD_LABELS } },
        spec: {
          serviceAccountName: NODE_ACCOUNT,
          priorityClassName: "system-node-critical",
          dnsPolicy: "ClusterFirstWithHostNet",
          tolerations: [{ key: "CriticalAddonsOnly", operator: "Exists" }],
          containers: [
            {
              name: "juicefs-plugin",
              image: imageReference(csi.image),
              args: ["--endpoint=$(CSI_ENDPOINT)", "--logtostderr", "--nodeid=$(NODE_NAME)", "--enable-manager=true", "--config=/etc/config/config.yaml"],
              env: [
                { name: "CSI_ENDPOINT", value: "unix:/csi/csi.sock" },
                { name: "HOST_IP", valueFrom: { fieldRef: { fieldPath: "status.hostIP" } } },
                { name: "KUBELET_PORT", value: "10250" },
                ...pluginEnv(config),
              ],
              ports: [{ name: "healthz", containerPort: 9909, protocol: "TCP" }],
              livenessProbe: PLUGIN_PROBE,
              securityContext: { privileged: true },
              resources: csi.node.resources,
              volumeMounts: [
                { name: "kubelet-dir", mountPath: "/var/lib/kubelet", mountPropagation: "Bidirectional" },
                { name: "plugin-dir", mountPath: "/csi" },
                { name: "device-dir", mountPath: "/dev" },
                { name: "jfs-dir", mountPath: "/jfs", mountPropagation: "Bidirectional" },
                { name: "jfs-root-dir", mountPath: "/root/.juicefs", mountPropagation: "Bidirectional" },
                { name: "juicefs-config", mountPath: "/etc/config" },
                // Where it keeps each mount's FUSE descriptor, by which a mount pod that
                // crashes comes back under the same mount (guide/configurations.md
                // "Automatic mount point recovery").
                { name: "jfs-fuse-fd", mountPath: "/tmp" },
              ],
            },
            {
              name: "node-driver-registrar",
              image: imageReference(csi.registrarImage),
              args: ["--csi-address=$(ADDRESS)", "--kubelet-registration-path=$(DRIVER_REG_SOCK_PATH)", "--v=5"],
              env: [{ name: "ADDRESS", value: "/csi/csi.sock" }, { name: "DRIVER_REG_SOCK_PATH", value: `${pluginDir}csi.sock` }],
              resources: { requests: { cpu: "5m", memory: "16Mi" }, limits: { memory: "64Mi" } },
              volumeMounts: [{ name: "plugin-dir", mountPath: "/csi" }, { name: "registration-dir", mountPath: "/registration" }],
            },
            livenessProbe(config, "plugin-dir"),
          ],
          volumes: [
            { name: "kubelet-dir", hostPath: { path: "/var/lib/kubelet", type: "Directory" } },
            { name: "plugin-dir", hostPath: { path: pluginDir, type: "DirectoryOrCreate" } },
            { name: "registration-dir", hostPath: { path: "/var/lib/kubelet/plugins_registry/", type: "Directory" } },
            { name: "device-dir", hostPath: { path: "/dev", type: "Directory" } },
            { name: "jfs-dir", hostPath: { path: MOUNT_PATH, type: "DirectoryOrCreate" } },
            { name: "jfs-root-dir", hostPath: { path: CONFIG_PATH, type: "DirectoryOrCreate" } },
            { name: "juicefs-config", configMap: { name: CONFIG } },
            { name: "jfs-fuse-fd", hostPath: { path: "/var/run/juicefs-csi", type: "DirectoryOrCreate" } },
          ],
        },
      },
    },
  };
}

/** The driver's objects, unless the cluster already has the driver: the CSIDriver and the mount pods' PriorityClass among them, which are the cluster's. */
export function juicefsCsiObjects(config: InstallConfig): KubernetesObject[] {
  const { csi } = config.workspaceStorage;
  if (!csi.enabled) return [];
  const csiDriver: V1CSIDriver = {
    apiVersion: "storage.k8s.io/v1",
    kind: "CSIDriver",
    metadata: { name: CSI_DRIVER, labels: LABELS },
    spec: { attachRequired: false, podInfoOnMount: true },
  };
  const priorityClass: V1PriorityClass = {
    apiVersion: "scheduling.k8s.io/v1",
    kind: "PriorityClass",
    metadata: { name: MOUNT_PRIORITY_CLASS, labels: LABELS },
    // As high as a pod's may be, so mount pods outlast the workspaces they serve, and
    // never preempting, so a new one evicts nothing.
    value: 1_000_000_000,
    preemptionPolicy: "Never",
    globalDefault: false,
    description: "JuiceFS's mount pods, which serve alasio's workspaces",
  };
  const configMap: V1ConfigMap = {
    apiVersion: "v1",
    kind: "ConfigMap",
    metadata: { name: CONFIG, namespace: csi.namespace, labels: LABELS },
    data: { "config.yaml": goJson(driverConfig(config), "  ") },
  };
  return [csiDriver, priorityClass, ...identities(config), configMap, controller(config), nodeService(config)];
}
