/**
 * alasio itself: its Deployment and volume, its telemetry receiver's Service, the
 * namespaces its workspaces run in, its identity in them, and the templates it makes
 * its workspaces' Sandboxes from (src/kube/config.ts).
 */
import type {
  KubernetesObject,
  V1ConfigMap,
  V1Deployment,
  V1EnvVar,
  V1Namespace,
  V1ObjectMeta,
  V1PersistentVolumeClaim,
  V1Role,
  V1RoleBinding,
  V1Service,
  V1ServiceAccount,
} from "@kubernetes/client-node";

import type { HostProfile, SessionsProfile } from "../../../src/kube/config.ts";
import { imageReference } from "../images.ts";
import {
  componentName,
  databaseSecret,
  envOf,
  given,
  goJson,
  helperResources,
  hostVolumeMounts,
  hostVolumes,
  imagePullSecrets,
  labels,
  NAMESPACE,
  otelEnv,
  RELEASE,
  restrictedContainer,
  restrictedPod,
  selectorLabels,
  sha256,
} from "./common.ts";
import type { InstallConfig } from "./config.ts";
import { cloneJob } from "./workspace-storage.ts";

/** The port every workspace's bayma serves MCP on. */
const BAYMA_PORT = 7290;

/** The namespaces whose Sandboxes alasio drives: the sessions', and the host profile's. */
function workspaceNamespaces(config: InstallConfig): string[] {
  return [...(config.sessions.enabled ? [config.sessions.namespace] : []), ...(config.host.enabled ? [config.host.namespace] : [])];
}

/**
 * The template of session filesystems' Sandboxes: an empty, isolated workspace under
 * the sessions' runtime, restricted, on a volume of workspace storage's class when it is
 * on. alasio adds what is per Sandbox: its name and labels, bayma's token, and its
 * network mode.
 */
function sessionsProfile(config: InstallConfig): SessionsProfile {
  const { sessions, workspaceStorage } = config;
  const agent = imageReference(config.images.agent);
  const storageClassName = sessions.storage.storageClassName || (workspaceStorage.enabled ? workspaceStorage.storageClassName : "");
  // On workspace storage, the volume's mounts follow its mount pod's, as upstream's
  // template has them (gVisor does not, and alasio restarts a session whose mount is
  // gone), and its files are given the pod's group only when its root is not the group's,
  // as walking a JuiceFS volume is slow.
  const propagation = workspaceStorage.enabled ? { mountPropagation: "HostToContainer" } : {};
  const ownership = workspaceStorage.enabled ? { fsGroupChangePolicy: "OnRootMismatch" } : {};
  return {
    namespace: sessions.namespace,
    port: BAYMA_PORT,
    workspaceDir: "/workspace",
    egressGate: sessions.egressGate,
    fullModeNameservers: [...sessions.fullModeNameservers],
    ...(mountPodsRemoved(config) ? { mountPodNamespace: workspaceStorage.csi.namespace } : {}),
    podTemplate: {
      metadata: { labels: { "app.kubernetes.io/part-of": "alasio", "app.kubernetes.io/instance": RELEASE, "app.kubernetes.io/component": "session" } },
      spec: {
        ...(sessions.runtimeClassName ? { runtimeClassName: sessions.runtimeClassName } : {}),
        securityContext: { ...restrictedPod(1000, 1000), ...ownership },
        ...imagePullSecrets(config),
        ...given("nodeSelector", { ...sessions.nodeSelector }),
        ...given("tolerations", [...sessions.tolerations]),
        initContainers: [
          // The workspace and home on the session's volume, and bayma's toolbelt, which the
          // image installs, linked into the home.
          {
            name: "prepare",
            image: agent,
            imagePullPolicy: config.images.pullPolicy,
            command: [
              "/bin/sh",
              "-c",
              "mkdir -p /data/workspace /data/home/.local/share/bayma && ln -sfn /opt/bayma-data/bayma/toolbelt /data/home/.local/share/bayma/toolbelt",
            ],
            securityContext: restrictedContainer(),
            ...helperResources(),
            volumeMounts: [{ name: "data", mountPath: "/data" }],
          },
        ],
        containers: [{
          name: "bayma",
          image: agent,
          imagePullPolicy: config.images.pullPolicy,
          command: ["/usr/bin/tini", "-s", "--", "node", "/opt/bayma/bayma.js"],
          args: ["mcp-http", "--host", "0.0.0.0", "--port", String(BAYMA_PORT), "--state-dir", "/home/agent/.bayma"],
          workingDir: "/workspace",
          env: [
            { name: "HOME", value: "/home/agent" },
            { name: "USER", value: "agent" },
            { name: "BAYMA_PAYLOAD_DIR", value: "/opt/bayma/payload" },
          ],
          ports: [{ name: "mcp", containerPort: BAYMA_PORT }],
          securityContext: restrictedContainer(),
          resources: sessions.resources,
          volumeMounts: [
            { name: "data", mountPath: "/workspace", subPath: "workspace", ...propagation },
            { name: "data", mountPath: "/home/agent", subPath: "home", ...propagation },
            { name: "tmp", mountPath: "/tmp" },
          ],
        }],
        volumes: [{ name: "tmp", emptyDir: { sizeLimit: "2Gi" } }],
      },
    },
    volumeClaimTemplates: [{
      metadata: { name: "data" },
      spec: {
        accessModes: ["ReadWriteOnce"],
        ...(storageClassName ? { storageClassName } : {}),
        resources: { requests: { storage: sessions.storage.size } },
      },
    }],
    ...(sessionsForked(config) ? { clone: { claimTemplate: "data", job: cloneJob(config) } } : {}),
  };
}

/** Whether alasio forks sessions: where their volumes are workspace storage's, which JuiceFS clones. */
function sessionsForked({ sessions, workspaceStorage }: InstallConfig): boolean {
  return workspaceStorage.enabled && [workspaceStorage.storageClassName, ""].includes(sessions.storage.storageClassName);
}

/** The template of folder workspaces' bayma: bayma itself, as the operator, with the machine's mounts. */
function hostProfile(config: InstallConfig): HostProfile {
  const { host } = config;
  return {
    namespace: host.namespace,
    port: BAYMA_PORT,
    stateRoot: host.stateRoot,
    podTemplate: {
      metadata: { labels: { "app.kubernetes.io/part-of": "alasio", "app.kubernetes.io/instance": RELEASE, "app.kubernetes.io/component": "folder-bayma" } },
      spec: {
        // May restart alasio, by rolling out its Deployment, as an agent restarts it.
        serviceAccountName: componentName("host-agent"),
        securityContext: {
          runAsUser: host.uid,
          runAsGroup: host.gid,
          // The group the bayma token's Secret volume is given, which the operator's user
          // reads it as; the node's own paths the mounts give are left as they are.
          fsGroup: host.gid,
          ...given("supplementalGroups", [...host.supplementalGroups]),
        },
        ...imagePullSecrets(config),
        containers: [{
          name: "bayma",
          image: imageReference(host.baymaImage),
          imagePullPolicy: "IfNotPresent",
          args: ["mcp-http", "--host", "0.0.0.0", "--port", String(BAYMA_PORT), "--default-durability", "checkpointed"],
          workingDir: host.home,
          env: [{ name: "HOME", value: host.home }, ...envOf(host.env)],
          ports: [{ name: "mcp", containerPort: BAYMA_PORT }],
          securityContext: {
            // What bayma needs to snapshot its REPL sessions' processes as it stops and
            // restore them whole as it starts again.
            capabilities: { add: ["CHECKPOINT_RESTORE", "SYS_PTRACE"] },
            seccompProfile: { type: "Unconfined" },
            appArmorProfile: { type: "Unconfined" },
          },
          resources: host.resources,
          ...given("volumeMounts", hostVolumeMounts(config)),
        }],
        ...given("volumes", hostVolumes(config)),
      },
    },
  };
}

/** The ConfigMap of the templates, and their checksum, which alasio's pod carries so it is replaced when they change. */
function sandboxTemplates(config: InstallConfig): { readonly configMap: V1ConfigMap; readonly checksum: string } {
  const name = componentName("sandbox-templates");
  const templates = goJson(
    { ...(config.sessions.enabled ? { sessions: sessionsProfile(config) } : {}), ...(config.host.enabled ? { host: hostProfile(config) } : {}) },
    "  ",
  );
  return {
    configMap: { apiVersion: "v1", kind: "ConfigMap", metadata: { name, namespace: NAMESPACE, labels: labels("alasio") }, data: { "templates.json": templates } },
    checksum: sha256(templates),
  };
}

/** alasio's environment: where its templates, state, database and receiver are, and what the configuration adds. */
function alasioEnv(config: InstallConfig, home: string): V1EnvVar[] {
  const { alasio, host } = config;
  return [
    { name: "ALASIO_KUBE_TEMPLATES", value: "/etc/alasio/templates.json" },
    { name: "ALASIO_STATE_DIR", value: "/var/lib/alasio/state" },
    { name: "HOME", value: home },
    { name: "ALASIO_DEPLOYMENT", value: RELEASE },
    { name: "ALASIO_NAMESPACE", valueFrom: { fieldRef: { fieldPath: "metadata.namespace" } } },
    { name: "TELEGRAM_BOT_TOKEN", valueFrom: { secretKeyRef: { name: alasio.telegram.existingSecret, key: "token" } } },
    { name: "TELEGRAM_ALLOWED_USER_IDS", valueFrom: { secretKeyRef: { name: alasio.telegram.existingSecret, key: "allowedUserIds" } } },
    { name: "ALASIO_DATABASE_URL_FILE", value: "/run/alasio/database/url" },
    { name: "ALASIO_LAKE_PASSWORD_FILE", value: "/run/alasio/database/lake-password" },
    { name: "ALASIO_LAKE_ENABLED", value: config.lake.enabled && config.neon.enabled ? "1" : "0" },
    { name: "ALASIO_TELEMETRY_RECEIVER_SERVICE", value: `${componentName("telemetry")}.${NAMESPACE}.svc` },
    { name: "ALASIO_TELEMETRY_RECEIVER_PORT", value: String(config.telemetry.receiverPort) },
    ...(alasio.defaultHarness ? [{ name: "ALASIO_DEFAULT_HARNESS", value: alasio.defaultHarness }] : []),
    ...(host.enabled && host.workspaceRoot ? [{ name: "ALASIO_WORKSPACE_ROOT", value: host.workspaceRoot }] : []),
    ...(alasio.claude.existingSecret
      ? [{ name: "CLAUDE_CODE_OAUTH_TOKEN", valueFrom: { secretKeyRef: { name: alasio.claude.existingSecret, key: alasio.claude.key } } }]
      : []),
    ...otelEnv(config),
    ...envOf(alasio.env),
  ];
}

/**
 * alasio's Deployment: one replica, as one Telegram poller and one SQLite writer, so an
 * update stops the old pod before the new one starts. Under the host profile it runs as
 * the operator, in their home, with the machine's mounts.
 */
function deployment(config: InstallConfig, templatesChecksum: string): V1Deployment {
  const { alasio, host } = config;
  const home = host.enabled && host.alasioHome ? host.home : "/var/lib/alasio/home";
  const uid = host.enabled ? host.uid : alasio.runAsUser;
  const gid = host.enabled ? host.gid : alasio.runAsGroup;
  const groups = [...new Set([...alasio.supplementalGroups, ...(host.enabled ? host.supplementalGroups : [])])];
  const image = imageReference(config.images.alasio);
  return {
    apiVersion: "apps/v1",
    kind: "Deployment",
    metadata: { name: RELEASE, namespace: NAMESPACE, labels: labels("alasio") },
    spec: {
      replicas: 1,
      strategy: { type: "Recreate" },
      selector: { matchLabels: selectorLabels("alasio") },
      template: {
        metadata: { labels: labels("alasio"), annotations: { "checksum/sandbox-templates": templatesChecksum } },
        spec: {
          serviceAccountName: RELEASE,
          // alasio finishes its turns' bookkeeping and records what it interrupts as it stops.
          terminationGracePeriodSeconds: 60,
          securityContext: { ...restrictedPod(uid, gid), ...given("supplementalGroups", groups) },
          ...imagePullSecrets(config),
          initContainers: [
            // alasio's state directory and, unless it is the operator's, its home, on its volume.
            {
              name: "prepare",
              image,
              imagePullPolicy: config.images.pullPolicy,
              command: ["mkdir", "-p", "/var/lib/alasio/state", "/var/lib/alasio/home"],
              securityContext: restrictedContainer(),
              ...helperResources(),
              volumeMounts: [{ name: "state", mountPath: "/var/lib/alasio" }],
            },
          ],
          containers: [{
            name: "alasio",
            image,
            imagePullPolicy: config.images.pullPolicy,
            workingDir: home,
            env: alasioEnv(config, home),
            ...given("envFrom", [...alasio.envFrom]),
            ports: [{ name: "telemetry", containerPort: config.telemetry.receiverPort }],
            securityContext: restrictedContainer(),
            resources: alasio.resources,
            volumeMounts: [
              { name: "state", mountPath: "/var/lib/alasio" },
              { name: "templates", mountPath: "/etc/alasio", readOnly: true },
              { name: "database", mountPath: "/run/alasio/database", readOnly: true },
              ...(host.enabled ? hostVolumeMounts(config) : []),
            ],
          }],
          volumes: [
            { name: "state", persistentVolumeClaim: { claimName: alasio.persistence.existingClaim || RELEASE } },
            { name: "templates", configMap: { name: componentName("sandbox-templates") } },
            {
              name: "database",
              secret: { secretName: databaseSecret(config), items: [{ key: "url", path: "url" }, { key: "lake-password", path: "lake-password" }] },
            },
            ...(host.enabled ? hostVolumes(config) : []),
          ],
          ...given("nodeSelector", { ...alasio.nodeSelector }),
          ...given("tolerations", [...alasio.tolerations]),
          ...given("affinity", alasio.affinity),
        },
      },
    },
  };
}

/** alasio's volume, unless the configuration names an existing claim. */
function volume(config: InstallConfig): V1PersistentVolumeClaim[] {
  const { persistence } = config.alasio;
  if (persistence.existingClaim) return [];
  return [{
    apiVersion: "v1",
    kind: "PersistentVolumeClaim",
    metadata: { name: RELEASE, namespace: NAMESPACE, labels: labels("alasio") },
    spec: {
      accessModes: ["ReadWriteOnce"],
      ...(persistence.storageClassName ? { storageClassName: persistence.storageClassName } : {}),
      resources: { requests: { storage: persistence.size } },
    },
  }];
}

/** Where session sandboxes export their telemetry: alasio's receiver. */
function telemetryService(config: InstallConfig): V1Service {
  return {
    apiVersion: "v1",
    kind: "Service",
    metadata: { name: componentName("telemetry"), namespace: NAMESPACE, labels: labels("alasio") },
    spec: { selector: selectorLabels("alasio"), ports: [{ name: "otlp-http", port: config.telemetry.receiverPort, targetPort: "telemetry" }] },
  };
}

/**
 * The namespaces workspaces run in, which alasio makes unless told not to. Sessions'
 * enforces Pod Security "restricted": nothing privileged, no host paths, no added
 * capabilities. The host profile's is "privileged", as its pods mount the machine.
 */
function namespaces(config: InstallConfig): V1Namespace[] {
  const { sessions, host } = config;
  return [
    ...(sessions.enabled && sessions.createNamespace
      ? [{
        apiVersion: "v1",
        kind: "Namespace",
        metadata: {
          name: sessions.namespace,
          labels: {
            ...labels("session"),
            "pod-security.kubernetes.io/enforce": "restricted",
            "pod-security.kubernetes.io/enforce-version": "latest",
            "pod-security.kubernetes.io/warn": "restricted",
          },
        },
      }]
      : []),
    ...(host.enabled && host.createNamespace
      ? [{
        apiVersion: "v1",
        kind: "Namespace",
        metadata: { name: host.namespace, labels: { ...labels("folder-bayma"), "pod-security.kubernetes.io/enforce": "privileged" } },
      }]
      : []),
  ];
}

/**
 * Whether alasio removes the JuiceFS mount pods of a session whose mount is lost as it
 * restarts it (src/sandbox/index.ts): where it installs JuiceFS's driver, whose mount pods
 * it then knows to be in the driver's namespace.
 */
function mountPodsRemoved({ workspaceStorage }: InstallConfig): boolean {
  return workspaceStorage.enabled && workspaceStorage.csi.enabled;
}

/**
 * alasio's identity: it drives its workspaces' Sandboxes, their token Secrets, and reads
 * files from sessions through exec, in the namespaces it owns; where it forks sessions,
 * reads sessions' volume claims and runs the Jobs that clone them in its own; and, where
 * it installs JuiceFS's driver, lists and deletes pods in the driver's namespace, its
 * mount pods; and nothing else.
 */
function identity(config: InstallConfig): KubernetesObject[] {
  const binding = (metadata: V1ObjectMeta): V1RoleBinding => ({
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "RoleBinding",
    metadata,
    subjects: [{ kind: "ServiceAccount", name: RELEASE, namespace: NAMESPACE }],
    roleRef: { apiGroup: "rbac.authorization.k8s.io", kind: "Role", name: RELEASE },
  });
  const inDriverNamespace = { name: RELEASE, namespace: config.workspaceStorage.csi.namespace, labels: labels("alasio") };
  const mountPods: V1Role = {
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "Role",
    metadata: inDriverNamespace,
    rules: [{ apiGroups: [""], resources: ["pods"], verbs: ["list", "delete"] }],
  };
  const inOwnNamespace = { name: RELEASE, namespace: NAMESPACE, labels: labels("alasio") };
  const clones: V1Role = {
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "Role",
    metadata: inOwnNamespace,
    rules: [{ apiGroups: ["batch"], resources: ["jobs"], verbs: ["create", "get", "delete"] }],
  };
  const serviceAccount: V1ServiceAccount = { apiVersion: "v1", kind: "ServiceAccount", metadata: { name: RELEASE, namespace: NAMESPACE, labels: labels("alasio") } };
  return [
    serviceAccount,
    ...workspaceNamespaces(config).flatMap((namespace): KubernetesObject[] => {
      const metadata = { name: RELEASE, namespace, labels: labels("alasio") };
      const forked = sessionsForked(config) && namespace === config.sessions.namespace;
      const role: V1Role = {
        apiVersion: "rbac.authorization.k8s.io/v1",
        kind: "Role",
        metadata,
        rules: [
          { apiGroups: ["agents.x-k8s.io"], resources: ["sandboxes"], verbs: ["get", "list", "watch", "create", "patch", "delete"] },
          { apiGroups: [""], resources: ["secrets"], verbs: ["get", "create"] },
          { apiGroups: [""], resources: ["pods"], verbs: ["get"] },
          { apiGroups: [""], resources: ["pods/exec"], verbs: ["create", "get"] },
          ...(forked ? [{ apiGroups: [""], resources: ["persistentvolumeclaims"], verbs: ["get"] }] : []),
        ],
      };
      return [role, binding(metadata)];
    }),
    ...(sessionsForked(config) ? [clones, binding(inOwnNamespace)] : []),
    ...(mountPodsRemoved(config) ? [mountPods, binding(inDriverNamespace)] : []),
    ...(config.host.enabled ? hostAgentIdentity(config) : []),
  ];
}

/**
 * Folder workspaces' bayma: the operator's agents, which may restart alasio as they
 * would restart it on the machine, by rolling out its Deployment, and do nothing else
 * with the cluster.
 */
function hostAgentIdentity(config: InstallConfig): KubernetesObject[] {
  const agent = componentName("host-agent");
  const restart = componentName("restart");
  const serviceAccount: V1ServiceAccount = {
    apiVersion: "v1",
    kind: "ServiceAccount",
    metadata: { name: agent, namespace: config.host.namespace, labels: labels("folder-bayma") },
  };
  const role: V1Role = {
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "Role",
    metadata: { name: restart, namespace: NAMESPACE, labels: labels("folder-bayma") },
    rules: [
      { apiGroups: ["apps"], resources: ["deployments"], resourceNames: [RELEASE], verbs: ["get", "patch"] },
      { apiGroups: ["apps"], resources: ["replicasets"], verbs: ["get", "list", "watch"] },
    ],
  };
  const binding: V1RoleBinding = {
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "RoleBinding",
    metadata: { name: restart, namespace: NAMESPACE, labels: labels("folder-bayma") },
    subjects: [{ kind: "ServiceAccount", name: agent, namespace: config.host.namespace }],
    roleRef: { apiGroup: "rbac.authorization.k8s.io", kind: "Role", name: restart },
  };
  return [serviceAccount, role, binding];
}

/** alasio's objects. */
export function alasioObjects(config: InstallConfig): KubernetesObject[] {
  const templates = sandboxTemplates(config);
  return [
    ...namespaces(config),
    ...identity(config),
    templates.configMap,
    ...volume(config),
    deployment(config, templates.checksum),
    telemetryService(config),
  ];
}
