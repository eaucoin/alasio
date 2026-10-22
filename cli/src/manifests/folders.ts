/**
 * Folder conversations' instances (src/gateway/folders.ts): the template of their pods, of the
 * host profile, which the sandbox gateway makes each of at its bind, in the host profile's
 * namespace, as the folder pods' policy pins it (./admission.ts folderPodPolicy); and the
 * gateway's identity there, which makes and deletes them and their tokens' Secrets; and the
 * budget that lets no drain evict a bound one.
 */
import type { KubernetesObject, V1PodTemplateSpec, V1Volume } from "@kubernetes/client-node";

import { FOLDER_TOKEN_FILE } from "../../../src/gateway/names.ts";
import { sandboxBaymaTelemetryEnv } from "../../../src/sandbox/telemetry.ts";
import { imageReference } from "../images.ts";
import { accountRole, componentName, envOf, given, hostVolumeMounts, hostVolumes, imagePullSecrets, NAMESPACE, otelEnv, RELEASE } from "./common.ts";
import type { InstallConfig } from "./config.ts";
import { folderDeletionPolicy, folderPodPolicy, folderSecretPolicy } from "./admission.ts";
import { boundBudget } from "./disruption.ts";
import { priority } from "./priorities.ts";

/** The port a folder instance's bayma serves MCP on, as every bound pod's does, which the gateway passes requests to (./sandbox-gateway.ts POOL_POD_PORT). */
export const FOLDER_PORT = 7290;

/** The gateway's ServiceAccount, its user's name, and the receiver's Service (./sandbox-gateway.ts). */
const GATEWAY = componentName("sandbox-gateway");
const GATEWAY_USER = `system:serviceaccount:${NAMESPACE}:${GATEWAY}`;
const RECEIVER = componentName("receiver");

/** Whether the installation has folder workspaces, whose instances the sandbox gateway makes: the host profile. */
export function foldersRun(config: InstallConfig): boolean {
  return config.host.enabled;
}

/** The template of a folder instance's pod, as the operator runs bayma on their own machine: bayma itself, as them, with the machine's mounts, under the cluster's own runtime, which its process snapshots need. */
export function hostPodTemplate(config: InstallConfig): V1PodTemplateSpec {
  const { host } = config;
  return {
    metadata: { labels: { "app.kubernetes.io/part-of": "alasio", "app.kubernetes.io/instance": RELEASE, "app.kubernetes.io/component": "folder-bayma" } },
    spec: {
      // May restart alasio, by rolling out its Deployment, as an agent restarts it; its token
      // mounted by the template itself, where the API server would mount one, so the pod the
      // folder pods' policy pins is the pod as made.
      serviceAccountName: componentName("host-agent"),
      automountServiceAccountToken: false,
      ...priority("turn"),
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
        args: ["mcp-http", "--host", "0.0.0.0", "--port", String(FOLDER_PORT), "--default-durability", "checkpointed"],
        workingDir: host.home,
        env: [{ name: "HOME", value: host.home }, ...envOf(host.env)],
        ports: [{ name: "mcp", containerPort: FOLDER_PORT }],
        securityContext: {
          // What bayma needs to snapshot its REPL sessions' processes as it stops and
          // restore them whole as it starts again.
          capabilities: { add: ["CHECKPOINT_RESTORE", "SYS_PTRACE"] },
          seccompProfile: { type: "Unconfined" },
          appArmorProfile: { type: "Unconfined" },
        },
        resources: host.resources,
        volumeMounts: [...hostVolumeMounts(config), { name: ACCOUNT_VOLUME, mountPath: ACCOUNT_DIRECTORY, readOnly: true }],
      }],
      volumes: [...hostVolumes(config), accountVolume],
    },
  };
}

/** The volume of the folder instances' account's token, and where it is mounted, as the API server mounts one. */
const ACCOUNT_VOLUME = "alasio-host-agent-token";
const ACCOUNT_DIRECTORY = "/var/run/secrets/kubernetes.io/serviceaccount";
const accountVolume: V1Volume = {
  name: ACCOUNT_VOLUME,
  projected: {
    defaultMode: 0o644,
    sources: [
      { serviceAccountToken: { expirationSeconds: 3607, path: "token" } },
      { configMap: { name: "kube-root-ca.crt", items: [{ key: "ca.crt", path: "ca.crt" }] } },
      { downwardAPI: { items: [{ path: "namespace", fieldRef: { apiVersion: "v1", fieldPath: "metadata.namespace" } }] } },
    ],
  },
};

/**
 * The folder instances' template, as the gateway's configuration carries it: the host
 * profile's, its bayma exporting its telemetry to the session telemetry receiver, as a
 * bound pool pod's does, with its binding's token, the signals the installation exports, in
 * their protocols, where it exports any.
 */
export function folderTemplate(config: InstallConfig): V1PodTemplateSpec {
  const template = hostPodTemplate(config);
  const exported = sandboxBaymaTelemetryEnv(Object.fromEntries(otelEnv(config).map(({ name, value }) => [name, value ?? ""])));
  if (Object.keys(exported).length === 0) return template;
  const telemetry = [
    { name: "OTEL_EXPORTER_OTLP_ENDPOINT", value: `http://${RECEIVER}.${NAMESPACE}.svc.cluster.local:${config.telemetry.receiverPort}` },
    ...Object.entries(exported).map(([name, value]) => ({ name, value })),
  ];
  return {
    ...template,
    spec: {
      ...template.spec,
      containers: (template.spec?.containers ?? []).map((container) => ({
        ...container,
        args: [...(container.args ?? []), "--telemetry-token-file", FOLDER_TOKEN_FILE],
        env: [...(container.env ?? []), ...telemetry],
      })),
    },
  };
}

/**
 * The gateway's identity in the folder namespace: it makes and deletes the folder instances'
 * pods, reads their status and what the metrics server says they use, and holds their
 * bindings' Leases there, and its folders' records beside them; and makes the Secret of each
 * one's token, which the pod then owns. A bound one no drain evicts.
 */
export function folderObjects(config: InstallConfig): KubernetesObject[] {
  if (!foldersRun(config)) return [];
  const scope = { namespace: config.host.namespace, maker: GATEWAY_USER };
  return [
    ...accountRole(GATEWAY, "sandbox-gateway", config.host.namespace, [
      { apiGroups: [""], resources: ["pods"], verbs: ["get", "list", "watch", "create", "update", "delete"] },
      { apiGroups: [""], resources: ["secrets"], verbs: ["create", "patch", "delete"] },
      { apiGroups: ["coordination.k8s.io"], resources: ["leases"], verbs: ["get", "list", "watch", "create", "update", "patch", "delete"] },
      // What each instance's bayma uses, which its folder's next instance requests (src/gateway/folders.ts SIZING).
      { apiGroups: ["metrics.k8s.io"], resources: ["pods"], verbs: ["list"] },
    ]),
    // The receiver, which takes the telemetry of folder instances' bindings, watching their Leases.
    ...accountRole(RECEIVER, "receiver", config.host.namespace, [{ apiGroups: ["coordination.k8s.io"], resources: ["leases"], verbs: ["list", "watch"] }]),
    // Every pod of the folder namespace held to the folder template, as Pod Security cannot, and the gateway's Secrets there to their tokens.
    ...folderPodPolicy(folderTemplate(config), { ...scope, stateRoot: config.host.stateRoot }),
    ...folderSecretPolicy(scope),
    // And what it deletes there, its own instances' pods and tokens alone.
    ...folderDeletionPolicy(scope),
    // A bound instance holds its REPLs live until its release: no drain evicts it, as none does a bound pool pod.
    boundBudget(config.host.namespace),
  ];
}
