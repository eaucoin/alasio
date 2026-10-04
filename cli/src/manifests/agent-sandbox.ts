/**
 * agent-sandbox's Sandbox API and controller (kubernetes-sigs/agent-sandbox v1.0.5),
 * which alasio's workspaces are: its CRD as upstream's release manifest, sandbox.yaml
 * (sha256 e89fd95c0aa57609fa24be4112bd52ce67fe8939ecf6f3c17edf2f1e8f1eb860), has it,
 * unchanged, and its controller as that manifest installs it, in alasio's namespace,
 * with a restricted security context and probes added. One cluster has one controller.
 */
import type {
  KubernetesObject,
  V1ClusterRole,
  V1ClusterRoleBinding,
  V1Deployment,
  V1Role,
  V1RoleBinding,
  V1ServiceAccount,
} from "@kubernetes/client-node";

import { given, NAMESPACE, RELEASE } from "./common.ts";
import type { InstallConfig } from "./config.ts";
import sandboxes from "./sandboxes.agents.x-k8s.io.json" with { type: "json" };

/**
 * The Sandbox CRD, agents.x-k8s.io's `sandboxes`, as the API server takes it, whose
 * schema's keys (`x-kubernetes-*`, say) are not client-node's model's.
 */
export const SANDBOX_CRD: KubernetesObject = sandboxes;

const NAME = `${RELEASE}-agent-sandbox-controller`;

/** The labels of the controller's objects: the controller's name and version, the installation, and alasio as their manager. */
const LABELS = {
  "app.kubernetes.io/name": "agent-sandbox-controller",
  "app.kubernetes.io/instance": RELEASE,
  "app.kubernetes.io/version": "v1.0.5",
  "app.kubernetes.io/managed-by": "alasio",
};

/** The controller: its identity, what it may do (Sandboxes and their pods, volumes and Services, cluster-wide; its leader lease, here), and its Deployment. */
function controller(config: InstallConfig): KubernetesObject[] {
  const { image, resources, nodeSelector, tolerations, affinity } = config.agentSandbox;
  const serviceAccount: V1ServiceAccount = { apiVersion: "v1", kind: "ServiceAccount", metadata: { name: NAME, namespace: NAMESPACE, labels: LABELS } };
  const clusterRole: V1ClusterRole = {
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "ClusterRole",
    metadata: { name: NAME, labels: LABELS },
    rules: [
      { apiGroups: [""], resources: ["persistentvolumeclaims", "pods", "services"], verbs: ["create", "delete", "get", "list", "patch", "update", "watch"] },
      { apiGroups: ["", "events.k8s.io"], resources: ["events"], verbs: ["create", "patch"] },
      { apiGroups: ["agents.x-k8s.io"], resources: ["sandboxes"], verbs: ["create", "delete", "get", "list", "patch", "update", "watch"] },
      { apiGroups: ["agents.x-k8s.io"], resources: ["sandboxes/finalizers", "sandboxes/status"], verbs: ["get", "patch", "update"] },
    ],
  };
  const subjects = [{ kind: "ServiceAccount", name: NAME, namespace: NAMESPACE }];
  const clusterRoleBinding: V1ClusterRoleBinding = {
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "ClusterRoleBinding",
    metadata: { name: NAME, labels: LABELS },
    subjects,
    roleRef: { apiGroup: "rbac.authorization.k8s.io", kind: "ClusterRole", name: NAME },
  };
  // Leader election's lease, in the controller's own namespace only.
  const role: V1Role = {
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "Role",
    metadata: { name: NAME, namespace: NAMESPACE, labels: LABELS },
    rules: [{ apiGroups: ["coordination.k8s.io"], resources: ["leases"], verbs: ["create", "get", "list", "patch", "update", "watch"] }],
  };
  const roleBinding: V1RoleBinding = {
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "RoleBinding",
    metadata: { name: NAME, namespace: NAMESPACE, labels: LABELS },
    subjects,
    roleRef: { apiGroup: "rbac.authorization.k8s.io", kind: "Role", name: NAME },
  };
  const deployment: V1Deployment = {
    apiVersion: "apps/v1",
    kind: "Deployment",
    metadata: { name: NAME, namespace: NAMESPACE, labels: LABELS },
    spec: {
      replicas: 1,
      selector: { matchLabels: { "app.kubernetes.io/name": "agent-sandbox-controller", "app.kubernetes.io/instance": RELEASE } },
      template: {
        metadata: { labels: LABELS },
        spec: {
          serviceAccountName: NAME,
          securityContext: { runAsNonRoot: true, seccompProfile: { type: "RuntimeDefault" } },
          containers: [{
            name: "controller",
            image: `${image.repository}:${image.tag}${image.digest ? `@${image.digest}` : ""}`,
            imagePullPolicy: image.pullPolicy,
            args: ["--leader-elect=true"],
            ports: [{ name: "metrics", containerPort: 8080 }, { name: "healthz", containerPort: 8081 }],
            livenessProbe: { httpGet: { path: "/healthz", port: "healthz" } },
            readinessProbe: { httpGet: { path: "/readyz", port: "healthz" } },
            securityContext: { allowPrivilegeEscalation: false, readOnlyRootFilesystem: true, runAsNonRoot: true, capabilities: { drop: ["ALL"] } },
            resources,
          }],
          ...given("nodeSelector", { ...nodeSelector }),
          ...given("tolerations", [...tolerations]),
          ...given("affinity", affinity),
        },
      },
    },
  };
  return [serviceAccount, clusterRole, clusterRoleBinding, role, roleBinding, deployment];
}

/** agent-sandbox's objects, unless the cluster already has it. */
export function agentSandboxObjects(config: InstallConfig): KubernetesObject[] {
  return config.agentSandbox.enabled ? [SANDBOX_CRD, ...controller(config)] : [];
}
