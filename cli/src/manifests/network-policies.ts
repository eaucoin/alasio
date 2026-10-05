/**
 * The NetworkPolicies confining sessions, folder workspaces' bayma, alasio, Neon and
 * workspace storage,
 * which need a cluster that enforces NetworkPolicy, as session filesystems do.
 */
import type { V1NetworkPolicy, V1NetworkPolicyIngressRule, V1NetworkPolicyPeer, V1NetworkPolicySpec } from "@kubernetes/client-node";

import { componentName, labels, NAMESPACE, RELEASE, selectorLabels } from "./common.ts";
import type { InstallConfig } from "./config.ts";
import { CONTROLLER_POD_LABELS, MOUNT_POD_LABELS, NODE_POD_LABELS } from "./juicefs-csi.ts";
import { VALKEY_PORT } from "./valkey.ts";
import { JUICEFS_ADMIN } from "./workspace-storage.ts";

/**
 * A NetworkPolicy as the API server takes it: client-node's model calls an ingress
 * rule's `from` `_from`, which its serializer renames.
 */
type NetworkPolicy = Omit<V1NetworkPolicy, "spec"> & { spec: NetworkPolicySpec };
type NetworkPolicySpec = Omit<V1NetworkPolicySpec, "ingress"> & { ingress?: IngressRule[] };
type IngressRule = Omit<V1NetworkPolicyIngressRule, "_from"> & { from?: V1NetworkPolicyPeer[] };

/** The label of every session's pod, which alasio gives it. */
const SESSION = { "alasio.dev/workload": "session" };

/** alasio's pod, as a peer. */
const FROM_ALASIO: V1NetworkPolicyPeer = {
  namespaceSelector: { matchLabels: { "kubernetes.io/metadata.name": NAMESPACE } },
  podSelector: { matchLabels: selectorLabels("alasio") },
};

/** A NetworkPolicy of `name` in `namespace`, labelled as `component`'s. */
function policy(name: string, namespace: string, component: string, spec: NetworkPolicySpec): NetworkPolicy {
  return { apiVersion: "networking.k8s.io/v1", kind: "NetworkPolicy", metadata: { name, namespace, labels: labels(component) }, spec };
}

/**
 * Sessions: nothing in or out but bayma from alasio, alasio's telemetry receiver (the
 * one egress of a session without internet), and, for a session with internet, the
 * public internet, none of the cluster's or its network's private addresses.
 */
function sessionPolicies(config: InstallConfig): NetworkPolicy[] {
  const { namespace, blockedCidrs } = config.sessions;
  return [
    policy("default-deny", namespace, "session", { podSelector: {}, policyTypes: ["Ingress", "Egress"] }),
    policy("bayma-from-alasio", namespace, "session", {
      podSelector: { matchLabels: SESSION },
      policyTypes: ["Ingress"],
      ingress: [{ from: [FROM_ALASIO], ports: [{ protocol: "TCP", port: 7290 }] }],
    }),
    policy("telemetry-to-alasio", namespace, "session", {
      podSelector: { matchLabels: SESSION },
      policyTypes: ["Egress"],
      egress: [{ to: [FROM_ALASIO], ports: [{ protocol: "TCP", port: config.telemetry.receiverPort }] }],
    }),
    policy("full-internet", namespace, "session", {
      podSelector: { matchLabels: { ...SESSION, "alasio.dev/net-mode": "full" } },
      policyTypes: ["Egress"],
      egress: [{ to: [{ ipBlock: { cidr: "0.0.0.0/0", except: [...blockedCidrs] } }] }],
    }),
  ];
}

/** Neon's services, the object store, the lake and their jobs reach each other; alasio reaches the compute, its database. */
function neonPolicies(): NetworkPolicy[] {
  const stack = { "app.kubernetes.io/instance": RELEASE, "alasio.dev/stack": "neon" };
  return [
    policy(componentName("neon"), NAMESPACE, "neon", {
      podSelector: { matchLabels: stack },
      policyTypes: ["Ingress"],
      ingress: [{ from: [{ podSelector: { matchLabels: stack } }] }],
    }),
    policy(componentName("neon-compute"), NAMESPACE, "neon", {
      podSelector: { matchLabels: selectorLabels("neon-compute") },
      policyTypes: ["Ingress"],
      ingress: [{ from: [{ podSelector: { matchLabels: selectorLabels("alasio") } }], ports: [{ protocol: "TCP", port: 55433 }] }],
    }),
  ];
}

/**
 * Workspace storage's metadata and data are reached by JuiceFS's own pods alone (its
 * controller, its node service and the mount pods it runs, in the driver's namespace) and
 * alasio's JuiceFS admin pods: Valkey takes no other connection but the stack's
 * collector's, which reads its metrics, and the bundled object store takes theirs on its
 * S3 port beside the stack's own.
 */
function workspaceStoragePolicies(config: InstallConfig): NetworkPolicy[] {
  const driverNamespace = { matchLabels: { "kubernetes.io/metadata.name": config.workspaceStorage.csi.namespace } };
  const juicefs: V1NetworkPolicyPeer[] = [
    { namespaceSelector: driverNamespace, podSelector: { matchLabels: MOUNT_POD_LABELS } },
    { namespaceSelector: driverNamespace, podSelector: { matchLabels: CONTROLLER_POD_LABELS } },
    { namespaceSelector: driverNamespace, podSelector: { matchLabels: NODE_POD_LABELS } },
    { namespaceSelector: { matchLabels: { "kubernetes.io/metadata.name": NAMESPACE } }, podSelector: { matchLabels: JUICEFS_ADMIN } },
  ];
  return [
    policy(componentName("valkey"), NAMESPACE, "valkey", {
      podSelector: { matchLabels: selectorLabels("valkey") },
      policyTypes: ["Ingress"],
      ingress: [
        { from: juicefs, ports: [{ protocol: "TCP", port: VALKEY_PORT }] },
        { from: [{ podSelector: { matchLabels: selectorLabels("neon-collector") } }], ports: [{ protocol: "TCP", port: VALKEY_PORT }] },
      ],
    }),
    ...(config.objectStore.bundled.enabled
      ? [policy(componentName("seaweedfs-workspaces"), NAMESPACE, "seaweedfs", {
        podSelector: { matchLabels: selectorLabels("seaweedfs") },
        policyTypes: ["Ingress"],
        ingress: [{ from: juicefs, ports: [{ protocol: "TCP", port: 8333 }] }],
      })]
      : []),
  ];
}

/** The NetworkPolicies, unless they are turned off. */
export function networkPolicyObjects(config: InstallConfig): NetworkPolicy[] {
  if (!config.networkPolicies.enabled) return [];
  const { sessions, host } = config;
  return [
    ...(sessions.enabled ? sessionPolicies(config) : []),
    // Folder workspaces' bayma is reached by alasio alone; what it reaches is the machine's.
    ...(host.enabled
      ? [policy("bayma-from-alasio", host.namespace, "folder-bayma", {
        podSelector: {},
        policyTypes: ["Ingress"],
        ingress: [{ from: [FROM_ALASIO], ports: [{ protocol: "TCP", port: 7290 }] }],
      })]
      : []),
    // alasio takes no connections but sessions' telemetry.
    policy(componentName("alasio"), NAMESPACE, "alasio", {
      podSelector: { matchLabels: selectorLabels("alasio") },
      policyTypes: ["Ingress"],
      ...(sessions.enabled
        ? {
          ingress: [{
            from: [{ namespaceSelector: { matchLabels: { "kubernetes.io/metadata.name": sessions.namespace } }, podSelector: { matchLabels: SESSION } }],
            ports: [{ protocol: "TCP", port: config.telemetry.receiverPort }],
          }],
        }
        : {}),
    }),
    ...(config.neon.enabled ? neonPolicies() : []),
    ...(config.workspaceStorage.enabled ? workspaceStoragePolicies(config) : []),
  ];
}
