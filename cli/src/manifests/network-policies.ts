/**
 * The NetworkPolicies confining sessions, folder workspaces' bayma, alasio, Neon, the
 * telemetry collector, Grafana and workspace storage, which need a cluster that enforces
 * NetworkPolicy, as session filesystems do; and branch environments' (./branch.ts), whose
 * namespaces main's policies admit by the label each has, to what they share alone.
 */
import type { V1NetworkPolicy, V1NetworkPolicyIngressRule, V1NetworkPolicyPeer, V1NetworkPolicySpec } from "@kubernetes/client-node";

import { BRANCH_FORK_PORT } from "../../../src/branch/names.ts";
import { forksForBranches, sessionsNamespace } from "./alasio.ts";
import {
  BRANCH_LABEL,
  COLLECTOR_PORT,
  collectorRuns,
  componentName,
  type Environment,
  given,
  grafanaRuns,
  labels,
  MAIN_ENVIRONMENT,
  NAMESPACE,
  RELEASE,
  selectorLabels,
} from "./common.ts";
import type { InstallConfig } from "./config.ts";
import { LAKE_QUERY_PORT } from "./lake.ts";
import { CONTROLLER_POD_LABELS, JOB_POD_SELECTOR, MOUNT_POD_LABELS, NODE_POD_LABELS } from "./juicefs-csi.ts";
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

/** The namespace `name`, as a peer's selector. */
const namespaceNamed = (name: string) => ({ matchLabels: { "kubernetes.io/metadata.name": name } });

/** The alasio pod of `environment`, as a peer. */
const alasioOf = ({ namespace }: Environment): V1NetworkPolicyPeer => ({ namespaceSelector: namespaceNamed(namespace), podSelector: { matchLabels: selectorLabels("alasio") } });

/** Every branch environment's namespace, as a peer's selector, by the label each has. */
const BRANCH_NAMESPACES = { matchExpressions: [{ key: BRANCH_LABEL, operator: "Exists" }] };

/** The labels of the data stack's pods, which its policy admits each other by. */
const STACK = { "app.kubernetes.io/instance": RELEASE, "alasio.dev/stack": "neon" };

/** A NetworkPolicy of `name` in `namespace`, labelled as `component`'s. */
function policy(name: string, namespace: string, component: string, spec: NetworkPolicySpec): NetworkPolicy {
  return { apiVersion: "networking.k8s.io/v1", kind: "NetworkPolicy", metadata: { name, namespace, labels: labels(component) }, spec };
}

/**
 * Sessions: nothing in or out but bayma from their alasio, its telemetry receiver (the
 * one egress of a session without internet), and, for a session with internet, the
 * public internet, none of the cluster's or its network's private addresses. A branch
 * environment's sessions are its alasio's alone, as main's are main's.
 */
function sessionPolicies(config: InstallConfig, environment: Environment): NetworkPolicy[] {
  const { blockedCidrs } = config.sessions;
  const namespace = sessionsNamespace(config, environment);
  const alasio = alasioOf(environment);
  return [
    policy("default-deny", namespace, "session", { podSelector: {}, policyTypes: ["Ingress", "Egress"] }),
    policy("bayma-from-alasio", namespace, "session", {
      podSelector: { matchLabels: SESSION },
      policyTypes: ["Ingress"],
      ingress: [{ from: [alasio], ports: [{ protocol: "TCP", port: 7290 }] }],
    }),
    policy("telemetry-to-alasio", namespace, "session", {
      podSelector: { matchLabels: SESSION },
      policyTypes: ["Egress"],
      egress: [{ to: [alasio], ports: [{ protocol: "TCP", port: config.telemetry.receiverPort }] }],
    }),
    policy("full-internet", namespace, "session", {
      podSelector: { matchLabels: { ...SESSION, "alasio.dev/net-mode": "full" } },
      policyTypes: ["Egress"],
      egress: [{ to: [{ ipBlock: { cidr: "0.0.0.0/0", except: [...blockedCidrs] } }] }],
    }),
  ];
}

/**
 * Neon's services, the object store, the lake and their jobs reach each other; alasio
 * reaches the compute, its database. In a branch environment, its compute and its lake
 * reach each other, and its alasio its compute, as main's do.
 */
function neonPolicies({ namespace }: Environment): NetworkPolicy[] {
  return [
    policy(componentName("neon"), namespace, "neon", {
      podSelector: { matchLabels: STACK },
      policyTypes: ["Ingress"],
      ingress: [{ from: [{ podSelector: { matchLabels: STACK } }] }],
    }),
    policy(componentName("neon-compute"), namespace, "neon", {
      podSelector: { matchLabels: selectorLabels("neon-compute") },
      policyTypes: ["Ingress"],
      ingress: [{ from: [{ podSelector: { matchLabels: selectorLabels("alasio") } }], ports: [{ protocol: "TCP", port: 55433 }] }],
    }),
  ];
}

/**
 * Branch environments' computes and lakes reach main's storage, which they share: the
 * pageserver and safekeepers their timelines are on, neon-control their specs are
 * served by, and the object store the lake's files are in; nothing else of main's, nor
 * its compute, which is main's database.
 */
function branchStoragePolicy(config: InstallConfig): NetworkPolicy {
  const shared = ["neon-pageserver", "neon-safekeeper", "neon-control", ...(config.objectStore.bundled.enabled ? ["seaweedfs"] : [])];
  return policy(componentName("neon-branches"), NAMESPACE, "neon", {
    podSelector: { matchLabels: { "app.kubernetes.io/instance": RELEASE }, matchExpressions: [{ key: "app.kubernetes.io/component", operator: "In", values: shared }] },
    policyTypes: ["Ingress"],
    ingress: [{
      from: [{ namespaceSelector: BRANCH_NAMESPACES, podSelector: { matchLabels: STACK } }],
      ports: [6400, 5454, 8080, 8333].map((port) => ({ protocol: "TCP", port })),
    }],
  });
}

/**
 * Workspace storage's metadata and data are reached by JuiceFS's own pods alone (its
 * controller, its node service, the mount pods it runs and the Jobs that delete volumes'
 * directories, in the driver's namespace) and alasio's JuiceFS admin pods: Valkey takes no
 * other connection but the stack's collector's, which reads its metrics, and the bundled
 * object store takes theirs on its S3 port beside the stack's own, and Valkey's, whose pod
 * formats the file system. The driver's Jobs are told by no label of their own, so any
 * Job's pod in its namespace is admitted, which the credentials of each still guard.
 */
function workspaceStoragePolicies(config: InstallConfig): NetworkPolicy[] {
  const driverNamespace = { matchLabels: { "kubernetes.io/metadata.name": config.workspaceStorage.csi.namespace } };
  const juicefs: V1NetworkPolicyPeer[] = [
    { namespaceSelector: driverNamespace, podSelector: { matchLabels: MOUNT_POD_LABELS } },
    { namespaceSelector: driverNamespace, podSelector: { matchLabels: CONTROLLER_POD_LABELS } },
    { namespaceSelector: driverNamespace, podSelector: { matchLabels: NODE_POD_LABELS } },
    { namespaceSelector: driverNamespace, podSelector: JOB_POD_SELECTOR },
    { namespaceSelector: { matchLabels: { "kubernetes.io/metadata.name": NAMESPACE } }, podSelector: { matchLabels: JUICEFS_ADMIN } },
  ];
  return [
    policy(componentName("valkey"), NAMESPACE, "valkey", {
      podSelector: { matchLabels: selectorLabels("valkey") },
      policyTypes: ["Ingress"],
      ingress: [
        { from: juicefs, ports: [{ protocol: "TCP", port: VALKEY_PORT }] },
        { from: [{ podSelector: { matchLabels: selectorLabels("collector") } }], ports: [{ protocol: "TCP", port: VALKEY_PORT }] },
      ],
    }),
    ...(config.objectStore.bundled.enabled
      ? [policy(componentName("seaweedfs-workspaces"), NAMESPACE, "seaweedfs", {
        podSelector: { matchLabels: selectorLabels("seaweedfs") },
        policyTypes: ["Ingress"],
        ingress: [{ from: [...juicefs, { podSelector: { matchLabels: selectorLabels("valkey") } }], ports: [{ protocol: "TCP", port: 8333 }] }],
      })]
      : []),
  ];
}

/**
 * The telemetry collector takes OTLP from alasio and folder workspaces' bayma, beside the
 * stack's own pods (neonPolicies), and from branch environments' alasio, compute and
 * lake, and from nothing else: sessions' telemetry reaches it through their alasio's
 * receiver alone.
 */
function collectorPolicy({ host }: InstallConfig): NetworkPolicy {
  return policy(componentName("collector"), NAMESPACE, "collector", {
    podSelector: { matchLabels: selectorLabels("collector") },
    policyTypes: ["Ingress"],
    ingress: [{
      from: [
        { podSelector: { matchLabels: selectorLabels("alasio") } },
        ...(host.enabled ? [{ namespaceSelector: namespaceNamed(host.namespace) }] : []),
        { namespaceSelector: BRANCH_NAMESPACES, podSelector: { matchLabels: selectorLabels("alasio") } },
        { namespaceSelector: BRANCH_NAMESPACES, podSelector: { matchLabels: STACK } },
      ],
      ports: [{ protocol: "TCP", port: COLLECTOR_PORT }],
    }],
  });
}

/**
 * Grafana takes no connection: `alasio grafana` reaches it through the API server's
 * port-forward, which no NetworkPolicy sees. It reaches the cluster's DNS, its database
 * on the compute and the lake's query endpoint, each of which admits it, and Telegram's
 * Bot API, as alasio does, on the internet's HTTPS port; nothing else, and no other
 * address of the cluster or its network.
 */
function grafanaPolicies({ sessions }: InstallConfig): NetworkPolicy[] {
  const grafana = { podSelector: { matchLabels: selectorLabels("grafana") } };
  const admitted = (component: string, port: number): NetworkPolicy =>
    policy(componentName(`${component}-from-grafana`), NAMESPACE, component, {
      podSelector: { matchLabels: selectorLabels(component) },
      policyTypes: ["Ingress"],
      ingress: [{ from: [grafana], ports: [{ protocol: "TCP", port }] }],
    });
  return [
    policy(componentName("grafana"), NAMESPACE, "grafana", {
      ...grafana,
      policyTypes: ["Ingress", "Egress"],
      egress: [
        {
          to: [{ namespaceSelector: { matchLabels: { "kubernetes.io/metadata.name": "kube-system" } }, podSelector: { matchLabels: { "k8s-app": "kube-dns" } } }],
          ports: [{ protocol: "UDP", port: 53 }, { protocol: "TCP", port: 53 }],
        },
        { to: [{ podSelector: { matchLabels: selectorLabels("neon-compute") } }], ports: [{ protocol: "TCP", port: 55433 }] },
        { to: [{ podSelector: { matchLabels: selectorLabels("lake") } }], ports: [{ protocol: "TCP", port: LAKE_QUERY_PORT }] },
        // The addresses sessions with internet may not reach, the cluster's and its network's, are Grafana's too.
        { to: [{ ipBlock: { cidr: "0.0.0.0/0", except: [...sessions.blockedCidrs] } }], ports: [{ protocol: "TCP", port: 443 }] },
      ],
    }),
    admitted("neon-compute", 55433),
    admitted("lake", LAKE_QUERY_PORT),
  ];
}

/**
 * alasio takes no connections but its sessions' telemetry, and, main, where it forks
 * sessions for branch environments, their alasio's asking it to (src/branch/fork.ts).
 */
function alasioPolicy(config: InstallConfig, environment: Environment): NetworkPolicy {
  const forks = environment.branch === null && forksForBranches(config);
  const ingress: IngressRule[] = [
    ...(config.sessions.enabled
      ? [{
        from: [{ namespaceSelector: namespaceNamed(sessionsNamespace(config, environment)), podSelector: { matchLabels: SESSION } }],
        ports: [{ protocol: "TCP", port: config.telemetry.receiverPort }],
      }]
      : []),
    ...(forks
      ? [{ from: [{ namespaceSelector: BRANCH_NAMESPACES, podSelector: { matchLabels: selectorLabels("alasio") } }], ports: [{ protocol: "TCP", port: BRANCH_FORK_PORT }] }]
      : []),
  ];
  return policy(componentName("alasio"), environment.namespace, "alasio", {
    podSelector: { matchLabels: selectorLabels("alasio") },
    policyTypes: ["Ingress"],
    ...given("ingress", ingress),
  });
}

/**
 * The NetworkPolicies of the alasio of `environment`, unless they are turned off: main's,
 * and those that admit branch environments to what of main's they share; or a branch
 * environment's own.
 */
export function networkPolicyObjects(config: InstallConfig, environment: Environment = MAIN_ENVIRONMENT): NetworkPolicy[] {
  if (!config.networkPolicies.enabled) return [];
  const { sessions, host } = config;
  if (environment.branch !== null) {
    return [...(sessions.enabled ? sessionPolicies(config, environment) : []), alasioPolicy(config, environment), ...neonPolicies(environment)];
  }
  return [
    ...(sessions.enabled ? sessionPolicies(config, environment) : []),
    // Folder workspaces' bayma is reached by alasio alone; what it reaches is the machine's.
    ...(host.enabled
      ? [policy("bayma-from-alasio", host.namespace, "folder-bayma", {
        podSelector: {},
        policyTypes: ["Ingress"],
        ingress: [{ from: [alasioOf(environment)], ports: [{ protocol: "TCP", port: 7290 }] }],
      })]
      : []),
    alasioPolicy(config, environment),
    ...(config.neon.enabled ? [...neonPolicies(environment), branchStoragePolicy(config)] : []),
    ...(collectorRuns(config) ? [collectorPolicy(config)] : []),
    ...(grafanaRuns(config) ? grafanaPolicies(config) : []),
    ...(config.workspaceStorage.enabled ? workspaceStoragePolicies(config) : []),
  ];
}
