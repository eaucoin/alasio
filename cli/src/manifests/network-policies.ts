/**
 * The NetworkPolicies confining sessions, folder workspaces' bayma, alasio, Neon, the
 * object store's parts and its filer store, the telemetry collector, Grafana, workspace
 * storage and the node agents, which need a cluster that enforces NetworkPolicy, as
 * session filesystems do; and branch environments' (./branch.ts), whose namespaces main's
 * policies admit by the label each has, to what they share alone.
 */
import type { V1NetworkPolicy, V1NetworkPolicyEgressRule, V1NetworkPolicyIngressRule, V1NetworkPolicyPeer, V1NetworkPolicySpec } from "@kubernetes/client-node";

import { COMPUTE_CTL_PORT, COMPUTE_PORT } from "../../../neon/control/branches.ts";
import { BRANCH_FORK_PORT } from "../../../src/branch/names.ts";
import { NODE_AGENT_PORT } from "../../../src/node-agent/names.ts";
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
  nodeAgentRuns,
  RELEASE,
  selectorLabels,
} from "./common.ts";
import type { InstallConfig } from "./config.ts";
import { replicated } from "./disruption.ts";
import { GOSSIP_PORT } from "./grafana.ts";
import { LAKE_QUERY_PORT } from "./lake.ts";
import {
  BACKUP_COMPONENT,
  COMPONENTS,
  FILER,
  FILER_STORE,
  GATEWAY,
  LIFECYCLE_COMPONENT,
  MASTER,
  METRICS_PORT,
  RESTORE_COMPONENTS,
  RESTORE_SOURCE,
  VOLUME_SERVER,
} from "./object-store.ts";
import { PROXY_PORT } from "./neon.ts";
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

/** The node agents' pods, as a peer. */
const NODE_AGENT = { podSelector: { matchLabels: selectorLabels("node-agent") } };

/** The cluster's DNS, as an egress rule. */
const CLUSTER_DNS: V1NetworkPolicyEgressRule = {
  to: [{ namespaceSelector: namespaceNamed("kube-system"), podSelector: { matchLabels: { "k8s-app": "kube-dns" } } }],
  ports: [{ protocol: "UDP", port: 53 }, { protocol: "TCP", port: 53 }],
};

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

/** The object store's parts, of the stack, to each of which partPolicies and gatewayPolicy admit what talks to it alone. */
const PARTS = [COMPONENTS.gateway, COMPONENTS.master, COMPONENTS.volumeServer, COMPONENTS.filer];

/** A component of main's, as a peer from any namespace. */
const ofMain = (component: string): V1NetworkPolicyPeer => ({ namespaceSelector: namespaceNamed(NAMESPACE), podSelector: { matchLabels: selectorLabels(component) } });

/**
 * Neon's services, the lake and their jobs reach each other, but the compute, which
 * takes its clients' sessions from main's Neon proxy alone, which every client connects
 * through, and neon-control's, which wakes it and counts its sessions, and on
 * compute_ctl's port neon-control's, which gives it its spec anew, and the collector's
 * scrapes of compute_ctl's metrics. In a branch environment, its lake and compute
 * likewise, but for the scrapes, as the collector scrapes main's compute alone. The object
 * store's parts, of the stack too, are reached as partPolicies and gatewayPolicy say alone.
 */
function neonPolicies({ namespace }: Environment): NetworkPolicy[] {
  return [
    policy(componentName("neon"), namespace, "neon", {
      podSelector: { matchLabels: STACK, matchExpressions: [{ key: "app.kubernetes.io/component", operator: "NotIn", values: [...PARTS, "neon-compute"] }] },
      policyTypes: ["Ingress"],
      ingress: [{ from: [{ podSelector: { matchLabels: STACK } }] }],
    }),
    policy(componentName("neon-compute"), namespace, "neon", {
      podSelector: { matchLabels: selectorLabels("neon-compute") },
      policyTypes: ["Ingress"],
      ingress: [
        { from: [ofMain("neon-proxy"), ofMain("neon-control")], ports: [{ protocol: "TCP", port: COMPUTE_PORT }] },
        { from: [ofMain("neon-control")], ports: [{ protocol: "TCP", port: COMPUTE_CTL_PORT }] },
        // The collector scrapes main's compute_ctl alone (collector.ts).
        ...(namespace === NAMESPACE ? [{ from: [ofMain("collector")], ports: [{ protocol: "TCP", port: COMPUTE_CTL_PORT }] }] : []),
      ],
    }),
  ];
}

/**
 * Neon's proxy takes the sessions of every client of a compute: the stack's own pods of
 * main's (neonPolicies), alasio, and branch environments' alasio (their lakes reach it as
 * what branches share, branchStoragePolicy; Grafana as grafanaPolicies admit it).
 */
function proxyPolicy(): NetworkPolicy {
  return policy(componentName("neon-proxy"), NAMESPACE, "neon", {
    podSelector: { matchLabels: selectorLabels("neon-proxy") },
    policyTypes: ["Ingress"],
    ingress: [{
      from: [
        { podSelector: { matchLabels: selectorLabels("alasio") } },
        { namespaceSelector: BRANCH_NAMESPACES, podSelector: { matchLabels: selectorLabels("alasio") } },
      ],
      ports: [{ protocol: "TCP", port: PROXY_PORT }],
    }],
  });
}

/**
 * Branch environments' computes and lakes reach main's storage, which they share: the
 * pageserver and safekeepers their timelines are on, neon-control their specs are
 * served by, the proxy their lakes reach their computes through, and the object store the
 * lake's files are in; nothing else of main's, nor its compute, which is main's database.
 */
function branchStoragePolicy(config: InstallConfig): NetworkPolicy {
  const shared = ["neon-pageserver", "neon-safekeeper", "neon-control", "neon-proxy", ...(config.objectStore.bundled.enabled ? ["seaweedfs"] : [])];
  return policy(componentName("neon-branches"), NAMESPACE, "neon", {
    podSelector: { matchLabels: { "app.kubernetes.io/instance": RELEASE }, matchExpressions: [{ key: "app.kubernetes.io/component", operator: "In", values: shared }] },
    policyTypes: ["Ingress"],
    ingress: [{
      from: [{ namespaceSelector: BRANCH_NAMESPACES, podSelector: { matchLabels: STACK } }],
      ports: [6400, 5454, 8080, PROXY_PORT, 8333].map((port) => ({ protocol: "TCP", port })),
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
 * The bundled object store's parts behind its gateways, each reached by what talks to it
 * alone, on its HTTP and gRPC ports (SeaweedFS's gRPC port is the HTTP port's plus 10000),
 * and by the collector on its metrics port:
 *
 * - the masters, by each other, in Raft, and by every part that asks them where volumes
 *   are, or registers: the volume servers, the filers, the gateways (whose sidecar makes
 *   the buckets through `weed shell`), and the lifecycle pass;
 * - the volume servers, by each other, which copy what they are written to the others,
 *   the masters, which compact their volumes, the filers and gateways, which read and
 *   write objects' chunks there, and the lifecycle pass, which copies volumes again;
 * - the filers, by each other, which follow each other's changes, the gateways, and the
 *   lifecycle pass, which replays their log.
 *
 * The stack's other pods, which reach the store through its gateways, reach none of them.
 */
function partPolicies(): NetworkPolicy[] {
  const of = (component: string): V1NetworkPolicyPeer => ({ podSelector: { matchLabels: selectorLabels(component) } });
  const ports = (http: number) => [http, http + 10_000].map((port) => ({ protocol: "TCP", port }));
  const part = (name: string, component: string, http: number, from: readonly string[]) =>
    policy(name, NAMESPACE, component, {
      podSelector: { matchLabels: selectorLabels(component) },
      policyTypes: ["Ingress"],
      ingress: [
        { from: from.map(of), ports: ports(http) },
        { from: [of("collector")], ports: [{ protocol: "TCP", port: METRICS_PORT }] },
      ],
    });
  const { gateway, master, volumeServer, filer } = COMPONENTS;
  return [
    part(MASTER, master, 9333, [master, volumeServer, filer, gateway, LIFECYCLE_COMPONENT]),
    part(VOLUME_SERVER, volumeServer, 8080, [volumeServer, master, filer, gateway, LIFECYCLE_COMPONENT]),
    part(FILER, filer, 8888, [filer, gateway, LIFECYCLE_COMPONENT]),
  ];
}

/**
 * The bundled object store's gateways take the stack's pods on their S3 port, the
 * store's endpoint (and branch environments' and workspace storage's, as their own
 * policies say); the lifecycle pass alone on their gRPC port, through which it deletes;
 * and the collector alone on their metrics port.
 */
function gatewayPolicy(): NetworkPolicy {
  return policy(GATEWAY, NAMESPACE, COMPONENTS.gateway, {
    podSelector: { matchLabels: selectorLabels(COMPONENTS.gateway) },
    policyTypes: ["Ingress"],
    ingress: [
      { from: [{ podSelector: { matchLabels: STACK } }], ports: [{ protocol: "TCP", port: 8333 }] },
      { from: [{ podSelector: { matchLabels: selectorLabels(LIFECYCLE_COMPONENT) } }], ports: [{ protocol: "TCP", port: 18_333 }] },
      { from: [{ podSelector: { matchLabels: selectorLabels("collector") } }], ports: [{ protocol: "TCP", port: METRICS_PORT }] },
    ],
  });
}

/**
 * The bundled object store's filer store takes no connection but its filers', the
 * collector's, which reads its metrics, and its backups': it is not of the data stack,
 * whose pods reach each other. As `alasio restore-filer-store` puts a backup back, the
 * Valkey that serves it is reached by the Job that loads it alone.
 */
function filerStorePolicies(): NetworkPolicy[] {
  const of = (component: string): V1NetworkPolicyPeer => ({ podSelector: { matchLabels: selectorLabels(component) } });
  const valkey = (name: string, component: string, from: readonly string[]) =>
    policy(name, NAMESPACE, component, {
      podSelector: { matchLabels: selectorLabels(component) },
      policyTypes: ["Ingress"],
      ingress: [{ from: from.map(of), ports: [{ protocol: "TCP", port: VALKEY_PORT }] }],
    });
  return [
    valkey(FILER_STORE, COMPONENTS.filerStore, [COMPONENTS.filer, "collector", BACKUP_COMPONENT]),
    valkey(RESTORE_SOURCE, RESTORE_COMPONENTS.source, [RESTORE_COMPONENTS.restore]),
  ];
}

/**
 * The telemetry collector takes OTLP from alasio, the node agents and folder workspaces'
 * bayma, beside the stack's own pods (neonPolicies), and from branch environments'
 * alasio, compute and lake, and from nothing else: sessions' telemetry reaches it through
 * their alasio's receiver alone.
 */
function collectorPolicy(config: InstallConfig): NetworkPolicy {
  const { host } = config;
  return policy(componentName("collector"), NAMESPACE, "collector", {
    podSelector: { matchLabels: selectorLabels("collector") },
    policyTypes: ["Ingress"],
    ingress: [{
      from: [
        { podSelector: { matchLabels: selectorLabels("alasio") } },
        ...(nodeAgentRuns(config) ? [NODE_AGENT] : []),
        ...(host.enabled ? [{ namespaceSelector: namespaceNamed(host.namespace) }] : []),
        { namespaceSelector: BRANCH_NAMESPACES, podSelector: { matchLabels: selectorLabels("alasio") } },
        { namespaceSelector: BRANCH_NAMESPACES, podSelector: { matchLabels: STACK } },
      ],
      ports: [{ protocol: "TCP", port: COLLECTOR_PORT }],
    }],
  });
}

/**
 * Grafana takes no connection but its replicas' gossip, where it may run several:
 * `alasio grafana` reaches it through the API server's port-forward, which no
 * NetworkPolicy sees. It reaches the cluster's DNS, its database through Neon's proxy and
 * the lake's query endpoint, each of which admits it, its replicas, and Telegram's Bot
 * API, as alasio does, on the internet's HTTPS port; nothing else, and no other address
 * of the cluster or its network.
 */
function grafanaPolicies({ sessions, grafana: { scaling } }: InstallConfig): NetworkPolicy[] {
  const grafana = { podSelector: { matchLabels: selectorLabels("grafana") } };
  const gossip = replicated(scaling) ? [{ protocol: "TCP", port: GOSSIP_PORT }, { protocol: "UDP", port: GOSSIP_PORT }] : [];
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
      ...(gossip.length > 0 ? { ingress: [{ from: [grafana], ports: gossip }] } : {}),
      egress: [
        ...(gossip.length > 0 ? [{ to: [grafana], ports: gossip }] : []),
        CLUSTER_DNS,
        { to: [{ podSelector: { matchLabels: selectorLabels("neon-proxy") } }], ports: [{ protocol: "TCP", port: PROXY_PORT }] },
        { to: [{ podSelector: { matchLabels: selectorLabels("lake") } }], ports: [{ protocol: "TCP", port: LAKE_QUERY_PORT }] },
        // The addresses sessions with internet may not reach, the cluster's and its network's, are Grafana's too.
        { to: [{ ipBlock: { cidr: "0.0.0.0/0", except: [...sessions.blockedCidrs] } }], ports: [{ protocol: "TCP", port: 443 }] },
      ],
    }),
    admitted("neon-proxy", PROXY_PORT),
    admitted("lake", LAKE_QUERY_PORT),
  ];
}

/**
 * The node agents take alasio's calls, on their API's port, and nothing else; they reach
 * the cluster's DNS, the object store's S3 port, the bundled one's or the operator's at
 * any address, and the collector where it runs, and nothing else. The bundled object
 * store admits them.
 */
function nodeAgentPolicies(config: InstallConfig): NetworkPolicy[] {
  const { bundled, external } = config.objectStore;
  const seaweedfs = { podSelector: { matchLabels: selectorLabels("seaweedfs") } };
  const endpoint = bundled.enabled ? null : new URL(external.endpoint);
  const objectStore: V1NetworkPolicyEgressRule = endpoint
    ? { to: [{ ipBlock: { cidr: "0.0.0.0/0" } }], ports: [{ protocol: "TCP", port: Number(endpoint.port) || (endpoint.protocol === "https:" ? 443 : 80) }] }
    : { to: [seaweedfs], ports: [{ protocol: "TCP", port: 8333 }] };
  return [
    policy(componentName("node-agent"), NAMESPACE, "node-agent", {
      ...NODE_AGENT,
      policyTypes: ["Ingress", "Egress"],
      ingress: [{ from: [{ podSelector: { matchLabels: selectorLabels("alasio") } }], ports: [{ protocol: "TCP", port: NODE_AGENT_PORT }] }],
      egress: [
        CLUSTER_DNS,
        objectStore,
        ...(collectorRuns(config) ? [{ to: [{ podSelector: { matchLabels: selectorLabels("collector") } }], ports: [{ protocol: "TCP", port: COLLECTOR_PORT }] }] : []),
      ],
    }),
    ...(bundled.enabled
      ? [policy(componentName("seaweedfs-snapshots"), NAMESPACE, "seaweedfs", {
        ...seaweedfs,
        policyTypes: ["Ingress"],
        ingress: [{ from: [NODE_AGENT], ports: [{ protocol: "TCP", port: 8333 }] }],
      })]
      : []),
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
    ...(config.neon.enabled ? [...neonPolicies(environment), proxyPolicy(), branchStoragePolicy(config)] : []),
    ...(config.neon.enabled && config.objectStore.bundled.enabled ? [gatewayPolicy(), ...partPolicies(), ...filerStorePolicies()] : []),
    ...(collectorRuns(config) ? [collectorPolicy(config)] : []),
    ...(grafanaRuns(config) ? grafanaPolicies(config) : []),
    ...(config.workspaceStorage.enabled ? workspaceStoragePolicies(config) : []),
    ...(nodeAgentRuns(config) ? nodeAgentPolicies(config) : []),
  ];
}
