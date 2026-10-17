/**
 * The NetworkPolicies confining sessions, folder workspaces' bayma, alasio, Neon, the
 * object store's parts and its filer store, the log, the telemetry collector, Grafana,
 * workspace storage, the node agents and the sandbox service, which need a cluster that
 * enforces NetworkPolicy, as session filesystems do; and branch environments' (./branch.ts),
 * whose namespaces main's policies admit by the label each has, to what they share alone.
 */
import type { V1NetworkPolicy, V1NetworkPolicyEgressRule, V1NetworkPolicyIngressRule, V1NetworkPolicyPeer, V1NetworkPolicySpec } from "@kubernetes/client-node";

import { COMPUTE_CTL_PORT, COMPUTE_PORT } from "../../../neon/control/branches.ts";
import { BRANCH_FORK_PORT } from "../../../src/branch/names.ts";
import type { ApiServer } from "../api-server.ts";
import { POOL_LABEL } from "../../../src/binder/names.ts";
import { NET_MODE_LABEL } from "../../../src/kube/sandbox-shape.ts";
import { NODE_AGENT_PORT } from "../../../src/node-agent/names.ts";
import { forksForBranches, sessionsNamespace } from "./alasio.ts";
import { GATEWAY_COMPONENT, SCRAPER_COMPONENT } from "./collector.ts";
import {
  BRANCH_LABEL,
  COLLECTOR_PORT,
  collectorRuns,
  componentName,
  type Environment,
  given,
  grafanaRuns,
  labels,
  lakeRuns,
  MAIN_ENVIRONMENT,
  NAMESPACE,
  nodeAgentRuns,
  RELEASE,
  selectorLabels,
} from "./common.ts";
import type { InstallConfig } from "./config.ts";
import { replicated } from "./disruption.ts";
import { GOSSIP_PORT } from "./grafana.ts";
import { INTAKE_COMPONENT, LAKE_QUERY_PORT, LOADER_COMPONENT, QUERY_COMPONENT, RECONCILE_COMPONENT } from "./lake.ts";
import { LOG, LOG_COMPONENT, LOG_CONTROLLER_PORT, LOG_METRICS_PORT, LOG_PORT } from "./log.ts";
import { WEBHOOK, WEBHOOK_COMPONENT, WEBHOOK_HEALTH_PORT } from "./webhook.ts";
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
import {
  GATEWAY as SANDBOX_GATEWAY_NAME,
  GATEWAY_COMPONENT as SANDBOX_GATEWAY_COMPONENT,
  GATEWAY_PORT as SANDBOX_GATEWAY_PORT,
  POOL_POD_PORT,
  RECEIVER,
  RECEIVER_COMPONENT,
  sandboxServiceRuns,
} from "./sandbox-gateway.ts";

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

/** The computes' components: a primary's, and its read replica's. */
const COMPUTES = ["neon-compute", "neon-compute-ro"];

/** The stack's pods that what talks to each of them alone reaches: the object store's parts, and the log. */
const APART = [...PARTS, LOG_COMPONENT];

/**
 * Neon's services, the lake and their jobs reach each other, but the computes, primary
 * and read replica, which take their clients' sessions from main's Neon proxy alone,
 * which every client connects through, and neon-control's, which wakes them and counts
 * their sessions, and on compute_ctl's port neon-control's, which gives them their specs
 * anew, and the collector's scraper's scrapes of its metrics. In a branch environment, its
 * lake and computes likewise, but for the scrapes, as the scraper scrapes main's computes
 * alone. The object store's parts and the log, of the stack too, are reached as
 * partPolicies, gatewayPolicy and logPolicy say alone.
 */
function neonPolicies({ namespace }: Environment): NetworkPolicy[] {
  return [
    policy(componentName("neon"), namespace, "neon", {
      podSelector: { matchLabels: STACK, matchExpressions: [{ key: "app.kubernetes.io/component", operator: "NotIn", values: [...APART, ...COMPUTES] }] },
      policyTypes: ["Ingress"],
      ingress: [{ from: [{ podSelector: { matchLabels: STACK } }] }],
    }),
    ...COMPUTES.map((component) =>
      policy(componentName(component), namespace, "neon", {
        podSelector: { matchLabels: selectorLabels(component) },
        policyTypes: ["Ingress"],
        ingress: [
          { from: [ofMain("neon-proxy"), ofMain("neon-control")], ports: [{ protocol: "TCP", port: COMPUTE_PORT }] },
          { from: [ofMain("neon-control")], ports: [{ protocol: "TCP", port: COMPUTE_CTL_PORT }] },
          // The collector's scraper scrapes main's computes' compute_ctl alone (collector.ts).
          ...(namespace === NAMESPACE ? [{ from: [ofMain(SCRAPER_COMPONENT)], ports: [{ protocol: "TCP", port: COMPUTE_CTL_PORT }] }] : []),
        ],
      })
    ),
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
 * other connection but the collector's scraper's, which reads its metrics, and the bundled
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
        { from: [{ podSelector: { matchLabels: selectorLabels(SCRAPER_COMPONENT) } }], ports: [{ protocol: "TCP", port: VALKEY_PORT }] },
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
 * and by the collector's scraper on its metrics port:
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
 * And each environment's lake's reconciliation, main's and branch environments', reaches the
 * filers, masters and volume servers on their HTTP ports alone, which it asks whether a file
 * the store fails a read of has its data still (neon/lake/src/reconcile.ts's gone). The
 * stack's other pods, which reach the store through its gateways, reach none of them.
 */
function partPolicies(): NetworkPolicy[] {
  const of = (component: string): V1NetworkPolicyPeer => ({ podSelector: { matchLabels: selectorLabels(component) } });
  const ports = (http: number) => [http, http + 10_000].map((port) => ({ protocol: "TCP", port }));
  const reconciliations: V1NetworkPolicyPeer[] = [
    ofMain(RECONCILE_COMPONENT),
    { namespaceSelector: BRANCH_NAMESPACES, podSelector: { matchLabels: selectorLabels(RECONCILE_COMPONENT) } },
  ];
  const part = (name: string, component: string, http: number, from: readonly string[]) =>
    policy(name, NAMESPACE, component, {
      podSelector: { matchLabels: selectorLabels(component) },
      policyTypes: ["Ingress"],
      ingress: [
        { from: from.map(of), ports: ports(http) },
        { from: reconciliations, ports: [{ protocol: "TCP", port: http }] },
        { from: [of(SCRAPER_COMPONENT)], ports: [{ protocol: "TCP", port: METRICS_PORT }] },
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
 * and the collector's scraper alone on their metrics port.
 */
function gatewayPolicy(): NetworkPolicy {
  return policy(GATEWAY, NAMESPACE, COMPONENTS.gateway, {
    podSelector: { matchLabels: selectorLabels(COMPONENTS.gateway) },
    policyTypes: ["Ingress"],
    ingress: [
      { from: [{ podSelector: { matchLabels: STACK } }], ports: [{ protocol: "TCP", port: 8333 }] },
      { from: [{ podSelector: { matchLabels: selectorLabels(LIFECYCLE_COMPONENT) } }], ports: [{ protocol: "TCP", port: 18_333 }] },
      { from: [{ podSelector: { matchLabels: selectorLabels(SCRAPER_COMPONENT) } }], ports: [{ protocol: "TCP", port: METRICS_PORT }] },
    ],
  });
}

/**
 * The bundled object store's filer store takes no connection but its filers', the
 * collector's scraper's, which reads its metrics, and its backups': it is not of the data stack,
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
    valkey(FILER_STORE, COMPONENTS.filerStore, [COMPONENTS.filer, SCRAPER_COMPONENT, BACKUP_COMPONENT]),
    valkey(RESTORE_SOURCE, RESTORE_COMPONENTS.source, [RESTORE_COMPONENTS.restore]),
  ];
}

/**
 * KEDA's operator, which reads what it scales on: alasio's, or, where the cluster's own KEDA
 * scales alasio (autoscaling.keda.enabled false), the operator its release manifest
 * labels, in any namespace.
 */
const kedaOperator = (config: InstallConfig): V1NetworkPolicyPeer =>
  config.autoscaling.keda.enabled
    ? { podSelector: { matchLabels: selectorLabels("keda-operator") } }
    : { namespaceSelector: {}, podSelector: { matchLabels: { "app.kubernetes.io/name": "keda-operator" } } };

/**
 * The log takes its clients on their port from the collector's gateway, the webhook intake
 * and the sandbox gateway, which write to it, and the lake's intake, KEDA's operator (kedaOperator), and
 * each environment's alasio and lake loader, main's and branch environments', which read
 * it, alone; its controllers' port, and its clients' as its brokers reach each other,
 * from its own nodes; and its metrics port from the collector's scraper.
 */
function logPolicy(config: InstallConfig): NetworkPolicy {
  const of = (component: string): V1NetworkPolicyPeer => ({ podSelector: { matchLabels: selectorLabels(component) } });
  const operator = kedaOperator(config);
  return policy(LOG, NAMESPACE, LOG_COMPONENT, {
    podSelector: { matchLabels: selectorLabels(LOG_COMPONENT) },
    policyTypes: ["Ingress"],
    ingress: [
      { from: [of(LOG_COMPONENT)], ports: [LOG_PORT, LOG_CONTROLLER_PORT].map((port) => ({ protocol: "TCP", port })) },
      {
        from: [
          of(GATEWAY_COMPONENT),
          of(WEBHOOK_COMPONENT),
          ...(sandboxServiceRuns(config) ? [of(SANDBOX_GATEWAY_COMPONENT)] : []),
          of(INTAKE_COMPONENT),
          operator,
          of("alasio"),
          of(LOADER_COMPONENT),
          { namespaceSelector: BRANCH_NAMESPACES, podSelector: { matchLabels: selectorLabels("alasio") } },
          { namespaceSelector: BRANCH_NAMESPACES, podSelector: { matchLabels: selectorLabels(LOADER_COMPONENT) } },
        ],
        ports: [{ protocol: "TCP", port: LOG_PORT }],
      },
      { from: [of(SCRAPER_COMPONENT)], ports: [{ protocol: "TCP", port: LOG_METRICS_PORT }] },
    ],
  });
}

/**
 * The webhook intake takes its port from webhook.sources alone, Telegram's ranges, where it
 * is on this machine's public address, and from anywhere else: a LoadBalancer Service takes
 * those alone, and the cluster in Docker is reached from this machine. On the machine, the
 * policy is what guards the port: what is forwarded to a pod, the policy's controller
 * accepts ahead of the machine's firewall, which never sees it (cli/src/cluster/node.ts);
 * the machine itself is admitted to its own pods all the same. It takes its health's port
 * from the collector's scraper; it reaches the cluster's DNS and the log's brokers alone,
 * so nothing it is sent can make it reach anything else.
 */
function webhookPolicy(config: InstallConfig): NetworkPolicy {
  const sources = config.webhook.exposure === "hostAddress" ? config.webhook.sources : ["0.0.0.0/0"];
  return policy(WEBHOOK, NAMESPACE, WEBHOOK_COMPONENT, {
    podSelector: { matchLabels: selectorLabels(WEBHOOK_COMPONENT) },
    policyTypes: ["Ingress", "Egress"],
    ingress: [
      { from: sources.map((cidr) => ({ ipBlock: { cidr } })), ports: [{ protocol: "TCP", port: config.webhook.port }] },
      { from: [{ podSelector: { matchLabels: selectorLabels(SCRAPER_COMPONENT) } }], ports: [{ protocol: "TCP", port: WEBHOOK_HEALTH_PORT }] },
    ],
    egress: [
      {
        to: [{ namespaceSelector: { matchLabels: { "kubernetes.io/metadata.name": "kube-system" } }, podSelector: { matchLabels: { "k8s-app": "kube-dns" } } }],
        ports: [{ protocol: "UDP", port: 53 }, { protocol: "TCP", port: 53 }],
      },
      { to: [{ podSelector: { matchLabels: selectorLabels(LOG_COMPONENT) } }], ports: [{ protocol: "TCP", port: LOG_PORT }] },
    ],
  });
}

/**
 * The telemetry collector's gateway takes OTLP from alasio, the node agents, the sandbox
 * service's gateway and receiver, and folder workspaces' bayma, beside the stack's own pods
 * (neonPolicies), its scraper among them, and from branch environments' alasio, compute and
 * lake, and from nothing else: sessions' telemetry reaches it through a receiver alone, their
 * alasio's or the sandbox service's.
 */
function collectorPolicy(config: InstallConfig): NetworkPolicy {
  const { host } = config;
  return policy(componentName("collector"), NAMESPACE, GATEWAY_COMPONENT, {
    podSelector: { matchLabels: selectorLabels(GATEWAY_COMPONENT) },
    policyTypes: ["Ingress"],
    ingress: [{
      from: [
        { podSelector: { matchLabels: selectorLabels("alasio") } },
        ...(nodeAgentRuns(config) ? [NODE_AGENT] : []),
        ...(sandboxServiceRuns(config) ? [SANDBOX_GATEWAY, SANDBOX_RECEIVER] : []),
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
        { to: [{ podSelector: { matchLabels: selectorLabels(QUERY_COMPONENT) } }], ports: [{ protocol: "TCP", port: LAKE_QUERY_PORT }] },
        // The addresses sessions with internet may not reach, the cluster's and its network's, are Grafana's too.
        { to: [{ ipBlock: { cidr: "0.0.0.0/0", except: [...sessions.blockedCidrs] } }], ports: [{ protocol: "TCP", port: 443 }] },
      ],
    }),
    admitted("neon-proxy", PROXY_PORT),
    admitted(QUERY_COMPONENT, LAKE_QUERY_PORT),
  ];
}

/** The lake's readers take KEDA's operator on their query port, which asks any of them how many queries the pool has not answered (their /pool). */
function lakeQueryPolicy(config: InstallConfig): NetworkPolicy {
  return policy(componentName(`${QUERY_COMPONENT}-from-autoscaler`), NAMESPACE, QUERY_COMPONENT, {
    podSelector: { matchLabels: selectorLabels(QUERY_COMPONENT) },
    policyTypes: ["Ingress"],
    ingress: [{ from: [kedaOperator(config)], ports: [{ protocol: "TCP", port: LAKE_QUERY_PORT }] }],
  });
}

/**
 * The node agents take alasio's calls and the sandbox service's gateway's, which attaches
 * workspaces through them, on their API's port, and nothing else; they reach
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
      ingress: [{
        from: [{ podSelector: { matchLabels: selectorLabels("alasio") } }, ...(sandboxServiceRuns(config) ? [SANDBOX_GATEWAY] : [])],
        ports: [{ protocol: "TCP", port: NODE_AGENT_PORT }],
      }],
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

/** The pods of the sandbox service's pools (./sandbox-gateway.ts), free or bound, as a selector: by the label each has of its pool. */
const POOLED = { matchExpressions: [{ key: POOL_LABEL, operator: "Exists" }] };

/** The sandbox service's gateway's pods, as a peer. */
const SANDBOX_GATEWAY = { podSelector: { matchLabels: selectorLabels(SANDBOX_GATEWAY_COMPONENT) } };

/** The sandbox service's receiver's pods, as a peer. */
const SANDBOX_RECEIVER = { podSelector: { matchLabels: selectorLabels(RECEIVER_COMPONENT) } };

/**
 * The cluster's API server, as an egress rule: its addresses on its ports, as `alasio up`
 * read them (../api-server.ts); or, where it could not, anywhere, which up warns of.
 */
function apiServerRule(apiServer: ApiServer | null): V1NetworkPolicyEgressRule {
  if (!apiServer) return {};
  return { to: apiServer.addresses.map((address) => ({ ipBlock: { cidr: `${address}/32` } })), ports: apiServer.ports.map((port) => ({ protocol: "TCP", port })) };
}

/**
 * The sandbox service's pools' pods (./sandbox-gateway.ts) of `environment`, free or bound:
 * they take main's gateway alone, on bayma's port, and reach main's receiver alone, on its
 * port, at the address their binding gives them, and a pod with internet the public internet
 * beside, none of the cluster's or its network's private addresses, as a session does.
 */
function poolPolicies(config: InstallConfig, environment: Environment): NetworkPolicy[] {
  const namespace = sessionsNamespace(config, environment);
  return [
    policy(componentName("sandboxes"), namespace, "session", {
      podSelector: POOLED,
      policyTypes: ["Ingress", "Egress"],
      ingress: [{ from: [{ namespaceSelector: namespaceNamed(NAMESPACE), ...SANDBOX_GATEWAY }], ports: [{ protocol: "TCP", port: POOL_POD_PORT }] }],
      egress: [{ to: [{ namespaceSelector: namespaceNamed(NAMESPACE), ...SANDBOX_RECEIVER }], ports: [{ protocol: "TCP", port: config.telemetry.receiverPort }] }],
    }),
    policy(componentName("sandboxes-full-internet"), namespace, "session", {
      podSelector: { ...POOLED, matchLabels: { [NET_MODE_LABEL]: "full" } },
      policyTypes: ["Egress"],
      egress: [{ to: [{ ipBlock: { cidr: "0.0.0.0/0", except: [...config.sessions.blockedCidrs] } }] }],
    }),
  ];
}

/**
 * The sandbox service's gateway and receiver, which take what sandboxes send, and so reach
 * what they need of the cluster alone:
 *
 * - the gateway takes main's alasio, which binds through it, its own replicas, which pass each
 *   other what is bound on the pods each holds, and KEDA's operator (kedaOperator), which asks
 *   it how many binds wait for a pool, on its port alone; it reaches the cluster's DNS, the
 *   pools' pods of every environment on bayma's port, the node agents, its replicas, the
 *   log, which it writes each environment's events to, the collector where it runs, and the
 *   API server;
 * - the receiver takes the pools' pods alone, of main's sessions and of branch environments',
 *   on its port; it reaches the cluster's DNS, the collector where it runs, and the API server.
 *
 * The API server is reached at its addresses, as `apiServer` says (apiServerRule).
 */
function sandboxServicePolicies(config: InstallConfig, apiServer: ApiServer | null): NetworkPolicy[] {
  const port = config.telemetry.receiverPort;
  const pools = [
    { namespaceSelector: namespaceNamed(sessionsNamespace(config, MAIN_ENVIRONMENT)), podSelector: POOLED },
    { namespaceSelector: BRANCH_NAMESPACES, podSelector: POOLED },
  ];
  const collector: V1NetworkPolicyEgressRule[] = collectorRuns(config)
    ? [{ to: [{ podSelector: { matchLabels: selectorLabels(GATEWAY_COMPONENT) } }], ports: [{ protocol: "TCP", port: COLLECTOR_PORT }] }]
    : [];
  return [
    policy(SANDBOX_GATEWAY_NAME, NAMESPACE, SANDBOX_GATEWAY_COMPONENT, {
      ...SANDBOX_GATEWAY,
      policyTypes: ["Ingress", "Egress"],
      ingress: [{ from: [{ podSelector: { matchLabels: selectorLabels("alasio") } }, SANDBOX_GATEWAY, kedaOperator(config)], ports: [{ protocol: "TCP", port: SANDBOX_GATEWAY_PORT }] }],
      egress: [
        CLUSTER_DNS,
        { to: pools, ports: [{ protocol: "TCP", port: POOL_POD_PORT }] },
        { to: [NODE_AGENT], ports: [{ protocol: "TCP", port: NODE_AGENT_PORT }] },
        { to: [SANDBOX_GATEWAY], ports: [{ protocol: "TCP", port: SANDBOX_GATEWAY_PORT }] },
        // The log, its events written to it.
        { to: [{ podSelector: { matchLabels: selectorLabels(LOG_COMPONENT) } }], ports: [{ protocol: "TCP", port: LOG_PORT }] },
        ...collector,
        apiServerRule(apiServer),
      ],
    }),
    policy(RECEIVER, NAMESPACE, RECEIVER_COMPONENT, {
      ...SANDBOX_RECEIVER,
      policyTypes: ["Ingress", "Egress"],
      ingress: [{ from: pools, ports: [{ protocol: "TCP", port }] }],
      egress: [CLUSTER_DNS, ...collector, apiServerRule(apiServer)],
    }),
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
 * and those that admit branch environments to what of main's they share, what reaches its
 * API server reaching it where `apiServer` says (../api-server.ts); or a branch
 * environment's own.
 */
export function networkPolicyObjects(config: InstallConfig, environment: Environment = MAIN_ENVIRONMENT, apiServer: ApiServer | null = null): NetworkPolicy[] {
  if (!config.networkPolicies.enabled) return [];
  const { sessions, host } = config;
  if (environment.branch !== null) {
    return [
      ...(sessions.enabled ? sessionPolicies(config, environment) : []),
      ...(sandboxServiceRuns(config) ? poolPolicies(config, environment) : []),
      alasioPolicy(config, environment),
      ...neonPolicies(environment),
    ];
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
    ...(config.objectStore.bundled.enabled ? [gatewayPolicy(), ...partPolicies(), ...filerStorePolicies()] : []),
    logPolicy(config),
    webhookPolicy(config),
    ...(collectorRuns(config) ? [collectorPolicy(config)] : []),
    ...(grafanaRuns(config) ? grafanaPolicies(config) : []),
    ...(lakeRuns(config) ? [lakeQueryPolicy(config)] : []),
    ...(config.workspaceStorage.enabled ? workspaceStoragePolicies(config) : []),
    ...(nodeAgentRuns(config) ? nodeAgentPolicies(config) : []),
    ...(sandboxServiceRuns(config) ? [...poolPolicies(config, environment), ...sandboxServicePolicies(config, apiServer)] : []),
  ];
}
