/**
 * The local cluster: k3s in Docker, made through Docker's Engine API (./docker.ts) from
 * alasio's node image (cluster/node), which is k3s with gVisor registered as containerd's
 * `runsc` runtime and an entrypoint that readies its container for k3s.
 *
 * A cluster NAME is a bridge network NAME, a server container NAME-server-0 and agent
 * containers NAME-agent-I, each with named volumes for what k3s, kubelet and the CNI
 * keep (NAME-server-0-k3s and so on), so a node made anew, as when its image or settings
 * change, keeps its state. All of it is labelled as the cluster's, and nothing that is
 * not is changed. The nodes have fixed addresses in the network's subnet: the server the
 * second, after the gateway, and the agents those after it. Docker gives fixed addresses
 * only in a subnet chosen for the network, so one is chosen when none is given: the first
 * 172.30.N.0/24 no other network overlaps.
 *
 * The API server is published on the loopback only. What is done in the cluster itself,
 * waiting for it and configuring it, is done with the kubectl k3s carries in the server
 * node, so this machine needs none of Kubernetes' tools.
 */
import { createHash, randomUUID } from "node:crypto";
import { dirname } from "node:path";

import { type Cluster, KubeConfig, type User, type V1ConfigMap, type V1NodeList } from "@kubernetes/client-node";
import { Context, Duration, Effect, FileSystem, Layer, type PlatformError, Schedule, Schema } from "effect";

import { imageReference } from "../images.ts";
import { NODE_IMAGE } from "../release.ts";
import { type ContainerCreate, DockerEngine, type DockerError, type ExecOptions, type Mount, type NetworkInspect } from "./docker.ts";

/** A name the cluster's DNS resolves to an address, as an /etc/hosts line says. */
export interface HostAlias {
  readonly ip: string;
  readonly hostnames: readonly string[];
}

/** A path of this machine mounted in every node, at `target`. */
export interface HostMount {
  readonly source: string;
  readonly target: string;
  readonly readOnly?: boolean;
}

/** What the local cluster is made with. */
export interface LocalClusterOptions {
  readonly name: string;
  /** The port on the loopback the API server is reached at. */
  readonly apiPort: number;
  /** The directory persistent volumes are made in (k3s's local-path), mounted at the same path in every node. */
  readonly storagePath: string;
  /** The node image; this release's unless given. */
  readonly image?: string;
  /** The network's IPv4 subnet (a.b.c.d/n); the first free 172.30.N.0/24 unless given. */
  readonly subnet?: string;
  readonly hostAliases?: readonly HostAlias[];
  readonly mounts?: readonly HostMount[];
  /** Agent nodes beside the server. */
  readonly agents?: number;
  /** How long each wait for the cluster may take, and how often it looks. */
  readonly readyTimeout?: Duration.Input;
  readonly poll?: Duration.Input;
}

export type NodeRole = "server" | "agent";

/** A node's container, and the state Docker gives it (running, exited, ...). */
export interface NodeStatus {
  readonly name: string;
  readonly role: NodeRole;
  readonly state: string;
}

/** The Docker the cluster runs on, by its version, and the cluster's nodes, the server first; none when there is no cluster. */
export interface ClusterStatus {
  readonly docker: string;
  readonly nodes: readonly NodeStatus[];
}

/** The cluster cannot be made as asked: something of its name is not its own, or its settings cannot hold. */
export class ClusterUnusable extends Schema.TaggedError<ClusterUnusable>()("ClusterUnusable", {
  cluster: Schema.String,
  reason: Schema.String,
}) {
  override get message(): string {
    return `cluster ${this.cluster} cannot be made: ${this.reason}`;
  }
}

/** The cluster did not come up in time: what it was waited for, and why it last was not. */
export class ClusterNotReady extends Schema.TaggedError<ClusterNotReady>()("ClusterNotReady", {
  cluster: Schema.String,
  waitingFor: Schema.String,
  /** How long it was given, in seconds. */
  within: Schema.Number,
  reason: Schema.String,
}) {
  override get message(): string {
    return `cluster ${this.cluster} was not ready within ${this.within}s, waiting for ${this.waitingFor}: ${this.reason}`;
  }
}

/** What a wait for the cluster has not seen yet; the wait's ClusterNotReady says it when it gives up. */
class NotYet extends Schema.TaggedError<NotYet>()("NotYet", {
  message: Schema.String,
}) {}

/** A command run in a node exited with another code than 0. */
export class NodeCommandFailed extends Schema.TaggedError<NodeCommandFailed>()("NodeCommandFailed", {
  node: Schema.String,
  command: Schema.String,
  exitCode: Schema.Number,
  stderr: Schema.String,
}) {
  override get message(): string {
    return `${this.command} in ${this.node} exited with ${this.exitCode}${this.stderr.trim() ? `: ${this.stderr.trim()}` : ""}`;
  }
}

/** What `remove` removes beside the nodes and network. */
export interface RemoveOptions {
  readonly volumes?: boolean;
  readonly storage?: boolean;
}

/** How bringing the cluster up fails. */
export type LocalClusterError = DockerError | ClusterUnusable | ClusterNotReady | PlatformError.PlatformError;

/** The local cluster of the options its layer was given. */
export class LocalCluster extends Context.Service<LocalCluster, {
  /**
   * Makes the cluster, or what of it is missing, starts what is stopped, makes nodes
   * whose image or settings changed anew, and waits until its nodes are ready; then gives
   * CoreDNS the host aliases, applies the `gvisor` RuntimeClass, and writes a kubeconfig
   * that reaches it to `kubeconfig` (0600).
   */
  readonly up: (kubeconfig: string) => Effect.Effect<void, LocalClusterError>;
  /** Stops its nodes, keeping everything. */
  readonly down: Effect.Effect<void, DockerError>;
  /**
   * Removes its nodes and network; with `volumes`, what they keep; with `storage`, its
   * storage directory, its persistent volumes', whose files are its pods' users', removed
   * in its server node, as root, first.
   */
  readonly remove: (options?: RemoveOptions) => Effect.Effect<void, DockerError | NodeCommandFailed | PlatformError.PlatformError>;
  readonly status: Effect.Effect<ClusterStatus, DockerError>;
  /** Loads images of this machine's Docker into every node's containerd, where pods find them without a pull. */
  readonly loadImages: (references: readonly string[]) => Effect.Effect<void, DockerError | NodeCommandFailed>;
}>()("alasio/cluster/LocalCluster") {
  static readonly layer = (options: LocalClusterOptions): Layer.Layer<LocalCluster, never, DockerEngine | FileSystem.FileSystem> =>
    Layer.effect(LocalCluster, makeLocalCluster(options));
}

const CLUSTER_LABEL = "alasio.cluster";
const ROLE_LABEL = "alasio.role";
/** The label of a node's container that says what it was made from: specHash's. */
const SPEC_LABEL = "alasio.spec";

/** What a node keeps, each in a volume of its own: the volume's suffix, and where it is mounted. */
const NODE_VOLUMES = [
  ["k3s", "/var/lib/rancher/k3s"],
  ["kubelet", "/var/lib/kubelet"],
  ["cni", "/var/lib/cni"],
  ["log", "/var/log"],
] as const;

/** The API server's port in the server node. */
const K3S_PORT = 6443;
const API_PORT = `${K3S_PORT}/tcp`;
const K3S_KUBECONFIG = "/etc/rancher/k3s/k3s.yaml";
/** The token the server made on its first start, which agents join with. */
const K3S_TOKEN = "/var/lib/rancher/k3s/server/token";

/**
 * kubelet's thresholds for a machine whose disk the cluster shares with everything else
 * on it: it evicts only when the disk is nearly full, and collects unused images only then,
 * so images in use are not evicted.
 */
const KUBELET_ARGS = [
  "--kubelet-arg=eviction-hard=imagefs.available<5%,nodefs.available<5%",
  "--kubelet-arg=eviction-minimum-reclaim=imagefs.available=1%,nodefs.available=1%",
  "--kubelet-arg=image-gc-high-threshold=98",
  "--kubelet-arg=image-gc-low-threshold=95",
];

/** gVisor, as the node image registers it with containerd. */
const GVISOR_RUNTIME_CLASS = { apiVersion: "node.k8s.io/v1", kind: "RuntimeClass", metadata: { name: "gvisor" }, handler: "runsc" };

// A first start pulls k3s's own images, so it is given minutes.
const READY_TIMEOUT: Duration.Input = "5 minutes";
const POLL: Duration.Input = "1 second";

/** The `index`th address of the IPv4 subnet `cidr` (a.b.c.d/n), its network address the 0th; null when it has no such host address. */
export function subnetAddress(cidr: string, index: number): string | null {
  const match = /^(\d+)\.(\d+)\.(\d+)\.(\d+)\/(\d+)$/u.exec(cidr);
  if (!match) return null;
  const octets = match.slice(1, 5).map(Number);
  const prefix = Number(match[5]);
  if (octets.some((octet) => octet > 255) || prefix > 30) return null;
  // The last address is the subnet's broadcast address.
  if (index < 1 || index >= 2 ** (32 - prefix) - 1) return null;
  const network = octets.reduce((value, octet) => value * 256 + octet, 0);
  const address = network - (network % 2 ** (32 - prefix)) + index;
  return [24, 16, 8, 0].map((shift) => Math.floor(address / 2 ** shift) % 256).join(".");
}

/** The first and last addresses of the IPv4 subnet `cidr`, as numbers; null when it is none. */
function subnetRange(cidr: string): readonly [number, number] | null {
  const match = /^(\d+)\.(\d+)\.(\d+)\.(\d+)\/(\d+)$/u.exec(cidr);
  if (!match) return null;
  const octets = match.slice(1, 5).map(Number);
  const prefix = Number(match[5]);
  if (octets.some((octet) => octet > 255) || prefix > 32) return null;
  const size = 2 ** (32 - prefix);
  const first = octets.reduce((value, octet) => value * 256 + octet, 0);
  return [first - (first % size), first - (first % size) + size - 1];
}

/** The first 172.30.N.0/24 that overlaps none of the `taken` subnets (others than IPv4 are not in the way), or null when all do. Pure, for tests. */
export function freeSubnet(taken: readonly string[]): string | null {
  const ranges = taken.flatMap((cidr) => {
    const range = subnetRange(cidr);
    return range ? [range] : [];
  });
  for (let third = 0; third < 256; third++) {
    const candidate = `172.30.${third}.0/24`;
    // A /24 written as such always has a range.
    const [first, last] = subnetRange(candidate)!;
    if (!ranges.some(([start, end]) => start <= last && first <= end)) return candidate;
  }
  return null;
}

/**
 * CoreDNS's NodeHosts, a hosts file, with `aliases`: k3s keeps a line for each node in
 * it, which stays, and every other line is the cluster's aliases, which are replaced.
 * Pure, for tests.
 */
export function nodeHostsWith(nodeHosts: string, nodes: readonly string[], aliases: readonly HostAlias[]): string {
  const kept = nodeHosts.split("\n").filter((line) => line.trim().split(/\s+/u).slice(1).some((host) => nodes.includes(host)));
  return [...kept, ...aliases.map(({ ip, hostnames }) => `${ip} ${hostnames.join(" ")}`)].join("\n");
}

/**
 * The kubeconfig k3s wrote in the server node (`k3sYaml`), named after the cluster and
 * pointed at the API server's port on the loopback; null when it is not a kubeconfig with
 * a cluster and a user, as while k3s writes it.
 */
export function localKubeconfig(k3sYaml: string, cluster: string, apiPort: number): string | null {
  const written = new KubeConfig();
  try {
    written.loadFromString(k3sYaml);
  } catch {
    return null;
  }
  const server: Cluster | null = written.getCurrentCluster();
  const user: User | null = written.getCurrentUser();
  if (!server || !user) return null;
  const local = new KubeConfig();
  local.loadFromOptions({
    clusters: [{ ...server, name: cluster, server: `https://127.0.0.1:${apiPort}` }],
    users: [{ ...user, name: cluster }],
    contexts: [{ name: cluster, cluster, user: cluster }],
    currentContext: cluster,
  });
  return local.exportConfig();
}

/** A digest of `spec` that two specs share only when they are the same. */
const specHash = (spec: ContainerCreate): string => createHash("sha256").update(JSON.stringify(spec)).digest("hex");

const makeLocalCluster = Effect.fnUntraced(function*({
  name: cluster,
  apiPort,
  storagePath,
  image = imageReference(NODE_IMAGE),
  subnet,
  hostAliases = [],
  mounts = [],
  agents = 0,
  readyTimeout = READY_TIMEOUT,
  poll = POLL,
}: LocalClusterOptions): Effect.fn.Return<LocalCluster["Service"], never, DockerEngine | FileSystem.FileSystem> {
  const docker = yield* DockerEngine;
  const fs = yield* FileSystem.FileSystem;
  const labels = { [CLUSTER_LABEL]: cluster };
  const server = `${cluster}-server-0`;
  const nodes = [server, ...Array.from({ length: agents }, (_, index) => `${cluster}-agent-${index}`)];

  const unusable = (reason: string) => new ClusterUnusable({ cluster, reason });

  /** The container of `node`, with what every node has: privileged, as k3s in Docker needs, and its volumes, the storage directory and the host mounts. */
  const nodeContainer = (node: string, role: NodeRole, address: string, command: readonly string[], env: readonly string[]): ContainerCreate => ({
    Image: image,
    Hostname: node,
    Cmd: command,
    Env: env,
    Labels: { ...labels, [ROLE_LABEL]: role },
    ...(role === "server" ? { ExposedPorts: { [API_PORT]: {} } } : {}),
    HostConfig: {
      Privileged: true,
      Init: true,
      CgroupnsMode: "private",
      RestartPolicy: { Name: "unless-stopped" },
      SecurityOpt: ["label=disable"],
      Tmpfs: { "/run": "", "/var/run": "" },
      Mounts: [
        ...NODE_VOLUMES.map(([suffix, target]): Mount => ({ Type: "volume", Source: `${node}-${suffix}`, Target: target })),
        { Type: "bind", Source: storagePath, Target: storagePath },
        ...mounts.map(({ source, target, readOnly = false }): Mount => ({ Type: "bind", Source: source, Target: target, ReadOnly: readOnly })),
      ],
      ExtraHosts: hostAliases.flatMap(({ ip, hostnames }) => hostnames.map((hostname) => `${hostname}:${ip}`)),
      ...(role === "server" ? { PortBindings: { [API_PORT]: [{ HostIp: "127.0.0.1", HostPort: String(apiPort) }] } } : {}),
    },
    NetworkingConfig: { EndpointsConfig: { [cluster]: { IPAMConfig: { IPv4Address: address } } } },
  });

  /** The cluster's network, made unless it exists; its subnet, which must be `subnet` when that is given. */
  const ensureNetwork = Effect.fnUntraced(function*(): Effect.fn.Return<string, DockerError | ClusterUnusable> {
    const existing = yield* docker.inspectNetwork(cluster);
    if (existing && existing.Labels?.[CLUSTER_LABEL] !== cluster) return yield* unusable(`a network named ${cluster} is not its own`);
    let network: NetworkInspect | null = existing;
    if (!network) {
      const chosen = subnet ?? freeSubnet((yield* docker.listNetworks).flatMap(({ IPAM }) => (IPAM.Config ?? []).flatMap(({ Subnet }) => Subnet ?? [])));
      if (!chosen) return yield* unusable("every 172.30.N.0/24 is another network's: give the cluster a subnet");
      yield* Effect.logInfo(`making network ${cluster} (${chosen})`);
      yield* docker.createNetwork({ Name: cluster, Driver: "bridge", IPAM: { Config: [{ Subnet: chosen }] }, Labels: labels });
      network = yield* docker.inspectNetwork(cluster);
    }
    const actual = network?.IPAM.Config?.[0]?.Subnet;
    if (!actual) return yield* unusable(`network ${cluster} has no subnet`);
    if (subnet && actual !== subnet) return yield* unusable(`network ${cluster} has the subnet ${actual}, not ${subnet}`);
    return actual;
  });

  /** The `index`th node's address in `networkSubnet`. */
  const nodeAddress = (networkSubnet: string, index: number): Effect.Effect<string, ClusterUnusable> => {
    const address = subnetAddress(networkSubnet, 2 + index);
    return address ? Effect.succeed(address) : Effect.fail(unusable(`its subnet ${networkSubnet} has no room for ${nodes.length} nodes`));
  };

  /** `node`'s container made as `spec` says, anew when it was made otherwise, and running. */
  const ensureNode = Effect.fnUntraced(function*(node: string, spec: ContainerCreate): Effect.fn.Return<void, DockerError | ClusterUnusable> {
    const hash = specHash(spec);
    const existing = yield* docker.inspectContainer(node);
    if (existing && existing.Config.Labels?.[CLUSTER_LABEL] !== cluster) return yield* unusable(`a container named ${node} is not one of its nodes`);
    const current = existing !== null && existing.Config.Labels?.[SPEC_LABEL] === hash;
    if (existing && !current) {
      yield* Effect.logInfo(`making node ${node} anew, from its current image and settings`);
      yield* docker.stopContainer(node);
      yield* docker.removeContainer(node);
    }
    if (!current) {
      yield* Effect.logInfo(`making node ${node}`);
      yield* Effect.forEach(NODE_VOLUMES, ([suffix]) => docker.createVolume({ Name: `${node}-${suffix}`, Labels: labels }), { discard: true });
      yield* docker.createContainer(node, { ...spec, Labels: { ...spec.Labels, [SPEC_LABEL]: hash } });
    }
    if (!current || !existing.State.Running) {
      yield* Effect.logInfo(`starting node ${node}`);
      yield* docker.startContainer(node);
    }
  });

  /** The output of `command` in `node`, which fails unless it exits with 0. */
  const inNode = (node: string, command: readonly string[], options?: ExecOptions): Effect.Effect<Buffer, DockerError | NodeCommandFailed> =>
    docker.exec(node, command, options).pipe(
      Effect.flatMap(({ exitCode, stdout, stderr }) =>
        exitCode === 0 ? Effect.succeed(stdout) : Effect.fail(new NodeCommandFailed({ node, command: command.join(" "), exitCode, stderr }))
      ),
    );

  const kubectl = (args: readonly string[], options?: ExecOptions) => inNode(server, ["kubectl", ...args], options);

  /** `check` once it succeeds, tried every `poll` for `readyTimeout`; why it last failed, when it never does. */
  const awaitReady = <A>(waitingFor: string, check: Effect.Effect<A, DockerError | NodeCommandFailed | NotYet>): Effect.Effect<A, ClusterNotReady> =>
    Effect.logInfo(`waiting for ${waitingFor}`).pipe(
      Effect.andThen(
        check.pipe(
          Effect.mapError((error) => new ClusterNotReady({ cluster, waitingFor, within: Duration.toSeconds(readyTimeout), reason: error.message })),
          Effect.retry(Schedule.max([Schedule.spaced(poll), Schedule.during(readyTimeout)])),
        ),
      ),
    );

  /** When each node's container last started, in milliseconds since the epoch. */
  const startTimes: Effect.Effect<ReadonlyMap<string, number>, DockerError> = Effect.forEach(nodes, (node) =>
    Effect.map(docker.inspectContainer(node), (container) => [node, Date.parse(container?.State.StartedAt ?? "")] as const)).pipe(
      Effect.map((entries) => new Map(entries)),
    );

  /**
   * Every node ready, by a Ready condition its kubelet reported since its container
   * started: one from before, which a node stopped while ready keeps until its kubelet
   * reports again, is not yet the node's. The condition's heartbeat is to the second.
   */
  const nodesReady = (started: ReadonlyMap<string, number>) =>
    kubectl(["get", "nodes", "--output=json"]).pipe(
      Effect.flatMap((stdout) => {
        const { items } = JSON.parse(stdout.toString("utf8")) as V1NodeList;
        const ready = new Set(
          items
            .filter((node) =>
              node.status?.conditions?.some(({ type, status, lastHeartbeatTime }) =>
                type === "Ready" && status === "True" && lastHeartbeatTime !== undefined
                && new Date(lastHeartbeatTime).getTime() >= Math.floor((started.get(node.metadata?.name ?? "") ?? Infinity) / 1000) * 1000
              )
            )
            .map((node) => node.metadata?.name),
        );
        const waiting = nodes.filter((node) => !ready.has(node));
        return waiting.length === 0 ? Effect.void : Effect.fail(new NotYet({ message: `not reported ready since started: ${waiting.join(", ")}` }));
      }),
    );

  /** CoreDNS's NodeHosts with the host aliases, patched only when they differ, and only over the version read. */
  const applyHostAliases = Effect.gen(function*() {
    const coredns = JSON.parse((yield* kubectl(["--namespace=kube-system", "get", "configmap", "coredns", "--output=json"])).toString("utf8")) as V1ConfigMap;
    const nodeHosts = coredns.data?.["NodeHosts"] ?? "";
    const desired = nodeHostsWith(nodeHosts, nodes, hostAliases);
    if (desired === nodeHosts) return;
    const patch = { metadata: { resourceVersion: coredns.metadata?.resourceVersion }, data: { NodeHosts: desired } };
    yield* kubectl(["--namespace=kube-system", "patch", "configmap", "coredns", "--type=merge", `--patch=${JSON.stringify(patch)}`]);
  });

  const writeKubeconfig = Effect.fnUntraced(function*(path: string): Effect.fn.Return<void, ClusterNotReady | PlatformError.PlatformError> {
    const content = yield* awaitReady(
      "its kubeconfig",
      docker.readFile(server, K3S_KUBECONFIG).pipe(
        Effect.flatMap((written) => {
          const local = localKubeconfig(written.toString("utf8"), cluster, apiPort);
          return local ? Effect.succeed(local) : Effect.fail(new NotYet({ message: `${K3S_KUBECONFIG} is not yet a kubeconfig with a cluster and a user` }));
        }),
      ),
    );
    // Written whole beside it, readable only by its owner, and moved over it.
    yield* fs.makeDirectory(dirname(path), { recursive: true, mode: 0o700 });
    const written = `${path}.${randomUUID()}`;
    yield* fs.writeFileString(written, content, { flag: "wx", mode: 0o600 });
    yield* fs.rename(written, path);
    yield* Effect.logInfo(`wrote the kubeconfig of cluster ${cluster} to ${path}`);
  });

  const up = Effect.fnUntraced(function*(kubeconfig: string): Effect.fn.Return<void, LocalClusterError> {
    if (!(yield* docker.inspectImage(image))) {
      yield* Effect.logInfo(`pulling ${image}`);
      yield* docker.pullImage(image);
    }
    const networkSubnet = yield* ensureNetwork();
    yield* fs.makeDirectory(storagePath, { recursive: true });
    yield* ensureNode(
      server,
      nodeContainer(server, "server", yield* nodeAddress(networkSubnet, 0), [
        "server",
        "--disable=traefik",
        // Where the kubeconfig reaches it.
        "--tls-san=127.0.0.1",
        `--default-local-storage-path=${storagePath}`,
        ...KUBELET_ARGS,
      ], []),
    );
    yield* awaitReady("its API server", kubectl(["get", "--raw=/readyz"]));
    if (agents > 0) {
      const token = (yield* awaitReady("its token", docker.readFile(server, K3S_TOKEN))).toString("utf8").trim();
      for (const [index, agent] of nodes.slice(1).entries()) {
        yield* ensureNode(
          agent,
          nodeContainer(agent, "agent", yield* nodeAddress(networkSubnet, 1 + index), ["agent", ...KUBELET_ARGS], [
            `K3S_URL=https://${server}:${K3S_PORT}`,
            `K3S_TOKEN=${token}`,
          ]),
        );
      }
    }
    yield* awaitReady("its nodes", Effect.flatMap(startTimes, nodesReady));
    yield* awaitReady("CoreDNS's NodeHosts", applyHostAliases);
    yield* awaitReady("the gvisor RuntimeClass", kubectl(["apply", "--filename=-"], { stdin: JSON.stringify(GVISOR_RUNTIME_CLASS) }));
    yield* writeKubeconfig(kubeconfig);
    yield* Effect.logInfo(`cluster ${cluster} is up`);
  });

  /** The cluster's nodes, the server first. */
  const existingNodes: Effect.Effect<NodeStatus[], DockerError> = docker.listContainers(labels).pipe(
    Effect.map((found) =>
      found
        .map((container): NodeStatus => ({
          name: container.Names[0]?.replace(/^\//u, "") ?? "",
          role: container.Labels[ROLE_LABEL] === "server" ? "server" : "agent",
          state: container.State,
        }))
        .sort((a, b) => (a.role === b.role ? a.name.localeCompare(b.name) : a.role === "server" ? -1 : 1))
    ),
  );

  return LocalCluster.of({
    up,

    // The agents first, so none is left without its server.
    down: existingNodes.pipe(
      Effect.flatMap((found) => Effect.forEach(found.toReversed(), ({ name }) => docker.stopContainer(name), { discard: true })),
      Effect.andThen(Effect.logInfo(`stopped cluster ${cluster}`)),
    ),

    remove: ({ volumes = false, storage = false } = {}) =>
      Effect.gen(function*() {
        const found = yield* existingNodes;
        if (storage && found.some(({ name }) => name === server)) {
          yield* docker.startContainer(server);
          yield* inNode(server, ["find", storagePath, "-mindepth", "1", "-delete"]);
        }
        yield* Effect.forEach(found, ({ name }) => docker.removeContainer(name), { discard: true });
        const network = yield* docker.inspectNetwork(cluster);
        if (network?.Labels?.[CLUSTER_LABEL] === cluster) yield* docker.removeNetwork(cluster);
        if (volumes) yield* Effect.forEach(yield* docker.listVolumes(labels), ({ Name }) => docker.removeVolume(Name), { discard: true });
        if (storage) yield* fs.remove(storagePath, { recursive: true, force: true });
        yield* Effect.logInfo(`removed cluster ${cluster}${volumes ? " and its volumes" : ""}${storage ? `, and ${storagePath}` : ""}`);
      }),

    status: Effect.all({
      docker: Effect.map(docker.version, ({ Version }) => Version),
      nodes: existingNodes,
    }),

    loadImages: (references) =>
      Effect.forEach(
        nodes,
        (node) =>
          Effect.scoped(
            Effect.flatMap(docker.exportImages(references), (archive) => inNode(node, ["ctr", "--namespace=k8s.io", "images", "import", "-"], { stdin: archive })),
          ).pipe(Effect.andThen(Effect.logInfo(`loaded ${references.join(", ")} into ${node}`))),
        { discard: true },
      ),
  });
});
