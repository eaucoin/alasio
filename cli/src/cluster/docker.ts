/**
 * The cluster in Docker: k3s in Docker's containers, made through its Engine API
 * (./docker-engine.ts) from alasio's node image (cluster/node), which is k3s with the
 * gVisor releases alasio keeps beside it and an entrypoint that readies its container for
 * k3s. The files that make each release a runtime of containerd's (./k3s.ts) are put into
 * each node's container before it first starts, and a node is made anew when they change,
 * as when its other settings do.
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
 * The registries the nodes pull from are k3s's registries.yaml, which the node image's
 * entrypoint writes from the variable each node is made with, so a node is made anew when
 * they change, as when its other settings do.
 *
 * The API server is published on the loopback only. What is done in the cluster itself,
 * waiting for it and configuring it, is done with the kubectl k3s carries in the server
 * node, so this machine needs none of Kubernetes' tools.
 */
import { createHash } from "node:crypto";

import type { V1ConfigMap, V1NodeList, V1RuntimeClassList } from "@kubernetes/client-node";
import { Context, type Duration, Effect, FileSystem, Layer, type PlatformError, Schedule, Schema } from "effect";

import { SNAPSHOT_CACHE_DIR } from "../../../src/node-agent/names.ts";
import { imageReference } from "../images.ts";
import { NODE_IMAGE } from "../release.ts";
import { type ContainerCreate, DockerEngine, type DockerError, type ExecOptions, type Mount, type NetworkInspect } from "./docker-engine.ts";
import {
  awaitReady as waitFor,
  clusterKubeconfig,
  type ClusterNotReady,
  containerdFiles,
  gvisorRuntimeClasses,
  type HostAlias,
  KUBELET_ARGS,
  nodeHostsWith,
  NotYet,
  readySince,
  type Registries,
  registriesYaml,
  SANDBOX_IMAGE,
  staleRuntimeClasses,
  WAITS,
  writeKubeconfig,
} from "./k3s.ts";
import { gvisorReleases, NODE_PINS } from "./node.ts";
import { STARGZ_MEMORY_LIMIT, STARGZ_ROOT, stargzConfig } from "./stargz.ts";
import { tarArchive, type TarEntry } from "./tar.ts";

/** A path of this machine mounted in every node, at `target`. */
export interface HostMount {
  readonly source: string;
  readonly target: string;
  readonly readOnly?: boolean;
}

/** What the cluster in Docker is made with. */
export interface DockerClusterOptions {
  readonly name: string;
  /** The port on the loopback the API server is reached at. */
  readonly apiPort: number;
  /** The directory persistent volumes are made in (k3s's local-path), mounted at the same path in every node. */
  readonly storagePath: string;
  /** The node image; this release's unless given. */
  readonly image?: string;
  /** The gVisor releases the node image holds, the current first; this version's pins' unless given. */
  readonly gvisor?: readonly string[];
  /** The network's IPv4 subnet (a.b.c.d/n); the first free 172.30.N.0/24 unless given. */
  readonly subnet?: string;
  readonly hostAliases?: readonly HostAlias[];
  readonly mounts?: readonly HostMount[];
  /** Agent nodes beside the server. */
  readonly agents?: number;
  /** The registries the nodes pull from; containerd's defaults unless given. */
  readonly registries?: Registries;
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

/** A node's container, and, while it runs, whether stargz-snapshotter's daemon, which its k3s pulls images through, runs in it. */
export interface NodeHealth extends NodeStatus {
  readonly snapshotter: boolean | null;
}

/** The Docker the cluster runs on, by its version, and the cluster's nodes, the server first; none when there is no cluster. */
export interface ClusterStatus {
  readonly docker: string;
  readonly nodes: readonly NodeHealth[];
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
export type DockerClusterError = DockerError | ClusterUnusable | ClusterNotReady | PlatformError.PlatformError;

/** The cluster in Docker of the options its layer was given. */
export class DockerCluster extends Context.Service<DockerCluster, {
  /**
   * Makes the cluster, or what of it is missing, starts what is stopped, makes nodes
   * whose image or settings changed anew, and waits until its nodes are ready; then gives
   * CoreDNS the host aliases, applies gVisor's RuntimeClasses, deleting those of releases
   * no longer kept, and writes a kubeconfig that reaches it to `kubeconfig` (0600).
   */
  readonly up: (kubeconfig: string) => Effect.Effect<void, DockerClusterError>;
  /** Stops its nodes, keeping everything. */
  readonly down: Effect.Effect<void, DockerError>;
  /**
   * Removes its nodes and network; with `volumes`, what they keep; with `storage`, its
   * storage directory, its persistent volumes', whose files are its pods' users', removed
   * in its server node, as root, first.
   */
  readonly remove: (options?: RemoveOptions) => Effect.Effect<void, DockerError | NodeCommandFailed | PlatformError.PlatformError>;
  readonly status: Effect.Effect<ClusterStatus, DockerError>;
}>()("alasio/cluster/DockerCluster") {
  static readonly layer = (options: DockerClusterOptions): Layer.Layer<DockerCluster, never, DockerEngine | FileSystem.FileSystem> =>
    Layer.effect(DockerCluster, makeDockerCluster(options));
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
  // The snapshots of sandboxes gVisor saves whole, which the node restores them from.
  ["snapshots", SNAPSHOT_CACHE_DIR],
  ["stargz", STARGZ_ROOT],
] as const;

/** The API server's port in the server node. */
const K3S_PORT = 6443;
const API_PORT = `${K3S_PORT}/tcp`;
const K3S_KUBECONFIG = "/etc/rancher/k3s/k3s.yaml";
/** The token the server made on its first start, which agents join with. */
const K3S_TOKEN = "/var/lib/rancher/k3s/server/token";
/** The variable the node image's entrypoint writes k3s's registries.yaml from, before k3s starts. */
const REGISTRIES_ENV = "ALASIO_REGISTRIES";
/** The variable the node image's entrypoint writes stargz-snapshotter's daemon's configuration from, before it starts it. */
const STARGZ_CONFIG_ENV = "ALASIO_STARGZ_CONFIG";
/** The variable the node image's entrypoint gives the daemon, and its FUSE manager, as Go's soft limit on their heaps (./stargz.ts's STARGZ_MEMORY_LIMIT). */
const STARGZ_MEMORY_ENV = "ALASIO_STARGZ_MEMORY_LIMIT";
/** Where the node image's entrypoint writes the pid of stargz-snapshotter's daemon, at each of the daemon's starts. */
const STARGZ_PID = "/run/containerd-stargz-grpc/daemon.pid";

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
 * Aborts the FUSE connections of what is mounted in a node, as JuiceFS's workspaces are: a
 * process waiting on one whose client is gone, as a node's are once they are killed, can
 * itself be killed, and its node removed, only once it is. The machine's connections are
 * all in /sys/fs/fuse/connections, each named after the minor of its mount's device, and
 * only the node's own are aborted.
 */
const ABORT_FUSE = [
  "grep -q ' /sys/fs/fuse/connections ' /proc/self/mounts || mount -t fusectl fusectl /sys/fs/fuse/connections",
  "for device in $(awk '/ - fuse/ { print $3 }' /proc/self/mountinfo | sort -u); do",
  '  abort="/sys/fs/fuse/connections/${device#*:}/abort"',
  '  if [ -e "$abort" ]; then echo 1 >"$abort"; fi',
  "done",
].join("\n");

/**
 * What every node's k3s is given: kubelet's settings, no snapshotter, as containerd's
 * files in its container say stargz-snapshotter's daemon's (./stargz.ts), and its sandbox
 * image pinned (./k3s.ts).
 */
const nodeArgs = [...KUBELET_ARGS.map((arg) => `--kubelet-arg=${arg}`), "--snapshotter=", `--pause-image=${SANDBOX_IMAGE}`];

/** A digest of `spec` and the `files` put into its container that two share only when they are the same. */
const specHash = (spec: ContainerCreate, files: ReadonlyMap<string, string>): string => createHash("sha256").update(JSON.stringify([spec, [...files]])).digest("hex");

/** The tar archive of `files`, by their paths, which Docker extracts at a container's root: each file with the directories it is in. */
function filesArchive(files: ReadonlyMap<string, string>): Buffer {
  const entries = new Map<string, TarEntry>();
  for (const [path, content] of files) {
    const file = path.slice(1);
    const parts = file.split("/");
    for (let depth = 1; depth < parts.length; depth++) {
      const directory = `${parts.slice(0, depth).join("/")}/`;
      entries.set(directory, { name: directory, type: "directory", mode: 0o755, content: Buffer.alloc(0) });
    }
    entries.set(file, { name: file, type: "file", mode: 0o644, content: Buffer.from(content) });
  }
  return tarArchive([...entries.values()]);
}

const makeDockerCluster = Effect.fnUntraced(function*({
  name: cluster,
  apiPort,
  storagePath,
  image = imageReference(NODE_IMAGE),
  gvisor = gvisorReleases(NODE_PINS),
  subnet,
  hostAliases = [],
  mounts = [],
  agents = 0,
  registries,
  readyTimeout = WAITS.readyTimeout,
  poll = WAITS.poll,
}: DockerClusterOptions): Effect.fn.Return<DockerCluster["Service"], never, DockerEngine | FileSystem.FileSystem> {
  const docker = yield* DockerEngine;
  const fs = yield* FileSystem.FileSystem;
  const labels = { [CLUSTER_LABEL]: cluster };
  const server = `${cluster}-server-0`;
  const nodes = [server, ...Array.from({ length: agents }, (_, index) => `${cluster}-agent-${index}`)];

  const unusable = (reason: string) => new ClusterUnusable({ cluster, reason });

  /** containerd's files, which pull through stargz-snapshotter's daemon and make each gVisor release the node image holds a runtime, in every node. */
  const files = containerdFiles(gvisor, { systemd: false });

  /**
   * The container of `node`, with what every node has: privileged, as k3s in Docker needs,
   * its volumes, the storage directory and the host mounts, the registries, and
   * stargz-snapshotter's daemon's configuration.
   */
  const nodeContainer = (node: string, role: NodeRole, address: string, command: readonly string[], env: readonly string[]): ContainerCreate => ({
    Image: image,
    Hostname: node,
    Cmd: command,
    Env: [...env, ...(registries ? [`${REGISTRIES_ENV}=${registriesYaml(registries)}`] : []), `${STARGZ_CONFIG_ENV}=${stargzConfig(registries)}`, `${STARGZ_MEMORY_ENV}=${STARGZ_MEMORY_LIMIT}`],
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

  /**
   * `node`'s container made as `spec` says, with the files put into it before it first
   * starts, anew when it was made otherwise, and running. The files are in its k3s volume,
   * which keeps them, and those of a node made anew replace them.
   */
  const ensureNode = Effect.fnUntraced(function*(node: string, spec: ContainerCreate): Effect.fn.Return<void, DockerError | ClusterUnusable> {
    const hash = specHash(spec, files);
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
      yield* docker.putArchive(node, "/", filesArchive(files));
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

  const awaitReady = waitFor(`cluster ${cluster}`, { readyTimeout, poll });

  /** When each node's container last started, in milliseconds since the epoch. */
  const startTimes: Effect.Effect<ReadonlyMap<string, number>, DockerError> = Effect.forEach(nodes, (node) =>
    Effect.map(docker.inspectContainer(node), (container) => [node, Date.parse(container?.State.StartedAt ?? "")] as const)).pipe(
      Effect.map((entries) => new Map(entries)),
    );

  /** Every node ready since its container started. */
  const nodesReady = (started: ReadonlyMap<string, number>) =>
    kubectl(["get", "nodes", "--output=json"]).pipe(
      Effect.flatMap((stdout) => {
        const { items } = JSON.parse(stdout.toString("utf8")) as V1NodeList;
        const ready = new Set(items.filter((node) => readySince(node, started.get(node.metadata?.name ?? "") ?? Infinity)).map((node) => node.metadata?.name));
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

  /** gVisor's RuntimeClasses applied, those of releases no longer kept, or with another handler, deleted first. */
  const applyRuntimeClasses = Effect.gen(function*() {
    const desired = gvisorRuntimeClasses(gvisor);
    const { items } = JSON.parse((yield* kubectl(["get", "runtimeclasses", "--output=json"])).toString("utf8")) as V1RuntimeClassList;
    const stale = staleRuntimeClasses(items, desired);
    if (stale.length > 0) {
      yield* kubectl(["delete", "runtimeclasses", ...stale]);
      yield* Effect.logInfo(`deleted RuntimeClasses ${stale.join(", ")}`);
    }
    yield* kubectl(["apply", "--filename=-"], { stdin: JSON.stringify({ apiVersion: "v1", kind: "List", items: desired }) });
  });

  /** The kubeconfig the server wrote, pointed at the API server's port on the loopback, written to `path`. */
  const fetchKubeconfig = Effect.fnUntraced(function*(path: string): Effect.fn.Return<void, ClusterNotReady | PlatformError.PlatformError> {
    const content = yield* awaitReady(
      "its kubeconfig",
      docker.readFile(server, K3S_KUBECONFIG).pipe(
        Effect.flatMap((written) => {
          const local = clusterKubeconfig(written.toString("utf8"), cluster, `https://127.0.0.1:${apiPort}`);
          return local ? Effect.succeed(local) : Effect.fail(new NotYet({ message: `${K3S_KUBECONFIG} is not yet a kubeconfig with a cluster and a user` }));
        }),
      ),
    );
    yield* Effect.provideService(writeKubeconfig(path, content), FileSystem.FileSystem, fs);
    yield* Effect.logInfo(`wrote the kubeconfig of cluster ${cluster} to ${path}`);
  });

  const up = Effect.fnUntraced(function*(kubeconfig: string): Effect.fn.Return<void, DockerClusterError> {
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
        ...nodeArgs,
      ], []),
    );
    yield* awaitReady("its API server", kubectl(["get", "--raw=/readyz"]));
    if (agents > 0) {
      const token = (yield* awaitReady("its token", docker.readFile(server, K3S_TOKEN))).toString("utf8").trim();
      for (const [index, agent] of nodes.slice(1).entries()) {
        yield* ensureNode(
          agent,
          nodeContainer(agent, "agent", yield* nodeAddress(networkSubnet, 1 + index), ["agent", ...nodeArgs], [
            `K3S_URL=https://${server}:${K3S_PORT}`,
            `K3S_TOKEN=${token}`,
          ]),
        );
      }
    }
    yield* awaitReady("its nodes", Effect.flatMap(startTimes, nodesReady));
    yield* awaitReady("CoreDNS's NodeHosts", applyHostAliases);
    yield* awaitReady("gVisor's RuntimeClasses", applyRuntimeClasses);
    yield* fetchKubeconfig(kubeconfig);
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

  return DockerCluster.of({
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
          // The pods still running write as it empties the directory, and one of its
          // directories they write in as it is removed is not empty: it is emptied again.
          yield* inNode(server, ["find", storagePath, "-mindepth", "1", "-delete"]).pipe(Effect.retry({ times: 9, schedule: Schedule.spaced("1 second") }));
        }
        for (const { name } of found.filter(({ state }) => state === "running")) yield* Effect.ignore(inNode(name, ["sh", "-c", ABORT_FUSE]));
        yield* Effect.forEach(found, ({ name }) => docker.removeContainer(name), { discard: true });
        const network = yield* docker.inspectNetwork(cluster);
        if (network?.Labels?.[CLUSTER_LABEL] === cluster) {
          // Docker removes no network a container is on: others' (a collector given an
          // address on it, say) are taken off it, and keep running.
          yield* Effect.forEach(Object.values(network.Containers ?? {}), ({ Name }) =>
            docker.disconnectNetwork(cluster, Name).pipe(Effect.andThen(Effect.logInfo(`took ${Name} off network ${cluster}`))), { discard: true });
          yield* docker.removeNetwork(cluster);
        }
        if (volumes) yield* Effect.forEach(yield* docker.listVolumes(labels), ({ Name }) => docker.removeVolume(Name), { discard: true });
        if (storage) yield* fs.remove(storagePath, { recursive: true, force: true });
        yield* Effect.logInfo(`removed cluster ${cluster}${volumes ? " and its volumes" : ""}${storage ? `, and ${storagePath}` : ""}`);
      }),

    status: Effect.all({
      docker: Effect.map(docker.version, ({ Version }) => Version),
      // The daemon's pid, as the node's entrypoint writes it at each of its starts.
      nodes: Effect.flatMap(existingNodes, (found) =>
        Effect.forEach(found, (node): Effect.Effect<NodeHealth, DockerError> =>
          node.state !== "running"
            ? Effect.succeed({ ...node, snapshotter: null })
            : inNode(node.name, ["sh", "-c", `kill -0 "$(cat ${STARGZ_PID})"`]).pipe(
              Effect.as(true),
              Effect.catchTag("NodeCommandFailed", () => Effect.succeed(false)),
              Effect.map((snapshotter) => ({ ...node, snapshotter })),
            ))),
    }),
  });
});
