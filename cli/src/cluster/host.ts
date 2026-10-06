/**
 * The cluster alasio makes on this machine outside Docker: k3s on the machine itself, a
 * node of it as ./node.ts makes one, with gVisor, whose API alasio reaches with a copy of
 * k3s's kubeconfig, the operator's own, beside the config.
 *
 * up asks root (./root.ts), all at once, for what is not as this version and the config
 * say: the inotify limits raised when they are too low, k3s's configuration written when
 * its files differ from those alasio wrote, gVisor and k3s installed when their pins
 * differ from what alasio installed, k3s started or restarted when that is what it needs,
 * and its kubeconfig read when k3s started anew, or the operator's copy is missing or its
 * certificate near its end. An up that changes nothing asks root for nothing. Then, as the
 * operator, through the API, it waits for the node, gives CoreDNS the host aliases and
 * applies the gvisor RuntimeClass, as the cluster in Docker has them.
 *
 * The host aliases are CoreDNS's alone: pods resolve them, and the machine resolves names
 * as it always has, as k3s runs on the machine itself rather than in a container whose
 * /etc/hosts alasio writes.
 */
import { spawn } from "node:child_process";
import { X509Certificate } from "node:crypto";

import { KubeConfig, type V1ConfigMap, type V1Node } from "@kubernetes/client-node";
import { Context, type Duration, Effect, FileSystem, Layer, type PlatformError, Schema } from "effect";

import type { Registries } from "../config.ts";
import { kind, KubeApi, type KubeApiError, type KubeconfigUnusable } from "../kube/api.ts";
import {
  awaitReady,
  clusterKubeconfig,
  type ClusterNotReady,
  GVISOR_RUNTIME_CLASS,
  type HostAlias,
  nodeHostsWith,
  NotYet,
  readySince,
  WAITS,
  writeKubeconfig,
} from "./k3s.ts";
import { forgetInotifyStep, inotifyStep, Machine, under } from "./machine.ts";
import {
  type Artifact,
  type ChecksumMismatch,
  checked,
  CLUSTER_NETWORKS,
  ConfigureNode,
  containerdTemplate,
  type Download,
  downloads,
  filesDigest,
  type Firewall,
  InstallGvisor,
  InstallK3s,
  NODE_PINS,
  type NodeConfig,
  nodeFiles,
  type NodePins,
  type NodeStamp,
  OpenFirewall,
  ReadKubeconfig,
  readStamp,
  RemoveNode,
  RemoveStorage,
  SERVICE,
  StartK3s,
  StopK3s,
} from "./node.ts";
import { Root, type RootError, type RootStep } from "./root.ts";

/** How the cluster names itself in what alasio says. */
const CLUSTER = "k3s on this machine";

/** A systemd service's state: whether it is installed, its activity (active, inactive, failed...), and whether it starts at boot. */
export interface UnitState {
  readonly loaded: boolean;
  readonly active: string;
  readonly enabled: boolean;
}

/** systemd cannot say how a service is: it does not run this machine, or systemctl is not there. */
export class SystemdUnavailable extends Schema.TaggedError<SystemdUnavailable>()("SystemdUnavailable", {
  reason: Schema.String,
}) {
  override get message(): string {
    return `k3s runs on this machine as a service of systemd, and ${this.reason}: run alasio in Docker here (alasio init --target docker), ` +
      "or give it a cluster elsewhere with alasio init --kubeconfig";
  }
}

/** systemd's services on this machine, as everyone reads them. */
export class Systemd extends Context.Service<Systemd, {
  readonly unit: (name: string) => Effect.Effect<UnitState, SystemdUnavailable>;
}>()("alasio/cluster/Systemd") {
  /** As `systemctl show` says them. */
  static readonly layer: Layer.Layer<Systemd> = Layer.succeed(
    Systemd,
    Systemd.of({
      unit: (name) =>
        Effect.callback<UnitState, SystemdUnavailable>((resume) => {
          const child = spawn("systemctl", ["show", name, "--property=LoadState,ActiveState,UnitFileState"], { stdio: ["ignore", "pipe", "pipe"] });
          let stdout = "";
          let stderr = "";
          child.stdout.on("data", (chunk: Buffer) => (stdout += chunk));
          child.stderr.on("data", (chunk: Buffer) => (stderr += chunk));
          child.on("error", (cause) => resume(Effect.fail(new SystemdUnavailable({ reason: `systemctl did not start: ${cause.message}` }))));
          child.on("close", (code) => {
            if (code !== 0) return resume(Effect.fail(new SystemdUnavailable({ reason: `systemctl show exited with ${code}: ${stderr.trim()}` })));
            const properties = new Map(stdout.split("\n").map((line) => [line.slice(0, line.indexOf("=")), line.slice(line.indexOf("=") + 1)] as const));
            resume(Effect.succeed({
              loaded: properties.get("LoadState") === "loaded",
              active: properties.get("ActiveState") ?? "",
              enabled: properties.get("UnitFileState") === "enabled",
            }));
          });
          return Effect.sync(() => child.kill());
        }),
    }),
  );
}

/** A download failed: what, and why. */
export class DownloadFailed extends Schema.TaggedError<DownloadFailed>()("DownloadFailed", {
  url: Schema.String,
  reason: Schema.String,
}) {
  override get message(): string {
    return `${this.url} could not be downloaded: ${this.reason}`;
  }
}

/** The releases of k3s and gVisor a node is made of: those pinned, and how what they pin is downloaded. */
export class NodeReleases extends Context.Service<NodeReleases, {
  readonly pins: NodePins;
  readonly fetch: (url: string) => Effect.Effect<Uint8Array, DownloadFailed>;
}>()("alasio/cluster/NodeReleases") {
  /** This version's pins, downloaded over HTTPS with Node's fetch, which follows redirects. */
  static readonly layer: Layer.Layer<NodeReleases> = Layer.succeed(
    NodeReleases,
    NodeReleases.of({
      pins: NODE_PINS,
      fetch: (url) =>
        Effect.tryPromise({
          try: async (signal) => {
            const response = await fetch(url, { signal });
            if (!response.ok) throw new Error(`it answered ${response.status} ${response.statusText}`);
            return new Uint8Array(await response.arrayBuffer());
          },
          catch: (cause) => new DownloadFailed({ url, reason: cause instanceof Error ? cause.message : String(cause) }),
        }),
    }),
  );
}

/** k3s is installed on this machine, but not by alasio, which leaves it be. */
export class ForeignK3s extends Schema.TaggedError<ForeignK3s>()("ForeignK3s", {}) {
  override get message(): string {
    return "k3s is installed on this machine, but not by alasio, which leaves it as it is: run alasio in it with " +
      "alasio init --kubeconfig /etc/rancher/k3s/k3s.yaml, or uninstall it first (k3s-uninstall.sh)";
  }
}

/** What the cluster on this machine is made with. */
export interface HostClusterOptions {
  /** The directory persistent volumes are made in (k3s's local-path). */
  readonly storagePath: string;
  readonly hostAliases?: readonly HostAlias[];
  /** The registries the node pulls from; containerd's defaults unless given. */
  readonly registries?: typeof Registries.Type;
  /** How long each wait for the cluster may take, and how often it looks. */
  readonly readyTimeout?: Duration.Input;
  readonly poll?: Duration.Input;
}

/** The operator's copy of k3s's kubeconfig: current, missing (or not one), or with a certificate near its end. */
export type KubeconfigState = "current" | "missing" | "expiring";

/** A copy whose certificate ends within this many days is read anew, k3s renewing it first. */
const RENEW_WITHIN_DAYS = 30;

/** The state of the kubeconfig `text` (null when there is none) at `now`; one that cannot be read is missing. Pure, for tests. */
export function kubeconfigState(text: string | null, now: Date): KubeconfigState {
  const config = new KubeConfig();
  let ends: Date | null;
  try {
    config.loadFromString(text ?? "");
    const certificate = config.getCurrentUser()?.certData;
    ends = certificate ? new Date(new X509Certificate(Buffer.from(certificate, "base64")).validTo) : null;
  } catch {
    return "missing";
  }
  if (!config.getCurrentCluster() || !config.getCurrentUser()) return "missing";
  return ends !== null && ends.getTime() - now.getTime() < RENEW_WITHIN_DAYS * 24 * 60 * 60 * 1000 ? "expiring" : "current";
}

/**
 * What the node needs of root: the cluster's networks let in through the active firewall,
 * its files written, gVisor installed, k3s installed, restarted or started, and its
 * kubeconfig read, after renewing it.
 */
export interface NodePlan {
  readonly firewall: Firewall | null;
  readonly configure: boolean;
  readonly gvisor: boolean;
  readonly k3s: "install" | "restart" | "start" | null;
  readonly kubeconfig: boolean;
  readonly renew: boolean;
}

/**
 * What the node needs of root, by what alasio installed (`stamp`), the files that
 * configure it (by their digest), its service, the operator's kubeconfig, and the
 * firewall active on the machine, which lets the cluster's networks in once alasio has
 * seen to it. Pure, for tests.
 */
export function planNode(pins: NodePins, { stamp, config, unit, kubeconfig, firewall }: {
  readonly stamp: NodeStamp | null;
  readonly config: string;
  readonly unit: UnitState;
  readonly kubeconfig: KubeconfigState;
  readonly firewall: Firewall | null;
}): NodePlan {
  const opened = stamp?.firewall?.tool === firewall && CLUSTER_NETWORKS.every((network) => stamp?.firewall?.networks.includes(network));
  const configure = stamp?.config !== config;
  const gvisor = stamp?.gvisor !== pins.gvisor.release;
  const k3s = stamp?.k3s !== pins.k3s.version || !unit.loaded
    ? "install"
    : configure || gvisor
    ? "restart"
    : unit.active !== "active" || !unit.enabled
    ? "start"
    : null;
  const startsAnew = k3s === "install" || k3s === "restart";
  return { firewall: firewall !== null && !opened ? firewall : null, configure, gvisor, k3s, kubeconfig: k3s !== null || kubeconfig !== "current", renew: kubeconfig === "expiring" && !startsAnew };
}

/** k3s's service, and what alasio installed, when anything. */
export interface HostClusterStatus {
  readonly unit: UnitState;
  readonly installed: NodeStamp | null;
}

/** How bringing the cluster up fails. */
export type HostClusterError =
  | SystemdUnavailable
  | ForeignK3s
  | DownloadFailed
  | ChecksumMismatch
  | RootError
  | ClusterNotReady
  | KubeconfigUnusable
  | PlatformError.PlatformError;

/** The cluster on this machine of the options its layer was given. */
export class HostCluster extends Context.Service<HostCluster, {
  /**
   * Makes this machine a node, or what of it is not as it should be, as root, and starts
   * it; writes a kubeconfig that reaches it to `kubeconfig` (0600) when there is a new one
   * to; waits until the node is ready; then gives CoreDNS the host aliases and applies
   * the `gvisor` RuntimeClass.
   */
  readonly up: (kubeconfig: string) => Effect.Effect<void, HostClusterError>;
  /** Stops k3s, as root, keeping everything; nothing when it is not installed. */
  readonly down: Effect.Effect<void, SystemdUnavailable | ForeignK3s | RootError | PlatformError.PlatformError>;
  /**
   * Removes k3s and gVisor, as root, and the file the inotify limits are raised in; with
   * `storage`, its storage directory too.
   */
  readonly remove: (options: { readonly storage: boolean }) => Effect.Effect<void, SystemdUnavailable | ForeignK3s | RootError | PlatformError.PlatformError>;
  /** k3s's service and what alasio installed, refusing a k3s alasio did not install. */
  readonly status: Effect.Effect<HostClusterStatus, SystemdUnavailable | ForeignK3s | PlatformError.PlatformError>;
}>()("alasio/cluster/HostCluster") {
  static readonly layer = (options: HostClusterOptions): Layer.Layer<HostCluster, never, FileSystem.FileSystem | Machine | Systemd | Root | NodeReleases> =>
    Layer.effect(HostCluster, makeHostCluster(options));
}

const makeHostCluster = Effect.fnUntraced(function*({
  storagePath,
  hostAliases = [],
  registries,
  readyTimeout = WAITS.readyTimeout,
  poll = WAITS.poll,
}: HostClusterOptions): Effect.fn.Return<HostCluster["Service"], never, FileSystem.FileSystem | Machine | Systemd | Root | NodeReleases> {
  const fs = yield* FileSystem.FileSystem;
  const machine = yield* Machine;
  const systemd = yield* Systemd;
  const root = yield* Root;
  const { pins, fetch } = yield* NodeReleases;
  const onMachine = <A, E>(effect: Effect.Effect<A, E, FileSystem.FileSystem | Machine>) =>
    effect.pipe(Effect.provideService(FileSystem.FileSystem, fs), Effect.provideService(Machine, machine));
  const config: NodeConfig = { role: "server", storagePath, ...(registries ? { registries } : {}) };
  const waitFor = awaitReady(CLUSTER, { readyTimeout, poll });

  /** The firewall active on this machine, which lets in only what it is told to: ufw, as its config says, or firewalld, as its service is; none. */
  const activeFirewall = Effect.gen(function*() {
    const ufw = under(machine.root, "/etc/ufw/ufw.conf");
    if ((yield* fs.exists(ufw)) && /^ENABLED=yes$/mu.test(yield* fs.readFileString(ufw))) return "ufw" as const;
    return (yield* systemd.unit("firewalld")).active === "active" ? ("firewalld" as const) : null;
  });

  /** What alasio installed, refusing a k3s it did not. */
  const installed = Effect.gen(function*() {
    const unit = yield* systemd.unit(SERVICE);
    const stamp = yield* onMachine(readStamp);
    if (unit.loaded && stamp === null) return yield* new ForeignK3s();
    return { unit, stamp };
  });

  /** `download` fetched into `directory`, as `name`, and checked against its pin. */
  const fetched = Effect.fnUntraced(function*(directory: string, name: string, download: Download): Effect.fn.Return<Artifact, DownloadFailed | ChecksumMismatch | PlatformError.PlatformError> {
    yield* Effect.logInfo(`downloading ${download.url}`);
    const content = yield* Effect.flatMap(fetch(download.url), (fetchedContent) => checked(download, fetchedContent));
    const path = `${directory}/${name}`;
    yield* fs.writeFile(path, content);
    return { path, ...download };
  });

  /** The node ready, by a Ready condition reported since `since`. */
  const nodeReady = (since: number) =>
    Effect.flatMap(KubeApi, (kube) => kube.list<V1Node>(kind("Node"))).pipe(
      Effect.flatMap((nodes) => {
        const waiting = nodes.filter((node) => !readySince(node, since)).map((node) => node.metadata?.name ?? "");
        if (nodes.length === 0) return Effect.fail(new NotYet({ message: "no node has registered yet" }));
        return waiting.length === 0 ? Effect.succeed(nodes) : Effect.fail(new NotYet({ message: `not reported ready since started: ${waiting.join(", ")}` }));
      }),
    );

  /** CoreDNS's NodeHosts with the host aliases, patched only when they differ, and only over the version read. */
  const applyHostAliases = (nodes: readonly V1Node[]): Effect.Effect<void, KubeApiError | NotYet, KubeApi> =>
    Effect.gen(function*() {
      const kube = yield* KubeApi;
      const coredns = yield* kube.get<V1ConfigMap>({ ...kind("ConfigMap"), namespace: "kube-system", name: "coredns" });
      if (!coredns) return yield* new NotYet({ message: "k3s has not made CoreDNS's ConfigMap yet" });
      const nodeHosts = coredns.data?.["NodeHosts"] ?? "";
      const desired = nodeHostsWith(nodeHosts, nodes.map((node) => node.metadata?.name ?? ""), hostAliases);
      if (desired === nodeHosts) return;
      yield* kube.patch(
        { ...kind("ConfigMap"), namespace: "kube-system", name: "coredns" },
        { metadata: { resourceVersion: coredns.metadata?.resourceVersion }, data: { NodeHosts: desired } },
        "alasio",
      );
    });

  const up = Effect.fnUntraced(function*(kubeconfig: string): Effect.fn.Return<void, HostClusterError> {
    const { unit, stamp } = yield* installed;
    // Made by the operator, whose it stays, though k3s makes the volumes in it as root.
    yield* fs.makeDirectory(storagePath, { recursive: true });
    const files = nodeFiles(config, yield* onMachine(containerdTemplate));
    const copy = (yield* fs.exists(kubeconfig)) ? yield* fs.readFileString(kubeconfig) : null;
    const plan = planNode(pins, { stamp, config: filesDigest(files), unit, kubeconfig: kubeconfigState(copy, new Date()), firewall: yield* activeFirewall });
    const raise = yield* onMachine(inotifyStep);
    const started = plan.k3s === null && !plan.renew ? 0 : Date.now();
    const outcome = yield* Effect.scoped(Effect.gen(function*() {
      // Downloaded as the operator, and read by root, which checks them again.
      const directory = plan.gvisor || plan.k3s === "install" ? yield* fs.makeTempDirectoryScoped({ prefix: "alasio-node-" }) : "";
      const from = downloads(pins);
      const archive = plan.gvisor ? yield* fetched(directory, "gvisor.tar.zstd", from.gvisor) : null;
      const binary = plan.k3s === "install" ? yield* fetched(directory, "k3s", from.k3s) : null;
      const script = plan.k3s === "install" ? yield* fetched(directory, "install.sh", from.installScript) : null;
      const steps: RootStep[] = [
        ...(raise ? [raise] : []),
        ...(plan.firewall ? [OpenFirewall.make({ firewall: plan.firewall, networks: CLUSTER_NETWORKS })] : []),
        ...(plan.configure ? [ConfigureNode.make({ config })] : []),
        ...(archive ? [InstallGvisor.make({ release: pins.gvisor.release, ...(stamp?.gvisor ? { previous: stamp.gvisor } : {}), archive })] : []),
        ...(binary && script ? [InstallK3s.make({ version: pins.k3s.version, ...(stamp?.k3s ? { previous: stamp.k3s } : {}), role: config.role, binary, script })] : []),
        ...(plan.k3s === "restart" || plan.k3s === "start" ? [StartK3s.make({ restart: plan.k3s === "restart" })] : []),
        ...(plan.kubeconfig ? [ReadKubeconfig.make({ renew: plan.renew })] : []),
      ];
      return yield* root.run(steps);
    }));
    if (outcome.kubeconfig) {
      // k3s's own wrote it, a kubeconfig with a cluster and a user.
      yield* Effect.provideService(writeKubeconfig(kubeconfig, clusterKubeconfig(outcome.kubeconfig, "alasio") ?? ""), FileSystem.FileSystem, fs);
      yield* Effect.logInfo(`wrote the kubeconfig of ${CLUSTER} to ${kubeconfig}`);
    }
    yield* Effect.gen(function*() {
      const nodes = yield* waitFor("its node", nodeReady(started));
      yield* waitFor("CoreDNS's NodeHosts", applyHostAliases(nodes));
      yield* waitFor("the gvisor RuntimeClass", Effect.flatMap(KubeApi, (kube) => kube.apply(GVISOR_RUNTIME_CLASS)));
    }).pipe(Effect.provide(KubeApi.layer({ path: kubeconfig })));
    yield* Effect.logInfo(`${CLUSTER} is up`);
  });

  return HostCluster.of({
    up,

    down: Effect.gen(function*() {
      if (!(yield* installed).unit.loaded) return;
      yield* root.run([StopK3s.make({})]);
      yield* Effect.logInfo(`stopped ${CLUSTER}`);
    }),

    remove: ({ storage }) =>
      Effect.gen(function*() {
        const { unit, stamp } = yield* installed;
        const forget = yield* onMachine(forgetInotifyStep);
        yield* root.run([
          ...(unit.loaded || stamp !== null ? [RemoveNode.make(stamp?.firewall ? { firewall: { tool: stamp.firewall.tool, added: stamp.firewall.added } } : {})] : []),
          ...(storage && (yield* fs.exists(storagePath)) ? [RemoveStorage.make({ path: storagePath })] : []),
          ...(forget ? [forget] : []),
        ]);
      }),

    status: Effect.map(installed, ({ unit, stamp }) => ({ unit, installed: stamp })),
  });
});
