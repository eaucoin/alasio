/**
 * What the clusters alasio makes on this machine, in Docker (./docker.ts) and on the
 * machine itself (./host.ts), share of k3s: how kubelet is set, the registries containerd
 * pulls from, the host aliases CoreDNS resolves, the `gvisor` RuntimeClass, nodes ready
 * since they started, the kubeconfig k3s writes made the operator's, and waits for all
 * that.
 */
import { randomUUID } from "node:crypto";
import { dirname } from "node:path";

import { type Cluster, KubeConfig, type User, type V1Node } from "@kubernetes/client-node";
import { Duration, Effect, FileSystem, type PlatformError, Schedule, Schema } from "effect";

/** A name the cluster's DNS resolves to an address, as an /etc/hosts line says. */
export interface HostAlias {
  readonly ip: string;
  readonly hostnames: readonly string[];
}

/** A registry's TLS: files in the nodes, and whether its certificate goes unchecked. */
export interface RegistryTls {
  readonly caFile?: string;
  readonly certFile?: string;
  readonly keyFile?: string;
  readonly insecureSkipVerify?: boolean;
}

/**
 * The registries the nodes' containerd pulls from, as k3s's registries.yaml says them:
 * the endpoints that stand for a registry, by its host, in the order they are tried, with
 * how image names are rewritten there; and a registry's TLS, by its host.
 */
export interface Registries {
  readonly mirrors?: Readonly<Record<string, { readonly endpoint: readonly string[]; readonly rewrite?: Readonly<Record<string, string>> }>>;
  readonly configs?: Readonly<Record<string, { readonly tls: RegistryTls }>>;
}

/**
 * kubelet's settings, as its arguments say them, for a machine whose disk the cluster
 * shares with everything else on it: it evicts only when the disk is nearly full, and
 * collects unused images only then, so images in use are not evicted.
 */
export const KUBELET_ARGS: readonly string[] = [
  "eviction-hard=imagefs.available<5%,nodefs.available<5%",
  "eviction-minimum-reclaim=imagefs.available=1%,nodefs.available=1%",
  "image-gc-high-threshold=98",
  "image-gc-low-threshold=95",
];

/** gVisor, as containerd has it registered: the runtime `runsc` (cluster/node/config-v3.toml.tmpl). */
export const GVISOR_RUNTIME_CLASS = { apiVersion: "node.k8s.io/v1", kind: "RuntimeClass", metadata: { name: "gvisor" }, handler: "runsc" };

/** k3s's registries.yaml of `registries`, written as JSON, which YAML reads as it is. Pure, for tests. */
export function registriesYaml({ mirrors = {}, configs = {} }: Registries): string {
  return JSON.stringify({
    mirrors,
    configs: Object.fromEntries(
      Object.entries(configs).map(([host, { tls }]) => [
        host,
        { tls: { ca_file: tls.caFile, cert_file: tls.certFile, key_file: tls.keyFile, insecure_skip_verify: tls.insecureSkipVerify } },
      ]),
    ),
  });
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
 * Whether `node` is ready by a Ready condition its kubelet reported since `started`, in
 * milliseconds since the epoch: one from before, which a node stopped while ready keeps
 * until its kubelet reports again, is not yet the node's. The condition's heartbeat is to
 * the second.
 */
export const readySince = (node: V1Node, started: number): boolean =>
  node.status?.conditions?.some(({ type, status, lastHeartbeatTime }) =>
    type === "Ready" && status === "True" && lastHeartbeatTime !== undefined && new Date(lastHeartbeatTime).getTime() >= Math.floor(started / 1000) * 1000
  ) ?? false;

/**
 * The kubeconfig k3s wrote (`k3sYaml`), named `cluster`, and pointed at `server` when
 * given; null when it is not a kubeconfig with a cluster and a user, as while k3s writes it.
 */
export function clusterKubeconfig(k3sYaml: string, cluster: string, server?: string): string | null {
  const written = new KubeConfig();
  try {
    written.loadFromString(k3sYaml);
  } catch {
    return null;
  }
  const current: Cluster | null = written.getCurrentCluster();
  const user: User | null = written.getCurrentUser();
  if (!current || !user) return null;
  const named = new KubeConfig();
  named.loadFromOptions({
    clusters: [{ ...current, name: cluster, server: server ?? current.server }],
    users: [{ ...user, name: cluster }],
    contexts: [{ name: cluster, cluster, user: cluster }],
    currentContext: cluster,
  });
  return named.exportConfig();
}

/** Writes `content`, a kubeconfig, to `path`, readable by its owner alone: whole, beside it, then moved over it. */
export const writeKubeconfig = Effect.fnUntraced(function*(path: string, content: string): Effect.fn.Return<void, PlatformError.PlatformError, FileSystem.FileSystem> {
  const fs = yield* FileSystem.FileSystem;
  yield* fs.makeDirectory(dirname(path), { recursive: true, mode: 0o700 });
  const written = `${path}.${randomUUID()}`;
  yield* fs.writeFileString(written, content, { flag: "wx", mode: 0o600 });
  yield* fs.rename(written, path);
});

/** The cluster did not come up in time: what it was waited for, and why it last was not. */
export class ClusterNotReady extends Schema.TaggedError<ClusterNotReady>()("ClusterNotReady", {
  cluster: Schema.String,
  waitingFor: Schema.String,
  /** How long it was given, in seconds. */
  within: Schema.Number,
  reason: Schema.String,
}) {
  override get message(): string {
    return `${this.cluster} was not ready within ${this.within}s, waiting for ${this.waitingFor}: ${this.reason}`;
  }
}

/** What a wait for the cluster has not seen yet; the wait's ClusterNotReady says it when it gives up. */
export class NotYet extends Schema.TaggedError<NotYet>()("NotYet", {
  message: Schema.String,
}) {}

/** How long each wait for a cluster may take, and how often it looks. */
export interface Waits {
  readonly readyTimeout: Duration.Input;
  readonly poll: Duration.Input;
}

/** A cluster's first start pulls k3s's own images, so each wait is given minutes. */
export const WAITS: Waits = { readyTimeout: "5 minutes", poll: "1 second" };

/**
 * A wait for `cluster` (as a sentence names it): `check` once it succeeds, tried every
 * `poll` for `readyTimeout`; why it last failed, when it never does.
 */
export const awaitReady = (cluster: string, { readyTimeout, poll }: Waits) =>
<A, R>(waitingFor: string, check: Effect.Effect<A, { readonly message: string }, R>): Effect.Effect<A, ClusterNotReady, R> =>
  Effect.logInfo(`waiting for ${waitingFor}`).pipe(
    Effect.andThen(
      check.pipe(
        Effect.mapError((error) => new ClusterNotReady({ cluster, waitingFor, within: Duration.toSeconds(readyTimeout), reason: error.message })),
        Effect.retry(Schedule.max([Schedule.spaced(poll), Schedule.during(readyTimeout)])),
      ),
    ),
  );
