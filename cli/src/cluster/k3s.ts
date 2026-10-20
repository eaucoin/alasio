/**
 * What the clusters alasio makes on this machine, in Docker (./docker.ts) and on the
 * machine itself (./host.ts), share of k3s: how kubelet is set, the registries containerd
 * pulls from, the host aliases CoreDNS resolves, containerd's files, which pull images
 * through stargz-snapshotter's daemon and keep the gVisor releases as runtimes, and their
 * RuntimeClasses, nodes ready since they started, the kubeconfig k3s writes made the
 * operator's, and waits for all that.
 */
import { randomUUID } from "node:crypto";
import { dirname } from "node:path";

import { type Cluster, KubeConfig, type User, type V1Node, type V1Pod, type V1RuntimeClass } from "@kubernetes/client-node";
import { Duration, Effect, FileSystem, type PlatformError, Schedule, Schema } from "effect";

import pins from "../../../cluster/node/pins.json" with { type: "json" };
import { CONTAINERD, gvisorDirectory, gvisorHandler, gvisorRuntimeClass } from "../../../src/node-agent/names.ts";
import { SNAPSHOTTER, STARGZ_ROOT, STARGZ_SOCKET } from "./stargz.ts";

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
 * kubelet's settings, as its arguments say them, for a machine the cluster shares with
 * everything else on it:
 *
 * - its disk: kubelet evicts only when the disk is nearly full, and collects unused images
 *   only then, so images in use are not evicted;
 * - its memory: what k3s itself takes (its server, containerd and the containers' shims,
 *   about 1.3 GB) and the system beside it are reserved, so pods are scheduled against
 *   what is left rather than the whole machine; and kubelet evicts pods, lowest priority
 *   first, while 500 MiB is still free, before the kernel kills a process of its choosing.
 */
export const KUBELET_ARGS: readonly string[] = [
  "kube-reserved=cpu=250m,memory=1Gi",
  "system-reserved=cpu=250m,memory=512Mi",
  "eviction-hard=imagefs.available<5%,nodefs.available<5%,memory.available<500Mi",
  "eviction-minimum-reclaim=imagefs.available=1%,nodefs.available=1%",
  "image-gc-high-threshold=98",
  "image-gc-low-threshold=95",
];

/**
 * `reference` as Docker's references normalize it, as containerd's CRI names the image it
 * pulls: with its registry, docker.io where it names none, and library/ for one of
 * docker.io's own.
 */
export function normalizedReference(reference: string): string {
  const [first = "", ...rest] = reference.split("/");
  if (rest.length > 0 && (first.includes(".") || first.includes(":") || first === "localhost")) return reference;
  return `docker.io/${rest.length > 0 ? reference : `library/${reference}`}`;
}

/**
 * The pods' sandbox image, k3s's own (its --pause-image's default, cluster/node/pins.json's
 * k3s.pauseImage), by the name containerd's CRI pins it by, so that neither kubelet's
 * collection of unused images nor a prune of the node's images deletes it. k3s writes the
 * name it is given as containerd's pinned sandbox image, and the CRI pins an image only where
 * that is the name it pulls by, normalized: k3s's default, "rancher/mirrored-pause:...", pins
 * nothing. Unpinned, it is deleted while pods run on its snapshot, as no container is of it,
 * and pulled again by the next pod onto that snapshot, which containerd fetches no layer of:
 * recorded without its layer's content, which a node whose snapshotter changes cannot make a
 * pod without, as one moved onto stargz-snapshotter's daemon could not. So it is no check of
 * an image's content that keeps a node sound: an image stargz-snapshotter's daemon pulls
 * lazily is recorded without its layers' content by design.
 */
export const SANDBOX_IMAGE = normalizedReference(pins.k3s.pauseImage);

/**
 * The pod annotations containerd passes on to gVisor's runtimes, and drops otherwise:
 * those alasio's fork of gVisor restores a sandbox by, the CPU features its sandboxes are
 * held to, and gVisor's hints for a pod's mounts.
 */
const GVISOR_POD_ANNOTATIONS = ["dev.gvisor.internal.restore.*", "dev.gvisor.internal.cpufeatures", "dev.gvisor.spec.mount.*"];

/**
 * The files of k3s's containerd on a node pulling images through stargz-snapshotter's
 * daemon and running each of gVisor's `releases`, by their paths: the containerd template,
 * k3s's own configuration with the CRI's images pulled through the daemon (./stargz.ts),
 * and a runtime for each release, which runs that release's shim; and each runtime's runsc
 * options, which run that release's runsc, and, where `systemd` runs the machine, place
 * its sandboxes' cgroups through systemd, as kubelet does there. alasio writes them on the
 * machine (the host target), and puts them into each node's container (the cluster in
 * Docker). Pure, for tests.
 */
export function containerdFiles(releases: readonly string[], { systemd }: { readonly systemd: boolean }): ReadonlyMap<string, string> {
  const optionsPath = (release: string) => `${CONTAINERD}/${gvisorHandler(release)}.toml`;
  const runtime = (release: string) => {
    const table = `plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.${gvisorHandler(release)}`;
    return [
      "",
      `[${table}]`,
      `  runtime_type = "io.containerd.runsc.v1"`,
      `  runtime_path = "${gvisorDirectory(release)}/containerd-shim-runsc-v1"`,
      `  pod_annotations = [${GVISOR_POD_ANNOTATIONS.map((annotation) => `"${annotation}"`).join(", ")}]`,
      "",
      `[${table}.options]`,
      `  TypeUrl = "io.containerd.runsc.v1.options"`,
      `  ConfigPath = "${optionsPath(release)}"`,
    ];
  };
  const options = (release: string) => [
    `# runsc's options for gVisor ${release}, containerd's runtime ${gvisorHandler(release)}: that release's runsc,`,
    "# which restores a sandbox from its snapshot directly, checking every file it reopens is the one saved, and in",
    "# the background: the sandbox runs once what it touched first is back, the rest of its memory read after.",
    `binary_name = "${gvisorDirectory(release)}/runsc"`,
    "restore_direct = true",
    "restore_background = true",
    "",
    "[runsc_config]",
    `  restore-validate-files = "all"`,
    "  # The pages a sandbox writes are tracked from its first checkpoint or restore on, by write-protecting them, so",
    "  # that the node agent's next checkpoint of it saves only those (src/node-agent/agent.ts); and the pages it",
    "  # touches in its first seconds after a restore are recorded, for its next restore to read first.",
    `  dirty-tracking = "wp"`,
    `  working-set-window = "3s"`,
    "  # A checkpoint ends every TCP connection as it saves it, sending nothing, as the save takes the sandbox's",
    "  # network first; so a sandbox restored from it holds none whose timers would send while it is left paused,",
    "  # before its pod's network policies are in force (src/node-agent/agent.ts resume). One restored keeps none",
    "  # anyway, as its pod has another address.",
    `  allow-live-tcp-migration = "false"`,
    ...(systemd ? ["  # Where systemd runs the machine, kubelet's cgroups are systemd's, and runsc places its sandboxes' through it too.", `  systemd-cgroup = "true"`] : []),
  ];
  const file = (lines: readonly string[]) => `${lines.join("\n")}\n`;
  return new Map([
    [`${CONTAINERD}/config-v3.toml.tmpl`, file([
      `{{ template "base" . }}`,
      "",
      "# Images pulled through stargz-snapshotter's daemon (cli/src/cluster/stargz.ts), which mounts an eStargz layer",
      "# lazily: k3s, its snapshotter set to none, says nothing of the CRI's images, and alasio says it here, the",
      "# snapshot annotations the daemon reads passed.",
      "[plugins.'io.containerd.cri.v1.images']",
      `  snapshotter = "${SNAPSHOTTER}"`,
      "  disable_snapshot_annotations = false",
      "  use_local_image_pull = true",
      "",
      `[proxy_plugins.${SNAPSHOTTER}]`,
      `  type = "snapshot"`,
      `  address = "${STARGZ_SOCKET}"`,
      `[proxy_plugins.${SNAPSHOTTER}.exports]`,
      `  root = "${STARGZ_ROOT}/"`,
      `  enable_remote_snapshot_annotations = "true"`,
      "",
      "# Each gVisor release alasio keeps, a runtime of its own, runsc-RELEASE (its dots made dashes), which runs that",
      "# release's shim, with the pod annotations gVisor reads, and the runsc options beside this file.",
      ...releases.flatMap(runtime),
    ])],
    ...releases.map((release) => [optionsPath(release), file(options(release))] as const),
  ]);
}

/** The labels of the RuntimeClasses alasio makes, which tell them from others'. */
const GVISOR_RUNTIME_CLASS_LABELS = { "app.kubernetes.io/managed-by": "alasio", "app.kubernetes.io/component": "gvisor" };

/** The annotation of a RuntimeClass of alasio's naming the gVisor release it runs, as `alasio up` reads what the cluster keeps (../kept-releases.ts). */
export const GVISOR_RELEASE_ANNOTATION = "alasio.dev/gvisor-release";

/**
 * The RuntimeClasses of gVisor's `releases`, the current first: `gvisor`, which new
 * sandboxes run with, the current release's, and `gvisor-RELEASE` (as a DNS label) for
 * each, which a sandbox snapshotted under that release is restored with. Pure, for tests.
 */
export function gvisorRuntimeClasses(releases: readonly string[]): V1RuntimeClass[] {
  const runtimeClass = (name: string, release: string): V1RuntimeClass => ({
    apiVersion: "node.k8s.io/v1",
    kind: "RuntimeClass",
    metadata: { name, labels: GVISOR_RUNTIME_CLASS_LABELS, annotations: { [GVISOR_RELEASE_ANNOTATION]: release } },
    handler: gvisorHandler(release),
  });
  return [
    ...releases.slice(0, 1).map((release) => runtimeClass("gvisor", release)),
    ...releases.map((release) => runtimeClass(gvisorRuntimeClass(release), release)),
  ];
}

/**
 * Of the cluster's RuntimeClasses, `existing`, the names of those to delete before
 * `desired` are applied: alasio's that are not desired, as of a release no longer kept,
 * and those desired with another handler, which a RuntimeClass cannot change. Pure, for
 * tests.
 */
export function staleRuntimeClasses(existing: readonly V1RuntimeClass[], desired: readonly V1RuntimeClass[]): string[] {
  const handlers = new Map(desired.map(({ metadata, handler }) => [metadata?.name, handler]));
  return existing.flatMap(({ metadata, handler }) => {
    const ours = Object.entries(GVISOR_RUNTIME_CLASS_LABELS).every(([key, value]) => metadata?.labels?.[key] === value);
    const wanted = handlers.get(metadata?.name);
    return (wanted === undefined ? ours : wanted !== handler) ? [metadata?.name ?? ""] : [];
  });
}

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
 * Of the pods the API server has bound to a node (`bound`), those its kubelet does not serve
 * yet (`served`, the pods it lists as its own), by their namespace and name: each that has
 * not ended, nor is being deleted. A node reports itself ready before its kubelet, started
 * anew, has taken its pods back; until it has, what is asked of one through the API server, an
 * exec or its logs, is answered 404, though the pod's own status, from before, says it runs.
 */
export function unservedPods(bound: readonly V1Pod[], served: readonly V1Pod[]): string[] {
  const serving = new Set(served.map((pod) => pod.metadata?.uid));
  return bound
    .filter((pod) => pod.status?.phase !== "Succeeded" && pod.status?.phase !== "Failed" && pod.metadata?.deletionTimestamp === undefined)
    .filter((pod) => !serving.has(pod.metadata?.uid))
    .map((pod) => `${pod.metadata?.namespace}/${pod.metadata?.name}`);
}

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
