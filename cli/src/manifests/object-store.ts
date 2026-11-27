/**
 * The bundled object store: SeaweedFS as its parts, each at its scale
 * (objectStore.bundled):
 *
 * - masters, which place objects on volume servers and keep where each is, in Raft when
 *   there are three, each with a volume of its state;
 * - volume servers, which keep the objects on volumes of their own, as many copies of each
 *   as the replication says, each on another server, and then each server on a node of its
 *   own while there are as many;
 * - filers, which keep the store's namespace (buckets, keys, versions, tags, and the log of
 *   changes lifecycle passes replay) in the filer store, a Valkey of their own (./valkey.ts),
 *   so what one filer writes every other reads at once; started one after another, so no
 *   two make the store's identity at once, each once a volume server has registered with
 *   the masters, so no filer writes before the volumes there are known, and a fresh master
 *   makes no volume's id again;
 * - S3 gateways, on the filers, the store's endpoint, under the name the stack reaches it
 *   by (alasio-seaweedfs:8333), scaled on their CPU where their scale is a range. Each makes
 *   the buckets, idempotently, as it starts: Neon's, the lake's, backups', workspace
 *   storage's, and session sandboxes' snapshots'; and
 *   gives each bucket written in parts its lifecycle rule (ABORT_INCOMPLETE_UPLOADS). It is
 *   ready, and the stack's writers start, only once they are.
 *
 * And the periodic pass that applies the lifecycle rules, expires what of Neon's bucket Neon
 * needs no more, and copies again what has fewer copies than it should; and the filer store's
 * backups. The identities the gateways take are the stack's setup's, as are the filer store's
 * users' passwords, and what the parts prove themselves to each other with: each its
 * certificate on their gRPC ports, mutual TLS throughout, and the filers' signing keys on
 * their HTTP port (storeSecurityToml).
 */
import type {
  KubernetesObject,
  V1ConfigMap,
  V1Container,
  V1CronJob,
  V1Deployment,
  V1EnvVar,
  V1Job,
  V1PersistentVolumeClaim,
  V1PodSpec,
  V1Service,
  V1ServicePort,
  V1StatefulSet,
  V1Volume,
  V1VolumeMount,
  V1VolumeProjection,
  V1WeightedPodAffinityTerm,
} from "@kubernetes/client-node";

import { commonName, type StorePart } from "../../../neon/control/store-security.ts";
import { memoryBytes } from "../capacity.ts";
import { imageReference } from "../images.ts";
import {
  APPLIED_FIRST_ANNOTATION,
  BACKGROUND_CPU,
  claimSpec,
  componentName,
  labels,
  NAMESPACE,
  neonName,
  oncePolicyApplies,
  replicasOf,
  restrictedContainer,
  ROLLS_WITH_CONFIG_ANNOTATION,
  s3Endpoint,
  scaled,
  script,
  selectorLabels,
  stackLabels,
  stackPodSpec,
} from "./common.ts";
import { filerStoreMaxmemoryBytes, type InstallConfig, type Scale } from "./config.ts";
import { replicaBudget, spread } from "./disruption.ts";
import { priority } from "./priorities.ts";
import { cpuScaledObject } from "./scalers.ts";
import { verticalPodAutoscaler } from "./sizing.ts";
import { VALKEY_ONE_AT_A_TIME, VALKEY_PORT, valkeyObjects, valkeyPassword, valkeyPod, type ValkeyUser } from "./valkey.ts";
import { oneAtATime } from "./volume-lock.ts";

/** The gateways' name, their Service's, by which the stack reaches the store. */
export const GATEWAY = componentName("seaweedfs");
export const MASTER = componentName("seaweedfs-master");
export const VOLUME_SERVER = componentName("seaweedfs-volume");
/** The filers' StatefulSet's name, and their Service's, by which a client reaches any of them. */
export const FILER = componentName("seaweedfs-filer");
/** The filers' headless Service, which names each, as they reach each other and the gateways reach them. */
export const FILER_PEERS = componentName("seaweedfs-filer-peers");
/** The filer store's name: its Service's and its StatefulSet's. */
export const FILER_STORE = componentName("seaweedfs-filer-store");

/**
 * The filer store's users, each with its password in a Secret of its own, its default user
 * off: the filers', to the keys under their prefix alone, and of no command that could
 * take down or reconfigure the store; the collector's, which reads INFO and PINGs, as its
 * readiness probe does; and the backups', which take a snapshot of the store as a replica
 * would, and nothing else.
 */
export const FILER_STORE_USERS = {
  filer: { name: "filer", secret: `${FILER_STORE}-filer`, rules: ["~seaweedfs:*", "+@all", "-@dangerous"] },
  collector: { name: "collector", secret: `${FILER_STORE}-collector`, rules: ["+info", "+ping"], probes: true },
  backup: { name: "backup", secret: `${FILER_STORE}-backup`, rules: ["+replconf", "+sync", "+psync", "+ping"] },
} as const satisfies Record<string, ValkeyUser>;

/** The components of the store's parts, by which their pods are told. */
export const COMPONENTS = { gateway: "seaweedfs", master: "seaweedfs-master", volumeServer: "seaweedfs-volume", filer: "seaweedfs-filer", filerStore: "seaweedfs-filer-store" };

/** The component of the lifecycle pass's pods. */
export const LIFECYCLE_COMPONENT = "seaweedfs-lifecycle";

/** The component of the filer store's backups' pods, and the name of the volume they are kept on. */
export const BACKUP_COMPONENT = "seaweedfs-filer-store-backup";
export const BACKUPS = componentName("seaweedfs-filer-store-backups");

/** The port each part serves its Prometheus metrics on. */
export const METRICS_PORT = 9327;

/** The key prefix of the filers' entries in their store. */
const KEY_PREFIX = "seaweedfs:";

/** Each part's ports: its HTTP port's, and its gRPC port's, the HTTP port plus 10000, as SeaweedFS reaches them. */
const PORTS = {
  master: [{ name: "http", port: 9333 }, { name: "grpc", port: 19333 }],
  volumeServer: [{ name: "http", port: 8080 }, { name: "grpc", port: 18080 }],
  filer: [{ name: "http", port: 8888 }, { name: "grpc", port: 18888 }],
  gateway: [{ name: "s3", port: 8333 }, { name: "s3-grpc", port: 18333 }],
};

/** The component of each part that holds a certificate (neon/control/store-security.ts), by which its pods are told. */
export const PART_COMPONENTS: Record<StorePart, string> = {
  master: COMPONENTS.master,
  volume: COMPONENTS.volumeServer,
  filer: COMPONENTS.filer,
  gateway: COMPONENTS.gateway,
  lifecycle: LIFECYCLE_COMPONENT,
};

/**
 * The parts each part that serves takes, on its HTTP and gRPC ports alike
 * (./network-policies.ts partPolicies), and on its gRPC port by their certificates alone
 * (storeSecurityToml); the gateways, on their gRPC port alone, their S3 port being the stack's
 * (gatewayPolicy):
 *
 * - the masters, by each other, in Raft, and by every part that asks them where volumes
 *   are, or registers;
 * - the volume servers, by each other, which copy what they are written to the others, the
 *   masters, which compact their volumes, the filers and gateways, which read and write
 *   objects' chunks there, and the lifecycle pass, which copies volumes again;
 * - the filers, by each other, which follow each other's changes, the gateways, and the
 *   lifecycle pass, which replays their log;
 * - the gateways, by the lifecycle pass, which deletes through them.
 *
 * The `weed shell` beside each gateway, which makes the buckets, is of the gateway's pod, and
 * dials as the gateway.
 */
export const PEERS: Record<Exclude<StorePart, "lifecycle">, readonly StorePart[]> = {
  master: ["master", "volume", "filer", "gateway", "lifecycle"],
  volume: ["volume", "master", "filer", "gateway", "lifecycle"],
  filer: ["filer", "gateway", "lifecycle"],
  gateway: ["lifecycle"],
};

/** The Secret of `part`'s certificate, its authority's, and the filers' signing keys where it takes them, as the stack's setup renders it (neon/control/kube-setup.ts). */
export const storePartSecret = (part: StorePart): string => componentName(`seaweedfs-${part}-security`);

/** The ConfigMap of the store's security.toml (storeSecurityToml), which every part reads. */
export const SECURITY = componentName("seaweedfs-security");

/** Where each part reads SeaweedFS's configuration from, its security.toml among it, and its certificate's files under TLS_DIRECTORY. */
const CONFIG_DIRECTORY = "/etc/seaweedfs";
const TLS_DIRECTORY = `${CONFIG_DIRECTORY}/tls`;

/**
 * The store's security.toml, which every part reads, each its own certificate under
 * TLS_DIRECTORY: its gRPC, mutual TLS, each part presenting its certificate as a server
 * ([grpc.master], [grpc.volume], [grpc.filer] and the gateways' [grpc.s3]) and as a client
 * (the same sections, but for the gateways and `weed shell`, which dial as [grpc.client]),
 * and each server taking the certificates of its authority, ca.crt, of the parts it takes
 * (PEERS) alone, by their common names; a plaintext connection is refused. And the filers take
 * requests over HTTP, and calls of their IAM service, signed with their signing keys alone, of
 * writes and of reads, which no part makes, nor anything but main's lake's reconciliation, its
 * reads of the lake's entries (neon/control/store-security.ts filerReadToken); and the gateways'
 * gRPC port takes an admin's calls signed with the key of writes alone, as the lifecycle pass
 * signs its deletes. The keys are each such part's environment's (filerSigningKey,
 * FILER_READ_SIGNING_KEY), not this file's, which is no Secret.
 */
export function storeSecurityToml(): string {
  const section = (name: string, peers?: readonly StorePart[]) => [
    `[grpc.${name}]`,
    `cert = "${TLS_DIRECTORY}/tls.crt"`,
    `key = "${TLS_DIRECTORY}/tls.key"`,
    ...(peers ? [`allowed_commonNames = "${peers.map(commonName).join(",")}"`] : []),
    "",
  ];
  return [
    "[grpc]",
    `ca = "${TLS_DIRECTORY}/ca.crt"`,
    "",
    ...section("master", PEERS.master),
    ...section("volume", PEERS.volume),
    ...section("filer", PEERS.filer),
    ...section("s3", PEERS.gateway),
    ...section("client"),
  ].join("\n");
}

/**
 * The volume `name` of a part's configuration, mounted at CONFIG_DIRECTORY: the store's
 * security.toml, `part`'s certificate and its authority's, which the part's user alone reads,
 * and what of its own `others` project there.
 */
function configVolume(name: string, part: StorePart, others: readonly V1VolumeProjection[] = []): V1Volume {
  return {
    name,
    projected: {
      defaultMode: 0o440,
      sources: [
        ...others,
        { configMap: { name: SECURITY } },
        { secret: { name: storePartSecret(part), items: ["ca.crt", "tls.crt", "tls.key"].map((key) => ({ key, path: `tls/${key}` })) } },
      ],
    },
  };
}

/** The mount of a part's configuration volume `name` (configVolume), where SeaweedFS reads it. */
const configMount = (name: string) => ({ name, mountPath: CONFIG_DIRECTORY, readOnly: true });

/**
 * Each part of the store that runs, each StatefulSet and the gateways' Deployment, of `objects`:
 * applied before the rest of the stack's workloads, which are applied only once these are ready
 * (APPLIED_FIRST_ANNOTATION), so the store is away no longer than its own parts restart, and back
 * before anything that reads it restarts; and replaced as what they read as they start alone
 * changes (ROLLS_WITH_CONFIG_ANNOTATION): security.toml, the Secrets of their certificates and
 * keys, which the stack setup makes, and the rest of their configuration.
 */
const storeParts = (objects: readonly KubernetesObject[]): KubernetesObject[] =>
  objects.map((object) =>
    object.kind === "StatefulSet" || object.kind === "Deployment"
      ? { ...object, metadata: { ...object.metadata, annotations: { ...object.metadata?.annotations, [APPLIED_FIRST_ANNOTATION]: "true", [ROLLS_WITH_CONFIG_ANNOTATION]: "true" } } }
      : object
  );

/**
 * The filers' signing key of writes, of `part`'s Secret: which the filers take requests over
 * HTTP, and calls of their IAM service, signed with alone, and the gateways' gRPC port an
 * admin's calls, the lifecycle pass's among them, which the pass signs with it.
 */
const filerSigningKey = (part: "filer" | "gateway" | "lifecycle"): V1EnvVar => ({ name: "WEED_JWT_FILER_SIGNING_KEY", valueFrom: { secretKeyRef: { name: storePartSecret(part), key: "filer-signing-key" } } });

/** The filers' signing key of reads, which they take reads over HTTP signed with alone. */
const FILER_READ_SIGNING_KEY: V1EnvVar = { name: "WEED_JWT_FILER_SIGNING_READ_KEY", valueFrom: { secretKeyRef: { name: storePartSecret("filer"), key: "filer-signing-read-key" } } };

/**
 * The lifecycle rule of each bucket a writer puts files larger than a part into, in parts: a
 * multipart upload left neither completed nor aborted, as an uploader ended mid-upload
 * leaves the parts it had sent, aborted after a day, the least S3 allows. Neon's, the
 * lake's (DuckDB's files), the backups' (`aws s3 cp`'s dumps) and the snapshots' (a node
 * agent puts a file larger than a part in parts, src/node-agent/s3.ts, and aborts an upload
 * that fails, but not one it was stopped in). Workspace storage's needs none: JuiceFS puts
 * each block whole. What else of Neon's bucket Neon needs no more, the lifecycle pass
 * deletes itself (lifecyclePass). An external store is given no rule by alasio: its
 * operator gives the buckets this one, and Neon's those of the ages neon/control/expiry.ts
 * keeps its objects for.
 */
const ABORT_INCOMPLETE_UPLOADS = {
  Rules: [{ ID: "abort-incomplete-uploads", Status: "Enabled", Filter: { Prefix: "" }, AbortIncompleteMultipartUpload: { DaysAfterInitiation: 1 } }],
};

/** The address of the pod `ordinal` of the StatefulSet `name`, by its headless Service `service`, at `port`. */
const podAddress = (name: string, service: string, ordinal: number, port: number) => `${name}-${ordinal}.${service}.${NAMESPACE}.svc.cluster.local:${port}`;

/** The addresses of the `count` pods of the StatefulSet `name`, by `service`, at `port`, as SeaweedFS takes a list of them. */
const podAddresses = (name: string, service: string, count: number, port: number) =>
  Array.from({ length: count }, (_, ordinal) => podAddress(name, service, ordinal, port)).join(",");

/** The `count` masters, as the other parts, and `weed shell`, reach them. */
export const masterAddresses = (count: number): string => podAddresses(MASTER, MASTER, count, 9333);

/** The `count` volume servers, by their HTTP URLs, as a client asks each of what it holds. */
export const volumeServerUrls = (count: number): string[] =>
  Array.from({ length: count }, (_, ordinal) => `http://${podAddress(VOLUME_SERVER, VOLUME_SERVER, ordinal, 8080)}`);

/** The masters, as the other parts reach them. */
const masters = ({ objectStore }: InstallConfig) => masterAddresses(objectStore.bundled.masters.scaling.min);

/** A pod's name and namespace, by which a part names itself to the others. */
const POD_ENV: V1EnvVar[] = [
  { name: "POD_NAME", valueFrom: { fieldRef: { fieldPath: "metadata.name" } } },
  { name: "POD_NAMESPACE", valueFrom: { fieldRef: { fieldPath: "metadata.namespace" } } },
];

/** The address a pod of a StatefulSet names itself by, its own in its headless Service `service`. */
const ownAddress = (service: string) => `-ip=$(POD_NAME).${service}.$(POD_NAMESPACE).svc.cluster.local`;

/** The headless Service `name` of `component`'s pods, on `ports`, which names each pod, ready or not. */
function headless(name: string, component: string, ports: readonly { readonly name: string; readonly port: number }[]): V1Service {
  return {
    apiVersion: "v1",
    kind: "Service",
    metadata: { name, namespace: NAMESPACE, labels: stackLabels(component) },
    spec: { clusterIP: "None", publishNotReadyAddresses: true, selector: selectorLabels(component), ports: [...ports, { name: "metrics", port: METRICS_PORT }] },
  };
}

/** How a part's StatefulSet differs from the others': its headless Service, unless its own name; its pods started in order; spread across nodes strictly. */
interface PartOptions {
  readonly service?: string;
  readonly ordered?: boolean;
  readonly strictly?: boolean;
}

/** The StatefulSet `name` of `component`, of `scale`, running `containers`, as every part of the store's is: on the floor, above it where its claims hold it to a node, as the data stack's user. */
function partStatefulSet(
  config: InstallConfig,
  name: string,
  component: string,
  scale: Scale,
  pod: { readonly containers: V1Container[]; readonly initContainers?: V1Container[]; readonly volumes?: V1Volume[] },
  storage?: { readonly size: string; readonly storageClassName: string },
  { service = name, ordered = false, strictly = false }: PartOptions = {},
): V1StatefulSet {
  return {
    apiVersion: "apps/v1",
    kind: "StatefulSet",
    metadata: { name, namespace: NAMESPACE, labels: stackLabels(component) },
    spec: {
      serviceName: service,
      ...replicasOf(scale),
      podManagementPolicy: ordered ? "OrderedReady" : "Parallel",
      selector: { matchLabels: selectorLabels(component) },
      template: {
        metadata: { labels: stackLabels(component) },
        spec: {
          ...priority(storage ? "bound" : "floor"),
          ...stackPodSpec(config),
          ...spread(component, scale, strictly),
          terminationGracePeriodSeconds: 30,
          ...(pod.initContainers ? { initContainers: pod.initContainers } : {}),
          containers: pod.containers,
          ...(pod.volumes ? { volumes: pod.volumes } : {}),
        },
      },
      ...(storage ? { volumeClaimTemplates: [{ metadata: { name: "data" }, spec: claimSpec(storage) }] } : {}),
    },
  };
}

/**
 * The container of a part, `name`, running `weed` with `args`, probed at `path` on its HTTP
 * port, its configuration the volume "config" (configVolume); of a part with a volume, the
 * `server` on it at /data.
 */
function partContainer(
  config: InstallConfig,
  name: string,
  args: string[],
  path: string,
  port: number,
  resources: V1Container["resources"],
  server?: string,
): V1Container {
  const probe = { httpGet: { path, port } };
  return {
    name,
    image: imageReference(config.objectStore.bundled.image),
    imagePullPolicy: "IfNotPresent",
    // On a volume, started once no other server holds the volume's lock (./volume-lock.ts),
    // a file of its own beside the part's, by the image's entrypoint, as one without is,
    // which runs weed with the image's defaults for the part.
    ...(server ? { command: ["/bin/sh", "-c", oneAtATime(`/data/.alasio-${name}.lock`, server), "/entrypoint.sh"] } : {}),
    args: [...args, `-metricsPort=${METRICS_PORT}`],
    env: POD_ENV,
    ports: [{ name: "http", containerPort: port }, { name: "grpc", containerPort: port + 10_000 }, { name: "metrics", containerPort: METRICS_PORT }],
    readinessProbe: { ...probe, periodSeconds: 5 },
    // Started once it answers, which one on a volume does only once the server before it
    // there has ended (within that pod's grace): five minutes for that and its start, so the
    // wait is not ended by the liveness probe and put back by a crash loop.
    ...(server ? { startupProbe: { ...probe, periodSeconds: 5, failureThreshold: 60 } } : {}),
    livenessProbe: { ...probe, initialDelaySeconds: 30, periodSeconds: 10, failureThreshold: 6 },
    securityContext: restrictedContainer(),
    ...(resources ? { resources } : {}),
    volumeMounts: [configMount("config"), ...(server ? [{ name: "data", mountPath: "/data" }] : [])],
  };
}

/** The masters: of their state, a volume each; of three, in Raft, each naming the others. */
function masterObjects(config: InstallConfig): KubernetesObject[] {
  const { bundled } = config.objectStore;
  const { scaling } = bundled.masters;
  const args = [
    "master",
    "-mdir=/data",
    ownAddress(MASTER),
    "-ip.bind=0.0.0.0",
    // One alone needs no quorum: none, it is the leader as it starts.
    `-peers=${scaling.min > 1 ? masters(config) : "none"}`,
    "-volumeSizeLimitMB=1024",
    `-defaultReplication=${bundled.replication}`,
    // A volume is compacted once a tenth of it is deleted, not a third as by default:
    // Neon's objects all expire within days, so every volume comes to hold deleted ones,
    // and the store would keep up to a third of its disk dead. A compaction copies the
    // volume's live rest, at about half a gigabyte a second.
    "-garbageThreshold=0.1",
    "-telemetry=false",
    // Its HTTP port's requests that read or change where volumes are, or delete a bucket's
    // volumes whole (/dir/*, /col/delete, /vol/*), taken from its own pod alone, as an
    // operator's shell in it asks them, not from whatever else reaches the port: the store's
    // parts reach each other through gRPC, of mutual TLS, and the filers' wait for a volume
    // server, the lake's reconciliation and alasio's checks ask through it, or the volume
    // servers, or in a master's pod. Its probes, /cluster/status, are no such request.
    "-whiteList=127.0.0.1,::1",
  ];
  const container = partContainer(config, "master", args, "/cluster/status", 9333, bundled.masters.resources, "master");
  const statefulSet = partStatefulSet(config, MASTER, COMPONENTS.master, scaling, { containers: [container], volumes: [configVolume("config", "master")] }, bundled.masters.storage);
  return [headless(MASTER, COMPONENTS.master, PORTS.master), statefulSet, ...replicaBudget(COMPONENTS.master, NAMESPACE, scaling), verticalPodAutoscaler(statefulSet, "Recommended")];
}

/**
 * The volume servers, each keeping its volumes on a volume of its own, in one data center
 * and rack, so a copy goes to another server; and, where copies are kept, each on a node
 * of its own while there are as many, so a copy is on another node, a server left
 * unplaced rather than put beside another.
 */
function volumeServerObjects(config: InstallConfig): KubernetesObject[] {
  const { bundled } = config.objectStore;
  const { volumeServers } = bundled;
  const args = [
    "volume",
    "-dir=/data",
    `-max=${volumeServers.volumes}`,
    `-minFreeSpace=${volumeServers.minFreeSpace}`,
    `-master=${masters(config)}`,
    ownAddress(VOLUME_SERVER),
    "-ip.bind=0.0.0.0",
    "-dataCenter=dc1",
    "-rack=rack1",
  ];
  const container = partContainer(config, "volume", args, "/status", 8080, volumeServers.resources, "volume server");
  const statefulSet = partStatefulSet(config, VOLUME_SERVER, COMPONENTS.volumeServer, volumeServers.scaling, { containers: [container], volumes: [configVolume("config", "volume")] }, volumeServers.storage, {
    strictly: bundled.replication !== "000",
  });
  return [
    headless(VOLUME_SERVER, COMPONENTS.volumeServer, PORTS.volumeServer),
    statefulSet,
    ...replicaBudget(COMPONENTS.volumeServer, NAMESPACE, volumeServers.scaling),
    verticalPodAutoscaler(statefulSet, "Recommended"),
  ];
}

/** The filers' configuration: their store, the filer store, as its user `filer`, whose password each is given as WEED_REDIS2_PASSWORD. */
export function filerToml(): string {
  return [
    "[redis2]",
    "enabled = true",
    `address = "${FILER_STORE}:${VALKEY_PORT}"`,
    `username = "${FILER_STORE_USERS.filer.name}"`,
    "database = 0",
    `keyPrefix = "${KEY_PREFIX}"`,
    "superLargeDirectories = []",
    "",
  ].join("\n");
}

/**
 * The filers, started one after another on the filer store, each once the store has
 * loaded what it keeps and a volume server has registered with the masters.
 */
function filerObjects(config: InstallConfig): KubernetesObject[] {
  const { bundled } = config.objectStore;
  const { filers } = bundled;
  const image = imageReference(bundled.image);
  const configMap: V1ConfigMap = { apiVersion: "v1", kind: "ConfigMap", metadata: { name: FILER, namespace: NAMESPACE, labels: stackLabels(COMPONENTS.filer) }, data: { "filer.toml": filerToml() } };
  const waiting = (name: string, waitImage: string, line: string, { env = [], memory = "64Mi", mounts = [] }: { env?: V1EnvVar[]; memory?: string; mounts?: V1VolumeMount[] } = {}): V1Container => ({
    name: `wait-for-${name}`,
    image: waitImage,
    imagePullPolicy: "IfNotPresent",
    command: ["/bin/sh", "-c", line],
    ...(env.length > 0 ? { env } : {}),
    securityContext: restrictedContainer(),
    resources: { requests: { cpu: "10m", memory: "16Mi" }, limits: { memory } },
    ...(mounts.length > 0 ? { volumeMounts: mounts } : {}),
  });
  const initContainers = [
    // Answers PONG once it has loaded its append-only file, LOADING before.
    waiting(
      "filer-store",
      imageReference(bundled.filerStore.image),
      `until valkey-cli -h ${FILER_STORE} --user ${FILER_STORE_USERS.filer.name} ping 2>/dev/null | grep -q PONG; do sleep 2; done`,
      { env: [{ name: "VALKEYCLI_AUTH", valueFrom: valkeyPassword(FILER_STORE_USERS.filer.secret) }] },
    ),
    // Through the masters' gRPC, as the filer, as their HTTP port takes none but their own pods'
    // requests (masterObjects): their topology's room for volumes, none until a volume server
    // has registered, or a volume server of theirs, which it names once it holds a volume.
    waiting(
      "volume-server",
      image,
      `until printf 'volume.list\\n' | weed shell -master=${masters(config)} 2>/dev/null | grep -qE '^Topology .*volume:[0-9]+/[1-9]| DataNode '; do sleep 2; done`,
      { memory: "256Mi", mounts: [configMount("config")] },
    ),
  ];
  const args = ["filer", `-master=${masters(config)}`, ownAddress(FILER_PEERS), "-ip.bind=0.0.0.0", "-defaultStoreDir=/tmp"];
  const container: V1Container = {
    ...partContainer(config, "filer", args, "/healthz", 8888, filers.resources),
    env: [...POD_ENV, { name: "WEED_REDIS2_PASSWORD", valueFrom: valkeyPassword(FILER_STORE_USERS.filer.secret) }, filerSigningKey("filer"), FILER_READ_SIGNING_KEY],
    volumeMounts: [configMount("config"), { name: "tmp", mountPath: "/tmp" }],
  };
  const statefulSet = partStatefulSet(
    config,
    FILER,
    COMPONENTS.filer,
    filers.scaling,
    { initContainers, containers: [container], volumes: [configVolume("config", "filer", [{ configMap: { name: FILER } }]), { name: "tmp", emptyDir: {} }] },
    undefined,
    { service: FILER_PEERS, ordered: true },
  );
  // Any filer, for the shells that need one: the buckets' and the lifecycle pass's.
  const service: V1Service = {
    apiVersion: "v1",
    kind: "Service",
    metadata: { name: FILER, namespace: NAMESPACE, labels: stackLabels(COMPONENTS.filer) },
    spec: { selector: selectorLabels(COMPONENTS.filer), ports: PORTS.filer },
  };
  return [
    configMap,
    headless(FILER_PEERS, COMPONENTS.filer, PORTS.filer),
    service,
    statefulSet,
    ...replicaBudget(COMPONENTS.filer, NAMESPACE, filers.scaling),
    verticalPodAutoscaler(statefulSet, "InPlace"),
  ];
}

/** The filer store: a Valkey of the filers' own, its memory sized for its entries. */
function filerStoreObjects(config: InstallConfig): KubernetesObject[] {
  const { filerStore } = config.objectStore.bundled;
  const maxmemory = `${filerStoreMaxmemoryBytes(filerStore.entries) / 2 ** 20}mb`;
  const { filer, collector, backup } = FILER_STORE_USERS;
  return valkeyObjects(config, {
    name: FILER_STORE,
    component: COMPONENTS.filerStore,
    image: filerStore.image,
    storage: filerStore.storage,
    resources: filerStore.resources,
    maxmemory,
    users: [filer, collector, backup],
  });
}

/**
 * How many MiB an S3 gateway of `resources` holds at most of the uploads under way: a request
 * past it waits for room, its part held whole as it is read. An eighth of its memory limit,
 * 256 MiB of 2 GiB; 256 where it has no limit.
 *
 * SeaweedFS 4.54 waits for room (s3api_circuit_breaker.go waitForUploadSlot) on a sync.Cond,
 * under its lock, which every upload that ends wakes once it has given back its bytes, so no
 * wait misses it; but one woken that finds no room waits again, while one that comes and finds
 * room goes in at once, so one may wait as others pass it; and it looks before it adds its own,
 * so the uploads under way may go past the bound by one request's. One that waits a minute
 * (-inflightUploadDataTimeout) is answered 503 SlowDown, its body unread, which each of the
 * store's clients sends again later, the node agents too (src/node-agent/s3.ts
 * SLOW_DOWN_SENDS), rather than hold its connection and what the kernel buffers of it.
 */
export function gatewayUploadBufferMiB(resources: { readonly limits?: { readonly memory?: string } }): number {
  const limit = resources.limits?.memory;
  return limit === undefined ? 256 : Math.max(16, Math.floor(memoryBytes(limit) / 8 / 2 ** 20));
}

/**
 * The sizes, at least, by default and at most, of each of a gateway's connections' kernel
 * buffers, received and sent: at most 1 MiB, of the 6 and 4 MiB a node allows by default. A
 * request waiting for the gateway's budgets keeps its connection, its client's bytes filling
 * the buffer it receives into, all charged to the gateway's cgroup: 243 MiB of them, beside
 * 766 MiB of its own, as it was killed at its 1 GiB limit under the box's load (CI). Safe
 * sysctls since Kubernetes 1.32, namespaced to the pod.
 */
export const GATEWAY_SYSCTLS = [
  { name: "net.ipv4.tcp_rmem", value: "4096 131072 1048576" },
  { name: "net.ipv4.tcp_wmem", value: "4096 16384 1048576" },
] as const;

/**
 * The Go runtime's soft memory limit of a container of `resources`, GOMEMLIMIT: three fifths
 * of its memory limit, so that its collector runs before the kernel's OOM killer would,
 * rather than once its heap has doubled, beside what the kernel charges the container that
 * Go does not count, its connections' buffers among it; none where it has no limit. Its
 * live heap under the load job was 130 to 330 MiB of the 620 to 770 MiB it held (CI).
 */
export function goMemoryLimit(resources: { readonly limits?: { readonly memory?: string } }): { env?: V1EnvVar[] } {
  const limit = resources.limits?.memory;
  return limit === undefined ? {} : { env: [{ name: "GOMEMLIMIT", value: `${Math.floor((memoryBytes(limit) * 0.6) / 2 ** 20)}MiB` }] };
}

/**
 * The S3 gateways, on the filers, each making the buckets and their lifecycle rules as
 * it starts, in sidecars, and ready only once they are made.
 */
function gatewayObjects(config: InstallConfig): KubernetesObject[] {
  const { bundled, buckets } = config.objectStore;
  const { scaling } = bundled.gateways;
  const made = [buckets.neon, buckets.lake, buckets.backups, buckets.workspaces, buckets.snapshots];
  // Each bucket's rule given as an admin of the bucket, which SeaweedFS asks of its rules (from
  // 4.52): Neon's as its admin, the snapshots' as their own writer, which is their admin, and the
  // lake's and the backups' as the gateways' own identity, which is theirs, each by the keys of
  // the Secret it is given (neon/control/secrets.ts s3Identities, neon/control/kube-setup.ts).
  const aws = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"] as const;
  const lifecycles = [
    { bucket: buckets.neon, as: "ADMIN", secret: neonName("s3-admin"), keys: aws },
    { bucket: buckets.lake, as: "RULES", secret: componentName("seaweedfs-rules"), keys: aws },
    { bucket: buckets.backups, as: "RULES", secret: componentName("seaweedfs-rules"), keys: aws },
    { bucket: buckets.snapshots, as: "SNAPSHOTS", secret: componentName("snapshots"), keys: aws },
  ];
  const image = imageReference(bundled.image);
  const ready = { exec: { command: ["test", "-f", "/tmp/ready"] }, periodSeconds: 5 };
  const service: V1Service = {
    apiVersion: "v1",
    kind: "Service",
    metadata: { name: GATEWAY, namespace: NAMESPACE, labels: stackLabels(COMPONENTS.gateway) },
    spec: { selector: selectorLabels(COMPONENTS.gateway), ports: [...PORTS.gateway, { name: "metrics", port: METRICS_PORT }] as V1ServicePort[] },
  };
  const deployment: V1Deployment = {
    apiVersion: "apps/v1",
    kind: "Deployment",
    metadata: { name: GATEWAY, namespace: NAMESPACE, labels: stackLabels(COMPONENTS.gateway) },
    spec: {
      ...replicasOf(scaling),
      selector: { matchLabels: selectorLabels(COMPONENTS.gateway) },
      template: {
        metadata: { labels: stackLabels(COMPONENTS.gateway) },
        spec: {
          ...priority("floor"),
          ...stackPodSpec(config),
          // Its connections' buffers held small: the kernel's are charged to its cgroup,
          // and it holds a connection for each request under way or waiting (GATEWAY_SYSCTLS).
          securityContext: { ...stackPodSpec(config).securityContext, sysctls: [...GATEWAY_SYSCTLS] },
          ...spread(COMPONENTS.gateway, scaling),
          terminationGracePeriodSeconds: 30,
          containers: [
            {
              name: "s3",
              image,
              imagePullPolicy: "IfNotPresent",
              args: [
                "s3",
                `-filer=${podAddresses(FILER, FILER_PEERS, bundled.filers.scaling.min, 8888)}`,
                "-ip.bind=0.0.0.0",
                "-config=/etc/seaweedfs/s3.json",
                "-allowDeleteBucketNotEmpty=false",
                "-autoCreateBucket=false",
                `-metricsPort=${METRICS_PORT}`,
                // What it holds of uploads under way, bounded (gatewayUploadBufferMiB), as
                // SeaweedFS holds none, part by part, whole.
                `-concurrentUploadLimitMB=${gatewayUploadBufferMiB(bundled.gateways.resources)}`,
                // What its GETs hold is not budgeted (no -readerCacheSizeMB). Each GET holds its
                // chunk and four ahead, of the filers' 4 MiB, so the box's clients' GETs at once, 86
                // with eight restores of images of their own, want 1.2 GiB, and 2 with fifteen,
                // more than a budget a gateway can afford; and a budget bought little
                // under the load job (test/gateway-memory.test.ts, SeaweedFS 4.54, whose reader
                // keeps the chunk a GET reads from its budget's evictions): of 512 MiB, its most
                // held fell from 1334 to 1181 MiB with 86 GETs, and from 1424 to 1285 MiB with
                // 142, whose median GET slowed from 6.6 to 2.7 MiB/s; of 1024 MiB, neither moved.
                // Unbudgeted, they hold at most SeaweedFS's 256 chunks in flight, 1 GiB, which
                // its limit (2 GiB) has room for beside the rest: under the load job it held
                // 1.4 GiB at most.
              ],
              env: [...(goMemoryLimit(bundled.gateways.resources).env ?? []), filerSigningKey("gateway")],
              ports: [...PORTS.gateway.map(({ name, port }) => ({ name, containerPort: port })), { name: "metrics", containerPort: METRICS_PORT }],
              readinessProbe: { httpGet: { path: "/healthz", port: "s3" }, periodSeconds: 5 },
              livenessProbe: { httpGet: { path: "/healthz", port: "s3" }, initialDelaySeconds: 30, periodSeconds: 10, failureThreshold: 6 },
              securityContext: restrictedContainer(),
              resources: bundled.gateways.resources,
              // Its identities beside its configuration.
              volumeMounts: [configMount("config")],
            },
            // Makes the buckets, idempotently, as the gateway starts: Neon's versioned (its
            // time-travel recovery needs that), writes fsynced. One a gateway beside it has
            // made at once is found made. A bucket whose objects the volumes hold, by its
            // collection, while the filers know no such bucket, is not made anew, empty: the
            // filer store has lost what it kept, which `alasio restore-filer-store` puts back instead,
            // and the gateway is not ready until it is.
            {
              name: "buckets",
              image,
              imagePullPolicy: "IfNotPresent",
              command: [
                "/bin/sh",
                "-c",
                script(
                  "set -e",
                  `shell() { printf '%s\\n' "$1" | weed shell -master=${masters(config)} -filer=${FILER}:8888; }`,
                  // A bucket, by its name exactly, as each line of the list starts with one.
                  "listed() { shell 's3.bucket.list' | awk '{print $1}' | grep -qxF \"$1\"; }",
                  "until wget -q -O /dev/null http://127.0.0.1:8333/healthz; do sleep 2; done",
                  "shell 'fs.configure -locationPrefix=/buckets/ -volumeGrowthCount=1 -fsync -apply'",
                  `for bucket in ${made.join(" ")}; do`,
                  "  if ! listed \"$bucket\"; then",
                  "    if shell 'volume.list' | grep -q \"Collection:$bucket,\"; then",
                  "      echo \"the store's volumes hold objects of the bucket $bucket, which its filers do not know: their store has lost what it kept; alasio restore-filer-store puts it back from its backups, rather than make $bucket anew, empty\" >&2",
                  "      exit 1",
                  "    fi",
                  "    shell \"s3.bucket.create -name $bucket\"",
                  "  fi",
                  "  listed \"$bucket\"",
                  "done",
                  `shell 's3.bucket.versioning -name ${buckets.neon} -enable'`,
                  `shell 's3.bucket.versioning -name ${buckets.neon}' | grep -qi enabled`,
                  "touch /tmp/ready",
                  "exec sleep infinity",
                ),
              ],
              readinessProbe: ready,
              securityContext: restrictedContainer(),
              resources: { requests: { cpu: "10m", memory: "32Mi" }, limits: { memory: "256Mi" } },
              // Its configuration, the gateway's, but for its identities.
              volumeMounts: [configMount("security"), { name: "shell-tmp", mountPath: "/tmp" }],
            },
            // Gives each bucket written in parts its lifecycle rule once it exists.
            {
              name: "lifecycle",
              image: imageReference(config.neon.image),
              imagePullPolicy: "IfNotPresent",
              command: [
                "/bin/sh",
                "-c",
                script(
                  "set -e",
                  ...lifecycles.flatMap(({ bucket, as }) => {
                    const s3api = `AWS_ACCESS_KEY_ID="$${as}_KEY_ID" AWS_SECRET_ACCESS_KEY="$${as}_SECRET" aws s3api --endpoint-url http://127.0.0.1:8333`;
                    return [
                      `until ${s3api} head-bucket --bucket ${bucket} 2>/dev/null; do sleep 2; done`,
                      `${s3api} put-bucket-lifecycle-configuration --bucket ${bucket} --lifecycle-configuration '${JSON.stringify(ABORT_INCOMPLETE_UPLOADS)}'`,
                    ];
                  }),
                  "touch /tmp/ready",
                  "exec sleep infinity",
                ),
              ],
              env: [
                { name: "AWS_DEFAULT_REGION", value: "us-east-1" },
                { name: "HOME", value: "/tmp" },
                // Each identity's keys once, however many buckets it gives rules.
                ...[...new Map(lifecycles.map((lifecycle) => [lifecycle.as, lifecycle])).values()].flatMap(({ as, secret, keys: [id, key] }) => [
                  { name: `${as}_KEY_ID`, valueFrom: { secretKeyRef: { name: secret, key: id } } },
                  { name: `${as}_SECRET`, valueFrom: { secretKeyRef: { name: secret, key } } },
                ]),
              ],
              readinessProbe: ready,
              securityContext: restrictedContainer(),
              resources: { requests: { cpu: "10m", memory: "64Mi" }, limits: { memory: "256Mi" } },
              volumeMounts: [{ name: "lifecycle-tmp", mountPath: "/tmp" }],
            },
          ],
          volumes: [
            configVolume("config", "gateway", [{ secret: { name: neonName("seaweedfs") } }]),
            configVolume("security", "gateway"),
            { name: "shell-tmp", emptyDir: {} },
            { name: "lifecycle-tmp", emptyDir: {} },
          ],
        },
      },
    },
  };
  return [
    service,
    deployment,
    // Its start is the gateway reaching its filer and, beside it, `weed shell` making the
    // buckets and their lifecycle rules, which it is not ready until done.
    ...cpuScaledObject(deployment, scaling, { percent: 80, startUpSeconds: 120 }),
    ...replicaBudget(COMPONENTS.gateway, NAMESPACE, scaling),
    // Of memory alone where KEDA scales the gateways on their CPU, as the two would chase each other.
    verticalPodAutoscaler(deployment, "InPlace", scaled(scaling) ? ["memory"] : undefined),
  ];
}

/**
 * SeaweedFS applies lifecycle rules only when asked: a pass every hour, deleting through
 * the gateways' gRPC port. A pass replays the filers' log of changes from where each
 * shard's last stopped, which is the oldest change with an expiry not yet due; so it reads
 * to now (`-events 0`), not a count of changes, which would hold a short rule's expiries
 * behind a long one's until that one's came due. It stops at its runtime, well before the
 * next, and the one after it goes on from there.
 *
 * The pass then sets each volume's replication to the store's, which differs where the
 * store's was changed since the volume was made, and, where copies are kept, copies again
 * each volume with fewer than that, as a volume server lost, or come back empty, or a
 * replication raised leaves it; holding the shell's lock while it does, which one shell
 * holds at a time.
 *
 * Beside it, the pass expires what of Neon's bucket Neon needs no more
 * (neon/control/expiry.ts), with alasio's image, as the bucket's admin, asking the storage
 * controller each timeline's remote consistent LSN, which it records in the bucket, and
 * below the one recorded RESTORABLE_DAYS before alone deletes the timeline's WAL, with a
 * token of the scrubber's scope (neon/control/kube-setup.ts).
 */
function lifecyclePass(config: InstallConfig): V1CronJob {
  const component = LIFECYCLE_COMPONENT;
  const { replication } = config.objectStore.bundled;
  const commands = [
    `s3.lifecycle.run-shard -shards 0-15 -s3 ${GATEWAY}:18333 -events 0 -runtime 10m`,
    "lock",
    `volume.configure.replication -replication ${replication} -collectionPattern *`,
    ...(replication === "000" ? [] : ["volume.fix.replication -apply"]),
    "unlock",
  ];
  return {
    apiVersion: "batch/v1",
    kind: "CronJob",
    metadata: { name: componentName(component), namespace: NAMESPACE, labels: stackLabels(component) },
    spec: {
      schedule: "41 * * * *",
      concurrencyPolicy: "Forbid",
      successfulJobsHistoryLimit: 1,
      failedJobsHistoryLimit: 3,
      jobTemplate: {
        spec: {
          backoffLimit: 1,
          // Its copies' too, which a lost volume server's many take.
          activeDeadlineSeconds: 2700,
          template: {
            metadata: { labels: stackLabels(component) },
            spec: {
              restartPolicy: "Never",
              ...priority("component"),
              ...stackPodSpec(config),
              containers: [{
                name: "lifecycle",
                image: imageReference(config.objectStore.bundled.image),
                imagePullPolicy: "IfNotPresent",
                command: [
                  "/bin/sh",
                  "-c",
                  `${oncePolicyApplies(`wget -q -O /dev/null http://${MASTER}:9333/cluster/status`)} && ` +
                  `printf '${commands.map((command) => `${command}\\n`).join("")}' | weed shell -master=${masters(config)} -filer=${FILER}:8888`,
                ],
                // What it signs its deletes through the gateways with.
                env: [filerSigningKey("lifecycle")],
                securityContext: restrictedContainer(),
                resources: { requests: { cpu: BACKGROUND_CPU, memory: "32Mi" }, limits: { memory: "256Mi" } },
                volumeMounts: [configMount("config"), { name: "tmp", mountPath: "/tmp" }],
              }, {
                name: "neon-expiry",
                image: imageReference(config.images.alasio),
                imagePullPolicy: config.images.pullPolicy,
                // Under the image's entrypoint, as each of its programs is: its shell, then the expiry in its place.
                args: [
                  "/bin/sh",
                  "-c",
                  `${oncePolicyApplies(`curl -fsS -o /dev/null --max-time 5 ${s3Endpoint(config)}/healthz`)} && exec node /opt/alasio/neon/control/expiry.ts`,
                ],
                env: [
                  { name: "S3_ENDPOINT", value: s3Endpoint(config) },
                  { name: "S3_REGION", value: config.objectStore.external.region },
                  { name: "S3_BUCKET_NEON", value: config.objectStore.buckets.neon },
                  { name: "STORAGE_CONTROLLER_URL", value: `http://${neonName("storage-controller")}:1234` },
                  // Its admin's keys, and its token of the scrubber's scope (neon/control/kube-setup.ts).
                  ...["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "STORAGE_CONTROLLER_TOKEN"].map((key) => ({ name: key, valueFrom: { secretKeyRef: { name: neonName("expiry"), key } } })),
                ],
                securityContext: restrictedContainer(),
                resources: { requests: { cpu: BACKGROUND_CPU, memory: "128Mi" }, limits: { memory: "512Mi" } },
              }],
              volumes: [configVolume("config", "lifecycle"), { name: "tmp", emptyDir: {} }],
            },
          },
        },
      },
    },
  };
}

/**
 * The filer store's backups: a snapshot of the whole store, every bucket, key, version and
 * tag, and the log of changes lifecycle passes replay, as Valkey writes one for a replica,
 * taken by the user that may do that alone, on the backups' schedule; written under a
 * name of its own, then renamed, so a backup that fails leaves none behind but its
 * predecessors, of which the last `keep` are kept. On a volume of their own, outside the
 * store they copy, made, where the stack has several nodes and another can take it, on
 * another of them than the filer store's (the volume is placed by the pod that first mounts
 * it, and keeps its pods on its node from then on). `alasio restore-filer-store` puts one
 * back (filerStoreRestore).
 */
function filerStoreBackup(config: InstallConfig): KubernetesObject[] {
  const { filerStore } = config.objectStore.bundled;
  if (!filerStore.backups.enabled) return [];
  const claim: V1PersistentVolumeClaim = {
    apiVersion: "v1",
    kind: "PersistentVolumeClaim",
    metadata: { name: BACKUPS, namespace: NAMESPACE, labels: labels(BACKUP_COMPONENT) },
    spec: claimSpec(filerStore.backups.storage),
  };
  const save: V1Container = {
    name: "save",
    image: imageReference(filerStore.image),
    imagePullPolicy: "IfNotPresent",
    command: [
      "/bin/sh",
      "-c",
      script(
        "set -eu",
        "stamp=$(date -u +%Y%m%dT%H%M%SZ)",
        // What a backup that failed left behind, which no other is writing: they run one at a time.
        "rm -f /backups/.filer-store-*.rdb",
        oncePolicyApplies(`valkey-cli -h ${FILER_STORE} --user ${FILER_STORE_USERS.backup.name} ping 2>/dev/null | grep -q PONG`),
        `valkey-cli -h ${FILER_STORE} --user ${FILER_STORE_USERS.backup.name} --rdb "/backups/.filer-store-$stamp.rdb"`,
        'test -s "/backups/.filer-store-$stamp.rdb"',
        'mv "/backups/.filer-store-$stamp.rdb" "/backups/filer-store-$stamp.rdb"',
        // All but the newest KEEP, which sort by when they were made.
        "ls /backups | grep '^filer-store-.*\\.rdb$' | sort -r | tail -n +$((KEEP + 1)) | while read -r old; do rm \"/backups/$old\"; done",
      ),
    ],
    env: [{ name: "KEEP", value: String(filerStore.backups.keep) }, { name: "VALKEYCLI_AUTH", valueFrom: valkeyPassword(FILER_STORE_USERS.backup.secret) }],
    securityContext: { ...restrictedContainer(), readOnlyRootFilesystem: true },
    resources: { requests: { cpu: BACKGROUND_CPU, memory: "64Mi" }, limits: { memory: "512Mi" } },
    volumeMounts: [{ name: "backups", mountPath: "/backups" }],
  };
  const cronJob: V1CronJob = {
    apiVersion: "batch/v1",
    kind: "CronJob",
    metadata: { name: componentName(BACKUP_COMPONENT), namespace: NAMESPACE, labels: labels(BACKUP_COMPONENT) },
    spec: {
      schedule: filerStore.backups.schedule,
      concurrencyPolicy: "Forbid",
      successfulJobsHistoryLimit: 1,
      failedJobsHistoryLimit: 3,
      jobTemplate: {
        spec: {
          backoffLimit: 2,
          activeDeadlineSeconds: 1800,
          template: {
            metadata: { labels: labels(BACKUP_COMPONENT) },
            spec: {
              restartPolicy: "Never",
              ...priority("component"),
              ...valkeyPod(config),
              affinity: { podAntiAffinity: { preferredDuringSchedulingIgnoredDuringExecution: [awayFrom(COMPONENTS.filerStore)] } },
              containers: [save],
              volumes: [{ name: "backups", persistentVolumeClaim: { claimName: BACKUPS } }],
            },
          },
        },
      },
    },
  };
  return [claim, cronJob];
}

/** A preference for another node than `component`'s pods'. */
const awayFrom = (component: string): V1WeightedPodAffinityTerm => ({
  weight: 100,
  podAffinityTerm: { labelSelector: { matchLabels: selectorLabels(component) }, topologyKey: "kubernetes.io/hostname" },
});

/** The components of filerStoreRestore's pods: the one that serves the backup, and the one that loads it. */
export const RESTORE_COMPONENTS = { source: "seaweedfs-filer-store-restore-source", restore: "seaweedfs-filer-store-restore" };

/** The Jobs, and the source's Service, of filerStoreRestore, by their names. */
export const RESTORE_SOURCE = componentName(RESTORE_COMPONENTS.source);
export const RESTORE = componentName(RESTORE_COMPONENTS.restore);
export const BACKUPS_LISTING = componentName("seaweedfs-filer-store-backups-listing");

/** The filer store's volume, its StatefulSet's one pod's. */
export const FILER_STORE_CLAIM = `data-${FILER_STORE}-0`;

/** What filerStoreRestore's Job prints as its last line once it has loaded the backup: `restored <keys> keys`. */
export const RESTORED = /^restored (\d+) keys$/mu;

/**
 * A Job that lists the filer store's backups, on their volume: what it prints, each
 * backup's file's name, one a line.
 */
export function backupsListing(config: InstallConfig): V1Job {
  return {
    apiVersion: "batch/v1",
    kind: "Job",
    metadata: { name: BACKUPS_LISTING, namespace: NAMESPACE, labels: labels(BACKUP_COMPONENT) },
    spec: {
      backoffLimit: 0,
      activeDeadlineSeconds: 600,
      ttlSecondsAfterFinished: 3600,
      template: {
        metadata: { labels: labels(BACKUP_COMPONENT) },
        spec: {
          restartPolicy: "Never",
          ...priority("component"),
          ...valkeyPod(config),
          containers: [{
            name: "list",
            image: imageReference(config.objectStore.bundled.filerStore.image),
            imagePullPolicy: "IfNotPresent",
            command: ["/bin/sh", "-c", "ls /backups | grep '^filer-store-.*\\.rdb$' || true"],
            securityContext: { ...restrictedContainer(), readOnlyRootFilesystem: true },
            resources: { requests: { cpu: "10m", memory: "16Mi" }, limits: { memory: "64Mi" } },
            volumeMounts: [{ name: "backups", mountPath: "/backups", readOnly: true }],
          }],
          volumes: [{ name: "backups", persistentVolumeClaim: { claimName: BACKUPS, readOnly: true } }],
        },
      },
    },
  };
}

/**
 * The filer store put back from its backup `file`, which `alasio restore-filer-store` runs
 * with the filer store stopped, as two Jobs, since the backups' volume and the filer
 * store's may be on different nodes: on the backups' node, a Valkey that loads the backup
 * and serves it, behind a Service, to the other alone (./network-policies.ts), with the
 * backup user's password; and on the filer store's, holding the lock of its volume as its
 * Valkey does (./volume-lock.ts), one that takes a snapshot of it, as the backups do, onto
 * the filer store's volume beside what is there, loads it in a Valkey of its own, checks it
 * holds as many keys as the backup, and only then deletes what was
 * there and writes the append-only file the filer store loads as it starts, then prints
 * how many keys it restored (RESTORED).
 */
export function filerStoreRestore(config: InstallConfig, file: string): KubernetesObject[] {
  const { filerStore } = config.objectStore.bundled;
  const image = imageReference(filerStore.image);
  const auth = { name: "VALKEYCLI_AUTH", valueFrom: valkeyPassword(FILER_STORE_USERS.backup.secret) };
  const container = { image, imagePullPolicy: "IfNotPresent", securityContext: { ...restrictedContainer(), readOnlyRootFilesystem: true }, env: [auth] };
  const job = (name: string, component: string, spec: V1PodSpec): V1Job => ({
    apiVersion: "batch/v1",
    kind: "Job",
    metadata: { name, namespace: NAMESPACE, labels: labels(component) },
    spec: {
      backoffLimit: 0,
      activeDeadlineSeconds: 3600,
      template: { metadata: { labels: labels(component) }, spec: { restartPolicy: "Never", ...priority("component"), ...valkeyPod(config), ...spec } },
    },
  });
  const service: V1Service = {
    apiVersion: "v1",
    kind: "Service",
    metadata: { name: RESTORE_SOURCE, namespace: NAMESPACE, labels: labels(RESTORE_COMPONENTS.source) },
    spec: { selector: selectorLabels(RESTORE_COMPONENTS.source), ports: [{ name: "valkey", port: VALKEY_PORT }] },
  };
  const source = job(RESTORE_SOURCE, RESTORE_COMPONENTS.source, {
    containers: [{
      ...container,
      name: "serve",
      // Writes nothing: it neither snapshots nor appends.
      command: ["valkey-server", "--port", String(VALKEY_PORT), "--dir", "/backups", "--dbfilename", file, "--appendonly", "no", "--save", "", "--requirepass", "$(VALKEYCLI_AUTH)"],
      ports: [{ name: "valkey", containerPort: VALKEY_PORT }],
      readinessProbe: { exec: { command: ["/bin/sh", "-c", "valkey-cli ping | grep -q PONG"] }, periodSeconds: 2 },
      resources: { requests: { cpu: "10m", memory: "64Mi" }, limits: { memory: filerStore.resources.limits["memory"] ?? "512Mi" } },
      volumeMounts: [{ name: "backups", mountPath: "/backups", readOnly: true }],
    }],
    volumes: [{ name: "backups", persistentVolumeClaim: { claimName: BACKUPS, readOnly: true } }],
  });
  const local = `valkey-cli -p ${VALKEY_PORT + 1}`;
  const restore = job(RESTORE, RESTORE_COMPONENTS.restore, {
    containers: [{
      ...container,
      name: "restore",
      // Once no Valkey runs on the volume, as one whose pod was deleted outright may still,
      // and holding its lock, so none starts there until the restore has ended.
      command: [
        "/bin/sh",
        "-c",
        VALKEY_ONE_AT_A_TIME,
        "/bin/sh",
        "-c",
        script(
          "set -eu",
          `backup() { valkey-cli -h ${RESTORE_SOURCE} "$@"; }`,
          // Waits for the backup to be loaded and served, and for its NetworkPolicy to apply, as a new pod.
          "until backup ping 2>/dev/null | grep -q PONG; do sleep 2; done",
          "expected=$(backup dbsize)",
          // The backup fetched and checked beside what the volume holds, which goes only once it is.
          "rm -f /data/restore.rdb",
          "backup --rdb /data/restore.rdb",
          "test -s /data/restore.rdb",
          `valkey-server --port ${VALKEY_PORT + 1} --bind 127.0.0.1 --dir /data --dbfilename restore.rdb --appendonly no --save "" --requirepass "$VALKEYCLI_AUTH" &`,
          "server=$!",
          // Each wait ends with the server, should it stop.
          `until ${local} ping 2>/dev/null | grep -q PONG; do kill -0 "$server"; sleep 1; done`,
          `loaded=$(${local} dbsize)`,
          '[ "$loaded" = "$expected" ] || { echo "loaded $loaded keys, and the backup has $expected" >&2; exit 1; }',
          "rm -rf /data/appendonlydir /data/dump.rdb",
          `${local} config set appendonly yes >/dev/null`,
          // The rewrite that writes the append-only file from what is loaded, done, and well.
          `until persistence=$(${local} info persistence) && echo "$persistence" | grep -q '^aof_enabled:1' && echo "$persistence" | grep -q '^aof_rewrite_in_progress:0'; do kill -0 "$server"; sleep 1; done`,
          `${local} info persistence | grep -q '^aof_last_bgrewrite_status:ok' && [ -n "$(ls /data/appendonlydir)" ] || { echo "the append-only file could not be written" >&2; exit 1; }`,
          `${local} shutdown`,
          "wait",
          "rm /data/restore.rdb",
          'echo "restored $loaded keys"',
        ),
      ],
      resources: { requests: { cpu: "10m", memory: "64Mi" }, limits: { memory: filerStore.resources.limits["memory"] ?? "512Mi" } },
      volumeMounts: [{ name: "data", mountPath: "/data" }],
    }],
    volumes: [{ name: "data", persistentVolumeClaim: { claimName: FILER_STORE_CLAIM } }],
  });
  return [service, source, restore];
}

/** The bundled object store's objects, when it runs. */
export function objectStoreObjects(config: InstallConfig): KubernetesObject[] {
  if (!config.objectStore.bundled.enabled) return [];
  const security: V1ConfigMap = {
    apiVersion: "v1",
    kind: "ConfigMap",
    metadata: { name: SECURITY, namespace: NAMESPACE, labels: stackLabels(COMPONENTS.gateway) },
    data: { "security.toml": storeSecurityToml() },
  };
  return [
    security,
    ...storeParts([...masterObjects(config), ...volumeServerObjects(config), ...filerStoreObjects(config), ...filerObjects(config), ...gatewayObjects(config)]),
    lifecyclePass(config),
    ...filerStoreBackup(config),
  ];
}
