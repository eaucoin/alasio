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
 *   gives each bucket with lifecycle rules its rules (Neon's, and the snapshots'). It is
 *   ready, and the stack's writers start, only once they are.
 *
 * And the periodic pass that applies the lifecycle rules, and copies again what has fewer
 * copies than it should; and the filer store's backups. The identities the gateways take
 * are the stack's setup's, as are the filer store's users' passwords.
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
  V1WeightedPodAffinityTerm,
} from "@kubernetes/client-node";

import { HISTORY_DAYS } from "../../../neon/control/branches.ts";
import { memoryBytes } from "../capacity.ts";
import { imageReference } from "../images.ts";
import { BACKGROUND_CPU, claimSpec, componentName, labels, NAMESPACE, neonName, oncePolicyApplies, replicasOf, restrictedContainer, scaled, script, selectorLabels, stackLabels, stackPodSpec } from "./common.ts";
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

/**
 * The days back to which Neon's time-travel recovery can put the pageserver's objects:
 * the history Neon keeps (HISTORY_DAYS), and a day more to notice a deletion that should
 * not have been.
 */
const RESTORABLE_DAYS = HISTORY_DAYS + 1;

/** The size of each of the safekeepers' full WAL segments, Postgres's own. */
const WAL_SEGMENT_BYTES = 16 * 1024 * 1024;

/**
 * The lifecycle rules of Neon's bucket. Old versions of the pageserver's objects expire
 * RESTORABLE_DAYS after they are replaced, so its time-travel recovery can put the bucket
 * back as it was at any moment of those days. The safekeepers' old versions expire after
 * a day, the least S3 allows, since each re-uploads the segment it writes, whole, as it
 * grows.
 *
 * Neon deletes none of a timeline's full WAL segments while it lives, so they expire a
 * day past RESTORABLE_DAYS. The safekeepers serve from them what they no longer hold on
 * their volumes: to a pageserver put back as it was up to RESTORABLE_DAYS ago, which
 * ingests again from what it had uploaded by then, and to a safekeeper that was away,
 * from where it stopped (one away for longer is rebuilt from its peers once its volume
 * is deleted). Otherwise the pageserver resumes from what it uploaded minutes ago, which
 * the safekeepers keep on their volumes until it has. The segment each safekeeper writes
 * is smaller than a full one, so it is not expired, however long it goes unchanged: a
 * safekeeper rebuilt from its peers copies it.
 *
 * An operator's store (objectStore.external) is given none of this: its operator gives
 * Neon's bucket these rules.
 */
const NEON_LIFECYCLE = {
  Rules: [
    {
      ID: "expire-noncurrent",
      Status: "Enabled",
      Filter: { Prefix: "pageserver/" },
      NoncurrentVersionExpiration: { NoncurrentDays: RESTORABLE_DAYS },
      AbortIncompleteMultipartUpload: { DaysAfterInitiation: 1 },
    },
    {
      ID: "expire-noncurrent-wal",
      Status: "Enabled",
      Filter: { Prefix: "safekeeper/" },
      NoncurrentVersionExpiration: { NoncurrentDays: 1 },
      AbortIncompleteMultipartUpload: { DaysAfterInitiation: 1 },
    },
    {
      ID: "expire-wal",
      Status: "Enabled",
      Filter: { And: { Prefix: "safekeeper/", ObjectSizeGreaterThan: WAL_SEGMENT_BYTES - 1 } },
      Expiration: { Days: RESTORABLE_DAYS + 1 },
    },
  ],
};

/**
 * The lifecycle rule of the snapshots bucket. A node agent puts a file larger than a
 * part in parts (src/node-agent/s3.ts), and aborts an upload that fails; one it could not
 * abort, ended mid-upload, leaves the parts it had sent, which this aborts after a day,
 * the least S3 allows. An external store is given no rule by alasio: its operator gives
 * the bucket this one.
 */
const SNAPSHOTS_LIFECYCLE = {
  Rules: [{ ID: "abort-incomplete-uploads", Status: "Enabled", Filter: { Prefix: "" }, AbortIncompleteMultipartUpload: { DaysAfterInitiation: 1 } }],
};

/** The address of the pod `ordinal` of the StatefulSet `name`, by its headless Service `service`, at `port`. */
const podAddress = (name: string, service: string, ordinal: number, port: number) => `${name}-${ordinal}.${service}.${NAMESPACE}.svc.cluster.local:${port}`;

/** The addresses of the `count` pods of the StatefulSet `name`, by `service`, at `port`, as SeaweedFS takes a list of them. */
const podAddresses = (name: string, service: string, count: number, port: number) =>
  Array.from({ length: count }, (_, ordinal) => podAddress(name, service, ordinal, port)).join(",");

/** The masters, as the other parts reach them. */
const masters = ({ objectStore }: InstallConfig) => podAddresses(MASTER, MASTER, objectStore.bundled.masters.scaling.min, 9333);

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
 * port; of a part with a volume, the `server` on it at /data.
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
    ...(server ? { volumeMounts: [{ name: "data", mountPath: "/data" }] } : {}),
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
  ];
  const container = partContainer(config, "master", args, "/cluster/status", 9333, bundled.masters.resources, "master");
  const statefulSet = partStatefulSet(config, MASTER, COMPONENTS.master, scaling, { containers: [container] }, bundled.masters.storage);
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
  const statefulSet = partStatefulSet(config, VOLUME_SERVER, COMPONENTS.volumeServer, volumeServers.scaling, { containers: [container] }, volumeServers.storage, {
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
  const waiting = (name: string, waitImage: string, line: string, env: V1EnvVar[] = []): V1Container => ({
    name: `wait-for-${name}`,
    image: waitImage,
    imagePullPolicy: "IfNotPresent",
    command: ["/bin/sh", "-c", line],
    ...(env.length > 0 ? { env } : {}),
    securityContext: restrictedContainer(),
    resources: { requests: { cpu: "10m", memory: "16Mi" }, limits: { memory: "64Mi" } },
  });
  const initContainers = [
    // Answers PONG once it has loaded its append-only file, LOADING before.
    waiting(
      "filer-store",
      imageReference(bundled.filerStore.image),
      `until valkey-cli -h ${FILER_STORE} --user ${FILER_STORE_USERS.filer.name} ping 2>/dev/null | grep -q PONG; do sleep 2; done`,
      [{ name: "VALKEYCLI_AUTH", valueFrom: valkeyPassword(FILER_STORE_USERS.filer.secret) }],
    ),
    waiting("volume-server", image, `until wget -q -O - http://${MASTER}:9333/dir/status | grep -q '"DataNodes":\\[{'; do sleep 2; done`),
  ];
  const args = ["filer", `-master=${masters(config)}`, ownAddress(FILER_PEERS), "-ip.bind=0.0.0.0", "-defaultStoreDir=/tmp"];
  const container: V1Container = {
    ...partContainer(config, "filer", args, "/healthz", 8888, filers.resources),
    env: [...POD_ENV, { name: "WEED_REDIS2_PASSWORD", valueFrom: valkeyPassword(FILER_STORE_USERS.filer.secret) }],
    volumeMounts: [{ name: "config", mountPath: "/etc/seaweedfs", readOnly: true }, { name: "tmp", mountPath: "/tmp" }],
  };
  const statefulSet = partStatefulSet(
    config,
    FILER,
    COMPONENTS.filer,
    filers.scaling,
    { initContainers, containers: [container], volumes: [{ name: "config", configMap: { name: FILER } }, { name: "tmp", emptyDir: {} }] },
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
 * SeaweedFS 4.48 waits for room (s3api_circuit_breaker.go Limit) on a sync.Cond, which an
 * upload that ends signals once it has given back its bytes, by an atomic and without the
 * Cond's lock: a request between its look and its wait misses that signal and sleeps on
 * until the next upload ends. A request woken that finds no room waits again behind those
 * waiting, while one that comes and finds room goes in at once, so one may wait as others
 * pass it; and it looks before it adds its own, so the uploads under way may go past the
 * bound by one request's. Under the load job (test/gateway-memory.test.ts) a PUT waited
 * 22 s at the most.
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
  // Each bucket's rules given as its admin: Neon's as the admin's, the snapshots' as their own
  // identity's, as nothing else may write that bucket (neon/control/secrets.ts s3Identities).
  const lifecycles = [
    { bucket: buckets.neon, rules: NEON_LIFECYCLE, as: "ADMIN" },
    { bucket: buckets.snapshots, rules: SNAPSHOTS_LIFECYCLE, as: "SNAPSHOTS" },
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
                // What its GETs hold is not budgeted (no -readerCacheSizeMB): SeaweedFS's
                // reader budget, short of room, takes back the oldest chunk held, even one a
                // GET is reading, which that GET then reads again. Each GET holds its chunk
                // and four ahead, of the filers' 4 MiB, so the box's clients' GETs at once, 86
                // with eight restores of images of their own, want 1.2 GiB, and 2 with
                // fifteen, more than a budget a gateway can afford. Unbudgeted, they hold at most SeaweedFS's 256
                // chunks in flight, 1 GiB, which its limit (2 GiB) has room for beside the
                // rest: under the load job it held 1.6 GiB at most.
              ],
              ...goMemoryLimit(bundled.gateways.resources),
              ports: [...PORTS.gateway.map(({ name, port }) => ({ name, containerPort: port })), { name: "metrics", containerPort: METRICS_PORT }],
              readinessProbe: { httpGet: { path: "/healthz", port: "s3" }, periodSeconds: 5 },
              livenessProbe: { httpGet: { path: "/healthz", port: "s3" }, initialDelaySeconds: 30, periodSeconds: 10, failureThreshold: 6 },
              securityContext: restrictedContainer(),
              resources: bundled.gateways.resources,
              volumeMounts: [{ name: "identities", mountPath: "/etc/seaweedfs", readOnly: true }],
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
              volumeMounts: [{ name: "shell-tmp", mountPath: "/tmp" }],
            },
            // Gives each bucket with lifecycle rules its rules once it exists.
            {
              name: "lifecycle",
              image: imageReference(config.neon.image),
              imagePullPolicy: "IfNotPresent",
              command: [
                "/bin/sh",
                "-c",
                script(
                  "set -e",
                  ...lifecycles.flatMap(({ bucket, rules, as }) => {
                    const aws = `AWS_ACCESS_KEY_ID="$${as}_KEY_ID" AWS_SECRET_ACCESS_KEY="$${as}_SECRET" aws s3api --endpoint-url http://127.0.0.1:8333`;
                    return [
                      `until ${aws} head-bucket --bucket ${bucket} 2>/dev/null; do sleep 2; done`,
                      `${aws} put-bucket-lifecycle-configuration --bucket ${bucket} --lifecycle-configuration '${JSON.stringify(rules)}'`,
                    ];
                  }),
                  "touch /tmp/ready",
                  "exec sleep infinity",
                ),
              ],
              env: [
                { name: "AWS_DEFAULT_REGION", value: "us-east-1" },
                { name: "HOME", value: "/tmp" },
                ...lifecycles.flatMap(({ as }) => {
                  const secret = as === "ADMIN" ? neonName("s3-admin") : componentName("snapshots");
                  return [
                    { name: `${as}_KEY_ID`, valueFrom: { secretKeyRef: { name: secret, key: "AWS_ACCESS_KEY_ID" } } },
                    { name: `${as}_SECRET`, valueFrom: { secretKeyRef: { name: secret, key: "AWS_SECRET_ACCESS_KEY" } } },
                  ];
                }),
              ],
              readinessProbe: ready,
              securityContext: restrictedContainer(),
              resources: { requests: { cpu: "10m", memory: "64Mi" }, limits: { memory: "256Mi" } },
              volumeMounts: [{ name: "lifecycle-tmp", mountPath: "/tmp" }],
            },
          ],
          volumes: [
            { name: "identities", secret: { secretName: neonName("seaweedfs") } },
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
                securityContext: restrictedContainer(),
                resources: { requests: { cpu: BACKGROUND_CPU, memory: "32Mi" }, limits: { memory: "256Mi" } },
                volumeMounts: [{ name: "tmp", mountPath: "/tmp" }],
              }],
              volumes: [{ name: "tmp", emptyDir: {} }],
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
  return [
    ...masterObjects(config),
    ...volumeServerObjects(config),
    ...filerStoreObjects(config),
    ...filerObjects(config),
    ...gatewayObjects(config),
    lifecyclePass(config),
    ...filerStoreBackup(config),
  ];
}
