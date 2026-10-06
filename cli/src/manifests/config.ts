/**
 * How alasio is installed: the settings an operator chooses, which alasio's command line
 * keeps in its config file as plain JSON, checked here: what is refused is said by its
 * key, and why. Every key may be left out for its default, and alasio's own images
 * default to this package's (../release.ts).
 *
 * A map of quantities given for resources adds to its defaults rather than replacing
 * them, so `{ "limits": { "cpu": "2" } }` keeps the default memory limit.
 */
import { isIPv4 } from "node:net";

import type { V1Affinity, V1EnvFromSource, V1Toleration } from "@kubernetes/client-node";
import { Effect, Predicate, Result, Schema, SchemaGetter, SchemaIssue } from "effect";

import { IMAGES } from "../release.ts";

/** The install configuration is not what alasio installs from; the message says which key and why. */
export class InstallConfigError extends Schema.TaggedError<InstallConfigError>()("InstallConfigError", {
  message: Schema.String,
}) {}

/** `schema`, which is `value` (as the config file would hold it) when its key is left out. */
export const defaulted = <S extends Schema.Top>(schema: S, value: S["Encoded"]) => schema.pipe(Schema.withDecodingDefaultKey(Effect.succeed(value)));

/** A string that matches `pattern`, which `message` says it must otherwise. */
export const matching = (pattern: RegExp, message: string) => Schema.String.check(Schema.isPattern(pattern, { message }));

const NAME = /^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$/u;

const Namespace = matching(NAME, "must be a namespace name");
const SecretName = matching(/^[a-z0-9]([-a-z0-9.]{0,251}[a-z0-9])?$/u, "must be a Secret's name");
/** A Secret's name, or empty for none. */
const OptionalSecretName = matching(/^(|[a-z0-9]([-a-z0-9.]{0,251}[a-z0-9])?)$/u, "must be a Secret's name, or empty");
export const NonEmpty = Schema.String.check(Schema.isMinLength(1, { message: "must not be empty" }));
export const AbsolutePath = matching(/^\//u, "must be an absolute path");
/** An http(s) URL, or empty for none. */
export const OptionalUrl = matching(/^(|https?:\/\/.+)$/u, "must be an http(s) URL, or empty");
const Id = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0, { message: "must be a user or group id" }));
const Bucket = matching(/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/u, "must be a bucket's name");
const Size = matching(/^[0-9]+(\.[0-9]+)?(Ki|Mi|Gi|Ti|Pi|k|M|G|T|P)?$/u, "must be a storage size, such as 20Gi");
const PullPolicy = Schema.Literals(["Always", "IfNotPresent", "Never"]);
/** An image's digest, or empty for an image not pinned. */
const Digest = matching(/^(|sha256:[0-9a-f]{64})$/u, "must be a sha256 digest, or empty");

/** A JSON object, which the API server checks as the object it is (a toleration, say) when alasio is applied. */
const object = <T>() => Schema.declare((value): value is T => Predicate.isObject(value), { message: "must be an object" });

/** An image: its repository, its tag (this package's version when empty), and its digest, if pinned. */
const Image = (image: { readonly repository: string; readonly tag: string; readonly digest: string }) =>
  defaulted(
    Schema.Struct({
      repository: defaulted(NonEmpty, image.repository),
      tag: defaulted(Schema.String, image.tag),
      digest: defaulted(Digest, image.digest),
    }),
    {},
  );

/** A resource quantity; one given as a number (`2` CPUs) is that number's quantity. */
const Quantity = Schema.Union([Schema.String, Schema.Finite.pipe(Schema.decodeTo(Schema.String, { decode: SchemaGetter.String(), encode: SchemaGetter.Number() }))]);
const Quantities = Schema.Record(Schema.String, Quantity);
type Quantities = typeof Quantities.Type;

/** Quantities that add to `defaults`, or replace theirs. */
const quantities = (defaults: Quantities) =>
  defaulted(
    Quantities.pipe(
      Schema.decodeTo(Schema.Record(Schema.String, Schema.String), {
        decode: SchemaGetter.transform((given) => ({ ...defaults, ...given })),
        encode: SchemaGetter.passthrough(),
      }),
    ),
    {},
  );

/** A container's resources, its requests and limits over `defaults`. */
const Resources = (defaults: { readonly requests: Quantities; readonly limits: Quantities }) =>
  defaulted(Schema.Struct({ requests: quantities(defaults.requests), limits: quantities(defaults.limits) }), {});

/** A volume: its size, and its StorageClass, the cluster's default when empty. */
const Storage = (size: string) => defaulted(Schema.Struct({ size: defaulted(Size, size), storageClassName: defaulted(Schema.String, "") }), {});

const Env = defaulted(Schema.Record(Schema.String, Schema.String), {});
const NodeSelector = defaulted(Schema.Record(Schema.String, Schema.String), {});
const Tolerations = defaulted(Schema.Array(object<V1Toleration>()), []);
const Affinity = defaulted(object<V1Affinity>(), {});
const Ids = defaulted(Schema.Array(Id), []);

/** A path of the machine the host profile mounts into alasio and each folder's bayma. */
const HostMount = Schema.Struct({
  name: matching(NAME, "must be a volume name"),
  hostPath: AbsolutePath,
  mountPath: AbsolutePath,
  readOnly: defaulted(Schema.Boolean, false),
  /** What must be at hostPath; empty checks nothing. */
  type: defaulted(Schema.Literals(["", "Directory", "DirectoryOrCreate", "File", "FileOrCreate", "Socket", "CharDevice", "BlockDevice"]), ""),
});

const Alasio = Schema.Struct({
  /** The bot's token and who may use it: a Secret with keys `token` and `allowedUserIds`. */
  telegram: Schema.Struct({ existingSecret: SecretName.annotateKey({ messageMissingKey: "is required: the Secret of the bot's token and allowed users" }) })
    .annotateKey({ messageMissingKey: "is required, with existingSecret" }),
  /** The harness a new conversation uses; empty leaves alasio's default. */
  defaultHarness: defaulted(Schema.Literals(["", "claude", "codex"]), ""),
  /** Claude Code's login: a Secret whose `key` holds a token from `claude setup-token`; none finds it in alasio's home. */
  claude: defaulted(Schema.Struct({ existingSecret: defaulted(OptionalSecretName, ""), key: defaulted(NonEmpty, "token") }), {}),
  /** alasio's home and state: a volume of its own, or an existing claim. */
  persistence: defaulted(
    Schema.Struct({ size: defaulted(Size, "20Gi"), storageClassName: defaulted(Schema.String, ""), existingClaim: defaulted(Schema.String, "") }),
    {},
  ),
  env: Env,
  envFrom: defaulted(Schema.Array(object<V1EnvFromSource>()), []),
  resources: Resources({ requests: { cpu: "500m", memory: "1Gi" }, limits: { memory: "8Gi" } }),
  /** Whom alasio runs as, unless the host profile says. */
  runAsUser: defaulted(Id, 1000),
  runAsGroup: defaulted(Id, 1000),
  supplementalGroups: Ids,
  nodeSelector: NodeSelector,
  tolerations: Tolerations,
  affinity: Affinity,
});

const Telemetry = Schema.Struct({
  /** Where alasio, its harnesses, bayma, the lake and Neon's collector export to; empty exports nothing. */
  otlpEndpoint: defaulted(OptionalUrl, ""),
  otlpProtocol: defaulted(Schema.Literals(["http/protobuf", "http/json", "grpc"]), "http/protobuf"),
  /** A Secret whose `headersKey` holds OTEL_EXPORTER_OTLP_HEADERS. */
  headersSecret: defaulted(OptionalSecretName, ""),
  headersKey: defaulted(NonEmpty, "headers"),
  resourceAttributes: defaulted(Schema.String, ""),
  /** The port alasio receives session sandboxes' telemetry on. */
  receiverPort: defaulted(Schema.Int.check(Schema.isBetween({ minimum: 1024, maximum: 65535 }, { message: "must be a port from 1024 to 65535" })), 4318),
});

const Sessions = Schema.Struct({
  enabled: defaulted(Schema.Boolean, true),
  namespace: defaulted(Namespace, "alasio-sessions"),
  createNamespace: defaulted(Schema.Boolean, true),
  /** The RuntimeClass sessions run under; empty runs them under the cluster's default runtime. */
  runtimeClassName: defaulted(Schema.String, "gvisor"),
  /** A new session's volume; its class, when empty, is workspace storage's, or the cluster's default without it. */
  storage: Storage("10Gi"),
  resources: Resources({ requests: { cpu: "250m", memory: "512Mi" }, limits: { cpu: "2", memory: "2Gi" } }),
  /** Whether a session's pod waits until NetworkPolicy confines it. */
  egressGate: defaulted(Schema.Boolean, true),
  /** The resolvers sessions with internet access use. */
  fullModeNameservers: defaulted(
    Schema.Array(Schema.String.check(Schema.makeFilter((address) => isIPv4(address) || "must be an IPv4 address"))).check(
      Schema.isMinLength(1, { message: "must list from one to three resolvers" }),
      Schema.isMaxLength(3, { message: "must list from one to three resolvers" }),
    ),
    ["1.1.1.1", "8.8.8.8"],
  ),
  /** What sessions with internet access may not reach. */
  blockedCidrs: defaulted(
    Schema.Array(matching(/^[0-9.]+\/[0-9]{1,2}$/u, "must be an IPv4 CIDR")),
    ["0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8", "169.254.0.0/16", "172.16.0.0/12", "192.168.0.0/16", "224.0.0.0/4", "240.0.0.0/4"],
  ),
  nodeSelector: NodeSelector,
  tolerations: Tolerations,
});

const Host = Schema.Struct({
  /** The host profile: folder workspaces on the machine's own files. Privileged, for a single-node cluster the operator owns. */
  enabled: defaulted(Schema.Boolean, false),
  namespace: defaulted(Namespace, "alasio-host"),
  createNamespace: defaulted(Schema.Boolean, true),
  baymaImage: Image({
    repository: "ghcr.io/eaucoin/bayma",
    tag: "0.12.1",
    digest: "sha256:186b9e9e24bdf7d031faa5906c7c49e9be7d0fa9a47ac8d33a949eae62b31eb2",
  }),
  /** The operator's user and home, which alasio and each folder's bayma run as and in. */
  uid: defaulted(Id, 1000),
  gid: defaulted(Id, 1000),
  supplementalGroups: Ids,
  home: defaulted(AbsolutePath, "/home/operator"),
  /** Where folder workspaces' bayma keeps its state, under a mount. */
  stateRoot: defaulted(AbsolutePath, "/home/operator/.alasio/bayma"),
  mounts: defaulted(Schema.Array(HostMount), []),
  /** The directory folder workspaces are created under, a path of the mounts; empty leaves alasio's default. */
  workspaceRoot: defaulted(matching(/^(|\/.*)$/u, "must be an absolute path, or empty"), ""),
  /** Whether alasio's home is the operator's rather than its volume. */
  alasioHome: defaulted(Schema.Boolean, true),
  env: Env,
  resources: Resources({ requests: { cpu: "100m", memory: "256Mi" }, limits: { memory: "8Gi" } }),
});

/** A service of Neon's with a volume. */
const NeonStored = (size: string, resources: Parameters<typeof Resources>[0]) =>
  defaulted(Schema.Struct({ storage: Storage(size), resources: Resources(resources) }), {});

/** A service of Neon's without one. */
const NeonService = (resources: Parameters<typeof Resources>[0]) => defaulted(Schema.Struct({ resources: Resources(resources) }), {});

const Neon = Schema.Struct({
  enabled: defaulted(Schema.Boolean, true),
  /** Without Neon, alasio's database: a Secret with `url` and `lake-password`. */
  external: defaulted(Schema.Struct({ existingSecret: defaulted(OptionalSecretName, "") }), {}),
  image: Image({
    repository: "neondatabase/neon",
    tag: "release-9129",
    digest: "sha256:166022a72bf9983eba96d061d794f4740edbd4c3301e66202c1180acce9a323c",
  }),
  computeImage: Image({
    repository: "neondatabase/compute-node-v17",
    tag: "release-compute-9073",
    digest: "sha256:ed6a613231d7026b4df8b00563444b9f33745370a3b3f0a2183e723f460ba974",
  }),
  controllerDbImage: Image({
    repository: "postgres",
    tag: "17-bookworm",
    digest: "sha256:639ab7ceb90e13123085b741fb31ef493fba25463002f6da665352e7b534b652",
  }),
  /** Whom Neon's services run as, owning their volumes. */
  runAsUser: defaulted(Id, 1000),
  runAsGroup: defaulted(Id, 1000),
  safekeepers: defaulted(
    Schema.Struct({
      /** How many safekeepers hold each timeline's WAL. */
      replicas: defaulted(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 7 }, { message: "must be from 1 to 7" })), 3),
      storage: Storage("20Gi"),
      resources: Resources({ requests: { cpu: "100m", memory: "256Mi" }, limits: { memory: "1Gi" } }),
    }),
    {},
  ),
  pageserver: NeonStored("50Gi", { requests: { cpu: "250m", memory: "512Mi" }, limits: { memory: "4Gi" } }),
  storageController: NeonService({ requests: { cpu: "50m", memory: "128Mi" }, limits: { memory: "512Mi" } }),
  storageBroker: NeonService({ requests: { cpu: "50m", memory: "64Mi" }, limits: { memory: "256Mi" } }),
  controllerDb: NeonStored("2Gi", { requests: { cpu: "50m", memory: "128Mi" }, limits: { memory: "512Mi" } }),
  control: NeonStored("100Mi", { requests: { cpu: "20m", memory: "64Mi" }, limits: { memory: "256Mi" } }),
  compute: NeonService({ requests: { cpu: "500m", memory: "1Gi" }, limits: { memory: "4Gi" } }),
  /**
   * A daily logical dump of alasio's database to the object store, and a copy of
   * JuiceFS's newest dump of workspace storage's metadata beside it; of each, the last
   * `keep` kept.
   */
  backup: defaulted(
    Schema.Struct({
      enabled: defaulted(Schema.Boolean, true),
      schedule: defaulted(Schema.String.check(Schema.isMinLength(9, { message: "must be a cron schedule" })), "17 3 * * *"),
      keep: defaulted(Schema.Int.check(Schema.isGreaterThanOrEqualTo(1, { message: "must be at least 1" })), 14),
    }),
    {},
  ),
  /** The stack's own metrics, scraped and sent where telemetry goes. */
  collector: defaulted(
    Schema.Struct({
      enabled: defaulted(Schema.Boolean, true),
      image: Image({
        repository: "otel/opentelemetry-collector-contrib",
        tag: "0.161.0",
        digest: "sha256:fd328de2552466ad78385e1b1289c3f2402b1c45f265b252aab1955b42845ac1",
      }),
      /** Where the collector sends to, when not telemetry.otlpEndpoint. */
      otlpEndpoint: defaulted(OptionalUrl, ""),
      resources: Resources({ requests: { cpu: "50m", memory: "128Mi" }, limits: { memory: "512Mi" } }),
    }),
    {},
  ),
  nodeSelector: NodeSelector,
  tolerations: Tolerations,
});

const ObjectStore = Schema.Struct({
  /** SeaweedFS, run beside Neon. */
  bundled: defaulted(
    Schema.Struct({
      enabled: defaulted(Schema.Boolean, true),
      image: Image({
        repository: "chrislusf/seaweedfs",
        tag: "4.47",
        digest: "sha256:ce9e796f1fe6f06968f4c04bdaf8f678dad9c8acdfef3d244133d71bfa6bf882",
      }),
      storage: Storage("100Gi"),
      /** The most volumes it keeps, each up to 1GiB. */
      volumes: defaulted(Schema.Int.check(Schema.isGreaterThanOrEqualTo(30, { message: "must be at least 30" })), 100),
      /** The disk it leaves free. */
      minFreeSpace: defaulted(matching(/^[0-9]+(KiB|MiB|GiB|TiB)$/u, "must be a size in KiB, MiB, GiB or TiB"), "5GiB"),
      resources: Resources({ requests: { cpu: "100m", memory: "256Mi" }, limits: { memory: "2Gi" } }),
    }),
    {},
  ),
  /** Or an S3-compatible store of the operator's: its endpoint, region, and a Secret with `accessKey` and `secretKey`. */
  external: defaulted(
    Schema.Struct({
      endpoint: defaulted(OptionalUrl, ""),
      region: defaulted(NonEmpty, "us-east-1"),
      existingSecret: defaulted(OptionalSecretName, ""),
    }),
    {},
  ),
  buckets: defaulted(
    Schema.Struct({
      neon: defaulted(Bucket, "neon"),
      lake: defaulted(Bucket, "lake"),
      backups: defaulted(Bucket, "backups"),
      /** Workspace storage's data; an external store must have it already, as it must the others. */
      workspaces: defaulted(Bucket, "workspaces"),
    }),
    {},
  ),
});

/** The seconds of a duration as JuiceFS takes one (1h, 1h30m, 90s), or null when it is none. */
function durationSeconds(duration: string): number | null {
  const match = /^(?:([0-9]+)h)?(?:([0-9]+)m)?(?:([0-9]+)s)?$/u.exec(duration);
  if (!match || duration === "") return null;
  return Number(match[1] ?? 0) * 3600 + Number(match[2] ?? 0) * 60 + Number(match[3] ?? 0);
}

/** A component of JuiceFS's CSI driver: its resources. */
const CsiComponent = (resources: Parameters<typeof Resources>[0]) => defaulted(Schema.Struct({ resources: Resources(resources) }), {});

/**
 * JuiceFS's CSI driver (juicedata/juicefs-csi-driver v0.33.0), which makes each session's
 * claim a directory of the file system, its size the directory's quota; off where the
 * cluster already has the driver. Its images are those that release names.
 */
const Csi = Schema.Struct({
  enabled: defaulted(Schema.Boolean, true),
  /** Where the driver and its mount pods run, as upstream installs them. */
  namespace: defaulted(Namespace, "kube-system"),
  image: Image({
    repository: "juicedata/juicefs-csi-driver",
    tag: "v0.33.0",
    digest: "sha256:f918e7331c055ec7efb5a13ff4f81def57620defb003f7379c28fa97822ff44d",
  }),
  /** The JuiceFS client mount pods run, Community Edition. */
  mountImage: Image({
    repository: "juicedata/mount",
    tag: "ce-v1.4.1",
    digest: "sha256:ab99388a397fe52575fdeb84a9e017c3c594a6b07d63966cd4805bbf6f172673",
  }),
  registrarImage: Image({
    repository: "registry.k8s.io/sig-storage/csi-node-driver-registrar",
    tag: "v2.9.0",
    digest: "sha256:cd21e19cd8bbd5bc56f1b4f1398a436e7897da2995d6d036c9729be3f4e456e6",
  }),
  resizerImage: Image({
    repository: "registry.k8s.io/sig-storage/csi-resizer",
    tag: "v1.9.0",
    digest: "sha256:f1f352df97874442624fcef23eab04aa038f66cb0e361212f6ec09e92998184d",
  }),
  livenessProbeImage: Image({
    repository: "registry.k8s.io/sig-storage/livenessprobe",
    tag: "v2.11.0",
    digest: "sha256:82adbebdf5d5a1f40f246aef8ddbee7f89dea190652aefe83336008e69f9a89f",
  }),
  controller: CsiComponent({ requests: { cpu: "20m", memory: "64Mi" }, limits: { memory: "512Mi" } }),
  node: CsiComponent({ requests: { cpu: "20m", memory: "64Mi" }, limits: { memory: "512Mi" } }),
  /** Each mount pod's: no CPU limit, which JuiceFS advises against below 4, and memory for its buffer and cache index. */
  mountPod: CsiComponent({ requests: { cpu: "20m", memory: "128Mi" }, limits: { memory: "1Gi" } }),
  /** One mount pod per node for every workspace rather than one per workspace: less memory, and a crash reaches each workspace on the node. */
  shareMountPod: defaulted(Schema.Boolean, false),
});

/** Valkey, the file system's metadata engine. */
const Valkey = Schema.Struct({
  image: Image({
    repository: "valkey/valkey",
    tag: "9.1.2-alpine",
    digest: "sha256:48332870af354a799964c0012ae1194a0bf2bf894eb508f945810596dc2d8d11",
  }),
  /** Its snapshots and append-only file. */
  storage: Storage("2Gi"),
  /** What it may hold, half its memory limit, leaving room for the fork that writes its snapshots; about a million files at 384mb. */
  maxmemory: defaulted(matching(/^[0-9]+(kb|mb|gb)$/u, "must be an amount of memory such as 384mb"), "384mb"),
  resources: Resources({ requests: { cpu: "20m", memory: "64Mi" }, limits: { memory: "768Mi" } }),
});

/**
 * Workspace storage: each new session's workspace a volume of one JuiceFS file system,
 * its metadata in a Valkey of alasio's own and its data in the object store's
 * `workspaces` bucket. Without it, new workspaces are volumes of sessions.storage's
 * class, the cluster's default unless it names one; a workspace keeps the volume it was
 * made with either way.
 */
const WorkspaceStorage = Schema.Struct({
  enabled: defaulted(Schema.Boolean, true),
  /** The file system's name, which its data is kept under in the bucket. */
  name: defaulted(matching(/^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$/u, "must be a JuiceFS file system name"), "workspaces"),
  /** The StorageClass of its volumes, which sessions' claims are of unless sessions.storage names another. */
  storageClassName: defaulted(matching(NAME, "must be a StorageClass name"), "alasio-workspaces"),
  /** How many days a deleted file is kept in the file system's trash, 0 for none; set as the file system is first made. */
  trashDays: defaulted(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0, { message: "must be 0 or more" })), 1),
  /** How often JuiceFS dumps the file system's metadata to its bucket's meta/: a duration of 5m or more, the least it takes. */
  backupInterval: defaulted(
    Schema.String.check(Schema.makeFilter((duration) => (durationSeconds(duration) ?? 0) >= 300 || "must be a duration of 5m or more, such as 1h")),
    "1h",
  ),
  /** Each mount's read cache on its node's disk, in MiB. */
  cacheSizeMiB: defaulted(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0, { message: "must be 0 or more" })), 1024),
  csi: defaulted(Csi, {}),
  valkey: defaulted(Valkey, {}),
});

/** agent-sandbox's controller, installed once per cluster; off where the cluster already has it. */
const AgentSandbox = Schema.Struct({
  enabled: defaulted(Schema.Boolean, true),
  image: defaulted(
    Schema.Struct({
      repository: defaulted(NonEmpty, "registry.k8s.io/agent-sandbox/agent-sandbox-controller"),
      tag: defaulted(Schema.String, "v1.0.5"),
      digest: defaulted(Digest, "sha256:28a9cbdbfd6ac0a4e5c7e9261ace1aa30ee2da681cb640dccdfed98e8dd9d98b"),
      pullPolicy: defaulted(PullPolicy, "IfNotPresent"),
    }),
    {},
  ),
  resources: Resources({ requests: { cpu: "50m", memory: "64Mi" }, limits: { memory: "256Mi" } }),
  nodeSelector: NodeSelector,
  tolerations: Tolerations,
  affinity: Affinity,
});

/** What a configuration must also hold, given what it turns off: the issue of the first it lacks. */
function completeness(config: typeof InstallStruct.Type): { readonly path: ReadonlyArray<string>; readonly issue: string } | undefined {
  if (!config.neon.enabled && !config.neon.external.existingSecret) {
    return { path: ["neon", "external", "existingSecret"], issue: "is required when neon.enabled is false" };
  }
  if (config.neon.enabled && !config.objectStore.bundled.enabled) {
    if (!config.objectStore.external.endpoint) return { path: ["objectStore", "external", "endpoint"], issue: "is required when objectStore.bundled.enabled is false" };
    if (!config.objectStore.external.existingSecret) return { path: ["objectStore", "external", "existingSecret"], issue: "is required with an external object store" };
  }
  if (config.workspaceStorage.enabled && !config.sessions.enabled) {
    return { path: ["workspaceStorage", "enabled"], issue: "must be false when sessions.enabled is false, as its volumes are sessions' workspaces" };
  }
  if (config.workspaceStorage.enabled && !config.neon.enabled) {
    return { path: ["workspaceStorage", "enabled"], issue: "must be false when neon.enabled is false, as its data is in the object store that runs with Neon" };
  }
  return undefined;
}

const InstallStruct = Schema.Struct({
  images: defaulted(
    Schema.Struct({ alasio: Image(IMAGES.alasio), agent: Image(IMAGES.agent), lake: Image(IMAGES.lake), pullPolicy: defaulted(PullPolicy, "IfNotPresent") }),
    {},
  ),
  /** Pull Secrets for private registries, in alasio's namespace and the sessions and host ones. */
  imagePullSecrets: defaulted(Schema.Array(SecretName), []),
  /** alasio itself; its telegram.existingSecret has no default, so every configuration says it. */
  alasio: Alasio.annotateKey({ messageMissingKey: "is required, with telegram.existingSecret" }),
  telemetry: defaulted(Telemetry, {}),
  sessions: defaulted(Sessions, {}),
  host: defaulted(Host, {}),
  neon: defaulted(Neon, {}),
  objectStore: defaulted(ObjectStore, {}),
  workspaceStorage: defaulted(WorkspaceStorage, {}),
  /** The analytics lake, loaded from Neon into DuckLake. */
  lake: defaulted(
    Schema.Struct({ enabled: defaulted(Schema.Boolean, true), resources: Resources({ requests: { cpu: "100m", memory: "512Mi" }, limits: { memory: "1536Mi" } }) }),
    {},
  ),
  /** NetworkPolicies confining sessions, folder workspaces' bayma and Neon. */
  networkPolicies: defaulted(Schema.Struct({ enabled: defaulted(Schema.Boolean, true) }), {}),
  agentSandbox: defaulted(AgentSandbox, {}),
});

/** The install configuration: what the config file holds, every key left out its default. */
export const InstallConfig = InstallStruct.check(Schema.makeFilter(completeness));

export type InstallConfig = typeof InstallConfig.Type;
/** What the config file holds. */
export type InstallConfigFile = typeof InstallConfig.Encoded;
/** An image of the configuration's. */
export type Image = InstallConfig["images"]["alasio"];

const formatIssue = SchemaIssue.makeFormatterStandardSchemaV1();

/** The first thing wrong with a configuration: the key's path, then what it must be. */
export function describeIssue(issue: SchemaIssue.Issue): string {
  const [first] = formatIssue(issue).issues;
  const path = first?.path?.map((segment) => String(Predicate.hasProperty(segment, "key") ? segment.key : segment)).join(".") ?? "";
  return [path, first?.message ?? "is not what alasio installs from"].filter(Boolean).join(" ");
}

/** The configuration in `input`, parsed JSON, checked, with its defaults: a key alasio does not know is refused. */
export const decodeInstallConfig = (input: unknown): Result.Result<InstallConfig, InstallConfigError> =>
  Result.mapError(
    Schema.decodeUnknownResult(InstallConfig)(input, { onExcessProperty: "error" }),
    (error) => new InstallConfigError({ message: describeIssue(error.issue) }),
  );
