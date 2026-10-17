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

import { memoryBytes, memoryText } from "../capacity.ts";
import { NODE_AGENT_MEMORY_FLOOR } from "../../../src/node-agent/names.ts";
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

/** How many replicas a component runs: from `min` to `max`. */
export interface Scale {
  readonly min: number;
  readonly max: number;
}

const Count = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0, { message: "must be a number of replicas, 0 or more" }));

/** What a component's scale may be beyond `min <= max`, as its component allows. */
interface ScaleLimits {
  /** Whether it is of the floor, what never sleeps, so its `min` is at least 1. */
  readonly floor: boolean;
  /** Whether a scaler moves it between `min` and `max`; without one, they are equal. */
  readonly scaled: boolean;
  /** The most replicas it runs, if any. */
  readonly most?: number;
}

/** What is wrong with `scale`, given `limits`: the key, and why; none when nothing is. */
function scaleProblem({ min, max }: Scale, { floor, scaled, most }: ScaleLimits): { readonly path: ReadonlyArray<string>; readonly issue: string } | undefined {
  if (min > max) return { path: ["max"], issue: "must be at least min" };
  if (floor && min === 0) return { path: ["min"], issue: "must be at least 1: it never sleeps" };
  if (!scaled && min !== max) return { path: ["max"], issue: "must equal min: nothing scales it between them" };
  if (most !== undefined && max > most) return { path: ["max"], issue: `must be at most ${most}` };
  return undefined;
}

/**
 * A component's scale, `scaling`, as every component declares it: `{ min, max }`, how
 * many replicas it runs. Equal, they fix the count; apart, the component's scaler moves
 * it between them; a `min` of 0 lets it sleep. One left out is its default, moved as far
 * as the other given needs: `{ min: 2 }` of a scale of 1 is 2 to 2.
 */
const Scaling = (defaults: Scale, limits: ScaleLimits) =>
  defaulted(
    Schema.Struct({ min: Schema.optionalKey(Count), max: Schema.optionalKey(Count) }).pipe(
      Schema.decodeTo(Schema.Struct({ min: Count, max: Count }).check(Schema.makeFilter((scale) => scaleProblem(scale, limits))), {
        decode: SchemaGetter.transform(({ min, max }) => ({
          min: min ?? Math.min(defaults.min, max ?? defaults.min),
          max: max ?? Math.max(defaults.max, min ?? defaults.max),
        })),
        encode: SchemaGetter.transform((scale) => scale),
      }),
    ),
    {},
  );

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
  env: Env,
  envFrom: defaulted(Schema.Array(object<V1EnvFromSource>()), []),
  resources: Resources({ requests: { cpu: "250m", memory: "768Mi" }, limits: { memory: "8Gi" } }),
  /** Whom alasio runs as, unless the host profile says. */
  runAsUser: defaulted(Id, 1000),
  runAsGroup: defaulted(Id, 1000),
  supplementalGroups: Ids,
  nodeSelector: NodeSelector,
  tolerations: Tolerations,
  affinity: Affinity,
});

/**
 * alasio's telemetry: everything (alasio's, its harnesses', bayma's, the lake's and the
 * stack's) goes through the stack's collector, by the log, to the lake, and to
 * otlpEndpoint too when one is set.
 */
const Telemetry = Schema.Struct({
  /** Where the collector also sends all it receives; empty sends it to the lake alone. */
  otlpEndpoint: defaulted(OptionalUrl, ""),
  otlpProtocol: defaulted(Schema.Literals(["http/protobuf", "http/json", "grpc"]), "http/protobuf"),
  /** A Secret whose `headersKey` holds the headers otlpEndpoint is sent, in OTEL_EXPORTER_OTLP_HEADERS's form. */
  headersSecret: defaulted(OptionalSecretName, ""),
  headersKey: defaulted(NonEmpty, "headers"),
  resourceAttributes: defaulted(Schema.String, ""),
  /** The port alasio receives session sandboxes' telemetry on. */
  receiverPort: defaulted(Schema.Int.check(Schema.isBetween({ minimum: 1024, maximum: 65535 }, { message: "must be a port from 1024 to 65535" })), 4318),
  /** How many days of telemetry the lake keeps. */
  retentionDays: defaulted(Schema.Int.check(Schema.isGreaterThanOrEqualTo(1, { message: "must be at least 1" })), 30),
  /**
   * The stack's collector, through which all of it goes, in two parts on one image
   * (images.collector): its gateway, which every producer exports to and which writes it to
   * the log, scaling on its CPU where min and max are apart; and its scraper, one replica,
   * which scrapes the stack and exports what it scrapes to the gateway.
   */
  collector: defaulted(
    Schema.Struct({
      gateway: defaulted(
        Schema.Struct({
          scaling: Scaling({ min: 1, max: 1 }, { floor: true, scaled: true }),
          resources: Resources({ requests: { cpu: "50m", memory: "96Mi" }, limits: { memory: "512Mi" } }),
        }),
        {},
      ),
      scraper: defaulted(Schema.Struct({ resources: Resources({ requests: { cpu: "50m", memory: "128Mi" }, limits: { memory: "512Mi" } }) }), {}),
    }),
    {},
  ),
});

/**
 * Sessions' whole-sandbox snapshots, taken where they run under gVisor
 * (sessions.runtimeClassName), by the node agent on each node sessions run on.
 */
const Snapshots = Schema.Struct({
  /** How long a session's bayma runs no exec before its sandbox is snapshotted and its pod let go: a duration such as 5m. */
  releaseAfter: defaulted(Schema.String.check(Schema.makeFilter((duration) => (durationSeconds(duration) ?? 0) > 0 || "must be a duration such as 5m")), "5m"),
  /** The most of snapshots each node keeps on its disk, for restores there. */
  nodeCache: defaulted(Size, "5Gi"),
  /**
   * The node agent, which takes snapshots on its node and prepares them there. Its memory
   * request is its own, as measured (NODE_AGENT_OWN_BYTES, src/node-agent/names.ts); its
   * limit is at least that and its budget of what it holds written and not yet on the disk
   * (NODE_AGENT_MEMORY_FLOOR, 192 MiB), and leaves room over both for the page cache it
   * reads through, which the kernel reclaims under it.
   */
  nodeAgent: defaulted(Schema.Struct({ resources: Resources({ requests: { cpu: "20m", memory: "160Mi" }, limits: { memory: "512Mi" } }) }), {}),
});

/**
 * The webhook intake (./webhook.ts), the one thing of alasio's the internet reaches,
 * which Telegram delivers each environment's bot's updates to, on `port` of
 * `publicAddress`, over HTTPS with a certificate of its own that alasio uploads to
 * Telegram as it registers each bot. How it is reached its target makes (`exposure`,
 * alasio's own to say): on this machine's address, from k3s on it, which then needs
 * `publicAddress` to be that address, and binds it alone, leaving the machine's other
 * addresses' port to whatever has it; on its node's, in Docker, whose port the machine
 * maps; or through a LoadBalancer Service. `publicAddress` is the address Telegram is told,
 * which its certificate is made for: the machine's, or the LoadBalancer's, or a name of
 * either. On a machine's or a node's address it runs one replica. `sources` are the
 * networks it takes its port from on this machine's address or a LoadBalancer's, its
 * NetworkPolicy's and the LoadBalancer's: Telegram's, as its Bot API's guide to webhooks
 * lists them (core.telegram.org/bots/webhooks), which Telegram may change; a change shows
 * in each bot's getWebhookInfo as a last error, which alasio logs and alasio status says,
 * each pointing here, where it is set.
 */
const Webhook = Schema.Struct({
  publicAddress: defaulted(matching(/^(|(\d{1,3}\.){3}\d{1,3}|[a-z0-9]([-a-z0-9.]{0,251}[a-z0-9])?)$/u, "must be an IPv4 address or a host name, or empty"), ""),
  /** One of the ports Telegram delivers to. */
  port: defaulted(Schema.Literals([443, 80, 88, 8443]), 443),
  exposure: defaulted(Schema.Literals(["hostAddress", "node", "loadBalancer"]), "node"),
  sources: defaulted(
    Schema.Array(matching(/^[0-9.]+\/[0-9]{1,2}$/u, "must be an IPv4 CIDR")).check(Schema.isMinLength(1, { message: "must list a network at least" })),
    ["149.154.160.0/20", "91.108.4.0/22"],
  ),
  scaling: Scaling({ min: 1, max: 1 }, { floor: true, scaled: false }),
  resources: Resources({ requests: { cpu: "10m", memory: "48Mi" }, limits: { memory: "192Mi" } }),
});

/**
 * The least memory the log's broker is given: the heap of 512 MiB it is run at (./log.ts),
 * and the JVM's own beside it.
 */
export const LOG_MEMORY = "768Mi";

/** A segment of the log's partitions at most: what its retention deletes at once, a closed one. */
export const LOG_SEGMENT_BYTES = 64 * 1024 * 1024;

/** How many partitions the log's telemetry topics have together: three topics, of as many as the lake's intake runs replicas at most. */
const telemetryPartitions = (config: Pick<InstallConfig, "lake">): number => 3 * Math.max(1, config.lake.intake.scaling.max);

/**
 * How many bytes each partition of the log's telemetry topics keeps at most: three
 * quarters of a node's volume shared among them, the rest Telegram's topics' and KRaft's,
 * less the segment each writes beside those it keeps, as retention deletes closed segments
 * alone.
 */
/** How much longer than the filer store's backups go back the log keeps its records: the day a restore and the lake's reconciliation take, and more. */
export const LOG_RETENTION_MARGIN_HOURS = 24;

/**
 * How far back the filer store's backups go, in hours: `keep` of them, taken on `schedule`
 * at a fixed minute of every hour, of every Nth hour, or of a fixed hour each day; null for a
 * schedule of another form, which is not known so.
 */
export function filerStoreBackupHours({ schedule, keep }: { readonly schedule: string; readonly keep: number }): number | null {
  const [minute = "", hour = "", ...rest] = schedule.trim().split(/\s+/u);
  if (rest.length !== 3 || rest.some((field) => field !== "*") || !/^[0-9]+$/u.test(minute)) return null;
  const every = hour === "*" ? 1 : /^\*\/([1-9][0-9]*)$/u.exec(hour)?.[1];
  if (every !== undefined) return Number(every) * keep;
  return /^[0-9]+$/u.test(hour) ? 24 * keep : null;
}

export function telemetryRetentionBytes(config: Pick<InstallConfig, "log" | "lake">): number {
  return Math.floor((memoryBytes(config.log.storage.size) * 3) / 4 / telemetryPartitions(config)) - LOG_SEGMENT_BYTES;
}

/**
 * The log (./log.ts), beside Neon: Apache Kafka in KRaft, its partitions' segments on a
 * volume of each of its nodes, which KRaft's metadata, its principals and ACLs among it,
 * is on too. Its nodes are brokers and KRaft's controllers both, 1, or 3 in a quorum, their
 * count set as the log is made and never changed after, as KRaft's voters are fixed. It is
 * a buffer, not a store: what it holds is kept for `retention`, within its volume, the lake
 * keeping what is read from it. That is also what the lake's telemetry is refilled from where
 * the lake loses files, as a restore of the filer store's oldest backup loses what was written
 * since (filerStoreBackupHours): so it keeps them, and a day more, LOG_RETENTION_MARGIN_HOURS.
 */
const Log = Schema.Struct({
  scaling: Scaling({ min: 1, max: 1 }, { floor: true, scaled: false, most: 3 }),
  retention: defaulted(matching(/^[1-9][0-9]*h$/u, "must be a number of hours, such as 24h"), "72h"),
  storage: Storage("4Gi"),
  resources: Resources({ requests: { cpu: "100m", memory: "512Mi" }, limits: { memory: "1Gi" } }),
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
  snapshots: defaulted(Snapshots, {}),
  /**
   * The sandbox pools' free pods, of each of a session's internets (cli/src/manifests/
   * sandbox-gateway.ts): `min` kept running before any is bound, more made, to `max`, for
   * the binds that wait for one.
   */
  pools: Scaling({ min: 1, max: 10 }, { floor: false, scaled: true }),
  /** The sandbox gateway, which binds a session's workspace to a pool's pod. */
  gateway: defaulted(Schema.Struct({ resources: Resources({ requests: { cpu: "20m", memory: "96Mi" }, limits: { memory: "256Mi" } }) }), {}),
  /** The session telemetry receiver, which bound pods' bayma exports to. */
  receiver: defaulted(Schema.Struct({ resources: Resources({ requests: { cpu: "20m", memory: "96Mi" }, limits: { memory: "256Mi" } }) }), {}),
});

const Host = Schema.Struct({
  /** The host profile: folder workspaces on the machine's own files. Privileged, for a single-node cluster the operator owns. */
  enabled: defaulted(Schema.Boolean, false),
  namespace: defaulted(Namespace, "alasio-host"),
  createNamespace: defaulted(Schema.Boolean, true),
  baymaImage: Image({
    repository: "ghcr.io/eaucoin/bayma",
    tag: "0.13.0",
    digest: "sha256:840bfbbb660c5a0fefb562448db2ee93ceef3715142e538c7717ef9c34f3acc1",
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
  resources: Resources({ requests: { cpu: "100m", memory: "512Mi" }, limits: { memory: "8Gi" } }),
});

/** A service of Neon's with a volume. */
const NeonStored = (size: string, resources: Parameters<typeof Resources>[0]) =>
  defaulted(Schema.Struct({ storage: Storage(size), resources: Resources(resources) }), {});

/** A service of Neon's without one. */
const NeonService = (resources: Parameters<typeof Resources>[0]) => defaulted(Schema.Struct({ resources: Resources(resources) }), {});

const Neon = Schema.Struct({
  enabled: defaulted(Schema.Boolean, true),
  /** Without Neon, alasio's database: a Secret with `url` and `lake-verifier`, the SCRAM verifier of the lake role's password. */
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
      /** How many safekeepers hold each timeline's WAL: a count, which neon-control registers them by. */
      scaling: Scaling({ min: 3, max: 3 }, { floor: true, scaled: false, most: 7 }),
      storage: Storage("20Gi"),
      resources: Resources({ requests: { cpu: "50m", memory: "64Mi" }, limits: { memory: "1Gi" } }),
    }),
    {},
  ),
  pageserver: defaulted(
    Schema.Struct({
      /**
       * How many pageservers serve the tenant's shards: a count, each registering itself
       * with the storage controller, which keeps it; lowered, alasio up refuses, saying so.
       */
      scaling: Scaling({ min: 1, max: 1 }, { floor: true, scaled: false }),
      storage: Storage("50Gi"),
      resources: Resources({ requests: { cpu: "100m", memory: "256Mi" }, limits: { memory: "4Gi" } }),
    }),
    {},
  ),
  storageController: NeonService({ requests: { cpu: "20m", memory: "64Mi" }, limits: { memory: "512Mi" } }),
  storageBroker: NeonService({ requests: { cpu: "20m", memory: "32Mi" }, limits: { memory: "256Mi" } }),
  controllerDb: NeonStored("2Gi", { requests: { cpu: "50m", memory: "64Mi" }, limits: { memory: "512Mi" } }),
  /** neon-control, alasio's stand-in for Neon's control plane: a count, its replicas sharing its record of the branches. */
  control: defaulted(
    Schema.Struct({
      scaling: Scaling({ min: 1, max: 1 }, { floor: true, scaled: false }),
      resources: Resources({ requests: { cpu: "20m", memory: "96Mi" }, limits: { memory: "256Mi" } }),
    }),
    {},
  ),
  /** Neon's proxy, which every client of the computes connects through, and which wakes them: a count. */
  proxy: defaulted(
    Schema.Struct({
      scaling: Scaling({ min: 1, max: 1 }, { floor: true, scaled: false }),
      resources: Resources({ requests: { cpu: "20m", memory: "64Mi" }, limits: { memory: "256Mi" } }),
    }),
    {},
  ),
  compute: defaulted(
    Schema.Struct({
      /** How long the compute may have no client session before it sleeps, until the next connection wakes it: a duration of 1m or more. */
      suspendAfter: defaulted(
        Schema.String.check(Schema.makeFilter((duration) => (durationSeconds(duration) ?? 0) >= 60 || "must be a duration of 1m or more, such as 5m")),
        "5m",
      ),
      resources: Resources({ requests: { cpu: "250m", memory: "384Mi" }, limits: { memory: "4Gi" } }),
    }),
    {},
  ),
  /**
   * How many shards the storage controller splits alasio's tenant into, across the
   * pageservers: a power of two, at most 16. A split is one-way, as Neon merges no shards:
   * raised, the tenant is split; lowered, alasio up refuses, saying so.
   */
  shards: defaulted(
    Schema.Int.check(Schema.makeFilter((shards) => (shards >= 1 && shards <= 16 && (shards & (shards - 1)) === 0) || "must be a power of two from 1 to 16")),
    1,
  ),
  /** Each branch's read replica, which its lake loads from, beside its primary: smaller, as it serves the loader alone. */
  readReplica: defaulted(
    Schema.Struct({ resources: Resources({ requests: { cpu: "100m", memory: "192Mi" }, limits: { memory: "1Gi" } }) }),
    {},
  ),
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
  nodeSelector: NodeSelector,
  tolerations: Tolerations,
});

/** The Valkey of workspace storage's metadata and of the object store's filers. */
const VALKEY_IMAGE = { repository: "valkey/valkey", tag: "9.1.2-alpine", digest: "sha256:48332870af354a799964c0012ae1194a0bf2bf894eb508f945810596dc2d8d11" };

/** What an entry of the filer store, an object or a directory, takes in its Valkey: 1,339 bytes as measured, rounded up. */
const FILER_STORE_ENTRY_BYTES = 1400;

/** The filer store's `maxmemory` for `entries`: what they take and a quarter more, in whole 64 MiB. */
export function filerStoreMaxmemoryBytes(entries: number): number {
  const unit = 64 * 2 ** 20;
  return Math.ceil((entries * FILER_STORE_ENTRY_BYTES * 1.25) / unit) * unit;
}

const ObjectStore = Schema.Struct({
  /**
   * SeaweedFS, run beside Neon, as its parts: masters, which place objects on volume
   * servers, which keep them; filers, which keep the store's namespace in a Valkey of its
   * own, the filer store, so every filer sees what any has written; and S3 gateways, on the
   * filers. Each takes its scale; the gateways', apart, scale on their CPU.
   */
  bundled: defaulted(
    Schema.Struct({
      enabled: defaulted(Schema.Boolean, true),
      image: Image({
        repository: "chrislusf/seaweedfs",
        tag: "4.48",
        digest: "sha256:4e61d15fd35994cb1e43e1e553dff106794841fd9a99ade2fc8c8bfce4d7872d",
      }),
      /**
       * How many copies of each object are kept, as SeaweedFS's replication says it: 000,
       * one; 00N, N more, each on another volume server, so N + 1 of them at least. A copy
       * outlives a disk's loss only on another disk: volume servers whose volumes are on one
       * disk, as local-path's are on one node's, lose every copy with it, and need room
       * there for all of them. Where copies are kept, each volume server is on a node of
       * its own while there are as many, one left unplaced rather than beside another, and
       * what a lost server held is copied again, each hour, onto the others. Changed, it
       * holds for what is written from then on at once, and for what was written before
       * from the next hour's lifecycle pass, which sets each volume's own to it and copies
       * each that lacks copies; until then, those are kept as they were.
       */
      replication: defaulted(matching(/^00[0-9]$/u, "must be 000, or 00N for N more copies, each on another volume server"), "000"),
      /**
       * The masters, 1, or 3 in Raft, each with a volume of its state; their count is set
       * as the store is made, and never changed after: fewer would lose what their Raft
       * keeps, and one master's state, made without peers, is no Raft's that more could join.
       */
      masters: defaulted(
        Schema.Struct({
          scaling: Scaling({ min: 1, max: 1 }, { floor: true, scaled: false, most: 3 }),
          storage: Storage("1Gi"),
          resources: Resources({ requests: { cpu: "10m", memory: "64Mi" }, limits: { memory: "512Mi" } }),
        }),
        {},
      ),
      /**
       * The volume servers, each keeping objects on a volume of its own. Their count is
       * lowered only once those it would remove hold no volumes, moved to the others first.
       */
      volumeServers: defaulted(
        Schema.Struct({
          scaling: Scaling({ min: 1, max: 1 }, { floor: true, scaled: false }),
          storage: Storage("100Gi"),
          /** The most volumes each keeps, each up to 1GiB. */
          volumes: defaulted(Schema.Int.check(Schema.isGreaterThanOrEqualTo(30, { message: "must be at least 30" })), 100),
          /**
           * The disk each leaves free. Once its disk has less free, a volume server takes no
           * more writes; once none takes any, nor does the store, as each of its filers'
           * writes is logged to a volume: `alasio status`, the Stack health dashboard and an
           * alert say how near each is.
           */
          minFreeSpace: defaulted(matching(/^[0-9]+(KiB|MiB|GiB|TiB)$/u, "must be a size in KiB, MiB, GiB or TiB"), "5GiB"),
          resources: Resources({ requests: { cpu: "50m", memory: "256Mi" }, limits: { memory: "2Gi" } }),
        }),
        {},
      ),
      /** The filers, started one after another. */
      filers: defaulted(
        Schema.Struct({
          scaling: Scaling({ min: 1, max: 1 }, { floor: true, scaled: false }),
          resources: Resources({ requests: { cpu: "20m", memory: "128Mi" }, limits: { memory: "1Gi" } }),
        }),
        {},
      ),
      /** The S3 gateways, the store's endpoint. */
      gateways: defaulted(
        Schema.Struct({
          scaling: Scaling({ min: 1, max: 1 }, { floor: true, scaled: true }),
          resources: Resources({ requests: { cpu: "20m", memory: "96Mi" }, limits: { memory: "1Gi" } }),
        }),
        {},
      ),
      /**
       * The filers' store: a Valkey of its own, its memory sized for `entries`, the store's
       * objects and directories, as about 1.4 KB each, with a quarter more. Full, it
       * refuses writes, and the store with it: an alert says when it nears that.
       */
      filerStore: defaulted(
        Schema.Struct({
          image: Image(VALKEY_IMAGE),
          storage: Storage("2Gi"),
          entries: defaulted(Schema.Int.check(Schema.isGreaterThanOrEqualTo(10_000, { message: "must be at least 10000" })), 150_000),
          resources: Resources({ requests: { cpu: "10m", memory: "32Mi" }, limits: { memory: "512Mi" } }),
          /**
           * A snapshot of the filer store on `schedule`, hourly, onto a volume of its own,
           * the last `keep` kept. One put back rolls the store's namespace back to when it
           * was taken, so what was written since is lost, as an hour's at most. On a
           * cluster of several nodes the volume is made on another node than the filer
           * store's where one can take it, so a node's loss takes one of the two alone; on
           * a single node, its disk's loss takes both.
           */
          backups: defaulted(
            Schema.Struct({
              enabled: defaulted(Schema.Boolean, true),
              schedule: defaulted(Schema.String.check(Schema.isMinLength(9, { message: "must be a cron schedule" })), "23 * * * *"),
              keep: defaulted(Schema.Int.check(Schema.isGreaterThanOrEqualTo(1, { message: "must be at least 1" })), 48),
              storage: Storage("10Gi"),
            }),
            {},
          ),
        }),
        {},
      ),
    }),
    {},
  ),
  /**
   * Or an S3-compatible store of the operator's: its endpoint, region, and a Secret with
   * `accessKey` and `secretKey`; and, where sessions are snapshotted, a Secret of the same
   * keys for a key of the store's that reaches the snapshots bucket alone, which only the
   * node agents hold, as what writes that bucket writes what they restore as root: the
   * first key may not write it, and Neon's setup, which tries, refuses one that can.
   */
  external: defaulted(
    Schema.Struct({
      endpoint: defaulted(OptionalUrl, ""),
      region: defaulted(NonEmpty, "us-east-1"),
      existingSecret: defaulted(OptionalSecretName, ""),
      snapshotsSecret: defaulted(OptionalSecretName, ""),
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
      /** Session sandboxes' snapshots, when sessions run under gVisor (sessions.runtimeClassName); an external store must have it then, as it must the others. */
      snapshots: defaulted(Bucket, "snapshots"),
    }),
    {},
  ),
});

/** The seconds of a duration, as alasio's settings and JuiceFS take one (1h, 1h30m, 90s), or null when it is none. */
export function durationSeconds(duration: string): number | null {
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
  image: Image(VALKEY_IMAGE),
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
  /** Each node's mount of the file system, which the node agent attaches session workspaces from (./sandbox-gateway.ts). */
  mount: defaulted(Schema.Struct({ resources: Resources({ requests: { cpu: "50m", memory: "128Mi" }, limits: { memory: "1Gi" } }) }), {}),
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
  resources: Resources({ requests: { cpu: "20m", memory: "32Mi" }, limits: { memory: "256Mi" } }),
  nodeSelector: NodeSelector,
  tolerations: Tolerations,
  affinity: Affinity,
});

/** A container of a controller alasio installs: its pinned image and its resources. */
const ControllerContainer = (image: Parameters<typeof Image>[0], resources: Parameters<typeof Resources>[0]) =>
  defaulted(Schema.Struct({ image: Image(image), resources: Resources(resources) }), {});

/**
 * How alasio's components scale: KEDA, which moves a component's replicas between its
 * scale's min and max on what its ScaledObject names; VPA, which sizes every workload's
 * requests from what it uses; each installed once per cluster, and off where the cluster
 * has its own already, which alasio then uses; and headroom for a node autoscaler.
 */
const Autoscaling = Schema.Struct({
  /**
   * KEDA (kedacore/keda v2.21.0), as its release's manifest installs it, but that it reads
   * Secrets in its own namespace alone, and of what ScaledObjects scale, Deployments and
   * StatefulSets, and runs no ScaledJob's Jobs (./keda.ts): a cluster whose ScaledObjects
   * scale custom resources, or authenticate through TriggerAuthentications in their own
   * namespaces, or whose ScaledJobs run, brings a KEDA of its own, which enabled false uses.
   */
  keda: defaulted(
    Schema.Struct({
      enabled: defaulted(Schema.Boolean, true),
      operator: ControllerContainer(
        { repository: "ghcr.io/kedacore/keda", tag: "2.21.0", digest: "sha256:81fe6547ce8d1cc29273f887b76507e9b853e5bc1f875ec23fe60061a38ed809" },
        { requests: { cpu: "20m", memory: "64Mi" }, limits: { memory: "512Mi" } },
      ),
      metricsApiServer: ControllerContainer(
        { repository: "ghcr.io/kedacore/keda-metrics-apiserver", tag: "2.21.0", digest: "sha256:255375037fe592732eeb554743aae360c2da88cf0b9b1258d0a987ef885f0881" },
        { requests: { cpu: "10m", memory: "48Mi" }, limits: { memory: "256Mi" } },
      ),
      admissionWebhooks: ControllerContainer(
        { repository: "ghcr.io/kedacore/keda-admission-webhooks", tag: "2.21.0", digest: "sha256:e1969628cca6123e32eea13c47c75817091af2657fa68d9d0d58a8620d1b75b1" },
        { requests: { cpu: "10m", memory: "32Mi" }, limits: { memory: "256Mi" } },
      ),
    }),
    {},
  ),
  /** VPA (kubernetes/autoscaler vertical-pod-autoscaler-1.8.0), as its deploy/ installs it, with its chart's certificate Job. */
  vpa: defaulted(
    Schema.Struct({
      enabled: defaulted(Schema.Boolean, true),
      recommender: ControllerContainer(
        { repository: "registry.k8s.io/autoscaling/vpa-recommender", tag: "1.8.0", digest: "sha256:e743e3a7e58a614f850063dba5df88eec5a19cf4cc43edb2050248df2d7dd8d3" },
        { requests: { cpu: "20m", memory: "64Mi" }, limits: { memory: "512Mi" } },
      ),
      updater: ControllerContainer(
        { repository: "registry.k8s.io/autoscaling/vpa-updater", tag: "1.8.0", digest: "sha256:1d0229e52f90f645f1ee2e86ddfac8f451d166777c2aab066b9c5ab5e8bc5d5c" },
        { requests: { cpu: "10m", memory: "32Mi" }, limits: { memory: "256Mi" } },
      ),
      admissionController: ControllerContainer(
        { repository: "registry.k8s.io/autoscaling/vpa-admission-controller", tag: "1.8.0", digest: "sha256:3d94f53e4c5a6891b8b32af545dda975e45842486432b69475afaf8df278d23d" },
        { requests: { cpu: "10m", memory: "32Mi" }, limits: { memory: "256Mi" } },
      ),
      /** What makes the admission controller's certificate, and gives its webhook the CA. */
      certgenImage: Image({
        repository: "registry.k8s.io/ingress-nginx/kube-webhook-certgen",
        tag: "v1.6.9",
        digest: "sha256:01038e7de14b78d702d2849c3aad72fd25903c4765af63cf16aa3398f5d5f2dd",
      }),
    }),
    {},
  ),
  /**
   * Whether alasio's pods take the places of pods of lower priority to be scheduled where
   * there is no room for them (./priorities.ts): alasio's own, and, on a cluster it shares,
   * the cluster's others, of priority 0 unless their class says; or wait for room. So true
   * on a cluster alasio makes, and false on one a kubeconfig reaches, unless said
   * (installConfigOf in ../config.ts).
   */
  preemption: defaulted(Schema.Boolean, true),
  /**
   * Placeholder pods, each requesting `resources`, which any pod takes the place of: room a
   * node autoscaler keeps ready, of use only where one runs, and where pods preempt. None
   * unless a fixed count is given.
   */
  headroom: defaulted(
    Schema.Struct({
      scaling: Scaling({ min: 0, max: 0 }, { floor: false, scaled: false }),
      image: Image({ repository: "registry.k8s.io/pause", tag: "3.10.1", digest: "sha256:278fb9dbcca9518083ad1e11276933a2e96f23de604a3a08cc3c80002767d24c" }),
      resources: Resources({ requests: { cpu: "500m", memory: "512Mi" }, limits: {} }),
    }),
    {},
  ),
});

/** What a configuration must also hold, given what it turns off: the issue of the first it lacks. */
function completeness(config: typeof InstallStruct.Type): { readonly path: ReadonlyArray<string>; readonly issue: string } | undefined {
  if (!config.neon.enabled && !config.neon.external.existingSecret) {
    return { path: ["neon", "external", "existingSecret"], issue: "is required when neon.enabled is false" };
  }
  const { webhook } = config;
  if (webhook.exposure === "hostAddress" && webhook.publicAddress && !/^(\d{1,3}\.){3}\d{1,3}$/u.test(webhook.publicAddress)) {
    return { path: ["webhook", "publicAddress"], issue: "must be this machine's public IPv4 address, which Telegram delivers to and the webhook intake binds alone: alasio init finds it" };
  }
  if (webhook.exposure !== "loadBalancer" && webhook.scaling.min > 1) {
    return { path: ["webhook", "scaling"], issue: "must be 1 where the webhook intake binds its node's port, which one pod holds" };
  }
  if (config.neon.enabled && !config.objectStore.bundled.enabled) {
    if (!config.objectStore.external.endpoint) return { path: ["objectStore", "external", "endpoint"], issue: "is required when objectStore.bundled.enabled is false" };
    if (!config.objectStore.external.existingSecret) return { path: ["objectStore", "external", "existingSecret"], issue: "is required with an external object store" };
    if (config.sessions.enabled && config.sessions.runtimeClassName !== "" && !config.objectStore.external.snapshotsSecret) {
      return { path: ["objectStore", "external", "snapshotsSecret"], issue: "is required with an external object store where sessions are snapshotted: a key that reaches the snapshots bucket alone" };
    }
  }
  if (config.workspaceStorage.enabled && !config.sessions.enabled) {
    return { path: ["workspaceStorage", "enabled"], issue: "must be false when sessions.enabled is false, as its volumes are sessions' workspaces" };
  }
  if (config.workspaceStorage.enabled && !config.neon.enabled) {
    return { path: ["workspaceStorage", "enabled"], issue: "must be false when neon.enabled is false, as its data is in the object store that runs with Neon" };
  }
  const { bundled } = config.objectStore;
  if (bundled.masters.scaling.min === 2) return { path: ["objectStore", "bundled", "masters", "scaling"], issue: "must be 1 or 3: two masters elect no leader once one is gone" };
  const copies = Number(bundled.replication.slice(2)) + 1;
  if (bundled.volumeServers.scaling.min < copies) {
    return { path: ["objectStore", "bundled", "volumeServers", "scaling"], issue: `must be at least ${copies}, one for each copy objectStore.bundled.replication keeps` };
  }
  if (memoryBytes(bundled.filerStore.resources.limits["memory"] ?? "0") < 2 * filerStoreMaxmemoryBytes(bundled.filerStore.entries)) {
    return {
      path: ["objectStore", "bundled", "filerStore", "resources", "limits", "memory"],
      issue: `must be at least ${memoryText(2 * filerStoreMaxmemoryBytes(bundled.filerStore.entries))}, twice what its entries take, for the fork that writes its snapshots`,
    };
  }
  const { nodeAgent } = config.sessions.snapshots;
  if (config.sessions.enabled && config.sessions.runtimeClassName !== "" && memoryBytes(nodeAgent.resources.limits["memory"] ?? "0") < NODE_AGENT_MEMORY_FLOOR) {
    return {
      path: ["sessions", "snapshots", "nodeAgent", "resources", "limits", "memory"],
      issue: `must be at least ${memoryText(NODE_AGENT_MEMORY_FLOOR)}: the node agent's own memory, and what a restore holds written and not yet on the disk`,
    };
  }
  if (config.log.scaling.min === 2) return { path: ["log", "scaling"], issue: "must be 1 or 3: two of KRaft's voters elect no leader once one is gone" };
  if (memoryBytes(config.log.resources.limits["memory"] ?? "0") < memoryBytes(LOG_MEMORY)) {
    return { path: ["log", "resources", "limits", "memory"], issue: `must be at least ${LOG_MEMORY}: a heap of 512 MiB, and the JVM's own` };
  }
  const backedUp = bundled.enabled && bundled.filerStore.backups.enabled ? filerStoreBackupHours(bundled.filerStore.backups) : null;
  if (backedUp !== null && Number(config.log.retention.slice(0, -1)) < backedUp + LOG_RETENTION_MARGIN_HOURS) {
    return {
      path: ["log", "retention"],
      issue: `must be at least ${backedUp + LOG_RETENTION_MARGIN_HOURS}h: the filer store's backups go back ${backedUp}h (objectStore.bundled.filerStore.backups), ` +
        `a restore of the oldest loses the lake's telemetry written since, which the log refills only while it holds it, and a day more`,
    };
  }
  if (telemetryRetentionBytes(config) < LOG_SEGMENT_BYTES) {
    const least = Math.ceil((telemetryPartitions(config) * 2 * LOG_SEGMENT_BYTES * 4) / 3);
    return {
      path: ["log", "storage", "size"],
      issue: `must be at least ${memoryText(least)}: each of the telemetry topics' ${telemetryPartitions(config)} partitions keeps a segment of ${memoryText(LOG_SEGMENT_BYTES)} at least beside the one it writes, in three quarters of it`,
    };
  }
  if (config.autoscaling.headroom.scaling.min > 0 && !config.autoscaling.preemption) {
    return { path: ["autoscaling", "headroom", "scaling"], issue: "must be 0 when autoscaling.preemption is false, as no pod takes a placeholder's place then" };
  }
  return undefined;
}

const InstallStruct = Schema.Struct({
  images: defaulted(
    Schema.Struct({
      alasio: Image(IMAGES.alasio),
      agent: Image(IMAGES.agent),
      lake: Image(IMAGES.lake),
      grafana: Image(IMAGES.grafana),
      log: Image(IMAGES.log),
      collector: Image(IMAGES.collector),
      webhook: Image(IMAGES.webhook),
      pullPolicy: defaulted(PullPolicy, "IfNotPresent"),
    }),
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
  log: defaulted(Log, {}),
  webhook: defaulted(Webhook, {}),
  /**
   * The analytics lake, in DuckLake: its loader, which loads it from Neon while alasio says
   * it wrote transcripts and sleeps otherwise; its maintenance, main's, in passes on a
   * schedule; its readers, main's, which Grafana and alasio lake read it through, scaled on
   * the queries waiting on them, never below one; and its telemetry intake, which writes
   * what the log holds of the stack's telemetry to it, scaled on how much is waiting there,
   * to none while nothing is.
   */
  lake: defaulted(
    Schema.Struct({
      enabled: defaulted(Schema.Boolean, true),
      loader: defaulted(
        Schema.Struct({
          /** None while nothing is left to load, which KEDA wakes; one, never two, as a load is not parallel. At one, it never sleeps, and needs no KEDA. */
          scaling: Scaling({ min: 0, max: 1 }, { floor: false, scaled: true, most: 1 }),
          resources: Resources({ requests: { cpu: "100m", memory: "512Mi" }, limits: { memory: "1536Mi" } }),
        }),
        {},
      ),
      maintenance: defaulted(
        Schema.Struct({
          /** When its daily pass runs, away from Neon's backup; the telemetry's files are merged hourly besides. */
          schedule: defaulted(Schema.String.check(Schema.isMinLength(9, { message: "must be a cron schedule" })), "17 2 * * *"),
          resources: Resources({ requests: { cpu: "50m", memory: "256Mi" }, limits: { memory: "1Gi" } }),
        }),
        {},
      ),
      query: defaulted(
        Schema.Struct({
          scaling: Scaling({ min: 1, max: 1 }, { floor: true, scaled: true }),
          resources: Resources({ requests: { cpu: "50m", memory: "128Mi" }, limits: { memory: "1Gi" } }),
        }),
        {},
      ),
      intake: defaulted(
        Schema.Struct({
          scaling: Scaling({ min: 0, max: 2 }, { floor: false, scaled: true }),
          resources: Resources({ requests: { cpu: "50m", memory: "256Mi" }, limits: { memory: "1Gi" } }),
        }),
        {},
      ),
    }),
    {},
  ),
  /**
   * Grafana, with the lake: dashboards and alerts on it, alerting through the Telegram bot;
   * alasio grafana reaches it. Two replicas or more are its high availability, which sends
   * each alert once; min and max apart scale it on its CPU.
   */
  grafana: defaulted(
    Schema.Struct({
      enabled: defaulted(Schema.Boolean, true),
      scaling: Scaling({ min: 1, max: 1 }, { floor: true, scaled: true }),
      resources: Resources({ requests: { cpu: "50m", memory: "256Mi" }, limits: { memory: "768Mi" } }),
    }),
    {},
  ),
  /** NetworkPolicies confining sessions, folder workspaces' bayma and Neon. */
  networkPolicies: defaulted(Schema.Struct({ enabled: defaulted(Schema.Boolean, true) }), {}),
  agentSandbox: defaulted(AgentSandbox, {}),
  autoscaling: defaulted(Autoscaling, {}),
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
