/**
 * The log: Apache Kafka 4.2, in KRaft, its partitions' segments on a volume of each of its
 * nodes. It carries the stack's telemetry, which the collector's gateway writes to it
 * (./collector.ts) and the lake's intake reads from it (./lake.ts), and keeps what it
 * carries for log.retention alone, within its volume: the lake is the store.
 *
 * Its nodes are brokers and KRaft's controllers both, combined, 1 or 3 of them, as its
 * scale fixes, on the floor, above the rest of it as their volumes hold them to a node
 * (./priorities.ts), each partition on every one of them and acknowledged once a
 * majority has it. Each runs on alasio-log (neon/log/Dockerfile), restricted, its root
 * file system read-only, its logs on its output.
 *
 * Clients reach it on SASL_PLAINTEXT with SCRAM-SHA-512, each as a principal of its own,
 * and do what its ACLs allow it alone, under KRaft's StandardAuthorizer, which allows
 * nothing without one but to its admin. Its controllers reach each other on a listener of
 * their own, which its NetworkPolicy (./network-policies.ts) admits its nodes alone to.
 * Its admin is made as each node is formatted, with KRaft's bootstrap; its other
 * principals, its topics and their ACLs by its access applier (src/log/access.ts),
 * beside each node, idempotently, from what LOG_ACCESS says and the passwords the stack's
 * setup made (neon/control/kube-setup.ts): a node is ready once its applier has applied
 * them. Its metrics are served in Prometheus's format by the JMX exporter in its image,
 * which the collector's scraper scrapes.
 */
import type { KubernetesObject, V1ConfigMap, V1Container, V1EnvVar, V1Service, V1StatefulSet } from "@kubernetes/client-node";

import type { LogPrincipal, MainEnvironmentPrincipal } from "../../../neon/control/secrets.ts";
import { imageReference } from "../images.ts";
import { claimLabels, claimSpec, componentName, type Environment, NAMESPACE, replicasOf, restrictedContainer, script, selectorLabels, sha256, stackLabels, stackPodSpec } from "./common.ts";
import { type InstallConfig, LOG_SEGMENT_BYTES, telemetryRetentionBytes } from "./config.ts";
import { replicaBudget, spread } from "./disruption.ts";
import { priority } from "./priorities.ts";
import { verticalPodAutoscaler } from "./sizing.ts";

/** The log's name: its StatefulSet's, its Service's, its ConfigMap's and its Secret's. */
export const LOG = componentName("log");
export const LOG_COMPONENT = "log";
/** The ports of its clients, of its controllers, and of its metrics. */
export const LOG_PORT = 9092;
export const LOG_CONTROLLER_PORT = 9093;
export const LOG_METRICS_PORT = 9464;
/** The port its access applier answers whether it has applied what it applies on. */
const ACCESS_PORT = 8080;

/** The telemetry topics, by signal, which the gateway writes and the intake reads. */
export const TELEMETRY_TOPICS = { traces: "otel.spans", logs: "otel.logs", metrics: "otel.metrics" } as const;

/**
 * The largest record the log takes, as large as a minute's scrape of the stack in one export
 * may be: one the lake's intake writes a part at a time, whatever its size (test/lake.test.ts
 * proves one this large within a bound of its memory).
 */
export const LOG_RECORD_BYTES = 8 * 1024 * 1024;

/** The consumer group of the lake's intake. */
export const INTAKE_GROUP = "lake-intake";

/**
 * How long an environment's Telegram updates are kept: a week, so an environment asleep
 * for one still finds them, beyond the day Telegram itself keeps what it could not deliver.
 */
export const TELEGRAM_RETENTION_MS = 7 * 86_400_000;

/** An environment's name, as its topic, group and principal are named: main's, or its branch's. */
export const environmentName = ({ branch }: Environment): string => branch ?? "main";

/** The topic of the Telegram updates of the environment `name`, which the webhook intake writes and its alasio reads. */
export const telegramTopic = (name: string): string => `telegram.${name}`;
/** The principal the environment `name`'s alasio reaches the log as. */
export const alasioPrincipal = (name: string): string => `alasio-${name}`;
/** The consumer group the environment `name`'s alasio reads its Telegram topic in. */
export const alasioGroup = (name: string): string => `alasio.${name}`;

/** How long an environment's wake records are kept: a day, long past its lake loader's waking at the first. */
export const WAKE_RETENTION_MS = 86_400_000;
/** The topic the environment `name`'s alasio writes as it commits transcripts, which wakes its lake loader. */
export const lakeWakeTopic = (name: string): string => `lake.${name}`;
/** The principal the environment `name`'s lake loader reaches the log as. */
export const loaderPrincipal = (name: string): string => `loader-${name}`;
/** The consumer group the environment `name`'s lake loader reads its wake topic in. */
export const loaderGroup = (name: string): string => `lake-loader.${name}`;

/**
 * What the log gives the environment `name`: a principal for each of its components that
 * reaches the log, each with what it may do alone, and the topics they use, made and
 * forgotten with the environment. Its alasio's, `alasio-<name>`, reading its Telegram
 * topic in its group, `alasio.<name>`, and writing its wake topic, `lake.<name>`, as it
 * commits transcripts; its lake loader's, `loader-<name>`, reading the wake topic in its
 * group, `lake-loader.<name>`; those topics, of one partition each, as one alasio and one
 * loader read them, a chat's updates in order, each keeping a segment beside the one it
 * writes at most: a week of a bot's updates, and a day of wakes, is kilobytes. Each update
 * is flushed to the volume as it is written; a wake, lost, costs nothing.
 */
export function environmentAccess(name: string): EnvironmentLogAccess {
  const topic = telegramTopic(name);
  const wake = lakeWakeTopic(name);
  return {
    principals: [
      {
        name: alasioPrincipal(name),
        acls: [
          { resource: "topic", name: topic, pattern: "literal", operations: ["read", "describe"] },
          { resource: "group", name: alasioGroup(name), pattern: "literal", operations: ["read"] },
          { resource: "topic", name: wake, pattern: "literal", operations: ["write", "describe"] },
        ],
      },
      {
        name: loaderPrincipal(name),
        acls: [{ resource: "topic", name: wake, pattern: "literal", operations: ["read", "describe"] }, { resource: "group", name: loaderGroup(name), pattern: "literal", operations: ["read"] }],
      },
    ],
    topics: [
      // Each update flushed to the volume as it is written: Telegram, answered once the log has it, sends it no more.
      { name: topic, partitions: 1, retentionMs: TELEGRAM_RETENTION_MS, retentionBytes: LOG_SEGMENT_BYTES, configs: { "flush.messages": "1" } },
      { name: wake, partitions: 1, retentionMs: WAKE_RETENTION_MS, retentionBytes: LOG_SEGMENT_BYTES, configs: {} },
    ],
  };
}

/** The log's brokers, as a client anywhere in the cluster bootstraps from them. */
export const LOG_BROKERS = `${LOG}.${NAMESPACE}.svc:${LOG_PORT}`;

/** The Secret of the principal `principal`'s name and password, which the stack's setup writes. */
export const logPrincipalSecret = (principal: LogPrincipal): string => componentName(`log-${principal}`);

/**
 * The Secret of the principal the alasio of `environment` reads its topic as, in its
 * namespace: main's, which the stack's setup writes; a branch environment's, which alasio's
 * command line writes as it makes the branch's principal.
 */
export const environmentLogSecret = ({ branch }: Environment): string => (branch === null ? logPrincipalSecret("alasio-main") : componentName("log-alasio"));

/** The Secret of the principal the lake loader of `environment` reads its wake topic as, in its namespace: main's, which the stack's setup writes; a branch environment's, which alasio's command line writes. */
export const loaderLogSecret = ({ branch }: Environment): string => (branch === null ? logPrincipalSecret("loader-main") : componentName("log-loader"));

/** A node's address, by its ordinal: its pod's name in the log's headless Service. */
const nodeAddress = (ordinal: number | string): string => `${LOG}-${ordinal}.${LOG}.${NAMESPACE}.svc`;

/** What a principal may do on a topic or a group: its name, a prefix of names or one name, and the operations. */
export interface LogAcl {
  readonly resource: "topic" | "group";
  readonly name: string;
  readonly pattern: "literal" | "prefixed";
  readonly operations: readonly ("read" | "write" | "describe")[];
}

/** A topic: its partitions, how long and how much of each it keeps, and its other configurations of its own, by Kafka's names. */
export interface LogTopic {
  readonly name: string;
  readonly partitions: number;
  readonly retentionMs: number;
  readonly retentionBytes: number;
  readonly configs: Readonly<Record<string, string>>;
}

/** A principal of the log, and what it may do. */
export interface LogPrincipalAccess {
  readonly name: string;
  readonly acls: readonly LogAcl[];
}

/**
 * What the log gives an environment (environmentAccess): its principals, each with, where
 * alasio's command line makes it for a branch (src/log/access.ts), its password; and its topics.
 */
export interface EnvironmentLogAccess {
  readonly principals: readonly (LogPrincipalAccess & { readonly password?: string })[];
  readonly topics: readonly LogTopic[];
}

/**
 * What the access applier applies: the topics, and each principal's ACLs, its password read
 * from the access Secret by its name, one of LOG_PRINCIPALS.
 */
export interface LogAccess {
  readonly topics: readonly LogTopic[];
  readonly principals: readonly LogPrincipalAccess[];
}

/** The hours of a retention as log.retention says it. */
const retentionMs = (retention: string): number => Number(retention.slice(0, -1)) * 3_600_000;

/**
 * What the log's access is: the telemetry topics, of as many partitions as the intake
 * runs replicas at most, so each has one to read, each partition keeping its share of the
 * volume at most (telemetryRetentionBytes), each flushed to its volume within a second
 * (flush.ms); the gateway's writing them, the intake's reading them in its group, and
 * KEDA's reading how far the group is behind on them, and each environment's lake loader on
 * its wake topic, which asks to describe both and to read neither; the webhook intake's
 * writing every environment's Telegram topic, a branch's among them, and nothing else; and
 * main's environment's principals and topics (environmentAccess).
 *
 * A write is acknowledged once a majority of the nodes have it in their page caches, not
 * on their volumes. On one node, the volume outlives the pod, so a restart of the pod loses
 * nothing acknowledged, but a crash of its host loses what the node had not flushed: up
 * to a second of acknowledged telemetry, as its log flusher looks each second for what is
 * older than flush.ms (log.flush.scheduler.interval.ms, serverProperties), and a flush a
 * second costs next to nothing at the rate the stack writes it; and nothing of Telegram's
 * updates, each flushed as it is written (flush.messages), as Telegram has had its answer.
 * On three, each record is on two hosts before it is acknowledged, so a host's crash loses
 * none.
 */
export function logAccess(config: InstallConfig): LogAccess {
  const partitions = Math.max(1, config.lake.intake.scaling.max);
  const telemetry = { resource: "topic", name: "otel.", pattern: "prefixed" } as const;
  const group = { resource: "group", name: INTAKE_GROUP, pattern: "literal" } as const;
  const main = environmentAccess("main");
  const services: Record<Exclude<LogPrincipal, MainEnvironmentPrincipal>, readonly LogAcl[]> = {
    gateway: [{ ...telemetry, operations: ["write", "describe"] }],
    intake: [{ ...telemetry, operations: ["read", "describe"] }, { ...group, operations: ["read"] }],
    // And every environment's wake topic and its loader's group, which it reads how far behind on too.
    scaler: [
      { ...telemetry, operations: ["describe"] },
      { ...group, operations: ["describe"] },
      { resource: "topic", name: "lake.", pattern: "prefixed", operations: ["describe"] },
      { resource: "group", name: "lake-loader.", pattern: "prefixed", operations: ["describe"] },
    ],
    webhook: [{ resource: "topic", name: "telegram.", pattern: "prefixed", operations: ["write", "describe"] }],
  };
  const retention = { retentionMs: retentionMs(config.log.retention), retentionBytes: telemetryRetentionBytes(config) };
  return {
    topics: [...Object.values(TELEMETRY_TOPICS).map((name) => ({ name, partitions, ...retention, configs: { "flush.ms": "1000" } })), ...main.topics],
    principals: [...Object.entries(services).map(([name, acls]) => ({ name, acls })), ...main.principals],
  };
}

/**
 * Its nodes' configuration, whole: combined nodes, KRaft's voters every one of them; clients
 * on SCRAM-SHA-512 and the controllers on a listener of their own; nothing allowed without an
 * ACL but to its admin, and to what reaches the controllers' listener, its nodes alone;
 * topics made by its access applier alone, each partition on every node, a write
 * acknowledged once a majority of them has it, and flushed to its volume within a second
 * where its topic says so (flush.ms), its log flusher looking each second, where it would
 * look never by default; a record of up to 8 MiB, as a minute's
 * scrape of the stack in one export may be; its segments on its volume, each closed within
 * the hour or at LOG_SEGMENT_BYTES, so its retention, which deletes closed ones alone,
 * reaches them in time; and a snapshot of its metadata every ten minutes where it changed,
 * rather than every hour, so that a start replays little of it (src/log/access.ts says
 * what a replay may lose). What differs from node to node (its id, its address, its
 * admin's password) its start script adds (START).
 */
function serverProperties(config: InstallConfig): string {
  const nodes = config.log.scaling.min;
  const voters = Array.from({ length: nodes }, (_, ordinal) => `${ordinal}@${nodeAddress(ordinal)}:${LOG_CONTROLLER_PORT}`);
  const majority = Math.floor(nodes / 2) + 1;
  return script(
    "process.roles=broker,controller",
    `controller.quorum.voters=${voters.join(",")}`,
    `listeners=CLIENT://:${LOG_PORT},CONTROLLER://:${LOG_CONTROLLER_PORT}`,
    "listener.security.protocol.map=CLIENT:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT",
    "inter.broker.listener.name=CLIENT",
    "controller.listener.names=CONTROLLER",
    "sasl.enabled.mechanisms=SCRAM-SHA-512",
    "sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512",
    "authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer",
    // ANONYMOUS is what reaches the controllers' listener, unauthenticated: the NetworkPolicy, admitting its port from the log's own pods alone, is its guard.
    "super.users=User:admin;User:ANONYMOUS",
    "allow.everyone.if.no.acl.found=false",
    "auto.create.topics.enable=false",
    `default.replication.factor=${nodes}`,
    `offsets.topic.replication.factor=${nodes}`,
    `transaction.state.log.replication.factor=${nodes}`,
    `min.insync.replicas=${majority}`,
    `transaction.state.log.min.isr=${majority}`,
    "log.dirs=/data/log",
    `log.retention.ms=${retentionMs(config.log.retention)}`,
    `log.segment.bytes=${LOG_SEGMENT_BYTES}`,
    "log.roll.ms=3600000",
    `message.max.bytes=${LOG_RECORD_BYTES}`,
    "log.flush.scheduler.interval.ms=1000",
    "metadata.log.max.snapshot.interval.ms=600000",
  );
}

/** Its logging, to its output alone, rather than to files in its /tmp. */
const LOG4J2 = script(
  "rootLogger.level=INFO",
  "rootLogger.appenderRef.stdout.ref=STDOUT",
  "appender.stdout.type=Console",
  "appender.stdout.name=STDOUT",
  "appender.stdout.layout.type=PatternLayout",
  "appender.stdout.layout.pattern=[%d] %p %m (%c)%n",
);

/**
 * What its JMX exporter serves, of the MBeans it reads alone: how long its Produce requests
 * take (p50 and p99, in ms), how many bytes each partition holds on its volume, and its
 * broker's state (3 while it runs); and the JVM's own metrics.
 */
const METRICS = script(
  "lowercaseOutputName: true",
  "includeObjectNames:",
  '  - "kafka.network:type=RequestMetrics,name=TotalTimeMs,request=Produce"',
  '  - "kafka.log:type=Log,name=Size,*"',
  '  - "kafka.server:type=KafkaServer,name=BrokerState"',
  "rules:",
  "  - pattern: 'kafka.network<type=RequestMetrics, name=TotalTimeMs, request=(.+)><>(50|99)thPercentile'",
  "    name: kafka_request_total_time_ms",
  "    type: GAUGE",
  '    labels: { request: "$1", percentile: "$2" }',
  "  - pattern: 'kafka.log<type=Log, name=Size, topic=(.+), partition=(.+)><>Value'",
  "    name: kafka_log_size_bytes",
  "    type: GAUGE",
  '    labels: { topic: "$1", partition: "$2" }',
  "  - pattern: 'kafka.server<type=KafkaServer, name=BrokerState><>Value'",
  "    name: kafka_server_broker_state",
  "    type: GAUGE",
);

/**
 * How a node starts, `format` or `serve`: its configuration written to /tmp, the log's,
 * then its id (its pod's ordinal), its address, and the admin it reaches the other nodes
 * as; then its volume formatted for the cluster's id, its admin made with KRaft's
 * bootstrap, once (a formatted volume is left as it is); or it serves.
 */
const START = script(
  "set -eu",
  'ordinal="${HOSTNAME##*-}"',
  "{",
  "  cat /etc/log/server.properties",
  '  echo "node.id=$ordinal"',
  `  echo "advertised.listeners=CLIENT://$HOSTNAME.${LOG}.${NAMESPACE}.svc:${LOG_PORT}"`,
  '  echo "listener.name.client.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\\"admin\\" password=\\"$LOG_ADMIN_PASSWORD\\";"',
  "} >/tmp/server.properties",
  'case "$1" in',
  '  format) exec /opt/kafka/bin/kafka-storage.sh format --ignore-formatted -t "$KAFKA_CLUSTER_ID" -c /tmp/server.properties --add-scram "SCRAM-SHA-512=[name=admin,password=$LOG_ADMIN_PASSWORD]" ;;',
  "  serve) exec /opt/kafka/bin/kafka-server-start.sh /tmp/server.properties ;;",
  "esac",
);

/** The log's objects: it always runs, Telegram's updates coming through it. */
export function logObjects(config: InstallConfig): KubernetesObject[] {
  const { log } = config;
  const metadata = { name: LOG, namespace: NAMESPACE, labels: stackLabels(LOG_COMPONENT) };
  const access = logAccess(config);
  const data = {
    "server.properties": serverProperties(config),
    "log4j2.properties": LOG4J2,
    "metrics.yaml": METRICS,
    "start.sh": START,
    "access.json": `${JSON.stringify(access, null, 2)}\n`,
  };
  const configMap: V1ConfigMap = { apiVersion: "v1", kind: "ConfigMap", metadata, data };
  // Headless, each node's address its pod's, which the others reach it by before it is ready, as KRaft's quorum forms.
  const service: V1Service = {
    apiVersion: "v1",
    kind: "Service",
    metadata,
    spec: {
      clusterIP: "None",
      publishNotReadyAddresses: true,
      selector: selectorLabels(LOG_COMPONENT),
      ports: [
        { name: "client", port: LOG_PORT },
        { name: "controller", port: LOG_CONTROLLER_PORT },
        { name: "metrics", port: LOG_METRICS_PORT },
      ],
    },
  };
  const secret = (key: string): V1EnvVar => ({ name: key, valueFrom: { secretKeyRef: { name: LOG, key } } });
  const node = (name: string, command: string): Pick<V1Container, "name" | "image" | "imagePullPolicy" | "command" | "securityContext"> => ({
    name,
    image: imageReference(config.images.log),
    imagePullPolicy: config.images.pullPolicy,
    command: ["/bin/sh", "/etc/log/start.sh", command],
    securityContext: { ...restrictedContainer(), readOnlyRootFilesystem: true },
  });
  const mounts = [{ name: "config", mountPath: "/etc/log", readOnly: true }, { name: "tmp", mountPath: "/tmp" }, { name: "data", mountPath: "/data" }];
  const common: V1EnvVar[] = [
    secret("KAFKA_CLUSTER_ID"),
    secret("LOG_ADMIN_PASSWORD"),
    { name: "LOG_DIR", value: "/tmp/logs" },
    { name: "KAFKA_LOG4J_OPTS", value: "-Dlog4j2.configurationFile=/etc/log/log4j2.properties" },
  ];
  const statefulSet: V1StatefulSet = {
    apiVersion: "apps/v1",
    kind: "StatefulSet",
    metadata,
    spec: {
      serviceName: LOG,
      ...replicasOf(log.scaling),
      // Every voter at once: a quorum of three forms only once two of them run.
      podManagementPolicy: "Parallel",
      selector: { matchLabels: selectorLabels(LOG_COMPONENT) },
      template: {
        metadata: { labels: stackLabels(LOG_COMPONENT), annotations: { "checksum/config": sha256(JSON.stringify(data)) } },
        spec: {
          ...priority("bound"),
          ...stackPodSpec(config),
          ...spread(LOG_COMPONENT, log.scaling, true),
          // Its partitions closed cleanly as it stops, so it starts again without recovering them.
          terminationGracePeriodSeconds: 120,
          initContainers: [{
            ...node("format", "format"),
            env: [...common, { name: "KAFKA_HEAP_OPTS", value: "-Xmx256m" }],
            resources: { requests: { cpu: "50m", memory: "256Mi" }, limits: { memory: "512Mi" } },
            volumeMounts: mounts,
          }],
          containers: [
            {
              ...node("log", "serve"),
              env: [
                ...common,
                { name: "KAFKA_HEAP_OPTS", value: "-Xms512m -Xmx512m" },
                { name: "KAFKA_OPTS", value: `-javaagent:/opt/jmx-exporter/jmx_prometheus_javaagent.jar=${LOG_METRICS_PORT}:/etc/log/metrics.yaml` },
                // Without -loggc: no GC log files in /tmp.
                { name: "EXTRA_ARGS", value: "-name kafkaServer" },
              ],
              ports: [
                { name: "client", containerPort: LOG_PORT },
                { name: "controller", containerPort: LOG_CONTROLLER_PORT },
                { name: "metrics", containerPort: LOG_METRICS_PORT },
              ],
              startupProbe: { tcpSocket: { port: "client" }, periodSeconds: 5, failureThreshold: 120 },
              readinessProbe: { tcpSocket: { port: "client" }, periodSeconds: 10 },
              livenessProbe: { tcpSocket: { port: "client" }, periodSeconds: 30, failureThreshold: 4 },
              resources: log.resources,
              volumeMounts: mounts,
            },
            {
              name: "access",
              image: imageReference(config.images.alasio),
              imagePullPolicy: config.images.pullPolicy,
              command: ["node", "/opt/alasio/src/log/access.ts"],
              env: [
                { name: "LOG_BROKERS", value: `127.0.0.1:${LOG_PORT}` },
                // Every voter's, as KRaft's active controller alone writes a credential.
                { name: "LOG_CONTROLLERS", value: Array.from({ length: log.scaling.min }, (_, ordinal) => `${nodeAddress(ordinal)}:${LOG_CONTROLLER_PORT}`).join(",") },
                secret("LOG_ADMIN_PASSWORD"),
                { name: "LOG_ACCESS", value: "/etc/log/access.json" },
                { name: "LOG_PASSWORDS", value: "/etc/log-passwords" },
                { name: "LOG_ACCESS_PORT", value: String(ACCESS_PORT) },
              ],
              ports: [{ name: "access", containerPort: ACCESS_PORT }],
              readinessProbe: { httpGet: { path: "/readyz", port: "access" }, periodSeconds: 5 },
              securityContext: { ...restrictedContainer(), readOnlyRootFilesystem: true },
              resources: { requests: { cpu: "10m", memory: "64Mi" }, limits: { memory: "192Mi" } },
              volumeMounts: [{ name: "config", mountPath: "/etc/log", readOnly: true }, { name: "passwords", mountPath: "/etc/log-passwords", readOnly: true }],
            },
          ],
          volumes: [
            { name: "config", configMap: { name: LOG } },
            { name: "passwords", secret: { secretName: componentName("log-access") } },
            { name: "tmp", emptyDir: { sizeLimit: "1Gi" } },
          ],
        },
      },
      volumeClaimTemplates: [{ metadata: { name: "data", labels: claimLabels(stackLabels(LOG_COMPONENT)) }, spec: claimSpec(log.storage) }],
    },
  };
  return [configMap, service, statefulSet, ...replicaBudget(LOG_COMPONENT, NAMESPACE, log.scaling), verticalPodAutoscaler(statefulSet, "Recommended")];
}
