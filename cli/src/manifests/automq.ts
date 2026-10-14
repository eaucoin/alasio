/**
 * The log: AutoMQ (automqinc/automq), Kafka whose partitions and write-ahead log are in
 * the object store's bucket objectStore.buckets.log, its nodes keeping nothing else but
 * KRaft's metadata, on a small volume each. It carries the stack's telemetry, which the
 * collector's gateway writes to it (./collector.ts) and the lake's intake reads from it
 * (./lake.ts), and keeps what it carries for log.retention alone: the lake is the store.
 *
 * Its nodes are brokers and KRaft's controllers both, combined, 1 or 3 of them, as its
 * scale fixes, on the floor. Each runs at AutoMQ's smallest documented tier (a heap of
 * 1 GiB, 1.5 GiB of direct memory, its write-ahead log's cache 500 MiB and its block
 * cache 100 MiB), restricted, its root file system read-only, its logs on its output.
 *
 * Clients reach it on SASL_PLAINTEXT with SCRAM-SHA-512, each as a principal of its own,
 * and do what its ACLs allow it alone, under KRaft's StandardAuthorizer, which allows
 * nothing without one but to its admin. Its controllers reach each other on a listener of
 * their own, which its NetworkPolicy (./network-policies.ts) admits its nodes alone to.
 * Its admin is made as each node is formatted, with KRaft's bootstrap; its other
 * principals, its topics and their ACLs by its access applier (src/log/access.ts),
 * beside each node, idempotently, from what LOG_ACCESS says and the passwords the stack's
 * setup made (neon/control/kube-setup.ts): a node is ready once its applier has applied
 * them. Its metrics are served in Prometheus's format, which the collector's scraper
 * scrapes.
 */
import type { KubernetesObject, V1ConfigMap, V1Container, V1EnvVar, V1Service, V1StatefulSet } from "@kubernetes/client-node";

import { LOG_PRINCIPALS, type LogPrincipal } from "../../../neon/control/secrets.ts";
import { imageReference } from "../images.ts";
import { claimSpec, componentName, type Environment, NAMESPACE, replicasOf, restrictedContainer, s3Endpoint, script, selectorLabels, sha256, stackLabels, stackPodSpec } from "./common.ts";
import type { InstallConfig } from "./config.ts";
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

/**
 * What the log gives the environment `name`'s alasio: its principal, `alasio-<name>`,
 * which reads its topic in its group, `alasio.<name>`, and nothing else; and its topic,
 * of one partition, as one alasio reads it, a chat's updates in order.
 */
export function environmentAccess(name: string): { readonly principal: string; readonly group: string; readonly topic: LogAccess["topics"][number]; readonly acls: readonly LogAcl[] } {
  const topic = telegramTopic(name);
  const group = `alasio.${name}`;
  return {
    principal: `alasio-${name}`,
    group,
    topic: { name: topic, partitions: 1, retentionMs: TELEGRAM_RETENTION_MS },
    acls: [{ resource: "topic", name: topic, pattern: "literal", operations: ["read", "describe"] }, { resource: "group", name: group, pattern: "literal", operations: ["read"] }],
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

/** A node's address, by its ordinal: its pod's name in the log's headless Service. */
const nodeAddress = (ordinal: number | string): string => `${LOG}-${ordinal}.${LOG}.${NAMESPACE}.svc`;

/** What a principal may do on a topic or a group: its name, a prefix of names or one name, and the operations. */
export interface LogAcl {
  readonly resource: "topic" | "group";
  readonly name: string;
  readonly pattern: "literal" | "prefixed";
  readonly operations: readonly ("read" | "write" | "describe")[];
}

/** What the access applier applies: the topics, and each principal's ACLs, its password read from the access Secret by its name. */
export interface LogAccess {
  readonly topics: readonly { readonly name: string; readonly partitions: number; readonly retentionMs: number }[];
  readonly principals: readonly { readonly name: LogPrincipal; readonly acls: readonly LogAcl[] }[];
}

/** The hours of a retention as log.retention says it. */
const retentionMs = (retention: string): number => Number(retention.slice(0, -1)) * 3_600_000;

/**
 * What the log's access is: the telemetry topics, of as many partitions as the intake
 * runs replicas at most, so each has one to read; the gateway's writing them, the
 * intake's reading them in its group, and KEDA's reading how far the group is behind on
 * them, which asks to describe both and to read neither; main's Telegram topic; the
 * webhook intake's writing every environment's, a branch's among them, and nothing else;
 * and main's alasio's reading its own (environmentAccess).
 */
export function logAccess(config: InstallConfig): LogAccess {
  const partitions = Math.max(1, config.lake.intake.scaling.max);
  const telemetry = { resource: "topic", name: "otel.", pattern: "prefixed" } as const;
  const group = { resource: "group", name: INTAKE_GROUP, pattern: "literal" } as const;
  const main = environmentAccess("main");
  const principals: Record<LogPrincipal, readonly LogAcl[]> = {
    gateway: [{ ...telemetry, operations: ["write", "describe"] }],
    intake: [{ ...telemetry, operations: ["read", "describe"] }, { ...group, operations: ["read"] }],
    scaler: [{ ...telemetry, operations: ["describe"] }, { ...group, operations: ["describe"] }],
    webhook: [{ resource: "topic", name: "telegram.", pattern: "prefixed", operations: ["write", "describe"] }],
    "alasio-main": main.acls,
  };
  return {
    topics: [...Object.values(TELEMETRY_TOPICS).map((name) => ({ name, partitions, retentionMs: retentionMs(config.log.retention) })), main.topic],
    principals: LOG_PRINCIPALS.map((name) => ({ name, acls: principals[name] })),
  };
}

/**
 * Its nodes' configuration, over AutoMQ's own (its kraft/server.properties, a later key
 * winning): combined nodes, KRaft's voters every one of them; clients on SCRAM-SHA-512 and
 * the controllers on a listener of their own; nothing allowed without an ACL but to its
 * admin, and to what reaches the controllers' listener, its nodes alone; topics made by
 * its access applier alone; a record of up to 8 MiB, as a minute's scrape of the stack in
 * one export may be; the object store's bucket for its data, its operations and its
 * write-ahead log; the smallest tier's caches; and its metrics, in Prometheus's format.
 * What differs from node to node (its id, its address, its admin's password) its start
 * script adds (START).
 */
function serverProperties(config: InstallConfig): string {
  const { buckets, bundled, external } = config.objectStore;
  const region = bundled.enabled ? "us-east-1" : external.region;
  const bucket = `s3://${buckets.log}?region=${region}&endpoint=${s3Endpoint(config)}&pathStyle=true`;
  const voters = Array.from({ length: config.log.scaling.min }, (_, ordinal) => `${ordinal}@${nodeAddress(ordinal)}:${LOG_CONTROLLER_PORT}`);
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
    // What reaches the controllers' listener, unauthenticated, is its own nodes alone.
    "super.users=User:admin;User:ANONYMOUS",
    "allow.everyone.if.no.acl.found=false",
    "auto.create.topics.enable=false",
    "log.dirs=/data/kraft",
    `log.retention.ms=${retentionMs(config.log.retention)}`,
    "message.max.bytes=8388608",
    `s3.data.buckets=0@${bucket}`,
    `s3.ops.buckets=1@${bucket}`,
    `s3.wal.path=0@${bucket}`,
    "s3.stream.allocator.policy=POOLED_DIRECT",
    "s3.wal.cache.size=524288000",
    "s3.block.cache.size=104857600",
    "s3.wal.upload.threshold=62914560",
    `s3.telemetry.metrics.exporter.uri=prometheus://?host=0.0.0.0&port=${LOG_METRICS_PORT}`,
    "s3.telemetry.metrics.level=INFO",
  );
}

/** Its logging, to its output alone, rather than to files it would fill its /tmp with. */
const LOG4J = script(
  "log4j.rootLogger=INFO, stdout",
  "log4j.appender.stdout=org.apache.log4j.ConsoleAppender",
  "log4j.appender.stdout.layout=org.apache.log4j.PatternLayout",
  "log4j.appender.stdout.layout.ConversionPattern=[%d] %p %m (%c)%n",
);

/**
 * How a node starts, `format` or `serve`: its configuration written to /tmp, AutoMQ's
 * and the log's, then its id (its pod's ordinal), its address, and the admin it reaches
 * the other nodes as; then its volume formatted for the cluster's id, its admin made with
 * KRaft's bootstrap, once (a formatted volume is left as it is); or it serves.
 */
const START = script(
  "set -eu",
  'ordinal="${HOSTNAME##*-}"',
  "{",
  "  cat /opt/automq/kafka/config/kraft/server.properties /etc/log/server.properties",
  '  echo "node.id=$ordinal"',
  `  echo "advertised.listeners=CLIENT://$HOSTNAME.${LOG}.${NAMESPACE}.svc:${LOG_PORT}"`,
  '  echo "listener.name.client.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\\"admin\\" password=\\"$LOG_ADMIN_PASSWORD\\";"',
  "} >/tmp/server.properties",
  'case "$1" in',
  '  format) exec /opt/automq/kafka/bin/kafka-storage.sh format --ignore-formatted -t "$KAFKA_CLUSTER_ID" -c /tmp/server.properties --add-scram "SCRAM-SHA-512=[name=admin,password=$LOG_ADMIN_PASSWORD]" ;;',
  "  serve) exec /opt/automq/kafka/bin/kafka-server-start.sh /tmp/server.properties ;;",
  "esac",
);

/** The log's objects: it always runs, on the bundled object store or the operator's. */
export function logObjects(config: InstallConfig): KubernetesObject[] {
  const { log } = config;
  const metadata = { name: LOG, namespace: NAMESPACE, labels: stackLabels(LOG_COMPONENT) };
  const access = logAccess(config);
  const data = { "server.properties": serverProperties(config), "log4j.properties": LOG4J, "start.sh": START, "access.json": `${JSON.stringify(access, null, 2)}\n` };
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
  const automq = imageReference(log.image);
  const node = (name: string, command: string): Pick<V1Container, "name" | "image" | "imagePullPolicy" | "command" | "securityContext"> => ({
    name,
    image: automq,
    imagePullPolicy: "IfNotPresent",
    command: ["/bin/sh", "/etc/log/start.sh", command],
    securityContext: { ...restrictedContainer(), readOnlyRootFilesystem: true },
  });
  const mounts = [{ name: "config", mountPath: "/etc/log", readOnly: true }, { name: "tmp", mountPath: "/tmp" }];
  const common: V1EnvVar[] = [
    secret("KAFKA_CLUSTER_ID"),
    secret("LOG_ADMIN_PASSWORD"),
    secret("KAFKA_S3_ACCESS_KEY"),
    secret("KAFKA_S3_SECRET_KEY"),
    { name: "LOG_DIR", value: "/tmp/logs" },
    { name: "KAFKA_LOG4J_OPTS", value: "-Dlog4j.configuration=file:/etc/log/log4j.properties" },
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
          ...priority("floor"),
          ...stackPodSpec(config),
          ...spread(LOG_COMPONENT, log.scaling, true),
          // What it has yet to upload of its write-ahead log, it uploads as it stops.
          terminationGracePeriodSeconds: 120,
          initContainers: [{
            ...node("format", "format"),
            env: [...common, { name: "KAFKA_HEAP_OPTS", value: "-Xmx256m" }],
            resources: { requests: { cpu: "50m", memory: "256Mi" }, limits: { memory: "512Mi" } },
            volumeMounts: [...mounts, { name: "data", mountPath: "/data" }],
          }],
          containers: [
            {
              ...node("log", "serve"),
              env: [
                ...common,
                { name: "KAFKA_HEAP_OPTS", value: "-Xms1g -Xmx1g -XX:MetaspaceSize=96m -XX:MaxDirectMemorySize=1500m" },
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
              volumeMounts: [...mounts, { name: "data", mountPath: "/data" }],
            },
            {
              name: "access",
              image: imageReference(config.images.alasio),
              imagePullPolicy: config.images.pullPolicy,
              command: ["node", "/opt/alasio/src/log/access.ts"],
              env: [
                { name: "LOG_BROKERS", value: `127.0.0.1:${LOG_PORT}` },
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
      volumeClaimTemplates: [{ metadata: { name: "data", labels: stackLabels(LOG_COMPONENT) }, spec: claimSpec(log.storage) }],
    },
  };
  return [configMap, service, statefulSet, ...replicaBudget(LOG_COMPONENT, NAMESPACE, log.scaling), verticalPodAutoscaler(statefulSet, "Recommended")];
}
