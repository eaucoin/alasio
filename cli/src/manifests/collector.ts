/**
 * The stack's telemetry collector, the one hub of alasio's telemetry, in two parts on
 * alasio's collector image (neon/collector), the OpenTelemetry Collector of the components
 * they configure:
 *
 * - its gateway, `alasio-collector`, which alasio, its harnesses, bayma (folder
 *   workspaces' directly, sessions' through alasio's receiver), the lake and Neon's
 *   compute export to over OTLP/HTTP (common.ts's otelEnv), and its scraper too. It
 *   writes what each export holds to the log's topic of its signal (./log.ts), as OTLP
 *   protobuf, and answers the export only once the log has it: with no queue and no retry
 *   of its own, an export the log did not take is refused, and its producer sends it
 *   again, so nothing it answered is lost to its crash. The lake's intake reads the
 *   topics into the lake (./lake.ts). It sends telemetry.otlpEndpoint everything too when
 *   one is set, which alone is sent the configured headers, from a queue in its memory.
 *   It scales on its CPU where its scale's min and max are apart;
 * - its scraper, one replica, so nothing is scraped twice: every Neon service's, the
 *   lake's loader's, readers' and intake's, the log's, the webhook intake's, each of SeaweedFS's parts' and, with workspace
 *   storage, JuiceFS's Prometheus metrics, each pod of each found among the pods of its
 *   namespace, which it may list, so a component's replicas are each scraped and one that
 *   sleeps is not; and it reads the Valkeys', the object store's filer store's and
 *   workspace storage's, each with its password. It sends what it scrapes to the gateway.
 *
 * They run whenever the lake does, or an endpoint is set; with neither, nothing exports
 * anything. Each pod carries its configuration's checksum, so it is replaced when that
 * changes.
 */
import type { KubernetesObject, V1ConfigMap, V1Container, V1Deployment, V1EnvVar, V1Role, V1RoleBinding, V1Service, V1ServiceAccount, V1Volume } from "@kubernetes/client-node";

import { COMPUTE_CTL_PORT } from "../../../neon/control/branches.ts";
import { imageReference } from "../images.ts";
import { COLLECTOR_PORT, collectorRuns, componentName, given, goJson, lakeRuns, NAMESPACE, ONE, replicasOf, restrictedContainer, scaled, selectorLabels, sha256, stackLabels, stackPodSpec } from "./common.ts";
import type { InstallConfig } from "./config.ts";
import { LOG_BROKERS, LOG_METRICS_PORT, LOG_RECORD_BYTES, logPrincipalSecret, TELEMETRY_TOPICS } from "./log.ts";
import { replicaBudget, spread } from "./disruption.ts";
import { DRIVER_METRICS_PORT, DRIVER_POD_NAME, JUICEFS_METRICS_PORT, MOUNT_POD_NAME } from "./juicefs-csi.ts";
import { INTAKE_COMPONENT, LAKE_METRICS_PORT, LAKE_QUERY_PORT, LOADER_COMPONENT, QUERY_COMPONENT } from "./lake.ts";
import { COMPONENTS, FILER_STORE, FILER_STORE_USERS, METRICS_PORT as SEAWEEDFS_METRICS_PORT } from "./object-store.ts";
import { priority } from "./priorities.ts";
import { cpuUtilization, scaledObject } from "./scalers.ts";
import { verticalPodAutoscaler } from "./sizing.ts";
import { VALKEY_PORT, valkeyPassword } from "./valkey.ts";
import { WEBHOOK_HEALTH_PORT } from "./webhook.ts";
import { VALKEY } from "./workspace-storage.ts";

/** The gateway's name, its pods' component, and its Service's, which every producer exports to. */
export const COLLECTOR = componentName("collector");
export const GATEWAY_COMPONENT = "collector";
/** The scraper's name and its pods' component. */
export const SCRAPER = componentName("collector-scraper");
export const SCRAPER_COMPONENT = "collector-scraper";

/**
 * How often it scrapes the stack. The stack's metrics are most of all the telemetry
 * there is (the pageserver's alone half of it), some 30 points a second at this interval.
 */
const SCRAPE_INTERVAL = "60s";

/**
 * How the scraper batches what it scrapes: a minute's at a time, an export of up to
 * 10,000 points, some megabytes at most, which the log takes as one record of up to
 * 8 MiB (./log.ts) and the intake writes in one commit, a file of each kind's.
 */
const METRICS_BATCH = { send_batch_size: 10_000, send_batch_max_size: 10_000, timeout: "60s" };

/** Whether the bundled object store runs, whose parts it scrapes and whose filer store it reads. */
const seaweedfsRuns = ({ neon, objectStore }: InstallConfig): boolean => neon.enabled && objectStore.bundled.enabled;

/** The object store's parts it scrapes, by their pods' component: all but the filer store, which it reads as a Valkey. */
const SEAWEEDFS_SCRAPED = [COMPONENTS.gateway, COMPONENTS.master, COMPONENTS.volumeServer, COMPONENTS.filer];

/** A Valkey it reads: its receiver, its name, the user it logs in as, if not the default, the Secret of its password, and the variable it is given that in. */
interface ReadValkey {
  readonly receiver: string;
  readonly name: string;
  readonly username?: string;
  readonly secret: string;
  readonly variable: string;
}

/** The Valkeys it reads: workspace storage's, as its default user, and the filer store's, as the user that may read its INFO alone. */
function valkeys(config: InstallConfig): ReadValkey[] {
  const { collector } = FILER_STORE_USERS;
  return [
    ...(config.workspaceStorage.enabled ? [{ receiver: "redis", name: VALKEY, secret: VALKEY, variable: "VALKEY_PASSWORD" }] : []),
    ...(seaweedfsRuns(config)
      ? [{ receiver: "redis/filer-store", name: FILER_STORE, username: collector.name, secret: collector.secret, variable: "FILER_STORE_PASSWORD" }]
      : []),
  ];
}

/**
 * The scrape job `job` of the pods of `namespace` whose component matches `components`:
 * each running one at its address, on `port`, named by its pod, and labelled with its
 * component where `labelled`. The collector reads `$` as the start of a variable, and
 * `$$` as a `$`.
 */
function podJob(job: string, namespace: string, components: readonly string[], port: number, labelled = false): object {
  return {
    job_name: job,
    scrape_interval: SCRAPE_INTERVAL,
    kubernetes_sd_configs: [{ role: "pod", namespaces: { names: [namespace] } }],
    relabel_configs: [
      { source_labels: ["__meta_kubernetes_pod_label_app_kubernetes_io_component"], regex: components.join("|"), action: "keep" },
      { source_labels: ["__meta_kubernetes_pod_phase"], regex: "Running", action: "keep" },
      { source_labels: ["__meta_kubernetes_pod_ip"], target_label: "__address__", replacement: `$$1:${port}` },
      { source_labels: ["__meta_kubernetes_pod_name"], target_label: "pod" },
      ...(labelled ? [{ source_labels: ["__meta_kubernetes_pod_label_app_kubernetes_io_component"], target_label: "component" }] : []),
    ],
  };
}

/** JuiceFS's pods of the name `name` in the driver's namespace, on the port `port` they serve their metrics on. */
function juicefsJob(config: InstallConfig, job: string, name: string, port: number): object {
  return {
    job_name: job,
    scrape_interval: SCRAPE_INTERVAL,
    kubernetes_sd_configs: [{
      role: "pod",
      namespaces: { names: [config.workspaceStorage.csi.namespace] },
      selectors: [{ role: "pod", label: `app.kubernetes.io/name=${name}` }],
    }],
    relabel_configs: [
      { source_labels: ["__meta_kubernetes_pod_phase"], regex: "Running", action: "keep" },
      { source_labels: ["__meta_kubernetes_pod_ip"], target_label: "__address__", replacement: `$$1:${port}` },
      { source_labels: ["__meta_kubernetes_pod_name"], target_label: "pod" },
    ],
  };
}

/** What it scrapes, a job of each component's pods, by the job's name, which its metrics' service is named. */
function scrapes(config: InstallConfig): object[] {
  const { neon, workspaceStorage } = config;
  const stack: Record<string, readonly [components: readonly string[], port: number]> = {
    ...(neon.enabled
      ? {
        pageserver: [["neon-pageserver"], 9898],
        "storage-controller": [["neon-storage-controller"], 1234],
        "storage-broker": [["neon-storage-broker"], 50051],
        // The primary and, where the lake runs, its read replica.
        compute: [["neon-compute", "neon-compute-ro"], COMPUTE_CTL_PORT],
        // Each replica's own wakes and sleeps, as any of them wakes a compute.
        "neon-control": [["neon-control"], 8080],
        proxy: [["neon-proxy"], 7001],
        safekeeper: [["neon-safekeeper"], 7676],
      } as const
      : {}),
    ...(lakeRuns(config)
      ? { "lake-loader": [[LOADER_COMPONENT], LAKE_METRICS_PORT], "lake-query": [[QUERY_COMPONENT], LAKE_QUERY_PORT], "lake-intake": [[INTAKE_COMPONENT], LAKE_METRICS_PORT] } as const
      : {}),
    log: [["log"], LOG_METRICS_PORT],
    webhook: [["webhook"], WEBHOOK_HEALTH_PORT],
  };
  return [
    ...Object.keys(stack).sort().map((job) => {
      const [components, port] = stack[job] ?? [[], 0];
      return podJob(job, NAMESPACE, components, port);
    }),
    // The object store's parts, each labelled with its part.
    ...(seaweedfsRuns(config) ? [podJob("seaweedfs", NAMESPACE, SEAWEEDFS_SCRAPED, SEAWEEDFS_METRICS_PORT, true)] : []),
    // The mount pods, the clients, and the controller's and node service's, the driver.
    ...(workspaceStorage.enabled
      ? [juicefsJob(config, "juicefs", MOUNT_POD_NAME, JUICEFS_METRICS_PORT), juicefsJob(config, "juicefs-csi", DRIVER_POD_NAME, DRIVER_METRICS_PORT)]
      : []),
  ];
}

/** The exporter to telemetry.otlpEndpoint, by its name in the collector's configuration, in the protocol it is set to, with its headers. */
function externalExporter({ telemetry }: InstallConfig): Record<string, object> {
  const { otlpEndpoint: endpoint, otlpProtocol, headersSecret } = telemetry;
  const headers = headersSecret ? { headers: "${file:/etc/otelcol/headers/headers.yaml}" } : {};
  if (otlpProtocol === "grpc") return { "otlp/external": { endpoint, tls: { insecure: endpoint.startsWith("http://") }, ...headers } };
  return { "otlphttp/external": { endpoint, encoding: otlpProtocol === "http/json" ? "json" : "proto", ...headers } };
}

/** What the gateway is given, and the scraper: its health, its OTLP receiver, and none of its own metrics. */
const BASE = {
  extensions: { health_check: { endpoint: "0.0.0.0:13133" } },
  receivers: { otlp: { protocols: { http: { endpoint: `0.0.0.0:${COLLECTOR_PORT}` } } } },
};
const SERVICE = { extensions: ["health_check"], telemetry: { metrics: { level: "none" } } };

/**
 * The gateway's configuration: every signal from its OTLP receiver straight to the log,
 * each to its topic, acked by every replica the log keeps of it, unbatched and unqueued,
 * so its producer's export is answered by the log's; and to the external endpoint.
 */
function gatewayConfig(config: InstallConfig): object {
  const exporters = {
    ...(lakeRuns(config)
      ? {
        kafka: {
          brokers: [LOG_BROKERS],
          protocol_version: "4.2.0",
          auth: { sasl: { username: "gateway", password: "${env:LOG_PASSWORD}", mechanism: "SCRAM-SHA-512" } },
          traces: { topic: TELEMETRY_TOPICS.traces, encoding: "otlp_proto" },
          logs: { topic: TELEMETRY_TOPICS.logs, encoding: "otlp_proto" },
          metrics: { topic: TELEMETRY_TOPICS.metrics, encoding: "otlp_proto" },
          producer: { required_acks: -1, max_message_bytes: LOG_RECORD_BYTES },
          timeout: "10s",
          sending_queue: { enabled: false },
          retry_on_failure: { enabled: false },
        },
      }
      : {}),
    ...(config.telemetry.otlpEndpoint ? externalExporter(config) : {}),
  };
  // Nothing between: a log record without an id of its own the lake's intake knows by what it
  // is (neon/lake/src/otel.ts logRecordId), so the same record sent twice is one.
  const sent = { receivers: ["otlp"], exporters: Object.keys(exporters) };
  return { ...BASE, exporters, service: { ...SERVICE, pipelines: { traces: sent, logs: sent, metrics: sent } } };
}

/** The scraper's configuration: what it scrapes and reads, of the stack's, batched a minute's at a time, to the gateway. */
function scraperConfig(scraped: readonly object[], read: readonly ReadValkey[]): object {
  return {
    extensions: BASE.extensions,
    receivers: {
      ...(scraped.length > 0 ? { prometheus: { config: { scrape_configs: scraped } } } : {}),
      ...Object.fromEntries(read.map(({ receiver, name, username, variable }) => [receiver, {
        endpoint: `${name}:${VALKEY_PORT}`,
        ...(username ? { username } : {}),
        password: `\${env:${variable}}`,
        collection_interval: SCRAPE_INTERVAL,
        // How near it is to refusing writes: used memory against this.
        metrics: { "redis.maxmemory": { enabled: true } },
        // Which Valkey it is, its Service's name, as the Stack health dashboard and the alert on their memory tell them.
        resource_attributes: { "server.address": { enabled: true } },
      }])),
    },
    processors: {
      batch: METRICS_BATCH,
      // What the scraper scrapes is the stack's.
      "resource/stack": { attributes: [{ key: "service.namespace", value: "alasio-neon", action: "upsert" }] },
    },
    exporters: { "otlphttp/gateway": { endpoint: `http://${COLLECTOR}:${COLLECTOR_PORT}` } },
    service: {
      ...SERVICE,
      pipelines: {
        metrics: {
          receivers: [...(scraped.length > 0 ? ["prometheus"] : []), ...read.map(({ receiver }) => receiver)],
          processors: ["resource/stack", "batch"],
          exporters: ["otlphttp/gateway"],
        },
      },
    },
  };
}

/** What a collector's container is given: its variables, its resources, its ports beside its health's, and whether it mounts the external exporter's headers. */
interface CollectorContainer {
  readonly env: V1EnvVar[];
  readonly resources: V1Container["resources"];
  readonly ports: NonNullable<V1Container["ports"]>;
  readonly headers: boolean;
}

/** A collector's container, `name`, run on the configuration its ConfigMap mounts at /etc/otelcol. */
function collectorContainer(config: InstallConfig, name: string, { env, resources, ports, headers }: CollectorContainer): V1Container {
  return {
    name,
    image: imageReference(config.images.collector),
    imagePullPolicy: config.images.pullPolicy,
    args: ["--config=/etc/otelcol/config.yaml"],
    ...given("env", env),
    ports: [...ports, { name: "health", containerPort: 13133 }],
    readinessProbe: { httpGet: { path: "/", port: "health" }, periodSeconds: 5 },
    securityContext: { ...restrictedContainer(), readOnlyRootFilesystem: true },
    ...(resources ? { resources } : {}),
    volumeMounts: [
      { name: "config", mountPath: "/etc/otelcol", readOnly: true },
      ...(headers ? [{ name: "headers", mountPath: "/etc/otelcol/headers", readOnly: true }] : []),
    ],
  };
}

/**
 * The gateway's objects: who it is, its configuration, its Service, its Deployment, and what
 * scales and sizes it. Its ServiceAccount, of no rights and no token in its pods, is named in
 * its pods as it is: where a pod template names none, Kubernetes keeps the one it was named
 * before in serviceAccount, deprecated, which a template applied over an older one keeps, so
 * its pods would name a ServiceAccount the installation no longer has.
 */
function gatewayObjects(config: InstallConfig): KubernetesObject[] {
  const { telemetry } = config;
  const { scaling, resources } = telemetry.collector.gateway;
  const configuration = gatewayConfig(config);
  const metadata = { name: COLLECTOR, namespace: NAMESPACE, labels: stackLabels(GATEWAY_COMPONENT) };
  const configMap: V1ConfigMap = { apiVersion: "v1", kind: "ConfigMap", metadata, data: { "config.yaml": goJson(configuration, "  ") } };
  const service: V1Service = {
    apiVersion: "v1",
    kind: "Service",
    metadata,
    spec: { selector: selectorLabels(GATEWAY_COMPONENT), ports: [{ name: "otlp-http", port: COLLECTOR_PORT, targetPort: "otlp-http" }] },
  };
  const env: V1EnvVar[] = lakeRuns(config) ? [{ name: "LOG_PASSWORD", valueFrom: { secretKeyRef: { name: logPrincipalSecret("gateway"), key: "password" } } }] : [];
  const volumes: V1Volume[] = [
    { name: "config", configMap: { name: COLLECTOR } },
    // The external exporter's headers: a YAML map, under the key headersKey + ".yaml"
    // in the headers Secret, since the collector takes headers as a map.
    ...(telemetry.headersSecret
      ? [{ name: "headers", secret: { secretName: telemetry.headersSecret, items: [{ key: `${telemetry.headersKey}.yaml`, path: "headers.yaml" }] } }]
      : []),
  ];
  const deployment: V1Deployment = {
    apiVersion: "apps/v1",
    kind: "Deployment",
    metadata,
    spec: {
      ...replicasOf(scaling),
      selector: { matchLabels: selectorLabels(GATEWAY_COMPONENT) },
      template: {
        metadata: { labels: stackLabels(GATEWAY_COMPONENT), annotations: { "checksum/config": sha256(goJson(configuration)) } },
        spec: {
          serviceAccountName: COLLECTOR,
          automountServiceAccountToken: false,
          ...priority("floor"),
          ...stackPodSpec(config),
          ...spread(GATEWAY_COMPONENT, scaling),
          containers: [
            collectorContainer(config, "collector", { env, resources, ports: [{ name: "otlp-http", containerPort: COLLECTOR_PORT }], headers: Boolean(telemetry.headersSecret) }),
          ],
          volumes,
        },
      },
    },
  };
  const serviceAccount: V1ServiceAccount = { apiVersion: "v1", kind: "ServiceAccount", metadata, automountServiceAccountToken: false };
  return [
    serviceAccount,
    configMap,
    service,
    deployment,
    ...scaledObject(deployment, scaling, [cpuUtilization(80)]),
    ...replicaBudget(GATEWAY_COMPONENT, NAMESPACE, scaling),
    verticalPodAutoscaler(deployment, "InPlace", scaled(scaling) ? ["memory"] : undefined),
  ];
}

/** The scraper's objects, when it scrapes or reads anything: who it is, that it may list the pods it scrapes, its configuration and its Deployment. */
function scraperObjects(config: InstallConfig): KubernetesObject[] {
  const { workspaceStorage } = config;
  const scraped = scrapes(config);
  const read = valkeys(config);
  if (scraped.length === 0 && read.length === 0) return [];
  const configuration = scraperConfig(scraped, read);
  const metadata = { name: SCRAPER, namespace: NAMESPACE, labels: stackLabels(SCRAPER_COMPONENT) };
  const configMap: V1ConfigMap = { apiVersion: "v1", kind: "ConfigMap", metadata, data: { "config.yaml": goJson(configuration, "  ") } };
  const deployment: V1Deployment = {
    apiVersion: "apps/v1",
    kind: "Deployment",
    metadata,
    spec: {
      ...replicasOf(ONE),
      selector: { matchLabels: selectorLabels(SCRAPER_COMPONENT) },
      template: {
        metadata: { labels: stackLabels(SCRAPER_COMPONENT), annotations: { "checksum/config": sha256(goJson(configuration)) } },
        spec: {
          ...(scraped.length > 0 ? { serviceAccountName: SCRAPER } : {}),
          ...priority("floor"),
          ...stackPodSpec(config),
          containers: [
            collectorContainer(config, "scraper", {
              env: read.map(({ secret, variable }) => ({ name: variable, valueFrom: valkeyPassword(secret) })),
              resources: config.telemetry.collector.scraper.resources,
              ports: [],
              headers: false,
            }),
          ],
          volumes: [{ name: "config", configMap: { name: SCRAPER } }],
        },
      },
    },
  };
  const sized = [configMap, deployment, verticalPodAutoscaler(deployment, "InPlace")];
  if (scraped.length === 0) return sized;
  // Who the scraper is, and that it may find the pods it scrapes in the namespaces they are in: its own, and JuiceFS's driver's.
  const listed = [NAMESPACE, ...(workspaceStorage.enabled ? [workspaceStorage.csi.namespace] : [])];
  const serviceAccount: V1ServiceAccount = { apiVersion: "v1", kind: "ServiceAccount", metadata };
  const podLister = (namespace: string): KubernetesObject[] => {
    const inNamespace = { ...metadata, namespace };
    const role: V1Role = {
      apiVersion: "rbac.authorization.k8s.io/v1",
      kind: "Role",
      metadata: inNamespace,
      rules: [{ apiGroups: [""], resources: ["pods"], verbs: ["get", "list", "watch"] }],
    };
    const binding: V1RoleBinding = {
      apiVersion: "rbac.authorization.k8s.io/v1",
      kind: "RoleBinding",
      metadata: inNamespace,
      subjects: [{ kind: "ServiceAccount", name: SCRAPER, namespace: NAMESPACE }],
      roleRef: { apiGroup: "rbac.authorization.k8s.io", kind: "Role", name: SCRAPER },
    };
    return [role, binding];
  };
  return [serviceAccount, ...listed.flatMap(podLister), ...sized];
}

/** The collector's objects, when it runs: its gateway's, and its scraper's. */
export function collectorObjects(config: InstallConfig): KubernetesObject[] {
  if (!collectorRuns(config)) return [];
  return [...gatewayObjects(config), ...scraperObjects(config)];
}
