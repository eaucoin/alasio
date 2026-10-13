/**
 * The stack's telemetry collector, the one hub of alasio's telemetry: alasio, its
 * harnesses, bayma (folder workspaces' directly, sessions' through alasio's receiver),
 * the lake and Neon's compute export to it over OTLP/HTTP (common.ts's otelEnv), and it
 * scrapes the rest: every Neon service's, each of SeaweedFS's parts' and the lake's
 * Prometheus metrics, and with workspace storage JuiceFS's; and it reads the Valkeys', the
 * object store's filer store's and workspace storage's. All of it goes to the lake's
 * telemetry intake (neon/lake/src/intake.ts), and to telemetry.otlpEndpoint too when one
 * is set, which alone is sent the configured headers. It runs whenever the lake does,
 * or an endpoint is set; with neither, nothing exports anything.
 *
 * It batches and retries in memory, so a restart of it may lose a few seconds of
 * telemetry. JuiceFS's metrics are its driver's and mount pods', found among the pods of
 * the driver's namespace, and SeaweedFS's its parts' pods', found among its own, which the
 * collector may list; each Valkey's are read with its password. Its pod carries its
 * configuration's checksum, so it is replaced when that changes.
 */
import type { KubernetesObject, V1ConfigMap, V1Deployment, V1Role, V1RoleBinding, V1Service, V1ServiceAccount } from "@kubernetes/client-node";

import { imageReference } from "../images.ts";
import {
  COLLECTOR_PORT,
  collectorRuns,
  componentName,
  given,
  goJson,
  lakeRuns,
  NAMESPACE,
  neonName,
  ONE,
  replicasOf,
  restrictedContainer,
  selectorLabels,
  sha256,
  stackLabels,
  stackPodSpec,
} from "./common.ts";
import type { InstallConfig } from "./config.ts";
import { priority } from "./priorities.ts";
import { verticalPodAutoscaler } from "./sizing.ts";
import { DRIVER_METRICS_PORT, DRIVER_POD_NAME, JUICEFS_METRICS_PORT, MOUNT_POD_NAME } from "./juicefs-csi.ts";
import { LAKE_INTAKE_PORT } from "./lake.ts";
import { COMPONENTS, FILER_STORE, FILER_STORE_USERS, METRICS_PORT as SEAWEEDFS_METRICS_PORT } from "./object-store.ts";
import { VALKEY_PORT, valkeyPassword } from "./valkey.ts";
import { VALKEY } from "./workspace-storage.ts";

/** The collector's name, its pods' component, and its Service's. */
export const COLLECTOR = componentName("collector");
const COMPONENT = "collector";

/**
 * How often it scrapes the stack. The stack's metrics are most of all the telemetry
 * there is (the pageserver's alone half of it), some 30 points a second at this interval.
 */
const SCRAPE_INTERVAL = "60s";

/**
 * How it batches spans and log records: a request of at most as many as one insert of
 * the lake inlines (neon/lake/src/otel.ts's INLINED_ROWS), within 5 seconds of the first,
 * so those few, which a query wants at once, are inlined in the catalog and make no file.
 */
const BATCH = { send_batch_size: 1000, send_batch_max_size: 1000, timeout: "5s" };

/**
 * How it batches metric points: a minute's at a time, a request of thousands, which the
 * lake writes as a file of each kind's rather than inline in its catalog. Measured on the
 * stack, inlining them all cost Neon some 45 KB of WAL a second and the catalog 130 MB an
 * hour until flushed; a request every 5 seconds not inlined, 50 files a minute.
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

/** What it scrapes: each job's targets, by job, the object store's parts, and with workspace storage JuiceFS's pods. */
function scrapes(config: InstallConfig): object[] {
  const { neon, workspaceStorage } = config;
  if (!neon.enabled) return [];
  const safekeeper = neonName("safekeeper");
  const targets: Record<string, string[]> = {
    pageserver: [`${neonName("pageserver")}:9898`],
    "storage-controller": [`${neonName("storage-controller")}:1234`],
    "storage-broker": [`${neonName("storage-broker")}:50051`],
    compute: [`${neonName("compute")}:3080`],
    safekeeper: Array.from({ length: neon.safekeepers.scaling.min }, (_, index) => `${safekeeper}-${index}.${safekeeper}:7676`),
    ...(lakeRuns(config) ? { lake: [`${componentName("lake")}:9464`] } : {}),
  };
  // JuiceFS's pods of the name `name` in the driver's namespace: each running one at its
  // address, on the port `port` they serve their metrics on, named by its pod. The
  // collector reads `$` as the start of a variable, and `$$` as a `$`.
  const juicefsJob = (job: string, name: string, port: number) => ({
    job_name: job,
    scrape_interval: SCRAPE_INTERVAL,
    kubernetes_sd_configs: [{
      role: "pod",
      namespaces: { names: [workspaceStorage.csi.namespace] },
      selectors: [{ role: "pod", label: `app.kubernetes.io/name=${name}` }],
    }],
    relabel_configs: [
      { source_labels: ["__meta_kubernetes_pod_phase"], regex: "Running", action: "keep" },
      { source_labels: ["__meta_kubernetes_pod_ip"], target_label: "__address__", replacement: `$$1:${port}` },
      { source_labels: ["__meta_kubernetes_pod_name"], target_label: "pod" },
    ],
  });
  // The object store's parts, each pod of each, which the collector may list in its own
  // namespace, as their gateways' count moves; each labelled with its part.
  const seaweedfsJob = {
    job_name: "seaweedfs",
    scrape_interval: SCRAPE_INTERVAL,
    kubernetes_sd_configs: [{ role: "pod", namespaces: { names: [NAMESPACE] } }],
    relabel_configs: [
      { source_labels: ["__meta_kubernetes_pod_label_app_kubernetes_io_component"], regex: SEAWEEDFS_SCRAPED.join("|"), action: "keep" },
      { source_labels: ["__meta_kubernetes_pod_phase"], regex: "Running", action: "keep" },
      { source_labels: ["__meta_kubernetes_pod_ip"], target_label: "__address__", replacement: `$$1:${SEAWEEDFS_METRICS_PORT}` },
      { source_labels: ["__meta_kubernetes_pod_name"], target_label: "pod" },
      { source_labels: ["__meta_kubernetes_pod_label_app_kubernetes_io_component"], target_label: "component" },
    ],
  };
  return [
    ...Object.keys(targets).sort().map((job) => ({ job_name: job, scrape_interval: SCRAPE_INTERVAL, static_configs: [{ targets: targets[job] }] })),
    ...(seaweedfsRuns(config) ? [seaweedfsJob] : []),
    // The mount pods, the clients, and the controller's and node service's, the driver.
    ...(workspaceStorage.enabled ? [juicefsJob("juicefs", MOUNT_POD_NAME, JUICEFS_METRICS_PORT), juicefsJob("juicefs-csi", DRIVER_POD_NAME, DRIVER_METRICS_PORT)] : []),
  ];
}

/** The exporter to telemetry.otlpEndpoint, by its name in the collector's configuration, in the protocol it is set to, with its headers. */
function externalExporter({ telemetry }: InstallConfig): Record<string, object> {
  const { otlpEndpoint: endpoint, otlpProtocol, headersSecret } = telemetry;
  const headers = headersSecret ? { headers: "${file:/etc/otelcol/headers/headers.yaml}" } : {};
  if (otlpProtocol === "grpc") return { "otlp/external": { endpoint, tls: { insecure: endpoint.startsWith("http://") }, ...headers } };
  return { "otlphttp/external": { endpoint, encoding: otlpProtocol === "http/json" ? "json" : "proto", ...headers } };
}

/** The collector's configuration. */
function collectorConfig(config: InstallConfig): object {
  const { telemetry } = config;
  const scraped = scrapes(config);
  const read = valkeys(config);
  const exporters = {
    ...(lakeRuns(config) ? { "otlphttp/lake": { endpoint: `http://${componentName("lake")}:${LAKE_INTAKE_PORT}` } } : {}),
    ...(telemetry.otlpEndpoint ? externalExporter(config) : {}),
  };
  const sent = (processor: string) => ({ receivers: ["otlp"], processors: [processor], exporters: Object.keys(exporters) });
  return {
    extensions: { health_check: { endpoint: "0.0.0.0:13133" } },
    receivers: {
      otlp: { protocols: { http: { endpoint: `0.0.0.0:${COLLECTOR_PORT}` } } },
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
      batch: BATCH,
      "batch/metrics": METRICS_BATCH,
      // What the collector scrapes is the stack's.
      ...(scraped.length > 0 ? { "resource/stack": { attributes: [{ key: "service.namespace", value: "alasio-neon", action: "upsert" }] } } : {}),
    },
    exporters,
    service: {
      extensions: ["health_check"],
      telemetry: { metrics: { level: "none" } },
      pipelines: {
        traces: sent("batch"),
        logs: sent("batch"),
        metrics: sent("batch/metrics"),
        ...(scraped.length > 0
          ? {
            "metrics/stack": {
              receivers: ["prometheus", ...read.map(({ receiver }) => receiver)],
              processors: ["resource/stack", "batch/metrics"],
              exporters: Object.keys(exporters),
            },
          }
          : {}),
      },
    },
  };
}

/** The collector's objects, when it runs. */
export function collectorObjects(config: InstallConfig): KubernetesObject[] {
  if (!collectorRuns(config)) return [];
  const { telemetry, workspaceStorage } = config;
  const configuration = collectorConfig(config);
  // The namespaces it finds pods to scrape in: the driver's, JuiceFS's, and its own, the object store's parts.
  const listed = [...(workspaceStorage.enabled ? [workspaceStorage.csi.namespace] : []), ...(seaweedfsRuns(config) ? [NAMESPACE] : [])];
  const metadata = { name: COLLECTOR, namespace: NAMESPACE, labels: stackLabels(COMPONENT) };
  const configMap: V1ConfigMap = { apiVersion: "v1", kind: "ConfigMap", metadata, data: { "config.yaml": goJson(configuration, "  ") } };
  const service: V1Service = {
    apiVersion: "v1",
    kind: "Service",
    metadata,
    spec: { selector: selectorLabels(COMPONENT), ports: [{ name: "otlp-http", port: COLLECTOR_PORT, targetPort: "otlp-http" }] },
  };
  const deployment: V1Deployment = {
    apiVersion: "apps/v1",
    kind: "Deployment",
    metadata,
    spec: {
      ...replicasOf(ONE),
      selector: { matchLabels: selectorLabels(COMPONENT) },
      template: {
        metadata: { labels: stackLabels(COMPONENT), annotations: { "checksum/config": sha256(goJson(configuration)) } },
        spec: {
          ...(listed.length > 0 ? { serviceAccountName: COLLECTOR } : {}),
          ...priority("floor"),
          ...stackPodSpec(config),
          containers: [{
            name: "collector",
            image: imageReference(telemetry.collector.image),
            imagePullPolicy: "IfNotPresent",
            args: ["--config=/etc/otelcol/config.yaml"],
            ...given("env", valkeys(config).map(({ secret, variable }) => ({ name: variable, valueFrom: valkeyPassword(secret) }))),
            ports: [{ name: "otlp-http", containerPort: COLLECTOR_PORT }, { name: "health", containerPort: 13133 }],
            readinessProbe: { httpGet: { path: "/", port: "health" }, periodSeconds: 5 },
            securityContext: { ...restrictedContainer(), readOnlyRootFilesystem: true },
            resources: telemetry.collector.resources,
            volumeMounts: [
              { name: "config", mountPath: "/etc/otelcol", readOnly: true },
              ...(telemetry.headersSecret ? [{ name: "headers", mountPath: "/etc/otelcol/headers", readOnly: true }] : []),
            ],
          }],
          volumes: [
            { name: "config", configMap: { name: COLLECTOR } },
            // The external exporter's headers: a YAML map, under the key headersKey + ".yaml"
            // in the headers Secret, since the collector takes headers as a map.
            ...(telemetry.headersSecret
              ? [{ name: "headers", secret: { secretName: telemetry.headersSecret, items: [{ key: `${telemetry.headersKey}.yaml`, path: "headers.yaml" }] } }]
              : []),
          ],
        },
      },
    },
  };
  const sized = [deployment, verticalPodAutoscaler(deployment, "InPlace")];
  if (listed.length === 0) return [configMap, service, ...sized];
  // Who the collector is, and that it may find the pods it scrapes in the namespaces they are in.
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
      subjects: [{ kind: "ServiceAccount", name: COLLECTOR, namespace: NAMESPACE }],
      roleRef: { apiGroup: "rbac.authorization.k8s.io", kind: "Role", name: COLLECTOR },
    };
    return [role, binding];
  };
  return [serviceAccount, ...listed.flatMap(podLister), configMap, service, ...sized];
}
