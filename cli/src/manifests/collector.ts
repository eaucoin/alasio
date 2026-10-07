/**
 * The stack's telemetry collector, the one hub of alasio's telemetry: alasio, its
 * harnesses, bayma (folder workspaces' directly, sessions' through alasio's receiver),
 * the lake and Neon's compute export to it over OTLP/HTTP (common.ts's otelEnv), and it
 * scrapes the rest: every Neon service's, SeaweedFS's and the lake's Prometheus metrics,
 * and with workspace storage JuiceFS's and Valkey's. All of it goes to the lake's
 * telemetry intake (neon/lake/src/intake.ts), and to telemetry.otlpEndpoint too when one
 * is set, which alone is sent the configured headers. It runs whenever the lake does,
 * or an endpoint is set; with neither, nothing exports anything.
 *
 * It batches and retries in memory, so a restart of it may lose a few seconds of
 * telemetry. JuiceFS's metrics are its driver's and mount pods', found among the pods of
 * the driver's namespace, which the collector may list; Valkey's are read with its
 * password. Its pod carries its configuration's checksum, so it is replaced when that
 * changes.
 */
import type { KubernetesObject, V1ConfigMap, V1Deployment, V1Role, V1RoleBinding, V1Service, V1ServiceAccount } from "@kubernetes/client-node";

import { imageReference } from "../images.ts";
import {
  COLLECTOR_PORT,
  collectorRuns,
  componentName,
  goJson,
  lakeRuns,
  NAMESPACE,
  neonName,
  restrictedContainer,
  selectorLabels,
  sha256,
  stackLabels,
  stackPodSpec,
} from "./common.ts";
import type { InstallConfig } from "./config.ts";
import { DRIVER_METRICS_PORT, DRIVER_POD_NAME, JUICEFS_METRICS_PORT, MOUNT_POD_NAME } from "./juicefs-csi.ts";
import { LAKE_INTAKE_PORT } from "./lake.ts";
import { VALKEY, VALKEY_PORT } from "./valkey.ts";

/** The collector's name, its pods' component, and its Service's. */
export const COLLECTOR = componentName("collector");
const COMPONENT = "collector";

/**
 * The most items (spans, data points, log records) it batches into one request, which
 * it sends at the latest 5 seconds after the first: as many as one insert of the lake
 * inlines at most (neon/lake/src/otel.ts's INLINED_ROWS), so no batch makes a file.
 */
const BATCH_ITEMS = 1000;

/** What it scrapes: each job's targets, by job, and with workspace storage JuiceFS's pods. */
function scrapes(config: InstallConfig): object[] {
  const { neon, workspaceStorage } = config;
  if (!neon.enabled) return [];
  const safekeeper = neonName("safekeeper");
  const targets: Record<string, string[]> = {
    pageserver: [`${neonName("pageserver")}:9898`],
    "storage-controller": [`${neonName("storage-controller")}:1234`],
    "storage-broker": [`${neonName("storage-broker")}:50051`],
    compute: [`${neonName("compute")}:3080`],
    safekeeper: Array.from({ length: neon.safekeepers.replicas }, (_, index) => `${safekeeper}-${index}.${safekeeper}:7676`),
    ...(config.objectStore.bundled.enabled ? { seaweedfs: [`${componentName("seaweedfs")}:9327`] } : {}),
    ...(lakeRuns(config) ? { lake: [`${componentName("lake")}:9464`] } : {}),
  };
  // JuiceFS's pods of the name `name` in the driver's namespace: each running one at its
  // address, on the port `port` they serve their metrics on, named by its pod. The
  // collector reads `$` as the start of a variable, and `$$` as a `$`.
  const juicefsJob = (job: string, name: string, port: number) => ({
    job_name: job,
    scrape_interval: "30s",
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
  return [
    ...Object.keys(targets).sort().map((job) => ({ job_name: job, scrape_interval: "30s", static_configs: [{ targets: targets[job] }] })),
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
  const { telemetry, workspaceStorage } = config;
  const scraped = scrapes(config);
  const exporters = {
    ...(lakeRuns(config) ? { "otlphttp/lake": { endpoint: `http://${componentName("lake")}:${LAKE_INTAKE_PORT}` } } : {}),
    ...(telemetry.otlpEndpoint ? externalExporter(config) : {}),
  };
  const sent = { receivers: ["otlp"], processors: ["batch"], exporters: Object.keys(exporters) };
  return {
    extensions: { health_check: { endpoint: "0.0.0.0:13133" } },
    receivers: {
      otlp: { protocols: { http: { endpoint: `0.0.0.0:${COLLECTOR_PORT}` } } },
      ...(scraped.length > 0 ? { prometheus: { config: { scrape_configs: scraped } } } : {}),
      ...(workspaceStorage.enabled
        ? {
          redis: {
            endpoint: `${VALKEY}:${VALKEY_PORT}`,
            password: "${env:VALKEY_PASSWORD}",
            collection_interval: "30s",
            // How near it is to refusing writes: used memory against this.
            metrics: { "redis.maxmemory": { enabled: true } },
          },
        }
        : {}),
    },
    processors: {
      batch: { send_batch_size: BATCH_ITEMS, send_batch_max_size: BATCH_ITEMS, timeout: "5s" },
      // What the collector scrapes is the stack's.
      ...(scraped.length > 0 ? { "resource/stack": { attributes: [{ key: "service.namespace", value: "alasio-neon", action: "upsert" }] } } : {}),
    },
    exporters,
    service: {
      extensions: ["health_check"],
      telemetry: { metrics: { level: "none" } },
      pipelines: {
        traces: sent,
        logs: sent,
        metrics: sent,
        ...(scraped.length > 0
          ? {
            "metrics/stack": {
              receivers: ["prometheus", ...(workspaceStorage.enabled ? ["redis"] : [])],
              processors: ["resource/stack", "batch"],
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
      replicas: 1,
      selector: { matchLabels: selectorLabels(COMPONENT) },
      template: {
        metadata: { labels: stackLabels(COMPONENT), annotations: { "checksum/config": sha256(goJson(configuration)) } },
        spec: {
          ...(workspaceStorage.enabled ? { serviceAccountName: COLLECTOR } : {}),
          ...stackPodSpec(config),
          containers: [{
            name: "collector",
            image: imageReference(telemetry.collector.image),
            imagePullPolicy: "IfNotPresent",
            args: ["--config=/etc/otelcol/config.yaml"],
            ...(workspaceStorage.enabled ? { env: [{ name: "VALKEY_PASSWORD", valueFrom: { secretKeyRef: { name: VALKEY, key: "password" } } }] } : {}),
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
  if (!workspaceStorage.enabled) return [configMap, service, deployment];
  // Who the collector is, and that it may find JuiceFS's pods in the driver's namespace.
  const serviceAccount: V1ServiceAccount = { apiVersion: "v1", kind: "ServiceAccount", metadata };
  const driverMetadata = { ...metadata, namespace: workspaceStorage.csi.namespace };
  const role: V1Role = {
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "Role",
    metadata: driverMetadata,
    rules: [{ apiGroups: [""], resources: ["pods"], verbs: ["get", "list", "watch"] }],
  };
  const binding: V1RoleBinding = {
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "RoleBinding",
    metadata: driverMetadata,
    subjects: [{ kind: "ServiceAccount", name: COLLECTOR, namespace: NAMESPACE }],
    roleRef: { apiGroup: "rbac.authorization.k8s.io", kind: "Role", name: COLLECTOR },
  };
  return [serviceAccount, role, binding, configMap, service, deployment];
}
