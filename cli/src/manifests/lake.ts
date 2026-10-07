/**
 * The analytics lake (neon/lake/src/model.ts): every transcript entry and rollout line,
 * loaded from alasio's database into DuckLake, as role `lake`, which alasio makes, and
 * alasio's telemetry, which the stack's collector sends its intake (./collector.ts); its
 * catalog in Neon and its Parquet files in the object store.
 *
 * Its pod runs the lake service, which writes it, and beside it its query endpoint,
 * through which Grafana and alasio lake read it (neon/lake/src/endpoint.ts), in a
 * container of its own, with resources of its own and only the reader's credentials.
 */
import type { KubernetesObject, V1Deployment, V1EnvVar, V1Service } from "@kubernetes/client-node";

import { imageReference } from "../images.ts";
import { componentName, lakeRuns, NAMESPACE, neonName, otelEnv, restrictedContainer, s3Endpoint, selectorLabels, stackLabels, stackPodSpec } from "./common.ts";
import type { InstallConfig } from "./config.ts";

/** The port its telemetry intake takes OTLP over HTTP on. */
export const LAKE_INTAKE_PORT = 4318;
/** The port its query endpoint takes queries on. */
export const LAKE_QUERY_PORT = 8090;

/** The lake's objects, when it runs, which is beside Neon. */
export function lakeObjects(config: InstallConfig): KubernetesObject[] {
  if (!lakeRuns(config)) return [];
  const name = componentName("lake");
  const service: V1Service = {
    apiVersion: "v1",
    kind: "Service",
    metadata: { name, namespace: NAMESPACE, labels: stackLabels("lake") },
    spec: {
      selector: selectorLabels("lake"),
      ports: [{ name: "metrics", port: 9464 }, { name: "otlp-http", port: LAKE_INTAKE_PORT }, { name: "query", port: LAKE_QUERY_PORT }],
    },
  };
  // Where the lake is, which both containers open it at.
  const lakeEnv: V1EnvVar[] = [
    { name: "LAKE_DATABASE_HOST", value: neonName("compute") },
    { name: "LAKE_DATABASE_PORT", value: "55433" },
    { name: "LAKE_DATA_PATH", value: `s3://${config.objectStore.buckets.lake}/` },
    { name: "LAKE_S3_ENDPOINT", value: s3Endpoint(config) },
    { name: "HOME", value: "/tmp" },
  ];
  const secret = (variable: string): V1EnvVar => ({ name: variable, valueFrom: { secretKeyRef: { name, key: variable } } });
  const image = imageReference(config.images.lake);
  // Live while its loads are not failing for long; ready once its intake has the lake open.
  const probe = (path: string) => ({ httpGet: { path, port: "metrics" } });
  const deployment: V1Deployment = {
    apiVersion: "apps/v1",
    kind: "Deployment",
    metadata: { name, namespace: NAMESPACE, labels: stackLabels("lake") },
    spec: {
      replicas: 1,
      // One loader: two would race each other's loads.
      strategy: { type: "Recreate" },
      selector: { matchLabels: selectorLabels("lake") },
      template: {
        metadata: { labels: stackLabels("lake") },
        spec: {
          ...stackPodSpec(config),
          terminationGracePeriodSeconds: 60,
          containers: [
            {
              name: "lake",
              image,
              imagePullPolicy: config.images.pullPolicy,
              env: [
                ...lakeEnv,
                { name: "LAKE_RETENTION_DAYS", value: String(config.telemetry.retentionDays) },
                // Whose files it keeps: while Neon has branches, their lakes read its files.
                { name: "LAKE_BRANCHES_URL", value: `http://${neonName("control")}:8080/branches` },
                ...otelEnv(config),
              ],
              envFrom: [{ secretRef: { name } }],
              ports: [{ name: "metrics", containerPort: 9464 }, { name: "otlp-http", containerPort: LAKE_INTAKE_PORT }],
              readinessProbe: { ...probe("/readyz"), periodSeconds: 10 },
              livenessProbe: { ...probe("/healthz"), initialDelaySeconds: 60, periodSeconds: 30, failureThreshold: 3 },
              securityContext: { ...restrictedContainer(), readOnlyRootFilesystem: true },
              resources: config.lake.resources,
              volumeMounts: [{ name: "tmp", mountPath: "/tmp" }],
            },
            {
              name: "query",
              image,
              imagePullPolicy: config.images.pullPolicy,
              command: ["node", "src/endpoint.ts"],
              env: [
                ...lakeEnv,
                ...["LAKE_READER_PASSWORD", "LAKE_READER_S3_KEY", "LAKE_READER_S3_SECRET", "LAKE_QUERY_TOKEN"].map(secret),
                { name: "LAKE_QUERY_PORT", value: String(LAKE_QUERY_PORT) },
                ...otelEnv(config),
                { name: "OTEL_SERVICE_NAME", value: "alasio-lake-query" },
              ],
              ports: [{ name: "query", containerPort: LAKE_QUERY_PORT }],
              // It serves whether or not the lake can be read, so the intake beside it is not held back.
              readinessProbe: { httpGet: { path: "/healthz", port: "query" }, periodSeconds: 10 },
              livenessProbe: { httpGet: { path: "/healthz", port: "query" }, periodSeconds: 30, failureThreshold: 3 },
              securityContext: { ...restrictedContainer(), readOnlyRootFilesystem: true },
              resources: config.lake.query.resources,
              volumeMounts: [{ name: "query-tmp", mountPath: "/tmp" }],
            },
          ],
          volumes: [{ name: "tmp", emptyDir: { sizeLimit: "4Gi" } }, { name: "query-tmp", emptyDir: { sizeLimit: "2Gi" } }],
        },
      },
    },
  };
  return [service, deployment];
}
