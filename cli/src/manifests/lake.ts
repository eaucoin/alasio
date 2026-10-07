/**
 * The analytics lake (neon/lake/src/model.ts): every transcript entry and rollout line,
 * loaded from alasio's database into DuckLake, as role `lake`, which alasio makes, and
 * alasio's telemetry, which the stack's collector sends its intake (./collector.ts); its
 * catalog in Neon and its Parquet files in the object store.
 */
import type { KubernetesObject, V1Deployment, V1Service } from "@kubernetes/client-node";

import { imageReference } from "../images.ts";
import { componentName, lakeRuns, NAMESPACE, neonName, otelEnv, restrictedContainer, s3Endpoint, selectorLabels, stackLabels, stackPodSpec } from "./common.ts";
import type { InstallConfig } from "./config.ts";

/** The port its telemetry intake takes OTLP over HTTP on. */
export const LAKE_INTAKE_PORT = 4318;

/** The lake's objects, when it runs, which is beside Neon. */
export function lakeObjects(config: InstallConfig): KubernetesObject[] {
  if (!lakeRuns(config)) return [];
  const name = componentName("lake");
  const service: V1Service = {
    apiVersion: "v1",
    kind: "Service",
    metadata: { name, namespace: NAMESPACE, labels: stackLabels("lake") },
    spec: { selector: selectorLabels("lake"), ports: [{ name: "metrics", port: 9464 }, { name: "otlp-http", port: LAKE_INTAKE_PORT }] },
  };
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
          containers: [{
            name: "lake",
            image: imageReference(config.images.lake),
            imagePullPolicy: config.images.pullPolicy,
            env: [
              { name: "LAKE_DATABASE_HOST", value: neonName("compute") },
              { name: "LAKE_DATABASE_PORT", value: "55433" },
              { name: "LAKE_DATA_PATH", value: `s3://${config.objectStore.buckets.lake}/` },
              { name: "LAKE_S3_ENDPOINT", value: s3Endpoint(config) },
              { name: "LAKE_RETENTION_DAYS", value: String(config.telemetry.retentionDays) },
              { name: "HOME", value: "/tmp" },
              ...otelEnv(config),
            ],
            envFrom: [{ secretRef: { name } }],
            ports: [{ name: "metrics", containerPort: 9464 }, { name: "otlp-http", containerPort: LAKE_INTAKE_PORT }],
            readinessProbe: { ...probe("/readyz"), periodSeconds: 10 },
            livenessProbe: { ...probe("/healthz"), initialDelaySeconds: 60, periodSeconds: 30, failureThreshold: 3 },
            securityContext: { ...restrictedContainer(), readOnlyRootFilesystem: true },
            resources: config.lake.resources,
            volumeMounts: [{ name: "tmp", mountPath: "/tmp" }],
          }],
          volumes: [{ name: "tmp", emptyDir: { sizeLimit: "4Gi" } }],
        },
      },
    },
  };
  return [service, deployment];
}
