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
import type { KubernetesObject, V1Container, V1Deployment, V1EnvVar, V1Service } from "@kubernetes/client-node";

import { computeId, MAIN, replicaId } from "../../../neon/control/branches.ts";
import { imageReference } from "../images.ts";
import {
  componentName,
  type Environment,
  lakeRuns,
  MAIN_ENVIRONMENT,
  neonName,
  ONE,
  otelEnv,
  replicasOf,
  restrictedContainer,
  s3Endpoint,
  selectorLabels,
  stackLabels,
  stackPodSpec,
} from "./common.ts";
import type { InstallConfig } from "./config.ts";
import { PROXY_HOST, PROXY_PORT } from "./neon.ts";
import { priority } from "./priorities.ts";
import { verticalPodAutoscaler } from "./sizing.ts";

/** The port its telemetry intake takes OTLP over HTTP on. */
export const LAKE_INTAKE_PORT = 4318;
/** The port its query endpoint takes queries on. */
export const LAKE_QUERY_PORT = 8090;

/**
 * The lake's objects, when it runs, which is beside Neon. A branch environment's is the
 * branch's copy of the catalog on main's files, which it reads where they are and writes
 * beside them, and maintains none of: main's maintenance keeps every file while Neon has
 * branches, which it asks neon-control about (neon/lake/src/sync.ts). A branch's has no
 * query endpoint, as it has no Grafana: what it sends is read in main's lake, tagged.
 */
export function lakeObjects(config: InstallConfig, environment: Environment = MAIN_ENVIRONMENT): KubernetesObject[] {
  if (!lakeRuns(config)) return [];
  const name = componentName("lake");
  const main = environment.branch === null;
  const service: V1Service = {
    apiVersion: "v1",
    kind: "Service",
    metadata: { name, namespace: environment.namespace, labels: stackLabels("lake") },
    spec: {
      selector: selectorLabels("lake"),
      ports: [{ name: "metrics", port: 9464 }, { name: "otlp-http", port: LAKE_INTAKE_PORT }, ...(main ? [{ name: "query", port: LAKE_QUERY_PORT }] : [])],
    },
  };
  // Where the lake is, which both containers open it at.
  const lakeEnv: V1EnvVar[] = [
    // Its environment's compute, through Neon's proxy, which wakes it if it sleeps.
    { name: "LAKE_DATABASE_HOST", value: PROXY_HOST },
    { name: "LAKE_DATABASE_PORT", value: String(PROXY_PORT) },
    { name: "LAKE_DATABASE_ENDPOINT", value: computeId(environment.branch ?? MAIN) },
    { name: "LAKE_DATA_PATH", value: `s3://${config.objectStore.buckets.lake}/` },
    { name: "LAKE_S3_ENDPOINT", value: s3Endpoint(config) },
    { name: "HOME", value: "/tmp" },
  ];
  const secret = (variable: string): V1EnvVar => ({ name: variable, valueFrom: { secretKeyRef: { name, key: variable } } });
  const image = imageReference(config.images.lake);
  // Its query endpoint, which Grafana and alasio lake read main's lake through; a branch environment has neither.
  const query: V1Container = {
    name: "query",
    image,
    imagePullPolicy: config.images.pullPolicy,
    command: ["node", "src/endpoint.ts"],
    env: [
      ...lakeEnv,
      ...["LAKE_READER_PASSWORD", "LAKE_READER_S3_KEY", "LAKE_READER_S3_SECRET", "LAKE_QUERY_TOKEN"].map(secret),
      { name: "LAKE_QUERY_PORT", value: String(LAKE_QUERY_PORT) },
      ...otelEnv(config, environment),
      { name: "OTEL_SERVICE_NAME", value: "alasio-lake-query" },
    ],
    ports: [{ name: "query", containerPort: LAKE_QUERY_PORT }],
    // It serves whether or not the lake can be read, so the intake beside it is not held back.
    readinessProbe: { httpGet: { path: "/healthz", port: "query" }, periodSeconds: 10 },
    livenessProbe: { httpGet: { path: "/healthz", port: "query" }, periodSeconds: 30, failureThreshold: 3 },
    securityContext: { ...restrictedContainer(), readOnlyRootFilesystem: true },
    resources: config.lake.query.resources,
    volumeMounts: [{ name: "query-tmp", mountPath: "/tmp" }],
  };
  // Live while its loads are not failing for long; ready once its intake has the lake open.
  const probe = (path: string) => ({ httpGet: { path, port: "metrics" } });
  const deployment: V1Deployment = {
    apiVersion: "apps/v1",
    kind: "Deployment",
    metadata: { name, namespace: environment.namespace, labels: stackLabels("lake") },
    spec: {
      ...replicasOf(ONE),
      // One loader: two would race each other's loads.
      strategy: { type: "Recreate" },
      selector: { matchLabels: selectorLabels("lake") },
      template: {
        metadata: { labels: stackLabels("lake") },
        spec: {
          // Main's serves Grafana's alerts, which never sleep; a branch's nothing.
          ...priority(main ? "floor" : "component"),
          ...stackPodSpec(config),
          terminationGracePeriodSeconds: 60,
          containers: [
            {
              name: "lake",
              image,
              imagePullPolicy: config.images.pullPolicy,
              env: [
                ...lakeEnv,
                // alasio's database, which it loads from, on its environment's read replica, which
                // takes nothing of what alasio's own compute serves alasio.
                { name: "LAKE_SOURCE_ENDPOINT", value: replicaId(environment.branch ?? MAIN) },
                { name: "LAKE_RETENTION_DAYS", value: String(config.telemetry.retentionDays) },
                // Main's keeps its files while Neon has branches, whose lakes read them; a branch's is never maintained.
                ...(main ? [{ name: "LAKE_BRANCHES_URL", value: `http://${neonName("control")}:8080/branches` }] : [{ name: "LAKE_MAINTENANCE_HOURS", value: "0" }]),
                ...otelEnv(config, environment),
              ],
              envFrom: [{ secretRef: { name } }],
              ports: [{ name: "metrics", containerPort: 9464 }, { name: "otlp-http", containerPort: LAKE_INTAKE_PORT }],
              readinessProbe: { ...probe("/readyz"), periodSeconds: 10 },
              livenessProbe: { ...probe("/healthz"), initialDelaySeconds: 60, periodSeconds: 30, failureThreshold: 3 },
              securityContext: { ...restrictedContainer(), readOnlyRootFilesystem: true },
              resources: config.lake.resources,
              volumeMounts: [{ name: "tmp", mountPath: "/tmp" }],
            },
            ...(main ? [query] : []),
          ],
          volumes: [{ name: "tmp", emptyDir: { sizeLimit: "4Gi" } }, ...(main ? [{ name: "query-tmp", emptyDir: { sizeLimit: "2Gi" } }] : [])],
        },
      },
    },
  };
  return [service, deployment, verticalPodAutoscaler(deployment, "InPlace")];
}
