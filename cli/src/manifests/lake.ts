/**
 * The analytics lake (neon/lake/src/model.ts): every transcript entry and rollout line,
 * loaded from alasio's database into DuckLake, as role `lake`, which alasio makes, and
 * alasio's telemetry, which its intake reads from the log (./log.ts); its catalog in
 * Neon and its Parquet files in the object store.
 *
 * Its pod runs the lake service, which loads it, and beside it its query endpoint,
 * through which Grafana and alasio lake read it (neon/lake/src/endpoint.ts), in a
 * container of its own, with resources of its own and only the reader's credentials.
 *
 * Its intake, main's alone, is a Deployment of its own, `lake-intake`, the log's consumer
 * group of that name (neon/lake/src/intake-service.ts), which KEDA scales on how far the
 * group is behind on the telemetry topics, as it reads that as a principal of its own
 * that may read none of them: from none, while nothing is waiting, to lake.intake's most,
 * as many as the topics have partitions.
 */
import type { KubernetesObject, V1Container, V1Deployment, V1EnvVar, V1Service } from "@kubernetes/client-node";

import { computeId, MAIN, replicaId } from "../../../neon/control/branches.ts";
import { imageReference } from "../images.ts";
import { INTAKE_GROUP, LOG_BROKERS, logPrincipalSecret, TELEMETRY_TOPICS } from "./log.ts";
import {
  componentName,
  type Environment,
  lakeRuns,
  MAIN_ENVIRONMENT,
  NAMESPACE,
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
import { replicaBudget, spread } from "./disruption.ts";
import type { InstallConfig } from "./config.ts";
import { PROXY_HOST, PROXY_PORT } from "./neon.ts";
import { priority } from "./priorities.ts";
import { scaledObject, type Trigger, type TriggerAuthentication } from "./scalers.ts";
import { verticalPodAutoscaler } from "./sizing.ts";

/** The port its query endpoint takes queries on. */
export const LAKE_QUERY_PORT = 8090;
/** The port the lake service and its intake serve their health and metrics on. */
const METRICS_PORT = 9464;
/** The intake's component, its Deployment's name's end. */
export const INTAKE_COMPONENT = "lake-intake";

/**
 * How far behind the intake's group may be on each partition, in records, before KEDA
 * gives it another replica: half of what a replica writes in a batch at most
 * (neon/lake/src/intake.ts's BATCH), a backlog, as after an outage, rather than
 * the stack's ordinary minutes, which are some tens of records.
 */
const LAG_THRESHOLD = 1_000;

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
      ports: [{ name: "metrics", port: METRICS_PORT }, ...(main ? [{ name: "query", port: LAKE_QUERY_PORT }] : [])],
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
    // It serves whether or not the lake can be read, so the loader beside it is not held back.
    readinessProbe: { httpGet: { path: "/healthz", port: "query" }, periodSeconds: 10 },
    livenessProbe: { httpGet: { path: "/healthz", port: "query" }, periodSeconds: 30, failureThreshold: 3 },
    securityContext: { ...restrictedContainer(), readOnlyRootFilesystem: true },
    resources: config.lake.query.resources,
    volumeMounts: [{ name: "query-tmp", mountPath: "/tmp" }],
  };
  // Live while its loads are not failing for long; ready once its loader has the lake open.
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
              ports: [{ name: "metrics", containerPort: METRICS_PORT }],
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
  return [service, deployment, verticalPodAutoscaler(deployment, "InPlace"), ...(main ? intakeObjects(config, lakeEnv) : [])];
}

/**
 * The intake's objects: its Deployment, the ScaledObject that scales it, a trigger of
 * each topic's lag, and what KEDA reads that as. A group that never committed on a
 * partition is as far behind as the partition is long, so the first records written wake
 * it.
 */
function intakeObjects(config: InstallConfig, lakeEnv: V1EnvVar[]): KubernetesObject[] {
  const name = componentName(INTAKE_COMPONENT);
  const { scaling, resources } = config.lake.intake;
  const metadata = { name, namespace: NAMESPACE, labels: stackLabels(INTAKE_COMPONENT) };
  const fromSecret = (variable: string, secret: string, key: string): V1EnvVar => ({ name: variable, valueFrom: { secretKeyRef: { name: secret, key } } });
  const lake = componentName("lake");
  const intake = logPrincipalSecret("intake");
  const probe = (path: string) => ({ httpGet: { path, port: "metrics" } });
  const deployment: V1Deployment = {
    apiVersion: "apps/v1",
    kind: "Deployment",
    metadata,
    spec: {
      ...replicasOf(scaling),
      selector: { matchLabels: selectorLabels(INTAKE_COMPONENT) },
      template: {
        metadata: { labels: stackLabels(INTAKE_COMPONENT) },
        spec: {
          ...priority("component"),
          ...stackPodSpec(config),
          ...spread(INTAKE_COMPONENT, scaling),
          // A batch being written is finished; one being gathered is read again by another.
          terminationGracePeriodSeconds: 60,
          containers: [{
            name: "intake",
            image: imageReference(config.images.lake),
            imagePullPolicy: config.images.pullPolicy,
            command: ["node", "src/intake-service.ts"],
            env: [
              ...lakeEnv,
              fromSecret("LAKE_DATABASE_PASSWORD", lake, "LAKE_DATABASE_PASSWORD"),
              fromSecret("LAKE_S3_KEY", lake, "LAKE_S3_KEY"),
              fromSecret("LAKE_S3_SECRET", lake, "LAKE_S3_SECRET"),
              { name: "LOG_BROKERS", value: LOG_BROKERS },
              fromSecret("LOG_USERNAME", intake, "username"),
              fromSecret("LOG_PASSWORD", intake, "password"),
              ...otelEnv(config),
              { name: "OTEL_SERVICE_NAME", value: "alasio-lake-intake" },
            ],
            ports: [{ name: "metrics", containerPort: METRICS_PORT }],
            readinessProbe: { ...probe("/readyz"), periodSeconds: 10 },
            // A batch being decoded holds its event loop a while: given time to answer.
            livenessProbe: { ...probe("/healthz"), initialDelaySeconds: 60, periodSeconds: 30, timeoutSeconds: 10, failureThreshold: 3 },
            securityContext: { ...restrictedContainer(), readOnlyRootFilesystem: true },
            resources,
            volumeMounts: [{ name: "tmp", mountPath: "/tmp" }],
          }],
          volumes: [{ name: "tmp", emptyDir: { sizeLimit: "2Gi" } }],
        },
      },
    },
  };
  const scaler = logPrincipalSecret("scaler");
  const authentication: TriggerAuthentication = {
    apiVersion: "keda.sh/v1alpha1",
    kind: "TriggerAuthentication",
    metadata,
    spec: { secretTargetRef: ["sasl", "username", "password"].map((parameter) => ({ parameter, name: scaler, key: parameter })) },
  };
  const lag = (topic: string): Trigger => ({
    type: "kafka",
    metadata: {
      bootstrapServers: LOG_BROKERS,
      consumerGroup: INTAKE_GROUP,
      topic,
      lagThreshold: String(LAG_THRESHOLD),
      activationLagThreshold: "0",
      offsetResetPolicy: "earliest",
    },
    authenticationRef: { name },
  });
  const scaledObjects = scaledObject(deployment, scaling, Object.values(TELEMETRY_TOPICS).map(lag));
  return [deployment, ...(scaledObjects.length > 0 ? [authentication, ...scaledObjects] : []), ...replicaBudget(INTAKE_COMPONENT, NAMESPACE, scaling), verticalPodAutoscaler(deployment, "InPlace")];
}
