/**
 * The analytics lake (neon/lake/src/model.ts): every transcript entry and rollout line,
 * loaded from alasio's database into DuckLake, as role `lake`, which alasio makes, and
 * alasio's telemetry, which its intake reads from the log (./log.ts); its catalog in
 * Neon and its Parquet files in the object store. Its writers and its readers run apart,
 * each at its own scale:
 *
 * - its loader, `lake-loader`, of each environment, from none to one (lake.loader's
 *   scale), which KEDA wakes while its group is behind on the environment's wake topic,
 *   which alasio writes as it commits transcripts (neon/lake/src/loader.ts), and puts back
 *   to sleep once it has caught up, past KEDA's cooldown; one loads at a time, which its
 *   advisory lock makes sure of, and so a new one starts only once the old is gone
 *   (Recreate);
 * - its maintenance, main's alone, a pass a process (neon/lake/src/maintain.ts): a
 *   CronJob of the daily pass, on lake.maintenance.schedule, and one of the hourly merge of
 *   the telemetry's files, each of which waits for the one before it; and, of each
 *   environment, the reconciliation of its catalog with the object store, a Job run on
 *   demand (lakeReconcile);
 * - its readers, `lake-query`, main's alone, through which Grafana and alasio lake read it
 *   (neon/lake/src/endpoint.ts), each with only the reader's credentials, from
 *   lake.query's least, never below one for Grafana's alert rules, to its most, which KEDA
 *   scales on the queries the pool has taken and not answered (its /pool, read through
 *   their Service, which keeps the lake's name); a branch environment has none, as it has
 *   no Grafana: what it sends is read in main's lake, tagged;
 * - its intake, main's alone, `lake-intake`, the log's consumer group of that name
 *   (neon/lake/src/intake-service.ts), which KEDA scales on how far the group is behind on
 *   the telemetry topics, as it reads that as a principal of its own that may read none of
 *   them: from none, while nothing is waiting, to lake.intake's most, as many as the
 *   topics have partitions.
 */
import type { KubernetesObject, V1CronJob, V1Deployment, V1EnvVar, V1Job, V1JobSpec, V1Service } from "@kubernetes/client-node";

import { computeId, MAIN, replicaId } from "../../../neon/control/branches.ts";
import { memoryBytes } from "../capacity.ts";
import { imageReference } from "../images.ts";
import { environmentName, INTAKE_GROUP, lakeWakeTopic, LOG_BROKERS, loaderGroup, loaderLogSecret, logPrincipalSecret, TELEMETRY_TOPICS } from "./log.ts";
import {
  componentName,
  type Environment,
  lakeRuns,
  MAIN_ENVIRONMENT,
  NAMESPACE,
  neonName,
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
import { FILER, MASTER } from "./object-store.ts";
import { priority } from "./priorities.ts";
import { scaledObject, type Trigger, type TriggerAuthentication } from "./scalers.ts";
import { verticalPodAutoscaler } from "./sizing.ts";

/** The port its readers take queries on. */
export const LAKE_QUERY_PORT = 8090;
/** The port the loader and the intake serve their health and metrics on. */
export const LAKE_METRICS_PORT = 9464;
/** Its components: the loader's, the readers', maintenance's and the intake's, each its Deployment's or CronJobs' name's end. */
export const LOADER_COMPONENT = "lake-loader";
export const QUERY_COMPONENT = "lake-query";
export const MAINTENANCE_COMPONENT = "lake-maintenance";
/** The reconciliation's, apart from maintenance's, as the bundled object store's filer, masters and volume servers admit it alone of the lake (./network-policies.ts). */
export const RECONCILE_COMPONENT = "lake-reconcile";
export const INTAKE_COMPONENT = "lake-intake";

/** When the telemetry's files are merged: hourly, away from the hour's other passes. */
const MERGE_SCHEDULE = "47 * * * *";

/**
 * How many queries each reader may have taken and not answered before KEDA gives the pool
 * another: one running and one waiting, as a dashboard opened asks a few at once.
 */
const QUERIES_PER_READER = 2;

/**
 * How far behind the intake's group may be on each partition, in records, before KEDA
 * gives it another replica: half of what a replica writes in a batch at most
 * (neon/lake/src/intake.ts's BATCH), a backlog, as after an outage, rather than
 * the stack's ordinary minutes, which are some tens of records.
 */
const LAG_THRESHOLD = 1_000;

/** Where the lake is, which each of its components opens it at: its environment's compute, through Neon's proxy, which wakes it if it sleeps. */
function lakeEnv(config: InstallConfig, environment: Environment = MAIN_ENVIRONMENT): V1EnvVar[] {
  return [
    { name: "LAKE_DATABASE_HOST", value: PROXY_HOST },
    { name: "LAKE_DATABASE_PORT", value: String(PROXY_PORT) },
    { name: "LAKE_DATABASE_ENDPOINT", value: computeId(environment.branch ?? MAIN) },
    { name: "LAKE_DATA_PATH", value: `s3://${config.objectStore.buckets.lake}/` },
    { name: "LAKE_S3_ENDPOINT", value: s3Endpoint(config) },
    { name: "HOME", value: "/tmp" },
  ];
}

/** `variable` of the Secret `secret`'s key `key`. */
const fromSecret = (variable: string, secret: string, key: string): V1EnvVar => ({ name: variable, valueFrom: { secretKeyRef: { name: secret, key } } });

/** The lake's writers' credentials, from its Secret: its role's password, and the object store's identity of the lake's. */
const writerSecrets = (): V1EnvVar[] => ["LAKE_DATABASE_PASSWORD", "LAKE_S3_KEY", "LAKE_S3_SECRET"].map((variable) => fromSecret(variable, componentName("lake"), variable));

/** A probe of `path` on the metrics port. */
const probe = (path: string) => ({ httpGet: { path, port: "metrics" } });

/** KEDA's reading of the log as the `scaler` principal, which may describe the groups and topics it scales on and read none, in `namespace`, the Secret's copy there. */
function logAuthentication(name: string, namespace: string, component: string): TriggerAuthentication {
  const scaler = logPrincipalSecret("scaler");
  return {
    apiVersion: "keda.sh/v1alpha1",
    kind: "TriggerAuthentication",
    metadata: { name, namespace, labels: stackLabels(component) },
    spec: { secretTargetRef: ["sasl", "username", "password"].map((parameter) => ({ parameter, name: scaler, key: parameter })) },
  };
}

/** A trigger of how far `group` is behind on `topic`, past `threshold` records a partition, authenticated as `authentication` says. */
const lagTrigger = (group: string, topic: string, threshold: number, authentication: string): Trigger => ({
  type: "kafka",
  metadata: { bootstrapServers: LOG_BROKERS, consumerGroup: group, topic, lagThreshold: String(threshold), activationLagThreshold: "0", offsetResetPolicy: "earliest" },
  authenticationRef: { name: authentication },
});

/**
 * The loader of `environment`: woken by KEDA once its group is behind on the environment's
 * wake topic at all, and back to none once it has caught up. A group that never committed
 * is as far behind as the topic is long, so the first record wakes it.
 */
function loaderObjects(config: InstallConfig, environment: Environment): KubernetesObject[] {
  const name = componentName(LOADER_COMPONENT);
  const { namespace } = environment;
  const topic = lakeWakeTopic(environmentName(environment));
  const group = loaderGroup(environmentName(environment));
  const log = loaderLogSecret(environment);
  const { scaling, resources } = config.lake.loader;
  const deployment: V1Deployment = {
    apiVersion: "apps/v1",
    kind: "Deployment",
    metadata: { name, namespace, labels: stackLabels(LOADER_COMPONENT) },
    spec: {
      ...replicasOf(scaling),
      // One loader: two would race each other's loads.
      strategy: { type: "Recreate" },
      selector: { matchLabels: selectorLabels(LOADER_COMPONENT) },
      template: {
        metadata: { labels: stackLabels(LOADER_COMPONENT) },
        spec: {
          ...priority("component"),
          ...stackPodSpec(config),
          // A load under way finishes; its records, untold, are read again.
          terminationGracePeriodSeconds: 60,
          containers: [{
            name: "loader",
            image: imageReference(config.images.lake),
            imagePullPolicy: config.images.pullPolicy,
            env: [
              ...lakeEnv(config, environment),
              ...writerSecrets(),
              sourceEnv(environment),
              { name: "LOG_BROKERS", value: LOG_BROKERS },
              fromSecret("LOG_USERNAME", log, "username"),
              fromSecret("LOG_PASSWORD", log, "password"),
              { name: "LAKE_WAKE_TOPIC", value: topic },
              { name: "LAKE_WAKE_GROUP", value: group },
              ...otelEnv(config, environment),
              { name: "OTEL_SERVICE_NAME", value: "alasio-lake-loader" },
            ],
            ports: [{ name: "metrics", containerPort: LAKE_METRICS_PORT }],
            // Live while its loads are not failing for long; ready once it has the lake open, under its lock, and its member of the group.
            readinessProbe: { ...probe("/readyz"), periodSeconds: 10 },
            livenessProbe: { ...probe("/healthz"), initialDelaySeconds: 60, periodSeconds: 30, failureThreshold: 3 },
            securityContext: { ...restrictedContainer(), readOnlyRootFilesystem: true },
            resources,
            volumeMounts: [{ name: "tmp", mountPath: "/tmp" }],
          }],
          volumes: [{ name: "tmp", emptyDir: { sizeLimit: "4Gi" } }],
        },
      },
    },
  };
  const scaledObjects = scaledObject(deployment, scaling, [lagTrigger(group, topic, 1, name)]);
  return [deployment, ...(scaledObjects.length > 0 ? [logAuthentication(name, namespace, LOADER_COMPONENT), ...scaledObjects] : []), verticalPodAutoscaler(deployment, "InPlace")];
}

/**
 * Where `environment`'s alasio's database is read from, which its loader and its
 * reconciliation load from: its read replica, which takes nothing of what alasio's own
 * compute serves alasio.
 */
const sourceEnv = (environment: Environment): V1EnvVar => ({ name: "LAKE_SOURCE_ENDPOINT", value: replicaId(environment.branch ?? MAIN) });

/**
 * What DuckDB is let hold in a container whose memory limit is `limit`: three fifths of it,
 * the rest Node's and the Parquet reader's, which DuckDB's own limit does not count; its
 * own default, where the container has none.
 */
const duckdbMemory = (limit: string | undefined): V1EnvVar[] =>
  limit ? [{ name: "LAKE_MEMORY_LIMIT", value: `${Math.floor((memoryBytes(limit) * 3) / 5 / 2 ** 20)}MiB` }] : [];

/**
 * A pass of maintenance's (neon/lake/src/maintain.ts) on `environment`'s lake: its
 * container, given `env` beside where the lake is and its writer's credentials, and its pod
 * template, as a Job of the pass runs it.
 */
function passTemplate(config: InstallConfig, environment: Environment, pass: "maintain" | "merge" | "reconcile", env: readonly V1EnvVar[]): V1JobSpec {
  return {
    backoffLimit: 2,
    activeDeadlineSeconds: 3600,
    template: {
      metadata: { labels: stackLabels(pass === "reconcile" ? RECONCILE_COMPONENT : MAINTENANCE_COMPONENT) },
      spec: {
        restartPolicy: "Never",
        ...priority("component"),
        ...stackPodSpec(config),
        containers: [{
          name: "maintenance",
          image: imageReference(config.images.lake),
          imagePullPolicy: config.images.pullPolicy,
          command: ["node", "src/maintain.ts", pass],
          env: [
            ...lakeEnv(config, environment),
            ...writerSecrets(),
            ...duckdbMemory(config.lake.maintenance.resources.limits?.["memory"]),
            ...env,
            ...otelEnv(config, environment),
            { name: "OTEL_SERVICE_NAME", value: "alasio-lake-maintenance" },
          ],
          securityContext: { ...restrictedContainer(), readOnlyRootFilesystem: true },
          resources: config.lake.maintenance.resources,
          volumeMounts: [{ name: "tmp", mountPath: "/tmp" }],
        }],
        volumes: [{ name: "tmp", emptyDir: { sizeLimit: "4Gi" } }],
      },
    },
  };
}

/** Main's maintenance: a CronJob of each of its passes, its daily one on `schedule`, the merge hourly. */
function maintenanceObjects(config: InstallConfig): KubernetesObject[] {
  const cronJob = (pass: "maintain" | "merge", schedule: string): V1CronJob => ({
    apiVersion: "batch/v1",
    kind: "CronJob",
    metadata: { name: componentName(pass === "merge" ? "lake-merge" : MAINTENANCE_COMPONENT), namespace: NAMESPACE, labels: stackLabels(MAINTENANCE_COMPONENT) },
    spec: {
      schedule,
      // A pass waits for the one before it; one missed by more than an hour waits for the next.
      concurrencyPolicy: "Forbid",
      startingDeadlineSeconds: 3600,
      successfulJobsHistoryLimit: 1,
      failedJobsHistoryLimit: 3,
      jobTemplate: {
        spec: passTemplate(config, MAIN_ENVIRONMENT, pass, [
          { name: "LAKE_RETENTION_DAYS", value: String(config.telemetry.retentionDays) },
          // It keeps its files while Neon has branches, whose lakes read them.
          { name: "LAKE_BRANCHES_URL", value: `http://${neonName("control")}:8080/branches` },
          fromSecret("LAKE_BRANCHES_TOKEN", componentName("lake"), "LAKE_BRANCHES_TOKEN"),
        ]),
      },
    },
  });
  return [cronJob("maintain", config.lake.maintenance.schedule), cronJob("merge", MERGE_SCHEDULE)];
}

/** The Job of a lake's reconciliation, in its environment's namespace. */
export const LAKE_RECONCILE = componentName("lake-reconcile");

/**
 * The reconciliation of `environment`'s lake, main's or a branch environment's, each with a
 * catalog of its own: a Job of maintenance's reconcile pass, which sets its catalog right with
 * the object store where files it names are gone, and loads again from its alasio's database
 * what its transcripts lost (neon/lake/src/reconcile.ts). It is run on demand alone, by alasio
 * lake reconcile and by alasio restore-filer-store, as a pass that changes the catalog
 * on its own would hide a loss a person is to look into.
 */
export function lakeReconcile(config: InstallConfig, environment: Environment = MAIN_ENVIRONMENT): V1Job {
  return {
    apiVersion: "batch/v1",
    kind: "Job",
    metadata: { name: LAKE_RECONCILE, namespace: environment.namespace, labels: stackLabels(RECONCILE_COMPONENT) },
    spec: {
      ...passTemplate(config, environment, "reconcile", [
        sourceEnv(environment),
        // A branch environment's lake's telemetry is main's as of its branch point, which main's reads again.
        ...(environment.branch === null ? [] : [{ name: "LAKE_BRANCH", value: environment.branch }]),
        // The bundled store's parts, which say whether a file the store fails a read of is gone.
        ...(config.objectStore.bundled.enabled
          ? [
            { name: "LAKE_STORE_FILER_URL", value: `http://${FILER}.${NAMESPACE}.svc:8888` },
            { name: "LAKE_STORE_MASTER_URL", value: `http://${MASTER}.${NAMESPACE}.svc:9333` },
            // Every one of which the masters are to know before their not knowing a volume says it is gone.
            { name: "LAKE_STORE_VOLUME_SERVERS", value: String(config.objectStore.bundled.volumeServers.scaling.min) },
          ]
          : []),
      ]),
      backoffLimit: 0,
    },
  };
}

/** Main's readers, their Service, which keeps the lake's name, and the headless one each finds the others by. */
function queryObjects(config: InstallConfig): KubernetesObject[] {
  const name = componentName(QUERY_COMPONENT);
  const { scaling, resources } = config.lake.query;
  const peers = `${name}-peers`;
  const service: V1Service = {
    apiVersion: "v1",
    kind: "Service",
    metadata: { name: componentName("lake"), namespace: NAMESPACE, labels: stackLabels(QUERY_COMPONENT) },
    spec: { selector: selectorLabels(QUERY_COMPONENT), ports: [{ name: "query", port: LAKE_QUERY_PORT }] },
  };
  // Ready readers alone, each by its address: the pool /pool sums the queues of.
  const headless: V1Service = {
    apiVersion: "v1",
    kind: "Service",
    metadata: { name: peers, namespace: NAMESPACE, labels: stackLabels(QUERY_COMPONENT) },
    spec: { clusterIP: "None", selector: selectorLabels(QUERY_COMPONENT), ports: [{ name: "query", port: LAKE_QUERY_PORT }] },
  };
  const deployment: V1Deployment = {
    apiVersion: "apps/v1",
    kind: "Deployment",
    metadata: { name, namespace: NAMESPACE, labels: stackLabels(QUERY_COMPONENT) },
    spec: {
      ...replicasOf(scaling),
      selector: { matchLabels: selectorLabels(QUERY_COMPONENT) },
      template: {
        metadata: { labels: stackLabels(QUERY_COMPONENT) },
        spec: {
          // They serve Grafana's alerts, which never sleep.
          ...priority("floor"),
          ...stackPodSpec(config),
          ...spread(QUERY_COMPONENT, scaling),
          containers: [{
            name: "query",
            image: imageReference(config.images.lake),
            imagePullPolicy: config.images.pullPolicy,
            command: ["node", "src/endpoint.ts"],
            env: [
              ...lakeEnv(config),
              ...["LAKE_READER_PASSWORD", "LAKE_READER_S3_KEY", "LAKE_READER_S3_SECRET", "LAKE_QUERY_TOKEN"].map((variable) => fromSecret(variable, componentName("lake"), variable)),
              { name: "LAKE_QUERY_PORT", value: String(LAKE_QUERY_PORT) },
              { name: "LAKE_QUERY_PEERS", value: `${peers}.${NAMESPACE}.svc.cluster.local` },
              ...otelEnv(config),
              { name: "OTEL_SERVICE_NAME", value: "alasio-lake-query" },
            ],
            ports: [{ name: "query", containerPort: LAKE_QUERY_PORT }],
            // Ready once it has the lake open, so its Service sends no query to one that cannot read it.
            readinessProbe: { httpGet: { path: "/readyz", port: "query" }, periodSeconds: 10 },
            livenessProbe: { httpGet: { path: "/healthz", port: "query" }, periodSeconds: 30, failureThreshold: 3 },
            securityContext: { ...restrictedContainer(), readOnlyRootFilesystem: true },
            resources,
            volumeMounts: [{ name: "tmp", mountPath: "/tmp" }],
          }],
          volumes: [{ name: "tmp", emptyDir: { sizeLimit: "2Gi" } }],
        },
      },
    },
  };
  // The queries the pool has not answered, as any reader sums them, so many to a reader.
  const queue: Trigger = {
    type: "metrics-api",
    metadata: { url: `http://${componentName("lake")}.${NAMESPACE}.svc.cluster.local:${LAKE_QUERY_PORT}/pool`, valueLocation: "queries", targetValue: String(QUERIES_PER_READER) },
  };
  return [service, headless, deployment, ...scaledObject(deployment, scaling, [queue]), ...replicaBudget(QUERY_COMPONENT, NAMESPACE, scaling), verticalPodAutoscaler(deployment, "InPlace")];
}

/**
 * The intake's objects: its Deployment, the ScaledObject that scales it, a trigger of
 * each topic's lag, and what KEDA reads that as.
 */
function intakeObjects(config: InstallConfig): KubernetesObject[] {
  const name = componentName(INTAKE_COMPONENT);
  const { scaling, resources } = config.lake.intake;
  const metadata = { name, namespace: NAMESPACE, labels: stackLabels(INTAKE_COMPONENT) };
  const intake = logPrincipalSecret("intake");
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
              ...lakeEnv(config),
              ...writerSecrets(),
              { name: "LOG_BROKERS", value: LOG_BROKERS },
              fromSecret("LOG_USERNAME", intake, "username"),
              fromSecret("LOG_PASSWORD", intake, "password"),
              ...otelEnv(config),
              { name: "OTEL_SERVICE_NAME", value: "alasio-lake-intake" },
            ],
            ports: [{ name: "metrics", containerPort: LAKE_METRICS_PORT }],
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
  const scaledObjects = scaledObject(deployment, scaling, Object.values(TELEMETRY_TOPICS).map((topic) => lagTrigger(INTAKE_GROUP, topic, LAG_THRESHOLD, name)));
  return [
    deployment,
    ...(scaledObjects.length > 0 ? [logAuthentication(name, NAMESPACE, INTAKE_COMPONENT), ...scaledObjects] : []),
    ...replicaBudget(INTAKE_COMPONENT, NAMESPACE, scaling),
    verticalPodAutoscaler(deployment, "InPlace"),
  ];
}

/**
 * The lake's objects, when it runs, which is beside Neon. A branch environment's is its
 * loader alone, of the branch's copy of the catalog on main's files, which it reads where
 * they are and writes beside them, and maintains none of: main's maintenance keeps every
 * file while Neon has branches, which it asks neon-control about (neon/lake/src/sync.ts).
 */
export function lakeObjects(config: InstallConfig, environment: Environment = MAIN_ENVIRONMENT): KubernetesObject[] {
  if (!lakeRuns(config)) return [];
  if (environment.branch !== null) return loaderObjects(config, environment);
  return [...loaderObjects(config, environment), ...maintenanceObjects(config), ...queryObjects(config), ...intakeObjects(config)];
}
