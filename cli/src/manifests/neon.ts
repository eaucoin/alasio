/**
 * Neon, alasio's database: Postgres whose storage is safekeepers and a pageserver on
 * object storage, with a storage controller and its own Postgres, a storage broker, the
 * compute, Neon's proxy, which every client connects to the compute through, waking it
 * when it sleeps, neon-control (alasio's stand-in for Neon's control plane,
 * neon/control/), the setup that makes the stack's secrets, and a daily backup.
 */
import type {
  KubernetesObject,
  V1Container,
  V1CronJob,
  V1Deployment,
  V1EnvVar,
  V1Job,
  V1Role,
  V1RoleBinding,
  V1Service,
  V1ServiceAccount,
  V1ServicePort,
  V1StatefulSet,
} from "@kubernetes/client-node";

import { COMPUTE_CTL_PORT, COMPUTE_PORT, computeId, MAIN, REPLICA_SUFFIX, replicaId } from "../../../neon/control/branches.ts";
import { SUSPEND_AFTER } from "../../../neon/control/endpoints.ts";
import { DATABASE, PAGESERVER_ZONE, ROLE } from "../../../neon/control/secrets.ts";
import { imageReference } from "../images.ts";
import { computeRestartPolicy } from "./admission.ts";
import {
  claimSpec,
  collectorRuns,
  databaseSecret,
  type Environment,
  given,
  helperResources,
  imagePullSecrets,
  lakeRuns,
  MAIN_ENVIRONMENT,
  NAMESPACE,
  neonName,
  ONE,
  otelEnv,
  RELEASE,
  replicasOf,
  restrictedContainer,
  s3Endpoint,
  sandboxSnapshots,
  script,
  selectorLabels,
  stackLabels,
  stackPodSpec,
  waitFor,
  waitForObjectStore,
} from "./common.ts";
import { durationSeconds, type InstallConfig } from "./config.ts";
import { replicaBudget, spread } from "./disruption.ts";
import { priority } from "./priorities.ts";
import { verticalPodAutoscaler } from "./sizing.ts";
import { VALKEY_ADDRESS, workspacesBucketUrl } from "./workspace-storage.ts";

/** A Service of the stack's: its name and component, selecting the component's pods, on `ports`, main's unless of `environment`. */
function service(name: string, component: string, ports: V1ServicePort[], environment: Environment = MAIN_ENVIRONMENT): V1Service {
  return {
    apiVersion: "v1",
    kind: "Service",
    metadata: { name, namespace: environment.namespace, labels: stackLabels(component) },
    spec: { selector: selectorLabels(component), ports },
  };
}

/** A Secret's key, as a variable of the same name. */
function secretVariable(secret: string, key: string): V1EnvVar {
  return { name: key, valueFrom: { secretKeyRef: { name: secret, key } } };
}

/**
 * The stack's secrets, made once and kept, and each service's rendered from them, by
 * neon/control/kube-setup.ts: the log's and the webhook intake's; and where Neon runs,
 * Neon's, the object store's, workspace storage's when it is on, and the snapshots
 * bucket's and the node agents' when sandboxes are snapshotted: a Job, which applying
 * alasio runs to completion before anything else of alasio starts (../kube/apply.ts), with
 * Neon or without it.
 */
function setup(config: InstallConfig): KubernetesObject[] {
  const name = neonName("setup");
  const { objectStore, workspaceStorage } = config;
  const metadata = { name, namespace: NAMESPACE, labels: stackLabels("neon-setup") };
  const serviceAccount: V1ServiceAccount = { apiVersion: "v1", kind: "ServiceAccount", metadata };
  const role: V1Role = {
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "Role",
    metadata,
    rules: [{ apiGroups: [""], resources: ["secrets"], verbs: ["get", "create", "update"] }],
  };
  const binding: V1RoleBinding = {
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "RoleBinding",
    metadata,
    subjects: [{ kind: "ServiceAccount", name, namespace: NAMESPACE }],
    roleRef: { apiGroup: "rbac.authorization.k8s.io", kind: "Role", name },
  };
  const job: V1Job = {
    apiVersion: "batch/v1",
    kind: "Job",
    metadata,
    spec: {
      backoffLimit: 3,
      activeDeadlineSeconds: 300,
      template: {
        metadata: { labels: stackLabels("neon-setup") },
        spec: {
          serviceAccountName: name,
          restartPolicy: "Never",
          // Of the floor, as applying alasio waits for it: it takes what sleeps' place
          // rather than fail alasio up where the node is full (./priorities.ts).
          ...priority("floor"),
          ...stackPodSpec(config),
          containers: [{
            name: "setup",
            image: imageReference(config.images.alasio),
            imagePullPolicy: config.images.pullPolicy,
            command: ["node", "/opt/alasio/neon/control/kube-setup.ts"],
            env: [
              { name: "NAMESPACE", valueFrom: { fieldRef: { fieldPath: "metadata.namespace" } } },
              { name: "SECRET_PREFIX", value: RELEASE },
              ...(config.neon.enabled
                ? [
                  { name: "NEON_BROKER_URL", value: `http://${neonName("storage-broker")}:50051` },
                  { name: "NEON_CONTROLLER_URL", value: `http://${neonName("storage-controller")}:1234` },
                  { name: "NEON_PROXY_HOST", value: PROXY_HOST },
                  { name: "NEON_CONTROLLER_DB_HOST", value: neonName("controller-db") },
                ]
                : []),
              { name: "S3_ENDPOINT", value: s3Endpoint(config) },
              { name: "S3_REGION", value: objectStore.external.region },
              { name: "S3_BUCKET_NEON", value: objectStore.buckets.neon },
              { name: "S3_BUCKET_LAKE", value: objectStore.buckets.lake },
              { name: "S3_BUCKET_BACKUPS", value: objectStore.buckets.backups },
              { name: "WEBHOOK_ADDRESS", value: config.webhook.publicAddress },
              ...(workspaceStorage.enabled
                ? [
                  { name: "WORKSPACES_NAME", value: workspaceStorage.name },
                  { name: "WORKSPACES_BUCKET", value: objectStore.buckets.workspaces },
                  { name: "WORKSPACES_BUCKET_URL", value: workspacesBucketUrl(config) },
                  { name: "WORKSPACES_TRASH_DAYS", value: String(workspaceStorage.trashDays) },
                  { name: "VALKEY_ADDRESS", value: VALKEY_ADDRESS },
                ]
                : []),
              ...(sandboxSnapshots(config) ? [{ name: "SNAPSHOTS_BUCKET", value: objectStore.buckets.snapshots }] : []),
              ...(objectStore.bundled.enabled ? [] : [
                { name: "S3_EXTERNAL", value: "1" },
                { name: "S3_ACCESS_KEY", valueFrom: { secretKeyRef: { name: objectStore.external.existingSecret, key: "accessKey" } } },
                { name: "S3_SECRET_KEY", valueFrom: { secretKeyRef: { name: objectStore.external.existingSecret, key: "secretKey" } } },
                ...(sandboxSnapshots(config)
                  ? [
                    { name: "S3_SNAPSHOTS_ACCESS_KEY", valueFrom: { secretKeyRef: { name: objectStore.external.snapshotsSecret, key: "accessKey" } } },
                    { name: "S3_SNAPSHOTS_SECRET_KEY", valueFrom: { secretKeyRef: { name: objectStore.external.snapshotsSecret, key: "secretKey" } } },
                  ]
                  : []),
              ]),
            ],
            securityContext: restrictedContainer(),
            resources: { requests: { cpu: "50m", memory: "64Mi" }, limits: { memory: "256Mi" } },
          }],
        },
      },
    },
  };
  return [serviceAccount, role, binding, job];
}

/** The storage controller's own Postgres. */
function controllerDb(config: InstallConfig): KubernetesObject[] {
  const name = neonName("controller-db");
  const component = "neon-controller-db";
  const statefulSet: V1StatefulSet = {
    apiVersion: "apps/v1",
    kind: "StatefulSet",
    metadata: { name, namespace: NAMESPACE, labels: stackLabels(component) },
    spec: {
      serviceName: name,
      ...replicasOf(ONE),
      selector: { matchLabels: selectorLabels(component) },
      template: {
        metadata: { labels: stackLabels(component) },
        spec: {
          ...priority("bound"),
          ...stackPodSpec(config),
          containers: [{
            name: "postgres",
            image: imageReference(config.neon.controllerDbImage),
            imagePullPolicy: "IfNotPresent",
            env: [
              { name: "POSTGRES_USER", value: "storage_controller" },
              { name: "POSTGRES_DB", value: "storage_controller" },
              { name: "PGDATA", value: "/var/lib/postgresql/data/pgdata" },
            ],
            envFrom: [{ secretRef: { name } }],
            ports: [{ name: "postgres", containerPort: 5432 }],
            readinessProbe: { exec: { command: ["pg_isready", "-U", "storage_controller", "-d", "storage_controller"] }, periodSeconds: 5 },
            securityContext: restrictedContainer(),
            resources: config.neon.controllerDb.resources,
            volumeMounts: [{ name: "data", mountPath: "/var/lib/postgresql/data" }, { name: "run", mountPath: "/var/run/postgresql" }],
          }],
          volumes: [{ name: "run", emptyDir: {} }],
        },
      },
      volumeClaimTemplates: [{ metadata: { name: "data" }, spec: claimSpec(config.neon.controllerDb.storage) }],
    },
  };
  return [service(name, component, [{ name: "postgres", port: 5432 }]), statefulSet, verticalPodAutoscaler(statefulSet, "Recommended")];
}

/** The storage broker, through which safekeepers and the pageserver learn of each other's progress. */
function storageBroker(config: InstallConfig): KubernetesObject[] {
  const name = neonName("storage-broker");
  const component = "neon-storage-broker";
  const deployment: V1Deployment = {
    apiVersion: "apps/v1",
    kind: "Deployment",
    metadata: { name, namespace: NAMESPACE, labels: stackLabels(component) },
    spec: {
      ...replicasOf(ONE),
      selector: { matchLabels: selectorLabels(component) },
      template: {
        metadata: { labels: stackLabels(component) },
        spec: {
          ...priority("floor"),
          ...stackPodSpec(config),
          containers: [{
            name: "storage-broker",
            image: imageReference(config.neon.image),
            imagePullPolicy: "IfNotPresent",
            command: ["storage_broker", "--listen-addr=0.0.0.0:50051"],
            ports: [{ name: "grpc", containerPort: 50051 }],
            readinessProbe: { tcpSocket: { port: "grpc" }, periodSeconds: 5 },
            securityContext: restrictedContainer(),
            resources: config.neon.storageBroker.resources,
          }],
        },
      },
    },
  };
  return [service(name, component, [{ name: "grpc", port: 50051 }]), deployment, verticalPodAutoscaler(deployment, "InPlace")];
}

/** The storage controller: one at a time, as it leads the storage it controls; strict, every timeline on every safekeeper. */
function storageController(config: InstallConfig): KubernetesObject[] {
  const name = neonName("storage-controller");
  const component = "neon-storage-controller";
  const deployment: V1Deployment = {
    apiVersion: "apps/v1",
    kind: "Deployment",
    metadata: { name, namespace: NAMESPACE, labels: stackLabels(component) },
    spec: {
      ...replicasOf(ONE),
      strategy: { type: "Recreate" },
      selector: { matchLabels: selectorLabels(component) },
      template: {
        metadata: { labels: stackLabels(component) },
        spec: {
          ...priority("floor"),
          ...stackPodSpec(config),
          initContainers: [{
            name: "wait-for-controller-db",
            image: imageReference(config.neon.controllerDbImage),
            imagePullPolicy: "IfNotPresent",
            command: ["/bin/sh", "-c", 'until pg_isready -h "$0" -U storage_controller; do sleep 2; done', neonName("controller-db")],
            securityContext: restrictedContainer(),
            ...helperResources(),
          }],
          containers: [{
            name: "storage-controller",
            image: imageReference(config.neon.image),
            imagePullPolicy: "IfNotPresent",
            command: [
              "/bin/sh",
              "-c",
              [
                'PUBLIC_KEY="$(cat /keys/auth_public_key.pem)"',
                "exec storage_controller",
                "--listen 0.0.0.0:1234",
                `--control-plane-url http://${neonName("control")}:8080`,
                "--timelines-onto-safekeepers",
                `--timeline-safekeeper-count ${config.neon.safekeepers.scaling.min}`,
              ].join(" "),
            ],
            envFrom: [{ secretRef: { name } }],
            ports: [{ name: "http", containerPort: 1234 }],
            readinessProbe: { httpGet: { path: "/status", port: "http" }, periodSeconds: 5 },
            securityContext: restrictedContainer(),
            resources: config.neon.storageController.resources,
            volumeMounts: [{ name: "keys", mountPath: "/keys", readOnly: true }],
          }],
          volumes: [{ name: "keys", secret: { secretName: name, items: [{ key: "auth_public_key.pem", path: "auth_public_key.pem" }] } }],
        },
      },
    },
  };
  return [service(name, component, [{ name: "http", port: 1234 }]), deployment, verticalPodAutoscaler(deployment, "Recommended")];
}

/**
 * The safekeepers: a timeline's WAL, each on its own volume, at stable names neon-control
 * registers them by, which resolve before they are ready, as the controller needs. They
 * spread across nodes and zones where the cluster has them, so a quorum survives one, and
 * a drain stops one of them at a time.
 */
function safekeepers(config: InstallConfig): KubernetesObject[] {
  const name = neonName("safekeeper");
  const component = "neon-safekeeper";
  const headless: V1Service = {
    apiVersion: "v1",
    kind: "Service",
    metadata: { name, namespace: NAMESPACE, labels: stackLabels(component) },
    spec: { clusterIP: "None", publishNotReadyAddresses: true, selector: selectorLabels(component), ports: [{ name: "pg", port: 5454 }, { name: "http", port: 7676 }] },
  };
  const statefulSet: V1StatefulSet = {
    apiVersion: "apps/v1",
    kind: "StatefulSet",
    metadata: { name, namespace: NAMESPACE, labels: stackLabels(component) },
    spec: {
      serviceName: name,
      ...replicasOf(config.neon.safekeepers.scaling),
      podManagementPolicy: "Parallel",
      selector: { matchLabels: selectorLabels(component) },
      template: {
        metadata: { labels: stackLabels(component) },
        spec: {
          ...priority("bound"),
          ...stackPodSpec(config),
          terminationGracePeriodSeconds: 30,
          ...given("initContainers", waitForObjectStore(config)),
          ...spread(component, config.neon.safekeepers.scaling),
          containers: [{
            name: "safekeeper",
            image: imageReference(config.neon.image),
            imagePullPolicy: "IfNotPresent",
            // Its id is its ordinal plus one; each is its own availability zone, so each
            // timeline is placed on distinct safekeepers.
            command: [
              "/bin/sh",
              "-c",
              [
                "SAFEKEEPER_ID=$((POD_INDEX + 1)) &&",
                'exec safekeeper -D /data --id "$SAFEKEEPER_ID"',
                `--listen-pg 0.0.0.0:5454 --advertise-pg "$POD_NAME.${name}.$POD_NAMESPACE.svc.cluster.local:5454"`,
                "--listen-http 0.0.0.0:7676",
                '--availability-zone "az-safekeeper-$SAFEKEEPER_ID"',
                `--broker-endpoint http://${neonName("storage-broker")}:50051`,
                '--remote-storage "$REMOTE_STORAGE"',
                // It uploads the segment it writes, whole, once it has grown, at most
                // this often, and the object store keeps each upload replaced a day: an
                // hour apart, not fifteen minutes, a quarter as many. What the bucket
                // lacks of the last hour is on the volume of each safekeeper.
                "--partial-backup-timeout 1h",
                "--pg-auth-public-key-path /keys/auth_public_key.pem",
                "--pg-tenant-only-auth-public-key-path /keys/auth_public_key.pem",
                "--http-auth-public-key-path /keys/auth_public_key.pem",
                "--auth-token-path /keys/safekeeper_peer_token",
              ].join(" "),
            ],
            env: [
              { name: "POD_NAME", valueFrom: { fieldRef: { fieldPath: "metadata.name" } } },
              { name: "POD_NAMESPACE", valueFrom: { fieldRef: { fieldPath: "metadata.namespace" } } },
              { name: "POD_INDEX", valueFrom: { fieldRef: { fieldPath: "metadata.labels['apps.kubernetes.io/pod-index']" } } },
              secretVariable(name, "AWS_ACCESS_KEY_ID"),
              secretVariable(name, "AWS_SECRET_ACCESS_KEY"),
              secretVariable(name, "REMOTE_STORAGE"),
            ],
            ports: [{ name: "pg", containerPort: 5454 }, { name: "http", containerPort: 7676 }],
            readinessProbe: { httpGet: { path: "/v1/status", port: "http" }, periodSeconds: 5 },
            securityContext: restrictedContainer(),
            resources: config.neon.safekeepers.resources,
            volumeMounts: [{ name: "data", mountPath: "/data" }, { name: "keys", mountPath: "/keys", readOnly: true }],
          }],
          volumes: [{
            name: "keys",
            secret: {
              secretName: name,
              defaultMode: 0o440,
              items: [{ key: "auth_public_key.pem", path: "auth_public_key.pem" }, { key: "safekeeper_peer_token", path: "safekeeper_peer_token" }],
            },
          }],
        },
      },
      volumeClaimTemplates: [{ metadata: { name: "data" }, spec: claimSpec(config.neon.safekeepers.storage) }],
    },
  };
  return [headless, statefulSet, ...replicaBudget(component, NAMESPACE, config.neon.safekeepers.scaling), verticalPodAutoscaler(statefulSet, "Recommended")];
}

/** The name of the pageserver of ordinal `index`, its Service's, which it registers itself at: its pod's. */
export const pageserverName = (index: number): string => `${neonName("pageserver")}-${index}`;

/** The pageservers' governing Service, headless, which their StatefulSet names: of no one pageserver, as pageserverName is. */
export const PAGESERVERS_NAME = neonName("pageservers");

/**
 * The pageservers: pages served from layers each keeps in the object store, its volume a
 * cache, as many as their scale fixes. Each registers itself with the storage controller
 * as its ordinal plus one, at a Service of its own alone (pageserverName), which resolves
 * before it is ready, as registration needs; the controller places the tenant's shards on
 * them, and computes reach each shard's pageserver at that name (neon-control's specs),
 * never through one address that would balance them. They start at once, none waiting for
 * another to be ready, which waits for the storage controller: so a restore that starts what
 * its volumes hold to a node before the rest (../commands/restore-filer-store.ts) finds
 * every pageserver's pod made. Their StatefulSet's governing Service (PAGESERVERS_NAME) is
 * headless, giving each pod its DNS identity, which no address uses: a pod's own Service
 * keeps its address while it is not ready, as registration needs, and alone resolves to it.
 */
function pageserver(config: InstallConfig): KubernetesObject[] {
  const name = neonName("pageserver");
  const component = "neon-pageserver";
  const image = imageReference(config.neon.image);
  const { scaling, storage, resources } = config.neon.pageserver;
  const services = Array.from({ length: scaling.min }, (_, index): V1Service => ({
    apiVersion: "v1",
    kind: "Service",
    metadata: { name: pageserverName(index), namespace: NAMESPACE, labels: stackLabels(component) },
    spec: {
      selector: { ...selectorLabels(component), "statefulset.kubernetes.io/pod-name": `${name}-${index}` },
      publishNotReadyAddresses: true,
      ports: [{ name: "pg", port: 6400 }, { name: "http", port: 9898 }],
    },
  }));
  const governing: V1Service = {
    apiVersion: "v1",
    kind: "Service",
    metadata: { name: PAGESERVERS_NAME, namespace: NAMESPACE, labels: stackLabels(component) },
    spec: { clusterIP: "None", publishNotReadyAddresses: true, selector: selectorLabels(component), ports: [{ name: "pg", port: 6400 }, { name: "http", port: 9898 }] },
  };
  const statefulSet: V1StatefulSet = {
    apiVersion: "apps/v1",
    kind: "StatefulSet",
    metadata: { name, namespace: NAMESPACE, labels: stackLabels(component) },
    spec: {
      serviceName: PAGESERVERS_NAME,
      ...replicasOf(scaling),
      podManagementPolicy: "Parallel",
      selector: { matchLabels: selectorLabels(component) },
      template: {
        metadata: { labels: stackLabels(component) },
        spec: {
          ...priority("bound"),
          ...stackPodSpec(config),
          ...spread(component, scaling, true),
          terminationGracePeriodSeconds: 30,
          initContainers: [
            {
              name: "config",
              image,
              imagePullPolicy: "IfNotPresent",
              // Its configuration, the stack's setup's, into its working directory, and its
              // identity, its own: its ordinal plus one, at its own Service's name, its pod's.
              // The ordinal is its pod's name's last part, as the StatefulSet names its pods,
              // on any Kubernetes (the pod-index label is 1.28's).
              command: [
                "/bin/sh",
                "-c",
                [
                  'POD_INDEX="${POD_NAME##*-}" &&',
                  "cp /config/pageserver.toml /config/auth_public_key.pem /data/.neon/ &&",
                  'echo "id=$((POD_INDEX + 1))" > /data/.neon/identity.toml &&',
                  `printf '{"host":"%s","port":6400,"http_host":"%s","http_port":9898,"availability_zone_id":"${PAGESERVER_ZONE}"}\\n' "$POD_NAME" "$POD_NAME" > /data/.neon/metadata.json`,
                ].join(" "),
              ],
              env: [{ name: "POD_NAME", valueFrom: { fieldRef: { fieldPath: "metadata.name" } } }],
              securityContext: restrictedContainer(),
              ...helperResources(),
              volumeMounts: [{ name: "data", mountPath: "/data/.neon" }, { name: "config", mountPath: "/config", readOnly: true }],
            },
            waitFor(config, "storage-controller", `http://${neonName("storage-controller")}:1234/status`),
            ...waitForObjectStore(config),
          ],
          containers: [{
            name: "pageserver",
            image,
            imagePullPolicy: "IfNotPresent",
            command: ["pageserver", "-D", "/data/.neon"],
            env: [secretVariable(name, "AWS_ACCESS_KEY_ID"), secretVariable(name, "AWS_SECRET_ACCESS_KEY"), secretVariable(name, "NEON_AUTH_TOKEN")],
            ports: [{ name: "pg", containerPort: 6400 }, { name: "http", containerPort: 9898 }],
            readinessProbe: { httpGet: { path: "/v1/status", port: "http" }, periodSeconds: 5 },
            securityContext: restrictedContainer(),
            resources,
            volumeMounts: [{ name: "data", mountPath: "/data/.neon" }],
          }],
          volumes: [{
            name: "config",
            secret: { secretName: name, items: ["pageserver.toml", "auth_public_key.pem"].map((key) => ({ key, path: key })) },
          }],
        },
      },
      volumeClaimTemplates: [{ metadata: { name: "data" }, spec: claimSpec(storage) }],
    },
  };
  return [governing, ...services, statefulSet, ...replicaBudget(component, NAMESPACE, scaling), verticalPodAutoscaler(statefulSet, "Recommended")];
}

/** Where every client reaches Neon's proxy, from main's namespace or a branch environment's. */
export const PROXY_HOST = `${neonName("proxy")}.${NAMESPACE}.svc`;
/** The port the proxy takes clients on, Postgres's own. */
export const PROXY_PORT = 5432;

/**
 * What neon-control may do to the compute of `namespace`, main's or a branch
 * environment's, and to its read replica: read its Deployment, and restart it, to start on
 * a spec anew, and scale it, waking it and putting it to sleep, and list its pods, to see
 * one put to sleep gone before it wakes it; a Role there, bound to neon-control's
 * ServiceAccount in main's. Of a Deployment it patches, it may change the restart's
 * annotation alone (./admission.ts's computeRestartPolicy).
 */
export function computeScaling(namespace: string): KubernetesObject[] {
  const name = neonName("control");
  const metadata = { name, namespace, labels: stackLabels("neon-control") };
  const role: V1Role = {
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "Role",
    metadata,
    rules: [
      { apiGroups: ["apps"], resources: ["deployments"], resourceNames: [neonName("compute"), REPLICA_NAME], verbs: ["get", "patch"] },
      { apiGroups: ["apps"], resources: ["deployments/scale"], resourceNames: [neonName("compute"), REPLICA_NAME], verbs: ["patch"] },
      // A list is of a namespace's pods, as no name narrows it.
      { apiGroups: [""], resources: ["pods"], verbs: ["list"] },
    ],
  };
  const binding: V1RoleBinding = {
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "RoleBinding",
    metadata,
    subjects: [{ kind: "ServiceAccount", name, namespace: NAMESPACE }],
    roleRef: { apiGroup: "rbac.authorization.k8s.io", kind: "Role", name },
  };
  return [role, binding];
}

/**
 * neon-control: it bootstraps the tenant and main's timeline, makes and deletes branches,
 * serves each branch's compute its spec, answers the storage controller's hooks and Neon's
 * proxy, wakes computes and puts them to sleep, and repairs a safekeeper that lost its
 * disk. Its record of the branches is in the storage controller's Postgres, so any of its
 * replicas serves.
 */
function control(config: InstallConfig): KubernetesObject[] {
  const name = neonName("control");
  const component = "neon-control";
  const { scaling, resources } = config.neon.control;
  const safekeeper = neonName("safekeeper");
  const safekeeperHosts = Array.from(
    { length: config.neon.safekeepers.scaling.min },
    (_, index) => `${safekeeper}-${index}.${safekeeper}.${NAMESPACE}.svc.cluster.local`,
  );
  const serviceAccount: V1ServiceAccount = { apiVersion: "v1", kind: "ServiceAccount", metadata: { name, namespace: NAMESPACE, labels: stackLabels(component) } };
  const deployment: V1Deployment = {
    apiVersion: "apps/v1",
    kind: "Deployment",
    metadata: { name, namespace: NAMESPACE, labels: stackLabels(component) },
    spec: {
      ...replicasOf(scaling),
      selector: { matchLabels: selectorLabels(component) },
      template: {
        metadata: { labels: stackLabels(component) },
        spec: {
          serviceAccountName: name,
          ...priority("floor"),
          ...stackPodSpec(config),
          ...spread(component, scaling),
          containers: [{
            name: "neon-control",
            image: imageReference(config.images.alasio),
            imagePullPolicy: config.images.pullPolicy,
            command: ["node", "/opt/alasio/neon/control/service.ts"],
            env: [
              { name: "CONTROLLER_URL", value: `http://${neonName("storage-controller")}:1234` },
              { name: "NEON_SAFEKEEPER_HOSTS", value: safekeeperHosts.join(",") },
              { name: "NEON_PAGESERVERS", value: String(config.neon.pageserver.scaling.min) },
              { name: "NEON_SHARDS", value: String(config.neon.shards) },
              { name: "NEON_CONTROLLER_DB_HOST", value: neonName("controller-db") },
              { name: "NAMESPACE", valueFrom: { fieldRef: { fieldPath: "metadata.namespace" } } },
              { name: "NEON_COMPUTE_NAME", value: neonName("compute") },
            ],
            ports: [{ name: "http", containerPort: 8080 }],
            readinessProbe: { httpGet: { path: "/healthz", port: "http" }, periodSeconds: 3 },
            securityContext: { ...restrictedContainer(), readOnlyRootFilesystem: true },
            resources,
            volumeMounts: [
              { name: "root", mountPath: "/secrets", readOnly: true },
              { name: "keys", mountPath: "/keys", readOnly: true },
            ],
          }],
          volumes: [
            {
              name: "root",
              secret: {
                secretName: neonName("root"),
                defaultMode: 0o440,
                items: [{ key: "secrets.json", path: "secrets.json" }, { key: "auth_private_key.pem", path: "auth_private_key.pem" }],
              },
            },
            { name: "keys", secret: { secretName: neonName("root"), items: [{ key: "auth_public_key.pem", path: "auth_public_key.pem" }] } },
          ],
        },
      },
    },
  };
  return [
    service(name, component, [{ name: "http", port: 8080 }]),
    serviceAccount,
    ...computeScaling(NAMESPACE),
    ...computeRestartPolicy(),
    deployment,
    ...replicaBudget(component, NAMESPACE, scaling),
    verticalPodAutoscaler(deployment, "Recommended"),
  ];
}

/**
 * Neon's proxy, from Neon's image: every client of a compute, main's or a branch
 * environment's, connects to it, naming the compute by `options=endpoint=<id>` (the
 * compute's id, as its spec is served under), without TLS, which would turn that option
 * off. It asks neon-control, its control plane, for the role's verifier, authenticates the
 * client against it, has neon-control wake the compute if it sleeps, and then passes the
 * session through, whole, so session state such as advisory locks holds. It keeps where
 * each compute is for four minutes, and asks again when a connection there fails, as one
 * to a compute that has gone to sleep since does. It cancels no query: that needs a Redis
 * of its own, and alasio cancels none.
 */
function proxy(config: InstallConfig): KubernetesObject[] {
  const name = neonName("proxy");
  const component = "neon-proxy";
  const { scaling, resources } = config.neon.proxy;
  const deployment: V1Deployment = {
    apiVersion: "apps/v1",
    kind: "Deployment",
    metadata: { name, namespace: NAMESPACE, labels: stackLabels(component) },
    spec: {
      ...replicasOf(scaling),
      selector: { matchLabels: selectorLabels(component) },
      template: {
        metadata: { labels: stackLabels(component) },
        spec: {
          ...priority("floor"),
          ...stackPodSpec(config),
          ...spread(component, scaling),
          containers: [{
            name: "proxy",
            image: imageReference(config.neon.image),
            imagePullPolicy: "IfNotPresent",
            command: [
              "proxy",
              "--proxy", `0.0.0.0:${PROXY_PORT}`,
              "--http", "0.0.0.0:7001",
              "--auth-backend", "control-plane",
              "--auth-endpoint", `http://${neonName("control")}:8080`,
              "--wake-compute-cache", "size=4000,ttl=4m",
              // A role's secret is kept ten seconds, not its four minutes: no Redis tells it of a
              // branch deleted and made again under its name, with new passwords.
              "--project-info-cache", "size=10000,ttl=10s,max_roles=10,gc_interval=60m",
            ],
            // Its token for neon-control, of the scope that answers it alone.
            env: [secretVariable(name, "NEON_PROXY_TO_CONTROLPLANE_TOKEN")],
            ports: [{ name: "postgres", containerPort: PROXY_PORT }, { name: "http", containerPort: 7001 }],
            readinessProbe: { tcpSocket: { port: "postgres" }, periodSeconds: 5 },
            securityContext: { ...restrictedContainer(), readOnlyRootFilesystem: true },
            resources,
          }],
        },
      },
    },
  };
  return [
    service(name, component, [{ name: "postgres", port: PROXY_PORT }, { name: "http", port: 7001 }]),
    deployment,
    ...replicaBudget(component, NAMESPACE, scaling),
    verticalPodAutoscaler(deployment, "InPlace"),
  ];
}

/**
 * The compute: Postgres on alasio's timeline, one primary (never two on one timeline),
 * its own disk rebuilt from the safekeepers and pageserver on every start, its spec
 * fetched from neon-control. It serves compute_ctl's metrics too, which the collector
 * scrapes, and exports compute_ctl's traces to the collector, when it runs. A branch
 * environment's is on the branch's timeline, in its namespace, reaching the stack's
 * storage and neon-control in main's.
 *
 * neon-control wakes it and puts it to sleep, scaling its Deployment between one replica
 * and none: so applying alasio gives it none, as it would take the count back, and a
 * compute made anew starts at one. Its Service takes connections before its pod is ready,
 * so a wake is answered as soon as Postgres takes them.
 *
 * Beside it, where the lake runs, its read replica (REPLICA_NAME), which the lake loads
 * from: a compute of its own, on the same timeline, streaming its WAL from the
 * safekeepers, as neon-control specifies it, smaller, woken and put to sleep as the
 * primary is, of the class of what sleeps, as it serves the loader alone.
 */
export function compute(config: InstallConfig, environment: Environment = MAIN_ENVIRONMENT): KubernetesObject[] {
  return [...computeObjects(config, environment, false), ...(lakeRuns(config) ? computeObjects(config, environment, true) : [])];
}

/** The name of a read replica's Deployment and Service, beside its primary's in each environment. */
export const REPLICA_NAME = `${neonName("compute")}${REPLICA_SUFFIX}`;

/** The objects of the environment's primary compute, or with `replica` its read replica's. */
function computeObjects(config: InstallConfig, environment: Environment, replica: boolean): KubernetesObject[] {
  const name = replica ? REPLICA_NAME : neonName("compute");
  const component = replica ? "neon-compute-ro" : "neon-compute";
  const control = `http://${neonName("control")}.${NAMESPACE}.svc:8080`;
  const { suspendAfter } = config.neon.compute;
  const resources = replica ? config.neon.readReplica.resources : config.neon.compute.resources;
  const branch = environment.branch ?? MAIN;
  const deployment: V1Deployment = {
    apiVersion: "apps/v1",
    kind: "Deployment",
    metadata: {
      name,
      namespace: environment.namespace,
      labels: stackLabels(component),
      // A valid duration, as the configuration checks.
      annotations: { [SUSPEND_AFTER]: String(durationSeconds(suspendAfter)) },
    },
    spec: {
      strategy: { type: "Recreate" },
      selector: { matchLabels: selectorLabels(component) },
      template: {
        metadata: { labels: stackLabels(component) },
        spec: {
          // Main's primary is alasio's database, which every turn needs; a branch's, and every
          // read replica, serve what may wait.
          ...priority(environment.branch === null && !replica ? "floor" : "component"),
          ...imagePullSecrets(config),
          // The compute image runs as its own postgres user.
          securityContext: { fsGroup: config.neon.runAsGroup, seccompProfile: { type: "RuntimeDefault" } },
          ...given("nodeSelector", { ...config.neon.nodeSelector }),
          ...given("tolerations", [...config.neon.tolerations]),
          terminationGracePeriodSeconds: 30,
          initContainers: [waitFor(config, "neon-control", `${control}/healthz`)],
          containers: [{
            name: "compute",
            image: imageReference(config.neon.computeImage),
            imagePullPolicy: "IfNotPresent",
            args: [
              "--pgdata",
              "/var/db/postgres/compute",
              "--connstr",
              `postgresql://cloud_admin@localhost:${COMPUTE_PORT}/postgres`,
              "--pgbin",
              "/usr/local/bin/postgres",
              "--compute-id",
              replica ? replicaId(branch) : computeId(branch),
              "--control-plane-uri",
              control,
            ],
            env: [
              // The primary's token, which fetches its replica's spec too.
              secretVariable(neonName("compute"), "NEON_CONTROL_PLANE_TOKEN"),
              ...(collectorRuns(config)
                ? [{ name: "OTEL_SERVICE_NAME", value: "compute_ctl" }, ...otelEnv(config, environment)]
                : [{ name: "OTEL_SDK_DISABLED", value: "true" }]),
            ],
            ports: [{ name: "postgres", containerPort: COMPUTE_PORT }, { name: "http", containerPort: COMPUTE_CTL_PORT }],
            readinessProbe: { exec: { command: ["pg_isready", "-h", "127.0.0.1", "-p", String(COMPUTE_PORT), "-U", "cloud_admin", "-d", "postgres"] }, periodSeconds: 5 },
            securityContext: restrictedContainer(),
            resources,
            volumeMounts: [{ name: "pgdata", mountPath: "/var/db/postgres" }],
          }],
          volumes: [{ name: "pgdata", emptyDir: {} }],
        },
      },
    },
  };
  const service: V1Service = {
    apiVersion: "v1",
    kind: "Service",
    metadata: { name, namespace: environment.namespace, labels: stackLabels(component) },
    spec: { selector: selectorLabels(component), publishNotReadyAddresses: true, ports: [{ name: "postgres", port: COMPUTE_PORT }, { name: "http", port: COMPUTE_CTL_PORT }] },
  };
  return [service, deployment, verticalPodAutoscaler(deployment, "Recommended")];
}

/**
 * A logical dump of alasio's database, restorable into any Postgres whatever becomes of
 * the stack, to the object store's backups bucket, and beside it, with workspace storage,
 * the newest dump JuiceFS has made of its file system's metadata, which it makes in the
 * workspaces bucket's `<name>/meta/`; of each, the last `keep` are kept.
 */
function backup(config: InstallConfig): V1CronJob {
  const component = "neon-backup";
  const { objectStore, workspaceStorage } = config;
  const dump: V1Container = {
    name: "dump",
    image: imageReference(config.neon.computeImage),
    imagePullPolicy: "IfNotPresent",
    command: [
      "/bin/sh",
      "-c",
      // A new pod's NetworkPolicy may apply a moment after it starts, so the proxy is
      // waited for rather than given up on at the first refusal.
      `until pg_isready --quiet; do sleep 2; done && pg_dump --username=${ROLE} --dbname=${DATABASE} --format=custom --file=/backup/alasio.dump`,
    ],
    env: [
      { name: "PGHOST", value: PROXY_HOST },
      { name: "PGPORT", value: String(PROXY_PORT) },
      // Main's compute, through the proxy, which wakes it if it sleeps.
      { name: "PGOPTIONS", value: `endpoint=${computeId(MAIN)}` },
      { name: "PGCONNECT_TIMEOUT", value: "60" },
      { name: "PGPASSWORD", valueFrom: { secretKeyRef: { name: databaseSecret(config), key: "password" } } },
    ],
    securityContext: restrictedContainer(),
    resources: { requests: { cpu: "100m", memory: "128Mi" }, limits: { memory: "1Gi" } },
    volumeMounts: [{ name: "backup", mountPath: "/backup" }],
  };
  const upload: V1Container = {
    name: "upload",
    image: imageReference(config.neon.image),
    imagePullPolicy: "IfNotPresent",
    command: [
      "/bin/sh",
      "-c",
      script(
        "set -eu",
        's3() { aws s3 --endpoint-url "$S3_ENDPOINT" "$@"; }',
        // Deletes all but the newest KEEP of the backups whose names match $1, which sort by when they were made.
        "prune() { s3 ls \"s3://$BUCKET/\" | awk '{print $4}' | grep \"$1\" | sort -r | tail -n +$((KEEP + 1)) \\",
        '  | while read -r old; do s3 rm "s3://$BUCKET/$old"; done; }',
        "stamp=$(date -u +%Y%m%dT%H%M%SZ)",
        's3 cp "$BACKUP_DIR/alasio.dump" "s3://$BUCKET/alasio-$stamp.dump"',
        "prune '^alasio-.*\\.dump$'",
        ...(workspaceStorage.enabled
          ? [
            'meta="s3://$WORKSPACES_BUCKET/$WORKSPACES_NAME/meta/"',
            "dump=$(s3 ls \"$meta\" | awk '{print $4}' | grep '^dump-.*\\.json\\.gz$' | sort -r | head -n 1)",
            // Copied by its bytes, read and written as every object is, not by the object store's
            // own copy (CopyObject), which SeaweedFS failed for a dump, all of the job's tries;
            // checked as the gzip it is, and read back the same once written. None until a
            // workspace's volume has been mounted for a while.
            'copied="s3://$BUCKET/$WORKSPACES_NAME-$dump"',
            'if [ -n "$dump" ]; then',
            '  s3 cp "$meta$dump" "$BACKUP_DIR/workspaces.json.gz"',
            '  gzip -t "$BACKUP_DIR/workspaces.json.gz" 2>/dev/null || { echo "$dump is no whole gzip" >&2; exit 1; }',
            '  s3 cp "$BACKUP_DIR/workspaces.json.gz" "$copied"',
            '  s3 cp "$copied" "$BACKUP_DIR/workspaces.copied"',
            '  cmp -s "$BACKUP_DIR/workspaces.json.gz" "$BACKUP_DIR/workspaces.copied" || { echo "the copy of $dump in $BUCKET is not the dump read" >&2; exit 1; }',
            'else echo "JuiceFS has dumped no metadata of $WORKSPACES_NAME yet"; fi',
            'prune "^$WORKSPACES_NAME-dump-.*\\.json\\.gz$"',
          ]
          : []),
      ),
    ],
    env: [
      { name: "S3_ENDPOINT", value: s3Endpoint(config) },
      { name: "BUCKET", value: objectStore.buckets.backups },
      { name: "KEEP", value: String(config.neon.backup.keep) },
      // Where the database's dump is, which the backup's own reads of JuiceFS's are put beside.
      { name: "BACKUP_DIR", value: "/backup" },
      ...(workspaceStorage.enabled
        ? [{ name: "WORKSPACES_BUCKET", value: objectStore.buckets.workspaces }, { name: "WORKSPACES_NAME", value: workspaceStorage.name }]
        : []),
      { name: "AWS_DEFAULT_REGION", value: objectStore.external.region },
      { name: "HOME", value: "/tmp" },
    ],
    // Its own identity: it writes the backups' bucket and reads what it backs up, and no more.
    envFrom: [{ secretRef: { name: neonName("s3-backups") } }],
    securityContext: restrictedContainer(),
    resources: { requests: { cpu: "50m", memory: "128Mi" }, limits: { memory: "512Mi" } },
    volumeMounts: [{ name: "backup", mountPath: "/backup" }, { name: "tmp", mountPath: "/tmp" }],
  };
  return {
    apiVersion: "batch/v1",
    kind: "CronJob",
    metadata: { name: neonName("backup"), namespace: NAMESPACE, labels: stackLabels(component) },
    spec: {
      schedule: config.neon.backup.schedule,
      concurrencyPolicy: "Forbid",
      successfulJobsHistoryLimit: 1,
      failedJobsHistoryLimit: 3,
      jobTemplate: {
        spec: {
          backoffLimit: 2,
          activeDeadlineSeconds: 7200,
          template: {
            metadata: { labels: stackLabels(component) },
            spec: {
              restartPolicy: "Never",
              ...priority("component"),
              ...stackPodSpec(config),
              initContainers: [dump],
              containers: [upload],
              volumes: [{ name: "backup", emptyDir: {} }, { name: "tmp", emptyDir: {} }],
            },
          },
        },
      },
    },
  };
}

/** The stack's setup, which runs with Neon or without it, and Neon's objects, when it runs. */
export function neonObjects(config: InstallConfig): KubernetesObject[] {
  const { neon } = config;
  if (!neon.enabled) return setup(config);
  return [
    ...setup(config),
    ...controllerDb(config),
    ...storageBroker(config),
    ...storageController(config),
    ...safekeepers(config),
    ...pageserver(config),
    ...control(config),
    ...proxy(config),
    ...compute(config),
    ...(neon.backup.enabled ? [backup(config)] : []),
  ];
}
