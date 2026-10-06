/**
 * Neon, alasio's database: Postgres whose storage is safekeepers and a pageserver on
 * object storage, with a storage controller and its own Postgres, a storage broker, the
 * compute, neon-control (alasio's stand-in for Neon's control plane, neon/control/),
 * the setup that makes the stack's secrets, a daily backup, and a collector of the
 * stack's metrics.
 */
import type {
  KubernetesObject,
  V1ConfigMap,
  V1Container,
  V1CronJob,
  V1Deployment,
  V1EnvVar,
  V1Job,
  V1PersistentVolumeClaim,
  V1Role,
  V1RoleBinding,
  V1Service,
  V1ServiceAccount,
  V1ServicePort,
  V1StatefulSet,
} from "@kubernetes/client-node";

import { imageReference } from "../images.ts";
import {
  claimSpec,
  componentName,
  databaseSecret,
  given,
  goJson,
  helperResources,
  imagePullSecrets,
  NAMESPACE,
  neonName,
  otelEnv,
  RELEASE,
  restrictedContainer,
  s3Endpoint,
  script,
  selectorLabels,
  sha256,
  stackLabels,
  stackPodSpec,
  waitFor,
  waitForObjectStore,
} from "./common.ts";
import type { InstallConfig } from "./config.ts";
import { DRIVER_METRICS_PORT, DRIVER_POD_NAME, JUICEFS_METRICS_PORT, MOUNT_POD_NAME, VOLUME_DRIVER } from "./juicefs-csi.ts";
import { VALKEY, VALKEY_PORT } from "./valkey.ts";
import { VALKEY_ADDRESS, workspacesBucketUrl } from "./workspace-storage.ts";

/** A Service of the stack's: its name and component, selecting the component's pods, on `ports`. */
function service(name: string, component: string, ports: V1ServicePort[]): V1Service {
  return {
    apiVersion: "v1",
    kind: "Service",
    metadata: { name, namespace: NAMESPACE, labels: stackLabels(component) },
    spec: { selector: selectorLabels(component), ports },
  };
}

/** A Secret's key, as a variable of the same name. */
function secretVariable(secret: string, key: string): V1EnvVar {
  return { name: key, valueFrom: { secretKeyRef: { name: secret, key } } };
}

/**
 * The stack's secrets, made once and kept, and each service's rendered from them, workspace
 * storage's among them when it is on, by neon/control/kube-setup.ts: a Job, which applying alasio runs to completion before
 * anything else of alasio starts (../kube/apply.ts).
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
          ...stackPodSpec(config),
          containers: [{
            name: "setup",
            image: imageReference(config.images.alasio),
            imagePullPolicy: config.images.pullPolicy,
            command: ["node", "/opt/alasio/neon/control/kube-setup.ts"],
            env: [
              { name: "NAMESPACE", valueFrom: { fieldRef: { fieldPath: "metadata.namespace" } } },
              { name: "SECRET_PREFIX", value: RELEASE },
              { name: "NEON_BROKER_URL", value: `http://${neonName("storage-broker")}:50051` },
              { name: "NEON_CONTROLLER_URL", value: `http://${neonName("storage-controller")}:1234` },
              { name: "NEON_PAGESERVER_HOST", value: neonName("pageserver") },
              { name: "NEON_COMPUTE_HOST", value: neonName("compute") },
              { name: "NEON_CONTROLLER_DB_HOST", value: neonName("controller-db") },
              { name: "S3_ENDPOINT", value: s3Endpoint(config) },
              { name: "S3_REGION", value: objectStore.external.region },
              { name: "S3_BUCKET_NEON", value: objectStore.buckets.neon },
              { name: "S3_BUCKET_LAKE", value: objectStore.buckets.lake },
              ...(workspaceStorage.enabled
                ? [
                  { name: "WORKSPACES_NAME", value: workspaceStorage.name },
                  { name: "WORKSPACES_BUCKET", value: objectStore.buckets.workspaces },
                  { name: "WORKSPACES_BUCKET_URL", value: workspacesBucketUrl(config) },
                  { name: "WORKSPACES_TRASH_DAYS", value: String(workspaceStorage.trashDays) },
                  { name: "VALKEY_ADDRESS", value: VALKEY_ADDRESS },
                  { name: "WORKSPACES_SECRET_LABELS", value: JSON.stringify(VOLUME_DRIVER) },
                ]
                : []),
              ...(objectStore.bundled.enabled ? [] : [
                { name: "S3_EXTERNAL", value: "1" },
                { name: "S3_ACCESS_KEY", valueFrom: { secretKeyRef: { name: objectStore.external.existingSecret, key: "accessKey" } } },
                { name: "S3_SECRET_KEY", valueFrom: { secretKeyRef: { name: objectStore.external.existingSecret, key: "secretKey" } } },
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
      replicas: 1,
      selector: { matchLabels: selectorLabels(component) },
      template: {
        metadata: { labels: stackLabels(component) },
        spec: {
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
  return [service(name, component, [{ name: "postgres", port: 5432 }]), statefulSet];
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
      replicas: 1,
      selector: { matchLabels: selectorLabels(component) },
      template: {
        metadata: { labels: stackLabels(component) },
        spec: {
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
  return [service(name, component, [{ name: "grpc", port: 50051 }]), deployment];
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
      replicas: 1,
      strategy: { type: "Recreate" },
      selector: { matchLabels: selectorLabels(component) },
      template: {
        metadata: { labels: stackLabels(component) },
        spec: {
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
                `--timeline-safekeeper-count ${config.neon.safekeepers.replicas}`,
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
  return [service(name, component, [{ name: "http", port: 1234 }]), deployment];
}

/**
 * The safekeepers: a timeline's WAL, each on its own volume, at stable names neon-control
 * registers them by, which resolve before they are ready, as the controller needs. They
 * spread across nodes and zones where the cluster has them, so a quorum survives one.
 */
function safekeepers(config: InstallConfig): KubernetesObject[] {
  const name = neonName("safekeeper");
  const component = "neon-safekeeper";
  const spread = (topologyKey: string) => ({
    maxSkew: 1,
    topologyKey,
    whenUnsatisfiable: "ScheduleAnyway",
    labelSelector: { matchLabels: selectorLabels(component) },
  });
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
      replicas: config.neon.safekeepers.replicas,
      podManagementPolicy: "Parallel",
      selector: { matchLabels: selectorLabels(component) },
      template: {
        metadata: { labels: stackLabels(component) },
        spec: {
          ...stackPodSpec(config),
          terminationGracePeriodSeconds: 30,
          ...given("initContainers", waitForObjectStore(config)),
          topologySpreadConstraints: [spread("kubernetes.io/hostname"), spread("topology.kubernetes.io/zone")],
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
  return [headless, statefulSet];
}

/**
 * The pageserver: pages served from layers it keeps in the object store, its volume a
 * cache. It registers itself with the controller by the name in its metadata.
 */
function pageserver(config: InstallConfig): KubernetesObject[] {
  const name = neonName("pageserver");
  const component = "neon-pageserver";
  const image = imageReference(config.neon.image);
  const statefulSet: V1StatefulSet = {
    apiVersion: "apps/v1",
    kind: "StatefulSet",
    metadata: { name, namespace: NAMESPACE, labels: stackLabels(component) },
    spec: {
      serviceName: name,
      replicas: 1,
      selector: { matchLabels: selectorLabels(component) },
      template: {
        metadata: { labels: stackLabels(component) },
        spec: {
          ...stackPodSpec(config),
          terminationGracePeriodSeconds: 30,
          initContainers: [
            {
              name: "config",
              image,
              imagePullPolicy: "IfNotPresent",
              // Its configuration, rendered by the stack's setup, into its working directory.
              command: ["/bin/sh", "-c", "cp /config/pageserver.toml /config/identity.toml /config/metadata.json /config/auth_public_key.pem /data/.neon/"],
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
            resources: config.neon.pageserver.resources,
            volumeMounts: [{ name: "data", mountPath: "/data/.neon" }],
          }],
          volumes: [{
            name: "config",
            secret: {
              secretName: name,
              items: ["pageserver.toml", "identity.toml", "metadata.json", "auth_public_key.pem"].map((key) => ({ key, path: key })),
            },
          }],
        },
      },
      volumeClaimTemplates: [{ metadata: { name: "data" }, spec: claimSpec(config.neon.pageserver.storage) }],
    },
  };
  return [service(name, component, [{ name: "pg", port: 6400 }, { name: "http", port: 9898 }]), statefulSet];
}

/**
 * neon-control: it bootstraps the tenant and timeline, serves the compute its spec,
 * answers the storage controller's hooks, and repairs a safekeeper that lost its disk.
 * Its record of what it bootstrapped is on a volume of its own.
 */
function control(config: InstallConfig): KubernetesObject[] {
  const name = neonName("control");
  const component = "neon-control";
  const safekeeper = neonName("safekeeper");
  const safekeeperHosts = Array.from(
    { length: config.neon.safekeepers.replicas },
    (_, index) => `${safekeeper}-${index}.${safekeeper}.${NAMESPACE}.svc.cluster.local`,
  );
  const claim: V1PersistentVolumeClaim = {
    apiVersion: "v1",
    kind: "PersistentVolumeClaim",
    metadata: { name, namespace: NAMESPACE, labels: stackLabels(component) },
    spec: claimSpec(config.neon.control.storage),
  };
  const deployment: V1Deployment = {
    apiVersion: "apps/v1",
    kind: "Deployment",
    metadata: { name, namespace: NAMESPACE, labels: stackLabels(component) },
    spec: {
      replicas: 1,
      strategy: { type: "Recreate" },
      selector: { matchLabels: selectorLabels(component) },
      template: {
        metadata: { labels: stackLabels(component) },
        spec: {
          ...stackPodSpec(config),
          containers: [{
            name: "neon-control",
            image: imageReference(config.images.alasio),
            imagePullPolicy: config.images.pullPolicy,
            command: ["node", "/opt/alasio/neon/control/service.ts"],
            env: [
              { name: "CONTROLLER_URL", value: `http://${neonName("storage-controller")}:1234` },
              { name: "NEON_SAFEKEEPER_HOSTS", value: safekeeperHosts.join(",") },
              { name: "NEON_PAGESERVER_HOST", value: neonName("pageserver") },
            ],
            ports: [{ name: "http", containerPort: 8080 }],
            readinessProbe: { httpGet: { path: "/healthz", port: "http" }, periodSeconds: 3 },
            securityContext: { ...restrictedContainer(), readOnlyRootFilesystem: true },
            resources: config.neon.control.resources,
            volumeMounts: [
              { name: "state", mountPath: "/state" },
              { name: "root", mountPath: "/secrets", readOnly: true },
              { name: "keys", mountPath: "/keys", readOnly: true },
            ],
          }],
          volumes: [
            { name: "state", persistentVolumeClaim: { claimName: name } },
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
  return [service(name, component, [{ name: "http", port: 8080 }]), claim, deployment];
}

/**
 * The compute: Postgres on alasio's timeline, one primary (never two on one timeline),
 * its own disk rebuilt from the safekeepers and pageserver on every start, its spec
 * fetched from neon-control. It serves compute_ctl's metrics too, which the collector
 * scrapes, and traces where alasio's telemetry goes, or nowhere.
 */
function compute(config: InstallConfig): KubernetesObject[] {
  const name = neonName("compute");
  const component = "neon-compute";
  const control = neonName("control");
  const deployment: V1Deployment = {
    apiVersion: "apps/v1",
    kind: "Deployment",
    metadata: { name, namespace: NAMESPACE, labels: stackLabels(component) },
    spec: {
      replicas: 1,
      strategy: { type: "Recreate" },
      selector: { matchLabels: selectorLabels(component) },
      template: {
        metadata: { labels: stackLabels(component) },
        spec: {
          ...imagePullSecrets(config),
          // The compute image runs as its own postgres user.
          securityContext: { fsGroup: config.neon.runAsGroup, seccompProfile: { type: "RuntimeDefault" } },
          ...given("nodeSelector", { ...config.neon.nodeSelector }),
          ...given("tolerations", [...config.neon.tolerations]),
          terminationGracePeriodSeconds: 30,
          initContainers: [waitFor(config, "neon-control", `http://${control}:8080/healthz`)],
          containers: [{
            name: "compute",
            image: imageReference(config.neon.computeImage),
            imagePullPolicy: "IfNotPresent",
            args: [
              "--pgdata",
              "/var/db/postgres/compute",
              "--connstr",
              "postgresql://cloud_admin@localhost:55433/postgres",
              "--pgbin",
              "/usr/local/bin/postgres",
              "--compute-id",
              "alasio",
              "--control-plane-uri",
              `http://${control}:8080`,
            ],
            env: [
              secretVariable(name, "NEON_CONTROL_PLANE_TOKEN"),
              ...(config.telemetry.otlpEndpoint
                ? [{ name: "OTEL_SERVICE_NAME", value: "compute_ctl" }, ...otelEnv(config)]
                : [{ name: "OTEL_SDK_DISABLED", value: "true" }]),
            ],
            ports: [{ name: "postgres", containerPort: 55433 }, { name: "http", containerPort: 3080 }],
            readinessProbe: { exec: { command: ["pg_isready", "-h", "127.0.0.1", "-p", "55433", "-U", "cloud_admin", "-d", "postgres"] }, periodSeconds: 5 },
            securityContext: restrictedContainer(),
            resources: config.neon.compute.resources,
            volumeMounts: [{ name: "pgdata", mountPath: "/var/db/postgres" }],
          }],
          volumes: [{ name: "pgdata", emptyDir: {} }],
        },
      },
    },
  };
  return [service(name, component, [{ name: "postgres", port: 55433 }, { name: "http", port: 3080 }]), deployment];
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
      // A new pod's NetworkPolicy may apply a moment after it starts, so the compute is
      // waited for rather than given up on at the first refusal.
      'until pg_isready --quiet --host="$0" --port=55433; do sleep 2; done && pg_dump --host="$0" --port=55433 --username=alasio --dbname=alasio --format=custom --file=/backup/alasio.dump',
      neonName("compute"),
    ],
    env: [{ name: "PGPASSWORD", valueFrom: { secretKeyRef: { name: databaseSecret(config), key: "password" } } }],
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
        's3 cp /backup/alasio.dump "s3://$BUCKET/alasio-$stamp.dump"',
        "prune '^alasio-.*\\.dump$'",
        ...(workspaceStorage.enabled
          ? [
            'meta="s3://$WORKSPACES_BUCKET/$WORKSPACES_NAME/meta/"',
            "dump=$(s3 ls \"$meta\" | awk '{print $4}' | grep '^dump-.*\\.json\\.gz$' | sort -r | head -n 1)",
            // None until a workspace's volume has been mounted for a while.
            'if [ -n "$dump" ]; then s3 cp "$meta$dump" "s3://$BUCKET/$WORKSPACES_NAME-$dump"; else echo "JuiceFS has dumped no metadata of $WORKSPACES_NAME yet"; fi',
            'prune "^$WORKSPACES_NAME-dump-.*\\.json\\.gz$"',
          ]
          : []),
      ),
    ],
    env: [
      { name: "S3_ENDPOINT", value: s3Endpoint(config) },
      { name: "BUCKET", value: objectStore.buckets.backups },
      { name: "KEEP", value: String(config.neon.backup.keep) },
      ...(workspaceStorage.enabled
        ? [{ name: "WORKSPACES_BUCKET", value: objectStore.buckets.workspaces }, { name: "WORKSPACES_NAME", value: workspaceStorage.name }]
        : []),
      { name: "AWS_DEFAULT_REGION", value: objectStore.external.region },
      { name: "HOME", value: "/tmp" },
    ],
    envFrom: [{ secretRef: { name: neonName("s3-admin") } }],
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

/**
 * The stack's telemetry: every Neon service's, SeaweedFS's and the lake's Prometheus
 * metrics, and with workspace storage JuiceFS's and Valkey's, scraped and sent on as OTLP
 * where alasio's telemetry goes, when it goes anywhere. JuiceFS's are its driver's and
 * mount pods', found among the pods of the driver's namespace, which the collector may
 * list; Valkey's are read with its password. Its pod carries its configuration's
 * checksum, so it is replaced when that changes.
 */
function collector(config: InstallConfig, endpoint: string): KubernetesObject[] {
  const name = neonName("collector");
  const component = "neon-collector";
  const { telemetry, workspaceStorage } = config;
  const safekeeper = neonName("safekeeper");
  const targets: Record<string, string[]> = {
    pageserver: [`${neonName("pageserver")}:9898`],
    "storage-controller": [`${neonName("storage-controller")}:1234`],
    "storage-broker": [`${neonName("storage-broker")}:50051`],
    compute: [`${neonName("compute")}:3080`],
    safekeeper: Array.from({ length: config.neon.safekeepers.replicas }, (_, index) => `${safekeeper}-${index}.${safekeeper}:7676`),
    ...(config.objectStore.bundled.enabled ? { seaweedfs: [`${componentName("seaweedfs")}:9327`] } : {}),
    ...(config.lake.enabled ? { lake: [`${componentName("lake")}:9464`] } : {}),
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
  // The mount pods, the clients, and the controller's and node service's, the driver.
  const juicefs = [juicefsJob("juicefs", MOUNT_POD_NAME, JUICEFS_METRICS_PORT), juicefsJob("juicefs-csi", DRIVER_POD_NAME, DRIVER_METRICS_PORT)];
  const collectorConfig = {
    receivers: {
      prometheus: {
        config: {
          scrape_configs: [
            ...Object.keys(targets).sort().map((job) => ({ job_name: job, scrape_interval: "30s", static_configs: [{ targets: targets[job] }] })),
            ...(workspaceStorage.enabled ? juicefs : []),
          ],
        },
      },
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
    processors: { resource: { attributes: [{ key: "service.namespace", value: "alasio-neon", action: "upsert" }] }, batch: {} },
    exporters: { otlphttp: { endpoint, ...(telemetry.headersSecret ? { headers: "${file:/etc/otelcol/headers/headers.yaml}" } : {}) } },
    service: {
      telemetry: { metrics: { level: "none" } },
      pipelines: {
        metrics: { receivers: ["prometheus", ...(workspaceStorage.enabled ? ["redis"] : [])], processors: ["resource", "batch"], exporters: ["otlphttp"] },
      },
    },
  };
  const configMap: V1ConfigMap = {
    apiVersion: "v1",
    kind: "ConfigMap",
    metadata: { name, namespace: NAMESPACE, labels: stackLabels(component) },
    data: { "config.yaml": goJson(collectorConfig, "  ") },
  };
  const deployment: V1Deployment = {
    apiVersion: "apps/v1",
    kind: "Deployment",
    metadata: { name, namespace: NAMESPACE, labels: stackLabels(component) },
    spec: {
      replicas: 1,
      selector: { matchLabels: selectorLabels(component) },
      template: {
        metadata: { labels: stackLabels(component), annotations: { "checksum/config": sha256(goJson(collectorConfig)) } },
        spec: {
          ...(workspaceStorage.enabled ? { serviceAccountName: name } : {}),
          ...stackPodSpec(config),
          containers: [{
            name: "collector",
            image: imageReference(config.neon.collector.image),
            imagePullPolicy: "IfNotPresent",
            args: ["--config=/etc/otelcol/config.yaml"],
            ...(workspaceStorage.enabled ? { env: [{ name: "VALKEY_PASSWORD", valueFrom: { secretKeyRef: { name: VALKEY, key: "password" } } }] } : {}),
            securityContext: { ...restrictedContainer(), readOnlyRootFilesystem: true },
            resources: config.neon.collector.resources,
            volumeMounts: [
              { name: "config", mountPath: "/etc/otelcol", readOnly: true },
              ...(telemetry.headersSecret ? [{ name: "headers", mountPath: "/etc/otelcol/headers", readOnly: true }] : []),
            ],
          }],
          volumes: [
            { name: "config", configMap: { name } },
            // The exporter's headers: a YAML map, under the key headersKey + ".yaml" in the
            // headers Secret, since the collector takes headers as a map.
            ...(telemetry.headersSecret
              ? [{ name: "headers", secret: { secretName: telemetry.headersSecret, items: [{ key: `${telemetry.headersKey}.yaml`, path: "headers.yaml" }] } }]
              : []),
          ],
        },
      },
    },
  };
  if (!workspaceStorage.enabled) return [configMap, deployment];
  // Who the collector is, and that it may find JuiceFS's pods in the driver's namespace.
  const serviceAccount: V1ServiceAccount = { apiVersion: "v1", kind: "ServiceAccount", metadata: { name, namespace: NAMESPACE, labels: stackLabels(component) } };
  const metadata = { name, namespace: workspaceStorage.csi.namespace, labels: stackLabels(component) };
  const role: V1Role = {
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "Role",
    metadata,
    rules: [{ apiGroups: [""], resources: ["pods"], verbs: ["get", "list", "watch"] }],
  };
  const binding: V1RoleBinding = {
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "RoleBinding",
    metadata,
    subjects: [{ kind: "ServiceAccount", name, namespace: NAMESPACE }],
    roleRef: { apiGroup: "rbac.authorization.k8s.io", kind: "Role", name },
  };
  return [serviceAccount, role, binding, configMap, deployment];
}

/** Neon's objects, when it runs. */
export function neonObjects(config: InstallConfig): KubernetesObject[] {
  const { neon } = config;
  if (!neon.enabled) return [];
  const collectorEndpoint = neon.collector.otlpEndpoint || config.telemetry.otlpEndpoint;
  return [
    ...setup(config),
    ...controllerDb(config),
    ...storageBroker(config),
    ...storageController(config),
    ...safekeepers(config),
    ...pageserver(config),
    ...control(config),
    ...compute(config),
    ...(neon.backup.enabled ? [backup(config)] : []),
    ...(neon.collector.enabled && collectorEndpoint ? collector(config, collectorEndpoint) : []),
  ];
}
