/**
 * The bundled object store: SeaweedFS, one server with its S3 gateway, its identities
 * from the stack's setup, and a bucket each for Neon, the lake, backups and, when it is
 * on, workspace storage; and the
 * periodic pass that applies its lifecycle rules.
 */
import type { KubernetesObject, V1CronJob, V1Service, V1StatefulSet } from "@kubernetes/client-node";

import { imageReference } from "../images.ts";
import { claimSpec, componentName, NAMESPACE, neonName, restrictedContainer, script, selectorLabels, stackLabels, stackPodSpec } from "./common.ts";
import type { InstallConfig } from "./config.ts";

/** Every port its servers advertise themselves on by the Service's name, gRPC's each the HTTP port plus 10000. */
const PORTS = [
  { name: "s3", port: 8333 },
  { name: "s3-grpc", port: 18333 },
  { name: "master", port: 9333 },
  { name: "master-grpc", port: 19333 },
  { name: "volume", port: 8080 },
  { name: "volume-grpc", port: 18080 },
  { name: "filer", port: 8888 },
  { name: "filer-grpc", port: 18888 },
  { name: "metrics", port: 9327 },
];

/** The ports by which the server reaches itself: its master, volume and filer servers'. */
const PEER_PORTS = PORTS.filter(({ name }) => /^(master|volume|filer)/u.test(name));

/**
 * The lifecycle rules of Neon's bucket: old versions of the pageserver's objects expire
 * a week after they are replaced, as Neon's own point-in-time window does; the
 * safekeepers' after a day, the least S3 allows, since each re-uploads the segment it
 * writes, whole, as it grows.
 */
const LIFECYCLE = {
  Rules: [
    {
      ID: "expire-noncurrent",
      Status: "Enabled",
      Filter: { Prefix: "pageserver/" },
      NoncurrentVersionExpiration: { NoncurrentDays: 7 },
      AbortIncompleteMultipartUpload: { DaysAfterInitiation: 1 },
    },
    {
      ID: "expire-noncurrent-wal",
      Status: "Enabled",
      Filter: { Prefix: "safekeeper/" },
      NoncurrentVersionExpiration: { NoncurrentDays: 1 },
      AbortIncompleteMultipartUpload: { DaysAfterInitiation: 1 },
    },
  ],
};

/** The server, and the Services by which it is reached and reaches itself. */
function server(config: InstallConfig): KubernetesObject[] {
  const name = componentName("seaweedfs");
  const peers = `${name}-peers`;
  const { bundled, buckets } = config.objectStore;
  const made = [buckets.neon, buckets.lake, buckets.backups, ...(config.workspaceStorage.enabled ? [buckets.workspaces] : [])];
  const image = imageReference(bundled.image);
  const ready = { exec: { command: ["test", "-f", "/tmp/ready"] }, periodSeconds: 5 };
  const service: V1Service = {
    apiVersion: "v1",
    kind: "Service",
    metadata: { name, namespace: NAMESPACE, labels: stackLabels("seaweedfs") },
    spec: { selector: selectorLabels("seaweedfs"), ports: PORTS },
  };
  // The server's own stable name, which it advertises and reaches itself by (its master
  // elects itself leader through it) before it is ready, as the Service above admits
  // clients only once its buckets exist.
  const peersService: V1Service = {
    apiVersion: "v1",
    kind: "Service",
    metadata: { name: peers, namespace: NAMESPACE, labels: stackLabels("seaweedfs") },
    spec: { clusterIP: "None", publishNotReadyAddresses: true, selector: selectorLabels("seaweedfs"), ports: PEER_PORTS },
  };
  const statefulSet: V1StatefulSet = {
    apiVersion: "apps/v1",
    kind: "StatefulSet",
    metadata: { name, namespace: NAMESPACE, labels: stackLabels("seaweedfs") },
    spec: {
      serviceName: peers,
      replicas: 1,
      selector: { matchLabels: selectorLabels("seaweedfs") },
      template: {
        metadata: { labels: stackLabels("seaweedfs") },
        spec: {
          ...stackPodSpec(config),
          terminationGracePeriodSeconds: 30,
          containers: [
            {
              name: "seaweedfs",
              image,
              imagePullPolicy: "IfNotPresent",
              args: [
                "server",
                `-ip=$(POD_NAME).${peers}.$(POD_NAMESPACE).svc.cluster.local`,
                "-ip.bind=0.0.0.0",
                "-dir=/data",
                `-volume.max=${bundled.volumes}`,
                "-master.volumeSizeLimitMB=1024",
                "-master.defaultReplication=000",
                "-master.telemetry=false",
                `-volume.minFreeSpace=${bundled.minFreeSpace}`,
                "-filer",
                "-s3",
                "-s3.config=/etc/seaweedfs/s3.json",
                "-s3.allowDeleteBucketNotEmpty=false",
                "-s3.autoCreateBucket=false",
                "-metricsPort=9327",
              ],
              env: [
                { name: "POD_NAME", valueFrom: { fieldRef: { fieldPath: "metadata.name" } } },
                { name: "POD_NAMESPACE", valueFrom: { fieldRef: { fieldPath: "metadata.namespace" } } },
              ],
              ports: PORTS.map(({ name, port }) => ({ name, containerPort: port })),
              readinessProbe: { httpGet: { path: "/healthz", port: "s3" }, periodSeconds: 5 },
              livenessProbe: { httpGet: { path: "/healthz", port: "s3" }, initialDelaySeconds: 30, periodSeconds: 10, failureThreshold: 6 },
              securityContext: restrictedContainer(),
              resources: bundled.resources,
              volumeMounts: [{ name: "data", mountPath: "/data" }, { name: "identities", mountPath: "/etc/seaweedfs", readOnly: true }],
            },
            // Makes the buckets, idempotently, each time the server starts: Neon's versioned
            // (its time-travel recovery needs that), writes fsynced. The pod is ready only
            // once they exist, so nothing that stores in them starts before.
            {
              name: "buckets",
              image,
              imagePullPolicy: "IfNotPresent",
              command: [
                "/bin/sh",
                "-c",
                script(
                  "set -e",
                  "shell() { printf '%s\\n' \"$1\" | weed shell -master=127.0.0.1:9333 -filer=127.0.0.1:8888; }",
                  "until wget -q -O /dev/null http://127.0.0.1:8333/healthz; do sleep 2; done",
                  "shell 'fs.configure -locationPrefix=/buckets/ -volumeGrowthCount=1 -fsync -apply'",
                  `for bucket in ${made.join(" ")}; do`,
                  "  shell 's3.bucket.list' | grep -qw \"$bucket\" || shell \"s3.bucket.create -name $bucket\"",
                  "done",
                  `shell 's3.bucket.versioning -name ${buckets.neon} -enable'`,
                  `shell 's3.bucket.versioning -name ${buckets.neon}' | grep -qi enabled`,
                  "touch /tmp/ready",
                  "exec sleep infinity",
                ),
              ],
              readinessProbe: ready,
              securityContext: restrictedContainer(),
              resources: { requests: { cpu: "10m", memory: "32Mi" }, limits: { memory: "256Mi" } },
              volumeMounts: [{ name: "shell-tmp", mountPath: "/tmp" }],
            },
            // Gives Neon's bucket its lifecycle rules once it exists.
            {
              name: "lifecycle",
              image: imageReference(config.neon.image),
              imagePullPolicy: "IfNotPresent",
              command: [
                "/bin/sh",
                "-c",
                script(
                  "set -e",
                  `until aws s3api --endpoint-url http://127.0.0.1:8333 head-bucket --bucket ${buckets.neon} 2>/dev/null; do sleep 2; done`,
                  `aws s3api --endpoint-url http://127.0.0.1:8333 put-bucket-lifecycle-configuration --bucket ${buckets.neon} --lifecycle-configuration '${JSON.stringify(LIFECYCLE)}'`,
                  "touch /tmp/ready",
                  "exec sleep infinity",
                ),
              ],
              env: [{ name: "AWS_DEFAULT_REGION", value: "us-east-1" }, { name: "HOME", value: "/tmp" }],
              envFrom: [{ secretRef: { name: neonName("s3-admin") } }],
              readinessProbe: ready,
              securityContext: restrictedContainer(),
              resources: { requests: { cpu: "10m", memory: "64Mi" }, limits: { memory: "256Mi" } },
              volumeMounts: [{ name: "lifecycle-tmp", mountPath: "/tmp" }],
            },
          ],
          volumes: [
            { name: "identities", secret: { secretName: neonName("seaweedfs") } },
            { name: "shell-tmp", emptyDir: {} },
            { name: "lifecycle-tmp", emptyDir: {} },
          ],
        },
      },
      volumeClaimTemplates: [{ metadata: { name: "data" }, spec: claimSpec(bundled.storage) }],
    },
  };
  return [service, peersService, statefulSet];
}

/**
 * SeaweedFS applies lifecycle rules only when asked: a pass every hour, deleting through
 * the S3 server's gRPC port. A pass replays the filer's log of changes from where each
 * shard's last stopped, which is the oldest change with an expiry not yet due; so it reads
 * to now (`-events 0`), not a count of changes, which would hold a short rule's expiries
 * behind a long one's until that one's came due. It stops at its runtime, well before the
 * next, and the one after it goes on from there.
 */
function lifecyclePass(config: InstallConfig): V1CronJob {
  const name = componentName("seaweedfs");
  const component = "seaweedfs-lifecycle";
  return {
    apiVersion: "batch/v1",
    kind: "CronJob",
    metadata: { name: componentName(component), namespace: NAMESPACE, labels: stackLabels(component) },
    spec: {
      schedule: "41 * * * *",
      concurrencyPolicy: "Forbid",
      successfulJobsHistoryLimit: 1,
      failedJobsHistoryLimit: 3,
      jobTemplate: {
        spec: {
          backoffLimit: 1,
          activeDeadlineSeconds: 900,
          template: {
            metadata: { labels: stackLabels(component) },
            spec: {
              restartPolicy: "Never",
              ...stackPodSpec(config),
              containers: [{
                name: "lifecycle",
                image: imageReference(config.objectStore.bundled.image),
                imagePullPolicy: "IfNotPresent",
                command: [
                  "/bin/sh",
                  "-c",
                  // Waits out the moment before its NetworkPolicy applies, as a new pod.
                  `until wget -q -O /dev/null http://${name}:9333/cluster/status; do sleep 2; done && printf 's3.lifecycle.run-shard -shards 0-15 -s3 ${name}:18333 -events 0 -runtime 10m\\n' | weed shell -master=${name}:9333 -filer=${name}:8888`,
                ],
                securityContext: restrictedContainer(),
                resources: { requests: { cpu: "10m", memory: "32Mi" }, limits: { memory: "256Mi" } },
                volumeMounts: [{ name: "tmp", mountPath: "/tmp" }],
              }],
              volumes: [{ name: "tmp", emptyDir: {} }],
            },
          },
        },
      },
    },
  };
}

/** The bundled object store's objects, when it runs, which is beside Neon. */
export function objectStoreObjects(config: InstallConfig): KubernetesObject[] {
  return config.neon.enabled && config.objectStore.bundled.enabled ? [...server(config), lifecyclePass(config)] : [];
}
