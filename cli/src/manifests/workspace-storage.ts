/**
 * Workspace storage: each session's workspace a directory of one JuiceFS file system,
 * `/<environment>/<workspace>`, made by a node agent with its quota (src/node-agent/
 * workspaces.ts), its metadata in Valkey (./valkey.ts), its data in the object store's
 * workspaces bucket, which each node mounts whole (./sandbox-gateway.ts workspaceMount) and
 * the node agents attach workspaces from. Deleting a workspace deletes its directory into
 * the file system's trash. The file system's credentials, the metadata engine's URL with
 * Valkey's password and the bucket's keys, are in a Secret the stack's setup makes
 * (./neon.ts), which the nodes' mount reads, and alasio's JuiceFS admin pods (the jobs
 * here), never a workspace.
 *
 * The nodes' mount of the file system runs no background job (`--no-bgjob`,
 * ./sandbox-gateway.ts workspaceMount), so what a client's background jobs would do is the
 * jobs' here, each of its own:
 * - the quota check (quotaCheck), daily: each workspace's usage counted anew and repaired;
 * - the collection (collection), daily, `juicefs gc --compact --delete`: what the trash keeps
 *   past its days expired, the trees of clones never attached removed, files and slices
 *   pending deletion deleted, leaked objects deleted, and chunks left in many slices
 *   compacted, which a mount otherwise compacts only as it reads and writes them;
 * - the metadata's dump (metadataDump), every backupInterval, as JuiceFS's own backup would
 *   have made it;
 * - the stale sessions' cleanup (staleSessions), every five minutes: the sessions of clients
 *   that died, their locks and the files they held open let go, which a lock a dead client
 *   held is held by until then, seven minutes at most.
 */
import type { KubernetesObject, V1Container, V1CronJob } from "@kubernetes/client-node";

import { WORKSPACE_QUOTAS_USER } from "../../../neon/control/secrets.ts";
import { imageReference } from "../images.ts";
import { componentName, imagePullSecrets, type Labels, labels, NAMESPACE, restrictedContainer, restrictedPod, s3Endpoint, script } from "./common.ts";
import { everyIntervalCron, type InstallConfig } from "./config.ts";
import { priority } from "./priorities.ts";
import { VALKEY_PORT, valkeyObjects, valkeyPassword } from "./valkey.ts";

/** The Secret of the file system's credentials. */
export const WORKSPACES_CREDENTIALS = componentName("workspaces-juicefs");
/** The Secret of the node agents' Valkey user, its password and the metadata engine's address as it logs in (neon/control/kube-setup.ts). */
export const WORKSPACE_QUOTAS = componentName("workspaces-quotas");
/** What the node agents' Valkey user may do, as Valkey's ACL rules say it. */
export const QUOTAS_RULES = ["~*", "&*", "+@all", "-@dangerous", "+info"] as const;

/**
 * The label of alasio's pods that run JuiceFS's command line against the file system, the
 * quota check's and a restore's, which its store policies admit (./network-policies.ts).
 */
export const JUICEFS_ADMIN: Labels = { "alasio.dev/workload": "juicefs-admin" };

/** The quota check runs daily, after the database's backup, and the collection after it. */
const QUOTA_CHECK_SCHEDULE = "47 3 * * *";
const COLLECTION_SCHEDULE = "17 4 * * *";
/** Whom the quota check and the collection run as: nobody, as they write nothing of their own. */
const NOBODY = 65534;

/** The file system's Valkey's name: its Service's, its StatefulSet's, and its password's Secret's. */
export const VALKEY = componentName("valkey");

/** The file system's Valkey's password, as a variable's source. */
export const VALKEY_PASSWORD = valkeyPassword(VALKEY);

/** Valkey's address, as what reaches it from another namespace does. */
export const VALKEY_ADDRESS = `${VALKEY}.${NAMESPACE}.svc.cluster.local:${VALKEY_PORT}`;

/** The workspaces bucket's URL, path-style, as what reaches it from another namespace does. */
export function workspacesBucketUrl({ objectStore }: InstallConfig): string {
  const endpoint = objectStore.bundled.enabled ? `http://${componentName("seaweedfs")}.${NAMESPACE}.svc.cluster.local:8333` : objectStore.external.endpoint.replace(/\/+$/u, "");
  return `${endpoint}/${objectStore.buckets.workspaces}`;
}

/**
 * A container of a JuiceFS admin pod, `name`, running `lines` with JuiceFS's command line
 * of the pinned client, the file system's metadata engine's URL in `META_URL`.
 */
function adminContainer(config: InstallConfig, name: string, ...lines: string[]): V1Container {
  return {
    name,
    image: imageReference(config.workspaceStorage.image),
    imagePullPolicy: "IfNotPresent",
    command: ["/bin/sh", "-c", script("set -eu", ...lines)],
    env: [{ name: "META_URL", valueFrom: { secretKeyRef: { name: WORKSPACES_CREDENTIALS, key: "metaurl" } } }],
    resources: { requests: { cpu: "50m", memory: "64Mi" }, limits: { memory: "512Mi" } },
  };
}

/**
 * A daily JuiceFS admin task, `component`, whose container `name` runs `lines` restricted
 * as nobody.
 */
function dailyAdminTask(config: InstallConfig, component: string, schedule: string, name: string, ...lines: string[]): V1CronJob {
  return {
    apiVersion: "batch/v1",
    kind: "CronJob",
    metadata: { name: componentName(component), namespace: NAMESPACE, labels: labels(component) },
    spec: {
      schedule,
      concurrencyPolicy: "Forbid",
      successfulJobsHistoryLimit: 1,
      failedJobsHistoryLimit: 3,
      jobTemplate: {
        spec: {
          backoffLimit: 2,
          activeDeadlineSeconds: 3600,
          template: {
            metadata: { labels: { ...labels(component), ...JUICEFS_ADMIN } },
            spec: {
              restartPolicy: "Never",
              ...priority("component"),
              ...imagePullSecrets(config),
              securityContext: restrictedPod(NOBODY, NOBODY),
              containers: [{ ...adminContainer(config, name, ...lines), securityContext: restrictedContainer() }],
            },
          },
        },
      },
    },
  };
}

/**
 * The workspaces' usage checked daily against what they hold, and repaired (guide/quota.md
 * "Usage check and fix"): a client that ends unexpectedly loses the usage it had yet to
 * write, after which a workspace's quota counts wrongly. Each workspace, as `juicefs quota
 * list` tables it, is checked in turn.
 */
function quotaCheck(config: InstallConfig): V1CronJob {
  return dailyAdminTask(
    config,
    "juicefs-quota-check",
    QUOTA_CHECK_SCHEDULE,
    "quota-check",
    'quotas=$(juicefs quota list "$META_URL")',
    // The table's rows of workspaces, the directories of the file system's environments
    // (`/<environment>/<workspace>`, as the node agents make them), not those deleted into its
    // trash, whose first column is their path.
    "printf '%s\\n' \"$quotas\" | awk -F '|' '$2 ~ /^ \\/[^\\/. ][^\\/ ]*\\/[^\\/ ]+ +$/ { gsub(/ /, \"\", $2); print $2 }' | while read -r path; do",
    '  juicefs quota check "$META_URL" --path "$path" --repair',
    "done",
  );
}

/**
 * The file system collected daily (`juicefs gc --compact --delete`, guide/gc.md): what no
 * file holds deleted from the bucket, its trash's expired entries and its files pending
 * deletion with it, chunks of many slices compacted, and the trees of clones that never
 * finished removed, a day after they were begun. A clone builds its tree detached and attaches it only once
 * it is whole, and its client removes it when the clone fails; nothing but this removes
 * one whose client ended first.
 */
function collection(config: InstallConfig): V1CronJob {
  return dailyAdminTask(config, "juicefs-gc", COLLECTION_SCHEDULE, "gc", 'juicefs gc "$META_URL" --compact --delete');
}

/**
 * How often the stale clients' sessions are cleaned (staleSessions): the most a lock a dead
 * client held blocks another, past the minute before its session counts as stale.
 */
export const STALE_SESSIONS_SCHEDULE = "*/5 * * * *";

/**
 * How long staleSessions runs its client, in seconds: past two of its refreshes, a
 * heartbeat's each (12 s, ±5%), as its first may come before the cleanups' last mark has
 * aged past 0.9 of a heartbeat, which the cleanup waits for (JuiceFS's pkg/meta base.go
 * refresh and CleanStaleSessions).
 */
export const STALE_SESSIONS_RUN_SECONDS = 40;

/**
 * The sessions of clients that died cleaned, every STALE_SESSIONS_SCHEDULE, one run at a
 * time: what a client's background jobs do, and no mount of the nodes' does. A session
 * counts as stale once its client has sent no heartbeat for five of them (60 s), and a client
 * with background jobs cleans the stale sessions as it refreshes its own, every heartbeat:
 * their locks let go and the files they held open after their deletion freed. So a lock a
 * client that died held blocks another for 60 s and up to a schedule's interval more, and
 * this job's run: seven minutes at most. The client is JuiceFS's S3 gateway, on the
 * pod's loopback, which opens a session with background jobs and needs no FUSE, run for
 * STALE_SESSIONS_RUN_SECONDS: no mount, so nothing of it is anywhere a workspace lives, and
 * no backup of its own (`--backup-meta 0`). Its other background jobs, the trash's expiry and
 * deletions pending, are the daily collection's too, which they overlap harmlessly. The
 * sessions are said as it begins and ends.
 */
function staleSessions(config: InstallConfig): V1CronJob {
  const component = "juicefs-sessions";
  const sessions = `juicefs status "$META_URL" 2>/dev/null | grep -c '"Sid"' || true`;
  const container: V1Container = {
    ...adminContainer(
      config,
      "sessions",
      `echo "sessions as it begins: $(${sessions})"`,
      // The gateway's own S3 credentials, which nothing reaches it by: random, as it listens on the pod's loopback alone.
      "MINIO_ROOT_USER=maintenance MINIO_ROOT_PASSWORD=$(head -c 18 /dev/urandom | od -An -tx1 | tr -d ' \\n') \\",
      '  juicefs gateway --no-usage-report --backup-meta 0 --cache-dir /tmp/cache --cache-size 0 "$META_URL" 127.0.0.1:9000 &',
      "client=$!",
      `sleep ${STALE_SESSIONS_RUN_SECONDS}`,
      'kill -TERM "$client"',
      'wait "$client" || true',
      `echo "sessions as it ends: $(${sessions})"`,
    ),
    securityContext: restrictedContainer(),
    env: [{ name: "META_URL", valueFrom: { secretKeyRef: { name: WORKSPACES_CREDENTIALS, key: "metaurl" } } }, { name: "HOME", value: "/tmp" }],
    resources: { requests: { cpu: "50m", memory: "64Mi" }, limits: { memory: "512Mi" } },
    volumeMounts: [{ name: "tmp", mountPath: "/tmp" }],
  };
  return {
    apiVersion: "batch/v1",
    kind: "CronJob",
    metadata: { name: componentName(component), namespace: NAMESPACE, labels: labels(component) },
    spec: {
      schedule: STALE_SESSIONS_SCHEDULE,
      concurrencyPolicy: "Forbid",
      successfulJobsHistoryLimit: 1,
      failedJobsHistoryLimit: 3,
      jobTemplate: {
        spec: {
          backoffLimit: 1,
          activeDeadlineSeconds: 240,
          template: {
            metadata: { labels: { ...labels(component), ...JUICEFS_ADMIN } },
            spec: {
              restartPolicy: "Never",
              ...priority("component"),
              ...imagePullSecrets(config),
              securityContext: restrictedPod(NOBODY, NOBODY),
              containers: [container],
              volumes: [{ name: "tmp", emptyDir: {} }],
            },
          },
        },
      },
    },
  };
}

/** How many of the metadata's dumps the bucket's meta/ keeps, the newest: a day's, at the default interval. */
export const DUMPS_KEPT = 24;

/**
 * The file system's metadata dumped to its bucket, every backupInterval, one at a time, as
 * JuiceFS's own backup would have, which no mount takes (the nodes' mount runs with no
 * background job): by `juicefs dump`, its object store's keys left out, then put in meta/ as
 * JuiceFS names its dumps, `dump-YYYY-MM-DD-hhmmss.json.gz` of UTC, which the nightly backup
 * copies the newest of (./neon.ts backup); the dump checked as the gzip it is, and read back
 * of the same size and checksum once put; the dumps but the newest DUMPS_KEPT deleted after.
 * Put by the file system's own keys, which may write its bucket, with the AWS command line
 * of the stack's image.
 */
function metadataDump(config: InstallConfig): V1CronJob {
  const component = "juicefs-dump";
  const credential = (name: string, key: string) => ({ name, valueFrom: { secretKeyRef: { name: WORKSPACES_CREDENTIALS, key } } });
  const schedule = everyIntervalCron(config.workspaceStorage.backupInterval);
  // The configuration holds its interval to one a cron line keeps.
  if (schedule === null) throw new Error(`workspaceStorage.backupInterval ${config.workspaceStorage.backupInterval} is kept by no cron line`);
  const dump: V1Container = {
    ...adminContainer(config, "dump", 'juicefs dump "$META_URL" /dump/dump.json.gz'),
    securityContext: restrictedContainer(),
    // Its metadata held whole, as Valkey's maxmemory bounds it.
    resources: { requests: { cpu: "50m", memory: "128Mi" }, limits: { memory: "1Gi" } },
    volumeMounts: [{ name: "dump", mountPath: "/dump" }],
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
        'meta="s3://$WORKSPACES_BUCKET/$WORKSPACES_NAME/meta/"',
        'gzip -t "$DUMP/dump.json.gz" 2>/dev/null || { echo "the dump is no whole gzip" >&2; exit 1; }',
        'name="dump-$(date -u +%Y-%m-%d-%H%M%S).json.gz"',
        's3 cp "$DUMP/dump.json.gz" "$meta$name"',
        's3 cp "$meta$name" "$DUMP/read.json.gz"',
        `[ "$(stat -c %s "$DUMP/dump.json.gz")" = "$(stat -c %s "$DUMP/read.json.gz")" ] || { echo "$name in the bucket is not of the dump's size" >&2; exit 1; }`,
        '[ "$(sha256sum < "$DUMP/dump.json.gz")" = "$(sha256sum < "$DUMP/read.json.gz")" ] || { echo "$name in the bucket is not the dump" >&2; exit 1; }',
        `s3 ls "$meta" | awk '{print $4}' | grep '^dump-.*\\.json\\.gz$' | sort -r | tail -n +${DUMPS_KEPT + 1} | while read -r old; do s3 rm "$meta$old"; done`,
        'echo "dumped the metadata of $WORKSPACES_NAME to $meta$name"',
      ),
    ],
    env: [
      { name: "S3_ENDPOINT", value: s3Endpoint(config) },
      { name: "WORKSPACES_BUCKET", value: config.objectStore.buckets.workspaces },
      { name: "WORKSPACES_NAME", value: config.workspaceStorage.name },
      credential("AWS_ACCESS_KEY_ID", "access-key"),
      credential("AWS_SECRET_ACCESS_KEY", "secret-key"),
      { name: "AWS_DEFAULT_REGION", value: config.objectStore.external.region },
      { name: "HOME", value: "/tmp" },
      { name: "DUMP", value: "/dump" },
    ],
    securityContext: restrictedContainer(),
    resources: { requests: { cpu: "50m", memory: "64Mi" }, limits: { memory: "512Mi" } },
    volumeMounts: [{ name: "dump", mountPath: "/dump" }, { name: "tmp", mountPath: "/tmp" }],
  };
  return {
    apiVersion: "batch/v1",
    kind: "CronJob",
    metadata: { name: componentName(component), namespace: NAMESPACE, labels: labels(component) },
    spec: {
      schedule,
      concurrencyPolicy: "Forbid",
      successfulJobsHistoryLimit: 1,
      failedJobsHistoryLimit: 3,
      jobTemplate: {
        spec: {
          backoffLimit: 2,
          activeDeadlineSeconds: 1800,
          template: {
            metadata: { labels: { ...labels(component), ...JUICEFS_ADMIN } },
            spec: {
              restartPolicy: "Never",
              ...priority("component"),
              ...imagePullSecrets(config),
              securityContext: restrictedPod(NOBODY, NOBODY),
              initContainers: [dump],
              containers: [upload],
              volumes: [{ name: "dump", emptyDir: {} }, { name: "tmp", emptyDir: {} }],
            },
          },
        },
      },
    },
  };
}

/**
 * Formats the file system, unless it is formatted already, beside Valkey's server, whose
 * pod is ready only once it is, so nothing mounts or sets a quota on one not yet formatted.
 * Formatted with the bucket and its keys; it reaches Valkey on its pod's loopback, in the
 * database the file system's `metaurl` names (neon/control/kube-setup.ts), and the object
 * store as the nodes' mount does.
 */
function formatter(config: InstallConfig): V1Container {
  const credential = (name: string, key: string) => ({ name, valueFrom: { secretKeyRef: { name: WORKSPACES_CREDENTIALS, key } } });
  return {
    name: "juicefs-format",
    image: imageReference(config.workspaceStorage.image),
    imagePullPolicy: "IfNotPresent",
    command: [
      "/bin/sh",
      "-c",
      script(
        "set -eu",
        `meta="redis://:$VALKEY_PASSWORD@127.0.0.1:${VALKEY_PORT}/1"`,
        'until juicefs status "$meta" >/dev/null 2>&1 || juicefs format --storage "$STORAGE" --bucket "$BUCKET" --access-key "$ACCESS_KEY" --secret-key "$SECRET_KEY" \\',
        `  --trash-days ${config.workspaceStorage.trashDays} "$meta" "$NAME"; do sleep 5; done`,
        "touch /tmp/ready",
        "exec sleep infinity",
      ),
    ],
    env: [
      { name: "VALKEY_PASSWORD", valueFrom: VALKEY_PASSWORD },
      credential("NAME", "name"),
      credential("STORAGE", "storage"),
      credential("BUCKET", "bucket"),
      credential("ACCESS_KEY", "access-key"),
      credential("SECRET_KEY", "secret-key"),
    ],
    readinessProbe: { exec: { command: ["test", "-f", "/tmp/ready"] }, periodSeconds: 5 },
    securityContext: restrictedContainer(),
    resources: { requests: { cpu: "10m", memory: "32Mi" }, limits: { memory: "256Mi" } },
  };
}

/** Workspace storage's objects: Valkey, and the jobs that do what a mount's background jobs would. */
export function workspaceStorageObjects(config: InstallConfig): KubernetesObject[] {
  const { valkey } = config.workspaceStorage;
  const instance = {
    name: VALKEY,
    component: "valkey",
    ...valkey,
    // The node agents', which set workspaces' quotas: every key, but no command Valkey calls
    // dangerous, those that read or change its server whole, but INFO, which JuiceFS reads its
    // settings in.
    besideDefault: [{ name: WORKSPACE_QUOTAS_USER, secret: WORKSPACE_QUOTAS, rules: QUOTAS_RULES }],
  };
  return [...valkeyObjects(config, instance, [formatter(config)]), quotaCheck(config), collection(config), metadataDump(config), staleSessions(config)];
}
