/**
 * Workspace storage: each new session's workspace a volume of one JuiceFS file system, its
 * metadata in Valkey (./valkey.ts), its data in the object store's workspaces bucket, made
 * into volumes by JuiceFS's CSI driver (./juicefs-csi.ts) from the StorageClass here, as
 * the driver's guide says (guide/pv.md "Volume credentials", "Dynamic provisioning";
 * guide/configurations.md "PV expansion", "Use more readable names for PV directory").
 *
 * A claim of the class is a directory of the file system named after it,
 * `<namespace>-<name>`, whose quota is the claim's size, grown as the claim is. Deleting
 * the claim, as deleting its Sandbox does, deletes the directory into the file system's
 * trash. The file system's credentials, the metadata engine's URL with Valkey's password
 * and the bucket's keys, are in a Secret the stack's setup makes (./neon.ts), which the
 * driver reads, and alasio's JuiceFS admin pods (the daily quota check and collection
 * here, and the Job that clones a workspace), never a workspace.
 */
import type { KubernetesObject, V1Container, V1CronJob, V1Job, V1StorageClass } from "@kubernetes/client-node";

import { imageReference } from "../images.ts";
import { componentName, imagePullSecrets, type Labels, labels, NAMESPACE, restrictedContainer, restrictedPod, script } from "./common.ts";
import type { InstallConfig } from "./config.ts";
import { CSI_DRIVER, juicefsCsiObjects, VOLUME_DRIVER } from "./juicefs-csi.ts";
import { VALKEY, VALKEY_PASSWORD, VALKEY_PORT, valkeyObjects } from "./valkey.ts";

/** The Secret of the file system's credentials. */
export const WORKSPACES_CREDENTIALS = componentName("workspaces-juicefs");

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

/**
 * The directory of the file system a claim of the class is, `<namespace>-<claim>`: of
 * the driver's template's variables in the StorageClass's pathPattern, of the shell's in
 * the clone's script.
 */
const volumeDirectory = (namespace: string, claim: string) => `${namespace}-${claim}`;

/** Valkey's address, as the driver's pods reach it from their own namespace. */
export const VALKEY_ADDRESS = `${VALKEY}.${NAMESPACE}.svc.cluster.local:${VALKEY_PORT}`;

/** The workspaces bucket's URL, path-style, as the driver's pods reach it from their own namespace. */
export function workspacesBucketUrl({ objectStore }: InstallConfig): string {
  const endpoint = objectStore.bundled.enabled ? `http://${componentName("seaweedfs")}.${NAMESPACE}.svc.cluster.local:8333` : objectStore.external.endpoint.replace(/\/+$/u, "");
  return `${endpoint}/${objectStore.buckets.workspaces}`;
}

/** The StorageClass of workspaces' volumes. */
function storageClass({ workspaceStorage }: InstallConfig): V1StorageClass {
  const secret = { name: WORKSPACES_CREDENTIALS, namespace: NAMESPACE };
  return {
    apiVersion: "storage.k8s.io/v1",
    kind: "StorageClass",
    metadata: { name: workspaceStorage.storageClassName, labels: labels("workspaces") },
    provisioner: CSI_DRIVER,
    parameters: {
      "csi.storage.k8s.io/provisioner-secret-name": secret.name,
      "csi.storage.k8s.io/provisioner-secret-namespace": secret.namespace,
      "csi.storage.k8s.io/node-publish-secret-name": secret.name,
      "csi.storage.k8s.io/node-publish-secret-namespace": secret.namespace,
      "csi.storage.k8s.io/controller-expand-secret-name": secret.name,
      "csi.storage.k8s.io/controller-expand-secret-namespace": secret.namespace,
      pathPattern: volumeDirectory("${.pvc.namespace}", "${.pvc.name}"),
    },
    reclaimPolicy: "Delete",
    allowVolumeExpansion: true,
    volumeBindingMode: "Immediate",
  };
}

/**
 * A container of a JuiceFS admin pod, `name`, running `lines` with JuiceFS's command line
 * of the pinned client, the file system's metadata engine's URL in `META_URL`.
 */
function adminContainer(config: InstallConfig, name: string, ...lines: string[]): V1Container {
  return {
    name,
    image: imageReference(config.workspaceStorage.csi.mountImage),
    imagePullPolicy: "IfNotPresent",
    command: ["/bin/sh", "-c", script("set -eu", ...lines)],
    env: [{ name: "META_URL", valueFrom: { secretKeyRef: { name: WORKSPACES_CREDENTIALS, key: "metaurl" } } }],
    resources: { requests: { cpu: "50m", memory: "64Mi" }, limits: { memory: "512Mi" } },
  };
}

/**
 * A daily JuiceFS admin task, `component`, whose container `name` runs `lines` restricted
 * as nobody. Kept, as the driver's objects are, while the volumes remain.
 */
function dailyAdminTask(config: InstallConfig, component: string, schedule: string, name: string, ...lines: string[]): V1CronJob {
  return {
    apiVersion: "batch/v1",
    kind: "CronJob",
    metadata: { name: componentName(component), namespace: NAMESPACE, labels: { ...labels(component), ...VOLUME_DRIVER } },
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
 * The volumes' usage checked daily against what they hold, and repaired (guide/quota.md
 * "Usage check and fix"): a client that ends unexpectedly loses the usage it had yet to
 * write, after which a volume's quota counts wrongly. Each volume, as `juicefs quota
 * list` tables it, is checked in turn.
 */
function quotaCheck(config: InstallConfig): V1CronJob {
  return dailyAdminTask(
    config,
    "juicefs-quota-check",
    QUOTA_CHECK_SCHEDULE,
    "quota-check",
    'quotas=$(juicefs quota list "$META_URL")',
    // The table's rows of volumes, the directories at the file system's top (not those
    // deleted into its trash), whose first column is their path.
    "printf '%s\\n' \"$quotas\" | awk -F '|' '$2 ~ /^ \\/[^\\/ ]+ +$/ { gsub(/ /, \"\", $2); print $2 }' | while read -r path; do",
    '  juicefs quota check "$META_URL" --path "$path" --repair',
    "done",
  );
}

/**
 * The file system collected daily (`juicefs gc --delete`, guide/gc.md): what no file
 * holds deleted from the bucket, and the trees of clones that never finished removed, a
 * day after they were begun. A clone builds its tree detached and attaches it only once
 * it is whole, and its client removes it when the clone fails; nothing but this removes
 * one whose client ended first.
 */
function collection(config: InstallConfig): V1CronJob {
  return dailyAdminTask(config, "juicefs-gc", COLLECTION_SCHEDULE, "gc", 'juicefs gc "$META_URL" --delete');
}

/**
 * The Job that clones the session volume `$SOURCE_CLAIM` of `$SOURCE_NAMESPACE` into
 * the newly made `$DESTINATION_CLAIM` of `$DESTINATION_NAMESPACE`, which alasio runs
 * as it forks a session (src/sandbox/index.ts), giving it those. The workspace and home,
 * `juicefs clone --preserve`d, keep their owners and modes, and their data, shared until
 * either copy changes it, is copied by none. They are cloned under the destination's
 * directory, which the driver makes and gives the claim's quota as it provisions the
 * volume, after the claim is bound: the Job waits for the quota, which the clones are
 * then charged to. The driver counts the new quota's usage in a scan of its own, though,
 * which may end once a clone is in the directory, counting it a second time; so once the
 * clones are done, and the client that charged them has stopped and written what it
 * counted, the usage is counted anew (`juicefs quota check --repair`). The directory is
 * given its source's owner and mode, as kubelet gives a volume whose root is not yet the
 * pod's group that group throughout (fsGroupChangePolicy OnRootMismatch), which would
 * widen every cloned file's mode. On a mount of the whole file system, as a volume's
 * mount pod mounts its directory alone: privileged, as FUSE needs, and as root, to give
 * each file its owner.
 */
export function cloneJob(config: InstallConfig): V1Job {
  const component = "workspace-clone";
  const source = `/jfs/${volumeDirectory("$SOURCE_NAMESPACE", "$SOURCE_CLAIM")}`;
  const destination = volumeDirectory("$DESTINATION_NAMESPACE", "$DESTINATION_CLAIM");
  return {
    apiVersion: "batch/v1",
    kind: "Job",
    metadata: { generateName: `${componentName(component)}-`, namespace: NAMESPACE, labels: labels(component) },
    spec: {
      // A clone that fails leaves half a destination, which alasio deletes with the fork.
      backoffLimit: 0,
      activeDeadlineSeconds: 3600,
      ttlSecondsAfterFinished: 3600,
      template: {
        metadata: { labels: { ...labels(component), ...JUICEFS_ADMIN } },
        spec: {
          restartPolicy: "Never",
          ...imagePullSecrets(config),
          containers: [{
            ...adminContainer(
              config,
              "clone",
              `until [ -n "$(juicefs quota get "$META_URL" --path "/${destination}" 2>/dev/null)" ]; do sleep 2; done`,
              "mkdir -p /jfs",
              'juicefs mount --no-bgjob --cache-size 0 "$META_URL" /jfs &',
              "client=$!",
              'until [ "$(stat --file-system --format=%T /jfs)" = fuseblk ]; do kill -0 "$client"; sleep 1; done',
              "for dir in workspace home; do",
              `  juicefs clone --preserve "${source}/$dir" "/jfs/${destination}/$dir"`,
              "done",
              `chown "$(stat --format=%u:%g "${source}")" "/jfs/${destination}"`,
              `chmod "$(stat --format=%a "${source}")" "/jfs/${destination}"`,
              "umount /jfs",
              'wait "$client"',
              `juicefs quota check "$META_URL" --path "/${destination}" --repair`,
            ),
            securityContext: { privileged: true },
          }],
        },
      },
    },
  };
}

/**
 * Formats the file system, unless it is formatted already, beside Valkey's server, whose
 * pod is ready only once it is: JuiceFS's driver sets a new volume's quota before the
 * volume's mount pod has formatted the file system, so the first volume made on one not
 * yet formatted would have none. Formatted as mount pods format it, with the bucket and
 * its keys; it reaches Valkey on its pod's loopback, in the database the file system's
 * `metaurl` names (neon/control/kube-setup.ts), and the object store as JuiceFS's pods do.
 */
function formatter(config: InstallConfig): V1Container {
  const credential = (name: string, key: string) => ({ name, valueFrom: { secretKeyRef: { name: WORKSPACES_CREDENTIALS, key } } });
  return {
    name: "juicefs-format",
    image: imageReference(config.workspaceStorage.csi.mountImage),
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

/** Workspace storage's objects, when it is on: Valkey, the CSI driver, the StorageClass, the quota check and the collection. */
export function workspaceStorageObjects(config: InstallConfig): KubernetesObject[] {
  if (!config.workspaceStorage.enabled) return [];
  return [...valkeyObjects(config, [formatter(config)]), ...juicefsCsiObjects(config), storageClass(config), quotaCheck(config), collection(config)];
}
