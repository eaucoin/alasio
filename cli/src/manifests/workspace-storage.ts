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
 * driver reads, and alasio's JuiceFS admin pods (the daily quota check here), never a
 * workspace.
 */
import type { KubernetesObject, V1CronJob, V1StorageClass } from "@kubernetes/client-node";

import { imageReference } from "../images.ts";
import { componentName, imagePullSecrets, type Labels, labels, NAMESPACE, restrictedContainer, restrictedPod, script } from "./common.ts";
import type { InstallConfig } from "./config.ts";
import { CSI_DRIVER, juicefsCsiObjects, VOLUME_DRIVER } from "./juicefs-csi.ts";
import { VALKEY, VALKEY_PORT, valkeyObjects } from "./valkey.ts";

/** The Secret of the file system's credentials. */
const WORKSPACES_CREDENTIALS = componentName("workspaces-juicefs");

/**
 * The label of alasio's pods that run JuiceFS's command line against the file system, the
 * quota check's and a restore's, which its store policies admit (./network-policies.ts).
 */
export const JUICEFS_ADMIN: Labels = { "alasio.dev/workload": "juicefs-admin" };

/** The quota check runs daily, after the database's backup. */
const QUOTA_CHECK_SCHEDULE = "47 3 * * *";
/** Whom the quota check runs as: nobody, as it reaches Valkey alone and writes nothing of its own. */
const NOBODY = 65534;

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
      pathPattern: "${.pvc.namespace}-${.pvc.name}",
    },
    reclaimPolicy: "Delete",
    allowVolumeExpansion: true,
    volumeBindingMode: "Immediate",
  };
}

/**
 * The volumes' usage checked daily against what they hold, and repaired (guide/quota.md
 * "Usage check and fix"): a client that ends unexpectedly loses the usage it had yet to
 * write, after which a volume's quota counts wrongly. Each volume, as `juicefs quota
 * list` tables it, is checked in turn. Kept, as the driver's objects are, while the
 * volumes remain.
 */
function quotaCheck(config: InstallConfig): V1CronJob {
  const component = "juicefs-quota-check";
  return {
    apiVersion: "batch/v1",
    kind: "CronJob",
    metadata: { name: componentName(component), namespace: NAMESPACE, labels: { ...labels(component), ...VOLUME_DRIVER } },
    spec: {
      schedule: QUOTA_CHECK_SCHEDULE,
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
              containers: [{
                name: "quota-check",
                image: imageReference(config.workspaceStorage.csi.mountImage),
                imagePullPolicy: "IfNotPresent",
                command: [
                  "/bin/sh",
                  "-c",
                  script(
                    "set -eu",
                    'quotas=$(juicefs quota list "$META_URL")',
                    // The table's rows of volumes, the directories at the file system's top
                    // (not those deleted into its trash), whose first column is their path.
                    "printf '%s\\n' \"$quotas\" | awk -F '|' '$2 ~ /^ \\/[^\\/ ]+ +$/ { gsub(/ /, \"\", $2); print $2 }' | while read -r path; do",
                    '  juicefs quota check "$META_URL" --path "$path" --repair',
                    "done",
                  ),
                ],
                env: [{ name: "META_URL", valueFrom: { secretKeyRef: { name: WORKSPACES_CREDENTIALS, key: "metaurl" } } }],
                securityContext: restrictedContainer(),
                resources: { requests: { cpu: "50m", memory: "64Mi" }, limits: { memory: "512Mi" } },
              }],
            },
          },
        },
      },
    },
  };
}

/** Workspace storage's objects, when it is on: Valkey, the CSI driver, the StorageClass, and the quota check. */
export function workspaceStorageObjects(config: InstallConfig): KubernetesObject[] {
  if (!config.workspaceStorage.enabled) return [];
  return [...valkeyObjects(config), ...juicefsCsiObjects(config), storageClass(config), quotaCheck(config)];
}
