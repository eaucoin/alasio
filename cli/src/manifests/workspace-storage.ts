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
 * driver alone reads, never a workspace.
 */
import type { KubernetesObject, V1StorageClass } from "@kubernetes/client-node";

import { componentName, labels, NAMESPACE } from "./common.ts";
import type { InstallConfig } from "./config.ts";
import { CSI_DRIVER, juicefsCsiObjects } from "./juicefs-csi.ts";
import { VALKEY, VALKEY_PORT, valkeyObjects } from "./valkey.ts";

/** The Secret of the file system's credentials. */
const WORKSPACES_CREDENTIALS = componentName("workspaces-juicefs");

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

/** Workspace storage's objects, when it is on: Valkey, the CSI driver, and the StorageClass. */
export function workspaceStorageObjects(config: InstallConfig): KubernetesObject[] {
  if (!config.workspaceStorage.enabled) return [];
  return [...valkeyObjects(config), ...juicefsCsiObjects(config), storageClass(config)];
}
