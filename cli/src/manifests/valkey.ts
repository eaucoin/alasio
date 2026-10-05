/**
 * Valkey, the metadata engine of workspace storage's file system: one server, configured
 * as JuiceFS's Redis best practices (administration/metadata/redis_best_practices.md at
 * juicedata/juicefs v1.4.1) say:
 *
 * - durability: snapshots and an append-only file together, the file fsynced every second
 *   and written with a snapshot as its preamble, so a rewrite is a snapshot; at a start the
 *   file is loaded, as the more complete, and the server is ready only once it has been;
 * - memory: a `maxmemory` under the container's limit, leaving room for the fork that
 *   writes snapshots and for deletions once it is reached, and the policy `noeviction`,
 *   which JuiceFS requires, as an evicted key is lost metadata.
 *
 * Its password is in a Secret the stack's setup makes (./neon.ts). It is not of the data
 * stack, whose pods reach each other: its NetworkPolicy (./network-policies.ts) admits
 * JuiceFS's pods alone.
 */
import type { KubernetesObject, V1ConfigMap, V1Container, V1EnvVarSource, V1Service, V1StatefulSet } from "@kubernetes/client-node";

import { imageReference } from "../images.ts";
import { claimSpec, componentName, imagePullSecrets, labels, NAMESPACE, restrictedContainer, restrictedPod, selectorLabels, sha256 } from "./common.ts";
import type { InstallConfig } from "./config.ts";
import { VOLUME_DRIVER } from "./juicefs-csi.ts";

/** Valkey's name: its Service's, its StatefulSet's, and its password's Secret's. */
export const VALKEY = componentName("valkey");
export const VALKEY_PORT = 6379;

/** The uid and gid of the image's `valkey` user. */
const VALKEY_UID = 999;

/** Valkey's configuration file. */
export function valkeyConf({ workspaceStorage }: InstallConfig): string {
  return [
    "bind * -::*",
    `port ${VALKEY_PORT}`,
    "dir /data",
    "appendonly yes",
    "appendfsync everysec",
    "aof-use-rdb-preamble yes",
    "save 3600 1 300 100 60 10000",
    // Writes stop, rather than go unsaved, when a snapshot cannot be written.
    "stop-writes-on-bgsave-error yes",
    `maxmemory ${workspaceStorage.valkey.maxmemory}`,
    "maxmemory-policy noeviction",
    "",
  ].join("\n");
}

/** The Valkey password's Secret, as a variable's source. */
export const VALKEY_PASSWORD: V1EnvVarSource = { secretKeyRef: { name: VALKEY, key: "password" } };

/**
 * Valkey: its configuration, its Service, and its StatefulSet, with a volume of its own,
 * and `sidecars` beside the server in its pod, which is ready once each of them is.
 */
export function valkeyObjects(config: InstallConfig, sidecars: readonly V1Container[]): KubernetesObject[] {
  const { valkey } = config.workspaceStorage;
  const conf = valkeyConf(config);
  // JuiceFS's driver needs it to delete its volumes' data, so it is kept while they remain.
  const metadata = { name: VALKEY, namespace: NAMESPACE, labels: { ...labels("valkey"), ...VOLUME_DRIVER } };
  const configMap: V1ConfigMap = { apiVersion: "v1", kind: "ConfigMap", metadata, data: { "valkey.conf": conf } };
  const service: V1Service = {
    apiVersion: "v1",
    kind: "Service",
    metadata,
    spec: { selector: selectorLabels("valkey"), ports: [{ name: "valkey", port: VALKEY_PORT }] },
  };
  const statefulSet: V1StatefulSet = {
    apiVersion: "apps/v1",
    kind: "StatefulSet",
    metadata,
    spec: {
      serviceName: VALKEY,
      replicas: 1,
      selector: { matchLabels: selectorLabels("valkey") },
      template: {
        metadata: { labels: labels("valkey"), annotations: { "checksum/config": sha256(conf) } },
        spec: {
          ...imagePullSecrets(config),
          securityContext: restrictedPod(VALKEY_UID, VALKEY_UID),
          // Long enough for its last snapshot and fsync as it stops.
          terminationGracePeriodSeconds: 60,
          containers: [{
            name: "valkey",
            image: imageReference(valkey.image),
            imagePullPolicy: "IfNotPresent",
            command: ["valkey-server", "/etc/valkey/valkey.conf", "--requirepass", "$(VALKEY_PASSWORD)"],
            env: [
              { name: "VALKEY_PASSWORD", valueFrom: VALKEY_PASSWORD },
              // valkey-cli's, for the readiness probe.
              { name: "VALKEYCLI_AUTH", valueFrom: VALKEY_PASSWORD },
            ],
            ports: [{ name: "valkey", containerPort: VALKEY_PORT }],
            // Ready once its append-only file is loaded, which it reads as off until then.
            readinessProbe: {
              exec: {
                command: [
                  "/bin/sh",
                  "-c",
                  "persistence=$(valkey-cli info persistence) && echo \"$persistence\" | grep -q '^loading:0' && echo \"$persistence\" | grep -q '^aof_enabled:1'",
                ],
              },
              periodSeconds: 5,
              timeoutSeconds: 3,
            },
            livenessProbe: { tcpSocket: { port: "valkey" }, initialDelaySeconds: 30, periodSeconds: 10, failureThreshold: 6 },
            securityContext: { ...restrictedContainer(), readOnlyRootFilesystem: true },
            resources: valkey.resources,
            volumeMounts: [{ name: "data", mountPath: "/data" }, { name: "config", mountPath: "/etc/valkey", readOnly: true }],
          }, ...sidecars],
          volumes: [{ name: "config", configMap: { name: VALKEY } }],
        },
      },
      volumeClaimTemplates: [{ metadata: { name: "data" }, spec: claimSpec(valkey.storage) }],
    },
  };
  return [configMap, service, statefulSet];
}
