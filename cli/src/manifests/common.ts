/**
 * What the objects of an installation share: their names and labels, image references,
 * security contexts, and the pieces of pod specs several components have alike.
 *
 * Every object is named after the installation, `alasio`, and labelled as Kubernetes
 * recommends, managed by alasio. Names, and the labels that select pods, stay as they
 * are from one version to the next, as a workload's selector cannot change.
 */
import { createHash } from "node:crypto";

import type {
  V1Container,
  V1EnvVar,
  V1LocalObjectReference,
  V1PersistentVolumeClaimSpec,
  V1PodSecurityContext,
  V1PodSpec,
  V1SecurityContext,
  V1Volume,
  V1VolumeMount,
} from "@kubernetes/client-node";

import { imageReference } from "../images.ts";
import { VERSION } from "../release.ts";
import type { InstallConfig } from "./config.ts";

/** The namespace alasio runs in, which its objects are in unless they say another. */
export const NAMESPACE = "alasio";

/**
 * The installation's name: alasio's own objects' name (its Deployment, ServiceAccount
 * and volume), the start of every other's, and the instance every object is labelled
 * with.
 */
export const RELEASE = "alasio";

/** A component's name: `alasio-<component>`. */
export function componentName(component: string): string {
  return `${RELEASE}-${component}`;
}

/** A Neon service's name: `alasio-neon-<component>`. */
export function neonName(component: string): string {
  return componentName(`neon-${component}`);
}

export type Labels = Record<string, string>;

/** The labels every object of a component carries. */
export function labels(component: string): Labels {
  return {
    "app.kubernetes.io/name": "alasio",
    "app.kubernetes.io/instance": RELEASE,
    "app.kubernetes.io/version": VERSION,
    "app.kubernetes.io/managed-by": "alasio",
    "app.kubernetes.io/part-of": "alasio",
    "app.kubernetes.io/component": component,
  };
}

/** The labels of a pod of the data stack (Neon, the object store, the lake), which its NetworkPolicy admits each other by. */
export function stackLabels(component: string): Labels {
  return { ...labels(component), "alasio.dev/stack": "neon" };
}

/** The labels that select a component's pods. */
export function selectorLabels(component: string): Labels {
  return { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": RELEASE, "app.kubernetes.io/component": component };
}

/** The pod security context of a workload that runs restricted, as `uid` and `gid`. */
export function restrictedPod(uid: number, gid: number): V1PodSecurityContext {
  return { runAsNonRoot: true, runAsUser: uid, runAsGroup: gid, fsGroup: gid, seccompProfile: { type: "RuntimeDefault" } };
}

/** The security context of a container that runs restricted. */
export function restrictedContainer(): V1SecurityContext {
  return { allowPrivilegeEscalation: false, capabilities: { drop: ["ALL"] } };
}

/** The resources of a short-lived helper container: an init container's, say. */
export function helperResources(): Pick<V1Container, "resources"> {
  return { resources: { requests: { cpu: "10m", memory: "16Mi" }, limits: { memory: "64Mi" } } };
}

/** `{ [key]: value }` when `value` holds anything, so an object says nothing of what was given empty. */
export function given<K extends string, V extends object>(key: K, value: V): { [P in K]?: V } {
  return Object.keys(value).length > 0 ? ({ [key]: value } as { [P in K]: V }) : {};
}

/** The variables of a record, in the order of their names, so an object lists them alike however they were given. */
export function envOf(variables: Readonly<Record<string, string>>): V1EnvVar[] {
  return Object.keys(variables).sort().map((name) => ({ name, value: variables[name] ?? "" }));
}

/**
 * OpenTelemetry's standard variables, which alasio, the lake and Neon's compute export
 * with: none when no endpoint is set.
 */
export function otelEnv({ telemetry }: InstallConfig): V1EnvVar[] {
  if (!telemetry.otlpEndpoint) return [];
  return [
    { name: "OTEL_EXPORTER_OTLP_ENDPOINT", value: telemetry.otlpEndpoint },
    { name: "OTEL_EXPORTER_OTLP_PROTOCOL", value: telemetry.otlpProtocol },
    ...(telemetry.headersSecret
      ? [{ name: "OTEL_EXPORTER_OTLP_HEADERS", valueFrom: { secretKeyRef: { name: telemetry.headersSecret, key: telemetry.headersKey } } }]
      : []),
    ...(telemetry.resourceAttributes ? [{ name: "OTEL_RESOURCE_ATTRIBUTES", value: telemetry.resourceAttributes }] : []),
  ];
}

/** The pull secrets every pod is given. */
export function imagePullSecrets(config: InstallConfig): { imagePullSecrets?: V1LocalObjectReference[] } {
  return given("imagePullSecrets", config.imagePullSecrets.map((name) => ({ name })));
}

/** The host profile's mounts, as volumes. */
export function hostVolumes(config: InstallConfig): V1Volume[] {
  return config.host.mounts.map((mount) => ({ name: mount.name, hostPath: { path: mount.hostPath, ...(mount.type ? { type: mount.type } : {}) } }));
}

/** The host profile's mounts, as volume mounts. */
export function hostVolumeMounts(config: InstallConfig): V1VolumeMount[] {
  return config.host.mounts.map((mount) => ({ name: mount.name, mountPath: mount.mountPath, ...(mount.readOnly ? { readOnly: true } : {}) }));
}

/** The spec of a claim of `storage`. */
export function claimSpec(storage: InstallConfig["neon"]["pageserver"]["storage"]): V1PersistentVolumeClaimSpec {
  return {
    accessModes: ["ReadWriteOnce"],
    ...(storage.storageClassName ? { storageClassName: storage.storageClassName } : {}),
    resources: { requests: { storage: storage.size } },
  };
}

/** The Secret alasio reads its database URL and the lake role's password from. */
export function databaseSecret(config: InstallConfig): string {
  return config.neon.enabled ? `${RELEASE}-database` : config.neon.external.existingSecret;
}

/** The object store's S3 endpoint, as pods in alasio's namespace reach it. */
export function s3Endpoint(config: InstallConfig): string {
  return config.objectStore.bundled.enabled ? `http://${componentName("seaweedfs")}:8333` : config.objectStore.external.endpoint;
}

/** What every pod of the data stack has in its spec: pull secrets, Neon's user, and placement. */
export function stackPodSpec(config: InstallConfig): Pick<V1PodSpec, "imagePullSecrets" | "securityContext" | "nodeSelector" | "tolerations"> {
  return {
    ...imagePullSecrets(config),
    securityContext: restrictedPod(config.neon.runAsUser, config.neon.runAsGroup),
    ...given("nodeSelector", { ...config.neon.nodeSelector }),
    ...given("tolerations", [...config.neon.tolerations]),
  };
}

/** An init container that waits until `url` answers 2xx, with Neon's image's curl. */
export function waitFor(config: InstallConfig, name: string, url: string): V1Container {
  return {
    name: `wait-for-${name}`,
    image: imageReference(config.neon.image),
    imagePullPolicy: "IfNotPresent",
    command: ["/bin/sh", "-c", 'until curl -fsS -o /dev/null --max-time 5 "$0"; do sleep 2; done', url],
    securityContext: restrictedContainer(),
    ...helperResources(),
  };
}

/** Until the bundled object store's buckets exist, nothing that stores in them starts: the init container that waits, if it is bundled. */
export function waitForObjectStore(config: InstallConfig): V1Container[] {
  return config.objectStore.bundled.enabled ? [waitFor(config, "object-store", `http://${componentName("seaweedfs")}:8333/healthz`)] : [];
}

/** A shell script of `lines`, one to a line. */
export function script(...lines: string[]): string {
  return `${lines.join("\n")}\n`;
}

/** Object keys in order, at every depth, as Go's encoding/json writes a map's. */
function sortedKeys(_key: string, value: unknown): unknown {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? Object.fromEntries(Object.entries(value).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)))
    : value;
}

/**
 * `value` as JSON as Go's encoding/json writes it: keys sorted, so a value is always
 * written alike, and its checksum with it; `<`, `>`, `&` and the line separators
 * escaped; and, with `indent`, indented by it.
 */
export function goJson(value: unknown, indent?: string): string {
  return JSON.stringify(value, sortedKeys, indent).replace(/[<>&\u2028\u2029]/gu, (char) => `\\u${char.charCodeAt(0).toString(16).padStart(4, "0")}`);
}

/** The sha256 of `text`, in hex. */
export function sha256(text: string): string {
  return createHash("sha256").update(text).digest("hex");
}
