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
  V1PolicyRule,
  V1Role,
  V1RoleBinding,
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
import type { InstallConfig, Scale } from "./config.ts";

/** The namespace alasio runs in, which its objects are in unless they say another. */
export const NAMESPACE = "alasio";

/**
 * The installation's name: alasio's own objects' name (its Deployment, ServiceAccount
 * and volume), the start of every other's, and the instance every object is labelled
 * with.
 */
export const RELEASE = "alasio";

/**
 * Where an alasio runs: main, in alasio's namespace, or a branch environment, a copy of
 * main's data with an alasio, a compute and a lake of its own in a namespace of its own
 * (./branch.ts), whose objects are named as main's are there. What is not branched (the
 * rest of Neon, the object store, the collector) is main's, which a branch reaches by
 * names that resolve from any namespace.
 */
export interface Environment {
  readonly namespace: string;
  /** The branch environment's name; null for main. */
  readonly branch: string | null;
}

/** main: alasio's own environment. */
export const MAIN_ENVIRONMENT: Environment = { namespace: NAMESPACE, branch: null };

export { BRANCH_LABEL } from "../../../src/branch/names.ts";

/** A component's name: `alasio-<component>`. */
export function componentName(component: string): string {
  return `${RELEASE}-${component}`;
}

/** The Secret of a branch environment's token, which its parent forks the sessions it inherited for (src/branch/fork.ts). */
export const BRANCH_FORK_SECRET = componentName("branch-fork");

/**
 * The Secret of an environment's callers' key to the sandbox gateway, of the gateway's
 * (src/binder/names.ts environmentKey), in its alasio's namespace: main's, as Neon's setup
 * renders it, and a branch environment's, as `alasio branch create` writes it.
 */
export const SESSIONS_CALLER_SECRET = componentName("sessions-caller");

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

/** The scale of a component that runs one replica, always. */
export const ONE: Scale = { min: 1, max: 1 };

/** Whether a scaler moves a component of `scale` between its min and max. */
export const scaled = ({ min, max }: Scale): boolean => min !== max;

/**
 * The replicas of a workload of `scale`: the count it fixes; or none where its scaler
 * moves it, which then owns the field, as applying alasio would take it back otherwise.
 */
export function replicasOf(scale: Scale): { replicas?: number } {
  return scaled(scale) ? {} : { replicas: scale.min };
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

/** Whether the lake runs: when it is on, beside Neon. */
export function lakeRuns(config: InstallConfig): boolean {
  return config.lake.enabled && config.neon.enabled;
}

/** Whether Grafana runs (./grafana.ts): when it is on, beside the lake, which it reads. */
export function grafanaRuns(config: InstallConfig): boolean {
  return config.grafana.enabled && lakeRuns(config);
}

/** Whether the stack's telemetry collector runs (./collector.ts): with the lake, or an endpoint to send to. */
export function collectorRuns(config: InstallConfig): boolean {
  return lakeRuns(config) || Boolean(config.telemetry.otlpEndpoint);
}

/**
 * Whether session sandboxes are snapshotted whole, into the object store's snapshots
 * bucket: when sessions run under a RuntimeClass, gVisor's, whose checkpoints the
 * snapshots are, and not under the cluster's default runtime.
 */
export function sandboxSnapshots(config: InstallConfig): boolean {
  return config.sessions.enabled && config.sessions.runtimeClassName !== "";
}

/**
 * Whether the node agents run (./node-agent.ts): where sandboxes are snapshotted, and
 * Neon's setup makes the Secrets they run with, their bucket's and their token.
 */
export function nodeAgentRuns(config: InstallConfig): boolean {
  return sandboxSnapshots(config) && config.neon.enabled;
}

/** The port the collector takes OTLP over HTTP on. */
export const COLLECTOR_PORT = 4318;

/**
 * OpenTelemetry's standard variables, which alasio, the lake and Neon's compute export
 * with: to the collector, by a name that resolves from any namespace, as alasio passes
 * them on to its harnesses and folder workspaces' bayma; none when it does not run. A
 * branch environment's are its resource's `alasio.branch` too, so what it sends lands in
 * main's lake beside main's, told apart.
 */
export function otelEnv(config: InstallConfig, { branch }: Environment = MAIN_ENVIRONMENT): V1EnvVar[] {
  if (!collectorRuns(config)) return [];
  const resourceAttributes = [config.telemetry.resourceAttributes, ...(branch ? [`alasio.branch=${branch}`] : [])].filter(Boolean).join(",");
  return [
    { name: "OTEL_EXPORTER_OTLP_ENDPOINT", value: `http://${componentName("collector")}.${NAMESPACE}.svc:${COLLECTOR_PORT}` },
    { name: "OTEL_EXPORTER_OTLP_PROTOCOL", value: "http/protobuf" },
    ...(resourceAttributes ? [{ name: "OTEL_RESOURCE_ATTRIBUTES", value: resourceAttributes }] : []),
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

/** The Secret alasio reads its database URL and the lake role's verifier from. */
export function databaseSecret(config: InstallConfig): string {
  return config.neon.enabled ? `${RELEASE}-database` : config.neon.external.existingSecret;
}

/**
 * The verifiers of the passwords of the roles alasio makes in its Neon for what reads it
 * beside alasio, given while that runs: the lake's query endpoint's, and Grafana's. Each
 * by its key in the database Secret, which the stack's setup writes it in, and the
 * variable that names its file to alasio (src/neon/connect.ts).
 */
export function roleVerifiers(config: InstallConfig): { readonly key: string; readonly variable: string }[] {
  return [
    ...(lakeRuns(config) ? [{ key: "lake-reader-verifier", variable: "ALASIO_LAKE_READER_VERIFIER_FILE" }] : []),
    ...(grafanaRuns(config) ? [{ key: "grafana-verifier", variable: "ALASIO_GRAFANA_VERIFIER_FILE" }] : []),
  ];
}

/** The object store's S3 endpoint, as pods in any namespace reach it, a branch environment's too. */
export function s3Endpoint(config: InstallConfig): string {
  return config.objectStore.bundled.enabled ? `http://${componentName("seaweedfs")}.${NAMESPACE}.svc:8333` : config.objectStore.external.endpoint;
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

/** A Role in `namespace` of `rules`, named for the ServiceAccount `account` of alasio's namespace, and its binding to it. */
export function accountRole(account: string, component: string, namespace: string, rules: V1PolicyRule[]): [V1Role, V1RoleBinding] {
  const metadata = { name: account, namespace, labels: labels(component) };
  return [
    { apiVersion: "rbac.authorization.k8s.io/v1", kind: "Role", metadata, rules },
    {
      apiVersion: "rbac.authorization.k8s.io/v1",
      kind: "RoleBinding",
      metadata,
      roleRef: { apiGroup: "rbac.authorization.k8s.io", kind: "Role", name: account },
      subjects: [{ kind: "ServiceAccount", name: account, namespace: NAMESPACE }],
    },
  ];
}
