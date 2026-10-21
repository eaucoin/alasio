/**
 * Conversations' harnesses' pools (src/gateway/harnesses.ts, src/harness/program.ts), where
 * the configuration turns them on (harness.pools.enabled): each conversation's harness host,
 * `alasio harness`, runs in a pod of its harness's pool, which the sandbox gateway binds to the
 * conversation while it has work, rather than in alasio's process.
 *
 * - A pool of each harness in each environment's own namespace, beside its alasio (main's, or
 *   a branch's, ./branch.ts), for its session conversations: alasio's image, restricted, as
 *   alasio's own user, with no ServiceAccount token, its own directory on its node
 *   (src/node-agent/names.ts ATTACH_ROOT, by `subPathExpr` of its namespace and its name) at
 *   /attach, where its node's agent writes its binding once the gateway binds it; it serves
 *   once it is there, and is ready only then. Its database is its environment's Neon, as the
 *   harness host's role, its rows its conversation's alone, as its grant says
 *   (src/harness/grant.ts); what its harness does on its own it tells the sandbox gateway, with
 *   its binding's token, which tells it on its environment's events topic, as the pod reaches
 *   no log; and alasio tells its lake's loader of what it commits.
 * - A pool of each harness of main's folder conversations, where the host profile is on,
 *   beside main's alasio: the same, run as the operator, in their home, with the machine's
 *   mounts, as alasio's own pod is under the host profile; not in the folder namespace, whose
 *   pods are the folder instances' alone, the operator's bayma, which the folder pods' policy
 *   holds them to (./admission.ts folderPodPolicy), and which has none of the Secrets a host
 *   runs on.
 *
 * KEDA keeps `min` free of each, and makes more, to `max`, for the binds that wait; a bound pod
 * is out of its Deployment, which replaces it, and is evicted by no drain (./disruption.ts
 * boundBudget). The gateway may claim, read and delete each pool's pods and hold their
 * bindings' Leases, in each of their namespaces, and make no pod, nor write another Lease there
 * (./admission.ts harnessPodPolicy). Every pool's pod is made of its template alone, and
 * changes but its binding (./admission.ts harnessPoolPodPolicy): a folder conversation's as the
 * operator, with the machine's mounts the host profile gives and no capability of its own, and
 * nothing else; and its NetworkPolicies give it the gateway in, and what it needs out
 * (./network-policies.ts). Its node's directory for it it reads alone: its binding, which it
 * marks served in a directory of its own (src/harness/program.ts).
 *
 * What a pool's pod holds of the installation's secrets, as its harness runs on them: Claude
 * Code's login, CLAUDE_CODE_OAUTH_TOKEN, where the installation gives one (alasio.claude), as
 * alasio's own pod does; and, of a folder conversation's Codex, the operator's Codex
 * configuration, copied each turn (src/codex/configuration.ts), whose config.toml may hold
 * secrets of its own (an MCP server's token among them). Never Codex's login, which alasio
 * alone holds and relays to it, each turn's token good for minutes (src/codex/login-relay.ts).
 */
import { posix } from "node:path";

import type { KubernetesObject, V1Deployment, V1EnvVar, V1PodTemplateSpec, V1Role, V1RoleBinding, V1Volume, V1VolumeMount } from "@kubernetes/client-node";

import { freeLabels, type Pool } from "../../../src/binder/names.ts";
import { ALASIO_GATEWAY_URL_VARIABLE, type GatewayConfig, HARNESS_CONTAINER } from "../../../src/gateway/names.ts";
import { HARNESS_NAMES, type HarnessName } from "../../../src/harness/names.ts";
import { ATTACH_ROOT, BINDING_FILE } from "../../../src/node-agent/names.ts";
import { imageReference } from "../images.ts";
import { READY_ONCE_BOUND_ANNOTATION } from "../kube/rollout.ts";
import {
  componentName,
  envOf,
  type Environment,
  given,
  hostVolumeMounts,
  hostVolumes,
  imagePullSecrets,
  labels,
  NAMESPACE,
  otelEnv,
  restrictedContainer,
  restrictedPod,
} from "./common.ts";
import { durationSeconds, type InstallConfig } from "./config.ts";
import { boundBudget } from "./disruption.ts";
import { foldersRun } from "./folders.ts";
import { priority } from "./priorities.ts";
import { GATEWAY, GATEWAY_COMPONENT, GATEWAY_PORT, GATEWAY_USER, sandboxServiceRuns } from "./sandbox-gateway.ts";
import { harnessPodPolicy, harnessPoolPodPolicy } from "./admission.ts";
import { scaledObject } from "./scalers.ts";
import { verticalPodAutoscaler } from "./sizing.ts";

/** The component of a harness pool's pods. */
export const HARNESS_COMPONENT = "harness";
/** The port a harness pool's pod's host serves on. */
export const HARNESS_POD_PORT = 8080;
/** How long a released harness pool's pod's host is given to drain: no turn after, the turn running waited for, its harness's session mirrored. */
const DRAIN_SECONDS = 120;

/** Each harness's pool's name, of session conversations, in each environment's namespace; and of main's folder conversations, in main's namespace beside them. */
export const HARNESS_POOLS: Readonly<Record<HarnessName, string>> = { codex: "harness-codex", claude: "harness-claude" };
export const FOLDER_HARNESS_POOLS: Readonly<Record<HarnessName, string>> = { codex: "harness-folder-codex", claude: "harness-folder-claude" };

/** Where a harness pool's pod keeps what it writes as it runs (received files, its session filesystems' Codex home): alasio's own, so a session's harness directory is where alasio's would be. */
const STATE_DIR = "/var/lib/alasio/state";
/** Where a pool's pod's Codex has its home, of its own: no login in it, alasio's relayed to it (src/harness/given-codex-login.ts). */
const CODEX_HOME = "/var/lib/alasio/codex";
/** Where a session pool's pod's harnesses have their home, of their own. */
const SESSION_HOME = "/var/lib/alasio/home";

/** Whether conversations' harnesses run in pools: where the configuration says, and the sandbox service runs, which binds them. */
export function harnessPoolsRun(config: InstallConfig): boolean {
  return config.harness.pools.enabled && sandboxServiceRuns(config);
}

/** Whether main's folder conversations' harnesses run in pools too: where the host profile renders folder workspaces. */
const folderHarnessPoolsRun = (config: InstallConfig): boolean => harnessPoolsRun(config) && foldersRun(config);

/**
 * The template of the pool `pool`'s pods, of `environment`, as the module says: of its session
 * conversations; or, `folder`, of main's folder conversations, as the operator.
 */
function harnessTemplate(config: InstallConfig, environment: Environment, pool: Pool, folder: boolean): V1PodTemplateSpec {
  const { alasio, host, sessions } = config;
  const home = folder ? host.home : SESSION_HOME;
  const [uid, gid] = folder ? [host.uid, host.gid] : [alasio.runAsUser, alasio.runAsGroup];
  const groups = folder ? [...new Set([...alasio.supplementalGroups, ...host.supplementalGroups])] : [...alasio.supplementalGroups];
  const env: V1EnvVar[] = [
    // alasio's, first, so that what the pod sets itself is its own: its homes, which the configuration may not set (config.ts), among it.
    ...envOf(alasio.env),
    { name: "POD_NAME", valueFrom: { fieldRef: { fieldPath: "metadata.name" } } },
    { name: "POD_NAMESPACE", valueFrom: { fieldRef: { fieldPath: "metadata.namespace" } } },
    { name: "ALASIO_HARNESS_BINDING_FILE", value: `/attach/${BINDING_FILE}` },
    { name: "ALASIO_HARNESS_PORT", value: String(HARNESS_POD_PORT) },
    { name: "ALASIO_STATE_DIR", value: STATE_DIR },
    { name: "HOME", value: home },
    // Its Codex's home, its own, which holds no login: a folder conversation's is given the operator's configuration each turn.
    { name: "CODEX_HOME", value: CODEX_HOME },
    { name: "ALASIO_DATABASE_URL_FILE", value: "/run/alasio/database/url" },
    ...(environment.branch ? [{ name: "ALASIO_BRANCH", value: environment.branch }] : []),
    // Its turns' workspaces' bayma, and its session's files, through the gateway, by what each turn gives.
    { name: ALASIO_GATEWAY_URL_VARIABLE, value: `http://${GATEWAY}.${NAMESPACE}.svc.cluster.local:${GATEWAY_PORT}` },
    ...(alasio.claude.existingSecret ? [{ name: "CLAUDE_CODE_OAUTH_TOKEN", valueFrom: { secretKeyRef: { name: alasio.claude.existingSecret, key: alasio.claude.key } } }] : []),
    ...otelEnv(config, environment),
  ];
  const volumeMounts: V1VolumeMount[] = [
    // Its own directory on its node, by its namespace and its name, as pods of two namespaces may share a name, where its binding is written: its to read alone, so it fills nothing of its node's.
    { name: "attach", mountPath: "/attach", subPathExpr: "$(POD_NAMESPACE)/$(POD_NAME)", readOnly: true },
    { name: "state", mountPath: STATE_DIR },
    { name: "codex", mountPath: CODEX_HOME },
    // The operator's own Codex home, their login in it, hidden where the machine's mounts would show it.
    ...(folder ? [...hostVolumeMounts(config), { name: "codex-hidden", mountPath: posix.join(home, ".codex"), readOnly: true }] : [{ name: "home", mountPath: home }]),
    { name: "database", mountPath: "/run/alasio/database", readOnly: true },
  ];
  const volumes: V1Volume[] = [
    { name: "attach", hostPath: { path: ATTACH_ROOT, type: "DirectoryOrCreate" } },
    { name: "state", emptyDir: {} },
    { name: "codex", emptyDir: {} },
    ...(folder ? [...hostVolumes(config), { name: "codex-hidden", emptyDir: {} }] : [{ name: "home", emptyDir: {} }]),
    { name: "database", secret: { secretName: componentName("harness-database"), items: [{ key: "url", path: "url" }] } },
  ];
  return {
    metadata: { labels: { ...labels(HARNESS_COMPONENT), ...freeLabels(pool) } },
    spec: {
      ...priority("turn"),
      securityContext: { ...restrictedPod(uid, gid), ...given("supplementalGroups", groups) },
      automountServiceAccountToken: false,
      enableServiceLinks: false,
      ...imagePullSecrets(config),
      // On the nodes the node agents run on, as its node's agent writes its binding beside it:
      // the sessions' nodes. The untrusted code there runs under gVisor, in pods of its own, in
      // its sessions' namespace, behind their deny-all, so a harness pod sits beside it, not in
      // it; the agents themselves run there alone, as they assume gVisor's runtime and are
      // privileged, so they are not run on alasio's nodes for it.
      ...given("nodeSelector", { ...sessions.nodeSelector }),
      ...given("tolerations", [...sessions.tolerations]),
      containers: [{
        name: HARNESS_CONTAINER,
        image: imageReference(config.images.alasio),
        imagePullPolicy: config.images.pullPolicy,
        command: ["node", "/opt/alasio/src/harness/main.ts"],
        workingDir: home,
        env,
        ports: [{ name: "host", containerPort: HARNESS_POD_PORT }],
        // Ready once it serves, which it does once bound.
        readinessProbe: { httpGet: { path: "/healthz", port: "host" }, periodSeconds: 2 },
        securityContext: restrictedContainer(),
        resources: config.harness.resources,
        volumeMounts,
      }],
      volumes,
    },
  };
}

/** A pool's Deployment, the ScaledObject that keeps its free pods, and its VPA. */
function poolObjects(config: InstallConfig, environment: Environment, pool: Pool, folder: boolean): KubernetesObject[] {
  const deployment: V1Deployment = {
    apiVersion: "apps/v1",
    kind: "Deployment",
    // Ready, as `alasio up` and `alasio status` say it, once its pods run: each is ready only once bound.
    metadata: { name: pool.name, namespace: pool.namespace, labels: labels(HARNESS_COMPONENT), annotations: { [READY_ONCE_BOUND_ANNOTATION]: "true" } },
    spec: {
      selector: { matchLabels: { ...freeLabels(pool) } },
      // None is ready before it is bound: the old free pods go as the new come.
      strategy: { type: "Recreate" },
      template: harnessTemplate(config, environment, pool, folder),
    },
  };
  // As many free as binds wait for, past `min`: each is claimed by one.
  const waiting = {
    type: "metrics-api",
    metadata: { url: `http://${GATEWAY}.${NAMESPACE}.svc.cluster.local:${GATEWAY_PORT}/counts/${pool.namespace}/${pool.name}`, valueLocation: "waiting", targetValue: "1" },
  };
  // Sized as recommended alone: a host's resources change only as a new pod is made.
  return [deployment, ...scaledObject(deployment, config.harness.pools.scaling, [waiting]), verticalPodAutoscaler(deployment, "Recommended")];
}

/** What the gateway may do with a namespace's harness pools: claim, read and delete their pods, and hold their Leases; make none. */
function gatewayRole(namespace: string, pools: readonly string[]): KubernetesObject[] {
  const metadata = { name: componentName("sandbox-gateway-harnesses"), namespace, labels: labels(GATEWAY_COMPONENT) };
  const role: V1Role = {
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "Role",
    metadata,
    rules: [
      // Its pools' pods, claimed by an update of the version read, and deleted as they are released.
      { apiGroups: [""], resources: ["pods"], verbs: ["get", "list", "watch", "update", "delete"] },
      { apiGroups: ["coordination.k8s.io"], resources: ["leases"], verbs: ["get", "list", "watch", "create", "update", "patch", "delete"] },
      { apiGroups: ["apps"], resources: ["deployments"], verbs: ["get"], resourceNames: [...pools] },
    ],
  };
  const binding: V1RoleBinding = {
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "RoleBinding",
    metadata,
    roleRef: { apiGroup: "rbac.authorization.k8s.io", kind: "Role", name: metadata.name },
    subjects: [{ kind: "ServiceAccount", name: GATEWAY, namespace: NAMESPACE }],
  };
  return [role, binding];
}

/** The templates of `environment`'s harness pools, its session conversations' and, main's, its folder conversations'. */
function harnessTemplates(config: InstallConfig, environment: Environment): V1PodTemplateSpec[] {
  const sessions = HARNESS_NAMES.map((harness) => harnessTemplate(config, environment, { name: HARNESS_POOLS[harness], namespace: environment.namespace }, false));
  const folders = environment.branch === null && foldersRun(config)
    ? HARNESS_NAMES.map((harness) => harnessTemplate(config, environment, { name: FOLDER_HARNESS_POOLS[harness], namespace: environment.namespace }, true))
    : [];
  return [...sessions, ...folders];
}

/** The pools of `environment`'s session conversations, by harness, in its own namespace. */
const harnessPoolsOf = (environment: Environment): Record<HarnessName, Pool> =>
  Object.fromEntries(HARNESS_NAMES.map((harness) => [harness, { name: HARNESS_POOLS[harness], namespace: environment.namespace }])) as Record<HarnessName, Pool>;

/** The harness pools of `environment`, main's or a branch environment's, in its own namespace, as the module says; and, main's, its folder conversations'. */
export function harnessPoolObjects(config: InstallConfig, environment: Environment): KubernetesObject[] {
  if (!harnessPoolsRun(config)) return [];
  const pools = harnessPoolsOf(environment);
  const folder = environment.branch === null && foldersRun(config);
  const folderPools = folder ? HARNESS_NAMES.map((harness) => ({ name: FOLDER_HARNESS_POOLS[harness], namespace: environment.namespace })) : [];
  return [
    ...gatewayRole(environment.namespace, [...Object.values(HARNESS_POOLS), ...folderPools.map(({ name }) => name)]),
    ...HARNESS_NAMES.flatMap((harness) => poolObjects(config, environment, pools[harness], false)),
    ...folderPools.flatMap((pool) => poolObjects(config, environment, pool, true)),
    boundBudget(environment.namespace),
    // What the gateway does to the pods of every environment's namespace held to its pools' alone, as RBAC cannot:
    // the cluster's, made with main's.
    ...(environment.branch === null ? harnessPodPolicy(GATEWAY_USER, [...Object.values(HARNESS_POOLS), ...Object.values(FOLDER_HARNESS_POOLS)]) : []),
    // Every pool's pod made in the environment's namespace held to its template, as this environment's
    // configuration renders it: its folder conversations' to the operator's user, mounts and capabilities as
    // the template has them, and no others. The environment's own, made and deleted with it.
    ...harnessPoolPodPolicy({ namespace: environment.namespace, branch: environment.branch, templates: harnessTemplates(config, environment) }),
  ];
}

/** What the gateway's configuration says of the harnesses' pools (src/gateway/names.ts GatewayConfig): none where they do not run. */
export function harnessesConfig(config: InstallConfig): GatewayConfig["harnesses"] {
  if (!harnessPoolsRun(config)) return null;
  const { alasio, host } = config;
  return {
    namespace: NAMESPACE,
    pools: { ...HARNESS_POOLS },
    podPort: HARNESS_POD_PORT,
    owner: { uid: alasio.runAsUser, gid: alasio.runAsGroup },
    // A duration, as the configuration checks it.
    releaseAfterSeconds: durationSeconds(config.harness.pools.releaseAfter) ?? 0,
    drainSeconds: DRAIN_SECONDS,
    folders: folderHarnessPoolsRun(config) ? { pools: { ...FOLDER_HARNESS_POOLS }, owner: { uid: host.uid, gid: host.gid } } : null,
  };
}
