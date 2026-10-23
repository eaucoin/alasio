/**
 * alasio itself: its Deployment, the namespaces its workspaces run in, its identity in
 * them, and the template it makes its folder workspaces' Sandboxes from (src/kube/
 * config.ts); main's, or a branch environment's in its own namespaces (./branch.ts), and
 * main's Service of the forks it asks the sandbox gateway for them.
 */
import type {
  KubernetesObject,
  V1Deployment,
  V1EnvVar,
  V1Namespace,
  V1Role,
  V1RoleBinding,
  V1Service,
  V1ServiceAccount,
  V1PodTemplateSpec,
  V1Volume,
} from "@kubernetes/client-node";

import { BRANCH_PARENT_LABEL, branchSessionsNamespace, MAIN_ENVIRONMENT_NAME } from "../../../src/branch/names.ts";
import { ALASIO_GATEWAY_CALLER_KEY_VARIABLE, ALASIO_GATEWAY_ENVIRONMENT_VARIABLE, ALASIO_GATEWAY_FOLDERS_VARIABLE, ALASIO_GATEWAY_URL_VARIABLE } from "../../../src/gateway/names.ts";
import type { ApiServer } from "../api-server.ts";
import { gvisorReleases, NODE_PINS } from "../cluster/node.ts";
import { imageReference } from "../images.ts";
import { busyLabelPolicy } from "./admission.ts";
import { foldersRun } from "./folders.ts";
import { carriedPools, GATEWAY, GATEWAY_PORT } from "./sandbox-gateway.ts";
import { alasioGroup, environmentLogSecret, environmentName, lakeWakeTopic, LOG_BROKERS, telegramTopic } from "./log.ts";
import { CODEX_LOGIN_PORT } from "../../../src/harness/names.ts";
import {
  BRANCH_FORK_SECRET,
  BRANCH_LABEL,
  componentName,
  DATABASE_SECRET,
  type Environment,
  envOf,
  given,
  hostVolumeMounts,
  hostVolumes,
  imagePullSecrets,
  labels,
  lakeRuns,
  MAIN_ENVIRONMENT,
  NAMESPACE,
  ONE,
  otelEnv,
  RELEASE,
  replicasOf,
  restrictedContainer,
  restrictedPod,
  roleVerifiers,
  selectorLabels,
  SESSIONS_CALLER_SECRET,
} from "./common.ts";
import { durationSeconds, type InstallConfig } from "./config.ts";
import { busyBudget } from "./disruption.ts";
import { priority } from "./priorities.ts";
import { verticalPodAutoscaler } from "./sizing.ts";

/** The namespace of the sessions of the alasio of `environment`: the configured one, or a branch environment's own. */
export function sessionsNamespace(config: InstallConfig, { branch }: Environment): string {
  return branch ? branchSessionsNamespace(branch) : config.sessions.namespace;
}

/** How long a session's bayma holds a stop alasio did not make (sessions.snapshots.stopWait). */
export function stopWaitSeconds(config: InstallConfig): number {
  // A duration, as the configuration checks it.
  return durationSeconds(config.sessions.snapshots.stopWait) ?? 0;
}

/**
 * The CPU features sessions' sandboxes are held to, so that a snapshot of one restores on
 * any node (gVisor's names, as runsc's cpu-features says them, none empty); null where
 * none is known yet, when sessions are not snapshotted (../cpu-baseline.ts).
 */
export type CpuBaseline = readonly string[] | null;

/**
 * What `alasio up` finds the cluster keeps: for the sessions' snapshots, the CPU baseline
 * it holds them to, which up keeps while every node it surveys has it (../cpu-baseline.ts),
 * and the gVisor releases it keeps, the current one first (../kept-releases.ts); the
 * checksum of the bot's Secret (../secrets.ts botChecksum), which alasio reads only as it
 * starts, so its pod carries it, and is replaced when the bot or its users change; and where
 * its API server is (../api-server.ts), which the NetworkPolicies of what may reach it alone
 * name, null where it cannot be read; and the UIDs of the Services the nodes' mount of workspace
 * storage reaches by their addresses (./sandbox-gateway.ts mountServices), each by its name,
 * null where it is not there, so a Service made anew rolls the mount onto its address.
 */
export interface Kept {
  readonly cpuBaseline: CpuBaseline;
  readonly gvisorReleases: readonly string[];
  readonly botChecksum: string | null;
  readonly apiServer: ApiServer | null;
  readonly mountServices: Readonly<Record<string, string | null>>;
}

/** What a cluster keeps where up has found nothing of it yet: no baseline, the releases pinned, no bot, no API server known, and none of the mount's Services. */
export const NOTHING_KEPT: Kept = { cpuBaseline: null, gvisorReleases: gvisorReleases(NODE_PINS), botChecksum: null, apiServer: null, mountServices: {} };

/**
 * What `alasio up` carries forward from the installation it replaces: the sandbox pools'
 * templates before the current ones (./sandbox-gateway.ts carriedPools), which their
 * snapshots may still restore onto, until the templates change again; and what the
 * cluster keeps.
 */
export interface Carried extends Kept {
  /** Each sandbox pool's template before the current one, by its sessions' internet, as the gateway's configuration carried it (./sandbox-gateway.ts carriedPools). */
  readonly previousPools: PreviousPools;
}

/** Each sandbox pool's template before the current one, by its sessions' internet; none where it has not changed. */
export type PreviousPools = Readonly<Record<"none" | "full", V1PodTemplateSpec | null>>;

/** Nothing carried forward: a first installation's, and a branch environment's. */
export const NOTHING_CARRIED: Carried = { previousPools: { none: null, full: null }, ...NOTHING_KEPT };

/**
 * What the installation `config` describes, with what the cluster keeps, `kept`, carries
 * forward from `liveGateway`, the sandbox gateway's configuration of the installation it
 * replaces (its ConfigMap's `config.json`, if it has one): its pools' templates before the
 * current ones (./sandbox-gateway.ts carriedPools). Pure, for tests.
 */
export function carriedFrom(config: InstallConfig, kept: Kept, liveGateway?: string): Carried {
  return { ...kept, previousPools: carriedPools(config, liveGateway, kept) };
}

/** Where alasio's Secrets of branch environments are mounted: main's key, or a branch's token. */
const BRANCH_SECRETS = "/run/alasio/branch";

/**
 * What alasio is told of branch environments: main, where it forks sessions, where the
 * key their tokens are signed with is, from the stack's Secret, which may be made after
 * it starts; a branch, which it is, and its token to ask its parent to fork the sessions it
 * inherited with, on the log (src/branch/fork.ts).
 */
function branchEnv({ branch }: Environment): V1EnvVar[] {
  if (branch) {
    return [
      { name: "ALASIO_BRANCH", value: branch },
      { name: "ALASIO_BRANCH_FORK_TOKEN_FILE", value: `${BRANCH_SECRETS}/token` },
    ];
  }
  return [{ name: "ALASIO_BRANCH_FORK_KEY_FILE", value: `${BRANCH_SECRETS}/fork-key` }];
}

/** The volume of branch environments' Secret: main's key, kept optional, or a branch's token. */
function branchSecretVolume({ branch }: Environment): V1Volume[] {
  if (branch) return [{ name: "branch", secret: { secretName: BRANCH_FORK_SECRET, items: [{ key: "token", path: "token" }] } }];
  return [{ name: "branch", secret: { secretName: componentName("branches"), items: [{ key: "fork-key", path: "fork-key" }], optional: true } }];
}

/**
 * The roles main's alasio makes for what reads its Neon beside it (common.ts
 * roleVerifiers); a branch environment's makes none, as it has none of them, and is given
 * none of their verifiers.
 */
function readerRoles(config: InstallConfig, { branch }: Environment): ReturnType<typeof roleVerifiers> {
  return branch === null ? roleVerifiers(config) : [];
}

/** alasio's environment: where its templates, state, database and receiver are, and what the configuration adds. */
function alasioEnv(config: InstallConfig, home: string, operatorHome: boolean, environment: Environment): V1EnvVar[] {
  const { alasio, host } = config;
  return [
    { name: "ALASIO_STATE_DIR", value: "/var/lib/alasio/state" },
    { name: "HOME", value: home },
    // A home of alasio's own does not outlast its pod; the operator's keeps their login.
    ...(operatorHome ? [] : [{ name: "ALASIO_KEEP_CODEX_LOGIN", value: "1" }]),
    { name: "ALASIO_NAMESPACE", valueFrom: { fieldRef: { fieldPath: "metadata.namespace" } } },
    { name: "ALASIO_POD", valueFrom: { fieldRef: { fieldPath: "metadata.name" } } },
    { name: "TELEGRAM_BOT_TOKEN", valueFrom: { secretKeyRef: { name: alasio.telegram.existingSecret, key: "token" } } },
    { name: "TELEGRAM_ALLOWED_USER_IDS", valueFrom: { secretKeyRef: { name: alasio.telegram.existingSecret, key: "allowedUserIds" } } },
    { name: "ALASIO_DATABASE_URL_FILE", value: "/run/alasio/database/url" },
    { name: "ALASIO_LAKE_VERIFIER_FILE", value: "/run/alasio/database/lake-verifier" },
    { name: "ALASIO_HARNESS_VERIFIER_FILE", value: "/run/alasio/database/harness-verifier" },
    ...readerRoles(config, environment).map(({ key, variable }) => ({ name: variable, value: `/run/alasio/database/${key}` })),
    { name: "ALASIO_LAKE_ENABLED", value: config.lake.enabled ? "1" : "0" },
    // Its Telegram updates: its environment's topic, read in its group as its principal.
    { name: "ALASIO_LOG_BROKERS", value: LOG_BROKERS },
    { name: "ALASIO_LOG_USERNAME", valueFrom: { secretKeyRef: { name: environmentLogSecret(environment), key: "username" } } },
    { name: "ALASIO_LOG_PASSWORD_FILE", value: "/run/alasio/log/password" },
    { name: "ALASIO_TELEGRAM_TOPIC", value: telegramTopic(environmentName(environment)) },
    { name: "ALASIO_TELEGRAM_GROUP", value: alasioGroup(environmentName(environment)) },
    // Its environment's wake topic, which it tells the lake's loader of each transcript it writes on, as that principal.
    ...(lakeRuns(config) ? [{ name: "ALASIO_LAKE_WAKE_TOPIC", value: lakeWakeTopic(environmentName(environment)) }] : []),
    ...(alasio.defaultHarness ? [{ name: "ALASIO_DEFAULT_HARNESS", value: alasio.defaultHarness }] : []),
    ...(host.enabled && host.workspaceRoot ? [{ name: "ALASIO_WORKSPACE_ROOT", value: host.workspaceRoot }] : []),
    ...(alasio.claude.existingSecret
      ? [{ name: "CLAUDE_CODE_OAUTH_TOKEN", valueFrom: { secretKeyRef: { name: alasio.claude.existingSecret, key: alasio.claude.key } } }]
      : []),
    // Its environment's sessions, through the sandbox gateway, by its environment's own key of the gateway's.
    { name: ALASIO_GATEWAY_URL_VARIABLE, value: `http://${GATEWAY}.${NAMESPACE}.svc.cluster.local:${GATEWAY_PORT}` },
    { name: ALASIO_GATEWAY_ENVIRONMENT_VARIABLE, value: environmentName(environment) },
    { name: ALASIO_GATEWAY_CALLER_KEY_VARIABLE, valueFrom: { secretKeyRef: { name: SESSIONS_CALLER_SECRET, key: "caller-key" } } },
    // Main's folder workspaces, the gateway's folder instances, where the host profile renders them.
    ...(environment.branch === null && foldersRun(config) ? [{ name: ALASIO_GATEWAY_FOLDERS_VARIABLE, value: "1" }] : []),
    // Where its conversations' harnesses' pools' pods reach its relay of its Codex login, which only it holds.
    { name: "ALASIO_CODEX_LOGIN_URL", value: `http://${CODEX_LOGIN}.${environment.namespace}.svc.cluster.local:${CODEX_LOGIN_PORT}/v1` },
    ...branchEnv(environment),
    ...otelEnv(config, environment),
    ...envOf(alasio.env),
  ];
}

/**
 * alasio's Deployment: one replica, the one member of its group on its topic's one
 * partition, which its turns run in, so an update stops the old pod before the new one
 * starts. It keeps no volume: its state is in Neon,
 * and what it writes to disk (received files written for the agent, its home unless it
 * is the operator's) is written again as needed, on emptyDirs. Under the
 * host profile it runs as the operator, in their home, with the machine's mounts. A
 * branch environment's runs in its namespace on the branch's database, told it is the
 * branch, with its token to ask main to fork the sessions it inherited; main's is given
 * the key branch environments' tokens are signed with, where it forks sessions. Its pod
 * carries, where up read it, the checksum of its bot's Secret, so it is replaced when it
 * changes.
 */
function deployment(config: InstallConfig, botChecksum: string | null, environment: Environment): V1Deployment {
  const { alasio, host } = config;
  const operatorHome = host.enabled && host.alasioHome;
  const home = operatorHome ? host.home : "/var/lib/alasio/home";
  const uid = host.enabled ? host.uid : alasio.runAsUser;
  const gid = host.enabled ? host.gid : alasio.runAsGroup;
  const groups = [...new Set([...alasio.supplementalGroups, ...(host.enabled ? host.supplementalGroups : [])])];
  const image = imageReference(config.images.alasio);
  const branchSecret = branchSecretVolume(environment);
  return {
    apiVersion: "apps/v1",
    kind: "Deployment",
    metadata: { name: RELEASE, namespace: environment.namespace, labels: labels("alasio") },
    spec: {
      ...replicasOf(ONE),
      strategy: { type: "Recreate" },
      selector: { matchLabels: selectorLabels("alasio") },
      template: {
        metadata: {
          labels: labels("alasio"),
          ...(botChecksum === null ? {} : { annotations: { "checksum/telegram": botChecksum } }),
        },
        spec: {
          serviceAccountName: RELEASE,
          ...priority("turn"),
          // alasio finishes its turns' bookkeeping and records what it interrupts as it stops.
          terminationGracePeriodSeconds: 60,
          securityContext: { ...restrictedPod(uid, gid), ...given("supplementalGroups", groups) },
          ...imagePullSecrets(config),
          containers: [{
            name: "alasio",
            image,
            imagePullPolicy: config.images.pullPolicy,
            workingDir: home,
            env: alasioEnv(config, home, operatorHome, environment),
            ...given("envFrom", [...alasio.envFrom]),
            ports: [{ name: "codex-login", containerPort: CODEX_LOGIN_PORT }],
            securityContext: restrictedContainer(),
            resources: alasio.resources,
            volumeMounts: [
              { name: "state", mountPath: "/var/lib/alasio/state" },
              ...(operatorHome ? [] : [{ name: "alasio-home", mountPath: home }]),
              { name: "database", mountPath: "/run/alasio/database", readOnly: true },
              { name: "log", mountPath: "/run/alasio/log", readOnly: true },
              ...branchSecret.map(({ name }) => ({ name, mountPath: BRANCH_SECRETS, readOnly: true })),
              ...(host.enabled ? hostVolumeMounts(config) : []),
            ],
          }],
          volumes: [
            { name: "state", emptyDir: {} },
            ...(operatorHome ? [] : [{ name: "alasio-home", emptyDir: {} }]),
            {
              name: "database",
              secret: {
                secretName: DATABASE_SECRET,
                items: ["url", "lake-verifier", "harness-verifier", ...readerRoles(config, environment).map(({ key }) => key)].map((key) => ({ key, path: key })),
              },
            },
            { name: "log", secret: { secretName: environmentLogSecret(environment), items: [{ key: "password", path: "password" }] } },
            ...branchSecret,
            ...(host.enabled ? hostVolumes(config) : []),
          ],
          ...given("nodeSelector", { ...alasio.nodeSelector }),
          ...given("tolerations", [...alasio.tolerations]),
          ...given("affinity", alasio.affinity),
        },
      },
    },
  };
}

/**
 * The namespaces workspaces run in, which alasio makes unless told not to. Sessions'
 * enforces Pod Security "restricted": nothing privileged, no host paths, no added
 * capabilities; but where the sandbox service runs, whose pools' pods mount their own
 * directories on their nodes, a host path no level but "privileged" allows, it enforces that
 * and warns and audits of "restricted", the sandbox pods' policy holding every pod of it to the
 * pools' templates instead (./admission.ts sandboxPodPolicy). The host profile's is
 * "privileged", as its pods mount the machine. A branch environment always has its own,
 * beside its own namespace, which records the environment it was branched from, main's
 * (BRANCH_PARENT_LABEL), as the sandbox gateway forks main's sessions into it for that alone.
 */
function namespaces(config: InstallConfig, environment: Environment): V1Namespace[] {
  const { sessions, host } = config;
  const { branch } = environment;
  const branched = branch ? { [BRANCH_LABEL]: branch } : {};
  return [
    ...(branch ? [{ apiVersion: "v1", kind: "Namespace", metadata: { name: environment.namespace, labels: { ...labels("alasio"), ...branched } } }] : []),
    ...(sessions.createNamespace || branch
      ? [{
        apiVersion: "v1",
        kind: "Namespace",
        metadata: {
          name: sessionsNamespace(config, environment),
          labels: {
            ...labels("session"),
            ...branched,
            ...(branch ? { [BRANCH_PARENT_LABEL]: MAIN_ENVIRONMENT_NAME } : {}),
            "pod-security.kubernetes.io/enforce": "privileged",
            "pod-security.kubernetes.io/audit": "restricted",
            "pod-security.kubernetes.io/audit-version": "latest",
            "pod-security.kubernetes.io/warn": "restricted",
            "pod-security.kubernetes.io/warn-version": "latest",
          },
        },
      }]
      : []),
    ...(host.enabled && host.createNamespace
      ? [{
        apiVersion: "v1",
        kind: "Namespace",
        metadata: { name: host.namespace, labels: { ...labels("folder-bayma"), "pod-security.kubernetes.io/enforce": "privileged" } },
      }]
      : []),
  ];
}

/**
 * alasio's identity: it labels its own pod busy while turns run in it (src/kube/busy.ts), and
 * nothing else. Its sessions and its folder workspaces are the sandbox gateway's, which it
 * reaches by its key alone.
 */
function identity(config: InstallConfig, environment: Environment): KubernetesObject[] {
  const inOwnNamespace = { name: RELEASE, namespace: environment.namespace, labels: labels("alasio") };
  const own: V1Role = { apiVersion: "rbac.authorization.k8s.io/v1", kind: "Role", metadata: inOwnNamespace, rules: [{ apiGroups: [""], resources: ["pods"], verbs: ["list", "patch"] }] };
  const binding: V1RoleBinding = {
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "RoleBinding",
    metadata: inOwnNamespace,
    subjects: [{ kind: "ServiceAccount", name: RELEASE, namespace: environment.namespace }],
    roleRef: { apiGroup: "rbac.authorization.k8s.io", kind: "Role", name: RELEASE },
  };
  const serviceAccount: V1ServiceAccount = { apiVersion: "v1", kind: "ServiceAccount", metadata: { name: RELEASE, namespace: environment.namespace, labels: labels("alasio") } };
  return [serviceAccount, own, binding, ...(environment.branch === null && foldersRun(config) ? hostAgentIdentity(config) : [])];
}

/**
 * Folder workspaces' bayma: the operator's agents, which may restart alasio as they
 * would restart it on the machine, by rolling out its Deployment, and do nothing else
 * with the cluster.
 */
function hostAgentIdentity(config: InstallConfig): KubernetesObject[] {
  const agent = componentName("host-agent");
  const restart = componentName("restart");
  const serviceAccount: V1ServiceAccount = {
    apiVersion: "v1",
    kind: "ServiceAccount",
    metadata: { name: agent, namespace: config.host.namespace, labels: labels("folder-bayma") },
  };
  const role: V1Role = {
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "Role",
    metadata: { name: restart, namespace: NAMESPACE, labels: labels("folder-bayma") },
    rules: [
      { apiGroups: ["apps"], resources: ["deployments"], resourceNames: [RELEASE], verbs: ["get", "patch"] },
      { apiGroups: ["apps"], resources: ["replicasets"], verbs: ["get", "list", "watch"] },
    ],
  };
  const binding: V1RoleBinding = {
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "RoleBinding",
    metadata: { name: restart, namespace: NAMESPACE, labels: labels("folder-bayma") },
    subjects: [{ kind: "ServiceAccount", name: agent, namespace: config.host.namespace }],
    roleRef: { apiGroup: "rbac.authorization.k8s.io", kind: "Role", name: restart },
  };
  return [serviceAccount, role, binding];
}

/** The Service of its relay of its Codex login, which its harness pools' pods reach it by (src/codex/login-relay.ts). */
export const CODEX_LOGIN = componentName("codex-login");

function codexLoginService(environment: Environment): V1Service {
  return {
    apiVersion: "v1",
    kind: "Service",
    metadata: { name: CODEX_LOGIN, namespace: environment.namespace, labels: labels("alasio") },
    spec: { selector: selectorLabels("alasio"), ports: [{ name: "codex-login", port: CODEX_LOGIN_PORT, targetPort: "codex-login" }] },
  };
}

/** The objects of the alasio of `environment`, main's unless a branch environment's, with what main's carries forward. */
export function alasioObjects(config: InstallConfig, environment: Environment = MAIN_ENVIRONMENT, carried: Carried = NOTHING_CARRIED): KubernetesObject[] {
  const alasio = deployment(config, carried.botChecksum, environment);
  return [
    ...namespaces(config, environment),
    ...identity(config, environment),
    alasio,
    // It runs turns, in its harnesses, which a recreation would end.
    verticalPodAutoscaler(alasio, "AtStart"),
    // A drain waits for the turns running in its pod to end.
    busyBudget(environment.namespace),
    codexLoginService(environment),
    // What alasio may do beyond what RBAC can say: the cluster's, made with main's.
    ...(environment.branch === null ? busyLabelPolicy() : []),
  ];
}
