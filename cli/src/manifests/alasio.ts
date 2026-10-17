/**
 * alasio itself: its Deployment, its telemetry receiver's Service, the
 * namespaces its workspaces run in, its identity in them, and the templates it makes
 * its workspaces' Sandboxes from (src/kube/config.ts); main's, or a branch environment's
 * in its own namespaces (./branch.ts), and main's Service of the forks it makes for them.
 */
import { isDeepStrictEqual } from "node:util";

import type {
  KubernetesObject,
  V1ConfigMap,
  V1Deployment,
  V1EnvVar,
  V1Namespace,
  V1ObjectMeta,
  V1Role,
  V1RoleBinding,
  V1Service,
  V1ServiceAccount,
  V1Volume,
} from "@kubernetes/client-node";

import { Result } from "effect";

import { BRANCH_FORK_PORT, branchSessionsNamespace } from "../../../src/branch/names.ts";
import { decodeKubeTemplates, type HostProfile, type SessionsProfile } from "../../../src/kube/config.ts";
import { CPU_FEATURES_ANNOTATION } from "../../../src/kube/sandbox-shape.ts";
import { gvisorRuntimeClass, NODE_AGENT_PORT, NODE_AGENT_TOKEN_VARIABLE } from "../../../src/node-agent/names.ts";
import type { ApiServer } from "../api-server.ts";
import { gvisorReleases, NODE_PINS } from "../cluster/node.ts";
import { imageReference } from "../images.ts";
import { alasioAccount, busyLabelPolicy, cloneJobPolicy, resumedConditionPolicy, sandboxPolicy, sandboxPolicyName, secretPolicy } from "./admission.ts";
import { sandboxServiceRuns } from "./sandbox-gateway.ts";
import { alasioGroup, environmentLogSecret, environmentName, lakeWakeTopic, LOG_BROKERS, telegramTopic } from "./log.ts";
import {
  BRANCH_FORK_SECRET,
  BRANCH_LABEL,
  componentName,
  databaseSecret,
  type Environment,
  envOf,
  given,
  goJson,
  helperResources,
  hostVolumeMounts,
  hostVolumes,
  imagePullSecrets,
  labels,
  lakeRuns,
  MAIN_ENVIRONMENT,
  NAMESPACE,
  nodeAgentRuns,
  ONE,
  otelEnv,
  RELEASE,
  replicasOf,
  restrictedContainer,
  restrictedPod,
  roleVerifiers,
  selectorLabels,
  sha256,
} from "./common.ts";
import { durationSeconds, type InstallConfig } from "./config.ts";
import { busyBudget } from "./disruption.ts";
import { priority } from "./priorities.ts";
import { verticalPodAutoscaler } from "./sizing.ts";
import { cloneJob } from "./workspace-storage.ts";

/** The port every workspace's bayma serves MCP on. */
const BAYMA_PORT = 7290;

/** The namespace of the sessions of the alasio of `environment`: the configured one, or a branch environment's own. */
export function sessionsNamespace(config: InstallConfig, { branch }: Environment): string {
  return branch ? branchSessionsNamespace(branch) : config.sessions.namespace;
}

/** The namespaces whose Sandboxes alasio drives: the sessions', and the host profile's. */
function workspaceNamespaces(config: InstallConfig, environment: Environment): string[] {
  return [...(config.sessions.enabled ? [sessionsNamespace(config, environment)] : []), ...(config.host.enabled ? [config.host.namespace] : [])];
}

/**
 * The template of session filesystems' Sandboxes: an empty, isolated workspace under
 * the sessions' runtime, restricted, on a volume of workspace storage's class when it is
 * on. alasio adds what is per Sandbox: its name and labels, bayma's token, and its
 * network mode. A branch environment's are in its own namespace, and neither clones a
 * session (its parent does, src/branch/fork.ts) nor removes JuiceFS's mount pods, which
 * are main's to drive.
 */
function sessionsProfile(config: InstallConfig, environment: Environment, { cpuBaseline, gvisorReleases }: Kept): SessionsProfile {
  const { sessions, workspaceStorage } = config;
  const main = environment.branch === null;
  // Snapshotted once every node's CPU features are known, its sandboxes held to those all have.
  const snapshotted = main && nodeAgentRuns(config) && cpuBaseline !== null;
  const agent = imageReference(config.images.agent);
  const storageClassName = sessions.storage.storageClassName || (workspaceStorage.enabled ? workspaceStorage.storageClassName : "");
  // On workspace storage, the volume's mounts follow its mount pod's, as upstream's
  // template has them (gVisor does not, and alasio restarts a session whose mount is
  // gone), and its files are given the pod's group only when its root is not the group's,
  // as walking a JuiceFS volume is slow.
  const propagation = workspaceStorage.enabled ? { mountPropagation: "HostToContainer" } : {};
  const ownership = workspaceStorage.enabled ? { fsGroupChangePolicy: "OnRootMismatch" } : {};
  return {
    namespace: sessionsNamespace(config, environment),
    port: BAYMA_PORT,
    workspaceDir: "/workspace",
    egressGate: sessions.egressGate,
    fullModeNameservers: [...sessions.fullModeNameservers],
    ...(main && mountPodsRemoved(config) ? { mountPodNamespace: workspaceStorage.csi.namespace } : {}),
    podTemplate: {
      metadata: {
        labels: { "app.kubernetes.io/part-of": "alasio", "app.kubernetes.io/instance": RELEASE, "app.kubernetes.io/component": "session" },
        // Under gVisor, /tmp is a tmpfs of the sandbox's own, by gVisor's mount hints, rather
        // than the emptyDir's directory on the node: a snapshot of the sandbox holds it, as it
        // holds the files that bayma's runtimes keep open there.
        annotations: {
          "dev.gvisor.spec.mount.tmp.share": "container",
          "dev.gvisor.spec.mount.tmp.type": "tmpfs",
          "dev.gvisor.spec.mount.tmp.options": "rw",
          ...(snapshotted ? { [CPU_FEATURES_ANNOTATION]: cpuBaseline.join(",") } : {}),
        },
      },
      spec: {
        ...(sessions.runtimeClassName ? { runtimeClassName: sessions.runtimeClassName } : {}),
        ...priority("turn"),
        // A pod that ends is ended: alasio brings the session up in a new one, and nothing
        // starts bayma again in a sandbox that a snapshot has just ended.
        restartPolicy: "Never",
        securityContext: { ...restrictedPod(1000, 1000), ...ownership },
        ...imagePullSecrets(config),
        ...given("nodeSelector", { ...sessions.nodeSelector }),
        ...given("tolerations", [...sessions.tolerations]),
        initContainers: [
          // The workspace and home on the session's volume, and bayma's toolbelt, which the
          // image installs, linked into the home.
          {
            name: "prepare",
            image: agent,
            imagePullPolicy: config.images.pullPolicy,
            command: [
              "/bin/sh",
              "-c",
              "mkdir -p /data/workspace /data/home/.local/share/bayma && ln -sfn /opt/bayma-data/bayma/toolbelt /data/home/.local/share/bayma/toolbelt",
            ],
            securityContext: restrictedContainer(),
            ...helperResources(),
            volumeMounts: [{ name: "data", mountPath: "/data" }],
          },
        ],
        containers: [{
          name: "bayma",
          image: agent,
          imagePullPolicy: config.images.pullPolicy,
          command: ["/usr/bin/tini", "-s", "--", "node", "/opt/bayma/bayma.js"],
          args: ["mcp-http", "--host", "0.0.0.0", "--port", String(BAYMA_PORT), "--state-dir", "/home/agent/.bayma"],
          workingDir: "/workspace",
          env: [
            { name: "HOME", value: "/home/agent" },
            { name: "USER", value: "agent" },
            { name: "BAYMA_PAYLOAD_DIR", value: "/opt/bayma/payload" },
          ],
          ports: [{ name: "mcp", containerPort: BAYMA_PORT }],
          securityContext: restrictedContainer(),
          resources: sessions.resources,
          volumeMounts: [
            { name: "data", mountPath: "/workspace", subPath: "workspace", ...propagation },
            { name: "data", mountPath: "/home/agent", subPath: "home", ...propagation },
            { name: "tmp", mountPath: "/tmp" },
          ],
        }],
        volumes: [{ name: "tmp", emptyDir: { sizeLimit: "2Gi" } }],
      },
    },
    volumeClaimTemplates: [{
      metadata: { name: "data" },
      spec: {
        accessModes: ["ReadWriteOnce"],
        ...(storageClassName ? { storageClassName } : {}),
        resources: { requests: { storage: sessions.storage.size } },
      },
    }],
    ...(main && sessionsForked(config) ? { clone: { claimTemplate: "data", job: cloneJob(config) } } : {}),
    ...(snapshotted ? { snapshots: sessionSnapshots(config, gvisorReleases) } : {}),
  };
}

/**
 * How session sandboxes are snapshotted as they are released (src/sandbox/snapshots.ts),
 * where the node agents run: once sessions.snapshots.releaseAfter has passed without an
 * exec or a turn, under the gVisor `releases` alasio keeps (../kept-releases.ts), the
 * current one first, each restored with a RuntimeClass of its own, by the agents in
 * alasio's namespace.
 */
function sessionSnapshots(config: InstallConfig, releases: readonly string[]): NonNullable<SessionsProfile["snapshots"]> {
  return {
    // A duration, as the configuration checks it.
    releaseAfterSeconds: durationSeconds(config.sessions.snapshots.releaseAfter) ?? 0,
    releases: releases.map((release) => ({ release, runtimeClassName: gvisorRuntimeClass(release) })),
    agents: {
      namespace: NAMESPACE,
      selector: Object.entries(selectorLabels("node-agent")).map(([label, value]) => `${label}=${value}`).join(","),
      port: NODE_AGENT_PORT,
    },
  };
}

/** Whether alasio forks sessions: where their volumes are workspace storage's, which JuiceFS clones. */
export function sessionsForked({ sessions, workspaceStorage }: InstallConfig): boolean {
  return workspaceStorage.enabled && [workspaceStorage.storageClassName, ""].includes(sessions.storage.storageClassName);
}

/** The template of folder workspaces' bayma: bayma itself, as the operator, with the machine's mounts. */
function hostProfile(config: InstallConfig): HostProfile {
  const { host } = config;
  return {
    namespace: host.namespace,
    port: BAYMA_PORT,
    stateRoot: host.stateRoot,
    podTemplate: {
      metadata: { labels: { "app.kubernetes.io/part-of": "alasio", "app.kubernetes.io/instance": RELEASE, "app.kubernetes.io/component": "folder-bayma" } },
      spec: {
        // May restart alasio, by rolling out its Deployment, as an agent restarts it.
        serviceAccountName: componentName("host-agent"),
        ...priority("turn"),
        securityContext: {
          runAsUser: host.uid,
          runAsGroup: host.gid,
          // The group the bayma token's Secret volume is given, which the operator's user
          // reads it as; the node's own paths the mounts give are left as they are.
          fsGroup: host.gid,
          ...given("supplementalGroups", [...host.supplementalGroups]),
        },
        ...imagePullSecrets(config),
        containers: [{
          name: "bayma",
          image: imageReference(host.baymaImage),
          imagePullPolicy: "IfNotPresent",
          args: ["mcp-http", "--host", "0.0.0.0", "--port", String(BAYMA_PORT), "--default-durability", "checkpointed"],
          workingDir: host.home,
          env: [{ name: "HOME", value: host.home }, ...envOf(host.env)],
          ports: [{ name: "mcp", containerPort: BAYMA_PORT }],
          securityContext: {
            // What bayma needs to snapshot its REPL sessions' processes as it stops and
            // restore them whole as it starts again.
            capabilities: { add: ["CHECKPOINT_RESTORE", "SYS_PTRACE"] },
            seccompProfile: { type: "Unconfined" },
            appArmorProfile: { type: "Unconfined" },
          },
          resources: host.resources,
          ...given("volumeMounts", hostVolumeMounts(config)),
        }],
        ...given("volumes", hostVolumes(config)),
      },
    },
  };
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
 * name, null where it cannot be read.
 */
export interface Kept {
  readonly cpuBaseline: CpuBaseline;
  readonly gvisorReleases: readonly string[];
  readonly botChecksum: string | null;
  readonly apiServer: ApiServer | null;
}

/** What a cluster keeps where up has found nothing of it yet: no baseline, the releases pinned, no bot, and no API server known. */
export const NOTHING_KEPT: Kept = { cpuBaseline: null, gvisorReleases: gvisorReleases(NODE_PINS), botChecksum: null, apiServer: null };

/**
 * What `alasio up` carries forward from the installation it replaces: the sessions'
 * profile before the current one, whose template their snapshots may still restore onto
 * (src/sandbox/snapshots.ts), and the Sandbox policy pins beside the current one
 * (./admission.ts), until the template changes again; and what the cluster keeps.
 */
export interface Carried extends Kept {
  readonly previousSessions: SessionsProfile | null;
}

/** Nothing carried forward: a first installation's, and a branch environment's. */
export const NOTHING_CARRIED: Carried = { previousSessions: null, ...NOTHING_KEPT };

/** The ConfigMap of the templates, by its name. */
export const SANDBOX_TEMPLATES = componentName("sandbox-templates");

/**
 * What the installation `config` describes, with what the cluster keeps, `kept`, carries
 * forward from `live`, the templates of the installation it replaces (its ConfigMap's
 * `templates.json`, if it has one): the sessions' profile it ran, where their template
 * changes, or else the one it carried. A profile is carried for one change of the
 * template, and no more. Pure, for tests.
 *
 * What it carries is pinned whole into the Sandbox policy (./admission.ts) as a template
 * alasio may make a Sandbox from, read back from the cluster rather than rendered from the
 * config: the ConfigMap is the anchor of that trust. It is one as long as only the
 * operator, by `alasio up`, writes it: alasio's ServiceAccount may write no ConfigMap
 * (./alasio.ts identity). Were alasio given that, it could write any pod template into
 * the ConfigMap, and the next `up` would let it make Sandboxes of it.
 */
export function carriedFrom(config: InstallConfig, live: string | undefined, kept: Kept): Carried {
  const nothing = { ...NOTHING_CARRIED, ...kept };
  if (!config.sessions.enabled || live === undefined) return nothing;
  // Carried as the ConfigMap held them, which alasio read as they are: the profile a snapshot recorded the hash of.
  if (Result.isFailure(decodeKubeTemplates(SANDBOX_TEMPLATES, live))) return nothing;
  const held = JSON.parse(live) as { readonly sessions?: SessionsProfile | null; readonly previousSessions?: SessionsProfile | null };
  if (!held.sessions) return nothing;
  // The current profile as the ConfigMap holds it, to compare with what it held.
  const current = JSON.parse(goJson(sessionsProfile(config, MAIN_ENVIRONMENT, kept), "")) as SessionsProfile;
  return { previousSessions: isDeepStrictEqual(held.sessions.podTemplate, current.podTemplate) ? (held.previousSessions ?? null) : held.sessions, ...kept };
}

/**
 * The CPU baseline the installation whose templates are `live` (as carriedFrom takes
 * them) holds sessions to: what its sessions' template sets, if anything.
 */
export function recordedCpuBaseline(live: string | undefined): CpuBaseline {
  if (live === undefined || Result.isFailure(decodeKubeTemplates(SANDBOX_TEMPLATES, live))) return null;
  const held = JSON.parse(live) as { readonly sessions?: SessionsProfile | null };
  const recorded = held.sessions?.podTemplate.metadata?.annotations?.[CPU_FEATURES_ANNOTATION];
  return recorded ? recorded.split(",") : null;
}

/** The ConfigMap of the templates, and their checksum, which alasio's pod carries so it is replaced when they change. */
function sandboxTemplates(config: InstallConfig, environment: Environment, carried: Carried): { readonly configMap: V1ConfigMap; readonly checksum: string } {
  const name = SANDBOX_TEMPLATES;
  const templates = goJson(
    {
      ...(config.sessions.enabled ? { sessions: sessionsProfile(config, environment, carried) } : {}),
      ...(config.sessions.enabled && carried.previousSessions ? { previousSessions: carried.previousSessions } : {}),
      ...(config.host.enabled ? { host: hostProfile(config) } : {}),
    },
    "  ",
  );
  return {
    configMap: { apiVersion: "v1", kind: "ConfigMap", metadata: { name, namespace: environment.namespace, labels: labels("alasio") }, data: { "templates.json": templates } },
    checksum: sha256(templates),
  };
}

/** Where alasio's Secrets of branch environments are mounted: main's key, or a branch's token. */
const BRANCH_SECRETS = "/run/alasio/branch";

/**
 * What alasio is told of branch environments: main, where it forks sessions, where the
 * key their tokens are signed with is, from the stack's Secret, which may be made after
 * it starts; a branch, which it is, and where its parent forks the sessions it inherited
 * for it, with its token (src/branch/fork.ts).
 */
function branchEnv(config: InstallConfig, { branch }: Environment): V1EnvVar[] {
  if (branch) {
    return [
      { name: "ALASIO_BRANCH", value: branch },
      { name: "ALASIO_PARENT_FORKS_URL", value: `http://${componentName("branches")}.${NAMESPACE}.svc:${BRANCH_FORK_PORT}` },
      { name: "ALASIO_BRANCH_FORK_TOKEN_FILE", value: `${BRANCH_SECRETS}/token` },
    ];
  }
  return forksForBranches(config) ? [{ name: "ALASIO_BRANCH_FORK_KEY_FILE", value: `${BRANCH_SECRETS}/fork-key` }] : [];
}

/** The volume of branch environments' Secret: main's key, kept optional, or a branch's token. */
function branchSecretVolume(config: InstallConfig, { branch }: Environment): V1Volume[] {
  if (branch) return [{ name: "branch", secret: { secretName: BRANCH_FORK_SECRET, items: [{ key: "token", path: "token" }] } }];
  return forksForBranches(config) ? [{ name: "branch", secret: { secretName: componentName("branches"), items: [{ key: "fork-key", path: "fork-key" }], optional: true } }] : [];
}

/** Whether main forks its sessions for branch environments: where it has Neon to branch, and forks sessions. */
export function forksForBranches(config: InstallConfig): boolean {
  return config.neon.enabled && sessionsForked(config);
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
    { name: "ALASIO_KUBE_TEMPLATES", value: "/etc/alasio/templates.json" },
    { name: "ALASIO_STATE_DIR", value: "/var/lib/alasio/state" },
    { name: "HOME", value: home },
    // A home of alasio's own does not outlast its pod; the operator's keeps their login.
    ...(operatorHome ? [] : [{ name: "ALASIO_KEEP_CODEX_LOGIN", value: "1" }]),
    { name: "ALASIO_DEPLOYMENT", value: RELEASE },
    { name: "ALASIO_NAMESPACE", valueFrom: { fieldRef: { fieldPath: "metadata.namespace" } } },
    { name: "ALASIO_POD", valueFrom: { fieldRef: { fieldPath: "metadata.name" } } },
    { name: "TELEGRAM_BOT_TOKEN", valueFrom: { secretKeyRef: { name: alasio.telegram.existingSecret, key: "token" } } },
    { name: "TELEGRAM_ALLOWED_USER_IDS", valueFrom: { secretKeyRef: { name: alasio.telegram.existingSecret, key: "allowedUserIds" } } },
    { name: "ALASIO_DATABASE_URL_FILE", value: "/run/alasio/database/url" },
    { name: "ALASIO_LAKE_VERIFIER_FILE", value: "/run/alasio/database/lake-verifier" },
    ...readerRoles(config, environment).map(({ key, variable }) => ({ name: variable, value: `/run/alasio/database/${key}` })),
    { name: "ALASIO_LAKE_ENABLED", value: config.lake.enabled && config.neon.enabled ? "1" : "0" },
    // Its Telegram updates: its environment's topic, read in its group as its principal.
    { name: "ALASIO_LOG_BROKERS", value: LOG_BROKERS },
    { name: "ALASIO_LOG_USERNAME", valueFrom: { secretKeyRef: { name: environmentLogSecret(environment), key: "username" } } },
    { name: "ALASIO_LOG_PASSWORD_FILE", value: "/run/alasio/log/password" },
    { name: "ALASIO_TELEGRAM_TOPIC", value: telegramTopic(environmentName(environment)) },
    { name: "ALASIO_TELEGRAM_GROUP", value: alasioGroup(environmentName(environment)) },
    // Its environment's wake topic, which it tells the lake's loader of each transcript it writes on, as that principal.
    ...(lakeRuns(config) ? [{ name: "ALASIO_LAKE_WAKE_TOPIC", value: lakeWakeTopic(environmentName(environment)) }] : []),
    { name: "ALASIO_TELEMETRY_RECEIVER_SERVICE", value: `${componentName("telemetry")}.${environment.namespace}.svc` },
    { name: "ALASIO_TELEMETRY_RECEIVER_PORT", value: String(config.telemetry.receiverPort) },
    ...(alasio.defaultHarness ? [{ name: "ALASIO_DEFAULT_HARNESS", value: alasio.defaultHarness }] : []),
    ...(host.enabled && host.workspaceRoot ? [{ name: "ALASIO_WORKSPACE_ROOT", value: host.workspaceRoot }] : []),
    ...(alasio.claude.existingSecret
      ? [{ name: "CLAUDE_CODE_OAUTH_TOKEN", valueFrom: { secretKeyRef: { name: alasio.claude.existingSecret, key: alasio.claude.key } } }]
      : []),
    // What it presents to the node agents, which snapshot its sessions.
    ...(environment.branch === null && nodeAgentRuns(config)
      ? [{ name: NODE_AGENT_TOKEN_VARIABLE, valueFrom: { secretKeyRef: { name: componentName("node-agent"), key: "token" } } }]
      : []),
    ...branchEnv(config, environment),
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
 * carries the checksums of its templates and, where up read it, of its bot's Secret, so
 * it is replaced when either changes.
 */
function deployment(config: InstallConfig, templatesChecksum: string, botChecksum: string | null, environment: Environment): V1Deployment {
  const { alasio, host } = config;
  const operatorHome = host.enabled && host.alasioHome;
  const home = operatorHome ? host.home : "/var/lib/alasio/home";
  const uid = host.enabled ? host.uid : alasio.runAsUser;
  const gid = host.enabled ? host.gid : alasio.runAsGroup;
  const groups = [...new Set([...alasio.supplementalGroups, ...(host.enabled ? host.supplementalGroups : [])])];
  const image = imageReference(config.images.alasio);
  const branchSecret = branchSecretVolume(config, environment);
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
          annotations: { "checksum/sandbox-templates": templatesChecksum, ...(botChecksum === null ? {} : { "checksum/telegram": botChecksum }) },
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
            ports: [
              { name: "telemetry", containerPort: config.telemetry.receiverPort },
              ...(environment.branch === null && forksForBranches(config) ? [{ name: "branch-forks", containerPort: BRANCH_FORK_PORT }] : []),
            ],
            securityContext: restrictedContainer(),
            resources: alasio.resources,
            volumeMounts: [
              { name: "state", mountPath: "/var/lib/alasio/state" },
              ...(operatorHome ? [] : [{ name: "alasio-home", mountPath: home }]),
              { name: "templates", mountPath: "/etc/alasio", readOnly: true },
              { name: "database", mountPath: "/run/alasio/database", readOnly: true },
              { name: "log", mountPath: "/run/alasio/log", readOnly: true },
              ...branchSecret.map(({ name }) => ({ name, mountPath: BRANCH_SECRETS, readOnly: true })),
              ...(host.enabled ? hostVolumeMounts(config) : []),
            ],
          }],
          volumes: [
            { name: "state", emptyDir: {} },
            ...(operatorHome ? [] : [{ name: "alasio-home", emptyDir: {} }]),
            { name: "templates", configMap: { name: componentName("sandbox-templates") } },
            {
              name: "database",
              secret: {
                secretName: databaseSecret(config),
                items: ["url", "lake-verifier", ...readerRoles(config, environment).map(({ key }) => key)].map((key) => ({ key, path: key })),
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

/** Where session sandboxes export their telemetry: alasio's receiver. */
function telemetryService(config: InstallConfig, environment: Environment): V1Service {
  return {
    apiVersion: "v1",
    kind: "Service",
    metadata: { name: componentName("telemetry"), namespace: environment.namespace, labels: labels("alasio") },
    spec: { selector: selectorLabels("alasio"), ports: [{ name: "otlp-http", port: config.telemetry.receiverPort, targetPort: "telemetry" }] },
  };
}

/** Where branch environments ask main to fork the sessions they inherited (src/branch/fork.ts). */
function branchForksService(): V1Service {
  return {
    apiVersion: "v1",
    kind: "Service",
    metadata: { name: componentName("branches"), namespace: NAMESPACE, labels: labels("alasio") },
    spec: { selector: selectorLabels("alasio"), ports: [{ name: "branch-forks", port: BRANCH_FORK_PORT, targetPort: "branch-forks" }] },
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
 * beside its own namespace.
 */
function namespaces(config: InstallConfig, environment: Environment): V1Namespace[] {
  const { sessions, host } = config;
  const { branch } = environment;
  const branched = branch ? { [BRANCH_LABEL]: branch } : {};
  return [
    ...(branch ? [{ apiVersion: "v1", kind: "Namespace", metadata: { name: environment.namespace, labels: { ...labels("alasio"), ...branched } } }] : []),
    ...(sessions.enabled && (sessions.createNamespace || branch)
      ? [{
        apiVersion: "v1",
        kind: "Namespace",
        metadata: {
          name: sessionsNamespace(config, environment),
          labels: {
            ...labels("session"),
            ...branched,
            ...(sandboxServiceRuns(config)
              ? { "pod-security.kubernetes.io/enforce": "privileged", "pod-security.kubernetes.io/audit": "restricted", "pod-security.kubernetes.io/audit-version": "latest" }
              : { "pod-security.kubernetes.io/enforce": "restricted", "pod-security.kubernetes.io/enforce-version": "latest" }),
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
 * Whether alasio removes the JuiceFS mount pods of a session whose mount is lost as it
 * restarts it (src/sandbox/index.ts): where it installs JuiceFS's driver, whose mount pods
 * it then knows to be in the driver's namespace.
 */
function mountPodsRemoved({ workspaceStorage }: InstallConfig): boolean {
  return workspaceStorage.enabled && workspaceStorage.csi.enabled;
}

/**
 * alasio's identity: it drives its workspaces' Sandboxes, their token Secrets, and reads
 * files from sessions through exec, in the namespaces it owns, labelling their pods busy
 * while turns run in them, as it does its own in its namespace (src/kube/busy.ts); where
 * it snapshots sessions, sets a restored session's readiness; where it forks
 * sessions, reads sessions' volume claims and runs the Jobs that clone them in its own;
 * where the node agents run, watches pods in its own, theirs among them; and, where it
 * installs JuiceFS's driver, lists and deletes pods in the driver's namespace, its mount
 * pods; and nothing else.
 */
function identity(config: InstallConfig, environment: Environment): KubernetesObject[] {
  const main = environment.branch === null;
  const binding = (metadata: V1ObjectMeta): V1RoleBinding => ({
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "RoleBinding",
    metadata,
    subjects: [{ kind: "ServiceAccount", name: RELEASE, namespace: environment.namespace }],
    roleRef: { apiGroup: "rbac.authorization.k8s.io", kind: "Role", name: RELEASE },
  });
  const inDriverNamespace = { name: RELEASE, namespace: config.workspaceStorage.csi.namespace, labels: labels("alasio") };
  const mountPods: V1Role = {
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "Role",
    metadata: inDriverNamespace,
    rules: [{ apiGroups: [""], resources: ["pods"], verbs: ["list", "delete"] }],
  };
  const inOwnNamespace = { name: RELEASE, namespace: environment.namespace, labels: labels("alasio") };
  const own: V1Role = {
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "Role",
    metadata: inOwnNamespace,
    rules: [
      // Its own pod's busy label; and each node's agent, found by its pod on the node.
      { apiGroups: [""], resources: ["pods"], verbs: main && nodeAgentRuns(config) ? ["list", "watch", "patch"] : ["list", "patch"] },
      ...(main && sessionsForked(config) ? [{ apiGroups: ["batch"], resources: ["jobs"], verbs: ["create", "get", "delete"] }] : []),
    ],
  };
  const serviceAccount: V1ServiceAccount = { apiVersion: "v1", kind: "ServiceAccount", metadata: { name: RELEASE, namespace: environment.namespace, labels: labels("alasio") } };
  return [
    serviceAccount,
    ...workspaceNamespaces(config, environment).flatMap((namespace): KubernetesObject[] => {
      const metadata = { name: RELEASE, namespace, labels: labels("alasio") };
      const forked = main && sessionsForked(config) && namespace === config.sessions.namespace;
      const snapshotted = main && nodeAgentRuns(config) && namespace === config.sessions.namespace;
      const role: V1Role = {
        apiVersion: "rbac.authorization.k8s.io/v1",
        kind: "Role",
        metadata,
        rules: [
          { apiGroups: ["agents.x-k8s.io"], resources: ["sandboxes"], verbs: ["get", "list", "watch", "create", "patch", "delete"] },
          { apiGroups: [""], resources: ["secrets"], verbs: ["get", "create"] },
          { apiGroups: [""], resources: ["pods"], verbs: ["get", "list", "patch"] },
          { apiGroups: [""], resources: ["pods/exec"], verbs: ["create", "get"] },
          ...(forked ? [{ apiGroups: [""], resources: ["persistentvolumeclaims"], verbs: ["get"] }] : []),
          // A restored session's readiness, once its sandbox is let run, and the network policies that must hold it first (src/sandbox/snapshots.ts).
          ...(snapshotted
            ? [{ apiGroups: [""], resources: ["pods/status"], verbs: ["patch"] }, { apiGroups: ["networking.k8s.io"], resources: ["networkpolicies"], verbs: ["list"] }]
            : []),
        ],
      };
      return [role, binding(metadata)];
    }),
    own,
    binding(inOwnNamespace),
    ...(main && mountPodsRemoved(config) ? [mountPods, binding(inDriverNamespace)] : []),
    ...(config.host.enabled ? hostAgentIdentity(config) : []),
  ];
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

/** The objects of the alasio of `environment`, main's unless a branch environment's, with what main's carries forward. */
export function alasioObjects(config: InstallConfig, environment: Environment = MAIN_ENVIRONMENT, carried: Carried = NOTHING_CARRIED): KubernetesObject[] {
  const templates = sandboxTemplates(config, environment, carried);
  const alasio = deployment(config, templates.checksum, carried.botChecksum, environment);
  return [
    ...namespaces(config, environment),
    ...identity(config, environment),
    templates.configMap,
    alasio,
    // It runs turns, in its harnesses, which a recreation would end.
    verticalPodAutoscaler(alasio, "Recommended"),
    // A drain waits for the turns running in its pod and its workspaces' to end.
    ...[environment.namespace, ...workspaceNamespaces(config, environment)].map(busyBudget),
    telemetryService(config, environment),
    ...(environment.branch === null && forksForBranches(config) ? [branchForksService()] : []),
    // What alasio may do beyond what RBAC can say; the cluster's, but for its Sandboxes', made with main's.
    ...(environment.branch === null ? [...busyLabelPolicy(), ...secretPolicy()] : []),
    // Where sessions are snapshotted, what alasio may set of a restored session's pod's status.
    ...(environment.branch === null && config.sessions.enabled && nodeAgentRuns(config)
      ? resumedConditionPolicy(componentName("resumed-condition"), alasioAccount(environment.namespace), config.sessions.namespace)
      : []),
    ...(environment.branch === null && sessionsForked(config) ? cloneJobPolicy(cloneJob(config)) : []),
    ...sandboxPolicy(
      sandboxPolicyName(environment.namespace),
      alasioAccount(environment.namespace),
      {
        sessions: config.sessions.enabled ? sessionsProfile(config, environment, carried) : null,
        previousSessions: config.sessions.enabled ? carried.previousSessions : null,
        host: config.host.enabled ? hostProfile(config) : null,
      },
    ),
  ];
}
