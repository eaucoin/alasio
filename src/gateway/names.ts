/**
 * What the sandbox gateway (./gateway.ts) and the session telemetry receiver
 * (../receiver/receiver.ts) are configured by: their variables, and the gateway's
 * configuration file. Free of anything but Effect's Schema and Node's own, so the command
 * line, which renders them, takes it without either.
 */
import { createHash } from "node:crypto";
import { posix } from "node:path";

import type { V1Container, V1PodSpec } from "@kubernetes/client-node";
import { Schema } from "effect";

import { keyHash } from "../binder/names.ts";
import { sanitizePathToken } from "../kube/sandbox-shape.ts";
import { ATTACH_ROOT } from "../node-agent/names.ts";

export { RESTORE_POD } from "../node-agent/names.ts";

/** The variable of the path of the gateway's configuration, a GatewayConfig as JSON. */
export const GATEWAY_CONFIG_VARIABLE = "ALASIO_GATEWAY_CONFIG";
/** The variables of the keys callers' tokens, and bindings', are signed with: the keys' text, as the stack's setup makes them. */
export const CALLER_KEY_VARIABLE = "CALLER_KEY";
export const BINDING_KEY_VARIABLE = "BINDING_KEY";

/**
 * The variables alasio reaches its environment's sessions through the gateway by: the gateway's
 * address, its environment's name, as its callers' tokens name it, and its environment's own key
 * of the gateway's caller key, which signs them (../binder/names.ts environmentKey).
 */
export const ALASIO_GATEWAY_URL_VARIABLE = "ALASIO_GATEWAY_URL";
export const ALASIO_GATEWAY_ENVIRONMENT_VARIABLE = "ALASIO_GATEWAY_ENVIRONMENT";
export const ALASIO_GATEWAY_CALLER_KEY_VARIABLE = "ALASIO_GATEWAY_CALLER_KEY";
/** The variable that says the gateway serves alasio's folder workspaces' instances too: "1" where it does. */
export const ALASIO_GATEWAY_FOLDERS_VARIABLE = "ALASIO_GATEWAY_FOLDERS";

/** The variables of where the gateway tells of events (../events/events.ts): the log's brokers, its principal's name, and the file of its password. */
export const EVENTS_BROKERS_VARIABLE = "EVENTS_BROKERS";
export const EVENTS_USERNAME_VARIABLE = "EVENTS_USERNAME";
export const EVENTS_PASSWORD_FILE_VARIABLE = "EVENTS_PASSWORD_FILE";

/** The variables of the receiver's port, and of main's sessions' namespace, beside which it takes branch environments' bindings. */
export const RECEIVER_PORT_VARIABLE = "RECEIVER_PORT";
export const SESSIONS_NAMESPACE_VARIABLE = "SESSIONS_NAMESPACE";
/** The variable of the folder namespace, whose folder conversations' instances' bindings the receiver takes too; empty where there are none. */
export const FOLDERS_NAMESPACE_VARIABLE = "FOLDERS_NAMESPACE";

/** The hash of `template`, of its JSON with every object's keys in order, as a pool's template records its own (cli/src/manifests/sandbox-gateway.ts poolTemplate). */
export function templateHash(template: object): string {
  const sorted = (_key: string, value: unknown): unknown =>
    typeof value === "object" && value !== null && !Array.isArray(value) ? Object.fromEntries(Object.entries(value).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))) : value;
  return createHash("sha256").update(JSON.stringify(template, sorted)).digest("hex");
}

/** The name of a restore's pod of the pool `pool`, by its random part, `suffix`, seven of base 36's digits, as RESTORE_POD matches it. */
export const restorePodName = (pool: string, suffix: string): string => `${pool}-restore-${suffix}`;

/** The volume of a pool's pod's own directory on its node, which its workspace is attached under (cli/src/manifests/sandbox-gateway.ts poolTemplate). */
export const ATTACH_VOLUME = "attach";

/**
 * The spec `spec`, a pool's template's, of a restore's pod named `name` in `namespace`: its own
 * directory on its node named by its volume's path, not by a subpath of the directory of every
 * pod's. A subpath is
 * bound into its container alone, without what is mounted beneath it, as kubelet binds one; a
 * volume's path is bound with it, so the workspace attached there before the pod is made is
 * there as its restore reopens the files it held open.
 */
export function ownDirectory(spec: V1PodSpec, namespace: string, name: string): V1PodSpec {
  const unsubpathed = (container: V1Container): V1Container => ({
    ...container,
    ...(container.volumeMounts
      ? {
        volumeMounts: container.volumeMounts.map((mount) => {
          if (mount.name !== ATTACH_VOLUME) return mount;
          const { subPathExpr: _subPath, ...own } = mount;
          return own;
        }),
      }
      : {}),
  });
  return {
    ...spec,
    containers: spec.containers.map(unsubpathed),
    ...(spec.initContainers ? { initContainers: spec.initContainers.map(unsubpathed) } : {}),
    // One there already, as its node's agent made it, its workspace attached; kubelet makes none, so the pod starts not where it is not there.
    ...(spec.volumes ? { volumes: spec.volumes.map((volume) => (volume.name === ATTACH_VOLUME ? { name: ATTACH_VOLUME, hostPath: { path: `${ATTACH_ROOT}/${namespace}/${name}`, type: "Directory" } } : volume)) } : {}),
  };
}

/** The pool of folder instances, by its name, in the folder namespace: its Leases' and its pods' label (./folders.ts). */
export const FOLDER_POOL = "folder";

/** What a folder instance's key begins with: `folder:<harness>:<conversation>` (./folders.ts). */
export const FOLDER_KEY_PREFIX = "folder:";

/** A harness's name, as a folder instance's key carries it. */
const HARNESS = /^[a-z][a-z0-9-]{0,30}$/u;

/** The key of the folder instance of the conversation `conversation` under the harness `harness`. */
export const folderKey = (harness: string, conversation: string): string => `${FOLDER_KEY_PREFIX}${harness}:${conversation}`;

/** The harness and conversation of the folder instance `key` is of; null where it is no folder instance's. */
export function folderOfKey(key: string): { readonly harness: string; readonly conversation: string } | null {
  if (!key.startsWith(FOLDER_KEY_PREFIX)) return null;
  const rest = key.slice(FOLDER_KEY_PREFIX.length);
  const at = rest.indexOf(":");
  const [harness, conversation] = [rest.slice(0, Math.max(at, 0)), rest.slice(at + 1)];
  return at > 0 && HARNESS.test(harness) && conversation.length > 0 && conversation.length <= 512 ? { harness, conversation } : null;
}

/**
 * Where the folder instance of `conversation` under `harness` keeps its state, under
 * `stateRoot`: bayma leases its state directory to a single server, and a Codex thread keeps
 * its server alive after the conversation switches to Claude, so the directory is keyed by
 * harness as well as conversation, as a folder's Sandbox kept it.
 */
export const folderStateDir = (stateRoot: string, harness: string, conversation: string): string =>
  posix.join(stateRoot, sanitizePathToken(harness), sanitizePathToken(conversation));

/** The name of a folder instance's pod of `key`, by its random part, `suffix`, five of base 36's digits, as FOLDER_POD matches it. */
export const folderPodName = (key: string, suffix: string): string => `bayma-${keyHash(key).slice(0, 16)}-${suffix}`;
export const FOLDER_POD = /^bayma-[0-9a-f]{16}-[0-9a-z]{5}$/u;

/** The Secret of a folder instance's pod `pod`'s binding's token, which its bayma takes requests with, mounted where FOLDER_TOKEN_FILE is. */
export const folderTokenSecret = (pod: string): string => `${pod}-bayma-token`;
export const FOLDER_TOKEN_VOLUME = "alasio-bayma-token";
export const FOLDER_TOKEN_DIRECTORY = "/run/alasio/bayma";
export const FOLDER_TOKEN_FILE = `${FOLDER_TOKEN_DIRECTORY}/token`;

/**
 * How long a folder instance's pod is given to stop: its bayma's CRIU dumps of its idle REPL
 * sessions, one at a time, each within bayma's own 120 s; a state of the box's size is dumped
 * in well under a minute (gate G8, program/13.4-folder-instances.md).
 */
export const FOLDER_STOP_SECONDS = 600;

/** How long a folder instance's release, or its next bind, waits for its pod to be gone, past its stop. */
export const FOLDER_GONE_MS = (FOLDER_STOP_SECONDS + 60) * 1000;
/** How long a folder instance's bind gives its pod to run, and its bayma, once it runs, to answer. */
export const FOLDER_START_MS = 120_000;
/** How long it gives its pod to run while its image is still being pulled, as a node that has never run it pulls it first. */
export const FOLDER_PULL_MS = 8 * 60_000;
/**
 * How long a folder instance's bind may take in all, given `pullMs` and `startMs`: a release
 * of it under way, or its pod before, gone first; then its pod run, its image pulled; then its
 * bayma answering. A bind of the key meanwhile waits as long for it.
 */
export const folderBindMs = (pullMs = FOLDER_PULL_MS, startMs = FOLDER_START_MS): number => FOLDER_GONE_MS + pullMs + startMs;

/**
 * The spec of a folder instance's pod, `pod`, of the folder template's `spec`: its bayma
 * given its binding's token, from its Secret, and its state directory, `stateDir`; stopped
 * within FOLDER_STOP_SECONDS; and never started again, as a bayma that ended is a binding to
 * release, not a container to start. Pure, as the gateway makes it and the folder pods' policy
 * pins it (cli/src/manifests/admission.ts folderPodPolicy).
 */
export function folderPodSpec(spec: V1PodSpec, pod: string, stateDir: string): V1PodSpec {
  return {
    ...spec,
    terminationGracePeriodSeconds: FOLDER_STOP_SECONDS,
    restartPolicy: "Never",
    containers: spec.containers.map((container) =>
      container.name !== "bayma" ? container : {
        ...container,
        args: [...(container.args ?? []), "--token-file", FOLDER_TOKEN_FILE, "--state-dir", stateDir],
        volumeMounts: [...(container.volumeMounts ?? []), { name: FOLDER_TOKEN_VOLUME, mountPath: FOLDER_TOKEN_DIRECTORY, readOnly: true }],
      }
    ),
    volumes: [...(spec.volumes ?? []), { name: FOLDER_TOKEN_VOLUME, secret: { secretName: folderTokenSecret(pod), defaultMode: 0o440 } }],
  };
}

/** A pod template, as the gateway's configuration holds one: an object, as the API server takes it. */
const Template = Schema.Record(Schema.String, Schema.Unknown);

/**
 * How main's sessions are snapshotted (./snapshots.ts), as alasio's own were: how often one whose
 * execs run is saved live, the gVisor releases a snapshot is restored under, each by its
 * RuntimeClass, the current one first, and each pool's template, by its sessions' internet, as
 * `alasio up` renders it now and as it rendered it before it last changed, which a snapshot taken
 * on it still restores onto, and nothing older.
 */
export const GatewaySnapshots = Schema.Struct({
  liveEverySeconds: Schema.Number,
  /** Each kept release, its RuntimeClass, and the checkpoint gofer a restore of a snapshot it took asks for, if any (../node-agent/names.ts goferOf). */
  releases: Schema.Array(Schema.Struct({
    release: Schema.String,
    runtimeClassName: Schema.String,
    gofer: Schema.NullOr(Schema.Struct({ transport: Schema.Literals(["socket", "tcp"]), pagesRetry: Schema.Boolean })),
  })),
  templates: Schema.Struct({
    none: Schema.Struct({ current: Template, previous: Schema.NullOr(Template) }),
    full: Schema.Struct({ current: Template, previous: Schema.NullOr(Template) }),
  }),
});
export type GatewaySnapshots = typeof GatewaySnapshots.Type;

/** The sandbox pods' policy, which holds every pod of a sessions' namespace to its pools' templates (cli/src/manifests/admission.ts sandboxPodPolicy). */
export const SANDBOX_POD_POLICY = "alasio-sandbox-pods";

/** The gateway's configuration, as `alasio up` renders it (cli/src/manifests/sandbox-gateway.ts). */
export const GatewayConfig = Schema.Struct({
  /** The port it serves on, and the host its replicas' headless Service resolves to, each at that port. */
  port: Schema.Number,
  peers: Schema.String,
  /** The port pool pods' bayma serves on. */
  podPort: Schema.Number,
  /** How long a bind waits for a free pod, and an idle pod is kept. */
  waitSeconds: Schema.Number,
  releaseAfterSeconds: Schema.Number,
  /** main's environment of sessions, by its name, and the namespace its pools are in; branch environments' are found (./environments.ts). */
  environment: Schema.Struct({ name: Schema.String, namespace: Schema.String }),
  /** The quota of a workspace the gateway makes, in bytes. */
  quotaBytes: Schema.Number,
  /** How main's sessions are snapshotted; none where they are not. */
  snapshots: Schema.NullOr(GatewaySnapshots),
  /** Each pool's name, by its sessions' internet. */
  pools: Schema.Struct({ none: Schema.String, full: Schema.String }),
  /** Where the node agents are. */
  nodeAgents: Schema.Struct({ namespace: Schema.String, selector: Schema.String, port: Schema.Number }),
  /** Where bound pods' bayma exports its telemetry, the session telemetry receiver's Service, by its name and port; none where none runs. */
  receiver: Schema.NullOr(Schema.Struct({ host: Schema.String, port: Schema.Number })),
  /**
   * Folder conversations' instances (./folders.ts): the namespace they run in, the template of
   * their pods, as `alasio up` renders it of the host profile, the directory under which each
   * keeps its state, and how long an idle one is kept; none where the installation has no
   * folder workspaces.
   */
  folders: Schema.NullOr(Schema.Struct({ namespace: Schema.String, template: Template, stateRoot: Schema.String, releaseAfterSeconds: Schema.Number })),
});
export type GatewayConfig = typeof GatewayConfig.Type;
