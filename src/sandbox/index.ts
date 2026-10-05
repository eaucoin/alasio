/**
 * Session filesystems: the empty, isolated workspace per session an operator may choose
 * instead of a folder. Each is an agent-sandbox Sandbox (../kube/sandboxes.ts) in the
 * sessions namespace, made from the deployment's `sessions` template, whose volume claim
 * is the workspace and whose pod runs bayma under the sandboxing runtime the template
 * names (gVisor by default). The harness runs in alasio and reaches the session through
 * one door, bayma, with the session's token; nothing of the harness, and no credential,
 * is ever inside a session.
 *
 * What a session may reach is NetworkPolicy's to enforce, by the labels set here: the
 * installation's policies admit alasio alone in, and let a session out to alasio's
 * telemetry receiver only ("none") or to the internet's public addresses too ("full").
 * DNS is set to match: public resolvers in "full", and none at all in "none", so a name
 * cannot carry anything out through the cluster's resolver.
 *
 * Policies reach a new pod asynchronously, so a session's pod first waits, in an init
 * container, until its egress is confined: until the cluster's API server, a private
 * address both modes refuse, no longer answers. Where NetworkPolicy is not enforced at
 * all a session therefore never starts, rather than starting open.
 */
import { createHash } from "node:crypto";
import type { LookupAddress } from "node:dns";
import { lookup } from "node:dns/promises";
import { mkdirSync } from "node:fs";
import { join } from "node:path";

import type { V1Pod } from "@kubernetes/client-node";
import { Config, Context, Duration, Effect, FiberSet, Layer, Option, RcRef, Schedule, Schema } from "effect";

import { type KubeApiError, KubeClient, type KubeExecError } from "../kube/client.ts";
import type { SessionsProfile } from "../kube/config.ts";
import {
  BAYMA_CONTAINER,
  type BaymaEndpoint,
  makeSandboxes,
  SANDBOX_API_VERSION,
  SANDBOX_KIND,
  type Sandbox,
  type SandboxError,
  type SandboxFault,
  sameToken,
  sandboxManifest,
  tokenSecretName,
} from "../kube/sandboxes.ts";
import { withLogScope } from "../shared/log.ts";
import { overLimitNote } from "../telegram/rich-media.ts";
import type { OtlpForwarder } from "../telemetry/forward.ts";
import { assertValidVolumeId, SessionToken } from "./names.ts";
import { sandboxBaymaTelemetryEnv, sandboxResource } from "./telemetry.ts";
import { serveTelemetryReceiver } from "./telemetry-receiver.ts";

/** A session's internet: none, or the internet's public addresses. */
export type NetMode = "none" | "full";

/** Where a session's bayma exports its telemetry: alasio's receiver, and the variables that say so. */
export interface SessionTelemetry {
  readonly endpoint: string;
  readonly env: Readonly<Record<string, string>>;
}

/** What sessionSandboxManifest is given. */
export interface SessionSandboxManifestOptions {
  readonly volumeId: string;
  readonly netMode: NetMode;
  readonly profile: SessionsProfile;
  readonly telemetry: SessionTelemetry | null;
}

/** A file read to attach to a reply: its bytes, or a note saying why it is not attached. */
export type FileRead =
  | { readonly bytes: Buffer; readonly note?: never }
  | { readonly note: string; readonly bytes?: never };

/** Sessions' telemetry could not be received: its forwarder or its receiver did not start, or the receiver's address is unknown. */
export class SessionTelemetryUnavailable extends Schema.TaggedError<SessionTelemetryUnavailable>()("SessionTelemetryUnavailable", {
  cause: Schema.Defect(),
}) {
  override get message(): string {
    return this.cause instanceof Error ? this.cause.message : String(this.cause);
  }
}

/** A session's lost mount could not be made anew: why. */
class SessionMountError extends Schema.TaggedError<SessionMountError>()("SessionMountError", {
  message: Schema.String,
}) {}

/** Reading a file in a session came to neither its bytes nor a reason it has none. */
export class SessionFileError extends Schema.TaggedError<SessionFileError>()("SessionFileError", {
  message: Schema.String,
}) {}

/** How making a session's Sandbox and reaching its bayma fails. */
export type SessionError = SandboxError | SessionTelemetryUnavailable;

/** A conversation's workspace is a session filesystem, and the deployment renders no sessions template. */
export class SessionFilesystemsDisabled extends Schema.TaggedError<SessionFilesystemsDisabled>()("SessionFilesystemsDisabled", {}) {
  override get message(): string {
    return "this conversation's workspace is a session filesystem, which this deployment does not enable";
  }
}

/** What SessionSandboxes is made with; see there. */
export interface SessionSandboxesOptions {
  readonly profile: SessionsProfile;
  readonly stateDir: string;
  /** The standard OpenTelemetry variables alasio exports its own telemetry with. */
  readonly env?: Readonly<NodeJS.ProcessEnv>;
  readonly createForwarder?: (env: Readonly<NodeJS.ProcessEnv>, warn: (message: string) => void) => Promise<OtlpForwarder>;
  readonly resolve?: (hostname: string) => Promise<LookupAddress>;
}

/**
 * What a lost FUSE mount answers, in the C locale: its client gone (ENOTCONN), its
 * connection aborted (ECONNABORTED), or its I/O failing (EIO).
 */
const BROKEN_MOUNT = /Transport endpoint is not connected|Software caused connection abort|Input\/output error/u;
/** How long a look at a session's workspace mount may take; a mount that hangs is not one a new pod mends. */
const MOUNT_CHECK_TIMEOUT: Duration.Input = "10 seconds";

/**
 * JuiceFS's mount pods, by the labels its CSI driver (v0.33.0, pkg/common/common.go)
 * gives them, and the label naming the volume one mounts, by its id (its volume handle).
 */
const MOUNT_POD_SELECTOR = "app.kubernetes.io/name=juicefs-mount";
const MOUNT_POD_VOLUME_LABEL = "volume-id";
/** How long a restarted session's pod is waited for to go, and how often it is looked at. */
const POD_GONE_TIMEOUT: Duration.Input = "2 minutes";
const POLL: Duration.Input = "1 second";

/**
 * Whether the annotation `key: value` of a mount pod is the driver's reference to a target
 * path it serves, `value`: "juicefs-" and the path's SHA-256, cut to 63 characters
 * (GetReferenceKey in pkg/util/util.go).
 */
function isTargetReference(key: string, value: string): boolean {
  return key === `juicefs-${createHash("sha256").update(value).digest("hex")}`.slice(0, 63);
}

/** The target paths a mount pod serves, as its annotations reference them. */
const targetsOf = (mountPod: V1Pod): string[] =>
  Object.entries(mountPod.metadata?.annotations ?? {}).filter(([key, value]) => isTargetReference(key, value)).map(([, value]) => value);

/** Whether a target path is one of `pod`'s volumes, under kubelet's directory of the pod. */
const ofPod = (pod: V1Pod) => (target: string): boolean => target.includes(`/pods/${pod.metadata?.uid}/volumes/`);

const NET_MODE_LABEL = "alasio.dev/net-mode";
const WORKLOAD_LABEL = "alasio.dev/workload";
const DEFAULT_FULL_MODE_NAMESERVERS = ["1.1.1.1", "8.8.8.8"];
// The token's environment variable in bayma's container, which the OTLP headers expand.
const TOKEN_ENV = "ALASIO_SANDBOX_TOKEN";

/** Where sessions' telemetry is received: the receiver's Service, when sessions export any, and its port. */
const ReceiverService = Config.String("ALASIO_TELEMETRY_RECEIVER_SERVICE").pipe(Config.map((service) => service.trim()), Config.withDefault(""));
const ReceiverPort = Config.Port("ALASIO_TELEMETRY_RECEIVER_PORT").pipe(Config.withDefault(4318));

/**
 * The egress gate's program: exits once three connections in a row to the API server
 * fail, and fails the pod if they still succeed after two minutes. Runs on the agent
 * image's Node.
 */
export const EGRESS_GATE_SCRIPT = [
  "const net = require('node:net');",
  "const host = process.env.KUBERNETES_SERVICE_HOST, port = Number(process.env.KUBERNETES_SERVICE_PORT);",
  "const deadline = Date.now() + 120000; let refused = 0;",
  "(function probe() {",
  "  const socket = net.connect({ host, port, timeout: 1000 });",
  "  const next = (blocked) => { socket.destroy(); refused = blocked ? refused + 1 : 0;",
  "    if (refused >= 3) process.exit(0);",
  "    if (Date.now() > deadline) { console.error('egress is not confined: is NetworkPolicy enforced in this cluster?'); process.exit(1); }",
  "    setTimeout(probe, 250); };",
  "  socket.once('connect', () => next(false)); socket.once('timeout', () => next(true)); socket.once('error', () => next(true));",
  "})();",
].join("\n");

/**
 * The Sandbox for a session on `volumeId` with internet mode `netMode`, from the
 * `profile` (ALASIO_KUBE_TEMPLATES `sessions`). `telemetry` is `{ endpoint, env }`, or
 * null when alasio exports none. Pure, for tests.
 */
export function sessionSandboxManifest({ volumeId, netMode, profile, telemetry }: SessionSandboxManifestOptions): Sandbox {
  const full = netMode === "full";
  return sandboxManifest({
    name: assertValidVolumeId(volumeId),
    namespace: profile.namespace,
    template: profile,
    labels: { [WORKLOAD_LABEL]: "session", [NET_MODE_LABEL]: full ? "full" : "none" },
    configure(spec, bayma) {
      if (telemetry) {
        bayma.env = [
          ...(bayma.env ?? []),
          { name: TOKEN_ENV, valueFrom: { secretKeyRef: { name: tokenSecretName(volumeId), key: "token" } } },
          ...Object.entries({ ...telemetry.env, OTEL_EXPORTER_OTLP_ENDPOINT: telemetry.endpoint }).map(([name, value]) => ({ name, value })),
          { name: "OTEL_EXPORTER_OTLP_HEADERS", value: `authorization=Bearer%20$(${TOKEN_ENV})` },
        ];
      }
      return {
        ...spec,
        // Nothing in a session speaks to the cluster as anyone, or learns its services.
        automountServiceAccountToken: false,
        enableServiceLinks: false,
        dnsPolicy: "None",
        dnsConfig: { nameservers: full ? [...(profile.fullModeNameservers ?? DEFAULT_FULL_MODE_NAMESERVERS)] : ["127.0.0.1"] },
        initContainers: [
          ...(profile.egressGate === false ? [] : [{
            name: "egress-gate",
            ...(bayma.image === undefined ? {} : { image: bayma.image }),
            command: ["node", "-e", EGRESS_GATE_SCRIPT],
            ...(bayma.securityContext === undefined ? {} : { securityContext: bayma.securityContext }),
            resources: { requests: { cpu: "10m", memory: "32Mi" }, limits: { memory: "64Mi" } },
          }]),
          ...(spec.initContainers ?? []),
        ],
      };
    },
  });
}

/** The forwarder sessions' telemetry is exported through, loaded only once it is needed. */
async function loadForwarder(env: Readonly<NodeJS.ProcessEnv>, warn: (message: string) => void): Promise<OtlpForwarder> {
  const { createOtlpForwarder } = await import("../telemetry/forward.ts");
  return createOtlpForwarder(env, { warn });
}

/**
 * Session filesystems, on the KubeClient, for a deployment that renders the `sessions`
 * template (../kube/config.ts); without one there is no such service, and callers offer
 * only folders. `stateDir` is alasio's state directory, under which each session's
 * harness directory lives. Sessions' telemetry is received, while the service lasts,
 * when alasio exports any (`env`'s standard OpenTelemetry variables) and
 * `ALASIO_TELEMETRY_RECEIVER_SERVICE` (and `_PORT`) say where.
 */
export class SessionSandboxes extends Context.Service<SessionSandboxes, {
  readonly volumes: {
    /** A new session's Sandbox, made now so its volume is ready by its first turn. */
    readonly create: (volumeId: string, netMode?: NetMode) => Effect.Effect<{ readonly volumeId: string; readonly netMode: NetMode }, SessionError>;
    readonly destroy: (volumeId: string) => Effect.Effect<void, KubeApiError>;
  };
  /**
   * The directory in alasio a session's harness runs in: empty, the session's own, and
   * none of the workspace's, which is only in the session. Its path keys the harness's
   * sessions to the workspace (Claude Code's project, Codex's thread list). Made as it
   * is asked for; synchronous, as making a directory is.
   */
  readonly harnessDirectory: (volumeId: string) => string;
  /**
   * Makes sure the session's Sandbox runs and bayma answers in it, resuming one that
   * was suspended; one that is gone is made again, empty, with no internet.
   */
  readonly ensureSession: (volumeId: string) => Effect.Effect<{ readonly bayma: BaymaEndpoint }, SessionError>;
  /**
   * A file from the session as its agent sees it: read by the agent's own user in its
   * container, relative to the workspace, so its path and symlinks reach only the
   * session's files. `{ bytes }`, or `{ note }` saying why not.
   */
  readonly readFile: (volumeId: string, path: string, maxBytes: number) => Effect.Effect<FileRead, KubeApiError | KubeExecError | SessionFileError>;
}>()("alasio/sandbox/SessionSandboxes") {
  static readonly layer = (options: SessionSandboxesOptions): Layer.Layer<SessionSandboxes, Config.ConfigError, KubeClient> =>
    Layer.effect(SessionSandboxes, makeSessionSandboxes(options));
}

const makeSessionSandboxes = Effect.fnUntraced(function*({
  profile,
  stateDir,
  env = process.env,
  createForwarder = loadForwarder,
  resolve = lookup,
}: SessionSandboxesOptions) {
  const kube = yield* KubeClient;

  /**
   * The ids of the volumes JuiceFS's mount pods in `mountPodNamespace` serve `pod`, as
   * their references to its target paths say. Looked up while the pod runs: the driver
   * drops a pod's references as the pod goes.
   */
  const servedVolumes = (mountPodNamespace: string, pod: V1Pod): Effect.Effect<readonly string[], KubeApiError> =>
    kube.list("v1", "Pod", mountPodNamespace, MOUNT_POD_SELECTOR).pipe(
      // Pods, as the API server lists them.
      Effect.map((mountPods) =>
        [...new Set((mountPods as readonly V1Pod[]).filter((mountPod) => targetsOf(mountPod).some(ofPod(pod))).flatMap(({ metadata }) => metadata?.labels?.[MOUNT_POD_VOLUME_LABEL] ?? []))]
      ),
    );

  /**
   * Removes the mount pods of `volumes` that served the session's pod `pod` on its node,
   * once that pod is gone, and serve no other pod: the driver then makes the volume's
   * mount anew, in a mount pod of its own, as the session's next pod is started on it,
   * rather than reusing the broken one or one made in its place on its mount point. What
   * is removed, or that nothing is, is logged; a removal that fails is logged and leaves
   * the session to resume as it is.
   */
  const removeMountPods = (mountPodNamespace: string, volumeId: string, pod: V1Pod, volumes: readonly string[]): Effect.Effect<void> =>
    Effect.gen(function*() {
      yield* kube.read("v1", "Pod", profile.namespace, volumeId).pipe(
        Effect.repeat({ schedule: Schedule.spaced(POLL), while: (current) => current?.metadata?.uid === pod.metadata?.uid }),
        Effect.timeoutOrElse({ duration: POD_GONE_TIMEOUT, orElse: () => Effect.fail(new SessionMountError({ message: `the pod of session ${volumeId} is not gone` })) }),
      );
      const listed = yield* Effect.forEach(volumes, (volume) => kube.list("v1", "Pod", mountPodNamespace, `${MOUNT_POD_SELECTOR},${MOUNT_POD_VOLUME_LABEL}=${volume}`));
      // Pods, as the API server lists them.
      const broken = (listed.flat() as V1Pod[]).filter((mountPod) =>
        mountPod.spec?.nodeName === pod.spec?.nodeName && !mountPod.metadata?.deletionTimestamp && targetsOf(mountPod).every(ofPod(pod))
      );
      if (broken.length === 0) {
        yield* Effect.logInfo(`found no JuiceFS mount pod of session ${volumeId}'s volume to remove; resuming it`);
        return;
      }
      for (const { metadata } of broken) {
        yield* kube.remove("v1", "Pod", mountPodNamespace, metadata?.name ?? "");
        yield* Effect.logInfo(`removed JuiceFS mount pod ${mountPodNamespace}/${metadata?.name} of volume ${metadata?.labels?.[MOUNT_POD_VOLUME_LABEL]}, whose mount session ${volumeId} lost`);
      }
    }).pipe(
      Effect.catch((error) => Effect.logWarning(`could not remove the JuiceFS mount pods of session ${volumeId}'s volume: ${error.message}; resuming it`)),
    );

  /**
   * What is wrong with a running session's workspace, or null: gVisor does not follow
   * mount propagation, so a JuiceFS mount lost and made anew under a session is never seen
   * in it again, and only a new pod mends it, on a mount made anew (removeMountPods). One
   * exec, reading the workspace's file system statistics in bayma's container; a look that
   * cannot be taken is logged, and finds nothing.
   */
  const brokenMount = (volumeId: string): Effect.Effect<SandboxFault | null> =>
    kube.exec(profile.namespace, volumeId, BAYMA_CONTAINER, ["env", "LC_ALL=C", "stat", "--file-system", "--format=%T", profile.workspaceDir]).pipe(
      Effect.timeout(MOUNT_CHECK_TIMEOUT),
      Effect.flatMap(({ exitCode, stderr }): Effect.Effect<SandboxFault | null> => {
        if (exitCode === 0) return Effect.succeed(null);
        const broken = BROKEN_MOUNT.exec(stderr)?.[0];
        if (broken) return lostMount(volumeId, `workspace mount is broken (${broken})`);
        return Effect.logWarning(`could not look at the workspace mount of session ${volumeId} (exit ${exitCode}): ${stderr.trim()}`).pipe(Effect.as(null));
      }),
      Effect.catch((error) => Effect.logWarning(`could not look at the workspace mount of session ${volumeId}: ${error.message}`).pipe(Effect.as(null))),
    );

  /** The fault of a session whose mount is lost: `why`, and, where alasio's JuiceFS driver runs, its mount pods removed before it resumes. */
  const lostMount = (volumeId: string, why: string): Effect.Effect<SandboxFault> => {
    const mountPodNamespace = profile.mountPodNamespace;
    if (!mountPodNamespace) return Effect.succeed({ why, beforeResume: Effect.void });
    return Effect.gen(function*() {
      // A core Pod, as the API server returns one.
      const pod = (yield* kube.read("v1", "Pod", profile.namespace, volumeId)) as V1Pod | null;
      const volumes = pod ? yield* servedVolumes(mountPodNamespace, pod) : [];
      return { why, beforeResume: pod ? removeMountPods(mountPodNamespace, volumeId, pod, volumes) : Effect.void };
    }).pipe(
      Effect.catch((error) =>
        Effect.logWarning(`could not find the JuiceFS mount pods of session ${volumeId}'s volume: ${error.message}`).pipe(Effect.as({ why, beforeResume: Effect.void }))
      ),
    );
  };

  const sandboxes = yield* makeSandboxes({ namespace: profile.namespace, port: profile.port, fault: brokenMount });
  const telemetryEnv = sandboxBaymaTelemetryEnv(env);
  const receiverPort = yield* ReceiverPort;
  const receiverService = yield* ReceiverService;
  const exportsTelemetry = Object.keys(telemetryEnv).length > 0 && Boolean(receiverService);
  if (Object.keys(telemetryEnv).length > 0 && !receiverService) {
    yield* Effect.logWarning("sessions' telemetry is not received: ALASIO_TELEMETRY_RECEIVER_SERVICE is not set");
  }
  // The forwarder's warnings, which it gives as it is made, logged here.
  const runFork = yield* FiberSet.makeRuntime();
  const warn = (message: string): void => {
    runFork(Effect.logWarning(message).pipe(withLogScope("sandbox")));
  };

  /**
   * The receiver, and the forwarder it exports through, started once they are first
   * needed and kept until the service stops; a start that fails is tried again by the
   * next session that needs them. Sessions keep running past alasio, and their exporters
   * retry what they could not send meanwhile.
   */
  const receiving = yield* RcRef.make({
    acquire: Effect.gen(function*() {
      const forwarder = yield* Effect.acquireRelease(
        Effect.tryPromise({ try: () => createForwarder(env, warn), catch: (cause) => new SessionTelemetryUnavailable({ cause }) }),
        (forwarder) => Effect.sync(() => forwarder.close()),
      );
      yield* serveTelemetryReceiver({
        port: receiverPort,
        forwarder,
        stampFor: (volumeId) => sandboxResource(volumeId, env),
        authenticate: (token) =>
          Option.match(Schema.decodeUnknownOption(SessionToken)(token), {
            onNone: () => Effect.succeed(null),
            onSome: ([volumeId]) =>
              sandboxes.token(volumeId).pipe(
                Effect.map((expected) => (expected && sameToken(token, expected) ? volumeId : null)),
                Effect.orElseSucceed(() => null),
              ),
          }),
      }).pipe(Effect.mapError((cause) => new SessionTelemetryUnavailable({ cause })));
    }),
    idleTimeToLive: Duration.infinity,
  });
  const receive = Effect.scoped(RcRef.get(receiving));

  // Sessions that outlived the last alasio export as soon as this one is up.
  if (exportsTelemetry) {
    yield* receive.pipe(
      Effect.catchTag("SessionTelemetryUnavailable", (error) => Effect.logWarning(`cannot receive sessions' telemetry: ${error.message}`)),
      Effect.forkScoped,
    );
  }

  /** Where a session's bayma exports, by IP since "none" has no DNS; null when alasio exports nothing. */
  const telemetry: Effect.Effect<SessionTelemetry | null, SessionTelemetryUnavailable> = exportsTelemetry
    ? receive.pipe(
      Effect.andThen(Effect.tryPromise({ try: () => resolve(receiverService), catch: (cause) => new SessionTelemetryUnavailable({ cause }) })),
      Effect.map(({ address }) => ({ endpoint: `http://${address}:${receiverPort}`, env: telemetryEnv })),
    )
    : Effect.succeed(null);

  /** Makes the Sandbox, with what it is made from resolved first. */
  const ensure = (volumeId: string, netMode: NetMode): Effect.Effect<BaymaEndpoint, SessionError> =>
    telemetry.pipe(
      Effect.map((telemetry) => sessionSandboxManifest({ volumeId, netMode, profile, telemetry })),
      Effect.flatMap((made) => sandboxes.ensure(volumeId, () => made)),
    );

  /** The internet a session was made with, as its Sandbox says; none for one not made, as a session is by default. */
  const netModeOf = (volumeId: string): Effect.Effect<NetMode, KubeApiError> =>
    kube.read(SANDBOX_API_VERSION, SANDBOX_KIND, profile.namespace, volumeId).pipe(
      Effect.map((sandbox) => (sandbox?.metadata?.labels?.[NET_MODE_LABEL] === "full" ? "full" : "none")),
    );

  const readFile = Effect.fnUntraced(function*(volumeId: string, path: string, maxBytes: number): Effect.fn.Return<FileRead, KubeApiError | KubeExecError | SessionFileError> {
    // A Sandbox, as the API server returns one.
    const sandbox = (yield* kube.read(SANDBOX_API_VERSION, SANDBOX_KIND, profile.namespace, assertValidVolumeId(volumeId))) as Sandbox | null;
    if (!sandbox || sandbox.spec?.operatingMode === "Suspended") return { note: "the session is not running" };
    const script = 'cd "$3" || exit 3; f="$1"; [ -f "$f" ] || exit 3; s=$(stat -L -c %s -- "$f") || exit 3; [ "$s" -le "$2" ] || { printf %s "$s" >&2; exit 4; }; exec cat -- "$f"';
    const { exitCode, stdout, stderr } = yield* kube.exec(
      profile.namespace,
      volumeId,
      BAYMA_CONTAINER,
      ["sh", "-c", script, "sh", path, String(maxBytes), profile.workspaceDir],
      { maxBytes: maxBytes + 1 },
    );
    if (exitCode === 0) return { bytes: stdout };
    if (exitCode === 3) return { note: "file not found" };
    if (exitCode === 4) return { note: overLimitNote(Number(stderr.trim()), maxBytes) };
    return yield* new SessionFileError({ message: `reading ${path} in session ${volumeId} failed (exit ${exitCode}): ${stderr.trim()}` });
  });

  return SessionSandboxes.of({
    volumes: {
      create: (volumeId, netMode = "none") =>
        Effect.suspend(() => ensure(volumeId, netMode)).pipe(
          Effect.andThen(Effect.logInfo(`created session ${volumeId} (${netMode} internet)`)),
          Effect.as({ volumeId, netMode }),
          withLogScope("sandbox"),
        ),
      destroy: (volumeId) => Effect.suspend(() => sandboxes.remove(assertValidVolumeId(volumeId))),
    },

    harnessDirectory: (volumeId) => {
      const directory = join(stateDir, "sessionfs", "workspaces", assertValidVolumeId(volumeId));
      mkdirSync(directory, { recursive: true });
      return directory;
    },

    // Brought up as it was made: a pod of another template would be replaced by this one.
    ensureSession: (volumeId) =>
      Effect.suspend(() => {
        const id = assertValidVolumeId(volumeId);
        return netModeOf(id).pipe(Effect.flatMap((netMode) => ensure(id, netMode)));
      }).pipe(Effect.map((bayma) => ({ bayma }))),

    readFile,
  });
}, withLogScope("sandbox"));
