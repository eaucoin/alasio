/**
 * Session filesystems: the empty, isolated workspace per session an operator may choose
 * instead of a folder. Each is a directory of workspace storage of its own, of a quota of its
 * own, made by a node agent (../node-agent/workspaces.ts), whose sandbox is a pod of the
 * sandbox service's pools of its internet, bound to it while it has work and released once it
 * has not (../gateway/sessions.ts). The harness runs in alasio and reaches the session through
 * one door, bayma, through the gateway, with a caller's token of the session's, which alasio
 * mints with the gateway's caller key (../binder/tokens.ts): alasio neither learns nor reaches
 * the pod it is bound to. Nothing of the harness, and no credential, is ever inside a session.
 *
 * A session's internet is what it was made with, as alasio's store records it, which its
 * token names to the gateway, beside the environment its pools are of.
 *
 * A session's files are read, as for a reply's media, and written, as the files the operator
 * sends, through the node agents, which reach them on workspace storage by their paths from
 * the workspace's root alone, whether or not it is bound, never through a symlink.
 *
 * A session is forked by its directory cloned into the fork's, by JuiceFS's own command
 * through a node agent, its quota its own: its files, and nothing of the source's processes,
 * which runs on as it was. A branch environment's alasio (../branch/fork.ts) has sessions of
 * its own, in its own sessions' directory, and none of its parent's: a session its
 * conversations inherited is forked from the parent's, by the parent, under the same id, as it
 * is first brought up here.
 *
 * While a turn runs on a session, its binding is asked after now and then, through the
 * gateway, binding nothing, so the gateway, which releases a binding its bayma has run nothing
 * of for a while, keeps it while the turn thinks between its calls.
 */
import { mkdirSync, readFileSync } from "node:fs";
import { join, posix } from "node:path";

import { Clock, Config, Context, Duration, Effect, Layer, Schedule, Schema, type Scope } from "effect";

import { NO_BIND_HEADER } from "../binder/server.ts";
import { mintToken } from "../binder/tokens.ts";
import type { KubeClient } from "../kube/client.ts";
import type { SessionsProfile } from "../kube/config.ts";
import type { BaymaEndpoint } from "../kube/sandboxes.ts";
import { withLogScope } from "../shared/log.ts";
import { overLimitNote } from "../telegram/rich-media.ts";
import { assertValidVolumeId, sessionKey } from "./names.ts";
import { makeNodeAgents, type NodeAgentError } from "./node-agents.ts";

/** A session's internet: none, or the internet's public addresses. */
export type NetMode = "none" | "full";

/** A file read to attach to a reply: its bytes, or a note saying why it is not attached. */
export type FileRead =
  | { readonly bytes: Buffer; readonly note?: never }
  | { readonly note: string; readonly bytes?: never };

/** A session's sandbox service could not be asked, or refused: the gateway, or the node agents, and why. */
export class SessionUnavailable extends Schema.TaggedError<SessionUnavailable>()("SessionUnavailable", {
  message: Schema.String,
}) {}

/** Reading or writing a file in a session came to neither what it was asked nor a reason it could not. */
export class SessionFileError extends Schema.TaggedError<SessionFileError>()("SessionFileError", {
  message: Schema.String,
}) {}

/**
 * A branch environment's session could not be forked from its parent's as it was first
 * brought up: why, as the operator is told.
 */
export class SessionInheritError extends Schema.TaggedError<SessionInheritError>()("SessionInheritError", {
  message: Schema.String,
}) {}

/** How making a session and reaching its bayma fails. */
export type SessionError = SessionUnavailable | SessionInheritError;

/** A session could not be forked: why. */
export class SessionForkError extends Schema.TaggedError<SessionForkError>()("SessionForkError", {
  message: Schema.String,
}) {}

/** A session could not be made as a fork: there is one of its id there already, as a fork made before. */
export class SessionExists extends Schema.TaggedError<SessionExists>()("SessionExists", {
  message: Schema.String,
}) {}

/** A conversation's workspace is a session filesystem, and the deployment renders no sessions profile. */
export class SessionFilesystemsDisabled extends Schema.TaggedError<SessionFilesystemsDisabled>()("SessionFilesystemsDisabled", {}) {
  override get message(): string {
    return "this conversation's workspace is a session filesystem, which this deployment does not enable";
  }
}

/**
 * How a branch environment's alasio comes by a session it inherited (../branch/fork.ts),
 * as it is first brought up here.
 */
export interface Inheritance {
  /**
   * Forks the session `volumeId` of the alasio it was branched from into its own sessions:
   * whether it did, which it does not for a session that one never had, made again empty
   * here as a session that is gone is.
   */
  readonly fork: (volumeId: string) => Effect.Effect<boolean, SessionInheritError>;
  /** Records a session so forked as made, with its internet, as alasio records those it makes. */
  readonly record: (volumeId: string, netMode: NetMode) => Effect.Effect<void, SessionInheritError>;
}

/** What SessionSandboxes is made with; see there. */
export interface SessionSandboxesOptions {
  readonly profile: SessionsProfile;
  readonly stateDir: string;
  /** The internet a session was made with, as alasio's store records it; null for one it has no record of. */
  readonly netModeOf: (volumeId: string) => Effect.Effect<NetMode | null, SessionUnavailable>;
  /** Where alasio is a branch environment, how it comes by the sessions it inherited. */
  readonly inherit?: Inheritance | undefined;
}

/** How long a caller's token alasio mints lasts: past any turn, which mints one anew. */
const TOKEN_LIFETIME = Duration.hours(12);
/** How long a bind is waited for, the gateway's own wait with room, a cold pool's pod among it. */
const BIND_WAIT = Duration.minutes(3);
/** How often a session a turn runs on is asked after, well within any wait of the gateway's to release it. */
const HOLD_EVERY = Duration.seconds(60);
/** How long a call of the gateway's may take, but a bind. */
const GATEWAY_CALL = Duration.seconds(30);

/**
 * `path`, of a file as a session's agent names it, as the node agents name it: under its
 * workspace (`workspaceDir`) or its home, `workspace/...` or `home/...`; relative, of its
 * workspace. Null where it names neither.
 */
export function sessionFilePath(path: string, workspaceDir: string, home = "/home/agent"): string | null {
  const absolute = posix.normalize(path.startsWith("/") ? path : posix.join(workspaceDir, path));
  for (const [root, under] of [[workspaceDir, "workspace"], [home, "home"]] as const) {
    if (absolute.startsWith(`${root}/`)) return `${under}/${absolute.slice(root.length + 1)}`;
  }
  return null;
}

/** A node agent's refusal, as the status it answered with; null for one that did not answer. */
const statusOf = (error: NodeAgentError): number | null => error.status ?? null;

export class SessionSandboxes extends Context.Service<SessionSandboxes, {
  readonly volumes: {
    /** A new session's workspace, made now, of no internet unless told. */
    readonly create: (volumeId: string, netMode?: NetMode) => Effect.Effect<{ readonly volumeId: string; readonly netMode: NetMode }, SessionError>;
    /**
     * A new session `volumeId`, in the sessions' of `namespace` (this alasio's unless told),
     * with the internet of the session `sourceVolumeId` and a clone of its files, as the
     * module says. A fork that fails leaves no session.
     */
    readonly fork: (sourceVolumeId: string, volumeId: string, namespace?: string) => Effect.Effect<{ readonly volumeId: string; readonly netMode: NetMode }, SessionError | SessionForkError | SessionExists>;
    /** Deletes the session: its binding released, its directory and quota gone. */
    readonly destroy: (volumeId: string) => Effect.Effect<void, SessionUnavailable>;
    /** The sessions whose workspaces there are, of this alasio's environment. */
    readonly listed: Effect.Effect<readonly string[], SessionUnavailable>;
  };
  /**
   * The directory in alasio a session's harness runs in: empty, the session's own, and
   * none of the workspace's, which is only in the session. Its path keys the harness's
   * sessions to the workspace (Claude Code's project, Codex's thread list). Made as it
   * is asked for; synchronous, as making a directory is.
   */
  readonly harnessDirectory: (volumeId: string) => string;
  /**
   * Makes sure the session's workspace is there and bound, bayma serving in its pod: the
   * gateway's door to it, with a caller's token of its own. One that is gone is made again,
   * empty, with no internet.
   */
  readonly ensureSession: (volumeId: string) => Effect.Effect<{ readonly bayma: BaymaEndpoint }, SessionError>;
  /** A file of the session, as its agent names it: `{ bytes }`, or `{ note }` saying why not. */
  readonly readFile: (volumeId: string, path: string, maxBytes: number) => Effect.Effect<FileRead, SessionFileError>;
  /** Writes `bytes` as the session's file at `path`, as its agent names it, its directories made, its agent's. */
  readonly writeFile: (volumeId: string, path: string, bytes: Uint8Array) => Effect.Effect<void, SessionFileError>;
  /** Holds the session as one a turn runs on, for as long as the scope lasts, as the module says. */
  readonly holding: (volumeId: string) => Effect.Effect<void, never, Scope.Scope>;
}>()("alasio/sandbox/SessionSandboxes") {
  static readonly layer = (options: SessionSandboxesOptions): Layer.Layer<SessionSandboxes, Config.ConfigError, KubeClient> =>
    Layer.effect(SessionSandboxes, makeSessionSandboxes(options));
}

const makeSessionSandboxes = Effect.fnUntraced(function*({ profile, stateDir, netModeOf, inherit }: SessionSandboxesOptions) {
  const agents = yield* makeNodeAgents(profile.agents);
  const callerKey = Buffer.from(readFileSync(profile.gateway.callerKeyFile, "utf8").trim());
  const refOf = (volumeId: string, namespace = profile.namespace) => ({ environment: namespace, workspace: assertValidVolumeId(volumeId) });
  const unavailable = (error: { readonly message: string }) => new SessionUnavailable({ message: error.message });

  /** A caller's token of the session `volumeId`, of `netMode`, as the gateway takes one. */
  const tokenOf = (volumeId: string, netMode: NetMode) =>
    Effect.map(Clock.currentTimeMillis, (now) =>
      mintToken(callerKey, { key: sessionKey(volumeId), expires: now + Duration.toMillis(TOKEN_LIFETIME), options: { environment: profile.environment, mode: netMode } }));

  /** What the gateway answers `method path` with of the session, by its caller's token, binding nothing where `unbound`. */
  const gateway = (method: string, path: string, token: string, timeout: Duration.Duration, unbound = false) =>
    Effect.tryPromise({
      try: async (signal) => {
        const response = await fetch(`${profile.gateway.url}${path}`, {
          method,
          headers: { authorization: `Bearer ${token}`, ...(unbound ? { [NO_BIND_HEADER]: "never" } : {}) },
          signal: AbortSignal.any([signal, AbortSignal.timeout(Duration.toMillis(timeout))]),
        });
        await response.arrayBuffer();
        return response.status;
      },
      catch: (cause) => new SessionUnavailable({ message: `the sandbox gateway could not be asked to ${method} ${path}: ${cause instanceof Error ? cause.message : String(cause)}` }),
    });

  /** Binds the session through the gateway, as long as BIND_WAIT, its pool's waits again each time it says to. */
  const bind = (volumeId: string, token: string) =>
    gateway("POST", "/bind", token, GATEWAY_CALL).pipe(
      Effect.flatMap((status) =>
        status === 204 ? Effect.void : Effect.fail(new SessionUnavailable({ message: `the sandbox gateway did not bind the session ${volumeId}: it answered ${status}` }))
      ),
      Effect.retry({ schedule: Schedule.spaced(Duration.seconds(1)), while: (error) => !/answered 403/u.test(error.message) }),
      Effect.timeoutOrElse({ duration: BIND_WAIT, orElse: () => Effect.fail(new SessionUnavailable({ message: `the session ${volumeId} was not bound within ${Duration.format(BIND_WAIT)}` })) }),
    );

  /** Makes the session's workspace, as a node agent does: again, as it was. */
  const make = (volumeId: string, namespace = profile.namespace) => agents.createWorkspace(refOf(volumeId, namespace), profile.quotaBytes).pipe(Effect.mapError(unavailable));

  /**
   * A session there is no workspace of, forked from the parent's where alasio is a branch
   * environment and the parent has it, and recorded as made.
   */
  const inherited = (volumeId: string): Effect.Effect<void, SessionError> =>
    inherit
      ? agents.quota(refOf(volumeId)).pipe(
        Effect.mapError(unavailable),
        Effect.flatMap((there) => (there ? Effect.succeed(false) : inherit.fork(volumeId))),
        Effect.flatMap((forked) =>
          forked
            ? netModeOf(volumeId).pipe(
              Effect.flatMap((netMode) => inherit.record(volumeId, netMode ?? "none")),
              Effect.andThen(Effect.logInfo(`forked session ${volumeId} from the alasio this branch was branched from`)),
            )
            : Effect.void
        ),
      )
      : Effect.void;

  const ensureSession = (volumeId: string): Effect.Effect<{ readonly bayma: BaymaEndpoint }, SessionError> =>
    Effect.gen(function*() {
      yield* inherited(assertValidVolumeId(volumeId));
      // Of one gone, or never made, made again, empty.
      if (!(yield* agents.quota(refOf(volumeId)).pipe(Effect.mapError(unavailable)))) yield* make(volumeId);
      const token = yield* tokenOf(volumeId, (yield* netModeOf(volumeId)) ?? "none");
      yield* bind(volumeId, token);
      return { bayma: { url: `${profile.gateway.url}/mcp`, headers: { authorization: `Bearer ${token}` } } };
    });

  /** The file at `path` of the session, as the node agents name it, refused where its agent names no file of the session's. */
  const pathOf = (volumeId: string, path: string) =>
    Effect.suspend(() => {
      const named = sessionFilePath(path, profile.workspaceDir);
      return named ? Effect.succeed(named) : Effect.fail(new SessionFileError({ message: `${path} is no file of session ${volumeId}'s workspace or home` }));
    });

  const readFile = (volumeId: string, path: string, maxBytes: number): Effect.Effect<FileRead, SessionFileError> =>
    Effect.gen(function*() {
      const named = yield* pathOf(volumeId, path);
      return yield* agents.readFile(refOf(volumeId), named, maxBytes).pipe(
        Effect.map((bytes): FileRead => ({ bytes: Buffer.from(bytes) })),
        Effect.catchTag("NodeAgentError", (error): Effect.Effect<FileRead, SessionFileError> => {
          const status = statusOf(error);
          if (status === 404 || status === 403) return Effect.succeed({ note: "file not found" });
          if (status === 413) return Effect.succeed({ note: overLimitNote(Number(/ is (\d+) bytes/u.exec(error.reason)?.[1] ?? maxBytes + 1), maxBytes) });
          return Effect.fail(new SessionFileError({ message: `reading ${path} in session ${volumeId} failed: ${error.message}` }));
        }),
        Effect.catchTag("KubeApiError", (error) => Effect.fail(new SessionFileError({ message: `reading ${path} in session ${volumeId} failed: ${error.message}` }))),
      );
    });

  const writeFile = (volumeId: string, path: string, bytes: Uint8Array): Effect.Effect<void, SessionFileError> =>
    Effect.flatMap(pathOf(volumeId, path), (named) =>
      agents.writeFile(refOf(volumeId), named, bytes).pipe(
        Effect.mapError((error) => new SessionFileError({ message: `writing ${path} in session ${volumeId} failed: ${error.message}` })),
      ));

  const fork = Effect.fnUntraced(function*(sourceVolumeId: string, volumeId: string, namespace: string): Effect.fn.Return<{ readonly volumeId: string; readonly netMode: NetMode }, SessionError | SessionForkError | SessionExists> {
    const netMode = (yield* netModeOf(sourceVolumeId)) ?? "none";
    yield* agents.cloneWorkspace(refOf(sourceVolumeId), refOf(volumeId, namespace), profile.quotaBytes).pipe(
      Effect.mapError((error): SessionForkError | SessionExists | SessionUnavailable => {
        const status = error._tag === "NodeAgentError" ? statusOf(error) : null;
        if (status === 404) return new SessionForkError({ message: `there is no session ${sourceVolumeId} to fork` });
        if (status === 409) return new SessionExists({ message: `there is a session ${volumeId} in ${namespace} already` });
        return unavailable(error);
      }),
    );
    yield* Effect.logInfo(`forked session ${sourceVolumeId} into ${namespace}/${volumeId} (${netMode} internet)`);
    return { volumeId, netMode };
  });

  const destroy = (volumeId: string): Effect.Effect<void, SessionUnavailable> =>
    Effect.gen(function*() {
      const token = yield* tokenOf(volumeId, (yield* netModeOf(volumeId)) ?? "none");
      const released = yield* gateway("POST", "/release", token, BIND_WAIT);
      if (released !== 204) return yield* new SessionUnavailable({ message: `the sandbox gateway did not release the session ${volumeId}: it answered ${released}` });
      yield* agents.removeWorkspace(refOf(volumeId)).pipe(Effect.mapError(unavailable));
    });

  /** Asks the gateway after the session, binding nothing, as the module says; one that fails is said, and asked again at the next. */
  const askAfter = (volumeId: string) =>
    Effect.gen(function*() {
      const token = yield* tokenOf(volumeId, (yield* netModeOf(volumeId)) ?? "none");
      yield* gateway("GET", "/status", token, GATEWAY_CALL, true);
    }).pipe(Effect.catch((error) => Effect.logDebug(`asking after session ${volumeId} failed: ${error.message}`)));

  return SessionSandboxes.of({
    volumes: {
      create: (volumeId, netMode = "none") =>
        make(volumeId).pipe(
          Effect.andThen(Effect.logInfo(`created session ${volumeId} (${netMode} internet)`)),
          Effect.as({ volumeId, netMode }),
          withLogScope("sandbox"),
        ),
      fork: (sourceVolumeId, volumeId, namespace = profile.namespace) => fork(sourceVolumeId, volumeId, namespace).pipe(withLogScope("sandbox")),
      destroy: (volumeId) => destroy(volumeId).pipe(withLogScope("sandbox")),
      listed: agents.workspaces(profile.namespace).pipe(Effect.mapError(unavailable)),
    },
    harnessDirectory: (volumeId) => {
      const directory = join(stateDir, "sessionfs", "workspaces", assertValidVolumeId(volumeId));
      mkdirSync(directory, { recursive: true });
      return directory;
    },
    ensureSession: (volumeId) => ensureSession(volumeId).pipe(withLogScope("sandbox")),
    readFile,
    writeFile,
    holding: (volumeId) => Effect.asVoid(Effect.forkScoped(Effect.repeat(askAfter(volumeId), Schedule.spaced(HOLD_EVERY)))),
  });
}, withLogScope("sandbox"));
