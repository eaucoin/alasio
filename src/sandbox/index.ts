/**
 * Session filesystems: the empty, isolated workspace per session an operator may choose
 * instead of a folder. Each is a workspace of the sandbox gateway's (../gateway/sessions.ts),
 * of alasio's environment, reached through it alone (./gateway-client.ts): made, forked and
 * deleted by it, and bound by it to a pod of its pool of the session's internet as the session
 * is brought up, whose bayma the harness, which runs in alasio, reaches through the gateway
 * with a caller's token of the workspace's key. Nothing of the harness, and no credential, is
 * ever inside a session; what a session may reach is the pool's to say (its NetworkPolicies
 * and DNS), and when it is let go, saved whole first where it is snapshotted, the gateway's.
 *
 * A session's internet is alasio's to keep, as its store records it (`netModeOf`), and it
 * travels in each token, which the gateway places the session by.
 *
 * A session is forked by the gateway: the fork, a new workspace, is a clone of the source's
 * files, given the snapshot the source was released into where it was, so it restores as the
 * source would; the fork of one bound carries its files alone. A fork carries nothing but
 * that: no conversation, no harness session.
 *
 * A branch environment's alasio (../branch/fork.ts) has sessions of its own, in its own
 * environment, and none of its parent's: a session its conversations inherited is forked from
 * the parent's, by the parent, into the branch, under the same id, as it is first brought up
 * here.
 *
 * A file a reply shows is read through the gateway, confined to the session's files, as a
 * received file is written into them: by the path the session sees it at, under its workspace
 * (SANDBOX_WORKSPACE) or its home (SANDBOX_HOME).
 */
import { mkdirSync } from "node:fs";
import { join } from "node:path";

import { Context, Effect, Layer, Schema } from "effect";

import { withLogScope } from "../shared/log.ts";
import { type BaymaEndpoint, type GatewayError, type GatewayWorkspaces, type GatewayWorkspacesOptions, makeGatewayWorkspaces, type NetMode } from "./gateway-client.ts";
import { assertValidVolumeId } from "./names.ts";

export type { NetMode } from "./gateway-client.ts";

/** Where a session sees its workspace's files, and its home's, as a pool's pod lays them out (cli/src/manifests/sandbox-gateway.ts poolTemplate). */
const SANDBOX_WORKSPACE = "/workspace";
export const SANDBOX_HOME = "/home/agent";

/** A file read to attach to a reply: its bytes, or a note saying why it is not attached. */
export type FileRead =
  | { readonly bytes: Buffer; readonly note?: never }
  | { readonly note: string; readonly bytes?: never };

/** Reading or writing a file of a session came to neither what was asked nor a reason it is not. */
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

/** How bringing a session up, or making, forking or deleting its workspace, fails. */
export type SessionError = GatewayError | SessionInheritError;

/** A conversation's workspace is a session filesystem, and the deployment gives alasio no sandbox gateway. */
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
   * the internet it was made with, where it did; null for a session that one never had,
   * which is then made here, empty, as a session that is gone is.
   */
  readonly fork: (volumeId: string) => Effect.Effect<NetMode | null, SessionInheritError>;
  /** Records a session so forked as made, with its internet, as alasio records those it makes. */
  readonly record: (volumeId: string, netMode: NetMode) => Effect.Effect<void, SessionInheritError>;
}

/** What SessionSandboxes is made with; see there. */
interface SessionSandboxesOptions {
  /** The gateway, and alasio's environment's name and key to it. */
  readonly gateway: GatewayWorkspacesOptions;
  /** alasio's state directory, under which each session's harness directory lives. */
  readonly stateDir: string;
  /** The internet the session `volumeId` was made with, as alasio's store records it; null for one it records not. */
  readonly netModeOf: (volumeId: string) => Effect.Effect<NetMode | null>;
  /** Where alasio is a branch environment, how it comes by the sessions it inherited. */
  readonly inherit?: Inheritance | undefined;
  /** The gateway's workspaces, but for tests, made of `gateway` otherwise. */
  readonly workspaces?: GatewayWorkspaces | undefined;
}

/**
 * The path in a session's files, as the gateway names it (`workspace/...` or `home/...`), of
 * `path` as the session sees it: under its workspace, as one relative to it is, or under its
 * home; null for one outside both.
 */
export function filesPath(path: string): string | null {
  const under = (root: string) => (path === root ? "" : path.startsWith(`${root}/`) ? path.slice(root.length + 1) : null);
  const named = (rest: string | null, top: string) => (rest === null || rest === "" || rest.split("/").some((part) => part === ".." || part === "") ? null : `${top}/${rest}`);
  if (path.startsWith("/")) return named(under(SANDBOX_WORKSPACE), "workspace") ?? named(under(SANDBOX_HOME), "home");
  return named(path.replace(/^\.\/+/u, ""), "workspace");
}

/**
 * Session filesystems, through the sandbox gateway, for a deployment that gives alasio one;
 * without one there is no such service, and callers offer only folders.
 */
export class SessionSandboxes extends Context.Service<SessionSandboxes, {
  readonly volumes: {
    /** A new session's workspace, made now, with its internet, which alasio records. */
    readonly create: (volumeId: string, netMode?: NetMode) => Effect.Effect<{ readonly volumeId: string; readonly netMode: NetMode }, SessionError>;
    /**
     * A new session `volumeId`, in the environment `environment` (alasio's own unless told),
     * with the internet of the session `sourceVolumeId` and a clone of its files, as the
     * module says.
     */
    readonly fork: (sourceVolumeId: string, volumeId: string, environment?: string) => Effect.Effect<{ readonly volumeId: string; readonly netMode: NetMode }, SessionError>;
    /** Deletes the session's workspace, its snapshots with it: its binding let go first, saved whole where it is snapshotted. */
    readonly destroy: (volumeId: string) => Effect.Effect<void, SessionError>;
    /** The workspaces of alasio's environment there are, as the gateway lists them. */
    readonly list: Effect.Effect<readonly string[], SessionError>;
  };
  /**
   * The directory in alasio a session's harness runs in: empty, the session's own, and
   * none of the workspace's, which is only in the session. Its path keys the harness's
   * sessions to the workspace (Claude Code's project, Codex's thread list). Made as it
   * is asked for; synchronous, as making a directory is.
   */
  readonly harnessDirectory: (volumeId: string) => string;
  /**
   * Brings the session up: bound by the gateway to a pod of its internet's pool, its bayma
   * serving, and its bayma's endpoint, through the gateway; one a branch inherited forked from
   * its parent's first.
   */
  readonly ensureSession: (volumeId: string) => Effect.Effect<{ readonly bayma: BaymaEndpoint }, SessionError>;
  /** A file from the session, at `path` as it sees it (filesPath), of `maxBytes` at most: `{ bytes }`, or `{ note }` saying why not. */
  readonly readFile: (volumeId: string, path: string, maxBytes: number) => Effect.Effect<FileRead, SessionFileError>;
  /** Writes `bytes` into the session's file at `path` as it sees it (filesPath), its directories made. */
  readonly writeFile: (volumeId: string, path: string, bytes: Uint8Array) => Effect.Effect<void, SessionFileError>;
}>()("alasio/sandbox/SessionSandboxes") {
  static readonly layer = (options: SessionSandboxesOptions): Layer.Layer<SessionSandboxes> => Layer.succeed(SessionSandboxes, makeSessionSandboxes(options));
}

/** The session filesystems of `options`, as SessionSandboxes says. */
export function makeSessionSandboxes({ gateway, stateDir, netModeOf, inherit, workspaces = makeGatewayWorkspaces(gateway) }: SessionSandboxesOptions): SessionSandboxes["Service"] {
  /** The internet a session was made with, as alasio records it; none for one it records not, as a session is by default. */
  const modeOf = (volumeId: string): Effect.Effect<NetMode> => Effect.map(netModeOf(volumeId), (mode) => mode ?? "none");

  /** A session of the branch's its conversations inherited, not yet here: forked from the parent's, which has it, and recorded as made. */
  const inherited = (volumeId: string): Effect.Effect<void, SessionError> =>
    inherit
      ? workspaces.quota(volumeId).pipe(
        Effect.flatMap((there) => (there ? Effect.succeed(null) : inherit.fork(volumeId))),
        Effect.flatMap((netMode) =>
          netMode === null
            ? Effect.void
            : inherit.record(volumeId, netMode).pipe(Effect.andThen(Effect.logInfo(`forked session ${volumeId} from the alasio this branch was branched from`)))
        ),
      )
      : Effect.void;

  return SessionSandboxes.of({
    volumes: {
      create: (volumeId, netMode = "none") =>
        Effect.suspend(() => workspaces.create(assertValidVolumeId(volumeId))).pipe(
          Effect.andThen(Effect.logInfo(`created session ${volumeId} (${netMode} internet)`)),
          Effect.as({ volumeId, netMode }),
          withLogScope("sandbox"),
        ),
      fork: (sourceVolumeId, volumeId, environment = gateway.environment) =>
        Effect.gen(function*() {
          const netMode = yield* modeOf(assertValidVolumeId(sourceVolumeId));
          yield* workspaces.fork(sourceVolumeId, { environment, workspace: assertValidVolumeId(volumeId) });
          yield* Effect.logInfo(`forked session ${sourceVolumeId} into ${environment}'s ${volumeId} (${netMode} internet)`);
          return { volumeId, netMode };
        }).pipe(withLogScope("sandbox")),
      destroy: (volumeId) =>
        Effect.gen(function*() {
          const id = assertValidVolumeId(volumeId);
          yield* workspaces.release(id, yield* modeOf(id));
          yield* workspaces.remove(id);
        }).pipe(withLogScope("sandbox")),
      list: workspaces.list,
    },

    harnessDirectory: (volumeId) => {
      const directory = join(stateDir, "sessionfs", "workspaces", assertValidVolumeId(volumeId));
      mkdirSync(directory, { recursive: true });
      return directory;
    },

    ensureSession: (volumeId) =>
      Effect.gen(function*() {
        const id = assertValidVolumeId(volumeId);
        yield* inherited(id);
        const mode = yield* modeOf(id);
        yield* workspaces.bind(id, mode);
        return { bayma: yield* workspaces.endpoint(id, mode) };
      }).pipe(withLogScope("sandbox")),

    readFile: (volumeId, path, maxBytes) => {
      const named = filesPath(path);
      if (named === null) return Effect.succeed({ note: "not in the session's files" });
      return workspaces.readFile(assertValidVolumeId(volumeId), named, maxBytes).pipe(
        Effect.map((bytes): FileRead => ({ bytes: Buffer.from(bytes) })),
        Effect.catch((error): Effect.Effect<FileRead, SessionFileError> => {
          if (error.status === 404 || error.status === 403 || error.status === 400) return Effect.succeed({ note: "file not found" });
          if (error.status === 413) return Effect.succeed({ note: `over Telegram's ${Math.round(maxBytes / (1024 * 1024))} MB limit` });
          return Effect.fail(new SessionFileError({ message: `reading ${path} in session ${volumeId} failed: ${error.message}` }));
        }),
      );
    },

    writeFile: (volumeId, path, bytes) => {
      const named = filesPath(path);
      if (named === null) return Effect.fail(new SessionFileError({ message: `${path} is not in session ${volumeId}'s files` }));
      return workspaces.writeFile(assertValidVolumeId(volumeId), named, bytes).pipe(
        Effect.mapError((error) => new SessionFileError({ message: `writing ${path} into session ${volumeId} failed: ${error.message}` })),
      );
    },
  });
}
