/**
 * What is mounted on a conversation, as the operator changes it: the service its turns
 * run on, and the folder or session filesystem they work in. A change is refused while a
 * turn runs in the conversation, or while prompts wait for what it would leave.
 *
 * The session filesystems alasio makes, empty or forked from another, are recorded in
 * its store before they are made and marked made once they are whole, which is what
 * lists them for the operator to switch to. One a failure or a crash left unmade is
 * deleted, at once or as alasio next starts.
 */
import { Context, Effect, Layer, Option, Schema } from "effect";

import type { AlasioConfig } from "../config.ts";
import { ActiveTurns } from "../harness/active-turns.ts";
import { harnessLabelOf, isHarnessName } from "../harness/index.ts";
import type { HarnessName } from "../harness/names.ts";
import type { StoreError } from "../persistence/sql.ts";
import { Store } from "../persistence/store.ts";
import { type NetMode, type SessionError, type SessionForkError, SessionSandboxes } from "../sandbox/index.ts";
import { isValidVolumeId, newVolumeId } from "../sandbox/names.ts";
import { withLogScope } from "../shared/log.ts";
import { isSessionFs, parseWorkspace, sessionFsWorkspace } from "../workspace/kind.ts";
import { createWorkspace, resolveWorkspacePath } from "../workspace/policy.ts";

/** The outcome of mounting a service on a conversation. */
export interface HarnessSwitch {
  /** Whether the service changed; false when it was already the active one. */
  readonly switched: boolean;
  readonly previous: HarnessName | null;
  readonly next: HarnessName;
  readonly sessionId: string | null;
  readonly workingDirectory?: string | null | undefined;
}

/** The outcome of mounting a folder on a conversation, or of creating one and mounting it. */
export interface WorkspaceChange {
  /** Whether the folder changed; false when it was already the mounted one. */
  readonly switched: boolean;
  /** Set when the folder was newly created. */
  readonly created?: boolean | undefined;
  /** The session filesystem the one mounted is a fork of, when it was forked just now. */
  readonly forkedFrom?: string | undefined;
  readonly previous: string | null;
  readonly workingDirectory: string;
}

/** A change of mount alasio will not make now, and why, as the operator is told. */
export class MountRefused extends Schema.TaggedError<MountRefused>()("MountRefused", {
  message: Schema.String,
}) {}

/** A folder the workspace policy will not mount or make (src/workspace/policy.ts), or one the filesystem failed to. */
export class WorkspaceFolderError extends Schema.TaggedError<WorkspaceFolderError>()("WorkspaceFolderError", {
  cause: Schema.Defect(),
}) {
  override get message(): string {
    return this.cause instanceof Error ? this.cause.message : String(this.cause);
  }
}

/** How changing a conversation's folder fails. */
export type WorkspaceChangeError = MountRefused | WorkspaceFolderError | SessionError | SessionForkError;

/** The scope these lines have always been logged in, kept for whatever reads alasio's logs. */
const LOG_SCOPE = "codex-turn-controller";

/** A folder operation of the workspace policy, its failure the operator's to read. */
const folder = <A>(operation: () => Promise<A>): Effect.Effect<A, WorkspaceFolderError> =>
  Effect.tryPromise({ try: operation, catch: (cause) => new WorkspaceFolderError({ cause }) });

export class Mounts extends Context.Service<Mounts, {
  /** Where the operator's folders are. */
  readonly workspaceRoot: string;
  /** Whether the deployment offers session filesystems. */
  readonly sessionFilesystems: boolean;
  /** Mounts the service `harness` on the conversation. */
  readonly switchHarness: (conversationId: string, harness: string) => Effect.Effect<HarnessSwitch, MountRefused | StoreError>;
  /** Mounts the folder `target` names under the workspace root. */
  readonly switchWorkspace: (conversationId: string, target: string) => Effect.Effect<WorkspaceChange, MountRefused | WorkspaceFolderError | StoreError>;
  /** Creates a git-initialized folder `name` under the workspace root and mounts it. */
  readonly createWorkspace: (conversationId: string, name: string) => Effect.Effect<WorkspaceChange, MountRefused | WorkspaceFolderError | StoreError>;
  /**
   * Creates an empty session filesystem with internet access `netMode` and mounts it. The
   * workspace is the sentinel `sessionfs:<volumeId>` (src/workspace/kind.ts), so it parks
   * and restores like any other workspace; its sandbox comes up when a turn needs it.
   */
  readonly createSessionWorkspace: (conversationId: string, netMode: NetMode) => Effect.Effect<WorkspaceChange, MountRefused | SessionError | StoreError>;
  /**
   * Forks the session filesystem mounted on the conversation (src/sandbox/index.ts) and
   * mounts the fork, with no session of its own yet. Refused for a folder, and while a
   * turn runs in the conversation or prompts wait in it, as the source is suspended while
   * it is cloned.
   */
  readonly forkSessionWorkspace: (conversationId: string) => Effect.Effect<WorkspaceChange, MountRefused | SessionError | SessionForkError | StoreError>;
  /**
   * Deletes the session filesystems a failure or a crash left unmade: those recorded but
   * not made, and forks' Sandboxes no record made. As alasio starts, before any is made.
   */
  readonly reconcileSessionWorkspaces: Effect.Effect<void, StoreError>;
}>()("alasio/operator/Mounts") {
  static readonly layer = ({ workspaceRoot }: Pick<AlasioConfig, "workspaceRoot">): Layer.Layer<Mounts, never, Store | ActiveTurns> =>
    Layer.effect(Mounts, makeMounts(workspaceRoot));
}

const makeMounts = Effect.fnUntraced(function*(workspaceRoot: string): Effect.fn.Return<Mounts["Service"], never, Store | ActiveTurns> {
  const store = yield* Store;
  const activeTurns = yield* ActiveTurns;
  const sandbox = Option.getOrNull(yield* Effect.serviceOption(SessionSandboxes));

  /** Refuses a change while a turn runs in the conversation or prompts wait for its service; `doing` is the change, as the operator is told. */
  const unblocked = Effect.fnUntraced(function*(conversationId: string, doing = "switching services"): Effect.fn.Return<void, MountRefused | StoreError> {
    if (yield* activeTurns.isBusy(conversationId)) {
      const mount = yield* store.getMount(conversationId);
      return yield* new MountRefused({ message: `${harnessLabelOf(mount)} is currently working. Stop the active turn before ${doing}.` });
    }
    if (yield* store.hasOpenPromptJobs(conversationId)) {
      return yield* new MountRefused({ message: `Queued prompts are still waiting for the current service. Let them finish or discard them before ${doing}.` });
    }
  });

  /** Deletes the session filesystem `volumeId`, and its record once it is gone. */
  const discard = (volumeId: string): Effect.Effect<void> =>
    Effect.gen(function*() {
      if (sandbox) yield* sandbox.volumes.destroy(volumeId);
      yield* store.forgetSessionWorkspace(volumeId);
      yield* Effect.logInfo(`workspace.discarded.sessionfs volume=${volumeId}`);
    }).pipe(Effect.catch((error) => Effect.logWarning(`could not delete the unmade session filesystem ${volumeId}, which alasio deletes as it next starts: ${error.message}`)));

  /** `make`, a session filesystem's making, recorded before and marked made after; one that fails is discarded. */
  const recorded = <E>(volumeId: string, forkedFrom: string | null, make: Effect.Effect<{ readonly netMode: NetMode }, E>): Effect.Effect<void, E | StoreError> =>
    store.recordSessionWorkspace({ volumeId, forkedFrom }).pipe(
      Effect.andThen(make.pipe(Effect.onError(() => discard(volumeId)))),
      Effect.flatMap(({ netMode }) => store.markSessionWorkspaceMade(volumeId, netMode)),
    );

  /** The session filesystem `target` names, `sessionfs:<volumeId>`, which alasio made. */
  const sessionWorkspace = Effect.fnUntraced(function*(target: string): Effect.fn.Return<string, MountRefused | StoreError> {
    const volumeId = target.trim().slice("sessionfs:".length);
    const made = (yield* store.listSessionWorkspaces).some((workspace) => workspace.volumeId === volumeId && workspace.madeAt !== null);
    if (!sandbox || !isValidVolumeId(volumeId) || !made) return yield* new MountRefused({ message: `There is no session workspace ${volumeId}.` });
    return sessionFsWorkspace(volumeId);
  });

  /** Mounts `workingDirectory`, created just now, on the conversation. */
  const mountCreated = Effect.fnUntraced(function*(conversationId: string, workingDirectory: string, logLine: string): Effect.fn.Return<WorkspaceChange, StoreError> {
    const previous = (yield* store.getMount(conversationId)).workingDirectory;
    yield* store.setWorkingDirectory(conversationId, workingDirectory);
    yield* Effect.logInfo(logLine);
    return { switched: true, created: true, previous, workingDirectory };
  });

  return Mounts.of({
    workspaceRoot,
    sessionFilesystems: sandbox !== null,
    switchHarness: Effect.fnUntraced(function*(conversationId, harness) {
      if (!isHarnessName(harness)) {
        return yield* new MountRefused({ message: `Unknown service: ${harness}` });
      }
      const before = yield* store.getMount(conversationId);
      const previous = before.harness;
      if (previous === harness) {
        return { switched: false, previous, next: harness, sessionId: before.sessionId };
      }
      yield* unblocked(conversationId);
      yield* store.setActiveHarness(conversationId, harness);
      yield* Effect.logInfo(`service.switched conversation=${JSON.stringify(conversationId)} from=${previous} to=${harness}`);
      const after = yield* store.getMount(conversationId);
      return { switched: true, previous, next: harness, sessionId: after.sessionId, workingDirectory: after.workingDirectory };
    }, withLogScope(LOG_SCOPE)),
    switchWorkspace: Effect.fnUntraced(function*(conversationId, target) {
      const workingDirectory = yield* (isSessionFs(target.trim()) ? sessionWorkspace(target) : folder(() => resolveWorkspacePath({ root: workspaceRoot, candidate: target })));
      const previous = (yield* store.getMount(conversationId)).workingDirectory;
      if (previous === workingDirectory) {
        return { switched: false, previous, workingDirectory };
      }
      yield* unblocked(conversationId);
      yield* store.setWorkingDirectory(conversationId, workingDirectory);
      yield* Effect.logInfo(`workspace.switched conversation=${JSON.stringify(conversationId)} from=${previous} to=${workingDirectory}`);
      return { switched: true, previous, workingDirectory };
    }, withLogScope(LOG_SCOPE)),
    createWorkspace: Effect.fnUntraced(function*(conversationId, name) {
      yield* unblocked(conversationId);
      const workingDirectory = yield* folder(() => createWorkspace({ root: workspaceRoot, name }));
      return yield* mountCreated(conversationId, workingDirectory, `workspace.created conversation=${JSON.stringify(conversationId)} path=${workingDirectory}`);
    }, withLogScope(LOG_SCOPE)),
    createSessionWorkspace: Effect.fnUntraced(function*(conversationId, netMode) {
      if (!sandbox) {
        return yield* new MountRefused({ message: "Session filesystems are not enabled on this deployment." });
      }
      yield* unblocked(conversationId);
      const volumeId = newVolumeId();
      yield* recorded(volumeId, null, sandbox.volumes.create(volumeId, netMode === "full" ? "full" : "none"));
      return yield* mountCreated(
        conversationId,
        sessionFsWorkspace(volumeId),
        `workspace.created.sessionfs conversation=${JSON.stringify(conversationId)} volume=${volumeId} net=${netMode}`,
      );
    }, withLogScope(LOG_SCOPE)),
    forkSessionWorkspace: Effect.fnUntraced(function*(conversationId) {
      if (!sandbox) {
        return yield* new MountRefused({ message: "Session filesystems are not enabled on this deployment." });
      }
      const source = parseWorkspace((yield* store.getMount(conversationId)).workingDirectory);
      if (source?.kind !== "sessionfs") {
        return yield* new MountRefused({ message: "Only a session workspace can be forked: a folder's files are the host's, which are not copied on write." });
      }
      yield* unblocked(conversationId, "forking its workspace");
      const volumeId = newVolumeId();
      yield* recorded(volumeId, source.volumeId, sandbox.volumes.fork(source.volumeId, volumeId));
      const change = yield* mountCreated(
        conversationId,
        sessionFsWorkspace(volumeId),
        `workspace.forked.sessionfs conversation=${JSON.stringify(conversationId)} volume=${volumeId} from=${source.volumeId}`,
      );
      return { ...change, forkedFrom: source.volumeId };
    }, withLogScope(LOG_SCOPE)),
    reconcileSessionWorkspaces: Effect.gen(function*() {
      if (!sandbox) return;
      const workspaces = yield* store.listSessionWorkspaces;
      const unmade = workspaces.filter(({ madeAt }) => madeAt === null).map(({ volumeId }) => volumeId);
      const made = new Set(workspaces.filter(({ madeAt }) => madeAt !== null).map(({ volumeId }) => volumeId));
      const forks = yield* sandbox.volumes.forks.pipe(
        Effect.catch((error) => Effect.logWarning(`could not list the forks' Sandboxes: ${error.message}`).pipe(Effect.as([]))),
      );
      const orphans = forks.filter((volumeId) => !made.has(volumeId) && !unmade.includes(volumeId));
      for (const volumeId of [...unmade, ...orphans]) yield* discard(volumeId);
    }).pipe(withLogScope(LOG_SCOPE)),
  });
});
