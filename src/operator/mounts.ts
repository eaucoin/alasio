/**
 * What is mounted on a conversation, as the operator changes it: the service its turns
 * run on, and the folder or session filesystem they work in. A change is refused while a
 * turn runs in the conversation, or while prompts wait for what it would leave.
 */
import { Context, Effect, Layer, Option, Schema } from "effect";

import type { AlasioConfig } from "../config.ts";
import { ActiveTurns } from "../harness/active-turns.ts";
import { harnessLabelOf, isHarnessName, resolveWorkingDirectory } from "../harness/index.ts";
import type { HarnessName } from "../harness/names.ts";
import { Store } from "../persistence/store.ts";
import { type NetMode, type SessionError, SessionSandboxes } from "../sandbox/index.ts";
import { newVolumeId } from "../sandbox/names.ts";
import { withLogScope } from "../shared/log.ts";
import { sessionFsWorkspace } from "../workspace/kind.ts";
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
export type WorkspaceChangeError = MountRefused | WorkspaceFolderError | SessionError;

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
  readonly switchHarness: (conversationId: string, harness: string) => Effect.Effect<HarnessSwitch, MountRefused>;
  /** Mounts the folder `target` names under the workspace root. */
  readonly switchWorkspace: (conversationId: string, target: string) => Effect.Effect<WorkspaceChange, MountRefused | WorkspaceFolderError>;
  /** Creates a git-initialized folder `name` under the workspace root and mounts it. */
  readonly createWorkspace: (conversationId: string, name: string) => Effect.Effect<WorkspaceChange, MountRefused | WorkspaceFolderError>;
  /**
   * Creates an empty session filesystem with internet access `netMode` and mounts it. The
   * workspace is the sentinel `sessionfs:<volumeId>` (src/workspace/kind.ts), so it parks
   * and restores like any other workspace; its sandbox comes up when a turn needs it.
   */
  readonly createSessionWorkspace: (conversationId: string, netMode: NetMode) => Effect.Effect<WorkspaceChange, MountRefused | SessionError>;
}>()("alasio/operator/Mounts") {
  static readonly layer = ({ workspaceRoot }: Pick<AlasioConfig, "workspaceRoot">): Layer.Layer<Mounts, never, Store | ActiveTurns> =>
    Layer.effect(Mounts, makeMounts(workspaceRoot));
}

const makeMounts = Effect.fnUntraced(function*(workspaceRoot: string): Effect.fn.Return<Mounts["Service"], never, Store | ActiveTurns> {
  const store = yield* Store;
  const activeTurns = yield* ActiveTurns;
  const sandbox = Option.getOrNull(yield* Effect.serviceOption(SessionSandboxes));

  /** Refuses a change while a turn runs in the conversation or prompts wait for its service. */
  const unblocked = (conversationId: string): Effect.Effect<void, MountRefused> =>
    Effect.flatMap(activeTurns.isBusy(conversationId), (busy) => {
      if (busy) {
        return Effect.fail(new MountRefused({ message: `${harnessLabelOf(store, conversationId)} is currently working. Stop the active turn before switching services.` }));
      }
      if (store.hasOpenPromptJobs(conversationId)) {
        return Effect.fail(new MountRefused({ message: "Queued prompts are still waiting for the current service. Let them finish or discard them before switching." }));
      }
      return Effect.void;
    });

  /** Mounts `workingDirectory`, created just now, on the conversation. */
  const mountCreated = (conversationId: string, workingDirectory: string, logLine: string): Effect.Effect<WorkspaceChange> =>
    Effect.suspend(() => {
      const previous = resolveWorkingDirectory(store, conversationId);
      store.setWorkingDirectory(conversationId, workingDirectory);
      return Effect.as(Effect.logInfo(logLine), { switched: true, created: true, previous, workingDirectory });
    });

  return Mounts.of({
    workspaceRoot,
    sessionFilesystems: sandbox !== null,
    switchHarness: Effect.fnUntraced(function*(conversationId, harness) {
      if (!isHarnessName(harness)) {
        return yield* new MountRefused({ message: `Unknown service: ${harness}` });
      }
      const previous = store.getActiveHarness(conversationId);
      if (previous === harness) {
        return { switched: false, previous, next: harness, sessionId: store.getSessionId(conversationId) ?? null };
      }
      yield* unblocked(conversationId);
      store.setActiveHarness(conversationId, harness);
      yield* Effect.logInfo(`service.switched conversation=${JSON.stringify(conversationId)} from=${previous} to=${harness}`);
      return {
        switched: true,
        previous,
        next: harness,
        sessionId: store.getSessionId(conversationId) ?? null,
        workingDirectory: resolveWorkingDirectory(store, conversationId),
      };
    }, withLogScope(LOG_SCOPE)),
    switchWorkspace: Effect.fnUntraced(function*(conversationId, target) {
      const workingDirectory = yield* folder(() => resolveWorkspacePath({ root: workspaceRoot, candidate: target }));
      const previous = resolveWorkingDirectory(store, conversationId);
      if (previous === workingDirectory) {
        return { switched: false, previous, workingDirectory };
      }
      yield* unblocked(conversationId);
      store.setWorkingDirectory(conversationId, workingDirectory);
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
      yield* sandbox.volumes.create(volumeId, netMode === "full" ? "full" : "none");
      return yield* mountCreated(
        conversationId,
        sessionFsWorkspace(volumeId),
        `workspace.created.sessionfs conversation=${JSON.stringify(conversationId)} volume=${volumeId} net=${netMode}`,
      );
    }, withLogScope(LOG_SCOPE)),
  });
});
