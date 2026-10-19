/**
 * The files the operator sends alasio: kept whole in alasio's store, and written for the
 * agent to read where its workspace is, each at a path of its own (its id, then its name),
 * which the prompt it came with names. A folder workspace's agent reads them under alasio's
 * state directory; a session filesystem's, in its session, under its home (RECEIVED_IN_SESSION),
 * written through the sandbox gateway, confined to the session's files, as the harness that runs
 * in alasio reaches none of alasio's own. Neither outlasts what holds it for long (alasio's pod,
 * a session's deletion), so before a prompt runs, its files are written again where they may be
 * missing, from the store: under the state directory where they are not there, into a session
 * each time, as alasio cannot see what a session holds but by reading it whole.
 */
import { existsSync } from "node:fs";
import { mkdir, rename, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";

import { Context, Effect, Layer, Option, Schema } from "effect";

import type { AlasioConfig } from "../config.ts";
import type { StoreError } from "../persistence/sql.ts";
import { Store } from "../persistence/store.ts";
import type { FileContent, NewFile, StoredFile } from "../persistence/telegram-content-repository.ts";
import { SANDBOX_HOME, SessionSandboxes } from "../sandbox/index.ts";
import type { Workspace } from "../workspace/kind.ts";

/** Where a session's agent reads the files it is sent: under its home, beside nothing of its workspace's. */
export const RECEIVED_IN_SESSION = `${SANDBOX_HOME}/.alasio/received`;

/** A received file could not be written where the agent reads it. */
export class ReceivedFileError extends Schema.TaggedError<ReceivedFileError>()("ReceivedFileError", {
  cause: Schema.Defect(),
}) {
  override get message(): string {
    return this.cause instanceof Error ? this.cause.message : String(this.cause);
  }
}

/** A file kept, and where the agent reads it. */
export interface ReceivedFile {
  readonly id: string;
  readonly path: string;
}

export class ReceivedFiles extends Context.Service<ReceivedFiles, {
  /** Keeps a file a message carried, and writes it where the agent of `workspace`, the conversation's, reads it. */
  readonly keep: (file: NewFile, workspace: Workspace | null) => Effect.Effect<ReceivedFile, StoreError | ReceivedFileError>;
  /** Where the agent of `workspace` reads a kept file. */
  readonly pathOf: (file: Pick<StoredFile, "id" | "name">, workspace: Workspace | null) => string;
  /** Writes the files `ids` name where the agent of `workspace` reads them, where they may be missing. */
  readonly materialize: (ids: readonly string[], workspace: Workspace | null) => Effect.Effect<void, StoreError | ReceivedFileError>;
}>()("alasio/telegram/ReceivedFiles") {
  /** The files kept in the store, written under `stateDir`, or into a session through the session filesystems there are. */
  static readonly layer = ({ stateDir }: Pick<AlasioConfig, "stateDir">): Layer.Layer<ReceivedFiles, never, Store> =>
    Layer.effect(ReceivedFiles, Effect.gen(function*() {
      const store = yield* Store;
      const sessions = Option.getOrNull(yield* Effect.serviceOption(SessionSandboxes));
      const root = join(stateDir, "telegram-files");
      const local = ({ id, name }: Pick<StoredFile, "id" | "name">): string => join(root, id, name);
      const pathOf = (file: Pick<StoredFile, "id" | "name">, workspace: Workspace | null): string =>
        workspace?.kind === "sessionfs" ? `${RECEIVED_IN_SESSION}/${file.id}/${file.name}` : local(file);
      // Written beside its place and moved there whole, so a file's directory is there only once the file is.
      const writeLocal = ({ id, name, content }: FileContent): Effect.Effect<void, ReceivedFileError> =>
        Effect.tryPromise({
          try: async () => {
            const partial = join(root, `.${id}.partial`);
            await rm(partial, { recursive: true, force: true });
            await mkdir(partial, { recursive: true });
            await writeFile(join(partial, name), content);
            await rename(partial, join(root, id));
          },
          catch: (cause) => new ReceivedFileError({ cause }),
        });
      /** Writes `file` where the agent of `workspace` reads it: into its session, or under the state directory. */
      const write = (file: FileContent, workspace: Workspace | null): Effect.Effect<void, ReceivedFileError> => {
        if (workspace?.kind !== "sessionfs") return writeLocal(file);
        if (!sessions) return Effect.fail(new ReceivedFileError({ cause: new Error("this deployment offers no session filesystems to write the file into") }));
        return sessions.writeFile(workspace.volumeId, pathOf(file, workspace), file.content).pipe(Effect.mapError((cause) => new ReceivedFileError({ cause })));
      };

      return ReceivedFiles.of({
        keep: Effect.fnUntraced(function*(file, workspace) {
          const id = yield* store.insertFile(file);
          // Written once: a file kept already, as for a message handled again, is there; one into a session is written
          // again, the same.
          if (workspace?.kind === "sessionfs" || !existsSync(join(root, id))) yield* write({ id, name: file.name, content: file.content }, workspace);
          return { id, path: pathOf({ id, name: file.name }, workspace) };
        }),
        pathOf,
        materialize: (ids, workspace) =>
          Effect.suspend(() => {
            const missing = workspace?.kind === "sessionfs" ? [...ids] : ids.filter((id) => !existsSync(join(root, id)));
            return missing.length === 0
              ? Effect.void
              : Effect.flatMap(store.getFileContents(missing), (files) => Effect.forEach(files, (file) => write(file, workspace), { discard: true }));
          }),
      });
    }));
}
