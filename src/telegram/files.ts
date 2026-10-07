/**
 * The files the operator sends alasio: kept whole in alasio's store, and written for the
 * agent to read under alasio's state directory, each at a path of its own (its id, then
 * its name), which the prompt it came with names. The state directory does not outlast
 * alasio's pod, so before a prompt runs, its files are written again where they are
 * missing, from the store.
 */
import { existsSync } from "node:fs";
import { mkdir, rename, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";

import { Context, Effect, Layer, Schema } from "effect";

import type { AlasioConfig } from "../config.ts";
import type { StoreError } from "../persistence/sql.ts";
import { Store } from "../persistence/store.ts";
import type { FileContent, NewFile, StoredFile } from "../persistence/telegram-content-repository.ts";

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
  /** Keeps a file a message carried, and writes it where the agent reads it. */
  readonly keep: (file: NewFile) => Effect.Effect<ReceivedFile, StoreError | ReceivedFileError>;
  /** Where the agent reads a kept file. */
  readonly pathOf: (file: Pick<StoredFile, "id" | "name">) => string;
  /** Writes the files `ids` name where the agent reads them, where they are missing. */
  readonly materialize: (ids: readonly string[]) => Effect.Effect<void, StoreError | ReceivedFileError>;
}>()("alasio/telegram/ReceivedFiles") {
  /** The files kept in the store, written under `stateDir`. */
  static readonly layer = ({ stateDir }: Pick<AlasioConfig, "stateDir">): Layer.Layer<ReceivedFiles, never, Store> =>
    Layer.effect(ReceivedFiles, Effect.map(Store, (store) => {
      const root = join(stateDir, "telegram-files");
      const pathOf = ({ id, name }: Pick<StoredFile, "id" | "name">): string => join(root, id, name);
      // Written beside its place and moved there whole, so a file's directory is there only once the file is.
      const write = ({ id, name, content }: FileContent): Effect.Effect<void, ReceivedFileError> =>
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

      return ReceivedFiles.of({
        keep: Effect.fnUntraced(function*(file) {
          const id = yield* store.insertFile(file);
          yield* write({ id, name: file.name, content: file.content });
          return { id, path: pathOf({ id, name: file.name }) };
        }),
        pathOf,
        materialize: (ids) =>
          Effect.suspend(() => {
            const missing = ids.filter((id) => !existsSync(join(root, id)));
            return missing.length === 0
              ? Effect.void
              : Effect.flatMap(store.getFileContents(missing), (files) => Effect.forEach(files, write, { discard: true }));
          }),
      });
    }));
}
