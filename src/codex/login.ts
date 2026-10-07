/**
 * Codex's login, kept in alasio's store where alasio's Codex home does not outlast its
 * pod (a default install's home is an emptyDir; the host profile's is the operator's
 * own, which keeps it). Codex keeps its login in its home's `auth.json`, and rewrites the
 * file as it refreshes the login, with a refresh token that works once: a copy that lags
 * behind the file is a login lost. So, as alasio starts, the file is written from the
 * store unless it is there already, newer than what was kept; and every change Codex
 * makes to it is kept again: as the home reports it, every thirty seconds besides in
 * case a report was missed, and once more as alasio stops.
 *
 * Only the login is kept. What else is in a default install's Codex home is Codex's
 * cache or alasio's (rollouts are kept in Neon by ./rollouts/); nothing there is the
 * operator's to author, as nothing but `alasio login codex` reaches it.
 */
import { watch } from "node:fs";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { join } from "node:path";

import { Effect, Schedule, Schema, type Scope, Semaphore } from "effect";

import type { StoreError } from "../persistence/sql.ts";
import { Store } from "../persistence/store.ts";
import { withLogScope } from "../shared/log.ts";
import { isNotFound } from "./rollouts/files.ts";

/** How often the login is checked for a change no report covered. */
const CHECK_EVERY = "30 seconds";

/** The file Codex keeps its login in, in its home. */
const AUTH_FILE = "auth.json";

/** Codex's login could not be read from, or written to, its home. */
export class CodexLoginError extends Schema.TaggedError<CodexLoginError>()("CodexLoginError", {
  cause: Schema.Defect(),
}) {
  override get message(): string {
    return this.cause instanceof Error ? this.cause.message : String(this.cause);
  }
}

const onFiles = <A>(work: () => Promise<A>): Effect.Effect<A, CodexLoginError> =>
  Effect.tryPromise({ try: work, catch: (cause) => new CodexLoginError({ cause }) });

/**
 * Keeps the login of the Codex home `home` in the store, until the scope closes: written
 * there as this starts, unless it is there, and kept as it changes.
 */
export const keepCodexLogin = Effect.fnUntraced(function*(home: string): Effect.fn.Return<void, StoreError | CodexLoginError, Store | Scope.Scope> {
  const store = yield* Store;
  const path = join(home, AUTH_FILE);
  const oneAtATime = yield* Semaphore.make(1);
  /** What the store keeps, as last read or written. */
  let kept = yield* store.getCodexLogin;

  /** The file's text, or null when Codex has no login there. */
  const read = onFiles(() => readFile(path, "utf8").catch((error: unknown) => (isNotFound(error) ? null : Promise.reject(error))));

  /** Keeps the file's login if it changed since it was last kept. */
  const keep = Effect.gen(function*() {
    const auth = yield* read;
    if (auth === kept) return;
    yield* store.setCodexLogin(auth);
    kept = auth;
    yield* Effect.logInfo(auth === null ? "Codex logged out; its login is no longer kept" : "kept Codex's login");
  }).pipe(Semaphore.withPermit(oneAtATime));

  yield* onFiles(() => mkdir(home, { recursive: true }));
  if (kept !== null && (yield* read) === null) {
    // Written beside it and moved into place, as Codex reads it whole or not at all.
    const written = kept;
    yield* onFiles(async () => {
      await writeFile(`${path}.alasio`, written, { mode: 0o600 });
      await rename(`${path}.alasio`, path);
    });
    yield* Effect.logInfo("wrote Codex's login back from the store");
  } else {
    yield* keep;
  }

  /** Keeps the login, saying so when it cannot; the next change or check tries again. */
  const keeping = keep.pipe(Effect.catch((error) => Effect.logWarning(`could not keep Codex's login: ${error.message}`)));
  const runFork = Effect.runForkWith(yield* Effect.context<never>());
  const watcher = yield* Effect.acquireRelease(
    Effect.try({
      try: () => watch(home, (_event, filename) => {
        if (filename === AUTH_FILE) runFork(keeping);
      }),
      catch: (cause) => new CodexLoginError({ cause }),
    }),
    (watcher) => Effect.sync(() => watcher.close()).pipe(Effect.andThen(keeping)),
  );
  watcher.on("error", (error) => runFork(Effect.logWarning(`stopped watching Codex's login, which the check still keeps: ${error.message}`)));
  yield* keeping.pipe(Effect.schedule(Schedule.spaced(CHECK_EVERY)), Effect.forkScoped);
}, withLogScope("codex-login"));
