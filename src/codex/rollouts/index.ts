/**
 * Codex's rollout files, kept in alasio's Neon: a mirror that copies each
 * change to them as it is written, and restore, which writes back what a
 * thread alasio points at needs (restore.ts).
 *
 * The kernel reports each write to the rollout directories, and the file is
 * mirrored within milliseconds. A check every half minute mirrors anything
 * a report missed: changes made while alasio was down, a directory made after
 * its watch could start, or a failed watch, which it starts again. `flush`
 * mirrors one thread now, for a turn to wait on before its reply is final.
 *
 * Every write to the store goes through one queue, so two changes to a
 * file are never mirrored at once, and one begun is finished: stopping waits
 * for it. The mirror is not on Codex's path: Codex writes its files as ever,
 * and if the mirror fails, the next check catches up on everything.
 */
import { watch, type FSWatcher, type WatchListener, type WatchOptionsWithStringEncoding } from "node:fs";
import { join } from "node:path";

import { Clock, Context, Effect, Fiber, Queue, Schedule, Schema, Scope, Semaphore } from "effect";

import { withLogScope } from "../../shared/log.ts";
import { isNotFound, listRolloutFiles, parseRolloutName, rolloutFile, type RolloutFile, ROLLOUT_DIRS } from "./files.ts";
import { mirrorRollout, type KnownRollouts } from "./mirror.ts";
import { restoreRollouts, type RolloutRestoreError } from "./restore.ts";
import type { NeonRolloutStore } from "./store.ts";

/** How often every rollout file is checked. */
const CHECK_EVERY = "30 seconds";
/** How long the check waits after an error. */
const RETRY_AFTER = "60 seconds";
/** How long a change may take to be reported before the check counts it as missed. */
const REPORT_GRACE_MS = 5_000;
/** How long a flush may take before its turn goes on without it. */
const FLUSH_TIMEOUT_MS = 5_000;
/** The key of the Codex home's own watcher, beside the rollout directories'. */
const HOME = ".";

/** What mirroring failed with: the store, or the file system, as it said. */
export class RolloutMirrorError extends Schema.TaggedError<RolloutMirrorError>()("RolloutMirrorError", {
  cause: Schema.Defect(),
}) {
  override get message(): string {
    return this.cause instanceof Error ? this.cause.message : String(this.cause);
  }
}

/** A flush that took longer than a turn waits for; it is still mirrored, in turn. */
export class RolloutFlushTimeout extends Schema.TaggedError<RolloutFlushTimeout>()("RolloutFlushTimeout", {}) {
  override get message(): string {
    return `not mirrored within ${FLUSH_TIMEOUT_MS / 1000}s`;
  }
}

/** The Codex home whose rollouts are kept, and the store they are kept in. */
export interface CodexRolloutsOptions {
  readonly store: NeonRolloutStore;
  readonly home: string;
}

/** The rollouts of one Codex home, kept in a store, as makeCodexRollouts keeps them. */
export interface KeptCodexRollouts {
  /** Writes back from the store what these threads need and this machine lacks. Succeeds with the paths written. */
  readonly restore: (threadIds: readonly string[]) => Effect.Effect<string[], RolloutRestoreError>;
  /** Mirrors a thread's files now. Fails if that fails or takes longer than FLUSH_TIMEOUT_MS. */
  readonly flush: (threadId: string) => Effect.Effect<void, RolloutMirrorError | RolloutFlushTimeout>;
}

/** What a watcher reports, as the mirror reads it from its queue. */
type WatchEvent =
  /** A change under a rollout directory; one without a file name, which the kernel may give, asks for a check. */
  | { readonly _tag: "Changed"; readonly dir: string; readonly filename: string | null }
  /** The Codex home made a rollout directory that was missing. */
  | { readonly _tag: "DirMade" }
  /** A watcher failed, and was closed for the next check to start again. */
  | { readonly _tag: "WatchFailed"; readonly path: string; readonly error: Error };

/**
 * Keeps the rollouts under `home` in `store` until the scope closes: the watchers'
 * reports and the check are mirrored by fibers of the scope, and restore and flush run
 * as asked.
 */
export const makeCodexRollouts = Effect.fnUntraced(
  function*({ store, home }: CodexRolloutsOptions): Effect.fn.Return<KeptCodexRollouts, never, Scope.Scope> {
    const scope = yield* Scope.Scope;
    const oneAtATime = yield* Semaphore.make(1);
    const events = yield* Queue.unbounded<WatchEvent>();
    const watchers = new Map<string, FSWatcher>();
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        for (const watcher of watchers.values()) watcher.close();
        watchers.clear();
      })
    );
    // Paths the watchers reported that are not yet mirrored.
    const reported = new Set<string>();
    let known: KnownRollouts | null = null;
    let failing = false;

    const mirroring = <A>(work: () => A | Promise<A>): Effect.Effect<A, RolloutMirrorError> =>
      Effect.tryPromise({ try: async () => await work(), catch: (cause) => new RolloutMirrorError({ cause }) });
    const mirror = (known: KnownRollouts, file: RolloutFile): Effect.Effect<boolean, RolloutMirrorError> =>
      mirroring(() => mirrorRollout({ store, home, known, file }));

    /** Runs `work` once everything queued before it is done, and once it starts, to its end. */
    const serially = <A>(work: (known: KnownRollouts) => Effect.Effect<A, RolloutMirrorError>): Effect.Effect<A, RolloutMirrorError> =>
      Effect.gen(function*() {
        const current = (known ??= new Map((yield* mirroring(() => store.list())).map(({ name, ...kept }) => [name, kept])));
        const result = yield* work(current);
        if (failing) yield* Effect.logInfo("mirroring is running again");
        failing = false;
        return result;
      }).pipe(
        // What the store keeps may have changed with the failure: read it again.
        Effect.tapError(() => Effect.sync(() => (known = null))),
        Effect.uninterruptible,
        Semaphore.withPermit(oneAtATime),
      );

    const failed = Effect.fnUntraced(function*(error: RolloutMirrorError) {
      if (!failing) yield* Effect.logWarning(`mirroring failed, and the next check retries it: ${error.message}`);
      failing = true;
    });

    const drain = Effect.fnUntraced(function*(known: KnownRollouts) {
      for (const path of reported) {
        reported.delete(path);
        const file = yield* mirroring(() => rolloutFile(home, path));
        if (file) yield* mirror(known, file);
      }
    });

    /** Mirrors every file that changed. Succeeds with how many, and how many of those no report had covered. */
    const check = Effect.fnUntraced(function*(known: KnownRollouts) {
      reported.clear();
      let mirrored = 0;
      let missed = 0;
      for (const file of yield* mirroring(() => listRolloutFiles(home))) {
        if (!(yield* mirror(known, file))) continue;
        mirrored += 1;
        if ((yield* Clock.currentTimeMillis) - file.modifiedMs > REPORT_GRACE_MS) missed += 1;
      }
      return { mirrored, missed };
    });

    const report = (event: WatchEvent): void => {
      Queue.offerUnsafe(events, event);
    };

    /** Starts a watcher kept under `key`, which the next check starts again if it fails. Succeeds with whether it started. */
    const startWatcher = (
      key: string,
      path: string,
      options: WatchOptionsWithStringEncoding,
      onChange: WatchListener<string>,
    ): Effect.Effect<boolean> =>
      Effect.try({ try: () => watch(path, options, onChange), catch: (cause) => new RolloutMirrorError({ cause }) }).pipe(
        Effect.map((watcher) => {
          watcher.on("error", (error) => {
            watcher.close();
            watchers.delete(key);
            report({ _tag: "WatchFailed", path, error });
          });
          watchers.set(key, watcher);
          return true;
        }),
        Effect.catchTag("RolloutMirrorError", (error) =>
          isNotFound(error.cause) ? Effect.succeed(false) : Effect.logWarning(`could not watch ${path}: ${error.message}`).pipe(Effect.as(false))
        ),
      );

    /**
     * Watches each rollout directory, and while one is missing, the Codex home
     * for it to be made, as Codex does with a new home's first thread.
     */
    const watchDirs: Effect.Effect<void> = Effect.gen(function*() {
      const missing: string[] = [];
      for (const dir of ROLLOUT_DIRS) {
        if (watchers.has(dir)) continue;
        const onChange: WatchListener<string> = (_event, filename) => report({ _tag: "Changed", dir, filename });
        if (!(yield* startWatcher(dir, join(home, dir), { recursive: true }, onChange))) missing.push(dir);
      }
      if (missing.length === 0) {
        watchers.get(HOME)?.close();
        watchers.delete(HOME);
      } else if (!watchers.has(HOME)) {
        yield* startWatcher(HOME, home, {}, (_event, filename) => {
          if (filename !== null && missing.includes(filename)) report({ _tag: "DirMade" });
        });
      }
    });

    /** Mirrors what the watchers reported since it last looked, all of it once it has a turn. */
    const mirrorReports = Effect.gen(function*() {
      let checkAll = false;
      for (const event of yield* Queue.takeAll(events)) {
        switch (event._tag) {
          case "Changed":
            if (event.filename === null) checkAll = true;
            else reported.add(join(event.dir, event.filename));
            break;
          case "DirMade":
            yield* watchDirs;
            checkAll = true;
            break;
          case "WatchFailed":
            yield* Effect.logWarning(`watching ${event.path} failed, and the next check watches it again: ${event.error.message}`);
            break;
        }
      }
      if (checkAll) yield* serially(check).pipe(Effect.asVoid, Effect.catch(failed));
      else if (reported.size > 0) yield* serially(drain).pipe(Effect.catch(failed));
    });

    let catchingUp = true;
    const checkEverything = Effect.gen(function*() {
      yield* watchDirs;
      const { mirrored, missed } = yield* serially(check);
      if (catchingUp) yield* Effect.logInfo(`mirrored ${mirrored} rollout file(s) changed since the store last saw them`);
      else if (missed > 0) yield* Effect.logWarning(`the check mirrored ${missed} change(s) no watch reported`);
      catchingUp = false;
    }).pipe(
      Effect.tapError((error) => failed(error).pipe(Effect.andThen(Effect.sync(() => (catchingUp = true))))),
    );

    yield* checkEverything.pipe(
      Effect.retry(Schedule.spaced(RETRY_AFTER)),
      Effect.repeat(Schedule.spaced(CHECK_EVERY)),
      Effect.forkScoped,
    );
    yield* mirrorReports.pipe(Effect.forever, Effect.forkScoped);

    return {
      restore: (threadIds) => restoreRollouts({ store, threadIds, home }),

      flush: (threadId) =>
        serially(Effect.fnUntraced(function*(known) {
          for (const file of yield* mirroring(() => listRolloutFiles(home))) {
            if (parseRolloutName(file.name)?.threadId === threadId) yield* mirror(known, file);
          }
        })).pipe(
          // A flush given up on is still mirrored, in its turn: it runs in the mirror's scope.
          Effect.forkIn(scope),
          Effect.flatMap(Fiber.join),
          Effect.timeoutOrElse({ duration: FLUSH_TIMEOUT_MS, orElse: () => Effect.fail(new RolloutFlushTimeout()) }),
          withLogScope("codex-rollouts"),
        ),
    };
  },
  withLogScope("codex-rollouts"),
);

/** The operator's Codex home's rollouts, kept in alasio's Neon. */
export class CodexRollouts extends Context.Service<CodexRollouts, KeptCodexRollouts>()(
  "alasio/codex/rollouts/CodexRollouts",
  { make: makeCodexRollouts },
) {}

/** The session-filesystem Codex home's rollouts (codex/sessionfs.ts), kept in a schema of their own. */
export class SessionFsCodexRollouts extends Context.Service<SessionFsCodexRollouts, KeptCodexRollouts>()(
  "alasio/codex/rollouts/SessionFsCodexRollouts",
  { make: makeCodexRollouts },
) {}
