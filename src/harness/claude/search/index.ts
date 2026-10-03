/**
 * Transcript search: keeps the session store's entries searchable, from
 * beside the store. While alasio runs, the indexer reads new entries into
 * passages within seconds of their append, and on a first start works
 * through every entry already stored.
 *
 * It is not on the SDK's path: an append never waits on it, and if it falls
 * behind or fails, it catches up when it can. Search itself is SQL,
 * claude_sessions.search() (schema.ts), for any client to call.
 */
import { Clock, Duration, Effect, Schedule, Schema, type Scope } from "effect";
import type { Pool } from "pg";

import { withLogScope } from "../../../shared/log.ts";
import { DEFAULT_SCHEMA } from "../session-store.ts";
import { collectOrphans, indexBatch, settle } from "./indexer.ts";
import { ensureSearchSchema } from "./schema.ts";

/** How long the indexer, with nothing to read, waits before looking again. */
const IDLE = "2 seconds";
/** How long it waits after an error. */
const RETRY_AFTER = Duration.seconds(60);
/** How often passages no entry holds are dropped. */
const ORPHANS_EVERY_MS = 60 * 60 * 1000;

/** What indexing failed with, as the database said it. */
export class TranscriptIndexError extends Schema.TaggedError<TranscriptIndexError>()("TranscriptIndexError", {
  cause: Schema.Defect(),
}) {
  override get message(): string {
    return this.cause instanceof Error ? this.cause.message : String(this.cause);
  }
}

/** The session store's pool, and the schema its entries are in. */
export interface TranscriptSearchOptions {
  readonly pool: Pool;
  readonly schema?: string;
}

/** What one pass of the indexer did: read entries, so that more may be waiting, or found none. */
type Pass = "read" | "idle";

/**
 * Keeps the store's entries searchable until the scope closes: the indexer runs in a
 * fiber of the scope, a pass at a time, and stopping interrupts it between passes.
 */
export const indexTranscripts = Effect.fnUntraced(
  function*({ pool, schema = DEFAULT_SCHEMA }: TranscriptSearchOptions): Effect.fn.Return<void, never, Scope.Scope> {
    let ready = false;
    let caughtUp = false;
    let orphansAt: number | null = null;
    let failing = false;

    const indexing = <A>(query: () => Promise<A>): Effect.Effect<A, TranscriptIndexError> =>
      Effect.tryPromise({ try: query, catch: (cause) => new TranscriptIndexError({ cause }) });

    /** One pass: a batch of entries, or when there are none, upkeep. */
    const pass: Effect.Effect<Pass, TranscriptIndexError> = Effect.gen(function*() {
      if (!ready) {
        yield* indexing(() => ensureSearchSchema(pool, schema));
        ready = true;
      }
      if ((yield* indexing(() => indexBatch(pool, schema))) > 0) return "read";
      if (!caughtUp) yield* Effect.logInfo("every stored entry is indexed");
      caughtUp = true;
      const now = yield* Clock.currentTimeMillis;
      yield* indexing(() => settle(pool, schema, { now }));
      if (orphansAt === null || now - orphansAt > ORPHANS_EVERY_MS) {
        const dropped = yield* indexing(() => collectOrphans(pool, schema));
        if (dropped > 0) yield* Effect.logInfo(`dropped ${dropped} passage(s) no entry holds any more`);
        orphansAt = yield* Clock.currentTimeMillis;
      }
      return "idle";
    });

    // An error is logged once, until indexing succeeds again, and waited out.
    const passOrRetry = pass.pipe(
      Effect.uninterruptible,
      Effect.tap(() =>
        Effect.gen(function*() {
          if (failing) yield* Effect.logInfo("indexing is running again");
          failing = false;
        })
      ),
      Effect.tapError((error) =>
        Effect.gen(function*() {
          if (!failing) {
            yield* Effect.logWarning(`indexing failed, and retries every ${Duration.toSeconds(RETRY_AFTER)}s: ${error.message}`);
          }
          failing = true;
        })
      ),
      Effect.retry(Schedule.spaced(RETRY_AFTER)),
    );

    // A pass that read entries is followed at once; one that found none, after IDLE.
    const afterPass = Schedule.spaced(IDLE).pipe(
      Schedule.setInputType<Pass>(),
      Schedule.modifyDelay(({ input, duration }) => Effect.succeed(input === "read" ? Duration.zero : duration)),
    );

    yield* passOrRetry.pipe(Effect.repeat(afterPass), Effect.forkScoped);
  },
  withLogScope("transcript-search"),
);
