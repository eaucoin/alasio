/**
 * The reads of the restores whose checkpoint gofers read the snapshots bucket straight from the
 * store, by URLs the agent presigned for each file they read (./agent.ts goferOptions), as the
 * agent follows them: not on their path, as through its proxy (./store-proxy.ts), but by what each
 * gofer counts in a file of its own (its options' `served_file`), the bytes of pages files it read
 * from the store, each counted once, as they arrive, the file replaced whole as the count grows.
 *
 * A restore is followed from its prepare (follow) for as long as the proxy's token would last
 * (PREPARED_FOR_MS), and from its resume as long as the agent says its background reads may take
 * (resumed), and for as long after as its count keeps growing: each look that finds it grown has
 * it followed ASKED_WINDOW_MS from then at least, never past RESUMED_CEILING_MS from its resume,
 * so that a store away for a while, which its gofer waits for, delays its reads rather than ends
 * its following under them. It is done reading once its gofer has read the bytes of pages its
 * image holds, where the agent told them (expect), which ends its following, said once; where it
 * did not, once it is resumed and its count has not grown for QUIET_MS. Its restore done with
 * (revoke), it is followed no more. What it is followed by is kept, where told (file), in a file
 * on the node's tmpfs, which an agent that starts again reads back: its gofer reads on without the
 * agent, and its count is on the node. One read back resumed and within its ceiling is followed
 * ASKED_WINDOW_MS from the agent's start at least; one not resumed yet past its time, and one past
 * its ceiling, are dropped. A file that cannot be read gives none, said loudly, and stops nothing.
 *
 * Unlike a token's, a restore's URLs cannot be revoked: its reads end as its load does, at its end
 * or as the agent fails it (runsc fail-load), or as its URLs expire.
 */
import { randomBytes } from "node:crypto";
import { mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";

import { Cause, Effect, Schedule, Schema } from "effect";

import { ASKED_WINDOW_MS, PREPARED_FOR_MS, QUIET_MS, RESUMED_CEILING_MS } from "./store-proxy.ts";

/** The reads of the restores whose gofers read the bucket by URLs, as the module says. */
export interface GoferReads {
  /**
   * Follows the reads of the restore `holder` names, whose gofer counts the bytes of pages it read
   * in `servedFile`, for PREPARED_FOR_MS unless it is resumed before: the id of its reads, which
   * expect and readsDone take, as a later restore of the same workspace is another's. One it
   * followed before is followed no more.
   */
  readonly follow: (holder: string, servedFile: string) => string;
  /** Whether the restore `holder` names is followed, its gofer reading the bucket by URLs. */
  readonly follows: (holder: string) => boolean;
  /** Follows the restore `holder` names no more, as it is done with. */
  readonly revoke: (holder: string) => void;
  /** The restore `holder` names, if followed, is resumed: it is followed `ms` from now, and as the module says after. */
  readonly resumed: (holder: string, ms: number) => void;
  /** The restore of the reads `reader` reads `bytes` bytes of pages, as runsc says of its image; Infinity while not known yet, null where not known. */
  readonly expect: (reader: string, bytes: number | null) => void;
  /** Ends once the restore of the reads `reader` is done reading, as the module says, or is followed no more. */
  readonly readsDone: (reader: string) => Effect.Effect<void>;
  /** How far the restore `holder` names has read, while it reads still: the bytes of pages its gofer read, and those it reads; null where it is done, or not followed. */
  readonly readingOf: (holder: string) => { readonly served: number; readonly expected: number | null } | null;
}

/** What makeGoferReads is given: the file it keeps what it follows in, if any, and, unless told, PREPARED_FOR_MS, QUIET_MS, ASKED_WINDOW_MS and RESUMED_CEILING_MS. */
export interface GoferReadsOptions {
  readonly file?: string;
  readonly preparedForMs?: number;
  readonly quietMs?: number;
  readonly askedWindowMs?: number;
  readonly resumedCeilingMs?: number;
}

/** How often what waits on a restore's reads looks at them. */
const READS_POLL = "1 second";

/** A restore followed, and how its reads go. */
interface Followed {
  readonly holder: string;
  readonly servedFile: string;
  /** Until when it is followed, in milliseconds since the epoch. */
  until: number;
  resumed: boolean;
  /** Until when it is followed at the latest: its prepare's end, and once resumed, RESUMED_CEILING_MS from then. */
  ceiling: number;
  /** The bytes of pages its restore reads, where the agent told them. */
  expected: number | null;
  /** The bytes its gofer said last it read, and when that last grew, or it was followed or resumed. */
  served: number;
  grewAt: number;
}

/** The file of what is followed (file): each restore, by the id of its reads, and how long it is followed. */
const FollowedFile = Schema.fromJsonString(Schema.Struct({
  schemaVersion: Schema.Literal(1),
  restores: Schema.Array(Schema.Struct({
    reader: Schema.String,
    holder: Schema.String,
    servedFile: Schema.String,
    until: Schema.Number,
    resumed: Schema.Boolean,
    ceiling: Schema.Number,
    expected: Schema.NullOr(Schema.Number),
  })),
}));

/** What a gofer's served file holds: a decimal number and a newline. */
const SERVED = /^(\d+)\n$/u;

/** The bytes the gofer counting in `servedFile` says it read, or null where it says none yet, as one not started has written none. */
function servedIn(servedFile: string): number | null {
  try {
    const said = SERVED.exec(readFileSync(servedFile, "utf8"))?.[1];
    return said === undefined ? null : Number(said);
  } catch {
    return null;
  }
}

/** The reads of the restores whose gofers read the bucket by URLs, as the module says. */
export const makeGoferReads = Effect.fnUntraced(function*({
  file,
  preparedForMs = PREPARED_FOR_MS,
  quietMs = QUIET_MS,
  askedWindowMs = ASKED_WINDOW_MS,
  resumedCeilingMs = RESUMED_CEILING_MS,
}: GoferReadsOptions = {}): Effect.fn.Return<GoferReads> {
  const followed = new Map<string, Followed>();
  const held = new Map<string, string>();
  // What the agent's logger says, from plain calls.
  const runSync = Effect.runSyncWith(yield* Effect.context<never>());

  /** Writes what is followed to `file`, if told one, whole or not at all: one not written is said, and changes nothing else. */
  const keep = (): void => {
    if (file === undefined) return;
    const written = JSON.stringify({
      schemaVersion: 1,
      restores: [...followed].map(([reader, { holder, servedFile, until, resumed, ceiling, expected }]) => ({ reader, holder, servedFile, until, resumed, ceiling, expected })),
    });
    try {
      mkdirSync(dirname(file), { recursive: true, mode: 0o700 });
      writeFileSync(`${file}.new`, written, { mode: 0o600 });
      renameSync(`${file}.new`, file);
    } catch (error) {
      runSync(Effect.logWarning(`the node agent could not keep the restores it follows the reads of in ${file}, which an agent that starts again would read back: ${error instanceof Error ? error.message : String(error)}`));
    }
  };

  if (file !== undefined) {
    const now = Date.now();
    let text: string | null = null;
    try {
      text = readFileSync(file, "utf8");
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
        runSync(Effect.logError(`the node agent cannot read back the restores it followed the reads of, ${file}: their saves are not bounded as they load, nor deferred (${error instanceof Error ? error.message : String(error)})`));
      }
    }
    if (text !== null) {
      const decoded = Schema.decodeUnknownExit(FollowedFile)(text);
      if (decoded._tag === "Failure") {
        runSync(Effect.logError(`the node agent cannot read back the restores it followed the reads of, ${file}: their saves are not bounded as they load, nor deferred (${Cause.pretty(decoded.cause)})`));
      } else {
        for (const { reader, ...restore } of decoded.value.restores) {
          // Its gofer read on as the agent was away: followed askedWindowMs from now at least, within its ceiling.
          const until = restore.resumed && now < restore.ceiling ? Math.max(restore.until, Math.min(restore.ceiling, now + askedWindowMs)) : restore.until;
          if (until <= now) continue;
          followed.set(reader, { ...restore, until, served: servedIn(restore.servedFile) ?? 0, grewAt: now });
          held.set(restore.holder, reader);
        }
        if (followed.size > 0) runSync(Effect.logInfo(`the node agent took back the following of ${followed.size} restores reading the snapshots bucket by URLs`));
      }
    }
  }

  /** Forgets the restore of the reads `reader`, `restore`. */
  const forget = (reader: string, restore: Followed): void => {
    followed.delete(reader);
    if (held.get(restore.holder) === reader) held.delete(restore.holder);
    keep();
  };

  /**
   * Whether the restore of the reads `reader` is done reading, as the module says, its count
   * looked at anew: one done by its bytes, its load complete, is followed no more, said once;
   * one past its time too.
   */
  const doneReading = (reader: string): boolean => {
    const restore = followed.get(reader);
    if (restore === undefined) return true;
    const now = Date.now();
    const served = servedIn(restore.servedFile);
    if (served !== null && served > restore.served) {
      restore.served = served;
      restore.grewAt = now;
      // Its gofer reading still, it is followed the longer.
      if (restore.resumed) restore.until = Math.max(restore.until, Math.min(restore.ceiling, now + askedWindowMs));
    }
    if (now >= restore.until) {
      forget(reader, restore);
      return true;
    }
    if (restore.expected === null) return restore.resumed && now - restore.grewAt >= quietMs;
    if (restore.served < restore.expected) return false;
    forget(reader, restore);
    runSync(Effect.logInfo(`the restore of ${restore.holder} has read all ${restore.expected} bytes of its pages from the bucket`));
    return true;
  };

  return {
    follow: (holder, servedFile) => {
      const before = held.get(holder);
      if (before !== undefined) followed.delete(before);
      const reader = `gofer-${randomBytes(10).toString("hex")}`;
      const now = Date.now();
      followed.set(reader, { holder, servedFile, until: now + preparedForMs, resumed: false, ceiling: now + preparedForMs, expected: null, served: 0, grewAt: now });
      held.set(holder, reader);
      keep();
      return reader;
    },
    follows: (holder) => held.has(holder),
    revoke: (holder) => {
      const reader = held.get(holder);
      const restore = followed.get(reader ?? "");
      if (reader !== undefined && restore !== undefined) forget(reader, restore);
    },
    resumed: (holder, ms) => {
      const restore = followed.get(held.get(holder) ?? "");
      if (restore === undefined) return;
      const now = Date.now();
      restore.ceiling = now + resumedCeilingMs;
      restore.until = Math.min(restore.ceiling, now + ms);
      restore.resumed = true;
      restore.grewAt = now;
      keep();
    },
    expect: (reader, bytes) => {
      const restore = followed.get(reader);
      if (restore === undefined) return;
      restore.expected = bytes;
      keep();
      doneReading(reader);
    },
    readsDone: (reader) =>
      Effect.sync(() => doneReading(reader)).pipe(
        Effect.repeat({ schedule: Schedule.spaced(READS_POLL), until: (done) => done }),
        Effect.asVoid,
      ),
    readingOf: (holder) => {
      const reader = held.get(holder);
      if (reader === undefined || doneReading(reader)) return null;
      const restore = followed.get(reader);
      return restore === undefined ? null : { served: restore.served, expected: restore.expected };
    },
  };
});
