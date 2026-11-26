/**
 * The reads of the restores whose checkpoint gofers read the snapshots bucket straight from the
 * store, by URLs the agent presigned for each file they read (./agent.ts goferOptions), as the
 * agent follows them: not on their path, as through its proxy (./store-proxy.ts), but by what each
 * gofer counts in a file of its own (its options' `served_file`): the bytes of pages files it read
 * from the store, each counted once, as they arrive, and the GETs of them it made, answered or
 * not, the file replaced whole as they grow.
 *
 * A restore is followed from its prepare (follow) for as long as the proxy's token would last
 * (PREPARED_FOR_MS), and from its resume as long as the agent says its background reads may take
 * (resumed), and for as long after as either count keeps growing, as a token asked of again
 * lasts: each look that finds one grown has it followed ASKED_WINDOW_MS from then at least, never
 * past RESUMED_CEILING_MS from its resume. A store away for a while, which its gofer asks again
 * and again, delivering nothing, so delays its reads rather than ends its following, and its URLs'
 * signing, under them: its URLs are signed on, and taken once the store is back, where they would
 * have lapsed, and been refused. It is done reading once its gofer has read the bytes of pages its
 * image holds, where the agent told them (expect), which ends its following, said once; where it
 * did not, once it is resumed and neither count has grown for QUIET_MS. Its restore done with
 * (revoke), it is followed no more. What it is followed by is kept, where told (file), in a file
 * on the node's tmpfs, which an agent that starts again reads back: its gofer reads on without the
 * agent, and its count is on the node. One read back resumed and within its ceiling is followed
 * ASKED_WINDOW_MS from the agent's start at least; one not resumed yet past its time, and one past
 * its ceiling, are dropped. A file that cannot be read gives none, said loudly, and stops nothing.
 *
 * Its URLs, one of each object it reads, by the path its gofer serves it at, are in a file of its
 * own on the node's tmpfs, which only root reads (its options' `urls_file`), each lasting
 * URL_TTL_MS: as long as it is followed, they are signed anew every third of that, and as the agent
 * starts, those of each restore it reads back (where they cannot be, again soon, after a while that
 * doubles from SIGN_RETRY_FIRST_MS), and its gofer reads the file again as one nears its
 * expiry, or once as the store refuses one, where the file changed. A store away for longer than
 * that delays the restore's reads, which go on by new URLs once it is back. Unlike a token, a URL
 * cannot be revoked: once its restore is followed no more, its file is removed and its URLs
 * signed no more, so that each lapses within URL_TTL_MS; its reads end before then as its load
 * does, at its end or as the agent fails it (runsc fail-load).
 */
import { randomBytes } from "node:crypto";
import { mkdirSync, readdirSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";

import { Cause, Effect, Schedule, Schema, type Scope } from "effect";

import { S3Error } from "./s3.ts";
import { ASKED_WINDOW_MS, PREPARED_FOR_MS, QUIET_MS, RESUMED_CEILING_MS } from "./store-proxy.ts";

/** How soon URLs that could not be signed are signed again first, the while doubling each time they cannot be, up to a third of their lifetime. */
export const SIGN_RETRY_FIRST_MS = 1_000;

/**
 * How long each URL a restore's gofer reads by lasts, as the module says: longer than twice what
 * an agent's start again takes, as it signs them anew once started, and short enough that those of
 * a restore followed no more lapse soon after.
 */
export const URL_TTL_MS = 20 * 60_000;

/** The reads of the restores whose gofers read the bucket by URLs, as the module says. */
export interface GoferReads {
  /**
   * Follows the reads of the restore `holder` names, whose gofer counts the bytes of pages it read
   * in `servedFile` and reads `objects`, each of the bucket by the path it serves it at, for
   * PREPARED_FOR_MS unless it is resumed before: the id of its reads, which expect and readsDone
   * take, as a later restore of the same workspace is another's, and the file of its URLs, signed
   * as the module says, which its gofer reads them from. One it followed before is followed no
   * more. Fails where its URLs could not be signed or written, the restore not followed.
   */
  readonly follow: (holder: string, servedFile: string, objects: Readonly<Record<string, string>>) => Effect.Effect<{ readonly reader: string; readonly urlsFile: string }, S3Error>;
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

/**
 * What makeGoferReads is given: the directory of its restores' files of URLs, on the node's tmpfs,
 * and what signs a URL of an object for so many seconds (./store-urls.ts); the file it keeps what
 * it follows in, if any; and, unless told, URL_TTL_MS, SIGN_RETRY_FIRST_MS, PREPARED_FOR_MS,
 * QUIET_MS, ASKED_WINDOW_MS and RESUMED_CEILING_MS.
 */
export interface GoferReadsOptions {
  readonly urlsDir: string;
  readonly presign: (key: string, expiresSeconds: number) => Effect.Effect<string, S3Error>;
  readonly file?: string;
  readonly urlTtlMs?: number;
  readonly signRetryFirstMs?: number;
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
  /** The objects its gofer reads, by the path it serves each at. */
  readonly objects: Readonly<Record<string, string>>;
  /** Until when it is followed, in milliseconds since the epoch. */
  until: number;
  resumed: boolean;
  /** Until when it is followed at the latest: its prepare's end, and once resumed, RESUMED_CEILING_MS from then. */
  ceiling: number;
  /** The bytes of pages its restore reads, where the agent told them. */
  expected: number | null;
  /** The bytes its gofer said last it read, and the GETs it said it made; when either last grew, or it was followed or resumed. */
  served: number;
  gets: number;
  grewAt: number;
}

/** The file of what is followed (file): each restore, by the id of its reads, and how long it is followed. */
const FollowedFile = Schema.fromJsonString(Schema.Struct({
  schemaVersion: Schema.Literal(1),
  restores: Schema.Array(Schema.Struct({
    reader: Schema.String,
    holder: Schema.String,
    servedFile: Schema.String,
    objects: Schema.Record(Schema.String, Schema.String),
    until: Schema.Number,
    resumed: Schema.Boolean,
    ceiling: Schema.Number,
    expected: Schema.NullOr(Schema.Number),
  })),
}));

/** What a gofer's served file holds: the bytes of pages it read and the GETs of them it made, decimal numbers, a space between, and a newline. */
const SERVED = /^(\d+) (\d+)\n$/u;

/** What the gofer counting in `servedFile` says it read, and asked, or null where it says nothing yet, as one not started has written nothing. */
function servedIn(servedFile: string): { readonly bytes: number; readonly gets: number } | null {
  try {
    const said = SERVED.exec(readFileSync(servedFile, "utf8"));
    return said === null ? null : { bytes: Number(said[1]), gets: Number(said[2]) };
  } catch {
    return null;
  }
}

/** Writes `text` to `file`, whole or not at all, only root reading it. */
function writeWhole(file: string, text: string): void {
  writeFileSync(`${file}.new`, text, { mode: 0o600 });
  renameSync(`${file}.new`, file);
}

/** The reads of the restores whose gofers read the bucket by URLs, as the module says, signing their URLs anew for as long as the scope is open. */
export const makeGoferReads = Effect.fnUntraced(function*({
  urlsDir,
  presign,
  file,
  urlTtlMs = URL_TTL_MS,
  signRetryFirstMs = SIGN_RETRY_FIRST_MS,
  preparedForMs = PREPARED_FOR_MS,
  quietMs = QUIET_MS,
  askedWindowMs = ASKED_WINDOW_MS,
  resumedCeilingMs = RESUMED_CEILING_MS,
}: GoferReadsOptions): Effect.fn.Return<GoferReads, never, Scope.Scope> {
  const followed = new Map<string, Followed>();
  const held = new Map<string, string>();
  // What the agent's logger says, from plain calls.
  const runSync = Effect.runSyncWith(yield* Effect.context<never>());
  const urlSeconds = Math.ceil(urlTtlMs / 1000);
  const urlsFileOf = (reader: string): string => join(urlsDir, `${reader}.json`);

  /**
   * Signs a URL of each of `objects` for urlSeconds, and writes them to the file of the reads
   * `reader`, whole, where `wanted` still holds once they are signed: a restore followed no more
   * meanwhile has none written.
   */
  const sign = (reader: string, objects: Readonly<Record<string, string>>, wanted: () => boolean): Effect.Effect<void, S3Error> =>
    Effect.forEach(Object.entries(objects), ([path, key]) => Effect.map(presign(key, urlSeconds), (url) => [path, url] as const)).pipe(
      Effect.flatMap((urls) =>
        Effect.try({
          try: () => {
            if (!wanted()) return;
            mkdirSync(urlsDir, { recursive: true, mode: 0o700 });
            writeWhole(urlsFileOf(reader), JSON.stringify(Object.fromEntries(urls)));
          },
          catch: (cause) => new S3Error({ operation: "signing of URLs", key: urlsFileOf(reader), reason: `they could not be written: ${cause instanceof Error ? cause.message : String(cause)}` }),
        })
      ),
    );

  /** Writes what is followed to `file`, if told one, whole or not at all: one not written is said, and changes nothing else. */
  const keep = (): void => {
    if (file === undefined) return;
    const written = JSON.stringify({
      schemaVersion: 1,
      restores: [...followed].map(([reader, { holder, servedFile, objects, until, resumed, ceiling, expected }]) => ({ reader, holder, servedFile, objects, until, resumed, ceiling, expected })),
    });
    try {
      mkdirSync(dirname(file), { recursive: true, mode: 0o700 });
      writeWhole(file, written);
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
          const counted = servedIn(restore.servedFile);
          followed.set(reader, { ...restore, until, served: counted?.bytes ?? 0, gets: counted?.gets ?? 0, grewAt: now });
          held.set(restore.holder, reader);
        }
        if (followed.size > 0) runSync(Effect.logInfo(`the node agent took back the following of ${followed.size} restores reading the snapshots bucket by URLs`));
      }
    }
  }

  /** Removes the file of the URLs of the reads `reader`; one not removed is said. */
  const removeUrls = (reader: string): void => {
    try {
      rmSync(urlsFileOf(reader), { force: true });
    } catch (error) {
      runSync(Effect.logWarning(`the node agent could not remove the URLs of a restore it follows no more, ${urlsFileOf(reader)}, which lapse as they expire: ${error instanceof Error ? error.message : String(error)}`));
    }
  };

  /** Forgets the restore of the reads `reader`, `restore`, its URLs' file removed: they are signed no more. */
  const forget = (reader: string, restore: Followed): void => {
    followed.delete(reader);
    if (held.get(restore.holder) === reader) held.delete(restore.holder);
    removeUrls(reader);
    keep();
  };

  /**
   * Signs anew the URLs of each restore still followed, as the module says: whether every one
   * was. One not signed is said, and signed again soon (signRetryFirstMs).
   */
  const signAgain: Effect.Effect<boolean> = Effect.suspend(() =>
    Effect.forEach([...followed.keys()], (reader) => {
      doneReading(reader);
      const restore = followed.get(reader);
      if (restore === undefined) return Effect.succeed(true);
      return sign(reader, restore.objects, () => followed.has(reader)).pipe(
        Effect.as(true),
        Effect.catch((error) =>
          Effect.logWarning(`could not sign anew the URLs the restore of ${restore.holder} reads the snapshots bucket by, so its reads fail once they expire unless the next try does: ${error.message}`).pipe(Effect.as(false))
        ),
      );
    }).pipe(Effect.map((signed) => signed.every(Boolean)))
  );

  /**
   * Whether the restore of the reads `reader` is done reading, as the module says, its count
   * looked at anew: one done by its bytes, its load complete, is followed no more, said once;
   * one past its time too.
   */
  const doneReading = (reader: string): boolean => {
    const restore = followed.get(reader);
    if (restore === undefined) return true;
    const now = Date.now();
    const counted = servedIn(restore.servedFile);
    if (counted !== null && (counted.bytes > restore.served || counted.gets > restore.gets)) {
      restore.served = Math.max(restore.served, counted.bytes);
      restore.gets = Math.max(restore.gets, counted.gets);
      restore.grewAt = now;
      // Its gofer reading still, or asking still, as of a store away, it is followed the longer.
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

  // The URLs of those read back signed anew at once, as theirs may have neared their expiry while the agent was away, and
  // those of restores it follows no more removed; and every third of their lifetime after, or, where some were not
  // signed, again soon, after a while that doubles from signRetryFirstMs up to that third.
  const signedAll = yield* signAgain;
  try {
    for (const name of readdirSync(urlsDir)) if (!followed.has(name.replace(/\.json(\.new)?$/u, ""))) rmSync(join(urlsDir, name), { force: true });
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") runSync(Effect.logWarning(`the node agent could not remove the URLs of restores it follows no more, in ${urlsDir}, which lapse as they expire: ${error instanceof Error ? error.message : String(error)}`));
  }
  yield* Effect.forkScoped(Effect.gen(function*() {
    let retryMs: number | null = signedAll ? null : signRetryFirstMs;
    for (;;) {
      yield* Effect.sleep(retryMs ?? urlTtlMs / 3);
      retryMs = (yield* signAgain) ? null : Math.min(urlTtlMs / 3, retryMs === null ? signRetryFirstMs : retryMs * 2);
    }
  }));

  return {
    follow: (holder, servedFile, objects) => {
      const reader = `gofer-${randomBytes(10).toString("hex")}`;
      return sign(reader, objects, () => true).pipe(
        Effect.andThen(Effect.sync(() => {
          const before = held.get(holder);
          const restore = followed.get(before ?? "");
          if (before !== undefined && restore !== undefined) forget(before, restore);
          const now = Date.now();
          followed.set(reader, { holder, servedFile, objects, until: now + preparedForMs, resumed: false, ceiling: now + preparedForMs, expected: null, served: 0, gets: 0, grewAt: now });
          held.set(holder, reader);
          keep();
          return { reader, urlsFile: urlsFileOf(reader) };
        })),
      );
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
