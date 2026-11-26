/**
 * Neon's bucket's expiry, on the bundled object store: what of the bucket Neon needs no more,
 * deleted for good, version by version, by its id, by a container of the store's hourly
 * lifecycle pass (cli/src/manifests/object-store.ts), as the bucket's admin. The bucket is
 * versioned, as Neon's time-travel recovery needs, so what Neon overwrites or deletes is kept
 * as a noncurrent version, a deletion leaving a delete marker; and Neon deletes none of a
 * timeline's full WAL segments while it lives. Each pass lists every version of the bucket
 * and deletes (expired):
 *
 * - the pageserver's noncurrent versions, under pageserver/, RESTORABLE_DAYS after they were
 *   replaced, and never a current one: its time-travel recovery puts the bucket back as it
 *   was at a moment of those days from the versions each key held then
 *   (time_travel_recover_tenant, pageserver/src/tenant/remote_timeline_client/upload.rs at
 *   release-9129), which one replaced since was;
 * - the safekeepers' noncurrent versions, under safekeeper/, a day after they were replaced:
 *   their partial segments, each deleted as the next is uploaded, which nothing reads after;
 * - the safekeepers' full WAL segments, every version of each, once written a day past
 *   RESTORABLE_DAYS ago and wholly a segment or more below the least of the remote consistent
 *   LSNs their timeline had over the last RESTORABLE_DAYS, as the passes recorded them
 *   (walHorizons). A safekeeper reads WAL from the bucket
 *   only below where its own volume holds it (open_segment, safekeeper/src/wal_storage.rs),
 *   which it keeps down to the pageserver's remote consistent LSN (calc_horizon_lsn,
 *   safekeeper/src/remove_wal.rs): what the bucket's WAL serves lies below that LSN, to a
 *   safekeeper that was away, from where it stopped (one away for longer is rebuilt from its
 *   peers once its volume is deleted), and to a pageserver put back by time travel as it was
 *   at a moment of the last RESTORABLE_DAYS, which ingests again from the remote consistent
 *   LSN it had then. Its age alone would not keep that WAL where the pageserver lagged, as one
 *   stopped for days does; the least LSN the timeline had over the window, which a time-travel
 *   restore lowers, does, whatever the pageserver did. A full segment's other versions go with
 *   it alone; one Neon deleted, its latest a delete marker, as it deletes a timeline's WAL with
 *   the timeline, is noncurrent as a partial segment is. The segment each safekeeper writes is
 *   not full, so it is kept, however long it goes unchanged: a safekeeper rebuilt from its
 *   peers copies it;
 * - delete markers left their key's only version, which hide nothing, in a request of their
 *   own once every other version of their key is deleted, so that no version it hides is
 *   found current again.
 *
 * A key whose versions the bucket lists in a way no S3 lists them, more than one its latest,
 * or one newer than its latest, is left as it is, and said.
 *
 * SeaweedFS's own lifecycle rules would not do it, which expire by age alone: a full WAL
 * segment is kept by where its timeline's remote consistent LSNs were, whatever its age.
 * An operator's store (objectStore.external) is expired by none of this: its operator gives
 * Neon's bucket lifecycle rules of these ages.
 *
 * Environment: S3_ENDPOINT, S3_REGION, S3_BUCKET_NEON, AWS_ACCESS_KEY_ID and
 * AWS_SECRET_ACCESS_KEY; STORAGE_CONTROLLER_URL and STORAGE_CONTROLLER_TOKEN, a token of the
 * scrubber's scope, which the storage controller takes for describing a tenant or a timeline,
 * and for recording a scrub's findings, which the expiry never asks, and nothing else, and the
 * pageservers and safekeepers for nothing (./kube-setup.ts); and, for a test alone,
 * EXPIRY_TEST_AGES_VARIABLE.
 */
import { Effect, Schema } from "effect";

import { type ListedVersion, makeS3Bucket, type ObjectVersion, type RefusedVersion, type S3Bucket, type S3Error } from "../../src/node-agent/s3.ts";

import { HISTORY_DAYS } from "./branches.ts";

const DAY_MS = 24 * 60 * 60_000;

/**
 * The days back to which Neon's time-travel recovery can put the pageserver's objects: the
 * history Neon keeps (HISTORY_DAYS), and a day more to notice a deletion that should not
 * have been.
 */
export const RESTORABLE_DAYS = HISTORY_DAYS + 1;

/**
 * How long after it was replaced each noncurrent version is kept, by its part of the bucket;
 * each full WAL segment after it was written; and how long ago a remote consistent LSN a full
 * segment must lie below was the timeline's.
 */
export interface ExpiryAges {
  readonly pageserverNoncurrentMs: number;
  readonly safekeeperNoncurrentMs: number;
  readonly walSegmentMs: number;
  readonly walHorizonMs: number;
}

/** The ages the module says. */
export const EXPIRY_AGES: ExpiryAges = {
  pageserverNoncurrentMs: RESTORABLE_DAYS * DAY_MS,
  safekeeperNoncurrentMs: DAY_MS,
  walSegmentMs: (RESTORABLE_DAYS + 1) * DAY_MS,
  walHorizonMs: RESTORABLE_DAYS * DAY_MS,
};

/**
 * The variable that gives the expiry, for a test alone, ages of its own, as JSON (ExpiryAges,
 * whole ms each, any of them): shorter than an installation keeps Neon's objects, as the
 * end-to-end run's proof of the expiry does. No installation's manifests set it.
 */
export const EXPIRY_TEST_AGES_VARIABLE = "ALASIO_NEON_EXPIRY_TEST_AGES";

const Age = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0, { message: "must be 0 ms or more" }));
/** What EXPIRY_TEST_AGES_VARIABLE gives. */
const ExpiryTestAges = Schema.fromJsonString(Schema.Struct({
  pageserverNoncurrentMs: Schema.optionalKey(Age),
  safekeeperNoncurrentMs: Schema.optionalKey(Age),
  walSegmentMs: Schema.optionalKey(Age),
  walHorizonMs: Schema.optionalKey(Age),
}));

/** The ages a pass keeps Neon's objects for: EXPIRY_AGES, but for what `testAges`, EXPIRY_TEST_AGES_VARIABLE's value where set, gives. */
export const expiryAges = (testAges: string | undefined): Effect.Effect<ExpiryAges, Schema.SchemaError> =>
  testAges === undefined ? Effect.succeed(EXPIRY_AGES) : Schema.decodeUnknownEffect(ExpiryTestAges)(testAges).pipe(Effect.map((ages) => ({ ...EXPIRY_AGES, ...ages })));

/** The pageserver's part of the bucket, and the safekeepers'. */
const PAGESERVER = "pageserver/";
const SAFEKEEPER = "safekeeper/";

/**
 * Where the expiry keeps, in the bucket, under neither part, the remote consistent LSNs it has
 * read of each timeline with full segments, and when (Horizons): each pass writes it anew, and
 * deletes its versions replaced.
 */
export const HORIZONS_KEY = ".alasio/neon-expiry/horizons.json";

/** The bytes of a WAL segment, Postgres's own, which Neon's are. */
const SEGMENT_BYTES = 16n * 1024n * 1024n;

/**
 * A full WAL segment's key: safekeeper/<tenant>/<timeline>/<segment>, the segment's name 24
 * hexadecimal digits, its timeline, log and segment number, as Neon's safekeepers upload each
 * once it is whole (safekeeper/src/wal_backup.rs); a partial segment's name goes on past it.
 */
const SEGMENT = /^safekeeper\/([0-9a-f]{32})\/([0-9a-f]{32})\/[0-9A-F]{8}([0-9A-F]{8})([0-9A-F]{8})$/u;

/** The timeline, as `<tenant>/<timeline>`, and the end of the WAL, the LSN past its last byte, of the full segment `key`; null where it is none. */
export function segmentOf(key: string): { readonly timeline: string; readonly end: bigint } | null {
  const match = SEGMENT.exec(key);
  if (match === null) return null;
  const [, tenant = "", timeline = "", log = "", segment = ""] = match;
  // A log of 4 GiB, of 256 segments.
  const number = BigInt(`0x${log}`) * ((1n << 32n) / SEGMENT_BYTES) + BigInt(`0x${segment}`);
  return { timeline: `${tenant}/${timeline}`, end: (number + 1n) * SEGMENT_BYTES };
}

/** An LSN as Neon writes one, `<high>/<low>` in hexadecimal, as a number; null where it is not one. */
export function lsnOf(text: string): bigint | null {
  const match = /^([0-9A-Fa-f]{1,8})\/([0-9A-Fa-f]{1,8})$/u.exec(text);
  return match === null ? null : (BigInt(`0x${match[1]}`) << 32n) + BigInt(`0x${match[2]}`);
}

/** An LSN as Neon writes one, of `lsn`. */
export const lsnText = (lsn: bigint): string => `${(lsn >> 32n).toString(16).toUpperCase()}/${(lsn & 0xFFFFFFFFn).toString(16).toUpperCase()}`;

/** The remote consistent LSNs read of each timeline, by `<tenant>/<timeline>`, and when, oldest first. */
export type Horizons = ReadonlyMap<string, readonly { readonly at: number; readonly lsn: bigint }[]>;

/** Horizons as the bucket keeps them (HORIZONS_KEY). */
const StoredHorizons = Schema.Struct({ timelines: Schema.Record(Schema.String, Schema.Array(Schema.Struct({ at: Schema.Number, lsn: Schema.String }))) });

/** The Horizons `stored`, as the bucket keeps them; none where there are none, or they cannot be read, which keeps the WAL a window longer. */
export function horizonsOf(stored: unknown): Horizons {
  const decoded = Schema.decodeUnknownOption(StoredHorizons)(stored);
  if (decoded._tag === "None") return new Map();
  return new Map(Object.entries(decoded.value.timelines).map(([timeline, samples]) => [
    timeline,
    samples.flatMap(({ at, lsn }) => {
      const read = lsnOf(lsn);
      return read === null ? [] : [{ at, lsn: read }];
    }).sort((a, b) => a.at - b.at),
  ]));
}

/** `horizons` as the bucket keeps them. */
export const storedHorizons = (horizons: Horizons) => ({
  timelines: Object.fromEntries([...horizons].map(([timeline, samples]) => [timeline, samples.map(({ at, lsn }) => ({ at, lsn: lsnText(lsn) }))])),
});

/** A timeline's remote consistent LSN as read now, or why it could not be. */
export type ReadHorizon = { readonly lsn: bigint } | { readonly unknown: string };

/** The span each recorded LSN stands for: those read within one hour are kept as one (recorded). */
const SAMPLE_MS = 60 * 60_000;

/**
 * What the pass keeps of `before` and reads now, `read` at `now`: the timelines with full
 * segments alone, each LSN read now added, and of the older ones those of the last
 * `walHorizonMs` and the newest before them alone, which is all walHorizons reads. Those of
 * one hour are kept as one, the least of their LSNs at the last of their times, which holds
 * back no less than each would: a timeline's record is so of `walHorizonMs` / 1 h + 2 samples
 * at most, 50 by default, some 2.5 KB of JSON, and the record of 400 timelines within the
 * 1 MiB its read takes (expire).
 */
export function recorded(before: Horizons, read: ReadonlyMap<string, ReadHorizon>, now: number, { walHorizonMs }: ExpiryAges): Horizons {
  return new Map([...read].map(([timeline, horizon]) => {
    const samples: { at: number; lsn: bigint }[] = [];
    for (const sample of [...(before.get(timeline) ?? []), ...("lsn" in horizon ? [{ at: now, lsn: horizon.lsn }] : [])].sort((a, b) => a.at - b.at)) {
      const previous = samples.at(-1);
      if (previous !== undefined && Math.floor(previous.at / SAMPLE_MS) === Math.floor(sample.at / SAMPLE_MS)) {
        samples[samples.length - 1] = { at: sample.at, lsn: sample.lsn < previous.lsn ? sample.lsn : previous.lsn };
      } else {
        samples.push({ ...sample });
      }
    }
    const old = samples.filter(({ at }) => at <= now - walHorizonMs);
    return [timeline, [...old.slice(-1), ...samples.filter(({ at }) => at > now - walHorizonMs)]];
  }));
}

/**
 * The LSN below which a full segment of each timeline in `read` may go, at `now`: the least of
 * the newest recorded `walHorizonMs` ago or longer, every one recorded since, and the one read
 * now, so that one lowered meanwhile, as a time-travel restore lowers it, holds the WAL back
 * too; or why there is none, which keeps the timeline's every segment: its LSN not read now, or
 * none recorded so long ago.
 */
export function walHorizons(recordedHorizons: Horizons, read: ReadonlyMap<string, ReadHorizon>, now: number, { walHorizonMs }: ExpiryAges): ReadonlyMap<string, ReadHorizon> {
  return new Map([...read].map(([timeline, horizon]): [string, ReadHorizon] => {
    if (!("lsn" in horizon)) return [timeline, horizon];
    const samples = recordedHorizons.get(timeline) ?? [];
    const then = samples.findLastIndex(({ at }) => at <= now - walHorizonMs);
    if (then < 0) return [timeline, { unknown: `no remote consistent LSN of its has been recorded ${Math.round(walHorizonMs / 3_600_000)} h ago or longer` }];
    const least = samples.slice(then).reduce((lowest, { lsn }) => (lsn < lowest ? lsn : lowest), horizon.lsn);
    return [timeline, { lsn: least }];
  }));
}

/** Why a version is expired, as the module says. */
export type Expiry = "pageserver noncurrent" | "safekeeper noncurrent" | "wal segment" | "lone delete marker" | "own record noncurrent";

/** A version expired, and why. */
export interface Expired {
  readonly version: ListedVersion;
  readonly why: Expiry;
}

/** Full WAL segments of a timeline old enough to expire, kept, of how many bytes, and why. */
export interface KeptSegments {
  readonly timeline: string;
  readonly segments: number;
  readonly bytes: number;
  readonly why: string;
}

/** A key whose versions the bucket lists in a way no S3 lists them, left as it is: which, and why. */
export interface SkippedKey {
  readonly key: string;
  readonly why: string;
}

/**
 * What a pass deletes: `first`, every version expired but the latest of its key, in one or more
 * requests; then `last`, the latest of each key expired whole, each only once every one of its
 * key's versions in `first` is deleted; and the full segments old enough it keeps, and the keys
 * it leaves as they are.
 */
export interface ExpiryPlan {
  readonly first: readonly Expired[];
  readonly last: readonly Expired[];
  readonly kept: readonly KeptSegments[];
  readonly skipped: readonly SkippedKey[];
}

/**
 * `listed`, every version of the bucket as it lists them, by key: its latest first, then the
 * newest first; and the keys whose versions are listed as no S3 lists them.
 */
function byKey(listed: readonly ListedVersion[]): { readonly keys: ListedVersion[][]; readonly skipped: SkippedKey[] } {
  const keys = new Map<string, ListedVersion[]>();
  for (const version of listed) {
    const versions = keys.get(version.key);
    if (versions === undefined) keys.set(version.key, [version]);
    else versions.push(version);
  }
  const ordered: ListedVersion[][] = [];
  const skipped: SkippedKey[] = [];
  for (const [key, versions] of keys) {
    const latest = versions.filter((version) => version.latest);
    if (latest.length !== 1) {
      skipped.push({ key, why: `${latest.length} of its ${versions.length} versions are listed as its latest` });
      continue;
    }
    const [current] = latest;
    if (current !== undefined && versions.some((version) => version.writtenAt > current.writtenAt)) {
      skipped.push({ key, why: "a version of it is listed as written after its latest" });
      continue;
    }
    ordered.push(versions.sort((a, b) => Number(b.latest) - Number(a.latest) || b.writtenAt - a.writtenAt));
  }
  return { keys: ordered, skipped };
}

/**
 * The full segment whose versions are `ordered`, and whether its latest was written
 * `walSegmentMs` ago or longer; null where they are no full segment's, or Neon deleted it,
 * its latest a delete marker.
 */
function fullSegment(ordered: readonly ListedVersion[], now: number, { walSegmentMs }: ExpiryAges) {
  const [latest] = ordered;
  if (latest === undefined || latest.deleteMarker) return null;
  const segment = segmentOf(latest.key);
  return segment === null ? null : { ...segment, old: now - latest.writtenAt >= walSegmentMs };
}

/** The timelines, as `<tenant>/<timeline>`, of the full segments of `listed`: those whose remote consistent LSN the pass reads, and records. */
export function walTimelines(listed: readonly ListedVersion[]): readonly string[] {
  return [...new Set(listed.flatMap(({ key, latest, deleteMarker }) => (latest && !deleteMarker ? segmentOf(key)?.timeline ?? [] : [])))];
}

/**
 * What of `listed`, every version of the bucket as it lists them, is expired at `now` as the
 * module says, kept for `ages`, its full segments below `horizons`, each timeline's (walHorizons).
 */
export function expired(listed: readonly ListedVersion[], now: number, ages: ExpiryAges, horizons: ReadonlyMap<string, ReadHorizon>): ExpiryPlan {
  const first: Expired[] = [];
  const last: Expired[] = [];
  const kept = new Map<string, KeptSegments>();
  const { keys, skipped } = byKey(listed);
  for (const ordered of keys) {
    const [latest, ...noncurrent] = ordered;
    if (latest === undefined) continue;
    // A full segment's versions, every one, go with it alone, once it is old enough and below its timeline's LSN.
    const segment = fullSegment(ordered, now, ages);
    if (segment !== null) {
      if (!segment.old) continue;
      const horizon = horizons.get(segment.timeline) ?? { unknown: "its remote consistent LSN was not read" };
      // Wholly a segment or more below it.
      if ("lsn" in horizon && segment.end + SEGMENT_BYTES <= horizon.lsn) {
        first.push(...noncurrent.map((version): Expired => ({ version, why: "wal segment" })));
        last.push({ version: latest, why: "wal segment" });
      } else {
        const why = "unknown" in horizon ? horizon.unknown : `their range lies less than a segment below ${lsnText(horizon.lsn)}, the least of its remote consistent LSNs of the last ${Math.round(ages.walHorizonMs / 3_600_000)} h`;
        const of = kept.get(segment.timeline) ?? { timeline: segment.timeline, segments: 0, bytes: 0, why };
        kept.set(segment.timeline, { ...of, segments: of.segments + 1, bytes: of.bytes + latest.bytes });
      }
      continue;
    }
    const part = latest.key.startsWith(PAGESERVER)
      ? { keptMs: ages.pageserverNoncurrentMs, why: "pageserver noncurrent" as const }
      : latest.key.startsWith(SAFEKEEPER)
      ? { keptMs: ages.safekeeperNoncurrentMs, why: "safekeeper noncurrent" as const }
      : latest.key === HORIZONS_KEY
      ? { keptMs: 0, why: "own record noncurrent" as const }
      : null;
    // Each noncurrent version replaced as the version before it in `ordered` was written; elsewhere, none expires.
    const gone = part === null ? [] : noncurrent.filter((_, index) => now - (ordered[index]?.writtenAt ?? now) >= part.keptMs).map((version): Expired => ({ version, why: part.why }));
    first.push(...gone);
    if (latest.deleteMarker && gone.length === noncurrent.length) last.push({ version: latest, why: "lone delete marker" });
  }
  return { first, last, kept: [...kept.values()], skipped };
}

/** The storage controller could not tell a timeline's remote consistent LSN: which, and why. */
export class HorizonUnknown extends Schema.TaggedError<HorizonUnknown>()("HorizonUnknown", { timeline: Schema.String, reason: Schema.String }) {
  override get message(): string {
    return `the storage controller could not tell the remote consistent LSN of the timeline ${this.timeline}: ${this.reason}`;
  }
}

/** The storage controller, as the pass asks it: a timeline's remote consistent LSN, the least of its shards'. */
export interface StorageController {
  readonly remoteConsistentLsn: (timeline: string) => Effect.Effect<bigint, HorizonUnknown>;
}

/** What the storage controller describes of a timeline: each of its shards, as its pageserver has it. */
const TimelineDescription = Schema.Struct({
  shards: Schema.Array(Schema.Struct({ remote_consistent_lsn: Schema.String, remote_consistent_lsn_visible: Schema.optionalKey(Schema.String) })),
});

/** How long the storage controller may take to describe a timeline, which asks each of its shards' pageservers. */
const DESCRIBE_TIMEOUT_MS = 60_000;

/**
 * The storage controller at `url`, asked with `token`: a timeline's remote consistent LSN, the
 * least of its shards' remote consistent LSNs, each of the uploaded index and of the one its
 * deletions are validated against, whichever is lower (GET
 * /control/v1/tenant/<tenant>/timeline/<timeline>, the scrubber's).
 */
export function makeStorageController(url: string, token: string): StorageController {
  return {
    remoteConsistentLsn: (timeline) =>
      Effect.gen(function*() {
        const unknown = (reason: string) => new HorizonUnknown({ timeline, reason });
        const text = yield* Effect.tryPromise({
          try: async (signal) => {
            const response = await fetch(`${url}/control/v1/tenant/${timeline.replace("/", "/timeline/")}`, {
              headers: { authorization: `Bearer ${token}` },
              signal: AbortSignal.any([signal, AbortSignal.timeout(DESCRIBE_TIMEOUT_MS)]),
            });
            const body = await response.text();
            if (response.status === 404) throw new Error(`the pageservers do not know it (404 ${body.slice(0, 200)})`);
            if (!response.ok) throw new Error(`it answered ${response.status} ${body.slice(0, 200)}`);
            return body;
          },
          catch: (cause) => unknown(cause instanceof Error ? cause.message : String(cause)),
        });
        const { shards } = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(TimelineDescription))(text).pipe(Effect.mapError((error) => unknown(`its answer is not a timeline's description: ${error.message}`)));
        if (shards.length === 0) return yield* unknown("it has no shard");
        let least: bigint | null = null;
        for (const text of shards.flatMap(({ remote_consistent_lsn, remote_consistent_lsn_visible }) => [remote_consistent_lsn, ...(remote_consistent_lsn_visible === undefined ? [] : [remote_consistent_lsn_visible])])) {
          const lsn = lsnOf(text);
          if (lsn === null) return yield* unknown(`a shard's remote consistent LSN is not one: ${text}`);
          if (least === null || lsn < least) least = lsn;
        }
        return least ?? 0n;
      }),
  };
}

/** The remote consistent LSN of each of `timelines` as `controller` says it now, or, of each it cannot be told of, why. */
export const readHorizons = (controller: StorageController, timelines: readonly string[]): Effect.Effect<ReadonlyMap<string, ReadHorizon>> =>
  Effect.forEach(timelines, (timeline) =>
    controller.remoteConsistentLsn(timeline).pipe(
      Effect.map((lsn): [string, ReadHorizon] => [timeline, { lsn }]),
      Effect.catch((error) => Effect.succeed<[string, ReadHorizon]>([timeline, { unknown: error.message }])),
    )).pipe(Effect.map((read) => new Map(read)));

/** Versions of some classes: how many, and of how many bytes. */
interface Tally {
  readonly versions: number;
  readonly bytes: number;
}

/** What a pass did, as its log says it. */
export interface ExpiryPass {
  /** How many versions the bucket listed. */
  readonly listed: number;
  /** What it deleted, by why. */
  readonly deleted: Readonly<Record<Expiry, Tally>>;
  /** What full segments old enough it kept, of each timeline, and why. */
  readonly kept: readonly KeptSegments[];
  /** The keys it left as they are, and why. */
  readonly skipped: readonly SkippedKey[];
  /** The versions the bucket did not delete, and why. */
  readonly refused: readonly RefusedVersion[];
  /** The latest versions it kept as their key's other versions were not all deleted: lone delete markers, and full segments. */
  readonly withheld: number;
  /** Why its record of remote consistent LSNs could not be read, where it could not. */
  readonly recordUnread: string | null;
}

/**
 * A pass of the expiry over `bucket` at `now`, kept for `ages`, its full segments below the
 * remote consistent LSNs `controller` says and the bucket has recorded (HORIZONS_KEY), which
 * it records anew: what it did. The latest versions of the keys it expires whole are deleted in
 * a request of their own, once the bucket has said each of their keys' other versions
 * deleted, by the answers to the requests before it, and, of a key whose other versions this
 * pass deleted, lists it with none but its latest.
 */
export const expire = (bucket: S3Bucket, controller: StorageController, now: number, ages: ExpiryAges): Effect.Effect<ExpiryPass, S3Error> =>
  Effect.gen(function*() {
    const listed = yield* bucket.listVersions("");
    const read = yield* readHorizons(controller, walTimelines(listed));
    // A record that cannot be read is none: the WAL is kept a window longer, and said.
    const record = yield* bucket.getJson(HORIZONS_KEY).pipe(Effect.map((value) => ({ value, unread: null })), Effect.catch((error) => Effect.succeed({ value: null, unread: error.message })));
    const horizons = recorded(horizonsOf(record.value), read, now, ages);
    const plan = expired(listed, now, ages, walHorizons(horizons, read, now, ages));
    const refusedFirst = yield* bucket.removeVersions(plan.first.map(({ version }) => version));
    const unfinished = new Set(refusedFirst.map(({ key }) => key));
    // Of the keys whose other versions this pass deleted, those the bucket lists with a version beside the latest still,
    // whatever it answered, keep their latest for a pass after.
    const cleared = new Set(plan.first.map(({ version }) => version.key));
    for (const { version } of plan.last.filter(({ version }) => cleared.has(version.key) && !unfinished.has(version.key))) {
      const left = (yield* bucket.listVersions(version.key)).filter(({ key }) => key === version.key);
      if (left.some(({ versionId }) => versionId !== version.versionId)) unfinished.add(version.key);
    }
    const last = plan.last.filter(({ version }) => !unfinished.has(version.key));
    const refusedLast = yield* bucket.removeVersions(last.map(({ version }) => version));
    yield* bucket.putJson(HORIZONS_KEY, storedHorizons(horizons));
    const refused = [...refusedFirst, ...refusedLast];
    const isRefused = (version: ObjectVersion) => refused.some(({ key, versionId }) => key === version.key && versionId === version.versionId);
    const deleted: Record<Expiry, { versions: number; bytes: number }> = {
      "pageserver noncurrent": { versions: 0, bytes: 0 },
      "safekeeper noncurrent": { versions: 0, bytes: 0 },
      "wal segment": { versions: 0, bytes: 0 },
      "lone delete marker": { versions: 0, bytes: 0 },
      "own record noncurrent": { versions: 0, bytes: 0 },
    };
    for (const { version, why } of [...plan.first, ...last]) {
      if (isRefused(version)) continue;
      deleted[why].versions += 1;
      deleted[why].bytes += version.bytes;
    }
    return { listed: listed.length, deleted, kept: plan.kept, skipped: plan.skipped, refused, withheld: plan.last.length - last.length, recordUnread: record.unread };
  });

/** What a pass did, as its log says. */
export function said(bucket: string, { listed, deleted, kept, skipped, refused, withheld, recordUnread }: ExpiryPass): string {
  const of = ({ versions, bytes }: Tally, what: string) => `${versions} ${what} (${bytes} bytes)`;
  const deletions = [
    of(deleted["pageserver noncurrent"], "noncurrent versions of the pageserver's"),
    of(deleted["safekeeper noncurrent"], "noncurrent versions of the safekeepers'"),
    of(deleted["wal segment"], "versions of full WAL segments"),
    `${deleted["own record noncurrent"].versions} noncurrent versions of its own record`,
  ].join(", ");
  const some = <A>(all: readonly A[], each: (one: A) => string) => `${all.slice(0, 5).map(each).join("; ")}${all.length > 5 ? `; and ${all.length - 5} more` : ""}`;
  return [
    `of the ${listed} versions and delete markers of the bucket ${bucket}, deleted ${deletions} and ${deleted["lone delete marker"].versions} delete markers left their key's only version`,
    ...kept.map(({ timeline, segments, bytes, why }) => `kept ${segments} full WAL segments of the timeline ${timeline} old enough to expire (${bytes} bytes), as ${why}`),
    ...(skipped.length > 0 ? [`left ${skipped.length} keys as they are, as their versions are listed as no S3 lists them: ${some(skipped, ({ key, why }) => `${key}: ${why}`)}`] : []),
    ...(refused.length > 0 ? [`the bucket did not delete ${refused.length} versions: ${some(refused, ({ key, versionId, code, message }) => `${key} ${versionId}: ${code} ${message}`)}`] : []),
    ...(withheld > 0 ? [`kept ${withheld} latest versions of keys whose other versions were not all deleted, for a pass after`] : []),
    ...(recordUnread !== null ? [`read no record of remote consistent LSNs, as ${recordUnread}, which keeps the WAL a window longer`] : []),
  ].join("; ");
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const environment = (name: string): string => {
    const value = process.env[name];
    if (!value) throw new Error(`${name} is not set`);
    return value;
  };
  const name = environment("S3_BUCKET_NEON");
  const bucket = makeS3Bucket({
    endpoint: environment("S3_ENDPOINT"),
    region: environment("S3_REGION"),
    bucket: name,
    accessKeyId: environment("AWS_ACCESS_KEY_ID"),
    secretAccessKey: environment("AWS_SECRET_ACCESS_KEY"),
  });
  const controller = makeStorageController(environment("STORAGE_CONTROLLER_URL"), environment("STORAGE_CONTROLLER_TOKEN"));
  const pass = expiryAges(process.env[EXPIRY_TEST_AGES_VARIABLE]).pipe(
    Effect.flatMap((ages) => expire(bucket, controller, Date.now(), ages)),
    Effect.map((done) => said(name, done)),
  );
  Effect.runPromise(pass).then(
    (line) => console.log(line),
    (error: unknown) => {
      console.error(`expiring Neon's bucket failed: ${error instanceof Error ? error.message : String(error)}`);
      process.exit(1);
    },
  );
}
