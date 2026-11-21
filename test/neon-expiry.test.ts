/**
 * Neon's bucket's expiry (neon/control/expiry.ts): what a pass deletes of every version the
 * bucket lists, and in what order; the full WAL segments it keeps, below the remote consistent
 * LSN their timeline had a window ago alone, each timeline as it can be told; the keys it leaves
 * as they are; and passes against a stand-in bucket and a stand-in storage controller, a delete
 * marker deleted only once the bucket has deleted every other version of its key.
 */
import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { test } from "node:test";

import { Effect, Exit } from "effect";

import {
  EXPIRY_AGES,
  expire,
  expired,
  type ExpiryPlan,
  expiryAges,
  HorizonUnknown,
  horizonsOf,
  HORIZONS_KEY,
  lsnOf,
  lsnText,
  makeStorageController,
  type ReadHorizon,
  readHorizons,
  recorded,
  RESTORABLE_DAYS,
  said,
  segmentOf,
  type StorageController,
  storedHorizons,
  walHorizons,
  walTimelines,
} from "../neon/control/expiry.ts";
import { type ListedVersion, makeS3Bucket } from "../src/node-agent/s3.ts";
import { startFakeS3 } from "./support/fake-s3.ts";

const HOUR = 60 * 60_000;
const DAY = 24 * HOUR;
const MiB = 1024 * 1024;
const SEGMENT = 16n * 1024n * 1024n;
const NOW = Date.parse("2026-11-21T12:00:00.000Z");
const TENANT = "b3243211dcfa4572bcc1e83a96dd410a";
const TIMELINE = "24e66ddb1cfb41caaca87a63cc864e6d";
const OTHER = "0123456789abcdef0123456789abcdef";

/** A version of `key`, written `daysAgo` days before NOW, of `bytes`, its key's latest or not, a delete marker or not. */
const version = (key: string, versionId: string, daysAgo: number, { bytes = 0, latest = false, deleteMarker = false } = {}): ListedVersion => ({
  key,
  versionId,
  writtenAt: NOW - daysAgo * DAY,
  bytes: deleteMarker ? 0 : bytes,
  latest,
  deleteMarker,
});

/** The full segment `name` of `timeline`'s WAL, as a safekeeper uploads it. */
const segment = (name: string, timeline = TIMELINE) => `safekeeper/${TENANT}/${timeline}/${name}`;

/** Of `plan`, what it deletes first and last, each as `<key's last part>@<version> <why>`. */
const planned = (plan: ExpiryPlan) => ({
  first: plan.first.map(({ version: { key, versionId }, why }) => `${key.split("/").at(-1)}@${versionId} ${why}`),
  last: plan.last.map(({ version: { key, versionId }, why }) => `${key.split("/").at(-1)}@${versionId} ${why}`),
});

test("a full WAL segment's key names its timeline and the end of its WAL; a partial segment's names none; an LSN is read and written as Neon writes it", () => {
  assert.deepEqual(segmentOf(segment("000000010000000B00000021")), { timeline: `${TENANT}/${TIMELINE}`, end: (0xB21n + 1n) * SEGMENT });
  assert.equal(segmentOf(segment("000000010000000B00000021_53_0000000B21BD3CE8_0000000B21BD3CE8_sk1.partial")), null);
  assert.equal(segmentOf(`pageserver/tenants/${TENANT}/timelines/${TIMELINE}/000000010000000B00000021`), null);
  assert.equal(lsnOf("B/21BD3CE8"), 0xB21BD3CE8n);
  assert.equal(lsnOf("0/0"), 0n);
  assert.equal(lsnOf("B21BD3CE8"), null);
  assert.equal(lsnText(0xB21BD3CE8n), "B/21BD3CE8");
});

test("a pass deletes the pageserver's versions noncurrent for RESTORABLE_DAYS, the safekeepers' for a day, and its own record's at once, never a current version; delete markers left their key's only version last, in a request of their own", () => {
  assert.equal(RESTORABLE_DAYS, 2);
  const layer = `pageserver/tenants/${TENANT}/timelines/${TIMELINE}/layer-1`;
  const index = `pageserver/tenants/${TENANT}/timelines/${TIMELINE}/index_part.json-00000001`;
  const deletedLayer = `pageserver/tenants/${TENANT}/timelines/${TIMELINE}/layer-2`;
  const recentlyDeleted = `pageserver/tenants/${TENANT}/timelines/${TIMELINE}/layer-3`;
  const partial = segment("000000010000000B00000022_53_0000000B22000000_0000000B22000000_sk1.partial");
  const listed = [
    // The index, written ten days ago, replaced three days ago and again a day ago: its oldest version alone goes.
    version(index, "i3", 1, { bytes: 30, latest: true }),
    version(index, "i2", 3, { bytes: 20 }),
    version(index, "i1", 10, { bytes: 10 }),
    // A layer the pageserver deleted three days ago: its version first, then, last, its delete marker.
    version(deletedLayer, "m", 3, { latest: true, deleteMarker: true }),
    version(deletedLayer, "d", 20, { bytes: 500 }),
    // One it deleted a day ago: kept, its marker with it.
    version(recentlyDeleted, "m", 1, { latest: true, deleteMarker: true }),
    version(recentlyDeleted, "d", 20, { bytes: 700 }),
    // A layer current for forty days: kept.
    version(layer, "l", 40, { bytes: 900, latest: true }),
    // A partial segment the safekeeper deleted two days ago, as it uploaded the next.
    version(partial, "m", 2, { latest: true, deleteMarker: true }),
    version(partial, "p", 2.5, { bytes: 12 * MiB }),
    // A delete marker alone, anywhere: gone, last.
    version("elsewhere/x", "m", 0, { latest: true, deleteMarker: true }),
    // Noncurrent outside both parts: kept.
    version("elsewhere/y", "y2", 0, { bytes: 1, latest: true }),
    version("elsewhere/y", "y1", 30, { bytes: 1 }),
    // Its own record's versions replaced, whenever: gone.
    version(HORIZONS_KEY, "h2", 0, { bytes: 50, latest: true }),
    version(HORIZONS_KEY, "h1", 1 / 24, { bytes: 50 }),
  ];
  const plan = expired(listed, NOW, EXPIRY_AGES, new Map());
  assert.deepEqual(planned(plan), {
    first: ["index_part.json-00000001@i1 pageserver noncurrent", "layer-2@d pageserver noncurrent", `${partial.split("/").at(-1)}@p safekeeper noncurrent`, "horizons.json@h1 own record noncurrent"],
    last: ["layer-2@m lone delete marker", `${partial.split("/").at(-1)}@m lone delete marker`, "x@m lone delete marker"],
  });
  assert.deepEqual([plan.kept, plan.skipped], [[], []]);
  // Every version listed in another order is planned alike.
  const sorted = (of: ExpiryPlan) => [...of.first, ...of.last].map(({ version: { versionId, key } }) => `${key}@${versionId}`).sort();
  assert.deepEqual(sorted(expired([...listed].reverse(), NOW, EXPIRY_AGES, new Map())), sorted(plan));
});

test("a key whose versions are listed as no S3 lists them, more than one its latest, none, or one written after its latest, is left as it is, and said", () => {
  const listed = [
    version("pageserver/two-latest", "a", 3, { bytes: 1, latest: true }),
    version("pageserver/two-latest", "b", 5, { bytes: 1, latest: true }),
    version("pageserver/no-latest", "a", 5, { bytes: 1 }),
    version("pageserver/newer", "a", 5, { latest: true, deleteMarker: true }),
    version("pageserver/newer", "b", 3, { bytes: 1 }),
    version("pageserver/fine", "b", 3, { latest: true, deleteMarker: true }),
    version("pageserver/fine", "a", 5, { bytes: 1 }),
  ];
  const plan = expired(listed, NOW, EXPIRY_AGES, new Map());
  assert.deepEqual(planned(plan), { first: ["fine@a pageserver noncurrent"], last: ["fine@b lone delete marker"] });
  assert.deepEqual(plan.skipped, [
    { key: "pageserver/two-latest", why: "2 of its 2 versions are listed as its latest" },
    { key: "pageserver/no-latest", why: "0 of its 1 versions are listed as its latest" },
    { key: "pageserver/newer", why: "a version of it is listed as written after its latest" },
  ]);
});

test("a full WAL segment, every version of it, is deleted only once written three days ago and wholly a segment or more below its timeline's LSN; one above is kept, as are those of a timeline whose LSN cannot be told, each timeline on its own, said", () => {
  const old = segment("000000010000000B00000021");
  const atHorizon = segment("000000010000000B00000022");
  const young = segment("000000010000000A00000001");
  const ofOther = segment("000000010000000100000001", OTHER);
  const deleted = segment("000000010000000900000001");
  const listed = [
    version(old, "v2", 4, { bytes: 16 * MiB, latest: true }),
    version(old, "v1", 4.1, { bytes: 16 * MiB }),
    version(atHorizon, "v", 3.5, { bytes: 16 * MiB, latest: true }),
    // Written two days ago, below the LSN: kept, every version of it, a re-upload's too.
    version(young, "v2", 2, { bytes: 16 * MiB, latest: true }),
    version(young, "v1", 2.5, { bytes: 16 * MiB }),
    version(ofOther, "v", 5, { bytes: 16 * MiB, latest: true }),
    // One Neon deleted with its timeline two days ago: noncurrent as a partial segment is.
    version(deleted, "m", 2, { latest: true, deleteMarker: true }),
    version(deleted, "v", 30, { bytes: 16 * MiB }),
  ];
  assert.deepEqual([...walTimelines(listed)].sort(), [`${TENANT}/${OTHER}`, `${TENANT}/${TIMELINE}`].sort());
  // The timeline's LSN at the end of B/22: B/21 lies a segment below it and goes, its every version, its latest last; B/22 does not.
  const horizon = (0xB22n + 1n) * SEGMENT;
  const plan = expired(listed, NOW, EXPIRY_AGES, new Map<string, ReadHorizon>([[`${TENANT}/${TIMELINE}`, { lsn: horizon }], [`${TENANT}/${OTHER}`, { lsn: 1n << 40n }]]));
  assert.deepEqual(planned(plan), {
    first: ["000000010000000B00000021@v1 wal segment", "000000010000000900000001@v safekeeper noncurrent"],
    last: ["000000010000000B00000021@v2 wal segment", "000000010000000100000001@v wal segment", "000000010000000900000001@m lone delete marker"],
  });
  assert.deepEqual(plan.kept, [{
    timeline: `${TENANT}/${TIMELINE}`,
    segments: 1,
    bytes: 16 * MiB,
    why: "their range lies less than a segment below B/23000000, the least of its remote consistent LSNs of the last 48 h",
  }]);
  // A timeline whose LSN cannot be told keeps its segments, said; the other's go as they would.
  const halfKnown = expired(listed, NOW, EXPIRY_AGES, new Map<string, ReadHorizon>([[`${TENANT}/${TIMELINE}`, { unknown: "it answered 503" }], [`${TENANT}/${OTHER}`, { lsn: 1n << 40n }]]));
  assert.deepEqual(planned(halfKnown).last, ["000000010000000100000001@v wal segment", "000000010000000900000001@m lone delete marker"]);
  assert.deepEqual(halfKnown.kept, [{ timeline: `${TENANT}/${TIMELINE}`, segments: 2, bytes: 32 * MiB, why: "it answered 503" }]);
  // One whose LSN was not read at all: kept too.
  assert.deepEqual(expired(listed, NOW, EXPIRY_AGES, new Map()).kept.map(({ timeline, segments, why }) => [timeline, segments, why]), [
    [`${TENANT}/${TIMELINE}`, 2, "its remote consistent LSN was not read"],
    [`${TENANT}/${OTHER}`, 1, "its remote consistent LSN was not read"],
  ]);
});

test("a full segment goes only below the LSN its timeline had RESTORABLE_DAYS ago, as the pass recorded it then, or the one read now where lower: none before one is recorded so long ago, nor where the LSN is not read now; the record keeps the window and the newest before it alone", () => {
  const timeline = `${TENANT}/${TIMELINE}`;
  const window = EXPIRY_AGES.walHorizonMs;
  assert.equal(window, 2 * DAY);
  // Recorded hourly for three days, the LSN a segment more each hour.
  const before = new Map([[timeline, Array.from({ length: 72 }, (_, hour) => ({ at: NOW - (72 - hour) * HOUR, lsn: BigInt(hour + 1) * SEGMENT }))]]);
  const now = (0x100n) * SEGMENT;
  const read = new Map<string, ReadHorizon>([[timeline, { lsn: now }]]);
  const kept = recorded(before, read, NOW, EXPIRY_AGES);
  // Of those before the window, the newest alone (48 h ago, the 25th's), then those within it, then now's.
  assert.deepEqual(kept.get(timeline)?.map(({ at, lsn }) => [(NOW - at) / HOUR, Number(lsn / SEGMENT)]), [
    [48, 25],
    ...Array.from({ length: 47 }, (_, index) => [47 - index, 26 + index]),
    [0, 256],
  ]);
  // Its horizon, that of 48 h ago, though now's is past it.
  assert.deepEqual(walHorizons(kept, read, NOW, EXPIRY_AGES).get(timeline), { lsn: 25n * SEGMENT });
  // Lower now than then, as after a time travel: now's.
  assert.deepEqual(walHorizons(kept, new Map<string, ReadHorizon>([[timeline, { lsn: 3n * SEGMENT }]]), NOW, EXPIRY_AGES).get(timeline), { lsn: 3n * SEGMENT });
  // Lowered within the window and back up since, as a time travel and the ingest after it do: the dip's.
  const dipped = new Map([[timeline, (kept.get(timeline) ?? []).map((sample) => (sample.at === NOW - 20 * HOUR ? { ...sample, lsn: 2n * SEGMENT } : sample))]]);
  assert.deepEqual(walHorizons(dipped, read, NOW, EXPIRY_AGES).get(timeline), { lsn: 2n * SEGMENT });
  // None recorded so long ago, as when the expiry first runs: none.
  const fresh = recorded(new Map(), read, NOW, EXPIRY_AGES);
  assert.deepEqual(walHorizons(fresh, read, NOW, EXPIRY_AGES).get(timeline), { unknown: "no remote consistent LSN of its has been recorded 48 h ago or longer" });
  // Not read now: none, whatever was recorded, and nothing recorded of now.
  const unread = new Map<string, ReadHorizon>([[timeline, { unknown: "it answered 503" }]]);
  assert.deepEqual(walHorizons(recorded(before, unread, NOW, EXPIRY_AGES), unread, NOW, EXPIRY_AGES).get(timeline), { unknown: "it answered 503" });
  assert.equal(recorded(before, unread, NOW, EXPIRY_AGES).get(timeline)?.at(-1)?.at, NOW - HOUR);
  // Those read within an hour of each other are kept as one, the least LSN at the last time: a record of 50 samples at most.
  const often = new Map([[timeline, Array.from({ length: 300 }, (_, index) => ({ at: NOW - 49 * HOUR + index * 10 * 60_000, lsn: BigInt(1000 - index) * SEGMENT }))]]);
  const merged = recorded(often, read, NOW, EXPIRY_AGES).get(timeline) ?? [];
  assert.ok(merged.length <= 50, `${merged.length} samples`);
  const hourOf = (at: number) => Math.floor(at / HOUR);
  assert.equal(new Set(merged.map(({ at }) => hourOf(at))).size, merged.length);
  for (const { at, lsn } of merged.slice(0, -1)) {
    const ofHour = (often.get(timeline) ?? []).filter((sample) => hourOf(sample.at) === hourOf(at));
    assert.deepEqual({ at, lsn }, { at: Math.max(...ofHour.map((sample) => sample.at)), lsn: ofHour.reduce((least, sample) => (sample.lsn < least ? sample.lsn : least), ofHour[0]?.lsn ?? 0n) });
  }
  // A timeline with no full segment left is recorded no more.
  assert.deepEqual([...recorded(before, new Map(), NOW, EXPIRY_AGES).keys()], []);
  // Kept as the bucket keeps it, and read back; what cannot be read is none.
  assert.deepEqual(horizonsOf(JSON.parse(JSON.stringify(storedHorizons(kept)))), kept);
  assert.deepEqual(horizonsOf({ timelines: { [timeline]: [{ at: 1, lsn: "not one" }] } }), new Map([[timeline, []]]));
  assert.deepEqual(horizonsOf(null), new Map());
  assert.deepEqual(horizonsOf("garbage"), new Map());
});

test("a test's ages replace those it gives alone, and a pass refuses ages that are not whole milliseconds of 0 or more", async () => {
  assert.deepEqual(await Effect.runPromise(expiryAges(undefined)), EXPIRY_AGES);
  assert.deepEqual(await Effect.runPromise(expiryAges(JSON.stringify({ walSegmentMs: 0, walHorizonMs: 0 }))), { ...EXPIRY_AGES, walSegmentMs: 0, walHorizonMs: 0 });
  for (const refused of ["{\"walSegmentMs\":-1}", "{\"pageserverNoncurrentMs\":1.5}", "not JSON"]) assert.ok(Exit.isFailure(await Effect.runPromiseExit(expiryAges(refused))), refused);
});

test("the storage controller's remote consistent LSN of a timeline is the least of its shards', of the uploaded index and of the one validated alike, asked with its token; one it does not know, or cannot tell, is not told, each timeline on its own", async () => {
  const asked: { url: string; authorization: string }[] = [];
  let answer: { status: number; body: string } = { status: 200, body: "" };
  const server = createServer((incoming, outgoing) => {
    asked.push({ url: incoming.url ?? "", authorization: incoming.headers.authorization ?? "" });
    outgoing.writeHead(answer.status, { "content-type": "application/json" }).end(answer.body);
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  try {
    const controller = makeStorageController(`http://127.0.0.1:${(server.address() as AddressInfo).port}`, "scrubber-token");
    const timeline = `${TENANT}/${TIMELINE}`;
    answer = {
      status: 200,
      body: JSON.stringify({
        shards: [
          { remote_consistent_lsn: "B/3FC24D88", remote_consistent_lsn_visible: "B/3F000000", other: 1 },
          { remote_consistent_lsn: "B/40000000", remote_consistent_lsn_visible: "B/40000000" },
        ],
      }),
    };
    assert.equal(await Effect.runPromise(controller.remoteConsistentLsn(timeline)), 0xB3F000000n);
    assert.deepEqual(asked, [{ url: `/control/v1/tenant/${TENANT}/timeline/${TIMELINE}`, authorization: "Bearer scrubber-token" }]);
    const refused = async () => {
      const exit = await Effect.runPromiseExit(controller.remoteConsistentLsn(timeline));
      assert.ok(Exit.isFailure(exit));
      return String(exit.cause);
    };
    answer = { status: 404, body: "{\"msg\":\"Tenant not found\"}" };
    assert.match(await refused(), /could not tell the remote consistent LSN of the timeline b3243211dcfa4572bcc1e83a96dd410a\/24e66ddb1cfb41caaca87a63cc864e6d: the pageservers do not know it \(404/u);
    answer = { status: 503, body: "unavailable" };
    assert.match(await refused(), /it answered 503 unavailable/u);
    answer = { status: 200, body: JSON.stringify({ shards: [] }) };
    assert.match(await refused(), /it has no shard/u);
    answer = { status: 200, body: JSON.stringify({ shards: [{ remote_consistent_lsn: "nonsense" }] }) };
    assert.match(await refused(), /a shard's remote consistent LSN is not one: nonsense/u);
  } finally {
    server.close();
  }
  const halfKnown: StorageController = { remoteConsistentLsn: (timeline) => (timeline === "a" ? Effect.succeed(7n) : Effect.fail(new HorizonUnknown({ timeline, reason: "it answered 503" }))) };
  assert.deepEqual(await Effect.runPromise(readHorizons(halfKnown, ["a", "b"])), new Map<string, ReadHorizon>([
    ["a", { lsn: 7n }],
    ["b", { unknown: "the storage controller could not tell the remote consistent LSN of the timeline b: it answered 503" }],
  ]));
});

test("a pass deletes a lone delete marker only in a request after the one of its key's other versions, and only once the bucket has deleted every one of them: one it did not leaves the marker, and the version it hides noncurrent, for a pass after", async () => {
  const s3 = await startFakeS3({ bucket: "neon", region: "us-east-1", accessKeyId: "admin", secretAccessKey: "secret", pageSize: 3 });
  try {
    const bucket = makeS3Bucket({ endpoint: s3.endpoint, region: "us-east-1", bucket: "neon", accessKeyId: "admin", secretAccessKey: "secret" });
    const none: StorageController = { remoteConsistentLsn: () => Effect.die("no timeline is asked of") };
    const index = `pageserver/tenants/${TENANT}/timelines/${OTHER}/index_part.json-00000002`;
    s3.versions.push(
      version(index, "m", 3, { latest: true, deleteMarker: true }),
      version(index, "v2", 10, { bytes: 20 }),
      version(index, "v1", 11, { bytes: 10 }),
      version("pageserver/other", "m", 3, { latest: true, deleteMarker: true }),
      version("pageserver/other", "v", 10, { bytes: 1 }),
    );
    // The bucket does not delete one of the index's old versions: its marker is kept, the other key's goes.
    s3.refuseVersion = (key, versionId) => (key === index && versionId === "v2" ? "InternalError" : null);
    const first = await Effect.runPromise(expire(bucket, none, NOW, EXPIRY_AGES));
    assert.deepEqual(s3.versions.map(({ key, versionId }) => `${key.split("/").at(-1)}@${versionId}`), ["index_part.json-00000002@m", "index_part.json-00000002@v2"]);
    // Its markers in a request of their own, after the one of the versions they hide.
    assert.equal(s3.requests.filter((asked) => asked === "POST ?delete=").length, 2);
    assert.deepEqual(first.deleted["lone delete marker"], { versions: 1, bytes: 0 });
    assert.deepEqual(first.deleted["pageserver noncurrent"], { versions: 2, bytes: 11 });
    assert.equal(first.withheld, 1);
    assert.match(said("neon", first), /; the bucket did not delete 1 versions: pageserver\/tenants\/[0-9a-f]+\/timelines\/[0-9a-f]+\/index_part\.json-00000002 v2: InternalError refused, as the test asks; kept 1 latest versions of keys whose other versions were not all deleted, for a pass after$/u);
    // The bucket deletes it at the next pass: the marker goes then, in a request after it.
    s3.refuseVersion = () => null;
    const second = await Effect.runPromise(expire(bucket, none, NOW, EXPIRY_AGES));
    assert.deepEqual(s3.versions, []);
    assert.deepEqual([second.deleted["pageserver noncurrent"].versions, second.deleted["lone delete marker"].versions, second.withheld], [1, 1, 0]);
  } finally {
    await s3.close();
  }
});

test("a pass lists the bucket across pages, records each timeline's LSN, deletes the full segments below the LSN recorded a window ago by each version's id, and says what it deleted and what full segments it kept, of which timeline, and why", async () => {
  const s3 = await startFakeS3({ bucket: "neon", region: "us-east-1", accessKeyId: "admin", secretAccessKey: "secret", pageSize: 3 });
  try {
    const bucket = makeS3Bucket({ endpoint: s3.endpoint, region: "us-east-1", bucket: "neon", accessKeyId: "admin", secretAccessKey: "secret" });
    const index = `pageserver/tenants/${TENANT}/timelines/${TIMELINE}/index_part.json-00000001`;
    const old = segment("000000010000000B00000021");
    const ofOther = segment("000000010000000100000001", OTHER);
    s3.versions.push(
      version(index, "i2", 3, { bytes: 20, latest: true }),
      version(index, "i1", 10, { bytes: 10 }),
      version(old, "v", 4, { bytes: 16 * MiB, latest: true }),
      version(ofOther, "v", 4, { bytes: 16 * MiB, latest: true }),
      version("pageserver/gone", "m", 1, { latest: true, deleteMarker: true }),
    );
    const lsns = new Map([[`${TENANT}/${TIMELINE}`, 1n << 40n]]);
    const controller: StorageController = {
      remoteConsistentLsn: (timeline) => {
        const lsn = lsns.get(timeline);
        return lsn === undefined ? Effect.fail(new HorizonUnknown({ timeline, reason: "the pageservers do not know it (404)" })) : Effect.succeed(lsn);
      },
    };

    // The first pass: nothing recorded a window ago, so no segment goes; the rest does, and the LSNs read now are recorded.
    const first = await Effect.runPromise(expire(bucket, controller, NOW, EXPIRY_AGES));
    assert.deepEqual(s3.versions.map(({ key, versionId }) => `${key.split("/").at(-1)}@${versionId}`), ["index_part.json-00000001@i2", "000000010000000B00000021@v", "000000010000000100000001@v"]);
    assert.equal(
      said("neon", first),
      "of the 5 versions and delete markers of the bucket neon, deleted 1 noncurrent versions of the pageserver's (10 bytes), 0 noncurrent versions of the safekeepers' (0 bytes), 0 versions of full WAL segments (0 bytes), 0 noncurrent versions of its own record and 1 delete markers left their key's only version; " +
        `kept 1 full WAL segments of the timeline ${TENANT}/${TIMELINE} old enough to expire (16777216 bytes), as no remote consistent LSN of its has been recorded 48 h ago or longer; ` +
        `kept 1 full WAL segments of the timeline ${TENANT}/${OTHER} old enough to expire (16777216 bytes), as the storage controller could not tell the remote consistent LSN of the timeline ${TENANT}/${OTHER}: the pageservers do not know it (404)`,
    );
    assert.deepEqual(s3.objects.has(HORIZONS_KEY) && JSON.parse(s3.objects.get(HORIZONS_KEY)?.toString() ?? ""), { timelines: { [`${TENANT}/${TIMELINE}`]: [{ at: NOW, lsn: "100/0" }], [`${TENANT}/${OTHER}`]: [] } });

    // A window later, the LSN recorded then: the known timeline's segment goes, the other's stays, said as before.
    const later = await Effect.runPromise(expire(bucket, controller, NOW + EXPIRY_AGES.walHorizonMs, EXPIRY_AGES));
    assert.deepEqual(s3.versions.map(({ key }) => key), [index, ofOther]);
    assert.deepEqual(later.deleted["wal segment"], { versions: 1, bytes: 16 * MiB });
    assert.deepEqual(later.kept.map(({ timeline }) => timeline), [`${TENANT}/${OTHER}`]);
    // Of the second pass, one request alone: no version but the segment's latest to delete first.
    assert.equal(s3.requests.filter((asked) => asked === "POST ?delete=").length, 3);
  } finally {
    await s3.close();
  }
});

test("a pass keeps a lone delete marker where the bucket said every version it hides deleted but lists one still, for a pass after", async () => {
  const s3 = await startFakeS3({ bucket: "neon", region: "us-east-1", accessKeyId: "admin", secretAccessKey: "secret", pageSize: 3 });
  try {
    const bucket = makeS3Bucket({ endpoint: s3.endpoint, region: "us-east-1", bucket: "neon", accessKeyId: "admin", secretAccessKey: "secret" });
    const none: StorageController = { remoteConsistentLsn: () => Effect.die("no timeline is asked of") };
    const index = `pageserver/tenants/${TENANT}/timelines/${OTHER}/index_part.json-00000002`;
    s3.versions.push(version(index, "m", 3, { latest: true, deleteMarker: true }), version(index, "v", 10, { bytes: 20 }));
    // It keeps the version, and answers as though it had deleted it.
    s3.refuseVersion = (_key, versionId) => (versionId === "v" ? "InternalError" : null);
    s3.answers = (asked, body) => (asked.startsWith("POST ?delete") ? body.replace(/<Error>.*?<\/Error>/gsu, "") : body);
    const pass = await Effect.runPromise(expire(bucket, none, NOW, EXPIRY_AGES));
    assert.deepEqual(s3.versions.map(({ versionId }) => versionId), ["m", "v"]);
    assert.equal(pass.withheld, 1);
    assert.equal(s3.requests.filter((asked) => /^GET \?.*\bversions=/u.test(asked) && asked.includes("index_part")).length, 1);
    // Deleted at last: the marker goes at the next pass.
    s3.refuseVersion = () => null;
    s3.answers = (_asked, body) => body;
    await Effect.runPromise(expire(bucket, none, NOW, EXPIRY_AGES));
    assert.deepEqual(s3.versions, []);
  } finally {
    await s3.close();
  }
});

test("a pass whose record of remote consistent LSNs cannot be read takes it for none, keeping the WAL a window longer, and says so, expiring the rest as it would", async () => {
  const s3 = await startFakeS3({ bucket: "neon", region: "us-east-1", accessKeyId: "admin", secretAccessKey: "secret" });
  try {
    const bucket = makeS3Bucket({ endpoint: s3.endpoint, region: "us-east-1", bucket: "neon", accessKeyId: "admin", secretAccessKey: "secret" });
    const old = segment("000000010000000B00000021");
    s3.versions.push(version(old, "v", 4, { bytes: 16 * MiB, latest: true }), version("pageserver/gone", "m", 1, { latest: true, deleteMarker: true }));
    s3.objects.set(HORIZONS_KEY, Buffer.from("not JSON"));
    const controller: StorageController = { remoteConsistentLsn: () => Effect.succeed(1n << 40n) };
    const pass = await Effect.runPromise(expire(bucket, controller, NOW, EXPIRY_AGES));
    assert.deepEqual(s3.versions.map(({ key }) => key), [old]);
    assert.match(said("neon", pass), /; read no record of remote consistent LSNs, as the bucket's read of \.alasio\/neon-expiry\/horizons\.json failed: it is not JSON, which keeps the WAL a window longer$/u);
    // Written anew, it is read at the next pass.
    assert.deepEqual(JSON.parse(s3.objects.get(HORIZONS_KEY)?.toString() ?? ""), { timelines: { [`${TENANT}/${TIMELINE}`]: [{ at: NOW, lsn: "100/0" }] } });
  } finally {
    await s3.close();
  }
});
