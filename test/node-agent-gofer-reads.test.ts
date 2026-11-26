/**
 * The node agent's following of the reads of restores whose checkpoint gofers read the bucket by
 * presigned URLs (src/node-agent/gofer-reads.ts), by what each gofer counts in its served file,
 * and its signing of their URLs anew as long as it follows them.
 */
import assert from "node:assert/strict";
import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, renameSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { Effect, Exit, Fiber, Schedule, type Scope } from "effect";

import { makeGoferReads } from "../src/node-agent/gofer-reads.ts";
import { S3Error } from "../src/node-agent/s3.ts";
import { makeStoreUrls } from "../src/node-agent/store-urls.ts";
import { startFakeS3 } from "./support/fake-s3.ts";

/** A directory of the test's, removed after `body`, whose scope it ends first. */
async function inDirectory(body: (directory: string) => Effect.Effect<void, unknown, Scope.Scope>): Promise<void> {
  const directory = mkdtempSync(join(tmpdir(), "alasio-gofer-reads-"));
  try {
    await Effect.runPromise(Effect.scoped(body(directory)));
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

/** What signs a URL, as the agent's keys would: each URL says the object, how long it lasts, and how many were signed before it. */
const counting = () => {
  let signed = 0;
  return (key: string, expiresSeconds: number) => Effect.sync(() => `https://store.test/snapshots/${key}?expires=${expiresSeconds}&n=${++signed}`);
};

/** The objects a restore's gofer reads, by the path it serves each at. */
const OBJECTS = { "pages.img": "main/fs-abc/layers/d1/pages.img", "layers/d0/pages.img": "main/fs-abc/layers/d0/pages.img" };

/** The URLs in the file `urlsFile`, by path. */
const urlsIn = (urlsFile: string): Record<string, string> => JSON.parse(readFileSync(urlsFile, "utf8")) as Record<string, string>;

/** Writes `bytes`, and `gets` GETs, to the served file `file`, whole, as a gofer replaces it. */
const serve = (file: string, bytes: number, gets = 0): void => {
  writeFileSync(`${file}.new`, `${bytes} ${gets}\n`);
  renameSync(`${file}.new`, file);
};

const HOLDER = "main/fs-abc";

test("a restore is followed by what its gofer counts, read as it grows, done once its gofer has read the bytes of pages it was told, which ends its following, or, told none, once resumed and quiet; revoked, or followed anew, it is done", () =>
  inDirectory((directory) =>
    Effect.gen(function*() {
      const reads = yield* makeGoferReads({ urlsDir: join(directory, "urls"), presign: counting(), quietMs: 200 });
      const served = join(directory, "served");
      const { reader } = yield* reads.follow(HOLDER, served, OBJECTS);
      assert.ok(reads.follows(HOLDER));
      // Its gofer not started yet: nothing read, its bytes not known.
      assert.deepEqual(reads.readingOf(HOLDER), { served: 0, expected: null });
      reads.expect(reader, Number.POSITIVE_INFINITY);
      serve(served, 4096);
      assert.deepEqual(reads.readingOf(HOLDER), { served: 4096, expected: Number.POSITIVE_INFINITY });
      reads.expect(reader, 8192);
      const done = yield* Effect.forkChild(reads.readsDone(reader));
      serve(served, 8192);
      yield* Fiber.join(done);
      // Done by its bytes: followed no more.
      assert.equal(reads.readingOf(HOLDER), null);
      assert.ok(!reads.follows(HOLDER));

      // Told none: done once resumed and its count has not grown for its quiet, not before it is resumed.
      const { reader: quiet } = yield* reads.follow(HOLDER, served, OBJECTS);
      reads.expect(quiet, null);
      yield* Effect.sleep("300 millis");
      assert.deepEqual(reads.readingOf(HOLDER), { served: 8192, expected: null });
      reads.resumed(HOLDER, 60_000);
      assert.notEqual(reads.readingOf(HOLDER), null);
      // Its count grown, looked at 150 ms on: quiet from that look.
      yield* Effect.sleep("150 millis");
      serve(served, 12_288);
      assert.notEqual(reads.readingOf(HOLDER), null);
      yield* Effect.sleep("150 millis");
      assert.notEqual(reads.readingOf(HOLDER), null);
      yield* Effect.sleep("100 millis");
      assert.equal(reads.readingOf(HOLDER), null);
      yield* reads.readsDone(quiet);

      // Revoked, as its restore is done with; or followed anew, its reads before another's.
      const { reader: first } = yield* reads.follow(HOLDER, served, OBJECTS);
      const { reader: second } = yield* reads.follow(HOLDER, served, OBJECTS);
      yield* reads.readsDone(first);
      reads.revoke(HOLDER);
      assert.equal(reads.readingOf(HOLDER), null);
      assert.ok(!reads.follows(HOLDER));
      yield* reads.readsDone(second);
    })
  ));

test("a restore is followed for as long as its prepare is given, and once resumed as long as its background reads may take, longer as its count grows, never past its ceiling", () =>
  inDirectory((directory) =>
    Effect.gen(function*() {
      const reads = yield* makeGoferReads({ urlsDir: join(directory, "urls"), presign: counting(), preparedForMs: 200, askedWindowMs: 300, resumedCeilingMs: 700 });
      const served = join(directory, "served");
      // Never resumed: done past its prepare's time.
      const { reader: unresumed } = yield* reads.follow(HOLDER, served, OBJECTS);
      reads.expect(unresumed, 1_000_000);
      yield* Effect.sleep("250 millis");
      assert.equal(reads.readingOf(HOLDER), null);
      assert.ok(!reads.follows(HOLDER));

      // Resumed for 200 ms, its count growing: followed 300 ms past each look that finds it grown, at most 700 ms.
      const { reader: resumed } = yield* reads.follow(HOLDER, served, OBJECTS);
      reads.expect(resumed, 1_000_000);
      reads.resumed(HOLDER, 200);
      const start = Date.now();
      let bytes = 0;
      while (reads.readingOf(HOLDER) !== null) {
        serve(served, (bytes += 4096));
        yield* Effect.sleep("50 millis");
      }
      const followedFor = Date.now() - start;
      assert.ok(followedFor >= 650 && followedFor < 900, `followed for ${followedFor} ms, want its ceiling's 700`);
    })
  ));

test("what is followed is kept in a file only root reads, which an agent that starts again reads back, signing the URLs of each restore it took back anew at once and removing those of the rest: a resumed restore followed from its start, within its ceiling, one past its time not; one that cannot be read gives none", () =>
  inDirectory((directory) =>
    Effect.gen(function*() {
      const file = join(directory, "run", "gofer-reads.json");
      const urlsDir = join(directory, "urls");
      const served = join(directory, "served");
      const presign = counting();
      const before = yield* makeGoferReads({ urlsDir, presign, file, askedWindowMs: 500, preparedForMs: 100 });
      const { reader, urlsFile } = yield* before.follow(HOLDER, served, OBJECTS);
      before.expect(reader, 1_000_000);
      before.resumed(HOLDER, 10);
      const other = yield* before.follow("main/fs-other", join(directory, "other"), OBJECTS);
      assert.equal((statSync(file).mode & 0o777).toString(8), "600");
      const signedBefore = urlsIn(urlsFile);
      // One a stopped agent left half written.
      writeFileSync(join(urlsDir, "gofer-0123.json.new"), "{");
      yield* Effect.sleep("150 millis");
      serve(served, 4096);
      const again = yield* makeGoferReads({ urlsDir, presign, file, askedWindowMs: 500 });
      // Resumed, followed past its own time, as its gofer read on while the agent was away, its count and its bytes kept.
      assert.deepEqual(again.readingOf(HOLDER), { served: 4096, expected: 1_000_000 });
      // Its URLs signed anew, of the same objects, at once.
      const signedAgain = urlsIn(urlsFile);
      assert.deepEqual(Object.keys(signedAgain), Object.keys(signedBefore));
      for (const [path, url] of Object.entries(signedAgain)) assert.notEqual(url, signedBefore[path]);
      // Not resumed, past its prepare's time: dropped, its URLs removed, as is what was left half written.
      assert.ok(!again.follows("main/fs-other"));
      assert.ok(!existsSync(other.urlsFile));
      assert.deepEqual(readdirSync(urlsDir), [`${reader}.json`]);
      yield* Effect.sleep("550 millis");
      assert.equal(again.readingOf(HOLDER), null);
      // A file that is not one: none taken back.
      writeFileSync(file, "{not json");
      const lost = yield* makeGoferReads({ urlsDir, presign, file });
      assert.ok(!lost.follows(HOLDER));
      assert.ok(readFileSync(file, "utf8").startsWith("{not"));
    })
  ));

test("a restore's URLs, in a file only root reads, are signed anew every third of their lifetime for as long as it is followed, so that one read on past their lifetime, as with the store away that long, reads by URLs the store takes; once it is followed no more, its file is removed and its URLs signed no more; one whose URLs cannot be signed is not followed", () =>
  inDirectory((directory) =>
    Effect.gen(function*() {
      const credentials = { accessKeyId: "snapshots0123", secretAccessKey: "a secret" };
      const s3 = yield* Effect.acquireRelease(Effect.promise(() => startFakeS3({ bucket: "snapshots", region: "us-east-1", ...credentials })), (store) => Effect.promise(() => store.close()));
      s3.objects.set(OBJECTS["pages.img"], Buffer.from("pages"));
      const presign = makeStoreUrls({ store: { endpoint: s3.endpoint, region: "us-east-1", bucket: "snapshots", ...credentials } });
      const urlsDir = join(directory, "urls");
      const reads = yield* makeGoferReads({ urlsDir, presign, urlTtlMs: 3000, preparedForMs: 60_000 });
      const { urlsFile } = yield* reads.follow(HOLDER, join(directory, "served"), OBJECTS);
      assert.equal((statSync(urlsFile).mode & 0o777).toString(8), "600");
      const first = urlsIn(urlsFile)["pages.img"] ?? "";
      const status = (url: string) => Effect.promise(() => fetch(url).then((response) => response.status));
      assert.equal(yield* status(first), 200);
      // Past their lifetime: the first refused, as expired; those in the file now taken.
      yield* Effect.sleep("3500 millis");
      assert.equal(yield* status(first), 403);
      assert.equal(yield* status(urlsIn(urlsFile)["pages.img"] ?? ""), 200);
      // Followed no more: its file removed, and not written again as the agent signs those it still follows.
      reads.revoke(HOLDER);
      assert.ok(!existsSync(urlsFile));
      yield* Effect.sleep("1200 millis");
      assert.ok(!existsSync(urlsFile));

      // Its URLs not signed: not followed, and no file of them.
      const unsigned = yield* makeGoferReads({ urlsDir: join(directory, "unsigned"), presign: () => Effect.fail(new S3Error({ operation: "signing of a URL", key: "k", reason: "the store's Service did not resolve" })) });
      assert.ok(Exit.isFailure(yield* unsigned.follow(HOLDER, join(directory, "served"), OBJECTS).pipe(Effect.exit)));
      assert.ok(!unsigned.follows(HOLDER));
      mkdirSync(join(directory, "unsigned"), { recursive: true });
      assert.deepEqual(readdirSync(join(directory, "unsigned")), []);
    })
  ));

test("a resumed restore whose gofer asks the store again and again, delivering nothing, as of a store away, is followed on, within its ceiling, as a token asked of again is, and its URLs signed on; once it asks no more, it is followed no more past its window", () =>
  inDirectory((directory) =>
    Effect.gen(function*() {
      const reads = yield* makeGoferReads({ urlsDir: join(directory, "urls"), presign: counting(), preparedForMs: 60_000, askedWindowMs: 300, resumedCeilingMs: 5000 });
      const served = join(directory, "served");
      const { reader, urlsFile } = yield* reads.follow(HOLDER, served, OBJECTS);
      reads.expect(reader, 1_000_000);
      serve(served, 4096, 1);
      reads.resumed(HOLDER, 200);
      // Asking, delivering nothing, for twice its window and more: followed, its bytes as they were.
      for (let gets = 2; gets <= 12; gets++) {
        serve(served, 4096, gets);
        yield* Effect.sleep("100 millis");
        assert.deepEqual(reads.readingOf(HOLDER), { served: 4096, expected: 1_000_000 }, `after ${gets} GETs`);
      }
      assert.ok(existsSync(urlsFile));
      // Asking no more: followed no more once its window is over, its URLs removed.
      yield* Effect.sleep("450 millis");
      assert.equal(reads.readingOf(HOLDER), null);
      assert.ok(!existsSync(urlsFile));
    })
  ));

test("URLs that cannot be signed as the agent starts are signed again soon, not a third of their lifetime later", () =>
  inDirectory((directory) =>
    Effect.gen(function*() {
      const file = join(directory, "run", "gofer-reads.json");
      const urlsDir = join(directory, "urls");
      const before = yield* makeGoferReads({ urlsDir, presign: counting(), file, urlTtlMs: 60_000 });
      const { urlsFile } = yield* before.follow(HOLDER, join(directory, "served"), OBJECTS);
      const signedBefore = urlsIn(urlsFile);
      // The store's Service not resolved for the first two URLs the started agent signs.
      let asked = 0;
      const presign = (key: string, expiresSeconds: number) =>
        ++asked <= 2
          ? Effect.fail(new S3Error({ operation: "signing of a URL", key, reason: "the store's Service did not resolve" }))
          : Effect.succeed(`https://store.test/snapshots/${key}?expires=${expiresSeconds}&again=${asked}`);
      const again = yield* makeGoferReads({ urlsDir, presign, file, urlTtlMs: 60_000 });
      assert.ok(again.follows(HOLDER));
      assert.deepEqual(urlsIn(urlsFile), signedBefore);
      yield* Effect.sync(() => urlsIn(urlsFile)["pages.img"] ?? "").pipe(
        Effect.repeat({ schedule: Schedule.spaced("50 millis"), until: (url) => url.includes("again=") }),
        Effect.timeout("5 seconds"),
      );
      assert.ok(Object.values(urlsIn(urlsFile)).every((url) => url.includes("again=")));
    })
  ));
