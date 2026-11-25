/**
 * The node agent's following of the reads of restores whose checkpoint gofers read the bucket by
 * presigned URLs (src/node-agent/gofer-reads.ts), by what each gofer counts in its served file.
 */
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, renameSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { Effect, Fiber } from "effect";

import { makeGoferReads } from "../src/node-agent/gofer-reads.ts";

/** A directory of the test's, removed after `body`. */
async function inDirectory(body: (directory: string) => Effect.Effect<void, unknown>): Promise<void> {
  const directory = mkdtempSync(join(tmpdir(), "alasio-gofer-reads-"));
  try {
    await Effect.runPromise(body(directory));
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

/** Writes `bytes` to the served file `file`, whole, as a gofer replaces it. */
const serve = (file: string, bytes: number): void => {
  writeFileSync(`${file}.new`, `${bytes}\n`);
  renameSync(`${file}.new`, file);
};

const HOLDER = "main/fs-abc";

test("a restore is followed by what its gofer counts, read as it grows, done once its gofer has read the bytes of pages it was told, which ends its following, or, told none, once resumed and quiet; revoked, or followed anew, it is done", () =>
  inDirectory((directory) =>
    Effect.gen(function*() {
      const reads = yield* makeGoferReads({ quietMs: 200 });
      const served = join(directory, "served");
      const reader = reads.follow(HOLDER, served);
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
      const quiet = reads.follow(HOLDER, served);
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
      const first = reads.follow(HOLDER, served);
      const second = reads.follow(HOLDER, served);
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
      const reads = yield* makeGoferReads({ preparedForMs: 200, askedWindowMs: 300, resumedCeilingMs: 700 });
      const served = join(directory, "served");
      // Never resumed: done past its prepare's time.
      const unresumed = reads.follow(HOLDER, served);
      reads.expect(unresumed, 1_000_000);
      yield* Effect.sleep("250 millis");
      assert.equal(reads.readingOf(HOLDER), null);
      assert.ok(!reads.follows(HOLDER));

      // Resumed for 200 ms, its count growing: followed 300 ms past each look that finds it grown, at most 700 ms.
      const resumed = reads.follow(HOLDER, served);
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

test("what is followed is kept in a file only root reads, which an agent that starts again reads back: a resumed restore followed from its start, within its ceiling, one past its time not; one that cannot be read gives none", () =>
  inDirectory((directory) =>
    Effect.gen(function*() {
      const file = join(directory, "run", "gofer-reads.json");
      const served = join(directory, "served");
      const before = yield* makeGoferReads({ file, askedWindowMs: 500, preparedForMs: 100 });
      const reader = before.follow(HOLDER, served);
      before.expect(reader, 1_000_000);
      before.resumed(HOLDER, 10);
      before.follow("main/fs-other", join(directory, "other"));
      assert.equal((statSync(file).mode & 0o777).toString(8), "600");
      yield* Effect.sleep("150 millis");
      serve(served, 4096);
      const again = yield* makeGoferReads({ file, askedWindowMs: 500 });
      // Resumed, followed past its own time, as its gofer read on while the agent was away, its count and its bytes kept.
      assert.deepEqual(again.readingOf(HOLDER), { served: 4096, expected: 1_000_000 });
      // Not resumed, past its prepare's time: dropped.
      assert.ok(!again.follows("main/fs-other"));
      yield* Effect.sleep("550 millis");
      assert.equal(again.readingOf(HOLDER), null);
      // A file that is not one: none taken back.
      writeFileSync(file, "{not json");
      const lost = yield* makeGoferReads({ file });
      assert.ok(!lost.follows(HOLDER));
      assert.ok(readFileSync(file, "utf8").startsWith("{not"));
    })
  ));
