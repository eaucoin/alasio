import assert from "node:assert/strict";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";

import { Array as Arr, Effect, Logger, Schedule } from "effect";

import { LakeWake } from "../src/neon/lake-wake.ts";

test("a wake record the log does not take is written again until it is, one at a time, the writes made meanwhile in the last", async () => {
  const records: { lsn: string | null }[] = [];
  const logged: string[] = [];
  let failing = 3;
  let attempts = 0;
  let writing = 0;
  let most = 0;
  // The log's brokers not yet up, as alasio starts beside them: its first three attempts fail.
  const send = async (value: Buffer) => {
    attempts += 1;
    writing += 1;
    most = Math.max(most, writing);
    await sleep(5);
    writing -= 1;
    if (failing > 0) {
      failing -= 1;
      throw new Error("metadata failed 4 times");
    }
    const { lsn } = JSON.parse(value.toString("utf8")) as { lsn: string | null };
    records.push({ lsn });
  };
  await Effect.runPromise(
    Effect.scoped(Effect.gen(function*() {
      const wake = yield* LakeWake.over(send, { delay: "10 millis", retry: Schedule.spaced("20 millis") });
      // Written as alasio starts, in its start's record; and again while that record is
      // being written again, in a record after it.
      wake.written(() => Promise.resolve("0/1"));
      yield* Effect.promise(() => sleep(40));
      assert.deepEqual(records, []);
      wake.written(() => Promise.resolve("0/2"));
      for (const deadline = Date.now() + 5000; records.length < 2; yield* Effect.promise(() => sleep(10))) {
        if (Date.now() > deadline) assert.fail(`the records were not written: ${JSON.stringify(records)}`);
      }
      // Nothing more is owed.
      yield* Effect.promise(() => sleep(100));
    })).pipe(Effect.provide(Logger.layer([Logger.make(({ message }) => void logged.push(Arr.ensure(message).join(" ")))]))),
  );
  assert.deepEqual(records, [{ lsn: "0/2" }, { lsn: "0/2" }]);
  assert.equal(attempts, 5);
  assert.equal(most, 1);
  assert.deepEqual(logged.filter((line) => line.startsWith("could not tell")), Array.from({ length: 3 }, () => "could not tell the lake's loader of transcripts written, trying again: metadata failed 4 times"));
});
