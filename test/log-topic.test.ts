/**
 * A member of a group on the log (src/log/topic.ts), as alasio's three groups join theirs: its
 * place kept by heartbeats often enough that the broker ends it within seconds of its last, and
 * its group joined again where its topic grows, as one is assigned no partition added after it
 * joined.
 */
import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";

import { Duration, Effect, Fiber, Logger } from "effect";

import { consumeGroup, REJOIN_BACKOFF } from "../src/log/consume.ts";
import { memberOptions, rejoinOnGrowth } from "../src/log/topic.ts";
import { memoryTopic } from "./support/topics.ts";

test("a member's session ends ten seconds after its last heartbeat, which come every three, and a rebalance waits ten seconds at most", () => {
  const directory = mkdtempSync(join(tmpdir(), "alasio-log-topic-"));
  try {
    const passwordFile = join(directory, "password");
    writeFileSync(passwordFile, "secret\n");
    const options = memberOptions({ brokers: ["log:9092"], username: "alasio-main", passwordFile, topic: "telegram.main", group: "alasio.main" });
    assert.deepEqual([options.sessionTimeout, options.heartbeatInterval, options.rebalanceTimeout], [10_000, 3_000, 10_000]);
    // Three heartbeats may go missing before the broker ends the session.
    assert.ok(options.heartbeatInterval * 3 <= options.sessionTimeout);
    // No session the broker refuses: none under its default group.min.session.timeout.ms.
    assert.ok(options.sessionTimeout >= 6_000);
    assert.deepEqual([options.groupId, options.sasl.password], ["alasio.main", "secret"]);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

/** How long a member that failed waits before it joins its group again. */
const REJOIN_BACKOFF_MS = Duration.toMillis(REJOIN_BACKOFF);

test("a member whose topic grows under it ends, its group joined again at once, reading on from the last told, the end said as no failure", async () => {
  const topic = memoryTopic("telegram.main", ["first"]);
  let partitions = 1;
  let joins = 0;
  const handled: string[] = [];
  const logged: { readonly level: string; readonly message: string }[] = [];
  // Of the topic's partitions as it joins, as joinTopic reads them before it joins.
  const join = Effect.flatMap(Effect.sync(() => partitions), (joinedWith) =>
    Effect.map(topic.join, (feed) => {
      joins++;
      return rejoinOnGrowth(feed, { topic: "telegram.main", joinedWith, partitions: async () => partitions, everyMs: 20 });
    }));
  const member = Effect.runFork(consumeGroup({ name: "telegram.main", join, handle: (record) => Effect.sync(() => void handled.push(String(record.value))) }).pipe(
    Effect.provide(Logger.layer([Logger.make(({ logLevel, message }) => void logged.push({ level: String(logLevel), message: String(message) }))])),
  ));
  try {
    const until = async (done: () => boolean) => {
      for (let waited = 0; !done(); waited += 20) {
        assert.ok(waited < 5_000, "waited five seconds");
        await sleep(20);
      }
    };
    await until(() => handled.length === 1);
    await sleep(100);
    assert.equal(joins, 1, "a member whose topic has the partitions it joined with joined again");
    partitions = 6;
    const grew = Date.now();
    await until(() => joins === 2);
    // At once, not after the backoff of a member that failed.
    assert.ok(Date.now() - grew < REJOIN_BACKOFF_MS / 3, `joined again ${Date.now() - grew} ms after its topic grew`);
    topic.write("second");
    await until(() => handled.length === 2);
    assert.deepEqual(handled, ["first", "second"]);
    assert.ok(logged.some(({ level, message }) => level === "Info" && message.includes("has 6 partitions, more than the 1 its member joined with")), JSON.stringify(logged));
    assert.ok(!logged.some(({ level }) => level === "Error" || level === "Warn"), JSON.stringify(logged));
  } finally {
    await Effect.runPromise(Fiber.interrupt(member));
  }
});
