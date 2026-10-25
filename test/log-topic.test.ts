/**
 * A member of a group on the log (src/log/topic.ts), as alasio's three groups join theirs: its
 * place kept by heartbeats often enough that the broker ends it within seconds of its last, and
 * its group joined again where its topic grows, as one is assigned no partition added after it
 * joined; and each generation of its group it joins counted, a rebalance.
 */
import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";

import { Consumer } from "@platformatic/kafka";
import { Duration, Effect, Fiber, Logger } from "effect";

import { added } from "./support/metrics.ts";
import { consumeGroup, REJOIN_BACKOFF } from "../src/log/consume.ts";
import { countRebalances, memberOptions, REBALANCES, rejoinOnGrowth, tellStarts } from "../src/log/topic.ts";
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

test("each generation of its group a member joins is counted a rebalance, by group, topic, and the topic's partitions it was assigned", async () => {
  const log = { brokers: ["log:9092"], username: "alasio-main", passwordFile: "/dev/null", topic: "telegram.main", group: "alasio.main" };
  // The client's member, never connected: what it says as it joins, as the client says it.
  const member = new Consumer({ clientId: log.username, groupId: log.group, bootstrapBrokers: log.brokers });
  try {
    countRebalances(member, log);
    const counted = await added(REBALANCES, async () => {
      member.emit("consumer:group:join", { groupId: log.group, memberId: "m-1", generationId: 1, assignments: [{ topic: log.topic, partitions: [0, 1, 2] }] });
      member.emit("consumer:group:join", { groupId: log.group, memberId: "m-1", generationId: 2, assignments: [{ topic: log.topic, partitions: [1] }, { topic: "another", partitions: [0] }] });
      member.emit("consumer:group:join", { groupId: log.group, memberId: "m-1", generationId: 3, assignments: [] });
      // As the consumer protocol's member joins: no assignment said.
      member.emit("consumer:group:join", { groupId: log.group, memberId: "m-1" });
    });
    const of = (partitions?: number) => `messaging.consumer.group.name=alasio.main,messaging.destination.name=telegram.main${partitions === undefined ? "" : `,alasio.log.partitions=${partitions}`}`;
    assert.deepEqual(counted, { [of(3)]: 1, [of(1)]: 1, [of(0)]: 1, [of()]: 1 });
  } finally {
    await member.close(true);
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

/**
 * A member's consumer and stream as tellStarts reads them (StartsTold): its group's offsets kept
 * by partition, each told as the commits it is asked come, the generation `rejoin` moves on, and
 * each commit asked kept, as what `refuse` first refuses is asked again in the next generation,
 * as the client asks one again once it rejoined.
 */
function member(topic: string, partitions: readonly number[], { committed = new Map<number, bigint>(), refuse = 0 }: { readonly committed?: Map<number, bigint>; readonly refuse?: number } = {}) {
  const stream = Object.assign(new EventEmitter(), { offsetsToFetch: new Map<string, bigint>() });
  const asked: { readonly generation: number; readonly offsets: readonly { readonly partition: number; readonly offset: bigint }[] }[] = [];
  let refusals = refuse;
  const consumer = {
    assignments: [{ topic, partitions }],
    generationId: 1,
    listCommittedOffsets: async ({ topics }: { topics: { topic: string; partitions: number[] }[] }) =>
      new Map(topics.map(({ topic: name, partitions: indexes }) => {
        const offsets: bigint[] = [];
        for (const index of indexes) offsets[index] = committed.get(index) ?? -1n;
        return [name, offsets] as const;
      })),
    commit: async ({ offsets }: { offsets: { topic: string; partition: number; offset: bigint; leaderEpoch: number }[] }) => {
      // Refused as the group rebalances: the client rejoins, in the next generation, and asks it again.
      while (refusals > 0) {
        refusals -= 1;
        consumer.generationId += 1;
      }
      asked.push({ generation: consumer.generationId, offsets: offsets.map(({ partition, offset }) => ({ partition, offset })) });
      for (const { partition, offset } of offsets) committed.set(partition, offset);
    },
  };
  /** The stream's offsets resolved, `starts` by partition, as it says it has them. */
  const resolved = (starts: Readonly<Record<number, bigint>>) => {
    for (const [partition, offset] of Object.entries(starts)) stream.offsetsToFetch.set(`${topic}:${partition}`, offset);
    stream.emit("offsets");
  };
  return { stream, consumer, committed, asked, resolved };
}

test("a member reading from the next written tells its group of where it reads each partition it was assigned and told of none from, so every one has an offset, no record written", async () => {
  // Partition 1 told of before; 0 and 2 never, no record of theirs ever written.
  const of = member("events.main", [0, 1, 2], { committed: new Map([[1, 7n]]) });
  const told = tellStarts(of, "events.main");
  of.resolved({ 0: 0n, 1: 7n, 2: 0n });
  await told();
  assert.deepEqual(of.asked, [{ generation: 1, offsets: [{ partition: 0, offset: 0n }, { partition: 2, offset: 0n }] }]);
  assert.deepEqual([...of.committed].sort(([a], [b]) => a - b), [[0, 0n], [1, 7n], [2, 0n]]);
  // Assigned them again, each told of: nothing more asked.
  of.resolved({ 0: 0n, 1: 7n, 2: 0n });
  await told();
  assert.equal(of.asked.length, 1);
});

test("a member's starts are told of in the generation it was assigned them in, and not where it joined another before it could tell them", async () => {
  const of = member("events.main", [0]);
  const told = tellStarts(of, "events.main");
  of.resolved({ 0: 3n });
  // Rejoined before the telling is asked: that generation's own resolution tells its starts.
  of.consumer.generationId = 2;
  await told();
  assert.deepEqual(of.asked, []);
  of.resolved({ 0: 3n });
  await told();
  assert.deepEqual(of.asked, [{ generation: 2, offsets: [{ partition: 0, offset: 3n }] }]);
});

test("a start's telling refused as the group rebalances, asked again in the next generation, never tells the group past a record not handled", async () => {
  const of = member("events.main", [0], { refuse: 1 });
  const told = tellStarts(of, "events.main");
  // Records 5 and on written after the member resolved its start, 5, none of them handled.
  of.resolved({ 0: 5n });
  await told();
  assert.deepEqual(of.asked, [{ generation: 2, offsets: [{ partition: 0, offset: 5n }] }]);
  // At the first record not handled, never past it: one asked again can only have records delivered again.
  assert.ok((of.committed.get(0) ?? -1n) <= 5n);
});

test("a member hands a record on only once its starts' telling is through, so the record's own telling, past it, comes after", async () => {
  const of = member("events.main", [0]);
  let release: () => void = () => {};
  const held = new Promise<void>((resolve) => (release = resolve));
  const commit = of.consumer.commit;
  of.consumer.commit = async (options) => {
    await held;
    await commit(options);
  };
  const told = tellStarts(of, "events.main");
  of.resolved({ 0: 0n });
  let through = false;
  const reading = told().then(() => void (through = true));
  await sleep(20);
  assert.equal(through, false, "a record was handed on before its member's starts were told");
  release();
  await reading;
  assert.deepEqual(of.committed.get(0), 0n);
});
