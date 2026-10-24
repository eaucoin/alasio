/**
 * A topic of the log, as a member of a consumer group reads it, with @platformatic/kafka,
 * alasio's one Kafka client: its records in turn, each told to the group only once whoever
 * reads it says so (commit), so a member started again reads on from the last record told,
 * neither losing one nor skipping one. A group that has been told of none begins at the
 * topic's first record still kept, or at the next written, as its reader asks. Each generation
 * of the group a member joins, a rebalance, is counted (REBALANCES).
 */
import { readFileSync } from "node:fs";

import { Consumer, type ConsumerGroupJoinPayload, MessagesStreamFallbackModes, MessagesStreamModes } from "@platformatic/kafka";

import { meter } from "../telemetry/index.ts";
import { type Feed, JoinAgain, type Received } from "./consume.ts";

/** Where a topic is read: the log's brokers, the principal read as, the file of its password, the topic, and the group read in. */
export interface TopicLog {
  readonly brokers: readonly string[];
  readonly username: string;
  readonly passwordFile: string;
  readonly topic: string;
  readonly group: string;
}

/** A record of the topic, as its group's member reads it (../log/consume.ts): its value besides. */
export interface TopicRecord extends Received {
  readonly value: Buffer | undefined;
}

/** A member of the group: the topic's records in turn, until it is closed. */
export type TopicFeed = Feed<TopicRecord>;

/** What joins a group on its topic: the log's (joinTopic), or a stand-in's in tests. */
export type JoinTopic = (log: TopicLog, untold: "earliest" | "latest") => Promise<TopicFeed>;

/**
 * How a member keeps its place in its group: the broker ends the membership of one it has not
 * heard from in SESSION_TIMEOUT_MS, and the member's heartbeats, on a timer of their own however
 * long a record's handling takes, come every HEARTBEAT_INTERVAL_MS. So the records of a member
 * killed, or lost with its node, go to another within seconds, where the client's default would
 * leave them a minute and more; a rebalance waits REBALANCE_TIMEOUT_MS at most for the members to
 * join it. The broker takes no session shorter than its group.min.session.timeout.ms, six seconds
 * by default.
 */
export const SESSION_TIMEOUT_MS = 10_000;
export const HEARTBEAT_INTERVAL_MS = 3_000;
export const REBALANCE_TIMEOUT_MS = 10_000;

export const REBALANCES = "alasio.log.rebalances";
const rebalances = meter.createCounter(REBALANCES, {
  description: "Rebalances of a group on the log alasio's member took part in, each generation of the group it joined: as it joined, and as members came and went or the topic grew; by group, topic, and how many of the topic's partitions the member was assigned",
  unit: "{rebalance}",
});

/** What a member says as it joins a generation of its group: its assignment, where the group's protocol gives one. */
export interface JoinsOf {
  readonly on: (event: "consumer:group:join", listener: (payload: Pick<ConsumerGroupJoinPayload, "assignments">) => void) => unknown;
}

/** Counts each generation of `log`'s group `member` joins (REBALANCES). */
export function countRebalances(member: JoinsOf, log: TopicLog): void {
  member.on("consumer:group:join", ({ assignments }) => {
    const partitions = assignments?.filter(({ topic }) => topic === log.topic).reduce((sum, { partitions }) => sum + partitions.length, 0);
    rebalances.add(1, { "messaging.consumer.group.name": log.group, "messaging.destination.name": log.topic, ...(partitions === undefined ? {} : { "alasio.log.partitions": partitions }) });
  });
}

/** The options of a member of `log`'s group, as joinTopic makes it. */
export const memberOptions = (log: TopicLog) => ({
  clientId: log.username,
  groupId: log.group,
  bootstrapBrokers: [...log.brokers],
  // Read at each join, as the kubelet keeps the Secret's file.
  sasl: { mechanism: "SCRAM-SHA-512" as const, username: log.username, password: readFileSync(log.passwordFile, "utf8").trim() },
  autocommit: false,
  maxWaitTime: 1_000,
  sessionTimeout: SESSION_TIMEOUT_MS,
  heartbeatInterval: HEARTBEAT_INTERVAL_MS,
  rebalanceTimeout: REBALANCE_TIMEOUT_MS,
});

/** How often a member looks at how many partitions its topic has, to join its group again where it has more than it joined with. */
export const PARTITIONS_LOOK_EVERY_MS = 30_000;

/** How a member looks at its topic's partitions (rejoinOnGrowth): the topic, how many it had as the member joined, and how many it has now. */
export interface PartitionsLook {
  readonly topic: string;
  readonly joinedWith: number;
  readonly partitions: () => Promise<number>;
  /** How often it looks: PARTITIONS_LOOK_EVERY_MS, unless a test says. */
  readonly everyMs?: number;
}

/**
 * `feed`, a member that joined its group as its topic had `joinedWith` partitions: ended, as it
 * looks every `everyMs` and finds its topic of more, with JoinAgain, for its group to be joined
 * again (../log/consume.ts) with them. A group is assigned the partitions its topic has as its
 * members join, and none added after until it rebalances.
 */
export function rejoinOnGrowth<R extends Received>(feed: Feed<R>, { topic, joinedWith, partitions, everyMs = PARTITIONS_LOOK_EVERY_MS }: PartitionsLook): Feed<R> {
  let grown: (error: JoinAgain) => void = () => {};
  const grew = new Promise<never>((_, reject) => (grown = reject));
  // Settled only by the look below, and raced by every read: never left unhandled.
  grew.catch(() => {});
  const looking = setInterval(() => {
    partitions().then((has) => {
      if (has > joinedWith) grown(new JoinAgain(`${topic} has ${has} partitions, more than the ${joinedWith} its member joined with`));
    }, () => {});
  }, everyMs);
  return {
    next: () => Promise.race([feed.next(), grew]),
    async close() {
      clearInterval(looking);
      await feed.close();
    },
  };
}

/**
 * Joins `log`'s group on its topic, reading on from where the group was last told, or, of a
 * group told of none, from the topic's first record still kept (`earliest`) or the next written
 * (`latest`); joined again where its topic grows (rejoinOnGrowth).
 */
export async function joinTopic(log: TopicLog, untold: "earliest" | "latest"): Promise<TopicFeed> {
  const consumer = new Consumer(memberOptions(log));
  countRebalances(consumer, log);
  const partitions = async (): Promise<number> => (await consumer.metadata({ topics: [log.topic], forceUpdate: true })).topics.get(log.topic)?.partitionsCount ?? 0;
  let stream;
  let joinedWith;
  try {
    // Before it joins: a partition added as it joins is one it may not be assigned, which it then finds more than this.
    joinedWith = await partitions();
    stream = await consumer.consume({
      topics: [log.topic],
      mode: MessagesStreamModes.COMMITTED,
      fallbackMode: untold === "earliest" ? MessagesStreamFallbackModes.EARLIEST : MessagesStreamFallbackModes.LATEST,
      autocommit: false,
    });
  } catch (error) {
    await consumer.close(true).catch(() => {});
    throw error;
  }
  const records = stream[Symbol.asyncIterator]();
  return rejoinOnGrowth({
    async next() {
      const read = await records.next();
      if (read.done) throw new Error("the topic's stream of records ended");
      const record = read.value;
      return { value: record.value, position: `${record.topic}/${record.partition}/${record.offset}`, commit: () => record.commit() as Promise<void> };
    },
    async close() {
      await stream.close().catch(() => {});
      await consumer.close(true);
    },
  }, { topic: log.topic, joinedWith, partitions });
}
