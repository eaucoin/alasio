/**
 * A topic of the log, as a member of a consumer group reads it, with @platformatic/kafka,
 * alasio's one Kafka client: its records in turn, each told to the group only once whoever
 * reads it says so (commit), so a member started again reads on from the last record told,
 * neither losing one nor skipping one. A group that has been told of none begins at the
 * topic's first record still kept, or at the next written, as its reader asks.
 */
import { readFileSync } from "node:fs";

import { Consumer, MessagesStreamFallbackModes, MessagesStreamModes } from "@platformatic/kafka";

import type { Feed, Received } from "./consume.ts";

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

/** Joins `log`'s group on its topic, reading on from where the group was last told, or, of a group told of none, from the topic's first record still kept (`earliest`) or the next written (`latest`). */
export async function joinTopic(log: TopicLog, untold: "earliest" | "latest"): Promise<TopicFeed> {
  const consumer = new Consumer(memberOptions(log));
  let stream;
  try {
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
  return {
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
  };
}
