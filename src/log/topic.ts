/**
 * A topic of the log, as a member of a consumer group reads it, with @platformatic/kafka,
 * alasio's one Kafka client: its records in turn, each told to the group only once whoever
 * reads it says so (commit), so a member started again reads on from the last record told,
 * neither losing one nor skipping one. A group that has been told of none begins at the
 * topic's first record still kept, or at the next written, as its reader asks.
 */
import { readFileSync } from "node:fs";

import { Consumer, MessagesStreamFallbackModes, MessagesStreamModes } from "@platformatic/kafka";

/** Where a topic is read: the log's brokers, the principal read as, the file of its password, the topic, and the group read in. */
export interface TopicLog {
  readonly brokers: readonly string[];
  readonly username: string;
  readonly passwordFile: string;
  readonly topic: string;
  readonly group: string;
}

/** A record of the topic: its value, where it is, and what tells the group it is read. */
export interface TopicRecord {
  readonly value: Buffer | undefined;
  readonly position: string;
  readonly commit: () => Promise<void>;
}

/** A member of the group: the topic's records in turn, until it is closed. */
export interface TopicFeed {
  readonly next: () => Promise<TopicRecord>;
  readonly close: () => Promise<void>;
}

/** Joins `log`'s group on its topic, reading on from where the group was last told, or, of a group told of none, from the topic's first record still kept (`earliest`) or the next written (`latest`). */
export async function joinTopic(log: TopicLog, untold: "earliest" | "latest"): Promise<TopicFeed> {
  const consumer = new Consumer({
    clientId: log.username,
    groupId: log.group,
    bootstrapBrokers: [...log.brokers],
    // Read at each join, as the kubelet keeps the Secret's file.
    sasl: { mechanism: "SCRAM-SHA-512", username: log.username, password: readFileSync(log.passwordFile, "utf8").trim() },
    autocommit: false,
    maxWaitTime: 1_000,
  });
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
