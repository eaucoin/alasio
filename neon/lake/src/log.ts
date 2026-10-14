/**
 * The intake's member of the log's consumer group (./intake.ts's IntakeSource), on the one
 * Kafka client of alasio's code, @platformatic/kafka: as the intake's principal, over
 * SCRAM-SHA-512, offsets committed by hand once the lake has what they cover, read from
 * where the group was last told, or from each partition's start for one it never was.
 */
import { Consumer, type Message, MessagesStreamFallbackModes, MessagesStreamModes } from "@platformatic/kafka";

import type { IntakeSource, LogRecord } from "./intake.ts";
import type { Position } from "./otel.ts";

/** The log as a client reaches it: its brokers, and the principal it is reached as. */
export interface LogAccess {
  brokers: string[];
  username: string;
  password: string;
}

/** How long a member's session lasts without a heartbeat before the group gives its partitions to another. */
const SESSION_TIMEOUT_MS = 30_000;
const HEARTBEAT_MS = 3_000;

/** Joins `group` on `topics` as `clientId`: the member's batches, and its commits. */
export async function joinLog(access: LogAccess, group: string, topics: readonly string[], clientId: string): Promise<IntakeSource> {
  const consumer = new Consumer({
    clientId,
    groupId: group,
    bootstrapBrokers: access.brokers,
    sasl: { mechanism: "SCRAM-SHA-512", username: access.username, password: access.password },
    autocommit: false,
    maxWaitTime: 1_000,
    sessionTimeout: SESSION_TIMEOUT_MS,
    heartbeatInterval: HEARTBEAT_MS,
  });
  let stream;
  try {
    stream = await consumer.consume({ topics: [...topics], mode: MessagesStreamModes.COMMITTED, fallbackMode: MessagesStreamFallbackModes.EARLIEST, autocommit: false });
  } catch (error) {
    await consumer.close(true).catch(() => {});
    throw error;
  }
  const messages = stream[Symbol.asyncIterator]();
  // The read under way, which a batch that ended before it answered leaves for the next.
  let pending: Promise<IteratorResult<Message<Buffer, Buffer, Buffer, Buffer>>> | null = null;
  return {
    async next(maxMs, maxRecords) {
      const records: LogRecord[] = [];
      const deadline = Date.now() + maxMs;
      while (records.length < maxRecords) {
        const left = deadline - Date.now();
        if (left <= 0) break;
        pending ??= messages.next();
        let timer: NodeJS.Timeout | undefined;
        const timeout = new Promise<null>((resolve) => {
          timer = setTimeout(() => resolve(null), left);
        });
        const read = await Promise.race([pending, timeout]).finally(() => clearTimeout(timer));
        if (read === null) break;
        pending = null;
        if (read.done) throw new Error("the log's stream of records ended");
        const { topic, partition, offset, value } = read.value;
        records.push({ topic, partition, offset, value });
      }
      return records;
    },
    async commit(offsets: readonly Position[]) {
      if (offsets.length === 0) return;
      // An epoch of -1: the commit names none, the broker checks none.
      await consumer.commit({ offsets: offsets.map(({ topic, partition, offset }) => ({ topic, partition, offset, leaderEpoch: -1 })) });
    },
    async lag() {
      // Each partition's end, against where the group was last told, or its start where it never was.
      const [ends, starts] = await Promise.all([consumer.listOffsets({ topics: [...topics] }), consumer.listOffsets({ topics: [...topics], timestamp: -2n })]);
      const committed = await consumer.listCommittedOffsets({ topics: [...ends].map(([topic, offsets]) => ({ topic, partitions: offsets.map((_, partition) => partition) })) });
      return new Map([...ends].map(([topic, offsets]) => [topic, offsets.reduce((behind, end, partition) => {
        const at = committed.get(topic)?.[partition] ?? -1n;
        return behind + Number(end - (at >= 0n ? at : (starts.get(topic)?.[partition] ?? 0n)));
      }, 0)]));
    },
    async close() {
      await stream.close().catch(() => {});
      await consumer.close(true);
    },
  };
}
