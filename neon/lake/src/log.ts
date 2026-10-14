/**
 * The intake's member of the log's consumer group (./intake.ts's IntakeSource), on the one
 * Kafka client of alasio's code, @platformatic/kafka: as the intake's principal, over
 * SCRAM-SHA-512, offsets committed by hand once the lake has what they cover, read from
 * where the group was last told, or from each partition's start for one it never was.
 */
import { Consumer, MessagesStreamFallbackModes, MessagesStreamModes } from "@platformatic/kafka";

import type { BatchBounds, IntakeSource } from "./intake.ts";
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
/**
 * The most a fetch reads, and the most records the client holds read ahead of a batch,
 * past the fetch that brought them: a record as large as the log takes (8 MiB,
 * cli/src/manifests/log.ts) still comes, as Kafka answers a fetch with one record at least,
 * and what waits read is some 24 MiB at most, however large the log's records, not the
 * client's own 50 MB a fetch and 1,024 records.
 */
const FETCH_BYTES = 8 * 1024 * 1024;
const READ_AHEAD = 2;

/** A record as the client reads it: where it is, and its value. */
export interface Read extends Position {
  readonly value: Buffer;
}

/**
 * What gathers batches of `records`, a read at a time: as many as a batch's bounds take
 * (BatchBounds), the read under way when a batch's time is up, and the record that would
 * take a batch past its bytes, left for the next batch.
 */
export function gathering(records: AsyncIterator<Read>): (bounds: Pick<BatchBounds, "ms" | "records" | "bytes">) => Promise<Read[]> {
  let pending: Promise<IteratorResult<Read>> | null = null;
  return async ({ ms, records: most, bytes }) => {
    const batch: Read[] = [];
    let size = 0;
    const deadline = Date.now() + ms;
    while (batch.length < most && size < bytes) {
      const left = deadline - Date.now();
      if (left <= 0) break;
      pending ??= records.next();
      let timer: NodeJS.Timeout | undefined;
      const timeout = new Promise<null>((resolve) => {
        timer = setTimeout(() => resolve(null), left);
      });
      const read = await Promise.race([pending, timeout]).finally(() => clearTimeout(timer));
      if (read === null) break;
      if (read.done) throw new Error("the log's stream of records ended");
      if (batch.length > 0 && size + read.value.value.length > bytes) break;
      pending = null;
      batch.push(read.value);
      size += read.value.value.length;
    }
    return batch;
  };
}

/** Joins `group` on `topics` as `clientId`: the member's batches, and its commits. */
export async function joinLog(access: LogAccess, group: string, topics: readonly string[], clientId: string): Promise<IntakeSource> {
  const consumer = new Consumer({
    clientId,
    groupId: group,
    bootstrapBrokers: access.brokers,
    sasl: { mechanism: "SCRAM-SHA-512", username: access.username, password: access.password },
    autocommit: false,
    maxWaitTime: 1_000,
    maxBytes: FETCH_BYTES,
    highWaterMark: READ_AHEAD,
    sessionTimeout: SESSION_TIMEOUT_MS,
    heartbeatInterval: HEARTBEAT_MS,
  });
  let stream;
  // Each topic's id, as the member joins: a topic made anew while it reads is one it does
  // not read on, and it joins again, its topics' ids anew.
  let ids: ReadonlyMap<string, string>;
  try {
    stream = await consumer.consume({ topics: [...topics], mode: MessagesStreamModes.COMMITTED, fallbackMode: MessagesStreamFallbackModes.EARLIEST, autocommit: false });
    ids = new Map([...(await consumer.metadata({ topics: [...topics] })).topics].map(([name, { id }]) => [name, id]));
  } catch (error) {
    await stream?.close().catch(() => {});
    await consumer.close(true).catch(() => {});
    throw error;
  }
  const gather = gathering(stream[Symbol.asyncIterator]());
  return {
    async next(bounds) {
      return (await gather(bounds)).map(({ topic, partition, offset, value }) => ({ topic, topicId: ids.get(topic) ?? "", partition, offset, value }));
    },
    async commit(offsets: readonly Position[]) {
      if (offsets.length === 0) return;
      // An epoch of -1: the commit names none, the broker checks none.
      await consumer.commit({ offsets: offsets.map(({ topic, partition, offset }) => ({ topic, partition, offset, leaderEpoch: -1 })) });
    },
    async rewind(held) {
      // Each partition's start, and where the group was told, -1 where it never was: read from the start as it is.
      const starts = await consumer.listOffsets({ topics: [...topics], timestamp: -2n });
      const committed = await consumer.listCommittedOffsets({ topics: [...starts].map(([topic, offsets]) => ({ topic, partitions: offsets.map((_, partition) => partition) })) });
      const lake = new Map(held.filter(({ topic, topicId }) => topicId === ids.get(topic)).map(({ topic, partition, offset }) => [`${topic}\u0000${partition}`, offset]));
      const back = [...starts].flatMap(([topic, offsets]) =>
        offsets.flatMap((start, partition) => {
          const told = committed.get(topic)?.[partition] ?? -1n;
          const kept = lake.get(`${topic}\u0000${partition}`) ?? start;
          const from = kept > start ? kept : start;
          return told >= 0n && from < told ? [{ topic, partition, offset: from, leaderEpoch: -1 }] : [];
        })
      );
      if (back.length > 0) await consumer.commit({ offsets: back });
      return back.length > 0;
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
