/**
 * What the log holds of an end-to-end run's spans, for a failure's message: run by node in
 * the access applier's container beside a node of the log (`node --input-type=module-typescript -`
 * from /opt/alasio, where the client library is), as the log's admin, a super user. It reads
 * every record of otel.spans, each partition from its start to its end, as a group of its
 * own, and prints, as JSON: each partition's start and end, the offsets the intake's group
 * was last told, and, of each record of the run, its partition, offset, timestamp and the
 * `e2e.seq` of its span.
 *
 * Its argument is the run's `e2e.run`; the admin's password is the container's LOG_ADMIN_PASSWORD.
 */
import { randomUUID } from "node:crypto";

import { Admin, Consumer, MessagesStreamModes } from "@platformatic/kafka";

const TOPIC = "otel.spans";
const INTAKE_GROUP = "lake-intake";
const run = Buffer.from(process.argv[2] ?? "");
const access = { bootstrapBrokers: ["127.0.0.1:9092"], sasl: { mechanism: "SCRAM-SHA-512" as const, username: "admin", password: process.env["LOG_ADMIN_PASSWORD"] ?? "" } };

/**
 * The `e2e.seq` of the span in `value`, an OTLP export request in binary protobuf: after the
 * attribute's key, its AnyValue (field 2, length-delimited), whose int_value is field 3, a varint.
 */
function seqOf(value: Buffer): number | null {
  const key = value.indexOf("e2e.seq");
  if (key < 0 || value[key + 7] !== 0x12 || value[key + 9] !== 0x18) return null;
  let seq = 0;
  for (let at = key + 10, shift = 0; at < value.length; at += 1, shift += 7) {
    const byte = value[at] ?? 0;
    seq += (byte & 0x7f) * 2 ** shift;
    if ((byte & 0x80) === 0) return seq;
  }
  return null;
}

const admin = new Admin({ clientId: "e2e-log-records", ...access });
const consumer = new Consumer({ clientId: "e2e-log-records", groupId: `e2e-log-records-${randomUUID()}`, autocommit: false, maxWaitTime: 500, ...access });
try {
  const ends = (await consumer.listOffsets({ topics: [TOPIC] })).get(TOPIC) ?? [];
  const starts = (await consumer.listOffsets({ topics: [TOPIC], timestamp: -2n })).get(TOPIC) ?? [];
  const intake = await admin.listConsumerGroupOffsets({ groups: [INTAKE_GROUP] });
  const committed = intake.flatMap(({ topics }) => topics).filter(({ name }) => name === TOPIC).flatMap(({ partitions }) => partitions)
    .map(({ partitionIndex, committedOffset }) => ({ partition: partitionIndex, offset: String(committedOffset) }));
  const records: { partition: number; offset: string; timestamp: string; seq: number | null }[] = [];
  // Each partition read to its last record; one with none waits for nothing.
  const left = new Map<number, bigint>();
  ends.forEach((end, partition) => {
    if (end > (starts[partition] ?? 0n)) left.set(partition, end - 1n);
  });
  if (left.size > 0) {
    const stream = await consumer.consume({ topics: [TOPIC], mode: MessagesStreamModes.EARLIEST, autocommit: false });
    const deadline = setTimeout(() => void stream.close(), 120_000);
    for await (const message of stream) {
      if (message.value.includes(run)) records.push({ partition: message.partition, offset: String(message.offset), timestamp: String(message.timestamp), seq: seqOf(message.value) });
      if (message.offset >= (left.get(message.partition) ?? -1n)) left.delete(message.partition);
      if (left.size === 0) break;
    }
    clearTimeout(deadline);
    await stream.close().catch(() => {});
  }
  console.log(JSON.stringify({
    partitions: ends.map((end, partition) => ({ partition, start: String(starts[partition] ?? 0n), end: String(end) })),
    unread: [...left.keys()],
    committed,
    records,
  }));
} finally {
  await consumer.close(true).catch(() => {});
  await admin.close().catch(() => {});
}
