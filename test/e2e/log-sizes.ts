/**
 * How compactly the log keeps a telemetry topic, for the end-to-end run to measure: run by
 * node in the access applier's container beside a node of the log (`node
 * --input-type=module-typescript -` from /opt/alasio, where the client library is), as the
 * log's admin, a super user. It reads the topic its argument names from its start to its end
 * as of then, and prints, as JSON, what its partitions hold on their volumes (diskBytes), the
 * bytes of the records' values as the log gives them back, uncompressed (valueBytes), how
 * many records there are, and the times of the first and the last.
 *
 * The admin's password is the container's LOG_ADMIN_PASSWORD.
 */
import { randomUUID } from "node:crypto";

import { Admin, Consumer, MessagesStreamModes } from "@platformatic/kafka";

const [topic = "otel.metrics"] = process.argv.slice(2);
const options = {
  clientId: "e2e-log-sizes",
  bootstrapBrokers: ["127.0.0.1:9092"],
  sasl: { mechanism: "SCRAM-SHA-512" as const, username: "admin", password: process.env["LOG_ADMIN_PASSWORD"] ?? "" },
};
const admin = new Admin(options);
const consumer = new Consumer({ ...options, groupId: `e2e-log-sizes-${randomUUID()}` });
try {
  const ends = (await consumer.listOffsets({ topics: [topic] })).get(topic) ?? [];
  const partitions = ends.map((_, partition) => partition);
  const described = await admin.describeLogDirs({ topics: [{ name: topic, partitions }] });
  // Each partition once, of the node that holds the most of it, as each node holds a replica of it.
  const sizes = new Map<number, number>();
  for (const { partitionIndex, partitionSize } of described.flatMap(({ results }) => results).flatMap(({ topics }) => topics).filter(({ name }) => name === topic).flatMap(({ partitions: held }) => held)) {
    sizes.set(partitionIndex, Math.max(sizes.get(partitionIndex) ?? 0, Number(partitionSize)));
  }
  const diskBytes = [...sizes.values()].reduce((sum, size) => sum + size, 0);
  let valueBytes = 0;
  let records = 0;
  let firstMs = Number.POSITIVE_INFINITY;
  let lastMs = 0;
  const left = new Map(ends.flatMap((end, partition) => (end > 0n ? [[partition, end] as const] : [])));
  if (left.size > 0) {
    const stream = await consumer.consume({ topics: [topic], mode: MessagesStreamModes.EARLIEST, autocommit: false });
    for await (const message of stream) {
      valueBytes += message.value.length;
      records += 1;
      const at = Number(message.timestamp);
      firstMs = Math.min(firstMs, at);
      lastMs = Math.max(lastMs, at);
      if (message.offset + 1n >= (left.get(message.partition) ?? 0n)) left.delete(message.partition);
      if (left.size === 0) break;
    }
    await stream.close();
  }
  console.log(JSON.stringify({ topic, partitions: partitions.length, diskBytes, valueBytes, records, firstMs: records > 0 ? firstMs : null, lastMs: records > 0 ? lastMs : null }));
} finally {
  await consumer.close().catch(() => {});
  await admin.close().catch(() => {});
}
