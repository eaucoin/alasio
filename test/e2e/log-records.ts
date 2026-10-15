/**
 * What the log holds of otel.spans around a moment, for a failure's message: run by node in
 * the access applier's container beside a node of the log (`node --input-type=module-typescript -`
 * from /opt/alasio, where the client library is), as the log's admin, a super user. It reads
 * every record of otel.spans, each partition from its start to its end, as a group of its
 * own, and prints, as JSON: each partition's start and end, the offsets the intake's group
 * was last told, and each record produced between its two arguments (in milliseconds since
 * the epoch), with its partition, offset and timestamp and the spans it carries, decoded
 * from the OTLP export request in binary protobuf it is: each one's trace and span ids, its
 * service, its name, and its `e2e.run` and `e2e.seq` where it has them.
 *
 * The admin's password is the container's LOG_ADMIN_PASSWORD.
 */
import { randomUUID } from "node:crypto";

import { Admin, Consumer, MessagesStreamModes } from "@platformatic/kafka";

const TOPIC = "otel.spans";
const INTAKE_GROUP = "lake-intake";
const [from, to] = process.argv.slice(2).map(BigInt) as [bigint, bigint];
const access = { bootstrapBrokers: ["127.0.0.1:9092"], sasl: { mechanism: "SCRAM-SHA-512" as const, username: "admin", password: process.env["LOG_ADMIN_PASSWORD"] ?? "" } };

/** The fields of the protobuf message `message`, in order: each one's number and value, a varint's as a bigint, a length-delimited one's as its bytes. */
function* fields(message: Buffer): Generator<readonly [number, bigint | Buffer]> {
  let at = 0;
  const varint = (): bigint => {
    let value = 0n;
    for (let shift = 0n; ; shift += 7n) {
      const byte = message[at++];
      if (byte === undefined) throw new Error("a varint runs past its message");
      value |= BigInt(byte & 0x7f) << shift;
      if ((byte & 0x80) === 0) return value;
    }
  };
  while (at < message.length) {
    const key = Number(varint());
    const wire = key & 7;
    if (wire === 0) yield [key >>> 3, varint()];
    else if (wire === 2) {
      const length = Number(varint());
      yield [key >>> 3, message.subarray(at, at + length)];
      at += length;
    } else if (wire === 1 || wire === 5) at += wire === 1 ? 8 : 4;
    else throw new Error(`wire type ${wire}`);
  }
}
const all = (message: Buffer, field: number): Buffer[] => [...fields(message)].filter(([number, value]) => number === field && Buffer.isBuffer(value)).map(([, value]) => value as Buffer);

/** The string and integer attributes of the KeyValues `attributes`: an AnyValue's string_value (1) or int_value (3). */
function attributesOf(attributes: readonly Buffer[]): Map<string, string> {
  const map = new Map<string, string>();
  for (const pair of attributes) {
    const key = all(pair, 1)[0]?.toString("utf8") ?? "";
    for (const [number, value] of fields(all(pair, 2)[0] ?? Buffer.alloc(0))) {
      if (number === 1 && Buffer.isBuffer(value)) map.set(key, value.toString("utf8"));
      if (number === 3 && typeof value === "bigint") map.set(key, String(value));
    }
  }
  return map;
}

/** The spans of an ExportTraceServiceRequest: resource_spans (1) of a resource (1) and scope_spans (2), each of spans (2). */
function spansOf(request: Buffer) {
  return all(request, 1).flatMap((resourceSpans) => {
    const service = attributesOf(all(all(resourceSpans, 1)[0] ?? Buffer.alloc(0), 1)).get("service.name");
    return all(resourceSpans, 2).flatMap((scopeSpans) => all(scopeSpans, 2)).map((span) => {
      const attributes = attributesOf(all(span, 9));
      return {
        traceId: all(span, 1)[0]?.toString("hex"),
        spanId: all(span, 2)[0]?.toString("hex"),
        service,
        name: all(span, 5)[0]?.toString("utf8"),
        ...(attributes.has("e2e.run") ? { run: attributes.get("e2e.run"), seq: Number(attributes.get("e2e.seq")) } : {}),
      };
    });
  });
}

const admin = new Admin({ clientId: "e2e-log-records", ...access });
const consumer = new Consumer({ clientId: "e2e-log-records", groupId: `e2e-log-records-${randomUUID()}`, autocommit: false, maxWaitTime: 500, ...access });
try {
  const ends = (await consumer.listOffsets({ topics: [TOPIC] })).get(TOPIC) ?? [];
  const starts = (await consumer.listOffsets({ topics: [TOPIC], timestamp: -2n })).get(TOPIC) ?? [];
  const intake = await admin.listConsumerGroupOffsets({ groups: [INTAKE_GROUP] });
  const committed = intake.flatMap(({ topics }) => topics).filter(({ name }) => name === TOPIC).flatMap(({ partitions }) => partitions)
    .map(({ partitionIndex, committedOffset }) => ({ partition: partitionIndex, offset: String(committedOffset) }));
  const records: { partition: number; offset: string; timestamp: string; spans: ReturnType<typeof spansOf> | string }[] = [];
  // Each partition read to its last record; one with none waits for nothing.
  const left = new Map<number, bigint>();
  ends.forEach((end, partition) => {
    if (end > (starts[partition] ?? 0n)) left.set(partition, end - 1n);
  });
  if (left.size > 0) {
    const stream = await consumer.consume({ topics: [TOPIC], mode: MessagesStreamModes.EARLIEST, autocommit: false });
    const deadline = setTimeout(() => void stream.close(), 120_000);
    for await (const message of stream) {
      if (message.timestamp >= from && message.timestamp <= to) {
        let spans: ReturnType<typeof spansOf> | string;
        try {
          spans = spansOf(message.value);
        } catch (error) {
          spans = `undecoded: ${error instanceof Error ? error.message : String(error)}`;
        }
        records.push({ partition: message.partition, offset: String(message.offset), timestamp: String(message.timestamp), spans });
      }
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
