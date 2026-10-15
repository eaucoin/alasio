/**
 * What the log's restart gate's scripts share (gate/log-restart/rounds.ts, restore.ts):
 * spans exported to the collector's gateway, each its own `gate.seq`; otel.spans read back
 * and counted by it; the broker's container told what to do; what they find, said.
 */
import { spawn } from "node:child_process";
import { randomBytes, randomUUID } from "node:crypto";
import { appendFileSync } from "node:fs";
import { join } from "node:path";

import { Consumer, MessagesStreamModes } from "@platformatic/kafka";

export const OUT = process.env["OUT"] ?? "";
const GATEWAY = "http://127.0.0.1:14318/v1/traces";
export const TOPIC = "otel.spans";
const BROKERS = ["localhost:19092"];

export const say = (line: string) => {
  const said = `${new Date().toISOString().slice(11, 19)} ${line}`;
  console.log(said);
  appendFileSync(join(OUT, "events.log"), `${said}\n`);
};

export const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** Runs `command`: its exit code, and what it wrote to its standard output. */
export const run = (command: string, args: readonly string[], env: NodeJS.ProcessEnv = {}) =>
  new Promise<{ code: number; stdout: string }>((resolve) => {
    const child = spawn(command, [...args], { stdio: ["ignore", "pipe", "ignore"], env: { ...process.env, ...env } });
    let stdout = "";
    child.stdout.on("data", (chunk: Buffer) => {
      stdout += chunk.toString("utf8");
    });
    child.on("exit", (code) => resolve({ code: code ?? 1, stdout }));
  });

export const docker = async (args: readonly string[]) => (await run("docker", args)).code;

/** Exports the span `seq` to the gateway, as a producer does: whether it was answered as taken. */
export async function exported(seq: number): Promise<boolean> {
  const now = BigInt(Date.now()) * 1_000_000n;
  const body = {
    resourceSpans: [{
      resource: { attributes: [{ key: "service.name", value: { stringValue: "gate" } }] },
      scopeSpans: [{
        spans: [{
          traceId: randomBytes(16).toString("hex"),
          spanId: randomBytes(8).toString("hex"),
          name: "gate.restart",
          kind: 1,
          startTimeUnixNano: String(now),
          endTimeUnixNano: String(now + 1000n),
          attributes: [{ key: "gate.seq", value: { intValue: String(seq) } }],
        }],
      }],
    }],
  };
  const status = await fetch(GATEWAY, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body), signal: AbortSignal.timeout(30_000) })
    .then(({ status: answered }) => answered, () => 0);
  return status === 200;
}

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

/** The `gate.seq` of each span of an ExportTraceServiceRequest: resource_spans (1), scope_spans (2), spans (2), attributes (9). */
function seqsOf(request: Buffer): number[] {
  return all(request, 1).flatMap((resourceSpans) => all(resourceSpans, 2)).flatMap((scopeSpans) => all(scopeSpans, 2)).flatMap((span) =>
    all(span, 9).flatMap((pair) => {
      if (all(pair, 1)[0]?.toString("utf8") !== "gate.seq") return [];
      const seq = [...fields(all(pair, 2)[0] ?? Buffer.alloc(0))].find(([number]) => number === 3)?.[1];
      return typeof seq === "bigint" ? [Number(seq)] : [];
    })
  );
}

/** What otel.spans holds, read from each partition's start: each span's `gate.seq`, as often as it holds it, and the partitions not read to their end within `withinMs`. */
export async function held(withinMs = 300_000): Promise<{ counts: Map<number, number>; unread: number[] }> {
  const consumer = new Consumer({ clientId: "gate", groupId: `gate-${randomUUID()}`, bootstrapBrokers: BROKERS, autocommit: false, maxWaitTime: 500 });
  try {
    const ends = (await consumer.listOffsets({ topics: [TOPIC] })).get(TOPIC) ?? [];
    const left = new Map<number, bigint>();
    ends.forEach((end, partition) => {
      if (end > 0n) left.set(partition, end - 1n);
    });
    const counts = new Map<number, number>();
    if (left.size === 0) return { counts, unread: [] };
    const stream = await consumer.consume({ topics: [TOPIC], mode: MessagesStreamModes.EARLIEST, autocommit: false });
    const deadline = setTimeout(() => void stream.close(), withinMs);
    try {
      for await (const message of stream) {
        for (const seq of seqsOf(message.value)) counts.set(seq, (counts.get(seq) ?? 0) + 1);
        if (message.offset >= (left.get(message.partition) ?? -1n)) left.delete(message.partition);
        if (left.size === 0) break;
      }
    } catch (error) {
      say(`otel.spans's read ended: ${error instanceof Error ? error.message : String(error)}`);
    }
    clearTimeout(deadline);
    await stream.close().catch(() => {});
    return { counts, unread: [...left.keys()] };
  } finally {
    await consumer.close(true).catch(() => {});
  }
}

/** otel.spans's topic id. */
export async function topicId(): Promise<string> {
  const consumer = new Consumer({ clientId: "gate", groupId: `gate-${randomUUID()}`, bootstrapBrokers: BROKERS });
  try {
    return (await consumer.metadata({ topics: [TOPIC] })).topics.get(TOPIC)?.id ?? "";
  } finally {
    await consumer.close(true).catch(() => {});
  }
}
