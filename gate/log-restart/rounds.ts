/**
 * The log's restart gate's rounds (gate/log-restart/run.sh): ROUNDS rounds, each 40 spans
 * exported one at a time to the collector's gateway, each its own `gate.seq`; then the
 * broker stopped one of three ways, in turn, and started again, as the exports go on until
 * 20 more are taken. Then otel.spans read whole, from each partition's start to its end, and
 * each span counted: taken and not there, lost; there more than once, a duplicate. What it
 * finds, by way of stopping, it says and writes to OUT/results.json; it fails only where it
 * cannot run.
 */
import { spawn } from "node:child_process";
import { randomBytes, randomUUID } from "node:crypto";
import { appendFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

import { Consumer, MessagesStreamModes } from "@platformatic/kafka";

const ROUNDS = Number(process.env["ROUNDS"] ?? 18);
const OUT = process.env["OUT"] ?? "";
const GATEWAY = "http://127.0.0.1:14318/v1/traces";
const TOPIC = "otel.spans";

/** The ways a broker is stopped, as Docker stops its container: with its grace, as a pod deleted is; cut short, as a pod deleted at once is; killed. */
const WAYS = { graceful: ["stop", "-t", "120", "broker"], "cut short": ["stop", "-t", "2", "broker"], killed: ["kill", "broker"] } as const;
type Way = keyof typeof WAYS;

const say = (line: string) => {
  const said = `${new Date().toISOString().slice(11, 19)} ${line}`;
  console.log(said);
  appendFileSync(join(OUT, "events.log"), `${said}\n`);
};

const docker = (args: readonly string[]) =>
  new Promise<number>((resolve) => spawn("docker", [...args], { stdio: "ignore" }).on("exit", (code) => resolve(code ?? 1)));

/** Exports the span `seq` to the gateway, as a producer does: whether it was answered as taken. */
async function exported(seq: number): Promise<boolean> {
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

/** Every span's `gate.seq` otel.spans holds, as often as it holds it. */
async function held(): Promise<Map<number, number>> {
  const consumer = new Consumer({ clientId: "gate", groupId: `gate-${randomUUID()}`, bootstrapBrokers: ["localhost:19092"], autocommit: false, maxWaitTime: 500 });
  try {
    const ends = (await consumer.listOffsets({ topics: [TOPIC] })).get(TOPIC) ?? [];
    const left = new Map<number, bigint>();
    ends.forEach((end, partition) => {
      if (end > 0n) left.set(partition, end - 1n);
    });
    const counts = new Map<number, number>();
    if (left.size === 0) return counts;
    const stream = await consumer.consume({ topics: [TOPIC], mode: MessagesStreamModes.EARLIEST, autocommit: false });
    const deadline = setTimeout(() => void stream.close(), 300_000);
    for await (const message of stream) {
      for (const seq of seqsOf(message.value)) counts.set(seq, (counts.get(seq) ?? 0) + 1);
      if (message.offset >= (left.get(message.partition) ?? -1n)) left.delete(message.partition);
      if (left.size === 0) break;
    }
    clearTimeout(deadline);
    await stream.close().catch(() => {});
    if (left.size > 0) throw new Error(`the topic was not read to its end: partitions ${[...left.keys()].join(", ")} left`);
    return counts;
  } finally {
    await consumer.close(true).catch(() => {});
  }
}

const taken = new Map<number, { readonly round: number; readonly way: Way; readonly during: boolean }>();
let seq = 0;
for (let round = 1; round <= ROUNDS; round += 1) {
  const way = (Object.keys(WAYS) as Way[])[(round - 1) % 3] ?? "graceful";
  // Before: each taken, the broker serving; one refused waits for it, as after the round before.
  for (let before = 0; before < 40; seq += 1) {
    if (await exported(seq)) {
      taken.set(seq, { round, way, during: false });
      before += 1;
    } else await new Promise((resolve) => setTimeout(resolve, 1000));
  }
  const began = Date.now();
  const restarted = docker(WAYS[way]).then(() => docker(["start", "broker"]));
  let after = 0;
  let done = false;
  void restarted.then(() => {
    done = true;
  });
  for (const deadline = Date.now() + 600_000; after < 20 || !done; seq += 1) {
    if (Date.now() > deadline) throw new Error(`round ${round}: the gateway took ${after} exports in ten minutes of the broker ${way}`);
    if (await exported(seq)) {
      taken.set(seq, { round, way, during: true });
      after += 1;
    } else await new Promise((resolve) => setTimeout(resolve, 250));
  }
  say(`round ${round}: the broker ${way}, back in ${Math.round((Date.now() - began) / 1000)} s; ${after} exports taken across it`);
}

say("otel.spans read whole");
const counts = await held();
const results = Object.fromEntries((Object.keys(WAYS) as Way[]).map((way) => {
  const ours = [...taken].filter(([, at]) => at.way === way);
  const lost = ours.filter(([number]) => !counts.has(number)).map(([number, at]) => ({ seq: number, round: at.round, during: at.during }));
  const twice = ours.filter(([number]) => (counts.get(number) ?? 0) > 1).map(([number]) => ({ seq: number, times: counts.get(number) }));
  return [way, { rounds: new Set(ours.map(([, at]) => at.round)).size, taken: ours.length, lost, duplicates: twice }];
}));
writeFileSync(join(OUT, "results.json"), `${JSON.stringify(results, null, 2)}\n`);
for (const [way, { rounds, taken: count, lost, duplicates }] of Object.entries(results)) {
  say(`the broker ${way}, ${rounds} rounds: ${count} spans taken, ${lost.length} lost${lost.length ? ` (${lost.map(({ seq: number }) => number).join(", ")})` : ""}, ${duplicates.length} written twice or more`);
}
