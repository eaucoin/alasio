/**
 * The store as the system sees it, through its profile (DESIGN.md section 4): first-byte
 * latency, the median of 50 ranged GETs of 4 KiB; one connection's GET and PUT throughput of
 * 256 MiB; eight connections' GET throughput, 16 MiB ranges. Of the store the environment names
 * (./s3.ts). Prints the result's `store` object.
 *
 *   node probe.ts [--prefix bench-probe/]
 */
import { randomBytes } from "node:crypto";
import { Agent } from "node:http";
import { parseArgs } from "node:util";

import { DeleteObjectCommand, GetObjectCommand, PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { NodeHttpHandler } from "@smithy/node-http-handler";

import { median } from "../aggregate/aggregate.ts";
import { clientConfig, storeFromEnvironment } from "./s3.ts";

const BIG = 256 * 1024 ** 2;
const RANGE = 16 * 1024 ** 2;

const seconds = (from: bigint) => Number(process.hrtime.bigint() - from) / 1e9;

/** A client of `connections` sockets at most, so one GET is one connection's. */
function clientOf(connections: number): S3Client {
  return new S3Client({ ...clientConfig(), requestHandler: new NodeHttpHandler({ httpAgent: new Agent({ keepAlive: true, maxSockets: connections }) }) });
}

export async function probe(prefix: string) {
  const { bucket } = storeFromEnvironment();
  const one = clientOf(1);
  const eight = clientOf(8);
  const big = `${prefix}big`;
  const body = randomBytes(BIG);

  let started = process.hrtime.bigint();
  await one.send(new PutObjectCommand({ Bucket: bucket, Key: big, Body: body }));
  const putOne = BIG / 1e6 / seconds(started);

  const firstBytes: number[] = [];
  for (let each = 0; each < 50; each++) {
    started = process.hrtime.bigint();
    const { Body } = await one.send(new GetObjectCommand({ Bucket: bucket, Key: big, Range: `bytes=${each * 4096}-${each * 4096 + 4095}` }));
    // The answer's head comes with its first bytes, as toxiproxy delays them together.
    firstBytes.push(seconds(started) * 1000);
    await Body?.transformToByteArray();
  }

  started = process.hrtime.bigint();
  await (await one.send(new GetObjectCommand({ Bucket: bucket, Key: big }))).Body?.transformToByteArray();
  const getOne = BIG / 1e6 / seconds(started);

  started = process.hrtime.bigint();
  let next = 0;
  await Promise.all(Array.from({ length: 8 }, async () => {
    while (next < BIG / RANGE) {
      const start = next++ * RANGE;
      await (await eight.send(new GetObjectCommand({ Bucket: bucket, Key: big, Range: `bytes=${start}-${start + RANGE - 1}` }))).Body?.transformToByteArray();
    }
  }));
  const getEight = BIG / 1e6 / seconds(started);

  await one.send(new DeleteObjectCommand({ Bucket: bucket, Key: big }));
  return { first_byte_ms: median(firstBytes.sort((a, b) => a - b)), get_one_mb_s: getOne, put_one_mb_s: putOne, get_eight_mb_s: getEight };
}

// Run as a program, not imported, nor bundled into another's (../driver/build.ts).
if (/(^|\/)probe\.(ts|mjs)$/u.test(process.argv[1] ?? "")) {
  const { values } = parseArgs({ options: { prefix: { type: "string", default: "bench-probe/" } } });
  console.log(JSON.stringify(await probe(values.prefix ?? "bench-probe/")));
}
