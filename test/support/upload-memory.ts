/**
 * A one-pass upload of a file, as a node agent's checkpoint makes it (../../src/node-agent/agent.ts hashAhead,
 * ../../src/node-agent/s3.ts putFile), in a process of its own, so that what it holds is its own: run as
 * `node upload-memory.ts <endpoint> <path> <times>`, it uploads the file so many times, one after another, as a node's saves
 * come, and prints, as JSON, the anonymous memory its process held as it began and at its most while it uploaded, sampled
 * every 10 ms, in bytes.
 */
import { readFileSync } from "node:fs";

import { Effect } from "effect";

import { hashAhead } from "../../src/node-agent/agent.ts";
import { makeS3Bucket } from "../../src/node-agent/s3.ts";

const [endpoint = "", path = "", times = "1"] = process.argv.slice(2);
const anonymous = () => Number(/RssAnon:\s+(\d+) kB/u.exec(readFileSync("/proc/self/status", "utf8"))?.[1] ?? Number.NaN) * 1024;
const bucket = makeS3Bucket({ endpoint, region: "us-east-1", bucket: "snapshots", accessKeyId: "id", secretAccessKey: "secret" });
const began = anonymous();
let most = began;
const sampling = setInterval(() => void (most = Math.max(most, anonymous())), 10);
for (let upload = 0; upload < Number(times); upload++) {
  const ahead = hashAhead(path);
  await Effect.runPromise(bucket.putFile(`main/fs-abc/layers/${upload}/pages.img`, path, ahead.through));
  await ahead.sha256;
}
clearInterval(sampling);
// Its connections to the bucket kept alive would hold it open.
process.stdout.write(JSON.stringify({ began, most: Math.max(most, anonymous()) }), () => process.exit(0));
