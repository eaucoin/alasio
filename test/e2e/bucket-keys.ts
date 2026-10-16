/**
 * Run inside a node agent's pod, by node there (inNodeAgent in harness.ts), with the
 * snapshots bucket's settings the agent is given, through the agent's own client of the
 * bucket, of whichever release the agent runs, as the upgrade from one runs it on both:
 *
 * - `<prefix>` prints one JSON line of the keys of the bucket's objects under the prefix,
 *   in order.
 *
 * It imports nothing of alasio's but the client's maker, which each release has, as
 * inNodeAgent in harness.ts rewrites its path to the image's as it pipes this in.
 */
import { Effect } from "effect";

import { makeS3Bucket } from "../../src/node-agent/s3.ts";

const [prefix] = process.argv.slice(2);
if (!prefix) throw new Error("usage: bucket-keys.ts <prefix>");

/** The variable of the agent's environment `name`, which it is given. */
function variable(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`the node agent is given no ${name}`);
  return value;
}

const bucket = makeS3Bucket({
  endpoint: variable("S3_ENDPOINT"),
  region: variable("S3_REGION"),
  bucket: variable("S3_BUCKET"),
  accessKeyId: variable("AWS_ACCESS_KEY_ID"),
  secretAccessKey: variable("AWS_SECRET_ACCESS_KEY"),
});

// 7.0.0's client lists the keys, later releases' the objects of them.
const listed: readonly (string | { readonly key: string })[] = await Effect.runPromise(bucket.list(prefix));
console.log(JSON.stringify(listed.map((each) => (typeof each === "string" ? each : each.key)).sort()));
