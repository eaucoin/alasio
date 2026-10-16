/**
 * Run inside a node agent's pod, by node there (inNodeAgent in harness.ts), with the
 * snapshots bucket's settings the agent is given, through the agent's own client of the
 * bucket, of whichever release the agent runs, as the upgrade from one runs it on both:
 *
 * - `get <key>` prints one JSON line of the object's JSON, or null for none.
 * - `put <key> <json>` writes the object's JSON, then prints one JSON line of what it wrote.
 *
 * It imports nothing of alasio's but the client's maker, which each release has, as
 * inNodeAgent in harness.ts rewrites its path to the image's as it pipes this in.
 */
import { Effect } from "effect";

import { makeS3Bucket } from "../../src/node-agent/s3.ts";

const [verb, key, json] = process.argv.slice(2);
if (!key || !(verb === "get" || (verb === "put" && json))) throw new Error("usage: bucket-json.ts get <key> | put <key> <json>");

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

if (verb === "put" && json) {
  const value: unknown = JSON.parse(json);
  await Effect.runPromise(bucket.putJson(key, value));
  console.log(JSON.stringify(value));
} else {
  console.log(JSON.stringify(await Effect.runPromise(bucket.getJson(key))));
}
