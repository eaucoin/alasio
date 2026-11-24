/**
 * Run inside a node agent's pod, by node there (inNodeAgent in harness.ts), with the
 * snapshots bucket's settings the agent is given, through the agent's own client of the
 * bucket:
 *
 * - `corrupt <environment> <workspace>` overwrites the largest file of the workspace's
 *   current snapshot's image in the bucket with as many zeros, as a disk's rot would.
 *   Prints one JSON line of the file overwritten.
 * - `release <environment> <workspace> <release>` says the workspace's current snapshot was
 *   taken by gVisor's `release`, as one taken by a release no longer kept says, in the bucket and
 *   in the workspace's record of its release, whose manifest a bind restores. Prints one JSON line
 *   of the snapshot and the release it said.
 * - `outlive <environment> <workspace>` says the workspace's current snapshot was taken live,
 *   at a state generation of bayma's its state directory, as the snapshot's pod template runs
 *   bayma with it, has never been at: one that holds the session no more, which a restore of it
 *   finds as it would resume its sandbox; in the bucket and in the workspace's record of its
 *   release. Prints one JSON line of the snapshot and the generation it said.
 *
 * The record is read and written through the agent's own API, on its pod's loopback, with the
 * token the agent is given.
 *
 * alasio's modules are imported by their paths in the repository, which inNodeAgent in
 * harness.ts rewrites to the image's as it pipes this in.
 */
import { writeFileSync } from "node:fs";

import { Effect, Schema } from "effect";

import { CURRENT_FILE, Manifest } from "../../src/node-agent/agent.ts";
import { LAYERS_DIR } from "../../src/node-agent/cache.ts";
import { NODE_AGENT_PORT, NODE_AGENT_TOKEN_VARIABLE } from "../../src/node-agent/names.ts";
import { makeS3Bucket } from "../../src/node-agent/s3.ts";

/** The file of the image overwritten: its snapshot, its image, its path in the image, and its size. */
export interface Corrupted {
  readonly snapshot: string;
  readonly image: string;
  readonly path: string;
  readonly bytes: number;
}

/** The snapshot whose release was said: its id, and the release it was said to be taken by. */
export interface Released {
  readonly snapshot: string;
  readonly release: string;
}

/** The snapshot said taken live at a generation bayma's state has never been at: its id, and that generation. */
export interface Outlived {
  readonly snapshot: string;
  readonly generation: number;
}

/** A generation of bayma's state no session reaches, as each exec moves it on by one. */
const NEVER_REACHED = 2 ** 31;

const [verb, environment, workspace, release] = process.argv.slice(2);
if (!(verb === "corrupt" || verb === "outlive" || (verb === "release" && release)) || !environment || !workspace) {
  throw new Error("usage: snapshot-bucket.ts corrupt <environment> <workspace> | release <environment> <workspace> <release> | outlive <environment> <workspace>");
}

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

const current = `${environment}/${workspace}/${CURRENT_FILE}`;

/** The workspace's record of its release, through the agent's API: what `change` makes of the one there is, of its manifest as `change` makes it. */
const recordSays = (change: (manifest: Manifest) => Manifest) =>
  Effect.promise(async () => {
    const url = `http://127.0.0.1:${NODE_AGENT_PORT}/workspaces/${encodeURIComponent(environment ?? "")}/${encodeURIComponent(workspace ?? "")}/released`;
    const headers = { authorization: `Bearer ${variable(NODE_AGENT_TOKEN_VARIABLE)}` };
    const read = await fetch(url, { headers });
    if (!read.ok) throw new Error(`the record of ${environment}/${workspace}'s release could not be read: ${read.status} ${await read.text()}`);
    const record = (await read.json()) as { readonly manifest: Manifest };
    const manifest = change(record.manifest);
    const written = await fetch(url, { method: "PUT", headers: { ...headers, "content-type": "application/json" }, body: JSON.stringify({ ...record, release: manifest.release, manifest }) });
    if (!written.ok) throw new Error(`the record of ${environment}/${workspace}'s release could not be written: ${written.status} ${await written.text()}`);
  });

const said = await Effect.runPromise(Effect.gen(function*() {
  const manifest = yield* Schema.decodeUnknownEffect(Manifest)(yield* bucket.getJson(current));
  if (verb === "release" && release) {
    yield* bucket.putJson(current, { ...manifest, release });
    yield* recordSays((recorded) => ({ ...recorded, release }));
    const done: Released = { snapshot: manifest.snapshot, release };
    return done;
  }
  if (verb === "outlive") {
    // The state directory bayma runs with, as the arguments of the template's containers give it.
    const containers = (manifest.podTemplate as { readonly spec?: { readonly containers?: readonly { readonly args?: readonly string[] }[] } }).spec?.containers ?? [];
    const stateDir = containers.flatMap(({ args = [] }) => (args.includes("--state-dir") ? [args[args.indexOf("--state-dir") + 1] ?? ""] : []))[0];
    if (!stateDir) return yield* Effect.die(new Error(`the snapshot ${manifest.snapshot} of ${environment}/${workspace} runs no bayma with a state directory`));
    yield* bucket.putJson(current, { ...manifest, live: { stateDir, generation: NEVER_REACHED, running: [] } });
    yield* recordSays((recorded) => ({ ...recorded, live: { stateDir, generation: NEVER_REACHED, running: [] } }));
    const done: Outlived = { snapshot: manifest.snapshot, generation: NEVER_REACHED };
    return done;
  }
  // The largest file of its image, which is the sandbox's memory, never all zeros.
  const [image] = manifest.layers;
  const [file] = [...(image?.files ?? [])].sort((a, b) => b.bytes - a.bytes);
  if (!image || !file) return yield* Effect.die(new Error(`the snapshot ${manifest.snapshot} of ${environment}/${workspace} has no file`));
  const zeros = "/tmp/e2e-corrupted";
  writeFileSync(zeros, Buffer.alloc(file.bytes));
  yield* bucket.putFile(`${environment}/${workspace}/${LAYERS_DIR}/${image.digest}/${file.path}`, zeros);
  const done: Corrupted = { snapshot: manifest.snapshot, image: image.digest, path: file.path, bytes: file.bytes };
  return done;
}));
console.log(JSON.stringify(said));
