/**
 * 12's gates on k3s (program/12-live-lazy.md, gates 2 to 7): alasio up on k3s on the runner,
 * the host target (test/e2e/harness.ts), its sessions' runtime given 12's options (dirty
 * tracking by write-protection, working sets recorded, restores in the background), and a
 * real session pod, checkpointed by runsc beside gVisor's shim as the node agent does, with
 * its flags, and restored into a new pod of its Sandbox through the fork's restore
 * annotation:
 *
 *   2. live checkpoints, during an exec, leave the pod, its container and the exec running;
 *   3. a chain of three (two live, one at release) restores through the adapter, its layers
 *      linked in the prepared directory's layers/<digest>/;
 *   4. a chain restores straight from the snapshots bucket, its gofer reaching the store from
 *      the node, from a directory holding s3_opts.json alone, each image of the chain in the
 *      bucket once, under layers/<digest>/, as the node agent keeps them;
 *   5. a delta after a restore from the bucket: whether its parent's pages_meta.img alone
 *      will do, or its pages too;
 *   6. a server-side copy of a 1 GiB layer in the bundled SeaweedFS: its time and bytes;
 *   7. a live snapshot restored after its pod went, during its exec, which goes on; and one
 *      whose held file changed since, which the restore refuses.
 *
 * What each records goes into $OUT/evidence.json.
 *
 *   ALASIO_E2E_TARGET=host ALASIO_E2E_REGISTRY=... OUT=... node --test gate/12/chain.test.ts
 */
import assert from "node:assert/strict";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { after, before, test } from "node:test";

import type { CoreV1Event, V1PersistentVolume, V1Pod, V1Secret, V1Service } from "@kubernetes/client-node";
import { Effect } from "effect";

import pins from "../../cluster/node/pins.json" with { type: "json" };
import { NAMESPACE } from "../../cli/src/manifests/common.ts";
import { CONTAINERD, gvisorDirectory, gvisorHandler, gvisorRuntimeClass } from "../../src/node-agent/names.ts";
import { runscFlags } from "../../src/node-agent/runsc.ts";
import { makeS3Bucket, type S3Bucket } from "../../src/node-agent/s3.ts";
import { hookOf } from "../../src/sandbox/snapshots.ts";
import { dumpClusterState, inAlasio, inSession, kube, onNode, ref, SESSIONS, setUp, TARGET, tearDown } from "../../test/e2e/harness.ts";
import type { Created, Read, Seen, Started } from "./session.ts";

const OUT = process.env["OUT"] ?? "/tmp/gate-12";
/** Where k3s's containerd has runsc keep its sandboxes' state, as gVisor's shims run it. */
const RUNSC_ROOT = "/run/containerd/runsc/k8s.io";
/** The images the gate takes, on the node, apart from the node agent's cache, which collects what no Sandbox is bound to. */
const IMAGES = "/var/lib/alasio-gate-12";
const RESTORE_ANNOTATION = "dev.gvisor.internal.restore.host-image-path";
/** What bayma tells the first exec of a session restored whole. */
const CAME_BACK_WHOLE = /^bayma: this session came back whole with bayma's sandbox, restored from a snapshot taken at /u;

const [{ release } = { release: "" }] = pins.gvisor.releases;
const RUNSC = `${gvisorDirectory(release)}/runsc`;
const OPTIONS = `${CONTAINERD}/${gvisorHandler(release)}.toml`;

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
/** On the node, which on the host target is this machine. */
const node = (script: string) => onNode("", script);
/** `value` as one word of sh. */
const q = (value: string) => `'${value.split("'").join(`'"'"'`)}'`;
/** Runs the release's runsc as the shims run it, with `args`: what it printed. */
let runsc: (args: readonly string[]) => Promise<string>;

const evidence: Record<string, unknown> = {};
const session = { volumeId: "", replId: "" };
/** Where the gate's image `name` is. */
const at = (name: string) => `${IMAGES}/${name}`;

const inGate = <Seen>(...args: string[]) => inAlasio<Seen>("../../gate/12/session.ts", ...args);

/** The session's pod's sandbox, and its bayma container, by their runtime's ids, while it runs. */
async function ids(): Promise<{ readonly pod: string; readonly bayma: string }> {
  const pod = (await node(`k3s crictl pods --namespace ${SESSIONS} --name '^${session.volumeId}$' --state ready --quiet`)).trim();
  assert.ok(pod, `the session ${session.volumeId} has no ready sandbox`);
  const bayma = (await node(`k3s crictl ps --pod ${pod} --name '^bayma$' --quiet`)).trim();
  assert.ok(bayma, `the sandbox ${pod} runs no bayma`);
  return { pod, bayma };
}

/** The hook alasio runs in bayma's container as it checkpoints it. */
async function hook(): Promise<string> {
  const pod = await kube.get<V1Pod>(ref("Pod", session.volumeId, SESSIONS));
  const bayma = pod?.spec?.containers.find(({ name }) => name === "bayma");
  const argv = bayma ? hookOf(bayma) : null;
  assert.ok(argv, "the session's pod runs no bayma with a state directory");
  return argv.join(" ");
}

/** An image of the session's sandbox, `name`, live or at release, a delta of `parent`'s if given: how long it took, and its bytes. */
async function checkpoint(name: string, { live, parent }: { readonly live: boolean; readonly parent?: string }): Promise<{ readonly ms: number; readonly bytes: number }> {
  const { bayma } = await ids();
  const image = at(name);
  const startedAt = Date.now();
  await runsc([
    "checkpoint", `--image-path=${image}`, "--compression=none", "--direct",
    ...(live ? ["--leave-running", "--precopy=auto", "--precopy-throttle=on"] : []),
    ...(parent ? [`--parent-image-path=${parent}`] : []),
    `--save-restore-exec-argv=${await hook()}`, "--save-restore-exec-timeout=60s",
    bayma,
  ]);
  const ms = Date.now() - startedAt;
  return { ms, bytes: Number((await node(`du -sb ${q(image)} | cut -f1`)).trim()) };
}

/** An image's identity, as runsc says it. */
async function digest(image: string): Promise<string> {
  return (JSON.parse(await runsc(["image", "inspect", "--json", image])) as { digest: string }).digest;
}

/**
 * A directory to restore `name` from: its files, and in layers/<digest>/ each image it
 * refers to, linked, each found among `layers`.
 */
async function prepared(name: string, layers: readonly string[]): Promise<string> {
  const image = at(name);
  const directory = at(`restore-${name}`);
  const wanted = (await runsc(["image", "layers", image])).split("\n").filter(Boolean);
  const found = new Map(await Promise.all(layers.map(async (layer) => [await digest(at(layer)), at(layer)] as const)));
  assert.deepEqual(wanted.filter((each) => !found.has(each)), [], `${name} refers to layers the gate did not take`);
  await node([
    `rm -rf ${q(directory)}`,
    `mkdir -p ${q(directory)}/layers`,
    `for file in ${q(image)}/*; do if [ -f "$file" ]; then ln "$file" ${q(directory)}/; fi; done`,
    ...wanted.map((each) => `ln -s ${q(found.get(each) ?? "")} ${q(`${directory}/layers/${each}`)}`),
  ].join(" && "));
  return directory;
}

/** The session's pod, in a word: gone, or its phase, readiness and bayma's state. */
async function podState(): Promise<string> {
  const pod = await kube.get<V1Pod>(ref("Pod", session.volumeId, SESSIONS));
  if (!pod) return "gone";
  const bayma = pod.status?.containerStatuses?.find(({ name }) => name === "bayma");
  const state = bayma?.state?.running ? "running" : bayma?.state?.waiting ? `waiting ${bayma.state.waiting.reason}` : bayma?.state?.terminated ? `terminated ${bayma.state.terminated.reason} ${bayma.state.terminated.exitCode}` : "none";
  return `${pod.status?.phase}${pod.status?.conditions?.some(({ type, status }) => type === "Ready" && status === "True") ? ", ready" : ""}, bayma ${state}`;
}

/** The latest events of the session's pod. */
async function events(): Promise<string[]> {
  const listed = await kube.list<CoreV1Event>("Event", { namespace: SESSIONS, fieldSelector: `involvedObject.name=${session.volumeId}` });
  return [...listed]
    .sort((a, b) => new Date(a.lastTimestamp ?? a.eventTime ?? 0).getTime() - new Date(b.lastTimestamp ?? b.eventTime ?? 0).getTime())
    .slice(-15)
    .map(({ type, reason, message }) => `${type} ${reason}: ${message?.trim()}`);
}

/**
 * The session's Sandbox suspended, its pod gone, and running again, its new pod restored
 * from `directory`, or started afresh for null: whether bayma came up ready, in how long,
 * and the pod's states and events meanwhile. Where its pod `crashed`, its sandbox is killed
 * first, as a node lost ends it: bayma stopped by SIGTERM persists its sessions, which moves
 * its state generation on from a live snapshot, which it then refuses to restore, as it
 * should (alasio takes a release snapshot of a pod it stops, before its SIGTERM).
 */
async function restoreFrom(directory: string | null, { crashed = false }: { readonly crashed?: boolean } = {}): Promise<{ readonly ready: boolean; readonly ms: number; readonly states: string[]; readonly events: string[] }> {
  // containerd stops a pod's sandbox by killing its containers at once, with no SIGTERM;
  // the Sandbox is suspended before the kubelet makes it again.
  if (crashed) await node(`k3s crictl stopp ${(await ids()).pod}`);
  await kube.patch(ref("Sandbox", session.volumeId, SESSIONS), { spec: { operatingMode: "Suspended" } });
  for (let waited = 0; (await podState()) !== "gone"; waited++) {
    assert.ok(waited < 120, `the pod of ${session.volumeId} is still there`);
    await sleep(1000);
  }
  const startedAt = Date.now();
  await kube.patch(ref("Sandbox", session.volumeId, SESSIONS), {
    spec: { operatingMode: "Running", podTemplate: { metadata: { annotations: { [RESTORE_ANNOTATION]: directory } } } },
  });
  const states: string[] = [];
  let last = "";
  for (;;) {
    const state = await podState();
    if (state !== last) states.push(`${(Date.now() - startedAt) / 1000}s ${state}`);
    last = state;
    if (state.includes("ready, bayma running")) return { ready: true, ms: Date.now() - startedAt, states, events: await events() };
    if (/terminated|CrashLoopBackOff|RunContainerError|Failed/u.test(state) || Date.now() - startedAt > 180_000) {
      return { ready: false, ms: Date.now() - startedAt, states, events: await events() };
    }
    await sleep(250);
  }
}

/** bayma's state generation on the disk, as its state directory's generation.json says it, read beside bayma. */
const generation = async (): Promise<unknown> =>
  JSON.parse(await inSession(session.volumeId, 'process.stdout.write(require("node:fs").readFileSync("/home/agent/.bayma/generation.json", "utf8"))'));

/** Lets the exec `start` began end. */
const letEnd = () => inSession(session.volumeId, 'require("node:fs").writeFileSync("/workspace/gate-12-go", "")');
/** Makes the next exec `start` begins wait again. */
const holdNext = () => inSession(session.volumeId, 'require("node:fs").rmSync("/workspace/gate-12-go", { force: true })');

/**
 * Records what `run` sees under `name`, the error it throws too, and then what the session's
 * bayma last logged, which says how its sandbox's hook went, dumping the cluster's state.
 */
async function recorded(name: string, run: (seen: Record<string, unknown>) => Promise<void>): Promise<void> {
  const seen: Record<string, unknown> = {};
  evidence[name] = seen;
  try {
    await run(seen);
  } catch (error) {
    seen["error"] = error instanceof Error ? error.stack : String(error);
    seen["baymaLogs"] = (await kube.logs(SESSIONS, session.volumeId, "bayma").catch((failed: unknown) => String(failed))).trimEnd().split("\n").slice(-80);
    await dumpClusterState();
    throw error;
  }
}

before(async () => {
  assert.equal(TARGET, "host", "12's gates on k3s run on the host target");
  try {
    await setUp();
    // 12's options for the sessions' runtime, which each new shim reads as it starts.
    await node([
      `sed -i 's/^restore_direct = true$/restore_direct = true\\nrestore_background = true/' ${OPTIONS}`,
      `printf '%s\\n' '  dirty-tracking = "wp"' '  working-set-window = "3s"' >>${OPTIONS}`,
      `mkdir -p ${IMAGES}`,
    ].join(" && "));
    const options = await node(`cat ${OPTIONS}`);
    evidence["options"] = options;
    const flags = runscFlags(options);
    assert.ok(flags.includes("--dirty-tracking=wp") && options.includes("restore_background = true"), options);
    runsc = (args) => node([RUNSC, `--root=${RUNSC_ROOT}`, ...flags, ...args].map(q).join(" "));
    const created = await inGate<Created>("create");
    Object.assign(session, created);
    evidence["session"] = created;
  } catch (error) {
    await dumpClusterState();
    throw error;
  }
}, { timeout: 120 * 60_000 });

after(async () => {
  mkdirSync(OUT, { recursive: true });
  writeFileSync(join(OUT, "evidence.json"), `${JSON.stringify(evidence, null, 2)}\n`);
  console.error(JSON.stringify(evidence, null, 2));
  await tearDown();
}, { timeout: 30 * 60_000 });

test("2. live checkpoints beside the shim, during an exec, leave the pod, its container and the exec running", { timeout: 30 * 60_000 }, () =>
  recorded("live", async (seen) => {
    const { execId } = await inGate<Started>("start", session.volumeId, session.replId);
    await sleep(2000);
    seen["A"] = await checkpoint("A", { live: true });
    seen["B"] = await checkpoint("B", { live: true, parent: at("A") });
    seen["pod"] = await podState();
    assert.match(String(seen["pod"]), /^Running, ready, bayma running$/u);
    seen["container"] = (await node(`k3s crictl inspect --output go-template --template '{{.status.state}}' ${(await ids()).bayma}`)).trim();
    assert.equal(seen["container"], "CONTAINER_RUNNING");
    assert.equal((await inSession(session.volumeId, 'console.log("answers")')).trim(), "answers");
    await letEnd();
    const finished = await inGate<Seen>("finish", session.volumeId, session.replId, execId);
    seen["finished"] = finished;
    assert.deepEqual(finished, { result: "42", notice: null });
    await holdNext();
  }));

test("3. a chain of three, two live and one at release, restores through the adapter, in the background, its layers linked", { timeout: 30 * 60_000 }, () =>
  recorded("chain", async (seen) => {
    await inGate("set", session.volumeId, session.replId, "3");
    seen["C"] = await checkpoint("C", { live: false, parent: at("B") });
    seen["layers"] = (await runsc(["image", "layers", at("C")])).split("\n").filter(Boolean);
    const restored = await restoreFrom(await prepared("C", ["A", "B"]));
    seen["restore"] = restored;
    assert.ok(restored.ready, JSON.stringify(restored));
    const read = await inGate<Read>("read", session.volumeId, session.replId);
    seen["read"] = read;
    assert.deepEqual([read.marker, read.step], ["42", "3"]);
    assert.match(read.notice ?? "", CAME_BACK_WHOLE);
  }));

/** The snapshots bucket, as the node agents reach it, at an address this machine reaches: the Service's. */
let bucket: S3Bucket;
let s3: { readonly endpoint: string; readonly region: string; readonly bucket: string; readonly accessKeyId: string; readonly secretAccessKey: string };

/** Whether this machine, as a node, reaches the bucket: what listing it said. */
const reaches = () => Effect.runPromise(bucket.list("gate-12/").pipe(Effect.as("listed"), Effect.catch((error) => Effect.succeed(error.message))));

/** Where gate 4 put image D in the bucket, for the diagnostic of copies. */
let uploadedD: { readonly own: string; readonly layersPrefix: string } | undefined;

/** Puts the files of `image` under `prefix`. */
async function upload(image: string, prefix: string): Promise<void> {
  for (const file of (await node(`find ${q(image)} -maxdepth 1 -type f -printf '%f\\n'`)).split("\n").filter(Boolean)) {
    // The bucket's client runs as this user, which reads only what root lets it.
    const readable = at(`upload-${file}`);
    await node(`cp ${q(`${image}/${file}`)} ${q(readable)} && chmod 644 ${q(readable)}`);
    await Effect.runPromise(bucket.putFile(`${prefix}${file}`, readable));
    await node(`rm -f ${q(readable)}`);
  }
}

/** The options of the S3 gofer for the image at `objectPrefix` in the bucket, its layers each under `layersPrefix`, as JSON. */
const goferOptions = (objectPrefix: string, layersPrefix: string) =>
  JSON.stringify({ endpoint: s3.endpoint, region: s3.region, bucket: s3.bucket, object_prefix: objectPrefix, layers_prefix: layersPrefix, addressing: "path", credentials: { access_key_id: s3.accessKeyId, secret_access_key: s3.secretAccessKey } });

test("4. a chain restores straight from the bucket, from a directory holding s3_opts.json alone", { timeout: 30 * 60_000 }, () =>
  recorded("bucket", async (seen) => {
    const secret = await kube.get<V1Secret>(ref("Secret", "alasio-snapshots", NAMESPACE));
    const value = (key: string) => Buffer.from(secret?.data?.[key] ?? "", "base64").toString("utf8");
    const service = new URL(value("S3_ENDPOINT"));
    const address = (await kube.get<V1Service>(ref("Service", service.hostname.split(".")[0] ?? "", NAMESPACE)))?.spec?.clusterIP;
    s3 = { endpoint: `http://${address}:${service.port || 80}`, region: value("S3_REGION"), bucket: value("S3_BUCKET"), accessKeyId: value("AWS_ACCESS_KEY_ID"), secretAccessKey: value("AWS_SECRET_ACCESS_KEY") };
    bucket = makeS3Bucket(s3);
    seen["endpoint"] = s3.endpoint;
    seen["reachedFromTheNode"] = await reaches();

    await inGate("set", session.volumeId, session.replId, "4");
    seen["D"] = await checkpoint("D", { live: false, parent: at("restore-C") });
    // Each image once, by its digest, as the node agent keeps a workspace's.
    const layersPrefix = `gate-12/${session.volumeId}/layers/`;
    const layers = (await runsc(["image", "layers", at("D")])).split("\n").filter(Boolean);
    seen["layers"] = layers;
    const byDigest = new Map(await Promise.all(["A", "B", "C"].map(async (name) => [await digest(at(name)), at(name)] as const)));
    const own = await digest(at("D"));
    const uploadingAt = Date.now();
    await upload(at("D"), `${layersPrefix}${own}/`);
    for (const layer of layers) await upload(byDigest.get(layer) ?? "", `${layersPrefix}${layer}/`);
    seen["uploadMs"] = Date.now() - uploadingAt;
    uploadedD = { own, layersPrefix };

    // runsc, and the shim through it, reads every file of the image from the bucket.
    const directory = at("bucket-D");
    await node(`rm -rf ${q(directory)} && mkdir -p ${q(directory)} && umask 077 && printf '%s' ${q(goferOptions(`${layersPrefix}${own}/`, layersPrefix))} >${q(`${directory}/s3_opts.json`)}`);
    const restored = await restoreFrom(directory);
    seen["restore"] = restored;
    assert.ok(restored.ready, JSON.stringify(restored));
    const read = await inGate<Read>("read", session.volumeId, session.replId);
    seen["read"] = read;
    assert.deepEqual([read.marker, read.step], ["42", "4"]);
    assert.match(read.notice ?? "", CAME_BACK_WHOLE);
  }));

test("5. a delta after a restore from the bucket, its parent's pages_meta.img alone on the node, or its pages too", { timeout: 30 * 60_000 }, () =>
  recorded("deltaAfterBucket", async (seen) => {
    await inGate("set", session.volumeId, session.replId, "5");
    const metadataOnly = at("parent-D-metadata");
    await node(`rm -rf ${q(metadataOnly)} && mkdir -p ${q(metadataOnly)} && cp ${q(`${at("D")}/pages_meta.img`)} ${q(metadataOnly)}/`);
    try {
      seen["E"] = await checkpoint("E", { live: false, parent: metadataOnly });
      seen["parentNeeds"] = "pages_meta.img";
    } catch (error) {
      seen["withMetadataOnly"] = error instanceof Error ? error.message : String(error);
      // The sandbox ran on if the checkpoint failed: with the parent whole, then.
      seen["E"] = await checkpoint("E", { live: false, parent: at("D") });
      seen["parentNeeds"] = "pages.img too";
    }
    seen["layers"] = (await runsc(["image", "layers", at("E")])).split("\n").filter(Boolean);
    const restored = await restoreFrom(await prepared("E", ["A", "B", "C", "D"]));
    seen["restore"] = restored;
    assert.ok(restored.ready, JSON.stringify(restored));
    const read = await inGate<Read>("read", session.volumeId, session.replId);
    seen["read"] = read;
    assert.deepEqual([read.marker, read.step], ["42", "5"]);
  }));

test("6. a server-side copy of a 1 GiB layer in the bundled SeaweedFS: its time, and the bytes it adds", { timeout: 30 * 60_000 }, () =>
  recorded("copy", async (seen) => {
    // What the object store keeps on this machine: its volume servers' claims, on k3s's local storage.
    const volumes = (await kube.list<V1PersistentVolume>("PersistentVolume"))
      .filter(({ spec }) => spec?.claimRef?.namespace === NAMESPACE && /seaweedfs-volume/u.test(spec.claimRef.name ?? ""))
      .map(({ spec }) => spec?.hostPath?.path ?? spec?.local?.path ?? "");
    seen["volumes"] = volumes;
    assert.ok(volumes.length > 0 && volumes.every(Boolean), "the object store's volume servers have no claim on this machine");
    const stored = async () => Number((await node(`du -sbc ${volumes.map(q).join(" ")} | tail -1 | cut -f1`)).trim());
    const source = at("gibibyte");
    const bytes = 1024 ** 3;
    // Random, as pages are, so that nothing on the way makes it smaller.
    await node(`head -c ${bytes} /dev/urandom >${q(source)} && chmod 644 ${q(source)}`);
    const before = await stored();
    await Effect.runPromise(bucket.putFile("gate-12/copy/source", source));
    const put = await stored();
    const copyingAt = Date.now();
    await Effect.runPromise(bucket.copy("gate-12/copy/source", "gate-12/copy/target", bytes));
    seen["copyMs"] = Date.now() - copyingAt;
    const copied = await stored();
    seen["bytes"] = { put: put - before, copied: copied - put };
    await Effect.runPromise(bucket.remove("gate-12/copy/source"));
    await Effect.runPromise(bucket.remove("gate-12/copy/target"));
    await node(`rm -f ${q(source)}`);
  }));

test("7. a live snapshot restores into a new pod while its exec runs, which goes on; one whose held file changed since is refused", { timeout: 45 * 60_000 }, () =>
  recorded("liveRestore", async (seen) => {
    await inGate("set", session.volumeId, session.replId, "7");
    const first = await inGate<Started>("start", session.volumeId, session.replId);
    await sleep(2000);
    seen["L"] = await checkpoint("L", { live: true, parent: at("restore-E") });
    // bayma's state generation as L caught it, and as its pod crashes: one moved on since makes
    // L outlived, which the restored bayma refuses, telling its exec nothing.
    seen["generationAfterL"] = await generation();
    const directory = await prepared("L", ["A", "B", "C", "D", "E"]);
    seen["generationAtCrash"] = await generation();
    const restored = await restoreFrom(directory, { crashed: true });
    seen["restore"] = restored;
    assert.ok(restored.ready, JSON.stringify(restored));
    await letEnd();
    const finished = await inGate<Seen>("finish", session.volumeId, session.replId, first.execId);
    seen["finished"] = finished;
    // Told nothing, the exec that ran on: the next, if it is told instead, as where the restored
    // bayma heard of its restore, which gVisor runs its hook for once it runs, after it ended.
    if (finished.notice === null) seen["next"] = await inGate<Read>("read", session.volumeId, session.replId).catch((error: unknown) => String(error));
    assert.equal(finished.result, "42");
    assert.match(finished.notice ?? "", CAME_BACK_WHOLE);
    await holdNext();

    const second = await inGate<Started>("start", session.volumeId, session.replId);
    await sleep(2000);
    seen["M"] = await checkpoint("M", { live: true, parent: at("restore-L") });
    await inSession(session.volumeId, 'require("node:fs").appendFileSync("/workspace/gate-12-held", " and changed")');
    const refused = await restoreFrom(await prepared("M", ["A", "B", "C", "D", "E", "L"]), { crashed: true });
    seen["refused"] = refused;
    assert.equal(refused.ready, false, JSON.stringify(refused));
    // For the file it holds, which changed, and for nothing else.
    assert.ok(refused.events.some((event) => event.includes("gate-12-held")), JSON.stringify(refused.events));
    assert.ok(!refused.events.some((event) => event.includes("generation.json")), JSON.stringify(refused.events));
    const afresh = await restoreFrom(null);
    seen["afresh"] = afresh;
    assert.ok(afresh.ready, JSON.stringify(afresh));
    seen["afterwards"] = await inGate<Read>("read", session.volumeId, session.replId).catch((error: unknown) => String(error));
    seen["exec"] = second.execId;
  }));

test("D. diagnostic: a server-side copy of an image reads the same whole and in ranges, and restores from the bucket as its source does; gVisor .6 restores from the bucket an image put there", { timeout: 40 * 60_000 }, () =>
  recorded("diagnostic", async (seen) => {
    assert.ok(uploadedD, "gate 4 put no image in the bucket");
    const { own, layersPrefix } = uploadedD;
    const copiedPrefix = `gate-12/${session.volumeId}/copied/layers/`;
    // Each object of D and its layers, copied server-side, as the agent's fork and the cut-over's move copy them.
    const objects = await Effect.runPromise(bucket.list(layersPrefix));
    // Each read through the AWS CLI on this machine: whole, and in ranges as a reader of pages reads, its SHA-256 and size.
    const credentials = `AWS_ACCESS_KEY_ID=${q(s3.accessKeyId)} AWS_SECRET_ACCESS_KEY=${q(s3.secretAccessKey)} AWS_DEFAULT_REGION=${q(s3.region)}`;
    const sizes = new Map<string, number>();
    for (const key of objects) {
      const bytes = Number((await node(`${credentials} aws --endpoint-url ${q(s3.endpoint)} s3api head-object --bucket ${q(s3.bucket)} --key ${q(key)} --query ContentLength --output text`)).trim());
      sizes.set(key, bytes);
      await Effect.runPromise(bucket.copy(key, `${copiedPrefix}${key.slice(layersPrefix.length)}`, bytes));
    }
    seen["copied"] = Object.fromEntries(sizes);
    const read = async (key: string, range?: string) =>
      (await node(`rm -f /tmp/diag-read && ${credentials} aws --endpoint-url ${q(s3.endpoint)} s3api get-object --bucket ${q(s3.bucket)} --key ${q(key)} ${range ? `--range ${q(range)}` : ""} /tmp/diag-read >/dev/null 2>/tmp/diag-err; echo "$(sha256sum /tmp/diag-read 2>/dev/null | cut -d' ' -f1) $(stat -c %s /tmp/diag-read 2>/dev/null) $(head -c 300 /tmp/diag-err | tr '\n' ' ')"`)).trim();
    const differing: Record<string, unknown>[] = [];
    let compared = 0;
    for (const key of objects) {
      const size = sizes.get(key) ?? 0;
      const ranges = [undefined, ...(size > 0 ? [`bytes=0-${Math.min(4095, size - 1)}`, `bytes=${Math.max(0, size - 4096)}-${size - 1}`] : []), ...(size > (16 << 20) ? [`bytes=4096-${(1 << 20) + 4095}`, `bytes=${Math.floor(size / 2)}-${Math.floor(size / 2) + (8 << 20) - 1}`] : [])];
      for (const range of ranges) {
        const [source, copy] = [await read(key, range), await read(`${copiedPrefix}${key.slice(layersPrefix.length)}`, range)];
        compared += 1;
        if (source !== copy) differing.push({ key, size, range: range ?? "whole", source, copy });
      }
    }
    seen["reads"] = { compared, differing };

    // The copy restored straight from the bucket, as gate 4 restored its source; from a new pod, as gate 7 left the session.
    const directory = at("bucket-D-copied");
    await node(`rm -rf ${q(directory)} && mkdir -p ${q(directory)} && umask 077 && printf '%s' ${q(goferOptions(`${copiedPrefix}${own}/`, copiedPrefix))} >${q(`${directory}/s3_opts.json`)}`);
    seen["copiedRestore"] = await restoreFrom(directory).catch((error: unknown) => String(error));

    // gVisor .6, as the box kept it, restoring an image it took and put in the bucket, of a pod of its own.
    const six = pins.gvisor.releases.find(({ release: each }) => each !== release)?.release;
    seen["six"] = six ?? "none kept";
    if (!six) return;
    const podOf = (name: string, annotations: Record<string, string> = {}) => ({
      apiVersion: "v1",
      kind: "Pod",
      metadata: { name, namespace: "default", annotations },
      spec: {
        runtimeClassName: gvisorRuntimeClass(six),
        restartPolicy: "Never",
        containers: [{ name: "main", image: "mirror.gcr.io/library/busybox:1.37.0", command: ["sh", "-c", "echo six >/tmp/marker; sleep 86400"] }],
      },
    });
    const phase = async (name: string) => (await kube.get<V1Pod>(ref("Pod", name, "default")))?.status?.phase;
    const until = async (name: string, ms: number) => {
      const deadline = Date.now() + ms;
      while (Date.now() < deadline && (await phase(name)) !== "Running") await sleep(1000);
      return (await phase(name)) ?? "gone";
    };
    try {
      await kube.apply(podOf("diag-six"));
      seen["sixRan"] = await until("diag-six", 180_000);
      const pod = (await node(`k3s crictl pods --namespace default --name '^diag-six$' --state ready --quiet`)).trim();
      const container = (await node(`k3s crictl ps --pod ${pod} --name '^main$' --quiet`)).trim();
      const flags = runscFlags(await node(`cat ${q(`${CONTAINERD}/${gvisorHandler(six)}.toml`)}`));
      const image = at("six");
      await node(`rm -rf ${q(image)}`);
      seen["sixCheckpoint"] = await node([`${gvisorDirectory(six)}/runsc`, `--root=${RUNSC_ROOT}`, ...flags, "checkpoint", `--image-path=${image}`, container].map(q).join(" ")).catch((error: unknown) => String(error));
      await upload(image, "gate-12/diag/six/");
      const restoring = at("bucket-six");
      await node(`rm -rf ${q(restoring)} && mkdir -p ${q(restoring)} && umask 077 && printf '%s' ${q(goferOptions("gate-12/diag/six/", "gate-12/diag/six-layers/"))} >${q(`${restoring}/s3_opts.json`)}`);
      await node("k3s kubectl delete pod diag-six --namespace default --grace-period=0 --force --wait=false").catch(() => "");
      await kube.apply(podOf("diag-six-restored", { [RESTORE_ANNOTATION]: restoring }));
      seen["sixRestored"] = await until("diag-six-restored", 180_000);
      const listed = await kube.list<CoreV1Event>("Event", { namespace: "default", fieldSelector: "involvedObject.name=diag-six-restored" });
      seen["sixEvents"] = listed.map(({ type, reason, message }) => `${type} ${reason}: ${message?.trim().slice(0, 600)}`).slice(-10);
      if (seen["sixRestored"] === "Running") seen["sixMarker"] = (await node(`k3s kubectl exec diag-six-restored --namespace default -- cat /tmp/marker`)).trim();
    } catch (error) {
      seen["sixError"] = error instanceof Error ? error.message : String(error);
    }
  }));
