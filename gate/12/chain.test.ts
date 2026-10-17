/**
 * 12's gates on k3s (program/12-live-lazy.md, gates 2 to 8 and 10): alasio up on k3s on the runner,
 * the host target (test/e2e/harness.ts), its sessions' runtime with 12's options, as alasio
 * installs them (dirty tracking by write-protection, working sets recorded, restores in the
 * background), and a
 * real session pod, checkpointed by runsc beside gVisor's shim as the node agent does, with
 * its flags, and restored into a new pod of its Sandbox through the fork's restore
 * annotation:
 *
 *   2. live checkpoints, during an exec, leave the pod, its container and the exec running;
 *   3. a chain of three (two live, one at release) restores through the adapter, its layers
 *      linked in the prepared directory's layers/<digest>/;
 *   4. a chain restores straight from the snapshots bucket, its gofer reaching the store from
 *      the node, from a directory holding s3_opts.json alone, each image of the chain in the
 *      bucket once, under layers/<digest>/, as the node agent keeps them: straight from the
 *      store, then, a checkpoint later, through the node agent's proxy of it, by a restore's
 *      own token, each timed to its pod ready and to the last read of its background;
 *   5. a delta after a restore from the bucket: whether its parent's pages_meta.img alone
 *      will do, or its pages too;
 *   6. a server-side copy of a 1 GiB layer in the bundled SeaweedFS: its time and bytes;
 *   7. a live snapshot restored after its pod went, during its exec, which goes on; and one
 *      whose held file changed since, which the restore refuses;
 *   8. a restore from the bucket whose proxy goes away during its background reads, for a
 *      while: briefly, as long as a node agent takes to come back, and for five minutes; its
 *      reads wait for the proxy, its memory comes back intact, and it saves again;
 *  10. a restore from the bucket whose proxy goes away for good during its background reads:
 *      its pod, deleted, goes within its grace, its sandbox and checkpoint gofer with it.
 *
 * What each records goes into $OUT/evidence.json.
 *
 *   ALASIO_E2E_TARGET=host ALASIO_E2E_REGISTRY=... OUT=... node --test gate/12/chain.test.ts
 */
import assert from "node:assert/strict";
import { mkdirSync, writeFileSync } from "node:fs";
import { type AddressInfo, connect, createServer, type Socket } from "node:net";
import { join } from "node:path";
import { after, before, test } from "node:test";

import type { CoreV1Event, V1PersistentVolume, V1Pod, V1Secret, V1Service } from "@kubernetes/client-node";
import { Effect } from "effect";

import pins from "../../cluster/node/pins.json" with { type: "json" };
import { NAMESPACE } from "../../cli/src/manifests/common.ts";
import { CONTAINERD, gvisorDirectory, gvisorHandler } from "../../src/node-agent/names.ts";
import { runscFlags } from "../../src/node-agent/runsc.ts";
import { makeS3Bucket, type S3Bucket } from "../../src/node-agent/s3.ts";
import { makeStoreProxy, type StoreProxy, type StoreToken } from "../../src/node-agent/store-proxy.ts";
import { hookOf } from "../../src/sandbox/snapshots.ts";
import { dumpClusterState, inAlasio, inSession, kube, nodeAgents, onNode, ref, SESSIONS, setUp, TARGET, tearDown } from "../../test/e2e/harness.ts";
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
    // 12's options for the sessions' runtime, as alasio installs them, which each new shim reads as it starts.
    await node(`mkdir -p ${IMAGES}`);
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

/**
 * The options of the S3 gofer for the image at `objectPrefix` in the bucket, its layers each
 * under `layersPrefix`, as JSON: at `endpoint` with `credentials`, the store's own unless said.
 */
/** A restore's s3_opts.json, its reads of pages waiting for a store that is away for as long as the sandbox runs, as the node agent writes it for a release that can. */
const goferOptions = (objectPrefix: string, layersPrefix: string, endpoint = s3.endpoint, credentials = { access_key_id: s3.accessKeyId, secret_access_key: s3.secretAccessKey }) =>
  JSON.stringify({ endpoint, region: s3.region, bucket: s3.bucket, object_prefix: objectPrefix, layers_prefix: layersPrefix, addressing: "path", credentials, pages_retry: "while-running" });

/** A relay to a store, as relayTo makes one. */
interface Relay {
  readonly endpoint: string;
  /** What it carried back, and when it last did. */
  readonly carried: () => { readonly bytes: number; readonly lastAt: number };
  /** Ends what it carries and listens no more, as a proxy that stops does: its port refuses. */
  readonly cut: () => Promise<void>;
  /** Listens again on its port, as a proxy that came back there would. */
  readonly back: () => Promise<void>;
  readonly close: () => void;
}

/**
 * A relay on this machine to `target` (`http://host:port`), as the gofer, on the node, reaches
 * it: what it carries back, and when it last did, so that a restore's reads, its working set
 * first and the rest in the background after, are timed to their end, whatever they go through;
 * at most `bytesPerSecond` of them, all together, where given, as a store farther away gives
 * them.
 */
async function relayTo(target: string, { bytesPerSecond }: { readonly bytesPerSecond?: number } = {}): Promise<Relay> {
  const url = new URL(target);
  let bytes = 0;
  let lastAt = 0;
  const startedAt = Date.now();
  const carrying = new Set<Socket>();
  const server = createServer((client) => {
    const upstream = connect({ host: url.hostname, port: Number(url.port) || 80 });
    for (const socket of [client, upstream]) {
      carrying.add(socket);
      socket.once("close", () => carrying.delete(socket));
    }
    client.pipe(upstream);
    upstream.on("data", (chunk: Buffer) => {
      bytes += chunk.length;
      lastAt = Date.now();
      // Ahead of its rate: held until it is not.
      const ahead = bytesPerSecond === undefined ? 0 : (bytes / bytesPerSecond) * 1000 - (Date.now() - startedAt);
      if (ahead > 0) {
        upstream.pause();
        setTimeout(() => upstream.resume(), ahead);
      }
    });
    upstream.pipe(client);
    upstream.on("error", () => client.destroy());
    client.on("error", () => upstream.destroy());
  });
  const listen = (port: number) => new Promise<void>((resolve) => server.listen(port, "127.0.0.1", resolve));
  await listen(0);
  const { port } = server.address() as AddressInfo;
  return {
    endpoint: `http://127.0.0.1:${port}`,
    carried: () => ({ bytes, lastAt }),
    cut: () => {
      const closed = new Promise<void>((resolve) => server.close(() => resolve()));
      for (const socket of carrying) socket.destroy();
      return closed;
    },
    back: () => listen(port),
    close: () => {
      for (const socket of carrying) socket.destroy();
      server.close();
    },
  };
}

/** How long a restore that began at `startedAt` read through `relay`, once its reads have stopped for `quietMs`: to its working set, and to its end. */
async function readsOf(relay: Relay, startedAt: number, quietMs = 5000): Promise<{ readonly bytes: number; readonly lastReadMs: number }> {
  for (;;) {
    const { lastAt } = relay.carried();
    if (lastAt > 0 && Date.now() - lastAt >= quietMs) break;
    await sleep(500);
  }
  const { bytes, lastAt } = relay.carried();
  return { bytes, lastReadMs: lastAt - startedAt };
}

/** The token `proxy` mints for the session's restore, of the images under `layersPrefix`: one it must, as it does not drain. */
function minted(proxy: StoreProxy, layersPrefix: string): StoreToken {
  const token = proxy.mint(`gate-12/${session.volumeId}`, layersPrefix);
  assert.ok(token, "the proxy minted no token");
  return token;
}

/** What `use` makes of the node agent's proxy of the bucket, in this process, on this machine, as long as it uses it. */
async function throughProxy<A>(use: (proxy: StoreProxy) => Promise<A>): Promise<A> {
  let release = () => {};
  const proxy = await new Promise<StoreProxy>((resolve, reject) => {
    Effect.runPromise(Effect.scoped(Effect.gen(function*() {
      resolve(yield* makeStoreProxy({ store: s3, host: "127.0.0.1", port: 0 }));
      yield* Effect.promise(() => new Promise<void>((resolved) => (release = resolved)));
    }))).catch(reject);
  });
  try {
    return await use(proxy);
  } finally {
    release();
  }
}

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
    // Each image once, by its digest, as the node agent keeps a workspace's.
    const layersPrefix = `gate-12/${session.volumeId}/layers/`;
    const byDigest = new Map(await Promise.all(["A", "B", "C"].map(async (name) => [await digest(at(name)), at(name)] as const)));
    const inBucket = new Set<string>();
    /** Checkpoints the image `name` at release, a delta of `parent`, and puts it in the bucket, with each of its layers not there yet: its own digest. */
    const checkpointed = async (name: string, parent: string) => {
      seen[name] = await checkpoint(name, { live: false, parent });
      const layers = (await runsc(["image", "layers", at(name)])).split("\n").filter(Boolean);
      const own = await digest(at(name));
      const uploadingAt = Date.now();
      for (const [each, image] of [[own, at(name)], ...layers.map((layer) => [layer, byDigest.get(layer) ?? ""] as const)] as const) {
        if (inBucket.has(each)) continue;
        await upload(image, `${layersPrefix}${each}/`);
        inBucket.add(each);
      }
      seen[`${name}Uploaded`] = { layers, ms: Date.now() - uploadingAt };
      byDigest.set(own, at(name));
      return own;
    };

    // runsc, and the shim through it, reads every file of the image from the bucket: straight
    // from the store, then through the node agent's proxy of it, as alasio's restores do,
    // a restore's own token signing it; each through a relay that times its reads to their end.
    // Each restores a checkpoint of its own, the last, as a session's restores do: the one
    // before ran bayma, whose files on the node the image held, and so would be refused.
    const restoredThrough = async (name: string, own: string, endpoint: string, credentials?: { access_key_id: string; secret_access_key: string }) => {
      const relay = await relayTo(endpoint);
      try {
        const directory = at(`bucket-${name}`);
        await node(`rm -rf ${q(directory)} && mkdir -p ${q(directory)} && umask 077 && printf '%s' ${q(goferOptions(`${layersPrefix}${own}/`, layersPrefix, relay.endpoint, credentials))} >${q(`${directory}/s3_opts.json`)}`);
        const startedAt = Date.now();
        const restored = await restoreFrom(directory);
        assert.ok(restored.ready, JSON.stringify(restored));
        const reads = await readsOf(relay, startedAt);
        const read = await inGate<Read>("read", session.volumeId, session.replId);
        assert.deepEqual([read.marker, read.step], ["42", "4"]);
        assert.match(read.notice ?? "", CAME_BACK_WHOLE);
        return { restore: restored, readyMs: restored.ms, ...reads, read };
      } finally {
        relay.close();
      }
    };
    seen["direct"] = await restoredThrough("D0", await checkpointed("D0", at("restore-C")), s3.endpoint);
    const own = await checkpointed("D", at("D0"));
    seen["proxied"] = await throughProxy(async (proxy) => {
      const token = minted(proxy, layersPrefix);
      return restoredThrough("D", own, proxy.endpoint, { access_key_id: token.accessKeyId, secret_access_key: token.secretAccessKey });
    });
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
    const restored = await restoreFrom(await prepared("E", ["A", "B", "C", "D0", "D"]));
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
    const directory = await prepared("L", ["A", "B", "C", "D0", "D", "E"]);
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
    const refused = await restoreFrom(await prepared("M", ["A", "B", "C", "D0", "D", "E", "L"]), { crashed: true });
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

/** The random bytes gate 8's session holds, which its restores read in the background, untouched. */
const BALLAST_MIB = 512;
/** How fast gate 8's relay carries a restore's reads: slow enough that its background lasts half a minute. */
const OUTAGE_RELAY_BYTES_PER_SECOND = 16 * 1024 ** 2;
/** How long gate 8's longest outage lasts: far past the gofer's attempts of a read, 5, each at most 20 s after the one before, which pages_retry outlasts. */
const LONG_OUTAGE_MS = 300_000;
/** How long gate 8 waits for an answer of a sandbox that may not give one. */
const ANSWER_WITHIN_MS = 180_000;

/** What `promise` came to within ANSWER_WITHIN_MS: its value, its error, or that it gave none. */
const within = <A>(promise: Promise<A>): Promise<A | string> =>
  Promise.race([promise.catch((error: unknown) => (error instanceof Error ? error.message : String(error))), sleep(ANSWER_WITHIN_MS).then(() => `no answer within ${ANSWER_WITHIN_MS / 1000} s`)]);

/** How long the node agent's pod takes to be replaced, as a rollout or a crash replaces it: from its deletion to the new one ready. */
async function agentReplaced(): Promise<{ readonly ms: number; readonly from: string; readonly to: string }> {
  const [[nodeName, from] = ["", ""]] = [...(await nodeAgents())];
  assert.ok(from, "no node agent runs");
  const deletedAt = Date.now();
  await kube.remove(ref("Pod", from, NAMESPACE));
  for (;;) {
    const to = (await nodeAgents()).get(nodeName);
    const pod = to === undefined || to === from ? null : await kube.get<V1Pod>(ref("Pod", to, NAMESPACE));
    if (to && pod?.status?.conditions?.some(({ type, status }) => type === "Ready" && status === "True")) return { ms: Date.now() - deletedAt, from, to };
    assert.ok(Date.now() - deletedAt < 600_000, `no node agent replaced ${from} within 10 minutes`);
    await sleep(250);
  }
}

test("8. a restore from the bucket whose proxy goes away during its background reads, for a while, comes back intact and saves again; and how long a node agent takes to come back", { timeout: 60 * 60_000 }, () =>
  recorded("outage", async (seen) => {
    seen["agentReplaced"] = await agentReplaced();
    const restartMs = (seen["agentReplaced"] as { readonly ms: number }).ms;
    // For a while, then back on the same port: briefly, as long as an agent takes to come
    // back, and for five minutes. Each of a session of its own, a whole image of whose
    // sandbox the restore reads every page of from the bucket, its reads of pages waiting
    // for the store as long as the sandbox runs (pages_retry).
    const outages = [["brief", 10_000], ["restart", restartMs], ["long", LONG_OUTAGE_MS]] as const;
    await throughProxy(async (proxy) => {
      for (const [name, outageMs] of outages) {
        const outcome: Record<string, unknown> = { outageMs };
        seen[name] = outcome;
        try {
          Object.assign(session, await inGate<Created>("create"));
          outcome["session"] = { ...session };
          outcome["ballast"] = await inGate("ballast", session.volumeId, session.replId, String(BALLAST_MIB));
          const layersPrefix = `gate-12/${session.volumeId}/layers/`;
          const image = `outage-${name}`;
          outcome["checkpoint"] = await checkpoint(image, { live: false });
          const own = await digest(at(image));
          await upload(at(image), `${layersPrefix}${own}/`);
          const pagesBytes = Number((await node(`stat -c %s ${q(`${at(image)}/pages.img`)}`)).trim());
          outcome["pagesBytes"] = pagesBytes;
          const token = minted(proxy, layersPrefix);
          const relay = await relayTo(proxy.endpoint, { bytesPerSecond: OUTAGE_RELAY_BYTES_PER_SECOND });
          try {
            const directory = at(`bucket-${image}`);
            await node(`rm -rf ${q(directory)} && mkdir -p ${q(directory)} && umask 077 && printf '%s' ${q(goferOptions(`${layersPrefix}${own}/`, layersPrefix, relay.endpoint, { access_key_id: token.accessKeyId, secret_access_key: token.secretAccessKey }))} >${q(`${directory}/s3_opts.json`)}`);
            const startedAt = Date.now();
            const restored = await restoreFrom(directory);
            outcome["restore"] = restored;
            assert.ok(restored.ready, JSON.stringify(restored));
            // Cut as the background reads go on, a quarter of the pages read at least.
            for (;;) {
              const { bytes, lastAt } = relay.carried();
              assert.ok(bytes < pagesBytes * 0.9, `the restore read ${bytes} of ${pagesBytes} bytes before the gate could cut it`);
              if (bytes >= pagesBytes / 4 && Date.now() - lastAt < 1000) break;
              assert.ok(Date.now() - startedAt < 300_000, `the restore read ${bytes} of ${pagesBytes} bytes in 5 minutes`);
              await sleep(100);
            }
            outcome["cutAt"] = { bytes: relay.carried().bytes, ms: Date.now() - startedAt };
            await relay.cut();
            await sleep(outageMs);
            await relay.back();
            outcome["reads"] = await readsOf(relay, startedAt, 30_000);
          } finally {
            relay.close();
          }
          outcome["pod"] = await podState();
          outcome["touched"] = await within(inGate("touch", session.volumeId, session.replId));
          outcome["read"] = await within(inGate<Read>("read", session.volumeId, session.replId));
          outcome["podAfter"] = await podState();
          // Whether the sandbox can still be saved.
          outcome["saved"] = await within(checkpoint(`${image}-after`, { live: true }));
        } catch (error) {
          outcome["error"] = error instanceof Error ? error.stack : String(error);
        }
      }
    });
    for (const [name] of outages) {
      const outcome = seen[name] as Record<string, unknown>;
      const said = `${name}: ${JSON.stringify(outcome)}`;
      // Each outage cut the restore mid-background, or the gate saw nothing; the restore then
      // read the rest of its pages, the sandbox's memory is what it was, and it saves again.
      assert.ok(outcome["cutAt"] && !outcome["error"], said);
      assert.ok((outcome["reads"] as { readonly bytes: number }).bytes >= (outcome["pagesBytes"] as number), said);
      assert.equal(outcome["touched"], "intact", said);
      const read = outcome["read"] as Read | string;
      assert.equal(typeof read === "string" ? read : read.marker, "42", said);
      assert.equal(typeof outcome["saved"], "object", said);
    }
  }));

/** How long past its grace a deleted session pod whose store went for good may take to go, its sandbox and checkpoint gofer with it. */
const TEARDOWN_MARGIN_MS = 15_000;
/** How long gate 10 leaves a restore's reads waiting for a store gone for good before its pod is deleted: past the gofer's attempts of a read. */
const WAITING_MS = 120_000;

/** Which of the processes `pids` run on the node, zombies aside, whose command lines are empty. */
const running = async (pids: readonly string[]): Promise<string[]> =>
  pids.length === 0 ? [] : (await node(pids.map((pid) => `[ -n "$(tr -d '\\0' </proc/${pid}/cmdline 2>/dev/null)" ] && echo ${pid}`).join("; ") + "; true")).split("\n").filter(Boolean);
/** The processes on the node whose command line `pattern` (pgrep -f's) matches. */
const pgrep = async (pattern: string): Promise<string[]> => (await node(`pgrep -f ${q(pattern)} || true`)).split("\n").filter(Boolean);

test("10. a restore from the bucket whose proxy goes away for good during its background reads: its pod, deleted, goes within its grace, its sandbox and checkpoint gofer with it", { timeout: 30 * 60_000 }, () =>
  recorded("goneForGood", async (seen) => {
    await throughProxy(async (proxy) => {
      Object.assign(session, await inGate<Created>("create"));
      seen["session"] = { ...session };
      seen["ballast"] = await inGate("ballast", session.volumeId, session.replId, String(BALLAST_MIB));
      const layersPrefix = `gate-12/${session.volumeId}/layers/`;
      const image = "gone-for-good";
      seen["checkpoint"] = await checkpoint(image, { live: false });
      const own = await digest(at(image));
      await upload(at(image), `${layersPrefix}${own}/`);
      const pagesBytes = Number((await node(`stat -c %s ${q(`${at(image)}/pages.img`)}`)).trim());
      seen["pagesBytes"] = pagesBytes;
      const token = minted(proxy, layersPrefix);
      const relay = await relayTo(proxy.endpoint, { bytesPerSecond: OUTAGE_RELAY_BYTES_PER_SECOND });
      try {
        const directory = at(`bucket-${image}`);
        await node(`rm -rf ${q(directory)} && mkdir -p ${q(directory)} && umask 077 && printf '%s' ${q(goferOptions(`${layersPrefix}${own}/`, layersPrefix, relay.endpoint, { access_key_id: token.accessKeyId, secret_access_key: token.secretAccessKey }))} >${q(`${directory}/s3_opts.json`)}`);
        const gofersBefore = new Set(await pgrep("^runsc-checkpointgofer "));
        const startedAt = Date.now();
        const restored = await restoreFrom(directory);
        seen["restore"] = restored;
        assert.ok(restored.ready, JSON.stringify(restored));
        const { pod } = await ids();
        const sandboxes = await pgrep(`^runsc-sandbox .*${pod}`);
        const gofers = (await pgrep("^runsc-checkpointgofer ")).filter((pid) => !gofersBefore.has(pid));
        seen["processes"] = { pod, sandboxes, gofers };
        assert.ok(sandboxes.length > 0 && gofers.length > 0, JSON.stringify(seen["processes"]));
        // Cut as the background reads go on, a quarter of the pages read at least, for good.
        for (;;) {
          const { bytes, lastAt } = relay.carried();
          assert.ok(bytes < pagesBytes * 0.9, `the restore read ${bytes} of ${pagesBytes} bytes before the gate could cut it`);
          if (bytes >= pagesBytes / 4 && Date.now() - lastAt < 1000) break;
          assert.ok(Date.now() - startedAt < 300_000, `the restore read ${bytes} of ${pagesBytes} bytes in 5 minutes`);
          await sleep(100);
        }
        seen["cutAt"] = { bytes: relay.carried().bytes, ms: Date.now() - startedAt };
        await relay.cut();
        await sleep(WAITING_MS);
        seen["podWaiting"] = await podState();
        // Its reads waiting for the store still, the pod is deleted, as alasio deletes a
        // session's pod it stops: its Sandbox suspended.
        const grace = (await kube.get<V1Pod>(ref("Pod", session.volumeId, SESSIONS)))?.spec?.terminationGracePeriodSeconds ?? 30;
        const deletedAt = Date.now();
        await kube.patch(ref("Sandbox", session.volumeId, SESSIONS), { spec: { operatingMode: "Suspended" } });
        while ((await podState()) !== "gone") {
          assert.ok(Date.now() - deletedAt < 600_000, `the pod of ${session.volumeId} is still there 10 minutes after its deletion`);
          await sleep(250);
        }
        const goneMs = Date.now() - deletedAt;
        let left = await running([...sandboxes, ...gofers]);
        while (left.length > 0 && Date.now() - deletedAt < goneMs + TEARDOWN_MARGIN_MS) {
          await sleep(250);
          left = await running(left);
        }
        seen["teardown"] = { graceSeconds: grace, goneMs, left, events: await events() };
        assert.ok(goneMs <= grace * 1000 + TEARDOWN_MARGIN_MS, JSON.stringify(seen["teardown"]));
        assert.deepEqual(left, [], JSON.stringify(seen["teardown"]));
      } finally {
        relay.close();
      }
    });
  }));
