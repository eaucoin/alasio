/**
 * The object store's S3 gateway under load as the box puts it under, in Docker, as alasio
 * runs it (cli/src/manifests/object-store.ts): SeaweedFS's image, master, volume server and
 * filer beside it, the gateway held to its memory limit and run with the arguments,
 * environment and sysctls alasio gives it. The load, as the box's clients of the store put
 * and read at once (CLIENTS): snapshots' multipart uploads, their 32 MiB parts four at a time
 * (the node agent's); restores' ranged reads, each of an image of its own, eight of 16 MiB at
 * a time (the checkpoint gofer's), with the node agent's 32 MiB gets beside; workspace
 * storage's 4 MiB blocks put and read (JuiceFS's); Neon's layers put and read whole (the
 * pageserver's) and its WAL's segments put (the safekeepers'); the lake's ranged reads of its
 * Parquet (DuckDB's); and a backup's multipart upload, its 8 MiB parts ten at a time (the aws
 * CLI's). 86 GETs at once with eight restores, as the box's may be, and 142 with fifteen,
 * whose chunks are past the 256 SeaweedFS's gateway reads at once.
 *
 * Its requests reach the gateway at its address on the containers' network, as pods reach the
 * gateway's, never through a port Docker publishes: its proxy takes them over loopback, whose
 * 64 KiB segments, filling a client's receive buffer as it lags, have the kernel prune it and
 * shrink its window under one segment, so the proxy sends only as it probes the window, and a
 * GET trickles at 0.01 MiB/s for twenty minutes, its gateway's bytes all ready.
 *
 * Several GiB go through it, so it runs only where ALASIO_GATEWAY_LOAD is set (CI's
 * gateway-memory job), never on a machine that runs anything else.
 */
import assert from "node:assert/strict";
import { execFile, execFileSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import { existsSync, rmSync, writeFileSync } from "node:fs";
import { Agent, request as httpRequest } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { promisify } from "node:util";

import type { KubernetesObject, V1Deployment } from "@kubernetes/client-node";
import { Result } from "effect";

import { decodeInstallConfig, manifests, NOTHING_CARRIED } from "../cli/src/manifests/index.ts";

const RUN = process.env["ALASIO_GATEWAY_LOAD"] !== undefined;
const MiB = 2 ** 20;
/**
 * The image each restore reads, its own, as each session's is: 64 of the filers' 4 MiB
 * chunks, so that each GET of it reads chunks no other does, as SeaweedFS's gateway shares
 * its chunks in flight between GETs of the same.
 */
const IMAGE_BYTES = 256 * MiB;
/** What each snapshot that is put holds. */
const SNAPSHOT_BYTES = 512 * MiB;
const PART_BYTES = 32 * MiB;
const UPLOADS = 8;

/**
 * What the box's other clients of the store put and read beside the restores, as each does:
 * workspace storage's JuiceFS, its 4 MiB blocks; Neon's pageserver, its layers, put and read
 * whole, and its safekeepers, their WAL's 16 MiB segments; the lake's DuckDB, ranges of its
 * Parquet; and the nightly backups' aws CLI, a multipart upload of 8 MiB parts, ten at a time.
 */
const CLIENTS = {
  blocks: { bytes: 4 * MiB, puts: 128, putting: 4, gets: 128, getting: 8 },
  layers: { bytes: 64 * MiB, puts: 4, putting: 2, gets: 4, getting: 2 },
  segments: { bytes: 16 * MiB, puts: 16, putting: 2 },
  parquet: { bytes: 256 * MiB, range: 2 * MiB, getting: 8 },
  backup: { bytes: 256 * MiB, part: 8 * MiB, putting: 10 },
} as const;

const docker = (...args: string[]) => execFileSync("docker", args, { encoding: "utf8", maxBuffer: 16 * MiB }).trim();
/** What `docker` with `args` wrote, its run not holding up the load's requests as it waits. */
const dockerAsync = (...args: string[]) => promisify(execFile)("docker", args, { encoding: "buffer", maxBuffer: 64 * MiB }).then(({ stdout }) => stdout);

/** A gateway as withStore runs it: `weed s3`'s image, arguments, environment and memory limit, and its pod's sysctls. */
interface Gateway {
  readonly image: string;
  readonly args: readonly string[];
  readonly env: readonly { readonly name: string; readonly value?: string }[];
  readonly limit: string;
  readonly sysctls: readonly { readonly name: string; readonly value: string }[];
}

/** The port `weed s3` run with `args` serves S3 on. */
const s3Port = (args: readonly string[]) => args.find((arg) => arg.startsWith("-port="))?.slice("-port=".length) ?? "8333";

/** The gateway as alasio's default install makes it. */
function gatewayContainer(): Gateway {
  const decoded = decodeInstallConfig({ alasio: { telegram: { existingSecret: "telegram" } } });
  if (Result.isFailure(decoded)) throw new Error(decoded.failure.message);
  const deployment = manifests(decoded.success, NOTHING_CARRIED).find((object: KubernetesObject) => object.kind === "Deployment" && object.metadata?.name === "alasio-seaweedfs") as V1Deployment;
  const s3 = deployment.spec?.template.spec?.containers.find(({ name }) => name === "s3");
  assert.ok(s3?.image && s3.args);
  return { image: s3.image, args: s3.args, env: s3.env ?? [], limit: s3.resources?.limits?.["memory"] ?? "", sysctls: deployment.spec?.template.spec?.securityContext?.sysctls ?? [] };
}

/** Docker's `--memory` of a Kubernetes quantity of Gi or Mi. */
const memoryOf = (limit: string) => {
  assert.match(limit, /^\d+[GM]i$/u);
  return limit.replace(/i$/u, "");
};

/**
 * A store of `gateway`'s image's parts in Docker, its gateway run as `gateway` says, given to
 * `use` with its address and its container; removed after.
 */
async function withStore<A>({ image, args, env, limit, sysctls }: Gateway, use: (endpoint: string, gateway: string) => Promise<A>): Promise<A> {
  const id = randomBytes(4).toString("hex");
  const network = `gateway-load-${id}`;
  const name = (part: string) => `${network}-${part}`;
  docker("network", "create", network);
  try {
    const run = (part: string, ...rest: string[]) => docker("run", "-d", "--name", name(part), "--network", network, "--network-alias", part, ...rest);
    run("master", image, "master", "-ip=master", "-port=9333", "-mdir=/tmp", "-volumeSizeLimitMB=1024");
    run("volume", image, "volume", "-ip=volume", "-port=8080", "-mserver=master:9333", "-dir=/tmp", "-max=40");
    run("filer", image, "filer", "-ip=filer", "-master=master:9333", "-defaultStoreDir=/tmp");
    // Its own arguments but where it reaches its filer, and no identities: anyone may ask.
    const own = args.filter((arg) => arg !== "s3" && !arg.startsWith("-filer=") && !arg.startsWith("-config="));
    run("s3", "--memory", memoryOf(limit), "--memory-swap", memoryOf(limit),
      ...sysctls.flatMap(({ name: key, value }) => ["--sysctl", `${key}=${value}`]), ...env.flatMap(({ name: key, value }) => ["-e", `${key}=${value ?? ""}`]),
      image, "s3", "-filer=filer:8888", "-debug", "-debug.port=6060", ...own);
    const endpoint = `http://${docker("inspect", "--format", `{{(index .NetworkSettings.Networks "${network}").IPAddress}}`, name("s3"))}:${s3Port(args)}`;
    for (let tries = 0; ; tries += 1) {
      const answered = await ask(`${endpoint}/load`, { method: "PUT" }).then(({ status }) => status, () => 0);
      if (answered === 200) break;
      assert.ok(tries < 120, "the gateway never made its bucket");
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
    return await use(endpoint, name("s3"));
  } finally {
    rmSync(profileOf(name("s3")), { force: true });
    for (const part of ["s3", "filer", "volume", "master"]) {
      try {
        docker("rm", "-f", name(part));
      } catch {}
    }
    docker("network", "rm", network);
  }
}

/** A body of `bytes`, of one buffer each size. */
const bodies = new Map<number, Buffer>();
const bodyOf = (bytes: number) => bodies.get(bytes) ?? bodies.set(bytes, Buffer.alloc(bytes, 7)).get(bytes) ?? Buffer.alloc(0);

/** Connections kept, as the store's clients keep theirs. */
const agent = new Agent({ keepAlive: true, maxSockets: 256 });

/**
 * How long a request may go without a byte moving before the client gives it up: the
 * shortest of the store's real clients', DuckDB's httpfs for the lake (http_timeout, 30 s,
 * which it retries thrice). The node agent's is 60 s (src/node-agent/s3.ts), the backups'
 * aws CLI's and JuiceFS's on the workspace mounts 60 s, retried, and Neon's remote
 * storage's 120 s, retried. A request the gateway holds longer fails.
 */
const CLIENT_IDLE_MS = 30_000;

/**
 * The least share of the median GET's rate any GET's may come at once it has begun: a GET
 * starved trickles, at a hundredth of it or less, its bytes never stopping long enough for its
 * client to give it up; the slowest of the rest came at a ninth at least, as 0.87 MiB/s
 * beside 6.3 did, and SeaweedFS's own defaults' 0.89 beside 8.1, on runners slow throughout.
 */
const LEAST_SHARE_OF_MEDIAN = 1 / 30;

/** How the load's requests went: the longest gap with nothing moving, the GETs' rates once begun, and the slowest. */
interface Timings {
  /** The longest a request went with nothing moving, from when it was sent whole to its last byte, and which. */
  gapMs: number;
  gap: string;
  /** Each GET's rate once its first byte came, in MiB/s, and the least, and which. */
  readonly rates: number[];
  leastRate: number;
  least: string;
  /** How long the longest request took, start to last byte, and which; and the longest PUT, as one waits for the gateway's room for uploads. */
  slowestMs: number;
  slowest: string;
  slowestPutMs: number;
  slowestPut: string;
}

const noTimings = (): Timings => ({ gapMs: 0, gap: "", rates: [], leastRate: Number.POSITIVE_INFINITY, least: "", slowestMs: 0, slowest: "", slowestPutMs: 0, slowestPut: "" });
let timings = noTimings();

/**
 * `method` of `url`, with `body` and `headers`: its status, its ETag, its body's text if
 * `text`, else only how long it was, and how it went in `timings`. One not answered 2xx
 * fails, as does one nothing moved of for CLIENT_IDLE_MS.
 */
function ask(url: string, { method = "GET", headers = {}, body, text = false }: { method?: string; headers?: Record<string, string>; body?: Buffer | string; text?: boolean } = {}): Promise<{ readonly status: number; readonly etag: string; readonly text: string; readonly bytes: number }> {
  const started = Date.now();
  const named = `${method} ${url}${headers["range"] ? ` ${headers["range"]}` : ""}`;
  let firstByteAt = 0;
  // When something last moved: the request sent whole, then each piece of its answer.
  let movedAt = started;
  let gapMs = 0;
  const moved = () => {
    const now = Date.now();
    gapMs = Math.max(gapMs, now - movedAt);
    movedAt = now;
  };
  return new Promise<{ readonly status: number; readonly etag: string; readonly text: string; readonly bytes: number }>((resolve, reject) => {
    const asked = httpRequest(url, { method, headers: { ...headers, ...(body === undefined ? {} : { "content-length": String(Buffer.byteLength(body)) }) }, agent }, (answer) => {
      const kept: Buffer[] = [];
      let bytes = 0;
      answer.on("data", (chunk: Buffer) => {
        moved();
        if (firstByteAt === 0) firstByteAt = movedAt;
        bytes += chunk.length;
        if (text || (answer.statusCode ?? 0) >= 300) kept.push(chunk);
      });
      answer.on("end", () => {
        const status = answer.statusCode ?? 0;
        const said = Buffer.concat(kept).toString("utf8");
        if (status < 200 || status >= 300) return reject(new Error(`${method} ${url}: ${status} ${said.slice(0, 200)}`));
        if (method === "GET" && bytes >= MiB) {
          const rate = bytes / MiB / (Math.max(1, Date.now() - firstByteAt) / 1000);
          timings.rates.push(rate);
          if (rate < timings.leastRate) {
            timings.leastRate = rate;
            timings.least = `${named}: ${bytes} bytes in ${Date.now() - firstByteAt} ms from its first, after ${firstByteAt - started} ms`;
          }
        }
        resolve({ status, etag: String(answer.headers.etag ?? ""), text: said, bytes });
      });
      answer.on("error", (error) => reject(new Error(`${method} ${url}: ${error.message} after ${Date.now() - started} ms, ${bytes} bytes in`)));
    });
    asked.on("finish", () => {
      movedAt = Date.now();
    });
    asked.setTimeout(CLIENT_IDLE_MS, () => asked.destroy(new Error(`${method} ${url}: nothing moved for ${CLIENT_IDLE_MS / 1000} s`)));
    asked.on("error", (error) => reject(error.message.startsWith(method) ? error : new Error(`${method} ${url}: ${error.message} after ${Date.now() - started} ms`)));
    asked.end(body);
  }).finally(() => {
    moved();
    if (gapMs > timings.gapMs) {
      timings.gapMs = gapMs;
      timings.gap = named;
    }
    if (Date.now() - started > timings.slowestMs) {
      timings.slowestMs = Date.now() - started;
      timings.slowest = `${named}, its first byte after ${firstByteAt === 0 ? "none" : `${firstByteAt - started} ms`}`;
    }
    if (method === "PUT" && Date.now() - started > timings.slowestPutMs) {
      timings.slowestPutMs = Date.now() - started;
      timings.slowestPut = named;
    }
  });
}

/** A multipart upload of `bytes` to `key`, its parts of `partBytes`, `parallel` at a time: four of 32 MiB, as the node agent puts a snapshot, unless said. */
async function upload(endpoint: string, key: string, bytes: number, partBytes = PART_BYTES, parallel = 4): Promise<void> {
  const id = /<UploadId>([^<]+)<\/UploadId>/u.exec((await ask(`${endpoint}/load/${key}?uploads`, { method: "POST", text: true })).text)?.[1];
  assert.ok(id);
  const count = bytes / partBytes;
  const etags: string[] = [];
  let next = 0;
  await Promise.all(Array.from({ length: parallel }, async () => {
    while (next < count) {
      const number = ++next;
      etags[number] = (await ask(`${endpoint}/load/${key}?partNumber=${number}&uploadId=${id}`, { method: "PUT", body: bodyOf(partBytes) })).etag;
    }
  }));
  const parts = etags.map((etag, number) => (etag ? `<Part><PartNumber>${number}</PartNumber><ETag>${etag}</ETag></Part>` : "")).join("");
  await ask(`${endpoint}/load/${key}?uploadId=${id}`, { method: "POST", body: `<CompleteMultipartUpload>${parts}</CompleteMultipartUpload>` });
}

/** `key`, of `total` bytes, read whole in ranges of `bytes`, `parallel` at a time: whether it was. */
async function read(endpoint: string, key: string, total: number, parallel: number, bytes: number): Promise<boolean> {
  let next = 0;
  let got = 0;
  await Promise.all(Array.from({ length: parallel }, async () => {
    while (next * bytes < total) {
      const at = next++ * bytes;
      const asked = Math.min(total, at + bytes) - at;
      const answered = (await ask(`${endpoint}/load/${key}`, { headers: { range: `bytes=${at}-${at + asked - 1}` } })).bytes;
      if (answered !== asked) throw new Error(`GET ${key} bytes=${at}-${at + asked - 1}: ${answered} bytes of ${asked}`);
      got += answered;
    }
  }));
  return got === total;
}

/** `count` objects, `key-<n>`, each of `bytes` put in one PUT, `parallel` at a time. */
async function puts(endpoint: string, key: string, count: number, bytes: number, parallel: number): Promise<boolean> {
  let next = 0;
  await Promise.all(Array.from({ length: parallel }, async () => {
    while (next < count) await ask(`${endpoint}/load/${key}-${next++}`, { method: "PUT", body: bodyOf(bytes) });
  }));
  return true;
}

/** `count` objects, `key-<n>`, each of `bytes` read whole in one GET, `parallel` at a time: whether each was. */
async function gets(endpoint: string, key: string, count: number, bytes: number, parallel: number): Promise<boolean> {
  let next = 0;
  let whole = true;
  await Promise.all(Array.from({ length: parallel }, async () => {
    while (next < count) {
      const named = `${key}-${next++}`;
      const answered = (await ask(`${endpoint}/load/${named}`)).bytes;
      if (answered !== bytes) throw new Error(`GET ${named}: ${answered} bytes of ${bytes}`);
      whole &&= answered === bytes;
    }
  }));
  return whole;
}

/** Where the heap profile the gateway gave at its largest heap is kept. */
const profileOf = (gateway: string) => join(tmpdir(), `${gateway}.heap.pb.gz`);

/**
 * The largest of each of `fields` the gateway's cgroup's memory.stat said, in MiB, sampled
 * every second until `until` settles; its Go heap's profile kept (profileOf) each time its
 * anonymous memory is the largest yet, as its pprof (-debug) gives it.
 */
async function sampled(gateway: string, until: Promise<unknown>, fields: readonly string[]): Promise<Record<string, number>> {
  const most: Record<string, number> = Object.fromEntries(fields.map((field) => [field, 0]));
  let profiledAt = 0;
  let done = false;
  void until.finally(() => {
    done = true;
  });
  while (!done) {
    try {
      const stat = (await dockerAsync("exec", gateway, "cat", "/sys/fs/cgroup/memory.stat")).toString("utf8");
      for (const field of fields) {
        const bytes = Number(new RegExp(`^${field} (\\d+)$`, "mu").exec(stat)?.[1] ?? 0);
        most[field] = Math.max(most[field] ?? 0, Math.round(bytes / MiB));
      }
      if ((most["anon"] ?? 0) > profiledAt + 32) {
        profiledAt = most["anon"] ?? 0;
        try {
          writeFileSync(profileOf(gateway), await dockerAsync("exec", gateway, "wget", "-q", "-O", "-", "http://127.0.0.1:6060/debug/pprof/heap"));
        } catch {}
      }
    } catch {
      break;
    }
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
  return most;
}

/** How a container came out of the load: whether it was killed for its memory, and the most its cgroup held, in MiB, where it still runs. */
function outcome(container: string): { readonly oomKilled: boolean; readonly peakMiB: number | null } {
  const state = JSON.parse(docker("inspect", "--format", "{{json .State}}", container)) as { OOMKilled: boolean; Running: boolean };
  return { oomKilled: state.OOMKilled, peakMiB: state.Running ? Math.round(Number(docker("exec", container, "cat", "/sys/fs/cgroup/memory.peak")) / MiB) : null };
}

/** What the load came to: whether every request was answered, and how `weed s3` came out of it. */
interface Loaded {
  readonly answered: boolean;
  readonly failed: readonly string[];
  readonly oomKilled: boolean;
  readonly peakMiB: number | null;
  /** Its requests' timings: the longest gap, the GETs' least, a hundredth's and median rates, the slowest, and the slowest PUT. */
  readonly timings: {
    readonly gapMs: number;
    readonly gap: string;
    readonly gets: number;
    readonly leastMiBps: number;
    readonly least: string;
    readonly p1MiBps: number;
    readonly medianMiBps: number;
    readonly floorMiBps: number;
    readonly slowestMs: number;
    readonly slowest: string;
    readonly slowestPutMs: number;
    readonly slowestPut: string;
  };
  readonly mostMiB: Record<string, number>;
}

/** The load, with `restores` restores, on a store whose `weed s3` is `gateway`. */
async function load(restores: number, endpoint: string, gateway: string): Promise<Loaded> {
  // What is read, put first: each restore's image, and the node agent's, the blocks, the layers and the Parquet.
  for (let index = 0; index <= restores; index += 1) await upload(endpoint, `image-${index}`, IMAGE_BYTES);
  const { blocks, layers, segments, parquet, backup } = CLIENTS;
  await puts(endpoint, "block", blocks.gets, blocks.bytes, 8);
  await puts(endpoint, "layer", layers.gets, layers.bytes, 2);
  await upload(endpoint, "parquet", parquet.bytes);
  timings = noTimings();
  const failed: string[] = [];
  const loading = Promise.all([
    ...Array.from({ length: UPLOADS }, (_, index) => upload(endpoint, `snapshot-${index}`, SNAPSHOT_BYTES).then(() => true)),
    ...Array.from({ length: restores }, (_, index) => read(endpoint, `image-${index}`, IMAGE_BYTES, 8, 16 * MiB)),
    read(endpoint, `image-${restores}`, IMAGE_BYTES, 4, PART_BYTES),
    puts(endpoint, "new-block", blocks.puts, blocks.bytes, blocks.putting),
    gets(endpoint, "block", blocks.gets, blocks.bytes, blocks.getting),
    puts(endpoint, "new-layer", layers.puts, layers.bytes, layers.putting),
    gets(endpoint, "layer", layers.gets, layers.bytes, layers.getting),
    puts(endpoint, "segment", segments.puts, segments.bytes, segments.putting),
    read(endpoint, "parquet", parquet.bytes, parquet.getting, parquet.range),
    upload(endpoint, "backup", backup.bytes, backup.part, backup.putting).then(() => true),
  ].map((each) => each.catch((error: unknown) => {
    if (failed.length < 5) failed.push(error instanceof Error ? `${error.message}${error.cause instanceof Error ? `: ${error.cause.message}` : ""}` : String(error));
    return false;
  }))).then((all) => all.every(Boolean));
  // What its memory goes to, at most: its Go heap and stacks (anon), the page cache (file),
  // and its connections' buffers (sock), all of them charged to its cgroup.
  const [answered, mostMiB] = await Promise.all([loading, sampled(gateway, loading, ["anon", "file", "sock", "kernel"])]);
  // What its Go heap held at its largest, by what allocated it, where Go is at hand to say.
  let heap = "no heap profile";
  if (existsSync(profileOf(gateway))) {
    try {
      heap = execFileSync("go", ["tool", "pprof", "-top", "-nodecount=20", "-sample_index=inuse_space", profileOf(gateway)], { encoding: "utf8" });
    } catch (error) {
      heap = `go tool pprof failed: ${error instanceof Error ? error.message : String(error)}`;
    }
  }
  console.log(`# ${gateway}'s heap at its largest:\n${heap}`);
  const rates = timings.rates.toSorted((one, other) => one - other);
  const rate = (share: number) => rates[Math.floor(share * (rates.length - 1))] ?? 0;
  const at = (share: number) => Math.round(rate(share) * 10) / 10;
  const { gapMs, gap, leastRate, least, slowestMs, slowest, slowestPutMs, slowestPut } = timings;
  const timed = { gapMs, gap, gets: rates.length, leastMiBps: Math.round(leastRate * 100) / 100, least, p1MiBps: at(0.01), medianMiBps: at(0.5), floorMiBps: Math.round(rate(0.5) * LEAST_SHARE_OF_MEDIAN * 100) / 100, slowestMs, slowest, slowestPutMs, slowestPut };
  return { answered, failed, ...outcome(gateway), timings: timed, mostMiB };
}

/** How many GETs the load has under way at once with `restores` restores: eight a restore, the node agent's four, and the other clients'. */
const streams = (restores: number) => restores * 8 + 4 + CLIENTS.blocks.getting + CLIENTS.layers.getting + CLIENTS.parquet.getting;

/**
 * That `loaded`, the load on the gateway as alasio runs it, held: every request answered,
 * none going as long as the store's most impatient client waits with nothing moving, no GET
 * starved, each coming at LEAST_SHARE_OF_MEDIAN of its peers' rate at least once begun,
 * however long it took whole, and `weed s3` never killed, within its limit.
 */
function assertHeld(loaded: Loaded, gateway: Gateway): void {
  const mebibytes = (limit: string) => Number(/^(\d+)([GM])i$/u.exec(limit)?.[1]) * (limit.endsWith("Gi") ? 1024 : 1);
  assert.deepEqual([loaded.answered, loaded.oomKilled, loaded.failed], [true, false, []]);
  assert.ok(loaded.timings.gapMs < CLIENT_IDLE_MS, `${loaded.timings.gap} went ${loaded.timings.gapMs} ms with nothing moving`);
  assert.ok(loaded.timings.gets > 0 && loaded.timings.leastMiBps >= loaded.timings.floorMiBps, `${loaded.timings.least}: ${loaded.timings.leastMiBps} MiB/s, under ${loaded.timings.floorMiBps}`);
  assert.ok(loaded.peakMiB !== null && loaded.peakMiB < mebibytes(gateway.limit), `weed s3's peak: ${loaded.peakMiB} MiB of ${gateway.limit}`);
}

/** What `loaded` came to, and how far under its limits. */
const said = (loaded: Loaded, gateway: Gateway) => `${JSON.stringify(loaded)}; weed s3 at most ${loaded.peakMiB} MiB of ${gateway.limit}`;

test(`an S3 gateway as alasio runs it serves what the box's clients put and read at once, ${streams(8)} GETs at once as the box's may be, past SeaweedFS's 256 chunks in flight, within its memory, no GET starved and no request still for 30 s`, { skip: !RUN && "ALASIO_GATEWAY_LOAD is not set: the load is several GiB" }, async (t) => {
  const gateway = gatewayContainer();
  assert.match(gateway.image, /^chrislusf\/seaweedfs:/u);
  // As SeaweedFS leaves it, its uploads unbounded, its connections' buffers the node's and its
  // runtime unaware of the limit, for comparison: killed at 1 GiB, as the box's was, and at
  // 2 GiB too under this load (CI); said, not asserted.
  const unbounded: Gateway = { ...gateway, args: gateway.args.filter((arg) => !/^-concurrentUploadLimitMB=/u.test(arg)), env: [], sysctls: [] };
  t.diagnostic(`as SeaweedFS leaves it: ${said(await withStore(unbounded, (...on) => load(8, ...on)), unbounded)}`);
  const loaded = await withStore(gateway, (...on) => load(8, ...on));
  t.diagnostic(`as alasio runs it: ${said(loaded, gateway)}`);
  assertHeld(loaded, gateway);
});

test(`an S3 gateway as alasio runs it serves ${streams(15)} GETs at once, of nearly twice SeaweedFS's 256 chunks in flight, within its memory, no GET starved and no request still for 30 s`, { skip: !RUN && "ALASIO_GATEWAY_LOAD is not set: the load is several GiB" }, async (t) => {
  const gateway = gatewayContainer();
  const loaded = await withStore(gateway, (...on) => load(15, ...on));
  t.diagnostic(`as alasio runs it: ${said(loaded, gateway)}`);
  assertHeld(loaded, gateway);
});
