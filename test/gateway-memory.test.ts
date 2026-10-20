/**
 * The object store's S3 gateway under load as the box puts it under, in Docker, as alasio
 * runs it (cli/src/manifests/object-store.ts): SeaweedFS's image, master, volume server and
 * filer beside it, the gateway held to its memory limit and run with the arguments,
 * environment and sysctls alasio gives it, behind its GET limiter (src/object-store/get-limiter.ts)
 * as its pod runs it, held to its own limit. The load: snapshots' multipart uploads, their
 * 32 MiB parts four at a time (the node agent's), and restores' ranged reads of a 1 GiB image,
 * eight of 16 MiB at a time each (the checkpoint gofer's), with the node agent's 32 MiB gets
 * beside: 68 GETs at once with eight restores, as the box's load is, and 124 with fifteen.
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
import { join, resolve } from "node:path";
import { test } from "node:test";
import { promisify } from "node:util";

import type { KubernetesObject, V1Deployment } from "@kubernetes/client-node";
import { Result } from "effect";

import { decodeInstallConfig, manifests, NOTHING_CARRIED } from "../cli/src/manifests/index.ts";

const RUN = process.env["ALASIO_GATEWAY_LOAD"] !== undefined;
const MiB = 2 ** 20;
/** The image a restore reads. */
const IMAGE_BYTES = 1024 * MiB;
/** What each snapshot that is put holds. */
const SNAPSHOT_BYTES = 512 * MiB;
const PART_BYTES = 32 * MiB;
const UPLOADS = 8;

const docker = (...args: string[]) => execFileSync("docker", args, { encoding: "utf8", maxBuffer: 16 * MiB }).trim();
/** What `docker` with `args` wrote, its run not holding up the load's requests as it waits. */
const dockerAsync = (...args: string[]) => promisify(execFile)("docker", args, { encoding: "buffer", maxBuffer: 64 * MiB }).then(({ stdout }) => stdout);

/** A gateway's container: its image, arguments, environment and memory limit. */
interface Container {
  readonly image: string;
  readonly args: readonly string[];
  readonly env: readonly { readonly name: string; readonly value?: string }[];
  readonly limit: string;
}

/** A gateway as withStore runs it: `weed s3`'s container and its pod's sysctls, and the GET limiter's container in front of it, if any. */
interface Gateway {
  readonly s3: Container;
  readonly sysctls: readonly { readonly name: string; readonly value: string }[];
  readonly limiter?: Container;
}

/** The value of `name` in `env`. */
const valueOf = (env: Container["env"], name: string) => env.find((each) => each.name === name)?.value ?? "";

/** The port `weed s3` run with `args` serves S3 on. */
const s3Port = (args: readonly string[]) => args.find((arg) => arg.startsWith("-port="))?.slice("-port=".length) ?? "8333";

/** The repository's root, whose src/ the alasio image holds at /opt/alasio. */
const ROOT = resolve(import.meta.dirname, "..");

/** The gateway as alasio's default install makes it: `weed s3`, and the GET limiter, of the alasio image, run here from this tree's src/ on this node. */
function gatewayContainer(): Gateway {
  const decoded = decodeInstallConfig({ alasio: { telegram: { existingSecret: "telegram" } } });
  if (Result.isFailure(decoded)) throw new Error(decoded.failure.message);
  const deployment = manifests(decoded.success, NOTHING_CARRIED).find((object: KubernetesObject) => object.kind === "Deployment" && object.metadata?.name === "alasio-seaweedfs") as V1Deployment;
  const containers = deployment.spec?.template.spec?.containers ?? [];
  const s3 = containers.find(({ name }) => name === "s3");
  const limiter = containers.find(({ name }) => name === "get-limiter");
  assert.ok(s3?.image && s3.args && limiter?.command);
  assert.deepEqual(limiter.command, ["node", "/opt/alasio/src/object-store/get-limiter-main.ts"]);
  const port = s3Port(s3.args);
  // It reaches `weed s3` across the pod, as here across the containers' network.
  assert.equal(valueOf(limiter.env ?? [], "GET_LIMITER_UPSTREAM"), `127.0.0.1:${port}`);
  return {
    s3: { image: s3.image, args: s3.args, env: s3.env ?? [], limit: s3.resources?.limits?.["memory"] ?? "" },
    sysctls: deployment.spec?.template.spec?.securityContext?.sysctls ?? [],
    limiter: {
      image: `node:${process.versions.node}-slim`,
      args: limiter.command,
      env: (limiter.env ?? []).map((each) => (each.name === "GET_LIMITER_UPSTREAM" ? { name: each.name, value: `s3:${port}` } : each)),
      limit: limiter.resources?.limits?.["memory"] ?? "",
    },
  };
}

/** Docker's `--memory` of a Kubernetes quantity of Gi or Mi. */
const memoryOf = (limit: string) => {
  assert.match(limit, /^\d+[GM]i$/u);
  return limit.replace(/i$/u, "");
};

/**
 * A store of `gateway`'s image's parts in Docker, its gateway run as `gateway` says, given to
 * `use` with the containers of `weed s3` and of its limiter if it has one; removed after.
 */
async function withStore<A>(gateway: Gateway, use: (endpoint: string, s3: string, limiter: string | null) => Promise<A>): Promise<A> {
  const { image, args, env, limit } = gateway.s3;
  const sysctls = gateway.sysctls.flatMap(({ name: key, value }) => ["--sysctl", `${key}=${value}`]);
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
    const environment = (of: Container["env"]) => of.flatMap(({ name: key, value }) => ["-e", `${key}=${value ?? ""}`]);
    run("s3", "--memory", memoryOf(limit), "--memory-swap", memoryOf(limit), "-p", `127.0.0.1::${s3Port(args)}`, ...sysctls, ...environment(env),
      image, "s3", "-filer=filer:8888", "-debug", "-debug.port=6060", ...own);
    // The limiter, of this tree, as the alasio image holds it, held to its limit, its
    // connections' buffers as its pod's.
    const { limiter } = gateway;
    if (limiter) {
      run("limiter", "--memory", memoryOf(limiter.limit), "--memory-swap", memoryOf(limiter.limit), "-p", `127.0.0.1::${valueOf(limiter.env, "GET_LIMITER_PORT")}`, ...sysctls, ...environment(limiter.env),
        "-v", `${join(ROOT, "src")}:/opt/alasio/src:ro`, "-v", `${join(ROOT, "package.json")}:/opt/alasio/package.json:ro`, limiter.image, ...limiter.args);
    }
    const port = limiter ? docker("port", name("limiter"), valueOf(limiter.env, "GET_LIMITER_PORT")).split(":").at(-1) : docker("port", name("s3"), s3Port(args)).split(":").at(-1);
    const endpoint = `http://127.0.0.1:${port}`;
    for (let tries = 0; ; tries += 1) {
      const answered = await ask(`${endpoint}/load`, { method: "PUT" }).then(({ status }) => status, () => 0);
      if (answered === 200) break;
      assert.ok(tries < 120, "the gateway never made its bucket");
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
    return await use(endpoint, name("s3"), gateway.limiter ? name("limiter") : null);
  } finally {
    rmSync(profileOf(name("s3")), { force: true });
    for (const part of [...(gateway.limiter ? ["limiter"] : []), "s3", "filer", "volume", "master"]) {
      try {
        docker("rm", "-f", name(part));
      } catch {}
    }
    docker("network", "rm", network);
  }
}

const part = Buffer.alloc(PART_BYTES, 7);

/** Connections kept, as the store's clients keep theirs. */
const agent = new Agent({ keepAlive: true, maxSockets: 256 });

/**
 * How long a request may go without a byte moving before the client gives it up: the
 * shortest of the store's real clients', DuckDB's httpfs for the lake (http_timeout, 30 s,
 * which it retries thrice). The node agent's is 60 s (src/node-agent/s3.ts), the backups'
 * aws CLI's and JuiceFS's on the workspace mounts 60 s, retried, and Neon's remote
 * storage's 120 s, retried. A request the gateway holds, or its limiter queues, longer fails.
 */
const CLIENT_IDLE_MS = 30_000;

/**
 * The least share of the median GET's rate any GET's may come at once it has begun: a GET
 * starved of its reads trickles, as 4.48's did at 0.1 to 0.2 MiB/s where the median GET's
 * runs at 8 to 24, a fortieth of it or less, its bytes never stopping long enough for its
 * client to give it up; the slowest of the rest come at an eighteenth at least, as 0.84 MiB/s
 * beside 15 did, and SeaweedFS's own unbounded 0.48 beside 8.9, on runners slow throughout.
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
  /** How long the longest request took, start to last byte, and which. */
  slowestMs: number;
  slowest: string;
}

const noTimings = (): Timings => ({ gapMs: 0, gap: "", rates: [], leastRate: Number.POSITIVE_INFINITY, least: "", slowestMs: 0, slowest: "" });
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
  });
}

/** A multipart upload of `bytes` to `key`, its parts four at a time, as the node agent puts a snapshot. */
async function upload(endpoint: string, key: string, bytes: number): Promise<void> {
  const id = /<UploadId>([^<]+)<\/UploadId>/u.exec((await ask(`${endpoint}/load/${key}?uploads`, { method: "POST", text: true })).text)?.[1];
  assert.ok(id);
  const count = bytes / PART_BYTES;
  const etags: string[] = [];
  let next = 0;
  await Promise.all(Array.from({ length: 4 }, async () => {
    while (next < count) {
      const number = ++next;
      etags[number] = (await ask(`${endpoint}/load/${key}?partNumber=${number}&uploadId=${id}`, { method: "PUT", body: part })).etag;
    }
  }));
  const parts = etags.map((etag, number) => (etag ? `<Part><PartNumber>${number}</PartNumber><ETag>${etag}</ETag></Part>` : "")).join("");
  await ask(`${endpoint}/load/${key}?uploadId=${id}`, { method: "POST", body: `<CompleteMultipartUpload>${parts}</CompleteMultipartUpload>` });
}

/** `key` read whole in ranges of `bytes`, `parallel` at a time: what was read. */
async function read(endpoint: string, key: string, parallel: number, bytes: number): Promise<number> {
  let next = 0;
  let got = 0;
  await Promise.all(Array.from({ length: parallel }, async () => {
    while (next * bytes < IMAGE_BYTES) {
      const at = next++ * bytes;
      const asked = Math.min(IMAGE_BYTES, at + bytes) - at;
      const answered = (await ask(`${endpoint}/load/${key}`, { headers: { range: `bytes=${at}-${at + asked - 1}` } })).bytes;
      if (answered !== asked) throw new Error(`GET ${key} bytes=${at}-${at + asked - 1}: ${answered} bytes of ${asked}`);
      got += answered;
    }
  }));
  return got;
}

/** Where the heap profile the gateway gave at its largest heap is kept. */
const profileOf = (gateway: string) => join(tmpdir(), `${gateway}.heap.pb.gz`);

/**
 * The largest of each of `fields` `container`'s cgroup's memory.stat said, in MiB, sampled
 * every second until `until` settles; where it is `weed s3` (`profiled`), its Go heap's
 * profile kept (profileOf) each time its anonymous memory is the largest yet, as its pprof
 * (-debug) gives it.
 */
async function sampled(container: string, profiled: boolean, until: Promise<unknown>, fields: readonly string[]): Promise<Record<string, number>> {
  const most: Record<string, number> = Object.fromEntries(fields.map((field) => [field, 0]));
  let profiledAt = 0;
  let done = false;
  void until.finally(() => {
    done = true;
  });
  while (!done) {
    try {
      const stat = (await dockerAsync("exec", container, "cat", "/sys/fs/cgroup/memory.stat")).toString("utf8");
      for (const field of fields) {
        const bytes = Number(new RegExp(`^${field} (\\d+)$`, "mu").exec(stat)?.[1] ?? 0);
        most[field] = Math.max(most[field] ?? 0, Math.round(bytes / MiB));
      }
      if (profiled && (most["anon"] ?? 0) > profiledAt + 32) {
        profiledAt = most["anon"] ?? 0;
        try {
          writeFileSync(profileOf(container), await dockerAsync("exec", container, "wget", "-q", "-O", "-", "http://127.0.0.1:6060/debug/pprof/heap"));
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

/** What the load came to: whether every request was answered, and how `weed s3`, and its limiter if it has one, came out of it. */
interface Loaded {
  readonly answered: boolean;
  readonly failed: readonly string[];
  readonly oomKilled: boolean;
  readonly peakMiB: number | null;
  /** Its requests' timings: the longest gap, the GETs' least, a hundredth's and median rates, and the slowest. */
  readonly timings: { readonly gapMs: number; readonly gap: string; readonly gets: number; readonly leastMiBps: number; readonly least: string; readonly p1MiBps: number; readonly medianMiBps: number; readonly floorMiBps: number; readonly slowestMs: number; readonly slowest: string };
  readonly mostMiB: Record<string, number>;
  readonly limiter: { readonly oomKilled: boolean; readonly peakMiB: number | null; readonly mostMiB: Record<string, number> } | null;
}

/** The load, with `restores` restores, on a store whose `weed s3` is `gateway` and whose limiter is `limiter`. */
async function load(restores: number, endpoint: string, gateway: string, limiter: string | null): Promise<Loaded> {
  await upload(endpoint, "image", IMAGE_BYTES);
  timings = noTimings();
  const failed: string[] = [];
  // A read that ended short, answered as if whole.
  const whole = (got: number, of: string) => {
    if (got !== IMAGE_BYTES && failed.length < 5) failed.push(`${of} read ${got} of ${IMAGE_BYTES} bytes`);
    return got === IMAGE_BYTES;
  };
  const loading = Promise.all([
    ...Array.from({ length: UPLOADS }, (_, index) => upload(endpoint, `snapshot-${index}`, SNAPSHOT_BYTES).then(() => true)),
    ...Array.from({ length: restores }, () => read(endpoint, "image", 8, 16 * MiB).then((got) => whole(got, "a restore's"))),
    read(endpoint, "image", 4, PART_BYTES).then((got) => whole(got, "the node agent's get")),
  ].map((each) => each.catch((error: unknown) => {
    if (failed.length < 5) failed.push(error instanceof Error ? `${error.message}${error.cause instanceof Error ? `: ${error.cause.message}` : ""}` : String(error));
    return false;
  }))).then((all) => all.every(Boolean));
  // What its memory goes to, at most: its Go heap and stacks (anon), the page cache (file),
  // and its connections' buffers (sock), all of them charged to its cgroup.
  const fields = ["anon", "file", "sock", "kernel"];
  const [answered, mostMiB, limiterMostMiB] = await Promise.all([loading, sampled(gateway, true, loading, fields), limiter === null ? null : sampled(limiter, false, loading, fields)]);
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
  const { gapMs, gap, leastRate, least, slowestMs, slowest } = timings;
  const timed = { gapMs, gap, gets: rates.length, leastMiBps: Math.round(leastRate * 100) / 100, least, p1MiBps: at(0.01), medianMiBps: at(0.5), floorMiBps: Math.round(rate(0.5) * LEAST_SHARE_OF_MEDIAN * 100) / 100, slowestMs, slowest };
  return { answered, failed, ...outcome(gateway), timings: timed, mostMiB, limiter: limiter === null || limiterMostMiB === null ? null : { ...outcome(limiter), mostMiB: limiterMostMiB } };
}

/** How many GETs the load has under way at once with `restores` restores: eight a restore, and the node agent's four. */
const streams = (restores: number) => restores * 8 + 4;

/**
 * That `loaded`, the load on the gateway as alasio runs it, held: every request answered,
 * none going as long as the store's most impatient client waits with nothing moving, no GET
 * starved, each coming at LEAST_SHARE_OF_MEDIAN of its peers' rate at least once begun,
 * however long it took whole,
 * and neither `weed s3` nor its limiter killed, each within its limit.
 */
function assertHeld(loaded: Loaded, gateway: Gateway): void {
  const mebibytes = (limit: string) => Number(/^(\d+)([GM])i$/u.exec(limit)?.[1]) * (limit.endsWith("Gi") ? 1024 : 1);
  assert.deepEqual([loaded.answered, loaded.oomKilled, loaded.limiter?.oomKilled, loaded.failed], [true, false, false, []]);
  assert.ok(loaded.timings.gapMs < CLIENT_IDLE_MS, `${loaded.timings.gap} went ${loaded.timings.gapMs} ms with nothing moving`);
  assert.ok(loaded.timings.gets > 0 && loaded.timings.leastMiBps >= loaded.timings.floorMiBps, `${loaded.timings.least}: ${loaded.timings.leastMiBps} MiB/s, under ${loaded.timings.floorMiBps}`);
  assert.ok(loaded.peakMiB !== null && loaded.peakMiB < mebibytes(gateway.s3.limit), `weed s3's peak: ${loaded.peakMiB} MiB of ${gateway.s3.limit}`);
  assert.ok(gateway.limiter && loaded.limiter?.peakMiB != null && loaded.limiter.peakMiB < mebibytes(gateway.limiter.limit), `its limiter's peak: ${loaded.limiter?.peakMiB} MiB of ${gateway.limiter?.limit}`);
}

/** What `loaded` came to, and how far under its limits. */
const said = (loaded: Loaded, gateway: Gateway) => `${JSON.stringify(loaded)}; weed s3 at most ${loaded.peakMiB} MiB of ${gateway.s3.limit}${gateway.limiter ? `, its limiter ${loaded.limiter?.peakMiB} MiB of ${gateway.limiter.limit}` : ""}`;

test(`an S3 gateway as alasio runs it serves snapshots put and restores read at once, ${streams(8)} GETs at once as the box's are, within its memory, no GET starved and no request still for 30 s`, { skip: !RUN && "ALASIO_GATEWAY_LOAD is not set: the load is several GiB" }, async (t) => {
  const gateway = gatewayContainer();
  assert.match(gateway.s3.image, /^chrislusf\/seaweedfs:/u);
  // As SeaweedFS leaves it, its uploads unbounded, its connections' buffers the node's, its
  // runtime unaware of the limit and its GETs unbounded, for comparison: killed at 1 GiB under
  // this load (CI), as the box's was, and at 2 GiB a GET at times of minutes; said, not asserted.
  const unbounded: Gateway = { s3: { ...gateway.s3, args: gateway.s3.args.filter((arg) => !/^-concurrentUploadLimitMB=/u.test(arg)), env: [] }, sysctls: [] };
  t.diagnostic(`as SeaweedFS leaves it: ${said(await withStore(unbounded, (...on) => load(8, ...on)), unbounded)}`);
  const loaded = await withStore(gateway, (...on) => load(8, ...on));
  t.diagnostic(`as alasio runs it: ${said(loaded, gateway)}`);
  assertHeld(loaded, gateway);
});

test(`an S3 gateway as alasio runs it serves ${streams(15)} GETs at once, past where they would take each other's reads, within its memory, no GET starved and no request still for 30 s`, { skip: !RUN && "ALASIO_GATEWAY_LOAD is not set: the load is several GiB" }, async (t) => {
  const gateway = gatewayContainer();
  const loaded = await withStore(gateway, (...on) => load(15, ...on));
  t.diagnostic(`as alasio runs it: ${said(loaded, gateway)}`);
  assertHeld(loaded, gateway);
});

/** A stand-in for `weed s3`, its fastest: a GET answered with its range's bytes, or 16 MiB, as fast as they are taken; a PUT's body read and answered. */
const STAND_IN = `
const http = require("node:http");
const piece = Buffer.alloc(1 << 20);
http.createServer((request, response) => {
  if (request.method !== "GET") {
    request.resume();
    return request.on("end", () => response.writeHead(200, { etag: '"part"' }).end());
  }
  const range = /bytes=(\\d+)-(\\d+)/.exec(request.headers.range ?? "");
  let left = range ? Number(range[2]) - Number(range[1]) + 1 : 16 << 20;
  response.writeHead(range ? 206 : 200, { "content-length": String(left) });
  const send = () => {
    while (left > 0) {
      const size = Math.min(left, piece.length);
      left -= size;
      if (!response.write(piece.subarray(0, size))) return response.once("drain", send);
    }
    response.end();
  };
  send();
}).listen(8334);
`;

/** What node said of its own memory every half second, to its output, imported before the limiter. */
const MEMORY_SAID = `data:text/javascript,${encodeURIComponent("setInterval(() => console.log(`memory ${JSON.stringify(process.memoryUsage())}`), 500).unref();")}`;

test(`the GET limiter alone, under ${streams(15)} GETs and 32 uploads' PUTs at once from a gateway at its fastest, holds what it passes on only as it passes it on, within its limit`, { skip: !RUN && "ALASIO_GATEWAY_LOAD is not set: the load is several GiB" }, async (t) => {
  const { limiter, sysctls } = gatewayContainer();
  assert.ok(limiter);
  const network = `gateway-limiter-${randomBytes(4).toString("hex")}`;
  const name = (part: string) => `${network}-${part}`;
  docker("network", "create", network);
  try {
    docker("run", "-d", "--name", name("s3"), "--network", network, "--network-alias", "s3", limiter.image, "node", "-e", STAND_IN);
    const [node = "", script = ""] = limiter.args;
    docker("run", "-d", "--name", name("limiter"), "--network", network, "--memory", memoryOf(limiter.limit), "--memory-swap", memoryOf(limiter.limit), "-p", `127.0.0.1::${valueOf(limiter.env, "GET_LIMITER_PORT")}`,
      ...sysctls.flatMap(({ name: key, value }) => ["--sysctl", `${key}=${value}`]), ...limiter.env.flatMap(({ name: key, value }) => ["-e", `${key}=${value ?? ""}`]),
      "-v", `${join(ROOT, "src")}:/opt/alasio/src:ro`, "-v", `${join(ROOT, "package.json")}:/opt/alasio/package.json:ro`, limiter.image, node, "--import", MEMORY_SAID, script);
    const endpoint = `http://127.0.0.1:${docker("port", name("limiter"), valueOf(limiter.env, "GET_LIMITER_PORT")).split(":").at(-1)}`;
    for (let tries = 0; ; tries += 1) {
      if (await ask(`${endpoint}/load/ready`, { method: "PUT", body: "" }).then(() => true, () => false)) break;
      assert.ok(tries < 60, "the limiter never answered");
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
    timings = noTimings();
    const loading = Promise.all([
      ...Array.from({ length: 15 }, () => read(endpoint, "image", 8, 16 * MiB)),
      read(endpoint, "image", 4, PART_BYTES),
      ...Array.from({ length: UPLOADS * 4 }, async (_, index) => {
        for (let number = 1; number <= 16; number += 1) await ask(`${endpoint}/load/alone-${index}?partNumber=${number}&uploadId=alone`, { method: "PUT", body: part });
      }),
    ]);
    const [, mostMiB] = await Promise.all([loading, sampled(name("limiter"), false, loading, ["anon", "file", "sock", "kernel"])]);
    // The most node said its process held, of each kind, in MiB.
    const said: Record<string, number> = {};
    for (const [, json] of docker("logs", name("limiter")).matchAll(/^memory (\{.*\})$/gmu)) {
      for (const [kind, bytes] of Object.entries(JSON.parse(json ?? "{}") as Record<string, number>)) said[kind] = Math.max(said[kind] ?? 0, Math.round(bytes / MiB));
    }
    const { oomKilled, peakMiB } = outcome(name("limiter"));
    t.diagnostic(`the limiter alone: ${JSON.stringify({ oomKilled, peakMiB, mostMiB, nodeSaidMiB: said, slowestMs: timings.slowestMs, leastMiBps: timings.leastRate })}, of ${limiter.limit}`);
    assert.equal(oomKilled, false);
    assert.ok(peakMiB !== null && peakMiB < Number(/^(\d+)Mi$/u.exec(limiter.limit)?.[1]), `its peak: ${peakMiB} MiB of ${limiter.limit}`);
  } finally {
    for (const part of ["limiter", "s3"]) {
      try {
        docker("rm", "-f", name(part));
      } catch {}
    }
    docker("network", "rm", network);
  }
});
