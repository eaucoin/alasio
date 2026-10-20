/**
 * The object store's S3 gateway under load as the box puts it under, in Docker, as alasio
 * runs it (cli/src/manifests/object-store.ts): SeaweedFS's image, master, volume server and
 * filer beside it, the gateway held to its memory limit and run with the arguments,
 * environment and sysctls alasio gives it. The load: snapshots' multipart uploads, their 32 MiB parts
 * four at a time (the node agent's), and restores' ranged reads of a 1 GiB image, eight of
 * 16 MiB at a time each (the checkpoint gofer's), with the node agent's 32 MiB gets beside.
 *
 * Several GiB go through it, so it runs only where ALASIO_GATEWAY_LOAD is set (CI's
 * gateway-memory job), never on a machine that runs anything else.
 */
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import { existsSync, rmSync, writeFileSync } from "node:fs";
import { Agent, request as httpRequest } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

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
const RESTORES = 8;

const docker = (...args: string[]) => execFileSync("docker", args, { encoding: "utf8", maxBuffer: 16 * MiB }).trim();

/** The gateway's container as alasio's default install makes it. */
function gatewayContainer() {
  const decoded = decodeInstallConfig({ alasio: { telegram: { existingSecret: "telegram" } } });
  if (Result.isFailure(decoded)) throw new Error(decoded.failure.message);
  const deployment = manifests(decoded.success, NOTHING_CARRIED).find((object: KubernetesObject) => object.kind === "Deployment" && object.metadata?.name === "alasio-seaweedfs") as V1Deployment;
  const s3 = deployment.spec?.template.spec?.containers.find(({ name }) => name === "s3");
  assert.ok(s3?.image && s3.args);
  return { image: s3.image, args: s3.args, env: s3.env ?? [], limit: s3.resources?.limits?.["memory"] ?? "", sysctls: deployment.spec?.template.spec?.securityContext?.sysctls ?? [] };
}

/** A store of the image's parts in Docker, its gateway run with `args` and `env` under `limit`, given to `use`; removed after. */
async function withStore<A>(image: string, args: readonly string[], env: readonly { name: string; value?: string }[], sysctls: readonly { name: string; value: string }[], limit: string, use: (endpoint: string, gateway: string) => Promise<A>): Promise<A> {
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
    run("s3", "--memory", limit.replace(/i$/u, ""), "--memory-swap", limit.replace(/i$/u, ""), "-p", "127.0.0.1::8333",
      ...sysctls.flatMap(({ name: key, value }) => ["--sysctl", `${key}=${value}`]),
      ...env.flatMap(({ name: key, value }) => ["-e", `${key}=${value ?? ""}`]), image, "s3", "-filer=filer:8888", "-debug", "-debug.port=6060", ...own);
    const port = docker("port", name("s3"), "8333").split(":").at(-1);
    const endpoint = `http://127.0.0.1:${port}`;
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

const part = Buffer.alloc(PART_BYTES, 7);

/** Connections kept, as the store's clients keep theirs. */
const agent = new Agent({ keepAlive: true, maxSockets: 256 });

/**
 * How long a request may go without a byte moving before the client gives it up: the
 * shortest of the store's real clients', DuckDB's httpfs for the lake (http_timeout, 30 s,
 * which it retries thrice). The node agent's is 60 s (src/node-agent/s3.ts), the backups'
 * aws CLI's and JuiceFS's on the workspace mounts 60 s, retried, and Neon's remote
 * storage's 120 s, retried. A request the gateway holds for its budgets longer fails.
 */
const CLIENT_IDLE_MS = 30_000;

/** How long the longest request took, from its start to its last byte, and which it was. */
let slowestMs = 0;
let slowest = "";

/**
 * `method` of `url`, with `body` and `headers`: its status, its ETag, its body's text if
 * `text`, else only how long it was. One not answered 2xx fails, as does one nothing moved
 * of for CLIENT_IDLE_MS.
 */
function ask(url: string, { method = "GET", headers = {}, body, text = false }: { method?: string; headers?: Record<string, string>; body?: Buffer | string; text?: boolean } = {}): Promise<{ readonly status: number; readonly etag: string; readonly text: string; readonly bytes: number }> {
  const started = Date.now();
  return new Promise<{ readonly status: number; readonly etag: string; readonly text: string; readonly bytes: number }>((resolve, reject) => {
    const asked = httpRequest(url, { method, headers: { ...headers, ...(body === undefined ? {} : { "content-length": String(Buffer.byteLength(body)) }) }, agent }, (answer) => {
      const kept: Buffer[] = [];
      let bytes = 0;
      answer.on("data", (chunk: Buffer) => {
        bytes += chunk.length;
        if (text || (answer.statusCode ?? 0) >= 300) kept.push(chunk);
      });
      answer.on("end", () => {
        const status = answer.statusCode ?? 0;
        const said = Buffer.concat(kept).toString("utf8");
        if (status < 200 || status >= 300) reject(new Error(`${method} ${url}: ${status} ${said.slice(0, 200)}`));
        else resolve({ status, etag: String(answer.headers.etag ?? ""), text: said, bytes });
      });
      answer.on("error", (error) => reject(new Error(`${method} ${url}: ${error.message} after ${Date.now() - started} ms, ${bytes} bytes in`)));
    });
    asked.setTimeout(CLIENT_IDLE_MS, () => asked.destroy(new Error(`${method} ${url}: nothing moved for ${CLIENT_IDLE_MS / 1000} s`)));
    asked.on("error", (error) => reject(error.message.startsWith(method) ? error : new Error(`${method} ${url}: ${error.message} after ${Date.now() - started} ms`)));
    asked.end(body);
  }).finally(() => {
    if (Date.now() - started > slowestMs) {
      slowestMs = Date.now() - started;
      slowest = `${method} ${url}${headers["range"] ? ` ${headers["range"]}` : ""}`;
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

/** The load, on a store whose gateway is `gateway`: whether every request was answered, and how the gateway came out of it. */
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
      const stat = docker("exec", gateway, "cat", "/sys/fs/cgroup/memory.stat");
      for (const field of fields) {
        const bytes = Number(new RegExp(`^${field} (\\d+)$`, "mu").exec(stat)?.[1] ?? 0);
        most[field] = Math.max(most[field] ?? 0, Math.round(bytes / MiB));
      }
      if ((most["anon"] ?? 0) > profiledAt + 32) {
        profiledAt = most["anon"] ?? 0;
        try {
          writeFileSync(profileOf(gateway), execFileSync("docker", ["exec", gateway, "wget", "-q", "-O", "-", "http://127.0.0.1:6060/debug/pprof/heap"], { maxBuffer: 64 * MiB }));
        } catch {}
      }
    } catch {
      break;
    }
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
  return most;
}

async function load(endpoint: string, gateway: string): Promise<{ readonly answered: boolean; readonly failed: readonly string[]; readonly oomKilled: boolean; readonly peakMiB: number | null; readonly slowestMs: number; readonly slowest: string; readonly mostMiB: Record<string, number> }> {
  await upload(endpoint, "image", IMAGE_BYTES);
  slowestMs = 0;
  slowest = "";
  const failed: string[] = [];
  // A read that ended short, answered as if whole.
  const whole = (got: number, of: string) => {
    if (got !== IMAGE_BYTES && failed.length < 5) failed.push(`${of} read ${got} of ${IMAGE_BYTES} bytes`);
    return got === IMAGE_BYTES;
  };
  const loading = Promise.all([
    ...Array.from({ length: UPLOADS }, (_, index) => upload(endpoint, `snapshot-${index}`, SNAPSHOT_BYTES).then(() => true)),
    ...Array.from({ length: RESTORES }, () => read(endpoint, "image", 8, 16 * MiB).then((got) => whole(got, "a restore's"))),
    read(endpoint, "image", 4, PART_BYTES).then((got) => whole(got, "the node agent's get")),
  ].map((each) => each.catch((error: unknown) => {
    if (failed.length < 5) failed.push(error instanceof Error ? `${error.message}${error.cause instanceof Error ? `: ${error.cause.message}` : ""}` : String(error));
    return false;
  }))).then((all) => all.every(Boolean));
  // What its memory goes to, at most: its Go heap and stacks (anon), the page cache (file),
  // and its connections' buffers (sock), all of them charged to its cgroup.
  const [answered, mostMiB] = await Promise.all([loading, sampled(gateway, loading, ["anon", "file", "sock", "kernel"])]);
  const state = JSON.parse(docker("inspect", "--format", "{{json .State}}", gateway)) as { OOMKilled: boolean; Running: boolean };
  const peak = state.Running ? Number(docker("exec", gateway, "cat", "/sys/fs/cgroup/memory.peak")) : Number.NaN;
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
  return { answered, failed, oomKilled: state.OOMKilled, peakMiB: Number.isNaN(peak) ? null : Math.round(peak / MiB), slowestMs, slowest, mostMiB };
}

test("an S3 gateway as alasio runs it serves snapshots put and restores read at once within its memory and within 30 s a request", { skip: !RUN && "ALASIO_GATEWAY_LOAD is not set: the load is several GiB" }, async (t) => {
  const { image, args, env, limit, sysctls } = gatewayContainer();
  assert.match(image, /^chrislusf\/seaweedfs:/u);
  const limitMiB = Number(/^(\d+)Gi$/u.exec(limit)?.[1]) * 1024;
  assert.ok(limitMiB > 0, `its limit: ${limit}`);
  // As SeaweedFS leaves it, its uploads unbounded, its connections' buffers the node's and its
  // runtime unaware of the limit, for comparison: killed at 1 GiB under this load (CI), as the
  // box's was; said, not asserted, at alasio's limit.
  const unbounded = await withStore(image, args.filter((arg) => !/^-concurrentUploadLimitMB=/u.test(arg)), [], [], limit, load);
  t.diagnostic(`as SeaweedFS leaves it, at ${limit}: ${JSON.stringify(unbounded)}`);
  // As alasio runs it: every request answered, within what the store's most impatient
  // client waits, and the gateway never killed, within its limit.
  const bounded = await withStore(image, args, env, sysctls, limit, load);
  t.diagnostic(`as alasio runs it, at ${limit}: ${JSON.stringify(bounded)}; ${bounded.peakMiB === null ? "" : `${limitMiB - bounded.peakMiB} MiB under its limit`}`);
  assert.deepEqual([bounded.answered, bounded.oomKilled, bounded.failed], [true, false, []]);
  assert.ok(bounded.slowestMs < CLIENT_IDLE_MS, `${bounded.slowest} took ${bounded.slowestMs} ms`);
  assert.ok(bounded.peakMiB !== null && bounded.peakMiB < limitMiB, `its peak: ${bounded.peakMiB} MiB of ${limitMiB}`);
});
