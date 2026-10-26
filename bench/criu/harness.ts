/**
 * The criu system's runs (DESIGN.md section 5): CRIU v4.3 on runc v1.5.2, as README.md says
 * and why. A run starts the instance with `runc run`, sets the fixture up, and saves it twice,
 * under load (pre-dump rounds and `runc checkpoint --leave-running`) and stopping (pre-dump
 * rounds on top of that image and `runc checkpoint`), each staged on the runner's disk and
 * uploaded to the bucket as it is written. It then restores the stopping save's image four
 * times, each variant warm (the staged images) and cold (the bucket's): `lazy`, `runc restore
 * --lazy-pages` beside `criu lazy-pages`, cold through a Mountpoint for Amazon S3 mount of the
 * bucket; `eager`, a plain `runc restore`, cold once the image is downloaded whole. Run as
 * root, with the store's variables (../common/store/s3.ts) in the environment.
 *
 *   node harness.ts --profile P --work DIR --host host.json --store store.json --dist DIR
 *     --out results.jsonl [--target host] [--runs 5] [--cases 'idle/16 poller/200 ...']
 *     [--rounds converge|phaul] [--pre-dump-mode read|splice] [--variant-suffix S]
 *
 * --dist is common's bundles (driver.mjs, s3.mjs); --cases, `<fixture>/<MiB>` words, all of
 * ../common/cases.json where empty.
 */
import { type ChildProcess, spawn, spawnSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import { appendFileSync, copyFileSync, existsSync, mkdirSync, readdirSync, readFileSync, rmSync, statSync, symlinkSync, writeFileSync } from "node:fs";
import { dirname, join, relative } from "node:path";
import { parseArgs } from "node:util";

import { config as ociConfig } from "../common/oci/config.ts";
import { list, remove, storeFromEnvironment, upload } from "../common/store/s3.ts";

const here = dirname(new URL(import.meta.url).pathname);

const VARIANTS = ["lazy", "eager"] as const;
type Variant = (typeof VARIANTS)[number];

const VERSIONS = { system: "criu 4.3, runc 1.5.2", criu: "4.3", runc: "1.5.2", "mountpoint-s3": "1.24.0", bayma: "0.15.2", seaweedfs: "4.48", toxiproxy: "2.13.0" };

/** The instance's network: a namespace of the job's own, external to CRIU, its veth's far end. */
const NETNS = "/var/run/netns/criu-bench";
const ADDRESS = "10.89.0.2";
const PORT = 8765;
/** runc's state of the job's containers. */
const RUNC_ROOT = "/run/criu-bench";
/** The cgroup the instances' cgroups go under. */
const CGROUP_PARENT = "criu-bench";

/**
 * The pre-dump rounds' policy, P.Haul's (phaul/iters.py, CRIU's own live-migration driver): a
 * pre-dump writing at most 64 pages, one growing by more than 10 % on the one before, or the
 * 8th, ends the rounds. `converge` (README.md) also ends them at a round that is not at least
 * 10 % smaller than the one before, where more rounds stage the same pages again.
 */
const MAX_ROUNDS = 8;
const MIN_ROUND_PAGES = 64;
const MAX_GROW_PERCENT = 10;
const MIN_SHRINK_PERCENT = 10;
type Rounds = "converge" | "phaul";

const PAGE = 4096;
/** How long a lazy restore's daemon may take to load every page, and a probe to answer. */
const RESIDENT_TIMEOUT_MS = 20 * 60_000;

const now = (): bigint => process.hrtime.bigint();
const msBetween = (from: bigint, to: bigint): number => Number(to - from) / 1e6;
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

const { values: options } = parseArgs({
  options: {
    profile: { type: "string" },
    target: { type: "string", default: "host" },
    work: { type: "string" },
    host: { type: "string" },
    store: { type: "string" },
    dist: { type: "string" },
    logs: { type: "string" },
    out: { type: "string" },
    runs: { type: "string", default: "5" },
    cases: { type: "string", default: "" },
    rounds: { type: "string", default: "converge" },
    "pre-dump-mode": { type: "string", default: "read" },
    "variant-suffix": { type: "string", default: "" },
  },
});
const required = (name: keyof typeof options): string => {
  const value = options[name];
  if (typeof value !== "string" || value === "") throw new Error(`--${name} is required`);
  return value;
};
const profile = required("profile");
const work = required("work");
const driver = join(required("dist"), "driver.mjs");
const s3 = join(required("dist"), "s3.mjs");
const out = required("out");
const logs = required("logs");
const host = JSON.parse(readFileSync(required("host"), "utf8")) as { readonly copy_mib_s: number };
const storeProbe = JSON.parse(readFileSync(required("store"), "utf8")) as Record<string, unknown>;
const profiles = JSON.parse(readFileSync(join(here, "../common/store/profiles.json"), "utf8")) as Record<string, { readonly emulated: boolean }>;
const rounds = required("rounds") as Rounds;
if (rounds !== "converge" && rounds !== "phaul") throw new Error("--rounds is converge or phaul");
const preDumpMode = required("pre-dump-mode");
if (preDumpMode !== "read" && preDumpMode !== "splice") throw new Error("--pre-dump-mode is read or splice");
const suffix = options["variant-suffix"] ? `-${options["variant-suffix"]}` : "";
const store = storeFromEnvironment();
/** kernel.io_uring_disabled, which run.sh sets (README.md). */
const ioUringDisabled = Number(readFileSync("/proc/sys/kernel/io_uring_disabled", "utf8").trim());
const rootfs = join(work, "rootfs");

/**
 * CRIU's options for every request runc makes of it (pre-dump, dump, restore), a configuration
 * file each container names in its `org.criu.config` annotation, which take precedence over
 * runc's own (runc libcontainer/criu_linux.go handleCriuConfigurationFile). Why each: README.md.
 */
const criuConfig = join(work, "criu.conf");
mkdirSync(work, { recursive: true });
writeFileSync(criuConfig, [
  // Established TCP connections neither dumped nor locked, and restored closed, as bayma promises.
  "tcp-close",
  // The file locks the instance holds (bayma's SQLite state), which only it uses.
  "file-locks",
  // No packets dropped while a save or a restore runs: with tcp-close there is no connection
  // to keep, and a client's SYN dropped by the lock waits out TCP's 1 s retransmission.
  "network-lock skip",
  // How a pre-dump reads the pages: after the tasks thaw (read), or spliced while they are frozen.
  `pre-dump-mode ${preDumpMode}`,
  "",
].join("\n"));

// ---------------------------------------------------------------------------------------------
// Commands

interface Ran {
  readonly status: number | null;
  readonly stdout: string;
  readonly stderr: string;
}

/** Runs `file` to its end; throws, with what it said, where it fails. */
function must(file: string, args: readonly string[]): string {
  const ran = spawnSync(file, args, { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"], maxBuffer: 64 * 1024 ** 2 });
  if (ran.error) throw ran.error;
  if (ran.status !== 0) throw new Error(`${file} ${args.join(" ")} exited ${ran.status}: ${(ran.stderr ?? "").trim().slice(-4000)}`);
  return ran.stdout ?? "";
}

/** Starts `file`; resolves with how it ended. */
function started(file: string, args: readonly string[], stdio: readonly (number | "ignore" | "pipe")[] = ["ignore", "pipe", "pipe"], detached = false): { readonly child: ChildProcess; readonly ended: Promise<Ran> } {
  const child = spawn(file, args, { stdio: [...stdio], detached });
  let stdout = "";
  let stderr = "";
  child.stdout?.setEncoding("utf8").on("data", (data: string) => (stdout += data));
  child.stderr?.setEncoding("utf8").on("data", (data: string) => (stderr += data));
  const ended = new Promise<Ran>((resolve, reject) => {
    child.on("error", reject);
    child.on("close", (status) => resolve({ status, stdout, stderr }));
  });
  return { child, ended };
}

/** `promise`, or a failure saying `what` where it has not settled within `ms`. */
async function within<T>(promise: Promise<T>, ms: number, what: string): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([promise, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`${what} within ${ms / 1000} s`)), ms);
    })]);
  } finally {
    clearTimeout(timer);
  }
}

/** The JSON object a driver command prints. */
async function driven(command: string, args: readonly string[]): Promise<Record<string, unknown>> {
  const { status, stdout, stderr } = await started(process.execPath, [driver, command, ...args]).ended;
  if (status !== 0) throw new Error(`driver ${command}: ${stderr.trim().slice(-4000)}`);
  return JSON.parse(stdout.trim().split("\n").at(-1) ?? "") as Record<string, unknown>;
}

/** The lines of a CRIU log that say what failed, as runc's logCriuErrors picks them. */
function criuErrors(log: string): string {
  if (!existsSync(log)) return "";
  const lines = readFileSync(log, "utf8").split("\n");
  const at = lines.findIndex((line) => line.includes("Error"));
  return at < 0 ? "" : ` (${log}: ${lines.slice(Math.max(0, at - 5), at + 1).join(" | ")})`;
}

const runcLog = join(work, "runc.log");
const instancesLog = join(work, "instances.log");

/** runc, its own log in the job's. */
const runc = (args: readonly string[]): string => must("runc", ["--root", RUNC_ROOT, "--log", runcLog, ...args]);

/**
 * `runc run` or `runc restore` of `instance`, detached, its stdio pipes, as a container's are
 * where containerd's shim starts it: stdin one a `sleep` holds open and never writes, stdout and
 * stderr one a `cat` drains into instances.log. Pipes (`pipe:[...]`) are what runc hands a
 * restored container anew (CRIU's --inherit-fd, as runc's checkpoint.bats runs it); a FIFO or a
 * file of the runner's would be a file CRIU looks for in the container's mounts.
 */
function runcAttached(instance: Instance, args: readonly string[]): void {
  try {
    must("bash", ["-c", 'exec 3< <(exec sleep infinity 2>/dev/null); echo $! >"$1"; exec 4> >(exec cat >>"$2" 2>/dev/null); shift 2; exec runc "$@" <&3 >&4 2>&4 3<&- 4>&-', "_", instance.holder, instancesLog, "--root", RUNC_ROOT, "--log", runcLog, ...args]);
  } catch (error) {
    const said = existsSync(runcLog) ? readFileSync(runcLog, "utf8").trim().split("\n").slice(-5).join(" | ") : "";
    throw new Error(`${(error as Error).message} (runc.log: ${said})`);
  }
}

// ---------------------------------------------------------------------------------------------
// CRIU's statistics

interface DumpStats {
  readonly freezing_us: number;
  readonly frozen_us: number;
  readonly memdump_us: number;
  readonly memwrite_us: number;
  readonly pages_scanned: number;
  readonly pages_skipped_parent: number;
  readonly pages_written: number;
}

/** The fields of a protobuf message: varints by field number, and length-delimited bytes. */
function fieldsOf(bytes: Buffer): Map<number, number | Buffer> {
  const fields = new Map<number, number | Buffer>();
  let at = 0;
  const varint = (): number => {
    let value = 0;
    let scale = 1;
    for (;;) {
      const byte = bytes[at++] ?? 0;
      value += (byte & 0x7f) * scale;
      if (byte < 0x80) return value;
      scale *= 128;
    }
  };
  while (at < bytes.length) {
    const key = varint();
    const wire = key & 7;
    if (wire === 0) fields.set(key >>> 3, varint());
    else if (wire === 2) {
      const length = varint();
      fields.set(key >>> 3, bytes.subarray(at, at + length));
      at += length;
    } else throw new Error(`stats-dump: wire type ${wire}`);
  }
  return fields;
}

/**
 * The dump's statistics CRIU writes to its work directory, `stats-dump`: the service magic, the
 * stats magic, a 32-bit size and a `stats_entry` (images/stats.proto).
 */
function dumpStats(workDir: string): DumpStats {
  const image = readFileSync(join(workDir, "stats-dump"));
  if (image.readUInt32LE(0) !== 0x55105940 || image.readUInt32LE(4) !== 0x57093306) throw new Error(`${workDir}/stats-dump is not a stats image`);
  const entry = fieldsOf(image.subarray(12, 12 + image.readUInt32LE(8)));
  const dump = entry.get(1);
  if (!(dump instanceof Buffer)) throw new Error(`${workDir}/stats-dump has no dump statistics`);
  const fields = fieldsOf(dump);
  const field = (number: number) => {
    const value = fields.get(number);
    return typeof value === "number" ? value : 0;
  };
  return { freezing_us: field(1), frozen_us: field(2), memdump_us: field(3), memwrite_us: field(4), pages_scanned: field(5), pages_skipped_parent: field(6), pages_written: field(7) };
}

// ---------------------------------------------------------------------------------------------
// Files

/** The bytes of an image directory's files (its `parent` link, to the image before, not followed). */
function bytesIn(dir: string): number {
  let bytes = 0;
  for (const entry of readdirSync(dir, { withFileTypes: true })) if (entry.isFile()) bytes += statSync(join(dir, entry.name)).size;
  return bytes;
}

/** A process's peak RSS as GNU time's `%M` wrote it (its last line, after any of a signal), in bytes; null where it wrote none. */
function peakOf(file: string): number | null {
  if (!existsSync(file)) return null;
  const kib = Number(readFileSync(file, "utf8").trim().split("\n").at(-1));
  return Number.isFinite(kib) ? kib * 1024 : null;
}

/** The RSS, in bytes, of the process GNU time `wrapper` runs, while it runs; 0 once it has ended. */
function rssUnder(wrapper: ChildProcess): number {
  try {
    const children = readFileSync(`/proc/${wrapper.pid}/task/${wrapper.pid}/children`, "utf8").trim().split(/\s+/u).filter(Boolean);
    let bytes = 0;
    for (const pid of children) {
      const line = readFileSync(`/proc/${pid}/status`, "utf8").split("\n").find((each) => each.startsWith("VmRSS:"));
      bytes += Number(line?.replace(/\D+/gu, "") ?? 0) * 1024;
    }
    return bytes;
  } catch {
    return 0;
  }
}

/** Keeps the logs under `dir` (CRIU's, runc's, Mountpoint's) in the job's, where a phase of `run` failed. */
function keepLogs(run: Run, dir: string): void {
  if (!run.failed || !existsSync(dir)) return;
  for (const entry of readdirSync(dir, { withFileTypes: true, recursive: true })) {
    const path = join(entry.parentPath, entry.name);
    if (entry.isFile() && (entry.name.endsWith(".log") || path.includes("mount-s3-logs"))) {
      const kept = join(logs, run.runId, relative(run.dir, path));
      mkdirSync(dirname(kept), { recursive: true });
      copyFileSync(path, kept);
    }
  }
}

/** Evicts every file from the page cache, as a cold restore starts (DESIGN.md section 4). */
function dropCaches(): void {
  must("sync", []);
  writeFileSync("/proc/sys/vm/drop_caches", "1\n");
}

// ---------------------------------------------------------------------------------------------
// The instance

let instances = 0;

interface Instance {
  readonly id: string;
  readonly bundle: string;
  readonly cgroup: string;
  /** Where the PID of the `sleep` that holds its stdin open is. */
  readonly holder: string;
}

/** A new container's bundle: the common OCI spec (../common/oci/config.ts), in a cgroup of its own, with CRIU's options. */
function bundleOf(attach: string): Instance {
  const id = `criu-${++instances}`;
  const bundle = join(work, "bundles", id);
  mkdirSync(bundle, { recursive: true });
  const spec = ociConfig({ rootfs, attach, netns: NETNS, port: PORT });
  writeFileSync(join(bundle, "config.json"), JSON.stringify({ ...spec, annotations: { "org.criu.config": criuConfig }, linux: { ...spec.linux, cgroupsPath: `/${CGROUP_PARENT}/${id}` } }, null, 2));
  return { id, bundle, cgroup: `/sys/fs/cgroup/${CGROUP_PARENT}/${id}`, holder: join(bundle, "stdin-holder.pid") };
}

/** Waits for bayma to answer its status, as a started instance's does once it listens. */
async function answering(token: string): Promise<void> {
  const deadline = Date.now() + 120_000;
  let said = "nothing";
  for (;;) {
    try {
      const answer = await fetch(`http://${ADDRESS}:${PORT}/status`, { headers: { authorization: `Bearer ${token}` } });
      if (answer.ok) return;
      said = `HTTP ${answer.status}`;
    } catch (error) {
      said = String((error as Error).cause ?? error);
    }
    if (Date.now() > deadline) {
      const printed = readFileSync(instancesLog, "utf8").trim().split("\n").slice(-10).join(" | ");
      throw new Error(`bayma did not answer within 120 s of the instance's start (${said}; it printed: ${printed})`);
    }
    await sleep(100);
  }
}

async function start(run: Run): Promise<Instance> {
  const instance = bundleOf(run.attach);
  runcAttached(instance, ["run", "--detach", "--bundle", instance.bundle, "--pid-file", join(instance.bundle, "pid"), instance.id]);
  await answering(run.token);
  return instance;
}

/** Ends `instance` without a save, and what holds its stdin; nothing where it has ended already. */
function end(instance: Instance): void {
  spawnSync("runc", ["--root", RUNC_ROOT, "kill", instance.id, "KILL"], { stdio: "ignore" });
  spawnSync("runc", ["--root", RUNC_ROOT, "delete", "--force", instance.id], { stdio: "ignore" });
  try {
    process.kill(Number(readFileSync(instance.holder, "utf8")), "SIGKILL");
  } catch {
    // Never started, or ended.
  }
}

/**
 * Resolves, as a probe that failed, once `instance` stops before `probe` ends: a probe of an
 * instance that died would wait out its HTTP timeouts. Never, once the probe has ended.
 */
async function stopsBefore(instance: Instance, probe: Promise<unknown>): Promise<Ran> {
  let probed = false;
  void probe.finally(() => {
    probed = true;
  });
  while (!probed) {
    await sleep(1000);
    const state = spawnSync("runc", ["--root", RUNC_ROOT, "state", instance.id], { encoding: "utf8" });
    if (!probed && (state.status !== 0 || (JSON.parse(state.stdout) as { status: string }).status === "stopped")) {
      return { status: 1, stdout: "", stderr: "the restored instance stopped before the probe was answered" };
    }
  }
  return new Promise<Ran>(() => {});
}

/** The cgroup's memory.peak or memory.current, in bytes; null once it is gone. */
function cgroupBytes(instance: Instance, file: "memory.peak" | "memory.current"): number | null {
  try {
    return Number(readFileSync(join(instance.cgroup, file), "utf8").trim());
  } catch {
    return null;
  }
}

// ---------------------------------------------------------------------------------------------
// A run

interface Case {
  readonly fixture: string;
  readonly sizeMib: number;
}

interface Run extends Case {
  readonly run: number;
  readonly runId: string;
  readonly dir: string;
  readonly attach: string;
  readonly stage: string;
  readonly prefix: string;
  readonly endpoint: readonly string[];
  /** The bearer bayma's clients send, which /attach/token holds. */
  readonly token: string;
  /** The fixture's bayma session, once setup has made it. */
  session: string;
  /** Whether a phase of it has failed, so its logs are kept. */
  failed: boolean;
}

type Metrics = Record<string, number | string | null>;
type Correctness = Record<string, boolean | string | null>;

const SAVE_METRICS = ["save_total_ms", "unavailable_ms", "pause_reported_ms", "slowdown", "precopy_passes", "precopy_bytes", "image_bytes", "uploaded_bytes"];
const RESTORE_METRICS = ["first_exec_ms", "verify_ms", "resident_ms", "resident_how", "memory_peak_bytes", "memory_bytes", "workload_rss_bytes"];
const UNCHECKED: Correctness = { marker: null, memory: null, children: null, file: null, socket: null, running: null, notice: null };

const nulls = (names: readonly string[]): Metrics => Object.fromEntries(names.map((name) => [name, null]));

function emit(run: Run, phase: string, variant: string, ok: boolean, failure: string | null, metrics: Metrics, correctness: Correctness, details: Record<string, unknown>): void {
  if (ok) console.log(`${run.runId} ${phase} ${variant}: ${Object.entries(metrics).filter(([, value]) => value !== null).map(([name, value]) => `${name} ${typeof value === "number" ? Math.round(value) : value}`).join(", ")}`);
  else {
    run.failed = true;
    console.error(`${run.runId} ${phase} ${variant}: ${failure}`);
  }
  const line = {
    schema: 1,
    system: "criu",
    variant: `${variant}${suffix}`,
    target: options.target,
    fixture: run.fixture,
    size_mib: run.sizeMib,
    profile,
    emulated: profiles[profile]?.emulated ?? true,
    run: run.run,
    phase,
    ok,
    failure,
    metrics,
    correctness,
    host,
    store: storeProbe,
    versions: VERSIONS,
    ci: { run_id: Number(process.env["GITHUB_RUN_ID"] ?? 0), job: `${process.env["GITHUB_JOB"] ?? "local"} criu ${options.target} ${profile}`, sha: process.env["GITHUB_SHA"] ?? "" },
    at: new Date().toISOString(),
    details: { rounds_policy: rounds, pre_dump_mode: preDumpMode, io_uring_disabled: ioUringDisabled, pause_check: pauseChecked, ...details },
  };
  appendFileSync(out, `${JSON.stringify(line)}\n`);
}

function prepare(c: Case, run: number): Run {
  const runId = `${c.fixture}-${c.sizeMib}-${profile}-${run}`;
  const dir = join(work, "runs", runId);
  const attach = join(dir, "attach");
  rmSync(dir, { recursive: true, force: true });
  // /attach as alasio's node agent lays it out (DESIGN.md section 2): the binding's token, its
  // variables, and the workspace and home the image's /workspace and /home/agent link to.
  for (const sub of ["ws/workspace", "ws/home"]) mkdirSync(join(attach, sub), { recursive: true });
  const token = randomBytes(32).toString("hex");
  writeFileSync(join(attach, "token"), token);
  writeFileSync(join(attach, "env"), "");
  must("chown", ["-R", "1000:1000", attach]);
  const stage = join(dir, "stage");
  mkdirSync(stage, { recursive: true });
  return { ...c, run, runId, dir, attach, stage, prefix: `criu/${runId}/`, endpoint: ["--url", `http://${ADDRESS}:${PORT}/mcp`, "--header", `Authorization: Bearer ${token}`], token, session: "", failed: false };
}

/** The bytes of the run's objects in the bucket. */
const bucketBytes = async (run: Run) => (await list(store, run.prefix)).reduce((sum, { bytes }) => sum + bytes, 0);

// ---------------------------------------------------------------------------------------------
// Saves

interface Round {
  readonly name: string;
  readonly ms: number;
  readonly bytes: number;
  readonly stats: DumpStats;
}

/** One `runc checkpoint` into `stage/<name>`, a pre-dump or the dump, on top of `parent`. */
function checkpoint(run: Run, instance: Instance, name: string, parent: string | null, how: "pre-dump" | "leave-running" | "stop"): Round {
  const images = join(run.stage, name);
  const workDir = `${images}.work`;
  const args = ["checkpoint", "--image-path", images, "--work-path", workDir];
  if (parent !== null) args.push("--parent-path", `../${parent}`);
  if (how === "pre-dump") args.push("--pre-dump");
  if (how === "leave-running") args.push("--leave-running");
  const from = now();
  try {
    runc([...args, instance.id]);
  } catch (error) {
    throw new Error(`${(error as Error).message}${criuErrors(join(workDir, "dump.log"))}`);
  }
  return { name, ms: msBetween(from, now()), bytes: bytesIn(images), stats: dumpStats(workDir) };
}

/** Whether another pre-dump round goes on after `done` (MAX_ROUNDS and the rules beside it). */
function goesOn(done: readonly Round[]): boolean {
  const last = done.at(-1)?.stats.pages_written ?? 0;
  const before = done.at(-2)?.stats.pages_written;
  if (last <= MIN_ROUND_PAGES) return false;
  if (before !== undefined && before > 0) {
    if (((last - before) * 100) / before > MAX_GROW_PERCENT) return false;
    if (rounds === "converge" && ((before - last) * 100) / before < MIN_SHRINK_PERCENT) return false;
  }
  return done.length < MAX_ROUNDS;
}

interface Saved {
  readonly metrics: Metrics;
  readonly details: Record<string, unknown>;
  /** The image's directories, oldest first: each one's parent is the one before it. */
  readonly chain: readonly string[];
}

/**
 * A save: pre-dump rounds while the instance runs, then the dump, `kind` its name, each
 * uploaded to the bucket as soon as it is written while the next is taken; then the chain's
 * manifest, `chain-<kind>.json`, which the restores read the order of the image's directories
 * from, last.
 */
async function save(run: Run, instance: Instance, kind: "live" | "stop", base: readonly string[]): Promise<Saved> {
  const before = await bucketBytes(run);
  const t0 = now();
  const done: Round[] = [];
  let uploads = Promise.resolve();
  let failed: unknown = null;
  const put = (name: string) => {
    uploads = uploads.then(() => upload(store, join(run.stage, name), `${run.prefix}${name}/`)).then(() => undefined, (error: unknown) => {
      failed = error;
    });
  };
  let parent = base.at(-1) ?? null;
  do {
    const round = checkpoint(run, instance, `${kind}-pre${done.length + 1}`, parent, "pre-dump");
    done.push(round);
    put(round.name);
    parent = round.name;
  } while (goesOn(done));
  const dump = checkpoint(run, instance, kind, parent, kind === "live" ? "leave-running" : "stop");
  put(kind);
  const chain = [...base, ...done.map(({ name }) => name), kind];
  const manifest = join(run.stage, `chain-${kind}`);
  mkdirSync(manifest, { recursive: true });
  writeFileSync(join(manifest, `chain-${kind}.json`), JSON.stringify({ chain }));
  uploads = uploads.then(() => upload(store, manifest, run.prefix)).then(() => undefined, (error: unknown) => {
    failed = error;
  });
  await uploads;
  const t1 = now();
  if (failed) throw failed;
  const after = await bucketBytes(run);
  const precopyPages = done.reduce((sum, { stats }) => sum + stats.pages_written, 0);
  return {
    chain,
    metrics: {
      ...nulls(SAVE_METRICS),
      save_total_ms: msBetween(t0, t1),
      pause_reported_ms: dump.stats.frozen_us / 1000,
      precopy_passes: done.length,
      precopy_bytes: precopyPages * PAGE,
      image_bytes: [...done, dump].reduce((sum, { bytes }) => sum + bytes, 0),
      uploaded_bytes: after - before,
    },
    details: { rounds: done, dump, chain },
  };
}

// ---------------------------------------------------------------------------------------------
// Restores

/** A process a restore starts for itself alone, run under GNU time for its peak RSS, in a process group of its own. */
interface Helper {
  readonly name: string;
  readonly child: ChildProcess;
  readonly ended: Promise<Ran>;
  readonly peakFile: string;
}

function helper(name: string, dir: string, file: string, args: readonly string[], stdio?: readonly (number | "ignore" | "pipe")[]): Helper {
  const peakFile = join(dir, `${name}.peak`);
  return { name, peakFile, ...started("/usr/bin/time", ["-f", "%M", "-o", peakFile, file, ...args], stdio, true) };
}

function kill(each: Helper): void {
  try {
    if (each.child.pid !== undefined) process.kill(-each.child.pid, "SIGKILL");
  } catch {
    // It has ended.
  }
}

/** A restore's images, made ready from t0 on: where they are, and the helpers that serve them. */
interface Images {
  readonly top: string;
  readonly helpers: readonly Helper[];
  /** Lets go of what serves them, once every page is loaded; again, nothing. */
  readonly release: () => Promise<void>;
  readonly details: Record<string, unknown>;
}

/** Links each of `chain`'s directories under `root` to the one before it, as CRIU's `parent` names it. */
function linkParents(root: string, chain: readonly string[]): void {
  chain.forEach((name, index) => {
    if (index > 0) symlinkSync(`../${chain[index - 1]}`, join(root, name, "parent"));
  });
}

/** The order of the stopping save's directories, as its manifest in `dir` says. */
const chainIn = (dir: string): string[] => (JSON.parse(readFileSync(join(dir, "chain-stop.json"), "utf8")) as { chain: string[] }).chain;

/** Warm: the directories the saves staged, linked to their parents as runc's checkpoints left them. */
function staged(run: Run, chain: readonly string[]): Images {
  return { top: join(run.stage, chain.at(-1) ?? ""), helpers: [], release: async () => {}, details: { source: "stage" } };
}

/**
 * Cold, lazy: the bucket mounted read-only by Mountpoint for Amazon S3 at the run's prefix, its
 * defaults otherwise (no cache); and beside it, since Mountpoint has no symlinks, a directory
 * per image directory of links to the mount's files, linked to their parents. CRIU reads every
 * image through the links: each page the lazy-pages daemon serves is read from the bucket when
 * it is asked for.
 */
async function mounted(run: Run, restoreDir: string): Promise<Images> {
  const mount = join(restoreDir, "bucket");
  const links = join(restoreDir, "images");
  mkdirSync(mount, { recursive: true });
  const served = helper("mount-s3", restoreDir, "mount-s3", [
    process.env["S3_BUCKET"] ?? "", mount, "--foreground", "--read-only", "--prefix", run.prefix,
    "--endpoint-url", process.env["S3_ENDPOINT"] ?? "", "--force-path-style", "--region", process.env["S3_REGION"] ?? "us-east-1",
    "--log-directory", join(restoreDir, "mount-s3-logs"),
  ]);
  let released: Promise<void> | null = null;
  const release = () => (released ??= (async () => {
    spawnSync("umount", [mount], { stdio: "ignore" });
    await within(served.ended, 60_000, "mount-s3 had not ended after its unmount");
  })());
  const deadline = Date.now() + 60_000;
  while (spawnSync("mountpoint", ["-q", mount]).status !== 0) {
    if (served.child.exitCode !== null || Date.now() > deadline) {
      kill(served);
      throw new Error(`mount-s3 did not mount the bucket: ${(await served.ended).stderr.trim().slice(-2000)}`);
    }
    await sleep(10);
  }
  const chain = chainIn(mount);
  for (const name of chain) {
    mkdirSync(join(links, name), { recursive: true });
    for (const file of readdirSync(join(mount, name))) symlinkSync(relative(join(links, name), join(mount, name, file)), join(links, name, file));
  }
  linkParents(links, chain);
  return { top: join(links, chain.at(-1) ?? ""), helpers: [served], release, details: { source: "mountpoint-s3", chain } };
}

/** Cold, eager: every object of the run's prefix downloaded (common's s3.mjs download), then the parents linked. */
async function downloaded(run: Run, restoreDir: string): Promise<Images> {
  const dir = join(restoreDir, "images");
  const download = helper("download", restoreDir, process.execPath, [s3, "download", run.prefix, dir]);
  const { status, stdout, stderr } = await download.ended;
  if (status !== 0) throw new Error(`the download failed: ${stderr.trim().slice(-2000)}`);
  const chain = chainIn(dir);
  linkParents(dir, chain);
  return { top: join(dir, chain.at(-1) ?? ""), helpers: [download], release: async () => {}, details: { source: "download", download: JSON.parse(stdout.trim()) as unknown, chain } };
}

/** What the probe prints, as far as a restore's row takes it. */
interface Probed {
  readonly first_exec_ms: number;
  readonly verify_ms: number;
  readonly ok: boolean;
  readonly failure: string | null;
  readonly correctness: Correctness;
  readonly workload_rss_bytes: number;
}

/**
 * One restore of the stopping save's image, measured as DESIGN.md section 5 says. From t0: the
 * probe started beside the rest; the images made ready (cold: mounted, or downloaded); the
 * lazy-pages daemon started (lazy); `runc restore`. Resident when the daemon has loaded every
 * page and ends (lazy), or when `runc restore` returns (eager: CRIU loads every page before the
 * tasks run on).
 */
async function restore(run: Run, variant: Variant, warmth: "warm" | "cold", chain: readonly string[], index: number): Promise<void> {
  const phase = `restore-${warmth}`;
  const restoreDir = join(run.dir, `restore-${index}`);
  const workDir = join(restoreDir, "work");
  mkdirSync(workDir, { recursive: true });
  const instance = bundleOf(run.attach);
  let images: Images | null = null;
  let daemon: Helper | null = null;
  const t0 = now();
  const probe = started(process.execPath, [driver, "probe", ...run.endpoint, "--session", run.session, "--fixture", run.fixture, "--run-id", run.runId, "--t0-ns", String(t0)]);
  try {
    images = warmth === "warm" ? staged(run, chain) : variant === "lazy" ? await mounted(run, restoreDir) : await downloaded(run, restoreDir);
    const ready = now();
    const args = ["restore", "--detach", "--bundle", instance.bundle, "--image-path", images.top, "--work-path", workDir, "--pid-file", join(instance.bundle, "pid")];
    if (variant === "lazy") {
      // The daemon and the restore meet at lazy-pages.socket in the work directory they share.
      const lazy = helper("lazy-pages", restoreDir, "criu", ["lazy-pages", "--images-dir", images.top, "--work-dir", workDir, "--log-file", "lazy-pages.log", "--status-fd", "3"], ["ignore", "pipe", "pipe", "pipe"]);
      daemon = lazy;
      await within(new Promise<void>((resolve, reject) => {
        lazy.child.stdio[3]?.once("data", () => resolve());
        lazy.ended.then(({ stderr }) => reject(new Error(`criu lazy-pages ended before it was ready: ${stderr.trim()}${criuErrors(join(workDir, "lazy-pages.log"))}`)), reject);
      }), 60_000, "criu lazy-pages was not ready");
      args.push("--lazy-pages");
    }
    try {
      runcAttached(instance, [...args, instance.id]);
    } catch (error) {
      throw new Error(`${(error as Error).message}${criuErrors(join(workDir, "restore.log"))}`);
    }
    const restored = now();
    const probed = await within(Promise.race([probe.ended, stopsBefore(instance, probe.ended)]), RESIDENT_TIMEOUT_MS, "the probe had not answered");
    if (probed.status !== 0) {
      const state = spawnSync("runc", ["--root", RUNC_ROOT, "state", instance.id], { encoding: "utf8" });
      const daemonSaid = daemon ? ` criu lazy-pages: ${daemon.child.exitCode === null ? "running" : `ended ${daemon.child.exitCode}`}.` : "";
      const kernel = spawnSync("dmesg", ["--ctime"], { encoding: "utf8" }).stdout.trim().split("\n").slice(-8).join(" | ");
      throw new Error(`the probe failed: ${probed.stderr.trim().slice(-4000)} (the instance: ${(state.stdout || state.stderr).replace(/\s+/gu, " ").trim()}.${daemonSaid} The kernel: ${kernel})`);
    }
    const said = JSON.parse(probed.stdout.trim().split("\n").at(-1) ?? "") as Probed;
    const helpers = [...(daemon ? [daemon] : []), ...images.helpers];
    const current = cgroupBytes(instance, "memory.current");
    const memory = current === null ? null : current + helpers.reduce((sum, each) => sum + rssUnder(each.child), 0);
    let resident = restored;
    if (daemon) {
      const ended = await within(daemon.ended, RESIDENT_TIMEOUT_MS, "criu lazy-pages had not loaded every page");
      resident = now();
      if (ended.status !== 0) throw new Error(`criu lazy-pages failed: ${ended.stderr.trim()}${criuErrors(join(workDir, "lazy-pages.log"))}`);
    }
    const peak = cgroupBytes(instance, "memory.peak");
    await images.release();
    const helperPeaks = Object.fromEntries(helpers.map(({ name, peakFile }) => [name, peakOf(peakFile)]));
    emit(run, phase, variant, said.ok, said.failure, {
      ...nulls(RESTORE_METRICS),
      first_exec_ms: said.first_exec_ms,
      verify_ms: said.verify_ms,
      resident_ms: msBetween(t0, resident),
      resident_how: "signal",
      memory_peak_bytes: peak === null ? null : peak + Object.values(helperPeaks).reduce<number>((sum, bytes) => sum + (bytes ?? 0), 0),
      memory_bytes: memory,
      workload_rss_bytes: said.workload_rss_bytes,
    }, said.correctness, { ...images.details, ready_ms: msBetween(t0, ready), runc_restore_ms: msBetween(ready, restored), cgroup_peak_bytes: peak, cgroup_current_bytes: current, helper_peak_bytes: helperPeaks, probe: said });
  } catch (error) {
    probe.child.kill("SIGKILL");
    emit(run, phase, variant, false, (error as Error).message, nulls(RESTORE_METRICS), UNCHECKED, {});
  } finally {
    end(instance);
    if (daemon) kill(daemon);
    await images?.release().catch(() => {});
    keepLogs(run, restoreDir);
    rmSync(restoreDir, { recursive: true, force: true });
  }
}

// ---------------------------------------------------------------------------------------------
// The job

/** The persistent files of the instance, as save-stop leaves them, and where they are put back from. */
const copyOf = (run: Run) => `${run.attach}.saved`;

function putBack(run: Run): void {
  rmSync(run.attach, { recursive: true, force: true });
  must("cp", ["-a", "--reflink=auto", copyOf(run), run.attach]);
}

/**
 * One run of a case (DESIGN.md section 5). Every phase of it is emitted: one that failed with
 * why, and each one after it, which then did not run, with the failure that stopped it.
 */
async function runCase(c: Case, number: number): Promise<void> {
  const run = prepare(c, number);
  const order: Variant[] = number % 2 === 1 ? ["lazy", "eager"] : ["eager", "lazy"];
  // The saves are the same for both variants and go under the first README.md names.
  const phases = [["save-live", "lazy"], ["save-stop", "lazy"], ...order.map((variant) => ["restore-warm", variant]), ...order.map((variant) => ["restore-cold", variant])];
  let instance: Instance | null = null;
  const failSave = (phase: string) => (error: unknown) => {
    emit(run, phase, "lazy", false, (error as Error).message, nulls(SAVE_METRICS), UNCHECKED, {});
    throw new Error(`${phase} failed`);
  };
  try {
    instance = await start(run);
    const set = await driven("setup", [...run.endpoint, "--fixture", run.fixture, "--size-mib", String(run.sizeMib), "--run-id", run.runId, "--rate-mib-s", String(host.copy_mib_s)]);
    run.session = String(set["session_id"]);
    await driven("mark", [...run.endpoint, "--session", run.session, "--fixture", run.fixture]);
    const live = await save(run, instance, "live", []).catch(failSave("save-live"));
    const gaps = await driven("gaps", [...run.endpoint, "--session", run.session, "--fixture", run.fixture]);
    emit(run, "save-live", "lazy", true, null, { ...live.metrics, unavailable_ms: gaps["max_gap_ms"] as number, slowdown: (gaps["slowdown"] as number | null) ?? null }, UNCHECKED, { ...live.details, gaps, setup: set });
    phases.shift();
    const stop = await save(run, instance, "stop", live.chain).catch(failSave("save-stop"));
    emit(run, "save-stop", "lazy", true, null, stop.metrics, UNCHECKED, stop.details);
    phases.shift();
    end(instance);
    instance = null;
    must("cp", ["-a", "--reflink=auto", run.attach, copyOf(run)]);
    let restores = 0;
    for (const [phase, variant] of [...phases]) {
      // Cold: no copy of the image on the runner but the bucket's.
      if (phase === "restore-cold") rmSync(run.stage, { recursive: true, force: true });
      if (restores > 0) putBack(run);
      if (phase === "restore-cold") dropCaches();
      await restore(run, variant as Variant, phase === "restore-warm" ? "warm" : "cold", stop.chain, ++restores);
      phases.shift();
    }
  } catch (error) {
    const reason = (error as Error).message;
    // A phase that failed has emitted its own row; the ones after it did not run.
    if (/^save-(live|stop) failed$/u.test(reason)) phases.shift();
    for (const [phase = "", variant = ""] of phases) emit(run, phase, variant, false, `not run: ${reason}`, nulls(phase.startsWith("save") ? SAVE_METRICS : RESTORE_METRICS), UNCHECKED, {});
  } finally {
    if (instance) end(instance);
    await remove(store, run.prefix).then(
      // The store's disk back, untimed (SeaweedFS frees a deleted object's space at a vacuum).
      () => fetch("http://127.0.0.1:9333/vol/vacuum?garbageThreshold=0.01"),
      (error: unknown) => console.error(`${run.runId}: the bucket's objects were not removed: ${String(error)}`),
    );
    keepLogs(run, run.dir);
    rmSync(run.dir, { recursive: true, force: true });
  }
}

/** The pause check's times and what the ticker saw, which every row carries. */
let pauseChecked: Record<string, number> = {};

/**
 * The job's check of the ticker against runc's own pause (DESIGN.md section 3): an idle fixture
 * paused for 1000 ms must report a gap no shorter than from `runc pause` returning to `runc
 * resume` starting, nor longer than from the pause starting to the resume returning, 20 ms
 * slack either side; or the job reports nothing.
 */
async function pauseCheck(): Promise<void> {
  const run = prepare({ fixture: "idle", sizeMib: 16 }, 0);
  const instance = await start(run);
  try {
    const set = await driven("setup", [...run.endpoint, "--fixture", "idle", "--size-mib", "16", "--run-id", run.runId]);
    const session = String(set["session_id"]);
    await driven("mark", [...run.endpoint, "--session", session, "--fixture", "idle"]);
    const pauseAsked = now();
    runc(["pause", instance.id]);
    const paused = now();
    await sleep(1000);
    const resumeAsked = now();
    runc(["resume", instance.id]);
    const resumed = now();
    const gaps = await driven("gaps", [...run.endpoint, "--session", session, "--fixture", "idle"]);
    const gap = gaps["max_gap_ms"] as number;
    const shortest = msBetween(paused, resumeAsked) - 20;
    const longest = msBetween(pauseAsked, resumed) + 20;
    pauseChecked = { ticker_gap_ms: gap, shortest_ms: shortest, longest_ms: longest, pause_asked_ns: Number(pauseAsked), paused_ns: Number(paused), resume_asked_ns: Number(resumeAsked), resumed_ns: Number(resumed) };
    writeFileSync(join(logs, "pause-check.json"), `${JSON.stringify(pauseChecked)}\n`);
    console.log(`pause check: runc paused the instance for ${shortest + 20} to ${longest - 20} ms, the ticker saw ${gap} ms`);
    if (!(gap >= shortest && gap <= longest)) throw new Error(`the ticker saw ${gap} ms of a pause of ${shortest + 20} to ${longest - 20} ms`);
  } finally {
    end(instance);
    rmSync(run.dir, { recursive: true, force: true });
  }
}

const cases: Case[] = options.cases
  ? options.cases.split(/\s+/u).filter(Boolean).map((word) => {
    const [fixture = "", size = ""] = word.split("/");
    return { fixture, sizeMib: Number(size) };
  })
  : (JSON.parse(readFileSync(join(here, "../common/cases.json"), "utf8")) as { fixture: string; size_mib: number }[]).map(({ fixture, size_mib }) => ({ fixture, sizeMib: size_mib }));
mkdirSync(dirname(out), { recursive: true });
try {
  await pauseCheck();
  for (let number = 1; number <= Number(options.runs); number++) for (const c of cases) await runCase(c, number);
} finally {
  for (const name of ["instances.log", "runc.log"]) if (existsSync(join(work, name))) copyFileSync(join(work, name), join(logs, name));
}
