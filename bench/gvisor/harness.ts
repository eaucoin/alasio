/**
 * The gvisor system's runs: upstream gVisor's runsc, from an OCI bundle of the instance image,
 * saved with `runsc checkpoint` (live: `--leave-running`) to the runner's disk and uploaded to
 * the bucket, restored with `runsc restore --background`: warm from the image directory the
 * save staged, cold from the bucket, either downloaded whole first (`fetch`) or read through
 * an rclone FUSE mount of the bucket as the sandbox runs, the pages file preloaded behind it
 * (`lazy`). README.md says why each setting is what it is. The `traced` setups run the fork's
 * measurement build instead, which install.sh installs where it is given one (`traced`).
 *
 *   node harness.ts <profile>
 *
 * Run as root, by run.sh, in the job ci.yml's bench job sets up: its directory BENCH_DIR,
 * holding host.json and store.json, which every line carries, and the store's variables in the
 * environment (common/store/up.sh); BENCH_CASES (fixture/MiB, all of common/cases.json if
 * empty) and BENCH_RUNS. Writes one line per run and phase to BENCH_DIR/results.jsonl, and
 * each run's runsc and rclone logs under BENCH_DIR/logs.
 */
import { spawn, type ChildProcess } from "node:child_process";
import { randomBytes } from "node:crypto";
import { appendFileSync, closeSync, existsSync, mkdirSync, openSync, readdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { connect } from "node:net";
import { join } from "node:path";

import { config, HOOK } from "../common/oci/config.ts";

const here = new URL(".", import.meta.url).pathname;
const DRIVER = join(here, "../common/dist/driver.mjs");
const S3 = join(here, "../common/dist/s3.mjs");
/**
 * A cold `fetch` restore's download, on its critical path: 16 MiB ranges, 16 at once. AWS's S3
 * performance whitepaper asks one connection for each 85-90 MB/s wanted, in 8-16 MB ranges;
 * gVisor's own remote reader reads 16 MiB ranges, up to 160 at once (runsc/checkpointgofer/gcs).
 */
const DOWNLOAD = ["--at-once", "16", "--range-bytes", String(16 << 20)];

const RUNSC = "/usr/local/bin/runsc";
const RCLONE = "/usr/local/bin/rclone";
export const RCLONE_VERSION = "v1.75.2";
const IMAGE = "ghcr.io/eaucoin/alasio-agent:10.0.0@sha256:b90f8164f77e9a56bd0da0fc1869bc76c1c541742a07751764ffc156fa8fe30a";
/** The session image of alasio 10.0.5 (bayma 0.15.6), which the traced setups run, as the box does. */
const IMAGE_10_0_5 = "ghcr.io/eaucoin/alasio-agent:10.0.5@sha256:b870b3981fe519b1ae199f628f939c5ee98eabb18adcc0da55fe35058e933949";

/** runsc's state for the job's sandboxes. */
const ROOT = "/run/gvbench";
const BENCH_DIR = process.env["BENCH_DIR"] ?? "";
/** Where images are staged, runs' files kept and the rootfs unpacked: the disk host.sh measured. */
const WORK = join(BENCH_DIR, "staging", "gvisor");
const ROOTFS = join(WORK, "rootfs");

/** The instance's network: a namespace of its own with one veth, the same addresses on every restore. */
const NETNS = "gvb";
const HOST_IP = "10.88.0.1";
const SANDBOX_IP = "10.88.0.2";
const PORT = 8765;
const URL_ = `http://${SANDBOX_IP}:${PORT}/mcp`;



const now = (): bigint => process.hrtime.bigint();
const msBetween = (from: bigint, to: bigint): number => Number(to - from) / 1e6;
const sleep = (wait: number) => new Promise<void>((resolve) => setTimeout(resolve, wait));

// ---------------------------------------------------------------- processes

/** How long any one command may take: a run whose command hangs fails, and the job goes on. */
const COMMAND_TIMEOUT_MS = 600_000;

interface Ran {
  readonly code: number | null;
  readonly stdout: string;
  readonly stderr: string;
  readonly timedOut: boolean;
}

/** A child that runs in the background, whose end can be awaited; killed if it outlasts `timeoutMs`. */
interface Background {
  readonly child: ChildProcess;
  readonly done: Promise<Ran>;
  readonly timeoutMs: number;
}

interface Spawned {
  readonly env?: NodeJS.ProcessEnv;
  readonly stdoutFd?: number;
  readonly timeoutMs?: number;
}

function background(file: string, args: readonly string[], { env = process.env, stdoutFd, timeoutMs = COMMAND_TIMEOUT_MS }: Spawned = {}): Background {
  const child = spawn(file, args, { env, stdio: ["ignore", stdoutFd ?? "pipe", stdoutFd ?? "pipe"] });
  let stdout = "";
  let stderr = "";
  let timedOut = false;
  child.stdout?.on("data", (chunk: Buffer) => { stdout += chunk.toString(); });
  child.stderr?.on("data", (chunk: Buffer) => { stderr += chunk.toString(); });
  const timer = setTimeout(() => {
    timedOut = true;
    child.kill("SIGKILL");
  }, timeoutMs);
  const done = new Promise<Ran>((resolve, reject) => {
    child.on("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
    child.on("close", (code) => {
      clearTimeout(timer);
      resolve({ code, stdout, stderr, timedOut });
    });
  });
  return { child, done, timeoutMs };
}

/** What `bg` ran, once it has ended well; `what` names it in the error if it did not. */
async function ok(bg: Background, what: string): Promise<Ran> {
  const ran = await bg.done;
  if (ran.timedOut) throw new Error(`${what} did not end in ${bg.timeoutMs / 1000} s: ${(ran.stderr || ran.stdout).trim().slice(-2000)}`);
  if (ran.code !== 0) throw new Error(`${what} exited ${ran.code}: ${(ran.stderr || ran.stdout).trim().slice(-4000)}`);
  return ran;
}

/** Runs `file` to its end; throws if it fails, unless `allowFail`. */
function run(file: string, args: readonly string[], options: Spawned & { readonly allowFail?: boolean; readonly what?: string } = {}): Promise<Ran> {
  const bg = background(file, args, options);
  return options.allowFail ? bg.done : ok(bg, options.what ?? `${file} ${args.join(" ")}`);
}

const json = async <T>(ran: Promise<Ran>): Promise<T> => JSON.parse((await ran).stdout.trim().split("\n").at(-1) ?? "") as T;

/** The largest VmHWM each of `pids` reaches while sampled, every 50 ms, until stopped. */
function peakSampler(pids: () => readonly number[]) {
  let peak = 0;
  const sample = () => {
    let sum = 0;
    for (const pid of pids()) {
      try {
        const kib = /VmHWM:\s+(\d+) kB/u.exec(readFileSync(`/proc/${pid}/status`, "utf8"))?.[1];
        sum += Number(kib ?? 0) * 1024;
      } catch {
        // Gone already: its last sample stands.
      }
    }
    peak = Math.max(peak, sum);
  };
  const timer = setInterval(sample, 50);
  return { stop: () => { sample(); clearInterval(timer); return peak; } };
}

// ---------------------------------------------------------------- the driver and the store

const token = randomBytes(32).toString("hex");
const endpointArgs = ["--url", URL_, "--header", `Authorization: Bearer ${token}`];
const driver = <T>(command: string, args: readonly string[]): Promise<T> => json<T>(run(process.execPath, [DRIVER, command, ...endpointArgs, ...args], { what: `the driver's ${command}` }));
const s3 = <T>(args: readonly string[]): Promise<T> => json<T>(run(process.execPath, [S3, ...args]));

/** The bytes of the objects under `prefix`. */
const bucketBytes = async (prefix: string): Promise<number> => (await s3<{ bytes: number }>(["bytes", prefix])).bytes;

// ---------------------------------------------------------------- runsc

/**
 * How a run's sandbox is saved and restored. `TUNED` is gvisor's configuration, every
 * optimization gvisor.dev documents for this workload (README.md); `DEFAULTS` is runsc and the
 * runner as they come, beside it.
 */
interface Setup {
  /** What the run's files and logs are named by. */
  readonly name: string;
  /** The variant its saves, its warm restore and its fetched cold restore carry. */
  readonly variant: string;
  /** runsc's `--host-settings`: `adjust` sets the host's memory settings as gVisor recommends. */
  readonly hostSettings: "adjust" | "check";
  /** `runsc checkpoint`'s flags, but its image path and `--leave-running`. */
  readonly checkpoint: readonly string[];
  /** Whether `runsc restore` runs the sandbox before its memory is loaded (`--background`). */
  readonly background: boolean;
  /** Whether the warm restore reads the staged pages file past the page cache (`--direct`). */
  readonly warmDirect: boolean;
  /** A cold restore through rclone's mount of the bucket as well, of this variant, with these `rclone mount` flags. */
  readonly lazy: { readonly variant: string; readonly mount: readonly string[] } | null;
  /** runsc's flags for every command, besides its root, its logs and its host settings. */
  readonly flags: readonly string[];
  /** The session image whose root filesystem the instance runs. */
  readonly image: string;
  /** The bayma that image holds. */
  readonly bayma: string;
  /** Whether a run saves live before it saves and stops (save-live). */
  readonly live: boolean;
  /**
   * The warm restores of each run, in order, and the variant each carries: with the page cache
   * dropped, kept, or holding all but the image's files (`evicted`, the endpoint package's G-2).
   */
  readonly warm: readonly { readonly variant: string; readonly cache: "dropped" | "kept" | "evicted" }[];
  /** Whether a run restores cold from the bucket downloaded whole (`fetch`). */
  readonly fetch: boolean;
  /** runsc's flags for its restores alone, which the sandboxes they start run with. */
  readonly restoreFlags: readonly string[];
  /** Whether the host's disk and pressure are sampled every 200 ms through each restore. */
  readonly sampleIO: boolean;
  /**
   * Whether the sandbox of the first warm restore is saved again once its first exec has
   * answered, and that image restored (variant `-again`): the working set the first restore
   * recorded is read first (the endpoint package's G-14).
   */
  readonly again: boolean;
  /** How long the probe waits between the first exec and the verify, which reads every page. */
  readonly verifyAfterMs: number;
  /** Variables the instance's processes run with, as `NAME=value`, beside the spec's. */
  readonly env: readonly string[];
  /** node's flags for bayma's server. */
  readonly nodeFlags: readonly string[];
  /**
   * The exec run in the fixture's session before it is saved and stopped, by runtime, as a hook's
   * hygiene before a save would run it (the endpoint package's X-HYG), or null for none.
   */
  readonly hygiene: { readonly bun: string; readonly python: string } | null;
  /**
   * The idle times, in seconds, after which the instance, saved live once, is saved live again,
   * each save incremental over the one before (the endpoint package's X-IDLE).
   */
  readonly idleSaves: readonly number[];
  /** Whether V8's heap statistics in a node the instance starts are recorded (X-SRV). */
  readonly heapStats: boolean;
  /**
   * Before which saves bayma's server runs a last-resort full collection, timed (the endpoint
   * package's X-SRV): none, each, or the release alone (the save that stops it); its nodeFlags
   * must load SERVER_GC, which runs it on SIGUSR2.
   */
  readonly serverGc: "none" | "each" | "release";
}

const TUNED: Setup = {
  name: "tuned",
  variant: "fetch",
  hostSettings: "adjust",
  // Written through the page cache, which the upload then reads: README.md, "Tuning".
  checkpoint: ["--compression=none", "--exclude-committed-zero-pages"],
  background: true,
  warmDirect: true,
  lazy: {
    variant: "lazy",
    // rclone's own chunked reading, which beat its suggested 16 streams of 4 MiB: README.md, "Tuning".
    mount: ["--read-only", "--vfs-cache-mode", "full"],
  },
  flags: [],
  image: IMAGE,
  bayma: "0.15.2",
  live: true,
  warm: [{ variant: "fetch", cache: "dropped" }],
  fetch: true,
  restoreFlags: [],
  sampleIO: false,
  again: false,
  verifyAfterMs: 0,
  env: [],
  nodeFlags: [],
  hygiene: null,
  idleSaves: [],
  heapStats: false,
  serverGc: "none",
};

const DEFAULTS: Setup = { ...TUNED, name: "defaults", variant: "defaults", hostSettings: "check", checkpoint: [], background: false, warmDirect: false, lazy: null, warm: [{ variant: "defaults", cache: "dropped" }] };

/**
 * The fork's measurement build (install.sh given its release's directory; the endpoint package's
 * G-3): runsc as alasio 10.0.5 configures it ([runsc_config] in cli/src/cluster/k3s.ts), the
 * working set recorded for 60 s rather than 3 s so that it covers the first exec and the verify,
 * and every await of pages logged (--async-awaits-trace=log); restored in the background but not
 * paused, so that the application runs while its pages load, warm with the page cache dropped
 * and then kept, and cold through rclone's mount. `uncached` also maps large pmas' accessed ranges
 * alone while they load, as upstream's c63a70c947 does.
 */
function traced(variant: string, { uncached = false, window = "60s", trace = true } = {}): Setup {
  return {
    ...TUNED,
    name: variant,
    variant,
    lazy: TUNED.lazy && { ...TUNED.lazy, variant: `${variant}-lazy` },
    flags: [
      "--restore-validate-files=all",
      "--dirty-tracking=wp",
      `--working-set-window=${window}`,
      "--allow-live-tcp-migration=false",
      "--app-huge-pages=false",
      ...(trace ? ["--async-awaits-trace=log"] : []),
      ...(uncached ? ["--async-load-internal-mappings=uncached"] : []),
    ],
    image: IMAGE_10_0_5,
    bayma: "0.15.6",
    live: false,
    warm: [{ variant, cache: "dropped" }, { variant: `${variant}-kept`, cache: "kept" }],
    fetch: false,
  };
}

/** The instance's file, under its /attach, that runs a server's collection on SIGUSR2. */
const SERVER_GC = "/attach/server-gc.cjs";

/** The script at SERVER_GC: a timed last-resort full collection on SIGUSR2, said on stderr. */
const SERVER_GC_SCRIPT = `"use strict";
// The bench's (the endpoint package's X-SRV): on SIGUSR2, a last-resort full collection, timed.
const v8 = require("node:v8");
process.on("SIGUSR2", () => {
  const before = v8.getHeapStatistics();
  const start = process.hrtime.bigint();
  globalThis.gc({ type: "major", execution: "sync", flavor: "last-resort" });
  const ms = Number(process.hrtime.bigint() - start) / 1e6;
  const after = v8.getHeapStatistics();
  process.stderr.write("BENCH-SERVER-GC " + JSON.stringify({ ms, used_before: before.used_heap_size, used_after: after.used_heap_size, total_before: before.total_heap_size, total_after: after.total_heap_size, physical_before: before.total_physical_size, physical_after: after.total_physical_size }) + "\\n");
});
`;

/** The server's flags for SERVER_GC: its memory reducer off, gc() exposed, the script loaded. */
const SERVER_GC_FLAGS = ["--no-memory-reducer", "--expose-gc", `--require=${SERVER_GC}`];

/** X-IDLE's source and X-SRV's setups: idle saves traced, then a warm restore, `serverFlags` added. */
function idleTraced(variant: string, serverFlags: readonly string[], serverGc: Setup["serverGc"]): Setup {
  const base = traced(variant, { trace: false });
  return {
    ...base,
    flags: [...base.flags, "--dirty-trace=log"],
    idleSaves: [5, 30, 120],
    warm: [{ variant, cache: "dropped" }],
    lazy: null,
    nodeFlags: ["--trace-gc", "--trace-memory-reducer", ...serverFlags],
    serverGc,
  };
}

/** The job's setups, by the variant its job runs. */
const SETUPS: Record<string, readonly Setup[]> = {
  tuned: [TUNED],
  defaults: [DEFAULTS],
  traced: [traced("traced")],
  "traced-uncached": [traced("traced-uncached", { uncached: true })],
  // G-4: the restored sandbox's system calls, the hook's among them, logged with their times.
  "traced-strace": [{ ...traced("traced-strace"), lazy: null, restoreFlags: ["--strace"] }],
  // G-14: the working set of the first exec alone (3 s, the probe's verify after it), saved with
  // the sandbox again and read first by the restore of that image.
  "traced-again": [{ ...traced("traced-again", { window: "3s" }), lazy: null, warm: [{ variant: "traced-again", cache: "dropped" }], again: true, verifyAfterMs: 4000 }],
  // X-FIRST and X-SRV: JavaScriptCore's collections logged (in the first exec's stderr), the
  // server's V8 collections and memory reducer logged (in the restore's output), the heap's
  // limits a node sees in the sandbox; Python's collections the fixture records itself.
  "traced-gc": [{ ...traced("traced-gc"), lazy: null, env: ["BUN_JSC_logGC=1", "BENCH_HEAP_DETECTOR=1"], nodeFlags: ["--trace-gc", "--trace-memory-reducer"], heapStats: true }],
  // X-IDLE: a session left idle, saved live, then live and incrementally after 5, 30 and 120 s,
  // as it comes and with Bun's GC timer and mimalloc's purge of holes off.
  idle: [
    { ...traced("idle", { trace: false }), idleSaves: [5, 30, 120], warm: [], lazy: null },
    { ...traced("idle-quiet", { trace: false }), idleSaves: [5, 30, 120], warm: [], lazy: null, env: ["BUN_GC_TIMER_DISABLE=1", "MIMALLOC_PURGE_HOLES=0"] },
  ],
  // X-IDLE's source and X-SRV: the idle saves with what each save's pages hold, by memory kind and
  // marking path, and what each process wrote, by vma (the fork's --dirty-trace=log), the server's
  // collections and memory reducer logged; as bayma runs today, and with the server's memory
  // reducer off and a last-resort full collection just before each save; then restored warm.
  "idle-traced": [idleTraced("idle-traced", [], "none"), idleTraced("idle-srv-gc", SERVER_GC_FLAGS, "each"), idleTraced("idle-no-reducer", ["--no-memory-reducer"], "none")],
  // X-SRV at a release: a session saved and stopped just after its setup, as bayma runs today,
  // with the server's memory reducer off, and with it off and a last-resort full collection just
  // before the save; then restored warm, its server's collections logged.
  "srv-release": [
    { ...traced("srv-today", { trace: false }), lazy: null, nodeFlags: ["--trace-gc", "--trace-memory-reducer"] },
    { ...traced("srv-no-reducer", { trace: false }), lazy: null, nodeFlags: ["--trace-gc", "--trace-memory-reducer", "--no-memory-reducer"] },
    { ...traced("srv-gc", { trace: false }), lazy: null, nodeFlags: ["--trace-gc", "--trace-memory-reducer", ...SERVER_GC_FLAGS], serverGc: "release" },
  ],
  // G-3 at 200 MiB: what the Bun runtime reads before its first answer, as it comes, with the
  // fixture's heap detector walking the heap at each exec, with Bun's GC timers off, and after a
  // collection in an exec just before the save.
  "bun-reads": [
    { ...traced("bun-plain"), lazy: null },
    { ...traced("bun-detector"), lazy: null, env: ["BENCH_HEAP_DETECTOR=1"] },
    { ...traced("bun-quiet"), lazy: null, env: ["BUN_GC_TIMER_DISABLE=1"] },
    { ...traced("bun-gc"), lazy: null, hygiene: { bun: "Bun.gc(true)", python: "None" } },
  ],
  // X-HYG: the image a session saves as it comes, with its allocators asked to return memory at
  // once, and after a collection (and Bun's shrink of its heap) in an exec just before the save.
  hygiene: [
    { ...traced("hygiene-none", { trace: false }), warm: [], lazy: null },
    { ...traced("hygiene-env", { trace: false }), warm: [], lazy: null, env: ["MIMALLOC_PURGE_DELAY=0", "PYTHONMALLOC=malloc"] },
    // An exec that does nothing, the others' time and effect apart from what they ask.
    { ...traced("hygiene-noop", { trace: false }), warm: [], lazy: null, hygiene: { bun: "undefined", python: "None" } },
    { ...traced("hygiene-gc", { trace: false }), warm: [], lazy: null, hygiene: { bun: "Bun.gc(true)", python: "import gc\ngc.collect()" } },
    { ...traced("hygiene-gc-trim", { trace: false }), warm: [], lazy: null, hygiene: { bun: "Bun.gc(true)\nBun.shrink()", python: "import ctypes, gc\ngc.collect()\nctypes.CDLL(\"libc.so.6\").malloc_trim(0)" } },
  ],
  // G-2: upstream's warm restore with the page cache dropped, holding all but the image, and kept,
  // the disk and pressure sampled through each.
  evicted: [{ ...TUNED, name: "evicted", variant: "evicted", live: false, fetch: false, lazy: null, sampleIO: true, warm: [{ variant: "evicted-dropped", cache: "dropped" }, { variant: "evicted", cache: "evicted" }, { variant: "evicted-kept", cache: "kept" }] }],
};

/** The host's settings `--host-settings=adjust` changes (runsc/hostsettings), as they are now. */
const HOST_SETTINGS = ["/sys/kernel/mm/transparent_hugepage/shmem_enabled", "/sys/kernel/mm/transparent_hugepage/defrag", "/proc/sys/vm/max_map_count"];

function hostSettings(): Record<string, string> {
  // A THP file names its choices and brackets the one made: "always [never] advise".
  return Object.fromEntries(HOST_SETTINGS.map((path) => {
    const said = readFileSync(path, "utf8").trim();
    return [path, /\[(\S+)\]/u.exec(said)?.[1] ?? said];
  }));
}

function putHostSettings(values: Record<string, string>): void {
  for (const [path, value] of Object.entries(values)) writeFileSync(path, value);
}

/** runsc's flags for every command of a sandbox: its root, its logs in `logDir`, its host settings. */
const runscFlags = (setup: Setup, logDir: string) => [`--root=${ROOT}`, `--debug-log=${logDir}/`, `--host-settings=${setup.hostSettings}`, ...setup.flags];

const runsc = (setup: Setup, logDir: string, args: readonly string[], allowFail = false) => run(RUNSC, [...runscFlags(setup, logDir), ...args], { allowFail });

/**
 * A runsc command that starts a sandbox, `run` or `restore`: the sandbox's own output, which
 * it holds open as long as it runs, goes to the file `out`, so the command's end is its exit.
 */
async function sandboxCommand(setup: Setup, logDir: string, out: string, args: readonly string[]): Promise<void> {
  const fd = openSync(out, "a");
  try {
    await run(RUNSC, [...runscFlags(setup, logDir), ...args], { stdoutFd: fd });
  } catch (error) {
    throw new Error(`${error instanceof Error ? error.message : String(error)}: ${readFileSync(out, "utf8").trim().slice(-2000)}`);
  } finally {
    closeSync(fd);
  }
}

async function netUp(): Promise<void> {
  await run("ip", ["netns", "add", NETNS]);
  await run("ip", ["link", "add", "gvb-host", "type", "veth", "peer", "name", "eth0", "netns", NETNS]);
  await run("ip", ["addr", "add", `${HOST_IP}/30`, "dev", "gvb-host"]);
  await run("ip", ["link", "set", "gvb-host", "up"]);
  await run("ip", ["-n", NETNS, "addr", "add", `${SANDBOX_IP}/30`, "dev", "eth0"]);
  await run("ip", ["-n", NETNS, "link", "set", "eth0", "up"]);
  await run("ip", ["-n", NETNS, "link", "set", "lo", "up"]);
  await run("ip", ["-n", NETNS, "route", "add", "default", "via", HOST_IP]);
}

async function netDown(): Promise<void> {
  await run("ip", ["netns", "del", NETNS], { allowFail: true });
  await run("ip", ["link", "del", "gvb-host"], { allowFail: true });
}

/** Unpacks the instance image into ROOTFS, once per job. */
async function prepareRootfs(image: string): Promise<void> {
  await run("docker", ["pull", "-q", image]);
  const container = (await run("docker", ["create", image])).stdout.trim();
  mkdirSync(ROOTFS, { recursive: true });
  await run("bash", ["-c", `docker export ${container} | tar -x -C ${ROOTFS}`]);
  await run("docker", ["rm", container]);
}

/** The bundle of an instance whose /attach is `attach`, in `dir`: the runner systems' spec, its cgroup named, the setup's environment and server's node flags. */
function writeBundle(dir: string, id: string, attach: string, setup: Setup): void {
  mkdirSync(dir, { recursive: true });
  writeFileSync(join(dir, "config.json"), JSON.stringify(config({ rootfs: ROOTFS, attach, netns: `/var/run/netns/${NETNS}`, port: PORT, cgroupsPath: `/gvbench/${id}`, env: setup.env, nodeFlags: setup.nodeFlags }), null, 2));
}

const cgroupDir = (id: string) => `/sys/fs/cgroup/gvbench/${id}`;
const cgroupRead = (id: string, file: string): number => Number(readFileSync(join(cgroupDir(id), file), "utf8").trim());

/** Waits until something accepts connections on the sandbox's port. */
async function portUp(timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const up = await new Promise<boolean>((resolve) => {
      const socket = connect({ host: SANDBOX_IP, port: PORT });
      socket.once("connect", () => { socket.destroy(); resolve(true); });
      socket.once("error", () => { socket.destroy(); resolve(false); });
    });
    if (up) return;
    if (Date.now() > deadline) throw new Error(`nothing listens on ${SANDBOX_IP}:${PORT} after ${timeoutMs} ms`);
    await sleep(50);
  }
}

/** Ends an instance without saving it. */
async function endInstance(setup: Setup, logDir: string, id: string): Promise<void> {
  await runsc(setup, logDir, ["kill", id, "KILL"], true);
  await runsc(setup, logDir, ["delete", "--force", id], true);
}

/** The Sentry's log of the sandbox whose commands logged into `logDir`. */
function bootLog(logDir: string): string {
  const name = readdirSync(logDir).filter((each) => each.endsWith(".boot.txt")).sort().at(-1);
  if (name === undefined) throw new Error(`no boot log in ${logDir}`);
  return readFileSync(join(logDir, name), "utf8");
}

/** The time of a glog line ("I1026 19:45:01.123456 ..."), in microseconds of its day. */
function lineMicros(line: string): number {
  const found = /^[IWEF]\d{4} (\d{2}):(\d{2}):(\d{2})\.(\d{6})/u.exec(line);
  if (!found) throw new Error(`not a log line: ${line}`);
  const [, h, m, s, us] = found.map(Number) as [number, number, number, number, number];
  return ((h * 60 + m) * 60 + s) * 1e6 + us;
}

/**
 * The `nth` save's pause in the Sentry's log: from "Sandbox save started, pausing all tasks."
 * to "Tasks resumed after save" (a save that leaves the sandbox running; "." after it upstream,
 * ", paused for <d>." in the fork) or "Save succeeded" (one that ends it).
 */
function savePause(log: string, nth: number, live: boolean): { readonly ms: number; readonly lines: readonly string[] } {
  const lines = log.split("\n");
  const starts = lines.flatMap((line, index) => (line.includes("Sandbox save started, pausing all tasks.") ? [index] : []));
  const start = starts[nth];
  if (start === undefined) throw new Error(`the Sentry's log has no save ${nth + 1}`);
  const endText = live ? "Tasks resumed after save" : "Save succeeded";
  const end = lines.findIndex((line, index) => index > start && line.includes(endText));
  if (end === -1) throw new Error(`the Sentry's log has no "${endText}" after save ${nth + 1}`);
  const between = lines.slice(start, end + 1).filter((line) => /save|saved|Save/u.test(line));
  let us = lineMicros(lines[end] ?? "") - lineMicros(lines[start] ?? "");
  if (us < 0) us += 86_400e6;
  return { ms: us / 1e3, lines: between };
}

/** The Sentry's last warnings and errors, for a failure's message. */
function sentryWarnings(logDir: string): string {
  try {
    return bootLog(logDir).split("\n").filter((line) => /^[WEF]\d{4} /u.test(line)).slice(-5).join(" | ");
  } catch (error) {
    return error instanceof Error ? error.message : String(error);
  }
}

/**
 * How long a restore has, from t0, for its first exec to answer and its pages to be resident
 * (DESIGN.md section 5); and how much longer its probe may take for the verify exec.
 */
const RESTORE_DEADLINE_MS = 300_000;
const VERIFY_ALLOWANCE_MS = 120_000;

/** How long a probe runs before the harness records what the sandbox is doing. */
const SLOW_PROBE_MS = 120_000;

/** Records in `out` the sandbox's processes, their states, and its TCP sockets, as the sandbox sees them. */
async function describeSandbox(setup: Setup, logDir: string, id: string, out: string): Promise<void> {
  const script = 'for d in /proc/[0-9]*; do echo "${d#/proc/} $(cat $d/comm) $(grep -E "^(State|Threads)" $d/status | tr "\\n" " ")"; done; cat /proc/net/tcp';
  const ran = await runsc(setup, logDir, ["exec", id, "/bin/sh", "-c", script], true);
  writeFileSync(out, `${ran.stdout}${ran.stderr}`);
}

/** What the restored Sentry logged of loading its memory. */
const loadLines = (log: string): string[] => log.split("\n").filter((line) => /MemoryFile.*loaded|Async page loading completed|All MemoryFile/u.test(line));

// ---------------------------------------------------------------- results

interface Common {
  readonly host: Record<string, unknown>;
  readonly store: Record<string, unknown>;
  readonly profile: string;
  readonly emulated: boolean;
  /** The gVisor the job runs, as its lines name it. */
  readonly system: string;
}

type Row = Record<string, unknown>;

const NULL_SAVE = { save_total_ms: null, unavailable_ms: null, pause_reported_ms: null, slowdown: null, precopy_passes: null, precopy_bytes: null, image_bytes: null, uploaded_bytes: null };
const NULL_RESTORE = { first_exec_ms: null, verify_ms: null, resident_ms: null, resident_how: null, memory_peak_bytes: null, memory_bytes: null, workload_rss_bytes: null };
const NULL_CORRECTNESS = { marker: null, memory: null, children: null, file: null, socket: null, running: null, notice: null };

function rowOf(common: Common, setup: Setup, at: { fixture: string; size: number; run: number }, variant: string, phase: string, result: { ok: boolean; failure: string | null; metrics: Row; correctness?: Row; details?: Row }, mounted = false): Row {
  const restore = phase.startsWith("restore");
  return {
    schema: 1,
    system: "gvisor",
    variant,
    target: "host",
    fixture: at.fixture,
    size_mib: at.size,
    profile: common.profile,
    emulated: common.emulated,
    run: at.run,
    phase,
    ok: result.ok,
    failure: result.failure,
    metrics: { ...(restore ? NULL_RESTORE : NULL_SAVE), ...result.metrics },
    correctness: { ...NULL_CORRECTNESS, ...result.correctness },
    host: common.host,
    store: common.store,
    versions: { system: `${common.system}${mounted ? `, rclone ${RCLONE_VERSION}` : ""}`, bayma: setup.bayma, seaweedfs: "4.48", toxiproxy: "2.13.0" },
    ci: { run_id: Number(process.env["GITHUB_RUN_ID"] ?? 0), job: process.env["GITHUB_JOB"] ?? "", sha: process.env["GITHUB_SHA"] ?? "" },
    at: new Date().toISOString(),
    details: result.details ?? {},
  };
}

const failed = (error: unknown) => ({ ok: false, failure: error instanceof Error ? error.message : String(error), metrics: {} });

// ---------------------------------------------------------------- a run

interface Paths {
  readonly id: string;
  readonly dir: string;
  /** Where its runsc and rclone logs go, which the job uploads. */
  readonly logs: string;
  readonly attach: string;
  readonly attachSaved: string;
  readonly bundle: string;
  readonly live: string;
  readonly stop: string;
  readonly prefix: string;
}

function pathsOf(profile: string, setup: Setup, fixture: string, size: number, runNo: number): Paths {
  const id = `gvb-${setup.name}-${fixture}-${size}-r${runNo}`;
  const dir = join(WORK, "runs", id);
  return {
    id,
    dir,
    logs: join(BENCH_DIR, "logs", id),
    attach: join(dir, "attach"),
    attachSaved: join(dir, "attach.saved"),
    bundle: join(dir, "bundle"),
    live: join(dir, "image-live"),
    stop: join(dir, "image-stop"),
    prefix: `gvisor/${profile}/${id}/`,
  };
}

/** The run's /attach, as alasio's node agent attaches a workspace: its token, an empty env, the workspace and home. */
async function makeAttach(attach: string): Promise<void> {
  mkdirSync(join(attach, "ws", "workspace"), { recursive: true });
  mkdirSync(join(attach, "ws", "home"), { recursive: true });
  writeFileSync(join(attach, "env"), "");
  writeFileSync(join(attach, "token"), token);
  writeFileSync(join(attach, SERVER_GC.slice("/attach/".length)), SERVER_GC_SCRIPT);
  await run("chown", ["-R", "1000:1000", attach]);
}

const dirBytes = (dir: string): number => readdirSync(dir).reduce((sum, name) => sum + statSync(join(dir, name)).size, 0);
const dirFiles = (dir: string) => readdirSync(dir).map((name) => ({ name, size: statSync(join(dir, name)).size }));

/** Drops the page cache, as every restore starts with none of the image in it. */
async function dropCaches(): Promise<void> {
  await run("sync", []);
  writeFileSync("/proc/sys/vm/drop_caches", "1");
}

/** Drops the page cache of the image's files alone, as a node evicts an image it no longer runs. */
async function evictImage(image: string): Promise<void> {
  await run("sync", []);
  for (const name of readdirSync(image)) await run("dd", [`if=${join(image, name)}`, "iflag=nocache", "count=0", "status=none"]);
}

/**
 * Samples the host's disks' counters (/proc/diskstats) and its I/O and CPU pressure
 * (/proc/pressure) every 200 ms into `out`, each sample's lines after its time on the wall clock,
 * until stopped.
 */
function ioSampler(out: string) {
  const disk = /^\s*\d+\s+\d+\s+(?:sd[a-z]+|vd[a-z]+|nvme\d+n\d+)\s/u;
  const sample = () => {
    const disks = readFileSync("/proc/diskstats", "utf8").split("\n").filter((line) => disk.test(line));
    const pressure = ["io", "cpu"].map((kind) => readFileSync(`/proc/pressure/${kind}`, "utf8").trim().split("\n").map((line) => `${kind} ${line}`));
    appendFileSync(out, `@${Date.now()}\n${[...disks, ...pressure.flat()].join("\n")}\n`);
  };
  sample();
  const timer = setInterval(sample, 200);
  return { stop: () => { clearInterval(timer); sample(); } };
}

/** Puts back /attach as the save that ended the instance left it. */
async function rewindAttach(p: Paths): Promise<void> {
  rmSync(p.attach, { recursive: true, force: true });
  await run("cp", ["-a", "--reflink=auto", p.attachSaved, p.attach]);
}

/**
 * One restore of the save-stop image: `prepare` (inside t0) readies the image directory and
 * says what to restore from; the restore, the probe and `runsc wait --restore` then run. Where
 * `saveTo` is given, the restored sandbox is then saved there, as it ends.
 */
async function restore(
  setup: Setup,
  p: Paths,
  fixture: string,
  runId: string,
  sessionId: string,
  name: string,
  prepare: () => Promise<{ readonly imagePath: string; readonly direct: boolean; readonly helpers: () => readonly number[]; readonly details: Row; readonly helperPeakBytes?: number }>,
  saveTo?: string,
): Promise<{ ok: boolean; failure: string | null; metrics: Row; correctness?: Row; details: Row }> {
  const logDir = join(p.logs, name);
  mkdirSync(logDir, { recursive: true });
  await netUp();
  const io = setup.sampleIO ? ioSampler(join(p.logs, `${name}.io.txt`)) : undefined;
  try {
    let helperPeak = 0;
    let sampler: ReturnType<typeof peakSampler> | undefined;
    // t0 on the wall clock as well, which the Sentry's log lines are on.
    const t0UnixMs = Date.now();
    const t0 = now();
    const probe = background(process.execPath, [
      DRIVER, "probe", ...endpointArgs, "--session", sessionId, "--fixture", fixture, "--run-id", runId, "--t0-ns", String(t0), "--verify-after-ms", String(setup.verifyAfterMs),
    ], { timeoutMs: RESTORE_DEADLINE_MS + VERIFY_ALLOWANCE_MS + setup.verifyAfterMs });
    try {
      const prepared = await prepare();
      helperPeak += prepared.helperPeakBytes ?? 0;
      sampler = peakSampler(prepared.helpers);
      const restoreStart = now();
      await sandboxCommand(setup, logDir, join(p.logs, `${name}.out`), [
        ...setup.restoreFlags, "restore", "--detach", `--bundle=${p.bundle}`, `--image-path=${prepared.imagePath}`, ...(setup.background ? ["--background"] : []), ...(prepared.direct ? ["--direct"] : []), p.id,
      ]);
      const restored = now();
      const waited = runsc(setup, logDir, ["wait", "--restore", p.id]).then(() => now());
      // A restore that fails once it has returned, as when bayma's hook refuses it, ends the
      // sandbox; the probe would wait on it for nothing.
      const never = new Promise<never>(() => {});
      const failed = waited.then(() => never);
      const ended = runsc(setup, logDir, ["wait", p.id], true).then((ran): never => {
        throw new Error(`the sandbox ended (${ran.stdout.trim()}) before the probe did: ${sentryWarnings(logDir)}`);
      });
      failed.catch(() => {});
      ended.catch(() => {});
      // What the sandbox runs, for the logs, where the probe takes long past any restore's time.
      const slow = setTimeout(() => void describeSandbox(setup, logDir, p.id, join(p.logs, `${name}.slow`)), SLOW_PROBE_MS);
      const probed = await Promise.race([ok(probe, "the probe"), failed, ended]).finally(() => clearTimeout(slow));
      const said = JSON.parse(probed.stdout.trim().split("\n").at(-1) ?? "") as {
        ok: boolean; failure: string | null; first_exec_ms: number; verify_ms: number; workload_rss_bytes: number; correctness: Row; verify_end_ns: string;
        first: Row; first_stderr: string;
      };
      if (said.first_exec_ms > RESTORE_DEADLINE_MS) throw new Error(`the first exec answered ${Math.round(said.first_exec_ms)} ms after the restore was asked, past the ${RESTORE_DEADLINE_MS / 1000} s a restore has`);
      const left = RESTORE_DEADLINE_MS - msBetween(t0, now());
      const residentAt = await Promise.race([waited, sleep(Math.max(0, left)).then((): never => {
        throw new Error(`the sandbox's pages were not all resident ${RESTORE_DEADLINE_MS / 1000} s after the restore was asked`);
      })]);
      const memoryBytes = cgroupRead(p.id, "memory.current");
      const memoryPeak = cgroupRead(p.id, "memory.peak");
      helperPeak += sampler.stop();
      sampler = undefined;
      const log = bootLog(logDir);
      if (saveTo !== undefined) await runsc(setup, logDir, ["checkpoint", `--image-path=${saveTo}`, ...setup.checkpoint, `--save-restore-exec-argv=${HOOK.join(" ")}`, p.id]);
      return {
        ok: said.ok,
        failure: said.failure,
        metrics: {
          first_exec_ms: said.first_exec_ms,
          verify_ms: said.verify_ms,
          resident_ms: msBetween(t0, residentAt),
          resident_how: "signal",
          memory_peak_bytes: memoryPeak + helperPeak,
          memory_bytes: memoryBytes,
          workload_rss_bytes: said.workload_rss_bytes,
        },
        correctness: said.correctness,
        details: {
          ...prepared.details,
          t0_unix_ms: t0UnixMs,
          first_answer_unix_ms: t0UnixMs + said.first_exec_ms,
          // What the fixture's first exec said, and what its runtime wrote to stderr meanwhile.
          first: said.first,
          first_stderr: said.first_stderr,
          prepare_ms: msBetween(t0, restoreStart),
          runsc_restore_ms: msBetween(restoreStart, restored),
          resident_after_verify_ms: msBetween(BigInt(said.verify_end_ns), residentAt),
          helper_peak_bytes: helperPeak,
          cgroup_memory_peak_bytes: memoryPeak,
          sentry: loadLines(log),
        },
      };
    } finally {
      sampler?.stop();
      probe.child.kill("SIGKILL");
      writeFileSync(join(p.logs, `${name}.probe.err`), (await probe.done).stderr);
    }
  } finally {
    io?.stop();
    await endInstance(setup, logDir, p.id);
    await netDown();
  }
}

interface Job {
  readonly common: Common;
  /** The host's settings as the runner came, which `DEFAULTS` runs on. */
  readonly runnerSettings: Record<string, string>;
  readonly copyMibS: number;
  readonly results: string;
  readonly rclone: NodeJS.ProcessEnv;
}

function emit(job: Job, row: Row): void {
  appendFileSync(job.results, `${JSON.stringify(row)}\n`);
  console.log(`  ${String(row["variant"])} ${String(row["phase"])}: ${row["ok"] ? "ok" : `failed: ${String(row["failure"])}`}`);
}

/** What the fixture's ticker says of a window. */
interface Gaps {
  readonly max_gap_ms: number;
  readonly slowdown: number | null;
}

/** What the server's collection before a save said (SERVER_GC_SCRIPT's line), or null. */
type ServerGc = Record<string, number> | null;

/**
 * Has bayma's server run its collection before a save, where the setup asks for one: SIGUSR2 to
 * the instance's first `node` process, then its line in the instance's output, `out`, awaited.
 */
async function serverGc(setup: Setup, logDir: string, id: string, out: string, release = false): Promise<ServerGc> {
  if (setup.serverGc === "none" || (setup.serverGc === "release" && !release)) return null;
  const said = () => readFileSync(out, "utf8").split("\n").filter((line) => line.startsWith("BENCH-SERVER-GC "));
  const before = said().length;
  // "UID PID PPID C STIME TIME CMD": the server is the first node, whose main thread node names
  // MainThread.
  const server = (await runsc(setup, logDir, ["ps", id])).stdout.split("\n").slice(1).map((line) => line.trim().split(/\s+/u)).find((cols) => cols.at(-1) === "node" || cols.at(-1) === "MainThread");
  if (!server?.[1]) throw new Error("no node process in the instance for the server's collection");
  await runsc(setup, logDir, ["kill", `--pid=${server[1]}`, id, "USR2"]);
  const deadline = Date.now() + 15_000;
  while (said().length <= before) {
    if (Date.now() > deadline) throw new Error("the server said nothing of its collection within 15 s");
    await sleep(100);
  }
  return JSON.parse(said()[before]!.slice("BENCH-SERVER-GC ".length)) as Record<string, number>;
}

/**
 * X-IDLE: the instance, idle since its setup, saved live, then live again after each of
 * setup.idleSaves, each save incremental over the one before (`--parent-image-path`, with the
 * dirty tracking the setup's flags turn on): a line per incremental save, of variant
 * `<variant>-idle-<seconds>s`, saying the bytes it wrote. Returns the instance's saves so far.
 */
async function idleSaves(job: Job, setup: Setup, logDir: string, p: Paths, at: { fixture: string; size: number; run: number }, saves: number): Promise<number> {
  const checkpoint = (image: string, parent?: string) =>
    runsc(setup, logDir, ["checkpoint", `--image-path=${image}`, "--leave-running", ...(parent === undefined ? [] : [`--parent-image-path=${parent}`]), ...setup.checkpoint, `--save-restore-exec-argv=${HOOK.join(" ")}`, p.id]);
  const out = join(p.logs, "instance.out");
  let parent = join(p.dir, "idle-0");
  await serverGc(setup, logDir, p.id, out);
  await checkpoint(parent);
  saves++;
  for (const [index, seconds] of setup.idleSaves.entries()) {
    const variant = `${setup.variant}-idle-${seconds}s`;
    const image = join(p.dir, `idle-${index + 1}`);
    try {
      await sleep(seconds * 1000);
      const gc = await serverGc(setup, logDir, p.id, out);
      const t0 = now();
      await checkpoint(image, parent);
      const t1 = now();
      const pause = savePause(bootLog(logDir), saves++, true);
      emit(job, rowOf(job.common, setup, at, variant, "save-live", {
        ok: true,
        failure: null,
        metrics: { save_total_ms: msBetween(t0, t1), pause_reported_ms: pause.ms, precopy_passes: 0, precopy_bytes: 0, image_bytes: dirBytes(image), uploaded_bytes: null },
        details: { idle_s: seconds, parent: `idle-${index}`, files: dirFiles(image), sentry: pause.lines, ...(gc === null ? {} : { server_gc: gc }) },
      }));
      rmSync(parent, { recursive: true, force: true });
      parent = image;
    } catch (error) {
      emit(job, rowOf(job.common, setup, at, variant, "save-live", failed(error)));
      break;
    }
  }
  rmSync(parent, { recursive: true, force: true });
  return saves;
}

/**
 * A save, the instance's `nth`: `runsc checkpoint` into the run's staging directory (live:
 * `--leave-running`, the fixture's window around it), then the image uploaded; done when its
 * last object is in.
 */
async function save(setup: Setup, logDir: string, p: Paths, live: boolean, nth: number, mark?: () => Promise<unknown>, gaps?: () => Promise<Gaps>) {
  const image = live ? p.live : p.stop;
  const prefix = `${p.prefix}${live ? "live" : "stop"}/`;
  const before = await bucketBytes(prefix);
  const gc = await serverGc(setup, logDir, p.id, join(p.logs, "instance.out"), !live);
  await mark?.();
  const t0 = now();
  await runsc(setup, logDir, ["checkpoint", `--image-path=${image}`, ...(live ? ["--leave-running"] : []), ...setup.checkpoint, `--save-restore-exec-argv=${HOOK.join(" ")}`, p.id]);
  const saved = now();
  const upload = await s3<{ files: number; bytes: number }>(["upload", image, prefix]);
  const t1 = now();
  const window = await gaps?.();
  const pause = savePause(bootLog(logDir), nth, live);
  return {
    ok: true,
    failure: null,
    metrics: {
      save_total_ms: msBetween(t0, t1),
      ...(window ? { unavailable_ms: window.max_gap_ms, slowdown: window.slowdown } : {}),
      pause_reported_ms: pause.ms,
      precopy_passes: 0,
      precopy_bytes: 0,
      image_bytes: dirBytes(image),
      uploaded_bytes: (await bucketBytes(prefix)) - before,
    },
    details: { checkpoint_ms: msBetween(t0, saved), upload_ms: msBetween(saved, t1), upload, files: dirFiles(image), ...(window ? { gaps: window } : {}), sentry: pause.lines, ...(gc === null ? {} : { server_gc: gc }) },
  };
}

/**
 * One run of `setup`: setup, save-live, save-stop, restore-warm, and the cold restores (both
 * variants' for the tuned setup, odd runs fetched first, even runs through the mount first).
 */
async function oneRun(job: Job, setup: Setup, fixture: string, size: number, runNo: number, pauseCheck: boolean): Promise<void> {
  const at = { fixture, size, run: runNo };
  const p = pathsOf(job.common.profile, setup, fixture, size, runNo);
  const runId = `gvisor-${job.common.profile}-${p.id}-${process.env["GITHUB_RUN_ID"] ?? "local"}`;
  const variant = setup.variant;
  rmSync(p.dir, { recursive: true, force: true });
  rmSync(p.logs, { recursive: true, force: true });
  await makeAttach(p.attach);
  writeBundle(p.bundle, p.id, p.attach, setup);
  const logDir = join(p.logs, "instance");
  mkdirSync(logDir, { recursive: true });
  if (setup.hostSettings === "check") putHostSettings(job.runnerSettings);

  let sessionId = "";
  let stopped: string | null = null;
  let settings: Record<string, string> = {};
  // The instance's saves so far, which its Sentry's log numbers.
  let saves = 0;
  // V8's heap statistics in a node the instance starts, as its bayma's server sees them.
  let serverHeap: unknown = null;
  const saved = (result: Awaited<ReturnType<typeof save>>) => ({ ...result, details: { ...result.details, host_settings: settings } });
  await netUp();
  try {
    await sandboxCommand(setup, logDir, join(p.logs, "instance.out"), ["run", "--detach", `--bundle=${p.bundle}`, p.id]);
    settings = hostSettings();
    await portUp(120_000);
    const ready = await driver<{ session_id: string }>("setup", ["--fixture", fixture, "--size-mib", String(size), "--run-id", runId, "--rate-mib-s", String(job.copyMibS)]);
    sessionId = ready.session_id;

    if (pauseCheck) await checkPause(setup, logDir, p.id, sessionId, fixture);
    if (setup.heapStats) serverHeap = JSON.parse((await runsc(setup, logDir, ["exec", p.id, "node", "-e", "console.log(JSON.stringify(require('v8').getHeapStatistics()))"])).stdout.trim());

    if (setup.live) {
      try {
        const mark = () => driver("mark", ["--session", sessionId, "--fixture", fixture]);
        const gaps = () => driver<Gaps>("gaps", ["--session", sessionId, "--fixture", fixture]);
        emit(job, rowOf(job.common, setup, at, variant, "save-live", saved(await save(setup, logDir, p, true, saves++, mark, gaps))));
      } catch (error) {
        emit(job, rowOf(job.common, setup, at, variant, "save-live", failed(error)));
      } finally {
        rmSync(p.live, { recursive: true, force: true });
      }
    }
    if (setup.idleSaves.length > 0) saves = await idleSaves(job, setup, logDir, p, at, saves);
    const hygiene = setup.hygiene === null ? null : await driver<{ exec_ms: number }>("run", ["--session", sessionId, "--code", setup.hygiene[fixture.startsWith("repl-python") ? "python" : "bun"]]);
    try {
      const result = saved(await save(setup, logDir, p, false, saves++));
      emit(job, rowOf(job.common, setup, at, variant, "save-stop", { ...result, details: { ...result.details, ...(hygiene === null ? {} : { hygiene_exec_ms: hygiene.exec_ms }), ...(serverHeap === null ? {} : { server_heap: serverHeap }) } }));
    } catch (error) {
      stopped = error instanceof Error ? error.message : String(error);
      emit(job, rowOf(job.common, setup, at, variant, "save-stop", failed(error)));
    }
  } catch (error) {
    if (error instanceof PauseCheckFailed) throw error;
    // The instance never got as far as its saves.
    stopped = error instanceof Error ? error.message : String(error);
    for (const phase of [...(setup.live ? ["save-live"] : []), "save-stop"]) emit(job, rowOf(job.common, setup, at, variant, phase, failed(error)));
  } finally {
    await endInstance(setup, logDir, p.id);
    await netDown();
  }

  const fetched = setup.fetch ? [{ variant, lazy: false }] : [];
  const lazy = setup.lazy === null ? [] : [{ variant: setup.lazy.variant, lazy: true }];
  const colds = runNo % 2 === 1 ? [...fetched, ...lazy] : [...lazy, ...fetched];
  if (stopped !== null) {
    const why = { ok: false, failure: `not reached: save-stop failed: ${stopped}`, metrics: {} };
    for (const warm of setup.warm) emit(job, rowOf(job.common, setup, at, warm.variant, "restore-warm", why));
    for (const cold of colds) emit(job, rowOf(job.common, setup, at, cold.variant, "restore-cold", why, cold.lazy));
    await cleanRun(p);
    return;
  }
  await run("cp", ["-a", "--reflink=auto", p.attach, p.attachSaved]);

  // The image directory the save staged, from the runner's disk: no restore starts with any of
  // it in the page cache (DESIGN.md section 4), but one of a setup that restores it again with
  // the page cache kept, as a node keeps the files of an image its sandboxes run.
  const again = { image: join(p.dir, "image-again"), attach: join(p.dir, "attach.again") };
  for (const [index, warm] of setup.warm.entries()) {
    const saveAgain = setup.again && index === 0;
    try {
      await rewindAttach(p);
      if (warm.cache === "dropped") await dropCaches();
      if (warm.cache === "evicted") await evictImage(p.stop);
      emit(job, rowOf(job.common, setup, at, warm.variant, "restore-warm", await restore(setup, p, fixture, runId, sessionId, warm.cache === "dropped" ? "warm" : `warm-${warm.cache}`, async () => ({
        imagePath: p.stop, direct: setup.warmDirect, helpers: () => [], details: { source: "staged image directory", page_cache: warm.cache },
      }), saveAgain ? again.image : undefined)));
      // The attach as the save again left it, for the restore of that image.
      if (saveAgain) await run("cp", ["-a", "--reflink=auto", p.attach, again.attach]);
    } catch (error) {
      emit(job, rowOf(job.common, setup, at, warm.variant, "restore-warm", failed(error)));
    }
  }
  rmSync(p.stop, { recursive: true, force: true });
  // The image saved again from the first warm restore's sandbox, which holds the working set that
  // restore recorded, restored as the first was; the attach it saved is the one to restore with.
  if (setup.again) {
    const variantAgain = `${variant}-again`;
    try {
      if (!existsSync(again.attach)) throw new Error("not reached: the first warm restore was not saved again");
      rmSync(p.attach, { recursive: true, force: true });
      await run("cp", ["-a", "--reflink=auto", again.attach, p.attach]);
      await dropCaches();
      emit(job, rowOf(job.common, setup, at, variantAgain, "restore-warm", await restore(setup, p, fixture, runId, sessionId, "warm-again", async () => ({
        imagePath: again.image, direct: setup.warmDirect, helpers: () => [], details: { source: "image saved again from the first warm restore", page_cache: "dropped" },
      }))));
    } catch (error) {
      emit(job, rowOf(job.common, setup, at, variantAgain, "restore-warm", failed(error)));
    }
    rmSync(again.image, { recursive: true, force: true });
    rmSync(again.attach, { recursive: true, force: true });
  }

  for (const cold of colds) {
    try {
      await rewindAttach(p);
      await dropCaches();
      const restored = setup.lazy !== null && cold.lazy ? await coldLazy(job, setup, setup.lazy.mount, p, fixture, runId, sessionId) : await coldFetch(setup, p, fixture, runId, sessionId);
      emit(job, rowOf(job.common, setup, at, cold.variant, "restore-cold", restored, cold.lazy));
    } catch (error) {
      emit(job, rowOf(job.common, setup, at, cold.variant, "restore-cold", failed(error), cold.lazy));
    }
  }
  await cleanRun(p);
}

/** Cold, fetched: the image downloaded whole from the bucket, then restored from the runner's disk. */
function coldFetch(setup: Setup, p: Paths, fixture: string, runId: string, sessionId: string) {
  const image = join(p.dir, "image-fetched");
  return restore(setup, p, fixture, runId, sessionId, "cold-fetch", async () => {
    const started = now();
    const download = background(process.execPath, [S3, ...DOWNLOAD, "download", `${p.prefix}stop/`, image]);
    const sampler = peakSampler(() => (download.child.pid === undefined ? [] : [download.child.pid]));
    const got = JSON.parse((await ok(download, "the download")).stdout.trim()) as { files: number; bytes: number };
    // Just written, the image is in the page cache: read through it, not past it.
    return { imagePath: image, direct: false, helpers: () => [], helperPeakBytes: sampler.stop(), details: { source: "bucket, downloaded whole", download: { ...got, ms: msBetween(started, now()) } } };
  }).finally(() => rmSync(image, { recursive: true, force: true }));
}

/**
 * Cold, `lazy`: the bucket mounted with rclone, the sandbox restored from the mount, its pages
 * read as it touches them and in the background, the pages file preloaded behind them.
 */
async function coldLazy(job: Job, setup: Setup, flags: readonly string[], p: Paths, fixture: string, runId: string, sessionId: string) {
  const mount = join(p.dir, "mount");
  const cache = join(p.dir, "rclone-cache");
  mkdirSync(mount, { recursive: true });
  let rclone: Background | undefined;
  let preload: Background | undefined;
  try {
    return await restore(setup, p, fixture, runId, sessionId, "cold-lazy", async () => {
      const started = now();
      rclone = background(RCLONE, [
        "mount", `bench:${process.env["S3_BUCKET"] ?? "snapshots"}/${p.prefix}stop`, mount,
        ...flags,
        "--cache-dir", cache,
        "--log-file", join(p.logs, "rclone.log"),
        "--log-level", "INFO",
      ], { env: job.rclone });
      while (!existsSync(join(mount, "checkpoint.img"))) {
        if (rclone.child.exitCode !== null) throw new Error(`rclone mount exited ${rclone.child.exitCode}: ${readFileSync(join(p.logs, "rclone.log"), "utf8").slice(-2000)}`);
        if (msBetween(started, now()) > 60_000) throw new Error("rclone's mount did not come up in 60 s");
        await sleep(5);
      }
      const mounted = now();
      // Modal's practice: the pages file read whole into the cache as early as it can be.
      preload = background("dd", [`if=${join(mount, "pages.img")}`, "of=/dev/null", "bs=16M"]);
      const pids = () => [rclone?.child.pid, preload?.child.pid].filter((pid): pid is number => pid !== undefined);
      return { imagePath: mount, direct: false, helpers: pids, details: { source: "bucket, through rclone's mount", mount_ms: msBetween(started, mounted) } };
    });
  } finally {
    preload?.child.kill("SIGKILL");
    await preload?.done;
    await run("fusermount3", ["-u", "-z", mount], { allowFail: true });
    rclone?.child.kill("SIGTERM");
    await rclone?.done;
    rmSync(cache, { recursive: true, force: true });
  }
}

/** Clears a run's files and its images, the store's space freed with them, before the next run. */
async function cleanRun(p: Paths): Promise<void> {
  rmSync(p.dir, { recursive: true, force: true });
  await s3(["delete", p.prefix]);
  const vacuumed = await fetch("http://127.0.0.1:9333/vol/vacuum?garbageThreshold=0.01");
  if (!vacuumed.ok) throw new Error(`the store's vacuum answered ${vacuumed.status}: ${await vacuumed.text()}`);
}

/**
 * The job's check of the ticker: the sandbox paused with runsc's own pause for 1000 ms; the
 * ticker must see a gap of 1000 ms +/- 20 ms.
 */
async function checkPause(setup: Setup, logDir: string, id: string, sessionId: string, fixture: string): Promise<void> {
  await driver("mark", ["--session", sessionId, "--fixture", fixture]);
  const a = now();
  await runsc(setup, logDir, ["pause", id]);
  const b = now();
  await sleep(1000);
  const c = now();
  await runsc(setup, logDir, ["resume", id]);
  const d = now();
  const gaps = await driver<Gaps>("gaps", ["--session", sessionId, "--fixture", fixture]);
  const check = { max_gap_ms: gaps.max_gap_ms, pause_call_ms: msBetween(a, b), held_ms: msBetween(b, c), resume_call_ms: msBetween(c, d) };
  console.log(`pause check: ${JSON.stringify(check)}`);
  if (Math.abs(gaps.max_gap_ms - 1000) > 20) throw new PauseCheckFailed(`the ticker saw ${gaps.max_gap_ms} ms of a 1000 ms pause: ${JSON.stringify(check)}`);
}

class PauseCheckFailed extends Error {
  override readonly name = "PauseCheckFailed";
}

// ---------------------------------------------------------------- the job

async function main([profile]: readonly string[]): Promise<void> {
  if (!profile || !BENCH_DIR) throw new Error("usage: BENCH_DIR=<dir> node harness.ts <profile>");
  const runs = Number(process.env["BENCH_RUNS"] || 5);
  const asked = (process.env["BENCH_CASES"] ?? "").split(/[\s,]+/u).filter(Boolean);
  const all = JSON.parse(readFileSync(join(here, "../common/cases.json"), "utf8")) as { fixture: string; size_mib: number }[];
  const cases = all.filter(({ fixture, size_mib }) => asked.length === 0 || asked.includes(`${fixture}/${size_mib}`));
  if (cases.length === 0) throw new Error(`no case of ${asked.join(" ")}`);
  const host = JSON.parse(readFileSync(join(BENCH_DIR, "host.json"), "utf8")) as Row;
  // "runsc version release-20261019.0": upstream's release, or the build install.sh was given.
  const version = (await run(RUNSC, ["--version"])).stdout.split("\n")[0]?.replace(/^runsc version /u, "") ?? "";
  const job: Job = {
    common: { host, store: JSON.parse(readFileSync(join(BENCH_DIR, "store.json"), "utf8")) as Row, profile, emulated: profile !== "local", system: `gVisor ${version}` },
    runnerSettings: hostSettings(),
    copyMibS: Number(host["copy_mib_s"]),
    results: join(BENCH_DIR, "results.jsonl"),
    // rclone's remote `bench`: the job's store, as the environment names it for common/store/s3.ts.
    rclone: {
      ...process.env,
      RCLONE_CONFIG_BENCH_TYPE: "s3",
      RCLONE_CONFIG_BENCH_PROVIDER: "SeaweedFS",
      RCLONE_CONFIG_BENCH_ENDPOINT: process.env["S3_ENDPOINT"] ?? "",
      RCLONE_CONFIG_BENCH_ACCESS_KEY_ID: process.env["AWS_ACCESS_KEY_ID"] ?? "",
      RCLONE_CONFIG_BENCH_SECRET_ACCESS_KEY: process.env["AWS_SECRET_ACCESS_KEY"] ?? "",
      RCLONE_CONFIG_BENCH_REGION: process.env["S3_REGION"] ?? "us-east-1",
    },
  };
  // The job's variant: `all` is the tuned setup, and upstream's defaults beside it on the store
  // as the box runs it; any other one of SETUPS, as a job of its own.
  const variantAsked = process.env["BENCH_VARIANT"] ?? "all";
  const setups = SETUPS[variantAsked] ?? (profile === "local" ? [TUNED, DEFAULTS] : [TUNED]);
  const images = new Set(setups.map((setup) => setup.image));
  if (images.size !== 1) throw new Error(`a job runs one session image, not ${[...images].join(" and ")}`);
  mkdirSync(WORK, { recursive: true });
  await netDown();
  await prepareRootfs([...images][0] ?? IMAGE);
  let checked = false;
  for (let runNo = 1; runNo <= runs; runNo++) {
    for (const { fixture, size_mib } of cases) {
      for (const setup of setups) {
        console.log(`run ${runNo}: ${fixture} ${size_mib} MiB, ${setup.name}`);
        await oneRun(job, setup, fixture, size_mib, runNo, !checked);
        checked = true;
      }
    }
  }
}

main(process.argv.slice(2)).catch((error: unknown) => {
  console.error(error instanceof Error ? (error.stack ?? error.message) : String(error));
  process.exit(1);
});
