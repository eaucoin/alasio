/**
 * The gvisor system's runs: upstream gVisor's runsc, from an OCI bundle of the instance image,
 * saved with `runsc checkpoint` (live: `--leave-running`) to the runner's disk and uploaded to
 * the bucket, restored with `runsc restore --background`: warm from the image directory the
 * save staged, cold from the bucket, either downloaded whole first (`fetch`) or read through
 * an rclone FUSE mount of the bucket as the sandbox runs, the pages file preloaded behind it
 * (`lazy`). README.md says why each setting is what it is.
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
export const GVISOR_RELEASE = "release-20261019.0";
export const RCLONE_VERSION = "v1.75.2";
const IMAGE = "ghcr.io/eaucoin/alasio-agent:10.0.0@sha256:b90f8164f77e9a56bd0da0fc1869bc76c1c541742a07751764ffc156fa8fe30a";

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
};

const DEFAULTS: Setup = { name: "defaults", variant: "defaults", hostSettings: "check", checkpoint: [], background: false, warmDirect: false, lazy: null };

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
const runscFlags = (setup: Setup, logDir: string) => [`--root=${ROOT}`, `--debug-log=${logDir}/`, `--host-settings=${setup.hostSettings}`];

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
async function prepareRootfs(): Promise<void> {
  await run("docker", ["pull", "-q", IMAGE]);
  const container = (await run("docker", ["create", IMAGE])).stdout.trim();
  mkdirSync(ROOTFS, { recursive: true });
  await run("bash", ["-c", `docker export ${container} | tar -x -C ${ROOTFS}`]);
  await run("docker", ["rm", container]);
}

/** The bundle of an instance whose /attach is `attach`, in `dir`: the runner systems' spec, its cgroup named. */
function writeBundle(dir: string, id: string, attach: string): void {
  mkdirSync(dir, { recursive: true });
  writeFileSync(join(dir, "config.json"), JSON.stringify(config({ rootfs: ROOTFS, attach, netns: `/var/run/netns/${NETNS}`, port: PORT, cgroupsPath: `/gvbench/${id}` }), null, 2));
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
 * to "Tasks resumed after save." (a save that leaves the sandbox running) or "Save succeeded"
 * (one that ends it).
 */
function savePause(log: string, nth: number, live: boolean): { readonly ms: number; readonly lines: readonly string[] } {
  const lines = log.split("\n");
  const starts = lines.flatMap((line, index) => (line.includes("Sandbox save started, pausing all tasks.") ? [index] : []));
  const start = starts[nth];
  if (start === undefined) throw new Error(`the Sentry's log has no save ${nth + 1}`);
  const endText = live ? "Tasks resumed after save." : "Save succeeded";
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
}

type Row = Record<string, unknown>;

const NULL_SAVE = { save_total_ms: null, unavailable_ms: null, pause_reported_ms: null, slowdown: null, precopy_passes: null, precopy_bytes: null, image_bytes: null, uploaded_bytes: null };
const NULL_RESTORE = { first_exec_ms: null, verify_ms: null, resident_ms: null, resident_how: null, memory_peak_bytes: null, memory_bytes: null, workload_rss_bytes: null };
const NULL_CORRECTNESS = { marker: null, memory: null, children: null, file: null, socket: null, running: null, notice: null };

function rowOf(common: Common, at: { fixture: string; size: number; run: number }, variant: string, phase: string, result: { ok: boolean; failure: string | null; metrics: Row; correctness?: Row; details?: Row }, mounted = false): Row {
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
    versions: { system: `gVisor ${GVISOR_RELEASE}${mounted ? `, rclone ${RCLONE_VERSION}` : ""}`, bayma: "0.15.2", seaweedfs: "4.48", toxiproxy: "2.13.0" },
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
  await run("chown", ["-R", "1000:1000", attach]);
}

const dirBytes = (dir: string): number => readdirSync(dir).reduce((sum, name) => sum + statSync(join(dir, name)).size, 0);
const dirFiles = (dir: string) => readdirSync(dir).map((name) => ({ name, size: statSync(join(dir, name)).size }));

/** Drops the page cache, as every restore starts with none of the image in it. */
async function dropCaches(): Promise<void> {
  await run("sync", []);
  writeFileSync("/proc/sys/vm/drop_caches", "1");
}

/** Puts back /attach as the save that ended the instance left it. */
async function rewindAttach(p: Paths): Promise<void> {
  rmSync(p.attach, { recursive: true, force: true });
  await run("cp", ["-a", "--reflink=auto", p.attachSaved, p.attach]);
}

/**
 * One restore of the save-stop image: `prepare` (inside t0) readies the image directory and
 * says what to restore from; the restore, the probe and `runsc wait --restore` then run.
 */
async function restore(
  setup: Setup,
  p: Paths,
  fixture: string,
  runId: string,
  sessionId: string,
  name: string,
  prepare: () => Promise<{ readonly imagePath: string; readonly direct: boolean; readonly helpers: () => readonly number[]; readonly details: Row; readonly helperPeakBytes?: number }>,
): Promise<{ ok: boolean; failure: string | null; metrics: Row; correctness?: Row; details: Row }> {
  const logDir = join(p.logs, name);
  mkdirSync(logDir, { recursive: true });
  await netUp();
  try {
    let helperPeak = 0;
    let sampler: ReturnType<typeof peakSampler> | undefined;
    const t0 = now();
    const probe = background(process.execPath, [DRIVER, "probe", ...endpointArgs, "--session", sessionId, "--fixture", fixture, "--run-id", runId, "--t0-ns", String(t0)], { timeoutMs: RESTORE_DEADLINE_MS + VERIFY_ALLOWANCE_MS });
    try {
      const prepared = await prepare();
      helperPeak += prepared.helperPeakBytes ?? 0;
      sampler = peakSampler(prepared.helpers);
      const restoreStart = now();
      await sandboxCommand(setup, logDir, join(p.logs, `${name}.out`), [
        "restore", "--detach", `--bundle=${p.bundle}`, `--image-path=${prepared.imagePath}`, ...(setup.background ? ["--background"] : []), ...(prepared.direct ? ["--direct"] : []), p.id,
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

/**
 * A save: `runsc checkpoint` into the run's staging directory (live: `--leave-running`, the
 * fixture's window around it), then the image uploaded; done when its last object is in.
 */
async function save(setup: Setup, logDir: string, p: Paths, live: boolean, mark?: () => Promise<unknown>, gaps?: () => Promise<Gaps>) {
  const image = live ? p.live : p.stop;
  const prefix = `${p.prefix}${live ? "live" : "stop"}/`;
  const before = await bucketBytes(prefix);
  await mark?.();
  const t0 = now();
  await runsc(setup, logDir, ["checkpoint", `--image-path=${image}`, ...(live ? ["--leave-running"] : []), ...setup.checkpoint, `--save-restore-exec-argv=${HOOK.join(" ")}`, p.id]);
  const saved = now();
  const upload = await s3<{ files: number; bytes: number }>(["upload", image, prefix]);
  const t1 = now();
  const window = await gaps?.();
  const pause = savePause(bootLog(logDir), live ? 0 : 1, live);
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
    details: { checkpoint_ms: msBetween(t0, saved), upload_ms: msBetween(saved, t1), upload, files: dirFiles(image), ...(window ? { gaps: window } : {}), sentry: pause.lines },
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
  writeBundle(p.bundle, p.id, p.attach);
  const logDir = join(p.logs, "instance");
  mkdirSync(logDir, { recursive: true });
  if (setup.hostSettings === "check") putHostSettings(job.runnerSettings);

  let sessionId = "";
  let stopped: string | null = null;
  let settings: Record<string, string> = {};
  const saved = (result: Awaited<ReturnType<typeof save>>) => ({ ...result, details: { ...result.details, host_settings: settings } });
  await netUp();
  try {
    await sandboxCommand(setup, logDir, join(p.logs, "instance.out"), ["run", "--detach", `--bundle=${p.bundle}`, p.id]);
    settings = hostSettings();
    await portUp(120_000);
    const ready = await driver<{ session_id: string }>("setup", ["--fixture", fixture, "--size-mib", String(size), "--run-id", runId, "--rate-mib-s", String(job.copyMibS)]);
    sessionId = ready.session_id;

    if (pauseCheck) await checkPause(setup, logDir, p.id, sessionId, fixture);

    try {
      const mark = () => driver("mark", ["--session", sessionId, "--fixture", fixture]);
      const gaps = () => driver<Gaps>("gaps", ["--session", sessionId, "--fixture", fixture]);
      emit(job, rowOf(job.common, at, variant, "save-live", saved(await save(setup, logDir, p, true, mark, gaps))));
    } catch (error) {
      emit(job, rowOf(job.common, at, variant, "save-live", failed(error)));
    } finally {
      rmSync(p.live, { recursive: true, force: true });
    }
    try {
      emit(job, rowOf(job.common, at, variant, "save-stop", saved(await save(setup, logDir, p, false))));
    } catch (error) {
      stopped = error instanceof Error ? error.message : String(error);
      emit(job, rowOf(job.common, at, variant, "save-stop", failed(error)));
    }
  } catch (error) {
    if (error instanceof PauseCheckFailed) throw error;
    // The instance never got as far as its saves.
    stopped = error instanceof Error ? error.message : String(error);
    for (const phase of ["save-live", "save-stop"]) emit(job, rowOf(job.common, at, variant, phase, failed(error)));
  } finally {
    await endInstance(setup, logDir, p.id);
    await netDown();
  }

  const fetched = { variant, lazy: false };
  const colds = setup.lazy === null ? [fetched] : runNo % 2 === 1 ? [fetched, { variant: setup.lazy.variant, lazy: true }] : [{ variant: setup.lazy.variant, lazy: true }, fetched];
  if (stopped !== null) {
    const why = { ok: false, failure: `not reached: save-stop failed: ${stopped}`, metrics: {} };
    emit(job, rowOf(job.common, at, variant, "restore-warm", why));
    for (const cold of colds) emit(job, rowOf(job.common, at, cold.variant, "restore-cold", why, cold.lazy));
    await cleanRun(p);
    return;
  }
  await run("cp", ["-a", "--reflink=auto", p.attach, p.attachSaved]);

  // The image directory the save staged, from the runner's disk: no restore starts with any of
  // it in the page cache (DESIGN.md section 4).
  try {
    await dropCaches();
    emit(job, rowOf(job.common, at, variant, "restore-warm", await restore(setup, p, fixture, runId, sessionId, "warm", async () => ({
      imagePath: p.stop, direct: setup.warmDirect, helpers: () => [], details: { source: "staged image directory" },
    }))));
  } catch (error) {
    emit(job, rowOf(job.common, at, variant, "restore-warm", failed(error)));
  }
  rmSync(p.stop, { recursive: true, force: true });

  for (const cold of colds) {
    try {
      await rewindAttach(p);
      await dropCaches();
      const restored = setup.lazy !== null && cold.lazy ? await coldLazy(job, setup, setup.lazy.mount, p, fixture, runId, sessionId) : await coldFetch(setup, p, fixture, runId, sessionId);
      emit(job, rowOf(job.common, at, cold.variant, "restore-cold", restored, cold.lazy));
    } catch (error) {
      emit(job, rowOf(job.common, at, cold.variant, "restore-cold", failed(error), cold.lazy));
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
  const job: Job = {
    common: { host, store: JSON.parse(readFileSync(join(BENCH_DIR, "store.json"), "utf8")) as Row, profile, emulated: profile !== "local" },
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
  mkdirSync(WORK, { recursive: true });
  await netDown();
  await prepareRootfs();
  // The job's variant: `all` is the tuned setup, and upstream's defaults beside it on the store
  // as the box runs it; `tuned` and `defaults` one of them, as a job of its own.
  const variantAsked = process.env["BENCH_VARIANT"] ?? "all";
  const setups = variantAsked === "tuned" ? [TUNED] : variantAsked === "defaults" ? [DEFAULTS] : profile === "local" ? [TUNED, DEFAULTS] : [TUNED];
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
