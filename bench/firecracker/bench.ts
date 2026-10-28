/**
 * Firecracker's runs (DESIGN.md section 5), every case's, round-robin, one line of
 * results.jsonl per run and phase. A run boots a microVM of 2 vCPUs and 5 GiB of guest memory
 * on 2 MiB hugetlbfs pages, the session image's filesystem read-only beside the run's /attach
 * disk, and sets the fixture up through bayma in it; then:
 *
 * - save-live: pause, a Diff snapshot (the guest's resident pages) to the runner's disk,
 *   resume; then `fcmem save` puts it into the bucket while the guest runs;
 * - save-stop: pause, a Diff snapshot, the instance ended; `fcmem save` puts it into the bucket
 *   over save-live's, storing only the blocks that changed;
 * - restore-warm and restore-cold of what save-stop wrote, once with the handler prefetching the
 *   hot set (`prefetch`) and once without (`no-prefetch`): `fcmem serve` takes the guest's
 *   faults, a new Firecracker loads the state file with its memory behind the handler, and the
 *   driver's probe times the first exec and the verify. Every restore starts with the page
 *   cache dropped; a warm one reads the node's cache, which holds the memory files the saves
 *   wrote, a cold one starts with the cache removed.
 *
 * Both variants' restores are of the one save, whose lines the first variant reports. With
 * --small-pages, each case also runs on 4 KiB pages, Firecracker's default, its restores
 * prefetching (`4k`).
 *
 *   node bench.ts --work <dir> --profile <p> --out <results.jsonl> --logs <dir> --host <host.json>
 *     --store <store.json> --driver <driver.mjs> --s3 <s3.ts> [--cases 'poller/256 idle/16'] [--runs 5]
 *     [--small-pages]
 */
import { type ChildProcess, execFileSync, spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { appendFileSync, closeSync, mkdirSync, openSync, readFileSync, rmdirSync, rmSync, writeFileSync } from "node:fs";
import { request } from "node:http";
import { connect } from "node:net";
import { join, resolve } from "node:path";
import { parseArgs } from "node:util";

const { values: options } = parseArgs({
  options: {
    work: { type: "string" },
    profile: { type: "string" },
    out: { type: "string" },
    logs: { type: "string" },
    host: { type: "string" },
    store: { type: "string" },
    driver: { type: "string" },
    s3: { type: "string" },
    cases: { type: "string", default: "" },
    runs: { type: "string", default: "5" },
    "small-pages": { type: "boolean", default: false },
    "e2b-reference": { type: "string", default: "" },
  },
});
const option = (name: keyof typeof options): string => {
  const value = options[name];
  if (typeof value !== "string" || value === "") throw new Error(`--${name} is required`);
  return value;
};
const work = resolve(option("work"));
const profile = option("profile");
const logs = option("logs");
const driverPath = option("driver");
const s3Path = option("s3");
const host = JSON.parse(readFileSync(option("host"), "utf8")) as { readonly copy_mib_s: number } & Record<string, unknown>;
const storeProbe = JSON.parse(readFileSync(option("store"), "utf8")) as Record<string, unknown>;
const runs = Number(options.runs);

type Fixture = "idle" | "poller" | "fan-out" | "saturating";
interface Case {
  readonly fixture: Fixture;
  readonly sizeMib: number;
}
/** Every system's cases (DESIGN.md section 7). */
const CASES: readonly Case[] = (JSON.parse(readFileSync(new URL("../common/cases.json", import.meta.url), "utf8")) as readonly { readonly fixture: Fixture; readonly size_mib: number }[])
  .map(({ fixture, size_mib }) => ({ fixture, sizeMib: size_mib }));
const cases = options.cases?.trim() ? CASES.filter(({ fixture, sizeMib }) => options.cases?.split(/[\s,]+/u).includes(`${fixture}/${sizeMib}`)) : CASES;

const VARIANTS = ["prefetch", "no-prefetch", "4k", "fcmem-on-e2b-fork", "fcmem-on-e2b-fork-no-prefetch", "e2b-handler", "e2b-handler-no-prefetch"] as const;
type Variant = (typeof VARIANTS)[number];
/** A restore's handler: fcmem, or E2B's own (e2b/main.go), and whether it prefetches the hot set. */
interface Restorer {
  readonly variant: Variant;
  readonly handler: "fcmem" | "e2b";
  readonly prefetch: boolean;
}
/** How a run's instance is made, and the restores of its save. */
interface Setup {
  readonly suffix: string;
  readonly firecracker: string;
  readonly version: string;
  readonly hugePages: "2M" | "None";
  /** Whether its snapshot API takes sync_snapshot_files, which v1.17.0 added. */
  readonly syncOption: boolean;
  /**
   * Its snapshots: Diff, the resident pages, where the release takes one of hugetlbfs memory
   * without dirty tracking; Full otherwise.
   */
  readonly snapshotType: "Diff" | "Full";
  /** Whether its saves are the system's, and reported; the reference's are only its restores'. */
  readonly reportsSaves: boolean;
  readonly restorers: readonly Restorer[];
  readonly cases: (theCase: Case) => boolean;
}
/**
 * Every case on 2 MiB hugetlbfs pages, restored with and without the hot set prefetched; with
 * --small-pages, each also on Firecracker's default page size, 4 KiB, prefetching (`4k`); with
 * --e2b-reference <its release>, poller/1024 also on E2B's Firecracker, restored by fcmem and by E2B's own
 * handler, each with and without prefetching.
 */
const SETUPS: readonly Setup[] = [
  {
    suffix: "", firecracker: "firecracker", version: "firecracker v1.17.0", hugePages: "2M", syncOption: true, snapshotType: "Diff", reportsSaves: true, cases: () => true,
    restorers: [{ variant: "prefetch", handler: "fcmem", prefetch: true }, { variant: "no-prefetch", handler: "fcmem", prefetch: false }],
  },
  ...(options["small-pages"] ? [{
    suffix: "-4k", firecracker: "firecracker", version: "firecracker v1.17.0", hugePages: "None" as const, syncOption: true, snapshotType: "Diff" as const, reportsSaves: true, cases: () => true,
    restorers: [{ variant: "4k" as const, handler: "fcmem" as const, prefetch: true }],
  }] : []),
  ...(options["e2b-reference"] ? [{
    suffix: "-e2b", firecracker: join(work, "firecracker-e2b"), version: `e2b-dev/fc-versions ${options["e2b-reference"]}`, hugePages: "2M" as const, syncOption: false,
    // Its v1.14's mincore bitmap of hugetlbfs memory is short of the slot ("Dirty bitmap is
    // smaller than the slot size"), so its snapshots are Full, which fcmem stores the same.
    snapshotType: "Full" as const, reportsSaves: false,
    cases: ({ fixture, sizeMib }: Case) => fixture === "poller" && sizeMib === 1024,
    restorers: [
      { variant: "fcmem-on-e2b-fork" as const, handler: "fcmem" as const, prefetch: true },
      { variant: "fcmem-on-e2b-fork-no-prefetch" as const, handler: "fcmem" as const, prefetch: false },
      { variant: "e2b-handler" as const, handler: "e2b" as const, prefetch: true },
      { variant: "e2b-handler-no-prefetch" as const, handler: "e2b" as const, prefetch: false },
    ],
  }] : []),
];
const PHASES = ["save-live", "save-stop", "restore-warm", "restore-cold"] as const;
type Phase = (typeof PHASES)[number];

/** The guest: its address on the tap, bayma's MCP endpoint there, and its kernel's command line. */
const GUEST = "172.16.0.2";
const BOOT_ARGS = [
  "console=ttyS0", "reboot=k", "panic=1", "pci=off", "quiet", "loglevel=1", "random.trust_cpu=on",
  "root=/dev/vda", "ro", "init=/sbin/bench-init", `ip=${GUEST}::172.16.0.1:255.255.255.252::eth0:off`,
].join(" ");
const MEMORY_MIB = 5120;
const ATTACH_BYTES = 2 * 1024 ** 3;
const CGROUP = "/sys/fs/cgroup/bench";

const now = (): bigint => process.hrtime.bigint();
const ms = (from: bigint, to: bigint): number => Number(to - from) / 1e6;
const sleep = (wait: number) => new Promise<void>((resolve) => setTimeout(resolve, wait));

/** Runs a program to its end, killed if it runs longer than `timeout` ms; its stdout. */
const run = (file: string, args: readonly string[], timeout = 0): string => execFileSync(file, args, { encoding: "utf8", maxBuffer: 64 * 1024 ** 2, timeout, killSignal: "SIGKILL" });

/**
 * How long a driver's command may take: a restore that waits on a fault no handler answers waits
 * for ever, and is a failure of its own rather than the job's.
 */
const DRIVER_TIMEOUT_MS = 600_000;

/** The driver's answer to a command. */
function driver(command: string, args: readonly string[], timeout = DRIVER_TIMEOUT_MS): Record<string, unknown> {
  return JSON.parse(run(process.execPath, [driverPath, command, ...args], timeout)) as Record<string, unknown>;
}

/** How long after t0 a restore has for its first exec and its every page (DESIGN.md section 5). */
const RESTORE_LIMIT_MS = 300_000;

const s3 = (...args: string[]): Record<string, unknown> => JSON.parse(run(process.execPath, [s3Path, ...args])) as Record<string, unknown>;

/** A cgroup of the bench's own, made anew so its peak counts from now. */
function cgroup(name: string): string {
  const path = join(CGROUP, name);
  removeCgroup(path);
  mkdirSync(path);
  return path;
}

/** Removes a cgroup whose processes have ended, if it is there. */
function removeCgroup(path: string): void {
  try {
    rmdirSync(path);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
  }
}
const counter = (cgroupPath: string, file: string): number => Number(readFileSync(join(cgroupPath, file), "utf8").trim().split(/\s+/u).at(-1));

/**
 * A process started in `cgroupPath`, its stderr, and its stdout unless `readStdout`, written to
 * `log` itself: the harness waits on the driver synchronously, so nothing it would read from a
 * pipe meanwhile is read, and a process whose pipe filled would stop.
 */
function startIn(cgroupPath: string, log: string, file: string, args: readonly string[], readStdout = false): ChildProcess {
  const out = openSync(log, "a");
  try {
    return spawn("sh", ["-c", `echo $$ > ${cgroupPath}/cgroup.procs && exec "$0" "$@"`, file, ...args], { stdio: ["ignore", readStdout ? "pipe" : out, out] });
  } finally {
    closeSync(out);
  }
}

const ended = (child: ChildProcess): Promise<void> =>
  child.exitCode !== null || child.signalCode !== null ? Promise.resolve() : new Promise((resolve) => child.once("exit", () => resolve()));

/** A Firecracker process and its API. */
class Firecracker {
  readonly process: ChildProcess;
  readonly cgroup: string;
  private readonly socket: string;

  private constructor(process: ChildProcess, cgroupPath: string, socket: string) {
    this.process = process;
    this.cgroup = cgroupPath;
    this.socket = socket;
  }

  private static started = 0;

  static async start(binary: string, log: string): Promise<Firecracker> {
    // One at a time, at a path short enough for a socket's.
    const socket = join(work, "fc.sock");
    rmSync(socket, { force: true });
    const cgroupPath = cgroup(`fc-${++Firecracker.started}`);
    const child = startIn(cgroupPath, log, binary, ["--api-sock", socket, "--level", "Warning"]);
    const fc = new Firecracker(child, cgroupPath, socket);
    for (let tries = 0; ; tries++) {
      try {
        await fc.api("GET", "/");
        return fc;
      } catch (error) {
        if (tries > 500) throw error;
        await sleep(2);
      }
    }
  }

  api(method: string, path: string, body?: unknown): Promise<string> {
    return new Promise((resolve, reject) => {
      const payload = body === undefined ? undefined : JSON.stringify(body);
      const req = request({ socketPath: this.socket, method, path, headers: payload === undefined ? {} : { "content-type": "application/json", "content-length": Buffer.byteLength(payload) } }, (res) => {
        let text = "";
        res.setEncoding("utf8");
        res.on("data", (part: string) => (text += part));
        res.on("end", () => ((res.statusCode ?? 500) < 300 ? resolve(text) : reject(new Error(`Firecracker ${method} ${path}: ${res.statusCode} ${text}`))));
      });
      req.on("error", reject);
      req.end(payload);
    });
  }

  async kill(): Promise<void> {
    this.process.kill("SIGKILL");
    await ended(this.process);
    removeCgroup(this.cgroup);
  }
}

/** Waits until something listens at the guest's port. */
async function listening(port: number, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const up = await new Promise<boolean>((resolve) => {
      const socket = connect({ host: GUEST, port, timeout: 1000 });
      socket.once("connect", () => (socket.destroy(), resolve(true)));
      socket.once("error", () => (socket.destroy(), resolve(false)));
      socket.once("timeout", () => (socket.destroy(), resolve(false)));
    });
    if (up) return;
    if (Date.now() > deadline) throw new Error(`nothing listened on ${GUEST}:${port} in ${timeoutMs} ms`);
    await sleep(50);
  }
}

/** The run's /attach disk: an ext4 image holding the token, the env and the workspace's two directories. */
function attachDisk(path: string, token: string): void {
  const tree = `${path}.tree`;
  rmSync(tree, { recursive: true, force: true });
  mkdirSync(join(tree, "ws", "workspace"), { recursive: true });
  mkdirSync(join(tree, "ws", "home"), { recursive: true });
  writeFileSync(join(tree, "env"), "");
  writeFileSync(join(tree, "token"), token);
  run("chown", ["-R", "1000:1000", tree]);
  rmSync(path, { force: true });
  writeFileSync(path, "");
  run("truncate", ["-s", String(ATTACH_BYTES), path]);
  run("mkfs.ext4", ["-q", "-F", "-E", "root_owner=1000:1000", "-d", tree, path]);
  rmSync(tree, { recursive: true, force: true });
}

const nullMetrics = {
  save_total_ms: null, unavailable_ms: null, pause_reported_ms: null, slowdown: null, precopy_passes: null, precopy_bytes: null,
  image_bytes: null, uploaded_bytes: null, first_exec_ms: null, verify_ms: null, resident_ms: null, resident_how: null,
  memory_peak_bytes: null, memory_bytes: null, workload_rss_bytes: null,
};
const nullCorrectness = { marker: null, memory: null, children: null, file: null, socket: null, running: null, notice: null };

interface Line {
  readonly variant: Variant;
  readonly phase: Phase;
  readonly ok: boolean;
  readonly failure: string | null;
  readonly metrics?: Partial<typeof nullMetrics> | Record<string, number | string | null>;
  readonly correctness?: Record<string, unknown>;
  readonly details?: Record<string, unknown>;
}

let pauseCheck: Record<string, unknown> | null = null;
class PauseCheckFailed extends Error {}

function write(theCase: Case, runIndex: number, setup: Setup, line: Line): void {
  const result = {
    schema: 1,
    system: "firecracker",
    variant: line.variant,
    target: "host",
    fixture: theCase.fixture,
    size_mib: theCase.sizeMib,
    profile,
    emulated: profile !== "local",
    run: runIndex,
    phase: line.phase,
    ok: line.ok,
    failure: line.failure,
    metrics: { ...nullMetrics, ...line.metrics },
    correctness: { ...nullCorrectness, ...line.correctness },
    host,
    store: storeProbe,
    versions: { system: setup.version, bayma: "0.15.2", seaweedfs: "4.48", toxiproxy: "2.13.0", guest_kernel: "6.1.188" },
    ci: { run_id: Number(process.env["GITHUB_RUN_ID"] ?? 0), job: process.env["GITHUB_JOB"] ?? "", sha: process.env["GITHUB_SHA"] ?? "" },
    at: new Date().toISOString(),
    details: { pause_check: pauseCheck, ...line.details },
  };
  appendFileSync(option("out"), `${JSON.stringify(result)}\n`);
}

/** One run of a case: its lines, written as each phase ends. */
async function runCase(theCase: Case, runIndex: number, setup: Setup, checkPause: boolean): Promise<void> {
  const name = `${theCase.fixture}-${theCase.sizeMib}-${runIndex}${setup.suffix}`;
  const variants = setup.restorers.map(({ variant }) => variant);
  const log = join(logs, `${name}.log`);
  const prefix = `firecracker/${process.env["GITHUB_RUN_ID"] ?? "local"}/${name}`;
  const cache = join(work, "cache");
  const snapshots = join(work, "snapshots");
  const staging = join(work, "staging");
  mkdirSync(staging, { recursive: true });
  const attach = join(work, "attach.ext4");
  const attachSaved = join(work, "attach.saved.ext4");
  const token = randomBytes(32).toString("hex");
  const endpoint = ["--url", `http://${GUEST}:8765/mcp`, "--header", `Authorization: Bearer ${token}`];
  const done = new Set<Phase>();
  // A save is one, whichever variants restore it: its lines go under the first (DESIGN.md section 6).
  const savedBy = variants.slice(0, 1);
  const say = (variants: readonly Variant[], line: Omit<Line, "variant">) => {
    if (setup.reportsSaves || !line.phase.startsWith("save")) for (const variant of variants) write(theCase, runIndex, setup, { ...line, variant });
    done.add(line.phase);
  };
  let fc: Firecracker | null = null;
  try {
    attachDisk(attach, token);
    fc = await Firecracker.start(setup.firecracker, log);
    await fc.api("PUT", "/boot-source", { kernel_image_path: join(work, "vmlinux"), boot_args: BOOT_ARGS });
    await fc.api("PUT", "/drives/rootfs", { drive_id: "rootfs", path_on_host: join(work, "rootfs.ext4"), is_root_device: true, is_read_only: true });
    await fc.api("PUT", "/drives/attach", { drive_id: "attach", path_on_host: attach, is_root_device: false, is_read_only: false });
    await fc.api("PUT", "/network-interfaces/eth0", { iface_id: "eth0", host_dev_name: "tap0", guest_mac: "06:00:ac:10:00:02" });
    await fc.api("PUT", "/machine-config", { vcpu_count: 2, mem_size_mib: MEMORY_MIB, huge_pages: setup.hugePages });
    const booted = now();
    await fc.api("PUT", "/actions", { action_type: "InstanceStart" });
    await listening(8765, 120_000);
    const bootMs = ms(booted, now());
    const rate = theCase.fixture === "saturating" ? ["--rate-mib-s", String(host.copy_mib_s)] : [];
    const installed = driver("setup", [...endpoint, "--fixture", theCase.fixture, "--size-mib", String(theCase.sizeMib), "--run-id", name, ...rate]);
    const session = String(installed["session_id"]);
    const fixtureArgs = ["--session", session, "--fixture", theCase.fixture];

    if (checkPause) {
      // The ticker against Firecracker's own pause, once a job (DESIGN.md section 3).
      driver("mark", [...endpoint, ...fixtureArgs]);
      const pausing = now();
      await fc.api("PATCH", "/vm", { state: "Paused" });
      const paused = now();
      await sleep(1000);
      const resuming = now();
      await fc.api("PATCH", "/vm", { state: "Resumed" });
      const resumed = now();
      const gaps = driver("gaps", [...endpoint, ...fixtureArgs]);
      const gap = Number(gaps["max_gap_ms"]);
      // At least the time between the pause's answer and the resume's ask, at most the time
      // between the pause's ask and the resume's answer, 20 ms either side.
      const [least, most] = [ms(paused, resuming) - 20, ms(pausing, resumed) + 20];
      pauseCheck = { max_gap_ms: gap, least_ms: least, most_ms: most, ok: gap >= least && gap <= most };
      // A job whose ticker does not see the pause reports nothing.
      if (!pauseCheck["ok"]) throw new PauseCheckFailed(`the ticker saw ${gap} ms of a pause it should see as ${least} to ${most} ms`);
    }

    // save-live
    const live = { mem: join(staging, "live.mem"), state: join(staging, "live.state") };
    driver("mark", [...endpoint, ...fixtureArgs]);
    let t0 = now();
    await fc.api("PATCH", "/vm", { state: "Paused" });
    const paused = now();
    await fc.api("PUT", "/snapshot/create", snapshotOf(setup, live));
    const snapshotted = now();
    await fc.api("PATCH", "/vm", { state: "Resumed" });
    const resumed = now();
    const liveStaged = stagedBytes(live.mem);
    const liveSaved = fcmemSave("live", prefix, live, null, snapshots, cache);
    let t1 = now();
    const gaps = driver("gaps", [...endpoint, ...fixtureArgs]);
    say(savedBy, {
      phase: "save-live",
      ok: true,
      failure: null,
      metrics: {
        save_total_ms: ms(t0, t1), unavailable_ms: Number(gaps["max_gap_ms"]), pause_reported_ms: ms(paused, resumed),
        slowdown: (gaps["slowdown"] as number | null) ?? null, precopy_passes: 0, precopy_bytes: 0,
        image_bytes: liveSaved["image_bytes"] as number, uploaded_bytes: bucketBytes(`${prefix}/live/`),
      },
      details: { boot_ms: bootMs, setup: installed, gaps, snapshot_ms: ms(paused, snapshotted), fcmem: liveSaved, staged_bytes: liveStaged, restored_by: variants },
    });

    // save-stop
    const stop = { mem: join(staging, "stop.mem"), state: join(staging, "stop.state") };
    t0 = now();
    await fc.api("PATCH", "/vm", { state: "Paused" });
    const stopPaused = now();
    await fc.api("PUT", "/snapshot/create", snapshotOf(setup, stop));
    const stopSnapshotted = now();
    await fc.kill();
    fc = null;
    const stopStaged = stagedBytes(stop.mem);
    const stopSaved = fcmemSave("stop", prefix, stop, "live", snapshots, cache);
    t1 = now();
    say(savedBy, {
      phase: "save-stop",
      ok: true,
      failure: null,
      metrics: {
        save_total_ms: ms(t0, t1), pause_reported_ms: ms(stopPaused, stopSnapshotted), precopy_passes: 0, precopy_bytes: 0,
        image_bytes: stopSaved["image_bytes"] as number, uploaded_bytes: bucketBytes(`${prefix}/stop/`),
      },
      details: { snapshot_ms: ms(stopPaused, stopSnapshotted), fcmem: stopSaved, staged_bytes: stopStaged, restored_by: variants },
    });
    rmSync(stop.state);
    rmSync(live.state);
    // The workspace as the instance left it, which each restore is given anew (DESIGN.md section 5).
    copySparse(attach, attachSaved);

    // The variants in turn, the first of them alternating by run, so neither always goes first.
    if (setup.restorers.some(({ handler }) => handler === "e2b")) run(join(work, "e2b-serve"), ["export", "--prefix", prefix, "--snapshot", "stop"]);
    const order = runIndex % 2 === 1 ? setup.restorers : [...setup.restorers].reverse();
    for (const phase of ["restore-warm", "restore-cold"] as const) {
      for (const restorer of order) {
        const { variant } = restorer;
        try {
          const restored = await restore({ restorer, firecracker: setup.firecracker, log, prefix, cache, attach, attachSaved, endpoint, session, theCase, runId: name, cold: phase === "restore-cold" });
          write(theCase, runIndex, setup, { variant, phase, ...restored });
        } catch (error) {
          write(theCase, runIndex, setup, { variant, phase, ok: false, failure: String(error instanceof Error ? error.message : error) });
        }
      }
      done.add(phase);
    }
  } catch (error) {
    if (error instanceof PauseCheckFailed) throw error;
    const failure = String(error instanceof Error ? error.message : error);
    for (const phase of PHASES) if (!done.has(phase)) say(phase.startsWith("save") ? savedBy : variants, { phase, ok: false, failure: `${phase} not reached: ${failure}` });
  } finally {
    if (fc !== null) await fc.kill();
    s3("delete", `${prefix}/`);
    rmSync(cache, { recursive: true, force: true });
    rmSync(snapshots, { recursive: true, force: true });
    rmSync(staging, { recursive: true, force: true });
    rmSync(attach, { force: true });
    rmSync(attachSaved, { force: true });
  }
}

/** A snapshot to the runner's disk, its files left in the page cache where the release lets them be. */
const snapshotOf = (setup: Setup, files: { readonly mem: string; readonly state: string }) => ({
  snapshot_type: setup.snapshotType, snapshot_path: files.state, mem_file_path: files.mem, ...(setup.syncOption ? { sync_snapshot_files: false } : {}),
});

function fcmemSave(id: string, prefix: string, files: { readonly mem: string; readonly state: string }, parent: string | null, snapshots: string, cache: string): Record<string, unknown> {
  const args = ["save", "--mem", files.mem, "--state", files.state, "--prefix", prefix, "--snapshot", id, "--work", snapshots, "--cache", cache, ...(parent === null ? [] : ["--parent", parent])];
  return JSON.parse(run(join(work, "fcmem"), args)) as Record<string, unknown>;
}

/** Copies a disk image, its holes kept holes. */
const copySparse = (from: string, to: string): void => void run("cp", ["--sparse=always", from, to]);

const bucketBytes = (prefix: string): number => Number(s3("bytes", prefix)["bytes"]);

/** The bytes the memory file holds on the disk, its holes not counted. */
const stagedBytes = (path: string): number => Number(run("du", ["--block-size=1", path]).split(/\s+/u)[0]);

interface RestoreOptions {
  readonly restorer: Restorer;
  readonly firecracker: string;
  readonly log: string;
  readonly prefix: string;
  readonly cache: string;
  readonly attach: string;
  readonly attachSaved: string;
  readonly endpoint: readonly string[];
  readonly session: string;
  readonly theCase: Case;
  readonly runId: string;
  readonly cold: boolean;
}

async function restore(o: RestoreOptions): Promise<Omit<Line, "variant" | "phase">> {
  copySparse(o.attachSaved, o.attach);
  if (o.cold) rmSync(o.cache, { recursive: true, force: true });
  // Every restore starts with nothing being written back and nothing in the page cache, so a
  // warm one reads the node's copy from its disk, as alasio's reads its node cache with O_DIRECT,
  // whatever ran before it.
  run("sync", []);
  writeFileSync("/proc/sys/vm/drop_caches", "1");
  const socket = join(work, "uffd.sock");
  const state = join(work, "restore.state");
  const stats = join(work, "uffd.stats.json");
  for (const path of [socket, state, stats]) rmSync(path, { force: true });
  const handlerGroup = cgroup("uffd");
  const { handler: kind, prefetch } = o.restorer;
  const serve = ["serve", "--socket", socket, "--prefix", o.prefix, "--snapshot", "stop", "--state-out", state, "--stats", stats, ...(prefetch ? ["--prefetch"] : [])];
  const startHandler = (args: readonly string[]): { readonly process: ChildProcess; readonly ready: Promise<void> } => {
    // Its stdout says only that it is ready; it logs to its stderr.
    const child = startIn(handlerGroup, o.log, join(work, kind === "fcmem" ? "fcmem" : "e2b-serve"), args, true);
    const ready = new Promise<void>((resolve, reject) => {
      let said = "";
      child.stdout?.on("data", (part: Buffer) => {
        said += part.toString();
        if (said.includes("ready\n")) resolve();
      });
      child.once("exit", (code) => reject(new Error(`the handler ended before it was ready: ${code}`)));
    });
    return { process: child, ready };
  };
  // E2B's cache lives as long as its process, an orchestrator that holds the snapshots it took:
  // warm, it is started and has read every block before t0; cold, after, as fcmem always is.
  const e2bCache = join(work, "e2b-cache");
  rmSync(e2bCache, { recursive: true, force: true });
  const early = kind === "e2b" && !o.cold ? startHandler([...serve, "--cache", e2bCache, "--warm"]) : null;
  if (early !== null) await early.ready;

  const t0 = now();
  const { process: handler, ready } = early ?? startHandler([...serve, "--cache", kind === "fcmem" ? o.cache : e2bCache]);
  let fc: Firecracker | null = null;
  // The guest's hugetlbfs pages, charged to the cgroup of the process that faults them in: the
  // handler's, for what it installs, and the instance's, for what Firecracker touches itself.
  const hugetlb = (): number => counter(handlerGroup, "hugetlb.2MB.current") + (fc === null ? 0 : counter(fc.cgroup, "hugetlb.2MB.current"));
  let hugetlbPeak = 0;
  const sampler = setInterval(() => (hugetlbPeak = Math.max(hugetlbPeak, hugetlb())), 20);
  try {
    [fc] = await Promise.all([Firecracker.start(o.firecracker, o.log), ready]);
    const handlerReady = now();
    // E2B's handler gets guest memory as E2B runs it, a memfd whose descriptor comes with the
    // userfaultfd (its use-memfd flag, on by default, ff:230), which only E2B's Firecracker has.
    const memfd = kind === "e2b" ? { use_memfd: true } : {};
    await fc.api("PUT", "/snapshot/load", { snapshot_path: state, mem_backend: { backend_type: "Uffd", backend_path: socket, ...memfd }, resume_vm: true });
    const loaded = now();
    let probe: Record<string, unknown>;
    try {
      probe = driver("probe", [...o.endpoint, "--session", o.session, "--fixture", o.theCase.fixture, "--run-id", o.runId, "--t0-ns", String(t0)], Math.max(1, RESTORE_LIMIT_MS - ms(t0, now())));
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ETIMEDOUT") throw error;
      throw new Error(`the first exec and the verify had not both answered ${RESTORE_LIMIT_MS / 1000} s after t0`);
    }
    const guest = hugetlb();
    hugetlbPeak = Math.max(hugetlbPeak, guest);
    const handlerRss = Number(/VmRSS:\s+(\d+)/u.exec(readFileSync(`/proc/${handler.pid}/status`, "utf8"))?.[1] ?? 0) * 1024;
    const memoryBytes = counter(fc.cgroup, "memory.current") + guest + handlerRss;
    const fcPeak = counter(fc.cgroup, "memory.peak");
    await fc.kill();
    fc = null;
    // fcmem ends with its Firecracker; E2B's handler is told to.
    handler.kill("SIGTERM");
    await ended(handler);
    const handlerStats = JSON.parse(readFileSync(stats, "utf8")) as Record<string, number>;
    const memoryPeak = fcPeak + hugetlbPeak + Number(handlerStats["max_rss_bytes"]);
    // The handler loads only what the guest touches, so its pages are all in once the verify,
    // which reads every page, has answered.
    const residentMs = ms(t0, BigInt(String(probe["verify_end_ns"])));
    const late = residentMs > RESTORE_LIMIT_MS ? [`the pages were not all resident ${RESTORE_LIMIT_MS / 1000} s after t0`] : [];
    const failures = [...(probe["failure"] === null || probe["failure"] === undefined ? [] : [String(probe["failure"])]), ...late];
    return {
      ok: probe["ok"] === true && late.length === 0,
      failure: failures.length === 0 ? null : failures.join("; "),
      metrics: {
        first_exec_ms: probe["first_exec_ms"] as number,
        verify_ms: probe["verify_ms"] as number,
        resident_ms: residentMs,
        resident_how: "touched",
        memory_peak_bytes: memoryPeak,
        memory_bytes: memoryBytes,
        workload_rss_bytes: Number(probe["workload_rss_bytes"]),
      },
      correctness: probe["correctness"] as Record<string, unknown>,
      details: {
        handler_ready_ms: ms(t0, handlerReady), load_ms: ms(handlerReady, loaded), handler: handlerStats,
        instance_memory_peak_bytes: fcPeak, instance_hugetlb_peak_bytes: hugetlbPeak, probe,
      },
    };
  } catch (error) {
    // What the handler was doing when the restore failed, into the run's log: a Go program
    // prints every goroutine's stack as SIGQUIT ends it.
    handler.kill("SIGQUIT");
    throw error;
  } finally {
    clearInterval(sampler);
    if (fc !== null) await fc.kill();
    handler.kill("SIGTERM");
    await ended(handler);
    removeCgroup(handlerGroup);
    rmSync(e2bCache, { recursive: true, force: true });
  }
}

mkdirSync(logs, { recursive: true });
for (let runIndex = 1; runIndex <= runs; runIndex++) {
  for (const [index, theCase] of cases.entries()) {
    console.log(`run ${runIndex} of ${theCase.fixture}/${theCase.sizeMib}`);
    for (const [each, setup] of SETUPS.entries()) {
      if (setup.cases(theCase)) await runCase(theCase, runIndex, setup, runIndex === 1 && index === 0 && each === 0);
    }
  }
}
