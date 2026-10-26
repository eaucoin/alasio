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
 *   driver's probe times the first exec and the verify. Warm restores read the node's chunk
 *   cache, which the saves filled; a cold restore starts with the cache removed and the page
 *   cache dropped.
 *
 * Both variants' restores are of the one save, whose two lines each variant reports. With
 * --small-pages, each case also runs on 4 KiB pages, Firecracker's default, its restores
 * prefetching (`4k`).
 *
 *   node bench.ts --work <dir> --profile <p> --out <results.jsonl> --logs <dir> --host <host.json>
 *     --store <store.json> --driver <driver.mjs> --s3 <s3.ts> [--cases 'poller/256 idle/16'] [--runs 5]
 *     [--small-pages]
 */
import { type ChildProcess, execFileSync, spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { appendFileSync, createWriteStream, mkdirSync, readFileSync, rmdirSync, rmSync, writeFileSync } from "node:fs";
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
const CASES: readonly Case[] = [
  { fixture: "idle", sizeMib: 16 },
  ...[200, 256, 1024, 3072].map((sizeMib) => ({ fixture: "poller" as const, sizeMib })),
  { fixture: "fan-out", sizeMib: 110 },
  ...[256, 1024, 3072].map((sizeMib) => ({ fixture: "saturating" as const, sizeMib })),
];
const cases = options.cases?.trim() ? CASES.filter(({ fixture, sizeMib }) => options.cases?.split(/[\s,]+/u).includes(`${fixture}/${sizeMib}`)) : CASES;

/**
 * The variants: on 2 MiB hugetlbfs pages, the restore with and without the hot set prefetched;
 * and, with --small-pages, the restore with it prefetched on Firecracker's default page size,
 * 4 KiB, as a run of its own, whose guest is booted so.
 */
const VARIANTS = ["prefetch", "no-prefetch", "4k"] as const;
type Variant = (typeof VARIANTS)[number];
interface Pages {
  readonly hugePages: "2M" | "None";
  readonly variants: readonly Variant[];
}
const PAGES: readonly Pages[] = [
  { hugePages: "2M", variants: ["prefetch", "no-prefetch"] },
  ...(options["small-pages"] ? [{ hugePages: "None" as const, variants: ["4k" as const] }] : []),
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

/** Runs a program to its end; its stdout. */
const run = (file: string, args: readonly string[]): string => execFileSync(file, args, { encoding: "utf8", maxBuffer: 64 * 1024 ** 2 });

/** The driver's answer to a command. */
function driver(command: string, args: readonly string[]): Record<string, unknown> {
  return JSON.parse(run(process.execPath, [driverPath, command, ...args])) as Record<string, unknown>;
}

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

/** A process started in `cgroupPath`, its output into `log`. */
function startIn(cgroupPath: string, log: string, file: string, args: readonly string[]): ChildProcess {
  const child = spawn("sh", ["-c", `echo $$ > ${cgroupPath}/cgroup.procs && exec "$0" "$@"`, file, ...args], { stdio: ["ignore", "pipe", "pipe"] });
  const out = createWriteStream(log, { flags: "a" });
  child.stdout?.pipe(out);
  child.stderr?.pipe(out);
  return child;
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

  static async start(name: string, log: string): Promise<Firecracker> {
    // One at a time, at a path short enough for a socket's.
    const socket = join(work, "fc.sock");
    rmSync(socket, { force: true });
    const cgroupPath = cgroup(`fc-${name}`);
    const child = startIn(cgroupPath, log, "firecracker", ["--api-sock", socket, "--level", "Warning"]);
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
  memory_peak_bytes: null, memory_bytes: null, workload_rss_bytes: null, overhead_bytes: null,
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

function write(theCase: Case, runIndex: number, line: Line): void {
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
    versions: { system: "firecracker v1.17.0", bayma: "0.15.2", seaweedfs: "4.48", toxiproxy: "2.13.0", guest_kernel: "6.1.188" },
    ci: { run_id: Number(process.env["GITHUB_RUN_ID"] ?? 0), job: process.env["GITHUB_JOB"] ?? "", sha: process.env["GITHUB_SHA"] ?? "" },
    at: new Date().toISOString(),
    details: { pause_check: pauseCheck, ...line.details },
  };
  appendFileSync(option("out"), `${JSON.stringify(result)}\n`);
}

/** One run of a case: its lines, written as each phase ends. */
async function runCase(theCase: Case, runIndex: number, { hugePages, variants }: Pages, checkPause: boolean): Promise<void> {
  const name = `${theCase.fixture}-${theCase.sizeMib}-${runIndex}${hugePages === "2M" ? "" : "-4k"}`;
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
  const say = (variants: readonly Variant[], line: Omit<Line, "variant">) => {
    for (const variant of variants) write(theCase, runIndex, { ...line, variant });
    done.add(line.phase);
  };
  let fc: Firecracker | null = null;
  try {
    attachDisk(attach, token);
    fc = await Firecracker.start(name, log);
    await fc.api("PUT", "/boot-source", { kernel_image_path: join(work, "vmlinux"), boot_args: BOOT_ARGS });
    await fc.api("PUT", "/drives/rootfs", { drive_id: "rootfs", path_on_host: join(work, "rootfs.ext4"), is_root_device: true, is_read_only: true });
    await fc.api("PUT", "/drives/attach", { drive_id: "attach", path_on_host: attach, is_root_device: false, is_read_only: false });
    await fc.api("PUT", "/network-interfaces/eth0", { iface_id: "eth0", host_dev_name: "tap0", guest_mac: "06:00:ac:10:00:02" });
    await fc.api("PUT", "/machine-config", { vcpu_count: 2, mem_size_mib: MEMORY_MIB, huge_pages: hugePages });
    const booted = now();
    await fc.api("PUT", "/actions", { action_type: "InstanceStart" });
    await listening(8765, 120_000);
    const bootMs = ms(booted, now());
    const rate = theCase.fixture === "saturating" ? ["--rate-mib-s", String(host.copy_mib_s)] : [];
    const setup = driver("setup", [...endpoint, "--fixture", theCase.fixture, "--size-mib", String(theCase.sizeMib), "--run-id", name, ...rate]);
    const session = String(setup["session_id"]);
    const fixtureArgs = ["--session", session, "--fixture", theCase.fixture];

    if (checkPause) {
      // The ticker against Firecracker's own pause, once a job (DESIGN.md section 3).
      driver("mark", [...endpoint, ...fixtureArgs]);
      const paused = now();
      await fc.api("PATCH", "/vm", { state: "Paused" });
      await sleep(1000);
      await fc.api("PATCH", "/vm", { state: "Resumed" });
      const resumed = now();
      const gaps = driver("gaps", [...endpoint, ...fixtureArgs]);
      const gap = Number(gaps["max_gap_ms"]);
      pauseCheck = { paused_ms: ms(paused, resumed), max_gap_ms: gap, ok: Math.abs(gap - 1000) <= 20 };
      // A job whose ticker does not see the pause reports nothing.
      if (!pauseCheck["ok"]) throw new PauseCheckFailed(`the ticker saw ${gap} ms of a 1000 ms pause`);
    }

    // save-live
    const live = { mem: join(staging, "live.mem"), state: join(staging, "live.state") };
    driver("mark", [...endpoint, ...fixtureArgs]);
    let t0 = now();
    await fc.api("PATCH", "/vm", { state: "Paused" });
    const paused = now();
    await fc.api("PUT", "/snapshot/create", { snapshot_type: "Diff", snapshot_path: live.state, mem_file_path: live.mem, sync_snapshot_files: false });
    const snapshotted = now();
    await fc.api("PATCH", "/vm", { state: "Resumed" });
    const resumed = now();
    const liveSaved = fcmemSave("live", prefix, live, null, snapshots, cache);
    let t1 = now();
    const gaps = driver("gaps", [...endpoint, ...fixtureArgs]);
    say(variants, {
      phase: "save-live",
      ok: true,
      failure: null,
      metrics: {
        save_total_ms: ms(t0, t1), unavailable_ms: Number(gaps["max_gap_ms"]), pause_reported_ms: ms(paused, resumed),
        slowdown: (gaps["slowdown"] as number | null) ?? null, precopy_passes: 0, precopy_bytes: 0,
        image_bytes: liveSaved["image_bytes"] as number, uploaded_bytes: bucketBytes(`${prefix}/live/`),
      },
      details: { boot_ms: bootMs, setup, gaps, snapshot_ms: ms(paused, snapshotted), fcmem: liveSaved, staged_bytes: stagedBytes(live.mem) },
    });
    rmSync(live.mem);

    // save-stop
    const stop = { mem: join(staging, "stop.mem"), state: join(staging, "stop.state") };
    t0 = now();
    await fc.api("PATCH", "/vm", { state: "Paused" });
    const stopPaused = now();
    await fc.api("PUT", "/snapshot/create", { snapshot_type: "Diff", snapshot_path: stop.state, mem_file_path: stop.mem, sync_snapshot_files: false });
    const stopSnapshotted = now();
    await fc.kill();
    fc = null;
    const stopSaved = fcmemSave("stop", prefix, stop, "live", snapshots, cache);
    t1 = now();
    say(variants, {
      phase: "save-stop",
      ok: true,
      failure: null,
      metrics: {
        save_total_ms: ms(t0, t1), pause_reported_ms: ms(stopPaused, stopSnapshotted), precopy_passes: 0, precopy_bytes: 0,
        image_bytes: stopSaved["image_bytes"] as number, uploaded_bytes: bucketBytes(`${prefix}/stop/`),
      },
      details: { snapshot_ms: ms(stopPaused, stopSnapshotted), fcmem: stopSaved, staged_bytes: stagedBytes(stop.mem) },
    });
    rmSync(stop.mem);
    rmSync(stop.state);
    rmSync(live.state);
    // The workspace as the instance left it, which each restore is given anew (DESIGN.md section 5).
    copySparse(attach, attachSaved);

    // The variants in turn, the first of them alternating by run, so neither always goes first.
    const order = runIndex % 2 === 1 ? variants : [...variants].reverse();
    for (const phase of ["restore-warm", "restore-cold"] as const) {
      for (const variant of order) {
        try {
          const restored = await restore({ name: `${name}-${phase}-${variant}`, log, prefix, cache, attach, attachSaved, endpoint, session, theCase, runId: name, cold: phase === "restore-cold", prefetch: variant !== "no-prefetch" });
          write(theCase, runIndex, { variant, phase, ...restored });
        } catch (error) {
          write(theCase, runIndex, { variant, phase, ok: false, failure: String(error instanceof Error ? error.message : error) });
        }
      }
      done.add(phase);
    }
  } catch (error) {
    if (error instanceof PauseCheckFailed) throw error;
    const failure = String(error instanceof Error ? error.message : error);
    for (const phase of PHASES) if (!done.has(phase)) say(variants, { phase, ok: false, failure: `${phase} not reached: ${failure}` });
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
  readonly name: string;
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
  readonly prefetch: boolean;
}

async function restore(o: RestoreOptions): Promise<Omit<Line, "variant" | "phase">> {
  copySparse(o.attachSaved, o.attach);
  if (o.cold) rmSync(o.cache, { recursive: true, force: true });
  // Nothing written before is still being written back as the restore runs.
  run("sync", []);
  if (o.cold) writeFileSync("/proc/sys/vm/drop_caches", "1");
  const socket = join(work, "uffd.sock");
  const state = join(work, "restore.state");
  const stats = join(work, "uffd.stats.json");
  for (const path of [socket, state, stats]) rmSync(path, { force: true });
  const handlerGroup = cgroup("uffd");

  const t0 = now();
  const handler = startIn(handlerGroup, o.log, join(work, "fcmem"), [
    "serve", "--socket", socket, "--prefix", o.prefix, "--snapshot", "stop", "--state-out", state, "--cache", o.cache, "--stats", stats, ...(o.prefetch ? ["--prefetch"] : []),
  ]);
  const ready = new Promise<void>((resolve, reject) => {
    let said = "";
    handler.stdout?.on("data", (part: Buffer) => {
      said += part.toString();
      if (said.includes("ready\n")) resolve();
    });
    handler.once("exit", (code) => reject(new Error(`the handler ended before it was ready: ${code}`)));
  });
  let fc: Firecracker | null = null;
  // The guest's hugetlbfs pages, charged to the cgroup of the process that faults them in: the
  // handler's, for what it installs, and the instance's, for what Firecracker touches itself.
  const hugetlb = (): number => counter(handlerGroup, "hugetlb.2MB.current") + (fc === null ? 0 : counter(fc.cgroup, "hugetlb.2MB.current"));
  let hugetlbPeak = 0;
  const sampler = setInterval(() => (hugetlbPeak = Math.max(hugetlbPeak, hugetlb())), 20);
  try {
    [fc] = await Promise.all([Firecracker.start(o.name, o.log), ready]);
    const handlerReady = now();
    await fc.api("PUT", "/snapshot/load", { snapshot_path: state, mem_backend: { backend_type: "Uffd", backend_path: socket }, resume_vm: true });
    const loaded = now();
    const probe = driver("probe", [...o.endpoint, "--session", o.session, "--fixture", o.theCase.fixture, "--run-id", o.runId, "--t0-ns", String(t0)]);
    const guest = hugetlb();
    hugetlbPeak = Math.max(hugetlbPeak, guest);
    const handlerRss = Number(/VmRSS:\s+(\d+)/u.exec(readFileSync(`/proc/${handler.pid}/status`, "utf8"))?.[1] ?? 0) * 1024;
    const memoryBytes = counter(fc.cgroup, "memory.current") + guest + handlerRss;
    const fcPeak = counter(fc.cgroup, "memory.peak");
    await fc.kill();
    fc = null;
    await ended(handler);
    const handlerStats = JSON.parse(readFileSync(stats, "utf8")) as Record<string, number>;
    const memoryPeak = fcPeak + hugetlbPeak + Number(handlerStats["max_rss_bytes"]);
    const workload = Number(probe["workload_rss_bytes"]);
    return {
      ok: probe["ok"] as boolean,
      failure: (probe["failure"] as string | null) ?? null,
      metrics: {
        first_exec_ms: probe["first_exec_ms"] as number,
        verify_ms: probe["verify_ms"] as number,
        // The handler installs what the guest touches and the hot set; nothing loads the rest.
        resident_ms: ms(t0, BigInt(String(probe["verify_end_ns"]))),
        resident_how: "touched",
        memory_peak_bytes: memoryPeak,
        memory_bytes: memoryBytes,
        workload_rss_bytes: workload,
        overhead_bytes: memoryBytes - workload,
      },
      correctness: probe["correctness"] as Record<string, unknown>,
      details: {
        handler_ready_ms: ms(t0, handlerReady), load_ms: ms(handlerReady, loaded), handler: handlerStats,
        instance_memory_peak_bytes: fcPeak, instance_hugetlb_peak_bytes: hugetlbPeak, probe,
      },
    };
  } finally {
    clearInterval(sampler);
    if (fc !== null) await fc.kill();
    handler.kill("SIGTERM");
    await ended(handler);
    removeCgroup(handlerGroup);
  }
}

mkdirSync(logs, { recursive: true });
for (let runIndex = 1; runIndex <= runs; runIndex++) {
  for (const [index, theCase] of cases.entries()) {
    console.log(`run ${runIndex} of ${theCase.fixture}/${theCase.sizeMib}`);
    for (const [each, pages] of PAGES.entries()) await runCase(theCase, runIndex, pages, runIndex === 1 && index === 0 && each === 0);
  }
}
