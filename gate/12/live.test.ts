/**
 * 12's first gate: live checkpoints under bayma, with the fork's fix of a thread's new malloc
 * arena. bayma, as alasio's session pod runs it (cli/src/manifests/alasio.ts), in a gVisor
 * sandbox of the release alasio pins, under Docker and gVisor's shim, its options file's
 * `[runsc_config]` that of a session's runtime with 12's dirty tracking; a session in each of
 * bayma's eight runtimes. Each cycle runs an exec in every runtime that, for WAVE_MS, keeps
 * starting threads that allocate and end, each a new arena, and checkpoints the sandbox while
 * they run, beside the shim, as the node agent will: `--leave-running`, pre-copied, throttled,
 * a delta of the checkpoint before it (a full one every CHAIN). After each, every exec must end
 * with its answer, every session still hold its state, and the sandbox still run.
 *
 * It needs root through sudo, Docker with the runtime `runsc` (the release's shim, its options
 * in RUNSC_OPTIONS), the release's runsc at RUNSC, the image GATE_IMAGE (sandbox/agent, built),
 * and writes its record to OUT/evidence.json: gate-12-live.yml sets them up. CHECKPOINTS is how
 * many this run takes; CHAIN, PRECOPY and PROFILE_EVERY vary it, to tell what a failure comes from.
 */
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";

import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import type { CallToolResult, ContentBlock } from "@modelcontextprotocol/sdk/types.js";

import { runscFlags } from "../../src/node-agent/runsc.ts";

const IMAGE = process.env["GATE_IMAGE"] ?? "alasio-agent:gate";
const RUNSC = process.env["RUNSC"] ?? "runsc";
const RUNSC_OPTIONS = process.env["RUNSC_OPTIONS"] ?? "/etc/containerd/runsc.toml";
const OUT = process.env["OUT"] ?? mkdtempSync(join(tmpdir(), "gate-12-"));
const CHECKPOINTS = Number(process.env["CHECKPOINTS"] ?? 1000);
/** Where runsc keeps the state of the sandboxes Docker's containerd starts through gVisor's shim. */
const RUNSC_ROOT = "/run/containerd/runsc/moby";
/** The longest chain of deltas, as 12's compaction keeps them: the next checkpoint is full. 1: every one is. */
const CHAIN = Number(process.env["CHAIN"] ?? 16);
/** Whether checkpoints pre-copy, as live ones will: PRECOPY=off takes them without. */
const PRECOPY = process.env["PRECOPY"] !== "off";
/** The runtime of Docker's that runs the sandbox: runsc, or runc to compare the workload's memory without gVisor (no checkpoints then). */
const SANDBOX_RUNTIME = process.env["SANDBOX_RUNTIME"] ?? "runsc";
/** Whether it checkpoints at all: CHECKPOINTING=off runs the same cycles without, to tell what the workload holds from what checkpoints do. */
const CHECKPOINTING = process.env["CHECKPOINTING"] !== "off" && SANDBOX_RUNTIME === "runsc";
/** Every how many checkpoints the Sentry's heap profile is written to OUT (0: never); runsc's `profile` must be on. */
const PROFILE_EVERY = Number(process.env["PROFILE_EVERY"] ?? 0);
/** The checkpoints the Sentry's memory settles over, which its trend leaves out. */
const WARM_UP = 10;
/**
 * The most the Sentry's own memory may grow by a checkpoint, as its trend over the rest says:
 * one that keeps what a checkpoint took ends the sandbox by the host's OOM killer (160 MB a
 * checkpoint, run 38744966370).
 */
const MAX_SENTRY_GROWTH_BYTES = 2 * 1024 ** 2;
/** How long each cycle's execs run, starting threads. */
const WAVE_MS = 3000;
/** bayma's port in the session pod. */
const PORT = 8080;

/** The runtimes bayma serves, as alasio's sessions may use them. */
const RUNTIMES = ["bun", "python", "dotnet-script", "rust", "c", "cpp", "lean", "go"] as const;
type Runtime = (typeof RUNTIMES)[number];
/** The runtimes this run gives sessions: GATE_RUNTIMES, a list of them, or all. */
const ACTIVE: readonly Runtime[] = process.env["GATE_RUNTIMES"] ? RUNTIMES.filter((runtime) => process.env["GATE_RUNTIMES"]!.split(",").includes(runtime)) : RUNTIMES;

/**
 * In each runtime: what leaves 41 in a live variable and reads it plus one, and `wave(ms)`,
 * defined once, which for `ms` keeps starting threads that each allocate and end, each a new
 * thread's malloc arena where the runtime's allocator is glibc's, and answers 7.
 */
const RUNTIME_CODE: Readonly<Record<Runtime, { readonly define: string; readonly read: string; readonly wave: string; readonly run: (ms: number) => string }>> = {
  bun: {
    define: "const marker = 41",
    read: "marker + 1",
    wave: `globalThis.gateWave = async (ms) => {
  const end = Date.now() + ms;
  const source = URL.createObjectURL(new Blob(["postMessage(new Uint8Array(1 << 20).fill(7)[0]);"], { type: "application/javascript" }));
  let held = [];
  while (Date.now() < end) {
    const workers = Array.from({ length: 2 }, () => new Worker(source));
    await Promise.all(workers.map((worker) => new Promise((resolve) => { worker.onmessage = resolve; })));
    for (const worker of workers) worker.terminate();
    held.push(new Uint8Array(1 << 20).fill(7));
    if (held.length > 32) held = [];
    await Bun.sleep(5);
  }
  return 7;
}`,
    run: (ms) => `await gateWave(${ms})`,
  },
  python: {
    define: "marker = 41",
    read: "marker + 1",
    wave: `import threading, time
def gate_wave(ms):
    end = time.monotonic() + ms / 1000
    held = []
    while time.monotonic() < end:
        threads = [threading.Thread(target=lambda: bytearray(1 << 20)) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        held.append(bytearray(1 << 20))
        if len(held) > 32:
            held.clear()
        time.sleep(0.005)
    return 7`,
    run: (ms) => `gate_wave(${ms})`,
  },
  "dotnet-script": {
    define: "var marker = 41;",
    read: "marker + 1",
    wave: `int GateWave(int ms)
{
    var end = DateTime.UtcNow.AddMilliseconds(ms);
    var held = new List<byte[]>();
    while (DateTime.UtcNow < end)
    {
        var threads = new List<System.Threading.Thread>();
        for (var i = 0; i < 4; i++) threads.Add(new System.Threading.Thread(() => { var block = new byte[1 << 20]; block[0] = 7; }));
        foreach (var thread in threads) thread.Start();
        foreach (var thread in threads) thread.Join();
        held.Add(new byte[1 << 20]);
        if (held.Count > 32) held.Clear();
        System.Threading.Thread.Sleep(5);
    }
    return 7;
}`,
    run: (ms) => `GateWave(${ms})`,
  },
  rust: {
    define: "let marker = 41;",
    read: "marker + 1",
    wave: `fn gate_wave(ms: u64) -> i64 {
    let end = std::time::Instant::now() + std::time::Duration::from_millis(ms);
    let mut held: Vec<Vec<u8>> = Vec::new();
    while std::time::Instant::now() < end {
        let threads: Vec<_> = (0..4).map(|_| std::thread::spawn(|| vec![7u8; 1 << 20].len())).collect();
        for thread in threads {
            thread.join().unwrap();
        }
        held.push(vec![7u8; 1 << 20]);
        if held.len() > 32 {
            held.clear();
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    7
}`,
    run: (ms) => `gate_wave(${ms})`,
  },
  c: {
    define: "int marker = 41;",
    read: "marker + 1",
    wave: `#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
static void *gate_block(void *unused) { char *block = malloc(1 << 20); memset(block, 7, 1 << 20); free(block); return unused; }
static long gate_now_ms(void) { struct timespec now; clock_gettime(CLOCK_MONOTONIC, &now); return now.tv_sec * 1000L + now.tv_nsec / 1000000L; }
int gate_wave(int ms) {
  long end = gate_now_ms() + ms;
  while (gate_now_ms() < end) {
    pthread_t threads[4];
    for (int i = 0; i < 4; i++) pthread_create(&threads[i], NULL, gate_block, NULL);
    for (int i = 0; i < 4; i++) pthread_join(threads[i], NULL);
    usleep(5000);
  }
  return 7;
}`,
    run: (ms) => `gate_wave(${ms})`,
  },
  cpp: {
    define: "int marker = 41;",
    read: "marker + 1",
    wave: `#include <chrono>
#include <thread>
#include <vector>
int gate_wave(int ms) {
  auto end = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
  std::vector<std::vector<char>> held;
  while (std::chrono::steady_clock::now() < end) {
    std::vector<std::thread> threads;
    for (int i = 0; i < 4; i++) threads.emplace_back([] { std::vector<char> block(1 << 20, 7); });
    for (auto &thread : threads) thread.join();
    held.emplace_back(1 << 20, 7);
    if (held.size() > 32) held.clear();
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  return 7;
}`,
    run: (ms) => `gate_wave(${ms})`,
  },
  lean: {
    define: "def marker := 41",
    read: "#eval marker + 1",
    wave: `def gateWave (ms : Nat) : IO Nat := do
  let stop := (← IO.monoMsNow) + ms
  repeat
    if (← IO.monoMsNow) ≥ stop then break
    let tasks ← (List.range 4).mapM fun i => IO.asTask (prio := .dedicated) (pure (List.replicate 100000 i).length)
    for task in tasks do
      discard <| IO.wait task
    IO.sleep 5
  return 7`,
    run: (ms) => `#eval gateWave ${ms}`,
  },
  go: {
    define: "var marker = 41",
    read: "marker + 1",
    // A goroutine that locks its thread and ends takes the thread with it: the next is new.
    wave: `import (
	"runtime"
	"sync"
	"time"
)

func gateWave(ms int) int {
	end := time.Now().Add(time.Duration(ms) * time.Millisecond)
	for time.Now().Before(end) {
		var wg sync.WaitGroup
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				runtime.LockOSThread()
				block := make([]byte, 1<<20)
				for j := range block {
					block[j] = 7
				}
			}()
		}
		wg.Wait()
		time.Sleep(5 * time.Millisecond)
	}
	return 7
}`,
    run: (ms) => `gateWave(${ms})`,
  },
};

/** What an exec of bayma's answers, as far as the gate reads it. */
interface ExecSnapshot {
  readonly exec_id: string;
  readonly status: string;
  readonly done: boolean;
  readonly next_seq: number;
  readonly result_text: string;
  readonly error_text: string;
}

/** One checkpoint, as the record keeps it. */
interface Cycle {
  readonly index: number;
  /** Its place in its chain: 0 for a full checkpoint. */
  readonly depth: number;
  /** How long after the execs began it began. */
  readonly afterMs: number;
  /** How long runsc took, the hook included. */
  readonly checkpointMs: number;
  /** The image's size on the disk. */
  readonly bytes: number;
  /** What the disk holding the images has free after it. */
  readonly diskFreeBytes: number;
  /** The host's available memory after it (MemAvailable). */
  readonly memoryAvailableBytes: number;
  /** The Sentry's memory after it: its own (anonymous), the sandbox's memory file it maps (shared), and swapped. */
  readonly sentry: SentryMemory;
  /** The sandbox's memory, as its /proc/meminfo says it, by kind, in bytes. */
  readonly sandbox: Readonly<Record<string, number>>;
  /** Each process of the container: its resident memory, in bytes, and its command line. */
  readonly processes: readonly { readonly rssBytes: number; readonly command: string }[];
}

/** Each process of the container, as its /proc says it: its resident memory and its command line. */
async function processesOf(name: string): Promise<{ rssBytes: number; command: string }[]> {
  const listed = await run("docker", ["exec", name, "sh", "-c", 'for d in /proc/[0-9]*; do r=$(sed -n "s/^VmRSS:[^0-9]*\\([0-9]*\\) kB$/\\1/p" "$d/status" 2>/dev/null); c=$(tr "\\0" " " 2>/dev/null < "$d/cmdline" | cut -c1-160); [ -n "$r" ] && echo "$r $c"; done; true']);
  return listed.split("\n").filter(Boolean).map((line) => {
    const [rss, ...command] = line.split(" ");
    return { rssBytes: Number(rss) * 1024, command: command.join(" ") };
  });
}

/** The sandbox's /proc/meminfo, as the Sentry accounts its memory, of the fields that say by kind. */
async function sandboxMemory(name: string): Promise<Record<string, number>> {
  const meminfo = await run("docker", ["exec", name, "cat", "/proc/meminfo"]);
  return Object.fromEntries(
    [...meminfo.matchAll(/^(MemTotal|MemFree|Cached|Shmem|AnonPages|Mapped|Active\(file\)|Inactive\(file\)):\s+(\d+) kB$/gmu)].map(([, field, kib]) => [field!, Number(kib) * 1024]),
  );
}

interface SentryMemory {
  readonly anonBytes: number;
  readonly sharedBytes: number;
  readonly swapBytes: number;
}

/** The memory of the sandbox's Sentry, the largest of the processes named for it (the others are its stubs). */
async function sentryMemory(): Promise<SentryMemory> {
  const statuses = await run("sudo", ["sh", "-c", "for pid in $(pgrep -x gvisor_sentry); do grep -E '^(VmRSS|RssAnon|RssShmem|VmSwap):' /proc/$pid/status; echo; done"]);
  const kib = (status: string, field: string): number => Number(new RegExp(`^${field}:\\s+(\\d+) kB$`, "mu").exec(status)?.[1] ?? 0) * 1024;
  const sentry = statuses.split("\n\n").reduce((largest, status) => (kib(status, "VmRSS") > kib(largest, "VmRSS") ? status : largest), "");
  return { anonBytes: kib(sentry, "RssAnon"), sharedBytes: kib(sentry, "RssShmem"), swapBytes: kib(sentry, "VmSwap") };
}

/** The slope of the least-squares line through `points`. */
function slope(points: readonly (readonly [number, number])[]): number {
  const n = points.length;
  const mean = (values: readonly number[]) => values.reduce((sum, value) => sum + value, 0) / n;
  const x = mean(points.map(([px]) => px));
  const y = mean(points.map(([, py]) => py));
  return points.reduce((sum, [px, py]) => sum + (px - x) * (py - y), 0) / points.reduce((sum, [px]) => sum + (px - x) ** 2, 0);
}

/** The bytes free on the filesystem of `path`. */
async function diskFree(path: string): Promise<number> {
  return Number((await run("df", ["--block-size=1", "--output=avail", path])).split("\n")[1]);
}

/** The host's available memory, in bytes. */
function memoryAvailable(): number {
  const kib = /^MemAvailable:\s+(\d+) kB$/mu.exec(readFileSync("/proc/meminfo", "utf8"));
  return Number(kib?.[1]) * 1024;
}

function run(command: string, args: readonly string[]): Promise<string> {
  return new Promise((resolve, reject) => {
    execFile(command, args, { maxBuffer: 64 * 1024 * 1024 }, (error, stdout, stderr) => {
      if (error) reject(new Error(`${command} ${args.join(" ")} failed: ${stderr.trim() || error.message}`));
      else resolve(stdout.trim());
    });
  });
}

function text(part: ContentBlock | undefined): string {
  if (part?.type !== "text") throw new Error(`bayma answered without text: ${JSON.stringify(part)}`);
  return part.text;
}

async function call<T>(client: Client, name: string, args: Record<string, unknown>): Promise<T> {
  const result = (await client.callTool({ name, arguments: args }, undefined, { timeout: 600_000 })) as CallToolResult;
  if (result.isError) throw new Error(`bayma's ${name} failed: ${text(result.content[0])}`);
  return (result.structuredContent as T | undefined) ?? JSON.parse(text(result.content.find((part) => part.type === "text")));
}

/** An exec followed until it ends. */
async function settled(client: Client, sessionId: string, snapshot: ExecSnapshot): Promise<ExecSnapshot> {
  let current = snapshot;
  while (!current.done) {
    current = await call<ExecSnapshot>(client, "wait", { session_id: sessionId, exec_id: current.exec_id, from_seq: current.next_seq, yield_time_ms: 60_000 });
  }
  return current;
}

/** Runs `code` in the session until it ends, which must be with `answer`. */
async function answers(client: Client, runtime: Runtime, sessionId: string, code: string, answer?: string): Promise<void> {
  const done = await settled(client, sessionId, await call<ExecSnapshot>(client, "exec", { session_id: sessionId, code, yield_time_ms: 60_000 }));
  assert.deepEqual({ runtime, status: done.status, error: done.error_text, ...(answer === undefined ? {} : { result: done.result_text.trim() }) }, { runtime, status: "ok", error: "", ...(answer === undefined ? {} : { result: answer }) });
}

test(`${CHECKPOINTS} live checkpoints of bayma while execs start threads in each of its runtimes: none aborts, every session answers after each`, { timeout: 4 * 60 * 60_000 }, async () => {
  const flags = runscFlags(readFileSync(RUNSC_OPTIONS, "utf8"));
  // A delta needs dirty tracking.
  assert.ok(CHAIN === 1 || flags.includes("--dirty-tracking=wp"), `${RUNSC_OPTIONS} gives runsc ${flags.join(" ")}`);
  const scratch = mkdtempSync(join(tmpdir(), "gate-12-live-"));
  const home = join(scratch, "home");
  const workspace = join(scratch, "workspace");
  const tokens = join(scratch, "tokens");
  for (const directory of [home, workspace, tokens]) mkdirSync(directory, { recursive: true });
  writeFileSync(join(tokens, "token"), "gate-token\n");
  await run("sudo", ["chown", "-R", "1000:1000", home, workspace]);
  const images = join(scratch, "images");
  mkdirSync(images);
  const name = `gate-12-live-${process.pid}`;
  const cycles: Cycle[] = [];
  let sentryBefore: SentryMemory | undefined;
  /** How much the Sentry's own memory grows by a checkpoint, as its trend after WARM_UP says. */
  let sentryGrowthBytes: number | undefined;
  let failure: string | undefined;
  try {
    // As the session pod runs bayma: its command, arguments, environment and working directory.
    await run("docker", [
      "run", "--detach", "--runtime", SANDBOX_RUNTIME, "--name", name, "--hostname", "bayma",
      "--user", "1000:1000", "--publish", `127.0.0.1::${PORT}`,
      "--env", "HOME=/home/agent", "--env", "USER=agent", "--env", "BAYMA_PAYLOAD_DIR=/opt/bayma/payload",
      "--volume", `${home}:/home/agent`, "--volume", `${workspace}:/workspace`, "--volume", `${tokens}:/run/alasio/bayma:ro`,
      "--workdir", "/workspace", "--entrypoint", "/usr/bin/tini", IMAGE,
      "-s", "--", "node", "/opt/bayma/bayma.js",
      "mcp-http", "--host", "0.0.0.0", "--port", String(PORT), "--state-dir", "/home/agent/.bayma", "--token-file", "/run/alasio/bayma/token",
    ]);
    // The toolbelt the session's prepare links into its home.
    await run("docker", ["exec", name, "sh", "-c", "mkdir -p /home/agent/.local/share/bayma && ln -sfn /opt/bayma-data/bayma/toolbelt /home/agent/.local/share/bayma/toolbelt"]);
    const url = `http://${(await run("docker", ["port", name, String(PORT)])).split("\n")[0]}/mcp`;
    for (let tries = 0; ; tries++) {
      try {
        await fetch(url, { method: "POST" });
        break;
      } catch (error) {
        if (tries > 480) throw error;
        await sleep(250);
      }
    }
    const client = new Client({ name: "gate-12", version: "1.0.0" });
    await client.connect(new StreamableHTTPClientTransport(new URL(url), { requestInit: { headers: { Authorization: "Bearer gate-token" } } }) as Transport);
    // The hook alasio runs in bayma's container as it checkpoints it (src/sandbox/snapshots.ts hookOf).
    const hook = "node /opt/bayma/bayma.js sandbox-hook --state-dir /home/agent/.bayma";
    const id = await run("docker", ["inspect", "--format", "{{.Id}}", name]);

    const sessions = new Map<Runtime, string>();
    for (const runtime of ACTIVE) {
      const { session } = await call<{ session: { session_id: string } }>(client, "session.create", { runtime, title: `gate-${runtime}`, cwd: "/workspace" });
      sessions.set(runtime, session.session_id);
      await answers(client, runtime, session.session_id, RUNTIME_CODE[runtime].define);
      await answers(client, runtime, session.session_id, RUNTIME_CODE[runtime].wave);
      // Once before any checkpoint, so that what compiles is compiled.
      await answers(client, runtime, session.session_id, RUNTIME_CODE[runtime].run(200), "7");
    }

    sentryBefore = await sentryMemory();
    const profile = async (label: string): Promise<void> => {
      if (PROFILE_EVERY > 0) await run("sudo", [RUNSC, `--root=${RUNSC_ROOT}`, ...flags, "debug", `--profile-heap=${join(OUT, `heap-${label}.pprof`)}`, id]);
    };
    mkdirSync(OUT, { recursive: true });
    await profile("before");
    let parent: string | undefined;
    let chain: string[] = [];
    for (let index = 0; index < CHECKPOINTS; index++) {
      const began = Date.now();
      const execs = await Promise.all(ACTIVE.map(async (runtime) => {
        const sessionId = sessions.get(runtime) ?? "";
        return { runtime, sessionId, started: await call<ExecSnapshot>(client, "exec", { session_id: sessionId, code: RUNTIME_CODE[runtime].run(WAVE_MS), yield_time_ms: 0 }) };
      }));
      // Somewhere in the waves, never at the same point twice running.
      await sleep(200 + Math.floor(Math.random() * (WAVE_MS - 1200)));
      const depth = chain.length;
      const image = join(images, String(index));
      const afterMs = Date.now() - began;
      if (CHECKPOINTING) await run("sudo", [
        RUNSC, `--root=${RUNSC_ROOT}`, ...flags, "checkpoint",
        `--image-path=${image}`, "--compression=none", "--direct", "--leave-running",
        ...(PRECOPY ? ["--precopy=auto", "--precopy-throttle=on"] : []),
        ...(parent ? [`--parent-image-path=${parent}`] : []),
        `--save-restore-exec-argv=${hook}`, "--save-restore-exec-timeout=60s",
        id,
      ]);
      const checkpointMs = Date.now() - began - afterMs;
      const bytes = CHECKPOINTING ? Number((await run("sudo", ["du", "-sb", image])).split(/\s/u)[0]) : 0;
      cycles.push({ index, depth, afterMs, checkpointMs, bytes, diskFreeBytes: await diskFree(images), memoryAvailableBytes: memoryAvailable(), sentry: await sentryMemory(), sandbox: await sandboxMemory(name), processes: await processesOf(name) });
      if (PROFILE_EVERY > 0 && (index + 1) % PROFILE_EVERY === 0) await profile(String(index));
      for (const { runtime, sessionId, started } of execs) {
        const done = await settled(client, sessionId, started);
        assert.deepEqual({ index, runtime, status: done.status, error: done.error_text, result: done.result_text.trim() }, { index, runtime, status: "ok", error: "", result: "7" });
      }
      for (const runtime of ACTIVE) await answers(client, runtime, sessions.get(runtime) ?? "", RUNTIME_CODE[runtime].read, "42");
      assert.equal(await run("docker", ["inspect", "--format", "{{.State.Running}}", name]), "true", `after checkpoint ${index}`);
      // The next a delta of this one, unless the chain is as long as compaction lets one be.
      if (chain.length + 1 >= CHAIN) {
        await run("sudo", ["rm", "-rf", ...chain, image]);
        chain = [];
        parent = undefined;
      } else {
        chain.push(image);
        parent = image;
      }
      if (index % 25 === 0) console.log(`checkpoint ${index}: depth ${depth}, ${checkpointMs} ms, ${bytes} bytes, ${cycles.at(-1)?.diskFreeBytes} free on disk, the Sentry's own ${cycles.at(-1)?.sentry.anonBytes} bytes`);
    }
    // The Sentry gives back what each checkpoint takes.
    sentryGrowthBytes = slope(cycles.slice(WARM_UP).map(({ index, sentry }) => [index, sentry.anonBytes]));
    assert.ok(sentryGrowthBytes < MAX_SENTRY_GROWTH_BYTES, `the Sentry's own memory grows by ${Math.round(sentryGrowthBytes)} bytes a checkpoint`);
    await client.close();
  } catch (error) {
    failure = error instanceof Error ? error.message : String(error);
    throw error;
  } finally {
    const logs = await run("docker", ["logs", name]).catch((error: unknown) => String(error));
    const aborts = logs.split("\n").filter((line) => /corrupted|malloc\(\)|double free|SIGABRT/u.test(line));
    mkdirSync(OUT, { recursive: true });
    writeFileSync(join(OUT, "evidence.json"), `${JSON.stringify({ checkpoints: cycles.length, asked: CHECKPOINTS, chain: CHAIN, precopy: PRECOPY, failure: failure ?? null, aborts, sentryBefore: sentryBefore ?? null, sentryGrowthBytes: sentryGrowthBytes ?? null, cycles }, null, 2)}\n`);
    writeFileSync(join(OUT, "bayma.log"), logs);
    // What the host says of a sandbox that ended: the kernel's log, its disk and its memory.
    const host = await Promise.all([
      run("sudo", ["dmesg", "--ctime"]).then((log) => log.split("\n").slice(-300).join("\n")),
      run("df", ["--block-size=1", scratch]),
      run("free", ["--bytes"]),
    ].map((read) => read.catch((error: unknown) => String(error))));
    writeFileSync(join(OUT, "host.log"), `${host.join("\n\n")}\n`);
    await run("docker", ["rm", "--force", name]).catch(() => undefined);
    await run("sudo", ["rm", "-rf", scratch]).catch((error: unknown) => console.error(error));
  }
});
