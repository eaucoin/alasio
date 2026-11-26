/**
 * What a job writes: a line of results.jsonl per run and phase, in the benchmark's result schema
 * (bench/common/schema/result.schema.json, schema 3); and a line of spans.jsonl per phase that
 * alasio's spans cover, its window and the spans of it, which the aggregate job reads each phase's
 * critical path from (bench/common/aggregate/critical-path.ts).
 */
import { appendFileSync } from "node:fs";
import { join } from "node:path";

import { TARGET } from "../../test/e2e/harness.ts";
import { type Span, spanLine } from "./spans.ts";
import { ALASIO_UNDER_TEST, BAYMA_UNDER_TEST, BENCH_DIR, HOST, PROFILE, PROFILES, RELEASE_UNDER_TEST, TOXIPROXY, VARIANT } from "./settings.ts";

/** The metrics a result has, null unless measured. */
export const METRICS = [
  "save_total_ms", "unavailable_ms", "pause_reported_ms", "slowdown", "precopy_passes", "precopy_bytes", "image_bytes", "uploaded_bytes", "first_exec_ms", "verify_ms", "resident_ms", "resident_how", "memory_peak_bytes", "memory_bytes", "workload_rss_bytes",
  // Of the restored sandbox's memory, as its node's agent reads it (src/node-agent/memory.ts): its MemoryFile's, its
  // application's memory with its page cache and tmpfs, and the rest, its Sentry's, gofer's and shims'; and the agent's own, of it
  // its anonymous memory and its page cache.
  "memory_file_bytes", "sandbox_overhead_bytes", "node_agent_memory_bytes", "node_agent_anon_bytes", "node_agent_file_bytes",
  // Of the overhead, its gofers'.
  "gofer_bytes",
  // The mechanism's own splits, from runsc's boot log; the product's, from alasio's spans; what moved, from the node's counters.
  "kernel_save_ms", "memory_save_ms", "overall_save_ms", "durable_store_ms", "store_sent_bytes", "uploaded_over_needed",
  "restore_kernel_load_ms", "restore_memory_load_ms", "restore_to_running_ms", "runsc_resume_ms", "policy_wait_ms", "probe_exec_ms", "orchestration_ms",
  "loaded_bytes", "store_received_bytes", "disk_read_bytes", "read_amplification", "page_cache", "compaction_at_t0",
  // The exec on the session running, which the first execs after a restore are reported net of; and a replayed turn's wait.
  "exec_running_ms", "turn_wait_ms", "added_latency_ms", "t_llm_ms",
] as const;
export type Metrics = Partial<Record<(typeof METRICS)[number], number | string | null>>;

/** The checks of a phase that restores nothing. */
export const NO_CHECKS = { marker: null, memory: null, children: null, file: null, socket: null, running: null, notice: null };

export type Phase = "exec-running" | "save-live" | "save-stop" | "restore-warm" | "restore-cold" | "turn-replay";
/** A replayed turn's session's tier as its message came: running (T0), released with its image on the node (T3), or in the store alone (T4); frozen (T1) and reclaimed (T2) are not tiers alasio has. */
export type Tier = "T0" | "T3" | "T4";

/** A phase's result, as a run builds it: what is measured, checked and seen. */
export interface Result {
  readonly fixture: string;
  readonly sizeMib: number;
  readonly run: number;
  readonly phase: Phase;
  readonly tier?: Tier;
  readonly ok: boolean;
  readonly failure: string | null;
  readonly metrics: Metrics;
  readonly correctness?: Record<string, unknown>;
  readonly details?: Record<string, unknown>;
}

/** How the store answered through the profile as the job began (./node.ts probeStore), which every line says. */
export const store: { probed: Record<string, number> } = { probed: {} };

/** Appends `result` to results.jsonl. */
export function emit({ fixture, sizeMib, run, phase, tier, ok, failure, metrics, correctness = NO_CHECKS, details = {} }: Result): void {
  const line = {
    schema: 3,
    system: "alasio",
    variant: VARIANT,
    target: TARGET,
    fixture,
    size_mib: sizeMib,
    profile: PROFILE,
    emulated: PROFILES[PROFILE]?.emulated ?? false,
    run,
    phase,
    tier: tier ?? null,
    ok,
    failure,
    metrics: Object.fromEntries(METRICS.map((name) => [name, metrics[name] ?? null])),
    correctness,
    host: HOST,
    store: store.probed,
    versions: { system: `alasio ${ALASIO_UNDER_TEST}, gVisor ${RELEASE_UNDER_TEST}`, bayma: BAYMA_UNDER_TEST, seaweedfs: "4.48", toxiproxy: TOXIPROXY },
    ci: { run_id: Number(process.env["GITHUB_RUN_ID"] ?? 0), job: process.env["GITHUB_JOB"] ?? "local", sha: process.env["GITHUB_SHA"] ?? "" },
    at: new Date().toISOString(),
    details,
  };
  appendFileSync(join(BENCH_DIR, "results.jsonl"), `${JSON.stringify(line)}\n`);
}

/**
 * Appends to spans.jsonl the phase `result`'s spans: those of `spans` that overlap its window, from
 * `fromMs` to `toMs` on the wall clock (a restore's, from its probe's t0 to its first exec's answer),
 * with the window, which the phase's critical path is read over.
 */
export function emitSpans(result: Result, fromMs: number, toMs: number, spans: readonly Span[]): void {
  const fromNs = BigInt(Math.round(fromMs * 1e6));
  const toNs = BigInt(Math.round(toMs * 1e6));
  const within = spans.filter((span) => span.endNs >= fromNs && span.startNs <= toNs);
  const line = {
    variant: VARIANT,
    profile: PROFILE,
    fixture: result.fixture,
    size_mib: result.sizeMib,
    run: result.run,
    phase: result.phase,
    tier: result.tier ?? null,
    ok: result.ok,
    from_ns: String(fromNs),
    to_ns: String(toNs),
    spans: within.map(spanLine),
  };
  appendFileSync(join(BENCH_DIR, "spans.jsonl"), `${JSON.stringify(line)}\n`);
}
