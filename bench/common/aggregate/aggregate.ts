/**
 * The aggregator: reads any number of results.jsonl, checks each line against the schema, and
 * writes, per system x variant x target x fixture x size x profile x phase x tier, the p50, p90, p95
 * and p99 (nearest rank, 05-verification/benchmark.md) of every metric over the runs that passed,
 * with an interval for the median that assumes nothing of the values' distribution, and the runs
 * that failed with why.
 *
 * A restore's cell is besides split by what it restored into, a pod made for it or a standby (its
 * details' `standby`): each is a mode of its own, so a statistic of both together falls between
 * them, or on either, as the runs happen to alternate.
 *
 *   node aggregate.ts --out <dir> results.jsonl...
 *
 * Writes <dir>/summary.json and <dir>/summary.md; exits 1 where a line does not fit the schema.
 */
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { parseArgs } from "node:util";

import { Ajv2020 } from "ajv/dist/2020.js";

/** A line of results.jsonl, as far as the aggregator reads it. */
export interface Result {
  readonly system: string;
  readonly variant: string;
  readonly target: string;
  readonly fixture: string;
  readonly size_mib: number;
  readonly profile: string;
  readonly emulated: boolean;
  readonly run: number;
  readonly phase: string;
  /** A replayed turn's session's tier, null for the other phases. */
  readonly tier: string | null;
  readonly ok: boolean;
  readonly failure: string | null;
  readonly metrics: Readonly<Record<string, number | string | null>>;
  /** Of a restore, whether it restored into a standby; the rest the aggregator does not read. */
  readonly details?: { readonly standby?: unknown };
}

/** An interval for the median: the `lower`th and `upper`th smallest of n values, which hold it between them at `confidence` whatever their distribution. */
export interface MedianRanks {
  readonly lower: number;
  readonly upper: number;
  readonly confidence: number;
}

/** One metric's statistics over the runs that passed: the nearest-rank p50, p90, p95 and p99, and the median's interval, none where n is too small for one. */
export interface Statistic {
  readonly n: number;
  readonly p50: number;
  readonly p50_interval: { readonly low: number; readonly high: number; readonly confidence: number } | null;
  readonly p90: number;
  readonly p95: number;
  readonly p99: number;
}

/** What a restore restored into: a pod made for it, or one of its node's standbys. */
export type Kind = "made_pod" | "standby";
const KINDS: readonly Kind[] = ["made_pod", "standby"];

/** Of a cell's runs, how many, how many passed, and their statistics. */
export interface Statistics {
  readonly runs: number;
  readonly passed: number;
  readonly metrics: Readonly<Record<string, Statistic>>;
}

/** A cell: one system x variant x target x fixture x size x profile x phase x tier; of a restore, its runs by what each restored into besides. */
export interface Cell extends Statistics {
  readonly key: Omit<Result, "run" | "ok" | "failure" | "metrics" | "details">;
  readonly failures: readonly { readonly run: number; readonly failure: string }[];
  readonly kinds?: Readonly<Partial<Record<Kind, Statistics>>>;
}

/** The median of `values`, sorted: the middle one, or the mean of the two middle ones. Not a percentile: the store's probe's alone (../store/probe.ts). */
export function median(sorted: readonly number[]): number {
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 1 ? (sorted[middle] ?? NaN) : ((sorted[middle - 1] ?? NaN) + (sorted[middle] ?? NaN)) / 2;
}

/** The nearest-rank `q` quantile of `values`, sorted: the ceil(q n)th smallest; with 20 runs, p90 the 18th, p95 the 19th and p99 the largest. */
export function nearestRank(sorted: readonly number[], q: number): number {
  return sorted[Math.max(0, Math.ceil(q * sorted.length) - 1)] ?? NaN;
}

/** The nearest-rank p95 of `values`, sorted; with 5 runs, the largest. */
export const p95 = (sorted: readonly number[]): number => nearestRank(sorted, 0.95);

/** The confidence the median's interval is to have at least. */
const MEDIAN_CONFIDENCE = 0.95;

/**
 * The narrowest interval of order statistics, the lth and the (n + 1 - l)th smallest, that holds
 * the median of n values at MEDIAN_CONFIDENCE at least, whatever their distribution: as each value
 * is below the median by even odds, it is so where fewer than l of n are below it, or fewer than l
 * above, each of binomial odds (n, 1/2). Of 20, the 6th and 15th (95.9 %); of 10, the 2nd and 9th
 * (97.9 %); none of 5 or fewer, whose widest, the least and the largest, holds it at 93.8 % alone.
 */
export function medianRanks(n: number): MedianRanks | null {
  // The binomial's odds of each count, by its logarithm, which neither of n's factorials holds as a number.
  const lnChoose = (k: number) => {
    let sum = 0;
    for (let i = 1; i <= k; i++) sum += Math.log(n - k + i) - Math.log(i);
    return sum;
  };
  let below = 0;
  let best: MedianRanks | null = null;
  for (let lower = 1; lower <= Math.floor((n + 1) / 2); lower++) {
    // The odds that fewer than `lower` are below it.
    below += Math.exp(lnChoose(lower - 1) - n * Math.LN2);
    const confidence = 1 - 2 * below;
    if (confidence < MEDIAN_CONFIDENCE) break;
    best = { lower, upper: n + 1 - lower, confidence };
  }
  return best;
}

/** A metric's statistics over `sorted`, its values that passed, sorted. */
export function statistic(sorted: readonly number[]): Statistic {
  const ranks = medianRanks(sorted.length);
  return {
    n: sorted.length,
    p50: nearestRank(sorted, 0.5),
    p50_interval: ranks === null ? null : { low: sorted[ranks.lower - 1] ?? NaN, high: sorted[ranks.upper - 1] ?? NaN, confidence: ranks.confidence },
    p90: nearestRank(sorted, 0.9),
    p95: p95(sorted),
    p99: nearestRank(sorted, 0.99),
  };
}

const KEY_FIELDS = ["system", "variant", "target", "fixture", "size_mib", "profile", "emulated", "phase", "tier"] as const;

/** The statistics of every metric of `runs`, those that passed. */
function statisticsOf(runs: readonly Result[]): Statistics {
  const passed = runs.filter(({ ok }) => ok);
  const names = [...new Set(passed.flatMap(({ metrics }) => Object.keys(metrics)))];
  const metrics: Record<string, Statistic> = {};
  for (const name of names) {
    const values = passed.map(({ metrics: each }) => each[name]).filter((value): value is number => typeof value === "number").sort((a, b) => a - b);
    if (values.length > 0) metrics[name] = statistic(values);
  }
  return { runs: runs.length, passed: passed.length, metrics };
}

/** What a restore restored into, as its details say; none of another phase, or of a line that does not say. */
const kindOf = ({ phase, details }: Result): Kind | null =>
  !phase.startsWith("restore-") || typeof details?.standby !== "boolean" ? null : details.standby ? "standby" : "made_pod";

export function aggregate(results: readonly Result[]): Cell[] {
  const cells = new Map<string, Result[]>();
  for (const result of results) {
    const key = JSON.stringify(KEY_FIELDS.map((field) => result[field]));
    cells.set(key, [...(cells.get(key) ?? []), result]);
  }
  return [...cells.values()].map((runs) => {
    const [first] = runs;
    if (!first) throw new Error("a cell holds no run");
    const key = Object.fromEntries(KEY_FIELDS.map((field) => [field, first[field]])) as Cell["key"];
    const kinds = Object.fromEntries(KINDS.flatMap((kind) => {
      const of = runs.filter((run) => kindOf(run) === kind);
      return of.length === 0 ? [] : [[kind, statisticsOf(of)]];
    })) as Partial<Record<Kind, Statistics>>;
    return {
      key,
      ...statisticsOf(runs),
      failures: runs.filter(({ ok }) => !ok).map(({ run, failure }) => ({ run, failure: failure ?? "no reason given" })),
      ...(Object.keys(kinds).length === 0 ? {} : { kinds }),
    };
  }).sort((a, b) => JSON.stringify(KEY_FIELDS.map((field) => a.key[field])).localeCompare(JSON.stringify(KEY_FIELDS.map((field) => b.key[field]))));
}

/** A restore's metrics as its tables show them: its times; the restored sandbox's memory; and its node's. */
const RESTORE_TABLES: readonly (readonly [string, readonly string[]])[] = [
  ["", ["first_exec_ms", "orchestration_ms", "policy_wait_ms", "runsc_resume_ms", "resident_ms", "verify_ms"]],
  ["the sandbox's memory", ["memory_peak_bytes", "memory_bytes", "memory_file_bytes", "sandbox_overhead_bytes", "gofer_bytes", "awaited_bytes", "standby_idle_bytes"]],
  ["the node's memory", ["node_agent_memory_bytes", "node_agent_anon_bytes", "node_agent_file_bytes", "sandboxes_bound_bytes", "sandboxes_standby_bytes", "sandboxes_free_bytes", "layer_cache_bytes"]],
];

/** The tables of each phase, by their titles, and the metrics each shows, in order. */
const SHOWN: Readonly<Record<string, readonly (readonly [string, readonly string[]])[]>> = {
  "exec-running": [["", ["exec_running_ms", "probe_exec_ms", "verify_ms"]]],
  "save-live": [["", ["save_total_ms", "unavailable_ms", "pause_reported_ms", "slowdown", "precopy_passes", "precopy_bytes", "uploaded_bytes"]]],
  "save-stop": [["", ["save_total_ms", "pause_reported_ms", "precopy_passes", "uploaded_bytes"]]],
  "restore-warm": RESTORE_TABLES,
  "restore-cold": RESTORE_TABLES,
  "turn-replay": [["", ["turn_wait_ms", "added_latency_ms", "probe_exec_ms"]]],
};

const shown = (name: string, value: number): string => {
  if (name.endsWith("_bytes")) return `${(value / 1024 ** 2).toFixed(1)} MiB`;
  if (name.endsWith("_ms")) return value >= 1000 ? `${(value / 1000).toFixed(2)} s` : `${value.toFixed(1)} ms`;
  return value.toFixed(2);
};

/** A statistic as a table shows it: p50 [its interval] / p90 / p95 / p99. */
const shownStatistic = (name: string, { p50, p50_interval: interval, p90, p95: of95, p99 }: Statistic): string =>
  `${shown(name, p50)} [${interval === null ? "n too small" : `${shown(name, interval.low)} – ${shown(name, interval.high)}`}] / ${shown(name, p90)} / ${shown(name, of95)} / ${shown(name, p99)}`;

const KIND_SHOWN: Readonly<Record<Kind | "either", string>> = { made_pod: "a pod made for it", standby: "a standby", either: "either (bimodal)" };

/** summary.md: tables per phase, a row per cell, and of a restore's per what it restored into; a replayed turn's tier beside its profile. */
export function markdown(cells: readonly Cell[]): string {
  const lines = [
    "# Snapshot benchmark summary",
    "",
    "Each metric over the runs that passed, `passed` of `runs`, as p50 [interval] / p90 / p95 / p99. Every percentile is the nearest rank: the ceil(q n)th smallest of n.",
    "The interval holds the median at the confidence summary.json gives it, whatever the distribution: the order statistics the binomial (n, 1/2) gives, the 6th and 15th smallest of 20 (95.9 %), the 2nd and 9th of 10 (97.9 %); \"n too small\" where no two hold it at 95 % (n of 5 or fewer).",
    "A restore's rows are by what it restored into: a pod made for it, or a standby; either, the two pooled, is bimodal, its percentiles of neither. Emulated profiles are labelled so.",
    "",
  ];
  for (const [phase, tables] of Object.entries(SHOWN)) {
    const of = cells.filter(({ key }) => key.phase === phase);
    if (of.length === 0) continue;
    const restores = phase.startsWith("restore-");
    for (const [title, names] of tables) {
      lines.push(`## ${phase}${title === "" ? "" : `: ${title}`}`, "", `| system | variant | target | fixture | MiB | profile |${restores ? " restored into |" : ""} passed | ${names.join(" | ")} |`, `|${" --- |".repeat((restores ? 8 : 7) + names.length)}`);
      for (const cell of of) {
        const { key } = cell;
        const profile = `${key.profile}${key.emulated ? " (emulated)" : ""}${key.tier === null ? "" : `, ${key.tier}`}`;
        const kinds = KINDS.flatMap((kind) => {
          const each = cell.kinds?.[kind];
          return each === undefined ? [] : [[kind, each] as const];
        });
        // Each kind's, and both pooled after them, said bimodal; or the pooled alone, of a cell whose runs do not say their kind.
        const rows: readonly (readonly [string, Statistics])[] = [...kinds.map(([kind, each]) => [KIND_SHOWN[kind], each] as const), [kinds.length === 0 ? "-" : kinds.length === 1 ? "all" : KIND_SHOWN.either, cell]];
        for (const [into, { runs, passed, metrics }] of restores ? rows : [["", cell] as const]) {
          const values = names.map((name) => {
            const each = metrics[name];
            return each ? shownStatistic(name, each) : "-";
          });
          lines.push(`| ${key.system} | ${key.variant} | ${key.target} | ${key.fixture} | ${key.size_mib} | ${profile} |${restores ? ` ${into} |` : ""} ${passed}/${runs} | ${values.join(" | ")} |`);
        }
      }
      lines.push("");
    }
  }
  const failed = cells.flatMap(({ key, failures }) => failures.map(({ run, failure }) => `- ${key.system}/${key.variant} ${key.target} ${key.fixture} ${key.size_mib} MiB ${key.profile} ${key.phase} run ${run}: ${failure}`));
  if (failed.length > 0) lines.push("## Failed runs", "", ...failed, "");
  return lines.join("\n");
}

/** The lines of `files`, each checked against the schema: the results, and what did not fit. */
export function read(files: readonly string[]): { readonly results: Result[]; readonly invalid: string[] } {
  const ajv = new Ajv2020({ allErrors: true });
  const validate = ajv.compile(JSON.parse(readFileSync(new URL("../schema/result.schema.json", import.meta.url), "utf8")) as object);
  const results: Result[] = [];
  const invalid: string[] = [];
  for (const file of files) {
    readFileSync(file, "utf8").split("\n").forEach((line, index) => {
      if (line.trim() === "") return;
      const where = `${file}:${index + 1}`;
      let parsed: unknown;
      try {
        parsed = JSON.parse(line);
      } catch (error) {
        invalid.push(`${where}: not JSON: ${(error as Error).message}`);
        return;
      }
      if (validate(parsed)) results.push(parsed as Result);
      else invalid.push(`${where}: ${ajv.errorsText(validate.errors)}`);
    });
  }
  return { results, invalid };
}

// Run as a program, not imported, nor bundled into another's (../driver/build.ts).
if (/(^|\/)aggregate\.(ts|mjs)$/u.test(process.argv[1] ?? "")) {
  const { values, positionals } = parseArgs({ allowPositionals: true, options: { out: { type: "string" } } });
  if (!values.out || positionals.length === 0) throw new Error("usage: aggregate.ts --out <dir> results.jsonl...");
  const { results, invalid } = read(positionals);
  const cells = aggregate(results);
  mkdirSync(values.out, { recursive: true });
  writeFileSync(join(values.out, "summary.json"), `${JSON.stringify({ cells, invalid }, null, 2)}\n`);
  writeFileSync(join(values.out, "summary.md"), markdown(cells));
  console.log(`${results.length} results in ${cells.length} cells; ${invalid.length} lines that fit no schema`);
  for (const line of invalid) console.error(line);
  if (invalid.length > 0) process.exit(1);
}
