/**
 * The aggregator: reads any number of results.jsonl, checks each line against the schema, and
 * writes, per system x variant x target x fixture x size x profile x phase, the median and p95
 * (nearest rank) of every metric over the runs that passed, and the runs that failed with why.
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
  readonly ok: boolean;
  readonly failure: string | null;
  readonly metrics: Readonly<Record<string, number | string | null>>;
}

/** The median and p95 of one metric over a cell's runs that passed. */
export interface Statistic {
  readonly n: number;
  readonly median: number;
  readonly p95: number;
}

/** A cell: one system x variant x target x fixture x size x profile x phase. */
export interface Cell {
  readonly key: Omit<Result, "run" | "ok" | "failure" | "metrics">;
  readonly runs: number;
  readonly passed: number;
  readonly failures: readonly { readonly run: number; readonly failure: string }[];
  readonly metrics: Readonly<Record<string, Statistic>>;
}

/** The median of `values`, sorted: the middle one, or the mean of the two middle ones. */
export function median(sorted: readonly number[]): number {
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 1 ? (sorted[middle] ?? NaN) : ((sorted[middle - 1] ?? NaN) + (sorted[middle] ?? NaN)) / 2;
}

/** The nearest-rank p95 of `values`, sorted: the ceil(0.95 n)th; with 5 runs, the largest. */
export function p95(sorted: readonly number[]): number {
  return sorted[Math.max(0, Math.ceil(0.95 * sorted.length) - 1)] ?? NaN;
}

const KEY_FIELDS = ["system", "variant", "target", "fixture", "size_mib", "profile", "emulated", "phase"] as const;

export function aggregate(results: readonly Result[]): Cell[] {
  const cells = new Map<string, Result[]>();
  for (const result of results) {
    const key = JSON.stringify(KEY_FIELDS.map((field) => result[field]));
    cells.set(key, [...(cells.get(key) ?? []), result]);
  }
  return [...cells.values()].map((runs) => {
    const [first] = runs;
    if (!first) throw new Error("a cell holds no run");
    const passed = runs.filter(({ ok }) => ok);
    const names = [...new Set(passed.flatMap(({ metrics }) => Object.keys(metrics)))];
    const metrics: Record<string, Statistic> = {};
    for (const name of names) {
      const values = passed.map(({ metrics: each }) => each[name]).filter((value): value is number => typeof value === "number").sort((a, b) => a - b);
      if (values.length > 0) metrics[name] = { n: values.length, median: median(values), p95: p95(values) };
    }
    const key = Object.fromEntries(KEY_FIELDS.map((field) => [field, first[field]])) as Cell["key"];
    return {
      key,
      runs: runs.length,
      passed: passed.length,
      failures: runs.filter(({ ok }) => !ok).map(({ run, failure }) => ({ run, failure: failure ?? "no reason given" })),
      metrics,
    };
  }).sort((a, b) => JSON.stringify(KEY_FIELDS.map((field) => a.key[field])).localeCompare(JSON.stringify(KEY_FIELDS.map((field) => b.key[field]))));
}

/** The metrics each phase's table shows, in order. */
const SHOWN: Readonly<Record<string, readonly string[]>> = {
  "save-live": ["save_total_ms", "unavailable_ms", "pause_reported_ms", "slowdown", "precopy_passes", "precopy_bytes", "uploaded_bytes"],
  "save-stop": ["save_total_ms", "pause_reported_ms", "precopy_passes", "uploaded_bytes"],
  "restore-warm": ["first_exec_ms", "resident_ms", "verify_ms", "memory_peak_bytes", "overhead_bytes"],
  "restore-cold": ["first_exec_ms", "resident_ms", "verify_ms", "memory_peak_bytes", "overhead_bytes"],
};

const shown = (name: string, value: number): string => {
  if (name.endsWith("_bytes")) return `${(value / 1024 ** 2).toFixed(1)} MiB`;
  if (name.endsWith("_ms")) return value >= 1000 ? `${(value / 1000).toFixed(2)} s` : `${value.toFixed(1)} ms`;
  return value.toFixed(2);
};

/** summary.md: a table per phase, a row per cell, median / p95 of each metric. */
export function markdown(cells: readonly Cell[]): string {
  const lines = ["# Snapshot benchmark summary", "", "Median / p95 (nearest rank) over the runs that passed; `passed` of `runs`. Emulated profiles are labelled so.", ""];
  for (const [phase, names] of Object.entries(SHOWN)) {
    const of = cells.filter(({ key }) => key.phase === phase);
    if (of.length === 0) continue;
    lines.push(`## ${phase}`, "", `| system | variant | target | fixture | MiB | profile | passed | ${names.join(" | ")} |`, `|${" --- |".repeat(7 + names.length)}`);
    for (const { key, runs, passed, metrics } of of) {
      const profile = key.emulated ? `${key.profile} (emulated)` : key.profile;
      const values = names.map((name) => {
        const statistic = metrics[name];
        return statistic ? `${shown(name, statistic.median)} / ${shown(name, statistic.p95)}` : "-";
      });
      lines.push(`| ${key.system} | ${key.variant} | ${key.target} | ${key.fixture} | ${key.size_mib} | ${profile} | ${passed}/${runs} | ${values.join(" | ")} |`);
    }
    lines.push("");
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
