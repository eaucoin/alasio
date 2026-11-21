/**
 * The critical path of each phase alasio's spans cover (spans.jsonl, a line per phase: its window and
 * its spans, bench/alasio/results.ts emitSpans): the chain of steps that ended last, read back from
 * the window's end, each step a span's own time not covered by a child of its that ended later, and
 * what of the window no span covers, the harness's and the network's. Then, per cell (variant,
 * profile, fixture, size, phase, tier), each step's share of the window over the cell's runs: its
 * p50 and p95 (nearest rank), and the share no span covers, which says how far the spans account for
 * what the phase measured.
 *
 *   node critical-path.ts --out <dir> spans.jsonl...
 *
 * Writes <dir>/critical-path.json, each phase's path, and <dir>/critical-path.md, the cells'.
 */
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { parseArgs } from "node:util";

import { nearestRank } from "./aggregate.ts";

/** A span as spans.jsonl keeps it, its times in nanoseconds as decimal text. */
export interface SpanLine {
  readonly name: string;
  readonly traceId: string;
  readonly spanId: string;
  readonly parentSpanId: string;
  readonly startNs: string;
  readonly endNs: string;
  readonly attributes?: Readonly<Record<string, string>>;
}

/** A line of spans.jsonl: a phase, its window and its spans. */
export interface PhaseSpans {
  readonly variant: string;
  readonly profile: string;
  readonly fixture: string;
  readonly size_mib: number;
  readonly run: number;
  readonly phase: string;
  readonly tier: string | null;
  readonly ok: boolean;
  readonly from_ns: string;
  readonly to_ns: string;
  readonly spans: readonly SpanLine[];
}

/** A step of a critical path: a span's own time on it, or the window's time no span covers (`(outside spans)`), in milliseconds. */
export interface Step {
  readonly name: string;
  readonly ms: number;
}

/** What no span covers. */
export const OUTSIDE = "(outside spans)";

interface Node {
  readonly name: string;
  readonly start: bigint;
  readonly end: bigint;
  readonly children: Node[];
}

const ms = (ns: bigint): number => Number(ns) / 1e6;

/**
 * The step a span is on a path: its name, but an HTTP span's, which HTTP instrumentation names by
 * its method alone, with the path it asked or served, a workspace's name and a snapshot's id in it
 * said as such, so one call's spans are one step over every run.
 */
export function stepName({ name, attributes = {} }: Pick<SpanLine, "name" | "attributes">): string {
  if (!/^(?:http\.server )?(?:GET|HEAD|POST|PUT|PATCH|DELETE)$/u.test(name)) return name;
  const path = attributes["url.path"] ?? (attributes["url.full"] ? new URL(attributes["url.full"]).pathname : undefined);
  if (path === undefined) return name;
  const said = path.replace(/fs-[0-9a-f]+/gu, "{workspace}").replace(/\/[0-9a-f]{16,}(?=\/|$)/gu, "/{id}");
  return `${name} ${attributes["server.address"] && !name.startsWith("http.server") ? `${attributes["server.address"].replace(/^[0-9.]+$/u, "{address}")}` : ""}${said}`;
}

/** How close two children's ends are that end together, as a call's client span and its server's do: 10 ms. */
const TOGETHER_NS = 10_000_000n;

/** The spans under `node`, all of them. */
const descendants = (node: Node): number => node.children.reduce((sum, child) => sum + 1 + descendants(child), 0);

/**
 * The steps of `node`'s critical path from its start to `until`, in order: read back from `until`,
 * the child that ended last before it is on the path, and the time between its end and `until` is
 * the node's own; then from that child's start, until the node's start. Of children that end
 * together, as a call's client span and the span of the server that answered it do, the path goes
 * through the one with the most under it, which says where the time went.
 */
function walk(node: Node, until: bigint): Step[] {
  const steps: Step[] = [];
  let cursor = until < node.end ? until : node.end;
  const own = (to: bigint, from: bigint) => {
    if (to > from) steps.push({ name: node.name, ms: ms(to - from) });
  };
  for (;;) {
    const open = node.children.filter((child) => child.start < cursor && child.end > node.start);
    const endOf = (child: Node) => (child.end < cursor ? child.end : cursor);
    const latest = open.reduce<bigint | null>((max, child) => (max === null || endOf(child) > max ? endOf(child) : max), null);
    if (latest === null) {
      own(cursor, node.start);
      break;
    }
    const last = open.filter((child) => endOf(child) >= latest - TOGETHER_NS).reduce((best, child) => (descendants(child) > descendants(best) ? child : best));
    const ends = endOf(last);
    own(cursor, ends);
    steps.push(...walk(last, ends).reverse());
    cursor = last.start > node.start ? last.start : node.start;
    if (cursor <= node.start) break;
  }
  return steps.reverse();
}

/**
 * The spans a phase's path runs through: where the phase is a request's to the gateway's front (a
 * restore's probe, a turn's), the traces of the requests that arrived in its window alone, as the
 * request waits on those and on nothing else alasio does meanwhile (a compaction, a fetch, a load,
 * each a trace of its own linked to the one that started it, and other sessions' requests begun
 * before it, as a mixed load's); every span otherwise.
 */
function ofTheRequests(fromNs: bigint, spans: readonly SpanLine[]): readonly SpanLine[] {
  const traces = new Set(spans.filter(({ name, startNs }) => name === "alasio.front.request" && BigInt(startNs) >= fromNs).map(({ traceId }) => traceId));
  return traces.size === 0 ? spans : spans.filter(({ traceId }) => traces.has(traceId));
}

/**
 * The critical path of a phase's window over its spans (ofTheRequests): the window a root whose
 * children are the spans with no parent among them, each trace's root, its own time what no span
 * covers. Consecutive steps of one name are merged.
 */
export function criticalPath({ from_ns, to_ns, spans: all }: Pick<PhaseSpans, "from_ns" | "to_ns" | "spans">): Step[] {
  const spans = ofTheRequests(BigInt(from_ns), all);
  const nodes = new Map(spans.map((span) => [`${span.traceId}/${span.spanId}`, { name: stepName(span), start: BigInt(span.startNs), end: BigInt(span.endNs), children: [] as Node[] }]));
  const root: Node = { name: OUTSIDE, start: BigInt(from_ns), end: BigInt(to_ns), children: [] };
  for (const span of spans) {
    const node = nodes.get(`${span.traceId}/${span.spanId}`);
    if (!node) continue;
    (nodes.get(`${span.traceId}/${span.parentSpanId}`) ?? root).children.push(node);
  }
  const merged: Step[] = [];
  for (const step of walk(root, root.end)) {
    const previous = merged.at(-1);
    if (previous?.name === step.name) merged[merged.length - 1] = { name: step.name, ms: previous.ms + step.ms };
    else merged.push(step);
  }
  return merged;
}

/** A cell's paths: each step's share of the window, p50 and p95 over its runs, the steps in the order of their median share. */
export function cellShares(paths: readonly { readonly windowMs: number; readonly steps: readonly Step[] }[]): { readonly name: string; readonly p50: number; readonly p95: number; readonly p50Ms: number }[] {
  const names = [...new Set(paths.flatMap(({ steps }) => steps.map(({ name }) => name)))];
  return names.map((name) => {
    const shares = paths.map(({ windowMs, steps }) => (windowMs <= 0 ? 0 : steps.filter((step) => step.name === name).reduce((sum, step) => sum + step.ms, 0) / windowMs)).sort((a, b) => a - b);
    const times = paths.map(({ steps }) => steps.filter((step) => step.name === name).reduce((sum, step) => sum + step.ms, 0)).sort((a, b) => a - b);
    return { name, p50: nearestRank(shares, 0.5), p95: nearestRank(shares, 0.95), p50Ms: nearestRank(times, 0.5) };
  }).sort((a, b) => b.p50 - a.p50);
}

/** critical-path.md: a table per cell, its steps' shares of the window. */
export function markdown(lines: readonly PhaseSpans[]): string {
  const cells = new Map<string, PhaseSpans[]>();
  for (const line of lines.filter(({ ok }) => ok)) {
    const key = [line.variant, line.profile, line.fixture, line.size_mib, line.phase, line.tier ?? ""].join(" ");
    cells.set(key, [...(cells.get(key) ?? []), line]);
  }
  const out = ["# Critical paths", "", "Each phase's critical path through alasio's spans over its window (a restore's: its probe's t0 to its first exec's answer); per step, its share of the window, p50 / p95 over the runs (nearest rank), and its p50 time. `(outside spans)` is what no span covers.", ""];
  for (const [key, of] of [...cells].sort(([a], [b]) => a.localeCompare(b))) {
    const paths = of.map((line) => ({ windowMs: ms(BigInt(line.to_ns) - BigInt(line.from_ns)), steps: criticalPath(line) }));
    out.push(`## ${key} (${of.length} runs)`, "", "| step | share p50 | share p95 | p50 ms |", "| --- | --- | --- | --- |");
    for (const { name, p50, p95, p50Ms } of cellShares(paths)) out.push(`| ${name} | ${(p50 * 100).toFixed(1)} % | ${(p95 * 100).toFixed(1)} % | ${p50Ms.toFixed(1)} |`);
    out.push("");
  }
  return out.join("\n");
}

// Run as a program, not imported.
if (/(^|\/)critical-path\.(ts|mjs)$/u.test(process.argv[1] ?? "")) {
  const { values, positionals } = parseArgs({ allowPositionals: true, options: { out: { type: "string" } } });
  if (!values.out || positionals.length === 0) throw new Error("usage: critical-path.ts --out <dir> spans.jsonl...");
  const lines = positionals.flatMap((file) => readFileSync(file, "utf8").split("\n").filter((line) => line.trim() !== "").map((line) => JSON.parse(line) as PhaseSpans));
  mkdirSync(values.out, { recursive: true });
  const paths = lines.map((line) => ({ variant: line.variant, profile: line.profile, fixture: line.fixture, size_mib: line.size_mib, run: line.run, phase: line.phase, tier: line.tier, ok: line.ok, window_ms: ms(BigInt(line.to_ns) - BigInt(line.from_ns)), steps: criticalPath(line) }));
  writeFileSync(join(values.out, "critical-path.json"), `${JSON.stringify(paths, null, 1)}\n`);
  writeFileSync(join(values.out, "critical-path.md"), markdown(lines));
  console.log(`${lines.length} phases' critical paths`);
}
