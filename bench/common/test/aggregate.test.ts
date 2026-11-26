import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";

import { aggregate, markdown, median, medianRanks, nearestRank, p95, read, type Result, statistic } from "../aggregate/aggregate.ts";
import { toxicsOf } from "../store/shape.ts";

const line = (run: number, ok: boolean, first: number | null): Result & Record<string, unknown> => ({
  schema: 3,
  system: "alasio",
  variant: "as-shipped",
  target: "host",
  fixture: "idle",
  size_mib: 16,
  profile: "local",
  emulated: false,
  run,
  phase: "restore-cold",
  tier: null,
  ok,
  failure: ok ? null : "the marker is not the run's",
  metrics: { first_exec_ms: first, resident_how: "signal" },
  correctness: { marker: ok, memory: true, children: null, file: null, socket: null, running: null, notice: null },
  host: { cpu_model: "x", kernel: "6.8", cores: 4, memory_bytes: 1, copy_mib_s: 500, runner_image: "ubuntu24/1" },
  store: { first_byte_ms: 1, get_one_mb_s: 1, put_one_mb_s: 1, get_eight_mb_s: 1 },
  versions: { system: "alasio 10.0.23, gVisor 20261102.0-eaucoin.9", bayma: "0.15.7", seaweedfs: "4.48", toxiproxy: "2.13.0" },
  ci: { run_id: 1, job: "j", sha: "s" },
  at: "2026-10-27T00:00:00.000Z",
  details: {},
});

test("the median is the middle value, or the mean of the two middle ones", () => {
  assert.equal(median([1, 2, 9]), 2);
  assert.equal(median([1, 2, 4, 9]), 3);
});

test("p95 is the nearest rank: with five runs, the largest", () => {
  assert.equal(p95([1, 2, 3, 4, 5]), 5);
  assert.equal(p95(Array.from({ length: 40 }, (_, index) => index + 1)), 38);
});

test("of 20 runs, the nearest-rank p50 is the 10th, p90 the 18th, p95 the second largest and p99 the largest; of 10, p90 the 9th", () => {
  const twenty = Array.from({ length: 20 }, (_, index) => index + 1);
  assert.deepEqual([nearestRank(twenty, 0.5), nearestRank(twenty, 0.9), nearestRank(twenty, 0.95), nearestRank(twenty, 0.99)], [10, 18, 19, 20]);
  assert.equal(nearestRank(twenty.slice(0, 10), 0.9), 9);
  // A cell's p50 is a value of its runs, never the mean of the two middle ones.
  const { p50, p90 } = statistic(twenty);
  assert.deepEqual([p50, p90, median(twenty)], [10, 18, 10.5]);
});

test("the median's interval is of the order statistics the binomial gives at 95 % or more: of 20 the 6th and 15th, of 10 the 2nd and 9th, none of 5", () => {
  assert.deepEqual(medianRanks(20), { lower: 6, upper: 15, confidence: medianRanks(20)?.confidence });
  assert.equal(medianRanks(20)?.confidence.toFixed(4), "0.9586");
  assert.deepEqual([medianRanks(10)?.lower, medianRanks(10)?.upper, medianRanks(10)?.confidence.toFixed(4)], [2, 9, "0.9785"]);
  assert.deepEqual([medianRanks(6)?.lower, medianRanks(6)?.upper], [1, 6]);
  assert.equal(medianRanks(5), null);
  assert.equal(medianRanks(1), null);
  // Of many, about 1.96 standard deviations of the count either side of its middle.
  assert.deepEqual([medianRanks(100)?.lower, medianRanks(100)?.upper], [40, 61]);
  const values = Array.from({ length: 20 }, (_, index) => (index + 1) * 10);
  assert.deepEqual(statistic(values).p50_interval, { low: 60, high: 150, confidence: medianRanks(20)?.confidence });
  assert.equal(statistic([1, 2, 3]).p50_interval, null);
});

test("a cell's statistics are of the runs that passed, the others listed with why", () => {
  const [cell] = aggregate([line(1, true, 10), line(2, true, 30), line(3, false, 1), line(4, true, 20), line(5, true, null)]);
  assert.ok(cell);
  assert.deepEqual([cell.runs, cell.passed], [5, 4]);
  assert.deepEqual(cell.metrics["first_exec_ms"], { n: 3, p50: 20, p50_interval: null, p90: 30, p95: 30, p99: 30 });
  assert.equal(cell.metrics["resident_how"], undefined);
  assert.deepEqual(cell.failures, [{ run: 3, failure: "the marker is not the run's" }]);
});

test("a restore's cell is split by what it restored into, a pod made for it or a standby, each shown apart from the two pooled, which is said bimodal", () => {
  // Odd runs into a pod made for them, even ones into a standby, as the benchmark alternates them.
  const restored = (run: number, ms: number) => ({ ...line(run, true, ms), details: { standby: run % 2 === 0 } });
  const runs = Array.from({ length: 20 }, (_, index) => restored(index + 1, index % 2 === 0 ? 1400 + index : 650 + index));
  const [cell] = aggregate([...runs, line(21, false, null)]);
  assert.ok(cell);
  assert.deepEqual([cell.kinds?.made_pod?.runs, cell.kinds?.made_pod?.passed, cell.kinds?.standby?.passed, cell.runs], [10, 10, 10, 21]);
  assert.deepEqual([cell.kinds?.made_pod?.metrics["first_exec_ms"]?.p50, cell.kinds?.standby?.metrics["first_exec_ms"]?.p50], [1408, 659]);
  assert.deepEqual(cell.kinds?.standby?.metrics["first_exec_ms"]?.p50_interval, { low: 653, high: 667, confidence: medianRanks(10)?.confidence });
  // Pooled, the 10th of 20 is the slowest standby's.
  assert.equal(cell.metrics["first_exec_ms"]?.p50, 669);
  const shown = markdown([cell]);
  assert.match(shown, /\| a pod made for it \| 10\/10 \| 1\.41 s \[1\.40 s – 1\.42 s\] \/ 1\.42 s \/ 1\.42 s \/ 1\.42 s \|/u);
  assert.match(shown, /\| a standby \| 10\/10 \| 659\.0 ms \[653\.0 ms – 667\.0 ms\] \/ 667\.0 ms /u);
  assert.match(shown, /\| either \(bimodal\) \| 20\/21 \| /u);
  assert.match(shown, /## restore-cold: the node's memory/u);
  // A phase not a restore's has no such split.
  assert.equal(aggregate([{ ...line(1, true, 10), phase: "exec-running", details: { standby: true } }])[0]?.kinds, undefined);
});

test("a turn replay's cells are its tiers', each apart", () => {
  const replayed = (run: number, tier: string, wait: number) => ({ ...line(run, true, null), phase: "turn-replay", tier, metrics: { turn_wait_ms: wait } });
  const cells = aggregate([replayed(1, "T0", 3100), replayed(1, "T3", 4900), replayed(2, "T0", 3200), replayed(2, "T3", 5100)]);
  assert.deepEqual(cells.map(({ key, metrics }) => [key.tier, metrics["turn_wait_ms"]?.p50]), [["T0", 3100], ["T3", 4900]]);
});

test("a line that does not fit the schema is said, with where it is", () => {
  const dir = mkdtempSync(join(process.env["TMPDIR"] ?? "/tmp", "aggregate-"));
  try {
    const file = join(dir, "results.jsonl");
    writeFileSync(file, `${JSON.stringify(line(1, true, 10))}\n${JSON.stringify({ ...line(2, true, 10), phase: "restore" })}\nnot json\n`);
    const { results, invalid } = read([file]);
    assert.equal(results.length, 1);
    assert.equal(invalid.length, 2);
    assert.match(invalid[0] ?? "", /results\.jsonl:2: .*phase/u);
  } finally {
    rmSync(dir, { recursive: true });
  }
});

test("a profile's toxics delay the answer's first byte and hold each direction to its bandwidth, in toxiproxy's KB/s", () => {
  assert.deepEqual(toxicsOf({ emulated: true, first_byte_ms: 150, bytes_per_second: 85_000_000 }).map(({ type, stream, attributes }) => [type, stream, attributes]), [
    ["latency", "downstream", { latency: 150, jitter: 0 }],
    ["bandwidth", "downstream", { rate: 85_000 }],
    ["bandwidth", "upstream", { rate: 85_000 }],
  ]);
  assert.deepEqual(toxicsOf({ emulated: false, first_byte_ms: null, bytes_per_second: null }), []);
});
