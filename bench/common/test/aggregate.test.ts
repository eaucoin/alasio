import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";

import { aggregate, median, p95, read, type Result } from "../aggregate/aggregate.ts";
import { toxicsOf } from "../store/shape.ts";

const line = (run: number, ok: boolean, first: number | null): Result & Record<string, unknown> => ({
  schema: 1,
  system: "alasio",
  variant: "live",
  target: "host",
  fixture: "idle",
  size_mib: 16,
  profile: "local",
  emulated: false,
  run,
  phase: "restore-cold",
  ok,
  failure: ok ? null : "the marker is not the run's",
  metrics: { first_exec_ms: first, resident_how: "signal" },
  correctness: { marker: ok, memory: true, children: null, file: null, socket: null, running: null, notice: null },
  host: { cpu_model: "x", kernel: "6.8", cores: 4, memory_bytes: 1, copy_mib_s: 500, runner_image: "ubuntu24/1" },
  store: { first_byte_ms: 1, get_one_mb_s: 1, put_one_mb_s: 1, get_eight_mb_s: 1 },
  versions: { system: "10.0.0", bayma: "0.15.2", seaweedfs: "4.48", toxiproxy: "2.13.0" },
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

test("a cell's statistics are of the runs that passed, the others listed with why", () => {
  const [cell] = aggregate([line(1, true, 10), line(2, true, 30), line(3, false, 1), line(4, true, 20), line(5, true, null)]);
  assert.ok(cell);
  assert.deepEqual([cell.runs, cell.passed], [5, 4]);
  assert.deepEqual(cell.metrics["first_exec_ms"], { n: 3, median: 20, p95: 30 });
  assert.equal(cell.metrics["resident_how"], undefined);
  assert.deepEqual(cell.failures, [{ run: 3, failure: "the marker is not the run's" }]);
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
