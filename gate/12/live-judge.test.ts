/**
 * Judges 12's first gate (live.test.ts) once its parts have run: for each part, what the
 * Sentry's own memory grows by a checkpoint beyond what it grows by a cycle of the baseline's,
 * the same cycles run without checkpoints on a machine of the same kind, must be under
 * MAX_SENTRY_GROWTH_BYTES. What the workload holds grows both alike, and is no checkpoint's
 * cost; what each grows by, the Sentry's and the sandbox's by kind, is said either way.
 *
 * EVIDENCE is the directory of the parts' records, each OUT of live.test.ts in a directory of
 * its own: gate-12-live-<checkpointed|baseline>-<part>/evidence.json, as gate-12-live.yml
 * downloads them.
 */
import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";

import { type CycleMemory, MAX_SENTRY_GROWTH_BYTES, trend } from "./live-memory.ts";

const EVIDENCE = process.env["EVIDENCE"] ?? "";

interface Evidence {
  readonly checkpointing: boolean;
  readonly checkpoints: number;
  readonly asked: number;
  readonly failure: string | null;
  readonly cycles: readonly CycleMemory[];
}

/** The record of the run `kind` of `part`, or null where it left none. */
function evidenceOf(kind: "checkpointed" | "baseline", part: string): Evidence | null {
  try {
    return JSON.parse(readFileSync(join(EVIDENCE, `gate-12-live-${kind}-${part}`, "evidence.json"), "utf8")) as Evidence;
  } catch {
    return null;
  }
}

const MIB = 1024 ** 2;
const mib = (bytes: number) => `${(bytes / MIB).toFixed(2)} MiB`;

const parts = [...new Set(readdirSync(EVIDENCE).flatMap((name) => /^gate-12-live-(?:checkpointed|baseline)-(.+)$/u.exec(name)?.[1] ?? []))].sort();

test("each part of the gate ran, checkpointed and as its baseline", () => {
  assert.ok(parts.length > 0, `${EVIDENCE} holds no part's record`);
});

for (const part of parts) {
  test(`part ${part}: the Sentry's own memory grows by under ${mib(MAX_SENTRY_GROWTH_BYTES)} a checkpoint beyond its baseline's`, () => {
    const checkpointed = evidenceOf("checkpointed", part);
    const baseline = evidenceOf("baseline", part);
    assert.ok(checkpointed && baseline, `part ${part} left ${checkpointed ? "no baseline" : "no checkpointed run"}'s record`);
    assert.deepEqual([checkpointed.checkpointing, baseline.checkpointing], [true, false]);
    for (const [kind, run] of [["checkpointed", checkpointed], ["baseline", baseline]] as const) {
      assert.equal(run.failure, null, `part ${part}'s ${kind} run failed: ${run.failure}`);
      assert.equal(run.checkpoints, run.asked, `part ${part}'s ${kind} run took ${run.checkpoints} cycles of ${run.asked}`);
    }
    const sentry = (run: Evidence) => trend(run.cycles, ({ sentry: { anonBytes } }) => anonBytes);
    const kinds = Object.keys(baseline.cycles.at(-1)?.sandbox ?? {});
    console.log(`part ${part}, a cycle: the Sentry's own ${mib(sentry(checkpointed))} checkpointed, ${mib(sentry(baseline))} as its baseline; ` +
      kinds.map((kind) => `the sandbox's ${kind} ${mib(trend(checkpointed.cycles, ({ sandbox }) => sandbox[kind] ?? 0))} and ${mib(trend(baseline.cycles, ({ sandbox }) => sandbox[kind] ?? 0))}`).join(", "));
    const beyond = sentry(checkpointed) - sentry(baseline);
    assert.ok(beyond < MAX_SENTRY_GROWTH_BYTES, `part ${part}: the Sentry's own memory grows by ${mib(beyond)} a checkpoint beyond its baseline's`);
  });
}
