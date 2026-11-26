import assert from "node:assert/strict";
import { test } from "node:test";

import { goDurationMs, restoreLogged, saveLogged } from "../runsc-log.ts";

/** That `actual` is `expected`, within what the floating point sums of the logs' times lose. */
function near(actual: number | null | undefined, expected: number): void {
  assert.ok(typeof actual === "number" && Math.abs(actual - expected) < 1e-6, `${actual} is not ${expected}`);
}

test("a Go duration is read in milliseconds, whatever its units", () => {
  near(goDurationMs("1.5s"), 1500);
  near(goDurationMs("250ms"), 250);
  assert.equal(goDurationMs("1m2.5s"), 62_500);
  near(goDurationMs("800µs"), 0.8);
});

// A live save's lines, as runsc logged them on the benchmark's runner (eaucoin.8).
const SAVED = [
  "I1118 17:09:59.715164       1 dirty_wp.go:67] Dirty tracking: armed 5 MemoryManagers in 215.366483ms (unit 4096 bytes)",
  "I1118 17:10:10.789104       1 precopy.go:422] Pre-copy done after 3 rounds (converged): 3923578880 bytes in 11.308657311s",
  "I1118 17:10:10.789149       1 state.go:161] Sandbox save started, pausing all tasks.",
  "I1118 17:10:10.850591       1 kernel.go:985] CPUID save took [92.843µs].",
  "I1118 17:10:10.850655       1 kernel.go:997] Pausing root network namespace took [25.037µs].",
  "I1118 17:10:10.873097       1 kernel.go:1007] Kernel save took [22.427411ms].",
  "I1118 17:10:11.040600       1 kernel.go:1166] Memory files save took [189.03328ms].",
  "I1118 17:10:11.040646       1 kernel.go:1046] Overall save took [251.00955ms].",
  "I1118 17:10:11.043146       1 state.go:168] Tasks resumed after save, paused for 253.970103ms.",
  "I1118 17:10:11.104013       1 state.go:308] Collected the save's garbage and returned it to the host in 55.854196ms, the world stopped for 1.542735ms of it: heap in use 54654600 -> 13675288 bytes, held from the host 100851712 -> 17055744 bytes",
];

test("a save's pause, pre-copy and splits are its last save's, as its boot log says them", () => {
  const logged = saveLogged(SAVED);
  near(logged.pauseMs, 253.970103);
  assert.equal(logged.passes, 3);
  assert.equal(logged.precopyBytes, 3_923_578_880);
  near(logged.kernelMs, 22.427411);
  near(logged.memoryMs, 189.03328);
  near(logged.overallMs, 251.00955);
  near(logged.split["gc_ms"], 55.854196);
  near(logged.split["gc_world_stopped_ms"], 1.542735);
  // A stopping save after it: its own pause and splits, the live save's pre-copy still the last said.
  const stopped = saveLogged([...SAVED, "I1118 17:11:27.508155       1 state.go:161] Sandbox save started, pausing all tasks.", "I1118 17:11:36.697778       1 kernel.go:1046] Overall save took [9.188861666s].", "I1118 17:11:36.875365       1 state.go:168] Tasks resumed after save, paused for 9.367186956s."]);
  near(stopped.pauseMs, 9367.186956);
  near(stopped.overallMs, 9188.861666);
  assert.equal(stopped.kernelMs, null);
});

test("a restore is timed from its boot log's first line to its success, a standby's from its restore's own start", () => {
  const lines = [
    "I1118 17:12:40.000000       1 main.go:1] Starting the sandbox",
    "I1118 17:12:40.844415       1 restore.go:200] Restoring a total of 2 containers",
    "I1118 17:12:42.440927       1 kernel.go:1251] Kernel load took [61.097902ms].",
    "I1118 17:12:42.593900       1 kernel.go:1300] Overall load took [222.752625ms] after async work",
    "I1118 17:12:42.371345       1 save_restore.go:3138] Async page loading completed in 1.47s (36200448 bytes, 24.626 MB/s); 0 waiters",
    "I1118 17:12:48.962410       1 save_restore.go:3138] Async page loading completed in 8.079s (3244572672 bytes, 401.616 MB/s); 62 waiters",
    "I1118 17:12:48.963937       1 restore.go:822] Restore successful",
  ];
  const made = restoreLogged(lines);
  near(made.runscRestoreMs, 8963.937);
  near(made.kernelLoadMs, 61.097902);
  near(made.toRunningMs, 2593.9);
  assert.equal(made.loadedBytes, 36_200_448 + 3_244_572_672);
  assert.equal(made.awaitedBytes, null);
  const standby = restoreLogged(lines, true);
  near(standby.runscRestoreMs, 8119.522);
});

test("what a restored sandbox touched before its pages were read, and waited for, is the sum of its layers' waits, as runsc says them", () => {
  const lines = [
    "I1125 15:52:29.722498       1 save_restore.go:3578] Async page loading completed in 1.2s (90435584 bytes, 75.363 MB/s; 0 bytes not read, freed or decommitted first); 0 waiters waited 0s~0s for 0 bytes; background reads in flight bounded to 786432 bytes",
    "I1125 15:52:32.252498       1 save_restore.go:3578] Async page loading completed in 2.53s (1011326976 bytes, 399.796 MB/s; 0 bytes not read, freed or decommitted first); 488 waiters waited 960ms~1.252s for 15323136 bytes; background reads in flight bounded to 786432 bytes",
  ];
  const logged = restoreLogged(lines);
  assert.equal(logged.loadedBytes, 90_435_584 + 1_011_326_976);
  assert.equal(logged.awaitedBytes, 15_323_136);
});
