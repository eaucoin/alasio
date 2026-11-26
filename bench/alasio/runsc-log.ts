/**
 * What a sandbox's boot log says of its saves and its restore: runsc's own timers, which it logs
 * as the benchmark asks it to (bootLogs in ./node.ts gives each kept release's runsc a boot log),
 * read from the log's lines. Pure, for ./test/runsc-log.test.ts.
 */

/** A Go duration, as runsc logs one (1.5s, 250ms, 1m2.5s, 800µs), in milliseconds. */
export function goDurationMs(text: string): number {
  const units: Record<string, number> = { h: 3_600_000, m: 60_000, s: 1000, ms: 1, "µs": 0.001, us: 0.001, ns: 0.000001 };
  let total = 0;
  for (const [, value = "0", unit = "s"] of text.matchAll(/([0-9.]+)(h|ms|m|s|µs|us|ns)/gu)) total += Number(value) * (units[unit] ?? 0);
  return total;
}

/** The time of day of a runsc log line (I1026 20:01:02.123456 ...), in milliseconds. */
export function logTimeMs(line: string): number | null {
  const matched = /^[IWEF]\d{4} (\d{2}):(\d{2}):(\d{2}\.\d+)/u.exec(line);
  return matched ? (Number(matched[1]) * 3600 + Number(matched[2]) * 60 + Number(matched[3])) * 1000 : null;
}

/** The last bracketed Go duration a line matching `pattern` of `lines` gives (runsc's "... took [1.5s]."), in milliseconds, or null. */
function tookMs(lines: readonly string[], pattern: RegExp): number | null {
  const said = lines.findLast((line) => pattern.test(line));
  const duration = said === undefined ? undefined : /took \[([^\]]+)\]/u.exec(said)?.[1];
  return duration === undefined ? null : goDurationMs(duration);
}

/**
 * What a sandbox's boot log says of its last save: its pause, its pre-copy's rounds and bytes,
 * and the save's own splits, of the lines after its last "Sandbox save started": the kernel's
 * state, the memory files', and the whole, and the collection before it with the time it
 * stopped the world.
 */
export function saveLogged(lines: readonly string[]): {
  readonly pauseMs: number | null;
  readonly passes: number | null;
  readonly precopyBytes: number | null;
  readonly kernelMs: number | null;
  readonly memoryMs: number | null;
  readonly overallMs: number | null;
  readonly split: Record<string, number | null>;
  readonly said: string[];
} {
  const started = lines.findLastIndex((line) => line.includes("Sandbox save started"));
  const last = started === -1 ? [] : lines.slice(started);
  const paused = last.findLast((line) => line.includes("Tasks resumed after save, paused for"));
  const done = lines.findLast((line) => line.includes("Pre-copy done after"));
  const skipped = lines.findLast((line) => line.includes("Pre-copy skipped"));
  // The last save's scan of its memory and what it wrote, zero pages among them.
  const scanned = last.findLast((line) => line.includes("save scanning took"));
  const written = last.findLast((line) => line.includes("bytes written to the pages file"));
  const collected = last.findLast((line) => line.includes("Collected the save's garbage"));
  // When the save paused the sandbox, and each arming of its dirty tracking, its time and size, before and after.
  const pausing = started === -1 ? undefined : lines[started];
  const armed = lines.filter((line) => line.includes("Dirty tracking: armed")).slice(-10);
  const timers = last.filter((line) => /(Kernel|Memory files|Overall|Split filesystem|CPUID) save took|Pausing root network namespace took|Kernel save stats/u.test(line));
  const rounds = /Pre-copy done after (\d+) rounds \([^)]*\): (\d+) bytes/u.exec(done ?? "");
  const gc = /in (\S+), the world stopped for (\S+) of it/u.exec(collected ?? "");
  const split = {
    gc_ms: gc?.[1] === undefined ? null : goDurationMs(gc[1]),
    gc_world_stopped_ms: gc?.[2] === undefined ? null : goDurationMs(gc[2]),
    cpuid_ms: tookMs(last, /CPUID save took/u),
    network_pause_ms: tookMs(last, /Pausing root network namespace took/u),
    kernel_ms: tookMs(last, /Kernel save took/u),
    split_fs_ms: tookMs(last, /Split filesystem save took/u),
    memory_files_ms: tookMs(last, /Memory files save took/u),
    overall_ms: tookMs(last, /Overall save took/u),
  };
  return {
    pauseMs: paused ? goDurationMs(/paused for (\S+?)\.?$/u.exec(paused)?.[1] ?? "") : null,
    passes: rounds ? Number(rounds[1]) : skipped ? 0 : null,
    precopyBytes: rounds ? Number(rounds[2]) : skipped ? 0 : null,
    kernelMs: split.kernel_ms,
    memoryMs: split.memory_files_ms,
    overallMs: split.overall_ms,
    split,
    said: [pausing, paused, done, skipped, scanned, written, collected, ...timers, ...armed].filter((line): line is string => line !== undefined),
  };
}

/**
 * What a restored sandbox's boot log says: how long runsc took from its start to the restore's
 * success, every page loaded; its splits, the kernel's state and the memory files' metadata
 * loaded, and its start to its load's end, runnable; the bytes its layers' loaders loaded, and of
 * them those the sandbox touched first and waited for, as runsc says each layer's; and those
 * lines.
 */
export function restoreLogged(lines: readonly string[], standby = false): {
  readonly runscRestoreMs: number | null;
  readonly kernelLoadMs: number | null;
  readonly memoryLoadMs: number | null;
  readonly toRunningMs: number | null;
  readonly loadedBytes: number | null;
  readonly awaitedBytes: number | null;
  readonly said: string[];
} {
  // A standby's sandbox booted long before its restore: its restore's own start, as its boot process takes it up, instead.
  const first = lines.find((line) => logTimeMs(line) !== null && (!standby || /Restoring a total of \d+ containers/u.test(line)));
  const restored = lines.find((line) => line.includes("Restore successful"));
  const loaded = lines.find((line) => line.includes("Overall load took"));
  // Each layer's loader's lines: its working set, its end, its rate and its budget.
  const loading = lines.filter((line) => /Async page loading (completed|read|: )|async loading \d+ bytes from|working set holds/u.test(line));
  const timers = lines.filter((line) => /(Kernel|Memory files|Overall|CPUID) load took|Starting to restore|Restored container|Dirty tracking: armed/u.test(line));
  const completed = loading.flatMap((line) => /Async page loading completed in \S+ \((\d+) bytes/u.exec(line)?.[1] ?? []);
  // Of each layer's, what the sandbox touched before its loader had read it, and waited for: none where it was all read first.
  const awaited = loading.flatMap((line) => /Async page loading completed in .* \d+ waiters waited \S+ for (\d+) bytes/u.exec(line)?.[1] ?? []);
  const from = first ? logTimeMs(first) : null;
  const between = (line: string | undefined) => {
    const to = line ? logTimeMs(line) : null;
    return from !== null && to !== null ? to - from : null;
  };
  return {
    runscRestoreMs: between(restored),
    kernelLoadMs: tookMs(lines, /Kernel load took/u),
    memoryLoadMs: tookMs(lines, /Memory files load took/u),
    toRunningMs: between(loaded),
    loadedBytes: completed.length === 0 ? null : completed.reduce((sum, bytes) => sum + Number(bytes), 0),
    awaitedBytes: awaited.length === 0 ? null : awaited.reduce((sum, bytes) => sum + Number(bytes), 0),
    said: [restored, ...timers, ...loading].filter((line): line is string => line !== undefined),
  };
}
