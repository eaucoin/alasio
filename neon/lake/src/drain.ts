/**
 * A reader's drain, its container's preStop hook, which runs as a scale-in or a node's drain
 * stops it, before its endpoint is told to stop:
 *
 *   node src/drain.ts
 *
 * Its endpoint, once stopped, takes no more queries, and its container's end ends every
 * process in it: `alasio lake`'s query (./query.ts), which runs in the container and asks the
 * endpoint beside it, would end with it, though the reader it picked was not stopping yet as
 * it picked it. So the drain waits DRAIN_SETTLE_MS, for a query run in the reader just before
 * its stop to start, then until no query runs in the container and the endpoint has answered
 * every query it took, Grafana's among them. A query is answered or given up within
 * QUERY_WITHIN_MS, so the drain ends within DRAIN_WITHIN_MS, which the reader's grace to stop
 * outlasts (cli/src/manifests/lake.ts).
 */
import { appendFileSync } from "node:fs";
import { readdir, readFile } from "node:fs/promises";

import { loadEndpointConfig } from "./config.ts";
import { QUERY_WITHIN_MS } from "./query.ts";

/** How long a query run in the reader just before its stop is waited for to start. */
export const DRAIN_SETTLE_MS = 10_000;

/** How often the drain looks again at the queries under way. */
const DRAIN_POLL_MS = 250;

/** The most a drain waits: the settling, then a query started as it ends, to its end. */
export const DRAIN_WITHIN_MS = DRAIN_SETTLE_MS + QUERY_WITHIN_MS + 5_000;

/** The script `alasio lake` runs in the container, as its arguments name it. */
const QUERY_SCRIPT = /(^|\/)src\/query\.ts$/u;

/** How many processes of `proc`, a procfs, run the query script, other than `self`. */
export async function queriesRunning(proc = "/proc", self = process.pid): Promise<number> {
  const pids = (await readdir(proc)).filter((name) => /^\d+$/u.test(name) && Number(name) !== self);
  const running = await Promise.all(pids.map(async (pid) => {
    // A process that ended as it was read runs nothing.
    const cmdline = await readFile(`${proc}/${pid}/cmdline`, "utf8").catch(() => "");
    return cmdline.split("\0").some((argument) => QUERY_SCRIPT.test(argument));
  }));
  return running.filter(Boolean).length;
}

/** The queries the endpoint at `base` has taken and not answered yet; none where it does not say. */
export async function queriesTaken(base: string): Promise<number> {
  const response = await fetch(`${base}/queue`, { signal: AbortSignal.timeout(2_000) }).catch(() => null);
  const body: unknown = response?.ok ? await response.json().catch(() => null) : null;
  return typeof body === "object" && body !== null && "queries" in body && typeof body.queries === "number" ? body.queries : 0;
}

export interface DrainOptions {
  /** The queries under way: those run in the container and those the endpoint took. */
  underway: () => Promise<number>;
  settleMs?: number;
  withinMs?: number;
  pollMs?: number;
  sleep?: (ms: number) => Promise<void>;
  now?: () => number;
}

/** Waits for the queries under way to end, as the module says: whether they did within its bound. */
export async function drain(options: DrainOptions): Promise<boolean> {
  const { underway, settleMs = DRAIN_SETTLE_MS, withinMs = DRAIN_WITHIN_MS, pollMs = DRAIN_POLL_MS, now = Date.now } = options;
  const sleep = options.sleep ?? ((ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms)));
  const deadline = now() + withinMs;
  await sleep(settleMs);
  for (;;) {
    if ((await underway()) === 0) return true;
    if (now() >= deadline) return false;
    await sleep(pollMs);
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const { port } = loadEndpointConfig();
  const base = `http://127.0.0.1:${port}`;
  const started = Date.now();
  // Past its bound, the reader stops all the same, as its grace to stop would have it.
  const drained = await drain({ underway: async () => (await queriesRunning()) + (await queriesTaken(base)) });
  const line = JSON.stringify({ time: new Date().toISOString(), message: drained ? "drained" : "stopping with queries under way", ms: Date.now() - started, withinMs: DRAIN_WITHIN_MS });
  // A hook's output goes nowhere: the endpoint's, PID 1's, is the container's log.
  try {
    appendFileSync("/proc/1/fd/1", `${line}\n`);
  } catch {
    console.log(line);
  }
}
