/**
 * What a benchmark job runs, as bench.yml's job sets it in the environment: where its results go,
 * its store profile, its variant, its cases and how many runs of each; and what every result line
 * says of the runner, the store and the releases under test.
 */
import { readFileSync } from "node:fs";
import { join } from "node:path";

import { NODE_PINS } from "../../cli/src/cluster/node.ts";
import { TARGET } from "../../test/e2e/harness.ts";

function variable(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is not set`);
  return value;
}

/** Where the job writes results.jsonl, spans.jsonl and logs/, and the runner's host.json is. */
export const BENCH_DIR = variable("BENCH_DIR");
export const PROFILE = variable("BENCH_PROFILE");
export const RUNS = Number(process.env["BENCH_RUNS"] || 20);

/**
 * The variant, one a job:
 * - `as-shipped`: alasio as it runs, its live save asked of the node agent as the gateway's
 *   keepLive asks it, its release a delta of the live save's image, the node's page cache dropped
 *   before every restore;
 * - `kept-cache`: the same, the page cache left as it is for the warm restore, as a node running
 *   other sandboxes keeps the sandbox image's files;
 * - `whole`: no live save, as of a session released before its first, its release a whole
 *   stop-and-copy;
 * - `restored`: as shipped, the session released and restored once before its saves, as one alasio
 *   released before is, so the images of its saves carry the working set its restore recorded;
 * - `chain-N`: as shipped, N - 1 live saves taken, each a delta of the one before, so the
 *   release's delta makes a chain of N images;
 * - `idle-saves`: as shipped, but the session left idle after its first live save and saved live
 *   again 5 s, 30 s, 120 s and 5 min into its idleness, and released 5 min after the last, each
 *   save's idleness in its details (IDLE_BEFORE_SAVES_S, IDLE_BEFORE_RELEASE_S);
 * - `trace`: as shipped, each restore's pod watched every 50 ms and kubelet's journal of it kept,
 *   for the steps between the pod's creation and runsc's start that no span covers;
 * - `mixed-release`, `mixed-saves`, `mixed-fill`, `mixed-all`: as shipped, each restore beside a
 *   load of other sessions' (./mixed.ts): a release and its compaction, two live saves, a cache
 *   fill after a cold restore, or all of them;
 * - `turn-replay`: turns replayed from a message at the webhook to their first exec, against the
 *   session in each tier (./turns.ts), in place of the save and restore phases.
 */
export const VARIANT = variable("BENCH_VARIANT");
const CHAIN = /^chain-([1-9][0-9]*)$/u.exec(VARIANT);
const MIXED = /^mixed-(release|saves|fill|all)$/u.exec(VARIANT);
if (!["as-shipped", "kept-cache", "whole", "restored", "trace", "turn-replay", "idle-saves"].includes(VARIANT) && !CHAIN && !MIXED) {
  throw new Error(`BENCH_VARIANT is ${VARIANT}: no variant of the benchmark's (./settings.ts VARIANT)`);
}
/**
 * How long the session is left idle before each live save of the `idle-saves` variant, in seconds:
 * the first right after the session's work, the others into its idleness, as one waiting on its
 * user is saved.
 */
const IDLE_SAVES_S = [0, 5, 30, 120, 300] as const;
/** How long the session is left idle before each live save a run takes, in seconds. */
export const IDLE_BEFORE_SAVES_S: readonly number[] = VARIANT === "idle-saves" ? IDLE_SAVES_S : [];
/** How long the session is left idle after its last live save before its release, in seconds. */
export const IDLE_BEFORE_RELEASE_S = VARIANT === "idle-saves" ? 300 : 0;
/** The live saves a run takes before its release. */
export const LIVE_SAVES = VARIANT === "whole" ? 0 : CHAIN ? Number(CHAIN[1]) - 1 : VARIANT === "idle-saves" ? IDLE_SAVES_S.length : 1;
/** The load a mixed variant's restores run beside, or null. */
export const MIXED_LOAD = (MIXED?.[1] ?? null) as "release" | "saves" | "fill" | "all" | null;
/** Whether the node's page cache is dropped before the warm restore: kept by `kept-cache` alone. */
export const PAGE_CACHE = VARIANT === "kept-cache" ? "kept" : "dropped";

/** The benchmark's own package (bench/common): its driver, store tools and aggregation, built (dist/). */
export const COMMON = new URL("../common/", import.meta.url);
export const readCommon = (relative: string): string => readFileSync(new URL(relative, COMMON), "utf8");
export const PROFILES = JSON.parse(readCommon("store/profiles.json")) as Readonly<Record<string, { readonly emulated: boolean }>>;
if (!PROFILES[PROFILE]) throw new Error(`BENCH_PROFILE is ${PROFILE}, not one of ${Object.keys(PROFILES).join(", ")}`);
const ALL_CASES = JSON.parse(readCommon("cases.json")) as readonly { readonly fixture: string; readonly size_mib: number }[];
const ASKED = (process.env["BENCH_CASES"] ?? "").split(/[\s,]+/u).filter(Boolean);
/** The cases asked, as fixture/MiB, of the benchmark's fixtures at any size; every case of cases.json where none is. */
export const CASES = ASKED.length === 0 ? ALL_CASES : ASKED.map((asked) => {
  const [fixture = "", size = ""] = asked.split("/");
  if (!ALL_CASES.some((each) => each.fixture === fixture) || !/^[1-9][0-9]*$/u.test(size)) throw new Error(`BENCH_CASES asks for ${asked}, no fixture/MiB of the benchmark's fixtures`);
  return { fixture, size_mib: Number(size) };
});

/**
 * The target alasio is installed on: the host alone. The harness runs node, nsenter, iptables and
 * crictl on each node as programs of the host's, which the docker target's nodes do not have, and
 * the bar's numbers are of the runner itself, one node, as the box is.
 */
if (TARGET !== "host") throw new Error(`the benchmark runs on the host target alone, not ${TARGET}`);

/** What host.sh said of the runner (bench/common/host.sh). */
export const HOST = JSON.parse(readFileSync(join(BENCH_DIR, "host.json"), "utf8")) as { readonly copy_mib_s: number } & Readonly<Record<string, unknown>>;
/** The gVisor release under test: the newest of alasio's pins. */
export const RELEASE_UNDER_TEST = NODE_PINS.gvisor.releases[0]?.release ?? "";
/** The release of alasio under test, and of the bayma its sessions run (sandbox/agent/Dockerfile's BAYMA_IMAGE). */
export const ALASIO_UNDER_TEST = (JSON.parse(readFileSync(new URL("../../package.json", import.meta.url), "utf8")) as { readonly version: string }).version;
export const BAYMA_UNDER_TEST = /^ARG BAYMA_IMAGE=\S+:([0-9][^@\s]*)/mu.exec(readFileSync(new URL("../../sandbox/agent/Dockerfile", import.meta.url), "utf8"))?.[1] ?? "";
/** toxiproxy's release, which shapes the emulated profiles (./node.ts shapeAgents). */
export const TOXIPROXY = "2.13.0";
