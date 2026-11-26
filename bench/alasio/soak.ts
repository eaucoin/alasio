/**
 * The pool-coverage soak (BENCH_MODE=soak): new sessions arrive at the box's rate, their gaps drawn
 * from the box's message gaps (one conversation on the box, 25 days) compressed BENCH_SOAK_COMPRESSION
 * times, for BENCH_SOAK_MINUTES; each binds a pool's free pod, as a first turn does, holds it
 * HOLD_MS, and is forgotten. Of each bind: how long it took, the pod it got, and whether that pod
 * was still opening the image's binaries (its start.sh's warm-up, where the image is served through
 * passthrough) as the bind began, and how long after it finished. Every SAMPLE_MS: the pool's free
 * pods, running, and how many of them have finished their warm-up; every MEMORY_SAMPLE_MS, what
 * each node's memory holds, its sessions among it; and of each session, whether it held its pod to
 * its end. Lines of logs/soak.jsonl.
 */
import { appendFileSync } from "node:fs";
import { randomBytes } from "node:crypto";
import { join } from "node:path";

import type { V1Pod } from "@kubernetes/client-node";

import { FREE_LABEL } from "../../src/binder/names.ts";
import type { NodeMemory } from "../../src/node-agent/memory.ts";
import { bindSession, gatewayCall, kube, mainToken, nodeAgents, ref, SESSIONS } from "../../test/e2e/harness.ts";
import { agentOk, errorText, forgetWorkspace, sleep } from "./alasio.ts";
import { BENCH_DIR } from "./settings.ts";

/** The box's message → next message gaps, in seconds, by quantile. */
const GAP_QUANTILES: readonly (readonly [number, number])[] = [[0, 5], [0.05, 34], [0.1, 102], [0.25, 236], [0.5, 1_717], [0.75, 11_444], [0.9, 34_088], [0.95, 50_997], [0.99, 160_435], [1, 167_790]];
const COMPRESSION = Number(process.env["BENCH_SOAK_COMPRESSION"] || 120);
const SOAK_MS = Number(process.env["BENCH_SOAK_MINUTES"] || 60) * 60_000;
/** The longest a compressed gap is, so a soak sees arrivals throughout. */
const GAP_MOST_MS = 600_000;
/** How long a session holds its pod: the box's median turn, 56 s, halved. */
const HOLD_MS = 28_000;
const SAMPLE_MS = 5_000;
const WARMED = "alasio-start: warmed";

/** A gap drawn from GAP_QUANTILES, linearly between quantiles, compressed, in ms. */
function gapMs(): number {
  const u = Math.random();
  const upper = GAP_QUANTILES.findIndex(([q]) => q >= u);
  const [q0, g0] = GAP_QUANTILES[Math.max(0, upper - 1)] ?? [0, 0];
  const [q1, g1] = GAP_QUANTILES[upper] ?? [1, 0];
  const seconds = q1 === q0 ? g1 : g0 + ((u - q0) / (q1 - q0)) * (g1 - g0);
  return Math.min(GAP_MOST_MS, Math.max(1_000, (seconds * 1000) / COMPRESSION));
}

const line = (record: Record<string, unknown>) => appendFileSync(join(BENCH_DIR, "logs", "soak.jsonl"), `${JSON.stringify(record)}\n`);

/** When `pod`'s warm-up ended, as its container's log stamps its line (ms); null where it said none. */
async function warmedAt(pod: string): Promise<{ readonly at: number | null; readonly said: string | null }> {
  const logs = await kube.logs(SESSIONS, pod, "bayma", { timestamps: true }).catch(() => "");
  const found = logs.split("\n").find((each) => each.includes(WARMED));
  if (found === undefined) return { at: null, said: null };
  return { at: new Date(found.split(" ")[0] ?? "").getTime(), said: found.slice(found.indexOf(WARMED)) };
}

/** One arrival: a new session bound, held, forgotten; its line. */
async function arrival(number: number): Promise<void> {
  const workspace = `fs-${randomBytes(6).toString("hex")}`;
  const started = Date.now();
  try {
    const created = await gatewayCall(await mainToken(workspace), "POST", "/workspace");
    if (created.status !== 204) throw new Error(`the gateway made no workspace: ${created.status} ${created.said}`);
    const bindFrom = Date.now();
    const { name } = await bindSession(workspace);
    const bindMs = Date.now() - bindFrom;
    const pod = await kube.get<V1Pod>(ref("Pod", name, SESSIONS));
    const warmed = await warmedAt(name);
    line({
      kind: "bind", number, at: bindFrom, bind_ms: bindMs, pod: name,
      pod_age_ms: pod?.metadata?.creationTimestamp ? bindFrom - new Date(pod.metadata.creationTimestamp).getTime() : null,
      warmed: warmed.said, warming_at_bind: warmed.at !== null && warmed.at > bindFrom, warmed_after_bind_ms: warmed.at === null ? null : warmed.at - bindFrom,
    });
    await sleep(HOLD_MS);
    // Whether it held its pod to the end, or lost it meanwhile, as one another's OOM ends would.
    const held = await kube.get<V1Pod>(ref("Pod", name, SESSIONS));
    const ended = held?.status?.containerStatuses?.flatMap(({ name: container, state, lastState, restartCount }) =>
      state?.terminated || lastState?.terminated || restartCount > 0 ? [{ container, reason: state?.terminated?.reason ?? lastState?.terminated?.reason ?? null, restarts: restartCount }] : []) ?? [];
    line({ kind: "held", number, pod: name, gone: held === null || held.metadata?.deletionTimestamp !== undefined, ended });
  } catch (error) {
    line({ kind: "bind", number, at: started, failure: errorText(error).slice(0, 400) });
  } finally {
    await forgetWorkspace(workspace);
  }
}

/** The pool's free pods now: running, and how many of those have finished their warm-up. */
async function sample(): Promise<void> {
  const free = (await kube.list<V1Pod>("Pod", { namespace: SESSIONS, labelSelector: `${FREE_LABEL}=true` })).filter(({ metadata }) => !metadata?.deletionTimestamp);
  const running = free.filter(({ status }) => status?.phase === "Running");
  let warmed = 0;
  for (const pod of running) if ((await warmedAt(pod.metadata?.name ?? "")).at !== null) warmed += 1;
  line({ kind: "pool", at: Date.now(), free: free.length, running: running.length, warmed });
}

/** How often the soak reads what each node's memory holds. */
const MEMORY_SAMPLE_MS = 15_000;

/**
 * What each node's memory holds, as its agent says it (GET /memory): its bound sandboxes, the
 * sessions it runs, and its standbys and free pods, each count and bytes; the agent's own, split,
 * with its cgroup's events; and what its guard limited or ended (GET /memory/guard).
 */
async function memorySample(): Promise<void> {
  for (const node of (await nodeAgents()).keys()) {
    const memory = JSON.parse(await agentOk(node, "GET", "/memory")) as NodeMemory;
    const guard = JSON.parse(await agentOk(node, "GET", "/memory/guard")) as { readonly sandboxes: readonly Record<string, unknown>[] };
    const of = (state: string) => memory.sandboxes.filter((sandbox) => sandbox.state === state);
    const states = Object.fromEntries(["bound", "standby", "free"].map((state) => [state, { count: of(state).length, bytes: of(state).reduce((sum, { bytes }) => sum + (bytes ?? 0), 0) }]));
    line({
      kind: "memory", at: Date.now(), node, states, available_bytes: memory.availableBytes, budget_bytes: memory.budgetBytes, budgeted_bytes: memory.budgetedBytes,
      agent: { bytes: memory.agentBytes, anon: memory.agentAnonBytes, file: memory.agentFileBytes, events: memory.agentEvents },
      guard: guard.sandboxes,
    });
  }
}

/** The soak, as the module says. */
export async function soak(): Promise<void> {
  const end = Date.now() + SOAK_MS;
  let sampling = true;
  const sampler = (async () => {
    while (sampling) {
      await sample().catch((error: unknown) => line({ kind: "pool", at: Date.now(), failure: errorText(error).slice(0, 200) }));
      await sleep(SAMPLE_MS);
    }
  })();
  const memorySampler = (async () => {
    while (sampling) {
      await memorySample().catch((error: unknown) => line({ kind: "memory", at: Date.now(), failure: errorText(error).slice(0, 200) }));
      await sleep(MEMORY_SAMPLE_MS);
    }
  })();
  const arrivals: Promise<void>[] = [];
  for (let number = 1; Date.now() < end; number++) {
    arrivals.push(arrival(number));
    await sleep(gapMs());
  }
  await Promise.all(arrivals);
  sampling = false;
  await sampler;
  await memorySampler;
}
