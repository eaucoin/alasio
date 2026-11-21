/**
 * The `turn-replay` variant: the latency a user's turn waits on its session, beyond what it waits on
 * a running one: a turn replayed from its message at the webhook to its first exec, against the
 * session in each tier.
 *
 * The operator's conversation is given a session workspace of its own, as /workspace gives one, and
 * the fixture is set up in a session of its bayma. Then, for each tier, the session is put in it
 * (T0: running; T3: released, its image on the node, the node's page cache dropped; T4: released,
 * every node's cache of it removed, its image in the store alone), and a message is said to the
 * conversation, through the Telegram stand-in and alasio's webhook, as an operator says one. Its
 * turn starts as alasio starts one, binding the session first, and its harness, the stand-in for
 * Codex (test/e2e/codex-stand-in.ts), waits T_llm from its turn's start, as the model's thinking
 * takes it, then makes its first tool call, the fixture's first exec, in a span of its own.
 *
 * Both of the wait's moments are the gateway's, on its own clock (./spans.ts turnWait): the arrival
 * at its front of the turn's first request for the session, and the hand-over to its proxy of the
 * exec's request. The added latency is that wait less the same of the run's T0 turn, the session
 * running.
 */
import { execFile } from "node:child_process";

import { sessionKey } from "../../src/sandbox/names.ts";
import { bindSession, boundPodOf, recordSession } from "../../test/e2e/harness.ts";
import { forward, operator } from "../../test/e2e/operator.ts";
import { TELEGRAM } from "../../test/e2e/stand-ins.ts";
import type { RecordedCall } from "../../test/e2e/telegram-stub.ts";
import { coldCaches, driver, errorText, forgetWorkspace, gone, release } from "./alasio.ts";
import { dropPageCache } from "./node.ts";
import { emit, emitSpans, type Result, type Tier } from "./results.ts";
import { awaitSpans, type Span, turnWait } from "./spans.ts";
import { COMMON, HOST } from "./settings.ts";

/** The tiers a run replays a turn against, in order: T0 first, which the others' added latency is of. */
const TIERS: readonly Tier[] = ["T0", "T3", "T4"];
/** How long the harness's model takes to its first tool call, from its turn's start, T_llm: 3 s, the quickest twentieth of the box's turns' (BENCH_T_LLM_MS where the job says another). */
const T_LLM_MS = Number(process.env["BENCH_T_LLM_MS"] ?? 3000);
/** How long a replayed turn may take to its answer. */
const TURN_BOUND_MS = 600_000;

/** The operator, through the Telegram stand-in, once the job's first turn replay asks for it. */
let tg: ReturnType<typeof operator> | null = null;

async function theOperator(): Promise<ReturnType<typeof operator>> {
  if (tg === null) {
    tg = operator((await forward(TELEGRAM)).base);
    // The conversation's harness is Codex's, whose stand-in replays the turn.
    await tg.calls();
    await tg.say("/service codex");
    await tg.waitFor((call) => /^Active: Codex$/mu.test(call.payload.text ?? ""));
  }
  return tg;
}

/** The text of a call alasio sent, its rich message's markdown for one. */
const textOf = (call: RecordedCall): string => String(call.payload.rich_message?.markdown ?? call.payload.text ?? "");

/** A new session workspace for the operator's conversation, made through /workspace as the operator makes one, and bound: its volume id. */
async function conversationWorkspace(): Promise<string> {
  const operating = await theOperator();
  await operating.calls();
  await operating.say("/workspace");
  await operating.choose(/New empty workspace/u);
  await operating.choose(/No internet/u);
  const created = await operating.waitFor((call) => /Created and mounted session workspace/u.test(textOf(call)));
  const volumeId = /fs-[0-9a-f]+/u.exec(textOf(created))?.[0];
  if (!volumeId) throw new Error(`alasio named no workspace: ${textOf(created)}`);
  recordSession(volumeId, "none");
  await bindSession(volumeId);
  return volumeId;
}

/** Puts the session of `workspace` in `tier`: running, bound; or released, and its caches then dropped or removed as the tier keeps them. */
async function intoTier(workspace: string, tier: Tier): Promise<Record<string, unknown>> {
  const bound = await boundPodOf(workspace);
  if (tier === "T0") {
    if (!bound) await bindSession(workspace);
    return { bound: (await boundPodOf(workspace))?.name ?? null };
  }
  if (!bound) throw new Error(`the session ${workspace} is not running, to be released into ${tier}`);
  const released = await release(workspace);
  if (released.status < 200 || released.status >= 300) throw new Error(`the gateway's release into ${tier} answered ${released.status}: ${released.said}`);
  await gone(bound.name);
  if (tier === "T3") {
    await dropPageCache(bound.node);
    return { released_ms: released.ms, page_cache: "dropped" };
  }
  return { released_ms: released.ms, cache_removed: await coldCaches(workspace) };
}

/**
 * One turn replayed against the session of `workspace`, its fixture's first exec its first tool
 * call: the exec's span, by its trace and its id, and the exec's own time as the harness saw it.
 */
async function replay(sessionId: string, code: string): Promise<{ readonly traceId: string; readonly spanId: string; readonly execMs: number; readonly saidAt: number }> {
  const operating = await theOperator();
  await operating.calls();
  const saidAt = Date.now();
  await operating.say(`bayma exec ${T_LLM_MS} ${sessionId}\n${code}`);
  const answered = await operating.waitFor((call) => /bayma exec: /u.test(textOf(call)), TURN_BOUND_MS);
  const said = /bayma exec: (.*)$/mu.exec(textOf(answered))?.[1] ?? "";
  const ok = /^ok ([0-9a-f]{32}) ([0-9a-f]{16}) ([0-9.]+)$/u.exec(said);
  if (!ok) throw new Error(`the turn's first exec did not answer: ${said}`);
  return { traceId: ok[1] ?? "", spanId: ok[2] ?? "", execMs: Number(ok[3]), saidAt };
}

/**
 * A run of the turn replay for a case: the conversation's workspace, the fixture set up in it, then
 * a turn against each tier, each a line of results.jsonl (phase `turn-replay`, its tier); what
 * fails is written as failed, and the run goes on to its cleanup.
 */
export async function replayTurns(fixture: string, sizeMib: number, run: number, runId: string): Promise<void> {
  const results: Result[] = [];
  const windows: { readonly result: Result; readonly fromMs: number; readonly toMs: number }[] = [];
  const started = Date.now();
  let workspace = "";
  let failure: string | null = null;
  try {
    workspace = await conversationWorkspace();
    const setup = await driver<{ readonly session_id: string }>(workspace, "setup", "--fixture", fixture, "--size-mib", String(sizeMib), "--run-id", runId, "--rate-mib-s", String(HOST.copy_mib_s), "--workspace", "/workspace");
    const code = await codeOf(fixture);
    const key = sessionKey(workspace);
    const waits = new Map<Tier, number>();
    for (const tier of TIERS) {
      const readied = await intoTier(workspace, tier);
      const { traceId, spanId, execMs, saidAt } = await replay(setup.session_id, code);
      const exec = { traceId, spanId };
      const { spans, complete } = await awaitSpans([workspace], saidAt - 1000, Date.now() + 60_000, [(found: readonly Span[]) => turnWait(found, key, exec, saidAt) !== null]);
      const waited = turnWait(spans, key, exec, saidAt);
      if (waited !== null) waits.set(tier, waited.waitMs);
      const running = waits.get("T0");
      const result: Result = {
        fixture, sizeMib, run, phase: "turn-replay", tier,
        ok: waited !== null,
        failure: waited !== null ? null : `the gateway's spans of the turn did not say its wait (complete: ${complete})`,
        metrics: { turn_wait_ms: waited?.waitMs ?? null, added_latency_ms: waited === null || running === undefined ? null : waited.waitMs - running, t_llm_ms: T_LLM_MS, probe_exec_ms: execMs },
        details: { workspace, readied, trace_id: traceId, span_id: spanId, said_wall_ms: saidAt, ...(waited === null ? {} : { first_ns: String(waited.firstNs), forwarded_ns: String(waited.forwardedNs) }) },
      };
      results.push(result);
      if (waited !== null) windows.push({ result, fromMs: Number(waited.firstNs) / 1e6, toMs: Number(waited.forwardedNs) / 1e6 });
    }
    const { spans } = await awaitSpans([workspace], started, Date.now() + 60_000, []);
    for (const { result, fromMs, toMs } of windows) emitSpans(result, fromMs, toMs, spans);
  } catch (error) {
    failure = errorText(error);
    console.error(`# ${fixture} ${sizeMib} MiB turn replay ${run} failed: ${failure}`);
  } finally {
    for (const result of results) emit(result);
    for (const tier of TIERS.slice(results.length)) emit({ fixture, sizeMib, run, phase: "turn-replay", tier, ok: false, failure: `not run: ${failure ?? "the run ended"}`, metrics: {} });
    if (workspace !== "") await forgetWorkspace(workspace);
  }
}

/** The fixture's first exec, as the driver's probe runs it (bench/common/driver/driver.ts call). */
async function codeOf(fixture: string): Promise<string> {
  const printed = await new Promise<string>((resolve, reject) => execFile(process.execPath, [new URL("dist/driver.mjs", COMMON).pathname, "code", "--fixture", fixture, "--method", "first"], (error, stdout) => (error ? reject(error) : resolve(stdout))));
  return (JSON.parse(printed.trim()) as { readonly code: string }).code;
}
