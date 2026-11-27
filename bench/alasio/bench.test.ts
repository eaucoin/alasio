/**
 * The memory-snapshot benchmark of alasio: alasio installed as the end-to-end runs install it
 * (test/e2e/harness.ts, its `bench` shard) on the runner itself, its sessions saved and restored
 * through its own path, each run of each case of bench/common/cases.json a line of
 * $BENCH_DIR/results.jsonl per phase, in the benchmark's result schema (bench/common/schema), and a
 * line of $BENCH_DIR/spans.jsonl per phase alasio's spans cover (./results.ts).
 *
 * A run of a case, but in the `turn-replay` variant (./turns.ts):
 * 1. exec-running: the probe's MCP session, control and first exec on the session as it runs,
 *    never stopped, which the first execs after a restore are reported net of;
 * 2. save-live: the node agent's live checkpoint, asked of the agent as the gateway's keepLive asks
 *    it; its unavailability the largest gap the fixture's ticker saw;
 * 3. save-stop: the gateway's release (POST /release), as alasio's own;
 * 4. restore-warm: the probe's first request to the gateway's bayma endpoint, which binds the
 *    session, which restores it from the node's cache, the node's page cache dropped first (kept,
 *    in the `kept-cache` variant);
 * 5. restore-cold: the workspace put back as it was at the release (a JuiceFS clone through the
 *    node agent), its snapshot made current again and its release record written again, and every
 *    node's cache of it removed, with the page cache, then restored as warm, from the bucket.
 *
 * The emulated store profiles put toxiproxy in each node agent's network namespace, the only client
 * of the snapshots bucket (bench/common/store/shape.ts). The sandboxes' boot logs (runsc's, which
 * the benchmark asks of each kept release) give the pause, the pre-copy rounds and the restore's
 * splits; alasio's spans, read from its lake once a run is over, give the product's steps.
 *
 *   bench/alasio/run.sh, in bench.yml's jobs
 */
import { mkdirSync, writeFileSync } from "node:fs";
import { randomBytes } from "node:crypto";
import { join } from "node:path";
import { after, before, test } from "node:test";

import type { V1Pod } from "@kubernetes/client-node";

import { BINDING_LABEL, keyHash } from "../../src/binder/names.ts";
import { compactionDue, type Manifest } from "../../src/node-agent/agent.ts";
import { gvisorDirectory, RUNSC_ROOT } from "../../src/node-agent/names.ts";
import { sessionKey } from "../../src/sandbox/names.ts";
import { dumpClusterState, kube, nodeAgents, onNode, SESSIONS, setUp, tearDown } from "../../test/e2e/harness.ts";
import {
  agentApi, agentOk, checkpointRequest, coldCaches, driver, endUnsaved, errorText, forgetWorkspace, gone, liveCheckpoint, loggedOf, msBetween, newSession, now, release, sleep,
  standbyArm, standbyFor,
} from "./alasio.ts";
import { mixedLoad } from "./mixed.ts";
import { bootLog, bootLogs, bucketBytes, countStore, dropPageCache, inAgentNetwork, k3sReady, probeStore, sandboxIdOf, sayK3sRestarted, sayRunner, shapeAgents, storeBytes, vacuum } from "./node.ts";
import { emit, emitSpans, type Phase, type Result, store } from "./results.ts";
import { probeExecMs, type Probed, restore, restoreBefore, restoreOutcome, restoreSpanMetrics } from "./restore.ts";
import { saveLogged } from "./runsc-log.ts";
import { awaitSpans, hasSpan, liveDurableMs, releaseDurableMs } from "./spans.ts";
import { BENCH_DIR, CASES, COMMON, IDLE_BEFORE_RELEASE_S, IDLE_BEFORE_SAVES_S, IDLE_UNTOUCHED, LIVE_SAVES, PAGE_CACHE, PROFILE, RELEASE_UNDER_TEST, RUNS, VARIANT } from "./settings.ts";
import { storeMeasures } from "./store.ts";
import { replayTurns } from "./turns.ts";

/** The job's ticker check failed: none of its measures is written. */
class Uncalibrated extends Error {}

/** Whether the job's ticker check has passed (calibrate). */
let calibrated = false;

/**
 * The job's check of the fixtures' ticker: the sandbox of the pod `name`, its container
 * `containerId`, paused for a second by runsc and resumed, the gap the ticker saw within what the
 * harness saw of the freeze, 20 ms either side. Written to logs/calibration.json; the job fails
 * where it is not.
 */
async function calibrate(workspace: string, sessionId: string, fixture: string, node: string, containerId: string): Promise<void> {
  const runsc = `${gvisorDirectory(RELEASE_UNDER_TEST)}/runsc --root=${RUNSC_ROOT}`;
  await driver(workspace, "mark", "--session", sessionId, "--fixture", fixture);
  const pauseStart = now();
  await onNode(node, `${runsc} pause ${containerId}`);
  const pauseEnd = now();
  await sleep(1000);
  const resumeStart = now();
  await onNode(node, `${runsc} resume ${containerId}`);
  const resumeEnd = now();
  const { max_gap_ms: gap } = await driver<{ readonly max_gap_ms: number }>(workspace, "gaps", "--session", sessionId, "--fixture", fixture);
  const least = msBetween(pauseEnd, resumeStart) - 20;
  const most = msBetween(pauseStart, resumeEnd) + 20;
  const seen = { gap_ms: gap, least_ms: least, most_ms: most, ok: gap >= least && gap <= most };
  writeFileSync(join(BENCH_DIR, "logs", "calibration.json"), `${JSON.stringify(seen)}\n`);
  if (!seen.ok) throw new Uncalibrated(`the ticker saw a gap of ${gap} ms of a freeze of ${least + 20} to ${most - 20} ms: its measures are not to be trusted`);
  calibrated = true;
}

/** How many times systemd had started k3s again as the last run began (node.ts k3sReady). */
let k3sRestarts = 0;

/** A phase's result as a run holds it until its spans are read: its result, and its window on the wall clock. */
interface Held {
  result: Result;
  readonly fromMs: number;
  readonly toMs: number;
}

/**
 * One run of a case, every phase of it written, as the module says; what fails is written as
 * failed, and the run goes on to its cleanup. Its phases' lines are written once the run is over and
 * alasio's spans of it are read from the lake, which fill what the spans say of each.
 */
async function runOnce(fixture: string, sizeMib: number, run: number, runId: string): Promise<void> {
  const phases: readonly Phase[] = ["exec-running", "save-live", "save-stop", "restore-warm", "restore-cold"];
  const runStarted = Date.now();
  const k3s = await k3sReady();
  // k3s ended and was started again since the run before, which that run may have passed through: what its journal says of it.
  if (k3s.restarts > k3sRestarts) await sayK3sRestarted(`${fixture}-${sizeMib}-before-run-${run}`);
  k3sRestarts = k3s.restarts;
  const held: Held[] = [];
  const hold = (result: Result, fromMs: number, toMs: number) => held.push({ result: { ...result, details: { ...result.details, k3s } }, fromMs, toMs });
  let phase = "setup";
  let workspace = "";
  let copy = "";
  let node = "";
  let load: Awaited<ReturnType<typeof mixedLoad>> = null;
  try {
    const made = await newSession(fixture, sizeMib, runId);
    workspace = made.workspace;
    copy = `${workspace}-copy`;
    const { sessionId } = made;
    load = await mixedLoad(runId);

    phase = "exec-running";
    const running = await driver<Probed>(workspace, "probe", "--session", sessionId, "--fixture", fixture, "--run-id", runId);
    hold({
      fixture, sizeMib, run, phase: "exec-running", ok: running.ok, failure: running.failure,
      metrics: { exec_running_ms: running.first_exec_ms, verify_ms: running.verify_ms, probe_exec_ms: probeExecMs(running), workload_rss_bytes: running.workload_rss_bytes },
      correctness: running.correctness, details: { t0_wall_ms: running.t0_wall_ms },
    }, running.t0_wall_ms, running.t0_wall_ms + running.first_exec_ms);
    const restoredBefore = VARIANT === "restored" ? await restoreBefore(workspace, sessionId, fixture) : null;

    phase = "save-live";
    const { node: on, request } = await checkpointRequest(workspace);
    node = on;
    if (!calibrated) await calibrate(workspace, sessionId, fixture, node, request.containerId);
    const sandboxId = await sandboxIdOf(node, request.pod);
    let heldLive = await bucketBytes(node, workspace);
    // A chain-N's N - 1 live saves, each a delta of the one before, all but the last written in its details; none of `whole`'s.
    const lives: Record<string, unknown>[] = [];
    for (let saved = 1; saved <= LIVE_SAVES; saved++) {
      // Left idle first, where the variant says.
      const idleS = IDLE_BEFORE_SAVES_S[saved - 1] ?? 0;
      await sleep(idleS * 1000);
      const heldBeforeSave = heldLive;
      // Of `idle-floor`, the session asked nothing past its first save's mark.
      const touched = !IDLE_UNTOUCHED || saved === 1;
      if (touched) await driver(workspace, "mark", "--session", sessionId, "--fixture", fixture);
      const liveT0 = Date.now();
      const liveSent = (await storeBytes(node)).sent;
      const checkpointed = await liveCheckpoint(node, request);
      const gaps = IDLE_UNTOUCHED
        ? { max_gap_ms: null, slowdown: null }
        : await driver<{ readonly max_gap_ms: number | null; readonly slowdown: number | null }>(workspace, "gaps", "--session", sessionId, "--fixture", fixture);
      const liveSentBytes = (await storeBytes(node)).sent - liveSent;
      heldLive = await bucketBytes(node, workspace);
      const logged = saveLogged(await bootLog(node, sandboxId));
      const details = { state_dir: request.live.stateDir, logged: logged.said, gaps, pause_split: logged.split, t0_wall_ms: liveT0, end_wall_ms: Date.now(), restored_before: restoredBefore, idle_s: idleS };
      const metrics = {
        unavailable_ms: gaps.max_gap_ms, pause_reported_ms: logged.pauseMs, slowdown: gaps.slowdown, precopy_passes: logged.passes, precopy_bytes: logged.precopyBytes, uploaded_bytes: heldLive - heldBeforeSave,
        kernel_save_ms: logged.kernelMs, memory_save_ms: logged.memoryMs, overall_save_ms: logged.overallMs, store_sent_bytes: liveSentBytes,
      };
      if ("failed" in checkpointed) {
        hold({ fixture, sizeMib, run, phase: "save-live", ok: false, failure: `the node agent's live checkpoint failed: ${checkpointed.failed}`, metrics, details: { ...details, lives } }, liveT0, Date.now());
        throw new Error(`the live checkpoint failed: ${checkpointed.failed}`);
      }
      const imageBytes = checkpointed.manifest.layers[0]?.files.reduce((sum, { bytes }) => sum + bytes, 0) ?? null;
      if (saved < LIVE_SAVES) {
        lives.push({ ...metrics, save_total_ms: checkpointed.ms, image_bytes: imageBytes, chain: checkpointed.manifest.layers.length, snapshot: checkpointed.manifest.snapshot, idle_s: idleS });
        continue;
      }
      hold({
        fixture, sizeMib, run, phase: "save-live", ok: true, failure: null,
        metrics: { ...metrics, save_total_ms: checkpointed.ms, image_bytes: imageBytes, uploaded_over_needed: imageBytes ? liveSentBytes / imageBytes : null },
        details: { ...details, snapshot: checkpointed.manifest.snapshot, runsc_checkpoint_ms: checkpointed.manifest.pauseMs, upload_ms: checkpointed.manifest.uploadMs, chain: checkpointed.manifest.layers.length, lives },
      }, liveT0, Date.now());
    }

    phase = "save-stop";
    await sleep(IDLE_BEFORE_RELEASE_S * 1000);
    const stopT0 = Date.now();
    const stopSent = (await storeBytes(node)).sent;
    const released = await release(workspace);
    const stopSentBytes = (await storeBytes(node)).sent - stopSent;
    if (released.status < 200 || released.status >= 300) throw new Error(`the gateway's release answered ${released.status}: ${released.said}`);
    const manifest = JSON.parse(await agentOk(node, "GET", `/snapshots/${SESSIONS}/${workspace}`)) as Manifest;
    const record = JSON.parse(await agentOk(node, "GET", `/workspaces/${SESSIONS}/${workspace}/released`)) as { readonly snapshot: string; readonly release: string };
    // Its current snapshot is the one it was released into, or one the agent compacted from it since, which is the same
    // (src/node-agent/agent.ts compactNow), as a chain of the live save's image and the release's delta may be due.
    if (record.snapshot !== manifest.snapshot && record.snapshot !== manifest.compactedFrom) {
      throw new Error(`${workspace} was released into ${record.snapshot}, not its current snapshot ${manifest.snapshot}`);
    }
    const heldStop = await bucketBytes(node, workspace);
    const stopLogged = saveLogged(await bootLog(node, sandboxId));
    // The release's own image, which a compaction since may have replaced as the manifest's.
    const releasedLayer = manifest.compactedFrom === undefined ? manifest.layers[0] : undefined;
    const stopImageBytes = releasedLayer?.files.reduce((sum, { bytes }) => sum + bytes, 0) ?? null;
    hold({
      fixture, sizeMib, run, phase: "save-stop", ok: true, failure: null,
      metrics: {
        save_total_ms: released.ms, pause_reported_ms: stopLogged.pauseMs, precopy_passes: 0, precopy_bytes: 0,
        image_bytes: manifest.layers[0]?.files.reduce((sum, { bytes }) => sum + bytes, 0) ?? null, uploaded_bytes: heldStop - heldLive,
        kernel_save_ms: stopLogged.kernelMs, memory_save_ms: stopLogged.memoryMs, overall_save_ms: stopLogged.overallMs, store_sent_bytes: stopSentBytes,
        uploaded_over_needed: stopImageBytes ? stopSentBytes / stopImageBytes : null,
      },
      details: {
        snapshot: manifest.snapshot, runsc_checkpoint_ms: manifest.pauseMs, upload_ms: manifest.uploadMs, chain: manifest.layers.length, bytes: manifest.bytes, compacted_from: manifest.compactedFrom ?? null,
        compaction_due: compactionDue(manifest.layers) !== null, logged: stopLogged.said, pause_split: stopLogged.split, t0_wall_ms: stopT0, end_wall_ms: Date.now(),
        idle_s: IDLE_BEFORE_RELEASE_S,
      },
    }, stopT0, Date.now());
    await gone(request.pod);
    // The workspace as the release left it, for the cold restore's.
    const quota = JSON.parse(await agentOk(node, "GET", `/workspaces/${SESSIONS}/${workspace}/quota`)) as { readonly capacityBytes: number };
    await agentOk(node, "POST", "/workspaces/clones", { from: { environment: SESSIONS, workspace }, to: { environment: SESSIONS, workspace: copy }, quotaBytes: quota.capacityBytes });

    phase = "restore-warm";
    if (PAGE_CACHE === "dropped") await dropPageCache(node);
    // What the agent sent the store from the release's end on, before the warm restore: the release's compaction's upload, where one ran.
    const compactionSent = (await storeBytes(node)).sent - stopSent - stopSentBytes;
    const warmStandby = await standbyFor(node, standbyArm(run));
    const warmBeside = load === null ? null : await load.beside("warm");
    const warm = await restore(node, workspace, sessionId, fixture, runId, manifest, "warm");
    hold({
      fixture, sizeMib, run, phase: "restore-warm", ...restoreOutcome(warm, "cache"), metrics: warm.metrics, correctness: warm.probed.correctness,
      details: { ...warm.details, standby_before: warmStandby, compaction_sent_bytes: compactionSent, mixed: warmBeside === null ? null : await warmBeside() },
    }, warm.window.fromMs, warm.window.toMs);
    await endUnsaved(workspace, warm.pod);

    phase = "restore-cold";
    await agentOk(node, "DELETE", `/workspaces/${SESSIONS}/${workspace}`);
    await agentOk(node, "POST", "/workspaces/clones", { from: { environment: SESSIONS, workspace: copy }, to: { environment: SESSIONS, workspace }, quotaBytes: quota.capacityBytes });
    // Its snapshot current again, its manifest put back as current.json, as a checkpoint commits it (src/node-agent/agent.ts commit).
    await inAgentNetwork(node, [new URL("dist/s3.mjs", COMMON).pathname, "copy", `${SESSIONS}/${workspace}/manifests/${manifest.snapshot}.json`, `${SESSIONS}/${workspace}/current.json`]);
    await agentOk(node, "PUT", `/workspaces/${SESSIONS}/${workspace}/released`, { snapshot: manifest.snapshot, release: manifest.release });
    const cacheRemoved = await coldCaches(workspace);
    const coldStandby = await standbyFor(node, standbyArm(run));
    const coldBeside = load === null ? null : await load.beside("cold");
    const cold = await restore(node, workspace, sessionId, fixture, runId, manifest, "cold");
    hold({
      fixture, sizeMib, run, phase: "restore-cold", ...restoreOutcome(cold, "bucket"), metrics: cold.metrics, correctness: cold.probed.correctness,
      details: { ...cold.details, standby_before: coldStandby, cache_removed: cacheRemoved, mixed: coldBeside === null ? null : await coldBeside() },
    }, cold.window.fromMs, cold.window.toMs);
    await endUnsaved(workspace, cold.pod);
    phase = "done";
  } catch (error) {
    if (error instanceof Uncalibrated) throw error;
    const why = `${phase}: ${errorText(error)}`;
    console.error(`# ${fixture} ${sizeMib} MiB run ${run} failed at ${why}`);
    await sayRunner(`${fixture}-${sizeMib}-${run}`);
    if (node !== "" && workspace !== "") {
      const logs = await loggedOf(node, workspace).catch((failed: unknown) => ({ agent: [errorText(failed)], gateway: [] }));
      // How each pod of its ended, where one did: its containers' last states, as kubelet said them.
      const pods = await kube.list<V1Pod>("Pod", { namespace: SESSIONS, labelSelector: `${BINDING_LABEL}=${keyHash(sessionKey(workspace))}` }).catch(() => [] as V1Pod[]);
      const states = pods.map((pod) => `${pod.metadata?.name}: ${pod.status?.phase} ${JSON.stringify(pod.status?.containerStatuses?.map(({ name, state, lastState, restartCount }) => ({ name, state, lastState, restartCount })))}`);
      writeFileSync(join(BENCH_DIR, "logs", `failure-${fixture}-${sizeMib}-${run}-alasio.txt`), `# the node agent of ${workspace}\n${logs.agent.join("\n")}\n\n# the gateway of ${workspace}\n${logs.gateway.join("\n")}\n\n# its pods\n${states.join("\n")}\n`);
    }
    for (const each of phases.filter((name) => !held.some(({ result }) => result.phase === name))) {
      hold({ fixture, sizeMib, run, phase: each, ok: false, failure: each === phase ? why : `not run: ${why}`, metrics: {} }, Date.now(), Date.now());
    }
  } finally {
    await written(held, workspace, load?.workspaces ?? [], runStarted);
    if (workspace !== "") await forgetWorkspace(workspace);
    if (node !== "" && copy !== "") await agentApi(node, "DELETE", `/workspaces/${SESSIONS}/${copy}`).catch(() => {});
    await load?.end();
    await vacuum().catch((error: unknown) => console.error(`# the store was not compacted after ${workspace}: ${errorText(error)}`));
  }
}

/**
 * Writes a run's phases' lines, once the lake has alasio's spans of each phase that passed, or
 * three minutes have gone: each filled with what its spans say, and its spans written beside.
 */
async function written(held: readonly Held[], workspace: string, others: readonly string[], runStarted: number): Promise<void> {
  const passed = held.filter(({ result }) => result.ok);
  const expected = workspace === "" ? [] : passed.flatMap(({ result, fromMs }) => {
    const from = BigInt(Math.round(fromMs * 1e6)) - 1_000_000_000n;
    if (result.phase === "save-live" || result.phase === "save-stop") return [hasSpan("alasio.node_agent.checkpoint", workspace, from)];
    if (result.phase === "restore-warm" || result.phase === "restore-cold") return [hasSpan("alasio.sandbox.ready", workspace, from), hasSpan("alasio.node_agent.resume", workspace, from)];
    return [];
  });
  const { spans, complete, waited_ms } = workspace === ""
    ? { spans: [], complete: false, waited_ms: 0 }
    : await awaitSpans([workspace, ...others], runStarted - 60_000, Date.now() + 60_000, expected).catch((error: unknown) => {
      console.error(`# the spans of ${workspace} were not read: ${errorText(error)}`);
      return { spans: [], complete: false, waited_ms: 0 };
    });
  const key = sessionKey(workspace);
  for (const { result, fromMs, toMs } of held) {
    const fromSpans = result.phase === "save-live" ? { durable_store_ms: liveDurableMs(spans, workspace, fromMs - 1000) }
      : result.phase === "save-stop" ? { durable_store_ms: releaseDurableMs(spans, workspace, key, fromMs - 1000) }
      : result.phase === "restore-warm" || result.phase === "restore-cold" ? restoreSpanMetrics(spans, workspace, fromMs, result.metrics)
      : {};
    const filled: Result = { ...result, metrics: { ...result.metrics, ...fromSpans }, details: { ...result.details, spans: { complete, waited_ms } } };
    emit(filled);
    if (result.ok && result.phase !== "exec-running") emitSpans(filled, fromMs, toMs, spans);
  }
}

/** What the job runs: `runs`, every case's runs, as the module says; or `store`, the store's own measures alone (./store.ts). */
const MODE = process.env["BENCH_MODE"] || "runs";
if (MODE !== "runs" && MODE !== "store") throw new Error(`BENCH_MODE is ${MODE}, not runs or store`);

before(async () => {
  try {
    mkdirSync(join(BENCH_DIR, "logs"), { recursive: true });
    await setUp();
    for (const node of (await nodeAgents()).keys()) await bootLogs(node);
    await shapeAgents();
    for (const node of (await nodeAgents()).keys()) await countStore(node);
    // How the store answers through the profile, from where the saves run: the node agent's network.
    const [node = ""] = (await nodeAgents()).keys();
    store.probed = await probeStore(node);
    console.error(`# the store through ${PROFILE}: ${JSON.stringify(store.probed)}`);
  } catch (error) {
    await dumpClusterState();
    throw error;
  }
}, { timeout: 150 * 60_000 });

after(async () => {
  await tearDown();
}, { timeout: 30 * 60_000 });

test(`alasio's sessions saved and restored, ${RUNS} runs of ${CASES.length} cases, through ${PROFILE}, ${VARIANT}`, { timeout: 330 * 60_000 }, async () => {
  if (MODE === "store") return await storeMeasures();
  for (let run = 1; run <= RUNS; run++) {
    for (const { fixture, size_mib } of CASES) {
      const started = Date.now();
      const runId = `${process.env["GITHUB_RUN_ID"] ?? "local"}-${PROFILE}-${fixture}-${size_mib}-${run}-${randomBytes(3).toString("hex")}`;
      // Where the cluster itself fails under a run, what the runner says of it is kept before the run is.
      await (VARIANT === "turn-replay" ? replayTurns(fixture, size_mib, run, runId) : runOnce(fixture, size_mib, run, runId)).catch(async (error: unknown) => {
        await sayRunner(`${fixture}-${size_mib}-${run}-aborted`);
        throw error;
      });
      console.error(`# ${fixture} ${size_mib} MiB run ${run} took ${Math.round((Date.now() - started) / 1000)} s`);
    }
  }
});
