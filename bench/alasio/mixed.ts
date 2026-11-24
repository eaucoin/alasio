/**
 * The mixed variants' load: other sessions on the node whose work runs beside each of a run's
 * restores, as a node that serves many sessions runs it, each source on its own and all together,
 * a release always among them where it is all of them:
 * - `release`: a whole-heap writer saved live once, whose release, its checkpoint, upload and the
 *   compaction its chain is due, runs beside the warm restore, and whose own restore from the
 *   node's cache runs beside the cold one;
 * - `saves`: two sessions, each saved live beside each restore, at once;
 * - `fill`: a session released whose image is then only in the store: its cold restore, through
 *   to its first exec, just before each restore, so that the node's cache is filled with the rest
 *   of its image (the agent's fetch after a restore from the bucket) beside the restore;
 * - `all`: each of them at once.
 * Each restore's result says which sources ran at its t0 and how each went (details.mixed).
 */
import type { Manifest } from "../../src/node-agent/agent.ts";
import { boundPodOf, SESSIONS } from "../../test/e2e/harness.ts";
import { agentApi, agentOk, checkpointRequest, coldCaches, driver, endUnsaved, errorText, forgetWorkspace, gone, liveCheckpoint, newSession, release, settledFrom, sleep } from "./alasio.ts";
import { inAgentNetwork, storeBytes } from "./node.ts";
import { COMMON, MIXED_LOAD } from "./settings.ts";

/** The releasing source's session: a whole-heap writer, whose release's delta makes its chain due to be compacted. */
const RELEASING = { fixture: "saturating", sizeMib: 1024 } as const;
/** Each saving source's session. */
const SAVING = { fixture: "poller", sizeMib: 256 } as const;
/** The filling source's session, whose image the node's cache is filled with. */
const FILLING = { fixture: "poller", sizeMib: 1024 } as const;
/** How long before a restore's t0 the load beside it starts, so that it runs as the restore does. */
const AHEAD_MS = 500;

/** What a source runs beside a restore, started: what came of it, once settled. */
interface Started {
  readonly settled: Promise<Record<string, unknown>>;
}

/**
 * A source of load: its sessions; what it readies, untimed, before a restore, as the restore is
 * `warm` or `cold`; and what it then starts beside it, which runs on once it is started.
 */
interface Source {
  readonly name: "release" | "saves" | "fill";
  readonly workspaces: readonly string[];
  /** The clones of its workspaces it keeps, on the node each is on, removed with it. */
  readonly copies?: readonly { readonly node: string; readonly workspace: string }[];
  readonly ready: (kind: "warm" | "cold") => Promise<void>;
  readonly start: (kind: "warm" | "cold") => Promise<Started>;
}

/** The release source: its session saved live once, so its release is a delta. */
async function releasing(runId: string): Promise<Source> {
  const { workspace, sessionId } = await newSession(RELEASING.fixture, RELEASING.sizeMib, `${runId}-release`);
  const { node, request } = await checkpointRequest(workspace);
  const saved = await liveCheckpoint(node, request);
  if ("failed" in saved) throw new Error(`the releasing session's live save failed: ${saved.failed}`);
  return {
    name: "release",
    workspaces: [workspace],
    ready: async () => {},
    start: async (kind) => {
      const t0 = Date.now();
      const work = kind === "warm" ? release(workspace) : driver(workspace, "first", "--session", sessionId, "--fixture", RELEASING.fixture);
      return { settled: settledFrom(work, t0).then((settled) => ({ beside: kind === "warm" ? "release" : "restore", ...settled })) };
    },
  };
}

/** The saves source: two sessions, each saved live beside each restore, at once. */
async function saving(runId: string): Promise<Source> {
  const sessions = [await newSession(SAVING.fixture, SAVING.sizeMib, `${runId}-save-1`), await newSession(SAVING.fixture, SAVING.sizeMib, `${runId}-save-2`)];
  return {
    name: "saves",
    workspaces: sessions.map(({ workspace }) => workspace),
    ready: async () => {},
    start: async () => {
      const requests = await Promise.all(sessions.map(({ workspace }) => checkpointRequest(workspace)));
      const t0 = Date.now();
      return { settled: Promise.all(requests.map(({ node, request }) => settledFrom(liveCheckpoint(node, request), t0))).then((saves) => ({ beside: "live saves", saves })) };
    },
  };
}

/**
 * The fill source: its session released once, its snapshot's manifest and its workspace as the
 * release left them kept (a clone); before each restore, as the run's own cold restore is readied,
 * its pod ended unsaved where its last restore left one, its workspace put back from the clone, its
 * snapshot made current again and its release record written again, and every node's cache of it
 * removed; then its cold restore, through to its first exec, after which the agent fills the node's
 * cache with the rest of its image, beside the restore.
 */
async function filling(runId: string): Promise<Source> {
  const { workspace, sessionId } = await newSession(FILLING.fixture, FILLING.sizeMib, `${runId}-fill`);
  const bound = await boundPodOf(workspace);
  if (!bound) throw new Error(`no pod is bound to the filling session ${workspace}`);
  const released = await release(workspace);
  if (released.status < 200 || released.status >= 300) throw new Error(`the filling session's release answered ${released.status}: ${released.said}`);
  const manifest = JSON.parse(await agentOk(bound.node, "GET", `/snapshots/${SESSIONS}/${workspace}`)) as Manifest;
  await gone(bound.name);
  const copy = `${workspace}-copy`;
  const quota = JSON.parse(await agentOk(bound.node, "GET", `/workspaces/${SESSIONS}/${workspace}/quota`)) as { readonly capacityBytes: number };
  await agentOk(bound.node, "POST", "/workspaces/clones", { from: { environment: SESSIONS, workspace }, to: { environment: SESSIONS, workspace: copy }, quotaBytes: quota.capacityBytes });
  return {
    name: "fill",
    workspaces: [workspace],
    copies: [{ node: bound.node, workspace: copy }],
    ready: async () => {
      const restored = await boundPodOf(workspace);
      if (restored) await endUnsaved(workspace, restored.name);
      await agentOk(bound.node, "DELETE", `/workspaces/${SESSIONS}/${workspace}`);
      await agentOk(bound.node, "POST", "/workspaces/clones", { from: { environment: SESSIONS, workspace: copy }, to: { environment: SESSIONS, workspace }, quotaBytes: quota.capacityBytes });
      await inAgentNetwork(bound.node, [new URL("dist/s3.mjs", COMMON).pathname, "copy", `${SESSIONS}/${workspace}/manifests/${manifest.snapshot}.json`, `${SESSIONS}/${workspace}/current.json`]);
      await agentOk(bound.node, "PUT", `/workspaces/${SESSIONS}/${workspace}/released`, { snapshot: manifest.snapshot, release: manifest.release, manifest });
      await coldCaches(workspace);
    },
    // Started once its restore's first exec has answered: the agent's fill of the node's cache runs on from there.
    start: async () => {
      const t0 = Date.now();
      const before = (await storeBytes(bound.node)).received;
      await driver(workspace, "first", "--session", sessionId, "--fixture", FILLING.fixture);
      const restoredMs = Date.now() - t0;
      const received = (await storeBytes(bound.node)).received - before;
      return { settled: Promise.resolve({ beside: "cache fill", ok: true, restored_ms: restoredMs, received_by_its_first_exec_bytes: received }) };
    },
  };
}

/** The job's mixed load, for a run: its sources' sessions made, each ready; null for a variant that runs none. */
export async function mixedLoad(runId: string): Promise<{ readonly workspaces: readonly string[]; readonly beside: (kind: "warm" | "cold") => Promise<() => Promise<Record<string, unknown>>>; readonly end: () => Promise<void> } | null> {
  if (MIXED_LOAD === null) return null;
  const sources: Source[] = [];
  const end = async () => {
    for (const workspace of sources.flatMap(({ workspaces }) => workspaces)) await forgetWorkspace(workspace).catch((error: unknown) => console.error(`# ${workspace} was not forgotten: ${errorText(error)}`));
    for (const { node, workspace } of sources.flatMap(({ copies }) => copies ?? [])) await agentApi(node, "DELETE", `/workspaces/${SESSIONS}/${workspace}`).catch(() => {});
  };
  try {
    if (MIXED_LOAD === "release" || MIXED_LOAD === "all") sources.push(await releasing(runId));
    if (MIXED_LOAD === "saves" || MIXED_LOAD === "all") sources.push(await saving(runId));
    if (MIXED_LOAD === "fill" || MIXED_LOAD === "all") sources.push(await filling(runId));
  } catch (error) {
    await end();
    throw error;
  }
  return {
    workspaces: sources.flatMap(({ workspaces }) => workspaces),
    /**
     * Readies every source for a restore about to begin, then starts each, the fill's first, as it is
     * started only once its own restore has answered, and the others AHEAD_MS before the restore:
     * what came of each, once all are settled.
     */
    beside: async (kind) => {
      for (const { ready } of sources) await ready(kind);
      const ordered = [...sources.filter(({ name }) => name === "fill"), ...sources.filter(({ name }) => name !== "fill")];
      const started: (readonly [string, Started])[] = [];
      for (const { name, start } of ordered) started.push([name, await start(kind)]);
      await sleep(AHEAD_MS);
      return async () => Object.fromEntries(await Promise.all(started.map(async ([name, { settled }]) => [name, await settled] as const)));
    },
    end,
  };
}
