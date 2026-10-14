/**
 * Whole-sandbox snapshots of session filesystems. A session whose bayma has run no exec,
 * and on which no turn has run, for the profile's `snapshots.releaseAfterSeconds` is
 * released: the node agent on its pod's node (./node-agents.ts) saves its sandbox whole,
 * bayma told first (`bayma sandbox-hook`), which ends it, and its Sandbox is suspended,
 * holding no pod. As it is next brought up, which alasio does as a prompt arrives, it is
 * restored from that snapshot, its REPLs and processes as they were, on a node that has
 * the gVisor release that took it and the CPU features it was saved with; the snapshot is
 * then spent, as the session moves on from it.
 *
 * A snapshot restores into the pod template it was taken on: alasio's current one, or the
 * one before it, which `alasio up` carries forward until the template changes again
 * (ALASIO_KUBE_TEMPLATES `previousSessions`), the admission policy pinning both whole
 * (cli/src/manifests/admission.ts); so a snapshot outlives one change of the template, as
 * it outlives one of the gVisor release (the profile's kept releases), and no more. Both
 * are the templates as the sessions' CPU baseline holds them, so a change of the baseline
 * is one of the template. A session restored onto the template before the current one
 * stays on it while its pod runs, and is moved onto the current one as it is resumed
 * without a snapshot.
 *
 * A snapshot that cannot restore is never so silently: the session is resumed as one
 * without a snapshot, from bayma's checkpoints of its sessions, which bayma tells its next
 * exec of, and why it was not restored is logged and counted, the snapshot forgotten.
 */
import type { V1Container, V1Pod, V1PodTemplateSpec } from "@kubernetes/client-node";
import { Clock, Duration, Effect, FiberMap, Schedule, Schema, type Scope } from "effect";
import { FetchHttpClient } from "effect/http";

import { type KubeApiError, KubeClient } from "../kube/client.ts";
import type { SessionsProfile } from "../kube/config.ts";
import {
  BAYMA_CONTAINER,
  CPU_FEATURES_ANNOTATION,
  RESTORE_ANNOTATION,
  RESTORE_PAUSED_ANNOTATION,
  RESUMED_CONDITION,
  SANDBOX_LABEL,
  SESSION_TEMPLATE_ANNOTATION,
} from "../kube/sandbox-shape.ts";
import {
  podTemplateHash,
  SANDBOX_API_VERSION,
  SANDBOX_KIND,
  type Sandbox,
  type SandboxRestore,
  SandboxRestoreRefused,
  type Saved,
  SNAPSHOT_ANNOTATION,
  type SandboxGone,
  type SandboxNotReady,
  type SandboxPodEnded,
  type Sandboxes,
  sandboxReady,
  type StoredSandbox,
} from "../kube/sandboxes.ts";
import type { Manifest } from "../node-agent/agent.ts";
import { withLogScope } from "../shared/log.ts";
import { meter, withAlasioSpan } from "../telemetry/index.ts";
import type { NodeAgents } from "./node-agents.ts";

/** How sessions are snapshotted, as the profile says. */
export type SnapshotsProfile = NonNullable<SessionsProfile["snapshots"]>;

/** How long a restored pod is waited for to run its containers, its images pulled. */
const POD_RUNNING_TIMEOUT = "3 minutes";
/** How often running sessions are looked at for one to release. */
const RELEASE_POLL: Duration.Input = "15 seconds";
/** How long a look at a bayma's status may take. */
const STATUS_TIMEOUT_MS = 5000;
/** How long alasio waits, as it starts, for a node agent to tell the CPU baseline by. */
const BASELINE_WAIT: Duration.Input = "1 minute";

const releases = meter.createCounter("alasio.sandbox.releases", {
  description: "Session sandboxes released, by outcome: snapshotted and suspended, kept running, or failed",
});
const restores = meter.createCounter("alasio.sandbox.restores", {
  description: "Session sandboxes brought up with a snapshot, by outcome: restored, or not, by why",
});
const restoreDuration = meter.createHistogram("alasio.sandbox.restore.duration", {
  description: "How long a session restored from its snapshot took to come back, which the prompt bringing it up waits: from its snapshot looked up to its pod ready",
  unit: "s",
});

/** Why a session with a snapshot was resumed without it. */
export type Fallback = "release" | "template" | "stale" | "node" | "prepare" | "restore" | "unreadable";

/** The profile's templates a snapshot restores into, by their hashes: the current one, and the one before it, if any. */
export interface KeptTemplates {
  readonly current: string;
  readonly previous: string | null;
}

/** A restored pod does not run its containers yet. */
class PodNotRunning extends Schema.TaggedError<PodNotRunning>()("PodNotRunning", {}) {}

/** What bayma's /status says: its execs running and queued, and since when none has run, if none does. */
const BaymaStatus = Schema.Struct({ execsRunning: Schema.Number, execsQueued: Schema.Number, idleSinceMs: Schema.NullOr(Schema.Number) });

/** The hash of the profile's template that `podTemplate` was made from, as it records it. */
const sessionTemplateOf = (podTemplate: V1PodTemplateSpec | Readonly<Record<string, unknown>>): string | undefined =>
  (podTemplate as V1PodTemplateSpec).metadata?.annotations?.[SESSION_TEMPLATE_ANNOTATION];

/**
 * The command that runs bayma's hook in bayma's container: bayma, as its container runs
 * it after the init that starts it (the arguments after its `--`), told the state
 * directory it serves. Null when the container runs no bayma with one. Pure, for tests.
 */
export function hookOf(bayma: V1Container): string[] | null {
  const command = bayma.command ?? [];
  const program = command.includes("--") ? command.slice(command.indexOf("--") + 1) : command;
  const args = bayma.args ?? [];
  const stateDir = args[args.indexOf("--state-dir") + 1];
  return program.length > 0 && args.includes("--state-dir") && stateDir ? [...program, "sandbox-hook", "--state-dir", stateDir] : null;
}

/**
 * The CPU features every node with an agent has under gVisor's `release`, which sessions'
 * sandboxes are held to, so that a snapshot of one restores on any of those nodes; null
 * when no agent tells within BASELINE_WAIT, said so. A node that joins later widens
 * nothing, and narrows the baseline as alasio next starts.
 */
export const cpuBaseline = (agents: NodeAgents, release: string): Effect.Effect<readonly string[] | null> =>
  Effect.gen(function*() {
    const nodes = yield* agents.nodes.pipe(
      Effect.filterOrFail((found) => found.length > 0, () => new Error("no node agent is ready")),
      Effect.retry(Schedule.max([Schedule.spaced("2 seconds"), Schedule.during(BASELINE_WAIT)])),
    );
    const held = yield* Effect.forEach(nodes, (node) => agents.holdings(node));
    const features = held.map(({ releases: installed }) => installed.find((kept) => kept.release === release)?.cpuFeatures ?? []);
    const [first = [], ...others] = features;
    return first.filter((feature) => others.every((other) => other.includes(feature)));
  }).pipe(
    Effect.catch((error) => Effect.logWarning(`sessions' sandboxes are held to no CPU baseline: ${error.message}`).pipe(Effect.as(null))),
  );

/** `profile`, its pods held to the CPU features `baseline` names, as part of the template its snapshots record. */
export const withCpuBaseline = (profile: SessionsProfile, baseline: readonly string[]): SessionsProfile => ({
  ...profile,
  podTemplate: {
    ...profile.podTemplate,
    metadata: { ...profile.podTemplate.metadata, annotations: { ...profile.podTemplate.metadata?.annotations, [CPU_FEATURES_ANNOTATION]: baseline.join(",") } },
  },
});

/** What a session's snapshots are given. */
export interface SessionSnapshotsOptions {
  readonly profile: SessionsProfile;
  readonly snapshots: SnapshotsProfile;
  readonly agents: NodeAgents;
  readonly templates: KeptTemplates;
}

/** How session Sandboxes are snapshotted and restored; see the module. */
export interface SessionSnapshots {
  /** Whether a running Sandbox stays on its template: one restored onto the template before the current one. */
  readonly keeps: (sandbox: StoredSandbox) => boolean;
  /** How a suspended Sandbox is restored from its snapshot, or null when it is resumed without one (said why, when it had one). */
  readonly restore: (sandbox: StoredSandbox, desired: Sandbox) => Effect.Effect<SandboxRestore | null>;
  /**
   * Releases each running session once it has run no exec and no turn for long enough,
   * looking every RELEASE_POLL, for as long as the scope lasts; `lastTurn(name)` is when
   * the session's last turn ended, Infinity while one runs, null when none has since
   * alasio started.
   */
  readonly watch: (sandboxes: Sandboxes, lastTurn: (name: string) => number | null) => Effect.Effect<void, never, Scope.Scope>;
  /** Releases the running session now, idle or not, as a fork of it does: whether it was suspended. */
  readonly release: (sandboxes: Sandboxes, name: string) => Effect.Effect<boolean, KubeApiError | SandboxNotReady | SandboxPodEnded | SandboxGone>;
  /** Gives the session `to`, a fork of `from`, a copy of `from`'s snapshot, if it has one: its id; null for none, one that cannot be copied said so. */
  readonly copy: (from: string, to: string) => Effect.Effect<string | null>;
}

/** A session's snapshots, as the module says, on the KubeClient. */
export const makeSessionSnapshots = Effect.fnUntraced(function*({ profile, snapshots, agents, templates }: SessionSnapshotsOptions): Effect.fn.Return<SessionSnapshots, never, KubeClient> {
  const kube = yield* KubeClient;
  const fetch = yield* FetchHttpClient.Fetch;
  const environment = profile.namespace;
  const [current] = snapshots.releases;
  const releaseAfterMs = snapshots.releaseAfterSeconds * 1000;
  const ref = (name: string) => ({ environment, workspace: name });

  /** The gVisor release a pod of `runtimeClassName` runs: the current one's for the profile's own, as its template names it. */
  const releaseOf = (runtimeClassName: string | undefined): string | null => {
    if (runtimeClassName !== undefined && runtimeClassName === profile.podTemplate.spec?.runtimeClassName) return current?.release ?? null;
    return snapshots.releases.find((kept) => kept.runtimeClassName === runtimeClassName)?.release ?? null;
  };

  /** Forgets the session's snapshot, saying why; one that cannot be forgotten is said so too. */
  const forget = (name: string, why: string): Effect.Effect<void> =>
    agents.forget(ref(name)).pipe(
      Effect.catch((error) => Effect.logWarning(`could not forget the snapshot of session ${name}: ${error.message}`)),
      Effect.andThen(Effect.logInfo(`forgot the snapshot of session ${name}: ${why}`)),
    );

  /** Resumes the session without its snapshot, which is forgotten, saying why, and counted as `fallback`. */
  const fallBack = (name: string, fallback: Fallback, why: string): Effect.Effect<null> =>
    Effect.logWarning(`session ${name} is resumed from bayma's checkpoints, not from its snapshot: ${why}`).pipe(
      Effect.andThen(Effect.sync(() => restores.add(1, { "alasio.sandbox.restore.outcome": "fallback", "alasio.sandbox.restore.fallback": fallback }))),
      Effect.andThen(forget(name, why)),
      Effect.as(null),
    );

  /** The nodes that can restore `manifest`, those holding its image first: they have its release, with every CPU feature it was saved with. */
  const nodesFor = (manifest: Manifest) =>
    Effect.gen(function*() {
      const held = yield* Effect.forEach(yield* agents.nodes, (node) =>
        agents.holdings(node).pipe(
          Effect.map((holdings) => [holdings]),
          Effect.catch((error) => Effect.logWarning(error.message).pipe(Effect.as([]))),
        ));
      const able = held.flat().filter(({ releases: installed }) =>
        installed.some(({ release, cpuFeatures }) => release === manifest.release && manifest.cpuFeatures.every((feature) => cpuFeatures.includes(feature)))
      );
      const holds = (cached: (typeof able)[number]["cached"]) => cached.some(({ snapshot }) => snapshot === manifest.snapshot);
      return [...able.filter(({ cached }) => holds(cached)), ...able.filter(({ cached }) => !holds(cached))].map(({ node }) => node);
    });

  /**
   * Lets the restored pod of session `name`, on `node`, run, once its containers run,
   * paused: its node's agent resumes its sandbox under `release` once the pod's egress is
   * confined there, which kube-router does a moment after the pod has its address, and the
   * pod is then ready, by its readiness gate. A pod that ends meanwhile, whose sandbox
   * the agent does not resume, or whose readiness cannot be set, is refused.
   */
  const resumeRestored = (name: string, node: string, release: string): Effect.Effect<void, SandboxRestoreRefused> =>
    Effect.gen(function*() {
      const running = yield* kube.read("v1", "Pod", profile.namespace, name).pipe(
        Effect.flatMap((read): Effect.Effect<V1Pod, PodNotRunning | SandboxRestoreRefused> => {
          // A core Pod, as the API server returns one.
          const pod = read as V1Pod | null;
          const statuses = pod?.status?.containerStatuses ?? [];
          if (pod?.status?.phase === "Failed" || pod?.status?.phase === "Succeeded" || statuses.some(({ state }) => state?.terminated)) {
            return Effect.fail(new SandboxRestoreRefused({ reason: `its pod ended before it ran (${pod?.status?.reason ?? pod?.status?.phase ?? "terminated"})` }));
          }
          const ready = pod && statuses.length > 0 && statuses.every(({ state }) => state?.running) && (pod.status?.podIPs?.length ?? 0) > 0;
          return ready ? Effect.succeed(pod) : Effect.fail(new PodNotRunning());
        }),
        Effect.retry({ while: (error) => error._tag === "PodNotRunning", schedule: Schedule.max([Schedule.spaced("500 millis"), Schedule.during(POD_RUNNING_TIMEOUT)]) }),
        Effect.catchTag("PodNotRunning", () => Effect.fail(new SandboxRestoreRefused({ reason: `its pod did not run within ${POD_RUNNING_TIMEOUT}` }))),
        Effect.catchTag("KubeApiError", (error) => Effect.fail(new SandboxRestoreRefused({ reason: error.message }))),
      );
      const containerIds = (running.status?.containerStatuses ?? []).flatMap(({ containerID }) => (containerID ? [containerID.replace(/^containerd:\/\//u, "")] : []));
      const podIPs = (running.status?.podIPs ?? []).map(({ ip }) => ip);
      yield* agents.resume(node, { ...ref(name), release, namespace: profile.namespace, pod: name, podIPs, containerIds }).pipe(
        Effect.mapError((error) => new SandboxRestoreRefused({ reason: error.message })),
      );
      const now = new Date(yield* Clock.currentTimeMillis).toISOString();
      yield* kube.patchPodStatus(profile.namespace, name, { status: { conditions: [{ type: RESUMED_CONDITION, status: "True", lastTransitionTime: now }] } }).pipe(
        Effect.mapError((error) => new SandboxRestoreRefused({ reason: `its readiness could not be set: ${error.message}` })),
      );
    });

  const restore = (sandbox: StoredSandbox, desired: Sandbox): Effect.Effect<SandboxRestore | null> => {
    const name = sandbox.metadata.name;
    return Effect.gen(function*() {
      const started = yield* Clock.currentTimeMillis;
      const read = yield* agents.current(ref(name)).pipe(Effect.result);
      if (read._tag === "Failure") return yield* fallBack(name, "unreadable", read.failure.message);
      const manifest = read.success;
      if (!manifest) return null;
      // Only the snapshot its Sandbox was released into: one it has moved on from since,
      // as a restore alasio stopped in the middle of, is stale.
      const bound = sandbox.metadata.annotations?.[SNAPSHOT_ANNOTATION];
      if (bound !== manifest.snapshot) {
        return yield* fallBack(name, "stale", `its snapshot ${manifest.snapshot} is not the one its Sandbox was released into (${bound ?? "none"})`);
      }
      const kept = snapshots.releases.find(({ release }) => release === manifest.release);
      if (!kept) return yield* fallBack(name, "release", `gVisor ${manifest.release}, which took it, is no longer kept`);
      const recorded = manifest.podTemplate as V1PodTemplateSpec;
      const template = sessionTemplateOf(recorded);
      // Onto the current template, it takes what is the Sandbox's own now; onto the one before, what it was saved with.
      // A fork's is its source's, which names the source: only onto the current template, then.
      const own = recorded.metadata?.labels?.[SANDBOX_LABEL] === name;
      const base = template === templates.current ? desired.spec.podTemplate : template !== undefined && template === templates.previous && own ? recorded : null;
      if (!base) return yield* fallBack(name, "template", "the session template it was taken on is no longer kept");
      const nodes = yield* nodesFor(manifest).pipe(Effect.result);
      if (nodes._tag === "Failure") return yield* fallBack(name, "unreadable", nodes.failure.message);
      if (nodes.success.length === 0) {
        return yield* fallBack(name, "node", `no node has gVisor ${manifest.release} and every CPU feature it was saved with`);
      }
      let why = "";
      for (const node of nodes.success) {
        const prepared = yield* agents.prepare(node, ref(name)).pipe(Effect.result);
        if (prepared._tag === "Failure") {
          why = prepared.failure.message;
          yield* Effect.logWarning(why);
          continue;
        }
        yield* Effect.logInfo(`restoring session ${name} on ${node} from its snapshot ${manifest.snapshot}, taken at ${manifest.takenAt} under gVisor ${manifest.release}`);
        const restoring: V1PodTemplateSpec = {
          ...base,
          // Left paused, until its node's agent sees its egress confined (resumeRestored).
          metadata: { ...base.metadata, annotations: { ...base.metadata?.annotations, [RESTORE_ANNOTATION]: prepared.success.imagePath, [RESTORE_PAUSED_ANNOTATION]: "true" } },
          spec: {
            ...base.spec,
            containers: base.spec?.containers ?? [],
            runtimeClassName: kept.runtimeClassName,
            readinessGates: [...(base.spec?.readinessGates ?? []), { conditionType: RESUMED_CONDITION }],
            affinity: {
              ...base.spec?.affinity,
              nodeAffinity: {
                requiredDuringSchedulingIgnoredDuringExecution: { nodeSelectorTerms: [{ matchFields: [{ key: "metadata.name", operator: "In", values: [node] }] }] },
              },
            },
          },
        };
        return {
          podTemplate: restoring,
          kept: base,
          resume: resumeRestored(name, node, kept.release),
          // The session moves on from it.
          restored: Clock.currentTimeMillis.pipe(
            Effect.map((now) => {
              restores.add(1, { "alasio.sandbox.restore.outcome": "restored" });
              restoreDuration.record((now - started) / 1000);
            }),
            Effect.andThen(forget(name, "the session restored from it runs on")),
          ),
          failed: (failure: string) => fallBack(name, "restore", failure).pipe(Effect.asVoid),
        };
      }
      return yield* fallBack(name, "prepare", why);
    }).pipe(withAlasioSpan("alasio.sandbox.restore", { attributes: { "alasio.sandbox.name": name } }), withLogScope("snapshots"));
  };

  /**
   * Saves the session's sandbox by its node's agent: suspended once it is saved, or once
   * a save that failed has ended it anyway; left running otherwise, said why.
   */
  const save = (sandbox: StoredSandbox): Effect.Effect<Saved, KubeApiError> => {
    const name = sandbox.metadata.name;
    const counted = (outcome: string) => Effect.sync(() => releases.add(1, { "alasio.sandbox.release.outcome": outcome }));
    return Effect.gen(function*() {
      // A core Pod, as the API server returns one.
      const pod = (yield* kube.read("v1", "Pod", profile.namespace, name)) as V1Pod | null;
      const containerId = pod?.status?.containerStatuses?.find((status) => status.name === BAYMA_CONTAINER)?.containerID?.replace(/^containerd:\/\//u, "");
      const release = releaseOf(pod?.spec?.runtimeClassName);
      const bayma = sandbox.spec.podTemplate.spec?.containers.find((container) => container.name === BAYMA_CONTAINER);
      const hook = bayma ? hookOf(bayma) : null;
      const node = pod?.spec?.nodeName;
      if (!node || !containerId || !release || !hook) {
        yield* Effect.logWarning(`session ${name} is not snapshotted: its pod runs no bayma under a gVisor release alasio keeps`);
        yield* counted("kept");
        return "keep" as const;
      }
      const taken = yield* agents.checkpoint(node, { ...ref(name), release, containerId, hook, podTemplate: { ...sandbox.spec.podTemplate } }).pipe(Effect.result);
      if (taken._tag === "Success") {
        const { snapshot, bytes, pauseMs, uploadMs } = taken.success;
        yield* Effect.logInfo(`snapshotted session ${name} as ${snapshot} on ${node}: ${bytes} bytes, paused ${pauseMs} ms, uploaded in ${uploadMs} ms`);
        yield* counted("snapshotted");
        return { snapshot };
      }
      yield* counted("failed");
      // A core Pod, as the API server returns one.
      const after = (yield* kube.read("v1", "Pod", profile.namespace, name)) as V1Pod | null;
      const ended = after?.status?.containerStatuses?.find((status) => status.name === BAYMA_CONTAINER)?.state?.terminated !== undefined;
      yield* Effect.logWarning(`could not snapshot session ${name}: ${taken.failure.message}; ${ended ? "its sandbox ended, so it is suspended" : "it runs on"}`);
      return ended ? { snapshot: null } : ("keep" as const);
    });
  };

  /** Whether the session's bayma has run no exec for releaseAfter, as it says. */
  const idle = (sandboxes: Sandboxes, sandbox: StoredSandbox, now: number): Effect.Effect<boolean> => {
    const name = sandbox.metadata.name;
    return Effect.gen(function*() {
      const token = yield* sandboxes.token(name);
      if (!token) return false;
      const host = sandbox.status?.serviceFQDN ?? `${name}.${profile.namespace}.svc`;
      const text = yield* Effect.tryPromise({
        try: async (signal) => {
          const response = await fetch(`http://${host}:${profile.port}/status`, { headers: { Authorization: `Bearer ${token}` }, signal: AbortSignal.any([signal, AbortSignal.timeout(STATUS_TIMEOUT_MS)]) });
          if (!response.ok) throw new Error(`it answered ${response.status}`);
          return await response.text();
        },
        catch: (cause) => new Error(cause instanceof Error ? cause.message : String(cause)),
      });
      const status = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(BaymaStatus))(text);
      return status.execsRunning === 0 && status.execsQueued === 0 && status.idleSinceMs !== null && now - status.idleSinceMs >= releaseAfterMs;
    }).pipe(Effect.catch((error) => Effect.logDebug(`could not ask session ${name}'s bayma whether it is idle: ${error.message}`).pipe(Effect.as(false))));
  };

  const watch = (sandboxes: Sandboxes, lastTurn: (name: string) => number | null): Effect.Effect<void, never, Scope.Scope> =>
    Effect.gen(function*() {
      const releasing = yield* FiberMap.make<string>();
      const look = Effect.gen(function*() {
        const listed = yield* kube.list(SANDBOX_API_VERSION, SANDBOX_KIND, profile.namespace, "alasio.dev/workload=session");
        const now = yield* Clock.currentTimeMillis;
        for (const sandbox of listed as readonly StoredSandbox[]) {
          const name = sandbox.metadata.name;
          if (sandbox.spec.operatingMode === "Suspended" || !sandboxReady(sandbox) || (yield* FiberMap.has(releasing, name))) continue;
          const turned = lastTurn(name);
          if (turned !== null && now - turned < releaseAfterMs) continue;
          if (!(yield* idle(sandboxes, sandbox, now))) continue;
          // Looked at again once nothing else may bring it up: a turn may have begun meanwhile.
          const stillIdle = (held: StoredSandbox): Effect.Effect<Saved, KubeApiError> =>
            Effect.gen(function*() {
              const at = yield* Clock.currentTimeMillis;
              const since = lastTurn(name);
              if ((since !== null && at - since < releaseAfterMs) || !(yield* idle(sandboxes, held, at))) return "keep" as const;
              return yield* save(held);
            });
          yield* FiberMap.run(
            releasing,
            name,
            sandboxes.release(name, stillIdle).pipe(
              Effect.tap((released) => (released ? Effect.logInfo(`released session ${name}`) : Effect.void)),
              Effect.catch((error) => Effect.logWarning(`could not release session ${name}: ${error.message}`)),
              withAlasioSpan("alasio.sandbox.release", { attributes: { "alasio.sandbox.name": name } }),
            ),
            { onlyIfMissing: true },
          );
        }
      }).pipe(Effect.catch((error) => Effect.logWarning(`could not look at sessions to release: ${error.message}`)));
      yield* look.pipe(Effect.repeat(Schedule.spaced(RELEASE_POLL)), Effect.forkScoped);
    }).pipe(withLogScope("snapshots"));

  return {
    // Never one running a restore's template, which only a restore alasio stopped in the middle of leaves.
    keeps: (sandbox) => {
      const template = sessionTemplateOf(sandbox.spec.podTemplate);
      const restoring = sandbox.spec.podTemplate.metadata?.annotations?.[RESTORE_ANNOTATION] !== undefined;
      return !restoring && template !== undefined && template !== templates.current && template === templates.previous;
    },
    restore,
    watch,
    release: (sandboxes, name) => sandboxes.release(name, save).pipe(withAlasioSpan("alasio.sandbox.release", { attributes: { "alasio.sandbox.name": name } }), withLogScope("snapshots")),
    copy: (from, to) =>
      agents.copy(ref(from), ref(to)).pipe(
        Effect.tap(({ snapshot }) => Effect.logInfo(`gave the fork ${to} of session ${from} its snapshot ${snapshot}`)),
        Effect.map(({ snapshot }): string | null => snapshot),
        Effect.catch((error) =>
          error._tag === "NodeAgentError" && error.status === 404
            ? Effect.succeed(null)
            : Effect.logWarning(`the fork ${to} of session ${from} carries no snapshot: ${error.message}`).pipe(Effect.as(null))
        ),
        withLogScope("snapshots"),
      ),
  };
});

/** The hash a session's pod template records of the profile's template it is made from. */
export const sessionTemplateHash = (profile: SessionsProfile): string => podTemplateHash(profile.podTemplate);
