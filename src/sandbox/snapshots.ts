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
 * hold sessions to the CPU baseline `alasio up` records in them (cli/src/cpu-baseline.ts),
 * so a change of the baseline is one of the template. A session restored onto the template before the current one
 * stays on it while its pod runs, and is moved onto the current one as it is resumed
 * without a snapshot.
 *
 * A snapshot that cannot restore is never so silently: the session is resumed as one
 * without a snapshot, from bayma's checkpoints of its sessions, which bayma tells its next
 * exec of, and why it was not restored is logged and counted, the snapshot forgotten.
 *
 * A restore refused before its sandbox resumed is abandoned on its node first: the node's
 * agent kills what runsc runs for the pod there, so that the pod's teardown, as its
 * Sandbox is suspended, waits on none of it.
 */
import type { V1Container, V1LabelSelector, V1NetworkPolicy, V1Pod, V1PodTemplateSpec } from "@kubernetes/client-node";
import { Clock, Duration, Effect, FiberMap, Schedule, Schema, type Scope } from "effect";
import { FetchHttpClient } from "effect/http";

import { type KubeApiError, KubeClient } from "../kube/client.ts";
import type { SessionsProfile } from "../kube/config.ts";
import {
  BAYMA_CONTAINER,
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
  type RestoreDeferred,
  type RestoreFailure,
  type SandboxRestore,
  SandboxRestoreRefused,
  type Saved,
  type SavedSnapshot,
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
import type { NodeAgentError, NodeAgents } from "./node-agents.ts";

/** How sessions are snapshotted, as the profile says. */
export type SnapshotsProfile = NonNullable<SessionsProfile["snapshots"]>;

/** How long a restored pod is waited for to run its containers, its images pulled. */
const POD_RUNNING_TIMEOUT = "3 minutes";
/** How often running sessions are looked at for one to release. */
const RELEASE_POLL: Duration.Input = "15 seconds";
/** How often what no Sandbox has a use for is collected: snapshots of sessions that are gone, and images nodes cache that no restore will read. */
const COLLECT_POLL: Duration.Input = "10 minutes";
/** How many sessions of one node are released at a time. */
const RELEASES_PER_NODE = 2;
/** How long a look at a bayma's status may take. */
const STATUS_TIMEOUT_MS = 5000;
/**
 * How long a session's restore is deferred for, from its first that was, while what it
 * needs fails for a while (an agent, the bucket, its pod's start, its network policies),
 * before it is brought up without its snapshot: the prompts meanwhile are told so.
 */
const DEFER_FOR_MS = 15 * 60_000;

const releases = meter.createCounter("alasio.sandbox.releases", {
  description:
    "Session sandboxes released, by outcome: snapshotted and suspended; kept, as they run under no gVisor; failed, running on; lost, their sandbox ended unsaved, its REPLs with it; or error, the release failing beside its save",
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
 * Whether the label selector `selector` selects what has `labels`, as Kubernetes matches
 * one: every label it names, and every expression of it; none selects nothing, an empty one
 * everything.
 */
export function selects(selector: V1LabelSelector | undefined, labels: Readonly<Record<string, string>>): boolean {
  if (!selector) return false;
  const named = Object.entries(selector.matchLabels ?? {}).every(([key, value]) => labels[key] === value);
  return named && (selector.matchExpressions ?? []).every(({ key, operator, values = [] }) => {
    const value = labels[key];
    if (operator === "In") return value !== undefined && values.includes(value);
    if (operator === "NotIn") return value === undefined || !values.includes(value);
    if (operator === "Exists") return value !== undefined;
    if (operator === "DoesNotExist") return value === undefined;
    return false;
  });
}

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
  /**
   * How a suspended Sandbox is restored from its snapshot; null when it is resumed without
   * one (said why, when it had one); or why its restore is deferred, its snapshot kept.
   */
  readonly restore: (sandbox: StoredSandbox, desired: Sandbox) => Effect.Effect<SandboxRestore | RestoreDeferred | null>;
  /**
   * Releases each running session once it has run no exec and no turn for long enough,
   * looking every RELEASE_POLL, for as long as the scope lasts; `lastTurn(name)` is when
   * the session's last turn ended, Infinity while one runs, null when none has since
   * alasio started.
   */
  readonly watch: (sandboxes: Sandboxes, lastTurn: (name: string) => number | null) => Effect.Effect<void, never, Scope.Scope>;
  /** Releases the running session now, idle or not, as a fork of it does: whether it was suspended. */
  readonly release: (sandboxes: Sandboxes, name: string) => Effect.Effect<boolean, KubeApiError | SandboxNotReady | SandboxPodEnded | SandboxGone>;
  /** Forgets the session's snapshot, as it is deleted, in the bucket and in every node's cache; one that cannot be is said so, and collected later. */
  readonly forget: (name: string) => Effect.Effect<void>;
  /** Gives the session `to`, a fork of `from`, a copy of `from`'s snapshot, if it has one: which; null for none, one that cannot be copied said so. */
  readonly copy: (from: string, to: string) => Effect.Effect<SavedSnapshot | null>;
}

/** A session's snapshots, as the module says, on the KubeClient. */
export const makeSessionSnapshots = Effect.fnUntraced(function*({ profile, snapshots, agents, templates }: SessionSnapshotsOptions): Effect.fn.Return<SessionSnapshots, never, KubeClient> {
  const kube = yield* KubeClient;
  const fetch = yield* FetchHttpClient.Fetch;
  const environment = profile.namespace;
  const releaseAfterMs = snapshots.releaseAfterSeconds * 1000;
  const ref = (name: string) => ({ environment, workspace: name });

  /** Forgets the session's snapshot, saying why; one that cannot be forgotten is said so too. */
  const forget = (name: string, why: string): Effect.Effect<void> =>
    agents.forget(ref(name)).pipe(
      Effect.catch((error) => Effect.logWarning(`could not forget the snapshot of session ${name}: ${error.message}`)),
      Effect.andThen(Effect.logInfo(`forgot the snapshot of session ${name}: ${why}`)),
    );

  /** Since when each session's restore has been deferred, from the first that was: in memory, as alasio's start begins a window anew. */
  const deferredSince = new Map<string, number>();

  /** Resumes the session without its snapshot, which is forgotten, saying why, and counted as `fallback`. */
  const fallBack = (name: string, fallback: Fallback, why: string): Effect.Effect<null> =>
    Effect.logWarning(`session ${name} is resumed from bayma's checkpoints, not from its snapshot: ${why}`).pipe(
      Effect.andThen(Effect.sync(() => {
        deferredSince.delete(name);
        restores.add(1, { "alasio.sandbox.restore.outcome": "fallback", "alasio.sandbox.restore.fallback": fallback });
      })),
      Effect.andThen(forget(name, why)),
      Effect.as(null),
    );

  /**
   * Defers the session's restore, which failed for `why`, as what it needs may come back:
   * its snapshot kept, as nothing has run since, for the next bring-up; once its restores
   * have been deferred for DEFER_FOR_MS, it falls back as `fallback` instead.
   */
  const deferOr = (name: string, fallback: Fallback, why: string): Effect.Effect<RestoreDeferred | null> =>
    Effect.gen(function*() {
      const now = yield* Clock.currentTimeMillis;
      const since = deferredSince.get(name) ?? now;
      if (now - since >= DEFER_FOR_MS) return yield* fallBack(name, fallback, `${why}, as its restores have been for ${Math.round((now - since) / 60_000)} minutes`);
      deferredSince.set(name, since);
      restores.add(1, { "alasio.sandbox.restore.outcome": "deferred", "alasio.sandbox.restore.fallback": fallback });
      yield* Effect.logWarning(`the restore of session ${name} from its snapshot is deferred, its snapshot kept for the next try: ${why}`);
      return { deferred: why };
    });

  /** Whether an agent's failure `error` may pass: one of reaching it, or of the bucket behind it, not one of what it was asked of. */
  const passing = (error: NodeAgentError | KubeApiError): boolean =>
    error._tag === "KubeApiError" || error.status === undefined || error.status >= 500;

  /**
   * The nodes that can restore `manifest`, those holding its images first: they have its
   * release, with every CPU feature it was saved with; and whether every agent said what
   * its node holds.
   */
  const nodesFor = (manifest: Manifest) =>
    Effect.gen(function*() {
      const found = yield* agents.nodes;
      const held = yield* Effect.forEach(found, (node) =>
        agents.holdings(node).pipe(
          Effect.map((holdings) => [holdings]),
          Effect.catch((error) => Effect.logWarning(error.message).pipe(Effect.as([]))),
        ));
      const answered = found.length > 0 && held.every((each) => each.length > 0);
      const able = held.flat().filter(({ releases: installed }) =>
        installed.some(({ release, cpuFeatures }) => release === manifest.release && manifest.cpuFeatures.every((feature) => cpuFeatures.includes(feature)))
      );
      const holds = (cached: (typeof able)[number]["cached"]) =>
        cached.some(({ environment: of, workspace, images }) => of === manifest.environment && workspace === manifest.workspace && manifest.layers.every(({ digest }) => images.includes(digest)));
      return { nodes: [...able.filter(({ cached }) => holds(cached)), ...able.filter(({ cached }) => !holds(cached))].map(({ node }) => node), answered };
    });

  /**
   * Lets the restored pod of session `name`, on `node`, run, once its containers run,
   * paused: its node's agent resumes its sandbox under `release` once the pod's egress is
   * confined there, which kube-router does a moment after the pod has its address, and the
   * pod is then ready, by its readiness gate. A pod that ends meanwhile, whose sandbox
   * the agent does not resume, or whose readiness cannot be set, is refused.
   */
  const resumeRestored = (name: string, node: string): Effect.Effect<void, SandboxRestoreRefused> =>
    Effect.gen(function*() {
      const running = yield* kube.read("v1", "Pod", profile.namespace, name).pipe(
        Effect.flatMap((read): Effect.Effect<V1Pod, PodNotRunning | SandboxRestoreRefused> => {
          // A core Pod, as the API server returns one.
          const pod = read as V1Pod | null;
          const statuses = pod?.status?.containerStatuses ?? [];
          if (pod?.status?.phase === "Failed" || pod?.status?.phase === "Succeeded" || statuses.some(({ state }) => state?.terminated)) {
            return Effect.fail(new SandboxRestoreRefused({ reason: `its pod ended before it ran (${pod?.status?.reason ?? pod?.status?.phase ?? "terminated"})`, resumed: false }));
          }
          const ready = pod && statuses.length > 0 && statuses.every(({ state }) => state?.running) && (pod.status?.podIPs?.length ?? 0) > 0;
          return ready ? Effect.succeed(pod) : Effect.fail(new PodNotRunning());
        }),
        Effect.retry({ while: (error) => error._tag === "PodNotRunning", schedule: Schedule.max([Schedule.spaced("500 millis"), Schedule.during(POD_RUNNING_TIMEOUT)]) }),
        Effect.catchTag("PodNotRunning", () => Effect.fail(new SandboxRestoreRefused({ reason: `its pod did not run within ${POD_RUNNING_TIMEOUT}`, resumed: false }))),
        Effect.catchTag("KubeApiError", (error) => Effect.fail(new SandboxRestoreRefused({ reason: error.message, resumed: false }))),
      );
      const containerIds = (running.status?.containerStatuses ?? []).flatMap(({ containerID }) => (containerID ? [containerID.replace(/^containerd:\/\//u, "")] : []));
      const podIPs = (running.status?.podIPs ?? []).map(({ ip }) => ip);
      // The policies that select it, which its node's rules for it must name, not those of a pod of its name before it.
      const policies = yield* kube.list("networking.k8s.io/v1", "NetworkPolicy", profile.namespace, "").pipe(
        Effect.map((found) => (found as V1NetworkPolicy[]).filter(({ spec }) => selects(spec?.podSelector, running.metadata?.labels ?? {})).flatMap(({ metadata }) => metadata?.name ?? [])),
        Effect.mapError((error) => new SandboxRestoreRefused({ reason: error.message, resumed: false })),
      );
      // Its agent leaves it paused where its network policies are not in force (504); of any other failure, its sandbox may have run.
      yield* agents.resume(node, { ...ref(name), namespace: profile.namespace, pod: name, podIPs, containerIds, policies }).pipe(
        Effect.mapError((error) => new SandboxRestoreRefused({ reason: error.message, resumed: !(error._tag === "NodeAgentError" && error.status === 504) })),
      );
      const now = new Date(yield* Clock.currentTimeMillis).toISOString();
      yield* kube.patchPodStatus(profile.namespace, name, { status: { conditions: [{ type: RESUMED_CONDITION, status: "True", lastTransitionTime: now }] } }).pipe(
        Effect.mapError((error) => new SandboxRestoreRefused({ reason: `its readiness could not be set: ${error.message}`, resumed: true })),
      );
    });

  /**
   * Has the agent of `node` abandon the restore of session `name` into its pod there,
   * refused before its sandbox resumed: it kills what runsc runs for the pod, which a
   * `runsc restore` that failed has left running, its shim tearing none of it down, and
   * which the pod's teardown would wait on. It stays as defence should a restore leave
   * runsc's processes behind. One the agent cannot do is said so, and changes nothing else.
   */
  const abandon = (name: string, node: string): Effect.Effect<void> =>
    agents.abandon(node, { ...ref(name), namespace: profile.namespace, pod: name }).pipe(
      Effect.flatMap((killed) =>
        killed.length === 0
          ? Effect.void
          : Effect.logInfo(`killed what runsc left running of the restore of session ${name} on ${node}: ${killed.map(({ containerId, pid }) => `${pid} of ${containerId}`).join(", ")}`)
      ),
      Effect.catch((error) => Effect.logWarning(`could not abandon the restore of session ${name} on ${node}: ${error.message}`)),
    );

  const restore = (sandbox: StoredSandbox, desired: Sandbox): Effect.Effect<SandboxRestore | RestoreDeferred | null> => {
    const name = sandbox.metadata.name;
    return Effect.gen(function*() {
      const started = yield* Clock.currentTimeMillis;
      const read = yield* agents.current(ref(name)).pipe(Effect.result);
      if (read._tag === "Failure") {
        return yield* (passing(read.failure) ? deferOr : fallBack)(name, "unreadable", read.failure.message);
      }
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
      const able = yield* nodesFor(manifest).pipe(Effect.result);
      if (able._tag === "Failure") return yield* deferOr(name, "unreadable", able.failure.message);
      if (able.success.nodes.length === 0) {
        const none = `no node has gVisor ${manifest.release} and every CPU feature it was saved with`;
        return yield* (able.success.answered ? fallBack(name, "node", none) : deferOr(name, "node", `${none} of the nodes whose agents answered`));
      }
      // Each node is tried in turn: one that cannot restore it (409) leaves the others; one whose bucket or agent fails may pass.
      const failures: (NodeAgentError | KubeApiError)[] = [];
      for (const node of able.success.nodes) {
        const prepared = yield* agents.prepare(node, ref(name)).pipe(Effect.result);
        if (prepared._tag === "Failure") {
          failures.push(prepared.failure);
          yield* Effect.logWarning(prepared.failure.message);
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
          snapshot: { snapshot: manifest.snapshot, release: manifest.release },
          podTemplate: restoring,
          kept: base,
          resume: resumeRestored(name, node),
          // The session moves on from it.
          restored: Clock.currentTimeMillis.pipe(
            Effect.map((now) => {
              deferredSince.delete(name);
              restores.add(1, { "alasio.sandbox.restore.outcome": "restored" });
              restoreDuration.record((now - started) / 1000);
            }),
            // The session moves on from it; its images go on as the layers of its next.
            Effect.andThen(
              agents.spend(ref(name), manifest.snapshot).pipe(
                Effect.catch((error) => Effect.logWarning(`could not spend the snapshot of session ${name}, which runs on from it: ${error.message}`)),
              ),
            ),
          ),
          // Its sandbox never having run, its restore is abandoned, whatever comes of it.
          failed: ({ why, resumed }: RestoreFailure) =>
            (resumed ? fallBack(name, "restore", why) : abandon(name, node).pipe(Effect.andThen(deferOr(name, "restore", why)))).pipe(
              Effect.map((came) => (came === null ? "fallen back" : "deferred")),
            ),
        };
      }
      // Gone or corrupt (404, 422), or of no node's (409), it never restores; otherwise it may yet.
      const why = failures.map(({ message }) => message).join("; ");
      return yield* (failures.some(passing) && !failures.some((failure) => failure._tag === "NodeAgentError" && (failure.status === 404 || failure.status === 422))
        ? deferOr(name, "prepare", why)
        : fallBack(name, "prepare", why));
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
      // Which release runs it is the node agent's to tell, from its sandbox: a pod started before a release was bumped runs the one before.
      const gvisor = pod?.spec?.runtimeClassName !== undefined;
      const bayma = sandbox.spec.podTemplate.spec?.containers.find((container) => container.name === BAYMA_CONTAINER);
      const hook = bayma ? hookOf(bayma) : null;
      const node = pod?.spec?.nodeName;
      if (!node || !containerId || !gvisor || !hook) {
        yield* Effect.logWarning(`session ${name} is not snapshotted: its pod runs no bayma under gVisor`);
        yield* counted("kept");
        return "keep" as const;
      }
      const taken = yield* agents.checkpoint(node, { ...ref(name), namespace: profile.namespace, pod: name, containerId, hook, podTemplate: { ...sandbox.spec.podTemplate } }).pipe(Effect.result);
      if (taken._tag === "Success") {
        const { snapshot, release, bytes, pauseMs, uploadMs } = taken.success;
        yield* Effect.logInfo(`snapshotted session ${name} as ${snapshot} on ${node}: ${bytes} bytes, paused ${pauseMs} ms, uploaded in ${uploadMs} ms`);
        yield* counted("snapshotted");
        return { saved: { snapshot, release } };
      }
      // A core Pod, as the API server returns one.
      const after = (yield* kube.read("v1", "Pod", profile.namespace, name)) as V1Pod | null;
      const ended = after?.status?.containerStatuses?.find((status) => status.name === BAYMA_CONTAINER)?.state?.terminated !== undefined;
      // One that ended its sandbox lost what ran in it but bayma's checkpoints keep.
      yield* counted(ended ? "lost" : "failed");
      yield* Effect.logWarning(`could not snapshot session ${name}: ${taken.failure.message}; ${ended ? "its sandbox ended, so it is suspended" : "it runs on"}`);
      return ended ? { saved: null } : ("keep" as const);
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
      // How many releases run on each node, which its agent checkpoints one beside the other.
      const onNode = new Map<string, number>();
      const look = Effect.gen(function*() {
        const listed = yield* kube.list(SANDBOX_API_VERSION, SANDBOX_KIND, profile.namespace, "alasio.dev/workload=session");
        const now = yield* Clock.currentTimeMillis;
        for (const sandbox of listed as readonly StoredSandbox[]) {
          const name = sandbox.metadata.name;
          if (sandbox.spec.operatingMode === "Suspended" || !sandboxReady(sandbox) || (yield* FiberMap.has(releasing, name))) continue;
          const turned = lastTurn(name);
          if (turned !== null && now - turned < releaseAfterMs) continue;
          if (!(yield* idle(sandboxes, sandbox, now))) continue;
          // A node runs RELEASES_PER_NODE at a time, its disk, CPU and uplink shared: the others wait for a later look.
          const pod = (yield* kube.read("v1", "Pod", profile.namespace, name)) as V1Pod | null;
          const node = pod?.spec?.nodeName;
          if (!node || (onNode.get(node) ?? 0) >= RELEASES_PER_NODE) continue;
          onNode.set(node, (onNode.get(node) ?? 0) + 1);
          const done = Effect.sync(() => onNode.set(node, (onNode.get(node) ?? 1) - 1));
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
              Effect.catch((error) =>
                Effect.logWarning(`could not release session ${name}: ${error.message}`).pipe(
                  Effect.andThen(Effect.sync(() => releases.add(1, { "alasio.sandbox.release.outcome": "error" }))),
                )
              ),
              withAlasioSpan("alasio.sandbox.release", { attributes: { "alasio.sandbox.name": name } }),
              Effect.ensuring(done),
            ),
            { onlyIfMissing: true },
          );
        }
      }).pipe(Effect.catch((error) => Effect.logWarning(`could not look at sessions to release: ${error.message}`)));
      yield* look.pipe(Effect.repeat(Schedule.spaced(RELEASE_POLL)), Effect.forkScoped);
      yield* collect.pipe(Effect.repeat(Schedule.spaced(COLLECT_POLL)), Effect.forkScoped);
    }).pipe(withLogScope("snapshots"));

  /** Forgets the session's snapshots in the bucket and in every node's cache that holds them. */
  const forgetEverywhere = (name: string): Effect.Effect<void, NodeAgentError | KubeApiError> =>
    Effect.gen(function*() {
      yield* agents.forget(ref(name));
      for (const node of yield* agents.nodes) yield* agents.discard(node, ref(name), []);
    });

  /**
   * Collects what no Sandbox has a use for, whose images hold what the sessions held,
   * secrets among it: the snapshots in the bucket of sessions that are gone, which a
   * deletion that could not forget them left; of each session, what its agent says none of
   * its snapshots needs any more (../node-agent/agent.ts), which alasio alone collects,
   * one workspace at a time; and the images a node caches that none of them needs, but
   * those a restore may be reading.
   */
  const collect: Effect.Effect<void> = Effect.gen(function*() {
    const listed = (yield* kube.list(SANDBOX_API_VERSION, SANDBOX_KIND, profile.namespace, "alasio.dev/workload=session")) as readonly StoredSandbox[];
    const sessions = new Set(listed.map((sandbox) => sandbox.metadata.name));
    const kept = new Map<string, readonly string[]>();
    for (const workspace of yield* agents.workspaces(environment)) {
      if (sessions.has(workspace)) {
        kept.set(workspace, yield* agents.collect(ref(workspace)));
        continue;
      }
      yield* agents.forget(ref(workspace));
      yield* Effect.logInfo(`forgot the snapshots of session ${workspace}, which is gone`);
    }
    for (const node of yield* agents.nodes) {
      for (const held of (yield* agents.holdings(node)).cached.filter((cached) => cached.environment === environment)) {
        const keep = kept.get(held.workspace) ?? [];
        if (held.images.some((digest) => !keep.includes(digest))) yield* agents.discard(node, ref(held.workspace), keep);
      }
    }
  }).pipe(Effect.catch((error) => Effect.logWarning(`could not collect sessions' snapshots: ${error.message}`)));

  return {
    // Never one running a restore's template, which only a restore alasio stopped in the middle of leaves.
    keeps: (sandbox) => {
      const template = sessionTemplateOf(sandbox.spec.podTemplate);
      const restoring = sandbox.spec.podTemplate.metadata?.annotations?.[RESTORE_ANNOTATION] !== undefined;
      return !restoring && template !== undefined && template !== templates.current && template === templates.previous;
    },
    restore,
    watch,
    forget: (name) =>
      forgetEverywhere(name).pipe(
        Effect.catch((error) => Effect.logWarning(`could not forget the snapshot of session ${name}, which is collected later: ${error.message}`)),
        withLogScope("snapshots"),
      ),
    release: (sandboxes, name) => sandboxes.release(name, save).pipe(withAlasioSpan("alasio.sandbox.release", { attributes: { "alasio.sandbox.name": name } }), withLogScope("snapshots")),
    copy: (from, to) =>
      agents.copy(ref(from), ref(to)).pipe(
        Effect.tap(({ snapshot }) => Effect.logInfo(`gave the fork ${to} of session ${from} its snapshot ${snapshot}`)),
        Effect.map(({ snapshot, release }): SavedSnapshot | null => ({ snapshot, release })),
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
