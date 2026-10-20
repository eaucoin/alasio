/**
 * The sandbox gateway's folder instances: the bayma of a folder conversation under a harness,
 * `folder:<harness>:<conversation>` (./names.ts folderKey), main's alone, a pod the gateway
 * makes at its bind, in the folder namespace, of the folder template `alasio up` renders of
 * the host profile, as the folder pods' policy pins it (cli/src/manifests/admission.ts
 * folderPodPolicy). No pool of free pods: its arguments carry its state directory, which is
 * the conversation's on the machine, under the template's state root (./names.ts
 * folderStateDir), and a warm pod of the operator's template would buy little.
 *
 * Its binding's token, which its bayma takes requests and exports telemetry with, is in a
 * Secret of its own, made before it and owned by it, so it goes as the pod goes.
 *
 * Released once its bayma has run no exec, and no request of it has come, for
 * `releaseAfterMs`: its pod deleted, and waited for until it is gone, as its bayma dumps each
 * idle REPL session as it stops (./names.ts FOLDER_STOP_SECONDS), so the next bind's pod, on
 * the same state directory, restores them; nothing of the node agents', nor of the bucket's. A
 * pod whose bayma ended is released as broken, the next bind making another.
 *
 * A bind makes its pod once no pod of the key before it is left, as one being deleted still
 * dumps its REPL sessions. It gives its pod FOLDER_START_MS to run (FOLDER_PULL_MS while its
 * image is being pulled), and its bayma as long again to answer once it runs, or deletes it
 * and its Lease, saying why: its pod pending, as unscheduled; its image not pulled; its bayma
 * ended; or its bayma not answering. A bind of a key another bind or a release of it is under
 * way of waits for it as long as a bind may take (./names.ts folderBindMs). FOLDER_FAILURES binds of a key failing one after another within
 * FOLDER_FAILING_MS hold it: it is bound again only as its caller asks (its token's `retry`),
 * until a bind goes through. Its failures are a Lease's of their own in the folder namespace
 * (failingLease), so every replica of the gateway holds it alike, and they are counted, of
 * every key held so (alasio.gateway.folders.failing), and listed (`GET /folders`) with each
 * instance: its key, its pod, since when it is bound, when its bayma last ran an exec, and its
 * binds' last failure. What a bind of a key under way waits on, its pods say, is answered of
 * the key a caller's token names (`GET /folders/starting`), as alasio tells the turn it waits.
 *
 * Its pod requests the memory its folder's instances came to use, as VPA sizes alasio's
 * workloads (cli/src/manifests/vpa.ts), which it cannot size, as no workload makes it (SIZING):
 * the gateway keeps the peak of each hour its instances run, of their working set as the
 * metrics server measures it, in a Lease of the folder's own (sizingLease), so that no other
 * conversation's instance, nor a branch's, is sized by them; and its next pod requests their
 * 90th percentile with a margin.
 */
import { randomBytes } from "node:crypto";

import type { KubernetesObject, V1Lease, V1Pod, V1PodSpec, V1PodTemplateSpec, V1Secret } from "@kubernetes/client-node";
import type { ObservableResult } from "@opentelemetry/api";
import { Clock, Effect, Schema, type Scope } from "effect";

import { type Binder, BindStepFailed } from "../binder/binder.ts";
import { heldLeases, RELEASING_ANNOTATION } from "../binder/bindings.ts";
import { BINDING_LABEL, KEY_ANNOTATION, keyHash, type Pool } from "../binder/names.ts";
import { type Answer, type Operation, type Placement, PlacementRefused } from "../binder/server.ts";
import type { TokenClaims } from "../binder/tokens.ts";
import { baymaIdleness, IDLE_POLL_MS, makeSweep } from "../binder/sweep.ts";
import { bindingToken } from "../binder/tokens.ts";
import { hasStatus, type KubeApiError, KubeClient } from "../kube/client.ts";
import { quantityOf } from "../kube/quantity.ts";
import { BAYMA_CONTAINER } from "../kube/sandbox-shape.ts";
import { meter } from "../telemetry/index.ts";
import { FOLDER_GONE_MS, FOLDER_POOL, FOLDER_PULL_MS, FOLDER_START_MS, folderBindMs, folderOfKey, folderPodName, folderPodSpec, folderStateDir, folderTokenSecret } from "./names.ts";
import { released } from "./releases.ts";

/** How many binds of a key failing one after another, within FOLDER_FAILING_MS, hold it. */
export const FOLDER_FAILURES = 3;
export const FOLDER_FAILING_MS = 10 * 60_000;
/** How often a bind asks after its pod, and its bayma, as it starts. */
const START_POLL_MS = 1_000;
/** How long an ask of a starting bayma's status may take. */
const STATUS_TIMEOUT_MS = 5_000;

/** How long a look at what the folder instances use may take, the metrics server not answering: past it, no peak is kept of that look. */
const METRICS_TIMEOUT_MS = 10_000;

/** How many times a failure is added again to its key's Lease that another replica changed meanwhile. */
const SETTLE_TRIES = 5;

/** The label of a key's failures' Lease, and its annotations: the times its binds failed, and why the last did. */
const FAILING_LABEL = "alasio.dev/folder-failing";
const FAILURES_ANNOTATION = "alasio.dev/failures";
const REASON_ANNOTATION = "alasio.dev/reason";
/** The name of the Lease of `key`'s binds' failures. */
export const failingLease = (key: string): string => `folder-failing-${keyHash(key)}`;

/**
 * How a folder instance's memory request is sized as its pod is made, as VPA's recommender
 * sizes alasio's workloads: of the peaks of the hours its folder's instances ran, over eight
 * days, each a day older weighing half as much, the 90th percentile, with a margin of 15%,
 * within its template's request and limit; a folder of none, as one the metrics server never
 * measured, its template's request.
 */
export const SIZING = { intervalMs: 3_600_000, intervals: 192, halfLifeIntervals: 24, percentile: 0.9, margin: 0.15 } as const;

/** A folder's memory peaks: of each interval its instances ran, its start, and the most they held in it, in bytes. */
export type MemoryPeaks = readonly (readonly [at: number, bytes: number])[];

/**
 * The memory request, in bytes, of a folder instance whose folder's instances held `peaks`, as
 * of `now`, each the peak of an interval of `intervalMs`, as SIZING says: in whole MiB, between
 * `floor` and `ceiling`, which it is where it would be past it, whole MiB or not.
 */
export function memoryRequestOf(peaks: MemoryPeaks, now: number, intervalMs: number, floor: number, ceiling: number): number {
  const weighed = peaks
    .filter(([at]) => now - at < SIZING.intervals * intervalMs)
    .map(([at, bytes]) => ({ bytes, weight: 0.5 ** ((now - at) / (SIZING.halfLifeIntervals * intervalMs)) }))
    .toSorted((one, other) => one.bytes - other.bytes);
  const total = weighed.reduce((sum, { weight }) => sum + weight, 0);
  let under = 0;
  const peak = weighed.find(({ weight }) => (under += weight) >= SIZING.percentile * total)?.bytes;
  if (peak === undefined) return floor;
  return Math.floor(Math.min(ceiling, Math.max(floor, Math.ceil((peak * (1 + SIZING.margin)) / 2 ** 20) * 2 ** 20)));
}

/** The label of a folder's Lease of its memory peaks, and its annotation of them, as JSON. */
const SIZING_LABEL = "alasio.dev/folder-sizing";
const PEAKS_ANNOTATION = "alasio.dev/memory-peaks";
/** The name of the Lease of `key`'s instances' memory peaks. */
export const sizingLease = (key: string): string => `folder-sizing-${keyHash(key)}`;

/** What the metrics server says a pod's containers use now, as the gateway reads it. */
interface PodMetrics extends KubernetesObject {
  readonly containers?: readonly { readonly name: string; readonly usage?: Readonly<Record<string, string>> }[];
}

/** The keys held, their binds having failed one after another, of a folder namespace's: each replica's count of the one record, so read the greatest. */
const failingGauge = meter.createObservableGauge("alasio.gateway.folders.failing", {
  description: "The folder conversations whose instances' binds failed one after another, so they are bound again only as asked; every gateway replica's count of the same, so read the greatest",
  unit: "{folder}",
});

/** What bayma's /status says of its last exec, as a folder instance's listing reads it. */
const BaymaStatus = Schema.Struct({ idleSinceMs: Schema.NullOr(Schema.Number) });

/** A folder instance, as `GET /folders` lists it. */
export interface FolderInstance {
  readonly key: string;
  readonly harness: string;
  readonly conversation: string;
  /** Its pod, where it is bound, and since when; null where it is released. */
  readonly pod: string | null;
  readonly boundAt: string | null;
  /** When its bayma last ran an exec, where it says; null where it runs one, has run none, or did not answer. */
  readonly lastExecAt: string | null;
  /** Its binds' last failure, where its last bind failed: why, when, how many in a row, and whether it is held. */
  readonly failing: { readonly reason: string; readonly at: string; readonly times: number; readonly held: boolean } | null;
}

/** What makeFolders is given; see the module. */
export interface FoldersOptions {
  readonly namespace: string;
  readonly template: V1PodTemplateSpec;
  readonly stateRoot: string;
  readonly releaseAfterMs: number;
  /** main's environment's name, whose callers alone bind a folder instance. */
  readonly main: string;
  readonly podPort: number;
  readonly bindingKey: Buffer;
  readonly idlePollMs?: number;
  /** How long a bind gives its pod to run and its bayma to answer: FOLDER_START_MS unless said; and its pod to run while its image is pulled: FOLDER_PULL_MS unless said. */
  readonly startMs?: number;
  readonly pullMs?: number;
  /** How long each memory peak kept of a folder's instances is the peak of: SIZING's hour unless said; and how long a look at what they use may take: METRICS_TIMEOUT_MS unless said. */
  readonly sizingIntervalMs?: number;
  readonly metricsTimeoutMs?: number;
}

/** The gateway's folder instances; see makeFolders. */
export interface Folders {
  /** The pool folder instances are bound in. */
  readonly pool: Pool;
  /** Where the folder instance `key` is bound, by its caller's token's options, and how its pod is made. */
  readonly place: (key: string, options: Readonly<Record<string, string>>) => Effect.Effect<Placement, PlacementRefused>;
  /** Releases each folder instance idle for its time, as the module says: which keys. */
  readonly sweep: Effect.Effect<readonly string[], KubeApiError>;
  /** Releases, as broken, each folder instance whose bayma ended, as the module says; and counts the keys held. */
  readonly live: Effect.Effect<void, KubeApiError>;
  /** Keeps the memory peak of each folder instance bound, as the metrics server measures it, and forgets each folder's peaks once all are past SIZING's days. */
  readonly measure: Effect.Effect<void>;
  /** Every folder instance bound, or whose last bind failed, as the module says. */
  readonly list: Effect.Effect<readonly FolderInstance[], KubeApiError>;
  /** What the front answers of `/folders`, for main's callers alone: `GET /folders`, the list. */
  readonly operate: (claims: TokenClaims, operation: Operation) => Effect.Effect<Answer>;
  /** How many keys are held now, as the last look counted them. */
  readonly failing: () => number;
}

/** The gateway's folder instances, as the module says, on the binder and the KubeClient. */
export const makeFolders = Effect.fnUntraced(function*(
  { namespace, template, stateRoot, releaseAfterMs, main, podPort, bindingKey, idlePollMs = IDLE_POLL_MS, startMs = FOLDER_START_MS, pullMs = FOLDER_PULL_MS, sizingIntervalMs = SIZING.intervalMs, metricsTimeoutMs = METRICS_TIMEOUT_MS }: FoldersOptions,
  binder: Binder["Service"],
): Effect.fn.Return<Folders, never, KubeClient | Scope.Scope> {
  const kube = yield* KubeClient;
  const pool: Pool = { name: FOLDER_POOL, namespace };
  /** How many keys are held, as the last look counted them, which the gauge reads. */
  let held = 0;
  const observe = (result: ObservableResult) => result.observe(held, { "k8s.namespace.name": namespace });
  yield* Effect.acquireRelease(Effect.sync(() => failingGauge.addCallback(observe)), () => Effect.sync(() => failingGauge.removeCallback(observe)));

  /** Deletes the pod `name`, which its bayma stops as its grace allows, dumping its REPL sessions, and waits until it is gone. */
  const removed = (name: string): Effect.Effect<void, KubeApiError | BindStepFailed> =>
    Effect.gen(function*() {
      yield* kube.remove("v1", "Pod", namespace, name);
      const deadline = (yield* Clock.currentTimeMillis) + FOLDER_GONE_MS;
      while ((yield* kube.read("v1", "Pod", namespace, name)) !== null) {
        if ((yield* Clock.currentTimeMillis) >= deadline) return yield* new BindStepFailed({ reason: `the folder instance's pod ${name} was not gone within ${FOLDER_GONE_MS / 1000} s` });
        yield* Effect.sleep(1_000);
      }
    });

  /** The template's bayma's memory request and limit, in bytes: the least and the most a folder instance requests. */
  const resources = template.spec?.containers.find(({ name }) => name === BAYMA_CONTAINER)?.resources;
  const [floor, ceiling] = [quantityOf(resources?.requests?.["memory"]), quantityOf(resources?.limits?.["memory"])];
  /** Of each folder bound, its instance's memory peak in the interval under way, not kept yet: the interval's start, and the bytes. */
  const peaking = new Map<string, { readonly at: number; readonly bytes: number }>();

  /** The Lease of `key`'s instances' memory peaks, where it has one. */
  const peaksLeaseOf = (key: string): Effect.Effect<V1Lease | null, KubeApiError> =>
    Effect.map(kube.read("coordination.k8s.io/v1", "Lease", namespace, sizingLease(key)), (lease) => lease as V1Lease | null);
  /** The memory peaks `lease` keeps, none where it keeps none that are. */
  const peaksOf = (lease: V1Lease | null): MemoryPeaks => {
    const peaks = Schema.decodeUnknownOption(Schema.fromJsonString(Schema.Array(Schema.Tuple([Schema.Number, Schema.Number]))))(lease?.metadata?.annotations?.[PEAKS_ANNOTATION] ?? "[]");
    return peaks._tag === "Some" ? peaks.value : [];
  };

  /**
   * Keeps the peak of `key`'s instance in the interval under way, where one is not kept yet, in
   * its folder's Lease: the greater of it and what another replica kept of the interval, those
   * past SIZING's eight days left out; read again where another replica changed it meanwhile.
   */
  const kept = (key: string): Effect.Effect<void> =>
    Effect.gen(function*() {
      const peak = peaking.get(key);
      if (!peak) return;
      peaking.delete(key);
      const written = Effect.gen(function*() {
        const now = yield* Clock.currentTimeMillis;
        const lease = yield* peaksLeaseOf(key);
        const peaks = new Map(peaksOf(lease).filter(([at]) => now - at < SIZING.intervals * sizingIntervalMs));
        peaks.set(peak.at, Math.max(peaks.get(peak.at) ?? 0, peak.bytes));
        const annotations = { [KEY_ANNOTATION]: key, [PEAKS_ANNOTATION]: JSON.stringify([...peaks].toSorted(([one], [other]) => one - other)) };
        if (lease) yield* kube.replace({ ...lease, metadata: { ...lease.metadata, annotations: { ...lease.metadata?.annotations, ...annotations } } });
        else yield* kube.create({ apiVersion: "coordination.k8s.io/v1", kind: "Lease", metadata: { name: sizingLease(key), namespace, labels: { [SIZING_LABEL]: "true" }, annotations } });
      });
      yield* written.pipe(Effect.retry({ while: hasStatus(409), times: SETTLE_TRIES }));
    }).pipe(Effect.catch((error) => Effect.logWarning(`keeping the memory peak of ${key}'s folder instance failed: ${error.message}`)));

  /** That `key`'s instance holds `bytes` now: the peak of the interval under way, the one before kept as it is over. */
  const peaked = (key: string, bytes: number): Effect.Effect<void> =>
    Effect.gen(function*() {
      const at = Math.floor((yield* Clock.currentTimeMillis) / sizingIntervalMs) * sizingIntervalMs;
      const peak = peaking.get(key);
      if (peak && peak.at !== at) yield* kept(key);
      peaking.set(key, { at, bytes: Math.max(peak?.at === at ? peak.bytes : 0, bytes) });
    });

  /** What the bayma of each pod of the folder namespace holds now, by its pod's name, as the metrics server measures it; none where it does not answer, nor in time, so no peak is kept. */
  const workingSets: Effect.Effect<ReadonlyMap<string, number>> = kube.list("metrics.k8s.io/v1beta1", "PodMetrics", namespace, "").pipe(
    Effect.map((items): ReadonlyMap<string, number> =>
      new Map((items as PodMetrics[]).flatMap(({ metadata, containers }) => {
        const bytes = quantityOf(containers?.find(({ name }) => name === BAYMA_CONTAINER)?.usage?.["memory"]);
        return metadata?.name && Number.isFinite(bytes) ? [[metadata.name, bytes] as const] : [];
      }))
    ),
    Effect.timeoutOrElse({ duration: metricsTimeoutMs, orElse: () => Effect.succeed(new Map()) }),
    Effect.orElseSucceed(() => new Map()),
  );

  /** When the folders' peaks were last looked at for those all past SIZING's days. */
  let collectedAt = 0;
  /** Deletes, once an interval, the Lease of each folder whose peaks are all past SIZING's days, its instance keeping none under way. */
  const collected: Effect.Effect<void> = Effect.gen(function*() {
    const now = yield* Clock.currentTimeMillis;
    if (now - collectedAt < sizingIntervalMs) return;
    collectedAt = now;
    for (const lease of (yield* kube.list("coordination.k8s.io/v1", "Lease", namespace, `${SIZING_LABEL}=true`)) as V1Lease[]) {
      const key = lease.metadata?.annotations?.[KEY_ANNOTATION] ?? "";
      if (peaking.has(key) || peaksOf(lease).some(([at]) => now - at < SIZING.intervals * sizingIntervalMs)) continue;
      yield* kube.remove("coordination.k8s.io/v1", "Lease", namespace, lease.metadata?.name ?? "").pipe(Effect.catchIf(hasStatus(404), () => Effect.void));
    }
  }).pipe(Effect.catch((error) => Effect.logWarning(`forgetting folders' memory peaks past their days failed: ${error.message}`)));

  /** Keeps each bound instance's memory, as the metrics server measures it, its folder's peak, apart from the release of those whose bayma ended (live), so that a slow metrics server holds up no release. */
  const measure: Effect.Effect<void> = Effect.gen(function*() {
    const used = yield* workingSets;
    for (const lease of yield* Effect.provideService(heldLeases(pool), KubeClient, kube).pipe(Effect.orElseSucceed(() => []))) {
      // Not one being released, whose release keeps its peak: one measured after would be kept by none.
      if (lease.metadata?.annotations?.[RELEASING_ANNOTATION]) continue;
      const bytes = used.get(lease.spec?.holderIdentity ?? "");
      if (bytes !== undefined) yield* peaked(lease.metadata?.annotations?.[KEY_ANNOTATION] ?? "", bytes);
    }
    yield* collected;
  });

  /**
   * The memory request of `key`'s next instance, in bytes, as SIZING says, of its folder's
   * peaks; null where its template's stands: the template's bayma requests none, or the folder
   * has no peaks that raise it.
   */
  const requestOf = (key: string): Effect.Effect<number | null> =>
    Effect.gen(function*() {
      if (!Number.isFinite(floor)) return null;
      const peaks = peaksOf(yield* peaksLeaseOf(key).pipe(Effect.orElseSucceed(() => null)));
      const bytes = memoryRequestOf(peaks, yield* Clock.currentTimeMillis, sizingIntervalMs, floor, Number.isFinite(ceiling) ? ceiling : Number.POSITIVE_INFINITY);
      return bytes > floor ? bytes : null;
    });

  /** Its peak kept, then the pod `name` of `key` deleted and waited for until it is gone (removed). */
  const releasedPod = (key: string, name: string): Effect.Effect<void, KubeApiError | BindStepFailed> => Effect.andThen(kept(key), removed(name));

  /**
   * Waits until no other pod of `key`, its pods' `labels`, is left: each deleted, where it is
   * not being deleted already, and waited for, as its bayma still runs, dumping its REPL
   * sessions to the state directory the next pod restores them from, until its grace ends.
   */
  const alone = (key: string, labels: Readonly<Record<string, string>>): Effect.Effect<void, KubeApiError | BindStepFailed> =>
    Effect.gen(function*() {
      const selector = Object.entries(labels).map(([label, value]) => `${label}=${value}`).join(",");
      const deadline = (yield* Clock.currentTimeMillis) + FOLDER_GONE_MS;
      for (;;) {
        const left = ((yield* kube.list("v1", "Pod", namespace, selector)) as V1Pod[]).filter((pod) => pod.metadata?.annotations?.[KEY_ANNOTATION] === key);
        if (left.length === 0) return;
        for (const pod of left) if (pod.metadata?.deletionTimestamp === undefined) yield* kube.remove("v1", "Pod", namespace, pod.metadata?.name ?? "").pipe(Effect.catchIf(hasStatus(404), () => Effect.void));
        if ((yield* Clock.currentTimeMillis) >= deadline) return yield* new BindStepFailed({ reason: `the folder instance's pod before, ${left.map((pod) => pod.metadata?.name).join(", ")}, was not gone within ${FOLDER_GONE_MS / 1000} s` });
        yield* Effect.sleep(1_000);
      }
    });

  /**
   * The pod of the folder instance `key`, of `harness` and `conversation`, made once no pod of
   * it before is left (alone), with its binding's token's Secret first, which it then owns.
   */
  const make = (key: string, harness: string, conversation: string) =>
  (labels: Readonly<Record<string, string>>, annotations: Readonly<Record<string, string>>, binding: string): Effect.Effect<V1Pod, KubeApiError | BindStepFailed> =>
    Effect.gen(function*() {
      yield* alone(key, labels);
      const name = folderPodName(key, randomBytes(4).readUInt32BE().toString(36).padStart(5, "0").slice(-5));
      const secret: V1Secret = {
        apiVersion: "v1",
        kind: "Secret",
        metadata: { name: folderTokenSecret(name), namespace, labels: { ...labels } },
        type: "Opaque",
        stringData: { token: bindingToken(bindingKey, { key, binding, namespace }) },
      };
      yield* kube.create(secret);
      const request = yield* requestOf(key);
      const made = folderPodSpec((template.spec ?? { containers: [] }) as V1PodSpec, name, folderStateDir(stateRoot, harness, conversation));
      // Its bayma requesting what its folder's instances came to use, where they used more than its template's, in bytes, as its limit may be no whole MiB.
      const spec: V1PodSpec = request === null ? made : {
        ...made,
        containers: made.containers.map((container) =>
          container.name === BAYMA_CONTAINER ? { ...container, resources: { ...container.resources, requests: { ...container.resources?.requests, memory: String(request) } } } : container
        ),
      };
      const pod = yield* kube.create<V1Pod>({
        apiVersion: "v1",
        kind: "Pod",
        metadata: { name, namespace, labels: { ...template.metadata?.labels, ...labels }, annotations: { ...template.metadata?.annotations, ...annotations } },
        spec,
      }).pipe(Effect.tapError(() => kube.remove("v1", "Secret", namespace, folderTokenSecret(name)).pipe(Effect.ignore)));
      // Owned by its pod, so it goes as the pod goes; where it cannot be, both deleted, as nothing would delete it after.
      yield* kube.patch("v1", "Secret", namespace, folderTokenSecret(name), {
        metadata: { ownerReferences: [{ apiVersion: "v1", kind: "Pod", name, uid: pod.metadata?.uid ?? "", blockOwnerDeletion: false, controller: false }] },
      }).pipe(Effect.tapError(() => Effect.andThen(kube.remove("v1", "Pod", namespace, name).pipe(Effect.ignore), kube.remove("v1", "Secret", namespace, folderTokenSecret(name)).pipe(Effect.ignore))));
      yield* Effect.logInfo(`made the folder instance ${name} of ${key}, on the state directory ${folderStateDir(stateRoot, harness, conversation)}${request === null ? "" : `, requesting ${Math.round(request / 2 ** 20)} MiB, as its folder's instances came to use`}`);
      return pod;
    });

  /** The status of the bayma of `pod`, bound to `key` by `binding`, as it answers: its text, or why it did not. */
  const statusOf = (pod: V1Pod, key: string, binding: string): Effect.Effect<string, { readonly message: string }> =>
    Effect.tryPromise({
      try: async (signal) => {
        const token = bindingToken(bindingKey, { key, binding, namespace });
        const response = await fetch(`http://${pod.status?.podIP}:${podPort}/status`, { headers: { authorization: `Bearer ${token}` }, signal: AbortSignal.any([signal, AbortSignal.timeout(STATUS_TIMEOUT_MS)]) });
        if (!response.ok) throw new Error(`it answered ${response.status}`);
        return await response.text();
      },
      catch: (error) => ({ message: error instanceof Error ? error.message : String(error) }),
    });

  /** Why the pod `pod`, as it is now, is not serving yet: pending, its image unpulled, its bayma ended, or not yet running. */
  const notServing = (pod: V1Pod): string => {
    const bayma = pod.status?.containerStatuses?.find(({ name }) => name === BAYMA_CONTAINER);
    if (bayma?.state?.terminated) return `its bayma ended (${bayma.state.terminated.reason ?? `exit code ${bayma.state.terminated.exitCode}`})`;
    const waiting = bayma?.state?.waiting;
    if (waiting?.reason && waiting.reason !== "ContainerCreating") return `its bayma did not start: ${waiting.reason}${waiting.message ? ` (${waiting.message})` : ""}`;
    const scheduled = pod.status?.conditions?.find(({ type }) => type === "PodScheduled");
    if (scheduled?.status === "False") return `its pod is pending: ${scheduled.message ?? scheduled.reason ?? "not scheduled"}`;
    return "its pod is not running yet";
  };

  /**
   * Waits for the pod `made`, bound to `key` by `binding`, to run, for `startMs`, or `pullMs`
   * while its image is still being pulled, and then its bayma to answer, for `startMs` of its
   * start; or fails, saying why it did not.
   */
  const prepare = (key: string) => (made: V1Pod, binding: string): Effect.Effect<void, BindStepFailed> =>
    Effect.gen(function*() {
      const name = made.metadata?.name ?? "";
      const asked = yield* Clock.currentTimeMillis;
      let runningSince: number | null = null;
      for (;;) {
        const now = yield* Clock.currentTimeMillis;
        const pod = (yield* kube.read("v1", "Pod", namespace, name).pipe(Effect.orElseSucceed(() => made))) as V1Pod | null;
        if (!pod) return yield* new BindStepFailed({ reason: `its pod ${name} is gone` });
        const bayma = pod.status?.containerStatuses?.find(({ name: container }) => container === BAYMA_CONTAINER);
        if (bayma?.state?.running !== undefined && pod.status?.podIP) {
          runningSince ??= now;
          const answered = yield* Effect.result(statusOf(pod, key, binding));
          if (answered._tag === "Success") return;
          if (now >= runningSince + startMs) return yield* new BindStepFailed({ reason: `its bayma did not answer within ${startMs / 1000} s of its start: ${answered.failure.message}` });
        } else {
          const why = notServing(pod);
          if (why.startsWith("its bayma ended")) return yield* new BindStepFailed({ reason: why });
          // Scheduled, its image being pulled, as a node that has never run it pulls it first.
          const pulling = bayma?.state?.waiting?.reason === "ContainerCreating" || (bayma === undefined && pod.status?.conditions?.some(({ type, status }) => type === "PodScheduled" && status === "True") === true);
          const waited = pulling ? pullMs : startMs;
          if (now >= asked + waited) return yield* new BindStepFailed({ reason: `its pod ${name} did not serve within ${waited / 1000} s: ${pulling ? "its image is still being pulled" : why}` });
        }
        yield* Effect.sleep(START_POLL_MS);
      }
    });

  /** The Lease of `key`'s binds' failures, where it has one. */
  const failuresOf = (key: string): Effect.Effect<V1Lease | null, KubeApiError> =>
    Effect.map(kube.read("coordination.k8s.io/v1", "Lease", namespace, failingLease(key)), (lease) => lease as V1Lease | null);

  /** The times of the binds `lease` records failed, within FOLDER_FAILING_MS of `now`. */
  const recent = (lease: V1Lease | null, now: number): number[] => {
    const times = Schema.decodeUnknownOption(Schema.fromJsonString(Schema.Array(Schema.Number)))(lease?.metadata?.annotations?.[FAILURES_ANNOTATION] ?? "[]");
    return times._tag === "Some" ? times.value.filter((at) => now - at < FOLDER_FAILING_MS) : [];
  };

  /** `key`'s binds' failures forgotten: its Lease of them gone, where it had one. */
  const forgotten = (key: string): Effect.Effect<void, KubeApiError> =>
    kube.remove("coordination.k8s.io/v1", "Lease", namespace, failingLease(key)).pipe(Effect.catchIf(hasStatus(404), () => Effect.void));

  /**
   * Records how a bind of `key` ended: its failure, or, where it went through, none, its
   * failures forgotten. A failure is added to the Lease as it was read, read again where
   * another replica's changed it meanwhile (409), so each is counted.
   */
  const settled = (key: string) => (failure: string | null): Effect.Effect<void> =>
    Effect.gen(function*() {
      if (failure === null) return yield* forgotten(key);
      const added = Effect.gen(function*() {
        const now = yield* Clock.currentTimeMillis;
        const lease = yield* failuresOf(key);
        const times = [...recent(lease, now), now].slice(-FOLDER_FAILURES);
        const annotations = { [KEY_ANNOTATION]: key, [FAILURES_ANNOTATION]: JSON.stringify(times), [REASON_ANNOTATION]: failure };
        if (lease) yield* kube.replace({ ...lease, metadata: { ...lease.metadata, annotations: { ...lease.metadata?.annotations, ...annotations } } });
        else yield* kube.create({ apiVersion: "coordination.k8s.io/v1", kind: "Lease", metadata: { name: failingLease(key), namespace, labels: { [FAILING_LABEL]: "true" }, annotations } });
        return times.length;
      });
      const times = yield* added.pipe(Effect.retry({ while: hasStatus(409), times: SETTLE_TRIES }));
      yield* Effect.logWarning(`binding the folder instance of ${key} failed, ${times} in a row: ${failure}`);
    }).pipe(Effect.catch((error) => Effect.logWarning(`recording how a bind of ${key} ended failed: ${error.message}`)));

  /** Why `key` is held, where its binds failed FOLDER_FAILURES times in a row within FOLDER_FAILING_MS; null where it is not. */
  const heldBy = (key: string): Effect.Effect<string | null> =>
    Effect.gen(function*() {
      const lease = yield* failuresOf(key);
      if (recent(lease, yield* Clock.currentTimeMillis).length < FOLDER_FAILURES) return null;
      return `its last ${FOLDER_FAILURES} binds failed, the last as ${lease?.metadata?.annotations?.[REASON_ANNOTATION] ?? "it did"}`;
    }).pipe(Effect.catch((error) => Effect.logWarning(`reading the failures of ${key} failed: ${error.message}`).pipe(Effect.as(null))));

  const place = (key: string, options: Readonly<Record<string, string>>): Effect.Effect<Placement, PlacementRefused> =>
    Effect.gen(function*() {
      const folder = folderOfKey(key);
      if (!folder) return yield* new PlacementRefused({ reason: `${key} is no folder instance's` });
      if (options["environment"] !== main) return yield* new PlacementRefused({ reason: `a folder is the machine's own files, of main's alone, not ${options["environment"] ?? "unsaid"}'s` });
      return {
        pool,
        make: make(key, folder.harness, folder.conversation),
        prepare: prepare(key),
        // Released as it is when idle: its pod gone, its REPL sessions dumped as it stopped.
        release: ({ pod }) => releasedPod(key, pod).pipe(Effect.tap(() => released(pool, "asked"))),
        // Bound again only as its caller asks, where its binds failed one after another.
        held: options["retry"] === "1" ? Effect.succeed(null) : heldBy(key),
        settled: settled(key),
        // As long as its pod takes to get, so a bind of it waits out another's, or a release, of none of its own failing.
        waitMs: folderBindMs(pullMs, startMs),
        // Its caller's restart forgets its binds' failures, so the next bind goes on, held or not.
        reset: forgotten(key),
      };
    });

  const sweep = yield* makeSweep({
    pools: Effect.succeed([pool]),
    idleness: baymaIdleness({ podPort, releaseAfterMs, idlePollMs, bindingKey }),
    release: (_pool: Pool, key: string) => binder.release(pool, key, ({ pod }) => releasedPod(key, pod)).pipe(Effect.tap((done) => (done === "released" ? released(pool, "idle") : Effect.void))),
  });

  const live: Effect.Effect<void, KubeApiError> = Effect.gen(function*() {
    for (const lease of yield* Effect.provideService(heldLeases(pool), KubeClient, kube)) {
      const key = lease.metadata?.annotations?.[KEY_ANNOTATION] ?? "";
      const pod = (yield* kube.read("v1", "Pod", namespace, lease.spec?.holderIdentity ?? "")) as V1Pod | null;
      if (!pod || pod.metadata?.deletionTimestamp !== undefined) continue;
      const bayma = pod.status?.containerStatuses?.find((status) => status.name === BAYMA_CONTAINER);
      if (bayma?.state?.terminated === undefined && pod.status?.phase !== "Failed" && pod.status?.phase !== "Succeeded") continue;
      yield* Effect.logWarning(`${key}'s bayma ended in its folder instance ${pod.metadata?.name}, which serves its binding no more: it is released, the next bind making another`);
      yield* Effect.forkDetach(binder.release(pool, key, ({ pod: name }) => releasedPod(key, name), pod.metadata?.name ?? "").pipe(
        Effect.tap((done) => (done === "released" ? released(pool, "broken") : Effect.void)),
        Effect.catch((error) => Effect.logWarning(`releasing ${key}, whose bayma ended, failed: ${error.message}`)),
      ));
    }
  });

  /** The folder instance `key`'s parts, as its key names them. */
  const named = (key: string) => folderOfKey(key) ?? { harness: "", conversation: "" };

  const list: Effect.Effect<readonly FolderInstance[], KubeApiError> = Effect.gen(function*() {
    const now = yield* Clock.currentTimeMillis;
    const failures = new Map<string, V1Lease>();
    for (const lease of (yield* kube.list("coordination.k8s.io/v1", "Lease", namespace, `${FAILING_LABEL}=true`)) as V1Lease[]) {
      const key = lease.metadata?.annotations?.[KEY_ANNOTATION];
      if (key) failures.set(key, lease);
    }
    const failingOf = (key: string): FolderInstance["failing"] => {
      const lease = failures.get(key);
      const times = recent(lease ?? null, now);
      const last = times.at(-1);
      if (!lease || last === undefined) return null;
      return { reason: lease.metadata?.annotations?.[REASON_ANNOTATION] ?? "", at: new Date(last).toISOString(), times: times.length, held: times.length >= FOLDER_FAILURES };
    };
    const bound = yield* Effect.forEach(yield* Effect.provideService(heldLeases(pool), KubeClient, kube), (lease) =>
      Effect.gen(function*() {
        const key = lease.metadata?.annotations?.[KEY_ANNOTATION] ?? "";
        const name = lease.spec?.holderIdentity ?? "";
        const pod = (yield* kube.read("v1", "Pod", namespace, name)) as V1Pod | null;
        const status = pod ? yield* Effect.option(statusOf(pod, key, lease.metadata?.uid ?? "")) : null;
        const idle = status?._tag === "Some" ? Schema.decodeUnknownOption(Schema.fromJsonString(BaymaStatus))(status.value) : null;
        const lastExecAt = idle?._tag === "Some" && idle.value.idleSinceMs !== null ? new Date(idle.value.idleSinceMs).toISOString() : null;
        return { key, ...named(key), pod: name, boundAt: pod?.metadata?.creationTimestamp ? new Date(pod.metadata.creationTimestamp).toISOString() : null, lastExecAt, failing: failingOf(key) } satisfies FolderInstance;
      }), { concurrency: 4 });
    const released = [...failures.keys()].filter((key) => !bound.some((each) => each.key === key) && failingOf(key) !== null)
      .map((key): FolderInstance => ({ key, ...named(key), pod: null, boundAt: null, lastExecAt: null, failing: failingOf(key) }));
    return [...bound, ...released].sort((a, b) => a.key.localeCompare(b.key));
  });

  /**
   * What a bind of `key` under way waits on, as its pods say, worded to follow "its folder's
   * bayma": its instance before saving its REPL sessions, its image being pulled, its pod
   * pending, or its bayma starting; null where its pod serves, or it has none.
   */
  const startingOf = (key: string): Effect.Effect<string | null, KubeApiError> =>
    Effect.gen(function*() {
      const pods = ((yield* kube.list("v1", "Pod", namespace, `${BINDING_LABEL}=${keyHash(key)}`)) as V1Pod[]).filter((pod) => pod.metadata?.annotations?.[KEY_ANNOTATION] === key);
      if (pods.some((pod) => pod.metadata?.deletionTimestamp !== undefined)) return "whose instance before is saving its REPL sessions";
      const [pod] = pods;
      if (!pod || pod.status?.conditions?.some(({ type, status }) => type === "Ready" && status === "True")) return null;
      const bayma = pod.status?.containerStatuses?.find(({ name }) => name === BAYMA_CONTAINER);
      if (bayma?.state?.running !== undefined) return "whose bayma is starting";
      const scheduled = pod.status?.conditions?.find(({ type }) => type === "PodScheduled");
      if (scheduled?.status === "False") return `whose pod is pending: ${scheduled.message ?? scheduled.reason ?? "not scheduled"}`;
      if (bayma?.state?.waiting?.reason === "ContainerCreating" || (bayma === undefined && scheduled?.status === "True")) return "whose image is being pulled";
      return "whose pod is starting";
    });

  const operate = (claims: TokenClaims, { method, path }: Operation): Effect.Effect<Answer> =>
    Effect.gen(function*() {
      if (claims.options?.["environment"] !== main) return { status: 403, json: { error: `the folder instances are main's alone, not ${claims.options?.["environment"] ?? "unsaid"}'s` } };
      if (method === "GET" && path === "/folders/starting") {
        if (!folderOfKey(claims.key)) return { status: 404, json: { error: `${claims.key} is no folder instance's` } };
        return { status: 200, json: { starting: yield* startingOf(claims.key) } };
      }
      if (method !== "GET" || path !== "/folders") return { status: 404, json: { error: `no ${method} ${path}` } };
      return { status: 200, json: { folders: yield* list } };
    }).pipe(Effect.catch((error) => Effect.succeed({ status: 503, json: { error: `the folder instances could not be read: ${error.message}` } })));

  /** Counts the keys held, for the gauge. */
  const countHeld = Effect.gen(function*() {
    const now = yield* Clock.currentTimeMillis;
    const leases = (yield* kube.list("coordination.k8s.io/v1", "Lease", namespace, `${FAILING_LABEL}=true`)) as V1Lease[];
    held = leases.filter((lease) => recent(lease, now).length >= FOLDER_FAILURES).length;
  });

  return { pool, place, sweep, live: Effect.andThen(live, countHeld), measure, list, operate, failing: () => held };
});
