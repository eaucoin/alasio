/**
 * `alasio restore-filer-store [backup]`: the bundled object store's filer store put back
 * from one of its backups (../manifests/object-store.ts), with everything that writes to
 * the store stopped while it is, and started again after; or, without a backup, its
 * backups listed.
 *
 * What it stops it marks with what it was (its replicas, a ScaledObject's pause, a
 * CronJob's suspension), unless already marked, and starts again from the mark, so a run
 * that failed midway is picked up by the next, and what was paused or suspended before
 * stays so.
 */
import type { KubernetesObject, V1CronJob, V1Job, V1Pod } from "@kubernetes/client-node";
import { Console, Effect, Option, Schema, Stream } from "effect";
import { Argument, Command, Flag, Prompt } from "effect/cli";

import { branchNamespaces } from "../branches.ts";
import { loadConfig } from "../config.ts";
import { installConfig } from "../install.ts";
import { type Kind, kind, KubeApi, type ObjectRef, refOf } from "../kube/api.ts";
import { awaitEnded, awaitGone, awaitReady, awaitScheduled, selectorOf, type WaitOptions } from "../kube/rollout.ts";
import { reconcileLake } from "../lake.ts";
import { branchEnvironment } from "../manifests/branch.ts";
import { lakeRuns, MAIN_ENVIRONMENT, NAMESPACE, RELEASE } from "../manifests/common.ts";
import type { InstallConfig } from "../manifests/config.ts";
import { LOG_COMPONENT } from "../manifests/log.ts";
import {
  BACKUPS_LISTING,
  backupsListing,
  COMPONENTS,
  FILER_STORE,
  filerStoreRestore,
  RESTORE,
  RESTORED,
} from "../manifests/object-store.ts";
import { onCluster, runIn, timeoutFlag, waitOptions } from "./common.ts";

/** The manager what the command changes is patched as. */
const MANAGER = "alasio-restore-filer-store";

/** The annotation that marks what the command stopped, with what it was, as JSON. */
const MARK = "alasio.dev/restore-filer-store";

/** KEDA's annotations that pause a ScaledObject: at a count of replicas, or where it is. */
const PAUSED_REPLICAS = "autoscaling.keda.sh/paused-replicas";
const PAUSED = "autoscaling.keda.sh/paused";

/** Every object of the installation, main's and its branch environments'. */
const INSTANCE = selectorOf({ "app.kubernetes.io/instance": RELEASE });

/** The label of a Job's pods, of its name. */
const JOB_NAME = "batch.kubernetes.io/job-name";

/** A backup's file, of its stamp. */
const BACKUP_FILE = /^filer-store-(\d{8}T\d{6}Z)\.rdb$/u;

type Workload = KubernetesObject & { spec?: { replicas?: number; selector?: { matchLabels?: Record<string, string> }; volumeClaimTemplates?: readonly unknown[] } };
type Scaled = KubernetesObject & { spec?: { scaleTargetRef?: { kind?: string; name?: string } } };

/** A restore alasio cannot make, and why. */
export class RestoreRefused extends Schema.TaggedError<RestoreRefused>()("RestoreRefused", {
  message: Schema.String,
}) {}

/** The operator did not confirm. */
export class RestoreNotConfirmed extends Schema.TaggedError<RestoreNotConfirmed>()("RestoreNotConfirmed", {}) {
  override get message(): string {
    return "nothing was stopped or restored";
  }
}

/** `stamp` as a time people read: 2026-10-13 09:23:00 UTC. */
const when = (stamp: string): string => stamp.replace(/^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z$/u, (_, y, m, d, hh, mm, ss) => `${y}-${m}-${d} ${hh}:${mm}:${ss} UTC`);

/** What `object`'s mark says it was, or null where it has none. */
const markOf = <T>(object: KubernetesObject): T | null => {
  const mark = object.metadata?.annotations?.[MARK];
  return mark === undefined ? null : (JSON.parse(mark) as T);
};

/** Everything of `kinds` of the installation, in every namespace. */
const listed = <T extends KubernetesObject>(kinds: readonly Kind[]) =>
  Effect.flatMap(KubeApi, (kube) => Effect.map(Effect.forEach(kinds, (each) => kube.list<T>(each, { labelSelector: INSTANCE })), (lists) => lists.flat()));

const WORKLOADS = [kind("Deployment"), kind("StatefulSet")];

/** Deletes `refs`, Jobs with their pods, and waits until they are gone. */
const removed = Effect.fnUntraced(function*(refs: readonly ObjectRef[], options: WaitOptions) {
  const kube = yield* KubeApi;
  const jobs = refs.filter((ref) => ref.kind === "Job");
  const pods = (yield* Effect.forEach(jobs, (job) => kube.list<V1Pod>(kind("Pod"), { namespace: NAMESPACE, labelSelector: selectorOf({ [JOB_NAME]: job.name }) }))).flat().map(refOf);
  yield* Effect.forEach([...refs, ...pods], (ref) => kube.remove(ref));
  yield* awaitGone([...refs, ...pods], options);
});

/** What the newest pod of the Job `name` printed, in `container`. */
const printed = Effect.fnUntraced(function*(name: string, container: string) {
  const kube = yield* KubeApi;
  const pods = yield* kube.list<V1Pod>(kind("Pod"), { namespace: NAMESPACE, labelSelector: selectorOf({ [JOB_NAME]: name }) });
  const newest = pods.toSorted((a, b) => Date.parse(b.metadata?.creationTimestamp?.toString() ?? "0") - Date.parse(a.metadata?.creationTimestamp?.toString() ?? "0"))[0];
  if (!newest?.metadata?.name) return yield* new RestoreRefused({ message: `the Job ${name} has no pod whose output could be read` });
  return yield* kube.logs(NAMESPACE, newest.metadata.name, { container }).pipe(Stream.decodeText(), Stream.mkString);
});

/** The filer store's backups, by their stamps, the newest first, as a Job on their volume lists them. */
const backups = Effect.fnUntraced(function*(config: InstallConfig, options: WaitOptions) {
  const kube = yield* KubeApi;
  const job = backupsListing(config);
  yield* removed([refOf(job)], options);
  yield* kube.apply(job, MANAGER);
  yield* awaitReady([refOf(job)], options);
  const files = (yield* printed(BACKUPS_LISTING, "list")).split("\n").map((line) => line.trim());
  yield* removed([refOf(job)], options);
  return files.flatMap((file) => BACKUP_FILE.exec(file)?.slice(1, 2) ?? []).toSorted().toReversed();
});

/**
 * The workloads that write to the store, through it: alasio and the data stack, main's and
 * each branch environment's, but the log, whose segments are on its own volumes, so it
 * keeps what it holds and what reaches it meanwhile for its consumers to read once they run
 * again, and the webhook intake, which writes Telegram's updates to the log alone; and its
 * filers and gateways, which write to the filer store.
 */
const writers = Effect.map(listed<Workload>(WORKLOADS), (workloads) => {
  const component = (workload: Workload) => workload.metadata?.labels?.["app.kubernetes.io/component"] ?? "";
  const store = new Set([...Object.values(COMPONENTS), LOG_COMPONENT]);
  const front = new Set([COMPONENTS.gateway, COMPONENTS.filer]);
  return {
    clients: workloads.filter((workload) => !store.has(component(workload)) && (component(workload) === "alasio" || workload.metadata?.labels?.["alasio.dev/stack"] === "neon")),
    front: workloads.filter((workload) => workload.metadata?.namespace === NAMESPACE && front.has(component(workload))),
  };
});

/** The filer store's StatefulSet. */
const filerStore = Effect.flatMap(KubeApi, (kube) => kube.get<Workload>({ ...kind("StatefulSet"), namespace: NAMESPACE, name: FILER_STORE })).pipe(
  Effect.flatMap((found) => (found ? Effect.succeed(found) : Effect.fail(new RestoreRefused({ message: "the object store has no filer store in this cluster: alasio up makes it" })))),
);

/** The ScaledObjects that scale any of `workloads`. */
const scalersOf = (workloads: readonly Workload[]) =>
  Effect.map(listed<Scaled>([kind("ScaledObject")]), (scalers) =>
    scalers.filter(({ metadata, spec }) =>
      workloads.some((workload) => workload.metadata?.namespace === metadata?.namespace && workload.kind === (spec?.scaleTargetRef?.kind ?? "Deployment") && workload.metadata?.name === spec?.scaleTargetRef?.name)
    ));

/**
 * Stops `workloads`: their ScaledObjects paused at none, each marked with how it was
 * paused before; each scaled to none, marked with its replicas before; then waits until
 * their pods are gone.
 */
const stop = Effect.fnUntraced(function*(workloads: readonly Workload[], options: WaitOptions) {
  const kube = yield* KubeApi;
  for (const scaler of yield* scalersOf(workloads)) {
    const annotations = scaler.metadata?.annotations ?? {};
    const before = markOf<Record<string, string | null>>(scaler) ?? { [PAUSED_REPLICAS]: annotations[PAUSED_REPLICAS] ?? null, [PAUSED]: annotations[PAUSED] ?? null };
    yield* kube.patch(refOf(scaler), { metadata: { annotations: { [MARK]: JSON.stringify(before), [PAUSED_REPLICAS]: "0", [PAUSED]: null } } }, MANAGER);
  }
  for (const workload of workloads) {
    const before = markOf<{ replicas: number }>(workload) ?? { replicas: workload.spec?.replicas ?? 1 };
    yield* kube.patch(refOf(workload), { metadata: { annotations: { [MARK]: JSON.stringify(before) } }, spec: { replicas: 0 } }, MANAGER);
  }
  const pods = yield* Effect.forEach(workloads, (workload) =>
    kube.list<V1Pod>(kind("Pod"), { namespace: workload.metadata?.namespace ?? NAMESPACE, labelSelector: selectorOf(workload.spec?.selector?.matchLabels ?? {}) }));
  yield* awaitGone(pods.flat().map(refOf), options);
});

/** Whether `workload`'s pods are held to a node by their volumes: a StatefulSet's claims, which a local disk binds to its node. */
const held = (workload: Workload): boolean => workload.kind === "StatefulSet" && (workload.spec?.volumeClaimTemplates?.length ?? 0) > 0;

/**
 * Starts again what of `workloads` was stopped, as its mark says, and their ScaledObjects,
 * and waits until they run. Those whose volumes hold them to a node start first, and are
 * scheduled before the rest start: the rest, which run on any node, would otherwise take
 * the room on theirs, where nothing else would do.
 */
const start = Effect.fnUntraced(function*(workloads: readonly Workload[], options: WaitOptions) {
  const kube = yield* KubeApi;
  const stopped = workloads.filter((workload) => markOf(workload) !== null);
  const scaled = (each: readonly Workload[]) =>
    Effect.forEach(each, (workload) => {
      const { replicas } = markOf<{ replicas: number }>(workload) ?? { replicas: 1 };
      return kube.patch(refOf(workload), { metadata: { annotations: { [MARK]: null } }, spec: { replicas } }, MANAGER);
    });
  const first = stopped.filter(held);
  yield* scaled(first);
  yield* awaitScheduled(first.map(refOf), options);
  yield* scaled(stopped.filter((workload) => !held(workload)));
  for (const scaler of (yield* scalersOf(stopped)).filter((each) => markOf(each) !== null)) {
    yield* kube.patch(refOf(scaler), { metadata: { annotations: { ...markOf<Record<string, string | null>>(scaler), [MARK]: null } } }, MANAGER);
  }
  yield* awaitReady(stopped.map(refOf), options);
});

/** Suspends the installation's CronJobs, each marked with whether it was before, and waits until the Jobs of any running end. */
const suspendCronJobs = Effect.fnUntraced(function*(options: WaitOptions) {
  const kube = yield* KubeApi;
  for (const cronJob of yield* listed<V1CronJob>([kind("CronJob")])) {
    const before = markOf<{ suspend: boolean }>(cronJob) ?? { suspend: cronJob.spec?.suspend ?? false };
    yield* kube.patch(refOf(cronJob), { metadata: { annotations: { [MARK]: JSON.stringify(before) } }, spec: { suspend: true } }, MANAGER);
  }
  const running = (yield* listed<V1Job>([kind("Job")])).filter(({ status }) => (status?.active ?? 0) > 0);
  if (running.length > 0) yield* Effect.logInfo(`waiting for the Jobs that run to end: ${running.map(({ metadata }) => metadata?.name).join(", ")}`);
  yield* awaitEnded(running.map(refOf), options);
});

/** Resumes the CronJobs suspendCronJobs suspended, as each was before. */
const resumeCronJobs = Effect.gen(function*() {
  const kube = yield* KubeApi;
  for (const cronJob of yield* listed<V1CronJob>([kind("CronJob")])) {
    const before = markOf<{ suspend: boolean }>(cronJob);
    if (before) yield* kube.patch(refOf(cronJob), { metadata: { annotations: { [MARK]: null } }, spec: { suspend: before.suspend } }, MANAGER);
  }
});

/**
 * Every environment's lake's catalog set right with the store put back, which no longer
 * holds the files written since: main's, and each branch environment's, each against its own
 * catalog (../lake.ts); a line of what each lost, or of why its reconciliation failed, as
 * alasio lake reconcile runs it again. None where the lake does not run.
 */
const reconciledLakes = Effect.fnUntraced(function*(settings: InstallConfig, options: WaitOptions) {
  if (!lakeRuns(settings)) return [];
  const environments = [MAIN_ENVIRONMENT, ...(yield* branchNamespaces).map(branchEnvironment)];
  const lines = yield* Effect.forEach(environments, (environment) => {
    const which = environment.branch === null ? "main's lake" : `the lake of branch environment ${environment.branch}`;
    return reconcileLake(settings, environment, options).pipe(
      Effect.map((lost) => (lost.length > 0 ? lost.map((line) => `${which}: ${line}`) : [`${which} lost no file`])),
      Effect.catch((error) =>
        Effect.succeed([`${which} could not be set right (${error.message}): alasio lake reconcile${environment.branch === null ? "" : ` --branch ${environment.branch}`} runs it again`])
      ),
    );
  });
  return lines.flat();
});

/** The keys the filer store holds, and whether its last append-only file rewrite went well, as it says, logged in as its collector. */
const filerStoreState = Effect.gen(function*() {
  const kube = yield* KubeApi;
  const pods = yield* kube.list<V1Pod>(kind("Pod"), { namespace: NAMESPACE, labelSelector: selectorOf({ "app.kubernetes.io/instance": RELEASE, "app.kubernetes.io/component": COMPONENTS.filerStore }) });
  const pod = pods.find(({ status }) => status?.phase === "Running")?.metadata?.name;
  if (!pod) return yield* new RestoreRefused({ message: "the filer store runs no pod to check" });
  const { stdout } = yield* runIn({ namespace: NAMESPACE, pod, container: "valkey" }, ["/bin/sh", "-c", "valkey-cli --user collector info keyspace; valkey-cli --user collector info persistence"]);
  const keys = [...stdout.matchAll(/^db\d+:keys=(\d+),/gmu)].reduce((sum, [, count]) => sum + Number(count), 0);
  return { keys, rewritten: /^aof_last_bgrewrite_status:ok\s*$/mu.test(stdout) };
});

/**
 * Puts the backup `stamp` back: stops what writes to the store, alasio and Neon first,
 * while the store takes what they write as they stop, then its filers, and the filer
 * store; loads the backup onto its volume; starts the filer store and checks it; and
 * starts the rest again.
 */
const restore = Effect.fnUntraced(function*(settings: InstallConfig, stamp: string, options: WaitOptions) {
  const kube = yield* KubeApi;
  const store = yield* filerStore;
  const { clients, front } = yield* writers;
  yield* Effect.logInfo("stopping alasio, Neon and the lake, and the installation's CronJobs");
  yield* suspendCronJobs(options);
  yield* stop(clients, options);
  yield* Effect.logInfo("stopping the object store's gateways and filers, and its filer store");
  yield* stop(front, options);
  yield* stop([store], options);

  yield* Effect.logInfo(`putting the backup of ${when(stamp)} back`);
  const objects = filerStoreRestore(settings, `filer-store-${stamp}.rdb`);
  const jobs = objects.map(refOf);
  yield* removed(jobs, options);
  yield* Effect.forEach(objects, (object) => kube.apply(object, MANAGER));
  yield* awaitReady([{ ...kind("Job"), namespace: NAMESPACE, name: RESTORE }], options);
  const restored = Number(RESTORED.exec(yield* printed(RESTORE, "restore"))?.[1] ?? Number.NaN);
  yield* removed(jobs, options);
  if (Number.isNaN(restored)) return yield* new RestoreRefused({ message: "the restore's Job did not say how many keys it restored" });

  yield* Effect.logInfo("starting the filer store on it");
  yield* start([yield* filerStore], options);
  const state = yield* filerStoreState;
  if (state.keys !== restored || !state.rewritten) {
    return yield* new RestoreRefused({
      message: `the filer store holds ${state.keys} keys of the ${restored} restored${state.rewritten ? "" : ", and its append-only file was not written well"}: ` +
        `everything else stays stopped; alasio restore-filer-store ${stamp} again puts the backup back anew`,
    });
  }
  yield* Effect.logInfo("starting the object store's filers and gateways, then alasio, Neon and the lake, and the CronJobs");
  const again = yield* writers;
  yield* start(again.front, options);
  yield* start(again.clients, options);
  yield* Effect.logInfo("setting each lake's catalog right with the store, and loading again what its transcripts lost");
  const lost = yield* reconciledLakes(settings, options);
  yield* resumeCronJobs;
  yield* Console.log([`The filer store is as it was at ${when(stamp)}, with its ${restored} keys, and alasio runs on it again.`, ...lost.map((line) => `  ${line}`)].join("\n"));
});

/** What a restore rolls back, as the command's confirmation and its help say. */
const ROLLS_BACK = "The store's namespace, every bucket, object, version and tag, goes back to when the backup was taken: what was written since is lost, " +
  "and what was deleted since is listed again with its data gone. Neon's own files in the store go back with it while its pageserver's and safekeepers' volumes do not: " +
  "what the database wrote since is likely lost, and Neon may not start on them at all, when the database's dumps in the backups bucket, of before the backup, are what restores it. " +
  "Workspace storage's files written since lose their data. The lake's files written since are lost: once the lake runs again, its catalog is set right with the store, " +
  "the transcripts' rows they held are loaded again from alasio's database, and the telemetry's read again from the log, where it still holds them, as it says. " +
  "The log, on its own volumes, keeps what it holds, and the webhook intake goes on taking Telegram's updates into it.";

export const restoreFilerStore = Command.make(
  "restore-filer-store",
  {
    backup: Argument.String("backup").pipe(
      Argument.optional,
      Argument.withDescription("The backup, by its stamp, as the list says it, such as 20261013T092300Z; without one, the backups are listed"),
    ),
    yes: Flag.Boolean("yes").pipe(Flag.withAlias("y"), Flag.withDefault(false), Flag.withDescription("Do not ask for confirmation")),
    timeout: timeoutFlag,
  },
  ({ backup, yes, timeout }) =>
    onCluster(() =>
      Effect.gen(function*() {
        const settings = yield* installConfig(yield* loadConfig);
        const { bundled } = settings.objectStore;
        if (!settings.neon.enabled || !bundled.enabled) return yield* new RestoreRefused({ message: "alasio runs no bundled object store, whose filer store this restores" });
        if (!bundled.filerStore.backups.enabled) return yield* new RestoreRefused({ message: "the filer store's backups are off (objectStore.bundled.filerStore.backups.enabled), so there is none to restore" });
        const options = waitOptions(timeout);
        const stamps = yield* backups(settings, options);
        if (Option.isNone(backup)) {
          yield* Console.log(
            stamps.length === 0
              ? "The filer store has no backups yet."
              : [`The filer store's backups, the newest first:`, ...stamps.map((stamp) => `  ${stamp}  ${when(stamp)}`), "", "alasio restore-filer-store <backup> puts one back."].join("\n"),
          );
          return;
        }
        const stamp = backup.value;
        if (!stamps.includes(stamp)) {
          return yield* new RestoreRefused({ message: `the filer store has no backup ${stamp}${stamps.length > 0 ? `; it has ${stamps.join(", ")}` : ", nor any other yet"}` });
        }
        const confirmed = yes || (yield* Prompt.run(Prompt.Confirm({
          message: `Put the filer store back as it was at ${when(stamp)}, stopping alasio, Neon, the lake and the object store until it is? ${ROLLS_BACK}`,
          initial: false,
        })));
        if (!confirmed) return yield* new RestoreNotConfirmed();
        yield* restore(settings, stamp, options).pipe(
          Effect.tapError(() => Effect.logInfo(`what was stopped stays stopped: alasio restore-filer-store ${stamp} again picks up where this stopped`)),
        );
      })
    ),
).pipe(
  Command.withShortDescription("Put the object store's filer store back from a backup"),
  Command.withDescription(
    "Lists the bundled object store's filer store's backups, the newest first, which are taken hourly; or, given one, puts the filer store back as it was then. " +
      "The filer store keeps the store's namespace: a store whose filer store is lost has its data on its volumes with nothing to find it by, which " +
      "the gateways refuse to start on. " +
      `${ROLLS_BACK} ` +
      "It stops alasio, Neon and the lake (main's and branch environments'), but not the log nor the webhook intake, then the object store's gateways and filers, and the installation's CronJobs, " +
      "waiting for their running Jobs to end; sessions keep running, their workspace storage failing to write until the store is back. " +
      "It loads the backup onto the filer store's volume, checks the filer store holds as many keys as the backup, and starts everything again as it was: " +
      "its replicas, and its ScaledObjects and CronJobs paused or suspended only where they were before; and once the lakes run, each environment's lake's " +
      "reconciliation, as alasio lake reconcile runs it, saying what each lost. " +
      "Should it stop midway, what it stopped stays stopped, and running it again with the same backup picks up where it stopped. " +
      "Past --timeout a wait gives up, saying what it still waits for.",
  ),
); 
