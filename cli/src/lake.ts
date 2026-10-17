/**
 * The reconciliation of an environment's lake with the object store, run as its Job
 * (manifests/lake.ts lakeReconcile), which alasio lake reconcile and alasio
 * restore-filer-store run: what it said each table lost, as people read it.
 */
import type { V1Job, V1Pod } from "@kubernetes/client-node";
import { Effect, Option, Schema, Stream } from "effect";

import { kind, KubeApi, refOf } from "./kube/api.ts";
import { awaitGone, awaitReady, selectorOf, type WaitOptions } from "./kube/rollout.ts";
import type { Environment } from "./manifests/common.ts";
import type { InstallConfig } from "./manifests/config.ts";
import { LAKE_RECONCILE, lakeReconcile } from "./manifests/lake.ts";

/** What the reconciliation logs of a table it lost (neon/lake/src/maintain.ts). */
const LostTable = Schema.Struct({
  message: Schema.String,
  table: Schema.String,
  files: Schema.Number,
  rows: Schema.Number,
  from: Schema.NullOr(Schema.String),
  to: Schema.NullOr(Schema.String),
  recovered: Schema.NullOr(Schema.Literals(["database", "log", "main"])),
});

/** The reconciliation's Job said nothing it could be read by. */
export class ReconcileUnread extends Schema.TaggedError<ReconcileUnread>()("ReconcileUnread", {
  namespace: Schema.String,
}) {
  override get message(): string {
    return `the lake's reconciliation in ${this.namespace} left no pod whose output could be read`;
  }
}

/** A reconciliation of the environment's lake runs already, as its Job says, which another would not run beside. */
export class ReconcileRunning extends Schema.TaggedError<ReconcileRunning>()("ReconcileRunning", {
  namespace: Schema.String,
}) {
  override get message(): string {
    return `a reconciliation of the lake in ${this.namespace} runs already, as its Job ${LAKE_RECONCILE} does: once it ends (kubectl -n ${this.namespace} get job ${LAKE_RECONCILE}), run this again`;
  }
}

/** Whether `job` has ended, complete or failed. */
const ended = (job: V1Job): boolean => job.status?.conditions?.some(({ type, status }) => (type === "Complete" || type === "Failed") && status === "True") ?? false;

/** `count` of `noun`, its plural where it is not one. */
const counted = (count: number, noun: string) => `${count} ${noun}${count === 1 ? "" : "s"}`;

/** What the lake lost of a table, as people read it. */
const describeLost = ({ table, files, rows, from, to, recovered }: typeof LostTable.Type): string =>
  `the lake lost ${counted(rows, "row")} of ${table}, in ${counted(files, "file")}${from && to ? `, from ${from} to ${to}` : ""}: ` +
  RECOVERED[recovered ?? "nowhere"];

/** Where a table's lost rows come back from, as people read it (neon/lake/src/reconcile.ts's Recovery). */
const RECOVERED: Record<"database" | "log" | "main" | "nowhere", string> = {
  database: "loaded again from alasio's database",
  log: "read again from the log, where it still holds it",
  main: "main's telemetry as of the branch point, which main's lake reads again from the log",
  nowhere: "kept nowhere else",
};

const decodeLost = Schema.decodeUnknownOption(Schema.fromJsonString(LostTable));

/** What the reconciliation logs of telemetry the log no longer holds, from one time to another (neon/lake/src/maintain.ts). */
const LostForGood = Schema.Struct({
  message: Schema.Literal("lost from the object store, and from the log, which no longer holds it"),
  signal: Schema.String,
  from: Schema.String,
  to: Schema.String,
});

const decodeLostForGood = Schema.decodeUnknownOption(Schema.fromJsonString(LostForGood));

/** What the reconciliation logs of telemetry its intake has not read again yet, as it ends. */
const StillToRead = Schema.Struct({ message: Schema.Literal("lost from the object store, still to be read again from the log"), signals: Schema.Array(Schema.String) });

const decodeStillToRead = Schema.decodeUnknownOption(Schema.fromJsonString(StillToRead));

/** What the reconciliation said of `line`, as people read it: what a table lost, telemetry nothing refills, or telemetry still to be read again; nothing of another line. */
function describeLine(line: string): string[] {
  const lost = decodeLost(line);
  if (Option.isSome(lost) && lost.value.message.startsWith("lost from the object store")) return [describeLost(lost.value)];
  const gone = decodeLostForGood(line);
  if (Option.isSome(gone)) return [`the lake's ${gone.value.signal} telemetry from ${gone.value.from} to ${gone.value.to} cannot be refilled: the log no longer holds it`];
  const pending = decodeStillToRead(line);
  if (Option.isSome(pending)) {
    return [`the lake's ${pending.value.signals.join(" and ")} telemetry is still to be read again from the log: its intake had not taken it up as the reconciliation ended`];
  }
  return [];
}

/** What the reconciliation logs where it failed (neon/lake/src/maintain.ts). */
const PassFailed = Schema.Struct({ message: Schema.Literal("reconciling the catalog with the object store failed"), error: Schema.String });

const decodeFailed = Schema.decodeUnknownOption(Schema.fromJsonString(PassFailed));

/** The reconciliation failed, as it said why. */
export class ReconcileFailed extends Schema.TaggedError<ReconcileFailed>()("ReconcileFailed", {
  namespace: Schema.String,
  error: Schema.String,
}) {
  override get message(): string {
    return `the lake's reconciliation in ${this.namespace} failed: ${this.error}`;
  }
}

/**
 * Runs `environment`'s lake's reconciliation as its Job, afresh, until it completes, and
 * removes it: a line of what the lake lost for each table that lost any, none where none did,
 * and of the telemetry the log no longer holds, or has yet to give back (describeLine); where
 * it failed, why, as it said. Refused while a Job of it runs still, which it leaves to
 * end; one that ended is removed first.
 */
export const reconcileLake = Effect.fnUntraced(function*(config: InstallConfig, environment: Environment, options: WaitOptions) {
  const kube = yield* KubeApi;
  const job = lakeReconcile(config, environment);
  const ref = refOf(job);
  const pods = selectorOf({ "batch.kubernetes.io/job-name": LAKE_RECONCILE });
  const removed = Effect.gen(function*() {
    const left = (yield* kube.list<V1Pod>(kind("Pod"), { namespace: environment.namespace, labelSelector: pods })).map(refOf);
    yield* Effect.forEach([ref, ...left], (each) => kube.remove(each));
    yield* awaitGone([ref, ...left], options);
  });
  /** What the Job's last pod said, a line each; none where it left no pod. */
  const said = Effect.gen(function*() {
    const made = yield* kube.list<V1Pod>(kind("Pod"), { namespace: environment.namespace, labelSelector: pods });
    const pod = made.toSorted((a, b) => (a.metadata?.creationTimestamp?.valueOf() ?? 0) - (b.metadata?.creationTimestamp?.valueOf() ?? 0)).at(-1)?.metadata?.name;
    if (!pod) return null;
    return (yield* kube.logs(environment.namespace, pod, { container: "maintenance" }).pipe(Stream.decodeText(), Stream.mkString)).split("\n");
  });
  const running = yield* kube.get<V1Job>(ref);
  if (running && !ended(running)) return yield* new ReconcileRunning({ namespace: environment.namespace });
  yield* removed;
  yield* kube.apply(job);
  return yield* Effect.gen(function*() {
    yield* awaitReady([ref], options).pipe(
      Effect.catchTag("RolloutFailed", (failed) =>
        Effect.flatMap(said, (lines): Effect.Effect<never, ReconcileFailed | typeof failed> => {
          const why = (lines ?? []).map((line) => decodeFailed(line)).find(Option.isSome);
          return why ? Effect.fail(new ReconcileFailed({ namespace: environment.namespace, error: why.value.error })) : Effect.fail(failed);
        })
      ),
    );
    const lines = yield* said;
    if (!lines) return yield* new ReconcileUnread({ namespace: environment.namespace });
    return lines.flatMap(describeLine);
  }).pipe(Effect.ensuring(Effect.ignore(removed)));
});
