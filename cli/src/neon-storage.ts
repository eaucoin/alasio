/**
 * What an installation may not do to Neon's storage: ask for fewer shards than its tenant
 * has, as Neon merges none, or for fewer pageservers than the storage controller has
 * registered, as alasio removes none. A pageserver stopped would stay registered, its
 * shards failed over and its secondaries lost for good, and one taken out of the storage
 * controller would need each of its shards drained first. Refused before anything is
 * applied, rather than once neon-control finds it, by what the storage controller says:
 * asked in its own pod, with the token it calls neon-control with, once its database says
 * it has registered a pageserver. Its API and its database are Neon's own, which every
 * release of alasio runs, so an upgrade is checked against the release it upgrades from as
 * well as against this one. As alasio is applied, the storage controller and its database,
 * where the installation has them, are waited for until they run first, as alasio may have
 * just restarted the cluster's k3s, its configuration changed, and their pods with it; and
 * each is asked again while its container does not answer yet (NOT_YET_ANSWERING_FOR).
 */
import type { V1PersistentVolumeClaim, V1Secret } from "@kubernetes/client-node";
import { Duration, Effect, Schedule, Schema } from "effect";

import type { DescribedShard, StorageNode } from "../../neon/control/branches.ts";
import type { StackSecrets } from "../../neon/control/secrets.ts";
import { runIn, runningPod } from "./commands/common.ts";
import { type ContainerRef, kind, KubeApi, type KubeApiError, type KubeExecError, type ObjectRef } from "./kube/api.ts";
import { awaitReady, type NotReadyInTime, type RolloutFailed, type WaitOptions } from "./kube/rollout.ts";
import { NAMESPACE, neonName } from "./manifests/common.ts";
import type { InstallConfig } from "./manifests/config.ts";

/** The configuration asks for fewer of Neon's shards than its tenant has: Neon merges none. */
export class ShardsLowered extends Schema.TaggedError<ShardsLowered>()("ShardsLowered", { configured: Schema.Number, current: Schema.Number }) {
  override get message(): string {
    return `neon.shards is ${this.configured}, but alasio's Neon has ${this.current} shards already, and Neon merges no shards: set it to ${this.current} or more`;
  }
}

/** The configuration asks for fewer of Neon's pageservers than are registered: alasio removes none. */
export class PageserversLowered extends Schema.TaggedError<PageserversLowered>()("PageserversLowered", { configured: Schema.Number, current: Schema.Number }) {
  override get message(): string {
    return `neon.pageserver.scaling is ${this.configured}, but alasio's Neon has ${this.current} pageservers registered, each holding shards or their secondaries, and alasio removes none: set it to ${this.current} or more`;
  }
}

/**
 * The storage controller, which has registered pageservers, did not say what Neon's storage
 * holds: a configuration is not checked against it, so nothing is applied, until it runs.
 */
export class StorageUnverified extends Schema.TaggedError<StorageUnverified>()("StorageUnverified", { reason: Schema.String }) {
  override get message(): string {
    return `alasio applies nothing it cannot check against Neon's storage, and its storage controller did not say what it holds (${this.reason}): ` +
      "once it runs, as alasio status says it does not, run alasio up again";
  }
}

/** The storage controller's container, as its Deployment runs it. */
const CONTAINER = "storage-controller";

/**
 * How long a container of Neon's storage that does not answer yet is asked again, at most, and
 * never past what alasio waits for: one its kubelet has not taken back since the cluster's
 * k3s started anew, which a status from before still says runs.
 */
export const NOT_YET_ANSWERING_FOR = Duration.minutes(2);

/**
 * Whether `error`, of an exec, is of its container's not answering yet, rather than of what it
 * ran: its pod not yet taken back by its kubelet, which answers 404, its container not found,
 * or its connection refused.
 */
const notYetAnswering = (error: KubeApiError | KubeExecError): boolean =>
  error._tag === "KubeApiError" && (error.status === 404 || /\b404\b|container not found|ECONNREFUSED|connection refused/iu.test(error.reason));

/**
 * `command` run in `target`, as runIn runs it, asked again every `wait.poll` while its
 * container does not answer yet, for NOT_YET_ANSWERING_FOR or `wait.timeout`, whichever is
 * shorter; asked once where nothing is waited for.
 */
const runAnswered = (target: ContainerRef, command: readonly string[], wait: WaitOptions | undefined) =>
  wait === undefined
    ? runIn(target, command)
    : runIn(target, command).pipe(
      Effect.retry({
        while: notYetAnswering,
        schedule: Schedule.max([Schedule.spaced(wait.poll), Schedule.during(Duration.min(NOT_YET_ANSWERING_FOR, Duration.fromInputUnsafe(wait.timeout)))]),
      }),
    );

/** Asks the storage controller for `path` of its API, in its pod: what it answered with, by its status. */
const asked = <Body>(pod: string, path: string, wait: WaitOptions | undefined): Effect.Effect<{ readonly status: number; readonly body: Body | null }, KubeApiError | StorageUnverified, KubeApi> =>
  runAnswered(
    { namespace: NAMESPACE, pod, container: CONTAINER },
    // The token is the container's own, which the shell's own printf gives curl on its
    // standard input, so it is on no command line; and a storage controller that does not
    // answer holds up for 10 s at most.
    ["sh", "-c", 'printf "authorization: Bearer %s\\n" "$CONTROL_PLANE_JWT_TOKEN" | curl -sS --max-time 10 -H @- -w "\\n%{http_code}" "http://127.0.0.1:1234$0"', path],
    wait,
  ).pipe(
    Effect.catch((error) => Effect.fail(new StorageUnverified({ reason: error.message }))),
    Effect.flatMap(({ exitCode, stdout, stderr }) => {
      if (exitCode !== 0) return Effect.fail(new StorageUnverified({ reason: `asking it for ${path} failed (exit ${exitCode}): ${stderr.trim() || stdout.trim()}` }));
      const lines = stdout.trimEnd().split("\n");
      const status = Number(lines.pop());
      const text = lines.join("\n");
      return Effect.try({ try: () => ({ status, body: text ? (JSON.parse(text) as Body) : null }), catch: () => new StorageUnverified({ reason: `it answered ${path} with no JSON` }) });
    }),
  );

/** What Neon's storage holds: its tenant's shards, none before it is made, and the pageservers registered. */
interface StorageHeld {
  readonly shards: number;
  readonly pageservers: number;
}

/** The storage controller's database's pod, and its claim, which holds what it registered. */
const DATABASE_POD = `${neonName("controller-db")}-0`;
const DATABASE_CLAIM = `data-${DATABASE_POD}`;

/**
 * The storage controller's database, which has held what it registered, did not answer: its
 * pod is to run before anything is applied, as alasio status and its pod's events say why
 * it does not.
 */
export class StorageDatabaseUnread extends Schema.TaggedError<StorageDatabaseUnread>()("StorageDatabaseUnread", { reason: Schema.String }) {
  override get message(): string {
    return `alasio applies nothing it cannot check against Neon's storage, and the storage controller's database, which holds what it registered, did not answer (${this.reason}): ` +
      `once its pod runs (kubectl -n ${NAMESPACE} describe pod ${DATABASE_POD} says why it does not), run alasio up again`;
  }
}

/** Asks the storage controller's database `sql`, as its own role, in its pod: what it printed. */
const askedDatabase = (sql: string, wait: WaitOptions | undefined): Effect.Effect<string, KubeApiError | StorageDatabaseUnread, KubeApi> =>
  runAnswered({ namespace: NAMESPACE, pod: DATABASE_POD, container: "postgres" }, ["psql", "-U", "storage_controller", "-d", "storage_controller", "-Atc", sql], wait).pipe(
    Effect.catch((error) => Effect.fail(new StorageDatabaseUnread({ reason: error.message }))),
    Effect.flatMap(({ exitCode, stdout, stderr }) =>
      exitCode === 0 ? Effect.succeed(stdout.trim()) : Effect.fail(new StorageDatabaseUnread({ reason: `psql exited with ${exitCode}: ${stderr.trim() || stdout.trim()}` }))
    ),
  );

/**
 * Whether the storage controller has registered a pageserver, as its own database says:
 * none where the database never held anything, its claim never bound, as before alasio first
 * runs, or where it holds no table of nodes, or no node in it; then there is nothing to check
 * a configuration against. One whose claim is bound is asked, and must answer.
 */
const registered = (wait: WaitOptions | undefined): Effect.Effect<boolean, KubeApiError | StorageDatabaseUnread, KubeApi> => Effect.gen(function*() {
  const kube = yield* KubeApi;
  const claim = yield* kube.get<V1PersistentVolumeClaim>({ ...kind("PersistentVolumeClaim"), namespace: NAMESPACE, name: DATABASE_CLAIM });
  if (claim?.status?.phase !== "Bound" && claim?.status?.phase !== "Lost") return false;
  if ((yield* askedDatabase("select to_regclass('nodes') is not null", wait)) !== "t") return false;
  return (yield* askedDatabase("select exists (select from nodes)", wait)) === "t";
});

/**
 * What Neon's storage holds, as the storage controller says it: null only where it has
 * registered no pageserver; StorageUnverified where it has but does not say, as a
 * configuration it would refuse must not slip through then.
 */
const storageHeld = (wait: WaitOptions | undefined): Effect.Effect<StorageHeld | null, KubeApiError | StorageDatabaseUnread | StorageUnverified, KubeApi> => Effect.gen(function*() {
  const kube = yield* KubeApi;
  if (!(yield* registered(wait))) return null;
  const pod = yield* runningPod("neon-storage-controller").pipe(Effect.catch((error) => Effect.fail(new StorageUnverified({ reason: error.message }))));
  const nodes = yield* asked<StorageNode[]>(pod, "/control/v1/node", wait);
  if (nodes.status !== 200 || !nodes.body) return yield* new StorageUnverified({ reason: `it answered its pageservers with ${nodes.status}` });
  const pageservers = nodes.body.length;
  // The tenant, which the stack's setup names before neon-control makes it.
  const encoded = (yield* kube.get<V1Secret>({ ...kind("Secret"), namespace: NAMESPACE, name: neonName("root") }))?.data?.["secrets.json"];
  const tenantId = encoded === undefined ? undefined : (yield* Effect.try({
    try: () => (JSON.parse(Buffer.from(encoded, "base64").toString("utf8")) as Partial<StackSecrets>).tenantId,
    catch: () => new StorageUnverified({ reason: `the Secret ${neonName("root")}'s secrets.json, which names Neon's tenant, is no JSON` }),
  }));
  if (!tenantId) return { shards: 0, pageservers };
  const tenant = yield* asked<{ readonly shards: readonly DescribedShard[] }>(pod, `/control/v1/tenant/${tenantId}`, wait);
  if (tenant.status === 404) return { shards: 0, pageservers };
  if (tenant.status !== 200 || !tenant.body) return yield* new StorageUnverified({ reason: `it answered Neon's tenant with ${tenant.status}` });
  return { shards: tenant.body.shards.length, pageservers };
});

/** The storage controller and its database, where the installation has them, waited for until they run, within `wait`. */
const running = (wait: WaitOptions): Effect.Effect<void, KubeApiError | NotReadyInTime | RolloutFailed, KubeApi> =>
  Effect.gen(function*() {
    const kube = yield* KubeApi;
    const refs: ObjectRef[] = [
      { ...kind("StatefulSet"), namespace: NAMESPACE, name: neonName("controller-db") },
      { ...kind("Deployment"), namespace: NAMESPACE, name: neonName("storage-controller") },
    ];
    const there = (yield* Effect.forEach(refs, (ref) => Effect.map(kube.get(ref), (object) => (object === null ? [] : [ref])))).flat();
    yield* awaitReady(there, wait);
  });

/**
 * Refuses `settings` where they ask for fewer of Neon's shards or pageservers than its storage
 * holds, as the module says; the storage controller and its database waited for first, within
 * `wait`, where it is given, as alasio is applied.
 */
export const checkNeonStorage = Effect.fnUntraced(function*(settings: InstallConfig, wait?: WaitOptions): Effect.fn.Return<void, KubeApiError | NotReadyInTime | RolloutFailed | PageserversLowered | ShardsLowered | StorageDatabaseUnread | StorageUnverified, KubeApi> {
  if (!settings.neon.enabled) return;
  if (wait) yield* running(wait);
  const held = yield* storageHeld(wait);
  if (!held) return;
  if (held.shards > settings.neon.shards) return yield* new ShardsLowered({ configured: settings.neon.shards, current: held.shards });
  const pageservers = settings.neon.pageserver.scaling.min;
  if (held.pageservers > pageservers) return yield* new PageserversLowered({ configured: pageservers, current: held.pageservers });
});
