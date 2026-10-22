/** `alasio status`: what runs, and whether it is healthy; how it is scaled and sized, and the cluster's memory. */
import type { KubernetesObject, V1Deployment, V1Node, V1Pod, V1Secret, V1StatefulSet } from "@kubernetes/client-node";
import { Console, Effect, Option, Redacted, Schema } from "effect";
import { Command } from "effect/cli";

import { DockerCluster } from "../cluster/docker.ts";
import { HostCluster } from "../cluster/host.ts";
import { readySince } from "../cluster/k3s.ts";
import { describeShortfall, InotifyLimitsTooLow, inotifyShortfalls, requireLocalMachine } from "../cluster/machine.ts";
import { gvisorReleases, NODE_PINS, SERVICE, STARGZ_SERVICE } from "../cluster/node.ts";
import { loadConfig } from "../config.ts";
import { describeBaseline, recordedBaseline, standingOf, surveyCpu } from "../cpu-baseline.ts";
import { clusterReleases, describeKept, snapshotReleases } from "../kept-releases.ts";
import { describeMemory, describeScale, describeSizing, memoryBytes, memoryText, type ScaledWorkload } from "../capacity.ts";
import { describeRef, hasStatus, type Kind, kind, KubeApi, type KubeApiError, refOf } from "../kube/api.ts";
import { INSTALLATION_SELECTOR, WORKLOADS } from "../kube/apply.ts";
import { diagnose, READY_ONCE_BOUND_ANNOTATION, readinessOf, selectorOf, starting } from "../kube/rollout.ts";
import { componentName, NAMESPACE, neonName, RELEASE } from "../manifests/common.ts";
import { LOADER_COMPONENT } from "../manifests/lake.ts";
import { REPLICA_NAME } from "../manifests/neon.ts";
import { MASTER, METRICS_PORT, VOLUME_SERVER } from "../manifests/object-store.ts";
import { GATEWAY_COMPONENT, GATEWAY_PORT } from "../manifests/sandbox-gateway.ts";
import type { ScaledObject } from "../manifests/scalers.ts";
import type { VerticalPodAutoscaler } from "../manifests/sizing.ts";
import { describeReplication, describeVolumeServer, volumeServerDisk } from "../object-store.ts";
import { type ComputeTimes, computeTimes, type TenantView, tenantView } from "../branches.ts";
import { describeTarget, dockerCluster, kubeApi, resolveTarget } from "../target.ts";
import { branchNamespaces } from "../branches.ts";
import { describeFolders, folderInstances } from "../folders.ts";
import { installConfig } from "../install.ts";
import { readTelegramBot, TELEGRAM_SECRET, type TelegramBot } from "../secrets.ts";
import { describeBots, type EnvironmentBot } from "../webhook.ts";
import { branchNamespace } from "../../../src/branch/names.ts";
import { ALASIO_GATEWAY_FOLDERS_VARIABLE } from "../../../src/gateway/names.ts";
import { notRunning, runIn, runningPodIn } from "./common.ts";

/** Some of alasio's workloads do not run as they should. */
export class Unhealthy extends Schema.TaggedError<Unhealthy>()("Unhealthy", {
  notReady: Schema.Number,
  of: Schema.Number,
}) {
  override get message(): string {
    return this.of === 0
      ? "alasio is not installed in this cluster: alasio up installs it"
      : `${this.notReady} of alasio's ${this.of} workloads are not ready`;
  }
}

/** Telegram cannot deliver to an environment's bot, or its deliveries fail: their fixes, each once. */
export class DeliveriesFailing extends Schema.TaggedError<DeliveriesFailing>()("DeliveriesFailing", {
  fixes: Schema.Array(Schema.String),
}) {
  override get message(): string {
    return `Telegram does not deliver to the webhook intake as it should: ${this.fixes.join("; ")}`;
  }
}

/** What `look` finds, or none where this kubeconfig's rights do not reach it. */
const seen = <A>(look: Effect.Effect<A, KubeApiError, KubeApi>): Effect.Effect<Option.Option<A>, KubeApiError, KubeApi> =>
  look.pipe(Effect.map(Option.some), Effect.catchIf(hasStatus(403), () => Effect.succeedNone));

/**
 * The installation's objects of `kindOf`, in every namespace, or in alasio's alone where
 * this kubeconfig may list no more (`whole` false); none where the cluster lacks their CRD.
 */
const installedOf = <T extends KubernetesObject>(kindOf: Kind): Effect.Effect<{ readonly objects: readonly T[]; readonly whole: boolean }, KubeApiError, KubeApi> =>
  Effect.gen(function*() {
    const kube = yield* KubeApi;
    const everywhere = yield* seen(kube.list<T>(kindOf, { labelSelector: INSTALLATION_SELECTOR }));
    if (Option.isSome(everywhere)) return { objects: everywhere.value, whole: true };
    return { objects: yield* kube.list<T>(kindOf, { namespace: NAMESPACE, labelSelector: INSTALLATION_SELECTOR }), whole: false };
  }).pipe(Effect.catchIf(hasStatus(404), () => Effect.succeed({ objects: [], whole: true })));

/** The object of `objects` in `workload`'s namespace that names it `by`. */
const of = <T extends KubernetesObject>(objects: readonly T[], workload: KubernetesObject, by: (object: T) => string | undefined): T | undefined =>
  objects.find((object) => object.metadata?.namespace === workload.metadata?.namespace && by(object) === workload.metadata?.name);

/** Where the API serves an aggregated API, as its APIService says: Available. */
interface APIService extends KubernetesObject {
  readonly status?: { readonly conditions?: readonly { readonly type: string; readonly status: string }[] };
}

/** A node's use of memory now, as the metrics server measures it. */
interface NodeMetrics extends KubernetesObject {
  readonly usage?: { readonly memory?: string };
}

/**
 * Whether the cluster's metrics server serves, as its APIService says, null where this
 * kubeconfig may not read it; and the cluster's memory: what its pods that run or are to
 * request, and use now where the metrics server measures it, of what the nodes have for
 * pods, null where this kubeconfig may not list them.
 */
const memory = Effect.gen(function*() {
  const kube = yield* KubeApi;
  const metrics = yield* seen(kube.get<APIService>({ ...kind("APIService"), name: "v1beta1.metrics.k8s.io" }));
  const measures = Option.match(metrics, {
    onNone: () => null,
    onSome: (service) => service?.status?.conditions?.some(({ type, status }) => type === "Available" && status === "True") ?? false,
  });
  const nodes = Option.getOrNull(yield* seen(kube.list<V1Node>(kind("Node"))));
  const pods = Option.getOrNull(yield* seen(kube.list<V1Pod>(kind("Pod"))));
  const usage = measures ? Option.getOrNull(yield* seen(kube.list<NodeMetrics>(kind("NodeMetrics")))) : null;
  const running = pods?.filter(({ status }) => status?.phase !== "Succeeded" && status?.phase !== "Failed");
  return {
    measures,
    memory: nodes && running
      ? {
        requested: running.flatMap(({ spec }) => spec?.containers ?? []).reduce((sum, { resources }) => sum + memoryBytes(resources?.requests?.["memory"] ?? "0"), 0),
        used: usage ? usage.reduce((sum, node) => sum + memoryBytes(node.usage?.memory ?? "0"), 0) : null,
        allocatable: nodes.reduce((sum, node) => sum + memoryBytes(node.status?.allocatable?.["memory"] ?? "0"), 0),
      }
      : null,
  };
});

/** The StatefulSet of `workloads` named `name`, in alasio's namespace. */
const statefulSet = (workloads: readonly KubernetesObject[], name: string): V1StatefulSet | undefined =>
  workloads.find(({ kind: of, metadata }) => of === "StatefulSet" && metadata?.namespace === NAMESPACE && metadata.name === name);

/**
 * What the bundled object store keeps, where it runs among `workloads`: the copies of
 * each object, as its masters are told; and of each volume server, its disk, as its
 * metrics say it, read in its pod; or why it is not measured.
 */
const objectStore = (workloads: readonly KubernetesObject[]): Effect.Effect<string[], KubeApiError, KubeApi> =>
  Effect.gen(function*() {
    const master = statefulSet(workloads, MASTER);
    const volumeServers = statefulSet(workloads, VOLUME_SERVER);
    if (!master || !volumeServers) return [];
    const kube = yield* KubeApi;
    const replication = master.spec?.template.spec?.containers[0]?.args?.find((arg) => arg.startsWith("-defaultReplication="))?.split("=")[1] ?? "000";
    const listed = yield* seen(kube.list<V1Pod>(kind("Pod"), { namespace: NAMESPACE, labelSelector: selectorOf(volumeServers.spec?.selector.matchLabels ?? {}) }));
    if (Option.isNone(listed)) return [describeReplication(replication), "  its volume servers: not seen, as this kubeconfig may not list their pods"];
    const disks = yield* Effect.forEach(listed.value.toSorted((a, b) => (a.metadata?.name ?? "").localeCompare(b.metadata?.name ?? "")), (pod) => {
      const name = pod.metadata?.name ?? "";
      if (pod.status?.phase !== "Running") return Effect.succeed(`${name}: not measured, as it does not run`);
      return runIn({ namespace: NAMESPACE, pod: name, container: "volume" }, ["wget", "-q", "-O", "-", `http://127.0.0.1:${METRICS_PORT}/metrics`]).pipe(
        Effect.map(({ exitCode, stdout }) => {
          const disk = exitCode === 0 ? volumeServerDisk(stdout) : null;
          return disk ? describeVolumeServer(name, disk) : `${name}: not measured, as its metrics do not say its disk`;
        }),
        Effect.catchIf((error) => error._tag === "KubeApiError" && hasStatus(403)(error), () => Effect.succeed(`${name}: not measured, as this kubeconfig may not run commands in its pods`)),
        Effect.catch((error) => Effect.succeed(`${name}: not measured: ${error.message}`)),
      );
    });
    return [describeReplication(replication), ...disks.map((line) => `  ${line}`)];
  });

/** Whether `workload` is a sandbox pool's, ready once its pods run, each ready only once bound. */
const isPool = (workload: KubernetesObject): boolean => workload.metadata?.annotations?.[READY_ONCE_BOUND_ANNOTATION] === "true";

/** A pool's counts, as the sandbox gateway says them (src/binder/server.ts `/counts`). */
const PoolCounts = Schema.fromJsonString(Schema.Struct({ free: Schema.Number, bound: Schema.Number, waiting: Schema.Number }));

/**
 * What each sandbox pool of `workloads` holds, as the gateway counts it, asked in its pod: its
 * free pods running (warm), its bindings, and the binds waiting for a free pod in every replica
 * of the gateway; by the pool's `namespace/name`, or why it is not seen, the same for every pool.
 */
type Counted = { readonly counted: string } | { readonly unseen: string };
const poolCounts = (workloads: readonly KubernetesObject[]): Effect.Effect<ReadonlyMap<string, Counted>, KubeApiError, KubeApi> =>
  Effect.gen(function*() {
    const pools = workloads.filter(isPool);
    if (pools.length === 0) return new Map();
    const unseen = (why: string) => new Map<string, Counted>(pools.map(({ metadata }) => [`${metadata?.namespace}/${metadata?.name}`, { unseen: why }]));
    const gateway = yield* runningPodIn(NAMESPACE, GATEWAY_COMPONENT).pipe(Effect.option);
    if (Option.isNone(gateway)) return unseen("the sandbox gateway does not run");
    const said = yield* Effect.forEach(pools, ({ metadata }) => {
      const pool = `${metadata?.namespace}/${metadata?.name}`;
      return runIn({ namespace: NAMESPACE, pod: gateway.value, container: GATEWAY_COMPONENT }, ["curl", "-sS", "--max-time", "5", `http://127.0.0.1:${GATEWAY_PORT}/counts/${pool}`]).pipe(
        Effect.map(({ exitCode, stdout, stderr }) => {
          const counted = exitCode === 0 ? Schema.decodeUnknownOption(PoolCounts)(stdout) : Option.none();
          return [pool, Option.match(counted, {
            onNone: (): Counted => ({ unseen: `the sandbox gateway did not count them: ${(stderr || stdout).trim() || `exit ${exitCode}`}` }),
            onSome: ({ free, bound, waiting }): Counted => ({ counted: `${free} warm, ${bound} bound, ${waiting} waiting` }),
          })] as const;
        }),
      );
    }).pipe(
      Effect.map((each) => new Map<string, Counted>(each)),
      Effect.catchIf((error) => error._tag === "KubeApiError" && hasStatus(403)(error), () => Effect.succeed(unseen("this kubeconfig may not run commands in the sandbox gateway's pods"))),
      Effect.catch((error) => Effect.succeed(unseen(`asking the sandbox gateway failed: ${error.message}`))),
    );
    return said;
  });

/**
 * A compute that is ready, a primary or a read replica: asleep at no replica, as
 * neon-control puts one no client has used for a while, until the next connection wakes
 * it, or awake; when it was last woken, as neon-control recorded it; and a read
 * replica's lag behind its primary, as neon-control last read it.
 */
function describeCompute(workload: ScaledWorkload, computes: ReadonlyMap<string, ComputeTimes>): string {
  const compute = computes.get(`${workload.metadata?.namespace ?? ""}/${workload.metadata?.name ?? ""}`);
  const lag = compute?.lagBytes;
  return [
    workload.spec?.replicas === 0 ? "asleep" : "awake",
    ...(compute?.wokenAt ? [`last woken ${compute.wokenAt}`] : []),
    ...(lag === undefined || lag === null ? [] : [`${memoryText(lag)} of WAL behind its primary`]),
  ].join(", ");
}

/** Neon's tenant: its shards, and each pageserver, with how many of them it holds. */
function describeTenant({ shards, pageservers }: TenantView): string[] {
  return [
    `Neon: ${shards} shard${shards === 1 ? "" : "s"} on ${pageservers.length} pageserver${pageservers.length === 1 ? "" : "s"}`,
    ...pageservers.map(({ id, address, availability, shards: held }) => `  pageserver ${id} at ${address}: ${availability}, ${held} shard${held === 1 ? "" : "s"}`),
  ];
}

/** KEDA's annotations pausing a ScaledObject: at its replicas now, or at a count of them. */
const PAUSED = "autoscaling.keda.sh/paused";
const PAUSED_REPLICAS = "autoscaling.keda.sh/paused-replicas";

/**
 * Why KEDA would not wake `scaler`'s workload from no replica, if it would not: its
 * ScaledObject gone, paused (its annotations, or its Paused condition), or not ready; null
 * where it would.
 */
function unwakeable(scaler: ScaledObject | undefined): string | null {
  if (!scaler) return "with no ScaledObject to wake it";
  const annotations = scaler.metadata?.annotations ?? {};
  const condition = (type: string) => scaler.status?.conditions?.find((each) => each.type === type);
  if (annotations[PAUSED] === "true" || annotations[PAUSED_REPLICAS] !== undefined || condition("Paused")?.status === "True") return "its ScaledObject paused, which wakes it no more";
  const ready = condition("Ready");
  if (ready && ready.status !== "True") return `its ScaledObject not ready (${ready.message || ready.reason || ready.status}), which wakes it no more`;
  return null;
}

/**
 * A workload that is ready: a compute, a read replica among them, or the lake's loader,
 * each asleep at no replica until it is woken, a compute by a connection and the loader
 * by alasio's writing, or awake; any other, at no replica too, ready, as KEDA leaves one.
 * The loader asleep is ready only while its ScaledObject would wake it (unwakeable): what
 * it says, and whether it is ready.
 */
function describeReady(workload: ScaledWorkload, computes: ReadonlyMap<string, ComputeTimes>, scaler: ScaledObject | undefined, pools: ReadonlyMap<string, Counted>): { said: string; ready: boolean } {
  // A sandbox pool's, ready as its pods run, each ready only once bound: what it holds, as the gateway counts it.
  if (isPool(workload)) {
    const counted = pools.get(`${workload.metadata?.namespace}/${workload.metadata?.name}`);
    const replicas = `${workload.spec?.replicas ?? 1} warm`;
    return { said: `pool, ${counted && "counted" in counted ? counted.counted : `${replicas}${counted ? `, its bindings not seen, as ${counted.unseen}` : ""}`}`, ready: true };
  }
  switch (workload.metadata?.name) {
    case neonName("compute"):
    case REPLICA_NAME:
      return { said: describeCompute(workload, computes), ready: true };
    case componentName(LOADER_COMPONENT): {
      if (workload.spec?.replicas !== 0) return { said: "awake", ready: true };
      const why = unwakeable(scaler);
      return why === null ? { said: "asleep, nothing left to load", ready: true } : { said: `at no replica, ${why}`, ready: false };
    }
    default:
      return { said: "ready", ready: true };
  }
}

/** How long Kubernetes gives a Deployment's pods to come up by default, of its progressDeadlineSeconds. */
const PROGRESS_DEADLINE_SECONDS = 600;

/**
 * Whether `workload`, one that wakes on demand (describeReady's: a compute, a read replica
 * among them, or the lake's loader) or that KEDA scales (`scaler`), as the lake's intake
 * woken from none or Grafana scaled out, is waking: each of its pods not ready starting
 * (../kube/rollout.ts), within its Deployment's progress deadline, or, where `scaler` woke
 * it, its pods not all made yet while KEDA still finds it work, as it last did within that
 * deadline, or its pods ready that the Deployment does not count yet, each come up within
 * that deadline. KEDA renews its lastActiveTime at each poll it finds work at, and records
 * no time it woke the loader, so a pod never made is bounded by the ReplicaSet's saying it
 * cannot make it (ReplicaFailure), not by time. One whose pod crashed, whose pods cannot be
 * made, or that has not come up within the deadline, is not ready.
 */
const waking = Effect.fnUntraced(function*(workload: ScaledWorkload, scaler: ScaledObject | undefined): Effect.fn.Return<boolean, KubeApiError, KubeApi> {
  if (scaler === undefined && ![neonName("compute"), REPLICA_NAME, componentName(LOADER_COMPONENT)].includes(workload.metadata?.name ?? "")) return false;
  const { metadata, spec, status } = workload as V1Deployment;
  // Past its progress deadline, or its pods refused, as the Deployment says: stuck, not
  // waking.
  const said = (type: string, is: string) => status?.conditions?.some((condition) => condition.type === type && condition.status === is) ?? false;
  if (said("Progressing", "False") || said("ReplicaFailure", "True")) return false;
  const pods = yield* (yield* KubeApi).list<V1Pod>(kind("Pod"), { namespace: metadata?.namespace, labelSelector: selectorOf(spec?.selector.matchLabels ?? {}) });
  const readyAt = (pod: V1Pod) => pod.status?.conditions?.find(({ type, status: ready }) => type === "Ready" && ready === "True")?.lastTransitionTime;
  const unready = pods.filter((pod) => readyAt(pod) === undefined);
  const now = Date.now();
  const deadlineMs = (spec?.progressDeadlineSeconds ?? PROGRESS_DEADLINE_SECONDS) * 1000;
  // Its pods not all made yet, as just after KEDA woke it, while KEDA still finds it work.
  const woken = scaler?.status?.lastActiveTime;
  const unmade = pods.length < (spec?.replicas ?? 0) && woken !== undefined && now - Date.parse(woken) < deadlineMs;
  // Its pods ready that the Deployment does not count available, each as one just come up,
  // within the deadline: ready since the Deployment was read, which status reads before
  // its pods, or before its controller counted them.
  const uncounted = pods.length - unready.length - (status?.availableReplicas ?? 0);
  const cameUp = pods.filter((pod) => {
    const at = readyAt(pod);
    return at !== undefined && now - new Date(at).getTime() < deadlineMs;
  }).length;
  const comingUp = uncounted > 0 && cameUp >= uncounted;
  return (unready.length > 0 || unmade || comingUp) && unready.every((pod) => starting(pod, deadlineMs, now));
});

/** How main's bot's and each branch environment's deliveries fare, as Telegram says. */
const deliveries = (config: Parameters<typeof installConfig>[0]) =>
  Effect.gen(function*() {
    const kube = yield* KubeApi;
    const main = yield* readTelegramBot;
    const branches = yield* branchNamespaces.pipe(Effect.catchIf(hasStatus(403), () => Effect.succeed([])));
    const branchBots = yield* Effect.forEach(branches, (name) =>
      kube.get<V1Secret>({ ...kind("Secret"), namespace: branchNamespace(name), name: TELEGRAM_SECRET }).pipe(
        Effect.map((found): EnvironmentBot[] => {
          const token = found?.data?.["token"];
          const bot: TelegramBot = { token: Redacted.make(token ? Buffer.from(token, "base64").toString("utf8") : ""), allowedUserIds: [], username: "" };
          return token ? [{ environment: name, bot }] : [];
        }),
        Effect.catchIf(hasStatus(403), () => Effect.succeed([])),
      ));
    return yield* describeBots(yield* installConfig(config), [...(main ? [{ environment: "main", bot: main }] : []), ...branchBots.flat()]);
  });

/** Whether `scaledObject` scales on what the metrics server measures: its pods' CPU. */
const onResources = (scaledObject: ScaledObject): boolean => scaledObject.spec.triggers.some(({ type }) => type === "cpu");

export const status = Command.make("status", {}, () =>
  Effect.gen(function*() {
    const config = yield* loadConfig;
    const target = yield* resolveTarget(config);
    // The cluster here runs only on a machine of its kind, whose limits are read from Linux's /proc.
    if (target._tag !== "Kubeconfig") yield* requireLocalMachine;
    const shortfalls = target._tag !== "Kubeconfig" ? yield* inotifyShortfalls : [];
    const sayShortfalls = Effect.forEach(shortfalls, (shortfall) => Console.log(`  this machine's ${describeShortfall(shortfall)}`), { discard: true });
    if (target._tag === "Docker") {
      const { docker, nodes } = yield* Effect.provide(Effect.flatMap(DockerCluster, (cluster) => cluster.status), dockerCluster(target.cluster));
      yield* Console.log(`cluster ${target.cluster.name}, in Docker ${docker}:`);
      yield* Console.log(nodes.length > 0 ? nodes.map(({ name, state, snapshotter }) =>
        `  ${name}: ${state}${snapshotter === null ? "" : `\n    stargz-snapshotter's daemon, which k3s pulls images through: ${snapshotter ? "running" : "not running"}`}`
      ).join("\n") : "  not made");
      yield* sayShortfalls;
      const server = nodes.find(({ role }) => role === "server");
      if (server?.state !== "running") return yield* notRunning(target, server !== undefined);
    }
    if (target._tag === "Host") {
      const { unit, snapshotter, installed } = yield* Effect.provide(Effect.flatMap(HostCluster, (cluster) => cluster.status), HostCluster.layer(target.cluster));
      const beside = [...(installed?.gvisor?.length ? [`gVisor ${installed.gvisor.join(" and ")}`] : []), ...(installed?.stargz ? [`stargz-snapshotter ${installed.stargz}`] : [])];
      yield* Console.log(`k3s${installed?.k3s ? ` ${installed.k3s}` : ""} on this machine${beside.length > 0 ? `, with ${beside.join(", ")}` : ""}:`);
      yield* Console.log(unit.loaded ? `  service ${SERVICE}: ${unit.active}, ${unit.enabled ? "enabled" : "disabled"}` : "  not installed");
      // Started by k3s's service, and stopped with it: whether it runs, not whether it is enabled.
      if (snapshotter.loaded) yield* Console.log(`  service ${STARGZ_SERVICE}, which k3s pulls images through: ${snapshotter.active}`);
      if (unit.active === "active") {
        const nodes = yield* Effect.provide(Effect.flatMap(KubeApi, (kube) => kube.list<V1Node>(kind("Node"))), kubeApi(target));
        for (const node of nodes) yield* Console.log(`  node ${node.metadata?.name}: ${readySince(node, 0) ? "ready" : "not ready"}`);
      }
      yield* sayShortfalls;
      if (unit.active !== "active") return yield* notRunning(target, unit.loaded);
    }
    yield* Effect.provide(
      Effect.gen(function*() {
        const kube = yield* KubeApi;
        const listed = yield* Effect.forEach(WORKLOADS, (name) => installedOf(kind(name)));
        const workloads = listed.flatMap(({ objects }) => objects).sort((a, b) => (a.metadata?.name ?? "").localeCompare(b.metadata?.name ?? ""));
        const version = workloads.find((object) => object.kind === "Deployment" && object.metadata?.name === RELEASE)?.metadata?.labels?.["app.kubernetes.io/version"];
        const scaledObjects = (yield* installedOf<ScaledObject>(kind("ScaledObject"))).objects;
        const sizings = (yield* installedOf<VerticalPodAutoscaler>(kind("VerticalPodAutoscaler"))).objects;
        yield* Console.log(`alasio${version ? ` ${version}` : ""}, in ${describeTarget(target)} (${kube.server}):`);
        const computes = yield* computeTimes;
        const pools = yield* poolCounts(workloads);
        let notReady = 0;
        for (const workload of workloads as readonly ScaledWorkload[]) {
          const state = yield* readinessOf(workload);
          // A DaemonSet runs a pod on every node it schedules to, of no scale of its own.
          const scaler = of(scaledObjects, workload, ({ spec }) => spec.scaleTargetRef.name);
          const scale = workload.kind === "DaemonSet" ? "" : `${describeScale(workload, scaler)}; `;
          const sizing = `    ${scale}${describeSizing(workload, of(sizings, workload, ({ spec }) => spec.targetRef.name))}`;
          if (state._tag === "Ready") {
            const { said, ready } = describeReady(workload, computes, scaler, pools);
            if (!ready) notReady += 1;
            yield* Console.log(`  ${describeRef(refOf(workload))}: ${said}`);
            yield* Console.log(sizing);
            continue;
          }
          if (state._tag === "Waiting" && (yield* waking(workload, scaler))) {
            yield* Console.log(`  ${describeRef(refOf(workload))}: waking, ${state.status}`);
            yield* Console.log(sizing);
            continue;
          }
          notReady += 1;
          yield* Console.log(`  ${describeRef(refOf(workload))}: ${state._tag === "Waiting" ? state.status : state.reason}`);
          yield* Console.log(sizing);
          for (const line of yield* diagnose(refOf(workload))) yield* Console.log(`    ${line}`);
        }
        // Where the node agents run, the CPU baseline sessions are held to, and the nodes that fall short of it.
        if (workloads.some((workload) => workload.kind === "DaemonSet" && workload.metadata?.name === componentName("node-agent"))) {
          const standing = yield* seen(Effect.all([recordedBaseline, surveyCpu]));
          const lines = Option.match(standing, {
            onNone: () => ["the CPU baseline: not seen, as this kubeconfig may not read alasio's templates and node agents"],
            onSome: ([{ baseline }, survey]) => describeBaseline(standingOf(survey, baseline)),
          });
          for (const line of lines) yield* Console.log(`  ${line}`);
          // The gVisor releases kept, and the snapshots naming each.
          const releases = describeKept(yield* clusterReleases(gvisorReleases(NODE_PINS)), yield* snapshotReleases);
          for (const line of releases) yield* Console.log(`  ${line}`);
        }
        // Where the sandbox gateway serves folder instances, as alasio is told it does: each, and its binds' last failure.
        const alasio = workloads.find((object) => object.kind === "Deployment" && object.metadata?.name === RELEASE) as V1Deployment | undefined;
        if (alasio?.spec?.template.spec?.containers.some(({ env }) => env?.some(({ name, value }) => name === ALASIO_GATEWAY_FOLDERS_VARIABLE && value === "1"))) {
          for (const line of describeFolders(yield* folderInstances, Date.now())) yield* Console.log(`  ${line}`);
        }
        const tenant = yield* tenantView;
        if (tenant) for (const line of describeTenant(tenant)) yield* Console.log(line);
        if (listed.some(({ whole }) => !whole)) {
          yield* Console.log(`  alasio's workloads outside its namespace, ${NAMESPACE}: not seen, as this kubeconfig may list them there alone`);
        }
        for (const line of yield* objectStore(workloads)) yield* Console.log(line);
        const telegram = workloads.length > 0 ? yield* deliveries(config) : { lines: [], fixes: [] };
        for (const line of telegram.lines) yield* Console.log(line);
        const cluster = yield* memory;
        yield* Console.log(cluster.memory ? describeMemory(cluster.memory) : "memory: not seen, as this kubeconfig may not list the cluster's nodes and pods");
        if (cluster.measures === null) yield* Console.log("whether the cluster has a metrics server: not seen, as this kubeconfig may not read its APIServices");
        if (cluster.measures === false) {
          const scaled = scaledObjects.filter(onResources).map(({ metadata }) => metadata?.name);
          yield* Console.log(
            `the cluster has no metrics server: VPA recommends nothing${scaled.length > 0 ? `, and what scales on its CPU (${scaled.join(", ")}) does not scale` : ""}`,
          );
        }
        if (workloads.length === 0 || notReady > 0) return yield* new Unhealthy({ notReady, of: workloads.length });
        if (telegram.fixes.length > 0) return yield* new DeliveriesFailing({ fixes: telegram.fixes });
      }),
      kubeApi(target),
    );
    if (shortfalls.length > 0) return yield* new InotifyLimitsTooLow({ shortfalls });
  })).pipe(
    Command.withShortDescription("Say what runs, and whether it is healthy"),
    Command.withDescription(
      "Says where alasio runs (and, for the cluster on this machine, k3s's service and node, with the stargz-snapshotter release " +
        "k3s pulls images through and whether its daemon's service runs, as no image is pulled while it does not; or its nodes in Docker, " +
        "each with whether that daemon runs in it; and this " +
        "machine's inotify limits when they are too low for it), its version, and each of its workloads: ready (a compute awake, or " +
        "asleep, as one no client has used for a while is until the next connection wakes it, and when it was last woken; the lake's loader " +
        "awake, or asleep with nothing left to load until alasio writes again; a sandbox pool with its warm pods, its bindings and the binds " +
        "waiting for a pod, as the sandbox gateway counts them; a bound pod is its pool's no more, so a node that holds one is drained only by " +
        "`kubectl drain --force`, which refuses it at once without --force, and whose eviction of it the bound pods' disruption budget refuses " +
        "while it is bound: the drain waits until the gateway releases the session, idle, into its snapshot, which its next bind restores on " +
        "another node; a lock a workspace's file held on a node that died, through that node's mount, holds for up to about seven minutes, " +
        "until workspace storage cleans the sessions of its clients that died), waking (a compute or the loader whose pod starts, within " +
        "its Deployment's progress deadline), or what it is at and " +
        "why; its replicas, and its scale where KEDA moves it; and its pods' requests, beside " +
        "what VPA recommends once it has measured them; the CPU baseline sessions are held to, and the nodes that lack it, and the gVisor releases kept, with the snapshots naming each; where the sandbox gateway serves folder conversations' instances, each, " +
        "bound or released, its pod, how long it has been bound, its bayma's last exec, and its binds' last failure, and whether three in a row hold it until asked. Of the bundled object store, the copies it keeps of each object, and of each " +
        "volume server, as its metrics say: what it may still fill of its disk before it takes no more writes, what it keeps " +
        "free (minFreeSpace), its volumes, and what they take, deleted and not yet compacted away. Then the memory the cluster's pods request and use, of what its nodes have " +
        "for pods, and whether a metrics server measures them, which VPA and what scales on CPU need; of these, what the kubeconfig's " +
        "rights do not reach, it says is not seen. Exits with 1 when something " +
        "is not ready, or the limits are too low, or this machine is not Linux on x86-64, which the cluster here needs.",
    ),
  );
