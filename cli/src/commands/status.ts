/** `alasio status`: what runs, and whether it is healthy; how it is scaled and sized, and the cluster's memory. */
import type { KubernetesObject, V1Node, V1Pod, V1Secret, V1StatefulSet } from "@kubernetes/client-node";
import { Console, Effect, Option, Redacted, Schema } from "effect";
import { Command } from "effect/cli";

import { DockerCluster } from "../cluster/docker.ts";
import { HostCluster } from "../cluster/host.ts";
import { readySince } from "../cluster/k3s.ts";
import { describeShortfall, InotifyLimitsTooLow, inotifyShortfalls, requireLocalMachine } from "../cluster/machine.ts";
import { SERVICE } from "../cluster/node.ts";
import { loadConfig } from "../config.ts";
import { describeMemory, describeScale, describeSizing, memoryBytes, type ScaledWorkload } from "../capacity.ts";
import { describeRef, hasStatus, type Kind, kind, KubeApi, type KubeApiError, refOf } from "../kube/api.ts";
import { INSTALLATION_SELECTOR, WORKLOADS } from "../kube/apply.ts";
import { diagnose, readiness, selectorOf } from "../kube/rollout.ts";
import { NAMESPACE, RELEASE } from "../manifests/common.ts";
import { MASTER, METRICS_PORT, VOLUME_SERVER } from "../manifests/object-store.ts";
import type { ScaledObject } from "../manifests/scalers.ts";
import type { VerticalPodAutoscaler } from "../manifests/sizing.ts";
import { describeReplication, describeVolumeServer, volumeServerDisk } from "../object-store.ts";
import { describeTarget, dockerCluster, kubeApi, resolveTarget } from "../target.ts";
import { branchNamespaces } from "../branches.ts";
import { installConfig } from "../install.ts";
import { readTelegramBot, TELEGRAM_SECRET, type TelegramBot } from "../secrets.ts";
import { describeBots, type EnvironmentBot } from "../webhook.ts";
import { branchNamespace } from "../../../src/branch/names.ts";
import { notRunning, runIn } from "./common.ts";

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

/** Telegram cannot deliver to an environment's bot, or failed to lately. */
export class DeliveriesFailing extends Schema.TaggedError<DeliveriesFailing>()("DeliveriesFailing", {}) {
  override get message(): string {
    return "Telegram does not deliver to the webhook intake as it should: alasio up registers each bot again";
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
      yield* Console.log(nodes.length > 0 ? nodes.map(({ name, state }) => `  ${name}: ${state}`).join("\n") : "  not made");
      yield* sayShortfalls;
      const server = nodes.find(({ role }) => role === "server");
      if (server?.state !== "running") return yield* notRunning(target, server !== undefined);
    }
    if (target._tag === "Host") {
      const { unit, installed } = yield* Effect.provide(Effect.flatMap(HostCluster, (cluster) => cluster.status), HostCluster.layer(target.cluster));
      yield* Console.log(`k3s${installed?.k3s ? ` ${installed.k3s}` : ""} on this machine${installed?.gvisor ? `, with gVisor ${installed.gvisor}` : ""}:`);
      yield* Console.log(unit.loaded ? `  service ${SERVICE}: ${unit.active}, ${unit.enabled ? "enabled" : "disabled"}` : "  not installed");
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
        let notReady = 0;
        for (const workload of workloads as readonly ScaledWorkload[]) {
          const state = readiness(workload);
          // A DaemonSet runs a pod on every node it schedules to, of no scale of its own.
          const scale = workload.kind === "DaemonSet" ? "" : `${describeScale(workload, of(scaledObjects, workload, ({ spec }) => spec.scaleTargetRef.name))}; `;
          const sizing = `    ${scale}${describeSizing(workload, of(sizings, workload, ({ spec }) => spec.targetRef.name))}`;
          if (state._tag === "Ready") {
            yield* Console.log(`  ${describeRef(refOf(workload))}: ready`);
            yield* Console.log(sizing);
            continue;
          }
          notReady += 1;
          yield* Console.log(`  ${describeRef(refOf(workload))}: ${state._tag === "Waiting" ? state.status : state.reason}`);
          yield* Console.log(sizing);
          for (const line of yield* diagnose(refOf(workload))) yield* Console.log(`    ${line}`);
        }
        if (listed.some(({ whole }) => !whole)) {
          yield* Console.log(`  alasio's workloads outside its namespace, ${NAMESPACE}: not seen, as this kubeconfig may list them there alone`);
        }
        for (const line of yield* objectStore(workloads)) yield* Console.log(line);
        const telegram = workloads.length > 0 ? yield* deliveries(config) : { lines: [], failing: false };
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
        if (telegram.failing) return yield* new DeliveriesFailing();
      }),
      kubeApi(target),
    );
    if (shortfalls.length > 0) return yield* new InotifyLimitsTooLow({ shortfalls });
  })).pipe(
    Command.withShortDescription("Say what runs, and whether it is healthy"),
    Command.withDescription(
      "Says where alasio runs (and, for the cluster on this machine, k3s's service and node, or its nodes in Docker, and this " +
        "machine's inotify limits when they are too low for it), its version, and each of its workloads: ready, or what it is at and " +
        "why; its replicas, and its scale where KEDA moves it; and its pods' requests, beside " +
        "what VPA recommends once it has measured them. Of the bundled object store, the copies it keeps of each object, and of each " +
        "volume server, as its metrics say: what it may still fill of its disk before it takes no more writes, what it keeps " +
        "free (minFreeSpace), its volumes, and what they take, deleted and not yet compacted away. Then the memory the cluster's pods request and use, of what its nodes have " +
        "for pods, and whether a metrics server measures them, which VPA and what scales on CPU need; of these, what the kubeconfig's " +
        "rights do not reach, it says is not seen. Exits with 1 when something " +
        "is not ready, or the limits are too low, or this machine is not Linux on x86-64, which the cluster here needs.",
    ),
  );
