/**
 * What alasio's commands share: the cluster the config targets, reached; its
 * components and their pods; and the flags several commands take.
 */
import { Readable, Writable } from "node:stream";

import type { KubernetesObject, V1Pod } from "@kubernetes/client-node";
import { Duration, Effect, Schema } from "effect";
import { Flag } from "effect/cli";

import { DockerCluster } from "../cluster/docker.ts";
import { HostCluster } from "../cluster/host.ts";
import { loadConfig } from "../config.ts";
import { type ContainerRef, kind, KubeApi, type KubeApiError, type KubeExecError } from "../kube/api.ts";
import { INSTALLATION_SELECTOR } from "../kube/apply.ts";
import { podProblems, selectorOf, type WaitOptions } from "../kube/rollout.ts";
import { NAMESPACE, RELEASE, selectorLabels } from "../manifests/common.ts";
import { dockerCluster, kubeApi, type ResolvedTarget, resolveTarget } from "../target.ts";

/** How often a wait looks. */
const POLL: Duration.Input = "2 seconds";

/** `text`, a duration as `30s`, `10m`, `2h` or `1d` say one. */
function parseDuration(text: string): Duration.Duration {
  const match = /^(\d+)(s|m|h|d)$/u.exec(text.trim());
  if (!match) throw new Error(`${text} is not a duration such as 30s, 10m, 2h or 1d`);
  const amount = Number(match[1]);
  return { s: Duration.seconds, m: Duration.minutes, h: Duration.hours, d: Duration.days }[match[2] as "s" | "m" | "h" | "d"](amount);
}

/** A flag of a duration, as parseDuration reads it. */
const durationFlag = (name: string) => Flag.String(name).pipe(Flag.mapTryCatch(parseDuration, (error) => (error instanceof Error ? error.message : String(error))));

/** --timeout: how long a command waits for alasio to run before it gives up, saying why. */
export const timeoutFlag = durationFlag("timeout").pipe(
  Flag.withDefault(Duration.minutes(15)),
  Flag.withMetavar("duration"),
  Flag.withDescription("How long to wait for alasio to run, such as 15m (the default) or 1h; past it, alasio says what it still waits for and why"),
);

/** --since: how far back logs go. */
export const sinceFlag = durationFlag("since").pipe(
  Flag.optional,
  Flag.withMetavar("duration"),
  Flag.withDescription("Only what was logged in the last duration, such as 10m or 2h"),
);

/** The waits of a command given `timeout`. */
export const waitOptions = (timeout: Duration.Duration): WaitOptions => ({ timeout, poll: POLL });

/** A cluster alasio makes on this machine is not running, so its API cannot be reached: the message says which, and whether it is made. */
export class ClusterNotRunning extends Schema.TaggedError<ClusterNotRunning>()("ClusterNotRunning", {
  message: Schema.String,
}) {}

/** `target`, a cluster alasio makes, is stopped, or not `made` at all. */
export const notRunning = (target: Exclude<ResolvedTarget, { readonly _tag: "Kubeconfig" }>, made: boolean): ClusterNotRunning =>
  new ClusterNotRunning({
    message: target._tag === "Host"
      ? made ? "k3s on this machine is stopped: alasio up starts it" : "there is no k3s on this machine yet: alasio up installs it"
      : made
      ? `the cluster ${target.cluster.name} is stopped: alasio up starts it`
      : `there is no cluster ${target.cluster.name} yet: alasio up makes it`,
  });

/** Whether the cluster `target` is, alasio made, is running: when not, whether it is made. */
export const running = (target: Exclude<ResolvedTarget, { readonly _tag: "Kubeconfig" }>) =>
  target._tag === "Host"
    ? Effect.provide(Effect.flatMap(HostCluster, (cluster) => cluster.status), HostCluster.layer(target.cluster)).pipe(
      Effect.map(({ unit }) => ({ running: unit.active === "active", made: unit.loaded })),
    )
    : Effect.provide(Effect.flatMap(DockerCluster, (cluster) => cluster.status), dockerCluster(target.cluster)).pipe(
      Effect.map(({ nodes }) => {
        const server = nodes.find(({ role }) => role === "server");
        return { running: server?.state === "running", made: server !== undefined };
      }),
    );

/**
 * The config, the cluster it targets, and its API to run `use` with; a cluster alasio
 * makes must be running, as only `up` starts it.
 */
export const onCluster = <A, E, R>(use: (target: ResolvedTarget) => Effect.Effect<A, E, R>) =>
  Effect.gen(function*() {
    const config = yield* loadConfig;
    const target = yield* resolveTarget(config);
    if (target._tag !== "Kubeconfig") {
      const { running: up, made } = yield* running(target);
      if (!up) return yield* notRunning(target, made);
    }
    return yield* Effect.provide(use(target), kubeApi(target));
  });

/** A component of alasio: one of its workloads in its namespace, named without the installation's prefix (`alasio` itself, `lake`, `neon-compute`). */
export interface Component {
  readonly name: string;
  readonly workload: KubernetesObject & { spec?: { selector?: { matchLabels?: Record<string, string> }; template?: { spec?: { containers?: { name: string }[] } } } };
}

/** alasio's components, its Deployments, StatefulSets and Jobs. */
export const components: Effect.Effect<Component[], KubeApiError, KubeApi> = Effect.gen(function*() {
  const kube = yield* KubeApi;
  const workloads = yield* Effect.forEach(
    [kind("Deployment"), kind("StatefulSet"), kind("Job")],
    (each) => kube.list<Component["workload"]>(each, { namespace: NAMESPACE, labelSelector: INSTALLATION_SELECTOR }),
  );
  return workloads.flat().map((workload) => {
    const name = workload.metadata?.name ?? "";
    return { name: name === RELEASE ? name : name.replace(`${RELEASE}-`, ""), workload };
  });
});

/** alasio has no component of a name. */
export class UnknownComponent extends Schema.TaggedError<UnknownComponent>()("UnknownComponent", {
  component: Schema.String,
  known: Schema.Array(Schema.String),
}) {
  override get message(): string {
    return this.known.length > 0
      ? `alasio has no component ${this.component}; it has ${this.known.join(", ")}`
      : "alasio is not installed in this cluster: alasio up installs it";
  }
}

/** The component of `name`. */
export const component = Effect.fnUntraced(function*(name: string): Effect.fn.Return<Component, KubeApiError | UnknownComponent, KubeApi> {
  const found = yield* components;
  const match = found.find((each) => each.name === name);
  if (!match) return yield* new UnknownComponent({ component: name, known: found.map((each) => each.name).sort() });
  return match;
});

/** The pods of `namespace` that `labels` select, the newest first. */
const podsSelected = (namespace: string, labels: Readonly<Record<string, string>>): Effect.Effect<readonly V1Pod[], KubeApiError, KubeApi> =>
  Effect.flatMap(KubeApi, (kube) => kube.list<V1Pod>(kind("Pod"), { namespace, labelSelector: selectorOf(labels) })).pipe(
    Effect.map((pods) => pods.toSorted((a, b) => new Date(b.metadata?.creationTimestamp ?? 0).getTime() - new Date(a.metadata?.creationTimestamp ?? 0).getTime())),
  );

/** The pods of `of`, the newest first. */
export const podsOf = (of: Component): Effect.Effect<readonly V1Pod[], KubeApiError, KubeApi> => podsSelected(NAMESPACE, of.workload.spec?.selector?.matchLabels ?? {});

/** No pod of a component runs ready, so nothing can be run in one. */
export class NoRunningPod extends Schema.TaggedError<NoRunningPod>()("NoRunningPod", {
  component: Schema.String,
  why: Schema.Array(Schema.String),
}) {
  override get message(): string {
    return `no pod of ${this.component} runs${this.why.length > 0 ? `: ${this.why.join("; ")}` : ""}; alasio status says more`;
  }
}

/** The newest of `pods`, of the component `name`, that runs ready. */
const readyPod = (name: string, pods: readonly V1Pod[]): Effect.Effect<string, NoRunningPod> => {
  const running = pods.find((pod) => pod.status?.phase === "Running" && podProblems(pod).length === 0);
  return running?.metadata?.name ? Effect.succeed(running.metadata.name) : Effect.fail(new NoRunningPod({ component: name, why: pods.flatMap(podProblems) }));
};

/** The newest pod of the component `name` that runs ready. */
export const runningPod = (name: string): Effect.Effect<string, KubeApiError | UnknownComponent | NoRunningPod, KubeApi> =>
  component(name).pipe(Effect.flatMap(podsOf), Effect.flatMap((pods) => readyPod(name, pods)));

/** The newest pod of `component` in `namespace`, a branch environment's, that runs ready. */
export const runningPodIn = (namespace: string, component: string): Effect.Effect<string, KubeApiError | NoRunningPod, KubeApi> =>
  podsSelected(namespace, selectorLabels(component)).pipe(Effect.flatMap((pods) => readyPod(`${component} of ${namespace}`, pods)));

/** What a command run in a container printed, and the code it exited with. */
export interface Ran {
  readonly exitCode: number;
  readonly stdout: string;
  readonly stderr: string;
}

/** Runs `command` in the container, given `stdin` when there is one: what it printed, and its exit code. */
export const runIn = (target: ContainerRef, command: readonly string[], stdin?: string): Effect.Effect<Ran, KubeApiError | KubeExecError, KubeApi> =>
  Effect.flatMap(KubeApi, (kube) => {
    const printed = { stdout: "", stderr: "" };
    const into = (stream: "stdout" | "stderr") =>
      new Writable({
        write: (chunk: Buffer, _encoding, done) => {
          printed[stream] += chunk.toString("utf8");
          done();
        },
      });
    return kube.exec(target, command, { stdin: stdin === undefined ? null : Readable.from([stdin]), stdout: into("stdout"), stderr: into("stderr"), tty: false }).pipe(
      Effect.map((exitCode) => ({ exitCode, ...printed })),
    );
  });

/** A command run in alasio's cluster exited with another code than 0. */
export class CommandFailed extends Schema.TaggedError<CommandFailed>()("CommandFailed", {
  command: Schema.String,
  exitCode: Schema.Number,
}) {
  override get message(): string {
    return `${this.command} exited with ${this.exitCode}`;
  }
}
