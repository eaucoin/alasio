/**
 * What alasio's commands share: the cluster the config targets, reached; its
 * components and their pods; and the flags several commands take.
 */
import type { KubernetesObject, V1Pod } from "@kubernetes/client-node";
import { Duration, Effect, Schema } from "effect";
import { Flag } from "effect/cli";

import { LocalCluster } from "../cluster/local.ts";
import { loadConfig } from "../config.ts";
import { kind, KubeApi, type KubeApiError } from "../kube/api.ts";
import { INSTALLATION_SELECTOR } from "../kube/apply.ts";
import { podProblems, selectorOf, type WaitOptions } from "../kube/rollout.ts";
import { NAMESPACE, RELEASE } from "../manifests/common.ts";
import { kubeApi, localCluster, type ResolvedTarget, resolveTarget } from "../target.ts";

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

/** The local cluster is not running, so its API cannot be reached. */
export class ClusterNotRunning extends Schema.TaggedError<ClusterNotRunning>()("ClusterNotRunning", {
  cluster: Schema.String,
  made: Schema.Boolean,
}) {
  override get message(): string {
    return this.made ? `the cluster ${this.cluster} is stopped: alasio up starts it` : `there is no cluster ${this.cluster} yet: alasio up makes it`;
  }
}

/**
 * The config, the cluster it targets, and its API to run `use` with; a local cluster
 * must be running, as only `up` starts it.
 */
export const onCluster = <A, E, R>(use: (target: ResolvedTarget) => Effect.Effect<A, E, R>) =>
  Effect.gen(function*() {
    const config = yield* loadConfig;
    const target = yield* resolveTarget(config);
    if (target._tag === "Local") {
      const { nodes } = yield* Effect.provide(Effect.flatMap(LocalCluster, (cluster) => cluster.status), localCluster(target.cluster));
      const server = nodes.find(({ role }) => role === "server");
      if (server?.state !== "running") return yield* new ClusterNotRunning({ cluster: target.cluster.name, made: server !== undefined });
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

/** The pods of `of`, the newest first. */
export const podsOf = (of: Component): Effect.Effect<readonly V1Pod[], KubeApiError, KubeApi> =>
  Effect.flatMap(KubeApi, (kube) =>
    kube.list<V1Pod>(kind("Pod"), { namespace: NAMESPACE, labelSelector: selectorOf(of.workload.spec?.selector?.matchLabels ?? {}) })).pipe(
      Effect.map((pods) => pods.toSorted((a, b) => new Date(b.metadata?.creationTimestamp ?? 0).getTime() - new Date(a.metadata?.creationTimestamp ?? 0).getTime())),
    );

/** No pod of a component runs ready, so nothing can be run in one. */
export class NoRunningPod extends Schema.TaggedError<NoRunningPod>()("NoRunningPod", {
  component: Schema.String,
  why: Schema.Array(Schema.String),
}) {
  override get message(): string {
    return `no pod of ${this.component} runs${this.why.length > 0 ? `: ${this.why.join("; ")}` : ""}; alasio status says more`;
  }
}

/** The newest pod of the component `name` that runs ready. */
export const runningPod = Effect.fnUntraced(function*(name: string): Effect.fn.Return<string, KubeApiError | UnknownComponent | NoRunningPod, KubeApi> {
  const pods = yield* podsOf(yield* component(name));
  const running = pods.find((pod) => pod.status?.phase === "Running" && podProblems(pod).length === 0);
  if (!running?.metadata?.name) return yield* new NoRunningPod({ component: name, why: pods.flatMap(podProblems) });
  return running.metadata.name;
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
