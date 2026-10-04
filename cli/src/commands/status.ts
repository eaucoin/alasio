/** `alasio status`: what runs, and whether it is healthy. */
import { Console, Effect, Schema } from "effect";
import { Command } from "effect/cli";

import { LocalCluster } from "../cluster/local.ts";
import { loadConfig } from "../config.ts";
import { describeRef, kind, KubeApi, refOf } from "../kube/api.ts";
import { INSTALLATION_SELECTOR } from "../kube/apply.ts";
import { diagnose, readiness } from "../kube/rollout.ts";
import { RELEASE } from "../manifests/common.ts";
import { describeTarget, kubeApi, localCluster, resolveTarget } from "../target.ts";
import { ClusterNotRunning } from "./common.ts";

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

export const status = Command.make("status", {}, () =>
  Effect.gen(function*() {
    const config = yield* loadConfig;
    const target = yield* resolveTarget(config);
    if (target._tag === "Local") {
      const { docker, nodes } = yield* Effect.provide(Effect.flatMap(LocalCluster, (cluster) => cluster.status), localCluster(target.cluster));
      yield* Console.log(`cluster ${target.cluster.name}, in Docker ${docker}:`);
      yield* Console.log(nodes.length > 0 ? nodes.map(({ name, state }) => `  ${name}: ${state}`).join("\n") : "  not made");
      const server = nodes.find(({ role }) => role === "server");
      if (server?.state !== "running") return yield* new ClusterNotRunning({ cluster: target.cluster.name, made: server !== undefined });
    }
    yield* Effect.provide(
      Effect.gen(function*() {
        const kube = yield* KubeApi;
        const workloads = (yield* Effect.forEach(["Deployment", "StatefulSet"] as const, (name) => kube.list(kind(name), { labelSelector: INSTALLATION_SELECTOR })))
          .flat()
          .sort((a, b) => (a.metadata?.name ?? "").localeCompare(b.metadata?.name ?? ""));
        const version = workloads.find((object) => object.kind === "Deployment" && object.metadata?.name === RELEASE)?.metadata?.labels?.["app.kubernetes.io/version"];
        yield* Console.log(`alasio${version ? ` ${version}` : ""}, in ${describeTarget(target)} (${kube.server}):`);
        let notReady = 0;
        for (const workload of workloads) {
          const state = readiness(workload);
          if (state._tag === "Ready") {
            yield* Console.log(`  ${describeRef(refOf(workload))}: ready`);
            continue;
          }
          notReady += 1;
          yield* Console.log(`  ${describeRef(refOf(workload))}: ${state._tag === "Waiting" ? state.status : state.reason}`);
          for (const line of yield* diagnose(refOf(workload))) yield* Console.log(`    ${line}`);
        }
        if (workloads.length === 0 || notReady > 0) return yield* new Unhealthy({ notReady, of: workloads.length });
      }),
      kubeApi(target),
    );
  })).pipe(
    Command.withShortDescription("Say what runs, and whether it is healthy"),
    Command.withDescription(
      "Says where alasio runs (and, for the cluster on this machine, its nodes), its version, and each of its workloads: " +
        "ready, or what it is at and why. Exits with 1 when something is not ready.",
    ),
  );
