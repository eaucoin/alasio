/** `alasio status`: what runs, and whether it is healthy. */
import type { V1Node } from "@kubernetes/client-node";
import { Console, Effect, Schema } from "effect";
import { Command } from "effect/cli";

import { DockerCluster } from "../cluster/docker.ts";
import { HostCluster } from "../cluster/host.ts";
import { readySince } from "../cluster/k3s.ts";
import { describeShortfall, InotifyLimitsTooLow, inotifyShortfalls, requireLocalMachine } from "../cluster/machine.ts";
import { SERVICE } from "../cluster/node.ts";
import { loadConfig } from "../config.ts";
import { describeRef, kind, KubeApi, refOf } from "../kube/api.ts";
import { INSTALLATION_SELECTOR, WORKLOADS } from "../kube/apply.ts";
import { diagnose, readiness } from "../kube/rollout.ts";
import { RELEASE } from "../manifests/common.ts";
import { describeTarget, dockerCluster, kubeApi, resolveTarget } from "../target.ts";
import { notRunning } from "./common.ts";

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
        const workloads = (yield* Effect.forEach(WORKLOADS, (name) => kube.list(kind(name), { labelSelector: INSTALLATION_SELECTOR })))
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
    if (shortfalls.length > 0) return yield* new InotifyLimitsTooLow({ shortfalls });
  })).pipe(
    Command.withShortDescription("Say what runs, and whether it is healthy"),
    Command.withDescription(
      "Says where alasio runs (and, for the cluster on this machine, k3s's service and node, or its nodes in Docker, and this " +
        "machine's inotify limits when they are too low for it), its version, and each of its workloads: ready, or what it is at and why. Exits with 1 when something " +
        "is not ready, or the limits are too low, or this machine is not Linux on x86-64, which the cluster here needs.",
    ),
  );
