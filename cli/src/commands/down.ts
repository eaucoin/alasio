/** `alasio down`: stops the cluster on this machine, keeping everything. */
import { Console, Effect, Schema } from "effect";
import { Command } from "effect/cli";

import { DockerCluster } from "../cluster/docker.ts";
import { HostCluster } from "../cluster/host.ts";
import { loadConfig } from "../config.ts";
import { describeTarget, dockerCluster, resolveTarget } from "../target.ts";

/** The command is for the cluster alasio makes, and the config targets another. */
export class NotLocal extends Schema.TaggedError<NotLocal>()("NotLocal", {
  command: Schema.String,
  target: Schema.String,
  instead: Schema.String,
}) {
  override get message(): string {
    return `alasio ${this.command} is for the cluster alasio makes on this machine, and alasio runs in ${this.target}, which alasio does not stop or start: ${this.instead}`;
  }
}

export const down = Command.make("down", {}, () =>
  Effect.gen(function*() {
    const target = yield* resolveTarget(yield* loadConfig);
    if (target._tag === "Kubeconfig") {
      return yield* new NotLocal({ command: "down", target: describeTarget(target), instead: "alasio uninstall removes alasio from it" });
    }
    if (target._tag === "Host") yield* Effect.provide(Effect.flatMap(HostCluster, (cluster) => cluster.down), HostCluster.layer(target.cluster));
    else yield* Effect.provide(Effect.flatMap(DockerCluster, (cluster) => cluster.down), dockerCluster(target.cluster));
    yield* Console.log(`alasio is stopped, with everything kept; alasio up starts it again.`);
  })).pipe(
    Command.withShortDescription("Stop the cluster on this machine"),
    Command.withDescription(
      "Stops the cluster alasio made on this machine, and alasio with it, keeping everything: alasio up starts it again where it was. " +
        "k3s on the machine itself it stops as root, through sudo, its service disabled, so it stays stopped until then. " +
        "For a cluster a kubeconfig reaches it does nothing, as that cluster is not alasio's to stop.",
    ),
  );
