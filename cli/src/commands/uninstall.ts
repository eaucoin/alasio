/** `alasio uninstall [--purge]`: removes alasio, and with --purge its data and the cluster on this machine. */
import { Console, Effect, FileSystem, Schema } from "effect";
import { Command, Flag, Prompt } from "effect/cli";

import { LocalCluster } from "../cluster/local.ts";
import { loadConfig } from "../config.ts";
import { removeInstallation } from "../kube/apply.ts";
import { describeTarget, localCluster, resolveTarget } from "../target.ts";
import { onCluster, timeoutFlag, waitOptions } from "./common.ts";

/** The operator did not confirm. */
export class NotConfirmed extends Schema.TaggedError<NotConfirmed>()("NotConfirmed", {}) {
  override get message(): string {
    return "nothing was removed";
  }
}

export const uninstall = Command.make(
  "uninstall",
  {
    purge: Flag.Boolean("purge").pipe(
      Flag.withDefault(false),
      Flag.withDescription("Also remove alasio's data: its database, its own and its workspaces' volumes, its Secrets; and the cluster on this machine, if that is where it runs"),
    ),
    yes: Flag.Boolean("yes").pipe(Flag.withAlias("y"), Flag.withDefault(false), Flag.withDescription("Do not ask for confirmation")),
    timeout: timeoutFlag,
  },
  ({ purge, yes, timeout }) =>
    Effect.gen(function*() {
      const config = yield* loadConfig;
      const target = yield* resolveTarget(config);
      const where = describeTarget(target);
      const confirmed = yes || (yield* Prompt.run(Prompt.Confirm({
        message: purge
          ? `Remove alasio and all its data from ${where}${target._tag === "Local" ? ", and the cluster itself" : ""}? This cannot be undone`
          : `Remove alasio from ${where}, keeping its data?`,
        initial: false,
      })));
      if (!confirmed) return yield* new NotConfirmed();
      if (purge && target._tag === "Local") {
        // The cluster is alasio's own: removing it, with its volumes and storage, removes everything.
        yield* Effect.provide(Effect.flatMap(LocalCluster, (cluster) => cluster.remove({ volumes: true, storage: true })), localCluster(target.cluster));
        yield* Effect.flatMap(FileSystem.FileSystem, (fs) => fs.remove(target.kubeconfig.path, { force: true }));
        yield* Console.log(`alasio and the cluster ${target.cluster.name} are removed, with all their data; the config at ${config.path} is kept.`);
        return;
      }
      const removed = yield* onCluster(() => removeInstallation({ purge }, waitOptions(timeout)));
      yield* Console.log(
        removed.length === 0
          ? `alasio is not installed in ${where}.`
          : purge
          ? `alasio is removed from ${where}, with all its data.`
          : `alasio is removed from ${where}; its volumes, namespaces and Secrets are kept, and alasio up installs it again with them.`,
      );
    }),
).pipe(
  Command.withShortDescription("Remove alasio"),
  Command.withDescription(
    "Removes alasio's objects from its cluster, after confirmation, keeping what holds data: volumes, namespaces (with alasio's Secrets " +
      "and its workspaces), and the Sandbox CRD, so alasio up installs it again as it was. With --purge it removes those too, and " +
      "when alasio runs in the cluster it made on this machine, that cluster with all it keeps. The config file is kept either way.",
  ),
);
