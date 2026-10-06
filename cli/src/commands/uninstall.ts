/** `alasio uninstall [--purge]`: removes alasio, and with --purge its data and the cluster on this machine, k3s and gVisor or the one in Docker. */
import { Console, Effect, FileSystem, Schema } from "effect";
import { Command, Flag, Prompt } from "effect/cli";

import { DockerCluster } from "../cluster/docker.ts";
import { HostCluster } from "../cluster/host.ts";
import { forgetInotifyStep } from "../cluster/machine.ts";
import { Root } from "../cluster/root.ts";
import { loadConfig } from "../config.ts";
import { removeInstallation } from "../kube/apply.ts";
import { describeTarget, dockerCluster, resolveTarget } from "../target.ts";
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
      Flag.withDescription("Also remove alasio's data: its database, its own and its workspaces' volumes, its Secrets; and the cluster on this machine, k3s and gVisor or the one in Docker, if that is where it runs"),
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
          ? `Remove alasio and all its data from ${where}${
            target._tag === "Host" ? ", and k3s and gVisor themselves" : target._tag === "Docker" ? ", and the cluster itself" : ""
          }? This cannot be undone`
          : `Remove alasio from ${where}, keeping its data?`,
        initial: false,
      })));
      if (!confirmed) return yield* new NotConfirmed();
      if (purge && target._tag === "Host") {
        // k3s here is alasio's own: removing it, with its storage, removes everything, as root at once.
        yield* Effect.provide(Effect.flatMap(HostCluster, (cluster) => cluster.remove({ storage: true })), HostCluster.layer(target.cluster));
        yield* Effect.flatMap(FileSystem.FileSystem, (fs) => fs.remove(target.kubeconfig.path, { force: true }));
        yield* Console.log(`alasio and k3s on this machine are removed, with gVisor and all their data; the config at ${config.path} is kept.`);
        return;
      }
      if (purge && target._tag === "Docker") {
        // The cluster is alasio's own: removing it, with its volumes and storage, removes everything.
        yield* Effect.provide(Effect.flatMap(DockerCluster, (cluster) => cluster.remove({ volumes: true, storage: true })), dockerCluster(target.cluster));
        yield* Effect.flatMap(FileSystem.FileSystem, (fs) => fs.remove(target.kubeconfig.path, { force: true }));
        const forget = yield* forgetInotifyStep;
        yield* Effect.flatMap(Root, (root) => root.run(forget ? [forget] : []));
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
      "and its workspaces), and the Sandbox CRD, so alasio up installs it again as it was; and what serves volumes that remain, " +
      "their StorageClasses and CSI drivers. With --purge it removes those too, workspaces and their volumes before the drivers " +
      "that delete their data, and when alasio runs in the cluster it made on this machine, that cluster with all it keeps: k3s " +
      "on the machine itself, with its own uninstall script, and gVisor, or the cluster in Docker; and the file of " +
      "/etc/sysctl.d/ it raised the machine's inotify limits in. What it removes as root it removes through sudo, at once, " +
      "after saying what. The config file is kept either way.",
  ),
);
