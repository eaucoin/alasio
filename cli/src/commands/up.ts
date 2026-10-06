/** `alasio up`: makes or starts the cluster on this machine, applies alasio, and waits until it runs. */
import { Console, Effect } from "effect";
import { Command } from "effect/cli";

import { DockerCluster } from "../cluster/docker.ts";
import { HostCluster } from "../cluster/host.ts";
import { inotifyStep, requireLocalMachine } from "../cluster/machine.ts";
import { Root } from "../cluster/root.ts";
import { loadConfig, type OperatorConfig } from "../config.ts";
import { install, type Installed } from "../install.ts";
import { VERSION } from "../release.ts";
import { describeTarget, dockerCluster, kubeApi, type ResolvedTarget, resolveTarget } from "../target.ts";
import { timeoutFlag, waitOptions } from "./common.ts";

/**
 * Makes or starts the cluster on this machine when it is the target, once this machine is
 * one it runs on, its inotify limits raised, as root, where they are too low, writing its
 * kubeconfig: k3s on the machine itself, with what it changes as root at once, or in
 * Docker; nothing for another.
 */
export const ensureCluster = (target: ResolvedTarget) =>
  Effect.gen(function*() {
    if (target._tag === "Kubeconfig") return;
    yield* requireLocalMachine;
    if (target._tag === "Host") {
      return yield* Effect.provide(Effect.flatMap(HostCluster, (cluster) => cluster.up(target.kubeconfig.path)), HostCluster.layer(target.cluster));
    }
    const raise = yield* inotifyStep;
    yield* Effect.flatMap(Root, (root) => root.run(raise ? [raise] : []));
    yield* Effect.provide(Effect.flatMap(DockerCluster, (cluster) => cluster.up(target.kubeconfig.path)), dockerCluster(target.cluster));
  });

/** Where alasio runs, and what the operator does next. */
export const announce = (target: ResolvedTarget, { bot, claude }: Installed): Effect.Effect<void> =>
  Console.log(
    [
      `alasio ${VERSION} runs in ${describeTarget(target)}.`,
      "",
      `Message ${bot.username ? `@${bot.username}` : "your bot"} on Telegram to talk to it.`,
      ...(claude ? [] : ["Claude Code has no token yet: alasio init asks for one, which `claude setup-token` makes."]),
      "Codex logs in once, in alasio: alasio login codex",
      "alasio status says how it is, and alasio logs --follow follows it.",
    ].join("\n"),
  );

/** Brings up what `config` describes, waiting `timeout` at most for it to run. */
export const bringUp = Effect.fnUntraced(function*(config: OperatorConfig, timeout: Parameters<typeof waitOptions>[0]) {
  const target = yield* resolveTarget(config);
  yield* ensureCluster(target);
  const installed = yield* Effect.provide(install(config, waitOptions(timeout)), kubeApi(target));
  yield* announce(target, installed);
});

export const up = Command.make("up", { timeout: timeoutFlag }, ({ timeout }) => Effect.flatMap(loadConfig, (config) => bringUp(config, timeout))).pipe(
  Command.withShortDescription("Start alasio, installing or updating it"),
  Command.withDescription(
    "Makes the cluster on this machine, or starts it, when that is where alasio runs, once this machine is one it runs on, " +
      "Linux on x86-64: k3s on the machine itself, installing or upgrading k3s and gVisor and setting k3s as the config says, or " +
      "a cluster in Docker; for either, raising its inotify limits where they are too low for it. What it changes on the machine " +
      "it changes as root, through sudo, at once, after saying what. Then it applies alasio as the config says, " +
      "with this version's images; and waits until it runs, saying what it waits for. Run it again after changing the config. " +
      "Past --timeout it says what is still not running and why.",
  ),
);
