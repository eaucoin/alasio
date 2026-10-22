/** `alasio up`: makes or starts the cluster on this machine, applies alasio, and waits until it runs. */
import { Console, Effect, Schema } from "effect";
import { Command } from "effect/cli";

import { DockerCluster } from "../cluster/docker.ts";
import { HostCluster } from "../cluster/host.ts";
import type { UpOptions } from "../cluster/k3s.ts";
import { inotifyStep, requireLocalMachine } from "../cluster/machine.ts";
import { Root } from "../cluster/root.ts";
import { loadConfig, type OperatorConfig } from "../config.ts";
import { baselineShortfalls } from "../cpu-baseline.ts";
import { checkInstallation, install, type Installed } from "../install.ts";
import { kind, KubeApi } from "../kube/api.ts";
import { NAMESPACE } from "../manifests/common.ts";
import { VERSION } from "../release.ts";
import { describeTarget, dockerCluster, kubeApi, type ResolvedTarget, resolveTarget } from "../target.ts";
import { timeoutFlag, waitOptions } from "./common.ts";

/**
 * Makes or starts the cluster on this machine when it is the target, once this machine is
 * one it runs on, its inotify limits raised, as root, where they are too low, writing its
 * kubeconfig: k3s on the machine itself, with what it changes as root at once, or in
 * Docker; nothing for another. What `options` say is run before the pods of a cluster that
 * runs are stopped, as up restarts it.
 */
export const ensureCluster = <E = never>(target: ResolvedTarget, options: UpOptions<E> = {}) =>
  Effect.gen(function*() {
    if (target._tag === "Kubeconfig") return;
    yield* requireLocalMachine;
    if (target._tag === "Host") {
      return yield* Effect.provide(Effect.flatMap(HostCluster, (cluster) => cluster.up(target.kubeconfig.path, options)), HostCluster.layer(target.cluster));
    }
    const raise = yield* inotifyStep;
    yield* Effect.flatMap(Root, (root) => root.run(raise ? [raise] : []));
    yield* Effect.provide(Effect.flatMap(DockerCluster, (cluster) => cluster.up(target.kubeconfig.path, options)), dockerCluster(target.cluster));
  });

/** The cluster on this machine alasio made is stopped, so what an upgrade would refuse cannot be checked before it changes the machine. */
export class ClusterStopped extends Schema.TaggedError<ClusterStopped>()("ClusterStopped", {
  cluster: Schema.String,
}) {
  override get message(): string {
    return `${this.cluster} does not answer, so alasio cannot check whether it would refuse this upgrade before it changes this machine ` +
      "for it: start it as it is, with the alasio that made it (its alasio up), then upgrade";
  }
}

/**
 * Runs `check`, which refuses what is not to be done, before `ensureCluster` changes anything
 * of this machine for the cluster on it (k3s, gVisor, alasio's stamp of them), where that
 * cluster answers. One alasio has not made yet holds nothing of alasio's to refuse. One it
 * made, its kubeconfig written, that does not answer, as it is stopped, holds an
 * installation: `whenStopped` says whether to go on, as up does, which starts it and checks
 * as it applies alasio, or to refuse, as an upgrade does, which would change the machine
 * before it could check. A cluster a kubeconfig reaches, which ensureCluster leaves alone, is
 * checked as alasio is applied.
 */
export const refuseFirst = <E>(target: ResolvedTarget, check: Effect.Effect<void, E, KubeApi>, whenStopped: "go on" | "refuse") =>
  Effect.gen(function*() {
    if (target._tag === "Kubeconfig") return;
    const answers = Effect.flatMap(KubeApi, (kube) => kube.get({ ...kind("Namespace"), name: NAMESPACE })).pipe(
      Effect.as(true),
      Effect.catchIf((error) => error.status === undefined, () => Effect.succeed(false)),
    );
    const checked = Effect.gen(function*() {
      if (yield* answers) return yield* check;
      if (whenStopped === "refuse") return yield* new ClusterStopped({ cluster: describeTarget(target) });
    });
    yield* Effect.provide(checked, kubeApi(target)).pipe(Effect.catchTag("KubeconfigUnusable", () => Effect.void));
  });

/** What up says where it could not read where the API server is, and so confines nothing to it. */
export const API_SERVER_UNCONFINED =
  "Warning: the cluster's API server could not be found (the default namespace's kubernetes Service has no EndpointSlice alasio can read), " +
  "so the sandbox gateway and the session telemetry receiver may reach any address, not that alone beside their own peers. alasio up confines them once it can read it.";

/** Where alasio runs, and what the operator does next. */
export const announce = (target: ResolvedTarget, { bot, claude, cpuBaseline, apiServerUnconfined }: Installed): Effect.Effect<void> =>
  Console.log(
    [
      `alasio ${VERSION} runs in ${describeTarget(target)}.`,
      // What keeps sessions from being snapshotted, or restored on a node, if anything.
      ...(cpuBaseline ? baselineShortfalls(cpuBaseline) : []),
      ...(apiServerUnconfined ? [API_SERVER_UNCONFINED] : []),
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
  yield* refuseFirst(target, Effect.asVoid(checkInstallation(config)), "go on");
  yield* ensureCluster(target);
  const installed = yield* Effect.provide(install(config, waitOptions(timeout)), kubeApi(target));
  yield* announce(target, installed);
});

export const up = Command.make("up", { timeout: timeoutFlag }, ({ timeout }) => Effect.flatMap(loadConfig, (config) => bringUp(config, timeout))).pipe(
  Command.withShortDescription("Start alasio, installing or updating it"),
  Command.withDescription(
    "Makes the cluster on this machine, or starts it, when that is where alasio runs, once this machine is one it runs on, " +
      "Linux on x86-64: k3s on the machine itself, installing or upgrading k3s, gVisor and stargz-snapshotter and setting k3s as the config says, or " +
      "a cluster in Docker; for either, raising its inotify limits where they are too low for it. Its nodes pull images through " +
      "stargz-snapshotter: an eStargz image's layers are read from its registry, or a mirror, as its pods read them, which " +
      "needs both to answer range requests, and fetched whole meanwhile; any other image is pulled whole. So a machine that " +
      "boots again while a registry it pulled from so is out of reach runs no pod until it answers, as the daemon mounts those " +
      "layers from it again before it serves any. What it changes on the machine " +
      "it changes as root, through sudo, at once, after saying what. Then it applies alasio as the config says, " +
      "with this version's images, and with KEDA and VPA, which scale and size its components, unless the cluster has its own, " +
      "which the config then says to use; and waits until it runs, saying what it waits for. Run it again after changing the config. " +
      "Past --timeout it says what is still not running and why.",
  ),
);
