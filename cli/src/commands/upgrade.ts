/** `alasio upgrade`: `up` with this version of the package's images, saying what changes; refused while a branch environment another release made exists. */
import type { KubernetesObject, V1PodSpec } from "@kubernetes/client-node";
import { Clock, Console, Duration, Effect } from "effect";
import { Command, Flag } from "effect/cli";

import { refuseBranchesOfOtherReleases } from "../branches.ts";
import { untilHarnessesReleased } from "../bound-harnesses.ts";
import { loadConfig, type OperatorConfig } from "../config.ts";
import { checkInstallation, install, installationObjects, installConfig } from "../install.ts";
import { kind, KubeApi } from "../kube/api.ts";
import { INSTALLATION_SELECTOR } from "../kube/apply.ts";
import { RELEASE } from "../manifests/common.ts";
import { VERSION } from "../release.ts";
import { setupPod, stackSetupPod } from "../setup-pod.ts";
import { describeTarget, kubeApi, resolveTarget } from "../target.ts";
import { timeoutFlag, waitOptions } from "./common.ts";
import { announce, ensureCluster, refuseFirst } from "./up.ts";

const WORKLOADS = ["Deployment", "StatefulSet", "CronJob"] as const;

/** A workload's pod spec, wherever its kind keeps it. */
type Workload = KubernetesObject & { spec?: { template?: { spec?: V1PodSpec }; jobTemplate?: { spec?: { template?: { spec?: V1PodSpec } } } } };

/** The images each workload of `objects` runs, by its name. */
function imagesOf(objects: readonly KubernetesObject[]): ReadonlyMap<string, string> {
  return new Map(
    objects
      .filter((object) => WORKLOADS.some((name) => name === object.kind))
      .map((object) => {
        const { spec } = object as Workload;
        const pod = spec?.template?.spec ?? spec?.jobTemplate?.spec?.template?.spec;
        const images = [...(pod?.initContainers ?? []), ...(pod?.containers ?? [])].map(({ image }) => image ?? "");
        return [object.metadata?.name ?? "", [...new Set(images)].join(", ")] as const;
      }),
  );
}

/** What changes from the images `before` to those `after`, one line a workload, with the version of alasio's own. */
function imageChanges(before: { readonly version: string | null; readonly images: ReadonlyMap<string, string> }, after: ReadonlyMap<string, string>): string[] {
  if (before.version === null) return [`alasio is not installed yet: installing ${VERSION}`];
  const changed = [...after].filter(([name, images]) => before.images.get(name) !== images);
  if (changed.length === 0) return [`alasio ${before.version} already runs this version's images; applying it again`];
  return [
    before.version === VERSION ? `alasio ${VERSION}, with other images:` : `alasio ${before.version} → ${VERSION}:`,
    ...changed.map(([name, images]) => `  ${name}: ${before.images.get(name) ?? "(new)"} → ${images}`),
  ];
}

/** What an upgrade to this version refuses: a branch environment another release made, or what applying alasio refuses. */
const refusals = (config: OperatorConfig) => Effect.andThen(refuseBranchesOfOtherReleases(VERSION), Effect.asVoid(checkInstallation(config)));

export const upgrade = Command.make("upgrade", {
  timeout: timeoutFlag,
  check: Flag.Boolean("check").pipe(
    Flag.withDefault(false),
    Flag.withDescription("Check only, changing nothing, that alasio would upgrade: fail with what it would refuse, if anything"),
  ),
  now: Flag.Boolean("now").pipe(
    Flag.withDefault(false),
    Flag.withDescription(
      "Restart the cluster, where the upgrade does, without waiting for the conversations' harnesses to be released: each turn running is ended, and run again on a harness of its own",
    ),
  ),
  setupPod: Flag.Boolean("setup-pod").pipe(
    Flag.withDefault(false),
    Flag.withDescription("Print, as JSON, changing nothing, the pod of this version's alasio that makes its state alone, for a cut-over to run before the upgrade"),
  ),
  setupJob: Flag.Boolean("setup-job").pipe(
    Flag.withDefault(false),
    Flag.withDescription(
      "Print, as JSON, changing nothing, the pod of this version's stack setup, which makes what the stack's secrets lack and renders each service's from them, for a cut-over to run before the setup pod",
    ),
  ),
}, ({ timeout, check, now, setupPod: printSetupPod, setupJob: printSetupJob }) =>
  Effect.gen(function*() {
    const config = yield* loadConfig;
    const target = yield* resolveTarget(config);
    if (printSetupPod || printSetupJob) {
      const objects = yield* Effect.provide(Effect.map(installConfig(config), (made) => installationObjects(made)), kubeApi(target));
      return yield* Console.log(JSON.stringify(printSetupJob ? stackSetupPod(objects) : setupPod(objects), null, 2));
    }
    if (check) {
      yield* Effect.provide(refusals(config), kubeApi(target));
      return yield* Console.log(`alasio would upgrade ${describeTarget(target)} to ${VERSION}: it refuses nothing there now; nothing was changed.`);
    }
    yield* refuseFirst(target, refusals(config), "refuse");
    // A harness bound on the cluster is released before its pod stops with the cluster, unless told not to wait.
    yield* ensureCluster(target, now ? {} : { beforeRestart: untilHarnessesReleased(waitOptions(timeout)) });
    // The install's deadline, from when its waits start: what the API server is too busy for asked again until then at most.
    const busyUntil = (yield* Clock.currentTimeMillis) + Duration.toMillis(timeout);
    yield* Effect.provide(
      Effect.gen(function*() {
        const kube = yield* KubeApi;
        const running = (yield* Effect.forEach(WORKLOADS, (name) => kube.list(kind(name), { labelSelector: INSTALLATION_SELECTOR }))).flat();
        const version = running.find((object) => object.kind === "Deployment" && object.metadata?.name === RELEASE)?.metadata?.labels?.["app.kubernetes.io/version"] ?? null;
        const desired = installationObjects(yield* installConfig(config));
        yield* Console.log(imageChanges({ version, images: imagesOf(running) }, imagesOf(desired)).join("\n"));
        yield* refuseBranchesOfOtherReleases(VERSION);
        yield* announce(target, yield* install(config, waitOptions(timeout)));
      }),
      kubeApi(target, { busyUntil }),
    );
  })).pipe(
    Command.withShortDescription("Upgrade alasio to this version"),
    Command.withDescription(
      "Does what alasio up does, with the images this version of alasio's package pins, after saying which images change " +
        "and from which version of alasio, and, for k3s on this machine, with the k3s, gVisor and stargz-snapshotter it pins, saying which it " +
        "installs in place of which; where that restarts the cluster, which stops its pods, it waits first for each conversation's harness " +
        "bound on it to be released, as it is once its conversation has been quiet for harness.pools.releaseAfter, for --timeout at most, " +
        "unless given --now, which ends the turns they run, each run again on a harness of its own. A conversation may bind a harness " +
        "while it waits, as its operator's message comes, so a busy machine may not come to have none bound in time: run it when the " +
        "conversations are quiet, or with --now. It upgrades main alone, and refuses while a branch environment another release of alasio " +
        "made exists, which would not run on what it changes: delete each first, and make it again once upgraded. What it refuses " +
        "it refuses before it changes anything, and with --check it checks for that alone, changing nothing; on this machine, of " +
        "a cluster it made that does not answer, it can check nothing, so refuses. Run as npx alasio@latest upgrade to upgrade " +
        "to the latest release.",
    ),
  );
