/** `alasio upgrade`: `up` with this version of the package's images, saying what changes; refused while a branch environment another release made exists. */
import type { KubernetesObject, V1PodSpec } from "@kubernetes/client-node";
import { Console, Effect } from "effect";
import { Command, Flag } from "effect/cli";

import { refuseBranchesOfOtherReleases } from "../branches.ts";
import { loadConfig, type OperatorConfig } from "../config.ts";
import { checkInstallation, install, installationObjects, installConfig } from "../install.ts";
import { kind, KubeApi } from "../kube/api.ts";
import { INSTALLATION_SELECTOR } from "../kube/apply.ts";
import { RELEASE } from "../manifests/common.ts";
import { VERSION } from "../release.ts";
import { setupPod } from "../setup-pod.ts";
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
  setupPod: Flag.Boolean("setup-pod").pipe(
    Flag.withDefault(false),
    Flag.withDescription("Print, as JSON, changing nothing, the pod of this version's alasio that makes its state alone, for a cut-over to run before the upgrade"),
  ),
}, ({ timeout, check, setupPod: printSetupPod }) =>
  Effect.gen(function*() {
    const config = yield* loadConfig;
    const target = yield* resolveTarget(config);
    if (printSetupPod) {
      const objects = yield* Effect.provide(Effect.map(installConfig(config), (made) => installationObjects(made)), kubeApi(target));
      return yield* Console.log(JSON.stringify(setupPod(objects), null, 2));
    }
    if (check) {
      yield* Effect.provide(refusals(config), kubeApi(target));
      return yield* Console.log(`alasio would upgrade ${describeTarget(target)} to ${VERSION}: it refuses nothing there now; nothing was changed.`);
    }
    yield* refuseFirst(target, refusals(config), "refuse");
    yield* ensureCluster(target);
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
      kubeApi(target),
    );
  })).pipe(
    Command.withShortDescription("Upgrade alasio to this version"),
    Command.withDescription(
      "Does what alasio up does, with the images this version of alasio's package pins, after saying which images change " +
        "and from which version of alasio, and, for k3s on this machine, with the k3s, gVisor and stargz-snapshotter it pins, saying which it " +
        "installs in place of which. It upgrades main alone, and refuses while a branch environment another release of alasio " +
        "made exists, which would not run on what it changes: delete each first, and make it again once upgraded. What it refuses " +
        "it refuses before it changes anything, and with --check it checks for that alone, changing nothing; on this machine, of " +
        "a cluster it made that does not answer, it can check nothing, so refuses. Run as npx alasio@latest upgrade to upgrade " +
        "to the latest release.",
    ),
  );
