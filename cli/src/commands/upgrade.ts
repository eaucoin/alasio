/** `alasio upgrade`: `up` with this version of the package's images, saying what changes. */
import type { KubernetesObject, V1PodSpec } from "@kubernetes/client-node";
import { Console, Effect } from "effect";
import { Command } from "effect/cli";

import { loadConfig } from "../config.ts";
import { install, installationObjects, installConfig } from "../install.ts";
import { kind, KubeApi } from "../kube/api.ts";
import { INSTALLATION_SELECTOR } from "../kube/apply.ts";
import { RELEASE } from "../manifests/common.ts";
import { VERSION } from "../release.ts";
import { kubeApi, resolveTarget } from "../target.ts";
import { timeoutFlag, waitOptions } from "./common.ts";
import { announce, ensureCluster } from "./up.ts";

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

export const upgrade = Command.make("upgrade", { timeout: timeoutFlag }, ({ timeout }) =>
  Effect.gen(function*() {
    const config = yield* loadConfig;
    const target = yield* resolveTarget(config);
    yield* ensureCluster(target);
    yield* Effect.provide(
      Effect.gen(function*() {
        const kube = yield* KubeApi;
        const running = (yield* Effect.forEach(WORKLOADS, (name) => kube.list(kind(name), { labelSelector: INSTALLATION_SELECTOR }))).flat();
        const version = running.find((object) => object.kind === "Deployment" && object.metadata?.name === RELEASE)?.metadata?.labels?.["app.kubernetes.io/version"] ?? null;
        const desired = installationObjects(yield* installConfig(config));
        yield* Console.log(imageChanges({ version, images: imagesOf(running) }, imagesOf(desired)).join("\n"));
        yield* announce(target, yield* install(config, waitOptions(timeout)));
      }),
      kubeApi(target),
    );
  })).pipe(
    Command.withShortDescription("Upgrade alasio to this version"),
    Command.withDescription(
      "Does what alasio up does, with the images this version of alasio's package pins, after saying which images change " +
        "and from which version of alasio. Run as npx alasio@latest upgrade to upgrade to the latest release.",
    ),
  );
