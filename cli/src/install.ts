/**
 * Installing alasio from the operator's config: its install configuration, with the
 * Secrets alasio names as the cluster has them, made into its objects (its namespace,
 * which its Secrets are in before anything else of it is, and its manifests), checked
 * against the cluster's autoscalers, its object store's layout and scale, Neon's storage
 * (./neon-storage.ts), and the objects the cluster has, applied, and waited for.
 */
import type { KubernetesObject, V1Namespace } from "@kubernetes/client-node";
import { Effect, Result, Schema } from "effect";

import { gvisorReleases as gvisorReleasesOf, NODE_PINS } from "./cluster/node.ts";
import { installConfigOf, type OperatorConfig, OperatorConfigError, sharedCluster } from "./config.ts";
import { type BaselineDecision, decideCpuBaseline, recordedBaseline, sameBaseline, surveyCpu } from "./cpu-baseline.ts";
import { clusterReleases } from "./kept-releases.ts";
import { describeRef, hasStatus, KubeApi, type KubeApiError, refOf } from "./kube/api.ts";
import { type ApplyError, applyInstallation, labelled } from "./kube/apply.ts";
import { type AutoscalerMismatch, checkAutoscalers } from "./kube/autoscalers.ts";
import { checkObjectStoreLayout, checkObjectStoreScale, type ObjectStoreScaleRefused } from "./kube/object-store-scale.ts";
import type { WaitOptions } from "./kube/rollout.ts";
import type { CpuBaseline } from "./manifests/alasio.ts";
import { labels, NAMESPACE, neonName, nodeAgentRuns } from "./manifests/common.ts";
import type { InstallConfig } from "./manifests/config.ts";
import { type Carried, carriedFrom, manifests, NOTHING_CARRIED } from "./manifests/index.ts";
import { checkNeonStorage, type PageserversLowered, type ShardsLowered, type StorageDatabaseUnread, type StorageUnverified } from "./neon-storage.ts";
import { readClaudeToken, readTelegramBot, type TelegramBot } from "./secrets.ts";

/** alasio's namespace. */
const NAMESPACE_OBJECT: V1Namespace = { apiVersion: "v1", kind: "Namespace", metadata: { name: NAMESPACE, labels: labels("alasio") } };

/** The objects of the installation `config` describes, with what it carries forward from the one it replaces. */
export function installationObjects(config: InstallConfig, carried: Carried = NOTHING_CARRIED): readonly KubernetesObject[] {
  return [NAMESPACE_OBJECT, ...manifests(config, carried)];
}

/** alasio's namespace, applied as the installation's, so its Secrets can be written before the rest of it is. */
export const applyNamespace: Effect.Effect<void, KubeApiError, KubeApi> = Effect.flatMap(KubeApi, (kube) => kube.apply(labelled(NAMESPACE_OBJECT))).pipe(
  Effect.asVoid,
);

/** The cluster has no bot's Secret: alasio has not been given its bot. */
export class BotNotSet extends Schema.TaggedError<BotNotSet>()("BotNotSet", {}) {
  override get message(): string {
    return "alasio has no Telegram bot in this cluster yet: alasio init gives it one";
  }
}

/** The install configuration of `config`, which names Claude Code's Secret when the cluster has it. */
export const installConfig = Effect.fnUntraced(function*(config: OperatorConfig): Effect.fn.Return<InstallConfig, KubeApiError | OperatorConfigError, KubeApi> {
  const claude = (yield* readClaudeToken) !== null;
  const install = installConfigOf(config.install, { claude, shared: sharedCluster(config.target) });
  if (Result.isFailure(install)) return yield* new OperatorConfigError({ path: config.path, reason: install.failure });
  return install.success;
});

/** What an installation came to: its bot, whether Claude Code has a token, and, where sessions are snapshotted, their CPU baseline. */
export interface Installed {
  readonly bot: TelegramBot;
  readonly claude: boolean;
  readonly cpuBaseline: BaselineDecision | null;
}

/**
 * The cluster has an object of the installation that cannot take this release's of it, as
 * applying it as a dry run said: nothing is applied, as one refused midway would leave the
 * installation half of one release, half of the other.
 */
export class ObjectRefused extends Schema.TaggedError<ObjectRefused>()("ObjectRefused", { object: Schema.String, reason: Schema.String, remedy: Schema.String }) {
  override get message(): string {
    return `the cluster's ${this.object} cannot take this release's of it (${this.reason}), so alasio applies nothing${this.remedy ? `: ${this.remedy}` : ""}`;
  }
}

/**
 * What makes an object the cluster has able to take this release's, by the object, where an
 * upgrade to this release is to be made by hand: the pageservers' StatefulSet of 7.0.0,
 * whose pods start in order, as this release's start at once, a field Kubernetes changes of
 * no StatefulSet.
 */
const REMEDIES: ReadonlyMap<string, string> = new Map([
  [`StatefulSet ${NAMESPACE}/${neonName("pageserver")}`, "an installation of 7.0.0 is upgraded by upgrades/from-7.0.0.sh, which makes it anew, keeping its pod and claim"],
]);

/** Refuses `objects` where the cluster has one that cannot take this release's of it, each applied as a dry run. */
const checkApplicable = (objects: readonly KubernetesObject[]): Effect.Effect<void, KubeApiError | ObjectRefused, KubeApi> =>
  Effect.flatMap(KubeApi, (kube) =>
    Effect.forEach(objects, (object) => {
      const ref = refOf(object);
      return Effect.flatMap(kube.get(ref), (live) =>
        live
          ? kube.dryApply(labelled(object)).pipe(
            Effect.asVoid,
            Effect.catchIf(hasStatus(422), (error) => Effect.fail(new ObjectRefused({ object: describeRef(ref), reason: error.reason, remedy: REMEDIES.get(describeRef(ref)) ?? "" }))),
          )
          : Effect.void);
    }, { concurrency: 8, discard: true }));

/** The installation `config` describes, as checkInstallation finds it, with what its objects carry forward. */
interface Checked {
  readonly bot: TelegramBot;
  readonly settings: InstallConfig;
  readonly objects: readonly KubernetesObject[];
  /** The templates the cluster runs, as their ConfigMap records them, and the CPU baseline they hold sessions to. */
  readonly live: string | undefined;
  readonly recorded: CpuBaseline;
  /** The releases the cluster keeps, as its RuntimeClasses say, which the sessions' snapshots restore with. */
  readonly gvisorReleases: readonly string[];
}

/**
 * The installation `config` describes, with its bot, refused where applying it would not do:
 * with no bot set, against the cluster's own autoscalers, its object store's layout and
 * scale, or Neon's storage (./neon-storage.ts), or where an object the cluster has cannot
 * take this release's of it. Its objects carry forward what the installation they replace
 * recorded.
 */
export const checkInstallation = Effect.fnUntraced(function*(
  config: OperatorConfig,
): Effect.fn.Return<Checked, KubeApiError | AutoscalerMismatch | ObjectStoreScaleRefused | BotNotSet | OperatorConfigError | PageserversLowered | ShardsLowered | StorageDatabaseUnread | StorageUnverified | ObjectRefused, KubeApi> {
  const bot = yield* readTelegramBot;
  if (!bot) return yield* new BotNotSet();
  const settings = yield* installConfig(config);
  const { live, baseline: recorded } = yield* recordedBaseline;
  const gvisorReleases = yield* clusterReleases(gvisorReleasesOf(NODE_PINS));
  const objects = installationObjects(settings, carriedFrom(settings, live, { cpuBaseline: recorded, gvisorReleases }));
  yield* checkAutoscalers(objects, settings);
  yield* checkObjectStoreLayout(objects);
  yield* checkObjectStoreScale(objects);
  yield* checkNeonStorage(settings);
  yield* checkApplicable(objects);
  return { bot, settings, objects, live, recorded, gvisorReleases };
});

/**
 * Applies the installation `config` describes, once its bot is set, and waits until it
 * runs. Where sessions are snapshotted, it holds them to the CPU baseline it recorded
 * before, then to the one the node agents' pods then say (./cpu-baseline.ts), applied
 * again where that differs.
 */
export const install = Effect.fnUntraced(function*(
  config: OperatorConfig,
  options: WaitOptions,
): Effect.fn.Return<Installed, ApplyError | AutoscalerMismatch | ObjectStoreScaleRefused | BotNotSet | OperatorConfigError | PageserversLowered | ShardsLowered | StorageDatabaseUnread | StorageUnverified | ObjectRefused, KubeApi> {
  const { bot, settings, objects, live, recorded, gvisorReleases } = yield* checkInstallation(config);
  yield* applyInstallation(objects, options);
  const installed = { bot, claude: settings.alasio.claude.existingSecret !== "" };
  if (!nodeAgentRuns(settings)) return { ...installed, cpuBaseline: null };
  const decision = decideCpuBaseline(yield* surveyCpu, recorded);
  // Carried from the installation before this one, as the first apply carried it.
  if (!sameBaseline(decision.baseline, recorded)) {
    yield* applyInstallation(installationObjects(settings, carriedFrom(settings, live, { cpuBaseline: decision.baseline, gvisorReleases })), options);
  }
  return { ...installed, cpuBaseline: decision };
});
