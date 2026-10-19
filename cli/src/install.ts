/**
 * Installing alasio from the operator's config: its install configuration, with the
 * Secrets alasio names as the cluster has them, made into its objects (its namespace,
 * which its Secrets are in before anything else of it is, and its manifests), checked
 * against the cluster's autoscalers, its object store's layout and scale, Neon's storage
 * (./neon-storage.ts), and the objects the cluster has, applied, and waited for; main's route
 * given the webhook intake before it starts, and main's bot registered with Telegram once it
 * serves it (./webhook.ts).
 */
import type { KubernetesObject, V1Namespace } from "@kubernetes/client-node";
import { Effect, Result, Schema } from "effect";

import { gvisorReleases as gvisorReleasesOf, NODE_PINS } from "./cluster/node.ts";
import { installConfigOf, type OperatorConfig, OperatorConfigError, targetKind } from "./config.ts";
import { type BaselineDecision, decideCpuBaseline, recordedBaseline, sameBaseline, surveyCpu } from "./cpu-baseline.ts";
import { clusterReleases } from "./kept-releases.ts";
import { describeRef, hasStatus, KubeApi, type KubeApiError, refOf } from "./kube/api.ts";
import { type ApplyError, applyInstallation, labelled } from "./kube/apply.ts";
import { type AutoscalerMismatch, checkAutoscalers } from "./kube/autoscalers.ts";
import { checkObjectStoreLayout, checkObjectStoreScale, type ObjectStoreScaleRefused } from "./kube/object-store-scale.ts";
import type { NotReadyInTime, RolloutFailed, WaitOptions } from "./kube/rollout.ts";
import type { CpuBaseline } from "./manifests/alasio.ts";
import { labels, NAMESPACE, nodeAgentRuns } from "./manifests/common.ts";
import type { InstallConfig } from "./manifests/config.ts";
import { type Carried, carriedFrom, manifests, NOTHING_CARRIED } from "./manifests/index.ts";
import { checkNeonStorage, type PageserversLowered, type ShardsLowered, type StorageDatabaseUnread, type StorageUnverified } from "./neon-storage.ts";
import { botChecksum, ensureBotChecksumKey, readBotChecksumKey, readClaudeToken, readTelegramBot, type TelegramBot } from "./secrets.ts";
import type { TelegramBotApi, WebhookError } from "./telegram.ts";
import { ensureRoute, registerBot, type WebhookNotServing } from "./webhook.ts";

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

/** The configuration names no address for Telegram to deliver to. */
export class PublicAddressNotSet extends Schema.TaggedError<PublicAddressNotSet>()("PublicAddressNotSet", {}) {
  override get message(): string {
    return "install.webhook.publicAddress is not set: the address Telegram delivers the bot's updates to, which the webhook intake's certificate is made for; alasio init asks for it";
  }
}

/** The bot's Secret allows no user: alasio would answer no one, as it admits no one by default. */
export class NoUserAllowed extends Schema.TaggedError<NoUserAllowed>()("NoUserAllowed", {}) {
  override get message(): string {
    return "the bot allows no Telegram user (alasio-telegram's allowedUserIds is empty), and alasio admits no one but those it allows: alasio init --allowed-user-ids gives it them";
  }
}

/** The cluster has no bot's Secret: alasio has not been given its bot. */
export class BotNotSet extends Schema.TaggedError<BotNotSet>()("BotNotSet", {}) {
  override get message(): string {
    return "alasio has no Telegram bot in this cluster yet: alasio init gives it one";
  }
}

/** The install configuration of `config`, which names Claude Code's Secret when the cluster has it. */
export const installConfig = Effect.fnUntraced(function*(config: OperatorConfig): Effect.fn.Return<InstallConfig, KubeApiError | OperatorConfigError, KubeApi> {
  const claude = (yield* readClaudeToken) !== null;
  const install = installConfigOf(config.install, { claude, target: targetKind(config.target) });
  if (Result.isFailure(install)) return yield* new OperatorConfigError({ path: config.path, reason: install.failure });
  return install.success;
});

/** What an installation came to: its bot, whether Claude Code has a token, and, where sessions are snapshotted, their CPU baseline. */
export interface Installed {
  readonly bot: TelegramBot;
  readonly claude: boolean;
  readonly cpuBaseline: BaselineDecision | null;
}

/** An object the cluster has that cannot take this release's of it, and why, as a dry run of its apply said. */
const Refusal = Schema.Struct({ object: Schema.String, reason: Schema.String });

/**
 * The cluster has objects of the installation that cannot take this release's of them, as
 * applying each as a dry run said, all of them told at once: nothing is applied, as one
 * refused midway would leave the installation half of one release, half of the other.
 */
export class ObjectRefused extends Schema.TaggedError<ObjectRefused>()("ObjectRefused", { refused: Schema.NonEmptyArray(Refusal) }) {
  override get message(): string {
    const [first, ...more] = this.refused;
    return more.length === 0
      ? `the cluster's ${first.object} cannot take this release's of it (${first.reason}), so alasio applies nothing`
      : `the cluster's objects cannot take this release's of them, so alasio applies nothing:\n${this.refused.map(({ object, reason }) => `  ${object}: ${reason}`).join("\n")}`;
  }
}

/**
 * Refuses `objects` where the cluster has any that cannot take this release's of it, each
 * applied as a dry run, every one refused told at once. A Job, whose pod template cannot
 * change, is left out: it is run afresh, one of its name deleted first (./kube/apply.ts).
 */
const checkApplicable = (objects: readonly KubernetesObject[]): Effect.Effect<void, KubeApiError | ObjectRefused, KubeApi> =>
  Effect.flatMap(KubeApi, (kube) =>
    Effect.forEach(objects.filter(({ kind }) => kind !== "Job"), (object) => {
      const ref = refOf(object);
      return Effect.flatMap(kube.get(ref), (live) =>
        live
          ? kube.dryApply(labelled(object)).pipe(
            Effect.as(null),
            Effect.catchIf(hasStatus(422), (error) => Effect.succeed({ object: describeRef(ref), reason: error.reason })),
          )
          : Effect.succeed(null));
    }, { concurrency: 8 }).pipe(
      Effect.flatMap((each) => {
        const refused = each.filter((refusal) => refusal !== null).sort((a, b) => a.object.localeCompare(b.object));
        const [first, ...more] = refused;
        return first === undefined ? Effect.void : Effect.fail(new ObjectRefused({ refused: [first, ...more] }));
      }),
    ));

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
  /** The checksum of the bot's Secret alasio's pod carries (../secrets.ts botChecksum), null where the Secret has no key for it yet. */
  readonly checksum: string | null;
}

/**
 * The installation `config` describes, with its bot, refused where applying it would not do:
 * with no bot set, or one that allows no user, with no address for Telegram to deliver to,
 * against the cluster's own autoscalers, its object store's layout and scale, or Neon's
 * storage (./neon-storage.ts), or where an object the cluster has cannot take this release's
 * of it. Its objects carry forward what the installation they replace recorded.
 */
export const checkInstallation = Effect.fnUntraced(function*(
  config: OperatorConfig,
  wait?: WaitOptions,
): Effect.fn.Return<
  Checked,
  KubeApiError | NotReadyInTime | RolloutFailed | AutoscalerMismatch | ObjectStoreScaleRefused | BotNotSet | NoUserAllowed | PublicAddressNotSet | OperatorConfigError | PageserversLowered | ShardsLowered | StorageDatabaseUnread | StorageUnverified | ObjectRefused,
  KubeApi
> {
  const bot = yield* readTelegramBot;
  if (!bot) return yield* new BotNotSet();
  if (bot.allowedUserIds.length === 0) return yield* new NoUserAllowed();
  const settings = yield* installConfig(config);
  if (!settings.webhook.publicAddress) return yield* new PublicAddressNotSet();
  const { live, baseline: recorded } = yield* recordedBaseline;
  const gvisorReleases = yield* clusterReleases(gvisorReleasesOf(NODE_PINS));
  // None where the bot's Secret has no key for it yet, as up gives it one before it checks (ensureBotChecksumKey).
  const key = yield* readBotChecksumKey;
  const checksum = key === null ? null : botChecksum(bot, key);
  const objects = installationObjects(settings, carriedFrom(settings, live, { cpuBaseline: recorded, gvisorReleases, botChecksum: checksum }));
  yield* checkAutoscalers(objects, settings);
  yield* checkObjectStoreLayout(objects);
  yield* checkObjectStoreScale(objects);
  yield* checkNeonStorage(settings, wait);
  yield* checkApplicable(objects);
  return { bot, settings, objects, live, recorded, gvisorReleases, checksum };
});

/**
 * Applies the installation `config` describes, once its bot is set, waits until it runs,
 * and has Telegram deliver main's bot's updates to it. Where sessions are snapshotted, it
 * holds them to the CPU baseline it recorded before, then to the one the node agents' pods
 * then say (./cpu-baseline.ts), applied again where that differs.
 */
export const install = Effect.fnUntraced(function*(
  config: OperatorConfig,
  options: WaitOptions,
): Effect.fn.Return<
  Installed,
  ApplyError | AutoscalerMismatch | ObjectStoreScaleRefused | BotNotSet | NoUserAllowed | PublicAddressNotSet | OperatorConfigError | PageserversLowered | ShardsLowered | StorageDatabaseUnread | StorageUnverified | ObjectRefused | WebhookNotServing | WebhookError,
  KubeApi | TelegramBotApi
> {
  yield* ensureBotChecksumKey;
  const { bot, settings, objects, live, recorded, gvisorReleases, checksum } = yield* checkInstallation(config, options);
  const route = yield* ensureRoute("main", bot.allowedUserIds);
  yield* applyInstallation(objects, options);
  yield* registerBot(settings, "main", bot, route, options);
  const installed = { bot, claude: settings.alasio.claude.existingSecret !== "" };
  if (!nodeAgentRuns(settings)) return { ...installed, cpuBaseline: null };
  const decision = decideCpuBaseline(yield* surveyCpu, recorded);
  // Carried from the installation before this one, as the first apply carried it.
  if (!sameBaseline(decision.baseline, recorded)) {
    yield* applyInstallation(installationObjects(settings, carriedFrom(settings, live, { cpuBaseline: decision.baseline, gvisorReleases, botChecksum: checksum })), options);
  }
  return { ...installed, cpuBaseline: decision };
});
