/**
 * Installing alasio from the operator's config: its install configuration, with the
 * Secrets alasio names as the cluster has them, made into its objects (its namespace,
 * which its Secrets are in before anything else of it is, and its manifests), checked
 * against the cluster's autoscalers and its object store's layout and scale, applied, and
 * waited for.
 */
import type { KubernetesObject, V1ConfigMap, V1Namespace } from "@kubernetes/client-node";
import { Effect, Result, Schema } from "effect";

import { installConfigOf, type OperatorConfig, OperatorConfigError, sharedCluster } from "./config.ts";
import { kind, KubeApi, type KubeApiError } from "./kube/api.ts";
import { type ApplyError, applyInstallation, labelled } from "./kube/apply.ts";
import { type AutoscalerMismatch, checkAutoscalers } from "./kube/autoscalers.ts";
import { checkObjectStoreLayout, checkObjectStoreScale, type ObjectStoreScaleRefused } from "./kube/object-store-scale.ts";
import type { WaitOptions } from "./kube/rollout.ts";
import { labels, NAMESPACE } from "./manifests/common.ts";
import type { InstallConfig } from "./manifests/config.ts";
import { type Carried, carriedFrom, manifests, NOTHING_CARRIED, SANDBOX_TEMPLATES } from "./manifests/index.ts";
import { readClaudeToken, readTelegramBot, type TelegramBot } from "./secrets.ts";

/** alasio's namespace. */
const NAMESPACE_OBJECT: V1Namespace = { apiVersion: "v1", kind: "Namespace", metadata: { name: NAMESPACE, labels: labels("alasio") } };

/** The objects of the installation `config` describes, with what it carries forward from the one it replaces. */
export function installationObjects(config: InstallConfig, carried: Carried = NOTHING_CARRIED): readonly KubernetesObject[] {
  return [NAMESPACE_OBJECT, ...manifests(config, carried)];
}

/** What the installation `config` describes carries forward from the one the cluster runs, as its templates' ConfigMap says. */
export const carried = Effect.fnUntraced(function*(config: InstallConfig): Effect.fn.Return<Carried, KubeApiError, KubeApi> {
  const live = yield* (yield* KubeApi).get<V1ConfigMap>({ ...kind("ConfigMap"), namespace: NAMESPACE, name: SANDBOX_TEMPLATES });
  return carriedFrom(config, live?.data?.["templates.json"]);
});

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

/** What an installation came to: its bot, and whether Claude Code has a token. */
export interface Installed {
  readonly bot: TelegramBot;
  readonly claude: boolean;
}

/**
 * The installation `config` describes, with its bot, refused where applying it would not do:
 * with no bot set, against the cluster's own autoscalers, or its object store's layout and scale.
 */
export const checkInstallation = Effect.fnUntraced(function*(
  config: OperatorConfig,
): Effect.fn.Return<
  { readonly bot: TelegramBot; readonly settings: InstallConfig; readonly objects: readonly KubernetesObject[] },
  KubeApiError | AutoscalerMismatch | ObjectStoreScaleRefused | BotNotSet | OperatorConfigError,
  KubeApi
> {
  const bot = yield* readTelegramBot;
  if (!bot) return yield* new BotNotSet();
  const settings = yield* installConfig(config);
  const objects = installationObjects(settings, yield* carried(settings));
  yield* checkAutoscalers(objects, settings);
  yield* checkObjectStoreLayout(objects);
  yield* checkObjectStoreScale(objects);
  return { bot, settings, objects };
});

/** Applies the installation `config` describes, once its bot is set, and waits until it runs. */
export const install = Effect.fnUntraced(function*(
  config: OperatorConfig,
  options: WaitOptions,
): Effect.fn.Return<Installed, ApplyError | AutoscalerMismatch | ObjectStoreScaleRefused | BotNotSet | OperatorConfigError, KubeApi> {
  const { bot, settings, objects } = yield* checkInstallation(config);
  yield* applyInstallation(objects, options);
  return { bot, claude: settings.alasio.claude.existingSecret !== "" };
});
