/**
 * Installing alasio from the operator's config: its install configuration, with the
 * Secrets alasio names as the cluster has them, made into its objects (its namespace,
 * which its Secrets are in before anything else of it is, and its manifests), applied,
 * and waited for.
 */
import type { KubernetesObject, V1Namespace } from "@kubernetes/client-node";
import { Effect, Result, Schema } from "effect";

import { installConfigOf, type OperatorConfig, OperatorConfigError } from "./config.ts";
import { KubeApi, type KubeApiError } from "./kube/api.ts";
import { type ApplyError, applyInstallation, labelled } from "./kube/apply.ts";
import type { WaitOptions } from "./kube/rollout.ts";
import { labels, NAMESPACE } from "./manifests/common.ts";
import type { InstallConfig } from "./manifests/config.ts";
import { manifests } from "./manifests/index.ts";
import { readClaudeToken, readTelegramBot, type TelegramBot } from "./secrets.ts";

/** alasio's namespace. */
const NAMESPACE_OBJECT: V1Namespace = { apiVersion: "v1", kind: "Namespace", metadata: { name: NAMESPACE, labels: labels("alasio") } };

/** The objects of the installation `config` describes. */
export function installationObjects(config: InstallConfig): readonly KubernetesObject[] {
  return [NAMESPACE_OBJECT, ...manifests(config)];
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
  const install = installConfigOf(config.install, { claude });
  if (Result.isFailure(install)) return yield* new OperatorConfigError({ path: config.path, reason: install.failure });
  return install.success;
});

/** What an installation came to: its bot, and whether Claude Code has a token. */
export interface Installed {
  readonly bot: TelegramBot;
  readonly claude: boolean;
}

/** Applies the installation `config` describes, once its bot is set, and waits until it runs. */
export const install = Effect.fnUntraced(function*(
  config: OperatorConfig,
  options: WaitOptions,
): Effect.fn.Return<Installed, ApplyError | BotNotSet | OperatorConfigError, KubeApi> {
  const bot = yield* readTelegramBot;
  if (!bot) return yield* new BotNotSet();
  const settings = yield* installConfig(config);
  yield* applyInstallation(installationObjects(settings), options);
  return { bot, claude: settings.alasio.claude.existingSecret !== "" };
});
