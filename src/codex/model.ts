import type { v2 } from "../../.types/codex/index.js";
import type { ModelChoice } from "../persistence/conversation-repository.ts";

export const ALASIO_CODEX_MODEL = "gpt-5.6-sol";
export const ALASIO_CODEX_REASONING_EFFORT = "high";

/**
 * The setting alasio adds to every Codex config it passes on: a type rather than an
 * interface, so the config it is part of stays a JSON object to the protocol's types.
 */
export type AlasioCodexModelConfig = {
  readonly model_reasoning_effort: typeof ALASIO_CODEX_REASONING_EFFORT;
};

export function withAlasioCodexModelConfig(): AlasioCodexModelConfig;
export function withAlasioCodexModelConfig<Config extends object>(config: Config): Config & AlasioCodexModelConfig;
export function withAlasioCodexModelConfig(config: object = {}): AlasioCodexModelConfig {
  return {
    ...config,
    model_reasoning_effort: ALASIO_CODEX_REASONING_EFFORT,
  };
}

/** The model and effort for a Codex turn: the conversation's /model choice, else the pins. */
export function resolveCodexModelChoice(choice: ModelChoice | null = null): ModelChoice {
  if (choice?.model) {
    return { model: choice.model, effort: choice.effort ?? null };
  }
  return { model: ALASIO_CODEX_MODEL, effort: ALASIO_CODEX_REASONING_EFFORT };
}

/** A turn's model as the login allows it: `choice`, or the login's own where it has not that model, and what to tell the operator of it. */
export interface LoginModelChoice {
  readonly choice: ModelChoice;
  /** Why the turn runs on another model than chosen, where it does. */
  readonly notice: string | null;
}

/**
 * The model and effort a turn runs on, of `wanted` (the conversation's /model choice, or
 * alasio's pins), on a login whose app-server offers `offered` (model/list): `wanted` where
 * the login has its model, or where nothing is offered, as when the list could not be read;
 * otherwise the login's default model, at `wanted`'s effort where that model has it and its
 * own default otherwise, saying so, as a login may lack a model (a plan's, or an account's
 * through a relay) and a turn on it would fail.
 */
export function choiceForLogin(wanted: ModelChoice, offered: readonly v2.Model[]): LoginModelChoice {
  const has = (model: v2.Model, name: string) => model.model === name || model.id === name;
  if (offered.length === 0 || !wanted.model || offered.some((model) => has(model, wanted.model ?? ""))) return { choice: wanted, notice: null };
  const fallback = offered.find((model) => model.isDefault) ?? offered[0];
  if (!fallback) return { choice: wanted, notice: null };
  const efforts: readonly string[] = fallback.supportedReasoningEfforts.map(({ reasoningEffort }) => reasoningEffort);
  const effort = wanted.effort && efforts.includes(wanted.effort) ? wanted.effort : fallback.defaultReasoningEffort;
  return {
    choice: { model: fallback.model, effort },
    notice: `This Codex login doesn't offer ${wanted.model}, so Codex used ${fallback.model}, the login's default (at ${effort} effort). Use /model to choose another.`,
  };
}
