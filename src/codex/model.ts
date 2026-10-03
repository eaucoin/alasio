import type { ModelChoice } from "../persistence/conversation-repository.ts";

export const ALASIO_CODEX_MODEL = "gpt-5.6-sol";
export const ALASIO_CODEX_REASONING_EFFORT = "high";

/** The setting alasio adds to every Codex config it passes on. */
export interface AlasioCodexModelConfig {
  readonly model_reasoning_effort: typeof ALASIO_CODEX_REASONING_EFFORT;
}

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
