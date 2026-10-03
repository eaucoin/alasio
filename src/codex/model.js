export const ALASIO_CODEX_MODEL = "gpt-5.6-terra";
export const ALASIO_CODEX_REASONING_EFFORT = "high";

export function withAlasioCodexModelConfig(config = {}) {
  return {
    ...config,
    model_reasoning_effort: ALASIO_CODEX_REASONING_EFFORT,
  };
}

/** The model and effort for a Codex turn: the conversation's /model choice, else the pins. */
export function resolveCodexModelChoice(choice = null) {
  if (choice?.model) {
    return { model: choice.model, effort: choice.effort ?? null };
  }
  return { model: ALASIO_CODEX_MODEL, effort: ALASIO_CODEX_REASONING_EFFORT };
}
