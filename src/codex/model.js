export const ALASIO_CODEX_MODEL = "gpt-5.6-sol";
export const ALASIO_CODEX_REASONING_EFFORT = "high";

export function withAlasioCodexModelConfig(config = {}) {
  return {
    ...config,
    model_reasoning_effort: ALASIO_CODEX_REASONING_EFFORT,
  };
}
