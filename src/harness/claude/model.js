const CLAUDE_EFFORT_LEVELS = new Set(["low", "medium", "high", "xhigh", "max"]);

/**
 * The model and effort Telegram turns run on, pinned here the way the Codex
 * harness pins its own. A pin rather than the CLI default means a turn does
 * not silently change model when the operator's settings change, and — because
 * the model is sent with every turn — a long-running session moves to the pin
 * on its next turn instead of staying on whatever it started with.
 */
export const ALASIO_CLAUDE_MODEL = "claude-opus-5-5";
export const ALASIO_CLAUDE_EFFORT = "high";

export function getClaudeModel(env = process.env) {
  const value = env.ALASIO_CLAUDE_MODEL?.trim();
  return value ? value : ALASIO_CLAUDE_MODEL;
}

export function getClaudeEffort(env = process.env) {
  const value = env.ALASIO_CLAUDE_EFFORT?.trim().toLowerCase();
  if (value && CLAUDE_EFFORT_LEVELS.has(value)) {
    return value;
  }
  return CLAUDE_EFFORT_LEVELS.has(ALASIO_CLAUDE_EFFORT) ? ALASIO_CLAUDE_EFFORT : null;
}

export function getClaudeBinaryOverride(env = process.env) {
  const value = env.ALASIO_CLAUDE_BIN?.trim();
  return value ? value : null;
}
