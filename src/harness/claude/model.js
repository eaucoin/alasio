const CLAUDE_EFFORT_LEVELS = new Set(["low", "medium", "high", "xhigh", "max"]);

/**
 * Claude model selection is optional: when unset, the Claude Code CLI default
 * (the operator's configured model) is used so Telegram turns match interactive
 * Claude Code sessions on the same account.
 */
export function getClaudeModel(env = process.env) {
  const value = env.ALASIO_CLAUDE_MODEL?.trim();
  return value ? value : null;
}

export function getClaudeEffort(env = process.env) {
  const value = env.ALASIO_CLAUDE_EFFORT?.trim().toLowerCase();
  return value && CLAUDE_EFFORT_LEVELS.has(value) ? value : null;
}

export function getClaudeBinaryOverride(env = process.env) {
  const value = env.ALASIO_CLAUDE_BIN?.trim();
  return value ? value : null;
}
