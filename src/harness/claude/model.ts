import type { EffortLevel } from "@anthropic-ai/claude-agent-sdk";

import type { ModelChoice } from "../../persistence/conversation-repository.ts";

const CLAUDE_EFFORT_LEVELS: ReadonlySet<string> = new Set(["low", "medium", "high", "xhigh", "max"] satisfies EffortLevel[]);

function isClaudeEffort(value: string): value is EffortLevel {
  return CLAUDE_EFFORT_LEVELS.has(value);
}

/**
 * The model and effort Telegram turns run on, pinned here the way the Codex
 * harness pins its own. A pin rather than the CLI default means a turn does
 * not silently change model when the operator's settings change, and — because
 * the model is sent with every turn — a long-running session moves to the pin
 * on its next turn instead of staying on whatever it started with.
 */
// "[1m]" selects the 1M-token context variant; the bare id is a smaller window.
export const ALASIO_CLAUDE_MODEL = "claude-opus-5-5[1m]";
export const ALASIO_CLAUDE_EFFORT = "high";

/** The model for a turn: the conversation's /model choice, then the env override, then the pin. */
export function getClaudeModel(env: Readonly<NodeJS.ProcessEnv> = process.env, choice: ModelChoice | null = null): string {
  if (choice?.model) {
    return choice.model;
  }
  const value = env["ALASIO_CLAUDE_MODEL"]?.trim();
  return value ? value : ALASIO_CLAUDE_MODEL;
}

export function getClaudeEffort(env: Readonly<NodeJS.ProcessEnv> = process.env, choice: ModelChoice | null = null): EffortLevel | null {
  if (choice?.model) {
    // A chosen model carries its own effort; null means the model has none.
    return choice.effort && isClaudeEffort(choice.effort) ? choice.effort : null;
  }
  const value = env["ALASIO_CLAUDE_EFFORT"]?.trim().toLowerCase();
  if (value && isClaudeEffort(value)) {
    return value;
  }
  return isClaudeEffort(ALASIO_CLAUDE_EFFORT) ? ALASIO_CLAUDE_EFFORT : null;
}

export function getClaudeBinaryOverride(env: Readonly<NodeJS.ProcessEnv> = process.env): string | null {
  const value = env["ALASIO_CLAUDE_BIN"]?.trim();
  return value ? value : null;
}
