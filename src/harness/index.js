import { createClaudeHarness } from "./claude/index.js";
import { createCodexHarness } from "./codex.js";
import {
  CLAUDE_HARNESS,
  CODEX_HARNESS,
  HARNESS_NAMES,
  getDefaultHarness,
  harnessDisplayName,
  isHarnessName,
  normalizeHarnessName,
} from "./names.js";

export {
  CLAUDE_HARNESS,
  CODEX_HARNESS,
  HARNESS_NAMES,
  getDefaultHarness,
  harnessDisplayName,
  isHarnessName,
  normalizeHarnessName,
};

/**
 * Resolve the active harness name for a conversation from any store shape.
 * Returns null when nothing is mounted; callers gate on that instead of
 * assuming a default.
 */
export function resolveHarnessName(store, conversationId) {
  const harness = store?.getActiveHarness?.(conversationId);
  return isHarnessName(harness) ? harness : null;
}

export const NO_SERVICE_MOUNTED = "No service is mounted. Use /service to choose Codex or Claude Code.";

/**
 * Registry of harness adapters bound to one working directory.
 */
export function createHarnessRegistry({ config, overrides = {} }) {
  const workingDirectory = config.workingDirectory;
  const adapters = {
    [CODEX_HARNESS]: overrides[CODEX_HARNESS] ?? createCodexHarness({ workingDirectory }),
    [CLAUDE_HARNESS]: overrides[CLAUDE_HARNESS] ?? createClaudeHarness({ workingDirectory }),
  };
  return {
    names: HARNESS_NAMES,
    get(name) {
      const adapter = adapters[name];
      if (!adapter) {
        throw new Error(`Unknown harness: ${String(name)}`);
      }
      return adapter;
    },
    forConversation(store, conversationId) {
      const name = resolveHarnessName(store, conversationId);
      return name ? adapters[name] : null;
    },
    requireForConversation(store, conversationId) {
      const adapter = this.forConversation(store, conversationId);
      if (!adapter) {
        throw new Error(NO_SERVICE_MOUNTED);
      }
      return adapter;
    },
    shutdownAll() {
      for (const adapter of Object.values(adapters)) {
        try {
          adapter.shutdown();
        } catch {
          // Shutdown is best-effort per harness.
        }
      }
    },
  };
}

/**
 * Interrupt whichever harness owns the active turn for a thread key.
 */
export async function interruptActiveTurn(activeQueries, threadKey, reason = "Interrupted from Telegram") {
  const activeQuery = activeQueries.get(threadKey);
  if (!activeQuery) {
    return false;
  }
  await activeQuery.abort(reason);
  return true;
}
