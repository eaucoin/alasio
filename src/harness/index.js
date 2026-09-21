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

/**
 * Resolve the folder a conversation works in, or null until one is chosen.
 */
export function resolveWorkingDirectory(store, conversationId) {
  const workingDirectory = store?.getWorkingDirectory?.(conversationId);
  return typeof workingDirectory === "string" && workingDirectory.trim() ? workingDirectory : null;
}

export const NO_SERVICE_MOUNTED = "No service is mounted. Use /service to choose Codex or Claude.";
export const NO_WORKSPACE_MOUNTED = "No folder is mounted. Use /workspace to choose or create one.";

const FACTORIES = {
  [CODEX_HARNESS]: createCodexHarness,
  [CLAUDE_HARNESS]: createClaudeHarness,
};

/**
 * Registry of harness adapters. Adapters are bound to one folder, so one is
 * created lazily per (harness, working directory) pair and cached; overrides
 * (test doubles) stand in for every folder of their harness but still require
 * a mounted folder so gating behaves the same as production.
 */
export function createHarnessRegistry({ config = {}, overrides = {} } = {}) {
  const adapters = new Map();
  const getFor = (name, workingDirectory) => {
    if (!isHarnessName(name)) {
      throw new Error(`Unknown harness: ${String(name)}`);
    }
    if (typeof workingDirectory !== "string" || !workingDirectory) {
      throw new Error(NO_WORKSPACE_MOUNTED);
    }
    if (overrides[name]) {
      return overrides[name];
    }
    const key = `${name}\0${workingDirectory}`;
    let adapter = adapters.get(key);
    if (!adapter) {
      adapter = FACTORIES[name]({ workingDirectory });
      adapters.set(key, adapter);
    }
    return adapter;
  };
  return {
    names: HARNESS_NAMES,
    getFor,
    /**
     * Adapter for a harness in the deployment's pre-mounted folder; only meaningful
     * when WORKING_DIRECTORY is configured.
     */
    get(name) {
      return getFor(name, config.workingDirectory);
    },
    forConversation(store, conversationId) {
      const name = resolveHarnessName(store, conversationId);
      const workingDirectory = resolveWorkingDirectory(store, conversationId);
      if (!name || !workingDirectory) {
        return null;
      }
      return getFor(name, workingDirectory);
    },
    requireForConversation(store, conversationId) {
      if (!resolveHarnessName(store, conversationId)) {
        throw new Error(NO_SERVICE_MOUNTED);
      }
      const adapter = this.forConversation(store, conversationId);
      if (!adapter) {
        throw new Error(NO_WORKSPACE_MOUNTED);
      }
      return adapter;
    },
    shutdownAll() {
      for (const adapter of [...new Set([...adapters.values(), ...Object.values(overrides)])]) {
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
