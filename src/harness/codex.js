import {
  executeCodexTurn,
  shutdownCodexRuntime,
  startFreshCodexSession,
  warmCodexSession,
} from "../codex/runtime.js";
import { codexAppServerClient } from "../codex/app-server/client.js";
import { buildCodexEnv } from "../codex/env.js";
import { resolveCodexModelChoice } from "../codex/model.js";
import { createSandboxCodexClient, sandboxCodexEnv } from "../codex/sandbox.js";
import { createCodexSessionApi } from "../codex/sessions.js";
import { parseWorkspace } from "../workspace/kind.js";
import { createLogger } from "../shared/log.js";
import { CODEX_HARNESS, harnessDisplayName } from "./names.js";

const log = createLogger("codex-harness");

/** The app-server's model entry, in the shape /model shows for either harness. */
function toModelOption(model) {
  const efforts = (model.supportedReasoningEfforts ?? [])
    .map((entry) => (typeof entry === "string" ? entry : entry?.reasoningEffort))
    .filter(Boolean);
  return {
    id: model.id ?? model.model,
    label: model.displayName ?? model.id ?? model.model,
    description: model.description ?? "",
    resolvedModel: model.model ?? model.id,
    efforts,
    defaultEffort: model.defaultReasoningEffort ?? null,
    isDefault: Boolean(model.isDefault),
  };
}

const errorText = (error) => (error instanceof Error ? error.message : String(error));

/**
 * Codex harness adapter over the app-server runtime, with its sessions as the
 * app-server reports them. With Codex's rollouts kept in Neon (see
 * codex/rollouts/), a thread's rollout files missing here are written back
 * before it is resumed or forked, and a turn's thread is mirrored before its
 * response is final.
 */
export function createCodexHarness({ workingDirectory, codexRollouts = null, sandbox = null }) {
  const workspace = parseWorkspace(workingDirectory);
  const sandboxed = Boolean(sandbox?.enabled) && workspace?.kind === "sessionfs";

  // A session-filesystem workspace runs Codex inside its gVisor sandbox: one
  // app-server per volume, spawned through the session host, on the gateway.
  // Resolved once and reused across the conversation's turns, like the shared
  // app-server is for folder workspaces. Folder workspaces resolve to null and
  // keep the shared, local app-server untouched.
  let codexSessionPromise = null;
  async function resolveCodexSession() {
    if (!sandboxed) return null;
    if (!codexSessionPromise) {
      codexSessionPromise = (async () => {
        const session = await sandbox.ensureSession(workspace.volumeId, sandboxCodexEnv);
        return { ...session, client: createSandboxCodexClient(session) };
      })();
    }
    return await codexSessionPromise;
  }

  async function ensureRollouts(sessionId) {
    // A sandbox session keeps its rollouts on its own durable volume, not in the
    // host's Codex home, so the Neon mirror does not apply to it.
    if (!codexRollouts || !sessionId || sandboxed) return;
    try {
      await codexRollouts.restore([sessionId]);
    } catch (error) {
      log.warn(`could not check Neon for the rollouts of ${sessionId}: ${errorText(error)}`);
    }
  }

  async function flushRollouts(sessionId) {
    if (!codexRollouts || !sessionId || sandboxed) return;
    try {
      await codexRollouts.flush(sessionId);
    } catch (error) {
      log.warn(`the turn's response goes on before ${sessionId} was mirrored: ${errorText(error)}`);
    }
  }

  return {
    name: CODEX_HARNESS,
    displayName: harnessDisplayName(CODEX_HARNESS),
    supportsGoals: true,
    supportsWarmup: true,
    supportsSteer: true,
    sessions: createCodexSessionApi({ workingDirectory, beforeFork: ensureRollouts }),
    async startFreshSession({ threadKey }) {
      return await startFreshCodexSession({ threadKey, workingDirectory, codexSession: await resolveCodexSession() });
    },
    async warmSession({ sessionId, threadKey }) {
      await ensureRollouts(sessionId);
      return await warmCodexSession({ sessionId, threadKey, workingDirectory, codexSession: await resolveCodexSession() });
    },
    async executeTurn(params) {
      await ensureRollouts(params.resumeSession);
      return await executeCodexTurn({ ...params, workingDirectory, beforeResponseComplete: flushRollouts, codexSession: await resolveCodexSession() });
    },
    async listModels() {
      const codexSession = await resolveCodexSession();
      const client = codexSession?.client ?? codexAppServerClient;
      const env = codexSession ? sandboxCodexEnv({ bearer: codexSession.bearer }) : buildCodexEnv();
      const cwd = codexSession ? "/workspace" : workingDirectory;
      const models = await client.listModels({ env, cwd });
      return models.filter((model) => !model.hidden).map(toModelOption);
    },
    /** What a turn runs on when no /model choice is stored. */
    defaultModelChoice() {
      return resolveCodexModelChoice(null);
    },
    async shutdown() {
      shutdownCodexRuntime();
      if (codexSessionPromise) {
        const session = await codexSessionPromise.catch(() => null);
        session?.client?.stop();
        await sandbox.releaseSession(workspace.volumeId, { stop: true }).catch(() => {});
      }
    },
  };
}
