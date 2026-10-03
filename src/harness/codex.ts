import type { v2 } from "../../.types/codex/index.js";
import {
  type CodexScope,
  executeCodexTurn,
  forkCodexSession,
  shutdownCodexRuntime,
  startFreshCodexSession,
  warmCodexSession,
} from "../codex/runtime.ts";
import { resolveCodexModelChoice } from "../codex/model.ts";
import { type CodexListingScope, createCodexSessionApi, folderListingScope } from "../codex/sessions.ts";
import type { SessionFsCodex } from "../codex/sessionfs.ts";
import type { SessionFilesystems } from "../sandbox/index.ts";
import { parseWorkspace } from "../workspace/kind.ts";
import { createLogger } from "../shared/log.ts";
import type { Harness, HarnessOptions, ModelOption } from "./index.ts";
import { CODEX_HARNESS, harnessDisplayName } from "./names.ts";

const log = createLogger("codex-harness");

/** The app-server's model entry, in the shape /model shows for either harness. */
function toModelOption(model: v2.Model): ModelOption {
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

const errorText = (error: unknown): string => (error instanceof Error ? error.message : String(error));

/** A session-filesystem workspace's volume, with the Sandbox and the app-server that serve it. */
interface ServedSessionFs {
  readonly volumeId: string;
  readonly sandbox: SessionFilesystems;
  readonly sessionFsCodex: SessionFsCodex;
}

/** What serves the workspace at `workingDirectory` if it is a session filesystem; null for a folder. */
function servedSessionFs(
  workingDirectory: string,
  sandbox: SessionFilesystems | null,
  sessionFsCodex: SessionFsCodex | null,
): ServedSessionFs | null {
  const workspace = parseWorkspace(workingDirectory);
  if (workspace?.kind !== "sessionfs") {
    return null;
  }
  if (!(sandbox && sessionFsCodex)) {
    throw new Error("this conversation's workspace is a session filesystem, which this deployment does not enable");
  }
  return { volumeId: workspace.volumeId, sandbox, sessionFsCodex };
}

/**
 * Codex harness adapter over the app-server runtime, with its sessions as the
 * app-server reports them. With Codex's rollouts kept in Neon (see
 * codex/rollouts/), a thread's rollout files missing here are written back
 * before it is resumed or forked, and a turn's thread is mirrored before its
 * response is final.
 *
 * A folder workspace runs on the shared app-server with the operator's Codex. A
 * session-filesystem workspace runs on the session-filesystem app-server
 * (`sessionFsCodex`, codex/sessionfs.ts), in the workspace's harness directory, reaching
 * the workspace only through its bayma; its rollouts are that app-server's home's,
 * mirrored by `sessionFsCodexRollouts`.
 */
export function createCodexHarness({
  workingDirectory,
  codexRollouts = null,
  sandbox = null,
  sessionFsCodex = null,
  sessionFsCodexRollouts = null,
}: HarnessOptions): Harness {
  const sessionFs = servedSessionFs(workingDirectory, sandbox, sessionFsCodex);
  const rollouts = sessionFs ? sessionFsCodexRollouts : codexRollouts;
  const directory = sessionFs ? sessionFs.sandbox.harnessDirectory(sessionFs.volumeId) : workingDirectory;

  // What each call runs against: `scope()` for a thread's work (a session filesystem's
  // brings its Sandbox up; a folder's is built by the runtime), `listingScope()` for
  // thread and model lists and goals, which need no Sandbox.
  const scope = sessionFs
    ? async (): Promise<CodexScope> => sessionFs.sessionFsCodex.scope({ directory, bayma: (await sessionFs.sandbox.ensureSession(sessionFs.volumeId)).bayma })
    : null;
  const listingScope = sessionFs
    ? async (): Promise<CodexListingScope> => await sessionFs.sessionFsCodex.listingScope({ directory })
    : folderListingScope(workingDirectory);

  async function ensureRollouts(sessionId: string | null | undefined): Promise<void> {
    if (!rollouts || !sessionId) return;
    try {
      await rollouts.restore([sessionId]);
    } catch (error) {
      log.warn(`could not check Neon for the rollouts of ${sessionId}: ${errorText(error)}`);
    }
  }

  async function flushRollouts(sessionId: string | null | undefined): Promise<void> {
    if (!rollouts || !sessionId) return;
    try {
      await rollouts.flush(sessionId);
    } catch (error) {
      log.warn(`the turn's response goes on before ${sessionId} was mirrored: ${errorText(error)}`);
    }
  }

  /** The app-server goals are read and set through, and the directory and env each call names. */
  async function goalScope() {
    const { cwd, codexEnv, client } = await listingScope();
    return { client, cwd, env: codexEnv };
  }

  return {
    name: CODEX_HARNESS,
    displayName: harnessDisplayName(CODEX_HARNESS),
    supportsGoals: true,
    supportsWarmup: true,
    supportsSteer: true,
    sessions: createCodexSessionApi({
      workingDirectory,
      listingScope,
      fork: (params) => forkCodexSession({ ...params, scope }),
      beforeFork: ensureRollouts,
    }),
    /** Codex's goals on a thread, for operator/goal-control.ts. */
    goals: {
      async read({ threadId }) {
        const { client, cwd, env } = await goalScope();
        return (await client.getGoal({ threadId, cwd, env }))?.goal ?? null;
      },
      async set({ threadId, objective, status }) {
        const { client, cwd, env } = await goalScope();
        return (await client.setGoal({ threadId, objective, status, cwd, env }))?.goal ?? null;
      },
      async clear({ threadId }) {
        const { client, cwd, env } = await goalScope();
        return await client.clearGoal({ threadId, cwd, env });
      },
      async waitForTurnId(threadId, timeoutMs) {
        const { client } = await listingScope();
        return await client.waitForTurnId(threadId, timeoutMs);
      },
    },
    async startFreshSession({ threadKey }) {
      return await startFreshCodexSession({ threadKey, workingDirectory, scope });
    },
    async warmSession({ sessionId, threadKey }) {
      await ensureRollouts(sessionId);
      return await warmCodexSession({ sessionId, threadKey, workingDirectory, scope });
    },
    async executeTurn(params) {
      await ensureRollouts(params.resumeSession);
      return await executeCodexTurn({ ...params, workingDirectory, beforeResponseComplete: flushRollouts, scope });
    },
    async listModels() {
      const { cwd, codexEnv, client } = await listingScope();
      const models = await client.listModels({ env: codexEnv, cwd });
      return models.filter((model) => !model.hidden).map(toModelOption);
    },
    /** What a turn runs on when no /model choice is stored. */
    defaultModelChoice() {
      return resolveCodexModelChoice(null);
    },
    shutdown() {
      shutdownCodexRuntime();
    },
  };
}
