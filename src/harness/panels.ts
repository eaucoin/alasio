/**
 * What the operator's panels reach of a conversation's mounted harness, alasio running none:
 * its sessions, its goals where it has them, and its models, each from where it is kept.
 *
 * - Claude Code's sessions: its transcripts in Neon (claude_sessions), as the Agent SDK's
 *   session helpers read them of the workspace's project, no Claude Code started for it
 *   (./claude/sessions.ts); a rewind's session forked there too, as alasio, and given the
 *   conversation as alasio mounts it.
 * - Codex's threads: its rollouts in Neon, the folder's store or the session filesystems'
 *   (../codex/rollouts/listing.ts), as Codex itself lists them of the workspace's directory; a
 *   rewind's fork, and a thread's goals, of the conversation's pod, bound for it
 *   (HarnessHost.forkSession, HarnessHost.goals), as Codex keeps a thread's goal and makes a fork
 *   in the app-server that runs it.
 * - Models: Codex's of alasio's own app-server, which runs anyway as the one refresher of the
 *   Codex login (../codex/login-refresh.ts); Claude Code's as its CLI lists them, once for the
 *   process's life, as the CLI is pinned with the release.
 *
 * What each panel shows of a workspace is the sessions of its folder's project, or of its session
 * filesystem's directory, and no conversation's beyond them.
 */
import { mkdirSync } from "node:fs";
import { join } from "node:path";

import { Context, Effect, Layer, Option } from "effect";

import { CodexAppServer } from "../codex/app-server/client.ts";
import { buildCodexEnv } from "../codex/env.ts";
import { resolveCodexModelChoice, toModelOption } from "../codex/model.ts";
import { rolloutListing } from "../codex/rollouts/listing.ts";
import type { NeonRolloutStore } from "../codex/rollouts/store.ts";
import { CodexScopeError } from "../codex/runtime.ts";
import { createCodexSessionApi } from "../codex/sessions.ts";
import type { Mount } from "../persistence/conversation-repository.ts";
import { parseWorkspace } from "../workspace/kind.ts";
import type { NeonSessionStore } from "./claude/session-store.ts";
import { createClaudeSessionApi } from "./claude/sessions.ts";
import { getClaudeEffort, getClaudeModel } from "./claude/model.ts";
import { listClaudeModels } from "./claude/models.ts";
import { HarnessHost, type HarnessPanels } from "./host.ts";
import { FolderOnBranch, type HarnessError, type HarnessUnavailable, type ModelOption, NoServiceMounted, NoWorkspaceMounted, UnknownHarness } from "./index.ts";
import { CLAUDE_HARNESS, CODEX_HARNESS, HARNESS_NAMES, harnessDisplayName, isHarnessName } from "./names.ts";

/** What the panels are made with: the stores, where the session filesystems' directories are, and the branch environment alasio is. */
export interface PanelsOptions {
  /** Claude Code's transcripts. */
  readonly sessionStore: NeonSessionStore;
  /** The operator's Codex home's rollouts, and the session filesystems' Codex's. */
  readonly codexRollouts: NeonRolloutStore;
  readonly sessionFsCodexRollouts: NeonRolloutStore;
  /** alasio's state directory, which a session filesystem's harness directory is under, as a pool's pod's is (../sandbox/index.ts harnessDirectory). */
  readonly stateDir: string;
  /** The branch environment alasio is, which gives no harness in a folder; none for main. */
  readonly branch?: string | null | undefined;
  /** Claude Code's models, as its CLI lists them: listClaudeModels, unless a test gives its own. */
  readonly claudeModels?: ((workingDirectory: string) => Effect.Effect<ModelOption[], HarnessError>) | undefined;
  /** Codex's models, as alasio's own app-server lists them, unless a test gives its own. */
  readonly codexModels?: ((workingDirectory: string) => Effect.Effect<ModelOption[], HarnessError>) | undefined;
}

/** The panels of each conversation's mounted harness, as the module says. */
export class Panels extends Context.Service<Panels, {
  readonly names: typeof HARNESS_NAMES;
  /** What the panels reach of the mount's harness; none until both are mounted. */
  readonly forMount: (conversationId: string, mount: Mount) => Effect.Effect<Option.Option<HarnessPanels>, HarnessUnavailable>;
  readonly requireForMount: (conversationId: string, mount: Mount) => Effect.Effect<HarnessPanels, NoServiceMounted | HarnessUnavailable>;
}>()("alasio/harness/Panels") {
  static readonly layer = (options: PanelsOptions): Layer.Layer<Panels, never, HarnessHost | CodexAppServer> => Layer.effect(Panels, makePanels(options));
}

/** The directory a session filesystem's harness runs in, under `stateDir`, as a pool's pod has it. */
export const sessionFsDirectory = (stateDir: string, volumeId: string): string => join(stateDir, "sessionfs", "workspaces", volumeId);

const makePanels = Effect.fnUntraced(function*({ sessionStore, codexRollouts, sessionFsCodexRollouts, stateDir, branch = null, claudeModels = (workingDirectory) => listClaudeModels({ workingDirectory }), codexModels }: PanelsOptions) {
  const host = yield* HarnessHost;
  const appServer = yield* CodexAppServer;
  // Of alasio's own app-server, run in a directory of the workspace's, as the models are the login's, whatever the workspace.
  const listCodexModels = codexModels ?? ((directory: string) =>
    Effect.sync(() => mkdirSync(directory, { recursive: true })).pipe(
      Effect.andThen(appServer.listModels({ env: buildCodexEnv(), cwd: directory })),
      Effect.map((models) => models.filter((model) => !model.hidden).map(toModelOption)),
    ));
  /** Claude Code's models, listed once a process's life, as its CLI is pinned with the release; a listing that failed is tried again as next asked. */
  let claudeListed: ModelOption[] | null = null;
  const claudeModelsOnce = (workingDirectory: string) =>
    claudeListed ? Effect.succeed(claudeListed) : Effect.tap(claudeModels(workingDirectory), (models) => Effect.sync(() => void (claudeListed = models)));

  const panelsFor = (conversationId: string, name: string, workingDirectory: string): Effect.Effect<HarnessPanels, HarnessUnavailable> =>
    Effect.suspend((): Effect.Effect<HarnessPanels, HarnessUnavailable> => {
      if (!isHarnessName(name)) return Effect.fail(new UnknownHarness({ name }));
      const workspace = parseWorkspace(workingDirectory);
      if (branch && workspace?.kind === "folder") return Effect.fail(new FolderOnBranch({ branch }));
      const sessionFs = workspace?.kind === "sessionfs" ? workspace : null;
      const directory = sessionFs ? sessionFsDirectory(stateDir, sessionFs.volumeId) : workingDirectory;
      const of = { conversationId, harness: name, workingDirectory };
      if (name === CLAUDE_HARNESS) {
        return Effect.succeed({
          name,
          displayName: harnessDisplayName(name),
          supportsGoals: false,
          sessions: createClaudeSessionApi({ workingDirectory: directory, store: sessionStore }),
          listModels: () => claudeModelsOnce(directory),
          defaultModelChoice: () => ({ model: getClaudeModel(), effort: getClaudeEffort() }),
        });
      }
      const store = sessionFs && sessionFsCodexRollouts ? sessionFsCodexRollouts : codexRollouts;
      const codexEnv = buildCodexEnv();
      return Effect.succeed({
        name: CODEX_HARNESS,
        displayName: harnessDisplayName(CODEX_HARNESS),
        supportsGoals: true,
        sessions: createCodexSessionApi({
          listingScope: Effect.succeed({ cwd: directory, codexEnv, appServer: rolloutListing(store) }),
          fork: ({ sessionId, beforeTurnId }) =>
            host.forkSession({ ...of, sessionId, beforeUuid: beforeTurnId }).pipe(
              Effect.mapError((cause) => new CodexScopeError({ cause })),
              Effect.flatMap((forked) => (forked === null ? Effect.fail(new CodexScopeError({ cause: new Error(`${sessionId} could not be forked before ${beforeTurnId}`) })) : Effect.succeed(forked))),
            ),
        }),
        goals: host.goals(of),
        listModels: () => listCodexModels(directory),
        defaultModelChoice: () => resolveCodexModelChoice(null),
      });
    });

  const forMount = (conversationId: string, { harness, workingDirectory }: Mount): Effect.Effect<Option.Option<HarnessPanels>, HarnessUnavailable> =>
    harness && workingDirectory ? Effect.asSome(panelsFor(conversationId, harness, workingDirectory)) : Effect.succeedNone;

  return Panels.of({
    names: HARNESS_NAMES,
    forMount,
    requireForMount: (conversationId, mount) =>
      mount.harness
        ? forMount(conversationId, mount).pipe(Effect.flatMap(Option.match({ onNone: () => Effect.fail(new NoWorkspaceMounted()), onSome: Effect.succeed })))
        : Effect.fail(new NoServiceMounted()),
  });
});
