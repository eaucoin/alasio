/**
 * Codex's sessions for the operator's session panels, as Codex's own
 * app-server reports them: the threads whose session ran in the working
 * directory, their turns as rewind points, and rewind as Codex's fork before
 * a turn. alasio reads and writes none of Codex's files for them.
 */
import type { v2 } from "../../.types/codex/index.js";
import { Effect } from "effect";

import type { HarnessSessions } from "../harness/index.ts";
import type { EffectRunner } from "../shared/effects.ts";
import { withLogScope } from "../shared/log.ts";
import { SESSIONS_PER_PAGE } from "../shared/runtime-constants.ts";
import { dateLabel, sessionLabel } from "../shared/session-labels.ts";
import type { AppServer } from "./app-server/client.ts";
import { type CodexEnv, buildCodexEnv } from "./env.ts";
import type { CodexScopeError, CodexSessionError } from "./runtime.ts";

type AgentMessage = Extract<v2.ThreadItem, { type: "agentMessage" }>;
type TextInput = Extract<v2.UserInput, { type: "text" }>;

/** What the operator wrote to start a turn. */
function promptOf(turn: v2.Turn): string {
  return (turn.items ?? [])
    .filter((item) => item.type === "userMessage")
    .flatMap((item) => item.content ?? [])
    .filter((part): part is TextInput => part.type === "text" && typeof part.text === "string")
    .map((part) => part.text)
    .join("\n")
    .trim();
}

/** What Codex last said in a turn, or "" if it said nothing. */
function answerOf(turn: v2.Turn): string {
  const messages = (turn.items ?? []).filter((item): item is AgentMessage => item.type === "agentMessage" && Boolean(item.text?.trim()));
  return messages.at(-1)?.text.trim() ?? "";
}

/** The app-server and directory threads are listed from, and the environment the app-server runs with. */
export interface CodexListingScope {
  readonly cwd: string;
  readonly codexEnv: CodexEnv;
  readonly appServer: AppServer;
}

/** What the session panels read of a listing scope: its directory and env, and its app-server's thread and turn lists. */
export interface SessionListingScope extends Omit<CodexListingScope, "appServer"> {
  readonly appServer: Pick<AppServer, "listThreads" | "listTurns">;
}

/** A thread forked before one of its turns, for the conversation `threadKey`: the new thread's id. */
export type ForkSession = (params: { readonly sessionId: string; readonly beforeTurnId: string; readonly threadKey: string }) => Effect.Effect<string, CodexSessionError>;

/** The listing scope of a folder workspace: the operator's app-server, in the folder itself. */
export function folderListingScope(workingDirectory: string, appServer: AppServer): Effect.Effect<CodexListingScope> {
  return Effect.sync(() => ({ cwd: workingDirectory, codexEnv: buildCodexEnv(), appServer }));
}

export interface CodexSessionApiOptions {
  /** The app-server and directory the threads are listed from (`{ cwd, codexEnv, appServer }`). */
  readonly listingScope: Effect.Effect<SessionListingScope, CodexScopeError>;
  /** Makes a rewind's thread. */
  readonly fork: ForkSession;
  /** Runs first with the thread forked from, for the rollout store to write back any file of it missing here. */
  readonly beforeFork?: ((sessionId: string) => Effect.Effect<void>) | undefined;
  /** What runs the panels' effects for their promise interface. */
  readonly effects: EffectRunner<never>;
}

/**
 * The session panels' view of the threads `listingScope` lists, which a folder workspace's
 * harness and a session filesystem's each give: listings, previews, rewind points, and
 * rewind, as promises run by `effects`.
 */
export function createCodexSessionApi({ listingScope, fork, beforeFork, effects }: CodexSessionApiOptions): HarnessSessions {
  const run = <A, E>(effect: Effect.Effect<A, E>): Promise<A> => effects.runPromise(effect.pipe(withLogScope("codex-sessions")));

  const listAll = Effect.flatMap(listingScope, ({ cwd, codexEnv, appServer }) => appServer.listThreads({ env: codexEnv, cwd }));

  /** A thread's turns, newest first; none for a thread Codex cannot read. */
  const turnsOf = (sessionId: string): Effect.Effect<v2.Turn[]> =>
    Effect.flatMap(listingScope, ({ cwd, codexEnv, appServer }) => appServer.listTurns({ threadId: sessionId, env: codexEnv, cwd })).pipe(
      Effect.catch((error) => Effect.logWarning(`could not list the turns of ${sessionId}: ${error.message}`).pipe(Effect.as([]))),
    );

  /** The operator's prompts, newest first, each a point to rewind to: its uuid is its turn's id. */
  const sessionMessages = (sessionId: string) =>
    Effect.map(turnsOf(sessionId), (turns) =>
      turns
        .map((turn) => ({ turn, text: promptOf(turn) }))
        .filter(({ text }) => text)
        .map(({ turn, text }, index) => ({
          index: -(index + 1),
          timestamp: turn.startedAt ? new Date(turn.startedAt * 1000).toISOString() : "",
          text,
          uuid: turn.id,
        })));

  return {
    listSessions: (page = 1) =>
      run(Effect.map(listAll, (all) => {
        const startIdx = (page - 1) * SESSIONS_PER_PAGE;
        return all.slice(startIdx, startIdx + SESSIONS_PER_PAGE).map((thread) => ({
          uuid: thread.id,
          timestamp: dateLabel(thread.updatedAt * 1000),
          label: sessionLabel(thread.name || thread.preview || thread.id),
        }));
      })),

    getTotalSessionPages: () => run(Effect.map(listAll, (all) => Math.ceil(all.length / SESSIONS_PER_PAGE) || 1)),

    getSessionByNumber: (num) => run(Effect.map(listAll, (all) => all[num - 1]?.id ?? null)),

    getSessionLastMessage: (sessionId) =>
      run(Effect.map(turnsOf(sessionId), (turns) => turns.map(answerOf).find((answer) => answer) ?? null)),

    listSessionMessages: (sessionId) => run(sessionMessages(sessionId)),

    getTotalRewindPages: (sessionId) =>
      run(Effect.map(sessionMessages(sessionId), (messages) => Math.ceil(messages.length / SESSIONS_PER_PAGE) || 1)),

    /** A new thread holding the session's history before the turn `beforeUuid`, for the conversation `threadKey`. */
    createForkedSession: (sessionId, beforeUuid, { threadKey }) =>
      run(Effect.gen(function*() {
        if (beforeFork) yield* beforeFork(sessionId);
        return yield* fork({ sessionId, beforeTurnId: beforeUuid, threadKey });
      }).pipe(
        Effect.catch((error) => Effect.logWarning(`could not fork ${sessionId} before turn ${beforeUuid}: ${error.message}`).pipe(Effect.as(null))),
      )),
  };
}
