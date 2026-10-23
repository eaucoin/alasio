/**
 * How a Codex turn reaches Codex: through its app-server, whose turns can be steered and
 * attached to, as a stream of the turn's events.
 */
import { Effect, Schema, type Scope, type Stream } from "effect";

import type { ModelChoice } from "../persistence/conversation-repository.ts";
import type { StoreError } from "../persistence/sql.ts";
import type { TurnFenced, TurnPersistence } from "../persistence/store.ts";
import type { AppServer, AppServerEventsError } from "./app-server/client.ts";
import type { AppServerEvent } from "./app-server/protocol.ts";
import type { AppServerRequestError, AppServerStartError } from "./app-server/rpc-client.ts";
import type { NoActiveTurn, StaleTurnCleanupError, ThreadIdMissing } from "./app-server/thread-client.ts";
import type { CodexEnv } from "./env.ts";
import { choiceForLogin, resolveCodexModelChoice } from "./model.ts";
import type { CodexThreadConfig } from "./thread-config.ts";
import type { TurnTimer } from "./turn-timing.ts";

/** What a turn reports: the app-server's notifications of it, as events. */
export type CodexEvent = AppServerEvent;

/** What was asked of the transport is something it does not do. */
export class CodexTransportRefused extends Schema.TaggedError<CodexTransportRefused>()("CodexTransportRefused", {
  message: Schema.String,
}) {}

/** How a turn's events stop before its end. */
export type CodexEventsError = AppServerEventsError;

/** How opening a turn's events fails. */
export type CodexOpenError = AppServerStartError | ThreadIdMissing | StaleTurnCleanupError | CodexTransportRefused | StoreError | TurnFenced;

/** A turn's events as they stream, with the session and turn they belong to, as far as they are known. */
export interface CodexEventStream {
  readonly sessionId: string | null | undefined;
  readonly turnId: string | null;
  readonly events: Stream.Stream<CodexEvent, CodexEventsError>;
  /** What the operator is told of the turn besides its reply: why it runs on another model than chosen. */
  readonly notice?: string | null | undefined;
}

/** Where a transport records the session a turn runs in. */
export type TurnSessionStore = Pick<TurnPersistence, "updatePendingSessionId" | "updateActiveTurnSessionId">;

/** A Codex session's location: its app-server, and the directory, environment, and config it runs with. */
export interface CodexSessionOptions {
  readonly threadKey: string;
  readonly workingDirectory: string;
  readonly codexEnv: CodexEnv;
  readonly codexConfig: CodexThreadConfig;
  readonly appServer: AppServer;
}

/** A turn to run: the prompt, the session to resume if any, and where its progress is recorded. */
export interface CodexStreamParams extends CodexSessionOptions {
  readonly resumeSession: string | null | undefined;
  readonly prompt: string;
  /** The model the conversation chose for Codex; null for alasio's default. */
  readonly modelChoice: ModelChoice | null;
  readonly persistence: TurnSessionStore;
  readonly pendingResponseId: string;
  readonly turnTimer: TurnTimer;
  /** Run just before the prompt is sent, after which the agent may act on it. */
  readonly onPromptDispatched?: Effect.Effect<void> | undefined;
}

/** A turn already running in the app-server (a goal's), to attach to. */
export interface AttachedCodexStreamParams extends CodexStreamParams {
  readonly sessionId: string | null | undefined;
  readonly turnId: string | null | undefined;
}

export interface SteerCodexTurnOptions {
  readonly sessionId: string;
  readonly turnId: string | null | undefined;
  readonly prompt: string;
  readonly appServer: AppServer;
}

/** A session to fork before one of its turns. */
export interface ForkCodexSessionOptions extends CodexSessionOptions {
  readonly sessionId: string;
  readonly beforeTurnId: string;
}

const createAppServerStream = Effect.fnUntraced(function*({ resumeSession, threadKey, workingDirectory, codexEnv, codexConfig, prompt, modelChoice, persistence, pendingResponseId, turnTimer, appServer, onPromptDispatched }: CodexStreamParams) {
  const scope = { threadKey, cwd: workingDirectory, env: codexEnv, config: codexConfig };
  const sessionId = resumeSession
    ? yield* appServer.ensureThread({ threadId: resumeSession, ...scope })
    : yield* appServer.startThread(scope);
  yield* persistence.updatePendingSessionId(pendingResponseId, sessionId);
  yield* persistence.updateActiveTurnSessionId(sessionId);
  // The model the login has: the one chosen where it has it, the login's own otherwise.
  const wanted = resolveCodexModelChoice(modelChoice);
  const { choice: { model, effort }, notice } = choiceForLogin(wanted, yield* appServer.loginModels({ env: codexEnv, cwd: workingDirectory }));
  if (notice !== null) yield* Effect.logInfo(`codex.model.fallback thread=${threadKey} wanted=${wanted.model} model=${model} effort=${effort}`);
  // The turn started, it is interrupted as the scope it was started in closes unless it has
  // ended, so that a stop before its events are read, as the turn's start is recorded, stops it
  // there too; one stopped as it starts, startTurn stops itself.
  const turnId = yield* Effect.uninterruptibleMask((restore) =>
    restore(appServer.startTurn({ threadId: sessionId, prompt, model, effort, onPromptDispatched, ...scope })).pipe(
      Effect.tap((started) =>
        Effect.addFinalizer(() => (started ? appServer.interrupt(sessionId, "stopped-before-its-events", started).pipe(Effect.ignore) : Effect.void))
      ),
    )
  );
  yield* turnTimer("app_server.turn_start.returned", { turn_id: turnId ?? "unknown" });
  const stream: CodexEventStream = { sessionId, turnId, events: appServer.eventsForTurn(sessionId, turnId), notice };
  return stream;
});

const createAttachedAppServerStream = Effect.fnUntraced(function*({ sessionId, turnId, persistence, pendingResponseId, turnTimer, appServer }: AttachedCodexStreamParams) {
  if (!sessionId || !turnId) {
    return yield* new CodexTransportRefused({ message: "Cannot attach to a Codex goal turn without both session and turn ids" });
  }
  yield* persistence.updatePendingSessionId(pendingResponseId, sessionId);
  yield* persistence.updateActiveTurnSessionId(sessionId);
  yield* appServer.claimTurn(sessionId, turnId);
  yield* turnTimer("app_server.goal_turn.attached", { turn_id: turnId });
  const stream: CodexEventStream = { sessionId, turnId, events: appServer.eventsForTurn(sessionId, turnId) };
  return stream;
});

/** The turn's events, from the app-server. */
export function openCodexEventStream(params: CodexStreamParams): Effect.Effect<CodexEventStream, CodexOpenError, Scope.Scope> {
  return createAppServerStream(params);
}

export function openAttachedCodexEventStream(params: AttachedCodexStreamParams): Effect.Effect<CodexEventStream, CodexOpenError> {
  return createAttachedAppServerStream(params);
}

export function steerCodexTransportTurn({ sessionId, turnId, prompt, appServer }: SteerCodexTurnOptions): Effect.Effect<boolean, AppServerRequestError | NoActiveTurn> {
  return appServer.steerTurn({ threadId: sessionId, turnId, prompt }).pipe(Effect.as(true));
}

export function startCodexTransportThread({ threadKey, workingDirectory, codexEnv, codexConfig, appServer }: CodexSessionOptions): Effect.Effect<string, AppServerStartError | ThreadIdMissing> {
  return appServer.startThread({ threadKey, cwd: workingDirectory, env: codexEnv, config: codexConfig });
}

/** Forks the session before one of its turns, the fork a rollout like any other. */
export function forkCodexTransportThread({ sessionId, beforeTurnId, threadKey, workingDirectory, codexEnv, codexConfig, appServer }: ForkCodexSessionOptions): Effect.Effect<string, AppServerStartError | ThreadIdMissing> {
  return appServer.forkThread({ threadId: sessionId, beforeTurnId, threadKey, cwd: workingDirectory, env: codexEnv, config: codexConfig });
}
