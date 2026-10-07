/**
 * Codex's sessions and turns, as the Codex harness (../harness/codex.ts) runs them: what
 * each call runs against (a folder workspace's app-server and bayma, or a session
 * filesystem's), a new, forked or warmed session, and a turn from its pending response
 * to its result, with the DB guardrail and restart provenance applied to its commands.
 */
import { Cause, Deferred, Effect, Exit, Fiber, Schema, Stream } from "effect";

import { buildCodexEnv } from "./env.ts";
import { appendBlock, completeResponse, errorBlock, isVisibleCodexItem, mapItemToBlocks, responseProjection, storeBlocks } from "./event-projection.ts";
import {
  type CodexEvent,
  type CodexTransportRefused,
  canWarmCodexSession,
  forkCodexTransportThread,
  openAttachedCodexEventStream,
  openCodexEventStream,
  startCodexTransportThread,
  steerCodexTransportTurn,
  warmCodexTransportThread,
} from "./transport.ts";
import { createTurnTimer, elapsedMs } from "./turn-timing.ts";
import {
  buildDbGuardrailFallbackText,
  buildDbGuardrailSyntheticText,
  MAX_DB_GUARDRAIL_RECOVERY_ATTEMPTS,
  createCommandEventPolicy,
} from "./command-event-policy.ts";
import { type CodexThreadConfig, buildCodexThreadConfig } from "./thread-config.ts";
import type { AppServer } from "./app-server/client.ts";
import type { AppServerStartError } from "./app-server/rpc-client.ts";
import type { ThreadIdMissing } from "./app-server/thread-client.ts";
import type { CodexListingScope } from "./sessions.ts";
import type { CodexStreamParams } from "./transport.ts";
import { type FolderBayma, noFolderBayma } from "../mcp/bayma.ts";
import { withLogScope } from "../shared/log.ts";
import { ActiveTurns, type RunningTurn, stopReasonText } from "../harness/active-turns.ts";
import { CODEX_HARNESS } from "../harness/names.ts";
import type { HarnessError, TurnParams, TurnResult } from "../harness/index.ts";
import type { StoreError } from "../persistence/sql.ts";

function getErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/** What a call runs against: a listing scope, with the config its threads run with. */
export interface CodexScope extends CodexListingScope {
  readonly codexConfig: CodexThreadConfig;
}

/** What a call could not be run against: the workspace's bayma, its session's host, or its Codex did not come up. */
export class CodexScopeError extends Schema.TaggedError<CodexScopeError>()("CodexScopeError", {
  cause: Schema.Defect(),
}) {
  override get message(): string {
    return this.cause instanceof Error ? this.cause.message : String(this.cause);
  }
}

/** The scope a harness's calls run against, in place of the folder workspace's. */
export type CodexScopeProvider = Effect.Effect<CodexScope, CodexScopeError | CodexTransportRefused>;

/** The conversation and folder a call is for, and what it runs against. */
export interface CodexScopeParams {
  readonly threadKey: string;
  readonly workingDirectory: string;
  /** The operator's app-server, which a folder workspace's calls run on. */
  readonly appServer: AppServer;
  /** What the call runs against when not the folder's own (a session filesystem's, from ./sessionfs.ts). */
  readonly scope?: CodexScopeProvider | null | undefined;
  /** The conversation's bayma in a folder workspace; none unless the deployment, or a test, gives one. */
  readonly folderBayma?: FolderBayma | undefined;
}

export interface ForkCodexSessionParams extends CodexScopeParams {
  readonly sessionId: string;
  readonly beforeTurnId: string;
}

export interface WarmCodexSessionParams extends CodexScopeParams {
  readonly sessionId: string | null;
}

/** A Codex turn: a harness's turn, with what the Codex harness runs it against and around. */
export interface CodexTurnParams extends TurnParams, CodexScopeParams {
  /** Runs once the turn has completed, before its response is marked complete. */
  readonly beforeResponseComplete?: ((sessionId: string | null | undefined) => Effect.Effect<void>) | undefined;
  readonly codexFactory?: CodexStreamParams["codexFactory"];
  /** How many times the DB guardrail has already sent the turn back to Codex. */
  readonly guardrailRecoveryDepth?: number | undefined;
}

/** The operator stopped a turn, with /stop or a swerve. */
class TurnStopped extends Schema.TaggedError<TurnStopped>()("TurnStopped", {
  reason: Schema.Literals(["interrupt", "swerve"]),
}) {
  override get message(): string {
    return stopReasonText(this.reason);
  }
}

/** The DB guardrail stopped a turn for `reason`. */
class GuardrailStopped extends Schema.TaggedError<GuardrailStopped>()("GuardrailStopped", {
  reason: Schema.String,
}) {
  override get message(): string {
    return this.reason;
  }
}

/**
 * What a folder workspace's calls run against: the operator's app-server, the operator's
 * Codex env, and the conversation's bayma (`folderBayma`).
 */
const folderCodexScope = ({
  workingDirectory,
  threadKey,
  appServer,
  folderBayma = noFolderBayma,
}: Omit<CodexScopeParams, "scope">): Effect.Effect<CodexScope, CodexScopeError> =>
  folderBayma({ harness: CODEX_HARNESS, threadKey }).pipe(
    Effect.mapError((cause) => new CodexScopeError({ cause })),
    Effect.map((bayma) => {
      const codexEnv = buildCodexEnv();
      return { cwd: workingDirectory, codexEnv, codexConfig: buildCodexThreadConfig({ codexEnv, bayma }), appServer };
    }),
  );

/**
 * The scope a call runs against: `scope`, when the harness gives one (a session
 * filesystem's, from ./sessionfs.ts), else the folder workspace's.
 */
function codexScope({ scope, ...params }: CodexScopeParams): Effect.Effect<CodexScope, CodexScopeError | CodexTransportRefused> {
  return scope ?? folderCodexScope(params);
}

/** How a call on a session fails: what it runs against did not come up, or its app-server failed it. */
export type CodexSessionError = CodexScopeError | CodexTransportRefused | AppServerStartError | ThreadIdMissing;

export const startFreshCodexSession = Effect.fnUntraced(function*(params: CodexScopeParams): Effect.fn.Return<string, CodexSessionError> {
  const startedAt = process.hrtime.bigint();
  const { cwd, codexEnv, codexConfig, appServer } = yield* codexScope(params);
  const sessionId = yield* startCodexTransportThread({ threadKey: params.threadKey, workingDirectory: cwd, codexEnv, codexConfig, appServer });
  yield* Effect.logInfo(
    `new_session.started total_ms=${elapsedMs(startedAt).toFixed(1)} thread_key=${JSON.stringify(params.threadKey)} session=${JSON.stringify(sessionId.slice(0, 8))}`,
  );
  return sessionId;
}, withLogScope("codex-runtime"));

/** A new thread holding a session's history before one of its turns: rewind. */
export const forkCodexSession = Effect.fnUntraced(function*({ sessionId, beforeTurnId, ...params }: ForkCodexSessionParams): Effect.fn.Return<string, CodexSessionError> {
  const { cwd, codexEnv, codexConfig, appServer } = yield* codexScope(params);
  return yield* forkCodexTransportThread({ sessionId, beforeTurnId, threadKey: params.threadKey, workingDirectory: cwd, codexEnv, codexConfig, appServer });
});

export const warmCodexSession = Effect.fnUntraced(function*({ sessionId, ...params }: WarmCodexSessionParams): Effect.fn.Return<boolean, CodexSessionError> {
  if (!sessionId || !canWarmCodexSession()) {
    return false;
  }
  const startedAt = process.hrtime.bigint();
  const { cwd, codexEnv, codexConfig, appServer } = yield* codexScope(params);
  yield* warmCodexTransportThread({ sessionId, threadKey: params.threadKey, workingDirectory: cwd, codexEnv, codexConfig, appServer });
  yield* Effect.logInfo(`warm_session.done total_ms=${elapsedMs(startedAt).toFixed(1)} thread_key=${JSON.stringify(params.threadKey)} session=${JSON.stringify(sessionId.slice(0, 8))}`);
  return true;
}, withLogScope("codex-runtime"));

/**
 * A Codex turn, from its pending response to its result. It fails only as alasio's store
 * does: what else goes wrong ends up in its response, as an error. While its events are read it is the
 * conversation's running turn, whose stop interrupts the reading (and so the turn
 * upstream) and is done once the turn has let go of the conversation.
 *
 * Interrupted itself, as alasio stops, the turn lets go of its events without stopping
 * the turn upstream, which its app-server's end ends as alasio's end always did: an
 * interrupt would have Codex record it as one the user made, which the turn continued
 * after the restart is not to believe.
 */
export const executeCodexTurn = Effect.fnUntraced(function*(params: CodexTurnParams): Effect.fn.Return<TurnResult, StoreError, ActiveTurns> {
  const { prompt, resumeSession, threadKey, chatId, messageId, persistence } = params;
  const activeTurns = yield* ActiveTurns;
  const guardrailRecoveryDepth = params.guardrailRecoveryDepth ?? 0;
  const turnTimer = createTurnTimer({ harness: CODEX_HARNESS, threadKey, resumeSession, prompt });
  yield* Effect.logInfo(`Querying Codex (resume=${resumeSession})`);
  yield* turnTimer("query.start");
  let sessionId: string | null | undefined = resumeSession;
  let interrupted = false;
  let responseCompleted = false;
  const pendingResponseId = yield* persistence.createPendingResponse(chatId, messageId, resumeSession);
  yield* persistence.updateActiveTurnPendingResponseId(threadKey, pendingResponseId);
  const projection = responseProjection(pendingResponseId);
  // Why the turn is stopped, once it is; and its letting go of the conversation.
  const stopped = yield* Deferred.make<never, TurnStopped | GuardrailStopped>();
  const finished = yield* Deferred.make<void>();
  // Steering needs the turn upstream, which the transport starts.
  let steer: (prompt: string) => Effect.Effect<boolean, HarnessError> = () => Effect.succeed(false);
  const running: RunningTurn = {
    stop: (reason) => Deferred.fail(stopped, new TurnStopped({ reason })).pipe(Effect.andThen(Deferred.await(finished))),
    steer: (steerPrompt) => steer(steerPrompt),
    cliInitiated: false,
  };
  const commandPolicy = createCommandEventPolicy({
    persistence,
    threadKey,
    chatId,
    messageId,
    onBlocked: (reason) => Effect.asVoid(Deferred.fail(stopped, new GuardrailStopped({ reason }))),
  });

  let firstEventLogged = false;
  let firstVisibleItemLogged = false;
  /** What the turn makes of each of its events, `turnId` being the transport's turn. */
  const onEvent = (turnId: string | null) => Effect.fnUntraced(function*(event: CodexEvent) {
    if (!firstEventLogged) {
      firstEventLogged = true;
      yield* turnTimer("first_event", { event_type: event.type });
    }
    switch (event.type) {
      case "thread.started":
        sessionId = event.thread_id;
        yield* persistence.updatePendingSessionId(pendingResponseId, sessionId);
        yield* persistence.updateActiveTurnSessionId(threadKey, sessionId);
        break;
      case "item.started":
      case "item.updated":
      case "item.completed":
        if (!firstVisibleItemLogged && isVisibleCodexItem(event.item)) {
          firstVisibleItemLogged = true;
          yield* turnTimer("first_visible_item", { event_type: event.type, item_type: event.item.type });
        }
        if (event.item.type === "command_execution") {
          const command = event.item.command ?? "";
          const policyResult = yield* commandPolicy.inspectCommand({ command, sessionId });
          if (policyResult.blocked) {
            break;
          }
        }
        mapItemToBlocks(event.item, projection);
        break;
      case "turn.completed":
        yield* turnTimer("turn.completed");
        // Nothing delivers a response before it is complete.
        if (params.beforeResponseComplete) {
          yield* params.beforeResponseComplete(sessionId);
        }
        yield* turnTimer("before_response_complete.done");
        yield* completeResponse(projection, persistence);
        responseCompleted = true;
        if (params.onTransportCompleted) {
          yield* params.onTransportCompleted({ sessionId, turnId });
        }
        if (sessionId && event.usage) {
          yield* persistence.updateSessionUsage(sessionId, {
            cacheReadInputTokens: event.usage.cached_input_tokens,
          });
        }
        break;
      case "usage.updated":
        if (sessionId && event.usage?.last) {
          yield* persistence.updateSessionUsage(sessionId, {
            cacheReadInputTokens: event.usage.last.cachedInputTokens,
          });
        }
        break;
      case "turn.failed":
        yield* turnTimer("turn.failed", { error: event.error.message });
        appendBlock(projection, errorBlock(event.error.message));
        break;
      case "error":
        yield* turnTimer("event.error", { error: event.message });
        appendBlock(projection, errorBlock(event.message));
        break;
      default:
        break;
    }
    yield* storeBlocks(projection, persistence);
  });

  /** The turn's events, read to their end, in a scope the transport keeps what it runs in. */
  const readEvents = Effect.scoped(Effect.gen(function*() {
    const { cwd, codexEnv, codexConfig, appServer } = yield* codexScope(params);
    yield* turnTimer("env.built");
    yield* turnTimer("bayma.ready");
    const streamParams: CodexStreamParams = {
      resumeSession,
      threadKey,
      workingDirectory: cwd,
      codexEnv,
      codexConfig,
      prompt,
      modelChoice: params.modelChoice,
      persistence,
      pendingResponseId,
      codexFactory: params.codexFactory,
      turnTimer,
      appServer,
      onPromptDispatched: params.onPromptDispatched,
    };
    const streamed = params.attachedTurn
      ? yield* openAttachedCodexEventStream({
        ...streamParams,
        sessionId: params.attachedTurn.sessionId,
        turnId: params.attachedTurn.turnId,
      })
      : yield* openCodexEventStream(streamParams);
    sessionId = streamed.sessionId;
    if (params.onTransportStarted) {
      yield* params.onTransportStarted({ sessionId, turnId: streamed.turnId });
    }
    if (sessionId && streamed.turnId) {
      const steered = { sessionId, turnId: streamed.turnId, appServer };
      steer = (steerPrompt) => steerCodexTransportTurn({ ...steered, prompt: steerPrompt });
    }
    yield* Stream.runForEach(streamed.events, onEvent(streamed.turnId));
  }));

  // The turn is the conversation's running turn while its events are read; stopping it
  // interrupts the reading, which interrupts it upstream. The reading is a fiber of its
  // own, which interrupting this one leaves be.
  const reader = yield* Effect.forkDetach(readEvents);
  const read = yield* Effect.scoped(activeTurns.register(threadKey, running).pipe(
    Effect.andThen(Fiber.join(reader).pipe(
      Effect.raceFirst(Deferred.await(stopped)),
      // Stopped, the reading is interrupted, and with it the turn upstream.
      Effect.tapError(() => Fiber.interrupt(reader)),
    )),
    Effect.exit,
  )).pipe(Effect.ensuring(Deferred.succeed(finished, undefined)));
  if (Exit.isFailure(read)) {
    if (commandPolicy.getGuardrailResult().guardrailBlocked) {
      yield* Effect.logWarning("Query aborted by DB guardrail");
    } else {
      const failure = Cause.squash(read.cause);
      const errMsg = getErrorMessage(failure);
      if (failure instanceof TurnStopped) {
        interrupted = true;
        projection.blockSequence.length = 0;
        yield* turnTimer("query.interrupted", { reason: errMsg });
        yield* Effect.logInfo(`Codex turn interrupted by operator control: ${errMsg}`);
      } else {
        yield* turnTimer("query.error", { error: errMsg });
        yield* Effect.logError(`Error querying Codex: ${errMsg}`);
        appendBlock(projection, errorBlock(errMsg));
      }
    }
  }
  const { guardrailBlocked, blockedGuardrailCommand } = commandPolicy.getGuardrailResult();
  yield* turnTimer("query.finished", { guardrail_blocked: guardrailBlocked });
  if (guardrailBlocked && blockedGuardrailCommand) {
    if (sessionId && guardrailRecoveryDepth < MAX_DB_GUARDRAIL_RECOVERY_ATTEMPTS) {
      yield* Effect.logInfo("DB guardrail matched a tool command; injecting synthetic user message back into Codex");
      yield* persistence.markPendingAsPosted(pendingResponseId);
      return yield* executeCodexTurn({
        ...params,
        prompt: buildDbGuardrailSyntheticText(blockedGuardrailCommand),
        resumeSession: sessionId,
        attachedTurn: undefined,
        guardrailRecoveryDepth: guardrailRecoveryDepth + 1,
      });
    }
    appendBlock(projection, {
      type: "text",
      content: buildDbGuardrailFallbackText(blockedGuardrailCommand),
    });
  }
  yield* storeBlocks(projection, persistence);
  return {
    blockSequence: projection.blockSequence,
    sessionId,
    pendingResponseId,
    interrupted,
    responseCompleted,
  };
}, withLogScope("codex-runtime"));
