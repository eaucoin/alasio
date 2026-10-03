/**
 * Codex runtime adapter for alasio turns.
 *
 * Owns alasio turn orchestration and local command guardrails.
 */
import { Cause, Context, Deferred, Effect, Exit, Schema, Stream } from "effect";

import { buildCodexEnv } from "./env.ts";
import { appendBlock, errorBlock, isVisibleCodexItem, mapItemToBlocks } from "./event-projection.ts";
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
import { effectRunnerHere } from "../shared/effects.ts";
import { createLogger, withLogScope } from "../shared/log.ts";
import { CODEX_HARNESS } from "../harness/names.ts";
import type { ActiveQueries, ActiveQuery, TurnParams, TurnResult } from "../harness/index.ts";
import type { ResponseBlock } from "./event-projection.ts";

const log = createLogger("codex-runtime");

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

/** The operator, or the DB guardrail, stopped a turn for `reason`. */
class TurnAborted extends Schema.TaggedError<TurnAborted>()("TurnAborted", {
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
export const folderCodexScope = ({
    workingDirectory,
    threadKey,
    appServer,
    folderBayma = noFolderBayma,
}: Omit<CodexScopeParams, "scope">): Effect.Effect<CodexScope, CodexScopeError> =>
    Effect.tryPromise({
        try: () => folderBayma({ harness: CODEX_HARNESS, threadKey }),
        catch: (cause) => new CodexScopeError({ cause }),
    }).pipe(Effect.map((bayma) => {
        const codexEnv = buildCodexEnv();
        return { cwd: workingDirectory, codexEnv, codexConfig: buildCodexThreadConfig({ codexEnv, bayma }), appServer };
    }));

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

function isIntentionalTurnInterrupt(message: string): boolean {
    return message === "Interrupted from Telegram" || message === "Telegram swerve";
}

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
 * A Codex turn, from its pending response to its result. It does not fail: what goes
 * wrong ends up in its response, as an error. While it runs it is the conversation's
 * active query, whose abort interrupts the fiber reading the turn's events (and so the
 * turn upstream) and resolves once the turn has let go of the conversation.
 */
export const executeCodexTurn = Effect.fnUntraced(function*(params: CodexTurnParams): Effect.fn.Return<TurnResult> {
    const { prompt, resumeSession, threadKey, chatId, messageId, persistence, activeQueries, onStarted } = params;
    const guardrailRecoveryDepth = params.guardrailRecoveryDepth ?? 0;
    const turnTimer = createTurnTimer({ harness: CODEX_HARNESS, threadKey, resumeSession, prompt, log });
    yield* Effect.logInfo(`Querying Codex (resume=${resumeSession})`);
    turnTimer("query.start");
    onStarted?.();
    const blockSequence: ResponseBlock[] = [];
    let sessionId: string | null | undefined = resumeSession;
    let interrupted = false;
    let responseCompleted = false;
    const pendingResponseId = persistence.createPendingResponse(chatId, messageId, resumeSession);
    persistence.updateActiveTurnPendingResponseId(threadKey, pendingResponseId);
    // Why the turn is stopped, once it is; and its letting go of the conversation.
    const aborted = yield* Deferred.make<string>();
    const finished = yield* Deferred.make<void>();
    const effects = yield* effectRunnerHere(Context.empty());
    const activeQuery: ActiveQuery = {
        abort: (reason) => effects.runPromise(Deferred.succeed(aborted, reason).pipe(Effect.andThen(Deferred.await(finished)))),
        steer: async () => false,
    };
    activeQueries.set(threadKey, activeQuery);
    const commandPolicy = createCommandEventPolicy({
        persistence,
        threadKey,
        chatId,
        messageId,
        controller: { abort: (reason) => Deferred.doneUnsafe(aborted, Exit.succeed(String(reason))) },
        log,
    });

    let firstEventLogged = false;
    let firstVisibleItemLogged = false;
    /** What the turn makes of each of its events, `turnId` being the transport's turn. */
    const onEvent = (turnId: string | null) => Effect.fnUntraced(function*(event: CodexEvent) {
        if (!firstEventLogged) {
            firstEventLogged = true;
            turnTimer("first_event", { event_type: event.type });
        }
        onStarted?.();
        switch (event.type) {
            case "thread.started":
                sessionId = event.thread_id;
                persistence.updatePendingSessionId(pendingResponseId, sessionId);
                persistence.updateActiveTurnSessionId(threadKey, sessionId);
                break;
            case "item.started":
            case "item.updated":
            case "item.completed":
                if (!firstVisibleItemLogged && isVisibleCodexItem(event.item)) {
                    firstVisibleItemLogged = true;
                    turnTimer("first_visible_item", { event_type: event.type, item_type: event.item.type });
                }
                if (event.item.type === "command_execution") {
                    const command = event.item.command ?? "";
                    const policyResult = commandPolicy.inspectCommand({ command, sessionId });
                    if (policyResult.blocked) {
                        break;
                    }
                }
                mapItemToBlocks(event.item, {
                    blockSequence,
                    persistence,
                    pendingResponseId,
                });
                break;
            case "turn.completed":
                turnTimer("turn.completed");
                // Nothing delivers a response before it is complete.
                if (params.beforeResponseComplete) {
                    yield* params.beforeResponseComplete(sessionId);
                }
                turnTimer("before_response_complete.done");
                persistence.markPendingResponseComplete(pendingResponseId);
                responseCompleted = true;
                params.onTransportCompleted?.({ sessionId, turnId });
                if (sessionId && event.usage) {
                    persistence.updateSessionUsage(sessionId, {
                        cacheReadInputTokens: event.usage.cached_input_tokens,
                    });
                }
                break;
            case "usage.updated":
                if (sessionId && event.usage?.last) {
                    persistence.updateSessionUsage(sessionId, {
                        cacheReadInputTokens: event.usage.last.cachedInputTokens,
                    });
                }
                break;
            case "turn.failed":
                turnTimer("turn.failed", { error: event.error.message });
                appendBlock(blockSequence, persistence, pendingResponseId, errorBlock(event.error.message));
                break;
            case "error":
                turnTimer("event.error", { error: event.message });
                appendBlock(blockSequence, persistence, pendingResponseId, errorBlock(event.message));
                break;
            default:
                break;
        }
    });

    /** The turn's events, read to their end, in a scope the transport keeps what it runs in. */
    const readEvents = Effect.scoped(Effect.gen(function*() {
        const { cwd, codexEnv, codexConfig, appServer } = yield* codexScope(params);
        turnTimer("env.built");
        turnTimer("bayma.ready");
        const streamParams: CodexStreamParams = {
            resumeSession,
            threadKey,
            workingDirectory: cwd,
            codexEnv,
            codexConfig,
            prompt,
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
        params.onTransportStarted?.({ sessionId, turnId: streamed.turnId });
        if (sessionId && streamed.turnId) {
            const steered = { sessionId, turnId: streamed.turnId, appServer };
            activeQuery.steer = (steerPrompt) => effects.runPromise(steerCodexTransportTurn({ ...steered, prompt: steerPrompt }));
        }
        yield* Stream.runForEach(streamed.events, onEvent(streamed.turnId));
    }));

    // Stopping the turn interrupts the reading of its events, which interrupts it upstream.
    const read = yield* readEvents.pipe(
        Effect.raceFirst(Effect.flatMap(Deferred.await(aborted), (reason) => Effect.fail(new TurnAborted({ reason })))),
        Effect.exit,
    );
    if (Exit.isFailure(read)) {
        if (commandPolicy.getGuardrailResult().guardrailBlocked) {
            yield* Effect.logWarning("Query aborted by DB guardrail");
        }
        else {
            const errMsg = getErrorMessage(Cause.squash(read.cause));
            if (isIntentionalTurnInterrupt(errMsg)) {
                interrupted = true;
                blockSequence.length = 0;
                turnTimer("query.interrupted", { reason: errMsg });
                yield* Effect.logInfo(`Codex turn interrupted by operator control: ${errMsg}`);
            }
            else {
                turnTimer("query.error", { error: errMsg });
                yield* Effect.logError(`Error querying Codex: ${errMsg}`);
                appendBlock(blockSequence, persistence, pendingResponseId, errorBlock(errMsg));
            }
        }
    }
    const { guardrailBlocked, blockedGuardrailCommand } = commandPolicy.getGuardrailResult();
    activeQueries.delete(threadKey);
    yield* Deferred.succeed(finished, undefined);
    turnTimer("query.finished", { guardrail_blocked: guardrailBlocked });
    if (guardrailBlocked && blockedGuardrailCommand) {
        if (sessionId && guardrailRecoveryDepth < MAX_DB_GUARDRAIL_RECOVERY_ATTEMPTS) {
            yield* Effect.logInfo("DB guardrail matched a tool command; injecting synthetic user message back into Codex");
            persistence.markPendingAsPosted(pendingResponseId);
            return yield* executeCodexTurn({
                ...params,
                prompt: buildDbGuardrailSyntheticText(blockedGuardrailCommand),
                resumeSession: sessionId,
                attachedTurn: undefined,
                guardrailRecoveryDepth: guardrailRecoveryDepth + 1,
            });
        }
        appendBlock(blockSequence, persistence, pendingResponseId, {
            type: "text",
            content: buildDbGuardrailFallbackText(blockedGuardrailCommand),
        });
    }
    return {
        blockSequence,
        sessionId,
        pendingResponseId,
        interrupted,
        responseCompleted,
    };
}, withLogScope("codex-runtime"));

export async function interruptCodexTurn(activeQueries: ActiveQueries, threadKey: string): Promise<boolean> {
    const activeQuery = activeQueries.get(threadKey);
    if (!activeQuery) {
        log.info(`No active query for thread key ${threadKey}`);
        return false;
    }
    log.info(`Stopping query for thread key ${threadKey}`);
    await activeQuery.abort("Interrupted from Telegram");
    log.info(`Stopped query for thread key ${threadKey}`);
    return true;
}
