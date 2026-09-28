/**
 * Codex runtime adapter for alasio turns.
 *
 * Owns alasio turn orchestration and local command guardrails.
 */
import { buildCodexEnv } from "./env.js";
import { appendBlock, isVisibleCodexItem, mapItemToBlocks } from "./event-projection.js";
import {
    canWarmCodexSession,
    forkCodexTransportThread,
    openAttachedCodexEventStream,
    openCodexEventStream,
    startCodexTransportThread,
    steerCodexTransportTurn,
    stopCodexTransport,
    warmCodexTransportThread,
} from "./transport.js";
import { createTurnTimer, elapsedMs } from "./turn-timing.js";
import {
    buildDbGuardrailFallbackText,
    buildDbGuardrailSyntheticText,
    MAX_DB_GUARDRAIL_RECOVERY_ATTEMPTS,
    createCommandEventPolicy,
} from "./command-event-policy.js";
import { buildCodexThreadConfig } from "./thread-config.js";
import { ensureBaymaReady } from "../mcp/bayma.js";
import { createLogger } from "../shared/log.js";

const log = createLogger("codex-runtime");

function getErrorMessage(error) {
    return error instanceof Error ? error.message : String(error);
}

export async function startFreshCodexSession({ threadKey, workingDirectory }) {
    const startedAt = process.hrtime.bigint();
    const codexEnv = buildCodexEnv();
    const codexConfig = buildCodexThreadConfig({ codexEnv, threadKey });
    await ensureBaymaReady(codexEnv);
    const sessionId = await startCodexTransportThread({
        threadKey,
        workingDirectory,
        codexEnv,
        codexConfig,
    });
    log.info(
        `new_session.started total_ms=${elapsedMs(startedAt).toFixed(1)} thread_key=${JSON.stringify(threadKey)} session=${JSON.stringify(sessionId.slice(0, 8))}`,
    );
    return sessionId;
}

/** A new thread holding a session's history before one of its turns: rewind. */
export async function forkCodexSession({ sessionId, beforeTurnId, threadKey, workingDirectory }) {
    const codexEnv = buildCodexEnv();
    const codexConfig = buildCodexThreadConfig({ codexEnv, threadKey });
    await ensureBaymaReady(codexEnv);
    return await forkCodexTransportThread({
        sessionId,
        beforeTurnId,
        threadKey,
        workingDirectory,
        codexEnv,
        codexConfig,
    });
}

function isIntentionalTurnInterrupt(error) {
    const message = getErrorMessage(error);
    return message === "Interrupted from Telegram" || message === "Telegram swerve";
}

export async function warmCodexSession({ sessionId, threadKey, workingDirectory }) {
    if (!sessionId || !canWarmCodexSession()) {
        return false;
    }
    const startedAt = process.hrtime.bigint();
    const codexEnv = buildCodexEnv();
    const codexConfig = buildCodexThreadConfig({ codexEnv, threadKey });
    await ensureBaymaReady(codexEnv);
    await warmCodexTransportThread({
        sessionId,
        threadKey,
        workingDirectory,
        codexEnv,
        codexConfig,
    });
    log.info(`warm_session.done total_ms=${elapsedMs(startedAt).toFixed(1)} thread_key=${JSON.stringify(threadKey)} session=${JSON.stringify(sessionId.slice(0, 8))}`);
    return true;
}
export function shutdownCodexRuntime() {
    stopCodexTransport();
}
export async function executeCodexTurn(params) {
    const { prompt, resumeSession, threadKey, chatId, messageId, workingDirectory, persistence, activeQueries, onStarted, } = params;
    const guardrailRecoveryDepth = params.guardrailRecoveryDepth ?? 0;
    const turnTimer = createTurnTimer({ threadKey, resumeSession, prompt, log });
    log.info(`Querying Codex (resume=${resumeSession})`);
    turnTimer("query.start");
    onStarted?.();
    const blockSequence = [];
    let sessionId = resumeSession;
    let interrupted = false;
    let responseCompleted = false;
    const pendingResponseId = persistence.createPendingResponse(chatId, messageId, resumeSession);
    persistence.updateActiveTurnPendingResponseId(threadKey, pendingResponseId);
    const controller = new AbortController();
    let resolveFinished;
    const finished = new Promise((resolve) => {
        resolveFinished = resolve;
    });
    const activeQuery = {
        abort: async (reason) => {
            controller.abort(reason);
            await finished;
        },
        steer: async () => false,
    };
    activeQueries.set(threadKey, activeQuery);
    const commandPolicy = createCommandEventPolicy({
        persistence,
        threadKey,
        chatId,
        messageId,
        controller,
        log,
    });
    try {
        const codexEnv = buildCodexEnv();
        turnTimer("env.built");
        const codexConfig = buildCodexThreadConfig({ codexEnv, threadKey });
        turnTimer("config.built");
        await ensureBaymaReady(codexEnv);
        turnTimer("bayma.ready");
        const streamParams = {
            resumeSession,
            threadKey,
            workingDirectory,
            codexEnv,
            codexConfig,
            prompt,
            persistence,
            pendingResponseId,
            codexFactory: params.codexFactory,
            controller,
            turnTimer,
        };
        const streamed = params.attachedTurn
            ? await openAttachedCodexEventStream({
                ...streamParams,
                sessionId: params.attachedTurn.sessionId,
                turnId: params.attachedTurn.turnId,
            })
            : await openCodexEventStream(streamParams);
        sessionId = streamed.sessionId;
        params.onTransportStarted?.({ sessionId, turnId: streamed.turnId });
        if (sessionId && streamed.turnId) {
            activeQuery.steer = async (steerPrompt) => await steerCodexTransportTurn({
                sessionId,
                turnId: streamed.turnId,
                prompt: steerPrompt,
            });
        }
        let firstEventLogged = false;
        let firstVisibleItemLogged = false;
        for await (const event of streamed.events) {
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
                    persistence.markPendingResponseComplete(pendingResponseId);
                    responseCompleted = true;
                    params.onTransportCompleted?.({ sessionId, turnId: streamed.turnId });
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
                    appendBlock(blockSequence, persistence, pendingResponseId, {
                        type: "text",
                        content: `Error: ${event.error.message}`,
                    });
                    break;
                case "error":
                    turnTimer("event.error", { error: event.message });
                    appendBlock(blockSequence, persistence, pendingResponseId, {
                        type: "text",
                        content: `Error: ${event.message}`,
                    });
                    break;
                default:
                    break;
            }
        }
    }
    catch (err) {
        const { guardrailBlocked } = commandPolicy.getGuardrailResult();
        if (guardrailBlocked) {
            log.warn("Query aborted by DB guardrail");
        }
        else {
            const errMsg = getErrorMessage(err);
            if (isIntentionalTurnInterrupt(err)) {
                interrupted = true;
                blockSequence.length = 0;
                turnTimer("query.interrupted", { reason: errMsg });
                log.info(`Codex turn interrupted by operator control: ${errMsg}`);
            }
            else {
                turnTimer("query.error", { error: errMsg });
                log.error(`Error querying Codex: ${errMsg}`);
                appendBlock(blockSequence, persistence, pendingResponseId, {
                    type: "text",
                    content: `Error: ${errMsg}`,
                });
            }
        }
    }
    finally {
        const { guardrailBlocked } = commandPolicy.getGuardrailResult();
        activeQueries.delete(threadKey);
        resolveFinished?.();
        turnTimer("query.finished", { guardrail_blocked: guardrailBlocked });
    }
    const { guardrailBlocked, blockedGuardrailCommand } = commandPolicy.getGuardrailResult();
    if (guardrailBlocked && blockedGuardrailCommand) {
        if (sessionId && guardrailRecoveryDepth < MAX_DB_GUARDRAIL_RECOVERY_ATTEMPTS) {
            log.info("DB guardrail matched a tool command; injecting synthetic user message back into Codex");
            persistence.markPendingAsPosted(pendingResponseId);
            return await executeCodexTurn({
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
}
export async function interruptCodexTurn(activeQueries, threadKey) {
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
