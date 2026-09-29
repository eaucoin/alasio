import { CODEX_HARNESS } from "../harness/names.js";
import { resolveCodexModelChoice } from "./model.js";
import { Codex } from "@openai/codex-sdk";
import { codexAppServerClient, stopCodexAppServer } from "./app-server/client.js";
import {
    ALASIO_CODEX_MODEL,
    withAlasioCodexModelConfig,
} from "./model.js";
import { getCodexTransportMode } from "../config.js";

const CODEX_TRANSPORT = getCodexTransportMode();

export function canWarmCodexSession() {
    return CODEX_TRANSPORT !== "exec";
}

export function stopCodexTransport() {
    stopCodexAppServer();
}

async function createAppServerStream({ resumeSession, threadKey, workingDirectory, codexEnv, codexConfig, prompt, persistence, pendingResponseId, controller, turnTimer, client = codexAppServerClient, }) {
    let sessionId = resumeSession;
    if (resumeSession) {
        sessionId = await client.ensureThread({
            threadId: resumeSession,
            threadKey,
            cwd: workingDirectory,
            env: codexEnv,
            config: codexConfig,
        });
    }
    else {
        sessionId = await client.startThread({
            threadKey,
            cwd: workingDirectory,
            env: codexEnv,
            config: codexConfig,
        });
    }
    persistence.updatePendingSessionId(pendingResponseId, sessionId);
    persistence.updateActiveTurnSessionId(threadKey, sessionId);
    const { model, effort } = resolveCodexModelChoice(persistence.getModelChoice?.(threadKey, CODEX_HARNESS) ?? null);
    const turnId = await client.startTurn({
        threadId: sessionId,
        threadKey,
        prompt,
        cwd: workingDirectory,
        env: codexEnv,
        config: codexConfig,
        model,
        effort,
    });
    turnTimer("app_server.turn_start.returned", { turn_id: turnId ?? "unknown" });
    return {
        sessionId,
        turnId,
        events: client.eventsForTurn(sessionId, turnId, controller.signal),
    };
}

async function createAttachedAppServerStream({ sessionId, turnId, persistence, pendingResponseId, controller, turnTimer, threadKey, client = codexAppServerClient, }) {
    if (!sessionId || !turnId) {
        throw new Error("Cannot attach to a Codex goal turn without both session and turn ids");
    }
    persistence.updatePendingSessionId(pendingResponseId, sessionId);
    persistence.updateActiveTurnSessionId(threadKey, sessionId);
    client.claimTurn(sessionId, turnId);
    turnTimer("app_server.goal_turn.attached", { turn_id: turnId });
    return {
        sessionId,
        turnId,
        events: client.eventsForTurn(sessionId, turnId, controller.signal),
    };
}

async function createExecSdkStream({ resumeSession, workingDirectory, codexEnv, codexConfig, prompt, codexFactory, controller, turnTimer, }) {
    const codex = codexFactory
        ? codexFactory()
        : new Codex({
            env: codexEnv,
            config: {
                ...withAlasioCodexModelConfig(codexConfig),
                "features.plugins": false,
            },
        });
    turnTimer("codex.client.created");
    const thread = resumeSession
        ? codex.resumeThread(resumeSession, {
            model: ALASIO_CODEX_MODEL,
            workingDirectory,
            skipGitRepoCheck: true,
            sandboxMode: "danger-full-access",
            approvalPolicy: "never",
            networkAccessEnabled: true,
        })
        : codex.startThread({
            model: ALASIO_CODEX_MODEL,
            workingDirectory,
            skipGitRepoCheck: true,
            sandboxMode: "danger-full-access",
            approvalPolicy: "never",
            networkAccessEnabled: true,
        });
    turnTimer("thread.handle.created", { mode: resumeSession ? "resume" : "start" });
    const streamed = await thread.runStreamed(prompt, { signal: controller.signal });
    turnTimer("run_streamed.returned");
    return {
        sessionId: resumeSession,
        turnId: null,
        events: streamed.events,
    };
}

export async function openCodexEventStream(params) {
    if (!params.codexFactory && CODEX_TRANSPORT !== "exec") {
        return await createAppServerStream(params);
    }
    return await createExecSdkStream(params);
}

export async function openAttachedCodexEventStream(params) {
    if (CODEX_TRANSPORT === "exec") {
        throw new Error("Attached Codex goal turns require the app-server transport");
    }
    return await createAttachedAppServerStream(params);
}

export async function steerCodexTransportTurn({ sessionId, turnId, prompt, client = codexAppServerClient }) {
    if (CODEX_TRANSPORT === "exec") {
        throw new Error("Steering active Codex turns requires the app-server transport");
    }
    await client.steerTurn({ threadId: sessionId, turnId, prompt });
    return true;
}

export async function startCodexTransportThread({ threadKey, workingDirectory, codexEnv, codexConfig, client = codexAppServerClient }) {
    if (CODEX_TRANSPORT === "exec") {
        throw new Error("Starting an empty Codex session requires the app-server transport");
    }
    return await client.startThread({
        threadKey,
        cwd: workingDirectory,
        env: codexEnv,
        config: codexConfig,
    });
}

/** Forks through the app-server under either transport: the fork is a rollout like any other, which exec resumes too. */
export async function forkCodexTransportThread({ sessionId, beforeTurnId, threadKey, workingDirectory, codexEnv, codexConfig, client = codexAppServerClient }) {
    return await client.forkThread({
        threadId: sessionId,
        beforeTurnId,
        threadKey,
        cwd: workingDirectory,
        env: codexEnv,
        config: codexConfig,
    });
}

export async function warmCodexTransportThread({ sessionId, threadKey, workingDirectory, codexEnv, codexConfig, client = codexAppServerClient }) {
    return await client.ensureThread({
        threadId: sessionId,
        threadKey,
        cwd: workingDirectory,
        env: codexEnv,
        config: codexConfig,
    });
}
