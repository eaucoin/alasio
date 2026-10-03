import { CODEX_HARNESS } from "../harness/names.ts";
import { resolveCodexModelChoice } from "./model.ts";
import { Codex, type ThreadEvent } from "@openai/codex-sdk";
import { type AppServerClient, codexAppServerClient, stopCodexAppServer } from "./app-server/client.ts";
import type { AppServerEvent } from "./app-server/protocol.ts";
import {
    ALASIO_CODEX_MODEL,
    withAlasioCodexModelConfig,
} from "./model.ts";
import { getCodexTransportMode } from "../config.ts";
import type { SqliteStore } from "../persistence/store.ts";
import type { CodexEnv } from "./env.ts";
import type { CodexThreadConfig } from "./thread-config.ts";
import type { TurnTimer } from "./turn-timing.ts";

const CODEX_TRANSPORT = getCodexTransportMode();

/** What a turn reports: the Codex SDK's thread events from exec, or the app-server's in their shape. */
export type CodexEvent = ThreadEvent | AppServerEvent;

/** A turn's events as they stream, with the session and turn they belong to, as far as they are known. */
export interface CodexEventStream {
    readonly sessionId: string | null | undefined;
    readonly turnId: string | null;
    readonly events: AsyncIterable<CodexEvent>;
}

/** Where a transport records the session a turn runs in, and reads the conversation's model choice. */
export type TurnSessionStore =
    & Pick<SqliteStore, "updatePendingSessionId" | "updateActiveTurnSessionId">
    & Partial<Pick<SqliteStore, "getModelChoice">>;

/** A Codex session's location: its app-server client, and the directory, environment, and config it runs with. */
export interface CodexSessionOptions {
    readonly threadKey: string;
    readonly workingDirectory: string;
    readonly codexEnv: CodexEnv;
    readonly codexConfig: CodexThreadConfig;
    readonly client?: AppServerClient | undefined;
}

/** A turn to run: the prompt, the session to resume if any, and where its progress is recorded. */
export interface CodexStreamParams extends CodexSessionOptions {
    readonly resumeSession: string | null | undefined;
    readonly prompt: string;
    readonly persistence: TurnSessionStore;
    readonly pendingResponseId: string;
    readonly controller: AbortController;
    readonly turnTimer: TurnTimer;
    /** Makes the Codex SDK client of the exec transport, for tests. */
    readonly codexFactory?: (() => Codex) | undefined;
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
    readonly client?: AppServerClient | undefined;
}

/** A session to fork before one of its turns. */
export interface ForkCodexSessionOptions extends CodexSessionOptions {
    readonly sessionId: string;
    readonly beforeTurnId: string;
}

/** A session to load in the app-server before its next turn. */
export interface WarmCodexSessionOptions extends CodexSessionOptions {
    readonly sessionId: string;
}

export function canWarmCodexSession(): boolean {
    return CODEX_TRANSPORT !== "exec";
}

export function stopCodexTransport(): void {
    stopCodexAppServer();
}

async function createAppServerStream({ resumeSession, threadKey, workingDirectory, codexEnv, codexConfig, prompt, persistence, pendingResponseId, controller, turnTimer, client = codexAppServerClient, }: CodexStreamParams): Promise<CodexEventStream> {
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

async function createAttachedAppServerStream({ sessionId, turnId, persistence, pendingResponseId, controller, turnTimer, threadKey, client = codexAppServerClient, }: AttachedCodexStreamParams): Promise<CodexEventStream> {
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

async function createExecSdkStream({ resumeSession, workingDirectory, codexEnv, codexConfig, prompt, codexFactory, controller, turnTimer, }: CodexStreamParams): Promise<CodexEventStream> {
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

export async function openCodexEventStream(params: CodexStreamParams): Promise<CodexEventStream> {
    if (!params.codexFactory && CODEX_TRANSPORT !== "exec") {
        return await createAppServerStream(params);
    }
    return await createExecSdkStream(params);
}

export async function openAttachedCodexEventStream(params: AttachedCodexStreamParams): Promise<CodexEventStream> {
    if (CODEX_TRANSPORT === "exec") {
        throw new Error("Attached Codex goal turns require the app-server transport");
    }
    return await createAttachedAppServerStream(params);
}

export async function steerCodexTransportTurn({ sessionId, turnId, prompt, client = codexAppServerClient }: SteerCodexTurnOptions): Promise<boolean> {
    if (CODEX_TRANSPORT === "exec") {
        throw new Error("Steering active Codex turns requires the app-server transport");
    }
    await client.steerTurn({ threadId: sessionId, turnId, prompt });
    return true;
}

export async function startCodexTransportThread({ threadKey, workingDirectory, codexEnv, codexConfig, client = codexAppServerClient }: CodexSessionOptions): Promise<string> {
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
export async function forkCodexTransportThread({ sessionId, beforeTurnId, threadKey, workingDirectory, codexEnv, codexConfig, client = codexAppServerClient }: ForkCodexSessionOptions): Promise<string> {
    return await client.forkThread({
        threadId: sessionId,
        beforeTurnId,
        threadKey,
        cwd: workingDirectory,
        env: codexEnv,
        config: codexConfig,
    });
}

export async function warmCodexTransportThread({ sessionId, threadKey, workingDirectory, codexEnv, codexConfig, client = codexAppServerClient }: WarmCodexSessionOptions): Promise<string> {
    return await client.ensureThread({
        threadId: sessionId,
        threadKey,
        cwd: workingDirectory,
        env: codexEnv,
        config: codexConfig,
    });
}
