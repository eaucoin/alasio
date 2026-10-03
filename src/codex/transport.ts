import { CODEX_HARNESS } from "../harness/names.ts";
import { resolveCodexModelChoice } from "./model.ts";
import { Codex, type Thread, type ThreadEvent, type ThreadOptions } from "@openai/codex-sdk";
import { Effect, Schema, type Scope, Stream } from "effect";
import type { AppServer, AppServerEventsError } from "./app-server/client.ts";
import type { AppServerEvent } from "./app-server/protocol.ts";
import type { AppServerRequestError, AppServerStartError } from "./app-server/rpc-client.ts";
import type { NoActiveTurn, StaleTurnCleanupError, ThreadIdMissing } from "./app-server/thread-client.ts";
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

/** The exec transport's Codex failed, as the Codex SDK reported it. */
export class CodexExecError extends Schema.TaggedError<CodexExecError>()("CodexExecError", {
    cause: Schema.Defect(),
}) {
    override get message(): string {
        return this.cause instanceof Error ? this.cause.message : String(this.cause);
    }
}

/** What was asked of the transport is something it does not do. */
export class CodexTransportRefused extends Schema.TaggedError<CodexTransportRefused>()("CodexTransportRefused", {
    message: Schema.String,
}) {}

/** How a turn's events stop before its end. */
export type CodexEventsError = AppServerEventsError | CodexExecError;

/** How opening a turn's events fails. */
export type CodexOpenError = AppServerStartError | ThreadIdMissing | StaleTurnCleanupError | CodexExecError | CodexTransportRefused;

/** A turn's events as they stream, with the session and turn they belong to, as far as they are known. */
export interface CodexEventStream {
    readonly sessionId: string | null | undefined;
    readonly turnId: string | null;
    readonly events: Stream.Stream<CodexEvent, CodexEventsError>;
}

/** Where a transport records the session a turn runs in, and reads the conversation's model choice. */
export type TurnSessionStore =
    & Pick<SqliteStore, "updatePendingSessionId" | "updateActiveTurnSessionId">
    & Partial<Pick<SqliteStore, "getModelChoice">>;

/** A Codex session's location: its app-server, and the directory, environment, and config it runs with. */
export interface CodexSessionOptions {
    readonly threadKey: string;
    readonly workingDirectory: string;
    readonly codexEnv: CodexEnv;
    readonly codexConfig: CodexThreadConfig;
    readonly appServer: AppServer;
}

/** What the exec transport uses of the Codex SDK's client: threads to stream a turn in. */
export interface CodexExecClient {
    startThread(options?: ThreadOptions): Pick<Thread, "runStreamed">;
    resumeThread(id: string, options?: ThreadOptions): Pick<Thread, "runStreamed">;
}

/** A turn to run: the prompt, the session to resume if any, and where its progress is recorded. */
export interface CodexStreamParams extends CodexSessionOptions {
    readonly resumeSession: string | null | undefined;
    readonly prompt: string;
    readonly persistence: TurnSessionStore;
    readonly pendingResponseId: string;
    readonly turnTimer: TurnTimer;
    /** Makes the Codex SDK client of the exec transport, for tests. */
    readonly codexFactory?: (() => CodexExecClient) | undefined;
    /** Called just before the prompt is sent, after which the agent may act on it. */
    readonly onPromptDispatched?: (() => void) | undefined;
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

/** A session to load in the app-server before its next turn. */
export interface WarmCodexSessionOptions extends CodexSessionOptions {
    readonly sessionId: string;
}

export function canWarmCodexSession(): boolean {
    return CODEX_TRANSPORT !== "exec";
}

const createAppServerStream = Effect.fnUntraced(function*({ resumeSession, threadKey, workingDirectory, codexEnv, codexConfig, prompt, persistence, pendingResponseId, turnTimer, appServer, onPromptDispatched }: CodexStreamParams) {
    const scope = { threadKey, cwd: workingDirectory, env: codexEnv, config: codexConfig };
    const sessionId = resumeSession
        ? yield* appServer.ensureThread({ threadId: resumeSession, ...scope })
        : yield* appServer.startThread(scope);
    persistence.updatePendingSessionId(pendingResponseId, sessionId);
    persistence.updateActiveTurnSessionId(threadKey, sessionId);
    const { model, effort } = resolveCodexModelChoice(persistence.getModelChoice?.(threadKey, CODEX_HARNESS) ?? null);
    const turnId = yield* appServer.startTurn({ threadId: sessionId, prompt, model, effort, onPromptDispatched, ...scope });
    turnTimer("app_server.turn_start.returned", { turn_id: turnId ?? "unknown" });
    const stream: CodexEventStream = { sessionId, turnId, events: appServer.eventsForTurn(sessionId, turnId) };
    return stream;
});

const createAttachedAppServerStream = Effect.fnUntraced(function*({ sessionId, turnId, persistence, pendingResponseId, turnTimer, threadKey, appServer }: AttachedCodexStreamParams) {
    if (!sessionId || !turnId) {
        return yield* new CodexTransportRefused({ message: "Cannot attach to a Codex goal turn without both session and turn ids" });
    }
    persistence.updatePendingSessionId(pendingResponseId, sessionId);
    persistence.updateActiveTurnSessionId(threadKey, sessionId);
    yield* appServer.claimTurn(sessionId, turnId);
    turnTimer("app_server.goal_turn.attached", { turn_id: turnId });
    const stream: CodexEventStream = { sessionId, turnId, events: appServer.eventsForTurn(sessionId, turnId) };
    return stream;
});

/** The exec transport's turn: a Codex SDK thread run with the prompt, stopped when the scope closes. */
const createExecSdkStream = Effect.fnUntraced(function*({ resumeSession, workingDirectory, codexEnv, codexConfig, prompt, codexFactory, turnTimer, onPromptDispatched }: CodexStreamParams) {
    const codex = yield* Effect.try({
        try: () => codexFactory
            ? codexFactory()
            : new Codex({
                env: codexEnv,
                config: {
                    ...withAlasioCodexModelConfig(codexConfig),
                    "features.plugins": false,
                },
            }),
        catch: (cause) => new CodexExecError({ cause }),
    });
    turnTimer("codex.client.created");
    const threadOptions: ThreadOptions = {
        model: ALASIO_CODEX_MODEL,
        workingDirectory,
        skipGitRepoCheck: true,
        sandboxMode: "danger-full-access",
        approvalPolicy: "never",
        networkAccessEnabled: true,
    };
    const thread = resumeSession ? codex.resumeThread(resumeSession, threadOptions) : codex.startThread(threadOptions);
    turnTimer("thread.handle.created", { mode: resumeSession ? "resume" : "start" });
    onPromptDispatched?.();
    // Codex runs as long as its events are read, until the turn's scope closes.
    const signal = yield* Effect.abortSignal;
    const streamed = yield* Effect.tryPromise({
        try: () => thread.runStreamed(prompt, { signal }),
        catch: (cause) => new CodexExecError({ cause }),
    });
    turnTimer("run_streamed.returned");
    const stream: CodexEventStream = {
        sessionId: resumeSession,
        turnId: null,
        events: Stream.fromAsyncIterable(streamed.events, (cause) => new CodexExecError({ cause })),
    };
    return stream;
});

/** The turn's events, from the app-server unless the exec transport is chosen (or a test's Codex SDK given). */
export function openCodexEventStream(params: CodexStreamParams): Effect.Effect<CodexEventStream, CodexOpenError, Scope.Scope> {
    if (!params.codexFactory && CODEX_TRANSPORT !== "exec") {
        return createAppServerStream(params);
    }
    return createExecSdkStream(params);
}

export function openAttachedCodexEventStream(params: AttachedCodexStreamParams): Effect.Effect<CodexEventStream, CodexOpenError> {
    if (CODEX_TRANSPORT === "exec") {
        return Effect.fail(new CodexTransportRefused({ message: "Attached Codex goal turns require the app-server transport" }));
    }
    return createAttachedAppServerStream(params);
}

export function steerCodexTransportTurn({ sessionId, turnId, prompt, appServer }: SteerCodexTurnOptions): Effect.Effect<boolean, AppServerRequestError | NoActiveTurn | CodexTransportRefused> {
    if (CODEX_TRANSPORT === "exec") {
        return Effect.fail(new CodexTransportRefused({ message: "Steering active Codex turns requires the app-server transport" }));
    }
    return appServer.steerTurn({ threadId: sessionId, turnId, prompt }).pipe(Effect.as(true));
}

export function startCodexTransportThread({ threadKey, workingDirectory, codexEnv, codexConfig, appServer }: CodexSessionOptions): Effect.Effect<string, AppServerStartError | ThreadIdMissing | CodexTransportRefused> {
    if (CODEX_TRANSPORT === "exec") {
        return Effect.fail(new CodexTransportRefused({ message: "Starting an empty Codex session requires the app-server transport" }));
    }
    return appServer.startThread({ threadKey, cwd: workingDirectory, env: codexEnv, config: codexConfig });
}

/** Forks through the app-server under either transport: the fork is a rollout like any other, which exec resumes too. */
export function forkCodexTransportThread({ sessionId, beforeTurnId, threadKey, workingDirectory, codexEnv, codexConfig, appServer }: ForkCodexSessionOptions): Effect.Effect<string, AppServerStartError | ThreadIdMissing> {
    return appServer.forkThread({ threadId: sessionId, beforeTurnId, threadKey, cwd: workingDirectory, env: codexEnv, config: codexConfig });
}

export function warmCodexTransportThread({ sessionId, threadKey, workingDirectory, codexEnv, codexConfig, appServer }: WarmCodexSessionOptions): Effect.Effect<string, AppServerStartError | ThreadIdMissing> {
    return appServer.ensureThread({ threadId: sessionId, threadKey, cwd: workingDirectory, env: codexEnv, config: codexConfig });
}
