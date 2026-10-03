/**
 * Codex app-server protocol objects as Codex reports them, for tests: each whole,
 * with what a test does not set filled in.
 */
import type { InitializeResponse, MessagePhase, ServerNotification, v2 } from "../../.types/codex/index.js";

/** A turn: `fields` over an in-progress turn with no items. */
export function codexTurn(id: string, fields: Partial<v2.Turn> = {}): v2.Turn {
  return { id, items: [], itemsView: "notLoaded", status: "inProgress", error: null, startedAt: null, completedAt: null, durationMs: null, ...fields };
}

/** A thread: `fields` over an idle thread, unnamed, with no turns. */
export function codexThread(id: string, fields: Partial<v2.Thread> = {}): v2.Thread {
  return {
    id,
    extra: null,
    sessionId: id,
    forkedFromId: null,
    parentThreadId: null,
    preview: "",
    ephemeral: false,
    section: null,
    sectionEnteredAt: null,
    projectId: null,
    historyMode: "paginated",
    modelProvider: "openai",
    model: null,
    reasoningEffort: null,
    createdAt: 0,
    updatedAt: 0,
    recencyAt: null,
    status: { type: "idle" },
    path: null,
    cwd: "/repo",
    cliVersion: "0.0.0",
    source: "appServer",
    canAcceptDirectInput: null,
    threadSource: null,
    agentNickname: null,
    agentRole: null,
    gitInfo: null,
    name: null,
    turns: [],
    ...fields,
  };
}

/** The operator's message, of `content`. */
export function userMessage(id: string, content: v2.UserInput[]): v2.ThreadItem {
  return { type: "userMessage", id, clientId: null, content };
}

/** An agent's message. */
export function agentMessage(id: string, text: string, phase: MessagePhase | null = null): v2.ThreadItem {
  return { type: "agentMessage", id, text, phase, memoryCitation: null, delivery: null, questions: null };
}

/** A shell command the agent ran, completed with exit code 0. */
export function commandExecution(id: string, command: string): v2.ThreadItem {
  return {
    type: "commandExecution",
    id,
    pluginId: null,
    scriptPath: null,
    command,
    cwd: "/repo",
    processId: null,
    source: "agent",
    status: "completed",
    commandActions: [],
    aggregatedOutput: "",
    exitCode: 0,
    durationMs: 0,
  };
}

/** What initialize answers. */
export function initializeResponse(): InitializeResponse {
  return { userAgent: "codex-stand-in/0.0.0", codexHome: "/codex-home", platformFamily: "unix", platformOs: "linux" };
}

/** What thread/start answers: `thread`, loaded as alasio loads threads. */
export function threadStartResponse(thread: v2.Thread): v2.ThreadStartResponse {
  return {
    thread,
    model: "gpt-5.6-sol",
    modelProvider: "openai",
    serviceTier: null,
    cwd: thread.cwd,
    runtimeWorkspaceRoots: [],
    instructionSources: [],
    approvalPolicy: "never",
    approvalsReviewer: "user",
    sandbox: { type: "dangerFullAccess" },
    activePermissionProfile: null,
    reasoningEffort: "high",
    multiAgentMode: "explicitRequestOnly",
  };
}

/** What thread/resume answers: `thread`, without its turns. */
export function threadResumeResponse(thread: v2.Thread): v2.ThreadResumeResponse {
  return { ...threadStartResponse(thread), initialTurnsPage: null, turnsBackwardsCursor: null, itemsBackwardsCursor: null };
}

/** A model of the app-server's catalogue: `fields` over a visible model with no efforts. */
export function codexModel(id: string, fields: Partial<v2.Model> = {}): v2.Model {
  return {
    id,
    model: id,
    upgrade: null,
    upgradeInfo: null,
    availabilityNux: null,
    displayName: id,
    description: "",
    modelSpecialty: null,
    hidden: false,
    supportedReasoningEfforts: [],
    defaultReasoningEffort: "medium",
    inputModalities: ["text"],
    supportsPersonality: false,
    multiAgentVersion: null,
    additionalSpeedTiers: [],
    serviceTiers: [],
    defaultServiceTier: null,
    isDefault: false,
    ...fields,
  };
}

/** A thread's goal: `fields` over an active goal with nothing used yet. */
export function threadGoal(threadId: string, objective: string, fields: Partial<v2.ThreadGoal> = {}): v2.ThreadGoal {
  return { threadId, objective, status: "active", tokenBudget: null, tokensUsed: 0, timeUsedSeconds: 0, createdAt: 0, updatedAt: 0, ...fields };
}

/** The app-server's report that `turn` of `threadId` started. */
export function turnStarted(threadId: string, turn: v2.Turn): ServerNotification {
  return { method: "turn/started", params: { threadId, turn } };
}

/** The app-server's report that `item` of a turn completed. */
export function itemCompleted(threadId: string, turnId: string, item: v2.ThreadItem): ServerNotification {
  return { method: "item/completed", params: { threadId, turnId, item, completedAtMs: 0 } };
}

/** The app-server's report that `turn` of `threadId` ended, as its status says. */
export function turnCompleted(threadId: string, turn: v2.Turn): ServerNotification {
  return { method: "turn/completed", params: { threadId, turn } };
}

/** The app-server's report of an error in a turn, which it is retrying or not. */
export function turnError(threadId: string, turnId: string, message: string, willRetry: boolean): ServerNotification {
  return {
    method: "error",
    params: { threadId, turnId, willRetry, error: { message, codexErrorInfo: null, additionalDetails: null, misalignment: null } },
  };
}
