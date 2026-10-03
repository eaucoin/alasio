/**
 * Codex app-server protocol objects as Codex reports them, for tests: each whole,
 * with what a test does not set filled in.
 */
import type { MessagePhase, v2 } from "../../.types/codex/index.js";

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
