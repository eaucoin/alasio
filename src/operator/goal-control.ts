import type { InlineKeyboardButton, InlineKeyboardMarkup } from "@grammyjs/types";
import type { v2 } from "../../.types/codex/index.js";
import type { HarnessGoals } from "../harness/index.ts";
import type { CallbackPayload } from "../persistence/callback-repository.ts";
import type { SqliteStore } from "../persistence/store.ts";
import type { ChatId, Client } from "../telegram/client.ts";
import type { ControlCallback, ControlPanel, ControlPanelOptions, StartNewSession } from "./session-control.ts";
import { truncateText } from "./text.ts";

/** A turn to run toward a goal: attached to the goal's own turn when it has one, else started with `prompt`. */
export interface GoalTurnRequest {
  readonly conversationId: string;
  readonly chatId: ChatId;
  readonly messageId: number;
  readonly sessionId: string;
  readonly turnId: string | null;
  readonly prompt: string;
}

/** Runs a goal's turn; resolves to whether it took the turn over, so no idle panel is needed. */
export type RunGoalTurn = (request: GoalTurnRequest) => Promise<boolean>;

/** Stops the conversation's running turn, if it has one. */
export type StopActiveTurn = () => Promise<unknown>;

/** The store's sessions and callback actions, as the goal panels read them. */
export type GoalControlStore = Pick<SqliteStore, "getSessionId" | "createCallbackAction">;

/** How a goal's turn is going, as the goal panel shows it. */
export type GoalTurnState = "working" | "starting" | "queued" | "idle";

type ButtonStore = Pick<SqliteStore, "createCallbackAction">;

const GOAL_KIND_PREFIX = "goal:";
const UNFINISHED_STATUSES = new Set<v2.ThreadGoalStatus>(["active", "paused", "blocked", "usageLimited", "budgetLimited"]);
const GOAL_TURN_WAIT_MS = 5 * 1000;

function goalKind(kind: string): string {
  return `${GOAL_KIND_PREFIX}${kind}`;
}

export function isGoalControlAction(kind: unknown): boolean {
  return typeof kind === "string" && kind.startsWith(GOAL_KIND_PREFIX);
}

function createButton(
  store: ButtonStore,
  conversationId: string,
  text: string,
  kind: string,
  payload: CallbackPayload = {},
): InlineKeyboardButton.CallbackButton {
  return {
    text,
    callback_data: store.createCallbackAction({
      conversationId,
      kind: goalKind(kind),
      payload,
    }),
  };
}

function createSessionControlButton(
  store: ButtonStore,
  conversationId: string,
  text: string,
  kind: string,
  payload: CallbackPayload = {},
): InlineKeyboardButton.CallbackButton {
  return {
    text,
    callback_data: store.createCallbackAction({
      conversationId,
      kind: `control:${kind}`,
      payload,
    }),
  };
}

function buildPanelOptions(replyMarkup: InlineKeyboardMarkup): ControlPanelOptions {
  return {
    format: "plain",
    reply_markup: replyMarkup,
  };
}

function closeRow(store: ButtonStore, conversationId: string): InlineKeyboardButton.CallbackButton[] {
  return [createButton(store, conversationId, "Close", "close")];
}

function shortSessionId(sessionId: string): string {
  return sessionId ? sessionId.slice(0, 8) : "-";
}

function formatTokens(value: number | null | undefined): string {
  const tokens = Number(value ?? 0);
  if (!Number.isFinite(tokens) || tokens <= 0) {
    return "0";
  }
  if (tokens >= 1_000_000) {
    return `${(tokens / 1_000_000).toFixed(tokens >= 10_000_000 ? 0 : 1).replace(/\.0$/, "")}M`;
  }
  if (tokens >= 1_000) {
    return `${(tokens / 1_000).toFixed(tokens >= 100_000 ? 0 : 1).replace(/\.0$/, "")}K`;
  }
  return String(Math.round(tokens));
}

function formatGoalDuration(seconds: number | null | undefined): string {
  const safeSeconds = Math.max(0, Number(seconds ?? 0));
  if (safeSeconds < 60) {
    return `${Math.floor(safeSeconds)}s`;
  }
  const minutes = Math.floor(safeSeconds / 60);
  if (minutes < 60) {
    return `${minutes}m`;
  }
  const hours = Math.floor(minutes / 60);
  const remainingMinutes = minutes % 60;
  if (hours >= 24) {
    const days = Math.floor(hours / 24);
    const remainingHours = hours % 24;
    return `${days}d ${remainingHours}h ${remainingMinutes}m`;
  }
  return remainingMinutes === 0 ? `${hours}h` : `${hours}h ${remainingMinutes}m`;
}

function formatStatus(status: v2.ThreadGoalStatus): string {
  switch (status) {
    case "active":
      return "active";
    case "paused":
      return "paused";
    case "blocked":
      return "blocked";
    case "usageLimited":
      return "usage limited";
    case "budgetLimited":
      return "limited by budget";
    case "complete":
      return "complete";
    default:
      return String(status ?? "unknown");
  }
}

function formatTurnState(turnState: GoalTurnState | null): string | null {
  switch (turnState) {
    case "working":
      return "working";
    case "starting":
      return "starting";
    case "queued":
      return "queued";
    case "idle":
      return "idle";
    default:
      return null;
  }
}

function formatTokenLine(goal: v2.ThreadGoal): string {
  const used = formatTokens(goal?.tokensUsed);
  if (goal?.tokenBudget == null) {
    return `Tokens: ${used}`;
  }
  return `Tokens: ${used} / ${formatTokens(goal.tokenBudget)}`;
}

function isUnfinishedGoal(goal: v2.ThreadGoal | null): goal is v2.ThreadGoal {
  return Boolean(goal && UNFINISHED_STATUSES.has(goal.status));
}

function shouldConfirmBeforeReplacing(goal: v2.ThreadGoal | null): goal is v2.ThreadGoal {
  return isUnfinishedGoal(goal);
}

export interface GoalPanelTarget {
  readonly store: ButtonStore;
  readonly conversationId: string;
}

export function buildNoMountedGoalPanel({ store, conversationId }: GoalPanelTarget): ControlPanel {
  return {
    text: [
      "Goal",
      "",
      "No Codex session is mounted.",
      "Start a new session or open Sessions to mount an existing one.",
    ].join("\n"),
    options: buildPanelOptions({
      inline_keyboard: [
        [createSessionControlButton(store, conversationId, "Sessions", "sessions", { page: 1 })],
        [createSessionControlButton(store, conversationId, "New Session", "new")],
        closeRow(store, conversationId),
      ],
    }),
  };
}

export function buildNoActiveGoalPanel({ store, conversationId, sessionId }: GoalPanelTarget & { readonly sessionId: string }): ControlPanel {
  return {
    text: [
      "Goal",
      "",
      "No active goal is set for this session.",
      "",
      `Session: ${shortSessionId(sessionId)}`,
      "",
      "Send:",
      "/goal <objective>",
    ].join("\n"),
    options: buildPanelOptions({
      inline_keyboard: [
        closeRow(store, conversationId),
      ],
    }),
  };
}

export interface GoalPanelRequest extends GoalPanelTarget {
  readonly goal: v2.ThreadGoal;
  readonly turnState?: GoalTurnState | null | undefined;
}

export function buildGoalPanel({ store, conversationId, goal, turnState = null }: GoalPanelRequest): ControlPanel {
  const formattedTurnState = formatTurnState(turnState);
  const lines = [
    "Goal",
    "",
    truncateText(goal.objective, 1100),
    "",
    `Status: ${formatStatus(goal.status)}`,
  ];
  if (formattedTurnState) {
    lines.push(`Turn: ${formattedTurnState}`);
  }
  if (Number(goal.timeUsedSeconds ?? 0) > 0) {
    lines.push(`Time: ${formatGoalDuration(goal.timeUsedSeconds)}`);
  }
  lines.push(formatTokenLine(goal));

  const keyboard: InlineKeyboardButton.CallbackButton[][] = [];
  if (goal.status === "active") {
    keyboard.push([
      createButton(store, conversationId, "Pause", "pause"),
      createButton(store, conversationId, "Clear", "clear_confirm"),
    ]);
  } else {
    keyboard.push([
      createButton(store, conversationId, "Resume", "resume"),
      createButton(store, conversationId, "Clear", "clear_confirm"),
    ]);
  }
  keyboard.push(closeRow(store, conversationId));

  return {
    text: lines.join("\n"),
    options: buildPanelOptions({ inline_keyboard: keyboard }),
  };
}

export function buildReplaceGoalPanel({ store, conversationId, currentGoal, objective }: GoalPanelTarget & {
  readonly currentGoal: v2.ThreadGoal;
  readonly objective: string;
}): ControlPanel {
  return {
    text: [
      "Replace goal?",
      "",
      "Current:",
      truncateText(currentGoal.objective, 420),
      "",
      "New:",
      truncateText(objective, 420),
    ].join("\n"),
    options: buildPanelOptions({
      inline_keyboard: [
        [createButton(store, conversationId, "Replace Goal", "replace", { objective })],
        [createButton(store, conversationId, "Keep Current Goal", "show")],
        closeRow(store, conversationId),
      ],
    }),
  };
}

export function buildClearGoalConfirmationPanel({ store, conversationId, goal }: GoalPanelTarget & { readonly goal: v2.ThreadGoal }): ControlPanel {
  return {
    text: [
      "Clear goal?",
      "",
      truncateText(goal.objective, 700),
    ].join("\n"),
    options: buildPanelOptions({
      inline_keyboard: [
        [createButton(store, conversationId, "Clear Goal", "clear")],
        [createButton(store, conversationId, "Keep Goal", "show")],
        closeRow(store, conversationId),
      ],
    }),
  };
}

function buildGoalTurnPrompt(goal: v2.ThreadGoal): string {
  return [
    "Continue working toward this Codex goal.",
    "",
    goal?.objective ?? "",
  ].join("\n");
}

async function ensureGoalTurn({ conversationId, chatId, messageId, sessionId, goal, runGoalTurn, goalApi }: {
  readonly conversationId: string;
  readonly chatId: ChatId;
  readonly messageId: number;
  readonly sessionId: string;
  readonly goal: v2.ThreadGoal | null;
  readonly runGoalTurn: RunGoalTurn | null | undefined;
  readonly goalApi: HarnessGoals;
}): Promise<boolean> {
  if (!runGoalTurn || goal?.status !== "active") {
    return false;
  }
  const turnId = await goalApi.waitForTurnId(sessionId, GOAL_TURN_WAIT_MS);
  return await runGoalTurn({
    conversationId,
    chatId,
    messageId,
    sessionId,
    turnId,
    prompt: buildGoalTurnPrompt(goal),
  });
}

async function setFreshObjectiveGoal({ sessionId, objective, currentGoal, goalApi }: {
  readonly sessionId: string;
  readonly objective: string;
  readonly currentGoal: v2.ThreadGoal | null;
  readonly goalApi: HarnessGoals;
}): Promise<v2.ThreadGoal | null> {
  if (currentGoal) {
    await goalApi.clear({ threadId: sessionId });
  }
  return await goalApi.set({ threadId: sessionId, objective, status: "active" });
}

async function buildGoalPanelFromState({ store, conversationId, turnState = null, goalApi }: {
  readonly store: GoalControlStore;
  readonly conversationId: string;
  readonly turnState?: GoalTurnState | null;
  readonly goalApi: HarnessGoals;
}): Promise<ControlPanel> {
  const sessionId = store.getSessionId(conversationId);
  if (!sessionId) {
    return buildNoMountedGoalPanel({ store, conversationId });
  }
  const goal = await goalApi.read({ threadId: sessionId });
  if (!isUnfinishedGoal(goal)) {
    return buildNoActiveGoalPanel({ store, conversationId, sessionId });
  }
  return buildGoalPanel({ store, conversationId, goal, turnState });
}

async function editPanel(client: Pick<Client, "editMessageText">, chatId: ChatId, messageId: number, panel: ControlPanel): Promise<void> {
  try {
    await client.editMessageText(chatId, messageId, panel.text, panel.options);
  } catch (error) {
    if (!String(error).includes("message is not modified")) {
      throw error;
    }
  }
}

async function sendErrorPanel({ client, chatId, error }: {
  readonly client: Pick<Client, "sendMessage">;
  readonly chatId: ChatId;
  readonly error: unknown;
}): Promise<void> {
  const message = error instanceof Error ? error.message : String(error);
  await client.sendMessage(chatId, [
    "Goal",
    "",
    `Failed to update goal: ${message}`,
  ].join("\n"), { format: "plain" });
}

interface KnownGoal {
  readonly store: ButtonStore;
  readonly conversationId: string;
  readonly sessionId: string;
  readonly goal: v2.ThreadGoal | null;
  readonly turnState?: GoalTurnState | null;
}

function buildPanelForKnownGoal({ store, conversationId, sessionId, goal, turnState = null }: KnownGoal): ControlPanel {
  if (!isUnfinishedGoal(goal)) {
    return buildNoActiveGoalPanel({ store, conversationId, sessionId });
  }
  return buildGoalPanel({ store, conversationId, goal, turnState });
}

async function sendKnownGoalPanel({ client, store, conversationId, chatId, sessionId, goal, turnState = null }: KnownGoal & {
  readonly client: Pick<Client, "sendMessage">;
  readonly chatId: ChatId;
}): Promise<void> {
  const panel = buildPanelForKnownGoal({ store, conversationId, sessionId, goal, turnState });
  await client.sendMessage(chatId, panel.text, panel.options);
}

export interface SendGoalPanelRequest {
  readonly client: Pick<Client, "sendMessage">;
  readonly store: GoalControlStore;
  readonly conversationId: string;
  readonly chatId: ChatId;
  readonly isTurnActive?: boolean | undefined;
  readonly goalApi: HarnessGoals;
}

export async function sendGoalPanel({ client, store, conversationId, chatId, isTurnActive = false, goalApi }: SendGoalPanelRequest): Promise<void> {
  try {
    const panel = await buildGoalPanelFromState({
      store,
      conversationId,
      turnState: isTurnActive ? "working" : "idle",
      goalApi,
    });
    await client.sendMessage(chatId, panel.text, panel.options);
  } catch (error) {
    await sendErrorPanel({ client, chatId, error });
  }
}

export interface GoalTextCommand extends SendGoalPanelRequest {
  /** The message the command came in, which a goal's turn replies to. */
  readonly messageId: number;
  /** What followed /goal: an objective, or one of clear, pause, resume, and edit. */
  readonly args: string;
  readonly runGoalTurn?: RunGoalTurn | null | undefined;
  readonly stopActiveTurn?: StopActiveTurn | null | undefined;
  /** Mounts a session to set the goal on when none is; without it, a session must be mounted first. */
  readonly startNewSession?: StartNewSession | null | undefined;
}

export async function handleGoalTextCommand({
  client,
  store,
  conversationId,
  chatId,
  messageId,
  args,
  runGoalTurn,
  stopActiveTurn,
  startNewSession,
  isTurnActive = false,
  goalApi,
}: GoalTextCommand): Promise<boolean> {
  let sessionId = store.getSessionId(conversationId);
  const trimmed = String(args ?? "").trim();
  if (!trimmed) {
    await sendGoalPanel({ client, store, conversationId, chatId, isTurnActive, goalApi });
    return true;
  }

  try {
    const control = trimmed.toLowerCase();
    if (!sessionId) {
      if (["clear", "pause", "resume", "edit"].includes(control) || !startNewSession) {
        const panel = buildNoMountedGoalPanel({ store, conversationId });
        await client.sendMessage(chatId, panel.text, panel.options);
        return true;
      }
      sessionId = await startNewSession({ conversationId });
    }
    if (control === "clear") {
      await goalApi.clear({ threadId: sessionId });
      await stopActiveTurn?.();
      await sendGoalPanel({ client, store, conversationId, chatId, isTurnActive: false, goalApi });
      return true;
    }
    if (control === "pause") {
      const currentGoal = await goalApi.read({ threadId: sessionId });
      if (!isUnfinishedGoal(currentGoal)) {
        await sendGoalPanel({ client, store, conversationId, chatId, isTurnActive, goalApi });
        return true;
      }
      const goal = await goalApi.set({ threadId: sessionId, status: "paused" });
      await stopActiveTurn?.();
      await sendKnownGoalPanel({ client, store, conversationId, chatId, sessionId, goal, turnState: "idle" });
      return true;
    }
    if (control === "resume") {
      const currentGoal = await goalApi.read({ threadId: sessionId });
      if (!isUnfinishedGoal(currentGoal)) {
        await sendGoalPanel({ client, store, conversationId, chatId, isTurnActive, goalApi });
        return true;
      }
      const goal = await goalApi.set({ threadId: sessionId, status: "active" });
      if (await ensureGoalTurn({ conversationId, chatId, messageId, sessionId, goal, runGoalTurn, goalApi })) {
        return true;
      }
      await sendKnownGoalPanel({ client, store, conversationId, chatId, sessionId, goal, turnState: "idle" });
      return true;
    }
    if (control === "edit") {
      await client.sendMessage(chatId, "Send /goal <objective> to replace the current goal.", { format: "plain" });
      return true;
    }

    const currentGoal = await goalApi.read({ threadId: sessionId });
    if (shouldConfirmBeforeReplacing(currentGoal)) {
      const panel = buildReplaceGoalPanel({ store, conversationId, currentGoal, objective: trimmed });
      await client.sendMessage(chatId, panel.text, panel.options);
      return true;
    }

    const goal = await setFreshObjectiveGoal({ sessionId, objective: trimmed, currentGoal, goalApi });
    if (await ensureGoalTurn({ conversationId, chatId, messageId, sessionId, goal, runGoalTurn, goalApi })) {
      return true;
    }
    await sendKnownGoalPanel({ client, store, conversationId, chatId, sessionId, goal, turnState: "idle" });
    return true;
  } catch (error) {
    await sendErrorPanel({ client, chatId, error });
    return true;
  }
}

export interface GoalControlCallback extends ControlCallback {
  readonly client: Pick<Client, "answerCallbackQuery" | "editMessageText" | "deleteMessage">;
  readonly store: GoalControlStore;
  readonly runGoalTurn?: RunGoalTurn | null | undefined;
  readonly stopActiveTurn?: StopActiveTurn | null | undefined;
  readonly isTurnActive?: boolean | undefined;
  /** The harness's goals; a harness without them can only close a goal panel. */
  readonly goalApi: HarnessGoals | undefined;
}

export async function handleGoalControlCallback({
  client,
  store,
  action,
  callbackQueryId,
  chatId,
  messageId,
  runGoalTurn,
  stopActiveTurn,
  isTurnActive = false,
  goalApi,
}: GoalControlCallback): Promise<void> {
  const kind = action.kind.slice(GOAL_KIND_PREFIX.length);
  const sessionId = store.getSessionId(action.conversationId);
  const payload = action.payload ?? {};

  if (kind === "close") {
    await client.answerCallbackQuery(callbackQueryId, "Closed.");
    try {
      await client.deleteMessage(chatId, messageId);
    } catch {
      await client.editMessageText(chatId, messageId, "Closed.", { format: "plain" });
    }
    return;
  }

  if (!goalApi) {
    await client.answerCallbackQuery(callbackQueryId, "Goals are not available for this service.");
    return;
  }

  if (!sessionId) {
    await client.answerCallbackQuery(callbackQueryId, "No mounted session.");
    const panel = buildNoMountedGoalPanel({ store, conversationId: action.conversationId });
    await editPanel(client, chatId, messageId, panel);
    return;
  }

  try {
    let goalToRun: v2.ThreadGoal | null = null;
    let panel: ControlPanel | null = null;
    if (kind === "show") {
      await client.answerCallbackQuery(callbackQueryId);
      const panel = await buildGoalPanelFromState({
        store,
        conversationId: action.conversationId,
        turnState: isTurnActive ? "working" : "idle",
        goalApi,
      });
      await editPanel(client, chatId, messageId, panel);
      return;
    }
    if (kind === "pause") {
      const goal = await goalApi.set({ threadId: sessionId, status: "paused" });
      await stopActiveTurn?.();
      panel = buildPanelForKnownGoal({ store, conversationId: action.conversationId, sessionId, goal, turnState: "idle" });
      await client.answerCallbackQuery(callbackQueryId, "Paused.");
    } else if (kind === "resume") {
      goalToRun = await goalApi.set({ threadId: sessionId, status: "active" });
      panel = buildPanelForKnownGoal({ store, conversationId: action.conversationId, sessionId, goal: goalToRun, turnState: "starting" });
      await client.answerCallbackQuery(callbackQueryId, "Starting.");
    } else if (kind === "replace") {
      await goalApi.clear({ threadId: sessionId });
      const objective = payload["objective"];
      goalToRun = await goalApi.set({ threadId: sessionId, objective: typeof objective === "string" ? objective : undefined, status: "active" });
      panel = buildPanelForKnownGoal({ store, conversationId: action.conversationId, sessionId, goal: goalToRun, turnState: "starting" });
      await client.answerCallbackQuery(callbackQueryId, "Starting.");
    } else if (kind === "clear_confirm") {
      const goal = await goalApi.read({ threadId: sessionId });
      const panel = goal
        ? buildClearGoalConfirmationPanel({ store, conversationId: action.conversationId, goal })
        : buildNoActiveGoalPanel({ store, conversationId: action.conversationId, sessionId });
      await client.answerCallbackQuery(callbackQueryId);
      await editPanel(client, chatId, messageId, panel);
      return;
    } else if (kind === "clear") {
      await goalApi.clear({ threadId: sessionId });
      await stopActiveTurn?.();
      panel = buildNoActiveGoalPanel({ store, conversationId: action.conversationId, sessionId });
      await client.answerCallbackQuery(callbackQueryId, "Cleared.");
    } else {
      await client.answerCallbackQuery(callbackQueryId, "Unknown action.");
      return;
    }

    panel ??= await buildGoalPanelFromState({
      store,
      conversationId: action.conversationId,
      turnState: isTurnActive ? "working" : "idle",
      goalApi,
    });
    await editPanel(client, chatId, messageId, panel);
    if (goalToRun) {
      const handled = await ensureGoalTurn({
        conversationId: action.conversationId,
        chatId,
        messageId,
        sessionId,
        goal: goalToRun,
        runGoalTurn,
        goalApi,
      });
      if (!handled) {
        const idlePanel = buildPanelForKnownGoal({ store, conversationId: action.conversationId, sessionId, goal: goalToRun, turnState: "idle" });
        await editPanel(client, chatId, messageId, idlePanel);
      }
    }
  } catch (error) {
    await client.answerCallbackQuery(callbackQueryId, "Goal update failed.");
    const message = error instanceof Error ? error.message : String(error);
    await client.editMessageText(chatId, messageId, `Goal\n\nFailed to update goal: ${message}`, { format: "plain" });
  }
}
