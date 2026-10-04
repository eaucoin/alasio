import type { InlineKeyboardButton } from "@grammyjs/types";
import type { v2 } from "../../.types/codex/index.js";
import { Effect } from "effect";

import { type TurnError, Turns } from "../codex/turns.ts";
import { ActiveTurns } from "../harness/active-turns.ts";
import type { HarnessError, HarnessGoals } from "../harness/index.ts";
import type { CallbackPayload } from "../persistence/callback-repository.ts";
import { type SqliteStore, Store } from "../persistence/store.ts";
import { type ChatId, TelegramClient, type TelegramError } from "../telegram/client.ts";
import { type ControlCallback, type ControlPanel, closePanel, editPanel, panelOptions, sendPanel } from "./panel.ts";
import type { NewSessionError } from "./session-control.ts";
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

/** How changing a goal fails, as its panel says. */
type GoalError = HarnessError | NewSessionError | TurnError | TelegramError;

/** What the goal controls run on: the store, Telegram, and the turns, running and to run. */
export type GoalServices = Store | TelegramClient | ActiveTurns | Turns;

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
    options: panelOptions({
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
    options: panelOptions({
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
    options: panelOptions({ inline_keyboard: keyboard }),
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
    options: panelOptions({
      inline_keyboard: [
        [createButton(store, conversationId, "Replace Goal", "replace", { objective })],
        [createButton(store, conversationId, "Keep Current Goal", "show")],
        closeRow(store, conversationId),
      ],
    }),
  };
}

function buildClearGoalConfirmationPanel({ store, conversationId, goal }: GoalPanelTarget & { readonly goal: v2.ThreadGoal }): ControlPanel {
  return {
    text: [
      "Clear goal?",
      "",
      truncateText(goal.objective, 700),
    ].join("\n"),
    options: panelOptions({
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

/**
 * Runs a goal's turn when the goal is active: attached to the turn Codex started for it,
 * if it starts one soon, or started with the goal as its prompt. Whether it took the
 * turn over, so that no idle panel is needed.
 */
const ensureGoalTurn = Effect.fnUntraced(function*({ conversationId, chatId, messageId, sessionId, goal, goals }: {
  readonly conversationId: string;
  readonly chatId: ChatId;
  readonly messageId: number;
  readonly sessionId: string;
  readonly goal: v2.ThreadGoal | null;
  readonly goals: HarnessGoals;
}): Effect.fn.Return<boolean, GoalError, Turns> {
  if (goal?.status !== "active") {
    return false;
  }
  const turnId = yield* goals.waitForTurnId(sessionId, GOAL_TURN_WAIT_MS);
  return yield* Effect.flatMap(Turns, (turns) => turns.runGoalTurn({
    conversationId,
    chatId,
    messageId,
    sessionId,
    turnId,
    prompt: buildGoalTurnPrompt(goal),
  }));
});

const setFreshObjectiveGoal = Effect.fnUntraced(function*({ sessionId, objective, currentGoal, goals }: {
  readonly sessionId: string;
  readonly objective: string;
  readonly currentGoal: v2.ThreadGoal | null;
  readonly goals: HarnessGoals;
}): Effect.fn.Return<v2.ThreadGoal | null, HarnessError> {
  if (currentGoal) {
    yield* goals.clear({ threadId: sessionId });
  }
  return yield* goals.set({ threadId: sessionId, objective, status: "active" });
});

const buildGoalPanelFromState = Effect.fnUntraced(function*({ conversationId, turnState = null, goals }: {
  readonly conversationId: string;
  readonly turnState?: GoalTurnState | null;
  readonly goals: HarnessGoals;
}): Effect.fn.Return<ControlPanel, HarnessError, Store> {
  const store = yield* Store;
  const sessionId = store.getSessionId(conversationId);
  if (!sessionId) {
    return buildNoMountedGoalPanel({ store, conversationId });
  }
  const goal = yield* goals.read({ threadId: sessionId });
  if (!isUnfinishedGoal(goal)) {
    return buildNoActiveGoalPanel({ store, conversationId, sessionId });
  }
  return buildGoalPanel({ store, conversationId, goal, turnState });
});

/** Says a goal could not be changed, and why. */
const sendErrorPanel = (chatId: ChatId, error: GoalError): Effect.Effect<void, TelegramError, TelegramClient> =>
  Effect.flatMap(TelegramClient, (client) => client.sendMessage(chatId, [
    "Goal",
    "",
    `Failed to update goal: ${error.message}`,
  ].join("\n"), { format: "plain" })).pipe(Effect.asVoid);

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

interface SendGoalPanelRequest {
  readonly conversationId: string;
  readonly chatId: ChatId;
  readonly isTurnActive?: boolean | undefined;
  /** The mounted harness's goals. */
  readonly goals: HarnessGoals;
}

/** Sends the conversation's goal panel, or, when the goal cannot be read, why. */
const sendGoalPanel = Effect.fnUntraced(function*({ conversationId, chatId, isTurnActive = false, goals }: SendGoalPanelRequest): Effect.fn.Return<
  void,
  TelegramError,
  Store | TelegramClient
> {
  yield* buildGoalPanelFromState({ conversationId, turnState: isTurnActive ? "working" : "idle", goals }).pipe(
    Effect.flatMap((panel) => sendPanel(chatId, panel)),
    Effect.catch((error) => sendErrorPanel(chatId, error)),
  );
});

export interface GoalTextCommand {
  readonly conversationId: string;
  readonly chatId: ChatId;
  /** The message the command came in, which a goal's turn replies to. */
  readonly messageId: number;
  /** What followed /goal: an objective, or one of clear, pause, resume, and edit. */
  readonly args: string;
  /** The mounted harness's goals. */
  readonly goals: HarnessGoals;
}

/**
 * /goal: the goal panel, a change to the goal (clear, pause, resume), or a new objective,
 * which a goal already unfinished asks before replacing. With no session mounted, a new
 * one is mounted to set an objective on.
 */
export const handleGoalTextCommand = Effect.fnUntraced(function*({ conversationId, chatId, messageId, args, goals }: GoalTextCommand): Effect.fn.Return<
  void,
  TelegramError,
  GoalServices
> {
  const store = yield* Store;
  const client = yield* TelegramClient;
  const activeTurns = yield* ActiveTurns;
  const isTurnActive = yield* activeTurns.isBusy(conversationId);
  const stopActiveTurn = activeTurns.stop(conversationId, "interrupt");
  const trimmed = String(args ?? "").trim();
  if (!trimmed) {
    return yield* sendGoalPanel({ conversationId, chatId, isTurnActive, goals });
  }

  const sendKnownGoalPanel = (sessionId: string, goal: v2.ThreadGoal | null) =>
    sendPanel(chatId, buildPanelForKnownGoal({ store, conversationId, sessionId, goal, turnState: "idle" }));

  yield* Effect.gen(function*() {
    const control = trimmed.toLowerCase();
    let sessionId = store.getSessionId(conversationId);
    if (!sessionId) {
      if (["clear", "pause", "resume", "edit"].includes(control)) {
        return yield* sendPanel(chatId, buildNoMountedGoalPanel({ store, conversationId }));
      }
      sessionId = yield* Effect.flatMap(Turns, (turns) => turns.startNewSession(conversationId));
    }
    if (control === "clear") {
      yield* goals.clear({ threadId: sessionId });
      yield* stopActiveTurn;
      return yield* sendGoalPanel({ conversationId, chatId, isTurnActive: false, goals });
    }
    if (control === "pause") {
      const currentGoal = yield* goals.read({ threadId: sessionId });
      if (!isUnfinishedGoal(currentGoal)) {
        return yield* sendGoalPanel({ conversationId, chatId, isTurnActive, goals });
      }
      const goal = yield* goals.set({ threadId: sessionId, status: "paused" });
      yield* stopActiveTurn;
      return yield* sendKnownGoalPanel(sessionId, goal);
    }
    if (control === "resume") {
      const currentGoal = yield* goals.read({ threadId: sessionId });
      if (!isUnfinishedGoal(currentGoal)) {
        return yield* sendGoalPanel({ conversationId, chatId, isTurnActive, goals });
      }
      const goal = yield* goals.set({ threadId: sessionId, status: "active" });
      if (yield* ensureGoalTurn({ conversationId, chatId, messageId, sessionId, goal, goals })) {
        return;
      }
      return yield* sendKnownGoalPanel(sessionId, goal);
    }
    if (control === "edit") {
      yield* client.sendMessage(chatId, "Send /goal <objective> to replace the current goal.", { format: "plain" });
      return;
    }

    const currentGoal = yield* goals.read({ threadId: sessionId });
    if (shouldConfirmBeforeReplacing(currentGoal)) {
      return yield* sendPanel(chatId, buildReplaceGoalPanel({ store, conversationId, currentGoal, objective: trimmed }));
    }
    const goal = yield* setFreshObjectiveGoal({ sessionId, objective: trimmed, currentGoal, goals });
    if (yield* ensureGoalTurn({ conversationId, chatId, messageId, sessionId, goal, goals })) {
      return;
    }
    yield* sendKnownGoalPanel(sessionId, goal);
  }).pipe(Effect.catch((error) => sendErrorPanel(chatId, error)));
});

export interface GoalControlCallback extends ControlCallback {
  /** The harness's goals; a harness without them can only close a goal panel. */
  readonly goals: HarnessGoals | undefined;
}

export const handleGoalControlCallback = Effect.fnUntraced(function*({
  action,
  callbackQueryId,
  chatId,
  messageId,
  goals,
}: GoalControlCallback): Effect.fn.Return<void, TelegramError, GoalServices> {
  const store = yield* Store;
  const client = yield* TelegramClient;
  const activeTurns = yield* ActiveTurns;
  const { conversationId } = action;
  const isTurnActive = yield* activeTurns.isBusy(conversationId);
  const stopActiveTurn = activeTurns.stop(conversationId, "interrupt");
  const kind = action.kind.slice(GOAL_KIND_PREFIX.length);
  const sessionId = store.getSessionId(conversationId);
  const payload = action.payload ?? {};

  if (kind === "close") {
    return yield* closePanel({ callbackQueryId, chatId, messageId });
  }

  if (!goals) {
    yield* client.answerCallbackQuery(callbackQueryId, "Goals are not available for this service.");
    return;
  }

  if (!sessionId) {
    yield* client.answerCallbackQuery(callbackQueryId, "No mounted session.");
    return yield* editPanel(chatId, messageId, buildNoMountedGoalPanel({ store, conversationId }));
  }

  yield* Effect.gen(function*() {
    let goalToRun: v2.ThreadGoal | null = null;
    let panel: ControlPanel | null = null;
    if (kind === "show") {
      yield* client.answerCallbackQuery(callbackQueryId);
      return yield* editPanel(chatId, messageId, yield* buildGoalPanelFromState({ conversationId, turnState: isTurnActive ? "working" : "idle", goals }));
    }
    if (kind === "pause") {
      const goal = yield* goals.set({ threadId: sessionId, status: "paused" });
      yield* stopActiveTurn;
      panel = buildPanelForKnownGoal({ store, conversationId, sessionId, goal, turnState: "idle" });
      yield* client.answerCallbackQuery(callbackQueryId, "Paused.");
    } else if (kind === "resume") {
      goalToRun = yield* goals.set({ threadId: sessionId, status: "active" });
      panel = buildPanelForKnownGoal({ store, conversationId, sessionId, goal: goalToRun, turnState: "starting" });
      yield* client.answerCallbackQuery(callbackQueryId, "Starting.");
    } else if (kind === "replace") {
      yield* goals.clear({ threadId: sessionId });
      const objective = payload["objective"];
      goalToRun = yield* goals.set({ threadId: sessionId, objective: typeof objective === "string" ? objective : undefined, status: "active" });
      panel = buildPanelForKnownGoal({ store, conversationId, sessionId, goal: goalToRun, turnState: "starting" });
      yield* client.answerCallbackQuery(callbackQueryId, "Starting.");
    } else if (kind === "clear_confirm") {
      const goal = yield* goals.read({ threadId: sessionId });
      yield* client.answerCallbackQuery(callbackQueryId);
      return yield* editPanel(chatId, messageId, goal
        ? buildClearGoalConfirmationPanel({ store, conversationId, goal })
        : buildNoActiveGoalPanel({ store, conversationId, sessionId }));
    } else if (kind === "clear") {
      yield* goals.clear({ threadId: sessionId });
      yield* stopActiveTurn;
      panel = buildNoActiveGoalPanel({ store, conversationId, sessionId });
      yield* client.answerCallbackQuery(callbackQueryId, "Cleared.");
    } else {
      yield* client.answerCallbackQuery(callbackQueryId, "Unknown action.");
      return;
    }

    yield* editPanel(chatId, messageId, panel);
    if (goalToRun && !(yield* ensureGoalTurn({ conversationId, chatId, messageId, sessionId, goal: goalToRun, goals }))) {
      yield* editPanel(chatId, messageId, buildPanelForKnownGoal({ store, conversationId, sessionId, goal: goalToRun, turnState: "idle" }));
    }
  }).pipe(
    Effect.catch((error) =>
      client.answerCallbackQuery(callbackQueryId, "Goal update failed.").pipe(
        Effect.andThen(client.editMessageText(chatId, messageId, `Goal\n\nFailed to update goal: ${error.message}`, { format: "plain" })),
        Effect.asVoid,
      )),
  );
});
