import type { v2 } from "../../.types/codex/index.js";
import { Effect } from "effect";

import { Turns } from "../codex/turns.ts";
import { HarnessHost } from "../harness/host.ts";
import { shownBusy, stopTurn } from "../harness/commands.ts";
import type { HarnessError, HarnessGoals } from "../harness/index.ts";
import type { CallbackPayload } from "../persistence/callback-repository.ts";
import type { StoreError } from "../persistence/sql.ts";
import { Store } from "../persistence/store.ts";
import { type ChatId, TelegramClient, type TelegramError } from "../telegram/client.ts";
import { madeOnce } from "./made-once.ts";
import { type ButtonDraft, type ControlCallback, closePanel, editPanel, keepPanel, type PanelDraft, sendPanel } from "./panel.ts";
import type { NewSessionError } from "./session-control.ts";
import { truncateText } from "./text.ts";

/** What a press of a goal's panel asks for its goal's turn as (ensureGoalTurn): its query, the same however often the press is run, another for another press. */
export const goalPressKey = (callbackQueryId: string): string => `goal:${callbackQueryId}`;

/** A turn to run toward a goal, on its session: attached to the goal's own turn as it starts where Codex runs one, else started with `prompt` (Turns.runGoalTurn). */
export interface GoalTurnRequest {
  readonly conversationId: string;
  readonly chatId: ChatId;
  /** What asked for the goal's turn, the message its prompt job is of: a command's, or a press's (ensureGoalTurn). */
  readonly messageId: number | string;
  readonly sessionId: string;
  readonly prompt: string;
}

/** How changing a goal fails, as its panel says. */
type GoalError = HarnessError | NewSessionError | TelegramError | StoreError;

/** What the goal controls run on: the store, Telegram, and the turns, running and to run. */
export type GoalServices = Store | TelegramClient | HarnessHost | Turns;

/** How a goal's turn is going, as the goal panel shows it. */
export type GoalTurnState = "working" | "starting" | "queued" | "idle";

const GOAL_KIND_PREFIX = "goal:";
const UNFINISHED_STATUSES = new Set<v2.ThreadGoalStatus>(["active", "paused", "blocked", "usageLimited", "budgetLimited"]);

function goalKind(kind: string): string {
  return `${GOAL_KIND_PREFIX}${kind}`;
}

export function isGoalControlAction(kind: unknown): boolean {
  return typeof kind === "string" && kind.startsWith(GOAL_KIND_PREFIX);
}

function button(text: string, kind: string, payload: CallbackPayload = {}): ButtonDraft {
  return { text, kind: goalKind(kind), payload };
}

function sessionControlButton(text: string, kind: string, payload: CallbackPayload = {}): ButtonDraft {
  return { text, kind: `control:${kind}`, payload };
}

function closeRow(): ButtonDraft[] {
  return [button("Close", "close")];
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

export function buildNoMountedGoalPanel(): PanelDraft {
  return {
    text: [
      "Goal",
      "",
      "No Codex session is mounted.",
      "Start a new session or open Sessions to mount an existing one.",
    ].join("\n"),
    keyboard: [
      [sessionControlButton("Sessions", "sessions", { page: 1 })],
      [sessionControlButton("New Session", "new")],
      closeRow(),
    ],
  };
}

export function buildNoActiveGoalPanel({ sessionId }: { readonly sessionId: string }): PanelDraft {
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
    keyboard: [closeRow()],
  };
}

export interface GoalPanelRequest {
  readonly goal: v2.ThreadGoal;
  readonly turnState?: GoalTurnState | null | undefined;
}

export function buildGoalPanel({ goal, turnState = null }: GoalPanelRequest): PanelDraft {
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

  return {
    text: lines.join("\n"),
    keyboard: [
      goal.status === "active"
        ? [button("Pause", "pause"), button("Clear", "clear_confirm")]
        : [button("Resume", "resume"), button("Clear", "clear_confirm")],
      closeRow(),
    ],
  };
}

export function buildReplaceGoalPanel({ currentGoal, objective }: {
  readonly currentGoal: v2.ThreadGoal;
  readonly objective: string;
}): PanelDraft {
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
    keyboard: [
      [button("Replace Goal", "replace", { objective })],
      [button("Keep Current Goal", "show")],
      closeRow(),
    ],
  };
}

function buildClearGoalConfirmationPanel({ goal }: { readonly goal: v2.ThreadGoal }): PanelDraft {
  return {
    text: [
      "Clear goal?",
      "",
      truncateText(goal.objective, 700),
    ].join("\n"),
    keyboard: [
      [button("Clear Goal", "clear")],
      [button("Keep Goal", "show")],
      closeRow(),
    ],
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
 * Asks for a goal's turn when the goal is active (Turns.runGoalTurn), queued as a prompt job of
 * `messageId`, which names what asked for it: a command's message, which the command run again
 * after its alasio stopped (../telegram/controls.ts) asks for again and finds it queued; or a
 * press of a goal's panel, by its query, as a press run again is the same press, and another
 * press of the panel another. Whether it was asked for, queued to run or held while a turn
 * runs; none where the goal is not active, so that its idle panel is shown.
 */
const ensureGoalTurn = Effect.fnUntraced(function*({ conversationId, chatId, messageId, sessionId, goal }: {
  readonly conversationId: string;
  readonly chatId: ChatId;
  readonly messageId: number | string;
  readonly sessionId: string;
  readonly goal: v2.ThreadGoal | null;
}): Effect.fn.Return<"queued" | "held" | null, GoalError, Turns> {
  if (goal?.status !== "active") {
    return null;
  }
  return yield* Effect.flatMap(Turns, (turns) => turns.runGoalTurn({ conversationId, chatId, messageId, sessionId, prompt: buildGoalTurnPrompt(goal) }));
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
}): Effect.fn.Return<PanelDraft, HarnessError | StoreError, Store> {
  const { sessionId } = yield* Effect.flatMap(Store, (store) => store.getMount(conversationId));
  if (!sessionId) {
    return buildNoMountedGoalPanel();
  }
  return buildPanelForKnownGoal({ sessionId, goal: yield* goals.read({ threadId: sessionId }), turnState });
});

/** Says a goal could not be changed, and why. */
const sendErrorPanel = (chatId: ChatId, error: GoalError): Effect.Effect<void, TelegramError, TelegramClient> =>
  Effect.flatMap(TelegramClient, (client) => client.sendMessage(chatId, [
    "Goal",
    "",
    `Failed to update goal: ${error.message}`,
  ].join("\n"), { format: "plain" })).pipe(Effect.asVoid);

interface KnownGoal {
  readonly sessionId: string;
  readonly goal: v2.ThreadGoal | null;
  readonly turnState?: GoalTurnState | null;
}

function buildPanelForKnownGoal({ sessionId, goal, turnState = null }: KnownGoal): PanelDraft {
  if (!isUnfinishedGoal(goal)) {
    return buildNoActiveGoalPanel({ sessionId });
  }
  return buildGoalPanel({ goal, turnState });
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
    Effect.flatMap((draft) => keepPanel(conversationId, draft)),
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
  const isTurnActive = yield* shownBusy(conversationId);
  // The goal's change is made whether or not a host elsewhere stops its turn, which is said.
  const stopActiveTurn = stopTurn(conversationId, "interrupt").pipe(Effect.catch((error) => Effect.logWarning(`could not stop ${conversationId}'s turn: ${error.message}`)));
  const trimmed = String(args ?? "").trim();
  if (!trimmed) {
    return yield* sendGoalPanel({ conversationId, chatId, isTurnActive, goals });
  }

  const send = (draft: PanelDraft) => Effect.flatMap(keepPanel(conversationId, draft), (panel) => sendPanel(chatId, panel));
  const sendKnownGoalPanel = (sessionId: string, goal: v2.ThreadGoal | null) => send(buildPanelForKnownGoal({ sessionId, goal, turnState: "idle" }));

  yield* Effect.gen(function*() {
    const control = trimmed.toLowerCase();
    let { sessionId } = yield* store.getMount(conversationId);
    if (!sessionId) {
      if (["clear", "pause", "resume", "edit"].includes(control)) {
        return yield* send(buildNoMountedGoalPanel());
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
      if (yield* ensureGoalTurn({ conversationId, chatId, messageId, sessionId, goal })) {
        return;
      }
      return yield* sendKnownGoalPanel(sessionId, goal);
    }
    if (control === "edit") {
      yield* client.sendMessage(chatId, "Send /goal <objective> to replace the current goal.", { format: "plain" });
      return;
    }

    const currentGoal = yield* goals.read({ threadId: sessionId });
    // Set once, or asked before replacing one unfinished: a run again finds the goal its run
    // before set, not one to ask before replacing.
    const objective = yield* madeOnce({ conversationId, messageId }, "goal", Effect.gen(function*() {
      if (shouldConfirmBeforeReplacing(currentGoal)) return { replacing: currentGoal } as const;
      return { goal: yield* setFreshObjectiveGoal({ sessionId, objective: trimmed, currentGoal, goals }) } as const;
    }));
    if ("replacing" in objective) {
      return yield* send(buildReplaceGoalPanel({ currentGoal: objective.replacing, objective: trimmed }));
    }
    const { goal } = objective;
    if (yield* ensureGoalTurn({ conversationId, chatId, messageId, sessionId, goal })) {
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
}: GoalControlCallback): Effect.fn.Return<void, TelegramError | StoreError, GoalServices> {
  const store = yield* Store;
  const client = yield* TelegramClient;
  const { conversationId } = action;
  const isTurnActive = yield* shownBusy(conversationId);
  // The goal's change is made whether or not a host elsewhere stops its turn, which is said.
  const stopActiveTurn = stopTurn(conversationId, "interrupt").pipe(Effect.catch((error) => Effect.logWarning(`could not stop ${conversationId}'s turn: ${error.message}`)));
  const kind = action.kind.slice(GOAL_KIND_PREFIX.length);
  const payload = action.payload ?? {};
  const edit = (draft: PanelDraft) => Effect.flatMap(keepPanel(conversationId, draft), (panel) => editPanel(chatId, messageId, panel));

  if (kind === "close") {
    return yield* closePanel({ callbackQueryId, chatId, messageId });
  }

  if (!goals) {
    yield* client.answerCallbackQuery(callbackQueryId, "Goals are not available for this service.");
    return;
  }

  const { sessionId } = yield* store.getMount(conversationId);
  if (!sessionId) {
    yield* client.answerCallbackQuery(callbackQueryId, "No mounted session.");
    return yield* edit(buildNoMountedGoalPanel());
  }

  yield* Effect.gen(function*() {
    let goalToRun: v2.ThreadGoal | null = null;
    let panel: PanelDraft | null = null;
    if (kind === "show") {
      yield* client.answerCallbackQuery(callbackQueryId);
      return yield* edit(yield* buildGoalPanelFromState({ conversationId, turnState: isTurnActive ? "working" : "idle", goals }));
    }
    if (kind === "pause") {
      const goal = yield* goals.set({ threadId: sessionId, status: "paused" });
      yield* stopActiveTurn;
      panel = buildPanelForKnownGoal({ sessionId, goal, turnState: "idle" });
      yield* client.answerCallbackQuery(callbackQueryId, "Paused.");
    } else if (kind === "resume") {
      goalToRun = yield* goals.set({ threadId: sessionId, status: "active" });
      panel = buildPanelForKnownGoal({ sessionId, goal: goalToRun, turnState: "starting" });
      yield* client.answerCallbackQuery(callbackQueryId, "Starting.");
    } else if (kind === "replace") {
      yield* goals.clear({ threadId: sessionId });
      const objective = payload["objective"];
      goalToRun = yield* goals.set({ threadId: sessionId, objective: typeof objective === "string" ? objective : undefined, status: "active" });
      panel = buildPanelForKnownGoal({ sessionId, goal: goalToRun, turnState: "starting" });
      yield* client.answerCallbackQuery(callbackQueryId, "Starting.");
    } else if (kind === "clear_confirm") {
      const goal = yield* goals.read({ threadId: sessionId });
      yield* client.answerCallbackQuery(callbackQueryId);
      return yield* edit(goal ? buildClearGoalConfirmationPanel({ goal }) : buildNoActiveGoalPanel({ sessionId }));
    } else if (kind === "clear") {
      yield* goals.clear({ threadId: sessionId });
      yield* stopActiveTurn;
      panel = buildNoActiveGoalPanel({ sessionId });
      yield* client.answerCallbackQuery(callbackQueryId, "Cleared.");
    } else {
      yield* client.answerCallbackQuery(callbackQueryId, "Unknown action.");
      return;
    }

    yield* edit(panel);
    if (goalToRun) {
      // Held while a turn runs, queued; not active, idle.
      const asked = yield* ensureGoalTurn({ conversationId, chatId, messageId: goalPressKey(callbackQueryId), sessionId, goal: goalToRun });
      if (asked !== "queued") yield* edit(buildPanelForKnownGoal({ sessionId, goal: goalToRun, turnState: asked === "held" ? "queued" : "idle" }));
    }
  }).pipe(
    Effect.catch((error) =>
      client.answerCallbackQuery(callbackQueryId, "Goal update failed.").pipe(
        Effect.andThen(client.editMessageText(chatId, messageId, `Goal\n\nFailed to update goal: ${error.message}`, { format: "plain" })),
        Effect.asVoid,
      )),
  );
});
