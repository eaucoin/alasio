/**
 * The operator's commands: /stop, /model, /service, /workspace, /session(s), /goal, and
 * the ! commands (sessions, resume, rewind), each on the conversation's mounted service.
 */
import { Effect, Option } from "effect";

import { type TurnError, Turns } from "../codex/turn-controller.ts";
import { ActiveTurns } from "../harness/active-turns.ts";
import { type HarnessError, Harnesses, type HarnessUnavailable, resolveHarnessName } from "../harness/index.ts";
import { Store } from "../persistence/store.ts";
import { type ChatId, TelegramClient, type TelegramError } from "../telegram/client.ts";
import { type OperatorCommand, parseCommand } from "./command-parser.ts";
import { handleGoalTextCommand } from "./goal-control.ts";
import { sendModelPanel } from "./model-control.ts";
import type { Mounts } from "./mounts.ts";
import { handleServiceTextCommand, sendChooseServicePanel } from "./service-control.ts";
import { type NewSessionError, sendCurrentSessionPanel, sendSessionsPanel } from "./session-control.ts";
import { formatRewindForTelegram, formatSessionsForTelegram } from "./session-replies.ts";
import { truncateText } from "./text.ts";
import { handleWorkspaceTextCommand, sendChooseWorkspacePanel } from "./workspace-control.ts";

/** What the operator's commands and controls run on. */
export type OperatorServices = Store | TelegramClient | ActiveTurns | Harnesses | Turns | Mounts;

/** How a command fails: the operator is told why. */
export type CommandError = TelegramError | HarnessError | HarnessUnavailable | NewSessionError | TurnError;

/** A prompt that may be a command; one with files attached never is. */
export interface CommandText {
  readonly text: string;
  readonly filePaths: readonly string[];
  readonly conversationId: string;
  readonly chatId: ChatId;
  readonly messageId: number;
}

/** A parsed command, and the conversation and message it came in. */
export interface CommandRequest {
  readonly cmd: OperatorCommand;
  readonly conversationId: string;
  readonly chatId: ChatId;
  readonly messageId: number;
}

function shortSessionId(sessionId: string): string {
  return sessionId ? sessionId.slice(0, 8) : "-";
}

/** Handles a parsed command: whether it was one alasio knows. */
export const handleCommand = Effect.fnUntraced(function*({ cmd, conversationId, chatId, messageId }: CommandRequest): Effect.fn.Return<
  boolean,
  CommandError,
  OperatorServices
> {
  const store = yield* Store;
  const client = yield* TelegramClient;
  const activeTurns = yield* ActiveTurns;
  const turns = yield* Turns;
  const conversation = { conversationId, chatId };
  const harnessName = resolveHarnessName(store, conversationId);
  const harness = Option.getOrNull(yield* Effect.flatMap(Harnesses, (harnesses) => harnesses.forConversation(conversationId)));
  const label = harness?.displayName ?? "The agent";
  if (cmd.type === "stop") {
    if (!(yield* activeTurns.isBusy(conversationId))) {
      yield* client.sendMessage(chatId, "No active query to stop.");
      return true;
    }
    const [status] = yield* client.sendMessage(chatId, `Stopping ${label}...`);
    const interrupted = yield* activeTurns.stop(conversationId, "interrupt");
    const text = interrupted ? `${label} stopped.` : "No active query to stop.";
    if (status?.message_id) {
      yield* client.editMessageText(chatId, status.message_id, text, { format: "plain" }).pipe(Effect.ignore);
    } else {
      yield* client.sendMessage(chatId, text);
    }
    return true;
  }
  if (cmd.type === "model") {
    yield* sendModelPanel({ harness, ...conversation });
    return true;
  }
  if (cmd.type === "service") {
    yield* handleServiceTextCommand({ ...conversation, target: cmd.target });
    return true;
  }
  if (cmd.type === "workspace") {
    yield* handleWorkspaceTextCommand({ ...conversation, args: cmd.args });
    return true;
  }
  if (!harnessName) {
    // Every remaining control acts on the mounted service's own sessions or turns.
    yield* sendChooseServicePanel(conversation);
    return true;
  }
  if (!harness) {
    yield* sendChooseWorkspacePanel(conversation);
    return true;
  }
  const sessions = harness.sessions;
  if (cmd.type === "sessions") {
    const sessionList = yield* sessions.listSessions(cmd.page);
    const totalPages = yield* sessions.getTotalSessionPages();
    yield* client.sendMessage(chatId, formatSessionsForTelegram(sessionList, cmd.page, totalPages));
    return true;
  }
  if (cmd.type === "sessions_panel") {
    yield* sendSessionsPanel({ harness, ...conversation });
    return true;
  }
  if (cmd.type === "session_panel") {
    yield* sendCurrentSessionPanel({ harness, ...conversation });
    return true;
  }
  if (cmd.type === "goal") {
    // A harness that supports goals has them; the check on goals only narrows.
    if (!harness.supportsGoals || !harness.goals) {
      yield* client.sendMessage(chatId, `Goals are a Codex feature. ${harness.displayName} is active; use /service codex to switch back.`);
      return true;
    }
    yield* handleGoalTextCommand({ ...conversation, messageId, args: cmd.args, goals: harness.goals });
    return true;
  }
  if (cmd.type === "sessions_new") {
    if (yield* activeTurns.isBusy(conversationId)) {
      yield* client.sendMessage(chatId, `${harness.displayName} is currently working. Use /stop first, then /sessions new.`);
      return true;
    }
    const sessionId = yield* turns.startNewSession(conversationId);
    yield* client.sendMessage(chatId, `New ${harness.displayName} session mounted: ${shortSessionId(sessionId)}. Send your next message to start a turn.`);
    return true;
  }
  if (cmd.type === "rewind_list") {
    const sessionId = store.getSessionId(conversationId);
    if (!sessionId) {
      yield* client.sendMessage(chatId, "No session linked to this Telegram conversation. Use !resume <#> first.");
      return true;
    }
    const messages = yield* sessions.listSessionMessages(sessionId);
    const totalPages = yield* sessions.getTotalRewindPages(sessionId);
    yield* client.sendMessage(chatId, formatRewindForTelegram(messages, cmd.page, totalPages));
    return true;
  }
  if (cmd.type === "rewind_exec") {
    const sessionId = store.getSessionId(conversationId);
    if (!sessionId) {
      yield* client.sendMessage(chatId, "No session linked. Use !resume <#> first.");
      return true;
    }
    const messages = yield* sessions.listSessionMessages(sessionId);
    const target = messages.find((message) => message.index === cmd.index);
    if (!target) {
      yield* client.sendMessage(chatId, `Message ${cmd.index} not found. Use !rewind to see available points.`);
      return true;
    }
    const forkedId = yield* sessions.createForkedSession(sessionId, target.uuid, { threadKey: conversationId });
    if (!forkedId) {
      yield* client.sendMessage(chatId, "Failed to create forked session.");
      return true;
    }
    store.setSessionId(conversationId, forkedId);
    yield* client.sendMessage(chatId, `Rewound to before message ${cmd.index}:\n\n${truncateText(target.text, 700)}\n\nReady to continue from earlier state.`);
    return true;
  }
  if (cmd.type === "resume") {
    let resolvedSessionId: string;
    if (/^\d+$/.test(cmd.ref)) {
      const numbered = yield* sessions.getSessionByNumber(Number.parseInt(cmd.ref, 10));
      if (!numbered) {
        yield* client.sendMessage(chatId, `Session #${cmd.ref} not found. Use !sessions to see available sessions.`);
        return true;
      }
      resolvedSessionId = numbered;
    } else {
      resolvedSessionId = cmd.ref;
    }
    store.setSessionId(conversationId, resolvedSessionId);
    if (cmd.followUp) {
      yield* turns.run({ conversationId, chatId, messageId, prompt: cmd.followUp });
      return true;
    }
    const lastMessage = yield* sessions.getSessionLastMessage(resolvedSessionId);
    yield* client.sendMessage(chatId, lastMessage ? `Resuming; most recent ${harness.displayName} message:\n\n${lastMessage}` : "Resuming session.");
    return true;
  }
  return false;
});

/** Handles `text` if it is a command: whether it was. */
export const handleTextCommand = ({ text, filePaths, ...request }: CommandText): Effect.Effect<boolean, CommandError, OperatorServices> => {
  const cmd = parseCommand(text);
  return !cmd || filePaths.length > 0 ? Effect.succeed(false) : handleCommand({ cmd, ...request });
};
