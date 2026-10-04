/**
 * What the operator sends a conversation: a command, or, once a service and a folder are
 * mounted, a prompt for its mounted session, queued as a prompt job.
 */
import { Effect } from "effect";

import { type ConversationChat, Turns } from "../codex/turns.ts";
import { resolveHarnessName, resolveWorkingDirectory } from "../harness/index.ts";
import { Store } from "../persistence/store.ts";
import { buildFilePromptSuffix } from "../shared/file-prompt.ts";
import type { TelegramError } from "../telegram/client.ts";
import { type CommandError, handleTextCommand, type OperatorServices } from "./command-handler.ts";
import { sendChooseServicePanel } from "./service-control.ts";
import { sendChooseWorkspacePanel } from "./workspace-control.ts";

/** A prompt as it arrives from Telegram: its text, and the files sent with it. */
export interface IncomingPrompt extends ConversationChat {
  readonly messageId: number;
  readonly text: string;
  readonly filePaths: readonly string[];
}

/**
 * Service first, then folder: sends the picker for the first one missing. Whether it
 * sent one, so that what asked can stop there.
 */
export const sendNextSetupStep = Effect.fnUntraced(function*(conversation: ConversationChat): Effect.fn.Return<boolean, TelegramError, OperatorServices> {
  const store = yield* Store;
  if (!resolveHarnessName(store, conversation.conversationId)) {
    yield* sendChooseServicePanel(conversation);
    return true;
  }
  if (!resolveWorkingDirectory(store, conversation.conversationId)) {
    yield* sendChooseWorkspacePanel(conversation);
    return true;
  }
  return false;
});

/** Handles what the operator sent: a command, or a prompt to queue once a service and a folder are mounted. */
export const processPrompt = Effect.fnUntraced(function*({ conversationId, chatId, messageId, text, filePaths }: IncomingPrompt): Effect.fn.Return<
  void,
  CommandError,
  OperatorServices
> {
  const effectiveText = text || (filePaths.length > 0 ? "Please inspect the attached file(s)." : "");
  if (yield* handleTextCommand({ text: effectiveText, filePaths, conversationId, chatId, messageId })) {
    return;
  }
  const prompt = effectiveText + buildFilePromptSuffix(filePaths);
  if (!prompt.trim()) {
    return;
  }
  if (yield* sendNextSetupStep({ conversationId, chatId })) {
    // Neutral by default: nothing is queued until a service and a folder are chosen.
    return;
  }
  yield* Effect.flatMap(Turns, (turns) => turns.submit({ conversationId, chatId, messageId, prompt, filePaths, visibleText: effectiveText }));
});
