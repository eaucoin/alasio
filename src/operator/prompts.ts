/**
 * What the operator sends a conversation: a command, or, once a service and a folder are
 * mounted, a prompt for its mounted session, queued as a prompt job.
 */
import { Effect } from "effect";

import { type ConversationChat, Turns } from "../codex/turns.ts";
import type { StoreError } from "../persistence/sql.ts";
import { Store } from "../persistence/store.ts";
import { buildFilePromptSuffix } from "../shared/file-prompt.ts";
import type { TelegramError } from "../telegram/client.ts";
import type { ReceivedFile } from "../telegram/files.ts";
import { type CommandError, handleTextCommand, type OperatorServices } from "./command-handler.ts";
import { sendChooseServicePanel } from "./service-control.ts";
import { sendChooseWorkspacePanel } from "./workspace-control.ts";

/** A prompt as it arrives from Telegram: its text, and the files sent with it, kept. */
export interface IncomingPrompt extends ConversationChat {
  readonly messageId: number;
  readonly text: string;
  readonly files: readonly ReceivedFile[];
}

/**
 * Service first, then folder: sends the picker for the first one missing. Whether it
 * sent one, so that what asked can stop there.
 */
export const sendNextSetupStep = Effect.fnUntraced(function*(conversation: ConversationChat): Effect.fn.Return<boolean, TelegramError | StoreError, OperatorServices> {
  const mount = yield* Effect.flatMap(Store, (store) => store.getMount(conversation.conversationId));
  if (!mount.harness) {
    yield* sendChooseServicePanel(conversation);
    return true;
  }
  if (!mount.workingDirectory) {
    yield* sendChooseWorkspacePanel(conversation);
    return true;
  }
  return false;
});

/** Handles what the operator sent: a command, or a prompt to queue once a service and a folder are mounted. */
export const processPrompt = Effect.fnUntraced(function*({ conversationId, chatId, messageId, text, files }: IncomingPrompt): Effect.fn.Return<
  void,
  CommandError,
  OperatorServices
> {
  const fileIds = files.map((file) => file.id);
  const effectiveText = text || (files.length > 0 ? "Please inspect the attached file(s)." : "");
  if (yield* handleTextCommand({ text: effectiveText, fileIds, conversationId, chatId, messageId })) {
    return;
  }
  const prompt = effectiveText + buildFilePromptSuffix(files.map((file) => file.path));
  if (!prompt.trim()) {
    return;
  }
  if (yield* sendNextSetupStep({ conversationId, chatId })) {
    // Neutral by default: nothing is queued until a service and a folder are chosen.
    return;
  }
  yield* Effect.flatMap(Turns, (turns) => turns.submit({ conversationId, chatId, messageId, prompt, fileIds, visibleText: effectiveText }));
});
