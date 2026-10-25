/**
 * What the operator sends a conversation: a command, or, once a service and a folder are
 * mounted, a prompt for its mounted session, queued as a prompt job. What it comes to is
 * decided as the update that brought it is handled (admitPrompt), in the update's transaction,
 * so the update is told handled only once what it asks for is kept: its prompt's job queued,
 * the panel of the setup step it lacks sent, or its command to run, which runs as a control
 * of its own (../telegram/controls.ts).
 */
import { Effect } from "effect";

import { type ConversationChat, Turns } from "../codex/turns.ts";
import { harnessLabelOf } from "../harness/index.ts";
import type { Control } from "../persistence/control-repository.ts";
import type { PromptJob } from "../persistence/prompt-job-repository.ts";
import type { StoreError } from "../persistence/sql.ts";
import { Store, type UpdateMarks } from "../persistence/store.ts";
import { buildFilePromptSuffix } from "../shared/file-prompt.ts";
import { TelegramClient, type TelegramError } from "../telegram/client.ts";
import type { ReceivedFile } from "../telegram/files.ts";
import { handleTextCommand, type OperatorServices } from "./command-handler.ts";
import { parseCommand } from "./command-parser.ts";
import { sendChooseServicePanel } from "./service-control.ts";
import { sendChooseWorkspacePanel } from "./workspace-control.ts";

/** A prompt as it arrives from Telegram: its text, and the files sent with it, kept. */
export interface IncomingPrompt extends ConversationChat {
  readonly messageId: number;
  readonly text: string;
  readonly files: readonly ReceivedFile[];
}

/** A command the operator sent, as a control to run (../telegram/controls.ts). */
export type CommandControl = Extract<Control, { readonly kind: "command" }>;

/**
 * What an operator's message comes to, as admitPrompt decides it: a command, to run; a prompt,
 * its job queued; or nothing more to do, as it carried nothing to queue or the setup step it
 * lacks was sent.
 */
export type Admitted =
  | { readonly _tag: "Command"; readonly command: CommandControl }
  | { readonly _tag: "Queued"; readonly job: PromptJob }
  | { readonly _tag: "Done" };

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

/**
 * Decides what the operator sent comes to, in the transaction of the update that brought it,
 * whose marks are `marks`: a command, one with no files attached, to run once decided; a
 * prompt, once a service and a folder are mounted, its job queued with `marks`, for
 * Turns.admitted to carry on once the transaction is through; or, neutral by default, where
 * one is not mounted yet, its picker sent, and nothing queued. The picker is sent to Telegram
 * as the chat's lock is held, as its handling waits for it: one Telegram holds off holds the
 * chat's lock as long, and one sent as the transaction then fails is sent again as the update
 * is handled again, as a reply may be twice, never lost.
 */
export const admitPrompt = Effect.fnUntraced(function*({ conversationId, chatId, messageId, text, files }: IncomingPrompt, marks: Pick<UpdateMarks, "enqueuePromptJob">): Effect.fn.Return<
  Admitted,
  TelegramError | StoreError,
  OperatorServices
> {
  const effectiveText = text || (files.length > 0 ? "Please inspect the attached file(s)." : "");
  if (files.length === 0 && parseCommand(effectiveText) !== null) {
    return { _tag: "Command", command: { kind: "command", conversationId, chatId, messageId, text: effectiveText } };
  }
  const prompt = effectiveText + buildFilePromptSuffix(files.map((file) => file.path));
  if (!prompt.trim() || (yield* sendNextSetupStep({ conversationId, chatId }))) return { _tag: "Done" };
  const queued = { conversationId, chatId, messageId, prompt, fileIds: files.map((file) => file.id), visibleText: effectiveText };
  return { _tag: "Queued", job: yield* Effect.flatMap(Turns, (turns) => turns.admit(queued, marks)) };
});

/** Runs the operator's command `command`, telling its chat where it fails. */
export const runCommand = (command: CommandControl): Effect.Effect<void, never, OperatorServices> => {
  const { conversationId, chatId, messageId, text } = command;
  const failed = Effect.fnUntraced(function*(error: unknown) {
    yield* Effect.logError(`Failed to run the command of ${conversationId}'s message ${messageId}: ${error}`);
    yield* Effect.gen(function*() {
      const agentName = harnessLabelOf(yield* Effect.flatMap(Store, (store) => store.getMount(conversationId)));
      const client = yield* TelegramClient;
      yield* client.sendMessage(chatId, `${agentName} hit an error: ${error instanceof Error ? error.message : String(error)}`);
    }).pipe(Effect.ignore);
  });
  return handleTextCommand({ text, fileIds: [], conversationId, chatId, messageId }).pipe(Effect.asVoid, Effect.catch(failed), Effect.catchDefect(failed));
};
