/**
 * Telegram sends an album as one message per photo or file, sharing a media group id. They
 * are kept in the store as they arrive and handled as one prompt once the group has been
 * quiet for a moment: by the alasio that kept its last message, or, where that one stopped
 * first or its chat's updates went to another, by whichever alasio looks for groups due
 * next (flushDue).
 */
import { Clock, Context, Duration, Effect, FiberMap, FiberSet, Layer } from "effect";

import type { OperatorServices } from "../operator/command-handler.ts";
import { processPrompt } from "../operator/prompts.ts";
import type { StoreError } from "../persistence/sql.ts";
import { Store } from "../persistence/store.ts";
import { withLogScope } from "../shared/log.ts";
import { parseWorkspace } from "../workspace/kind.ts";
import { ReceivedFiles } from "./files.ts";

/** How long a media group waits for more of its messages before it is handled. */
const DEFAULT_FLUSH_AFTER: Duration.Input = "1500 millis";

/** One message of a media group, as it arrives. */
export interface MediaGroupMessage {
  readonly mediaGroupId: string;
  readonly conversationId: string;
  readonly updateId: number;
  readonly chatId: number;
}

export class MediaGroups extends Context.Service<MediaGroups, {
  /** Keeps a message of its group, and handles the group once no more of it arrive for a moment. */
  readonly buffer: (message: MediaGroupMessage) => Effect.Effect<void, StoreError>;
  /**
   * Handles every group of any alasio's that waits and has been quiet for its moment: one
   * whose wait ended as alasio stopped, or as its chat's updates went to another alasio,
   * which saw none of it. One that fails is said, and looked at again by the next.
   */
  readonly flushDue: Effect.Effect<void>;
}>()("alasio/telegram/MediaGroups") {
  static readonly layer = (flushAfter: Duration.Input = DEFAULT_FLUSH_AFTER): Layer.Layer<MediaGroups, never, OperatorServices> =>
    Layer.effect(MediaGroups, makeMediaGroups(flushAfter));
}

const makeMediaGroups = Effect.fnUntraced(function*(flushAfter: Duration.Input) {
  const store = yield* Store;
  const receivedFiles = yield* ReceivedFiles;
  const services = yield* Effect.context<OperatorServices>();
  const flushMs = Duration.toMillis(flushAfter);
  // Each group's wait, which a later message of the group starts again; and the groups
  // being handled, which a later message no longer reaches.
  const waits = yield* FiberMap.make<string>();
  const flushing = yield* FiberSet.make();

  /**
   * The group's messages and files as one prompt, under its chat's handling lock, which its
   * messages are kept under too, so none of it comes meanwhile: claimed in the lock's
   * transaction, where it still waits and its last message has been quiet for its moment,
   * so of all the alasios on the store one handles it, once. Its files are written first;
   * where that fails, the claim is rolled back with the lock's transaction, and the group
   * waits for the next look. Its prompt failing is said, and the group is handled.
   */
  const flush = (mediaGroupId: string, conversationId: string, chatId: number | string) =>
    store.withHandlingLock(chatId, (marks) =>
      Effect.gen(function*() {
        if (!(yield* marks.claimMediaGroup(mediaGroupId, flushMs))) return;
        const messages = yield* store.getMediaGroupMessages(mediaGroupId);
        const files = yield* store.getFilesForMessages(messages.map((message) => message.id));
        const text = messages.map((message) => String(message.text ?? "").trim()).find(Boolean) ?? "";
        const messageId = Number(messages[0]?.transport_message_id ?? 0) || (yield* Clock.currentTimeMillis);
        // Written as they arrived, unless a restart came since, where the agent of the conversation's workspace reads them.
        const workspace = parseWorkspace((yield* store.getMount(conversationId)).workingDirectory);
        yield* receivedFiles.materialize(files.map((file) => file.id), workspace);
        const received = files.map((file) => ({ id: file.id, path: receivedFiles.pathOf(file, workspace) }));
        const promptFailed = (error: unknown) => Effect.logError(`Media group ${mediaGroupId}'s prompt failed: ${error}`);
        yield* processPrompt({ conversationId, chatId, messageId, text, files: received }).pipe(
          Effect.provideContext(services),
          Effect.catch(promptFailed),
          Effect.catchDefect(promptFailed),
        );
      })
    ).pipe(
      Effect.catch((error) => Effect.logError(`Failed to flush media group ${mediaGroupId}: ${error}`)),
      Effect.catchDefect((defect) => Effect.logError(`Failed to flush media group ${mediaGroupId}: ${defect}`)),
      withLogScope("telegram-app"),
    );

  return MediaGroups.of({
    buffer: Effect.fnUntraced(function*({ mediaGroupId, conversationId, updateId, chatId }) {
      yield* store.upsertMediaGroup({ mediaGroupId, conversationId, updateId });
      yield* FiberMap.run(waits, mediaGroupId, Effect.sleep(flushAfter).pipe(Effect.andThen(FiberSet.run(flushing, flush(mediaGroupId, conversationId, chatId)))));
    }),
    flushDue: store.getPendingMediaGroupsDue(flushMs).pipe(
      Effect.flatMap((groups) =>
        Effect.forEach(groups, (group) => flush(group.id, group.conversation_id, group.conversation_id.replace(/^telegram:/, "")), { discard: true })
      ),
      Effect.catch((error) => Effect.logError(`Looking for media groups due failed: ${error.message}`).pipe(withLogScope("telegram-app"))),
    ),
  });
});
