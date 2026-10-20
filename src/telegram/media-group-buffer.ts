/**
 * Telegram sends an album as one message per photo or file, sharing a media group id. They
 * are kept in the store as they arrive and handled as one prompt once the group has been
 * quiet for a moment: by the alasio that kept its last message, or, where that one stopped
 * first or its chat's updates went to another, by whichever alasio looks for groups due
 * next (flushDue). A message of an album that comes once the album has been handled is
 * not in its prompt, and its chat is told so.
 */
import { Clock, Context, Duration, Effect, FiberMap, FiberSet, Layer, Semaphore } from "effect";

import type { OperatorServices } from "../operator/command-handler.ts";
import type { IncomingPrompt } from "../operator/prompts.ts";
import type { StoreError } from "../persistence/sql.ts";
import { Store, type UpdateMarks } from "../persistence/store.ts";
import { withLogScope } from "../shared/log.ts";
import { parseWorkspace } from "../workspace/kind.ts";
import { type ReceivedFileError, ReceivedFiles } from "./files.ts";
import { processIncomingPrompt } from "./message-handler.ts";

/** How long a media group waits for more of its messages before it is handled. */
const DEFAULT_FLUSH_AFTER: Duration.Input = "1500 millis";
/** How many of an alasio's albums are handled under their chats' handling locks at once, at most: a bound on the pool's connections they take, as delivery's passes are bounded. */
const LOCK_HOLDERS = 2;
/** How many times an album is tried, its files failing to be written, before it is given up on and its chat told: a minute or so of looks for albums due. */
export const MEDIA_GROUP_TRIES = 6;

/** What a chat is told of a message of an album that came once the album had been handled. */
export const LATE_ALBUM_PART = "A photo or file of an album came after the rest of the album had been sent on as one prompt, so it was left out of it: send it again, with what it belongs to.";

/** One message of a media group, as it arrives. */
export interface MediaGroupMessage {
  readonly mediaGroupId: string;
  readonly conversationId: string;
  readonly updateId: number;
  readonly chatId: number;
}

export class MediaGroups extends Context.Service<MediaGroups, {
  /**
   * Keeps a message of its group, and handles the group once no more of it arrive for a
   * moment; tells the chat of one that came once its group had been handled, with `marks`,
   * those of the message's update, so the chat is told once.
   */
  readonly buffer: (message: MediaGroupMessage, marks: Pick<UpdateMarks, "enqueueOutboxText">) => Effect.Effect<void, StoreError>;
  /**
   * Handles every group of any alasio's that waits and has been quiet for its moment: one
   * whose wait ended as alasio stopped, or as its chat's updates went to another alasio,
   * which saw none of it. One that fails is said, and looked at again by the next: one whose
   * files could not be written is given up on once that has failed MEDIA_GROUP_TRIES times;
   * one the store failed, its lock not had in time or a statement of it failing, waits for
   * the store to answer, however long.
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
  // Each group's wait, which a later message of the group starts again; the groups being
  // handled, which a later message no longer reaches; and their prompts, which run on after.
  const waits = yield* FiberMap.make<string>();
  const flushing = yield* FiberSet.make();
  const prompts = yield* FiberSet.make();
  const holders = yield* Semaphore.make(LOCK_HOLDERS);

  /**
   * The group's messages and files made one prompt, under its chat's handling lock, which its
   * messages are kept under too, so none of it comes meanwhile: claimed in the lock's
   * transaction, where it still waits and its last message has been quiet for its moment,
   * so of all the alasios on the store one handles it, once. Its files are written under
   * the lock; where that fails, as of a workspace not answering for a while, the claim is
   * rolled back with the lock's transaction, and the group waits for the next look. Null
   * where it was not claimed.
   */
  const claimed = (mediaGroupId: string, conversationId: string, chatId: number | string): Effect.Effect<IncomingPrompt | null, StoreError | ReceivedFileError> =>
    holders.withPermit(store.withHandlingLock(chatId, (marks) =>
      Effect.gen(function*() {
        if (!(yield* marks.claimMediaGroup(mediaGroupId, flushMs))) return null;
        const messages = yield* store.getMediaGroupMessages(mediaGroupId);
        const files = yield* store.getFilesForMessages(messages.map((message) => message.id));
        const text = messages.map((message) => String(message.text ?? "").trim()).find(Boolean) ?? "";
        const messageId = Number(messages[0]?.transport_message_id ?? 0) || (yield* Clock.currentTimeMillis);
        // Written as they arrived, unless a restart came since, where the agent of the conversation's workspace reads them.
        const workspace = parseWorkspace((yield* store.getMount(conversationId)).workingDirectory);
        yield* receivedFiles.materialize(files.map((file) => file.id), workspace);
        const received = files.map((file) => ({ id: file.id, path: receivedFiles.pathOf(file, workspace) }));
        return { conversationId, chatId, messageId, text, files: received };
      })
    ));

  /**
   * The group handled after its handling failed `failures` times, for `why`: given up on
   * once it has failed MEDIA_GROUP_TRIES times, claimed and its chat told in one transaction
   * of its handling lock, so that it is told once, by whichever alasio gives up on it.
   */
  const givenUp = (mediaGroupId: string, chatId: number | string, failures: number, why: string) =>
    failures < MEDIA_GROUP_TRIES
      ? Effect.void
      : holders.withPermit(store.withHandlingLock(chatId, (marks) =>
        Effect.gen(function*() {
          if (!(yield* marks.claimMediaGroup(mediaGroupId, flushMs))) return;
          yield* marks.enqueueOutboxText({
            chatId,
            text: `An album's files could not be written for its prompt, ${failures} times, so its prompt was not run: ${why}. Send it again.`,
          });
          yield* Effect.logWarning(`Gave up on media group ${mediaGroupId} after ${failures} failures`);
        })
      ));

  /**
   * The group handled, as `claimed` says, its prompt then run on after its chat's handling
   * lock is let go of, as a single message's prompt is (./app.ts), so that a prompt that
   * takes a while holds back none of its chat's updates; one that fails is said in the
   * chat, and the group is handled. As a single message's, an album whose claim is committed
   * and whose prompt is not yet queued as alasio stops is lost: its prompt, forked once the
   * lock's transaction commits, queues its job within moments of it.
   */
  const flush = (mediaGroupId: string, conversationId: string, chatId: number | string): Effect.Effect<void> =>
    claimed(mediaGroupId, conversationId, chatId).pipe(
      Effect.flatMap((prompt) =>
        prompt === null ? Effect.void : Effect.asVoid(FiberSet.run(prompts, processIncomingPrompt(prompt).pipe(Effect.provideContext(services), Effect.interruptible)))
      ),
      // Its files not written: counted, and given up on at MEDIA_GROUP_TRIES.
      Effect.catchTag("ReceivedFileError", (error) =>
        Effect.logError(`Failed to write the files of media group ${mediaGroupId}: ${error.message}`).pipe(
          Effect.andThen(store.recordMediaGroupFailure(mediaGroupId)),
          Effect.flatMap((failures) => givenUp(mediaGroupId, chatId, failures, error.message)),
          Effect.catch((counting) => Effect.logError(`Failed to count the failure of media group ${mediaGroupId}: ${counting.message}`)),
        )),
      // The store not answering, or its lock not had in time: nothing of the album's own, which waits for the next look.
      Effect.catch((error) => Effect.logWarning(`Could not flush media group ${mediaGroupId} now, which waits for the next look: ${error.message}`)),
      Effect.catchDefect((defect) => Effect.logError(`Failed to flush media group ${mediaGroupId}: ${defect}`)),
      withLogScope("telegram-app"),
    );

  return MediaGroups.of({
    buffer: Effect.fnUntraced(function*({ mediaGroupId, conversationId, updateId, chatId }, marks) {
      if (!(yield* store.upsertMediaGroup({ mediaGroupId, conversationId, updateId }))) {
        yield* marks.enqueueOutboxText({ chatId, text: LATE_ALBUM_PART });
        return;
      }
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
