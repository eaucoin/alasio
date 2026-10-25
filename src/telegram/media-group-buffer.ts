/**
 * Telegram sends an album as one message per photo or file, sharing a media group id. They
 * are kept in the store as they arrive and handled as one prompt once the group has been
 * quiet for a moment: by the alasio that kept its last message, or, where that one stopped
 * first or its chat's updates went to another, by whichever alasio looks for groups due
 * next (flushDue). A message of an album that comes once the album has been handled is
 * not in its prompt, and its chat is told so.
 */
import { Clock, Context, Duration, Effect, FiberMap, FiberSet, Layer, Option, Semaphore } from "effect";

import { Turns } from "../codex/turns.ts";
import type { OperatorServices } from "../operator/command-handler.ts";
import { admitPrompt, runCommand } from "../operator/prompts.ts";
import type { StoreError } from "../persistence/sql.ts";
import { Store, type UpdateMarks } from "../persistence/store.ts";
import { withLogScope } from "../shared/log.ts";
import { parseWorkspace } from "../workspace/kind.ts";
import { ReceivedFiles } from "./files.ts";
import { type Awaited, awaitOrKeep, holdWhileRunning, rolledBack } from "./controls.ts";

/** How long a media group waits for more of its messages before it is handled. */
const DEFAULT_FLUSH_AFTER: Duration.Input = "1500 millis";
/** How many of an alasio's albums are handled under their chats' handling locks at once, at most: a bound on the pool's connections they take, as delivery's passes are bounded. */
const LOCK_HOLDERS = 2;

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
   * which saw none of it. One the store failed, its lock not had in time or a statement of it
   * failing, is said, and waits for the store to answer, however long.
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
  // handled, which a later message no longer reaches; and what their prompts lead to, which runs on after:
  // made first, so stopped last, once no group is being handled, as a command one awaits stopped under it
  // is kept to run again, not taken as done.
  const prompts = yield* FiberSet.make();
  const waits = yield* FiberMap.make<string>();
  const flushing = yield* FiberSet.make();
  const holders = yield* Semaphore.make(LOCK_HOLDERS);

  /**
   * The group's messages and files made one prompt, under its chat's handling lock, which its
   * messages are kept under too, so none of it comes meanwhile: claimed in the lock's
   * transaction, where it still waits and its last message has been quiet for its moment,
   * so of all the alasios on the store one handles it, once; and what the prompt comes to
   * kept in the same transaction (admitPrompt), its job queued, so an album claimed is never
   * one whose prompt is lost, as its alasio stops. Its files are named where the agent of the
   * conversation's workspace reads them, which its turn's host writes them to as the turn
   * starts (../harness/host.ts). What it leads to once the transaction is through: its job
   * carried on, or its command, one of an album that kept none of its files, held while it
   * runs on (../telegram/controls.ts); nothing where it was not claimed.
   */
  const claimed = (mediaGroupId: string, conversationId: string, chatId: number | string): Effect.Effect<Effect.Effect<void>, StoreError> => {
    // Its command's work, cut short where the transaction fails after all, keeping nothing of it (rolledBack).
    const awaited: Awaited[] = [];
    return holders.withPermit(store.withHandlingLock(chatId, (marks) =>
      Effect.gen(function*() {
        if (!(yield* marks.claimMediaGroup(mediaGroupId, flushMs))) return Effect.void;
        const messages = yield* store.getMediaGroupMessages(mediaGroupId);
        const files = yield* store.getFilesForMessages(messages.map((message) => message.id));
        const text = messages.map((message) => String(message.text ?? "").trim()).find(Boolean) ?? "";
        const messageId = Number(messages[0]?.transport_message_id ?? 0) || (yield* Clock.currentTimeMillis);
        const workspace = parseWorkspace((yield* store.getMount(conversationId)).workingDirectory);
        const received = files.map((file) => ({ id: file.id, path: receivedFiles.pathOf(file, workspace) }));
        const admitted = yield* admitPrompt({ conversationId, chatId, messageId, text, files: received }, marks).pipe(
          Effect.provideContext(services),
          // Telegram not taking the picker of the setup step it lacks: said, and the album handled, as an update would be skipped.
          Effect.catchTag(["TelegramApiError", "TelegramTransportError"], (error) =>
            Effect.as(Effect.logError(`Could not send the setup step media group ${mediaGroupId} lacks: ${error.message}`), { _tag: "Done" } as const)),
        );
        if (admitted._tag === "Command") {
          const { kept } = awaited[awaited.push(yield* awaitOrKeep(prompts, admitted.command, runCommand(admitted.command).pipe(Effect.provideContext(services)), marks)) - 1]!;
          return Option.match(kept, {
            onNone: () => Effect.void,
            onSome: ({ id, holder, running }) => Effect.asVoid(FiberSet.run(prompts, holdWhileRunning(id, holder, running).pipe(Effect.provideService(Store, store)))),
          });
        }
        if (admitted._tag === "Done") return Effect.void;
        const { job } = admitted;
        return Effect.asVoid(FiberSet.run(prompts, Effect.flatMap(Turns, (turns) => turns.admitted(job)).pipe(
          Effect.catch((error) => Effect.logError(`Could not carry on media group ${mediaGroupId}'s prompt ${job.id}, which waits for what takes up waiting prompts: ${error.message}`)),
          Effect.provideContext(services),
        )));
      })
    )).pipe(Effect.onError(() => rolledBack(awaited)));
  };

  /**
   * The group handled, as `claimed` says, what it leads to run on after its chat's handling
   * lock is let go of, as a single message's is (./app.ts), so that a prompt that takes a
   * while holds back none of its chat's updates.
   */
  const flush = (mediaGroupId: string, conversationId: string, chatId: number | string): Effect.Effect<void> =>
    claimed(mediaGroupId, conversationId, chatId).pipe(
      Effect.flatten,
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
