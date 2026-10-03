/**
 * Telegram sends an album as one message per photo or file, sharing a media group id. They
 * are kept in the store as they arrive and handled as one prompt once the group has been
 * quiet for a moment, or, when alasio stopped first, as it starts again.
 */
import { Clock, Context, Duration, Effect, FiberMap, FiberSet, Layer } from "effect";

import type { CommandError, OperatorServices } from "../operator/command-handler.ts";
import { processPrompt } from "../operator/prompts.ts";
import { Store } from "../persistence/store.ts";
import { withLogScope } from "../shared/log.ts";

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
  readonly buffer: (message: MediaGroupMessage) => Effect.Effect<void>;
  /** Handles every group whose moment passed while alasio was stopped. */
  readonly flushDue: Effect.Effect<void, CommandError>;
}>()("alasio/telegram/MediaGroups") {
  static readonly layer = (flushAfter: Duration.Input = DEFAULT_FLUSH_AFTER): Layer.Layer<MediaGroups, never, OperatorServices> =>
    Layer.effect(MediaGroups, makeMediaGroups(flushAfter));
}

const makeMediaGroups = Effect.fnUntraced(function*(flushAfter: Duration.Input) {
  const store = yield* Store;
  const services = yield* Effect.context<OperatorServices>();
  const flushMs = Duration.toMillis(flushAfter);
  // Each group's wait, which a later message of the group starts again; and the groups
  // being handled, which a later message no longer reaches.
  const waits = yield* FiberMap.make<string>();
  const flushing = yield* FiberSet.make();

  /** The group's messages and files as one prompt. */
  const flush = Effect.fnUntraced(function*(mediaGroupId: string, conversationId: string, chatId: number | string) {
    const messages = store.getMediaGroupMessages(mediaGroupId);
    const files = store.getFilesForMessages(messages.map((message) => message.id));
    const text = messages.map((message) => String(message.text ?? "").trim()).find(Boolean) ?? "";
    const messageId = Number(messages[0]?.transport_message_id ?? 0) || (yield* Clock.currentTimeMillis);
    const filePaths = files.map((file) => file.local_path).filter((path): path is string => Boolean(path));
    store.markMediaGroupFlushed(mediaGroupId);
    yield* processPrompt({ conversationId, chatId, messageId, text, filePaths }).pipe(Effect.provideContext(services));
  });

  return MediaGroups.of({
    buffer: ({ mediaGroupId, conversationId, updateId, chatId }) =>
      Effect.suspend(() => {
        store.upsertMediaGroup({ mediaGroupId, conversationId, updateId, flushAfterMs: flushMs });
        const failed = (error: unknown) => Effect.logError(`Failed to flush media group ${mediaGroupId}: ${error}`);
        const handle = flush(mediaGroupId, conversationId, chatId).pipe(Effect.catch(failed), Effect.catchDefect(failed), withLogScope("telegram-app"));
        return FiberMap.run(waits, mediaGroupId, Effect.sleep(flushAfter).pipe(Effect.andThen(FiberSet.run(flushing, handle))));
      }).pipe(Effect.asVoid),
    flushDue: Effect.suspend(() =>
      Effect.forEach(
        store.getPendingMediaGroupsDue(flushMs),
        (group) => flush(group.id, group.conversation_id, group.conversation_id.replace(/^telegram:/, "")),
        { discard: true },
      )
    ),
  });
});
