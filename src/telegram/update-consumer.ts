/**
 * Telegram's updates, as alasio takes them: from its environment's topic in the log,
 * `telegram.<environment>`, which the webhook intake writes each update Telegram delivers
 * to (webhook/src/server.ts), as a member of its group, `alasio.<environment>`. Each is
 * handed to the app in turn, a chat's in the order Telegram sent them, and the group is
 * told only once it is handled, so a restart neither loses one nor skips one; one handed
 * over again after a restart, which alasio had handled, it knows from the store and
 * handles once (./app.ts). An update whose handling fails is skipped, as it is kept in the
 * store first: taking it again forever would wedge the bot. One the store failed, keeping
 * it, handling it or marking it processed, is not: the group is left without being told
 * of it, and joined again, so it is handed over again, and handled, once the store answers.
 */
import type { Update } from "@grammyjs/types";
import { Context, Effect, Layer, Schema } from "effect";

import { joinTopic, type TopicLog } from "../log/topic.ts";
import { withLogScope } from "../shared/log.ts";

/** How long consuming waits after its member of the group failed before it joins again. */
const REJOIN_BACKOFF = "3 seconds";

/** Where the environment's updates are: the log's brokers, its principal on them, its topic and its group. */
export type TelegramLog = TopicLog;

/** A record of the topic: its update (null for one that is none), where it is, and what tells the group it is handled. */
export interface ReceivedUpdate {
  readonly update: Update | null;
  readonly position: string;
  readonly commit: () => Promise<void>;
}

/** The environment's member of its group: its records in turn, until it is closed. */
export interface UpdateFeed {
  readonly next: () => Promise<ReceivedUpdate>;
  readonly close: () => Promise<void>;
}

/** The log could not be joined, or its member failed. */
export class UpdateFeedError extends Schema.TaggedError<UpdateFeedError>()("UpdateFeedError", {
  message: Schema.String,
}) {}

const errorText = (error: unknown): string => (error instanceof Error ? error.message : String(error));

/** What joins the environment's group: the log's, or a stand-in's in alasio's tests. */
export class TelegramUpdates extends Context.Service<TelegramUpdates, {
  readonly join: Effect.Effect<UpdateFeed, UpdateFeedError>;
}>()("alasio/telegram/TelegramUpdates") {
  /** The environment's group on the log, as `log` says it, with @platformatic/kafka, alasio's one Kafka client. */
  static readonly layer = (log: TelegramLog): Layer.Layer<TelegramUpdates> =>
    Layer.succeed(TelegramUpdates, TelegramUpdates.of({
      join: Effect.tryPromise({ try: () => joinUpdates(log), catch: (error) => new UpdateFeedError({ message: `joining ${log.group} on ${log.topic}: ${errorText(error)}` }) }),
    }));
}

/** The update a record holds, or null where it holds none. */
function updateOf(value: Buffer | undefined): Update | null {
  try {
    const parsed: unknown = JSON.parse(value?.toString("utf8") ?? "");
    return typeof parsed === "object" && parsed !== null && typeof (parsed as { update_id?: unknown }).update_id === "number" ? (parsed as Update) : null;
  } catch {
    return null;
  }
}

/** Joins `log`'s group on its topic, reading from where the group was last told, or from the topic's start. */
async function joinUpdates(log: TelegramLog): Promise<UpdateFeed> {
  const feed = await joinTopic(log, "earliest");
  return {
    next: () => feed.next().then(({ value, position, commit }) => ({ update: updateOf(value), position, commit })),
    close: feed.close,
  };
}

/**
 * What is done with each update: it fails where the store failed, keeping the update,
 * handling it or marking it processed, and the update is taken again; it dies where
 * anything else failed its handling, and the update is skipped.
 */
export type ProcessUpdate = (update: Update) => Effect.Effect<void, unknown>;

/** What an update's handling died of, as the log says it: its stack, where it has one. */
function describeDefect(defect: unknown): string {
  return defect instanceof Error ? defect.stack ?? defect.message : String(defect);
}

/**
 * Hands each update of the environment's topic to `processUpdate` in turn, telling the
 * group of each once it is handled, until it is interrupted: an update being handled then
 * is finished first. A member that fails, or whose update the store could not keep, is
 * left, and the group joined again, after a backoff, from where it was last told.
 */
export const consumeUpdates = Effect.fnUntraced(
  function*(processUpdate: ProcessUpdate): Effect.fn.Return<never, never, TelegramUpdates> {
    const { join } = yield* TelegramUpdates;

    const handle = (received: ReceivedUpdate): Effect.Effect<void, UpdateFeedError> =>
      (received.update === null
        ? Effect.logError(`Skipping ${received.position}, which is no update of Telegram's`)
        : processUpdate(received.update).pipe(
          Effect.catchDefect((defect) => Effect.logError(`Skipping update ${received.update?.update_id} after processing failure: ${describeDefect(defect)}`)),
          Effect.mapError((error) => new UpdateFeedError({ message: `keeping update ${received.update?.update_id}: ${errorText(error)}` })),
        )).pipe(
          Effect.andThen(Effect.tryPromise({ try: () => received.commit(), catch: (error) => new UpdateFeedError({ message: `telling the group of ${received.position}: ${errorText(error)}` }) })),
          Effect.uninterruptible,
        );

    const member = Effect.acquireRelease(join, (feed) => Effect.promise(() => feed.close().catch(() => {}))).pipe(
      Effect.flatMap((feed) =>
        Effect.forever(Effect.tryPromise({ try: () => feed.next(), catch: (error) => new UpdateFeedError({ message: errorText(error) }) }).pipe(Effect.flatMap(handle)))
      ),
      Effect.scoped,
    );

    return yield* member.pipe(
      Effect.catch((error) => Effect.logError(`Consuming Telegram's updates failed: ${error.message}`)),
      Effect.andThen(Effect.sleep(REJOIN_BACKOFF)),
      Effect.forever,
    );
  },
  withLogScope("telegram-app"),
);

/** The environment's updates' log, from alasio's environment. */
export function telegramLogOf(env: NodeJS.ProcessEnv): TelegramLog {
  const required = (key: string) => {
    const value = env[key]?.trim();
    if (!value) throw new Error(`Missing required environment variable: ${key}`);
    return value;
  };
  return {
    brokers: required("ALASIO_LOG_BROKERS").split(","),
    username: required("ALASIO_LOG_USERNAME"),
    passwordFile: required("ALASIO_LOG_PASSWORD_FILE"),
    topic: required("ALASIO_TELEGRAM_TOPIC"),
    group: required("ALASIO_TELEGRAM_GROUP"),
  };
}
