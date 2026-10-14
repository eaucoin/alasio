/**
 * Telegram's updates, as alasio takes them: from its environment's topic in the log,
 * `telegram.<environment>`, which the webhook intake writes each update Telegram delivers
 * to (webhook/src/server.ts), as a member of its group, `alasio.<environment>`. Each is
 * handed to the app in turn, a chat's in the order Telegram sent them, and the group is
 * told only once it is handled, so a restart neither loses one nor skips one; one handed
 * over again after a restart, which alasio had handled, it knows from the store and
 * handles once (./app.ts). An update whose handling fails is skipped, as it is kept in the
 * store first: taking it again forever would wedge the bot.
 */
import { readFileSync } from "node:fs";

import type { Update } from "@grammyjs/types";
import { Consumer, MessagesStreamFallbackModes, MessagesStreamModes } from "@platformatic/kafka";
import { Cause, Context, Effect, Layer, Schema } from "effect";

import { withLogScope } from "../shared/log.ts";

/** How long consuming waits after its member of the group failed before it joins again. */
const REJOIN_BACKOFF = "3 seconds";

/** Where the environment's updates are: the log's brokers, its principal on them, its topic and its group. */
export interface TelegramLog {
  readonly brokers: readonly string[];
  readonly username: string;
  readonly passwordFile: string;
  readonly topic: string;
  readonly group: string;
}

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
      join: Effect.tryPromise({ try: () => joinTopic(log), catch: (error) => new UpdateFeedError({ message: `joining ${log.group} on ${log.topic}: ${errorText(error)}` }) }),
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
async function joinTopic(log: TelegramLog): Promise<UpdateFeed> {
  const consumer = new Consumer({
    clientId: log.username,
    groupId: log.group,
    bootstrapBrokers: [...log.brokers],
    // Read at each join, as the kubelet keeps the Secret's file.
    sasl: { mechanism: "SCRAM-SHA-512", username: log.username, password: readFileSync(log.passwordFile, "utf8").trim() },
    autocommit: false,
    maxWaitTime: 1_000,
  });
  let stream;
  try {
    stream = await consumer.consume({ topics: [log.topic], mode: MessagesStreamModes.COMMITTED, fallbackMode: MessagesStreamFallbackModes.EARLIEST, autocommit: false });
  } catch (error) {
    await consumer.close(true).catch(() => {});
    throw error;
  }
  const records = stream[Symbol.asyncIterator]();
  return {
    async next() {
      const read = await records.next();
      if (read.done) throw new Error("the topic's stream of records ended");
      const record = read.value;
      return { update: updateOf(record.value), position: `${record.topic}/${record.partition}/${record.offset}`, commit: () => record.commit() as Promise<void> };
    },
    async close() {
      await stream.close().catch(() => {});
      await consumer.close(true);
    },
  };
}

/** What is done with each update; however it fails, the update is skipped. */
export type ProcessUpdate = (update: Update) => Effect.Effect<void, unknown>;

/** What an update's processing failed with, as the log says it: its stack, where it has one. */
function describeFailure(cause: Cause.Cause<unknown>): string {
  const error = Cause.squash(cause);
  return error instanceof Error ? error.stack ?? error.message : String(error);
}

/**
 * Hands each update of the environment's topic to `processUpdate` in turn, telling the
 * group of each once it is handled, until it is interrupted: an update being handled then
 * is finished first. A member that fails is left, and the group joined again, after a
 * backoff, from where it was last told.
 */
export const consumeUpdates = Effect.fnUntraced(
  function*(processUpdate: ProcessUpdate): Effect.fn.Return<never, never, TelegramUpdates> {
    const { join } = yield* TelegramUpdates;

    const handle = (received: ReceivedUpdate): Effect.Effect<void, UpdateFeedError> =>
      (received.update === null
        ? Effect.logError(`Skipping ${received.position}, which is no update of Telegram's`)
        : processUpdate(received.update).pipe(
          Effect.catchCause((cause) => Effect.logError(`Skipping update ${received.update?.update_id} after processing failure: ${describeFailure(cause)}`)),
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
