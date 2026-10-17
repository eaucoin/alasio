/**
 * The lake's wake: as alasio commits transcript entries or rollout bytes to its Neon (its
 * stores, ./connect.ts), it writes a record on its environment's wake topic in the log
 * (`lake.<environment>`), which KEDA wakes the environment's lake loader on
 * (neon/lake/src/loader.ts), as alasio's principal on the log, which may write that topic
 * besides reading its Telegram topic (cli/src/manifests/log.ts). A record says only that
 * something was written, as a load is everything the source has that the lake does not:
 * one is written WAKE_DELAY after the first write since the last, the writes meanwhile in
 * it, and one as alasio starts, for what it wrote before. A record that is not written is
 * logged, and the next write's covers it. Each names the primary's WAL position as it is
 * written, past the writes it tells of, which the loader waits for its source, a read
 * replica, to have replayed before it loads (wakeRecord).
 */
import { readFileSync } from "node:fs";

import { ProduceAcks, Producer } from "@platformatic/kafka";
import { Context, Duration, Effect, FiberSet, type Scope } from "effect";

import { withLogScope } from "../shared/log.ts";

/** How long after a write its record is written, the writes meanwhile in it: a turn's transcript is written an entry at a time. */
export const WAKE_DELAY = Duration.seconds(2);

/** Where the environment's wake topic is: the log's brokers, alasio's principal on them, and the topic. */
export interface WakeLog {
  readonly brokers: readonly string[];
  readonly username: string;
  readonly passwordFile: string;
  readonly topic: string;
}

const errorText = (error: unknown): string => (error instanceof Error ? error.message : String(error));

/** The environment's wake topic, from alasio's environment: on the log alasio reads its Telegram updates from, as ALASIO_LAKE_WAKE_TOPIC names it; null where none is named, as the lake does not run. */
export function wakeLogOf(env: NodeJS.ProcessEnv): WakeLog | null {
  const topic = env["ALASIO_LAKE_WAKE_TOPIC"]?.trim();
  if (!topic) return null;
  const required = (key: string) => {
    const value = env[key]?.trim();
    if (!value) throw new Error(`Missing required environment variable: ${key}`);
    return value;
  };
  return { brokers: required("ALASIO_LOG_BROKERS").split(","), username: required("ALASIO_LOG_USERNAME"), passwordFile: required("ALASIO_LOG_PASSWORD_FILE"), topic };
}

/**
 * A record of the wake topic: when it was written, and `lsn`, the primary's WAL position
 * then, past every write it tells of, as pg_current_wal_lsn() says it; none where it
 * could not be read, the loader then loading what its source has.
 */
export const wakeRecord = (lsn: string | null, at = new Date()): Buffer => Buffer.from(JSON.stringify({ writtenBefore: at.toISOString(), lsn }));

/** What tells the lake's loader alasio wrote transcripts: on the log, or nothing where the lake does not run, as in alasio's tests. */
export class LakeWake extends Context.Service<LakeWake, {
  /** Says a write was committed on the primary whose WAL position `lsn` reads; never fails, nor waits. */
  readonly written: (lsn: () => Promise<string>) => void;
}>()("alasio/neon/LakeWake") {
  /** Nothing told: no lake runs. */
  static readonly none = LakeWake.of({ written: () => {} });

  /** The environment's wake topic on the log, as `log` says it, with @platformatic/kafka, alasio's one Kafka client, until the scope closes. */
  static readonly make = (log: WakeLog): Effect.Effect<LakeWake["Service"], never, Scope.Scope> =>
    Effect.gen(function*() {
      const producer = yield* Effect.acquireRelease(
        Effect.sync(() =>
          new Producer({
            clientId: log.username,
            bootstrapBrokers: [...log.brokers],
            // Read as alasio starts, as the kubelet keeps the Secret's file.
            sasl: { mechanism: "SCRAM-SHA-512", username: log.username, password: readFileSync(log.passwordFile, "utf8").trim() },
            acks: ProduceAcks.ALL,
          })
        ),
        (made) => Effect.promise(() => made.close().catch(() => {})),
      );
      const runFork = yield* FiberSet.makeRuntime();
      // Read as the record is written, after the writes it tells of: those meanwhile
      // write another.
      let lsnOf: (() => Promise<string>) | null = null;
      const position = Effect.suspend(() => {
        const read = lsnOf;
        return read === null ? Effect.succeed(null) : Effect.tryPromise({ try: read, catch: errorText });
      }).pipe(Effect.catch((error) => Effect.as(Effect.logWarning(`could not read the primary's WAL position for the lake's loader: ${error}`), null)));
      const send = Effect.flatMap(position, (lsn) =>
        Effect.tryPromise({
          try: () => producer.send({ messages: [{ topic: log.topic, value: wakeRecord(lsn) }] }),
          catch: errorText,
        })
      ).pipe(Effect.catch((error) => Effect.logWarning(`could not tell the lake's loader of transcripts written: ${error}`)));
      let due = false;
      const tell = (): void => {
        if (due) return;
        due = true;
        runFork(Effect.sleep(WAKE_DELAY).pipe(Effect.andThen(Effect.sync(() => {
          due = false;
        })), Effect.andThen(send)));
      };
      tell();
      return LakeWake.of({
        written: (lsn) => {
          lsnOf = lsn;
          tell();
        },
      });
    }).pipe(withLogScope("lake-wake"));
}
