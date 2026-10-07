/**
 * alasio's Neon: the connection alasio keeps to the database its installation runs (the
 * Neon alasio installs, or one of the operator's), and what alasio keeps in it.
 *
 * It makes the analytics lake's role and catalog database either way (./lake.ts), and
 * with the lake on (ALASIO_LAKE_ENABLED) grants the lake its reads; with it off, it
 * revokes them. It makes the lake reader's role (./lake.ts) when the deployment gives
 * its password, as it does while the lake runs.
 */
import { readFileSync } from "node:fs";

import { Config, Context, Effect, FiberSet, Option, Redacted, Schedule, Schema, type Scope } from "effect";
import pg, { type Pool } from "pg";

import { NeonRolloutStore, SESSION_FS_SCHEMA } from "../codex/rollouts/store.ts";
import { NeonSessionStore } from "../harness/claude/session-store.ts";
import { withLogScope } from "../shared/log.ts";
import { withAlasioSpan } from "../telemetry/index.ts";
import { ensureLakeReaderRole, ensureLakeRole, lakeEnabled, syncLakeReads } from "./lake.ts";

/** What Neon failed with, as the database or the driver said it. */
export class NeonUnavailable extends Schema.TaggedError<NeonUnavailable>()("NeonUnavailable", {
  cause: Schema.Defect(),
}) {
  override get message(): string {
    return this.cause instanceof Error ? this.cause.message : String(this.cause);
  }
}

/** A setting Neon is reached with that the deployment did not give, or gave in a file alasio could not read. */
export class NeonSettingError extends Schema.TaggedError<NeonSettingError>()("NeonSettingError", {
  key: Schema.String,
  cause: Schema.optional(Schema.Defect()),
}) {
  override get message(): string {
    if (this.cause === undefined) return `${this.key} or ${this.key}_FILE must be set`;
    return this.cause instanceof Error ? this.cause.message : String(this.cause);
  }
}

/** How to reach a deployment's Neon, whether the lake runs, and the passwords of the roles alasio makes there. */
interface NeonAccess {
  readonly databaseUrl: Redacted.Redacted;
  readonly lakePassword: Redacted.Redacted;
  readonly lake: boolean;
  /** The lake's reader's, given while the lake runs. */
  readonly lakeReaderPassword: Option.Option<Redacted.Redacted>;
}

/** alasio's stores in an open Neon, on the pool they share. */
interface OpenNeon {
  readonly pool: Pool;
  readonly sessionStore: NeonSessionStore;
  readonly rollouts: NeonRolloutStore;
}

/** alasio's Neon once connected, as the Neon service gives it. */
export interface ConnectedNeon extends OpenNeon {
  /** Whether the analytics lake runs. */
  readonly lake: boolean;
  /** The session-filesystem Codex home's rollout store, made in Neon (and for the lake to read) when asked for. */
  readonly sessionFsRollouts: Effect.Effect<NeonRolloutStore, NeonUnavailable>;
}

/** What connecting to Neon fails with: a setting missing, or Neon not answering in time. */
export type NeonError = NeonUnavailable | NeonSettingError | Config.ConfigError;

/** How long alasio waits for a deployment's Neon to answer as it starts, and how often it asks. */
const CONNECT_PATIENCE = "10 minutes";
const CONNECT_RETRY = "5 seconds";
/** How many of the retries go by between one "waiting for Neon" line and the next: a minute's. */
const RETRIES_PER_REPORT = 12;

/** A setting's value, trimmed, or empty when it is not set. */
const setting = (name: string): Config.Config<string> =>
  Config.String(name).pipe(Config.map((value) => value.trim()), Config.withDefault(""));

/** A secret the deployment gives in the file `<key>_FILE` names, or else as `<key>` itself; none when it gives neither. */
const givenSecret = Effect.fnUntraced(function*(key: string): Effect.fn.Return<Option.Option<Redacted.Redacted>, NeonSettingError | Config.ConfigError> {
  const file = yield* setting(`${key}_FILE`);
  if (file) {
    return yield* Effect.try({
      try: () => Option.some(Redacted.make(readFileSync(file, "utf8").trim())),
      catch: (cause) => new NeonSettingError({ key, cause }),
    });
  }
  const value = yield* setting(key);
  return value ? Option.some(Redacted.make(value)) : Option.none();
});

/** A secret the deployment must give, as givenSecret reads it. */
const deploymentSecret = (key: string): Effect.Effect<Redacted.Redacted, NeonSettingError | Config.ConfigError> =>
  Effect.flatMap(givenSecret(key), Option.match({ onNone: () => Effect.fail(new NeonSettingError({ key })), onSome: Effect.succeed }));

/** A promise on Neon, its rejection a NeonUnavailable. */
const onNeon = <A>(query: () => Promise<A>): Effect.Effect<A, NeonUnavailable> =>
  Effect.tryPromise({ try: query, catch: (cause) => new NeonUnavailable({ cause }) });

/** Ends a pool that is of no more use, whatever it says. */
const endPool = (pool: Pool): Effect.Effect<void> => onNeon(() => pool.end()).pipe(Effect.ignore);

/**
 * Connects to a Neon that is already up and makes what alasio keeps in it: the session
 * and rollout stores' schemas, the lake's role and reads, and the lake reader's
 * role when its password is given. A pool that cannot is ended: before the next
 * attempt when this one fails, and in the background when a stop ends the wait, which
 * does not wait for it.
 */
const openNeon = Effect.fnUntraced(function*(
  { databaseUrl, lakePassword, lake, lakeReaderPassword }: NeonAccess,
  onIdleError: (error: Error) => void,
): Effect.fn.Return<OpenNeon, NeonUnavailable> {
  const pool = new pg.Pool({
    connectionString: Redacted.value(databaseUrl),
    max: 8,
    idleTimeoutMillis: 30_000,
    connectionTimeoutMillis: 30_000,
  });
  // An idle connection dies with the compute when it restarts; the pool
  // replaces it on the next checkout.
  pool.on("error", onIdleError);
  const sessionStore = new NeonSessionStore(pool);
  const rollouts = new NeonRolloutStore(pool);
  yield* onNeon(async () => {
    await sessionStore.ensureSchema();
    await rollouts.ensureSchema();
    await ensureLakeRole(pool, Redacted.value(lakePassword));
    await syncLakeReads(pool, lake);
    if (Option.isSome(lakeReaderPassword)) await ensureLakeReaderRole(pool, Redacted.value(lakeReaderPassword.value));
  }).pipe(
    Effect.tapError(() => endPool(pool)),
    Effect.onInterrupt(() => Effect.forkDetach(endPool(pool))),
  );
  return { pool, sessionStore, rollouts };
});

/**
 * Neon's patience as alasio starts: every five seconds for ten minutes, saying why the
 * first time and once a minute after.
 */
const whileNeonStarts = Schedule.max([Schedule.spaced(CONNECT_RETRY), Schedule.during(CONNECT_PATIENCE)]).pipe(
  Schedule.setInputType<NeonUnavailable>(),
  Schedule.tap(({ attempt, input }) =>
    attempt === 1 || attempt % RETRIES_PER_REPORT === 0 ? Effect.logInfo(`waiting for Neon: ${input.message}`) : Effect.void
  ),
);

/**
 * Connects to the Neon the deployment provides, from `ALASIO_DATABASE_URL` and
 * `ALASIO_LAKE_PASSWORD`, and `ALASIO_LAKE_READER_PASSWORD` if given, or their `_FILE`
 * forms, until the scope closes. The stack starts
 * beside alasio, so it is waited for, up to ten minutes, retrying while it does not
 * answer; a stop meanwhile stops the wait.
 */
const connectNeon: Effect.Effect<ConnectedNeon, NeonError, Scope.Scope> = Effect.gen(function*() {
  const databaseUrl = yield* deploymentSecret("ALASIO_DATABASE_URL");
  const lakePassword = yield* deploymentSecret("ALASIO_LAKE_PASSWORD");
  const lake = yield* lakeEnabled;
  const lakeReaderPassword = yield* givenSecret("ALASIO_LAKE_READER_PASSWORD");
  const runFork = yield* FiberSet.makeRuntime();
  const onIdleError = (error: Error): void => {
    runFork(Effect.logWarning(`idle database connection lost: ${error.message}`));
  };
  const { pool, sessionStore, rollouts } = yield* Effect.acquireRelease(
    openNeon({ databaseUrl, lakePassword, lake, lakeReaderPassword }, onIdleError).pipe(
      Effect.retry(whileNeonStarts),
      withAlasioSpan("alasio.neon.connect", { attributes: { "alasio.neon.lake": lake } }),
      Effect.interruptible,
    ),
    ({ pool }) => Effect.promise(() => pool.end()),
  );
  yield* Effect.logInfo("connected to Neon");
  return {
    pool,
    sessionStore,
    rollouts,
    lake,
    sessionFsRollouts: onNeon(async () => {
      const store = new NeonRolloutStore(pool, { schema: SESSION_FS_SCHEMA });
      await store.ensureSchema();
      // The analytics lake loads this home too, once it may read it.
      await syncLakeReads(pool, lake);
      return store;
    }),
  };
}).pipe(withLogScope("neon"));

/**
 * alasio's connection to its deployment's Neon, open while the scope `Neon.make` is run in
 * lasts: `sessionStore` keeps Claude Code's transcripts, `rollouts` Codex's rollout
 * files, and `lake` says whether the analytics lake runs.
 */
export class Neon extends Context.Service<Neon, ConnectedNeon>()("alasio/neon/Neon", { make: connectNeon }) {}