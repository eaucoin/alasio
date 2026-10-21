/**
 * alasio's Neon: the connection alasio keeps to the database its installation runs (the
 * Neon alasio installs, or one of the operator's), and what alasio keeps in it.
 *
 * It makes the analytics lake's role and catalog database either way (./lake.ts), and
 * with the lake on (ALASIO_LAKE_ENABLED) grants the lake its reads; with it off, it
 * revokes them. It makes the harness host's role (./harness.ts) either way, which alasio's
 * state grants its part as the store opens (../persistence/harness-grants.ts). It makes the lake reader's role (./lake.ts) and Grafana's role and
 * database (./grafana.ts) when the deployment gives their verifiers, as it does while
 * they run. It sets each role's password to the SCRAM verifier the deployment gives, the
 * one neon-control answers Neon's proxy with, and never sees a password.
 */
import { readFileSync } from "node:fs";

import { Config, Context, Effect, FiberSet, Option, Redacted, Schedule, Schema, type Scope } from "effect";
import pg, { type Pool } from "pg";

import { isScramVerifier } from "../../neon/control/scram.ts";
import { NeonRolloutStore, SESSION_FS_SCHEMA } from "../codex/rollouts/store.ts";
import { NeonSessionStore } from "../harness/claude/session-store.ts";
import { hearingConnections } from "../persistence/sql.ts";
import { withLogScope } from "../shared/log.ts";
import { withAlasioSpan } from "../telemetry/index.ts";
import { ensureGrafanaRole } from "./grafana.ts";
import { GrantedPool } from "./granted-pool.ts";
import { ensureHarnessRole } from "./harness.ts";
import { ensureLakeReaderRole, ensureLakeRole, lakeEnabled, syncLakeReads } from "./lake.ts";
import { LakeWake } from "./lake-wake.ts";

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

/** How to reach a deployment's Neon, whether the lake runs, and the verifiers of the passwords of the roles alasio makes there. */
interface NeonAccess {
  readonly databaseUrl: Redacted.Redacted;
  readonly lakeVerifier: Redacted.Redacted;
  readonly harnessVerifier: Redacted.Redacted;
  readonly lake: boolean;
  /** The lake's reader's, given while the lake runs. */
  readonly lakeReaderVerifier: Option.Option<Redacted.Redacted>;
  /** Grafana's, given while Grafana runs. */
  readonly grafanaVerifier: Option.Option<Redacted.Redacted>;
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

/**
 * How long a connection may take: Neon's proxy holds a connection to a compute that sleeps
 * until neon-control has woken it, which on a cold storage path (the pageserver just
 * restarted, catching up on WAL) has taken half a minute.
 */
const CONNECT_TIMEOUT_MS = 60_000;

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

/** `secret`, given as `key`, if it is a role's SCRAM verifier, as alasio sets a role's password to; a NeonSettingError if it is not. */
const verifier = (key: string) => (secret: Redacted.Redacted): Effect.Effect<Redacted.Redacted, NeonSettingError> =>
  isScramVerifier(Redacted.value(secret))
    ? Effect.succeed(secret)
    : Effect.fail(new NeonSettingError({ key, cause: new Error(`${key} must be a SCRAM-SHA-256 verifier, as Postgres stores one, not a password`) }));

/** A role's verifier the deployment gives, if it gives one, as givenSecret reads it, checked to be one. */
const givenVerifier = (key: string) => Effect.flatMap(givenSecret(key), Option.match({ onNone: () => Effect.succeedNone, onSome: (secret) => Effect.asSome(verifier(key)(secret)) }));

/** A promise on Neon, its rejection a NeonUnavailable. */
const onNeon = <A>(query: () => Promise<A>): Effect.Effect<A, NeonUnavailable> =>
  Effect.tryPromise({ try: query, catch: (cause) => new NeonUnavailable({ cause }) });

/** Ends a pool that is of no more use, whatever it says. */
const endPool = (pool: Pool): Effect.Effect<void> => onNeon(() => pool.end()).pipe(Effect.ignore);

/**
 * What says a store's write was committed on `pool`'s database: `wake` told, the primary's
 * WAL position read as its record is written.
 */
const writtenOn = (pool: Pool, wake: LakeWake["Service"]): (() => void) => () =>
  wake.written(async () => {
    const { rows: [row] } = await pool.query<{ lsn: string }>("select pg_current_wal_lsn()::text as lsn");
    if (!row) throw new Error("the primary named no WAL position");
    return row.lsn;
  });

/** A pool of connections to `databaseUrl`, as `make` makes one, whose idle connections lost are said with `onIdleError`. */
function poolOf<P extends Pool>(databaseUrl: Redacted.Redacted, onIdleError: (error: Error) => void, make: (config: pg.PoolConfig) => P): P {
  const pool = make({
    connectionString: Redacted.value(databaseUrl),
    max: 8,
    idleTimeoutMillis: 30_000,
    connectionTimeoutMillis: CONNECT_TIMEOUT_MS,
  });
  // An idle connection dies with the compute when it restarts; the pool
  // replaces it on the next checkout.
  pool.on("error", onIdleError);
  return hearingConnections(pool);
}

/**
 * Connects to a Neon that is already up and makes what alasio keeps in it: the session
 * and rollout stores' schemas, the lake's role and reads, and the lake reader's and
 * Grafana's roles when their verifiers are given. A pool that cannot is ended: before
 * the next attempt when this one fails, and in the background when a stop ends the
 * wait, which does not wait for it.
 */
const openNeon = Effect.fnUntraced(function*(
  { databaseUrl, lakeVerifier, harnessVerifier, lake, lakeReaderVerifier, grafanaVerifier }: NeonAccess,
  onIdleError: (error: Error) => void,
  wake: LakeWake["Service"],
): Effect.fn.Return<OpenNeon, NeonUnavailable> {
  const pool = poolOf(databaseUrl, onIdleError, (config) => new pg.Pool(config));
  const written = writtenOn(pool, wake);
  const sessionStore = new NeonSessionStore(pool, { written });
  const rollouts = new NeonRolloutStore(pool, { written });
  yield* onNeon(async () => {
    await sessionStore.ensureSchema();
    await rollouts.ensureSchema();
    await ensureLakeRole(pool, Redacted.value(lakeVerifier));
    await syncLakeReads(pool, lake);
    await ensureHarnessRole(pool, Redacted.value(harnessVerifier));
    if (Option.isSome(lakeReaderVerifier)) await ensureLakeReaderRole(pool, Redacted.value(lakeReaderVerifier.value));
    if (Option.isSome(grafanaVerifier)) await ensureGrafanaRole(pool, Redacted.value(databaseUrl), Redacted.value(grafanaVerifier.value));
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
 * Connects to the Neon the deployment provides, from `ALASIO_DATABASE_URL`,
 * `ALASIO_LAKE_VERIFIER` and `ALASIO_HARNESS_VERIFIER`, and `ALASIO_LAKE_READER_VERIFIER` and `ALASIO_GRAFANA_VERIFIER`
 * if given, or their `_FILE` forms, until the scope closes. The stack starts
 * beside alasio, so it is waited for, up to ten minutes, retrying while it does not
 * answer; a stop meanwhile stops the wait.
 */
const connectNeon: Effect.Effect<ConnectedNeon, NeonError, Scope.Scope | LakeWake> = Effect.gen(function*() {
  const databaseUrl = yield* deploymentSecret("ALASIO_DATABASE_URL");
  const lakeVerifier = yield* Effect.flatMap(deploymentSecret("ALASIO_LAKE_VERIFIER"), verifier("ALASIO_LAKE_VERIFIER"));
  const harnessVerifier = yield* Effect.flatMap(deploymentSecret("ALASIO_HARNESS_VERIFIER"), verifier("ALASIO_HARNESS_VERIFIER"));
  const lake = yield* lakeEnabled;
  const lakeReaderVerifier = yield* givenVerifier("ALASIO_LAKE_READER_VERIFIER");
  const grafanaVerifier = yield* givenVerifier("ALASIO_GRAFANA_VERIFIER");
  const wake = yield* LakeWake;
  const runFork = yield* FiberSet.makeRuntime();
  const onIdleError = (error: Error): void => {
    runFork(Effect.logWarning(`idle database connection lost: ${error.message}`));
  };
  const { pool, sessionStore, rollouts } = yield* Effect.acquireRelease(
    openNeon({ databaseUrl, lakeVerifier, harnessVerifier, lake, lakeReaderVerifier, grafanaVerifier }, onIdleError, wake).pipe(
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
      const store = new NeonRolloutStore(pool, { schema: SESSION_FS_SCHEMA, written: writtenOn(pool, wake) });
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
/**
 * The harness host's connection to alasio's Neon: its pool, its connections carrying the
 * grant last given it (./granted-pool.ts), and the stores of Claude Code's transcripts and
 * Codex's rollouts, which alasio made.
 */
export interface HarnessNeonStores extends OpenNeon {
  readonly pool: GrantedPool;
  /** The session-filesystem Codex home's rollout store. */
  readonly sessionFsRollouts: NeonRolloutStore;
}

/**
 * Connects to the Neon the deployment provides as the harness host's role
 * (`ALASIO_DATABASE_URL`, or its `_FILE` form), until the scope closes, waiting for it as
 * alasio does; it makes nothing there, which alasio has made, and may not. Until its pool is
 * given a grant, it reaches nothing there but the server.
 */
const connectHarnessNeon: Effect.Effect<HarnessNeonStores, NeonError, Scope.Scope | LakeWake> = Effect.gen(function*() {
  const databaseUrl = yield* deploymentSecret("ALASIO_DATABASE_URL");
  const wake = yield* LakeWake;
  const runFork = yield* FiberSet.makeRuntime();
  const pool = yield* Effect.acquireRelease(
    Effect.sync(() => poolOf(databaseUrl, (error) => void runFork(Effect.logWarning(`idle database connection lost: ${error.message}`)), (config) => new GrantedPool(config))),
    (made) => Effect.promise(() => made.end()),
  );
  yield* onNeon(() => pool.query("select 1")).pipe(
    Effect.retry(whileNeonStarts),
    withAlasioSpan("alasio.neon.connect"),
    Effect.interruptible,
  );
  yield* Effect.logInfo("connected to Neon");
  const written = writtenOn(pool, wake);
  return {
    pool,
    sessionStore: new NeonSessionStore(pool, { written }),
    rollouts: new NeonRolloutStore(pool, { written }),
    sessionFsRollouts: new NeonRolloutStore(pool, { schema: SESSION_FS_SCHEMA, written }),
  };
}).pipe(withLogScope("neon"));

/** The harness host's connection to alasio's Neon, open while the scope `HarnessNeon.make` is run in lasts. */
export class HarnessNeon extends Context.Service<HarnessNeon, HarnessNeonStores>()("alasio/neon/HarnessNeon", { make: connectHarnessNeon }) {}
