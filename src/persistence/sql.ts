/**
 * How alasio's store reaches Postgres: statements, each on a connection of the pool, and
 * transactions, whose statements run on one connection between `begin` and `commit`.
 * A transaction whose work fails, or is interrupted, is rolled back.
 */
import { Effect, Exit, Schema } from "effect";
import type { Pool, PoolClient } from "pg";

/** What alasio's store failed with, as Postgres or the driver said it. */
export class StoreError extends Schema.TaggedError<StoreError>()("StoreError", {
  cause: Schema.Defect(),
}) {
  override get message(): string {
    return this.cause instanceof Error ? this.cause.message : String(this.cause);
  }
}

/** Statements on Postgres: the rows each returns. */
export interface Sql {
  readonly query: <Row extends object>(text: string, values?: readonly unknown[]) => Effect.Effect<Row[], StoreError>;
}

/** Statements on Postgres, one at a time or together in a transaction. */
export interface Database extends Sql {
  /** Runs `work`'s statements in one transaction: committed if it succeeds, rolled back if not. */
  readonly transaction: <A, E, R>(work: (sql: Sql) => Effect.Effect<A, E, R>) => Effect.Effect<A, E | StoreError, R>;
  /**
   * Runs `work`'s statements in one transaction, as `transaction` does, but for what may cut
   * it short: waiting for a connection, and `work`, may be interrupted, the transaction
   * rolled back, so nothing it did is kept; its commit may not.
   */
  readonly interruptibleTransaction: <A>(work: (sql: Sql) => Effect.Effect<A, StoreError>) => Effect.Effect<A, StoreError>;
}

const onPostgres = <A>(run: () => Promise<A>): Effect.Effect<A, StoreError> =>
  Effect.tryPromise({ try: run, catch: (cause) => new StoreError({ cause }) });

const sqlOn = (client: Pool | PoolClient): Sql => ({
  query: <Row extends object>(text: string, values: readonly unknown[] = []) =>
    onPostgres(async () => (await client.query<Row>(text, [...values])).rows),
});

/**
 * A connection of `pool`, waited for as long as it takes, or until interrupted: one that
 * comes after its wait was interrupted is given back to the pool, not lost to it, and so is
 * one that came as it was interrupted, before it was handed on. Only the wait may be
 * interrupted: run where nothing else may be, as an acquisition is, the connection is
 * handed on once it has come. The fiber may yield between the two, as its scheduler has it,
 * and a stop may land there.
 */
const connection = (pool: Pool): Effect.Effect<PoolClient, StoreError> =>
  Effect.suspend(() => {
    /** The connection that came, until it is handed on. */
    let came: PoolClient | null = null;
    return Effect.callback<PoolClient, StoreError>((resume) => {
      let abandoned = false;
      pool.connect().then(
        (client) => {
          if (abandoned) return client.release();
          came = client;
          resume(Effect.succeed(client));
        },
        (cause: unknown) => abandoned || resume(Effect.fail(new StoreError({ cause }))),
      );
      return Effect.sync(() => {
        abandoned = true;
      });
    }).pipe(
      Effect.interruptible,
      Effect.onInterrupt(() => Effect.sync(() => came?.release())),
    );
  });

/** Gives a transaction's connection back: as it is where it committed, rolled back first where not; one that cannot roll back is ended. */
const releasing = (client: PoolClient, exit: Exit.Exit<unknown, unknown>): Effect.Effect<void> =>
  Exit.isSuccess(exit)
    ? Effect.sync(() => client.release())
    : Effect.promise(() => client.query("rollback").then(() => client.release(), (error: Error) => client.release(error)));

/**
 * `pool`, each of its connections heard when it fails, whoever holds it. pg fails the
 * statements of a connection that dies, and tells the connection's 'error' listeners of it as
 * well, whether a statement of it was running or none was; the pool listens only while the
 * connection is idle, so one held, by a transaction above all, its holder at a statement or
 * at other work between its statements, would fail unheard, which Node takes as the
 * process's to die of. Heard, it fails no more than its holder's statements, as pg makes the
 * connection unqueryable, and the pool ends it as it is given back.
 */
export function hearingConnections<P extends Pool>(pool: P): P {
  pool.on("connect", (client) => void client.on("error", () => {}));
  return pool;
}

/** The database `pool` connects to. */
export function poolDatabase(pool: Pool): Database {
  return {
    ...sqlOn(pool),
    transaction: (work) =>
      Effect.acquireUseRelease(
        onPostgres(() => pool.connect()),
        (client) => {
          const sql = sqlOn(client);
          return sql.query("begin").pipe(
            Effect.andThen(work(sql)),
            Effect.tap(() => sql.query("commit")),
          );
        },
        releasing,
      ),
    interruptibleTransaction: (work) =>
      Effect.acquireUseRelease(
        connection(pool),
        (client) => {
          const sql = sqlOn(client);
          return sql.query("begin").pipe(
            Effect.andThen(work(sql)),
            Effect.tap(() => Effect.uninterruptible(sql.query("commit"))),
          );
        },
        releasing,
      ),
  };
}
