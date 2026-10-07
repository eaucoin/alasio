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
}

const onPostgres = <A>(run: () => Promise<A>): Effect.Effect<A, StoreError> =>
  Effect.tryPromise({ try: run, catch: (cause) => new StoreError({ cause }) });

const sqlOn = (client: Pool | PoolClient): Sql => ({
  query: <Row extends object>(text: string, values: readonly unknown[] = []) =>
    onPostgres(async () => (await client.query<Row>(text, [...values])).rows),
});

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
        // A connection that cannot roll back is not given back to the pool, but ended.
        (client, exit) =>
          Exit.isSuccess(exit)
            ? Effect.sync(() => client.release())
            : Effect.promise(() => client.query("rollback").then(() => client.release(), (error: Error) => client.release(error))),
      ),
  };
}
