/**
 * What the harness host's role reaches as alasio's setup grants it (src/persistence/
 * harness-grants.ts): each table's policy of the role made anew where it is not as this
 * release says, as a release that changes what the role reaches changes it, and left as it is,
 * its table not locked, where it is.
 */
import assert from "node:assert/strict";
import { test } from "node:test";

import { Effect } from "effect";
import pg from "pg";

import { HARNESS_ROLE } from "../neon/control/secrets.ts";
import { scramVerifier } from "../neon/control/scram.ts";
import { ensureHarnessRole } from "../src/neon/harness.ts";
import { Store } from "../src/persistence/store.ts";
import { startPostgres } from "./support/postgres.ts";

/** The schema alasio's state is in. */
const SCHEMA = "state";

test("setup makes a policy of the role's anew where a release before made it otherwise, and leaves one as this release makes it", async (t) => {
  const database = await startPostgres();
  const pool = new pg.Pool({ connectionString: database.url, max: 2 });
  t.after(async () => {
    await pool.end();
    await database.stop();
  });
  await ensureHarnessRole(pool, scramVerifier("harness-password"));
  await Effect.runPromise(Effect.provide(Store, Store.layer({ pool, harnessRole: HARNESS_ROLE })));
  /** The policy of the role's on `table`: its oid, and what it reads by. */
  const policy = async (table: string) =>
    (await pool.query<{ oid: number; qual: string }>(
      `select p.oid::int as oid, pg_get_expr(p.polqual, p.polrelid) as qual from pg_policy p where p.polrelid = $1::regclass and p.polname = 'harness_rows'`,
      [`${SCHEMA}.${table}`],
    )).rows[0];
  const kept = await policy("files");
  assert.match(kept?.qual ?? "", /harness_conversation/u);
  // As a release before made it: the turns read by what this one reads them by no more.
  await pool.query(`drop policy harness_rows on ${SCHEMA}.turns; create policy harness_rows on ${SCHEMA}.turns as restrictive to ${HARNESS_ROLE} using (false) with check (false)`);
  assert.equal((await policy("turns"))?.qual, "false");
  // Set up again, as this release's alasio starts.
  await Effect.runPromise(Effect.provide(Store, Store.layer({ pool, harnessRole: HARNESS_ROLE })));
  assert.match((await policy("turns"))?.qual ?? "", /harness_conversation/u);
  assert.equal((await policy("files"))?.oid, kept?.oid, "a policy as this release makes it was made anew");
});
