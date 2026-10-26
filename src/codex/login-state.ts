/**
 * What alasio's store keeps of its Codex login, for `alasio status`, run in a pod of the Codex
 * login relay's (cli/src/commands/status.ts, cli/src/commands/login-relay.ts), which reaches the
 * store as the relay's role does, the login's tables alone: one line of JSON, whether a login is
 * kept, and when its refresh was refused, if it was (./login-refresh.ts).
 */
import { readFileSync } from "node:fs";

import { Effect } from "effect";
import pg from "pg";

import { NeonCodexLoginRepository } from "../persistence/codex-login-repository.ts";
import { hearingConnections, poolDatabase } from "../persistence/sql.ts";
import { DEFAULT_SCHEMA } from "../persistence/store.ts";

/** What the line says. */
export interface CodexLoginState {
  readonly kept: boolean;
  /** When its refresh was refused, as ISO 8601; null where it was not, or none is kept. */
  readonly refusedAt: string | null;
}

/** How long it waits for the store, as a compute asleep is woken for it. */
const CONNECT_TIMEOUT_MS = 60_000;

const urlFile = process.env["ALASIO_DATABASE_URL_FILE"];
// A connection lost, idle or held, fails the read, and so the line, not the process unheard.
const pool = hearingConnections(new pg.Pool({
  connectionString: urlFile ? readFileSync(urlFile, "utf8").trim() : process.env["ALASIO_DATABASE_URL"],
  max: 1,
  connectionTimeoutMillis: CONNECT_TIMEOUT_MS,
}));
pool.on("error", () => {});
try {
  const record = await Effect.runPromise(new NeonCodexLoginRepository(poolDatabase(pool), DEFAULT_SCHEMA).getCodexLoginRecord());
  const state: CodexLoginState = { kept: record !== null, refusedAt: record && record.refusedAt !== null ? new Date(record.refusedAt).toISOString() : null };
  process.stdout.write(`${JSON.stringify(state)}\n`);
} finally {
  await pool.end();
}
