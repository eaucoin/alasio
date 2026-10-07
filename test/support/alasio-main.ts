/**
 * alasio's main for its tests: alasio assembled as src/main.ts assembles it, run by
 * ./alasio.ts as a process of its own, so that stopping it is what stopping alasio is.
 * Telegram and the Codex binary are the stand-ins the environment names, as production's
 * are (TELEGRAM_API_ROOT, ALASIO_CODEX_BIN); a folder workspace's bayma and Claude Code's
 * queries are the stand-ins the configuration names. Its state is in the test's Postgres,
 * in a schema of the test's. There is no Neon of its own and no Kubernetes: no transcript
 * store, no rollout mirroring, and no session filesystems.
 *
 *   node alasio-main.ts <AlasioProcessConfig as JSON>
 *
 * It tells its parent when alasio has started, and is run, and stopped on SIGTERM, as main runs alasio.
 */
import { Effect } from "effect";
import pg from "pg";

import type { BaymaMcpServer } from "../../src/mcp/bayma.ts";
import { runAlasio, serveAlasio } from "../../src/alasio.ts";
import { bridgedQueryFactory } from "./claude.ts";

/** What the test runs alasio with. */
export interface AlasioProcessConfig {
  /** Where alasio's state is kept: the test's Postgres, and a schema in it. */
  readonly databaseUrl: string;
  readonly stateSchema: string;
  readonly stateDir: string;
  readonly workspaceRoot: string;
  readonly allowedUserIds: string;
  /** Where Claude Code's queries are bridged to the test (./claude.ts). */
  readonly claudeSocket: string;
  /** The bayma every folder workspace is given. */
  readonly folderBayma: BaymaMcpServer;
}

/** What the process tells its parent once alasio has started. */
export const READY = "alasio started";

if (import.meta.main) {
  const [encoded] = process.argv.slice(2);
  if (encoded === undefined) throw new Error("usage: alasio-main.ts <config as JSON>");
  // The test writes the configuration.
  const config = JSON.parse(encoded) as AlasioProcessConfig;
  const alasio = Effect.gen(function*() {
    const pool = yield* Effect.acquireRelease(
      Effect.sync(() => new pg.Pool({ connectionString: config.databaseUrl, max: 4 })),
      (pool) => Effect.promise(() => pool.end()),
    );
    yield* serveAlasio({
      telegramBotToken: "123:test",
      allowedUserIds: config.allowedUserIds,
      workingDirectory: null,
      workspaceRoot: config.workspaceRoot,
      stateDir: config.stateDir,
      pool,
      stateSchema: config.stateSchema,
      hookPort: 0,
      warmLinkedSessions: false,
      defaultHarness: null,
      folderBayma: () => Effect.succeed(config.folderBayma),
      claudeQueryFactory: bridgedQueryFactory(config.claudeSocket),
    });
  });
  runAlasio(alasio.pipe(Effect.andThen(Effect.sync(() => process.send?.(READY)))));
}
