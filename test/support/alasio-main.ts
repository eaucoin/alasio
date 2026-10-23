/**
 * alasio's main for its tests: alasio assembled as src/main.ts assembles it, run by
 * ./alasio.ts as a process of its own, so that stopping it is what stopping alasio is.
 * Telegram and the Codex binary are the stand-ins the environment names, as production's
 * are (TELEGRAM_API_ROOT, ALASIO_CODEX_BIN). Its state is in a database of the test's own,
 * as alasio's Neon would keep it, with the stores of the harnesses' sessions beside it; its
 * conversations' harnesses run in the harness pools the test stands in, in the test's process
 * (./pools.ts standInPools), which outlast it, reached through their front, and their events
 * heard on the topic the test serves it (./topics.ts serveTopic), and its folder workspaces' bayma
 * the pools' stand-in of one. There is no Kubernetes and no lake.
 *
 *   node alasio-main.ts <AlasioProcessConfig as JSON>
 *
 * It tells its parent when alasio has started, and is run, and stopped on SIGTERM, as main runs alasio.
 */
import { Effect, Layer } from "effect";
import pg from "pg";

import { HARNESS_ROLE } from "../../neon/control/secrets.ts";
import { serveAlasio } from "../../src/alasio.ts";
import { NeonRolloutStore, SESSION_FS_SCHEMA } from "../../src/codex/rollouts/store.ts";
import type { BranchEnvironment } from "../../src/config.ts";
import { EnvironmentEvents, EventsFeedError } from "../../src/events/consume.ts";
import { NeonSessionStore } from "../../src/harness/claude/session-store.ts";
import { runAlasio } from "../../src/shared/run.ts";
import { stubUpdates } from "./stub-updates.ts";
import { remoteJoin } from "./topics.ts";

/** What the test runs alasio with. */
export interface AlasioProcessConfig {
  /** Where alasio's state is kept: the test's database, and the schema in it. */
  readonly databaseUrl: string;
  readonly stateSchema: string;
  readonly stateDir: string;
  readonly workspaceRoot: string;
  readonly allowedUserIds: string;
  /** The harness pools' front, the key alasio's calls to it are made with, as base64, the environment it is of, and where alasio tells its pods its Codex login is relayed. */
  readonly gatewayUrl: string;
  readonly callerKey: string;
  readonly environment: string;
  readonly codexLoginUrl: string;
  /** Where the environment's events topic is served, its group joined. */
  readonly eventsUrl: string;
  /** The branch environment alasio is, if it is one. */
  readonly branch?: BranchEnvironment;
  /** alasio's pod's name, the same each time it starts, as a container started again in its pod keeps it. */
  readonly pod: string;
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
      keepCodexLogin: false,
      defaultHarness: null,
      branch: config.branch ?? null,
      branchForkKeyFile: null,
      sessionsGateway: { url: config.gatewayUrl, environment: config.environment, callerKey: Buffer.from(config.callerKey, "base64") },
      folders: true,
      codexLoginUrl: config.codexLoginUrl,
      pod: config.pod,
      inKubernetes: false,
      harnessRole: HARNESS_ROLE,
      sessionStore: new NeonSessionStore(pool),
      rolloutStores: { codex: new NeonRolloutStore(pool), sessionFs: new NeonRolloutStore(pool, { schema: SESSION_FS_SCHEMA }) },
      events: Layer.succeed(EnvironmentEvents, EnvironmentEvents.of({
        join: Effect.tryPromise({ try: () => remoteJoin(config.eventsUrl), catch: (error) => new EventsFeedError({ message: `joining the test's events topic: ${String(error)}` }) }),
      })),
      // Telegram's updates are the stand-in's, at the Bot API's root the environment names.
      updates: stubUpdates(process.env["TELEGRAM_API_ROOT"] ?? ""),
    });
  });
  runAlasio(alasio.pipe(Effect.andThen(Effect.sync(() => process.send?.(READY)))));
}
