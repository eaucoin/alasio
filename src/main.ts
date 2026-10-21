/**
 * alasio as one program: what it runs is acquired in order and released in reverse,
 * when the process is told to stop (SIGTERM, SIGINT) or what it runs fails, after which
 * the telemetry it holds is flushed and the process exits.
 */
import { Cause, Effect, Layer } from "effect";

import { HARNESS_ROLE } from "../neon/control/secrets.ts";
import { ForkAsks, ForkRequests } from "./branch/fork.ts";
import { forksGroup, forksTopic, MAIN_ENVIRONMENT_NAME } from "./branch/names.ts";
import { loadAlasioConfig } from "./config.ts";
import { EnvironmentEvents } from "./events/consume.ts";
import { Events } from "./events/events.ts";
import { codexHome } from "./codex/env.ts";
import { CodexRollouts, SessionFsCodexRollouts } from "./codex/rollouts/index.ts";
import { sessionFsCodexHome } from "./codex/sessionfs.ts";
import { indexTranscripts } from "./harness/claude/search/index.ts";
import { Neon } from "./neon/connect.ts";
import { LakeWake } from "./neon/lake-wake.ts";
import { serveAlasio } from "./alasio.ts";
import { withLogScope } from "./shared/log.ts";
import { runAlasio } from "./shared/run.ts";
import { TelegramUpdates } from "./telegram/update-consumer.ts";

const alasio = Effect.gen(function*() {
  const config = loadAlasioConfig();
  // alasio's state, Claude Code's transcripts and Codex's rollouts are kept in alasio's
  // Neon, which the deployment runs, and Codex's are mirrored from the start so no turn
  // runs unmirrored; the lake's loader is told of each write, where the lake runs. Neon
  // may take minutes to answer, and a stop meanwhile stops the wait.
  const neon = yield* Neon.make.pipe(Effect.provideServiceEffect(LakeWake, config.lakeWake ? LakeWake.make(config.lakeWake) : Effect.succeed(LakeWake.none)));
  const codexRollouts = yield* CodexRollouts.make({ store: neon.rollouts, home: codexHome() });
  // Session filesystems (an empty, isolated workspace per session) are on when the
  // deployment gives alasio a sandbox gateway. Their Codex runs from a home of its own,
  // mirrored the same way into a schema of its own.
  const sessionFsCodexRollouts = config.sessionsGateway
    ? yield* SessionFsCodexRollouts.make({ store: yield* neon.sessionFsRollouts, home: sessionFsCodexHome(config.stateDir) })
    : null;
  // A branch asks main to fork the sessions it inherited, on main's forks topic, and hears
  // its answers among its own events; main reads its forks topic and answers on theirs.
  yield* serveAlasio({
    ...config,
    updates: TelegramUpdates.layer(config.telegramLog),
    parentLog: config.branch ? Layer.effect(ForkAsks, ForkAsks.make({ ...config.log, topic: forksTopic(MAIN_ENVIRONMENT_NAME) })) : undefined,
    events: config.harnessPools || config.branch ? EnvironmentEvents.layer(config.events, { branch: config.branch !== null }) : undefined,
    branchesLog: config.branch === null && config.branchForkKeyFile
      ? Layer.merge(
        ForkRequests.layer({ ...config.log, topic: forksTopic(MAIN_ENVIRONMENT_NAME), group: forksGroup(MAIN_ENVIRONMENT_NAME) }),
        Layer.effect(Events, Events.make(config.log)),
      )
      : undefined,
    pool: neon.pool,
    harnessRole: HARNESS_ROLE,
    lakeWritten: neon.written,
    sessionStore: neon.sessionStore,
    codexRollouts,
    sessionFsCodexRollouts,
  });
  yield* Effect.logInfo("Telegram Alasio bot is running");
  // Once the bot serves: the indexer's first pass reads every stored entry.
  yield* indexTranscripts({ pool: neon.pool });
}).pipe(
  Effect.tapCause((cause) => Effect.logError(`Fatal error: ${Cause.pretty(cause)}`)),
  withLogScope("index"),
);

runAlasio(alasio);
