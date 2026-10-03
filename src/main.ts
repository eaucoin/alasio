/**
 * alasio as one program: what it runs is acquired in order and released in reverse,
 * when the process is told to stop (SIGTERM, SIGINT) or what it runs fails, after which
 * the telemetry it holds is flushed and the process exits.
 */
import { Cause, Context, Effect } from "effect";

import { loadAlasioConfig } from "./config.ts";
import { codexHome } from "./codex/env.ts";
import { CodexRollouts, codexRolloutsFacade, SessionFsCodexRollouts } from "./codex/rollouts/index.ts";
import { sessionFsCodexHome } from "./codex/sessionfs.ts";
import { indexTranscripts } from "./harness/claude/search/index.ts";
import { loadKubeTemplates } from "./kube/config.ts";
import { Neon } from "./neon/connect.ts";
import { runAlasio, serveAlasio } from "./alasio.ts";
import { effectRunnerHere } from "./shared/effects.ts";
import { withLogScope } from "./shared/log.ts";

const alasio = Effect.gen(function*() {
  const config = loadAlasioConfig();
  // What the deployment makes workspaces from, checked before anything else starts.
  const kubeTemplates = loadKubeTemplates();
  // Claude Code's transcripts and Codex's rollouts are kept in alasio's Neon, which the
  // deployment runs, and Codex's are mirrored from the start so no turn runs unmirrored.
  // Neon may take minutes to answer, and a stop meanwhile stops the wait.
  const neon = yield* Neon.make;
  const codexRollouts = yield* CodexRollouts.make({ store: neon.rollouts, home: codexHome() });
  // Session filesystems (an empty, isolated workspace per session) are on when the
  // deployment renders their template. Their Codex runs from a home of its own,
  // mirrored the same way into a schema of its own.
  const sessionFsCodexRollouts = kubeTemplates.sessions
    ? yield* SessionFsCodexRollouts.make({ store: yield* neon.sessionFsRollouts, home: sessionFsCodexHome(config.stateDir) })
    : null;
  // The app, not yet written in Effect, calls the rollouts as promises run in this program.
  const effects = yield* effectRunnerHere(Context.empty());
  yield* serveAlasio({
    ...config,
    sessionStore: neon.sessionStore,
    codexRollouts: codexRolloutsFacade(codexRollouts, effects),
    sessionFsCodexRollouts: sessionFsCodexRollouts && codexRolloutsFacade(sessionFsCodexRollouts, effects),
    kubeTemplates,
  });
  yield* Effect.logInfo("Telegram Alasio bot is running");
  // Once the bot serves: the indexer's first pass reads every stored entry.
  yield* indexTranscripts({ pool: neon.pool });
}).pipe(
  Effect.tapCause((cause) => Effect.logError(`Fatal error: ${Cause.pretty(cause)}`)),
  withLogScope("index"),
);

runAlasio(alasio);
