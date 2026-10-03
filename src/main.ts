/**
 * alasio as one program: what it runs is acquired in order and released in reverse,
 * when the process is told to stop (SIGTERM, SIGINT) or what it runs fails, after which
 * the telemetry it holds is flushed and the process exits.
 */
import { Cause, Effect } from "effect";

import { loadAlasioConfig } from "./config.ts";
import { codexHome } from "./codex/env.ts";
import { startCodexRollouts } from "./codex/rollouts/index.ts";
import { NeonRolloutStore, SESSION_FS_SCHEMA } from "./codex/rollouts/store.ts";
import { sessionFsCodexHome } from "./codex/sessionfs.ts";
import { startTranscriptSearch } from "./harness/claude/search/index.ts";
import { loadKubeTemplates } from "./kube/config.ts";
import { syncLakeReads } from "./neon/lake.ts";
import { connectNeon } from "./neon/connect.ts";
import { runAlasio, serveAlasio } from "./alasio.ts";
import { withLogScope } from "./shared/log.ts";

const alasio = Effect.gen(function*() {
  const config = loadAlasioConfig();
  // What the deployment makes workspaces from, checked before anything else starts.
  const kubeTemplates = loadKubeTemplates();
  // Claude Code's transcripts and Codex's rollouts are kept in alasio's Neon, which the
  // deployment runs, and Codex's are mirrored from the start so no turn runs unmirrored.
  // Neon may take minutes to answer, and a stop meanwhile stops the wait.
  const neon = yield* Effect.acquireRelease(
    Effect.promise(() => connectNeon()).pipe(Effect.interruptible),
    (neon) => Effect.promise(() => neon.close()),
  );
  const codexRollouts = yield* Effect.acquireRelease(
    Effect.sync(() => startCodexRollouts({ store: neon.rollouts, home: codexHome() })),
    (rollouts) => Effect.promise(() => rollouts.close()),
  );
  // Session filesystems (an empty, isolated workspace per session) are on when the
  // deployment renders their template. Their Codex runs from a home of its own,
  // mirrored the same way into a schema of its own.
  const sessionFsCodexRollouts = kubeTemplates.sessions
    ? yield* Effect.acquireRelease(
      Effect.promise(async () => {
        const store = new NeonRolloutStore(neon.pool, { schema: SESSION_FS_SCHEMA });
        await store.ensureSchema();
        // The analytics lake loads this home too, once it may read it.
        await syncLakeReads(neon.pool, neon.lake);
        return startCodexRollouts({ store, home: sessionFsCodexHome(config.stateDir) });
      }),
      (rollouts) => Effect.promise(() => rollouts.close()),
    )
    : null;
  yield* serveAlasio({ ...config, sessionStore: neon.store, codexRollouts, sessionFsCodexRollouts, kubeTemplates });
  yield* Effect.logInfo("Telegram Alasio bot is running");
  // Once the bot serves: the indexer's first pass reads every stored entry.
  yield* Effect.acquireRelease(
    Effect.sync(() => startTranscriptSearch({ pool: neon.pool })),
    (search) => Effect.promise(() => search.close()),
  );
}).pipe(
  Effect.tapCause((cause) => Effect.logError(`Fatal error: ${Cause.pretty(cause)}`)),
  withLogScope("index"),
);

runAlasio(alasio);
