/**
 * `alasio harness`, a program of alasio's image: the harness host of one conversation,
 * which the control plane reaches over HTTP (./serve.ts) rather than in its process. It
 * reaches alasio's Neon as the harness host's role (../neon/harness.ts), which may write a
 * turn's part of the state and nothing more, and makes nothing there.
 *
 * As it binds the conversation, before it serves: its Codex home's rollouts are kept in
 * Neon (../codex/rollouts/index.ts), and those the conversation's Codex session needs
 * written back from there, so the session resumes where its last instance left it. Claude
 * Code's transcript is written back as its session is resumed, as alasio's harness writes
 * it back (../harness/claude/sessions.ts sessionExists): a resume runs from the local
 * transcript, the store given it mirroring alone (G14.1 found its own load would serve
 * too). The Codex login is not kept here: one relay keeps it (14.5), as two refreshing it
 * would end it. The files a prompt names are written as each turn starts (./host.ts).
 *
 * A session workspace's harness needs the sandbox gateway's binding of the workspace,
 * which the harness gateway brings (14.3); until then this host serves folder workspaces,
 * and refuses a session's turn, as alasio without session filesystems does.
 */
import { readFileSync } from "node:fs";

import { Context, Effect, Layer, type Scope } from "effect";

import { CodexAppServer } from "../codex/app-server/client.ts";
import { codexHome } from "../codex/env.ts";
import { CodexRollouts } from "../codex/rollouts/index.ts";
import { HarnessNeon } from "../neon/connect.ts";
import { LakeWake, type WakeLog, wakeLogOf } from "../neon/lake-wake.ts";
import { Store } from "../persistence/store.ts";
import { ReceivedFiles } from "../telegram/files.ts";
import { ActiveTurns } from "./active-turns.ts";
import { HarnessHost } from "./host.ts";
import { Harnesses, type HarnessesOptions } from "./index.ts";
import { serveHost } from "./serve.ts";

/** The port the host serves on unless its environment says. */
export const HARNESS_PORT = 8080;

/** What `alasio harness` runs with, as its environment sets it. */
export interface HarnessConfig {
  /** The conversation it is the host of: `ALASIO_HARNESS_CONVERSATION`. */
  readonly conversationId: string;
  /** Where it serves: `ALASIO_HARNESS_PORT`, HARNESS_PORT unless set. */
  readonly port: number;
  /** The token its caller bears, from the file `ALASIO_HARNESS_TOKEN_FILE` names. */
  readonly token: string;
  /** Where the files a prompt names are written: `ALASIO_STATE_DIR`, alasio's, where the prompt names them. */
  readonly stateDir: string;
  /** The branch environment it is of, which gives no harness in a folder; null for main. */
  readonly branch: string | null;
  /** Where it tells the lake's loader it wrote transcripts and rollouts; null where the lake does not run. */
  readonly lakeWake: WakeLog | null;
}

/** `key` of `env`, which must be set. */
function required(env: NodeJS.ProcessEnv, key: string): string {
  const value = env[key]?.trim();
  if (!value) throw new Error(`Missing required environment variable: ${key}`);
  return value;
}

/** The host's configuration from `env`; it fails, saying what, where one it needs is not given. */
export function loadHarnessConfig(env: NodeJS.ProcessEnv = process.env): HarnessConfig {
  const port = Number(env["ALASIO_HARNESS_PORT"]?.trim() || HARNESS_PORT);
  if (!Number.isInteger(port) || port < 0 || port > 65_535) throw new Error(`ALASIO_HARNESS_PORT must be a port, not ${env["ALASIO_HARNESS_PORT"]}`);
  const token = readFileSync(required(env, "ALASIO_HARNESS_TOKEN_FILE"), "utf8").trim();
  if (!token) throw new Error("ALASIO_HARNESS_TOKEN_FILE names an empty file");
  return {
    conversationId: required(env, "ALASIO_HARNESS_CONVERSATION"),
    port,
    token,
    stateDir: required(env, "ALASIO_STATE_DIR"),
    branch: env["ALASIO_BRANCH"]?.trim() || null,
    lakeWake: wakeLogOf(env),
  };
}

/** What a test gives the harnesses the host makes, in place of the deployment's. */
export type HarnessStandIns = Pick<HarnessesOptions, "overrides" | "claudeQueryFactory" | "folderBayma">;

/**
 * Binds the conversation `config` names, as the module says, and serves its harness host
 * until the scope closes: the port it serves on.
 */
export const startHarness = Effect.fnUntraced(function*(config: HarnessConfig, standIns: HarnessStandIns = {}): Effect.fn.Return<{ readonly port: number }, unknown, Scope.Scope> {
  const wake = config.lakeWake ? yield* LakeWake.make(config.lakeWake) : LakeWake.none;
  const neon = yield* Effect.provideService(HarnessNeon.make, LakeWake, wake);
  const stores = yield* Layer.build(Store.layer({ pool: neon.pool, makesSchema: false }));
  const store = Context.get(stores, Store);
  const home = codexHome();
  const codexRollouts = yield* CodexRollouts.make({ store: neon.rollouts, home });
  const conversation = yield* store.getConversation(config.conversationId);
  const written = yield* codexRollouts.restore(conversation?.codex_session_id ? [conversation.codex_session_id] : []);
  yield* Effect.logInfo(`bound ${config.conversationId}: wrote back ${written.length} Codex rollout file(s)`);

  const host = yield* Layer.build(HarnessHost.layer.pipe(
    Layer.provideMerge(Harnesses.layer({ ...standIns, branch: config.branch, sessionStore: neon.sessionStore, codexRollouts })),
    Layer.provideMerge(Layer.mergeAll(ActiveTurns.layer, CodexAppServer.layer, ReceivedFiles.layer({ stateDir: config.stateDir }))),
    Layer.provideMerge(Layer.succeedContext(stores)),
  ));
  const served = yield* Effect.provide(serveHost({ port: config.port, conversationId: config.conversationId, token: config.token }), host);
  yield* Effect.logInfo(`serving the harness host of ${config.conversationId} on port ${served.port}`);
  return served;
});

/** `alasio harness`, as ./main.ts runs it: the host, from its environment, until it is stopped. */
export const harness: Effect.Effect<never, unknown, Scope.Scope> = Effect.suspend(() => startHarness(loadHarnessConfig())).pipe(Effect.andThen(Effect.never));
