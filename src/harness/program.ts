/**
 * `alasio harness`, a program of alasio's image: the harness host of one conversation,
 * which the control plane reaches over HTTP (./serve.ts) rather than in its process. It
 * reaches alasio's Neon as the harness host's role (../neon/harness.ts), which may write a
 * turn's part of the state and nothing more, and makes nothing there.
 *
 * It starts free, of no conversation, as a pod of a harness pool waits to be bound: it
 * serves nothing until its binding is beside it, the file ALASIO_HARNESS_BINDING_FILE names,
 * which its node's agent writes as the sandbox gateway readies the pod, as the agent gives a
 * sandbox pool's pod's bayma its token (Binding: the conversation, and the token its caller
 * bears). The
 * preparation writes the file whole beside it and renames it into place, so the host reads
 * all of it or none; one it reads as no binding it reads again, as one written in place may
 * be half written, until BINDING_WHOLE_WITHIN has passed. The database it reaches is its
 * pool's, of the environment the pool is of, as its environment says.
 *
 * A pod serves one binding, and only once, as a sandbox pool's pods start bayma once
 * (sandbox/agent/start.sh, its served marker): once bound, the host marks it so in its state
 * directory (`served`), which outlives its container, as a pod's emptyDir does, never in its
 * binding's, which it reads alone; and a host started again in the pod, its container
 * restarted, finds the mark and stops, rather than binding the conversation again with the
 * state its last start lost. The gateway, finding the pod gone or broken, binds the
 * conversation anew in another.
 *
 * As it binds the conversation, before it serves: its Codex home's rollouts are kept in
 * Neon (../codex/rollouts/index.ts), and those the conversation's Codex session needs
 * written back from there, so the session resumes where its last instance left it. Claude
 * Code's transcript is written back as its session is resumed, as alasio's harness writes
 * it back (../harness/claude/sessions.ts sessionExists): a resume runs from the local
 * transcript, the store given it mirroring alone (G14.1 found its own load would serve
 * too). The Codex login is not kept here: one relay keeps it (14.5), as two refreshing it
 * would end it. The files a prompt names are written as each turn starts (./host.ts). What
 * its harness does on its own between prompts it tells on its environment's events topic
 * (./host-events.ts HostEvents.onLog), as Events tells them, which alasio reads.
 *
 * A session workspace's harness needs the sandbox gateway's binding of the workspace,
 * which the harness gateway brings (14.3); until then this host serves folder workspaces,
 * and refuses a session's turn, as alasio without session filesystems does.
 */
import { access, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";

import { Clock, Context, Duration, Effect, Layer, Result, Schema, type Scope } from "effect";

import { MAIN_ENVIRONMENT_NAME } from "../branch/names.ts";
import { CodexAppServer } from "../codex/app-server/client.ts";
import { codexHome } from "../codex/env.ts";
import { CodexRollouts } from "../codex/rollouts/index.ts";
import { Events } from "../events/events.ts";
import { HarnessNeon } from "../neon/connect.ts";
import { LakeWake, type WakeLog, wakeLogOf } from "../neon/lake-wake.ts";
import { Store } from "../persistence/store.ts";
import { ReceivedFiles } from "../telegram/files.ts";
import { ActiveTurns } from "./active-turns.ts";
import { HarnessHost } from "./host.ts";
import { HostEvents } from "./host-events.ts";
import { Harnesses, type HarnessesOptions } from "./index.ts";
import { serveHost } from "./serve.ts";

/** The port the host serves on unless its environment says. */
export const HARNESS_PORT = 8080;

/** How often a free host looks for its binding. */
const BINDING_POLL = "250 millis";
/** How long a binding's file read as no binding is read again, as one written in place may be half written. */
const BINDING_WHOLE_WITHIN = Duration.seconds(10);

/** What a free host is bound with, as its binding's file says: the conversation it is the host of, and the token its caller bears. */
const Binding = Schema.Struct({ conversationId: Schema.NonEmptyString, token: Schema.NonEmptyString });
export type Binding = typeof Binding.Type;

/** A binding's file that says no binding, or could not be read. */
export class BindingUnreadable extends Schema.TaggedError<BindingUnreadable>()("BindingUnreadable", { file: Schema.String, reason: Schema.String }) {
  override get message(): string {
    return `the harness host's binding ${this.file} is no binding: ${this.reason}`;
  }
}

/** A pod whose binding a host has served already, as its mark says, which it binds no more. */
export class BindingServed extends Schema.TaggedError<BindingServed>()("BindingServed", { file: Schema.String }) {
  override get message(): string {
    return `this pod's harness host has served its binding already (${this.file}), and binds no conversation again in it: a pod serves one binding`;
  }
}

/** `file`'s text, or null where it is not there yet. */
const readIfThere = (file: string): Effect.Effect<string | null, BindingUnreadable> =>
  Effect.tryPromise({
    try: () => readFile(file, "utf8").then((text) => text, (error: NodeJS.ErrnoException) => (error.code === "ENOENT" ? null : Promise.reject(error))),
    catch: (cause) => new BindingUnreadable({ file, reason: cause instanceof Error ? cause.message : String(cause) }),
  });

/**
 * The binding the file `file` holds, once it is there whole, as the module says: looked
 * for every BINDING_POLL, and read again while it reads as no binding, until `within` has
 * passed since it first did.
 */
export const awaitBinding = (file: string, within: Duration.Input = BINDING_WHOLE_WITHIN): Effect.Effect<Binding, BindingUnreadable> =>
  Effect.gen(function*() {
    const allowed = Duration.toMillis(Duration.fromInputUnsafe(within));
    let unreadSince: number | null = null;
    for (;;) {
      const text = yield* readIfThere(file);
      if (text !== null) {
        const read = yield* Effect.result(Schema.decodeUnknownEffect(Schema.fromJsonString(Binding))(text));
        if (Result.isSuccess(read)) return read.success;
        const now = yield* Clock.currentTimeMillis;
        unreadSince ??= now;
        if (now - unreadSince >= allowed) return yield* new BindingUnreadable({ file, reason: read.failure.message });
      }
      yield* Effect.sleep(BINDING_POLL);
    }
  });

/** Marks the pod's binding served, in `stateDir`, as the module says; failing where it was already. */
const markServed = (stateDir: string): Effect.Effect<void, BindingServed | BindingUnreadable> => {
  const mark = join(stateDir, "served");
  return Effect.tryPromise({
    try: () => access(mark).then(() => true, (error: NodeJS.ErrnoException) => (error.code === "ENOENT" ? false : Promise.reject(error))),
    catch: (cause) => new BindingUnreadable({ file: mark, reason: cause instanceof Error ? cause.message : String(cause) }),
  }).pipe(
    Effect.flatMap((served): Effect.Effect<void, BindingServed | BindingUnreadable> =>
      served
        ? Effect.fail(new BindingServed({ file: mark }))
        : Effect.tryPromise({ try: () => writeFile(mark, ""), catch: (cause) => new BindingUnreadable({ file: mark, reason: cause instanceof Error ? cause.message : String(cause) }) })
    ),
  );
};

/** What `alasio harness` runs with, as its environment sets it. */
export interface HarnessConfig {
  /** Where its binding is written: `ALASIO_HARNESS_BINDING_FILE`. */
  readonly bindingFile: string;
  /** Where it serves: `ALASIO_HARNESS_PORT`, HARNESS_PORT unless set. */
  readonly port: number;
  /** Where the files a prompt names are written, and its binding marked served: `ALASIO_STATE_DIR`, alasio's, where the prompt names them. */
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
  return {
    bindingFile: required(env, "ALASIO_HARNESS_BINDING_FILE"),
    port,
    stateDir: required(env, "ALASIO_STATE_DIR"),
    branch: env["ALASIO_BRANCH"]?.trim() || null,
    lakeWake: wakeLogOf(env),
  };
}

/** What a test gives the harnesses the host makes, in place of the deployment's. */
export type HarnessStandIns = Pick<HarnessesOptions, "overrides" | "claudeQueryFactory" | "folderBayma">;

/**
 * Waits for its binding, then binds the conversation it names, as the module says, and
 * serves its harness host until the scope closes, telling its events as Events does: the
 * port it serves on.
 */
export const startHarness = Effect.fnUntraced(function*(config: HarnessConfig, standIns: HarnessStandIns = {}): Effect.fn.Return<{ readonly port: number }, unknown, Scope.Scope | Events> {
  yield* Effect.logInfo(`free: serving once bound, as ${config.bindingFile} says`);
  const { conversationId, token } = yield* awaitBinding(config.bindingFile);
  yield* markServed(config.stateDir);
  const wake = config.lakeWake ? yield* LakeWake.make(config.lakeWake) : LakeWake.none;
  const neon = yield* Effect.provideService(HarnessNeon.make, LakeWake, wake);
  const stores = yield* Layer.build(Store.layer({ pool: neon.pool, makesSchema: false }));
  const store = Context.get(stores, Store);
  const home = codexHome();
  const codexRollouts = yield* CodexRollouts.make({ store: neon.rollouts, home });
  const conversation = yield* store.getConversation(conversationId);
  const written = yield* codexRollouts.restore(conversation?.codex_session_id ? [conversation.codex_session_id] : []);
  yield* Effect.logInfo(`bound ${conversationId}: wrote back ${written.length} Codex rollout file(s)`);

  const host = yield* Layer.build(HarnessHost.layer.pipe(
    Layer.provideMerge(Harnesses.layer({ ...standIns, branch: config.branch, sessionStore: neon.sessionStore, codexRollouts })),
    Layer.provideMerge(Layer.mergeAll(ActiveTurns.layer, CodexAppServer.layer, ReceivedFiles.layer({ stateDir: config.stateDir }), HostEvents.onLog(config.branch ?? MAIN_ENVIRONMENT_NAME))),
    Layer.provideMerge(Layer.succeedContext(stores)),
  ));
  const served = yield* Effect.provide(serveHost({ port: config.port, conversationId, token }), host);
  yield* Effect.logInfo(`serving the harness host of ${conversationId} on port ${served.port}`);
  return served;
});

/** `alasio harness`, as ./main.ts runs it: the host, from its environment, until it is stopped. */
export const harness: Effect.Effect<never, unknown, Scope.Scope> = Effect.suspend(() => startHarness(loadHarnessConfig())).pipe(
  Effect.provideServiceEffect(Events, Events.fromEnvironment),
  Effect.andThen(Effect.never),
);
