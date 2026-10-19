/**
 * bayma, the MCP server alasio itself gives its agents, beside whatever servers the
 * operator has configured for each harness.
 *
 * A folder workspace's bayma is the sandbox gateway's folder instance of the conversation
 * under the harness (../gateway/folders.ts): a pod of the deployment's host profile, the
 * operator's opt-in to agents that work on a machine's own files, which the gateway makes as
 * the instance is bound, on the conversation's state directory, and lets go once it is idle,
 * its REPL sessions saved whole as it stops. alasio binds it ahead of the harness, and the
 * harness reaches it through the gateway, over MCP HTTP, with a caller's token of its key;
 * each adapter turns the server into its own config shape.
 *
 * A bind that fails leaves the turn to run without bayma's tools: the harness is told why, to
 * put before the turn's prompt, and, for a turn, the operator is told in the chat first, with
 * buttons to bind it again now and to restart it (FOLDER_RETRY, FOLDER_RESTART). Three binds
 * failing in a row hold the instance, as the gateway says: its binds are refused at once until
 * the operator asks again, which the chat says once.
 *
 * A bind waits as long as the gateway gives it, as an instance before it saves its REPL
 * sessions, or its image is pulled. One that has waited BIND_TOLD_AFTER_MS says what it waits
 * on in the turn's status, as the gateway answers it (`waiting`), asked again every
 * BIND_ASKED_EVERY_MS, until it ends; it degrades the turn only where it fails.
 */
import { Context, Effect, Fiber, Layer, Option, Ref, Schema } from "effect";

import { type ButtonDraft, keepPanel, sendPanel } from "../operator/panel.ts";
import { Store } from "../persistence/store.ts";
import type { BaymaEndpoint, FolderBindFailed, FolderScope, GatewayError, GatewayWorkspaces } from "../sandbox/gateway-client.ts";
import { folderKey } from "../gateway/names.ts";
import { TelegramClient } from "../telegram/client.ts";

export const BAYMA_SERVER_NAME = "bayma";

/** bayma as an MCP server over HTTP, in Claude Code's MCP config shape. */
export interface BaymaMcpServer extends BaymaEndpoint {
  readonly type: "http";
}

/** One conversation under one harness, whose bayma it is. */
export type HostBaymaScope = FolderScope;

/**
 * A conversation's folder under a harness, as a harness asks for its bayma: the folder, and
 * the chat to tell of a bind that fails, where a turn asks; and, where a turn asks, what says
 * in its status what a long bind waits on, and that it waits no more (null).
 */
export interface FolderBaymaScope extends FolderScope {
  readonly workingDirectory: string;
  readonly chatId?: string | undefined;
  readonly waiting?: ((what: string | null) => Effect.Effect<void>) | undefined;
}

/** How long a bind waits before the turn's status says what it waits on, and how often it is asked again after. */
export const BIND_TOLD_AFTER_MS = 120_000;
export const BIND_ASKED_EVERY_MS = 15_000;

/** A folder workspace's bayma for a turn: its server; or none, the turn running without it, and why, for the harness to put before the turn's prompt. */
export type FolderBaymaOutcome =
  | { readonly server: BaymaMcpServer; readonly notice: null }
  | { readonly server: null; readonly notice: string };

/** The workspace panel's kinds of the buttons a failed bind's notice carries: bind it again now, and restart it. */
export const FOLDER_RETRY = "workspace:folder_retry";
export const FOLDER_RESTART = "workspace:folder_restart";

/**
 * How long a harness waits for bayma as it starts: a conversation's first turn binds its
 * folder instance, whose pod is made, and its REPL sessions restore on their first exec.
 */
export const BAYMA_STARTUP_TIMEOUT_MS = 60_000;

/** What the operator, and the turn, are told of a bind of the folder `folder` that failed as `error` says. */
export function degradedNotice(folder: string, error: FolderBindFailed | GatewayError): string {
  const reason = error._tag === "FolderBindFailed" ? error.reason : error.message;
  const kept = "This turn runs without bayma's REPL tools; your REPL sessions are kept and come back when it starts.";
  return error._tag === "FolderBindFailed" && error.held
    ? `Folder workspace ${folder}: its bayma failed to start three times in a row (${reason}), so alasio tries no more until you ask. ${kept}`
    : `Folder workspace ${folder}: its bayma did not start (${reason}). ${kept}`;
}

/**
 * The folder workspaces' bayma, through the sandbox gateway, for a deployment whose gateway
 * serves folder instances; without one there is no such service, and no folder workspace has
 * a bayma.
 */
export class HostBayma extends Context.Service<HostBayma, {
  /** The bayma MCP server of one conversation under one harness, bound; or none, as the module says. */
  readonly ensure: (scope: FolderBaymaScope) => Effect.Effect<FolderBaymaOutcome>;
  /** Binds it again now, as the operator asks, though it is held: what to tell them. */
  readonly retry: (scope: HostBaymaScope) => Effect.Effect<string>;
  /** Lets the instance go, saved whole as it stops, so the next bind makes it anew: what to tell the operator. */
  readonly restart: (scope: HostBaymaScope) => Effect.Effect<string>;
}>()("alasio/mcp/HostBayma") {
  static readonly layer = (
    folders: GatewayWorkspaces["folders"],
    { toldAfterMs = BIND_TOLD_AFTER_MS, askedEveryMs = BIND_ASKED_EVERY_MS }: { readonly toldAfterMs?: number; readonly askedEveryMs?: number } = {},
  ): Layer.Layer<HostBayma, never, Store | TelegramClient> =>
    Layer.effect(
      HostBayma,
      Effect.gen(function*() {
        const store = yield* Store;
        const telegram = yield* TelegramClient;

        /**
         * `binding`, the bind of `scope`'s folder instance, said in its turn's status, once it has
         * waited `toldAfterMs`, to wait on what the gateway says it waits on, asked again every
         * `askedEveryMs`; and to wait no more as it ends, where it was said to.
         */
        const told = <A, E>(scope: FolderBaymaScope, binding: Effect.Effect<A, E>): Effect.Effect<A, E> => {
          const waiting = scope.waiting;
          if (waiting === undefined) return binding;
          return Effect.gen(function*() {
            const said = yield* Ref.make<string | null>(null);
            const saying = yield* Effect.forkChild(Effect.gen(function*() {
              yield* Effect.sleep(toldAfterMs);
              for (;;) {
                const starting = yield* folders.starting(scope).pipe(Effect.orElseSucceed(() => null));
                const what = `its folder's bayma, ${starting ?? "which is starting"}`;
                if ((yield* Ref.getAndSet(said, what)) !== what) yield* waiting(what);
                yield* Effect.sleep(askedEveryMs);
              }
            }));
            return yield* binding.pipe(Effect.ensuring(Effect.andThen(Fiber.interrupt(saying), Effect.flatMap(Ref.get(said), (was) => (was === null ? Effect.void : waiting(null))))));
          });
        };
        /** The folder instances whose chat was told they are held, until a bind of one goes through. */
        const toldHeld = new Set<string>();
        const server = (scope: HostBaymaScope) => Effect.map(folders.endpoint(scope), (endpoint): BaymaMcpServer => ({ type: "http", ...endpoint }));

        /** Tells the chat `chatId` of the conversation of `scope` the notice, with its buttons. */
        const tell = (scope: FolderBaymaScope, chatId: string, notice: string): Effect.Effect<void> => {
          const buttons: ButtonDraft[] = [
            { text: "Retry now", kind: FOLDER_RETRY, payload: { harness: scope.harness } },
            { text: "Restart folder instance", kind: FOLDER_RESTART, payload: { harness: scope.harness } },
          ];
          return keepPanel(scope.threadKey, { text: notice, keyboard: [buttons] }).pipe(
            Effect.flatMap((panel) => sendPanel(chatId, panel)),
            Effect.provideService(Store, store),
            Effect.provideService(TelegramClient, telegram),
            Effect.catch((error) => Effect.logWarning(`could not tell ${scope.threadKey} its bayma did not start: ${error.message}`)),
          );
        };

        return HostBayma.of({
          ensure: (scope) =>
            Effect.gen(function*() {
              const key = folderKey(scope.harness, scope.threadKey);
              const bound = yield* Effect.result(told(scope, folders.bind(scope)));
              if (bound._tag === "Success") {
                toldHeld.delete(key);
                return { server: yield* server(scope), notice: null };
              }
              const notice = degradedNotice(scope.workingDirectory, bound.failure);
              yield* Effect.logWarning(`the folder instance of ${key} did not start, its turn running without it: ${bound.failure.message}`);
              const held = bound.failure._tag === "FolderBindFailed" && bound.failure.held;
              // Held, the chat is told so once.
              if (scope.chatId !== undefined && !(held && toldHeld.has(key))) {
                if (held) toldHeld.add(key);
                yield* tell(scope, scope.chatId, notice);
              }
              return { server: null, notice };
            }),
          retry: (scope) =>
            folders.bind(scope, { retry: true }).pipe(
              Effect.tap(() => Effect.sync(() => toldHeld.delete(folderKey(scope.harness, scope.threadKey)))),
              Effect.as("Its bayma started: the next message works with its REPL tools."),
              Effect.catch((error) => Effect.succeed(`Its bayma did not start: ${error.message}`)),
            ),
          restart: (scope) =>
            folders.release(scope).pipe(
              Effect.as("Its folder instance is stopped, its REPL sessions saved, and its failed binds forgotten: the next message starts it anew."),
              Effect.catch((error) =>
                Effect.succeed(
                  error.status === 409
                    ? "Its failed binds are forgotten, but it is being started or stopped now, so it was not restarted: ask again in a moment."
                    : `Its folder instance could not be restarted: ${error.message}`,
                )
              ),
              // Its failures forgotten by the gateway before it answers, as a held chat is told so again.
              Effect.tap(() => Effect.sync(() => toldHeld.delete(folderKey(scope.harness, scope.threadKey)))),
            ),
        });
      }),
    );
}

/** A folder workspace's bayma was asked of a deployment that offers no folder workspaces. */
export class NoFolderWorkspaces extends Schema.TaggedError<NoFolderWorkspaces>()("NoFolderWorkspaces", {}) {
  override get message(): string {
    return "this deployment offers no folder workspaces: it has no host profile";
  }
}

/** The bayma a folder workspace's conversation gets under `harness`, as HostBayma's ensure gives it. */
export type FolderBayma = (scope: FolderBaymaScope) => Effect.Effect<FolderBaymaOutcome, NoFolderWorkspaces>;

/** The folder bayma of a deployment that offers no folder workspaces. */
export const noFolderBayma: FolderBayma = () => Effect.fail(new NoFolderWorkspaces());

/** The folder bayma of the HostBayma alasio runs with, or noFolderBayma where the deployment has none. */
export const folderBayma: Effect.Effect<FolderBayma> = Effect.map(
  Effect.serviceOption(HostBayma),
  Option.match({ onNone: () => noFolderBayma, onSome: (hostBayma) => hostBayma.ensure }),
);
