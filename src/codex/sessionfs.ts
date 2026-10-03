/**
 * Codex for session-filesystem workspaces: one app-server for all of them, run by alasio
 * outside every sandbox, whose only way into a workspace is that workspace's bayma.
 *
 * Its Codex home is alasio's own (under the state directory) and written here, so none of
 * the operator's Codex configuration reaches an isolated workspace:
 *
 * - `environments.toml` turns the local environment off and names no other, so the
 *   app-server has no environment at all and registers no shell, apply_patch, or
 *   view_image: the agent cannot touch the machine alasio runs on, by construction. (A
 *   thread started with `environments: []` would lose them too, but thread resume and
 *   fork carry no such field, so a resumed thread would get them back.)
 * - `config.toml` makes the login relay (./login-relay.ts) the one model provider, so the
 *   operator's login is used without being copied here.
 *
 * Per thread, sent with every start, resume, fork, and turn (Codex merges it over
 * config.toml): the workspace's bayma, at its Sandbox with its token, as the `bayma` MCP
 * server, and alasio's instructions. A thread runs in the workspace's harness directory
 * in alasio, which keeps each workspace's thread list apart.
 */
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";

import { Context, Effect, Layer, RcRef, type Scope } from "effect";

import { getCodexTransportMode } from "../config.ts";
import { SESSION_FS_AGENT_INSTRUCTIONS } from "../harness/workspace-instructions.ts";
import type { BaymaEndpoint } from "../kube/sandboxes.ts";
import { makeAppServer } from "./app-server/client.ts";
import { type SpawnAppServer, spawnAppServer } from "./app-server/process.ts";
import { type CodexEnv, codexHome } from "./env.ts";
import { type StartLoginRelay, startLoginRelay } from "./login-relay.ts";
import { type CodexScope, CodexScopeError } from "./runtime.ts";
import type { CodexListingScope } from "./sessions.ts";
import type { CodexThreadConfig } from "./thread-config.ts";
import { CodexTransportRefused } from "./transport.ts";

/** The env var the relay's bearer is in, the model provider's `env_key`. */
const LOGIN_BEARER_ENV = "ALASIO_CODEX_LOGIN";
const PROVIDER = "alasio_login_relay";

const toml = (value: string): string => JSON.stringify(value);

/** Where the session-filesystem Codex keeps its home, under alasio's state directory. */
export function sessionFsCodexHome(stateDir: string): string {
  return join(stateDir, "sessionfs", "codex");
}

/** The `config.toml` of that home: the relay at `relayUrl` as the one model provider. */
export function sessionFsCodexConfigToml(relayUrl: string): string {
  return [
    `model_provider = ${toml(PROVIDER)}`,
    "",
    `[model_providers.${PROVIDER}]`,
    `name = "the operator's Codex login, through alasio"`,
    `base_url = ${toml(relayUrl)}`,
    `env_key = ${toml(LOGIN_BEARER_ENV)}`,
    `wire_api = "responses"`,
    "",
  ].join("\n");
}

/** The `environments.toml` of that home: no environment at all. */
export const SESSION_FS_ENVIRONMENTS_TOML = "include_local = false\n";

/** A thread's config in the workspace reached through `bayma` (`{ url, headers }`). */
export function sessionFsThreadConfig(bayma: BaymaEndpoint): CodexThreadConfig {
  return {
    developer_instructions: SESSION_FS_AGENT_INSTRUCTIONS,
    mcp_servers: {
      bayma: { url: bayma.url, http_headers: bayma.headers, startup_timeout_sec: 60 },
    },
  };
}

export interface SessionFsCodexOptions {
  readonly home: string;
  /** The operator's Codex login; their Codex home's `auth.json` when absent. */
  readonly authFile?: string | undefined;
  /** Starts the login relay; ./login-relay.ts's when absent. */
  readonly startRelay?: StartLoginRelay | undefined;
  /** Starts the app-server process; the local codex binary's when absent. */
  readonly spawnProcess?: SpawnAppServer | undefined;
}

/** What a session filesystem's call in `directory` runs against, its workspace reached through `bayma`. */
export interface SessionFsScopeParams {
  readonly directory: string;
  readonly bayma: BaymaEndpoint;
}

/**
 * Codex for session-filesystem workspaces, on when the deployment offers them: one
 * app-server, in a Codex home of alasio's own, for all of them.
 */
export class SessionFsCodex extends Context.Service<SessionFsCodex, {
  /** Its Codex home. */
  readonly home: string;
  /** The scope a call in the workspace reached through `bayma` runs against, in `directory`. */
  readonly scope: (params: SessionFsScopeParams) => Effect.Effect<CodexScope, CodexScopeError | CodexTransportRefused>;
  /** The scope a thread list, a model list, or a goal read runs against: no workspace needed. */
  readonly listingScope: (params: { readonly directory: string }) => Effect.Effect<CodexListingScope, CodexScopeError>;
  /** Stops its app-server, if one runs; the next call starts another. */
  readonly stop: Effect.Effect<void>;
}>()("alasio/codex/SessionFsCodex") {
  /** The session-filesystem Codex whose home is `home`, stopped with the layer. */
  static readonly layer = (options: SessionFsCodexOptions): Layer.Layer<SessionFsCodex> => Layer.effect(SessionFsCodex, makeSessionFsCodex(options));
}

/**
 * The session-filesystem Codex: `home` is its Codex home, `authFile` the operator's
 * login. The relay and the home's files are made by the first call that needs them (and
 * again by the next, if that fails), and kept until the scope closes; the app-server
 * runs in the home, not in the directory of whichever workspace asked first.
 */
export const makeSessionFsCodex = Effect.fnUntraced(function*({
  home,
  authFile = join(codexHome(), "auth.json"),
  startRelay = startLoginRelay,
  spawnProcess = spawnAppServer,
}: SessionFsCodexOptions): Effect.fn.Return<SessionFsCodex["Service"], never, Scope.Scope> {
  // The app-server's HOME too, so nothing of the operator's home (skills under
  // ~/.agents, say) is found through it.
  const homeDir = join(home, "home");
  const appServer = yield* makeAppServer({ spawn: (options) => spawnProcess({ ...options, cwd: home }) });
  // The app-server's environment, with the bearer of the relay started for it.
  const started = yield* RcRef.make({
    acquire: Effect.gen(function*() {
      const relay = yield* startRelay({ authFile });
      yield* Effect.try({
        try: () => {
          mkdirSync(homeDir, { recursive: true });
          writeFileSync(join(home, "config.toml"), sessionFsCodexConfigToml(relay.url));
          writeFileSync(join(home, "environments.toml"), SESSION_FS_ENVIRONMENTS_TOML);
        },
        catch: (cause) => new CodexScopeError({ cause }),
      });
      const env: CodexEnv = { PATH: process.env["PATH"] ?? "", HOME: homeDir, CODEX_HOME: home, [LOGIN_BEARER_ENV]: relay.bearer };
      return env;
    }).pipe(Effect.catchTag("LoginRelayError", (cause) => Effect.fail(new CodexScopeError({ cause })))),
    idleTimeToLive: "Infinity",
  });
  const start = Effect.scoped(RcRef.get(started));

  return SessionFsCodex.of({
    home,
    scope: ({ directory, bayma }) =>
      getCodexTransportMode() === "exec"
        ? Effect.fail(new CodexTransportRefused({ message: "session-filesystem workspaces run Codex through its app-server; unset ALASIO_CODEX_TRANSPORT=exec" }))
        : Effect.map(start, (codexEnv) => ({ cwd: directory, codexEnv, codexConfig: sessionFsThreadConfig(bayma), appServer })),
    listingScope: ({ directory }) => Effect.map(start, (codexEnv) => ({ cwd: directory, codexEnv, appServer })),
    stop: appServer.stop,
  });
});
