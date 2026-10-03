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
import { SESSION_FS_AGENT_INSTRUCTIONS } from "../harness/workspace-instructions.ts";
import type { BaymaEndpoint } from "../kube/sandboxes.ts";
import { AppServerClient } from "./app-server/client.ts";
import { type SpawnAppServer, startAppServerProcess } from "./app-server/process.ts";
import { type CodexEnv, codexHome } from "./env.ts";
import { type LoginRelay, startLoginRelay } from "./login-relay.ts";
import type { CodexScope } from "./runtime.ts";
import type { CodexListingScope } from "./sessions.ts";
import type { CodexThreadConfig } from "./thread-config.ts";
import { getCodexTransportMode } from "../config.ts";

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
  readonly authFile?: string;
  readonly startRelay?: typeof startLoginRelay;
  readonly spawnProcess?: SpawnAppServer;
}

/** The session-filesystem Codex, as createSessionFsCodex returns it. */
export interface SessionFsCodex {
  readonly home: string;
  scope(params: { readonly directory: string; readonly bayma: BaymaEndpoint }): Promise<CodexScope>;
  listingScope(params: { readonly directory: string }): Promise<CodexListingScope>;
  stop(): Promise<void>;
}

/** What the first call that needs them starts: the relay, the app-server's env, and its client. */
interface Started {
  readonly relay: LoginRelay;
  readonly env: CodexEnv;
  readonly client: AppServerClient;
}

/**
 * The session-filesystem Codex: `home` is its Codex home, `authFile` the operator's
 * login. The app-server and the relay start with the first call that needs them.
 * Returns `{ scope({ directory, bayma }), listingScope({ directory }), stop() }`, where a
 * scope is what ./runtime.ts runs a call against.
 */
export function createSessionFsCodex({
  home,
  authFile = join(codexHome(), "auth.json"),
  startRelay = startLoginRelay,
  spawnProcess = startAppServerProcess,
}: SessionFsCodexOptions): SessionFsCodex {
  // The app-server's HOME too, so nothing of the operator's home (skills under
  // ~/.agents, say) is found through it.
  const homeDir = join(home, "home");
  let started: Promise<Started> | null = null;

  function start(): Promise<Started> {
    started ??= (async () => {
      const relay = await startRelay({ authFile });
      mkdirSync(homeDir, { recursive: true });
      writeFileSync(join(home, "config.toml"), sessionFsCodexConfigToml(relay.url));
      writeFileSync(join(home, "environments.toml"), SESSION_FS_ENVIRONMENTS_TOML);
      const env = { PATH: process.env["PATH"] ?? "", HOME: homeDir, CODEX_HOME: home, [LOGIN_BEARER_ENV]: relay.bearer };
      // The process runs in the home, not in the first workspace directory that asks.
      const client = new AppServerClient({ spawnProcess: (options) => spawnProcess({ ...options, cwd: home }) });
      return { relay, env, client };
    })();
    started.catch(() => { started = null; });
    return started;
  }

  return {
    home,

    /** The scope a call in the workspace reached through `bayma` runs against, in `directory`. */
    async scope({ directory, bayma }) {
      if (getCodexTransportMode() === "exec") {
        throw new Error("session-filesystem workspaces run Codex through its app-server; unset ALASIO_CODEX_TRANSPORT=exec");
      }
      const { env, client } = await start();
      return { cwd: directory, codexEnv: env, codexConfig: sessionFsThreadConfig(bayma), client };
    },

    /** The scope a thread list, a model list, or a goal read runs against: no workspace needed. */
    async listingScope({ directory }) {
      const { env, client } = await start();
      return { cwd: directory, codexEnv: env, client };
    },

    async stop() {
      if (!started) return;
      const { relay, client } = await started.catch((): Partial<Started> => ({}));
      started = null;
      client?.stop();
      await relay?.close();
    },
  };
}
