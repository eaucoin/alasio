/**
 * Running Codex inside a session filesystem's gVisor sandbox.
 *
 * A folder workspace keeps the shared app-server that spawns the local `codex`
 * binary. A session-filesystem workspace instead gets its own app-server run
 * *inside* the volume's sandbox: the process is spawned through the session
 * host over `docker exec`, its threads work in `/workspace`, its Codex home is
 * on the volume, and its only model provider is the credential gateway, which
 * it reaches with a revocable per-session bearer (never a real credential).
 *
 * This mirrors the Claude adapter's sandbox path (harness/claude/sandbox.js)
 * and follows session-fs-research E5, which drove alasio's own AppServerClient
 * against `codex app-server` inside the sandbox.
 */
import { createInterface } from "node:readline";
import { AppServerClient } from "./app-server/client.js";

/** Where Codex keeps its state inside the sandbox: on the session volume, owned by the agent user. */
export const SANDBOX_CODEX_HOME = "/home/agent/.codex";

/** The env var the gateway provider reads its per-session bearer from (Codex's `env_key`). */
export const SANDBOX_GATEWAY_TOKEN_ENV = "ALASIO_GATEWAY_TOKEN";

/**
 * The env a Codex session runs with inside the sandbox: its home on the volume
 * and the gateway bearer under the provider's `env_key`. No host environment is
 * carried in, so no real credential can leak through it.
 */
export function sandboxCodexEnv({ bearer }) {
  return {
    HOME: "/home/agent",
    CODEX_HOME: SANDBOX_CODEX_HOME,
    [SANDBOX_GATEWAY_TOKEN_ENV]: bearer,
  };
}

/**
 * The Codex thread config for a sandbox session: the one model provider is the
 * gateway (Codex's Responses wire API), reached with the bearer from
 * `SANDBOX_GATEWAY_TOKEN_ENV`, and bayma is the session host's own HTTP server
 * rather than a spawned process. Merged over `$CODEX_HOME/config.toml` like any
 * thread config alasio sends.
 */
export function sandboxCodexConfig({ gatewayUrl, baymaHttpUrl }) {
  return {
    project_doc_max_bytes: 32768,
    model_provider: "gateway",
    model_providers: {
      gateway: {
        name: "gateway",
        base_url: `${gatewayUrl}/v1`,
        env_key: SANDBOX_GATEWAY_TOKEN_ENV,
        wire_api: "responses",
        request_max_retries: 0,
        stream_max_retries: 0,
      },
    },
    mcp_servers: {
      bayma: { url: baymaHttpUrl },
    },
    "features.plugins": false,
  };
}

/**
 * A `spawnProcess` for AppServerRpcClient that runs `codex app-server` inside the
 * session's sandbox over `docker exec`, returning the same `{ child, readline,
 * stop }` shape the local spawner does. The passed `cwd` is the app-server
 * thread's directory (`/workspace`) and does not exist on the host, so it is not
 * used for the local `docker exec` process; the sandbox sets `/workspace`
 * itself.
 */
export function sandboxCodexSpawnProcess(session) {
  return ({ env, onLine, onExit, onError }) => {
    const child = session.spawn(
      ["codex", "app-server", "--disable", "plugins", "--listen", "stdio://"],
      env,
    );
    child.stderr?.on("data", () => {});
    child.on("exit", (code, signal) => onExit(code, signal));
    child.on("error", (error) => onError(error));
    const readline = createInterface({ input: child.stdout, crlfDelay: Infinity });
    readline.on("line", onLine);
    return {
      child,
      readline,
      stop() {
        readline.close();
        child.kill("SIGTERM");
      },
    };
  };
}

/** An app-server client whose process runs inside `session`'s sandbox. */
export function createSandboxCodexClient(session) {
  return new AppServerClient({ spawnProcess: sandboxCodexSpawnProcess(session) });
}
