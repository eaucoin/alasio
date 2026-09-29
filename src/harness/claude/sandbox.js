/**
 * Running Claude Code inside a session filesystem's sandbox. When a conversation's
 * workspace is a session filesystem, the Claude Code process runs in the gVisor sandbox
 * (via the SDK's spawnClaudeCodeProcess hook, which the SDK offers exactly "to run
 * Claude Code in VMs, containers, or remote environments"), reaching the model only
 * through the session gateway and bayma over the sandbox's own loopback. Proven in
 * session-fs-research E5.
 *
 * The env here is deliberately minimal: it is the whole environment the CLI sees inside
 * the sandbox, so none of alasio's own environment (its secrets) is carried in.
 */

/** The base env the Claude CLI runs with inside the sandbox, written to /run/agent-env. */
export function sandboxClaudeEnv({ bearer, gatewayUrl }) {
  return {
    ANTHROPIC_BASE_URL: gatewayUrl,
    ANTHROPIC_AUTH_TOKEN: bearer,
    CLAUDE_CONFIG_DIR: "/home/agent/.claude",
    CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC: "1",
    DISABLE_AUTOUPDATER: "1",
    DISABLE_TELEMETRY: "1",
    DISABLE_ERROR_REPORTING: "1",
  };
}

/** bayma runs inside the sandbox as an HTTP MCP server on loopback; Claude reaches it there. */
export function sandboxMcpServers(baymaHttpUrl) {
  return { bayma: { type: "http", url: baymaHttpUrl } };
}

/**
 * A `spawnClaudeCodeProcess` that runs `claude` inside the sandbox instead of on the
 * host: the SDK's host binary path is dropped (the agent image's `claude` is on PATH
 * inside), and the SDK's per-spawn env is forwarded in on top of the session's base env.
 */
export function sandboxSpawn(session) {
  return (opts) => session.spawn(["claude", ...(opts.args ?? [])], opts.env ?? {});
}
