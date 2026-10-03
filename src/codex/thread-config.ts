/**
 * The config overrides alasio sends with every Codex thread it starts or resumes.
 *
 * Codex merges these over `$CODEX_HOME/config.toml`, so the operator's own MCP
 * servers and the ChatGPT apps connector stay available and bayma is added
 * alongside them.
 */
import { withReplyInstructions } from "../harness/reply-instructions.ts";
import type { BaymaEndpoint } from "../kube/sandboxes.ts";
import { BAYMA_SERVER_NAME, BAYMA_STARTUP_TIMEOUT_MS } from "../mcp/bayma.ts";
import { operatorDeveloperInstructions } from "./config-toml.ts";
import { type CodexEnv, codexHome } from "./env.ts";

// These two are types rather than interfaces so they are JSON objects to TypeScript, as
// the app-server's and the Codex SDK's config types take them.

/** An MCP server in Codex's config shape. */
export type CodexMcpServer = {
  readonly url: string;
  readonly http_headers: Readonly<Record<string, string>>;
  readonly startup_timeout_sec: number;
};

/** The config overrides of a Codex thread. */
export type CodexThreadConfig = {
  readonly project_doc_max_bytes?: number;
  readonly developer_instructions: string;
  readonly mcp_servers: Readonly<Record<string, CodexMcpServer>>;
};

/** A bayma server (../mcp/bayma.ts HostBayma) in Codex's MCP config shape. */
export function codexMcpServer(server: BaymaEndpoint): CodexMcpServer {
  return { url: server.url, http_headers: server.headers, startup_timeout_sec: BAYMA_STARTUP_TIMEOUT_MS / 1000 };
}

/** The overrides for a folder workspace's thread, whose bayma server is `bayma`. */
export function buildCodexThreadConfig({ codexEnv, bayma }: { readonly codexEnv: CodexEnv; readonly bayma: BaymaEndpoint }): CodexThreadConfig {
  return {
    project_doc_max_bytes: 32768,
    // An override replaces the config's own, so the operator's are kept, then alasio's.
    developer_instructions: withReplyInstructions(operatorDeveloperInstructions(codexHome(codexEnv))),
    mcp_servers: {
      [BAYMA_SERVER_NAME]: codexMcpServer(bayma),
    },
  };
}
