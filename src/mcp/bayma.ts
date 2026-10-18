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
 */
import { Context, Effect, Layer, Option, Schema } from "effect";

import type { BaymaEndpoint, FolderScope, GatewayError, GatewayWorkspaces } from "../sandbox/gateway-client.ts";

export const BAYMA_SERVER_NAME = "bayma";

/** bayma as an MCP server over HTTP, in Claude Code's MCP config shape. */
export interface BaymaMcpServer extends BaymaEndpoint {
  readonly type: "http";
}

/** One conversation under one harness, whose bayma it is. */
export type HostBaymaScope = FolderScope;

/**
 * How long a harness waits for bayma as it starts: a conversation's first turn binds its
 * folder instance, whose pod is made, and its REPL sessions restore on their first exec.
 */
export const BAYMA_STARTUP_TIMEOUT_MS = 60_000;

/**
 * The folder workspaces' bayma, through the sandbox gateway, for a deployment whose gateway
 * serves folder instances; without one there is no such service, and no folder workspace has
 * a bayma.
 */
export class HostBayma extends Context.Service<HostBayma, {
  /** The bayma MCP server of one conversation under one harness, bound. */
  readonly ensure: (scope: HostBaymaScope) => Effect.Effect<BaymaMcpServer, GatewayError>;
  /** Lets the instance go, saved whole as it stops, so the next bind makes it anew. */
  readonly release: (scope: HostBaymaScope) => Effect.Effect<void, GatewayError>;
}>()("alasio/mcp/HostBayma") {
  static readonly layer = (folders: GatewayWorkspaces["folders"]): Layer.Layer<HostBayma> =>
    Layer.succeed(HostBayma, HostBayma.of({
      ensure: (scope) => Effect.andThen(folders.bind(scope), Effect.map(folders.endpoint(scope), (endpoint) => ({ type: "http" as const, ...endpoint }))),
      release: folders.release,
    }));
}

/** A folder workspace's bayma was asked of a deployment that offers no folder workspaces. */
export class NoFolderWorkspaces extends Schema.TaggedError<NoFolderWorkspaces>()("NoFolderWorkspaces", {}) {
  override get message(): string {
    return "this deployment offers no folder workspaces: it has no host profile";
  }
}

/**
 * The bayma MCP server a folder workspace's conversation gets under `harness`, bound:
 * `{ type: "http", url, headers }`, in Claude Code's MCP config shape.
 */
export type FolderBayma = (scope: HostBaymaScope) => Effect.Effect<BaymaMcpServer, GatewayError | NoFolderWorkspaces>;

/** The folder bayma of a deployment that offers no folder workspaces. */
export const noFolderBayma: FolderBayma = () => Effect.fail(new NoFolderWorkspaces());

/** The folder bayma of the HostBayma alasio runs with, or noFolderBayma where the deployment has none. */
export const folderBayma: Effect.Effect<FolderBayma> = Effect.map(
  Effect.serviceOption(HostBayma),
  Option.match({ onNone: () => noFolderBayma, onSome: (hostBayma) => hostBayma.ensure }),
);
