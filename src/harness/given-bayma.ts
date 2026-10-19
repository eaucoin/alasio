/**
 * A harness pool's pod's bayma, as its turns give it. The pod has no key to reach the
 * sandbox gateway by: the control plane brings each turn's workspace's bayma up, a session's
 * or a folder instance's, and gives the host its endpoint in the turn's start, a caller's token
 * of the workspace's key its bearer, held to the pod's binding, good for as long as the pod is
 * bound (TurnStart.bayma): what the harness leaves going on reaches it with no turn after.
 *
 * Its harness's process is told its bayma once, as it starts: it reaches it at the pod's
 * loopback (../mcp/bayma-proxy.ts), as a harness in alasio's process does, which passes each
 * request on with the bearer the latest turn gave its workspace, its configuration never
 * changing as its conversation's turns switch workspaces.
 *
 * The session filesystems are the gateway's, as alasio's are (../sandbox/index.ts), each
 * workspace reached by the token its latest turn gave (../sandbox/gateway-client.ts
 * GivenTokens): a file a prompt names is written into it, and one a reply shows read from it,
 * through the gateway; its harness reaches its bayma at the loopback. A folder workspace's
 * bayma is the loopback too, or none, as the turn said, and why. What only alasio does with
 * sessions, making, forking and deleting them, no pod is asked.
 */
import { Effect, Option, Schema, type Scope } from "effect";

import { folderKey } from "../gateway/names.ts";
import type { FolderBayma } from "../mcp/bayma.ts";
import { makeBaymaProxy } from "../mcp/bayma-proxy.ts";
import { type BaymaEndpoint, GatewayError, makeGatewayWorkspaces } from "../sandbox/gateway-client.ts";
import { makeSessionSandboxes, type SessionSandboxes } from "../sandbox/index.ts";
import { sessionKey } from "../sandbox/names.ts";
import { BearerToken } from "../shared/tokens.ts";
import { parseWorkspace } from "../workspace/kind.ts";
import type { TurnStart } from "./host.ts";

/** What a pod's bayma is made with: the gateway's address, the pod's environment, and its state directory. */
export interface GivenBaymaOptions {
  readonly gatewayUrl: string;
  readonly environment: string;
  readonly stateDir: string;
}

/** A pod's bayma, as its turns give it; see the module. */
export interface GivenBayma {
  /** Takes what the turn `start` gives of its workspace's bayma: the loopback pointed at its endpoint, its bearer kept as its workspace's. */
  readonly give: (start: TurnStart) => Effect.Effect<void>;
  /** The session filesystems, as the turns give them. */
  readonly sessions: SessionSandboxes["Service"];
  /** A folder workspace's bayma, as the turns give it. */
  readonly folderBayma: FolderBayma;
}

/** What a pod's folder workspace's turn is told before any turn has said where its bayma is. */
const NOT_GIVEN = "No turn has said where this conversation's bayma is yet, so this turn runs without bayma's REPL tools.";

/** A pod's bayma, as the module says, until the scope closes. */
export const makeGivenBayma = Effect.fnUntraced(function*({ gatewayUrl, environment, stateDir }: GivenBaymaOptions): Effect.fn.Return<GivenBayma, never, Scope.Scope> {
  const proxy = yield* makeBaymaProxy;
  /** The latest endpoint a turn gave, by its workspace's key. */
  const given = new Map<string, BaymaEndpoint>();
  /** Why the latest turn of a folder workspace runs without its bayma; null where it has it. */
  let notice: string | null = NOT_GIVEN;
  /** The loopback's endpoint of the workspace `key`, each request passed on with the bearer the latest turn gave it. */
  const reach = (key: string) => proxy.reach(key, Effect.sync(() => given.get(key) ?? { url: gatewayUrl, headers: {} }));
  const workspaces = makeGatewayWorkspaces({
    url: gatewayUrl,
    given: (key) => {
      const bearer = Schema.decodeUnknownOption(BearerToken)(given.get(key)?.headers["Authorization"]);
      return Option.isSome(bearer) ? Effect.succeed(bearer.value[1]) : Effect.fail(new GatewayError({ call: `reach ${key}`, reason: "no turn has given its token" }));
    },
  });
  const sessions = makeSessionSandboxes({ workspaces, environment, stateDir, netModeOf: () => Effect.succeed(null), proxy });
  const alasios = <A>(what: string): Effect.Effect<A> => Effect.die(`a harness pool's pod ${what}: alasio does`);

  return {
    give: (start) =>
      Effect.sync(() => {
        if (start.bayma === null) return;
        if (start.bayma.endpoint === null) {
          notice = start.bayma.notice;
          return;
        }
        notice = null;
        const workspace = parseWorkspace(start.workingDirectory);
        given.set(workspace?.kind === "sessionfs" ? sessionKey(workspace.volumeId) : folderKey(start.harness, start.conversationId), start.bayma.endpoint);
      }),
    sessions: {
      ...sessions,
      volumes: {
        create: () => alasios("makes no session"),
        fork: () => alasios("forks no session"),
        destroy: () => alasios("deletes no session"),
        list: alasios("lists no session"),
      },
      // Brought up by alasio for the turn, and reached at the loopback.
      ensureSession: (volumeId) => Effect.map(reach(sessionKey(volumeId)), (bayma) => ({ bayma })),
    },
    folderBayma: (scope) =>
      notice === null
        ? Effect.map(reach(folderKey(scope.harness, scope.threadKey)), (endpoint) => ({ server: { type: "http" as const, ...endpoint }, notice: null }))
        : Effect.succeed({ server: null, notice }),
  };
});
