/**
 * How a branch environment's alasio comes by the sessions its conversations inherited,
 * without holding anything of the alasio it was branched from (its parent): no right in
 * the parent's namespaces, no credential of its own to the parent's workspaces.
 *
 * The parent forks them for it. It serves BRANCH_FORK_PORT (./names.ts), which its
 * NetworkPolicy opens to branch environments' alasio alone
 * (cli/src/manifests/network-policies.ts): `POST /branches/<branch>/sessions/<volumeId>/fork`,
 * with the bearer forkToken(key, branch), forks its session `volumeId` into the branch's
 * sessions namespace under the same id, suspended (../sandbox/index.ts fork), and
 * answers `{ volumeId }`. It forks only a session it made, or one of its conversations
 * knows, and none while a turn runs in it, as the fork suspends it: 401 without the
 * branch's token, 404 for a session it does not know, 409 while a turn runs in it, 502
 * when the fork failed. A session forked already is answered as forked.
 *
 * The key is the stack's (neon/control/kube-setup.ts), which the parent reads as each
 * request comes, from a Secret that may be made after it started; each branch is given
 * its own token by `alasio branch create`, and can sign no other branch's.
 *
 * The branch asks as it first brings an inherited session up (parentForks), finds it in
 * its own sessions namespace, under the id its conversations already name, so their
 * working directories and harness sessions stay as they are, and records it as a session
 * workspace it made, in its own database.
 */
import { timingSafeEqual } from "node:crypto";
import { readFile } from "node:fs/promises";
import { createServer } from "node:http";

import { NodeHttpServer } from "@effect/platform-node";
import { Effect, Option, Schema, type Scope } from "effect";
import { type HttpServerError, HttpServerRequest, HttpServerResponse } from "effect/http";

import { nameProblem } from "../../neon/control/branches.ts";
import { ActiveTurns } from "../harness/active-turns.ts";
import { hasStatus } from "../kube/client.ts";
import { Store } from "../persistence/store.ts";
import { SessionInheritError, SessionSandboxes } from "../sandbox/index.ts";
import { isValidVolumeId } from "../sandbox/names.ts";
import { withLogScope } from "../shared/log.ts";
import { sessionFsWorkspace } from "../workspace/kind.ts";
import { BRANCH_FORK_PORT, branchSessionsNamespace, forkToken } from "./names.ts";

/** The bearer token a request presents, from its `Authorization` header. */
const BearerToken = Schema.TemplateLiteralParser(["Bearer ", Schema.NonEmptyString]);

const FORK = /^\/branches\/([^/]+)\/sessions\/([^/]+)\/fork$/u;

const answer = (status: number, body: object): HttpServerResponse.HttpServerResponse => HttpServerResponse.jsonUnsafe(body, { status });

/** Whether `presented` is `expected`, compared in constant time. */
function same(presented: string, expected: string): boolean {
  const a = Buffer.from(presented);
  const b = Buffer.from(expected);
  return a.length === b.length && timingSafeEqual(a, b);
}

/** What serveBranchForks is given: its port, and where the key is. */
export interface BranchForksOptions {
  readonly port?: number;
  readonly host?: string;
  readonly keyFile: string;
}

/**
 * Serves forks to branch environments, as the module says, until the scope closes: the
 * port it listens on, once it does.
 */
export const serveBranchForks = Effect.fnUntraced(function*({
  port = BRANCH_FORK_PORT,
  host = "0.0.0.0",
  keyFile,
}: BranchForksOptions): Effect.fn.Return<{ readonly port: number }, HttpServerError.ServeError, Scope.Scope | Store | ActiveTurns | SessionSandboxes> {
  const store = yield* Store;
  const activeTurns = yield* ActiveTurns;
  const sandboxes = yield* SessionSandboxes;

  const handle = Effect.gen(function*() {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const asked = request.method === "POST" ? FORK.exec(request.url) : null;
    if (!asked) return answer(400, { error: "the request is no fork's" });
    const [, branch = "", volumeId = ""] = asked;
    const key = yield* Effect.promise(() => readFile(keyFile, "utf8").then((text) => text.trim(), () => ""));
    const bearer = Schema.decodeUnknownOption(BearerToken)(request.headers["authorization"]);
    if (!key || nameProblem(branch) !== null || Option.isNone(bearer) || !same(bearer.value[1], forkToken(key, branch))) {
      return answer(401, { error: "the request bears no branch's token" });
    }
    if (!isValidVolumeId(volumeId)) return answer(404, { error: `${volumeId} is no session's id` });
    const knowing = yield* store.listWorkspaceConversations(sessionFsWorkspace(volumeId));
    const made = (yield* store.listSessionWorkspaces).some((workspace) => workspace.volumeId === volumeId && workspace.madeAt !== null);
    if (knowing.length === 0 && !made) return answer(404, { error: `alasio has no session ${volumeId}` });
    const busy = yield* Effect.forEach(knowing.filter(({ mounted }) => mounted), ({ conversationId }) => activeTurns.isBusy(conversationId));
    if (busy.some(Boolean)) {
      return answer(409, { error: `a turn runs in the session ${volumeId} now, which a fork would suspend: send again once it ends` });
    }
    yield* sandboxes.volumes.fork(volumeId, volumeId, branchSessionsNamespace(branch)).pipe(
      // Forked already, by an earlier request of the branch's.
      Effect.catchIf((error) => error._tag === "KubeApiError" && hasStatus(409)(error), () => Effect.void),
    );
    yield* Effect.logInfo(`forked session ${volumeId} for the branch ${branch}`);
    return answer(200, { volumeId });
  }).pipe(
    Effect.catch((error) => Effect.logWarning(`a fork for a branch failed: ${error.message}`).pipe(Effect.as(answer(502, { error: error.message })))),
    withLogScope("branch-forks"),
  );

  const server = yield* NodeHttpServer.make(() => createServer(), { port, host });
  yield* server.serve(handle);
  // Listening on a TCP port, its address is an internet one.
  return { port: server.address._tag === "UnixPathAddress" ? port : server.address.port };
});

/** Where a branch environment's parent forks its sessions, and the branch's token to ask with. */
export interface ParentForksOptions {
  /** The parent's fork server, `http://<host>:<port>`. */
  readonly url: string;
  readonly branch: string;
  /** The file holding the branch's token, from its Secret. */
  readonly tokenFile: string;
}

/**
 * A branch environment's `inherit` (../sandbox/index.ts): its parent's session forked
 * into its own sessions as it is first brought up here; false for a session the parent
 * does not know.
 */
export const parentForks = ({ url, branch, tokenFile }: ParentForksOptions) => (volumeId: string): Effect.Effect<boolean, SessionInheritError> =>
  Effect.tryPromise({
    try: async () => {
      const token = (await readFile(tokenFile, "utf8")).trim();
      const response = await fetch(`${url}/branches/${branch}/sessions/${volumeId}/fork`, { method: "POST", headers: { authorization: `Bearer ${token}` } });
      const body: unknown = await response.json().catch(() => null);
      const said = typeof body === "object" && body !== null && "error" in body ? String(body.error) : `it answered ${response.status}`;
      return { status: response.status, said };
    },
    catch: (cause) => new SessionInheritError({ message: `the alasio this branch was branched from could not be asked to fork the session ${volumeId}: ${cause instanceof Error ? cause.message : String(cause)}` }),
  }).pipe(
    Effect.flatMap(({ status, said }) =>
      status === 200
        ? Effect.succeed(true)
        : status === 404
        ? Effect.succeed(false)
        : Effect.fail(new SessionInheritError({ message: `the alasio this branch was branched from did not fork the session ${volumeId}: ${said}` }))
    ),
  );
