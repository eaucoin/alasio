/**
 * The command line's calls of the sandbox gateway's front (operate, src/binder/server.ts),
 * through a port-forward to a gateway pod of its own: each call on a connection of its own,
 * closed once answered, so its forward ends with it, with a token of the environment it is
 * of, signed by that environment's key, which stops a minute after it is made.
 */
import type { V1Secret } from "@kubernetes/client-node";
import { request } from "node:http";
import { type AddressInfo, createServer, type Server, type Socket } from "node:net";

import { Clock, Effect, FiberSet, Schema, type Scope } from "effect";

import { mintToken } from "../../src/binder/tokens.ts";
import { branchNamespace, MAIN_ENVIRONMENT_NAME } from "../../src/branch/names.ts";
import { runningPodIn } from "./commands/common.ts";
import { kind, KubeApi, type KubeApiError } from "./kube/api.ts";
import { NAMESPACE, SESSIONS_CALLER_SECRET } from "./manifests/common.ts";
import { GATEWAY_COMPONENT, GATEWAY_PORT } from "./manifests/sandbox-gateway.ts";

/** The gateway could not be reached, or a call of it not made; the reason says why. */
export class GatewayUnreached extends Schema.TaggedError<GatewayUnreached>()("GatewayUnreached", { reason: Schema.String }) {
  override get message(): string {
    return this.reason;
  }
}

/** A call of the gateway's front: of the environment `environment`, its token signed by `callerKey`, its own; for the key `key`, as a workspace's is its session's. */
export interface GatewayCall {
  readonly callerKey: Buffer;
  readonly environment: string;
  readonly key: string;
  readonly method: "GET" | "POST" | "DELETE";
  readonly path: string;
}

/** What the gateway answered: its status, and its body's text. */
export interface GatewayAnswer {
  readonly status: number;
  readonly text: string;
}

/** A server on a free port of the loopback, open while the scope is, each connection forwarded by `forward`. */
const loopback = (forward: (socket: Socket) => void) =>
  Effect.acquireRelease(
    Effect.callback<Server, GatewayUnreached>((resume) => {
      const server = createServer(forward);
      server.once("error", (error) => resume(Effect.fail(new GatewayUnreached({ reason: `no port of this machine could be listened on: ${error.message}` }))));
      server.listen(0, "127.0.0.1", () => resume(Effect.succeed(server)));
    }),
    (server) => Effect.sync(() => server.close()),
  );

/**
 * What calls the gateway while the scope is open, through a forward to one of its pods: a
 * call answered whatever its status, failed only where it could not be made.
 */
export const gatewayCalls: Effect.Effect<
  (call: GatewayCall) => Effect.Effect<GatewayAnswer, GatewayUnreached>,
  GatewayUnreached,
  KubeApi | Scope.Scope
> = Effect.gen(function*() {
  const kube = yield* KubeApi;
  const pod = yield* runningPodIn(NAMESPACE, GATEWAY_COMPONENT).pipe(Effect.mapError((error) => new GatewayUnreached({ reason: error.message })));
  const run = yield* FiberSet.makeRuntime<never>();
  const server = yield* loopback((socket) => {
    socket.on("error", () => socket.destroy());
    run(kube.portForward({ namespace: NAMESPACE, pod }, GATEWAY_PORT, socket).pipe(Effect.catch(() => Effect.sync(() => socket.destroy()))));
  });
  // A server listening on a TCP port has an address of its own.
  const { port } = server.address() as AddressInfo;
  return ({ callerKey, environment, key, method, path }: GatewayCall) =>
    Effect.gen(function*() {
      const token = mintToken(callerKey, { key, expires: (yield* Clock.currentTimeMillis) + 60_000, options: { environment } });
      return yield* Effect.tryPromise({
        try: (signal) =>
          new Promise<GatewayAnswer>((resolve, reject) => {
            const asked = request({ host: "127.0.0.1", port, method, path, headers: { authorization: `Bearer ${token}` }, agent: false, signal }, (response) => {
              const chunks: Buffer[] = [];
              response.on("data", (chunk: Buffer) => chunks.push(chunk));
              response.on("error", reject);
              response.on("end", () => resolve({ status: response.statusCode ?? 0, text: Buffer.concat(chunks).toString("utf8") }));
            });
            asked.on("error", reject);
            asked.end();
          }),
        catch: (error) => new GatewayUnreached({ reason: error instanceof Error ? error.message : String(error) }),
      });
    });
});

/** Calls the gateway as the environment `environment`, main or a branch, for `key`, with the caller key its namespace keeps: what it answered. */
export const callAs = (environment: string, key: string, method: GatewayCall["method"], path: string): Effect.Effect<GatewayAnswer, GatewayUnreached | KubeApiError, KubeApi> =>
  Effect.gen(function*() {
    const kube = yield* KubeApi;
    const namespace = environment === MAIN_ENVIRONMENT_NAME ? NAMESPACE : branchNamespace(environment);
    const secret = yield* kube.get<V1Secret>({ ...kind("Secret"), namespace, name: SESSIONS_CALLER_SECRET });
    const callerKey = secret?.data?.["caller-key"];
    if (!callerKey) return yield* new GatewayUnreached({ reason: `${SESSIONS_CALLER_SECRET} holds no caller key of ${environment}'s` });
    const call = yield* gatewayCalls;
    return yield* call({ callerKey: Buffer.from(callerKey, "base64"), environment, key, method, path });
  }).pipe(Effect.scoped);
