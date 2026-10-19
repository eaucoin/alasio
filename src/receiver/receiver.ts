/**
 * The session telemetry receiver, a component of its own: what bayma inside a sandbox
 * instance exports to, with its binding's token (../binder/tokens.ts) as the bearer, which
 * is checked by the key it was signed with and against the bindings there are now
 * (../binder/live.ts), of the environments there are (../gateway/environments.ts), so a
 * binding released stops its instance's exports at once, and one of a namespace of no
 * environment's is none. Each
 * request is stamped with the workspace the binding is of, held to its workspace's rate,
 * and answered only once the collector has taken it (../sandbox/telemetry-receiver.ts).
 */
import { Cause, Clock, Config, Effect, FiberSet, Layer, Redacted, type Scope } from "effect";
import type { HttpServerError } from "effect/http";

import { type BindingNamespaces, LiveBindings } from "../binder/live.ts";
import { verifyToken } from "../binder/tokens.ts";
import { makeEnvironments, type SessionsEnvironment } from "../gateway/environments.ts";
import { BINDING_KEY_VARIABLE, folderOfKey, FOLDERS_NAMESPACE_VARIABLE, RECEIVER_PORT_VARIABLE, SESSIONS_NAMESPACE_VARIABLE } from "../gateway/names.ts";
import { KubeClient } from "../kube/client.ts";
import { volumeIdOfKey } from "../sandbox/names.ts";
import { folderResource, sandboxResource } from "../sandbox/telemetry.ts";
import { type Authenticated, AuthenticationUnknown, serveTelemetryReceiver } from "../sandbox/telemetry-receiver.ts";
import type { ResourceStamp } from "../sandbox/otlp-resource.ts";
import { withLogScope } from "../shared/log.ts";
import { createOtlpForwarder } from "../telemetry/forward.ts";

/**
 * What a session's telemetry is stamped with: its sandbox's, and, of a branch environment's
 * session, the branch, as the branch's own telemetry is tagged.
 */
export const sessionResource = (volumeId: string, environment: SessionsEnvironment | null, env: Readonly<NodeJS.ProcessEnv> = process.env): ResourceStamp => ({
  ...sandboxResource(volumeId, env),
  ...(environment && environment.name !== MAIN ? { "alasio.branch": environment.name } : {}),
});

/** main's environment's name, as the gateway's callers name it. */
const MAIN = "main";

/**
 * The session whose binding's token `token` is, signed with `bindingKey`, where its binding
 * is held now: a session workspace's, stamped as `stampFor` says of its volume id and its
 * binding's namespace, its rate its own, by both; or a folder conversation's instance's,
 * stamped as folderResource says, its rate its key's; null where it is no binding's, or its binding is
 * gone; AuthenticationUnknown where the bindings are not known now.
 */
export const authenticateBinding = (bindingKey: Buffer, stampFor: (volumeId: string, namespace: string) => Effect.Effect<ResourceStamp>, env: Readonly<NodeJS.ProcessEnv> = process.env) =>
(token: string): Effect.Effect<Authenticated | null, AuthenticationUnknown, LiveBindings> =>
  Effect.gen(function*() {
    const claims = verifyToken(bindingKey, token, yield* Clock.currentTimeMillis);
    if (!claims?.binding || !claims.namespace) return null;
    const volumeId = volumeIdOfKey(claims.key);
    const folder = folderOfKey(claims.key);
    if (!volumeId && !folder) return null;
    const held = yield* (yield* LiveBindings).holds(claims.key, claims.binding, claims.namespace);
    if (held === "unknown") return yield* new AuthenticationUnknown();
    if (held !== "held") return null;
    // Its rate its own by its binding's namespace too, as environments share volume ids.
    const session = `${claims.namespace}/${volumeId ?? claims.key}`;
    return volumeId ? { session, stamp: yield* stampFor(volumeId, claims.namespace) } : { session, stamp: folderResource(folder?.harness ?? "", folder?.conversation ?? "", env) };
  });

/**
 * The receiver as its environment configures it: its port (RECEIVER_PORT_VARIABLE), the key
 * bindings' tokens are signed with (BINDING_KEY_VARIABLE), main's sessions' namespace
 * (SESSIONS_NAMESPACE_VARIABLE), beside which it takes the tokens of the bindings of every
 * branch environment's, and where it exports,
 * as alasio's own telemetry is configured (../telemetry/forward.ts). What stops it is logged.
 */
export const receiver: Effect.Effect<void, Config.ConfigError | HttpServerError.ServeError, Scope.Scope> = Effect.gen(function*() {
  const port = yield* Config.Int(RECEIVER_PORT_VARIABLE);
  const bindingKey = Buffer.from(Redacted.value(yield* Config.Redacted(BINDING_KEY_VARIABLE)));
  const main = { name: MAIN, namespace: yield* Config.String(SESSIONS_NAMESPACE_VARIABLE) };
  // Folder conversations' instances', where the installation has folder workspaces.
  const folders = (yield* Config.String(FOLDERS_NAMESPACE_VARIABLE).pipe(Config.withDefault(""))) || null;
  // The forwarder's warnings, which it gives as it is made, logged here.
  const runFork = yield* FiberSet.makeRuntime();
  const forwarder = yield* Effect.acquireRelease(
    Effect.sync(() => createOtlpForwarder(process.env, { warn: (message) => void runFork(Effect.logWarning(message)) })),
    (forwarder) => Effect.sync(() => forwarder.close()),
  );
  const kube = yield* Layer.build(KubeClient.layer);
  const environments = yield* Effect.provide(makeEnvironments({ main }), kube);
  const namespaces: BindingNamespaces = {
    current: Effect.map(environments.current, (current) => [...current.map(({ namespace }) => namespace), ...(folders ? [folders] : [])]),
    known: (namespace) => (namespace === folders ? Effect.succeed(true) : Effect.map(environments.ofNamespace(namespace), (environment) => environment !== null)),
  };
  const live = yield* Layer.build(Layer.provide(LiveBindings.layer({ namespaces }), Layer.succeedContext(kube)));
  const authenticate = authenticateBinding(bindingKey, (volumeId, namespace) => Effect.map(environments.ofNamespace(namespace), (environment) => sessionResource(volumeId, environment)));
  const served = yield* serveTelemetryReceiver({
    port,
    forwarder,
    authenticate: (token) => Effect.provide(authenticate(token), live),
  });
  yield* Effect.logInfo(`the session telemetry receiver serves on port ${served.port}, for the bindings of ${(yield* namespaces.current).join(", ")}`);
}).pipe(
  Effect.tapCause((cause) => Effect.logError(`Fatal error: ${Cause.pretty(cause)}`)),
  withLogScope("receiver"),
);
