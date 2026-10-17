/**
 * The session telemetry receiver, a component of its own: what bayma inside a sandbox
 * instance exports to, with its binding's token (../binder/tokens.ts) as the bearer, which
 * is checked by the key it was signed with and against the bindings there are now
 * (../binder/live.ts), so a binding released stops its instance's exports at once. Each
 * request is stamped with the workspace the binding is of, held to its workspace's rate,
 * and answered only once the collector has taken it (../sandbox/telemetry-receiver.ts).
 */
import { Cause, Clock, Config, Effect, FiberSet, Layer, Redacted, type Scope } from "effect";
import type { HttpServerError } from "effect/http";

import { LiveBindings } from "../binder/live.ts";
import { verifyToken } from "../binder/tokens.ts";
import { KubeClient } from "../kube/client.ts";
import { volumeIdOfKey } from "../sandbox/names.ts";
import { sandboxResource } from "../sandbox/telemetry.ts";
import { AuthenticationUnknown, serveTelemetryReceiver } from "../sandbox/telemetry-receiver.ts";
import { withLogScope } from "../shared/log.ts";
import { createOtlpForwarder } from "../telemetry/forward.ts";

/** The variable of the port the receiver serves on. */
export const RECEIVER_PORT_VARIABLE = "RECEIVER_PORT";
/** The variable of the key bindings' tokens are signed with: its text, as the stack's setup makes it. */
export const BINDING_KEY_VARIABLE = "BINDING_KEY";
/** The variable of the namespaces whose bindings the receiver takes the tokens of, comma-separated. */
export const BINDING_NAMESPACES_VARIABLE = "BINDING_NAMESPACES";

/**
 * The volume id of the session whose binding's token `token` is, signed with `bindingKey`,
 * where its binding is held now; null where it is no binding's, or its binding is gone;
 * AuthenticationUnknown where the bindings are not known now.
 */
export const authenticateBinding = (bindingKey: Buffer) => (token: string): Effect.Effect<string | null, AuthenticationUnknown, LiveBindings> =>
  Effect.gen(function*() {
    const claims = verifyToken(bindingKey, token, yield* Clock.currentTimeMillis);
    const volumeId = claims?.binding === undefined ? null : volumeIdOfKey(claims.key);
    if (!claims?.binding || !volumeId) return null;
    const held = yield* (yield* LiveBindings).holds(claims.key, claims.binding);
    if (held === "unknown") return yield* new AuthenticationUnknown();
    return held === "held" ? volumeId : null;
  });

/**
 * The receiver as its environment configures it: its port (RECEIVER_PORT_VARIABLE), the key
 * bindings' tokens are signed with (BINDING_KEY_VARIABLE), the namespaces whose bindings it
 * takes the tokens of (BINDING_NAMESPACES_VARIABLE, comma-separated), and where it exports,
 * as alasio's own telemetry is configured (../telemetry/forward.ts). What stops it is logged.
 */
export const receiver: Effect.Effect<void, Config.ConfigError | HttpServerError.ServeError, Scope.Scope> = Effect.gen(function*() {
  const port = yield* Config.Int(RECEIVER_PORT_VARIABLE);
  const bindingKey = Buffer.from(Redacted.value(yield* Config.Redacted(BINDING_KEY_VARIABLE)));
  const namespaces = (yield* Config.String(BINDING_NAMESPACES_VARIABLE)).split(",").filter(Boolean);
  // The forwarder's warnings, which it gives as it is made, logged here.
  const runFork = yield* FiberSet.makeRuntime();
  const forwarder = yield* Effect.acquireRelease(
    Effect.sync(() => createOtlpForwarder(process.env, { warn: (message) => void runFork(Effect.logWarning(message)) })),
    (forwarder) => Effect.sync(() => forwarder.close()),
  );
  const live = yield* Layer.build(Layer.provide(LiveBindings.layer({ namespaces }), KubeClient.layer));
  const authenticate = authenticateBinding(bindingKey);
  const served = yield* serveTelemetryReceiver({
    port,
    forwarder,
    stampFor: (volumeId) => sandboxResource(volumeId),
    authenticate: (token) => Effect.provide(authenticate(token), live),
  });
  yield* Effect.logInfo(`the session telemetry receiver serves on port ${served.port}, for the bindings of ${namespaces.join(", ")}`);
}).pipe(
  Effect.tapCause((cause) => Effect.logError(`Fatal error: ${Cause.pretty(cause)}`)),
  withLogScope("receiver"),
);
