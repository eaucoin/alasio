/**
 * The sandbox gateway, a program of alasio's image on the floor: the binder's front
 * (../binder/server.ts) for session workspaces (./sessions.ts), as its configuration file
 * (GATEWAY_CONFIG_VARIABLE, which `alasio up` renders) and its keys (CALLER_KEY_VARIABLE,
 * BINDING_KEY_VARIABLE) say, a caller's token checked by the key of the environment it names,
 * of the caller key (../binder/names.ts environmentKey), as its alasio signs it, reaching the node agents with their token, as alasio does, forgetting a route it knew of a
 * binding another replica let go as the bindings' Leases are watched (../binder/live.ts), and
 * telling each environment's alasio of what happens to its sessions' bindings on the log
 * (../events/events.ts).
 * What a gateway that ended left between a binding's steps is set right as it starts, and
 * every RECONCILE_MS after; bound pods are looked at for one to release every SWEEP_MS, and for
 * one ending though the gateway did not end it, released as it ends (./sessions.ts drain); the
 * pools' refusals by the sandbox pods' policy are counted every REFUSALS_MS (./refusals.ts);
 * and what of main's snapshots no workspace has a use for is collected every COLLECT_MS
 * (./snapshots.ts collect).
 * What stops it is logged.
 */
import { lookup } from "node:dns/promises";
import { readFileSync } from "node:fs";

import type { V1PodTemplateSpec } from "@kubernetes/client-node";
import { Cause, Config, Context, Effect, Layer, Redacted, Schedule, Schema, type Scope } from "effect";
import { FetchHttpClient } from "effect/http";

import { Binder } from "../binder/binder.ts";
import { type BindingNamespaces, LiveBindings } from "../binder/live.ts";
import { reconcile } from "../binder/bindings.ts";
import { environmentKey } from "../binder/names.ts";
import { serveBinder } from "../binder/server.ts";
import { Events } from "../events/events.ts";
import { KubeClient } from "../kube/client.ts";
import { makeNodeAgents } from "../sandbox/node-agents.ts";
import { sandboxBaymaTelemetryEnv } from "../sandbox/telemetry.ts";
import { withLogScope } from "../shared/log.ts";
import { makeEnvironments } from "./environments.ts";
import { makeRefusalCount } from "./refusals.ts";
import {
  BINDING_KEY_VARIABLE,
  CALLER_KEY_VARIABLE,
  EVENTS_BROKERS_VARIABLE,
  EVENTS_PASSWORD_FILE_VARIABLE,
  EVENTS_USERNAME_VARIABLE,
  folderOfKey,
  GATEWAY_CONFIG_VARIABLE,
  GatewayConfig,
} from "./names.ts";
import { makeFolders } from "./folders.ts";
import { makeSessions } from "./sessions.ts";

/** How often bound pods are looked at for one to release: each pod's bayma is asked no oftener than its poll (./sessions.ts). */
const SWEEP_MS = 5_000;
/** How often the pools' bindings are set right where a gateway ended between their steps. */
const RECONCILE_MS = 60_000;
/** How often the pools' refusals by the sandbox pods' policy are counted. */
const REFUSALS_MS = 30_000;
/** How often what of main's snapshots no workspace has a use for is collected. */
const COLLECT_MS = 10 * 60_000;

/** The gateway's configuration, read from its file once, as it starts, and checked. */
const loadConfig = Effect.gen(function*() {
  const path = yield* Config.String(GATEWAY_CONFIG_VARIABLE);
  const text = yield* Effect.try({ try: () => readFileSync(path, "utf8"), catch: (error) => new Error(`${GATEWAY_CONFIG_VARIABLE} ${path} cannot be read: ${String(error)}`) });
  return yield* Schema.decodeUnknownEffect(Schema.fromJsonString(GatewayConfig))(text);
});

/** What tells of events, on the log its variables name (./names.ts). */
const eventsOf = Effect.gen(function*() {
  const brokers = yield* Config.String(EVENTS_BROKERS_VARIABLE);
  const username = yield* Config.String(EVENTS_USERNAME_VARIABLE);
  const passwordFile = yield* Config.String(EVENTS_PASSWORD_FILE_VARIABLE);
  return yield* Events.make({ brokers: brokers.split(","), username, passwordFile });
});

/** A key, its text, from the variable `name`. */
const keyFrom = (name: string) => Effect.map(Config.Redacted(name), (key) => Buffer.from(Redacted.value(key)));

/** The gateway, as the module says. */
export const gateway: Effect.Effect<void, unknown, Scope.Scope> = Effect.gen(function*() {
  const config = yield* loadConfig;
  const callerKey = yield* keyFrom(CALLER_KEY_VARIABLE);
  const bindingKey = yield* keyFrom(BINDING_KEY_VARIABLE);
  const context = yield* Layer.build(Layer.mergeAll(Layer.provideMerge(Binder.layer({ waitMs: config.waitSeconds * 1000 }), KubeClient.layer), FetchHttpClient.layer));
  yield* Effect.gen(function*() {
    const binder = yield* Binder;
    const agents = yield* makeNodeAgents(config.nodeAgents);
    const environments = yield* makeEnvironments({ main: config.environment });
    const foldersNamespace = config.folders?.namespace ?? null;
    // The bindings' Leases watched, every environment's and the folder instances', so a route this replica knew of a binding another released is let go.
    const namespaces: BindingNamespaces = {
      current: Effect.map(environments.current, (current) => [...current.map(({ namespace }) => namespace), ...(foldersNamespace ? [foldersNamespace] : [])]),
      known: (namespace) => namespace === foldersNamespace ? Effect.succeed(true) : Effect.map(environments.ofNamespace(namespace), (environment) => environment !== null),
    };
    const live = Context.get(yield* Layer.build(LiveBindings.layer({ namespaces })), LiveBindings);
    const events = yield* eventsOf;
    const baymaTelemetry = sandboxBaymaTelemetryEnv();
    const sessions = yield* makeSessions(
      {
        environments,
        pools: config.pools,
        releaseAfterMs: config.releaseAfterSeconds * 1000,
        podPort: config.podPort,
        bindingKey,
        events,
        quotaBytes: config.quotaBytes,
        snapshots: config.snapshots,
        mainNamespace: config.environment.namespace,
        // Bound pods' bayma exports each signal the gateway exports, to the receiver, where the installation exports any.
        telemetry: config.receiver && Object.keys(baymaTelemetry).length > 0 ? { ...config.receiver, env: baymaTelemetry } : undefined,
      },
      agents,
      binder,
    );
    // Folder conversations' instances, where the installation has folder workspaces (./folders.ts).
    const folders = config.folders
      ? yield* makeFolders({
        namespace: config.folders.namespace,
        template: config.folders.template as V1PodTemplateSpec,
        stateRoot: config.folders.stateRoot,
        releaseAfterMs: config.folders.releaseAfterSeconds * 1000,
        main: config.environment.name,
        podPort: config.podPort,
        bindingKey,
      }, binder)
      : null;
    // Of the pools there are as each round begins, the folder instances' among them.
    const setRight = Effect.flatMap(Effect.map(sessions.pools, (pools) => [...pools, ...(folders ? [folders.pool] : [])]), (pools) =>
      Effect.forEach(pools, (pool) =>
        reconcile(pool, (lease) => sessions.lost(pool, lease), (lease) => sessions.recordLost(pool, lease)).pipe(
          Effect.tap(({ pods, leases }) => (pods.length + leases.length > 0 ? Effect.logInfo(`set ${pool.namespace}/${pool.name} right: deleted ${pods.join(", ") || "no pod"}, let go ${leases.join(", ") || "no Lease"}`) : Effect.void)),
          Effect.catch((error) => Effect.logWarning(`setting ${pool.namespace}/${pool.name} right failed: ${error.message}`)),
        ), { discard: true }));
    yield* setRight;
    yield* Effect.forkScoped(Effect.repeat(setRight, Schedule.spaced(RECONCILE_MS)));
    const countRefusals = yield* makeRefusalCount(sessions.pools);
    yield* Effect.forkScoped(Effect.repeat(
      countRefusals.pipe(Effect.catch((error) => Effect.logWarning(`counting the pod policy's refusals failed: ${error.message}`))),
      Schedule.spaced(REFUSALS_MS),
    ));
    yield* Effect.forkScoped(Effect.repeat(
      sessions.sweep.pipe(
        Effect.tap((released) => (released.length > 0 ? Effect.logInfo(`released ${released.join(", ")}`) : Effect.void)),
        Effect.catch((error) => Effect.logWarning(`looking at bound pods failed: ${error.message}`)),
      ),
      Schedule.spaced(SWEEP_MS),
    ));
    // As often, each bound pod whose bayma ended, released, and the snapshot taken live of each that runs kept true to it.
    yield* Effect.forkScoped(Effect.repeat(
      sessions.live.pipe(Effect.catch((error) => Effect.logWarning(`looking at bound pods' sandboxes failed: ${error.message}`))),
      Schedule.spaced(SWEEP_MS),
    ));
    yield* Effect.forkScoped(Effect.repeat(sessions.collect, Schedule.spaced(COLLECT_MS)));
    // The folder instances', as often: each idle one released, and each whose bayma ended.
    if (folders) {
      yield* Effect.forkScoped(Effect.repeat(
        folders.sweep.pipe(
          Effect.tap((released) => (released.length > 0 ? Effect.logInfo(`released ${released.join(", ")}`) : Effect.void)),
          Effect.catch((error) => Effect.logWarning(`looking at folder instances failed: ${error.message}`)),
        ),
        Schedule.spaced(SWEEP_MS),
      ));
      yield* Effect.forkScoped(Effect.repeat(
        folders.live.pipe(Effect.catch((error) => Effect.logWarning(`looking at folder instances' bayma failed: ${error.message}`))),
        Schedule.spaced(SWEEP_MS),
      ));
    }
    // As often, the bound pods ending though the gateway did not end them, each released in the background as its bayma holds its stop.
    yield* Effect.forkScoped(Effect.repeat(
      sessions.drain.pipe(
        Effect.tap((began) => (began.length > 0 ? Effect.logInfo(`releasing ${began.join(", ")}, as their pods end`) : Effect.void)),
        Effect.catch((error) => Effect.logWarning(`looking at ending bound pods failed: ${error.message}`)),
      ),
      Schedule.spaced(SWEEP_MS),
    ));
    const served = yield* serveBinder({
      port: config.port,
      callerKey: ({ options }) => (options?.["environment"] === undefined ? null : environmentKey(callerKey, options["environment"])),
      bindingKey,
      podPort: config.podPort,
      poolOf: (key, options) => (folders && folderOfKey(key) ? folders.place(key, options) : sessions.place(key, options)),
      // The folder instances' listing, where there are folder instances; the rest, the sessions'.
      operate: (claims, operation) => (folders && /^\/folders(\/|$)/u.test(operation.path) ? folders.operate(claims, operation) : sessions.operate(claims, operation)),
      routeHeld: live.holds,
      // Every replica's address but its own, which answers its own waiting.
      peers: () =>
        Effect.tryPromise(() => lookup(config.peers, { all: true })).pipe(
          Effect.map((addresses) => addresses.map(({ address }) => `${address}:${config.port}`).filter((peer) => !peer.startsWith(`${process.env["POD_IP"] ?? ""}:`))),
          Effect.orElseSucceed(() => []),
        ),
    });
    yield* Effect.logInfo(`the sandbox gateway serves on port ${served.port}, for ${(yield* environments.current).map(({ name }) => name).join(", ")}`);
  }).pipe(Effect.provide(context));
}).pipe(
  Effect.tapCause((cause) => Effect.logError(`Fatal error: ${Cause.pretty(cause)}`)),
  withLogScope("sandbox-gateway"),
);
