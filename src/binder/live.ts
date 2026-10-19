/**
 * The bindings there are now, as their Leases are watched: what a binding's token
 * (./tokens.ts) is checked against wherever one is taken, so the token of a binding
 * released stops working as soon as its Lease's deletion is seen, with no call to the API
 * server per request.
 *
 * The Leases of bindings of each namespace there may be bindings in are watched, from every
 * one there is to each change: those the namespaces say now, looked at anew every
 * `refreshMs`, a watch started for each one new and ended for each gone, and one a token
 * names, as soon as it is asked of, where the namespaces know it. A watch that ends, as the
 * API server ends one now and then or its connection is lost, is watched again from the last
 * version it saw, so it misses no change between, and listed anew only where the API server
 * keeps no change from that far back (410); the bindings it knew are what it says meanwhile,
 * and those of the new list once it has them all. A namespace can say of no binding that it
 * is gone before its first list has sent every Lease there was, or once nothing of it has been
 * heard for `knownForMs`, the API server unreachable: what takes the token refuses it
 * meanwhile, to be asked again. One the namespaces do not know has no binding. A binding
 * under way, its pod not yet named, is no binding a token is minted for, and is not held.
 */
import { Clock, Context, Effect, FiberMap, Layer, Schedule, Stream } from "effect";

import { hasStatus, INITIAL_EVENTS_END, KubeClient, WATCH_SILENCE_MS } from "../kube/client.ts";
import { type BindingLease } from "./bindings.ts";
import { BINDING_LABEL, KEY_ANNOTATION } from "./names.ts";

/** How long a watch that failed waits before it watches again. */
const RETRY_MS = 1_000;
/** How often the namespaces are looked at anew, by default. */
const REFRESH_MS = 30_000;
/**
 * How long a namespace's bindings are what it says since its watch last heard of them, by
 * default: as long as a watch hears nothing, not even a bookmark, before its connection is
 * taken for lost, so a watch watched again in time keeps them, and a gateway cut off from
 * the API server keeps no binding another released for longer.
 */
const KNOWN_FOR_MS = WATCH_SILENCE_MS;

/** A namespace's bindings, by their Leases' uids, as of when its watch last heard of them. */
interface Known {
  readonly bindings: Map<string, string>;
  heard: number;
}

/** Whether a binding is held now: held; gone, as its namespace's watch says; or not known, its watch not caught up. */
export type Held = "held" | "gone" | "unknown";

/** The namespaces there may be bindings in: those there are now, and whether one is, as asked anew where it is not known. */
export interface BindingNamespaces {
  readonly current: Effect.Effect<readonly string[]>;
  readonly known: (namespace: string) => Effect.Effect<boolean>;
}

export class LiveBindings extends Context.Service<LiveBindings, {
  /**
   * Whether `binding`, a Lease's uid in `namespace`, is a binding of `key` now: not known, where
   * it has not been seen and its namespace's watch has heard nothing since `since`, as a binding
   * made then may not have been heard of yet.
   */
  readonly holds: (key: string, binding: string, namespace: string, since?: number) => Effect.Effect<Held>;
}>()("alasio/binder/LiveBindings") {
  /** On the KubeClient, the bindings of `namespaces`, each watched as the module says, for as long as the layer lives. */
  static readonly layer = (
    { namespaces, refreshMs = REFRESH_MS, knownForMs = KNOWN_FOR_MS }: { readonly namespaces: BindingNamespaces; readonly refreshMs?: number; readonly knownForMs?: number },
  ): Layer.Layer<LiveBindings, never, KubeClient> =>
    Layer.effect(
      LiveBindings,
      Effect.gen(function*() {
        const kube = yield* KubeClient;
        // Each namespace's bindings, once its first list has sent them all.
        const live = new Map<string, Known>();
        const watches = yield* FiberMap.make<string>();
        const watched = new Set<string>();

        /** Watches `namespace`, as the module says, until its fiber is ended. */
        const watchAlways = (namespace: string): Effect.Effect<void> => {
          // The last version the watch saw of what it knows, to be watched again from; none until a list has sent every Lease there was.
          let from: string | undefined;
          const once = Effect.suspend(() => {
            const listing = from === undefined;
            // Where a list is under way, the bindings it sends, said once it has sent them all; the bindings known, where it is watched again from `from`.
            const bindings = listing ? new Map<string, string>() : live.get(namespace)?.bindings ?? new Map<string, string>();
            let caughtUp = !listing;
            return kube.watch("coordination.k8s.io/v1", "leases", namespace, BINDING_LABEL, from).pipe(
              Stream.runForEach(({ type, object }) =>
                Effect.gen(function*() {
                  const { metadata, spec } = object as BindingLease;
                  const now = yield* Clock.currentTimeMillis;
                  if (type === "BOOKMARK") {
                    if (listing && metadata?.annotations?.[INITIAL_EVENTS_END] === "true") {
                      caughtUp = true;
                      live.set(namespace, { bindings, heard: now });
                    }
                  } else {
                    const uid = metadata?.uid ?? "";
                    const key = metadata?.annotations?.[KEY_ANNOTATION];
                    if (type === "DELETED" || !key || !spec?.holderIdentity) bindings.delete(uid);
                    else bindings.set(uid, key);
                  }
                  if (!caughtUp) return;
                  from = metadata?.resourceVersion ?? from;
                  const known = live.get(namespace);
                  if (known) known.heard = now;
                })
              ),
              Effect.catchIf(hasStatus(410), () => Effect.sync(() => void (from = undefined))),
              Effect.catch((error) => Effect.logWarning(`watching the bindings of ${namespace} failed: ${error.message}`).pipe(Effect.andThen(Effect.sleep(RETRY_MS)))),
            );
          });
          // Its namespace gone: nothing of it known.
          return Effect.forever(once).pipe(Effect.ensuring(Effect.sync(() => void live.delete(namespace))));
        };

        /** Watches `namespace`, unless it is watched. */
        const watch = (namespace: string) =>
          Effect.suspend(() => {
            if (watched.has(namespace)) return Effect.void;
            watched.add(namespace);
            return Effect.asVoid(FiberMap.run(watches, namespace, watchAlways(namespace), { onlyIfMissing: true }));
          });

        /** Watches each namespace there is now, and ends the watch of each gone. */
        const refresh = Effect.gen(function*() {
          const current = new Set(yield* namespaces.current);
          for (const namespace of [...watched]) {
            if (current.has(namespace)) continue;
            watched.delete(namespace);
            yield* FiberMap.remove(watches, namespace);
          }
          yield* Effect.forEach(current, watch, { discard: true });
        });
        yield* refresh;
        yield* Effect.forkScoped(Effect.repeat(refresh, Schedule.spaced(refreshMs)));

        return LiveBindings.of({
          holds: (key, binding, namespace, since) =>
            Effect.gen(function*() {
              if (!watched.has(namespace)) {
                if (!(yield* namespaces.known(namespace))) return "gone";
                yield* watch(namespace);
              }
              const known = live.get(namespace);
              if (!known || (yield* Clock.currentTimeMillis) - known.heard >= knownForMs) return "unknown";
              if (known.bindings.get(binding) === key) return "held";
              return since !== undefined && known.heard < since ? "unknown" : "gone";
            }),
        });
      }),
    );
}
