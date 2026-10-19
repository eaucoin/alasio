/**
 * The bindings there are now, as their Leases are watched: what a binding's token
 * (./tokens.ts) is checked against wherever one is taken, so the token of a binding
 * released stops working as soon as its Lease's deletion is seen, with no call to the API
 * server per request.
 *
 * The Leases of bindings of each namespace there may be bindings in are watched, from every
 * one there is to each change: those the namespaces say now, looked at anew every
 * `refreshMs`, a watch started for each one new and ended for each gone, and one a token
 * names, as soon as it is asked of, where the namespaces know it. A namespace whose watch has
 * not yet sent every Lease there was as it began, as at the start or once the API server has
 * ended it and it is watched anew, can say of no binding that it is gone: what takes the
 * token refuses it meanwhile, to be asked again. One the namespaces do not know has no
 * binding. A binding under way, its pod not yet named, is no binding a token is minted for,
 * and is not held.
 */
import { Context, Effect, FiberMap, Layer, Schedule, Stream } from "effect";

import { INITIAL_EVENTS_END, KubeClient } from "../kube/client.ts";
import { type BindingLease } from "./bindings.ts";
import { BINDING_LABEL, KEY_ANNOTATION } from "./names.ts";

/** How long a watch that failed waits before it watches again. */
const RETRY_MS = 1_000;
/** How often the namespaces are looked at anew, by default. */
const REFRESH_MS = 30_000;

/** Whether a binding is held now: held; gone, as its namespace's watch says; or not known, its watch not caught up. */
export type Held = "held" | "gone" | "unknown";

/** The namespaces there may be bindings in: those there are now, and whether one is, as asked anew where it is not known. */
export interface BindingNamespaces {
  readonly current: Effect.Effect<readonly string[]>;
  readonly known: (namespace: string) => Effect.Effect<boolean>;
}

export class LiveBindings extends Context.Service<LiveBindings, {
  /** Whether `binding`, a Lease's uid in `namespace`, is a binding of `key` now. */
  readonly holds: (key: string, binding: string, namespace: string) => Effect.Effect<Held>;
}>()("alasio/binder/LiveBindings") {
  /** On the KubeClient, the bindings of `namespaces`, each watched as the module says, for as long as the layer lives. */
  static readonly layer = ({ namespaces, refreshMs = REFRESH_MS }: { readonly namespaces: BindingNamespaces; readonly refreshMs?: number }): Layer.Layer<LiveBindings, never, KubeClient> =>
    Layer.effect(
      LiveBindings,
      Effect.gen(function*() {
        const kube = yield* KubeClient;
        // Each namespace's bindings by their Leases' uids, while its watch is caught up.
        const live = new Map<string, Map<string, string>>();
        const watches = yield* FiberMap.make<string>();
        const watched = new Set<string>();

        /** Watches `namespace` once, from every Lease there is, until the watch ends. */
        const watchOnce = (namespace: string): Effect.Effect<void> => {
          const seen = new Map<string, string>();
          return kube.watch("coordination.k8s.io/v1", "leases", namespace, BINDING_LABEL).pipe(
            Stream.runForEach(({ type, object }) =>
              Effect.sync(() => {
                const { metadata, spec } = object as BindingLease;
                if (type === "BOOKMARK") {
                  if (metadata?.annotations?.[INITIAL_EVENTS_END] === "true") live.set(namespace, seen);
                  return;
                }
                const uid = metadata?.uid ?? "";
                const key = metadata?.annotations?.[KEY_ANNOTATION];
                if (type === "DELETED" || !key || !spec?.holderIdentity) seen.delete(uid);
                else seen.set(uid, key);
              })
            ),
            Effect.catch((error) => Effect.logWarning(`watching the bindings of ${namespace} failed: ${error.message}`).pipe(Effect.andThen(Effect.sleep(RETRY_MS)))),
            // Ended, by the API server, a failure or its namespace gone: nothing of it confirmed until it is watched anew.
            Effect.ensuring(Effect.sync(() => void live.delete(namespace))),
          );
        };

        /** Watches `namespace`, unless it is watched. */
        const watch = (namespace: string) =>
          Effect.suspend(() => {
            if (watched.has(namespace)) return Effect.void;
            watched.add(namespace);
            return Effect.asVoid(FiberMap.run(watches, namespace, Effect.forever(watchOnce(namespace)), { onlyIfMissing: true }));
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
          holds: (key, binding, namespace) =>
            Effect.gen(function*() {
              if (!watched.has(namespace)) {
                if (!(yield* namespaces.known(namespace))) return "gone";
                yield* watch(namespace);
              }
              const bindings = live.get(namespace);
              if (!bindings) return "unknown";
              return bindings.get(binding) === key ? "held" : "gone";
            }),
        });
      }),
    );
}
