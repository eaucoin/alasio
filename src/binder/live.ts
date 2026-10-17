/**
 * The bindings there are now, as their Leases are watched: what a binding's token
 * (./tokens.ts) is checked against wherever one is taken, so the token of a binding
 * released stops working as soon as its Lease's deletion is seen, with no call to the API
 * server per request.
 *
 * Each namespace's Leases of bindings are watched, from every one there is to each change.
 * A namespace whose watch has not yet sent every Lease there was as it began, as at the start
 * or once the API server has ended it and it is watched anew, can say of no binding that it
 * is gone: what takes the token refuses it meanwhile, to be asked again. A binding under
 * way, its pod not yet named, is no binding a token is minted for, and is not held.
 */
import { Context, Effect, Layer, Stream } from "effect";

import { INITIAL_EVENTS_END, KubeClient } from "../kube/client.ts";
import { BINDING_LABEL, type BindingLease, KEY_ANNOTATION } from "./bindings.ts";

/** How long a watch that failed waits before it watches again. */
const RETRY_MS = 1_000;

/** Whether a binding is held now: held; gone, as every watch says; or not known, a watch not caught up. */
export type Held = "held" | "gone" | "unknown";

export class LiveBindings extends Context.Service<LiveBindings, {
  /** Whether `binding`, a Lease's uid, is a binding of `key` now. */
  readonly holds: (key: string, binding: string) => Effect.Effect<Held>;
}>()("alasio/binder/LiveBindings") {
  /** On the KubeClient, the bindings of `namespaces`, each watched for as long as the layer lives. */
  static readonly layer = ({ namespaces }: { readonly namespaces: readonly string[] }): Layer.Layer<LiveBindings, never, KubeClient> =>
    Layer.effect(
      LiveBindings,
      Effect.gen(function*() {
        const kube = yield* KubeClient;
        // Each namespace's bindings by their Leases' uids, while its watch is caught up.
        const live = new Map<string, Map<string, string>>();

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
            // Ended, by the API server or a failure: nothing of it confirmed until it is watched anew.
            Effect.ensuring(Effect.sync(() => void live.delete(namespace))),
          );
        };
        yield* Effect.forEach(namespaces, (namespace) => Effect.forkScoped(Effect.forever(watchOnce(namespace))), { discard: true });

        return LiveBindings.of({
          holds: (key, binding) =>
            Effect.sync((): Held => {
              if ([...live.values()].some((bindings) => bindings.get(binding) === key)) return "held";
              return namespaces.every((namespace) => live.has(namespace)) ? "gone" : "unknown";
            }),
        });
      }),
    );
}
