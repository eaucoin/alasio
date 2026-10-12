/**
 * BusyPods for tests (src/kube/busy.ts): each hold recorded, with the workspace pod it
 * holds beside alasio's own, and whether it has ended.
 */
import { Effect, Layer } from "effect";

import { ActiveTurns } from "../../src/harness/active-turns.ts";
import { BusyPods, type PodRef } from "../../src/kube/busy.ts";

/** A hold of a turn's: the workspace pod it holds, and whether it has let go. */
export interface Hold {
  readonly workspace: PodRef | null;
  released: boolean;
}

/** BusyPods that records its holds in `holds`, in the order they are taken. */
export function recordingBusyPods(holds: Hold[] = []): { readonly holds: Hold[]; readonly layer: Layer.Layer<BusyPods> } {
  const hold = (workspace: PodRef | null) =>
    Effect.acquireRelease(
      Effect.sync(() => {
        const taken: Hold = { workspace, released: false };
        holds.push(taken);
        return taken;
      }),
      (taken) => Effect.sync(() => (taken.released = true)),
    ).pipe(Effect.asVoid);
  return { holds, layer: Layer.succeed(BusyPods, BusyPods.of({ hold })) };
}

/** The running turns, holding their pods busy in `holds`. */
export function activeTurnsHolding(holds: Hold[]): ActiveTurns["Service"] {
  return Effect.runSync(Effect.provide(ActiveTurns, ActiveTurns.layer.pipe(Layer.provide(recordingBusyPods(holds).layer))));
}
