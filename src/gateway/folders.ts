/**
 * The sandbox gateway's folder instances: the bayma of a folder conversation under a harness,
 * `folder:<harness>:<conversation>` (./names.ts folderKey), main's alone, a pod the gateway
 * makes at its bind, in the folder namespace, of the folder template `alasio up` renders of
 * the host profile, as the folder pods' policy pins it (cli/src/manifests/admission.ts
 * folderPodPolicy). No pool of free pods: its arguments carry its state directory, which is
 * the conversation's on the machine, under the template's state root (./names.ts
 * folderStateDir), and a warm pod of the operator's template would buy little.
 *
 * Its binding's token, which its bayma takes requests and exports telemetry with, is in a
 * Secret of its own, made before it and owned by it, so it goes as the pod goes.
 *
 * Released once its bayma has run no exec, and no request of it has come, for
 * `releaseAfterMs`: its pod deleted, and waited for until it is gone, as its bayma dumps each
 * idle REPL session as it stops (./names.ts FOLDER_STOP_SECONDS), so the next bind's pod, on
 * the same state directory, restores them; nothing of the node agents', nor of the bucket's. A
 * pod whose bayma ended is released as broken, the next bind making another.
 */
import { randomBytes } from "node:crypto";

import type { V1Pod, V1PodSpec, V1PodTemplateSpec, V1Secret } from "@kubernetes/client-node";
import { Clock, Effect } from "effect";

import { type Binder, BindStepFailed } from "../binder/binder.ts";
import { heldLeases } from "../binder/bindings.ts";
import { KEY_ANNOTATION, type Pool } from "../binder/names.ts";
import { type Placement, PlacementRefused } from "../binder/server.ts";
import { baymaIdleness, IDLE_POLL_MS, makeSweep } from "../binder/sweep.ts";
import { bindingToken } from "../binder/tokens.ts";
import { type KubeApiError, KubeClient } from "../kube/client.ts";
import { BAYMA_CONTAINER } from "../kube/sandbox-shape.ts";
import { FOLDER_POOL, FOLDER_STOP_SECONDS, folderOfKey, folderPodName, folderPodSpec, folderStateDir, folderTokenSecret } from "./names.ts";

/** How long a folder instance's release waits for its pod to be gone, past its stop. */
const GONE_WAIT_MS = (FOLDER_STOP_SECONDS + 60) * 1000;

/** What makeFolders is given; see the module. */
export interface FoldersOptions {
  readonly namespace: string;
  readonly template: V1PodTemplateSpec;
  readonly stateRoot: string;
  readonly releaseAfterMs: number;
  /** main's environment's name, whose callers alone bind a folder instance. */
  readonly main: string;
  readonly podPort: number;
  readonly bindingKey: Buffer;
  readonly idlePollMs?: number;
}

/** The gateway's folder instances; see makeFolders. */
export interface Folders {
  /** The pool folder instances are bound in. */
  readonly pool: Pool;
  /** Where the folder instance `key` is bound, by its caller's token's options, and how its pod is made. */
  readonly place: (key: string, options: Readonly<Record<string, string>>) => Effect.Effect<Placement, PlacementRefused>;
  /** Releases each folder instance idle for its time, as the module says: which keys. */
  readonly sweep: Effect.Effect<readonly string[], KubeApiError>;
  /** Releases, as broken, each folder instance whose bayma ended, as the module says. */
  readonly live: Effect.Effect<void, KubeApiError>;
}

/** The gateway's folder instances, as the module says, on the binder and the KubeClient. */
export const makeFolders = Effect.fnUntraced(function*(
  { namespace, template, stateRoot, releaseAfterMs, main, podPort, bindingKey, idlePollMs = IDLE_POLL_MS }: FoldersOptions,
  binder: Binder["Service"],
): Effect.fn.Return<Folders, never, KubeClient> {
  const kube = yield* KubeClient;
  const pool: Pool = { name: FOLDER_POOL, namespace };

  /** Deletes the pod `name`, which its bayma stops as its grace allows, dumping its REPL sessions, and waits until it is gone. */
  const removed = (name: string): Effect.Effect<void, KubeApiError | BindStepFailed> =>
    Effect.gen(function*() {
      yield* kube.remove("v1", "Pod", namespace, name);
      const deadline = (yield* Clock.currentTimeMillis) + GONE_WAIT_MS;
      while ((yield* kube.read("v1", "Pod", namespace, name)) !== null) {
        if ((yield* Clock.currentTimeMillis) >= deadline) return yield* new BindStepFailed({ reason: `the folder instance's pod ${name} was not gone within ${GONE_WAIT_MS / 1000} s` });
        yield* Effect.sleep(1_000);
      }
    });

  /** The pod of the folder instance `key`, of `harness` and `conversation`, made with its binding's token's Secret first, which it then owns. */
  const make = (key: string, harness: string, conversation: string) =>
  (labels: Readonly<Record<string, string>>, annotations: Readonly<Record<string, string>>, binding: string): Effect.Effect<V1Pod, KubeApiError | BindStepFailed> =>
    Effect.gen(function*() {
      const name = folderPodName(key, randomBytes(4).readUInt32BE().toString(36).padStart(5, "0").slice(-5));
      const secret: V1Secret = {
        apiVersion: "v1",
        kind: "Secret",
        metadata: { name: folderTokenSecret(name), namespace, labels: { ...labels } },
        type: "Opaque",
        stringData: { token: bindingToken(bindingKey, { key, binding, namespace }) },
      };
      yield* kube.create(secret);
      const spec = folderPodSpec((template.spec ?? { containers: [] }) as V1PodSpec, name, folderStateDir(stateRoot, harness, conversation));
      const pod = yield* kube.create<V1Pod>({
        apiVersion: "v1",
        kind: "Pod",
        metadata: { name, namespace, labels: { ...template.metadata?.labels, ...labels }, annotations: { ...template.metadata?.annotations, ...annotations } },
        spec,
      }).pipe(Effect.tapError(() => kube.remove("v1", "Secret", namespace, folderTokenSecret(name)).pipe(Effect.ignore)));
      // Owned by its pod, so it goes as the pod goes.
      yield* kube.patch("v1", "Secret", namespace, folderTokenSecret(name), {
        metadata: { ownerReferences: [{ apiVersion: "v1", kind: "Pod", name, uid: pod.metadata?.uid ?? "", blockOwnerDeletion: false, controller: false }] },
      });
      yield* Effect.logInfo(`made the folder instance ${name} of ${key}, on the state directory ${folderStateDir(stateRoot, harness, conversation)}`);
      return pod;
    });

  const place = (key: string, options: Readonly<Record<string, string>>): Effect.Effect<Placement, PlacementRefused> =>
    Effect.gen(function*() {
      const folder = folderOfKey(key);
      if (!folder) return yield* new PlacementRefused({ reason: `${key} is no folder instance's` });
      if (options["environment"] !== main) return yield* new PlacementRefused({ reason: `a folder is the machine's own files, of main's alone, not ${options["environment"] ?? "unsaid"}'s` });
      return {
        pool,
        make: make(key, folder.harness, folder.conversation),
        // Released as it is when idle: its pod gone, its REPL sessions dumped as it stopped.
        release: ({ pod }) => removed(pod),
      };
    });

  const sweep = yield* makeSweep({
    pools: Effect.succeed([pool]),
    idleness: baymaIdleness({ podPort, releaseAfterMs, idlePollMs, bindingKey }),
    release: (_pool: Pool, key: string) => binder.release(pool, key, ({ pod }) => removed(pod)),
  });

  const live: Effect.Effect<void, KubeApiError> = Effect.gen(function*() {
    for (const lease of yield* Effect.provideService(heldLeases(pool), KubeClient, kube)) {
      const key = lease.metadata?.annotations?.[KEY_ANNOTATION] ?? "";
      const pod = (yield* kube.read("v1", "Pod", namespace, lease.spec?.holderIdentity ?? "")) as V1Pod | null;
      if (!pod || pod.metadata?.deletionTimestamp !== undefined) continue;
      const bayma = pod.status?.containerStatuses?.find((status) => status.name === BAYMA_CONTAINER);
      if (bayma?.state?.terminated === undefined && pod.status?.phase !== "Failed" && pod.status?.phase !== "Succeeded") continue;
      yield* Effect.logWarning(`${key}'s bayma ended in its folder instance ${pod.metadata?.name}, which serves its binding no more: it is released, the next bind making another`);
      yield* Effect.forkDetach(binder.release(pool, key, ({ pod: name }) => removed(name)).pipe(
        Effect.catch((error) => Effect.logWarning(`releasing ${key}, whose bayma ended, failed: ${error.message}`)),
      ));
    }
  });

  return { pool, place, sweep, live };
});
