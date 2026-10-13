/**
 * What an installation may not do to the bundled object store's parts as it would to
 * other workloads: change the masters' count, lowered, as their Raft, which keeps where
 * every volume is, would lose its quorum, and with it that, or raised, as one master's
 * state, made without peers, is no Raft's for more to join; or lower the volume servers'
 * while one it would remove still holds volumes, which it would take with it. The first
 * is refused; the second until those volume servers hold none, as their masters say, the
 * way to which the refusal says. Nor start the parts at all while the store still keeps its
 * data on the volume of the one pod it ran as before them. Before anything is applied.
 */
import { Writable } from "node:stream";

import type { KubernetesObject, V1Pod, V1StatefulSet } from "@kubernetes/client-node";
import { Effect, Schema } from "effect";

import { NAMESPACE } from "../manifests/common.ts";
import { MASTER, VOLUME_SERVER } from "../manifests/object-store.ts";
import { kind, KubeApi, type KubeApiError } from "./api.ts";
import { selectorOf } from "./rollout.ts";

/** A change of the object store's scale alasio does not make: why, and what to do instead. */
export class ObjectStoreScaleRefused extends Schema.TaggedError<ObjectStoreScaleRefused>()("ObjectStoreScaleRefused", {
  message: Schema.String,
}) {}

/** A volume server, as the masters' /dir/status says it: its address, and what it holds. */
interface DataNode {
  readonly Url: string;
  readonly Volumes?: number;
  readonly EcShards?: number;
}

/** The masters' /dir/status, as far as it says each volume server. */
interface DirStatus {
  readonly Topology?: { readonly DataCenters?: readonly { readonly Racks?: readonly { readonly DataNodes?: readonly DataNode[] }[] }[] };
}

/** The volume servers `status` says, by their address. */
export function dataNodes(status: DirStatus): readonly DataNode[] {
  return (status.Topology?.DataCenters ?? []).flatMap(({ Racks = [] }) => Racks.flatMap(({ DataNodes = [] }) => DataNodes));
}

/** The replicas of the StatefulSet `name` among `objects`. */
const rendered = (objects: readonly KubernetesObject[], name: string): number | undefined =>
  (objects.find((object) => object.kind === "StatefulSet" && object.metadata?.name === name) as V1StatefulSet | undefined)?.spec?.replicas;

/** What a running master's /dir/status says, read in its pod; null where no master runs, or it does not answer. */
const dirStatus = Effect.gen(function*() {
  const kube = yield* KubeApi;
  const masters = yield* kube.list<V1Pod>(kind("Pod"), { namespace: NAMESPACE, labelSelector: selectorOf((yield* kube.get<V1StatefulSet>({ ...kind("StatefulSet"), name: MASTER, namespace: NAMESPACE }))?.spec?.selector.matchLabels ?? {}) });
  const running = masters.find(({ status }) => status?.phase === "Running")?.metadata?.name;
  if (!running) return null;
  let printed = "";
  const stdout = new Writable({
    write: (chunk: Buffer, _encoding, done) => {
      printed += chunk.toString("utf8");
      done();
    },
  });
  const exitCode = yield* kube.exec({ namespace: NAMESPACE, pod: running, container: "master" }, ["wget", "-q", "-O", "-", "http://127.0.0.1:9333/dir/status"], {
    stdin: null,
    stdout,
    stderr: new Writable({ write: (_chunk, _encoding, done) => done() }),
    tty: false,
  }).pipe(Effect.orElseSucceed(() => 1));
  return exitCode === 0 ? parsed(printed) : null;
});

/** `text` as the masters' /dir/status, or null where it is not JSON. */
function parsed(text: string): DirStatus | null {
  try {
    return JSON.parse(text) as DirStatus;
  } catch {
    return null;
  }
}

/**
 * Refuses `objects` where the cluster's object store still runs all in one, as alasio before
 * its parts did, its data on that pod's volume, which the parts do not use: started, they
 * would run the stack on a store that is empty. Its data is moved onto them by the cut-over
 * that release's upgrade notes give, which deletes that volume's claim once it has.
 */
export const checkObjectStoreLayout = Effect.fnUntraced(function*(objects: readonly KubernetesObject[]): Effect.fn.Return<void, KubeApiError | ObjectStoreScaleRefused, KubeApi> {
  if (rendered(objects, VOLUME_SERVER) === undefined) return;
  const kube = yield* KubeApi;
  const claim = yield* kube.get({ ...kind("PersistentVolumeClaim"), name: ALL_IN_ONE_CLAIM, namespace: NAMESPACE });
  if (claim === null) return;
  return yield* new ObjectStoreScaleRefused({
    message: `the object store still keeps its data on the volume of its one pod, the claim ${ALL_IN_ONE_CLAIM}, which this release's parts do not use: ` +
      "they would start the stack on an empty store, so alasio does not apply them; move the data onto them first with the release's cut-over, " +
      `which deletes ${ALL_IN_ONE_CLAIM} once it has`,
  });
});

/** The claim of the volume the object store kept its data on while it ran all in one. */
const ALL_IN_ONE_CLAIM = "data-alasio-seaweedfs-0";

/** Refuses `objects`' scale of the object store's masters and volume servers where the cluster's would be lowered as alasio does not. */
export const checkObjectStoreScale = Effect.fnUntraced(function*(objects: readonly KubernetesObject[]): Effect.fn.Return<void, KubeApiError | ObjectStoreScaleRefused, KubeApi> {
  const kube = yield* KubeApi;
  const running = (name: string) => kube.get<V1StatefulSet>({ ...kind("StatefulSet"), name, namespace: NAMESPACE }).pipe(Effect.map((found) => found?.spec?.replicas));
  const masters = { now: yield* running(MASTER), then: rendered(objects, MASTER) };
  if (masters.now !== undefined && masters.then !== undefined && masters.then < masters.now) {
    return yield* new ObjectStoreScaleRefused({
      message: `the object store runs ${masters.now} masters, whose Raft keeps where every volume is: fewer would lose its quorum, and that with it, so alasio does not lower their count; objectStore.bundled.masters.scaling stays ${masters.now}`,
    });
  }
  if (masters.now !== undefined && masters.then !== undefined && masters.now > 0 && masters.then > masters.now) {
    return yield* new ObjectStoreScaleRefused({
      message: `the object store runs ${masters.now} master, whose state, made without peers, is no Raft's that more masters could join, so alasio does not raise their count; ` +
        `objectStore.bundled.masters.scaling stays ${masters.now}, as the count of masters is set as the store is made`,
    });
  }
  const now = yield* running(VOLUME_SERVER);
  const then = rendered(objects, VOLUME_SERVER);
  if (now === undefined || then === undefined || then >= now) return;
  const removed = Array.from({ length: now - then }, (_, index) => `${VOLUME_SERVER}-${then + index}.`);
  const status = yield* dirStatus;
  const holding = status === null ? null : dataNodes(status).filter(({ Url, Volumes = 0, EcShards = 0 }) => removed.some((name) => Url.startsWith(name)) && Volumes + EcShards > 0);
  if (holding === null) {
    return yield* new ObjectStoreScaleRefused({
      message: `objectStore.bundled.volumeServers.scaling would remove volume servers whose volumes no running master says: alasio lowers their count only once they hold none`,
    });
  }
  if (holding.length > 0) {
    const nodes = holding.map(({ Url }) => Url);
    return yield* new ObjectStoreScaleRefused({
      message: `objectStore.bundled.volumeServers.scaling would remove volume servers that still hold volumes, ${nodes.join(", ")}: move them to the others first, in weed shell in a gateway's buckets container, ` +
        `with lock, then ${nodes.map((node) => `volume.server.evacuate -node ${node} -apply`).join(", ")}, then unlock; then lower their count`,
    });
  }
});
