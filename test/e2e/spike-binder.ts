/**
 * Spike (gates G3, G4 and G5 of the sandbox service), not to land: two gateways, each its
 * own client of the API server, racing to make one key's Lease (G3) and to claim one free
 * pod by an update conditioned on its resourceVersion (G4); and a pool Deployment's pod
 * claimed by relabelling it out of the Deployment's selector, then the Deployment rolled
 * out anew and scaled to none (G5). Each finding is printed as `SPIKE <gate> <finding>: <value>`.
 */
import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { KubeConfig, KubernetesObjectApi, PatchStrategy, type V1Deployment, type V1Pod } from "@kubernetes/client-node";

import { kube, paths, ref } from "./harness.ts";
import { until } from "./workspace-storage.ts";

const NS = "alasio-spike-binder";

const said = (gate: string, finding: string, value: unknown) => console.error(`SPIKE ${gate} ${finding}: ${typeof value === "string" ? value : JSON.stringify(value)}`);

/** A gateway: a client of its own, with connections of its own. */
function gateway(): KubernetesObjectApi {
  const config = new KubeConfig();
  config.loadFromFile(paths().kubeconfig);
  return KubernetesObjectApi.makeApiClient(config);
}

/** The API's HTTP status of a failed call. */
const statusOf = (error: unknown): number => (typeof error === "object" && error !== null && "code" in error && typeof error.code === "number" ? error.code : -1);

/** What each of `racers` came to: the winners' indices, and the statuses the others failed with. */
async function race(racers: readonly (() => Promise<unknown>)[]): Promise<{ won: number[]; lost: number[] }> {
  const ran = await Promise.allSettled(racers.map((run) => run()));
  return {
    won: ran.flatMap((result, index) => (result.status === "fulfilled" ? [index] : [])),
    lost: ran.flatMap((result) => (result.status === "rejected" ? [statusOf(result.reason)] : [])),
  };
}

const pending = (name: string, labels: Record<string, string>): V1Pod => ({
  apiVersion: "v1",
  kind: "Pod",
  metadata: { name, namespace: NS, labels },
  // Never scheduled: a claim is an update of the object alone.
  spec: { nodeSelector: { "alasio.dev/spike": "nowhere" }, containers: [{ name: "pause", image: "registry.k8s.io/pause:3.10" }] },
});

export function spikeBinder(): void {
  describe("spike: one binding per key (G3), one claim per pod (G4), and bound pods out of their Deployment (G5)", () => {
    test("leases, claims, and the pool's rollout and scale", async () => {
      await kube.apply({ apiVersion: "v1", kind: "Namespace", metadata: { name: NS } });
      const gateways = [gateway(), gateway(), gateway(), gateway()];
      try {
        // G3: every gateway at once makes the key's Lease, 200 keys over.
        const keys = 200;
        const leaseStart = Date.now();
        const leases = await Promise.all(Array.from({ length: keys }, (_, key) =>
          race(gateways.map((api, index) => () =>
            api.create({
              apiVersion: "coordination.k8s.io/v1",
              kind: "Lease",
              metadata: { name: `key-${key}`, namespace: NS, annotations: { "alasio.dev/gateway": String(index) } },
              spec: { holderIdentity: `gateway-${index}`, leaseDurationSeconds: 60 },
            })
          ))
        ));
        const leaseMs = Date.now() - leaseStart;
        const leaseWinners = leases.map(({ won }) => won.length);
        said("G3", "keys raced, by four gateways each", keys);
        said("G3", "winners per key", [...new Set(leaseWinners)]);
        said("G3", "losers' statuses", [...new Set(leases.flatMap(({ lost }) => lost))]);
        said("G3", "creates a second", Math.round((keys * gateways.length * 1000) / leaseMs));
        assert.deepEqual([...new Set(leaseWinners)], [1]);

        // G4: free pods, each claimed at once by every gateway, each from the pod as it read it.
        const pods = 100;
        await Promise.all(Array.from({ length: pods }, (_, n) => gateways[0]!.create(pending(`free-${n}`, { "alasio.dev/pool": "spike", "alasio.dev/free": "true" }))));
        const rounds = 3;
        const winners: number[] = [];
        const lostWith = new Set<number>();
        let claims = 0;
        const claimStart = Date.now();
        for (let round = 0; round < rounds; round += 1) {
          const outcomes = await Promise.all(Array.from({ length: pods }, async (_, n) => {
            const read = await Promise.all(gateways.map((api) => api.read<V1Pod>({ apiVersion: "v1", kind: "Pod", metadata: { name: `free-${n}`, namespace: NS } })));
            claims += gateways.length;
            return race(gateways.map((api, index) => () => {
              const pod = read[index]!;
              return api.replace({ ...pod, metadata: { ...pod.metadata, labels: { "alasio.dev/pool": "spike", "alasio.dev/bound": `round-${round}-gateway-${index}` } } });
            }));
          }));
          winners.push(...outcomes.map(({ won }) => won.length));
          for (const { lost } of outcomes) for (const status of lost) lostWith.add(status);
          // Freed again for the next round.
          await Promise.all(Array.from({ length: pods }, (_, n) =>
            gateways[0]!.patch({ apiVersion: "v1", kind: "Pod", metadata: { name: `free-${n}`, namespace: NS, labels: { "alasio.dev/free": "true" } } }, undefined, undefined, "spike", undefined, PatchStrategy.MergePatch)
          ));
        }
        const claimMs = Date.now() - claimStart;
        said("G4", "pods raced, by four gateways each, rounds", { pods, rounds });
        said("G4", "winners per pod", [...new Set(winners)]);
        said("G4", "losers' statuses", [...lostWith]);
        said("G4", "conditioned updates a second (reads, and the frees between rounds, included)", Math.round((claims * 1000) / claimMs));
        assert.deepEqual([...new Set(winners)], [1]);

        // G5: a pool's pod claimed by relabelling it out of the pool's selector.
        const pool: V1Deployment = {
          apiVersion: "apps/v1",
          kind: "Deployment",
          metadata: { name: "pool", namespace: NS },
          spec: {
            replicas: 3,
            selector: { matchLabels: { "alasio.dev/pool": "pool", "alasio.dev/free": "true" } },
            template: {
              metadata: { labels: { "alasio.dev/pool": "pool", "alasio.dev/free": "true" }, annotations: { "alasio.dev/template": "1" } },
              spec: { terminationGracePeriodSeconds: 1, containers: [{ name: "pause", image: "registry.k8s.io/pause:3.10" }] },
            },
          },
        };
        await kube.apply(pool);
        const free = async () => (await kube.list<V1Pod>("Pod", { namespace: NS, labelSelector: "alasio.dev/pool=pool,alasio.dev/free=true" })).filter((pod) => !pod.metadata?.deletionTimestamp);
        await until("the pool's three pods", async () => ((await free()).filter((pod) => pod.status?.phase === "Running").length === 3 ? true : undefined));
        const [first] = await free();
        assert.ok(first?.metadata?.name);
        const bound = first.metadata.name;
        const claimed = await gateways[1]!.replace({ ...first, metadata: { ...first.metadata, labels: { "alasio.dev/pool": "pool", "alasio.dev/bound": "key-hash" } } });
        said("G5", "claimed, its owners as the claim left them", claimed.metadata?.ownerReferences?.map(({ kind, name }) => `${kind}/${name}`) ?? []);
        const owners = async () => (await kube.get<V1Pod>(ref("Pod", bound, NS)))?.metadata?.ownerReferences?.map(({ kind, name }) => `${kind}/${name}`) ?? null;
        await until("the bound pod released by its ReplicaSet", async () => ((await owners())?.length === 0 ? true : undefined), 60_000).catch(() => undefined);
        said("G5", "the bound pod's owners after its ReplicaSet saw it", await owners());
        await until("the pool's three free pods again", async () => ((await free()).length === 3 ? true : undefined), 60_000);
        said("G5", "free pods once replaced", (await free()).length);

        // Rolled out anew: a new template.
        await kube.patch(ref("Deployment", "pool", NS), { spec: { template: { metadata: { annotations: { "alasio.dev/template": "2" } } } } });
        await until("the pool rolled out", async () => {
          const pods = await free();
          return pods.length === 3 && pods.every((pod) => pod.metadata?.annotations?.["alasio.dev/template"] === "2" && pod.status?.phase === "Running") ? true : undefined;
        }, 180_000);
        const afterRollout = await kube.get<V1Pod>(ref("Pod", bound, NS));
        said("G5", "the bound pod after a rollout", { there: afterRollout !== null && !afterRollout.metadata?.deletionTimestamp, phase: afterRollout?.status?.phase });

        // Scaled to none, as KEDA does.
        await kube.patch(ref("Deployment", "pool", NS), { spec: { replicas: 0 } });
        await until("the pool at none", async () => ((await free()).length === 0 ? true : undefined), 120_000);
        const afterScale = await kube.get<V1Pod>(ref("Pod", bound, NS));
        said("G5", "the bound pod after a scale to none", { there: afterScale !== null && !afterScale.metadata?.deletionTimestamp, phase: afterScale?.status?.phase });

        // The Deployment deleted: the bound pod, no longer its ReplicaSet's, stays.
        await kube.remove(ref("Deployment", "pool", NS));
        await new Promise((resolve) => setTimeout(resolve, 15_000));
        const afterDelete = await kube.get<V1Pod>(ref("Pod", bound, NS));
        said("G5", "the bound pod after its Deployment's deletion", { there: afterDelete !== null && !afterDelete.metadata?.deletionTimestamp });
        assert.ok(afterRollout && afterScale && afterDelete);
      } finally {
        await kube.remove(ref("Namespace", NS)).catch(() => undefined);
      }
    });
  });
}
