import assert from "node:assert/strict";
import { test } from "node:test";

import type { V1Pod } from "@kubernetes/client-node";

import { decideCpuBaseline, describeBaseline, nodeCpuOf, sameBaseline, standingOf } from "../src/cpu-baseline.ts";

const RELEASE = "20990101.0";

/** A node agent's pod on `node`, its CPU features container ended as `terminated` says, if it has. */
const agentOn = (node: string | undefined, terminated?: { readonly exitCode: number; readonly message?: string }): V1Pod => ({
  ...(node ? { spec: { nodeName: node, containers: [] } } : {}),
  status: terminated ? { initContainerStatuses: [{ name: "cpu-features", ready: false, restartCount: 0, image: "alasio", imageID: "", state: { terminated } }] } : {},
});

const said = (features: readonly string[], release = RELEASE) => ({ exitCode: 0, message: JSON.stringify({ release, cpuFeatures: features }) });

test("a node agent's pod says its node's CPU features under the current release, or why they are not known", () => {
  assert.deepEqual(nodeCpuOf(agentOn("a", said(["sse", "avx"])), RELEASE), { node: "a", features: ["sse", "avx"] });
  assert.equal(nodeCpuOf(agentOn(undefined, said(["sse"])), RELEASE), null);
  assert.deepEqual(nodeCpuOf(agentOn("a"), RELEASE), { node: "a", unknown: "its node agent has not said its CPU features yet" });
  assert.deepEqual(nodeCpuOf(agentOn("a", { exitCode: 1, message: "runsc: no such file\n" }), RELEASE), { node: "a", unknown: "its node agent could not say its CPU features: runsc: no such file" });
  assert.deepEqual(nodeCpuOf(agentOn("a", { exitCode: 2 }), RELEASE), { node: "a", unknown: "its node agent could not say its CPU features: it exited 2" });
  assert.deepEqual(nodeCpuOf(agentOn("a", { exitCode: 0, message: "sse,avx" }), RELEASE), { node: "a", unknown: "its node agent said its CPU features as nothing alasio reads" });
  assert.deepEqual(nodeCpuOf(agentOn("a", said(["sse"], "20980101.0")), RELEASE), { node: "a", unknown: `its node agent said its CPU features under gVisor 20980101.0, not ${RELEASE}` });
});

test("the baseline is what every node has, in order, once every node has said; the one recorded while any has not; never empty", () => {
  const a = { node: "a", features: ["sse", "fma", "avx2", "avx"] };
  const b = { node: "b", features: ["avx", "sse", "avx2"] };
  const narrow = { node: "c", features: ["sse"] };
  const unsaid = { node: "d", unknown: "its node agent has not said its CPU features yet" };
  assert.deepEqual(decideCpuBaseline([a, b], null), { baseline: ["avx", "avx2", "sse"], held: null, lacking: [], unknown: [] });
  // Every node said: the baseline follows them, narrower or wider than the one recorded.
  assert.deepEqual(decideCpuBaseline([a, b, narrow], ["avx", "avx2", "sse"]).baseline, ["sse"]);
  assert.deepEqual(decideCpuBaseline([a], ["sse"]).baseline, ["avx", "avx2", "fma", "sse"]);
  // One has not: the one recorded stays, and a node that has said but lacks it is named.
  assert.deepEqual(decideCpuBaseline([a, narrow, unsaid], ["avx", "sse"]), { baseline: ["avx", "sse"], held: null, lacking: [{ node: "c", missing: ["avx"] }], unknown: [unsaid] });
  assert.deepEqual(decideCpuBaseline([a, unsaid], null), { baseline: null, held: "not every node's CPU features are known yet (d)", lacking: [], unknown: [unsaid] });
  // None shared, or no agent: none, which an empty annotation would not be.
  assert.deepEqual(decideCpuBaseline([a, { node: "e", features: ["neon"] }], ["sse"]), { baseline: null, held: "the nodes share no CPU feature under gVisor", lacking: [], unknown: [] });
  assert.deepEqual(decideCpuBaseline([{ node: "e", features: [] }], null).held, "the nodes share no CPU feature under gVisor");
  assert.deepEqual(decideCpuBaseline([], ["sse"]).baseline, ["sse"]);
  assert.equal(decideCpuBaseline([], null).held, "no node agent runs yet");
  assert.ok(sameBaseline(["avx", "sse"], ["avx", "sse"]) && sameBaseline(null, null));
  assert.ok(!sameBaseline(["avx", "sse"], ["sse"]) && !sameBaseline(null, ["sse"]));
});

test("status says the baseline alasio up recorded, the nodes that lack it since, and why there is none", () => {
  const joined = { node: "late", features: ["sse"] };
  assert.deepEqual(describeBaseline(standingOf([{ node: "a", features: ["avx", "sse"] }, joined], ["avx", "sse"])), [
    "sessions are held to 2 CPU features, those every node had as alasio up last surveyed them",
    "node late lacks avx of the CPU baseline: snapshots taken elsewhere are not restored there, and alasio up narrows the baseline to it",
  ]);
  assert.deepEqual(describeBaseline(standingOf([joined], null)), [
    "sessions are not snapshotted: they are held to no CPU baseline, as alasio up has not run since every node said its CPU features",
  ]);
  assert.deepEqual(describeBaseline(standingOf([{ node: "d", unknown: "its node agent has not said its CPU features yet" }], null)), [
    "sessions are not snapshotted: they are held to no CPU baseline, as not every node's CPU features are known yet (d)",
    "node d: its node agent has not said its CPU features yet",
  ]);
});
