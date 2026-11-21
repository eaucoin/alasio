import assert from "node:assert/strict";
import { test } from "node:test";

import { checkpointAfter, compactionAtT0, liveDurableMs, releaseDurableMs, resumeSteps, type Span, turnWait } from "../spans.ts";

/** A span of `name`, from `startMs` to `endMs` on the wall clock. */
function span(name: string, startMs: number, endMs: number, more: Partial<Omit<Span, "name" | "startNs" | "endNs">> = {}): Span {
  return {
    name,
    service: "alasio",
    traceId: "t1",
    spanId: `${name}@${startMs}`,
    parentSpanId: "",
    attributes: { "alasio.sandbox.name": "fs-1" },
    events: [],
    links: [],
    ...more,
    startNs: BigInt(startMs) * 1_000_000n,
    endNs: BigInt(endMs) * 1_000_000n,
  };
}

/** `child`, a child of `parent`, in its trace. */
const under = (parent: Span, child: Span): Span => ({ ...child, traceId: parent.traceId, parentSpanId: parent.spanId });

test("a live save is durable as its checkpoint's manifest is committed, from its ask", () => {
  const live = span("alasio.node_agent.checkpoint", 1000, 5000, { attributes: { "alasio.sandbox.name": "fs-1", "alasio.snapshot.live": "true" } });
  const spans = [live, under(live, span("alasio.node_agent.checkpoint.commit", 4000, 4600))];
  assert.equal(liveDurableMs(spans, "fs-1", 900), 3600);
  // Not another workspace's, nor one asked before the phase, nor a release's.
  assert.equal(liveDurableMs(spans, "fs-2", 900), null);
  assert.equal(liveDurableMs(spans, "fs-1", 1100), null);
  assert.equal(checkpointAfter(spans, "fs-1", 900, false), null);
});

test("a release is durable from its arrival at the front to its checkpoint's commit, a checkpoint whose commit is not traced saying nothing", () => {
  const arrived = span("alasio.front.request", 2000, 9000, { traceId: "t2", attributes: { "alasio.sandbox.key": "key-1", "url.path": "/release", "alasio.sandbox.name": "fs-1" } });
  const taken = span("alasio.node_agent.checkpoint", 2100, 8000, { traceId: "t2", attributes: { "alasio.sandbox.name": "fs-1", "alasio.snapshot.live": "false" } });
  const committed = under(taken, span("alasio.node_agent.checkpoint.commit", 7000, 7500));
  assert.equal(releaseDurableMs([arrived, taken, committed], "fs-1", "key-1", 1500), 5500);
  assert.equal(releaseDurableMs([arrived, taken], "fs-1", "key-1", 1500), null);
});

test("a resume says runsc's resume with the hook as its step's length, and the policy wait as its attribute", () => {
  const resumed = span("alasio.node_agent.resume", 3000, 4500, { attributes: { "alasio.sandbox.name": "fs-1", "alasio.resume.policy_wait_ms": "212" } });
  const spans = [resumed, under(resumed, span("alasio.node_agent.resume.runsc", 3300, 4100))];
  assert.deepEqual(resumeSteps(spans, "fs-1", 2900), { runscResumeMs: 800, policyWaitMs: 212 });
  assert.deepEqual(resumeSteps(spans, "fs-1", 3100), { runscResumeMs: null, policyWaitMs: null });
});

test("a compaction runs at a phase's start where it began before it and its upload ended after", () => {
  const compaction = span("alasio.node_agent.compaction", 1000, 9000);
  const uploaded = under(compaction, span("alasio.node_agent.compaction.upload", 4000, 6000));
  assert.equal(compactionAtT0([compaction, uploaded], "fs-1", 5000), "running");
  assert.equal(compactionAtT0([compaction, uploaded], "fs-1", 7000), "none");
  assert.equal(compactionAtT0([compaction, uploaded], "fs-1", 500), "none");
  // Without its upload's step, the compaction's own end.
  assert.equal(compactionAtT0([compaction], "fs-1", 7000), "running");
});

test("a turn waits from its first request for the session's key at the front to the forward of its exec's request, the one under the harness's span of it in the turn's trace", () => {
  const bind = span("alasio.front.request", 10_000, 12_000, { traceId: "turn", attributes: { "alasio.sandbox.key": "key-1", "url.path": "/bind" } });
  // The turn's other requests, of its trace too, forwarded before the exec's.
  const initialize = span("alasio.front.request", 15_000, 15_050, { traceId: "turn", attributes: { "alasio.sandbox.key": "key-1" }, events: [{ name: "alasio.front.forwarded", atNs: 15_010_000_000n }] });
  const exec = span("bayma.exec", 15_090, 15_400, { traceId: "turn", spanId: "exec" });
  const served = span("POST", 15_100, 15_300, { traceId: "turn", spanId: "served", parentSpanId: "exec" });
  const front = span("alasio.front.request", 15_100, 15_300, { traceId: "turn", parentSpanId: "served", attributes: { "alasio.sandbox.key": "key-1" }, events: [{ name: "alasio.front.forwarded", atNs: 15_120_000_000n }] });
  const earlier = span("alasio.front.request", 1000, 1100, { traceId: "before", attributes: { "alasio.sandbox.key": "key-1" } });
  const spans = [earlier, bind, initialize, exec, served, front];
  assert.deepEqual(turnWait(spans, "key-1", { traceId: "turn", spanId: "exec" }, 9000), { firstNs: 10_000_000_000n, forwardedNs: 15_120_000_000n, waitMs: 5120 });
  assert.equal(turnWait([bind, initialize, exec], "key-1", { traceId: "turn", spanId: "exec" }, 9000), null);
  assert.equal(turnWait(spans, "key-2", { traceId: "turn", spanId: "exec" }, 9000), null);
});
