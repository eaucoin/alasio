import assert from "node:assert/strict";
import { test } from "node:test";

import { checkpointAfter, compactionAtT0, frontWindow, liveDurableMs, orchestration, releaseDurableMs, resumeSteps, type Span, turnWait } from "../spans.ts";

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

test("a restore's orchestration is of the gateway's part of its first request, from its receipt at the front to bayma's answer of initialize, less runsc's restore and resume; the client's start and handshake are not of it", () => {
  // The probe's t0 at 1000; its first request at the front 55 ms on; the bind, then bayma's initialize; its answer at 1700.
  const front = span("alasio.front.request", 1055, 1700, { traceId: "bind", spanId: "front" });
  const initialize = span("initialize", 1650, 1690, { traceId: "bind", service: "bayma", attributes: {} });
  const next = span("alasio.front.request", 1740, 1750, { traceId: "next" });
  // Another workspace's initialize, in its own trace, earlier, which is not this one's.
  const other = span("initialize", 1100, 1110, { traceId: "other", service: "bayma", attributes: {} });
  // The run's next restore of the workspace, a minute on: its first request and bayma's initialize, of no use to this one's.
  const later = span("alasio.front.request", 61_000, 63_000, { traceId: "later" });
  const laterInitialize = span("initialize", 62_000, 62_100, { traceId: "later", service: "bayma", attributes: {} });
  const measured = { firstExecMs: 900, probeExecMs: 100, toRunningMs: 300, runscResumeMs: 150 };
  const client = { refused: false, requestToAnswerMs: 650 };
  assert.deepEqual(orchestration([other, front, initialize, next, later, laterInitialize], "fs-1", 1000, measured, client), { orchestrationMs: 645 - 450, withClientMs: 800 - 450, how: "front" });
  // Without bayma's span, the first request is the one that carried it, though a later restore's bayma said its own, unless the
  // client was refused and asked again.
  assert.deepEqual(frontWindow([front, next, later, laterInitialize], "fs-1", 1000, 1900, false), { receivedNs: 1_055_000_000n, answeredNs: 1_700_000_000n });
  assert.equal(frontWindow([front, next], "fs-1", 1000, 1900, true), null);
  // Without the front's spans, the client's own view of it, said so.
  assert.deepEqual(orchestration([], "fs-1", 1000, measured, client), { orchestrationMs: 650 - 450, withClientMs: 350, how: "client" });
  assert.deepEqual(orchestration([], "fs-1", 1000, measured, { refused: false, requestToAnswerMs: null }), { orchestrationMs: null, withClientMs: 350, how: null });
  // Without runsc's resume, none.
  assert.deepEqual(orchestration([front, initialize], "fs-1", 1000, { ...measured, runscResumeMs: null }, client), { orchestrationMs: null, withClientMs: null, how: null });
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
