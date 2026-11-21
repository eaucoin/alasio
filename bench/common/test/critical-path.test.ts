import assert from "node:assert/strict";
import { test } from "node:test";

import { cellShares, criticalPath, OUTSIDE, type SpanLine, stepName } from "../aggregate/critical-path.ts";

/** A span of `name` from `startMs` to `endMs`, under `parent` where given. */
const span = (name: string, startMs: number, endMs: number, parent = "", trace = "t"): SpanLine => ({
  name,
  traceId: trace,
  spanId: name,
  parentSpanId: parent,
  startNs: String(BigInt(startMs) * 1_000_000n),
  endNs: String(BigInt(endMs) * 1_000_000n),
});
const window = (fromMs: number, toMs: number) => ({ from_ns: String(BigInt(fromMs) * 1_000_000n), to_ns: String(BigInt(toMs) * 1_000_000n) });

test("a restore's critical path runs through the steps that ended last, each span's own time beside its children's, and what no span covers", () => {
  const spans = [
    span("front.request", 10, 90),
    span("binder.bind", 12, 80, "front.request"),
    span("sandbox.restore", 14, 40, "binder.bind"),
    // Under the bind beside the restore: on the path only for the millisecond before the restore began.
    span("binder.lease", 13, 20, "binder.bind"),
    span("sandbox.ready", 40, 78, "binder.bind"),
    // The agent's, in the gateway's trace, as the traceparent of the gateway's call continues it.
    span("agent.resume", 50, 75, "sandbox.ready"),
    // Another trace's, ended before the window's last step began: off the path.
    span("agent.fetch", 30, 60, "", "linked"),
  ];
  const steps = criticalPath({ ...window(0, 100), spans });
  assert.deepEqual(steps, [
    { name: OUTSIDE, ms: 10 },
    { name: "front.request", ms: 2 },
    { name: "binder.bind", ms: 1 },
    { name: "binder.lease", ms: 1 },
    { name: "sandbox.restore", ms: 26 },
    { name: "sandbox.ready", ms: 10 },
    { name: "agent.resume", ms: 25 },
    { name: "sandbox.ready", ms: 3 },
    { name: "binder.bind", ms: 2 },
    { name: "front.request", ms: 10 },
    { name: OUTSIDE, ms: 10 },
  ]);
  assert.equal(steps.reduce((sum, { ms }) => sum + ms, 0), 100);
});

test("a span that outlasts the window is cut at its end, and children of one trace nest across processes", () => {
  const spans = [span("ready", 0, 50), span("resume", 10, 200, "ready"), span("runsc", 20, 150, "resume")];
  assert.deepEqual(criticalPath({ ...window(0, 100), spans }), [
    { name: "ready", ms: 10 },
    { name: "resume", ms: 10 },
    { name: "runsc", ms: 30 },
    { name: OUTSIDE, ms: 50 },
  ]);
});

test("of a call's client span and its server's, which end together, the path goes through the one that says more", () => {
  const spans = [span("ready.resume", 0, 100), span("POST", 1, 99, "ready.resume"), span("http.server POST", 2, 98, "ready.resume"), span("agent.resume", 3, 97, "http.server POST")];
  assert.deepEqual(criticalPath({ ...window(0, 100), spans }).map(({ name }) => name), ["ready.resume", "POST", "http.server POST", "agent.resume", "http.server POST", "ready.resume"]);
});

test("a request's path runs through its own traces, not alasio's work in traces of their own beside it", () => {
  const spans = [
    span("alasio.front.request", 0, 90, "", "probe"),
    span("alasio.binder.bind", 5, 80, "alasio.front.request", "probe"),
    // A compaction, a trace of its own linked to the release's, ending later than the bind.
    span("alasio.node_agent.compaction", 2, 95, "", "compaction"),
    // Another session's request, begun before the window, as a mixed load's release.
    { ...span("alasio.front.request", -500, 98, "", "beside"), spanId: "beside-request" },
  ];
  assert.deepEqual(criticalPath({ ...window(0, 100), spans }).map(({ name }) => name), ["alasio.front.request", "alasio.binder.bind", "alasio.front.request", OUTSIDE]);
  // A save's phase, no request of the front among its spans: every span counts.
  assert.deepEqual(criticalPath({ ...window(0, 100), spans: [span("alasio.node_agent.checkpoint", 0, 100)] }).map(({ name }) => name), ["alasio.node_agent.checkpoint"]);
});

test("a cell's steps are said by their share of the window over its runs, nearest rank", () => {
  const shares = cellShares([
    { windowMs: 100, steps: [{ name: "a", ms: 60 }, { name: OUTSIDE, ms: 40 }] },
    { windowMs: 200, steps: [{ name: "a", ms: 180 }, { name: OUTSIDE, ms: 20 }] },
  ]);
  assert.deepEqual(shares.map(({ name }) => name), ["a", OUTSIDE]);
  assert.equal(shares[0]?.p50, 0.6);
  assert.equal(shares[0]?.p95, 0.9);
});

test("an HTTP span's step is its method with the path it asked or served, a workspace and an id in it said as such", () => {
  assert.equal(stepName({ name: "POST", attributes: { "url.path": "/resume", "server.address": "10.42.0.25" } }), "POST {address}/resume");
  assert.equal(stepName({ name: "http.server GET", attributes: { "url.path": "/snapshots/alasio-sessions/fs-de0dafaf0387" } }), "http.server GET /snapshots/alasio-sessions/{workspace}");
  assert.equal(stepName({ name: "GET", attributes: { "url.full": "http://alasio-seaweedfs.alasio.svc:8333/snapshots/alasio-sessions/fs-1a2b/layers/0123456789abcdef0123", "server.address": "alasio-seaweedfs.alasio.svc" } }), "GET alasio-seaweedfs.alasio.svc/snapshots/alasio-sessions/{workspace}/layers/{id}");
  assert.equal(stepName({ name: "alasio.sandbox.ready", attributes: { "url.path": "/x" } }), "alasio.sandbox.ready");
});
