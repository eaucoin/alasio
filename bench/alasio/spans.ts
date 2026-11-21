/**
 * alasio's spans of a run, as its lake has them (otel.traces, neon/lake/src/otel.ts): every span of
 * every trace that holds one of the run's workspaces' spans, the node agents' (which continue the
 * gateway's traces, and link the work they start for later) and bayma's among them; and what the
 * benchmark reads of them: when a save was durable in the store, the resume's own steps, whether a
 * compaction ran as a restore began, and a turn's wait in the gateway. The reading is pure, for
 * ./test/spans.test.ts.
 */
import { lakeQuery } from "../../test/e2e/harness.ts";

/** A span, as the benchmark reads it: its times in nanoseconds of the wall clock, as every process on the runner shares it. */
export interface Span {
  readonly name: string;
  readonly service: string;
  readonly traceId: string;
  readonly spanId: string;
  readonly parentSpanId: string;
  readonly startNs: bigint;
  readonly endNs: bigint;
  readonly attributes: Readonly<Record<string, string>>;
  readonly events: readonly { readonly name: string; readonly atNs: bigint }[];
  readonly links: readonly { readonly traceId: string; readonly spanId: string }[];
}

/** A span as spans.jsonl keeps it, its times as decimal text, which JSON holds exactly. */
export type SpanLine = Omit<Span, "startNs" | "endNs" | "events"> & { readonly startNs: string; readonly endNs: string; readonly events: readonly { readonly name: string; readonly atNs: string }[] };

export const spanLine = (span: Span): SpanLine => ({ ...span, startNs: String(span.startNs), endNs: String(span.endNs), events: span.events.map(({ name, atNs }) => ({ name, atNs: String(atNs) })) });

/** A quoted SQL text literal. */
const literal = (text: string): string => `'${text.replaceAll("'", "''")}'`;

/**
 * The spans of every trace that holds a span of one of `workspaces` (its `alasio.sandbox.name`, or
 * the `alasio.volume.id` alasio stamps on its bayma's telemetry), from `fromMs` to `toMs` on the wall
 * clock, ordered by their start.
 */
export async function spansOf(workspaces: readonly string[], fromMs: number, toMs: number): Promise<Span[]> {
  if (workspaces.length === 0) return [];
  const names = workspaces.map(literal).join(", ");
  const within = `Timestamp BETWEEN make_timestamp(${Math.floor(fromMs) * 1000}) AND make_timestamp(${Math.ceil(toMs) * 1000})`;
  // The reader answers a map, a list or a struct as its JSON text (neon/lake/src/reader.ts).
  const rows = await lakeQuery(`
    WITH theirs AS (
      SELECT DISTINCT TraceId FROM otel.traces
      WHERE ${within} AND (SpanAttributes['alasio.sandbox.name'] IN (${names}) OR ResourceAttributes['alasio.volume.id'] IN (${names}))
    )
    SELECT SpanName AS name, ServiceName AS service, TraceId AS trace_id, SpanId AS span_id, ParentSpanId AS parent_span_id,
      CAST(epoch_ns(Timestamp) AS VARCHAR) AS start_ns, CAST(Duration AS VARCHAR) AS duration_ns,
      SpanAttributes AS attributes,
      list_transform(Events, event -> {'name': event.Name, 'at_ns': CAST(epoch_ns(event.Timestamp) AS VARCHAR)}) AS events,
      list_transform(Links, link -> {'trace_id': link.TraceId, 'span_id': link.SpanId}) AS links
    FROM otel.traces WHERE TraceId IN (SELECT TraceId FROM theirs)
    ORDER BY Timestamp`);
  return rows.map((row) => {
    const text = (key: string) => String(row[key] ?? "");
    const startNs = BigInt(text("start_ns"));
    return {
      name: text("name"),
      service: text("service"),
      traceId: text("trace_id"),
      spanId: text("span_id"),
      parentSpanId: text("parent_span_id"),
      startNs,
      endNs: startNs + BigInt(text("duration_ns") || "0"),
      attributes: JSON.parse(text("attributes") || "{}") as Record<string, string>,
      events: (JSON.parse(text("events") || "[]") as { readonly name: string; readonly at_ns: string }[]).map(({ name, at_ns }) => ({ name, atNs: BigInt(at_ns) })),
      links: (JSON.parse(text("links") || "[]") as { readonly trace_id: string; readonly span_id: string }[]).map(({ trace_id, span_id }) => ({ traceId: trace_id, spanId: span_id })),
    };
  });
}

/**
 * The spans of `workspaces` from `fromMs` to `toMs`, once the lake has each span `expected` asks
 * for (its name and, where given, an attribute it holds), within `waitMs`: what the telemetry's
 * path, the collector, the log and the lake's intake, had brought by then, which may lack some.
 */
export async function awaitSpans(workspaces: readonly string[], fromMs: number, toMs: number, expected: readonly ((spans: readonly Span[]) => boolean)[], waitMs = 180_000): Promise<{ readonly spans: Span[]; readonly complete: boolean; readonly waited_ms: number }> {
  const started = Date.now();
  for (;;) {
    const spans = await spansOf(workspaces, fromMs, toMs);
    const complete = expected.every((holds) => holds(spans));
    if (complete || Date.now() - started > waitMs) return { spans, complete, waited_ms: Date.now() - started };
    await new Promise((resolve) => setTimeout(resolve, 5000));
  }
}

/** Whether `spans` hold one named `name` of `workspace` that started at or after `fromNs`. */
export const hasSpan = (name: string, workspace: string, fromNs: bigint) => (spans: readonly Span[]): boolean =>
  spans.some((span) => span.name === name && span.attributes["alasio.sandbox.name"] === workspace && span.startNs >= fromNs);

const ms = (ns: bigint): number => Number(ns) / 1e6;
const msToNs = (wallMs: number): bigint => BigInt(Math.round(wallMs * 1e6));

/** The children of `parent` among `spans`, named `name`. */
const childNamed = (spans: readonly Span[], parent: Span, name: string): Span | undefined =>
  spans.find((span) => span.parentSpanId === parent.spanId && span.traceId === parent.traceId && span.name === name);

/**
 * The first checkpoint of `workspace` started at or after `fromMs` (a live one, or a release's), its
 * start and when its manifest was committed in the store, its `.commit` step's end; null where the
 * spans hold none.
 */
export function checkpointAfter(spans: readonly Span[], workspace: string, fromMs: number, live: boolean): { readonly startNs: bigint; readonly committedNs: bigint | null } | null {
  const taken = spans.find((span) => span.name === "alasio.node_agent.checkpoint" && span.attributes["alasio.sandbox.name"] === workspace && span.startNs >= msToNs(fromMs) && (span.attributes["alasio.snapshot.live"] === "true") === live);
  if (!taken) return null;
  return { startNs: taken.startNs, committedNs: childNamed(spans, taken, "alasio.node_agent.checkpoint.commit")?.endNs ?? null };
}

/** From a live checkpoint's ask, as the gateway's keepLive asks it, to its manifest committed in the store; null where not traced. */
export function liveDurableMs(spans: readonly Span[], workspace: string, fromMs: number): number | null {
  const taken = checkpointAfter(spans, workspace, fromMs, true);
  return taken?.committedNs ? ms(taken.committedNs - taken.startNs) : null;
}

/**
 * From a release's arrival at the gateway's front (its POST /release) to its checkpoint's manifest
 * committed in the store; null where not traced.
 */
export function releaseDurableMs(spans: readonly Span[], workspace: string, key: string, fromMs: number): number | null {
  const arrived = spans.find((span) => span.name === "alasio.front.request" && span.attributes["alasio.sandbox.key"] === key && span.attributes["url.path"] === "/release" && span.startNs >= msToNs(fromMs));
  const taken = checkpointAfter(spans, workspace, fromMs, false);
  return arrived && taken?.committedNs ? ms(taken.committedNs - arrived.startNs) : null;
}

/** What a restore's spans say of its resume on the node: runsc's resume with bayma's hook, and the wait for the pod's network policies; null where not traced. */
export function resumeSteps(spans: readonly Span[], workspace: string, fromMs: number): { readonly runscResumeMs: number | null; readonly policyWaitMs: number | null } {
  const resumed = spans.find((span) => span.name === "alasio.node_agent.resume" && span.attributes["alasio.sandbox.name"] === workspace && span.startNs >= msToNs(fromMs));
  if (!resumed) return { runscResumeMs: null, policyWaitMs: null };
  const runsc = childNamed(spans, resumed, "alasio.node_agent.resume.runsc");
  const waited = resumed.attributes["alasio.resume.policy_wait_ms"];
  return { runscResumeMs: runsc ? ms(runsc.endNs - runsc.startNs) : null, policyWaitMs: waited === undefined ? null : Number(waited) };
}

/**
 * Whether the release's compaction of `workspace`'s chain ran as a phase began at `t0Ms`: started
 * before it, its upload not through by then.
 */
export function compactionAtT0(spans: readonly Span[], workspace: string, t0Ms: number): "running" | "none" {
  const t0 = msToNs(t0Ms);
  const running = spans.some((span) => {
    if (span.name !== "alasio.node_agent.compaction" || span.attributes["alasio.sandbox.name"] !== workspace || span.startNs >= t0) return false;
    const uploaded = childNamed(spans, span, "alasio.node_agent.compaction.upload")?.endNs ?? span.endNs;
    return uploaded > t0;
  });
  return running ? "running" : "none";
}

/** Whether `span` is `ancestor` or under it, among `spans`, by their parents. */
function under(spans: readonly Span[], span: Span, ancestor: { readonly traceId: string; readonly spanId: string }): boolean {
  if (span.traceId !== ancestor.traceId) return false;
  const byId = new Map(spans.filter(({ traceId }) => traceId === span.traceId).map((each) => [each.spanId, each]));
  for (let at: Span | undefined = span, steps = 0; at !== undefined && steps < 64; at = byId.get(at.parentSpanId), steps++) {
    if (at.spanId === ancestor.spanId || at.parentSpanId === ancestor.spanId) return true;
  }
  return false;
}

/**
 * A turn's wait in the gateway, on its own clock: from the arrival of the turn's first request for
 * the session's key `key` at the front, at or after `fromMs` (today the bind that starts the turn),
 * to the moment the front handed the request of the turn's first exec to its proxy, the
 * `alasio.front.forwarded` event of the front's span under the harness's span of the exec `exec`,
 * which the request carried, of the turn's trace; with both moments, or null where either is not
 * traced.
 */
export function turnWait(spans: readonly Span[], key: string, exec: { readonly traceId: string; readonly spanId: string }, fromMs: number): { readonly firstNs: bigint; readonly forwardedNs: bigint; readonly waitMs: number } | null {
  const first = spans.filter((span) => span.name === "alasio.front.request" && span.attributes["alasio.sandbox.key"] === key && span.startNs >= msToNs(fromMs))
    .reduce<Span | null>((earliest, span) => (earliest === null || span.startNs < earliest.startNs ? span : earliest), null);
  const forwarded = spans.filter((span) => span.name === "alasio.front.request" && under(spans, span, exec))
    .flatMap((span) => span.events.filter(({ name }) => name === "alasio.front.forwarded").map(({ atNs }) => atNs))
    .reduce<bigint | null>((earliest, at) => (earliest === null || at < earliest ? at : earliest), null);
  return first && forwarded !== null ? { firstNs: first.startNs, forwardedNs: forwarded, waitMs: ms(forwarded - first.startNs) } : null;
}
