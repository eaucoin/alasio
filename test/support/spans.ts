/**
 * alasio's spans, as a test reads them: an SDK registered as alasio's start registers one
 * (../../src/telemetry/start.ts), its context and W3C propagation with it, which keeps the
 * spans that end in memory, and exports no other signal. What a test runs with TracingLayer
 * records its spans here.
 */
import { NodeSDK } from "@opentelemetry/sdk-node";
import { InMemorySpanExporter, type ReadableSpan, SimpleSpanProcessor } from "@opentelemetry/sdk-trace-base";

const exporter = new InMemorySpanExporter();
// Metrics are read as ./metrics.ts registers them, and logs not at all.
process.env["OTEL_METRICS_EXPORTER"] = "none";
process.env["OTEL_LOGS_EXPORTER"] = "none";
new NodeSDK({ autoDetectResources: false, spanProcessors: [new SimpleSpanProcessor(exporter)], instrumentations: [] }).start();

/** The spans that ended since the test process started, in the order they ended. */
export const ended = (): readonly ReadableSpan[] => exporter.getFinishedSpans();

/** The spans that ended named `name`, in the order they ended. */
export const endedNamed = (name: string): readonly ReadableSpan[] => ended().filter((span) => span.name === name);

/** The span named `name` that ended last, which there must be. */
export function endedSpan(name: string): ReadableSpan {
  const span = ended().findLast((each) => each.name === name);
  if (span === undefined) throw new Error(`no span ${name} ended, of ${[...new Set(ended().map((each) => each.name))].join(", ")}`);
  return span;
}

/** The spans that ended whose parent is `parent`, in the order they began. */
export const childrenOf = (parent: ReadableSpan): readonly ReadableSpan[] =>
  ended()
    .filter((span) => span.parentSpanContext?.spanId === parent.spanContext().spanId)
    .toSorted((a, b) => a.startTime[0] - b.startTime[0] || a.startTime[1] - b.startTime[1]);

/** Whether `span` is `ancestor` or a descendant of it, among the spans that ended. */
export function descendsFrom(span: ReadableSpan, ancestor: ReadableSpan): boolean {
  const byId = new Map(ended().map((each) => [each.spanContext().spanId, each]));
  for (let at: ReadableSpan | undefined = span; at !== undefined; at = byId.get(at.parentSpanContext?.spanId ?? "")) {
    if (at.spanContext().spanId === ancestor.spanContext().spanId) return true;
  }
  return false;
}
