/**
 * alasio's metrics, as a test reads them. alasio creates its instruments on the
 * OpenTelemetry API's global meter as its modules load, so a test file imports this module
 * before any of alasio's: it registers a meter provider whose reader collects when asked.
 */
import { metrics } from "@opentelemetry/api";
import { DataPointType, MeterProvider, MetricReader } from "@opentelemetry/sdk-metrics";

class CollectingReader extends MetricReader {
  protected override async onForceFlush(): Promise<void> {}
  protected override async onShutdown(): Promise<void> {}
}

const reader = new CollectingReader();
metrics.setGlobalMeterProvider(new MeterProvider({ readers: [reader] }));

/** A point of a metric: its attributes, as `key=value` joined by commas in the order they were given, and its value, a histogram's count of values, and a histogram's sum of them. */
export interface CollectedPoint {
  readonly attributes: string;
  readonly value: number;
  readonly sum?: number | undefined;
}

/** The points of the metric `name`, collected now: running totals, since the test process started, for a counter or a histogram. */
export async function collected(name: string): Promise<CollectedPoint[]> {
  const { resourceMetrics } = await reader.collect();
  return resourceMetrics.scopeMetrics.flatMap((scope) => scope.metrics).filter((metric) => metric.descriptor.name === name).flatMap((metric) =>
    metric.dataPoints.map((point) => ({
      attributes: Object.entries(point.attributes).map(([key, value]) => `${key}=${String(value)}`).join(","),
      value: metric.dataPointType === DataPointType.HISTOGRAM ? (point.value as { count: number }).count : (point.value as number),
      ...(metric.dataPointType === DataPointType.HISTOGRAM ? { sum: (point.value as { sum?: number }).sum } : {}),
    }))
  );
}

/** What `effect` added to each point of the metric `name`, by its attributes, leaving out those it added nothing to. */
export async function added(name: string, effect: () => Promise<unknown>): Promise<Readonly<Record<string, number>>> {
  const before = new Map((await collected(name)).map(({ attributes, value }) => [attributes, value]));
  await effect();
  return Object.fromEntries((await collected(name)).flatMap(({ attributes, value }) => {
    const more = value - (before.get(attributes) ?? 0);
    return more === 0 ? [] : [[attributes, more]];
  }));
}

/** The values `effect` recorded of the histogram `name`, summed, by its points' attributes, leaving out those it recorded none of. */
export async function recorded(name: string, effect: () => Promise<unknown>): Promise<Readonly<Record<string, { readonly count: number; readonly sum: number }>>> {
  const before = new Map((await collected(name)).map((point) => [point.attributes, point]));
  await effect();
  return Object.fromEntries((await collected(name)).flatMap(({ attributes, value, sum = 0 }) => {
    const count = value - (before.get(attributes)?.value ?? 0);
    return count === 0 ? [] : [[attributes, { count, sum: sum - (before.get(attributes)?.sum ?? 0) }]];
  }));
}
