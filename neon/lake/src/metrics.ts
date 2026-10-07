/**
 * The lake's own metrics, in Prometheus's text format, for the stack's telemetry
 * collector to scrape (cli/src/manifests/collector.ts): how its loads, its maintenance
 * and its telemetry intake go, and how much each changed.
 */

interface Metric {
  type: "counter" | "gauge";
  help: string;
}

const METRICS = {
  lake_cycles_total: { type: "counter", help: "Loads run, by outcome" },
  lake_rows_total: { type: "counter", help: "Rows loaded into or deleted from the lake, by table and change" },
  lake_cycle_duration_seconds: { type: "gauge", help: "How long the last load took" },
  lake_last_success_timestamp_seconds: { type: "gauge", help: "When a load last succeeded" },
  lake_maintenance_total: { type: "counter", help: "Maintenance passes run, by outcome, and whether they deleted files or kept them for Neon's branches" },
  lake_telemetry_requests_total: { type: "counter", help: "OTLP requests the telemetry intake took, by signal and outcome" },
  lake_telemetry_rows_total: { type: "counter", help: "Rows the telemetry intake wrote, by table" },
} satisfies Record<string, Metric>;

export type MetricName = keyof typeof METRICS;

/** A series' labels, by name. */
export type Labels = Readonly<Record<string, string>>;

/** The lake's metrics: counters added to, gauges set, all rendered for a scrape. */
export interface Metrics {
  add(name: MetricName, labels?: Labels, value?: number): void;
  set(name: MetricName, labels: Labels | undefined, value: number): void;
  /** The exposition text, every metric with its HELP and TYPE. */
  render(): string;
}

const labelText = (labels: Labels) => {
  const pairs = Object.entries(labels).map(([name, value]) => `${name}="${String(value).replaceAll("\\", "\\\\").replaceAll("\"", "\\\"").replaceAll("\n", "\\n")}"`);
  return pairs.length ? `{${pairs.join(",")}}` : "";
};

export function createMetrics(): Metrics {
  const series = new Map(Object.entries(METRICS).map(([name, metric]) => [name, { metric, values: new Map<string, number>() }]));
  const at = (name: string, labels: Labels) => {
    const metric = series.get(name);
    if (!metric) throw new Error(`unknown metric ${name}`);
    return [metric.values, labelText(labels)] as const;
  };
  return {
    add(name, labels = {}, value = 1) {
      const [values, key] = at(name, labels);
      values.set(key, (values.get(key) ?? 0) + value);
    },
    set(name, labels = {}, value) {
      const [values, key] = at(name, labels);
      values.set(key, value);
    },
    /** The exposition text, every metric with its HELP and TYPE. */
    render() {
      return [...series].flatMap(([name, { metric, values }]) => [
        `# HELP ${name} ${metric.help}`,
        `# TYPE ${name} ${metric.type}`,
        ...[...values].map(([labels, value]) => `${name}${labels} ${value}`),
      ]).join("\n") + "\n";
    },
  };
}
