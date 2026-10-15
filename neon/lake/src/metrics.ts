/**
 * The lake's own metrics, in Prometheus's text format, for the stack's telemetry
 * collector to scrape (cli/src/manifests/collector.ts): how the lake service's loads and
 * maintenance go, and the telemetry intake's batches (each a process of its own), how much
 * each changed, and the memory the process holds.
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
  lake_telemetry_records_total: { type: "counter", help: "Records of the log's the telemetry intake consumed, by topic and outcome: written, malformed, or repeated (written before)" },
  lake_telemetry_batches_total: { type: "counter", help: "Batches the telemetry intake failed to write, which it reads again" },
  lake_telemetry_commit_seconds: { type: "gauge", help: "How long the telemetry intake's last commit to the lake took" },
  lake_telemetry_merges_total: { type: "counter", help: "Hourly merges of the telemetry's files, by outcome" },
  lake_telemetry_rows_total: { type: "counter", help: "Rows the telemetry intake wrote, by table" },
  lake_telemetry_duplicate_rows_total: { type: "counter", help: "Rows the telemetry intake did not write as the lake has them, by table: what the log holds twice" },
  lake_telemetry_reread_gaps_total: { type: "counter", help: "Rereads of what the lake lost, by signal, that the log no longer held all of, its retention past it" },
  lake_telemetry_reread_gap_seconds: { type: "gauge", help: "The time the last such reread could not refill, by signal, in seconds" },
  lake_telemetry_lag_records: { type: "gauge", help: "Records the telemetry intake's group has yet to read, by topic, as a member last asked the log" },
  lake_resident_memory_bytes: { type: "gauge", help: "The process's resident memory, what its container's memory limit counts" },
  lake_heap_used_bytes: { type: "gauge", help: "What V8's heap holds" },
  lake_duckdb_memory_bytes: { type: "gauge", help: "What the buffer manager of its DuckDB, the loader's or the intake's, holds; 0 while it is not open" },
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

/** A DuckDB whose memory is reported: the lake as openLake opened it (./lake.ts). */
export interface MeteredDuckDB {
  memory(): Promise<number>;
}

/**
 * Sets the memory gauges: the process's resident memory and V8 heap, and the memory each
 * of `duckdbs` holds, by name, 0 for one not open. Run before each scrape is answered.
 */
export async function sampleMemory(metrics: Pick<Metrics, "set">, duckdbs: Readonly<Record<string, MeteredDuckDB | null>>): Promise<void> {
  const { rss, heapUsed } = process.memoryUsage();
  metrics.set("lake_resident_memory_bytes", {}, rss);
  metrics.set("lake_heap_used_bytes", {}, heapUsed);
  for (const [duckdb, open] of Object.entries(duckdbs)) {
    metrics.set("lake_duckdb_memory_bytes", { duckdb }, open ? await open.memory() : 0);
  }
}
