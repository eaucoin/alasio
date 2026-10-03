// @ts-nocheck
/**
 * The lake's own metrics, in Prometheus's text format, for the stack's telemetry
 * collector to scrape (neon/control/setup.js): how its loads and maintenance go,
 * and how much each changed.
 */

const METRICS = {
  lake_cycles_total: { type: "counter", help: "Loads run, by outcome" },
  lake_rows_total: { type: "counter", help: "Rows loaded into or deleted from the lake, by table and change" },
  lake_cycle_duration_seconds: { type: "gauge", help: "How long the last load took" },
  lake_last_success_timestamp_seconds: { type: "gauge", help: "When a load last succeeded" },
  lake_maintenance_total: { type: "counter", help: "Maintenance passes run, by outcome" },
};

const labelText = (labels) => {
  const pairs = Object.entries(labels).map(([name, value]) => `${name}="${String(value).replaceAll("\\", "\\\\").replaceAll("\"", "\\\"").replaceAll("\n", "\\n")}"`);
  return pairs.length ? `{${pairs.join(",")}}` : "";
};

export function createMetrics() {
  const series = new Map(Object.keys(METRICS).map((name) => [name, new Map()]));
  const at = (name, labels) => {
    if (!series.has(name)) throw new Error(`unknown metric ${name}`);
    return [series.get(name), labelText(labels)];
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
      return [...series].flatMap(([name, values]) => [
        `# HELP ${name} ${METRICS[name].help}`,
        `# TYPE ${name} ${METRICS[name].type}`,
        ...[...values].map(([labels, value]) => `${name}${labels} ${value}`),
      ]).join("\n") + "\n";
    },
  };
}
