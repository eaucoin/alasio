/**
 * The lake's own telemetry: each event it logs, exported as an OTLP log record where
 * OpenTelemetry's standard variables say, which is the stack's collector, beside the
 * JSON line it writes. Its metrics are its Prometheus ones (./metrics.ts), which the
 * collector scrapes, but for those of maintenance's passes, each a process that ends before
 * a scrape would come, which exports its own as it ends.
 */
import { OTLPLogExporter } from "@opentelemetry/exporter-logs-otlp-proto";
import { OTLPMetricExporter } from "@opentelemetry/exporter-metrics-otlp-proto";
import { defaultResource, detectResources, envDetector, resourceFromAttributes } from "@opentelemetry/resources";
import { BatchLogRecordProcessor, LoggerProvider } from "@opentelemetry/sdk-logs";
import { MeterProvider, PeriodicExportingMetricReader } from "@opentelemetry/sdk-metrics";

/** Where the lake's events go as telemetry, once started. */
export interface LakeTelemetry {
  /** Exports an event, its message the body and its fields the attributes. */
  emit(message: string, fields: Record<string, unknown>): void;
  /** Records the gauge `name` at `value`, of `attributes`, exported as the process ends. */
  gauge(name: string, value: number, attributes: Record<string, string>): void;
  /** Exports what is still held, and stops. */
  shutdown(): Promise<void>;
}

/**
 * Starts exporting the lake's events, as the standard variables say, which the
 * exporters read; null when they name no endpoint for logs, or turn the SDK off.
 */
export function startTelemetry(): LakeTelemetry | null {
  const { env } = process;
  const endpoint = env["OTEL_EXPORTER_OTLP_LOGS_ENDPOINT"]?.trim() || env["OTEL_EXPORTER_OTLP_ENDPOINT"]?.trim();
  if (!endpoint || env["OTEL_SDK_DISABLED"]?.trim().toLowerCase() === "true") return null;
  // OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES override these.
  const resource = defaultResource().merge(resourceFromAttributes({ "service.name": "alasio-lake" })).merge(detectResources({ detectors: [envDetector] }));
  const provider = new LoggerProvider({ resource, processors: [new BatchLogRecordProcessor({ exporter: new OTLPLogExporter() })] });
  const logger = provider.getLogger("alasio-lake");
  // Made with the first gauge, as the processes that serve their metrics record none; its one
  // export is the one its shutdown makes.
  let meters: MeterProvider | null = null;
  return {
    emit(message, fields) {
      // An event's fields are JSON, as its line is.
      logger.emit({ body: message, attributes: JSON.parse(JSON.stringify(fields)) });
    },
    gauge(name, value, attributes) {
      meters ??= new MeterProvider({ resource, readers: [new PeriodicExportingMetricReader({ exporter: new OTLPMetricExporter(), exportIntervalMillis: 3_600_000 })] });
      meters.getMeter("alasio-lake").createGauge(name).record(value, attributes);
    },
    shutdown: async () => {
      await Promise.all([provider.shutdown(), meters?.shutdown()]);
    },
  };
}
