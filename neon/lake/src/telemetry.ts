/**
 * The lake's own telemetry: each event it logs, exported as an OTLP log record where
 * OpenTelemetry's standard variables say, which is the stack's collector, beside the
 * JSON line it writes. Its metrics are its Prometheus ones (./metrics.ts), which the
 * collector scrapes.
 */
import { OTLPLogExporter } from "@opentelemetry/exporter-logs-otlp-proto";
import { defaultResource, detectResources, envDetector, resourceFromAttributes } from "@opentelemetry/resources";
import { BatchLogRecordProcessor, LoggerProvider } from "@opentelemetry/sdk-logs";

/** Where the lake's events go as telemetry, once started. */
export interface LakeTelemetry {
  /** Exports an event, its message the body and its fields the attributes. */
  emit(message: string, fields: Record<string, unknown>): void;
  /** Exports what is still held, and stops. */
  shutdown(): Promise<void>;
}

/**
 * Starts exporting the lake's events, as the standard variables say, which the
 * exporter reads; null when they name no endpoint for logs, or turn the SDK off.
 */
export function startTelemetry(): LakeTelemetry | null {
  const { env } = process;
  const endpoint = env["OTEL_EXPORTER_OTLP_LOGS_ENDPOINT"]?.trim() || env["OTEL_EXPORTER_OTLP_ENDPOINT"]?.trim();
  if (!endpoint || env["OTEL_SDK_DISABLED"]?.trim().toLowerCase() === "true") return null;
  const provider = new LoggerProvider({
    // OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES override these.
    resource: defaultResource().merge(resourceFromAttributes({ "service.name": "alasio-lake" })).merge(detectResources({ detectors: [envDetector] })),
    processors: [new BatchLogRecordProcessor({ exporter: new OTLPLogExporter() })],
  });
  const logger = provider.getLogger("alasio-lake");
  return {
    emit(message, fields) {
      // An event's fields are JSON, as its line is.
      logger.emit({ body: message, attributes: JSON.parse(JSON.stringify(fields)) });
    },
    shutdown: () => provider.shutdown(),
  };
}
