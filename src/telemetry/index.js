/**
 * What the rest of alasio uses to record telemetry. Everything here is the
 * OpenTelemetry API, which does nothing until ./start.js registers the SDK, so alasio
 * records the same way whether or not anything is exported.
 */
import { context, INVALID_SPAN_CONTEXT, metrics, propagation, ROOT_CONTEXT, SpanKind, SpanStatusCode, trace } from "@opentelemetry/api";

export { SpanKind } from "@opentelemetry/api";
export { parseHeaders, resolveTelemetry, sharedResourceAttributes, SIGNALS, signalHeaders, telemetryEnabled, withoutTelemetry } from "./config.js";

const tracer = trace.getTracer("alasio");

/** The meter alasio's own metrics are created on. */
export const meter = metrics.getMeter("alasio");

const rpcDuration = meter.createHistogram("rpc.client.call.duration", {
  description: "Duration of the calls alasio makes to the Telegram Bot API and the Codex app-server",
  unit: "s",
});

/** The context a traceparent continues, or the root context for none. */
function contextOf(traceparent) {
  return traceparent ? propagation.extract(ROOT_CONTEXT, { traceparent }) : ROOT_CONTEXT;
}

/** The `error.type` of what was thrown. */
function errorType(error) {
  return error instanceof Error ? error.name : "Error";
}

/** What a span records of an error: the exception and its type, and an error status. */
function recordError(span, error) {
  span.recordException(error instanceof Error ? error : String(error));
  span.setAttribute("error.type", errorType(error));
  span.setStatus({ code: SpanStatusCode.ERROR, message: error instanceof Error ? error.message : String(error) });
}

/**
 * Runs `fn(span)` in a new span and ends the span when `fn` settles, recording what it
 * threw. The span's parent is the active span, unless `parent` is given: a traceparent
 * to continue, or null to start a trace of its own.
 */
export async function inSpan(name, { kind = SpanKind.INTERNAL, attributes = {}, parent } = {}, fn) {
  const parentContext = parent === undefined ? context.active() : contextOf(parent);
  return await tracer.startActiveSpan(name, { kind, attributes }, parentContext, async (span) => {
    try {
      return await fn(span);
    } catch (error) {
      recordError(span, error);
      throw error;
    } finally {
      span.end();
    }
  });
}

/**
 * A call alasio makes to `service` (`system` names its protocol): a client span named
 * `<service>/<method>` and its duration, labelled with the error it failed with.
 */
export async function rpcCall({ system, service, method, attributes = {} }, fn) {
  const startedAt = performance.now();
  let failure = null;
  try {
    return await inSpan(`${service}/${method}`, {
      kind: SpanKind.CLIENT,
      attributes: { "rpc.system.name": system, "rpc.service": service, "rpc.method": method, ...attributes },
    }, fn);
  } catch (error) {
    failure = errorType(error);
    throw error;
  } finally {
    rpcDuration.record((performance.now() - startedAt) / 1000, {
      "rpc.system.name": system,
      "rpc.service": service,
      "rpc.method": method,
      ...(failure ? { "error.type": failure } : {}),
    });
  }
}

/** The active span, or a span that records nothing when none is. */
export function currentSpan() {
  return trace.getActiveSpan() ?? trace.wrapSpanContext(INVALID_SPAN_CONTEXT);
}

/** The W3C trace context of the active span (`{ traceparent, tracestate? }`), or null. */
export function traceCarrier() {
  const carrier = {};
  propagation.inject(context.active(), carrier);
  return carrier.traceparent ? carrier : null;
}

/** The traceparent of the active span, kept with work that runs later, or null. */
export function currentTraceparent() {
  return traceCarrier()?.traceparent ?? null;
}

/** Runs `fn` outside every trace, for work that outlives the span it starts in. */
export function outsideTraces(fn) {
  return context.with(ROOT_CONTEXT, fn);
}
