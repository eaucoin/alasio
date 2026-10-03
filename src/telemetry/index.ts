/**
 * What the rest of alasio uses to record telemetry. Everything here is the
 * OpenTelemetry API, which does nothing until ./start.ts registers the SDK, so alasio
 * records the same way whether or not anything is exported.
 */
import {
  type Attributes,
  context,
  type Context,
  INVALID_SPAN_CONTEXT,
  metrics,
  propagation,
  ROOT_CONTEXT,
  type Span,
  SpanKind,
  SpanStatusCode,
  trace,
} from "@opentelemetry/api";
// By module: the package's index also loads its browser SDK, which alasio has not.
import * as OtelTracer from "@effect/opentelemetry/OtelTracer";
import * as Resource from "@effect/opentelemetry/Resource";
import { Layer } from "effect";

export { SpanKind } from "@opentelemetry/api";
export {
  conversationTelemetryEnv,
  parseKeyValueList,
  resolveTelemetry,
  sharedResourceAttributes,
  SIGNALS,
  signalHeaders,
  signalSetting,
  telemetryEnabled,
  withoutTelemetry,
} from "./config.ts";
export type { Signal, SignalExporter, Telemetry } from "./config.ts";

/**
 * Effects' spans as OpenTelemetry spans of the SDK ./start.ts registers (none, when it
 * registers none): they nest under the active span and become it, so the
 * instrumented modules' spans nest under theirs.
 */
export const TracingLayer: Layer.Layer<OtelTracer.OtelTracer> = OtelTracer.layerGlobal.pipe(Layer.provide(Resource.layerFromEnv()));

/** Where inSpan puts a span, and what it records on it at the start. */
export interface InSpanOptions {
  readonly kind?: SpanKind;
  readonly attributes?: Attributes;
  /** A traceparent to continue, or null to start a trace of its own; the active span when absent. */
  readonly parent?: string | null | undefined;
}

/** A call rpcCall records. */
export interface RpcCallOptions {
  readonly system: string;
  readonly service: string;
  readonly method: string;
  readonly attributes?: Attributes;
}

/** The W3C trace context of a span, as propagated. */
export interface TraceCarrier {
  traceparent?: string;
  tracestate?: string;
}

const tracer = trace.getTracer("alasio");

/** The meter alasio's own metrics are created on. */
export const meter = metrics.getMeter("alasio");

const rpcDuration = meter.createHistogram("rpc.client.call.duration", {
  description: "Duration of the calls alasio makes to the Telegram Bot API and the Codex app-server",
  unit: "s",
});

/** The context a traceparent continues, or the root context for none. */
function contextOf(traceparent: string | null): Context {
  return traceparent ? propagation.extract(ROOT_CONTEXT, { traceparent }) : ROOT_CONTEXT;
}

/** The `error.type` of what was thrown. */
function errorType(error: unknown): string {
  return error instanceof Error ? error.name : "Error";
}

/** What a span records of an error: the exception and its type, and an error status. */
function recordError(span: Span, error: unknown): void {
  span.recordException(error instanceof Error ? error : String(error));
  span.setAttribute("error.type", errorType(error));
  span.setStatus({ code: SpanStatusCode.ERROR, message: error instanceof Error ? error.message : String(error) });
}

/**
 * Runs `fn(span)` in a new span and ends the span when `fn` settles, recording what it
 * threw. The span's parent is the active span, unless `parent` is given: a traceparent
 * to continue, or null to start a trace of its own.
 */
export async function inSpan<T>(
  name: string,
  { kind = SpanKind.INTERNAL, attributes = {}, parent }: InSpanOptions = {},
  fn: (span: Span) => T | Promise<T>,
): Promise<T> {
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
export async function rpcCall<T>(
  { system, service, method, attributes = {} }: RpcCallOptions,
  fn: (span: Span) => T | Promise<T>,
): Promise<T> {
  const startedAt = performance.now();
  let failure: string | null = null;
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
export function currentSpan(): Span {
  return trace.getActiveSpan() ?? trace.wrapSpanContext(INVALID_SPAN_CONTEXT);
}

/** The W3C trace context of the active span (`{ traceparent, tracestate? }`), or null. */
export function traceCarrier(): TraceCarrier | null {
  const carrier: TraceCarrier = {};
  propagation.inject(context.active(), carrier);
  return carrier.traceparent ? carrier : null;
}

/** The traceparent of the active span, kept with work that runs later, or null. */
export function currentTraceparent(): string | null {
  return traceCarrier()?.traceparent ?? null;
}

/** Runs `fn` outside every trace, for work that outlives the span it starts in. */
export function outsideTraces<T>(fn: () => T): T {
  return context.with(ROOT_CONTEXT, fn);
}
