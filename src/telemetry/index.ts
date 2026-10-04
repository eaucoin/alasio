/**
 * What the rest of alasio uses to record telemetry. Everything here is the
 * OpenTelemetry API, which does nothing until ./start.ts registers the SDK, so alasio
 * records the same way whether or not anything is exported.
 */
import {
  type Attributes,
  context,
  type Context as OtelContext,
  INVALID_SPAN_CONTEXT,
  isSpanContextValid,
  metrics,
  propagation,
  ROOT_CONTEXT,
  type Span,
  SpanKind,
  trace,
} from "@opentelemetry/api";
// By module: the package's index also loads its browser SDK, which alasio has not.
import * as OtelTracer from "@effect/opentelemetry/OtelTracer";
import * as Resource from "@effect/opentelemetry/Resource";
import { Cause, Context, Effect, Exit, type Fiber, Layer, Tracer } from "effect";

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
 * OtelTracer's tracer, except for where an effect runs outside every span of its own:
 * - under a span whose propagation is disabled (as outsideTraces makes), in the root
 *   context, where OtelTracer would put the no-op span Effect gives it, which
 *   instrumentation takes for a parent;
 * - under none, in the context its fiber started in, where OtelTracer would leave
 *   whatever it resumed in: a promise's callback resumes it in the context the promise
 *   was awaited in, an ended span's.
 */
const alasioTracer = Effect.map(OtelTracer.make, (otel) => {
  const evaluate = <X>(primitive: Tracer.EffectPrimitive<X>, fiber: Fiber.Fiber<unknown, unknown>): X => primitive["~effect/Effect/evaluate"](fiber);
  const inSpan = otel.context ?? evaluate;
  const startedIn = new WeakMap<Fiber.Fiber<unknown, unknown>, OtelContext>();
  return Tracer.make({
    span: (options) => otel.span(options),
    context: (primitive, fiber) => {
      let started = startedIn.get(fiber);
      if (started === undefined) startedIn.set(fiber, started = context.active());
      const span = fiber.cache.span;
      if (span === undefined) return context.with(started, () => evaluate(primitive, fiber));
      return Context.get(span.annotations, Tracer.DisablePropagation)
        ? context.with(ROOT_CONTEXT, () => evaluate(primitive, fiber))
        : inSpan(primitive, fiber);
    },
  });
});

/**
 * Effects' spans as OpenTelemetry spans of the SDK ./start.ts registers (none, when it
 * registers none): they nest under the active span and become it, so the
 * instrumented modules' spans nest under theirs. Their tracer is alasio's, named "alasio":
 * the resource given here names only the tracer (the SDK has its own), and a tracer
 * without a name is one OTLP cannot encode, which drops the batch its spans are in.
 */
export const TracingLayer: Layer.Layer<OtelTracer.OtelTracer> = Layer.effect(Tracer.Tracer, alasioTracer).pipe(
  Layer.provideMerge(OtelTracer.layerGlobalTracer),
  Layer.provide(Resource.layer({ serviceName: "alasio" })),
);

/** Where withAlasioSpan puts a span, and what it records on it at the start. */
export interface AlasioSpanOptions {
  readonly kind?: SpanKind;
  readonly attributes?: Attributes;
  /** A traceparent to continue, or null to start a trace of its own; the active span when absent. */
  readonly parent?: string | null | undefined;
}

/** A call withRpcCall records. */
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

/** The meter alasio's own metrics are created on. */
export const meter = metrics.getMeter("alasio");

const rpcDuration = meter.createHistogram("rpc.client.call.duration", {
  description: "Duration of the calls alasio makes to the Telegram Bot API and the Codex app-server",
  unit: "s",
});

/** The context a traceparent continues, or the root context for none. */
function contextOf(traceparent: string | null): OtelContext {
  return traceparent ? propagation.extract(ROOT_CONTEXT, { traceparent }) : ROOT_CONTEXT;
}

/** The `error.type` of what an effect failed with. */
function errorType(error: unknown): string {
  return error instanceof Error ? error.name : "Error";
}

/** Effect's name for an OpenTelemetry span kind. */
function effectSpanKind(kind: SpanKind): Tracer.SpanKind {
  switch (kind) {
    case SpanKind.SERVER:
      return "server";
    case SpanKind.CLIENT:
      return "client";
    case SpanKind.PRODUCER:
      return "producer";
    case SpanKind.CONSUMER:
      return "consumer";
    case SpanKind.INTERNAL:
      return "internal";
  }
}

/** Labels the active span with the type of what `cause` failed with. */
const labelFailure = <E>(cause: Cause.Cause<E>): Effect.Effect<void> =>
  OtelTracer.currentOtelSpan.pipe(
    Effect.flatMap((span) => Effect.sync(() => span.setAttribute("error.type", errorType(Cause.squash(cause))))),
    Effect.ignore,
  );

/**
 * `effect` in a span of alasio's: its parent the active span, unless `parent` is given (a
 * traceparent to continue, or null to start a trace of its own), and what it fails with
 * recorded on it and labelling it (`error.type`).
 */
export const withAlasioSpan = (name: string, { kind = SpanKind.INTERNAL, attributes = {}, parent }: AlasioSpanOptions = {}) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> => {
    const spanned = effect.pipe(
      Effect.tapCause(labelFailure),
      Effect.withSpan(name, { kind: effectSpanKind(kind), attributes, ...(parent === null ? { root: true } : {}) }),
    );
    if (parent === null) return spanned;
    // Effect makes a span with no parent of its own a root, so the active span (the
    // caller's, or the effect's own as the tracer keeps it active) is made its parent;
    // outside every trace (outsideTraces) there is none to continue.
    return Effect.suspend(() => {
      const continued = trace.getSpanContext(parent === undefined ? context.active() : contextOf(parent));
      return continued && isSpanContextValid(continued) ? spanned.pipe(OtelTracer.withSpanContext(continued)) : spanned;
    });
  };

/**
 * A call alasio makes to `service` (`system` names its protocol): a client span named
 * `<service>/<method>` and its duration, labelled with the error it failed with.
 */
export const withRpcCall = ({ system, service, method, attributes = {} }: RpcCallOptions) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
    Effect.suspend(() => {
      const startedAt = performance.now();
      return effect.pipe(
        withAlasioSpan(`${service}/${method}`, {
          kind: SpanKind.CLIENT,
          attributes: { "rpc.system.name": system, "rpc.service": service, "rpc.method": method, ...attributes },
        }),
        Effect.onExit((exit) => Effect.sync(() => {
          const failure = Exit.isFailure(exit) && !Cause.hasInterruptsOnly(exit.cause) ? errorType(Cause.squash(exit.cause)) : null;
          rpcDuration.record((performance.now() - startedAt) / 1000, {
            "rpc.system.name": system,
            "rpc.service": service,
            "rpc.method": method,
            ...(failure ? { "error.type": failure } : {}),
          });
        })),
      );
    });

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

/**
 * `effect` outside every trace, for work that outlives the span it starts in. Its parent
 * span is one that records nothing and propagates nothing: the spans it makes start
 * traces of their own, and what it runs sees an OpenTelemetry context with no span, so
 * nothing it starts (a process given a TRACEPARENT, an instrumented call) continues the
 * trace it was started in, and instrumentation that records only inside a trace (pg's)
 * records nothing.
 */
export const outsideTraces = <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
  Effect.withSpan(effect, "alasio.outside-traces", { root: true, annotations: Context.make(Tracer.DisablePropagation, true) });
