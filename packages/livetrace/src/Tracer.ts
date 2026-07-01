import type { Context as EffectContext } from "effect/Context";
import type { Span } from "effect/Tracer";

/**
 * LiveTraceLayer - Effect Tracer decorator.
 *
 * Wraps whatever base tracer is in DefaultServices (native or OTel).
 * Intercepts span creation within `LiveTrace.withTrace()` scopes
 * and emits TraceEvents to a TraceSink.
 *
 * - Works standalone (no @effect/opentelemetry needed)
 * - Works alongside OTel (wraps the OTel tracer, both systems run)
 * - Zero FiberRef access in Tracer.span() - uses parent chain + attributes
 *
 * ## Layer composition: why ordering matters
 *
 * Effect tracer layers modify the current services - each tracer install
 * overwrites the previous tracer. When composing via `Layer.provideMerge`,
 * the argument (`self`) is built FIRST. In a pipe chain:
 *
 * ```
 * X.pipe(Layer.provideMerge(A), Layer.provideMerge(B))
 * ```
 *
 * Build order: B (outermost self) -> A (inner self) -> X
 *
 * For LiveTraceLayer to wrap OTel's tracer, TelemetryLive must build
 * BEFORE LiveTraceLayer so that this layer can capture the OTel tracer.
 * This means TelemetryLive must be OUTER (later in the pipe)
 * and LiveTraceLayer must be INNER (earlier in the pipe):
 *
 * ```ts
 * X.pipe(
 *   Layer.provideMerge(makeLiveTraceLayer()), // inner: builds 2nd, wraps OTel
 *   Layer.provideMerge(TelemetryLive),        // outer: builds 1st, sets OTel
 * )
 * ```
 */
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Tracer from "effect/Tracer";

import { TraceSink, type TraceSinkHandle } from "./Sink.js";
import { LIVE_TRACE, LIVE_TRACE_ID, LIVE_TRACE_SCOPE_ID, LIVE_TRACE_SCOPE_TYPE, type TraceScope } from "./types.js";
import { isWrappedSpan, shouldExclude, WrappedSpan } from "./WrappedSpan.js";

/**
 * Creates a LiveTraceLayer that wraps the current tracer.
 *
 * **Important**: This layer reads the current tracer from current services
 * at build time. For it to wrap the OTel tracer, TelemetryLive (or any
 * layer that installs a tracer) must be built BEFORE this layer. In a
 * `Layer.provideMerge` pipe chain, that means TelemetryLive should appear
 * AFTER (outer) this layer:
 *
 * @example
 * ```ts
 * const EnvLayer = ServerLive.pipe(
 *   Layer.provideMerge(ServicesLayer),
 *   // LiveTraceLayer BEFORE TelemetryLive in pipe = builds AFTER
 *   Layer.provideMerge(makeLiveTraceLayer()),
 *   // TelemetryLive AFTER in pipe = builds FIRST (sets OTel tracer)
 *   Layer.provideMerge(TelemetryLive),
 * )
 * ```
 */
export const LiveTraceLayer: Layer.Layer<never, never, TraceSink> = Layer.unwrap(
    Effect.map(Effect.all({ baseTracer: Effect.tracer, sink: TraceSink }), ({ baseTracer, sink }) =>
        Layer.succeed(
            Tracer.Tracer,
            Tracer.make({
                span(options) {
                    // In Effect v4 the tracer receives a single `options` object and
                    // span attributes arrive through `options.annotations` (a Context).
                    const attributes = attributesFromAnnotations(options.annotations);
                    const innerSpan = baseTracer.span(options);

                    // Should we exclude this span from live tracing?
                    if (shouldExclude(attributes)) {
                        return innerSpan;
                    }

                    // Check: is this the root of a live-traced scope?
                    if (attributes[LIVE_TRACE] === true) {
                        return createRootWrappedSpan(innerSpan, sink, attributes);
                    }

                    // Check: is the parent a WrappedSpan? (inside a traced scope)
                    if (Option.isSome(options.parent) && isWrappedSpan(options.parent.value)) {
                        const parentWrapped = options.parent.value;
                        const wrapped = new WrappedSpan(innerSpan, sink, parentWrapped.liveTraceId, parentWrapped.liveScope);

                        // IMPORTANT: span attributes (e.g. "ui.step") are applied by Effect
                        // AFTER tracer.span() returns, so innerSpan.attributes is empty here.
                        // We read them from the annotations Context instead.
                        sink.emit({
                            _tag: "SpanStart",
                            traceId: parentWrapped.liveTraceId,
                            spanId: innerSpan.spanId,
                            parentSpanId: parentWrapped.spanId,
                            name: options.name,
                            attributes: { ...Object.fromEntries(innerSpan.attributes), ...attributes },
                            timestamp: Date.now(),
                        });

                        return wrapped;
                    }

                    // Not in a traced scope, pass through unchanged
                    return innerSpan;
                },

                // Delegate context propagation to the base tracer when present.
                // Preserves OTel's OtelApi.context.with() behavior for W3C
                // traceparent header propagation.
                ...(baseTracer.context !== undefined ? { context: baseTracer.context.bind(baseTracer) } : {}),
            }),
        ),
    ),
);

const attributesFromAnnotations = (annotations: EffectContext<never>): Record<string, unknown> => Object.fromEntries(annotations.mapUnsafe);

function createRootWrappedSpan(innerSpan: Span, sink: TraceSinkHandle, attributes: Record<string, unknown>): WrappedSpan {
    const traceId = attributes[LIVE_TRACE_ID] as string;
    const scope: TraceScope = {
        type: (attributes[LIVE_TRACE_SCOPE_TYPE] as TraceScope["type"]) ?? "user",
        id: (attributes[LIVE_TRACE_SCOPE_ID] as string) ?? "unknown",
    };

    // Emit TraceStart
    sink.emit({
        _tag: "TraceStart",
        traceId,
        label: (attributes["live-trace.label"] as string) ?? innerSpan.name,
        scope,
        timestamp: Date.now(),
    });

    // Emit SpanStart for the root span itself
    // Include attributes from the annotations - Effect applies them AFTER tracer.span() returns
    sink.emit({
        _tag: "SpanStart",
        traceId,
        spanId: innerSpan.spanId,
        name: innerSpan.name,
        attributes: { ...Object.fromEntries(innerSpan.attributes), ...attributes },
        timestamp: Date.now(),
    });

    const wrapped = new WrappedSpan(innerSpan, sink, traceId, scope);

    // Override end to also emit TraceEnd after SpanEnd
    const originalEnd = wrapped.end.bind(wrapped);
    wrapped.end = (endTime: bigint, exit: any) => {
        originalEnd(endTime, exit);

        const startTime = innerSpan.status._tag === "Ended" ? innerSpan.status.startTime : BigInt(0);
        const durationMs = Number(endTime - startTime) / 1_000_000;

        sink.emit({
            _tag: "TraceEnd",
            traceId,
            status: exit._tag === "Success" ? "completed" : "failed",
            durationMs,
            error: exit._tag === "Failure" ? String(exit.cause) : undefined,
            timestamp: Date.now(),
        });
    };

    return wrapped;
}
