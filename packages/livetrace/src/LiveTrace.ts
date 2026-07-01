import type { AnySpan } from "effect/Tracer";

/**
 * LiveTrace — User-facing API for starting traced scopes.
 *
 * Usage:
 * ```ts
 * yield* pipe(
 *   myWorkflow,
 *   LiveTrace.withTrace({
 *     traceId: `doc:${documentId}`,
 *     label: "Processing report.pdf",
 *     scope: { type: "team", id: teamId },
 *   }),
 * )
 * ```
 *
 * Inside the scope, all `Effect.withSpan` and `Effect.log` calls
 * are automatically captured and streamed to the frontend.
 */
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

import * as FiberRef from "./effect-fiber-ref.js";

import {
    LIVE_TRACE,
    LIVE_TRACE_ID,
    LIVE_TRACE_LABEL,
    LIVE_TRACE_PROVIDER,
    LIVE_TRACE_SCOPE_ID,
    LIVE_TRACE_SCOPE_TYPE,
    type TraceScope,
    UI_STEP,
} from "./types.js";

export interface LiveTraceConfig {
    /** Logical trace ID for stream routing (e.g., "doc:abc123") */
    readonly traceId: string;
    /** Human-readable label (e.g., filename) */
    readonly label: string;
    /** Stream routing scope */
    readonly scope: TraceScope;
    /** Optional provider key for source-page filtering (e.g. "notion", "google-drive") */
    readonly provider?: string;
}

/**
 * FiberRef that holds the current WrappedSpan (if inside a withTrace/step scope).
 * Used by LiveTraceLogger to bridge Effect.log() → SpanEvent automatically.
 * Inherited by child fibers so forked work stays attributed to the correct span.
 */
export const LiveSpanRef: FiberRef.FiberRef<AnySpan | null> = FiberRef.unsafeMake<AnySpan | null>(null);

const spanAnnotations = (attributes: Record<string, unknown>): Context.Context<never> => Context.makeUnsafe(new Map(Object.entries(attributes)));

/**
 * After Effect.withSpan creates the span, read it via Effect.currentSpan
 * and store it in LiveSpanRef so the Logger can access it synchronously.
 */
const propagateSpan = <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
    Effect.gen(function* () {
        const span = yield* Effect.currentSpan.pipe(Effect.orElseSucceed(() => null));
        const previous = yield* FiberRef.get(LiveSpanRef);
        yield* FiberRef.set(LiveSpanRef, span);
        return yield* effect.pipe(Effect.ensuring(FiberRef.set(LiveSpanRef, previous)));
    }) as Effect.Effect<A, E, R>;

/**
 * Wrap an effect in a live-traced scope.
 *
 * All `Effect.withSpan` and `Effect.log` calls within this scope
 * are captured by the LiveTraceLayer and streamed to the sink.
 */
export const withTrace =
    (config: LiveTraceConfig) =>
    <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> => {
        const attributes = {
            [LIVE_TRACE]: true,
            [LIVE_TRACE_ID]: config.traceId,
            [LIVE_TRACE_LABEL]: config.label,
            [LIVE_TRACE_SCOPE_TYPE]: config.scope.type,
            [LIVE_TRACE_SCOPE_ID]: config.scope.id,
            ...(config.provider !== undefined ? { [LIVE_TRACE_PROVIDER]: config.provider } : {}),
        };

        return Effect.withSpan(propagateSpan(effect), config.label, {
            attributes,
            annotations: spanAnnotations(attributes),
        });
    };

/**
 * Create a traced step span. Shows as a top-level section in the UI.
 *
 * ```ts
 * yield* LiveTrace.step("Parsing")(parseDocument(doc))
 * yield* LiveTrace.step("Embedding")(embedChunks(chunks))
 * ```
 */
export const step =
    (name: string, attributes?: Record<string, unknown>) =>
    <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> => {
        const spanAttributes = { [UI_STEP]: true, ...attributes };

        return Effect.withSpan(propagateSpan(effect), name, {
            attributes: spanAttributes,
            annotations: spanAnnotations(spanAttributes),
        });
    };
