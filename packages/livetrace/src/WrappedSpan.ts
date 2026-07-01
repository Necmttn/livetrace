/**
 * WrappedSpan — Span decorator that intercepts lifecycle events.
 *
 * Implements the full Tracer.Span interface by delegating to an inner span
 * (OTel or native) while emitting TraceEvents to a TraceSink.
 *
 * Detection: Use `isWrappedSpan(span)` to check if a span is wrapped.
 * The Symbol brand avoids instanceof checks across package boundaries.
 */
import type { Context } from "effect/Context";
import type { Exit } from "effect/Exit";
import type { Option } from "effect/Option";
import type { AnySpan, Span, SpanKind, SpanLink, SpanStatus } from "effect/Tracer";

import type { TraceSinkHandle } from "./Sink.js";
import type { TraceScope } from "./types.js";

import { TRACE_INTERNAL } from "./types.js";

export const LiveTraceSymbol: unique symbol = Symbol.for("@livetrace/WrappedSpan");

export class WrappedSpan implements Span {
    readonly _tag = "Span" as const;
    readonly [LiveTraceSymbol] = true;

    /** The logical trace ID for routing (e.g., "doc:abc123") */
    readonly liveTraceId: string;

    /** Scope for stream routing */
    readonly liveScope: TraceScope;

    constructor(
        readonly inner: Span,
        readonly sink: TraceSinkHandle,
        liveTraceId: string,
        liveScope: TraceScope,
    ) {
        this.liveTraceId = liveTraceId;
        this.liveScope = liveScope;
    }

    // -- Delegated reads --

    get name(): string {
        return this.inner.name;
    }
    get spanId(): string {
        return this.inner.spanId;
    }
    get traceId(): string {
        return this.inner.traceId;
    }
    get parent(): Option<AnySpan> {
        return this.inner.parent;
    }
    get annotations(): Context<never> {
        return this.inner.annotations;
    }
    get status(): SpanStatus {
        return this.inner.status;
    }
    get attributes(): ReadonlyMap<string, unknown> {
        return this.inner.attributes;
    }
    get links(): ReadonlyArray<SpanLink> {
        return this.inner.links;
    }
    get sampled(): boolean {
        return this.inner.sampled;
    }
    get kind(): SpanKind {
        return this.inner.kind;
    }

    // -- Intercepted mutations --

    attribute(key: string, value: unknown): void {
        this.inner.attribute(key, value);
    }

    event(name: string, startTime: bigint, attributes?: Record<string, unknown>): void {
        this.inner.event(name, startTime, attributes);

        // Emit as SpanEvent to sink
        const level = attributes?.["effect.logLevel"] as string | undefined;
        this.sink.emit({
            _tag: "SpanEvent",
            traceId: this.liveTraceId,
            spanId: this.spanId,
            name,
            level: normalizeLevel(level),
            attributes,
            timestamp: Date.now(),
        });
    }

    end(endTime: bigint, exit: Exit<unknown, unknown>): void {
        this.inner.end(endTime, exit);

        const startTime = this.inner.status._tag === "Ended" ? this.inner.status.startTime : BigInt(0);
        const durationMs = Number(endTime - startTime) / 1_000_000;
        const status = exit._tag === "Success" ? ("ok" as const) : ("error" as const);

        this.sink.emit({
            _tag: "SpanEnd",
            traceId: this.liveTraceId,
            spanId: this.spanId,
            status,
            durationMs,
            timestamp: Date.now(),
        });
    }

    addLinks(links: ReadonlyArray<SpanLink>): void {
        this.inner.addLinks(links);
    }
}

export const isWrappedSpan = (span: AnySpan): span is WrappedSpan => LiveTraceSymbol in span;

export const shouldExclude = (attributes?: Record<string, unknown>): boolean => attributes?.[TRACE_INTERNAL] === true;

function normalizeLevel(level: string | undefined): "Debug" | "Info" | "Warning" | "Error" | undefined {
    if (!level) return undefined;
    switch (level) {
        case "DEBUG":
        case "Debug":
            return "Debug";
        case "INFO":
        case "Info":
            return "Info";
        case "WARNING":
        case "WARN":
        case "Warning":
        case "Warn":
            return "Warning";
        case "ERROR":
        case "Error":
            return "Error";
        default:
            return undefined;
    }
}
