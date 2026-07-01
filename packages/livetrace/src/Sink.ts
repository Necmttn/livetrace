/**
 * TraceSink — Buffered event sink with pluggable transport.
 *
 * The sink has two parts:
 * - TraceSinkHandle: synchronous emit() for use inside Tracer.span() (which is sync)
 * - TraceSink: Effect service that manages the handle + flush daemon
 *
 * TraceTransport: pluggable backend (Durable Streams, SSE, WebSocket, console, etc.)
 */
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schedule from "effect/Schedule";

import type { TraceEvent } from "./types.js";

// ============================================================================
// TraceTransport — pluggable destination
// ============================================================================

export interface TraceTransport {
    /** Send a batch of events. Called periodically by the flush daemon. */
    readonly send: (events: ReadonlyArray<TraceEvent>) => Effect.Effect<void>;
}

export class TraceTransportTag extends Context.Service<TraceTransportTag, TraceTransport>()("@livetrace/TraceTransport") {}

// ============================================================================
// TraceSinkHandle — the synchronous interface used by WrappedSpan
// ============================================================================

export interface TraceSinkHandle {
    /** Synchronous, non-blocking. Buffers internally. */
    readonly emit: (event: TraceEvent) => void;
}

// ============================================================================
// TraceSink — Effect service managing buffer + flush lifecycle
// ============================================================================

export class TraceSink extends Context.Service<TraceSink, TraceSinkHandle>()("@livetrace/TraceSink") {}

// ============================================================================
// Layer — creates a TraceSink backed by a TraceTransport
// ============================================================================

export interface TraceSinkConfig {
    /** Flush interval in milliseconds. Default: 200 */
    readonly flushIntervalMs?: number;
    /**
     * Maximum number of events buffered while the transport is unavailable or
     * slower than producers. Newer events are retained when the cap is reached.
     */
    readonly maxBufferEvents?: number;
}

const DEFAULT_MAX_BUFFER_EVENTS = 10_000;

export const TraceSinkLive = (config?: TraceSinkConfig): Layer.Layer<TraceSink, never, TraceTransportTag> =>
    Layer.effect(
        TraceSink,
        Effect.gen(function* () {
            const transport = yield* TraceTransportTag;
            const intervalMs = config?.flushIntervalMs ?? 200;
            const configuredMaxBufferEvents = config?.maxBufferEvents ?? DEFAULT_MAX_BUFFER_EVENTS;
            const maxBufferEvents = Number.isFinite(configuredMaxBufferEvents)
                ? Math.max(1, Math.trunc(configuredMaxBufferEvents))
                : DEFAULT_MAX_BUFFER_EVENTS;

            let buffer: TraceEvent[] = [];

            const flush = Effect.suspend(() => {
                if (buffer.length === 0) return Effect.void;
                const batch = buffer;
                buffer = [];
                return transport.send(batch);
            });

            // Daemon fiber: flush every intervalMs
            yield* flush.pipe(
                Effect.schedule(Schedule.spaced(intervalMs)),
                Effect.catchCause((cause) => Effect.logDebug("livetrace flush daemon error").pipe(Effect.annotateLogs("cause", String(cause)))),
                Effect.forkScoped,
            );

            // Final flush on scope close
            yield* Effect.addFinalizer(() =>
                flush.pipe(
                    Effect.catchCause((cause) =>
                        Effect.logDebug("livetrace finalizer flush error").pipe(Effect.annotateLogs("cause", String(cause))),
                    ),
                ),
            );

            const handle: TraceSinkHandle = {
                emit: (event) => {
                    buffer.push(event);
                    if (buffer.length > maxBufferEvents) {
                        buffer.splice(0, buffer.length - maxBufferEvents);
                    }
                },
            };

            return handle;
        }),
    );

// ============================================================================
// Console transport — for development/debugging
// ============================================================================

export const ConsoleTransport: TraceTransport = {
    send: (events) =>
        Effect.sync(() => {
            for (const event of events) {
                // eslint-disable-next-line no-console
                console.log(`[live-trace] ${event._tag}`, JSON.stringify(event));
            }
        }),
};

export const ConsoleTransportLayer: Layer.Layer<TraceTransportTag> = Layer.succeed(TraceTransportTag, ConsoleTransport);
