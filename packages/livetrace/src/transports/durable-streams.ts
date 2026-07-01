/**
 * Durable Streams TraceTransport.
 *
 * Backed by the `@durable-streams/client` package (HTTP NDJSON, resumable
 * via opaque offsets, multi-pod safe). Events are grouped by `TraceScope`
 * and appended to one stream per scope - typically `trace/{scope.type}/{scope.id}`.
 *
 * Two forms are exported:
 *
 *   1. `DurableStreamsTransportLayer({ baseUrl })`  - ready-made factory
 *      that wires a real `DurableStream` client per scope. Pass the base
 *      URL of your durable-streams server and (optionally) headers / a
 *      custom path mapper. Best DX for most apps.
 *
 *   2. `DurableStreamsAppenderLayer` + `DurableStreamsAppenderTag` +
 *      `StreamResolverTag` - abstract form for users who already wrap
 *      `@durable-streams/client` inside an Effect service (auth, multi-
 *      tenant URL resolution, observability, etc.). Provide your own
 *      appender and resolver and this layer plugs them into the sink.
 *
 * @example Factory form
 * ```ts
 * import { Layer, Logger } from "effect";
 * import { LiveTraceLayer, TraceSinkLive, liveTraceLogger } from "livetrace";
 * import { DurableStreamsTransportLayer } from "livetrace/transports/durable-streams";
 *
 * const TraceLive = LiveTraceLayer.pipe(
 *   Layer.provide(TraceSinkLive({ flushIntervalMs: 100 })),
 *   Layer.provide(DurableStreamsTransportLayer({
 *     baseUrl: process.env.DURABLE_STREAMS_URL!,
 *     headers: { Authorization: () => `Bearer ${getToken()}` },
 *   })),
 *   Layer.provideMerge(Logger.replaceScoped(Logger.defaultLogger, liveTraceLogger)),
 * );
 * ```
 *
 * Requires `@durable-streams/client` as a peer dependency.
 */
import { DurableStream, DurableStreamError, type HeadersRecord } from "@durable-streams/client";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import type { TraceEvent, TraceScope } from "../types.js";

import { TraceTransportTag, type TraceTransport } from "../Sink.js";

const MAX_TRACE_SCOPE_CACHE_ENTRIES = 10_000;

const rememberScope = (scopeByTraceId: Map<string, TraceScope>, traceId: string, scope: TraceScope) => {
    if (!scopeByTraceId.has(traceId) && scopeByTraceId.size >= MAX_TRACE_SCOPE_CACHE_ENTRIES) {
        const oldestTraceId = scopeByTraceId.keys().next().value;
        if (oldestTraceId !== undefined) scopeByTraceId.delete(oldestTraceId);
    }
    scopeByTraceId.set(traceId, scope);
};

const forgetCompletedScopes = (scopeByTraceId: Map<string, TraceScope>, events: ReadonlyArray<TraceEvent>) => {
    for (const e of events) {
        if (e._tag === "TraceEnd") scopeByTraceId.delete(e.traceId);
    }
};

// ============================================================================
// Factory form - wires @durable-streams/client directly
// ============================================================================

export interface DurableStreamsTransportConfig {
    /** Base URL of your durable-streams server. e.g. "https://streams.example.com" */
    readonly baseUrl: string;
    /** Optional path mapper. Default: `trace/{scope.type}/{scope.id}`. */
    readonly scopeToPath?: (scope: TraceScope) => string;
    /** Headers to send with every request. Functions are evaluated per-request. */
    readonly headers?: HeadersRecord;
    /** Custom fetch impl (auth proxies, undici, etc.). Defaults to globalThis.fetch. */
    readonly fetch?: typeof globalThis.fetch;
    /** Optional TTL (seconds) applied when creating a new stream. */
    readonly ttlSeconds?: number;
}

const defaultScopeToPath = (s: TraceScope): string => `trace/${s.type}/${s.id}`;

export function makeDurableStreamsTransport(config: DurableStreamsTransportConfig): TraceTransport {
    const toPath = config.scopeToPath ?? defaultScopeToPath;
    const base = config.baseUrl.replace(/\/$/, "");
    const scopeByTraceId = new Map<string, TraceScope>();
    const handles = new Map<string, Promise<DurableStream>>();

    const handleOpts = (url: string) => ({
        url,
        ...(config.headers ? { headers: config.headers } : {}),
        ...(config.fetch ? { fetch: config.fetch } : {}),
    });

    const getHandle = (scope: TraceScope): Promise<DurableStream> => {
        const path = toPath(scope);
        let pending = handles.get(path);
        if (pending) return pending;
        const url = `${base}/${path.replace(/^\//, "")}`;
        pending = (async () => {
            try {
                return await DurableStream.create({
                    ...handleOpts(url),
                    contentType: "application/x-ndjson",
                    ...(config.ttlSeconds != null ? { ttlSeconds: config.ttlSeconds } : {}),
                });
            } catch (err) {
                if (err instanceof DurableStreamError && err.code === "CONFLICT_EXISTS") {
                    return await DurableStream.connect(handleOpts(url));
                }
                throw err;
            }
        })();
        handles.set(path, pending);
        // Drop the cache entry if create/connect ultimately fails so we retry
        // the next batch instead of permanently swallowing the stream.
        pending.catch(() => {
            if (handles.get(path) === pending) handles.delete(path);
        });
        return pending;
    };

    return {
        send: (events) =>
            Effect.tryPromise({
                try: async () => {
                    try {
                        for (const e of events) {
                            if (e._tag === "TraceStart") rememberScope(scopeByTraceId, e.traceId, e.scope);
                        }

                        const grouped = new Map<string, { scope: TraceScope; batch: TraceEvent[] }>();
                        for (const e of events) {
                            const scope = scopeByTraceId.get(e.traceId);
                            if (!scope) continue;
                            const path = toPath(scope);
                            let entry = grouped.get(path);
                            if (!entry) {
                                entry = { scope, batch: [] };
                                grouped.set(path, entry);
                            }
                            entry.batch.push(e);
                        }

                        await Promise.all(
                            Array.from(grouped.values()).map(async ({ scope, batch }) => {
                                const handle = await getHandle(scope);
                                const ndjson = batch.map((ev) => JSON.stringify(ev)).join("\n") + "\n";
                                await handle.append(ndjson);
                            }),
                        );
                    } finally {
                        forgetCompletedScopes(scopeByTraceId, events);
                    }
                },
                catch: (err) => err,
            }).pipe(
                Effect.catchCause((cause) =>
                    Effect.logDebug("livetrace durable-streams send failed").pipe(Effect.annotateLogs("cause", String(cause))),
                ),
            ),
    };
}

/** Ready-made Effect Layer. Pass the same config as `makeDurableStreamsTransport`. */
export function DurableStreamsTransportLayer(
    config: DurableStreamsTransportConfig,
): Layer.Layer<TraceTransportTag> {
    return Layer.succeed(TraceTransportTag, makeDurableStreamsTransport(config));
}

// ============================================================================
// Abstract form - bring-your-own DurableStreams service wrapper
// ============================================================================

/**
 * Interface for the DurableStreams append capability. Mirrors the shape of
 * a typical service that wraps `@durable-streams/client` with auth, multi-
 * tenant URL resolution, and observability spans.
 */
export interface DurableStreamsAppender {
    readonly appendEvents: (streamId: string, events: ReadonlyArray<Record<string, unknown>>) => Effect.Effect<void>;
}

/** Bridges trace scope → stream ID. Caller decides naming + creation policy. */
export interface StreamResolver {
    readonly getOrCreateStreamId: (scope: TraceScope) => Effect.Effect<string>;
}

export class DurableStreamsAppenderTag extends Context.Service<DurableStreamsAppenderTag, DurableStreamsAppender>()(
    "@livetrace/DurableStreamsAppender",
) {}

export class StreamResolverTag extends Context.Service<StreamResolverTag, StreamResolver>()("@livetrace/StreamResolver") {}

const makeAppenderTransport: Effect.Effect<
    TraceTransport,
    never,
    DurableStreamsAppenderTag | StreamResolverTag
> = Effect.gen(function* () {
    const appender = yield* DurableStreamsAppenderTag;
    const resolver = yield* StreamResolverTag;
    const scopeByTraceId = new Map<string, TraceScope>();

    return {
        send: (events) =>
            Effect.gen(function* () {
                for (const e of events) {
                    if (e._tag === "TraceStart") rememberScope(scopeByTraceId, e.traceId, e.scope);
                }

                const grouped = new Map<string, { scope: TraceScope; batch: TraceEvent[] }>();
                for (const e of events) {
                    const scope = scopeByTraceId.get(e.traceId);
                    if (!scope) continue;
                    const key = `${scope.type}/${scope.id}`;
                    let entry = grouped.get(key);
                    if (!entry) {
                        entry = { scope, batch: [] };
                        grouped.set(key, entry);
                    }
                    entry.batch.push(e);
                }

                yield* Effect.forEach(
                    Array.from(grouped.values()),
                    ({ scope, batch }) =>
                        resolver
                            .getOrCreateStreamId(scope)
                            .pipe(
                                Effect.flatMap((streamId) =>
                                    appender.appendEvents(
                                        streamId,
                                        batch.map((ev) => ({ ...ev }) as Record<string, unknown>),
                                    ),
                                ),
                            ),
                    { discard: true, concurrency: "unbounded" },
                );
            }).pipe(
                Effect.ensuring(Effect.sync(() => forgetCompletedScopes(scopeByTraceId, events))),
                Effect.catchCause((cause) =>
                    Effect.logDebug("livetrace durable-streams (appender) send failed").pipe(
                        Effect.annotateLogs("cause", String(cause)),
                    ),
                ),
            ),
    };
});

/**
 * Layer form of the abstract transport. Requires `DurableStreamsAppenderTag`
 * and `StreamResolverTag` in the environment - provide them from your own
 * service implementation.
 */
export const DurableStreamsAppenderLayer: Layer.Layer<
    TraceTransportTag,
    never,
    DurableStreamsAppenderTag | StreamResolverTag
> = Layer.effect(TraceTransportTag, makeAppenderTransport);
