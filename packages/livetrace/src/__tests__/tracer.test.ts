import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { describe, expect, it } from "vitest";

import type { TraceEvent } from "../types.js";

import { withTrace, step } from "../LiveTrace.js";
import { TraceSink, TraceSinkLive, TraceTransportTag } from "../Sink.js";
import { LiveTraceLayer } from "../Tracer.js";
import {
    DurableStreamsAppenderLayer,
    DurableStreamsAppenderTag,
    StreamResolverTag,
} from "../transports/durable-streams.js";
import { spanEvent, traceStart } from "../types.js";

/**
 * Creates a test transport that collects events in an array.
 * Returns the layer and a reference to the collected events.
 */
function makeTestTransport() {
    const events: TraceEvent[] = [];
    const transport = {
        send: (batch: ReadonlyArray<TraceEvent>) =>
            Effect.sync(() => {
                events.push(...batch);
            }),
    };
    const layer = Layer.succeed(TraceTransportTag, transport);
    return { events, layer };
}

/**
 * Helper: build the full test layer (TraceSink + LiveTraceLayer)
 * with a zero-interval flush for deterministic tests.
 */
function makeTestLayer(transportLayer: Layer.Layer<TraceTransportTag>) {
    const sinkLayer = TraceSinkLive({ flushIntervalMs: 10 }).pipe(Layer.provide(transportLayer));
    const traceLayer = LiveTraceLayer.pipe(Layer.provide(sinkLayer));
    return Layer.merge(sinkLayer, traceLayer);
}

describe("LiveTraceLayer", () => {
    it("captures TraceStart, SpanStart, SpanEnd, TraceEnd for a traced scope", async () => {
        const { events, layer: transportLayer } = makeTestTransport();
        const testLayer = makeTestLayer(transportLayer);

        const program = withTrace({
            traceId: "test:123",
            label: "Test Workflow",
            scope: { type: "team", id: "team-1" },
        })(Effect.void);

        await Effect.runPromise(
            program.pipe(
                Effect.provide(testLayer),
                Effect.scoped,
                // Give flush daemon time to fire
                Effect.tap(() => Effect.sleep("50 millis")),
            ),
        );

        const tags = events.map((e) => e._tag);
        expect(tags).toContain("TraceStart");
        expect(tags).toContain("SpanStart");
        expect(tags).toContain("SpanEnd");
        expect(tags).toContain("TraceEnd");

        // Verify TraceStart has correct metadata
        const traceStart = events.find((e) => e._tag === "TraceStart");
        expect(traceStart).toMatchObject({
            _tag: "TraceStart",
            traceId: "test:123",
            label: "Test Workflow",
            scope: { type: "team", id: "team-1" },
        });

        // Verify TraceEnd
        const traceEnd = events.find((e) => e._tag === "TraceEnd");
        expect(traceEnd).toMatchObject({
            _tag: "TraceEnd",
            traceId: "test:123",
            status: "completed",
        });
    });

    it("captures nested child spans with parent-child relationships", async () => {
        const { events, layer: transportLayer } = makeTestTransport();
        const testLayer = makeTestLayer(transportLayer);

        const program = withTrace({
            traceId: "test:nested",
            label: "Nested Test",
            scope: { type: "team", id: "team-1" },
        })(
            Effect.gen(function* () {
                yield* Effect.withSpan(Effect.void, "child-1");
                yield* Effect.withSpan(Effect.void, "child-2");
            }),
        );

        await Effect.runPromise(
            program.pipe(
                Effect.provide(testLayer),
                Effect.scoped,
                Effect.tap(() => Effect.sleep("50 millis")),
            ),
        );

        const spanStarts = events.filter((e) => e._tag === "SpanStart");
        const spanEnds = events.filter((e) => e._tag === "SpanEnd");

        // Root span + 2 children = 3 SpanStarts
        expect(spanStarts.length).toBe(3);
        expect(spanEnds.length).toBe(3);

        // Children should reference root's spanId as parentSpanId
        const rootSpan = spanStarts.find((e) => e._tag === "SpanStart" && e.name === "Nested Test");
        const child1 = spanStarts.find((e) => e._tag === "SpanStart" && e.name === "child-1");
        const child2 = spanStarts.find((e) => e._tag === "SpanStart" && e.name === "child-2");

        expect(rootSpan).toBeDefined();
        expect(child1).toBeDefined();
        expect(child2).toBeDefined();

        if (child1?._tag === "SpanStart" && rootSpan?._tag === "SpanStart") {
            expect(child1.parentSpanId).toBe(rootSpan.spanId);
        }
        if (child2?._tag === "SpanStart" && rootSpan?._tag === "SpanStart") {
            expect(child2.parentSpanId).toBe(rootSpan.spanId);
        }
    });

    it("captures Effect.log calls as SpanEvents via built-in tracerLogger", async () => {
        const { events, layer: transportLayer } = makeTestTransport();
        const testLayer = makeTestLayer(transportLayer);

        const program = withTrace({
            traceId: "test:logs",
            label: "Log Test",
            scope: { type: "team", id: "team-1" },
        })(
            Effect.gen(function* () {
                yield* Effect.logInfo("Hello from traced scope");
                yield* Effect.logWarning("A warning");
            }),
        );

        await Effect.runPromise(
            program.pipe(
                Effect.provide(testLayer),
                Effect.scoped,
                Effect.tap(() => Effect.sleep("50 millis")),
            ),
        );

        const spanEvents = events.filter((e) => e._tag === "SpanEvent");
        expect(spanEvents.length).toBeGreaterThanOrEqual(2);

        const infoEvent = spanEvents.find((e) => e._tag === "SpanEvent" && e.name.includes("Hello from traced scope"));
        expect(infoEvent).toBeDefined();
    });

    it("does not capture spans outside a traced scope", async () => {
        const { events, layer: transportLayer } = makeTestTransport();
        const testLayer = makeTestLayer(transportLayer);

        const program = Effect.withSpan(Effect.void, "outside-span");

        await Effect.runPromise(
            program.pipe(
                Effect.provide(testLayer),
                Effect.scoped,
                Effect.tap(() => Effect.sleep("50 millis")),
            ),
        );

        // No trace events should be emitted
        expect(events.length).toBe(0);
    });

    it("LiveTrace.step creates ui.step attributed spans", async () => {
        const { events, layer: transportLayer } = makeTestTransport();
        const testLayer = makeTestLayer(transportLayer);

        const program = withTrace({
            traceId: "test:steps",
            label: "Step Test",
            scope: { type: "team", id: "team-1" },
        })(
            Effect.gen(function* () {
                yield* step("Parsing")(Effect.void);
                yield* step("Embedding")(Effect.void);
            }),
        );

        await Effect.runPromise(
            program.pipe(
                Effect.provide(testLayer),
                Effect.scoped,
                Effect.tap(() => Effect.sleep("50 millis")),
            ),
        );

        const spanStarts = events.filter((e) => e._tag === "SpanStart");
        const parsingSpan = spanStarts.find((e) => e._tag === "SpanStart" && e.name === "Parsing");
        const embeddingSpan = spanStarts.find((e) => e._tag === "SpanStart" && e.name === "Embedding");

        expect(parsingSpan).toBeDefined();
        expect(embeddingSpan).toBeDefined();
    });

    it("handles failed traces correctly", async () => {
        const { events, layer: transportLayer } = makeTestTransport();
        const testLayer = makeTestLayer(transportLayer);

        const program = withTrace({
            traceId: "test:fail",
            label: "Fail Test",
            scope: { type: "team", id: "team-1" },
        })(Effect.fail("boom"));

        await Effect.runPromise(
            program.pipe(
                Effect.provide(testLayer),
                Effect.scoped,
                Effect.tap(() => Effect.sleep("50 millis")),
                Effect.catchAll(() => Effect.void),
            ),
        );

        const traceEnd = events.find((e) => e._tag === "TraceEnd");
        expect(traceEnd).toMatchObject({
            _tag: "TraceEnd",
            traceId: "test:fail",
            status: "failed",
        });

        const spanEnd = events.find((e) => e._tag === "SpanEnd");
        expect(spanEnd).toMatchObject({
            status: "error",
        });
    });
});

describe("TraceSinkLive", () => {
    it("bounds buffered events while the transport is stalled", async () => {
        let releaseFirstSend: (() => void) | undefined;
        let firstSendStarted: (() => void) | undefined;
        const firstSendStartedPromise = new Promise<void>((resolve) => {
            firstSendStarted = resolve;
        });
        const releaseFirstSendPromise = new Promise<void>((resolve) => {
            releaseFirstSend = resolve;
        });

        const batches: ReadonlyArray<TraceEvent>[] = [];
        let sendCount = 0;
        const transport = {
            send: (batch: ReadonlyArray<TraceEvent>) =>
                Effect.promise(async () => {
                    batches.push([...batch]);
                    sendCount += 1;
                    if (sendCount === 1) {
                        firstSendStarted?.();
                        await releaseFirstSendPromise;
                    }
                }),
        };

        const layer = TraceSinkLive({ flushIntervalMs: 10, maxBufferEvents: 5 }).pipe(
            Layer.provide(Layer.succeed(TraceTransportTag, transport)),
        );

        await Effect.runPromise(
            Effect.gen(function* () {
                const sink = yield* TraceSink;
                sink.emit(traceStart("blocked-flush", "Blocked flush", { type: "team", id: "team-1" }));

                yield* Effect.promise(() => firstSendStartedPromise);

                for (let i = 0; i < 20; i += 1) {
                    sink.emit(spanEvent("blocked-flush", `span-${i}`, `event-${i}`));
                }

                releaseFirstSend?.();
                yield* Effect.sleep("100 millis");
            }).pipe(Effect.provide(layer), Effect.scoped),
        );

        const postStallBatches = batches.slice(1);
        expect(postStallBatches.length).toBeGreaterThanOrEqual(1);
        for (const batch of postStallBatches) {
            expect(batch.length).toBeLessThanOrEqual(5);
        }
        expect(postStallBatches[0]!.map((event) => ("name" in event ? event.name : event._tag))).toEqual([
            "event-15",
            "event-16",
            "event-17",
            "event-18",
            "event-19",
        ]);
    });
});

describe("DurableStreamsAppenderLayer", () => {
    it("evicts the oldest trace scopes when the scope cache reaches its cap", async () => {
        const appended: ReadonlyArray<Record<string, unknown>>[] = [];
        const appender = {
            appendEvents: (_streamId: string, events: ReadonlyArray<Record<string, unknown>>) =>
                Effect.sync(() => {
                    appended.push([...events]);
                }),
        };
        const resolver = {
            getOrCreateStreamId: () => Effect.succeed("trace/team/team-1"),
        };
        const supportLayer = Layer.merge(
            Layer.succeed(DurableStreamsAppenderTag, appender),
            Layer.succeed(StreamResolverTag, resolver),
        );
        const layer = DurableStreamsAppenderLayer.pipe(Layer.provide(supportLayer));

        await Effect.runPromise(
            Effect.gen(function* () {
                const transport = yield* TraceTransportTag;
                const events = Array.from({ length: 10_001 }, (_, i) =>
                    traceStart(`trace-${i}`, `Trace ${i}`, { type: "team", id: "team-1" }),
                );

                yield* transport.send(events);
            }).pipe(Effect.provide(layer)),
        );

        expect(appended).toHaveLength(1);
        const traceIds = appended.flatMap((batch) => batch.map((event) => event["traceId"]));
        expect(traceIds).toHaveLength(10_000);
        expect(traceIds[0]).toBe("trace-1");
        expect(traceIds.at(-1)).toBe("trace-10000");
        expect(traceIds).not.toContain("trace-0");
    });
});
