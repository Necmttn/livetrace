/**
 * LiveTraceLogger — Bridges Effect.log() → SpanEvent automatically.
 *
 * When inside a `withTrace()` or `step()` scope, this Logger reads
 * the current WrappedSpan from LiveSpanRef and calls span.event()
 * so the log message appears in the trace card's step event list.
 *
 * Outside a traced scope, this Logger is a no-op (other loggers
 * like Logger.pretty still handle the log normally).
 *
 * Wire via `Logger.add(liveTraceLogger)` in the services layer.
 */
import * as Logger from "effect/Logger";
import * as References from "effect/References";

import * as FiberRef from "./effect-fiber-ref.js";
import { LiveSpanRef } from "./LiveTrace.js";
import { isWrappedSpan } from "./WrappedSpan.js";

export const liveTraceLogger = Logger.make(({ message, logLevel, fiber }) => {
    const span = FiberRef.unsafeGet(LiveSpanRef);
    if (!span || !isWrappedSpan(span)) return;

    // Flatten message to string
    const msg = Array.isArray(message) ? message.join(" ") : String(message);

    const attrs: Record<string, unknown> = { "effect.logLevel": logLevel };
    const annotations = fiber.getRef(References.CurrentLogAnnotations);
    for (const [k, v] of Object.entries(annotations)) {
        attrs[k] = v;
    }

    span.event(msg, BigInt(Date.now()) * 1_000_000n, attrs);
});
