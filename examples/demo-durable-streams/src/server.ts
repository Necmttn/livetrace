/**
 * Demo server (Durable Streams).
 *
 * Boots two things in the same process:
 *
 *   1. A `DurableStreamTestServer` on :4437 - the reference durable-streams
 *      server. In production you'd point at https://durablestreams.com or
 *      a self-hosted Caddy deployment instead.
 *   2. A Bun HTTP app on :8789 that runs the Effect workflow. The
 *      `DurableStreamsTransportLayer` appends every trace batch to a
 *      per-scope durable stream (`trace/user/<id>` by default).
 *
 * The browser reads the stream back directly via `@durable-streams/client`
 * (proxied through Vite at `/ds`) - no in-process broker involved.
 *
 *   POST /run?scope=<scopeId>&fail=<bool>   → kicks off a workflow
 */
import { DurableStreamTestServer } from "@durable-streams/server";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";

import { LiveTraceLayer, TraceSinkLive, liveTraceLogger } from "livetrace";
import { DurableStreamsTransportLayer } from "livetrace/transports/durable-streams";

import { runWorkflow } from "./workflow.js";

const DS_PORT = Number(process.env.DS_PORT ?? 4437);
const APP_PORT = Number(process.env.PORT ?? 8789);

const ds = new DurableStreamTestServer({ port: DS_PORT, host: "127.0.0.1" });
await ds.start();
// eslint-disable-next-line no-console
console.log(`[demo-ds] durable-streams server ${ds.url}`);

const TraceLive = LiveTraceLayer.pipe(
    Layer.provide(TraceSinkLive({ flushIntervalMs: 100 })),
    Layer.provide(DurableStreamsTransportLayer({ baseUrl: ds.url })),
);

const LoggerLive = Logger.layer([liveTraceLogger], { mergeWithExisting: true });
const Runtime = Layer.merge(TraceLive, LoggerLive);

Bun.serve({
    port: APP_PORT,
    async fetch(req) {
        const url = new URL(req.url);

        if (req.method === "OPTIONS") {
            return new Response(null, {
                headers: {
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
                    "Access-Control-Allow-Headers": "Content-Type",
                },
            });
        }

        if (url.pathname === "/run" && req.method === "POST") {
            const scope = url.searchParams.get("scope") ?? "demo-user";
            const fail = url.searchParams.get("fail") === "true";
            const docId = url.searchParams.get("doc") ?? `report-${Date.now() % 10000}.pdf`;

            void Effect.runPromise(
                runWorkflow({ docId, scopeId: scope, fail }).pipe(
                    Effect.provide(Runtime),
                    Effect.scoped,
                    Effect.catchCause(() => Effect.void),
                ),
            );

            return new Response(JSON.stringify({ ok: true, traceScope: scope, docId }), {
                headers: {
                    "Content-Type": "application/json",
                    "Access-Control-Allow-Origin": "*",
                },
            });
        }

        return new Response("not found", { status: 404 });
    },
});

// eslint-disable-next-line no-console
console.log(`[demo-ds] app server http://localhost:${APP_PORT}`);
